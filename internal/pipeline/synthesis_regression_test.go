package pipeline

import (
	"context"
	"fmt"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/require"
)

type attemptOnlyConverse struct {
	llm.Converser
	llm.AttemptTokenCounter
}

func (attemptOnlyConverse) AWSRegion() string { return "us-east-1" }

type attemptOnlyModelConverse struct {
	llm.ModelConverser
	llm.AttemptTokenCounter
}

func (attemptOnlyModelConverse) AWSRegion() string { return "us-east-1" }

func TestStrictSynthesisRejectsPartialAndMalformedAdaptersBeforeFrames(t *testing.T) {
	for _, traced := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			result  llm.CountResult
			adapter string
			stream  bool
			wantErr error
		}{
			{"plain", llm.CountResult{}, "plain", true, ErrSynthesisProfileUnsupported},
			{"no streaming", llm.CountResult{}, "model", true, ErrSynthesisProfileUnsupported},
			{"attempt-only count with model-aware JSON", llm.CountResult{InputTokens: 42, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, "model", false, nil},
			{"other provider's method", llm.CountResult{InputTokens: 42, Attempts: 1, Method: llm.CountMethodOpenAICl100kChat}, "full", true, llm.ErrInvalidTokenCount},
			{"unaccounted success", llm.CountResult{InputTokens: 42, Method: llm.CountMethodBedrockCountTokens}, "full", true, llm.ErrInvalidTokenCount},
			{"beyond attempt limit", llm.CountResult{InputTokens: 42, Attempts: 4, Method: llm.CountMethodBedrockCountTokens}, "full", true, llm.ErrCountAttemptsExceeded},
		} {
			t.Run(tc.name+fmt.Sprintf("/traced=%t", traced), func(t *testing.T) {
				cfg := &config.Config{AWSRegion: "us-east-1", BedrockFallbackModel: testBedrockFallback, SynthesisInputProfile: "strict"}
				amb := llm.NewAmbassador(cfg)
				double := &calibratedBedrockDouble{countFn: func(context.Context, string, string, []brtypes.Message, int) (llm.CountResult, error) {
					return tc.result, nil
				}}
				var adapter llm.Converser = double
				switch tc.adapter {
				case "plain":
					adapter = attemptOnlyConverse{double, double}
				case "model":
					adapter = attemptOnlyModelConverse{double, double}
				}
				if traced {
					adapter = tracing.WrapConverser(adapter, "aws.bedrock", testBedrockFallback)
				}
				amb.SetClient(adapter)
				started := false
				qc := &QueryContext{OriginalQuery: "Q?", RetrievedChunks: []domain.ScoredChunk{{Chunk: domain.Chunk{Text: "evidence"}, Score: 1}}}
				if tc.stream {
					qc.Stream = &QueryStream{Context: func(context.Context) error { started = true; return nil }, Delta: func(context.Context, string) error { return nil }}
				}
				err := NewSynthesisStage(amb, cfg).Run(t.Context(), qc)
				if tc.wantErr == nil {
					require.NoError(t, err)
					require.Equal(t, 1, double.converseCalls)
				} else {
					require.ErrorIs(t, err, tc.wantErr)
					require.False(t, started)
					require.Zero(t, double.converseCalls)
				}
			})
		}
	}
}

func TestStrictLocalNonmonotoneExactFit(t *testing.T) {
	server := httptest.NewServer(nil)
	t.Cleanup(server.Close)
	const model = "gpt-4-0613"
	cfg := &config.Config{
		LLMProvider: "openai", OpenAIBaseURL: server.URL, OpenAIChatModel: model, SynthesisInputProfile: "strict",
		SynthesisOpenAIDeployment: "chatml-v1|" + model + "|" + server.URL,
	}
	amb := llm.NewAmbassador(cfg)
	client, err := llm.NewOpenAIClient(cfg)
	require.NoError(t, err)
	amb.SetGenerator(client)
	builder := synthesisPromptBuilder{
		question: strings.Repeat("㐀", 2322) + "Q?", boundary: "0123456789abcdef", modelID: model, maxTokens: 1024,
	}
	chunks := []domain.ScoredChunk{{Chunk: domain.Chunk{Text: "删除xyz"}}}
	for _, tc := range []struct {
		text string
		want int
	}{{"删", 7169}, {"删除", 7168}, {"删除x", 7169}, {"删除xyz", 7169}} {
		req, _, _ := builder.build([]domain.ScoredChunk{{Chunk: domain.Chunk{Text: tc.text}}})
		got, _, countErr := llm.CountOpenAIChatTokens(model, req)
		require.NoError(t, countErr)
		require.Equal(t, tc.want, got, tc.text)
	}
	out, err := budgetLocalStrictInput(t.Context(), amb, cfg, builder, 7168, chunks, []bool{false})
	require.NoError(t, err)
	require.Equal(t, "删除", out.chunks[0].Chunk.Text)
	require.Equal(t, 8192, out.count.InputTokens+out.request.MaxTokens)
}

func TestStrictLocalSelectionWithDistinctFixedBoundaries(t *testing.T) {
	server := httptest.NewServer(nil)
	t.Cleanup(server.Close)
	const model = "gpt-4-0613"
	cfg := &config.Config{
		LLMProvider: "openai", OpenAIBaseURL: server.URL, OpenAIChatModel: model, SynthesisInputProfile: "strict",
		SynthesisOpenAIDeployment: "chatml-v1|" + model + "|" + server.URL,
	}
	amb := llm.NewAmbassador(cfg)
	client, err := llm.NewOpenAIClient(cfg)
	require.NoError(t, err)
	amb.SetGenerator(client)
	chunks := []domain.ScoredChunk{
		{Chunk: domain.Chunk{Text: strings.Repeat("\u088f.com", 1000)}},
		{Chunk: domain.Chunk{Text: strings.Repeat("beta ", 4500)}},
	}
	var countedContents []string
	for _, boundary := range []string{"000123456789012345678", "019987654321098765432"} {
		builder := synthesisPromptBuilder{question: "Summarize alpha", boundary: boundary, modelID: model, maxTokens: 1024}
		out, budgetErr := budgetLocalStrictInput(t.Context(), amb, cfg, builder, 7168, chunks, []bool{false, false})
		require.NoError(t, budgetErr)
		require.Equal(t, 8192, out.count.InputTokens+out.request.MaxTokens)
		require.Len(t, out.chunks, 2)
		require.Equal(t, strings.Repeat("beta ", 1929)+"beta", out.chunks[1].Chunk.Text)
		content := out.request.Messages[0].Content[0].Text
		require.Contains(t, content, boundary)
		countedContents = append(countedContents, strings.ReplaceAll(content, boundary, ""))
	}
	require.Equal(t, countedContents[0], countedContents[1])
}

func TestStrictRemoteFitsHeterogeneousAndMinimalEvidence(t *testing.T) {
	bodyPattern := regexp.MustCompile(`(?s)<excerpt-[^ ]+ id="\d+">\n(.*?)\n</excerpt-`)
	for _, tc := range []struct {
		name          string
		chunks        []domain.ScoredChunk
		fixed, limit  int
		firstAttempts int
	}{
		{name: "heterogeneous", chunks: heterogeneousBudgetChunks(), fixed: 180, limit: 198976, firstAttempts: 1},
		{name: "retry leaves room for one rune", chunks: []domain.ScoredChunk{{Chunk: domain.Chunk{Text: strings.Repeat("界", 134)}}}, fixed: 198943, limit: 198976, firstAttempts: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{AWSRegion: "us-east-1", SynthesisInputProfile: "strict"}
			amb := llm.NewAmbassador(cfg)
			totalAttempts := 0
			double := &calibratedBedrockDouble{
				countFn: func(_ context.Context, _, _ string, messages []brtypes.Message, maxAttempts int) (llm.CountResult, error) {
					attempts := 1
					if totalAttempts == 0 {
						attempts = tc.firstAttempts
					}
					require.LessOrEqual(t, attempts, maxAttempts)
					totalAttempts += attempts
					total := tc.fixed
					for _, m := range bodyPattern.FindAllStringSubmatch(flattenBedrockMessages(messages), -1) {
						total += 30
						if len(m[1]) == utf8.RuneCountInString(m[1]) {
							total += (len(m[1]) + 7) / 8
						} else {
							total += 3 * utf8.RuneCountInString(m[1])
						}
					}
					return llm.CountResult{InputTokens: total, Attempts: attempts, Method: llm.CountMethodBedrockCountTokens}, nil
				},
			}
			amb.SetClient(double)
			builder := synthesisPromptBuilder{question: "Question?", boundary: "0123456789abcdef", modelID: testBedrockFallback, maxTokens: 1024}
			out, err := budgetRemoteStrictInput(t.Context(), amb, cfg, builder, tc.limit, tc.chunks, make([]bool, len(tc.chunks)))
			require.NoError(t, err)
			require.NotEmpty(t, out.chunks)
			require.LessOrEqual(t, out.count.InputTokens, tc.limit)
			require.LessOrEqual(t, totalAttempts, 3)
			if tc.firstAttempts == 2 {
				require.Equal(t, "界", out.chunks[0].Chunk.Text)
			} else {
				require.GreaterOrEqual(t, len(out.chunks), 8)
			}
		})
	}
}

func heterogeneousBudgetChunks() []domain.ScoredChunk {
	chunks := make([]domain.ScoredChunk, 18)
	for i := range chunks {
		text := strings.Repeat("a", 28000)
		if i < 11 {
			text = strings.Repeat("界", 8000)
		}
		chunks[i] = domain.ScoredChunk{Chunk: domain.Chunk{Text: text}}
	}
	return chunks
}

func TestStrictHostedSendsApplyBothEvidenceChecks(t *testing.T) {
	scopeChange := func(row domain.ScoredChunk) domain.ScoredChunk {
		row.Chunk.Metadata = map[string]any{"collection": "archive", "doc_type": "report"}
		return row
	}
	bodyChange := func(row domain.ScoredChunk) domain.ScoredChunk {
		row.Chunk.Text += " Revised."
		return row
	}
	for _, tc := range []struct {
		name, when string
		drift      func(domain.ScoredChunk) domain.ScoredChunk
		sent       int
		published  bool
	}{
		{"stable evidence", "never", scopeChange, 1, true},
		{"candidate scope changes after counting", "count", scopeChange, 0, false},
		{"excerpt body changes after counting", "count", bodyChange, 0, false},
		{"candidate scope changes after context", "context", scopeChange, 0, true},
		{"excerpt body changes after context", "context", bodyChange, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				LLMProvider: "bedrock", AWSRegion: "us-east-1", BedrockFallbackModel: testBedrockFallback,
				SynthesisInputProfile: "strict", SynthesisContextTokens: 2400, SynthesisTimeout: 5 * time.Second,
			}
			changed := false
			double := &calibratedBedrockDouble{countFn: func(ctx context.Context, _ string, _ string, _ []brtypes.Message, _ int) (llm.CountResult, error) {
				if err := domain.CheckEvidenceBeforeSend(ctx); err != nil {
					return llm.CountResult{Attempts: 1}, err
				}
				changed = tc.when == "count"
				return llm.CountResult{InputTokens: 100, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
			}}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(double)
			published := false
			qc := &QueryContext{
				OriginalQuery: "hours?", HostedVisibilityRefill: true,
				RetrievedChunks: []domain.ScoredChunk{refillHit("a", "doc", "Alpha.", .95)},
				EvidenceVisibility: currentChunks{check: func(hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
					current := slices.Clone(hits)
					if changed {
						for i := range current {
							current[i] = tc.drift(current[i])
						}
					}
					return current, nil
				}},
				Stream: &QueryStream{Context: func(context.Context) error {
					published, changed = true, changed || tc.when == "context"
					return nil
				}, Delta: func(context.Context, string) error { return nil }},
			}
			err := NewSynthesisStage(amb, cfg).Run(t.Context(), qc)
			require.Equal(t, tc.sent, double.converseCalls)
			require.Equal(t, tc.published, published)
			if tc.sent == 1 {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrEvidenceScopeChanged)
			require.ErrorIs(t, err, domain.ErrEvidenceCheck)
		})
	}
}

func TestStrictRemoteOverflowCannotAcceptEmptyFallback(t *testing.T) {
	cfg := &config.Config{AWSRegion: "us-east-1", SynthesisInputProfile: "strict"}
	amb := llm.NewAmbassador(cfg)
	double := &calibratedBedrockDouble{
		countFn: func(_ context.Context, _, _ string, messages []brtypes.Message, _ int) (llm.CountResult, error) {
			tokens := 994
			if strings.Contains(flattenBedrockMessages(messages), `id="1"`) {
				tokens = 1200
			}
			return llm.CountResult{InputTokens: tokens, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
		},
	}
	amb.SetClient(double)
	builder := synthesisPromptBuilder{question: "Q?", boundary: "0123456789abcdef", modelID: testBedrockFallback, maxTokens: 1024}
	chunks := []domain.ScoredChunk{{Chunk: domain.Chunk{Text: " \t"}}, {Chunk: domain.Chunk{Text: "\n"}}}
	out, err := budgetRemoteStrictInput(t.Context(), amb, cfg, builder, 1000, chunks, []bool{false, false})
	require.ErrorIs(t, err, ErrSynthesisInputTooLarge)
	require.Empty(t, out.chunks)
	require.Empty(t, out.truncated)
	require.Equal(t, 1, double.countCalls)
	require.Zero(t, double.converseCalls)
}

func TestStrictRemoteMinimumKeepsOriginalTruncationFlag(t *testing.T) {
	cfg := &config.Config{AWSRegion: "us-east-1", SynthesisInputProfile: "strict"}
	amb := llm.NewAmbassador(cfg)
	provider := &calibratedBedrockDouble{
		countFn: func(_ context.Context, _, _ string, messages []brtypes.Message, _ int) (llm.CountResult, error) {
			if strings.Contains(flattenBedrockMessages(messages), `id="2"`) {
				return llm.CountResult{InputTokens: 1200, Attempts: 2, Method: llm.CountMethodBedrockCountTokens}, nil
			}
			return llm.CountResult{InputTokens: 1000, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
		},
	}
	amb.SetClient(provider)
	builder := synthesisPromptBuilder{question: "Q?", boundary: "0123456789abcdef", modelID: testBedrockFallback, maxTokens: 1024}
	chunks := []domain.ScoredChunk{{Chunk: domain.Chunk{Text: "界"}}, {Chunk: domain.Chunk{Text: "More evidence."}}}
	for _, wasTruncated := range []bool{false, true} {
		out, err := budgetRemoteStrictInput(t.Context(), amb, cfg, builder, 1000, chunks, []bool{wasTruncated, false})
		require.NoError(t, err)
		require.Equal(t, chunks[:1], out.chunks)
		require.Equal(t, []bool{wasTruncated}, out.truncated)
	}
}

func TestStrictSynthesisUsesOnlySubstantiveEvidence(t *testing.T) {
	cfg := &config.Config{
		AWSRegion: "us-east-1", BedrockFallbackModel: testBedrockFallback, SynthesisInputProfile: "strict",
		SynthesisContextTokens: 256, SynthesisRelativeScoreFloor: .7,
	}
	provider := &calibratedBedrockDouble{
		countFn: func(context.Context, string, string, []brtypes.Message, int) (llm.CountResult, error) {
			return llm.CountResult{InputTokens: 250, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
		},
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(provider)
	qc := &QueryContext{OriginalQuery: "Q?", RetrievedChunks: []domain.ScoredChunk{
		{Chunk: domain.Chunk{Text: "\u200b \t"}, Score: .95},
		{Chunk: domain.Chunk{Text: "First."}, Score: .6},
		{Chunk: domain.Chunk{Text: "Second."}, Score: .5},
	}}
	require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
	require.Equal(t, 1, provider.countCalls)
	require.Equal(t, 1, provider.converseCalls)
	require.Equal(t, 2, qc.ContextChunks)
	require.Equal(t, []bool{false, false}, qc.Truncated)
	user := flattenBedrockMessages(provider.sent.messages)
	require.Contains(t, user, "id=\"1\">\nFirst.\n")
	require.Contains(t, user, "id=\"2\">\nSecond.\n")
}
