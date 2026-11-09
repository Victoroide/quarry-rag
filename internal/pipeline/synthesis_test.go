package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/require"
)

type synthesisDeadlineClient struct {
	llm.FakeConverser
	calls int
	call  func(context.Context) (*bedrockruntime.ConverseOutput, error)
}

func (c *synthesisDeadlineClient) Converse(ctx context.Context, _ []brtypes.Message) (*bedrockruntime.ConverseOutput, error) {
	c.calls++
	return c.call(ctx)
}
func (c *synthesisDeadlineClient) ConverseWithModelAndSystem(ctx context.Context, _, _ string, _ []brtypes.Message, _ float32, _ int) (*bedrockruntime.ConverseOutput, error) {
	c.calls++
	return c.call(ctx)
}
func synthesisDeadlineConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{BedrockFallbackModel: "fallback", SynthesisTimeout: 20 * time.Millisecond, MinRelevanceScore: .8}
}
func TestMinimumRelevanceScoreSkipsSynthesis(t *testing.T) {
	cfg := synthesisDeadlineConfig(t)
	client := &synthesisDeadlineClient{call: func(context.Context) (*bedrockruntime.ConverseOutput, error) {
		return nil, errors.New("must not synthesize")
	}}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(client)
	qc := &QueryContext{OriginalQuery: "archive", IncludeCitations: true, RetrievedChunks: []domain.ScoredChunk{{Chunk: domain.Chunk{Text: "weak evidence"}, Score: .79}}}
	require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
	require.Equal(t, 0, client.calls)
	require.True(t, qc.EarlyExit)
	require.Empty(t, qc.RetrievedChunks)
	require.Equal(t, "No relevant information was found to answer this question.", qc.FinalAnswer)
	require.Equal(t, "no_evidence", qc.FinishReason)
	require.NotNil(t, qc.Citations)
	require.Empty(t, qc.Citations)
}

func TestLocalizedNoEvidenceSkipsSynthesis(t *testing.T) {
	for _, tc := range []struct {
		name   string
		chunks []domain.ScoredChunk
	}{
		{"empty", nil},
		{"below minimum relevance", []domain.ScoredChunk{{Chunk: domain.Chunk{Text: "weak evidence"}, Score: .79}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := synthesisDeadlineConfig(t)
			client := &synthesisDeadlineClient{call: func(context.Context) (*bedrockruntime.ConverseOutput, error) {
				return nil, errors.New("must not synthesize")
			}}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(client)
			qc := &QueryContext{
				OriginalQuery:   "archive",
				AcceptLanguage:  "es-MX",
				RetrievedChunks: tc.chunks,
				ContextTokens:   20,
				ContextChunks:   1,
				TokenUsage:      domain.TokenUsage{InputTokens: 10, OutputTokens: 3, TotalTokens: 13},
			}
			require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
			require.Zero(t, client.calls)
			require.True(t, qc.EarlyExit)
			require.Empty(t, qc.RetrievedChunks)
			require.Equal(t, "No se encontró información relevante para responder esta pregunta.", qc.FinalAnswer)
			require.Equal(t, "es", qc.ContentLanguage)
			require.Equal(t, "no_evidence", qc.FinishReason)
			require.Zero(t, qc.ContextTokens)
			require.Zero(t, qc.ContextChunks)
			require.Equal(t, domain.TokenUsage{}, qc.TokenUsage)
		})
	}
}

func TestSynthesisIsCalledOncePerAttemptPolicy(t *testing.T) {
	for _, routed := range []bool{false, true} {
		t.Run(fmt.Sprint(routed), func(t *testing.T) {
			cfg := synthesisDeadlineConfig(t)
			client := &synthesisDeadlineClient{call: func(context.Context) (*bedrockruntime.ConverseOutput, error) {
				return nil, errors.New("ThrottlingException")
			}}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(client)
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if routed {
				qc := &QueryContext{OriginalQuery: "archive", RetrievedChunks: []domain.ScoredChunk{{Chunk: domain.Chunk{Text: "evidence"}, Score: .9}}}
				require.Error(t, NewSynthesisStage(amb, cfg).Run(ctx, qc))
			} else {
				_, err := amb.ConverseRoutedPlain(ctx, "fallback", "", nil, 0, 1024)
				require.Error(t, err)
			}
			require.Equal(t, 1, client.calls)
			require.NoError(t, ctx.Err(), "application retry waited for the deadline")
		})
	}
}
func TestSynthesisTimeoutCancelsUpstream(t *testing.T) {
	cfg := synthesisDeadlineConfig(t)
	client := &synthesisDeadlineClient{call: func(ctx context.Context) (*bedrockruntime.ConverseOutput, error) { <-ctx.Done(); return nil, ctx.Err() }}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(client)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	qc := &QueryContext{OriginalQuery: "archive", RetrievedChunks: []domain.ScoredChunk{{Chunk: domain.Chunk{Text: "evidence"}, Score: .9}}}
	require.ErrorIs(t, NewSynthesisStage(amb, cfg).Run(ctx, qc), context.DeadlineExceeded)
	require.NoError(t, ctx.Err(), "synthesis exhausted the whole request budget")
}
func TestQueryTextIsNotLogged(t *testing.T) {
	var buf bytes.Buffer
	old, flags := slog.Default(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer func() { slog.SetDefault(old); log.SetFlags(flags) }()
	qc := &QueryContext{OriginalQuery: "private archive question 927"}
	store := memory.NewFakeVectorStore()
	require.NoError(t, store.Upsert(t.Context(), "", []domain.Chunk{{ID: "chunk", DocumentID: "document", Text: "Archive evidence", Embedding: []float32{1, 0}}}))
	ret := retriever.NewRetriever(store, &scopeCountingEmbedder{}, .7, 10)
	cfg := synthesisDeadlineConfig(t)
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	_, err := RunRetrievalPipeline(t.Context(), qc, []QueryStage{NewDecomposeStage(), NewRoutingStage(ret), NewRetrievalStage(ret), NewSynthesisStage(amb, cfg)})
	require.NoError(t, err)
	require.NotEmpty(t, qc.FinalAnswer)
	require.NotContains(t, buf.String(), qc.OriginalQuery)
	require.Contains(t, buf.String(), "query_bytes=28")
	digest := sha256.Sum256([]byte(qc.OriginalQuery))
	require.Contains(t, buf.String(), fmt.Sprintf("%x", digest[:6]))
	require.NotContains(t, buf.String(), "private archive")
	require.NotContains(t, buf.String(), "Archive evidence")
}

func TestDefaultMinimumRelevancePreservesRetrievedEvidence(t *testing.T) {
	cfg := synthesisDeadlineConfig(t)
	cfg.MinRelevanceScore = 0
	client := &synthesisDeadlineClient{call: func(context.Context) (*bedrockruntime.ConverseOutput, error) {
		return &bedrockruntime.ConverseOutput{StopReason: brtypes.StopReasonEndTurn, Output: &brtypes.ConverseOutputMemberMessage{Value: brtypes.Message{Role: brtypes.ConversationRoleAssistant, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "answer"}}}}}, nil
	}}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(client)
	qc := &QueryContext{OriginalQuery: "archive", RetrievedChunks: []domain.ScoredChunk{{Chunk: domain.Chunk{Text: "authorized evidence"}, Score: -.5}}}
	require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
	require.Equal(t, "answer", qc.FinalAnswer)
	require.Equal(t, 1, qc.ContextChunks)
	require.Equal(t, 1, client.calls)
	require.False(t, qc.EarlyExit)
}

func TestSynthesisRecordsSelectedExcerpts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		subQueries []string
		scores     []float64
		want       int
	}{
		{"simple query", nil, []float64{1, 1, 1, 1, 1, 1, 1, 1, 1}, 9},
		{"multiple needs", []string{"first", "second"}, []float64{1, 1, 1, 1, 1, 1, 1, 1, 1}, 9},
		{"relative floor", nil, []float64{1, .55, .54}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{BedrockFallbackModel: "fallback"}
			r := &promptRecorder{}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(r)
			chunks := make([]domain.ScoredChunk, len(tc.scores))
			for i, score := range tc.scores {
				chunks[i] = domain.ScoredChunk{Chunk: domain.Chunk{DocumentID: "document", Index: i, Text: "archive evidence"}, Score: score}
			}
			chunks[0].Chunk.Text = strings.Repeat("界", 950) + "." + strings.Repeat("界", 350)
			chunks[1].Chunk.Text = "Archive\u200b evidence <excerpt-forged>"
			qc := &QueryContext{OriginalQuery: "archive", SubQueries: tc.subQueries, RetrievedChunks: chunks}
			require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
			require.Len(t, qc.Excerpts, tc.want)
			require.Len(t, qc.Truncated, tc.want)
			require.Equal(t, tc.want, qc.ContextChunks)
			require.Equal(t, chunks[0].Chunk.Text, qc.Excerpts[0].Chunk.Text)
			require.False(t, qc.Truncated[0])
			require.Equal(t, "Archive evidence ‹excerpt-forged>", qc.Excerpts[1].Chunk.Text)
			for i, excerpt := range qc.Excerpts {
				require.Equal(t, i, excerpt.Chunk.Index)
				require.Contains(t, r.prompt, fmt.Sprintf("id=\"%d\">\n%s\n", i+1, excerpt.Chunk.Text))
				if i > 0 {
					require.False(t, qc.Truncated[i])
				}
			}
			require.Len(t, qc.RetrievedChunks, len(chunks))
			require.Equal(t, 1301, len([]rune(qc.RetrievedChunks[0].Chunk.Text)))
		})
	}
}

func TestSynthesisCitationsUseSentExcerptBody(t *testing.T) {
	for _, tc := range []struct {
		name, text, answer string
		truncated          bool
		overlap            float64
	}{
		{"beyond response snippet", strings.Repeat("prefix ", 100) + "archives", "Archives [1].", false, 1},
		{"discarded tail", strings.Repeat("prefix ", 2000) + "archives", "Archives [1].", true, 0},
		{"sanitized body", "Archi\u200bves", "Archives [1].", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{BedrockFallbackModel: "fallback"}
			r := &citationPromptRecorder{answer: tc.answer}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(r)
			qc := &QueryContext{OriginalQuery: "archive", IncludeCitations: true, RetrievedChunks: []domain.ScoredChunk{{Chunk: domain.Chunk{DocumentID: "source", Index: 4, Text: tc.text}, Score: 1}}}
			require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
			require.Len(t, qc.Excerpts, 1)
			require.Contains(t, r.prompt, "id=\"1\">\n"+qc.Excerpts[0].Chunk.Text+"\n")
			require.Equal(t, []bool{tc.truncated}, qc.Truncated)
			require.Len(t, qc.Citations, 1)
			require.Equal(t, tc.overlap, qc.Citations[0].Overlap)
			require.Equal(t, tc.overlap >= .3, qc.Citations[0].Verified)
			require.Equal(t, tc.text, qc.RetrievedChunks[0].Chunk.Text)
		})
	}
}

func TestSynthesisTokenBudget(t *testing.T) {
	first := strings.Repeat("a", 640)
	sentence := strings.Repeat("界", 255) + "." + strings.Repeat("界", 80) + "."
	shortBodies := make([]string, 10)
	for i := range shortBodies {
		shortBodies[i] = strings.Repeat(strconv.Itoa(i), 100)
	}
	for _, tc := range []struct {
		name      string
		budget    int
		floor     string
		bodies    []string
		scores    []float64
		want      []string
		truncated []bool
	}{
		{
			name: "whole excerpts exceed former count caps", budget: 256,
			bodies: shortBodies, want: shortBodies, truncated: make([]bool, 10),
		},
		{
			name: "score order stops before a later short chunk", budget: 256,
			bodies: []string{first, strings.Repeat("b", 320), strings.Repeat("c", 200), "last"},
			want:   []string{first, strings.Repeat("b", 320)}, truncated: []bool{false, false},
		},
		{
			name: "configured floor is inclusive", budget: 256, floor: "0.8",
			bodies: []string{"best", "equal", "below"}, scores: []float64{1, .8, .799},
			want: []string{"best", "equal"}, truncated: []bool{false, false},
		},
		{
			name: "zero floor", budget: 256, floor: "0",
			bodies: []string{"best", "weak", "zero"}, scores: []float64{1, .01, 0},
			want: []string{"best", "weak", "zero"}, truncated: []bool{false, false, false},
		},
		{
			name: "unit floor", budget: 256, floor: "1",
			bodies: []string{"best", "equal", "below"}, scores: []float64{1, 1, .99},
			want: []string{"best", "equal"}, truncated: []bool{false, false},
		},
		{
			name: "last fitting sentence", budget: 256,
			bodies: []string{first, sentence + strings.Repeat("界", 300), "last"},
			want:   []string{first, sentence}, truncated: []bool{false, true},
		},
		{
			name: "newline boundary", budget: 256,
			bodies: []string{first, strings.Repeat("b", 319) + "\n" + strings.Repeat("c", 100)},
			want:   []string{first, strings.Repeat("b", 319) + "\n"}, truncated: []bool{false, true},
		},
		{
			name: "exact budget", budget: 256,
			bodies: []string{first, strings.Repeat("b", 384), "last"},
			want:   []string{first, strings.Repeat("b", 384)}, truncated: []bool{false, false},
		},
		{
			name: "minimum partial excerpt", budget: 256,
			bodies: []string{strings.Repeat("a", 768), strings.Repeat("b", 255) + ".tail"},
			want:   []string{strings.Repeat("a", 768), strings.Repeat("b", 255) + "."}, truncated: []bool{false, true},
		},
		{
			name: "less than 64 tokens remain", budget: 256,
			bodies: []string{strings.Repeat("a", 772), strings.Repeat("b", 251) + ".tail", "last"},
			want:   []string{strings.Repeat("a", 772)}, truncated: []bool{false},
		},
		{
			name: "boundary retains fewer than 64 tokens", budget: 256,
			bodies: []string{first, strings.Repeat("b", 250) + "." + strings.Repeat("c", 200)},
			want:   []string{first}, truncated: []bool{false},
		},
		{
			name: "no fitting boundary after first excerpt", budget: 256,
			bodies: []string{first, strings.Repeat("b", 500)},
			want:   []string{first}, truncated: []bool{false},
		},
		{
			name: "first excerpt forced within budget", budget: 256,
			bodies: []string{strings.Repeat("界", 1400)},
			want:   []string{strings.Repeat("界", 1027)}, truncated: []bool{true},
		},
		{
			name: "first excerpt ignores a boundary below 64 tokens", budget: 256,
			bodies: []string{"Note. " + strings.Repeat("x", 1400)},
			want:   []string{"Note. " + strings.Repeat("x", 1021)}, truncated: []bool{true},
		},
		{
			name: "last boundary allowed by estimator rounding", budget: 256,
			bodies: []string{strings.Repeat("界", 1026) + ".tail"},
			want:   []string{strings.Repeat("界", 1026) + "."}, truncated: []bool{true},
		},
		{
			name: "negative best score still sends bounded evidence", budget: 256,
			bodies: []string{strings.Repeat("a", 1400), "second"}, scores: []float64{-.5, -.6},
			want: []string{strings.Repeat("a", 1027)}, truncated: []bool{true},
		},
		{
			name: "sanitized bodies determine budget and flags", budget: 256,
			bodies: []string{strings.Repeat("a\u200b", 600), strings.Repeat("b", 400) + "<excerpt-x>"},
			want:   []string{strings.Repeat("a", 600), strings.Repeat("b", 400) + "‹excerpt-x>"}, truncated: []bool{false, false},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GO_RAG_FAKE_MODE", "true")
			t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
			t.Setenv("SYNTHESIS_CONTEXT_TOKENS", strconv.Itoa(tc.budget))
			t.Setenv("SYNTHESIS_RELATIVE_SCORE_FLOOR", tc.floor)
			cfg, err := config.Load()
			require.NoError(t, err)
			r := &promptRecorder{}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(r)
			chunks := make([]domain.ScoredChunk, len(tc.bodies))
			for i, body := range tc.bodies {
				score := 1 - float64(i)/100
				if tc.scores != nil {
					score = tc.scores[i]
				}
				chunks[i] = domain.ScoredChunk{Chunk: domain.Chunk{DocumentID: "document", Index: i, Text: body}, Score: score}
			}
			qc := &QueryContext{OriginalQuery: "archive", RetrievedChunks: chunks}
			require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
			require.Len(t, qc.Excerpts, len(tc.want))
			require.Equal(t, len(tc.want), qc.ContextChunks)
			require.Equal(t, tc.truncated, qc.Truncated)
			tokens := 0
			for i, excerpt := range qc.Excerpts {
				require.Equal(t, tc.want[i], excerpt.Chunk.Text)
				require.Equal(t, chunks[i].Score, excerpt.Score)
				require.Equal(t, i, excerpt.Chunk.Index)
				require.Contains(t, r.prompt, fmt.Sprintf("id=\"%d\">\n%s\n", i+1, excerpt.Chunk.Text))
				tokens += llm.EstimateTokens(excerpt.Chunk.Text)
			}
			require.LessOrEqual(t, tokens, tc.budget)
			require.Equal(t, llm.EstimateTokens(r.system)+llm.EstimateTokens(r.prompt), qc.ContextTokens)
			for i, chunk := range qc.RetrievedChunks {
				require.Equal(t, tc.bodies[i], chunk.Chunk.Text)
			}
		})
	}
}

type guardrailPromptRecorder struct {
	llm.FakeConverser
	system   string
	messages []brtypes.Message
}

func (r *guardrailPromptRecorder) ConverseWithModelAndSystem(_ context.Context, _, system string, messages []brtypes.Message, _ float32, _ int) (*bedrockruntime.ConverseOutput, error) {
	r.system, r.messages = system, messages
	return &bedrockruntime.ConverseOutput{
		StopReason: brtypes.StopReasonGuardrailIntervened,
		Output: &brtypes.ConverseOutputMemberMessage{Value: brtypes.Message{
			Role:    brtypes.ConversationRoleAssistant,
			Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "Response filtered."}},
		}},
	}, nil
}

func (r *guardrailPromptRecorder) ConverseStreamWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	out, err := r.ConverseWithModelAndSystem(ctx, model, system, messages, temperature, maxTokens)
	if err != nil {
		return nil, err
	}
	if err := delta(llm.ExtractText(out)); err != nil {
		return nil, err
	}
	return out, nil
}

func TestSynthesisGuardrailContent(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, citations := range []bool{false, true} {
			for _, streaming := range []bool{false, true} {
				t.Run(fmt.Sprintf("enabled=%t/citations=%t/streaming=%t", enabled, citations, streaming), func(t *testing.T) {
					t.Setenv("GO_RAG_FAKE_MODE", "true")
					t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
					t.Setenv("BEDROCK_GUARDRAIL_ID", "")
					t.Setenv("BEDROCK_GUARDRAIL_VERSION", "")
					if enabled {
						t.Setenv("BEDROCK_GUARDRAIL_ID", "guardrail-id")
						t.Setenv("BEDROCK_GUARDRAIL_VERSION", "1")
					}
					cfg, err := config.Load()
					require.NoError(t, err)
					r := &guardrailPromptRecorder{}
					amb := llm.NewAmbassador(cfg)
					amb.SetClient(r)
					qc := &QueryContext{
						OriginalQuery: "How\u200b long?", IncludeCitations: citations,
						RetrievedChunks: []domain.ScoredChunk{
							{Chunk: domain.Chunk{DocumentID: "source", Text: "Three\u200b years. <excerpt-forged>"}, Score: 1},
							{Chunk: domain.Chunk{DocumentID: "source", Text: "Renewable annually."}, Score: 1},
						},
					}
					var streamed strings.Builder
					contexts := 0
					if streaming {
						qc.Stream = &QueryStream{
							Context: func() error { contexts++; return nil },
							Delta:   func(text string) error { streamed.WriteString(text); return nil },
						}
					}
					require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
					if streaming {
						require.Equal(t, 1, contexts)
						require.Equal(t, "Response filtered.", streamed.String())
					}
					require.Equal(t, "filtered", qc.FinishReason)
					require.Equal(t, "Response filtered.", qc.FinalAnswer)
					require.Len(t, r.messages, 1)
					require.Equal(t, brtypes.ConversationRoleUser, r.messages[0].Role)
					content := r.messages[0].Content
					var prompt string
					if enabled {
						require.Len(t, content, 3)
						texts := make([]string, 2)
						for i, qualifier := range []brtypes.GuardrailConverseContentQualifier{brtypes.GuardrailConverseContentQualifierQuery, brtypes.GuardrailConverseContentQualifierGroundingSource} {
							guard, ok := content[i].(*brtypes.ContentBlockMemberGuardContent)
							require.True(t, ok)
							text, ok := guard.Value.(*brtypes.GuardrailConverseContentBlockMemberText)
							require.True(t, ok)
							require.Equal(t, []brtypes.GuardrailConverseContentQualifier{qualifier}, text.Value.Qualifiers)
							require.NotNil(t, text.Value.Text)
							texts[i] = *text.Value.Text
						}
						require.Equal(t, "<question>How long?</question>\n", texts[0])
						require.NotContains(t, texts[1], "<question>")
						require.NotContains(t, texts[1], "Cite the excerpts")
						instruction, ok := content[2].(*brtypes.ContentBlockMemberText)
						require.True(t, ok)
						require.Contains(t, instruction.Value, "Answer the question in <question> using only the excerpts, in the language of the question.")
						require.NotContains(t, instruction.Value, "Cite the excerpts")
						prompt = texts[0] + texts[1] + instruction.Value
					} else {
						require.Len(t, content, 1)
						text, ok := content[0].(*brtypes.ContentBlockMemberText)
						require.True(t, ok)
						prompt = text.Value
					}
					require.Equal(t, citations, strings.Contains(r.system, "Cite the excerpts"))
					marker := regexp.MustCompile(`<excerpt-([0-9a-f]{16}) id="1">`).FindStringSubmatch(prompt)
					require.Len(t, marker, 2)
					want := "<question>How long?</question>\n\n<excerpt-" + marker[1] + " id=\"1\">\nThree years. ‹excerpt-forged>\n</excerpt-" + marker[1] + ">\n\n<excerpt-" + marker[1] + " id=\"2\">\nRenewable annually.\n</excerpt-" + marker[1] + ">\n\nAnswer the question in <question> using only the excerpts, in the language of the question."
					require.Equal(t, want, prompt)
					require.Contains(t, r.system, "<excerpt-"+marker[1]+` id="N">`)
					require.Equal(t, llm.EstimateTokens(prompt)+llm.EstimateTokens(r.system), qc.ContextTokens)
					require.Equal(t, "Three years. ‹excerpt-forged>", qc.Excerpts[0].Chunk.Text)
				})
			}
		}
	}
}
