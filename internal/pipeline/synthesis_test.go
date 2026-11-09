package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"log/slog"
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
		{"simple cap", nil, []float64{1, 1, 1, 1, 1, 1, 1, 1, 1}, 5},
		{"multiple needs", []string{"first", "second"}, []float64{1, 1, 1, 1, 1, 1, 1, 1, 1}, 8},
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
			require.Equal(t, strings.Repeat("界", 950)+".", qc.Excerpts[0].Chunk.Text)
			require.True(t, qc.Truncated[0])
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
		{"discarded tail", strings.Repeat("prefix ", 200) + "archives", "Archives [1].", true, 0},
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
