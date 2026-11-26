package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

type rerankerFunc func(context.Context, string, []domain.ScoredChunk) ([]domain.ScoredChunk, error)

func (f rerankerFunc) Rerank(ctx context.Context, question string, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	return f(ctx, question, chunks)
}

func TestRerankingPrecedesSynthesisSelection(t *testing.T) {
	chunks := make([]domain.ScoredChunk, 6)
	for i, score := range []float64{.99, .95, .9, .85, .5, .3} {
		chunks[i] = domain.ScoredChunk{Chunk: domain.Chunk{ID: domain.ChunkID(fmt.Sprint(i)), DocumentID: "archive", Index: i, Text: fmt.Sprintf("Archive passage %d", i)}, Score: score}
	}
	r := rerankerFunc(func(_ context.Context, question string, in []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		require.Equal(t, "archive hours", question)
		require.Len(t, in, 6)
		out := []domain.ScoredChunk{in[5], in[0], in[4], in[1], in[2], in[3]}
		for i, score := range []float64{1, .7, .6, .55, .4, .3} {
			out[i].RerankScore = &score
		}
		return out, nil
	})
	cfg := &config.Config{BedrockFallbackModel: "fallback", MinRelevanceScore: 0}
	amb := llm.NewAmbassador(cfg)
	pr := &promptRecorder{}
	amb.SetClient(pr)
	qc := &QueryContext{OriginalQuery: "archive hours", RetrievedChunks: chunks, TopScore: .99}
	m := metrics.NewService(nil)
	_, err := RunRetrievalPipeline(metrics.WithService(t.Context(), m), qc, []QueryStage{NewRerankStage(r, time.Second), NewSynthesisStage(amb, cfg)})
	require.NoError(t, err)
	got := make([]int, 0, len(qc.Excerpts))
	for _, excerpt := range qc.Excerpts {
		got = append(got, excerpt.Chunk.Index)
	}
	require.Equal(t, []int{5, 0, 1, 2, 3}, got)
	require.Equal(t, []bool{false, false, false, false, false}, qc.Truncated)
	require.Equal(t, .3, qc.RetrievedChunks[0].Score)
	require.Equal(t, .99, qc.TopScore)
	require.Contains(t, pr.prompt, "id=\"1\">\nArchive passage 5\n")
	require.Contains(t, qc.StageTimingsMS, "rerank")
	var output strings.Builder
	require.NoError(t, m.Registry.WriteText(&output))
	require.Contains(t, output.String(), "quarry_rerank_total{result=\"reranked\"} 1\n")
}

func TestSynthesisKeepsVectorThresholdAfterReranking(t *testing.T) {
	cfg := &config.Config{BedrockFallbackModel: "fallback", MinRelevanceScore: .75}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(&promptRecorder{})
	chunks := []domain.ScoredChunk{sc("weak", .3), sc("relevant", .8), sc("similar", .99)}
	for i, score := range []float64{1, .9, .8} {
		chunks[i].RerankScore = &score
	}
	qc := &QueryContext{OriginalQuery: "archive", RetrievedChunks: chunks, TopScore: .99}
	require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
	require.Equal(t, []string{"relevant", "similar"}, docIDs(qc.RetrievedChunks))
	require.Equal(t, .99, qc.TopScore)
	require.Equal(t, "relevant", string(qc.Excerpts[0].Chunk.DocumentID))
}

func TestRerankFallbackPreservesEvidenceAndOmitsScores(t *testing.T) {
	for _, mode := range []string{"error", "api error", "mutated input", "timeout", "late success"} {
		t.Run(mode, func(t *testing.T) {
			m := metrics.NewService(nil)
			in := []domain.ScoredChunk{sc("a", .9), sc("b", .8)}
			qc := &QueryContext{OriginalQuery: "private question", RetrievedChunks: in, TopScore: .9}
			var logs bytes.Buffer
			old, flags := slog.Default(), log.Flags()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			defer func() { slog.SetDefault(old); log.SetFlags(flags) }()
			r := rerankerFunc(func(ctx context.Context, _ string, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
				if mode == "api error" {
					return nil, &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "private question"}
				}
				if mode == "mutated input" {
					score := .5
					chunks[0].RerankScore = &score
					return nil, errors.New("private question and private document")
				}
				if mode == "error" {
					return nil, errors.New("private question and private document")
				}
				<-ctx.Done()
				if mode == "late success" {
					return []domain.ScoredChunk{in[1], in[0]}, nil
				}
				return nil, ctx.Err()
			})
			require.NoError(t, NewRerankStage(r, 5*time.Millisecond).Run(metrics.WithService(t.Context(), m), qc))
			require.Equal(t, in, qc.RetrievedChunks)
			require.Nil(t, qc.RetrievedChunks[0].RerankScore)
			require.Equal(t, .9, qc.TopScore)
			require.Contains(t, logs.String(), "WARN")
			require.NotContains(t, logs.String(), "private")
			if mode == "api error" {
				require.Contains(t, logs.String(), "error_code=AccessDeniedException")
			}
			if mode == "timeout" || mode == "late success" {
				require.Contains(t, logs.String(), "reason=timeout")
			}
			var output strings.Builder
			require.NoError(t, m.Registry.WriteText(&output))
			require.Contains(t, output.String(), "quarry_rerank_total{result=\"fallback\"} 1\n")
		})
	}
}

func TestRerankDisabledAndEmptySkipCalls(t *testing.T) {
	m := metrics.NewService(nil)
	ctx := metrics.WithService(t.Context(), m)
	qc := &QueryContext{RetrievedChunks: []domain.ScoredChunk{sc("a", .9)}}
	require.NoError(t, NewRerankStage(nil, time.Second).Run(ctx, qc))
	require.Len(t, qc.RetrievedChunks, 1)
	r := rerankerFunc(func(context.Context, string, []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		t.Fatal("empty evidence must skip reranking")
		return nil, nil
	})
	require.NoError(t, NewRerankStage(r, time.Second).Run(ctx, &QueryContext{}))
	var output strings.Builder
	require.NoError(t, m.Registry.WriteText(&output))
	require.NotContains(t, output.String(), "quarry_rerank_total{result=")
}

func TestRerankHonorsEarlierQueryDeadline(t *testing.T) {
	for _, mode := range []string{"canceled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			m := metrics.NewService(nil)
			parent := metrics.WithService(t.Context(), m)
			ctx, cancel := context.WithCancel(parent)
			want := context.Canceled
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(parent, 5*time.Millisecond)
				want = context.DeadlineExceeded
			} else {
				cancel()
			}
			defer cancel()
			r := rerankerFunc(func(ctx context.Context, _ string, _ []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			})
			qc := &QueryContext{RetrievedChunks: []domain.ScoredChunk{sc("a", .9)}}
			require.ErrorIs(t, NewRerankStage(r, time.Second).Run(ctx, qc), want)
			require.Equal(t, "a", string(qc.RetrievedChunks[0].Chunk.DocumentID))
			var output strings.Builder
			require.NoError(t, m.Registry.WriteText(&output))
			require.NotContains(t, output.String(), "quarry_rerank_total{result=")
		})
	}
}

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
							Context: func(context.Context) error { contexts++; return nil },
							Delta:   func(_ context.Context, text string) error { streamed.WriteString(text); return nil },
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

func TestDiverseContextSelectionEdgeCases(t *testing.T) {
	runDiverse := func(t *testing.T, budget int, floor float64, chunks []domain.ScoredChunk) (*QueryContext, *citationPromptRecorder) {
		t.Helper()
		cfg := &config.Config{
			BedrockFallbackModel:        "fallback",
			SynthesisContextTokens:      budget,
			SynthesisRelativeScoreFloor: floor,
			SynthesisContextPolicy:      config.SynthesisContextPolicyDiverse,
		}
		rec := &citationPromptRecorder{answer: "Grounded [1] [2]."}
		amb := llm.NewAmbassador(cfg)
		amb.SetClient(rec)
		qc := &QueryContext{
			OriginalQuery:    "Compare retention and exceptions",
			IncludeCitations: true,
			RetrievedChunks:  chunks,
		}
		require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
		return qc, rec
	}

	t.Run("exact and near duplicates yield to complementary evidence", func(t *testing.T) {
		p1 := "Retention schedule: financial ledgers must be kept for seven years in immutable storage. " + strings.Repeat("a", 220)
		nearDup := "Retention schedule: financial ledgers must be kept for seven years in immutable storage! " + strings.Repeat("a", 220)
		distinct := "Access logs and security audit trails must be kept for ninety days with daily verification. " + strings.Repeat("b", 200)
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "c1", DocumentID: "doc-a", Index: 0, Text: p1}, Score: .95},
			{Chunk: domain.Chunk{ID: "c2", DocumentID: "doc-a", Index: 1, Text: p1}, Score: .92},
			{Chunk: domain.Chunk{ID: "c3", DocumentID: "doc-a", Index: 2, Text: nearDup}, Score: .90},
			{Chunk: domain.Chunk{ID: "c4", DocumentID: "doc-b", Index: 0, Text: distinct}, Score: .82},
		}
		qc, rec := runDiverse(t, 180, .55, chunks)
		require.Len(t, qc.Excerpts, 2)
		require.Equal(t, domain.ChunkID("c1"), qc.Excerpts[0].Chunk.ID)
		require.Equal(t, domain.ChunkID("c4"), qc.Excerpts[1].Chunk.ID)
		require.Equal(t, []bool{false, false}, qc.Truncated)
		require.Contains(t, rec.prompt, `id="1">`+"\n"+p1+"\n")
		require.Contains(t, rec.prompt, `id="2">`+"\n"+distinct+"\n")
	})

	t.Run("short trailing evidence follows unfitting oversized candidate", func(t *testing.T) {
		first := strings.Repeat("First fact clause ", 26) + "." // ~117 tokens
		oversizedNoBreak := strings.Repeat("Long clause without break ", 30)
		shortTrail := "Trailing critical exception: contractors need badge sign-off."
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "c1", DocumentID: "doc-a", Index: 0, Text: first}, Score: .95},
			{Chunk: domain.Chunk{ID: "c2", DocumentID: "doc-b", Index: 0, Text: oversizedNoBreak}, Score: .90},
			{Chunk: domain.Chunk{ID: "c3", DocumentID: "doc-c", Index: 0, Text: shortTrail}, Score: .85},
		}
		qc, _ := runDiverse(t, 180, .55, chunks)
		require.Len(t, qc.Excerpts, 2)
		require.Equal(t, domain.ChunkID("c1"), qc.Excerpts[0].Chunk.ID)
		require.Equal(t, domain.ChunkID("c3"), qc.Excerpts[1].Chunk.ID)
		require.Equal(t, []bool{false, false}, qc.Truncated)
	})

	t.Run("all-one-document query penalizes overlap without banning document", func(t *testing.T) {
		c0 := "Section 1: primary database backups run at 02:00 UTC every morning. " + strings.Repeat("x", 180)
		c1 := "Section 1: primary database backups run at 02:00 UTC every morning. " + strings.Repeat("x", 180)
		c4 := "Section 4: cross-region failover drills run quarterly on the second Sunday. " + strings.Repeat("y", 180)
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "c0", DocumentID: "single-doc", Index: 0, Text: c0}, Score: .95},
			{Chunk: domain.Chunk{ID: "c1", DocumentID: "single-doc", Index: 1, Text: c1}, Score: .92},
			{Chunk: domain.Chunk{ID: "c4", DocumentID: "single-doc", Index: 4, Text: c4}, Score: .80},
		}
		qc, _ := runDiverse(t, 160, .55, chunks)
		require.Len(t, qc.Excerpts, 2)
		require.Equal(t, domain.ChunkID("c0"), qc.Excerpts[0].Chunk.ID)
		require.Equal(t, domain.ChunkID("c4"), qc.Excerpts[1].Chunk.ID)
		require.Equal(t, domain.DocumentID("single-doc"), qc.Excerpts[1].Chunk.DocumentID)
	})

	t.Run("truncated first excerpt keeps leading passage and packs fitting short tail", func(t *testing.T) {
		headSentence := strings.Repeat("Primary rule ", 28) + "." // ~91 tokens
		first := headSentence + " " + strings.Repeat("tail detail ", 60)
		shortSecond := "Exception: emergency overrides expire after four hours."
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "c1", DocumentID: "doc-a", Index: 0, Text: first}, Score: .95},
			{Chunk: domain.Chunk{ID: "c2", DocumentID: "doc-b", Index: 0, Text: shortSecond}, Score: .85},
		}
		qc, _ := runDiverse(t, 160, .55, chunks)
		require.Len(t, qc.Excerpts, 2)
		require.Equal(t, headSentence, qc.Excerpts[0].Chunk.Text)
		require.Equal(t, shortSecond, qc.Excerpts[1].Chunk.Text)
		require.Equal(t, []bool{true, false}, qc.Truncated)
	})

	t.Run("empty and control-only bodies dropped before leading usable passage", func(t *testing.T) {
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "junk-1", DocumentID: "doc-a", Index: 0, Text: "\x00\u200b \t\n"}, Score: .99},
			{Chunk: domain.Chunk{ID: "junk-2", DocumentID: "doc-a", Index: 1, Text: "\u202e\x07"}, Score: .98},
			{Chunk: domain.Chunk{ID: "usable", DocumentID: "doc-b", Index: 0, Text: "Usable archive rule."}, Score: .80},
		}
		qc, rec := runDiverse(t, 256, .55, chunks)
		require.Len(t, qc.Excerpts, 1)
		require.Equal(t, domain.ChunkID("usable"), qc.Excerpts[0].Chunk.ID)
		require.Equal(t, "Usable archive rule.", qc.Excerpts[0].Chunk.Text)
		require.Contains(t, rec.prompt, `id="1">`+"\nUsable archive rule.\n")
	})

	t.Run("wholly empty or control-only bodies make zero synthesis calls", func(t *testing.T) {
		cfg := &config.Config{
			BedrockFallbackModel:   "fallback",
			SynthesisContextPolicy: config.SynthesisContextPolicyDiverse,
		}
		client := &synthesisDeadlineClient{call: func(context.Context) (*bedrockruntime.ConverseOutput, error) {
			t.Fatal("ineligible control-only input must not call synthesis")
			return nil, nil
		}}
		amb := llm.NewAmbassador(cfg)
		amb.SetClient(client)
		qc := &QueryContext{
			OriginalQuery: "archive?",
			RetrievedChunks: []domain.ScoredChunk{
				{Chunk: domain.Chunk{ID: "j1", DocumentID: "d", Text: ""}, Score: .9},
				{Chunk: domain.Chunk{ID: "j2", DocumentID: "d", Text: "\x00\u200b \n\t"}, Score: .8},
			},
		}
		require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
		require.Zero(t, client.calls)
		require.True(t, qc.EarlyExit)
		require.Equal(t, "no_evidence", qc.FinishReason)
		require.Empty(t, qc.RetrievedChunks)
	})

	t.Run("nominally free fragments obey chunk count and byte caps", func(t *testing.T) {
		chunks := make([]domain.ScoredChunk, 150)
		for i := range chunks {
			// Distinct 3-rune fragments (EstimateTokens == 0) across chunks.
			text := fmt.Sprintf("%03d", i)
			chunks[i] = domain.ScoredChunk{
				Chunk: domain.Chunk{ID: domain.ChunkID(fmt.Sprintf("f-%03d", i)), DocumentID: "doc", Index: i, Text: text},
				Score: .9,
			}
		}
		out, trunc, err := trimChunksForSynthesis(t.Context(), chunks, 256, .55, config.SynthesisContextPolicyDiverse)
		require.NoError(t, err)
		require.Len(t, out, maxSynthesisExcerpts)
		require.Len(t, trunc, maxSynthesisExcerpts)

		// Also verify tiny-budget excerpt count cap stops selection even when token estimates are zero.
		tinyBudgetChunks := make([]domain.ScoredChunk, 20)
		for i := range tinyBudgetChunks {
			tinyBudgetChunks[i] = domain.ScoredChunk{
				Chunk: domain.Chunk{ID: domain.ChunkID(fmt.Sprintf("b-%02d", i)), DocumentID: "doc", Index: i, Text: fmt.Sprintf("%02d!", i)},
				Score: .9,
			}
		}
		outTiny, _, err := trimChunksForSynthesis(t.Context(), tinyBudgetChunks, 2, .55, config.SynthesisContextPolicyDiverse)
		require.NoError(t, err)
		require.LessOrEqual(t, len(outTiny), 2)
	})

	t.Run("repeated boilerplate across documents acts as corroboration over same-doc duplicate", func(t *testing.T) {
		boilerplate := "Standard warranty: hardware defects are covered for twelve months from delivery. " + strings.Repeat("w", 120)
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "c1", DocumentID: "doc-a", Index: 0, Text: boilerplate}, Score: .95},
			{Chunk: domain.Chunk{ID: "c2", DocumentID: "doc-a", Index: 1, Text: boilerplate}, Score: .92},
			{Chunk: domain.Chunk{ID: "c3", DocumentID: "doc-b", Index: 0, Text: boilerplate}, Score: .90},
		}
		qc, _ := runDiverse(t, 120, .55, chunks)
		require.Len(t, qc.Excerpts, 2)
		require.Equal(t, domain.ChunkID("c1"), qc.Excerpts[0].Chunk.ID)
		require.Equal(t, domain.ChunkID("c3"), qc.Excerpts[1].Chunk.ID)
	})

	t.Run("unique qualifications, negations, and differing numbers are retained", func(t *testing.T) {
		unrelated := "Visitor parking permits validate for level three north garage only during weekday business hours." +
			strings.Repeat(" Garage rules are posted at each entrance and enforced by security staff.", 12)
		for _, tc := range []struct{ name, lead, variant, want string }{
			{"negation", "Remote work is approved for engineering staff with written manager sign-off and a signed equipment agreement on file.",
				"Remote work is not approved for engineering staff with written manager sign-off and a signed equipment agreement on file.", "is not approved"},
			{"deleted negation", "Remote work is not approved for engineering staff with written manager sign-off and a signed equipment agreement on file.",
				"Remote work is approved for engineering staff with written manager sign-off and a signed equipment agreement on file.", "is approved"},
			{"number", "Domestic travel is reimbursed up to 500 dollars per trip for staff with a receipt submitted within thirty days.",
				"Domestic travel is reimbursed up to 1500 dollars per trip for staff with a receipt submitted within thirty days.", "1500 dollars"},
			{"fullwidth number", "請求書は５００円まで承認されます。経理部門の事前確認と領収書の添付が必要です。",
				"請求書は１５００円まで承認されます。経理部門の事前確認と領収書の添付が必要です。", "１５００円"},
			{"qualifier", "Badge replacement is free for employees who report the loss to the security office within one business day.",
				"Badge replacement is free only for employees who report the loss to the security office within one business day.", "free only for"},
			{"spanish negation", "El acceso remoto está aprobado para el personal de ingeniería con autorización escrita de la gerencia.",
				"El acceso remoto no está aprobado para el personal de ingeniería con autorización escrita de la gerencia.", "no está aprobado"},
			{"portuguese negation", "O acesso remoto está aprovado para a equipe de engenharia com autorização por escrito da gerência.",
				"O acesso remoto não está aprovado para a equipe de engenharia com autorização por escrito da gerência.", "não está aprovado"},
			{"french qualifier", "Le remplacement du badge est gratuit pour les employés qui déclarent la perte au bureau de sécurité.",
				"Le remplacement du badge est gratuit uniquement pour les employés qui déclarent la perte au bureau de sécurité.", "gratuit uniquement"},
			{"german negation", "Die Fernarbeit ist für Ingenieure mit schriftlicher Genehmigung der Abteilungsleitung ausdrücklich gestattet.",
				"Die Fernarbeit ist für Ingenieure mit schriftlicher Genehmigung der Abteilungsleitung nicht gestattet.", "nicht gestattet"},
			{"italian exception", "Il rimborso delle spese di viaggio è consentito per le trasferte autorizzate dalla direzione generale.",
				"Il rimborso delle spese di viaggio è consentito eccetto per le trasferte non autorizzate dalla direzione generale.", "eccetto per"},
			{"chinese negation", "经主管书面批准且已提交设备协议的工程人员获准进行远程办公安排。",
				"经主管书面批准且已提交设备协议的工程人员不获准进行远程办公安排。", "不获准"},
			{"japanese negation", "上長の書面承認および機器管理同意書を提出した開発担当者の在宅勤務は承認されます。",
				"上長の書面承認および機器管理同意書を提出した開発担当者の在宅勤務は承認されません。", "承認されません"},
			{"korean negation", "부서장의 서면 승인과 장비 관리 서약서를 제출한 엔지니어의 원격 근무는 승인됩니다.",
				"부서장의 서면 승인과 장비 관리 서약서를 제출한 엔지니어의 원격 근무는 승인되지 않습니다.", "않습니다"},
			{"thai negation", "พนักงานวิศวกรรมที่ได้รับอนุมัติเป็นลายลักษณ์อักษรจากผู้จัดการได้รับอนุญาตให้ทำงานทางไกลได้",
				"พนักงานวิศวกรรมที่ได้รับอนุมัติเป็นลายลักษณ์อักษรจากผู้จัดการไม่ได้รับอนุญาตให้ทำงานทางไกลได้", "ไม่ได้รับอนุญาต"},
		} {
			for _, loc := range []struct {
				label string
				doc   domain.DocumentID
				idx   int
			}{
				{"same-doc-adjacent", "doc-a", 1},
				{"same-doc-nonadjacent", "doc-a", 7},
				{"cross-doc", "doc-c", 0},
			} {
				in := []domain.ScoredChunk{
					{Chunk: domain.Chunk{ID: "lead", DocumentID: "doc-a", Index: 0, Text: tc.lead}, Score: .90},
					{Chunk: domain.Chunk{ID: "variant", DocumentID: loc.doc, Index: loc.idx, Text: tc.variant}, Score: .88},
					{Chunk: domain.Chunk{ID: "unrelated", DocumentID: "doc-b", Index: 0, Text: unrelated}, Score: .60},
				}
				rankedOut, _, err := trimChunksForSynthesis(t.Context(), in, 256, .55, config.SynthesisContextPolicyRanked)
				require.NoError(t, err)
				diverseOut, _, err := trimChunksForSynthesis(t.Context(), in, 256, .55, config.SynthesisContextPolicyDiverse)
				require.NoError(t, err)
				require.True(t, strings.Contains(rankedOut[1].Chunk.Text, tc.want), "%s (%s): ranked should keep variant", tc.name, loc.label)
				require.GreaterOrEqual(t, len(diverseOut), 2, "%s (%s)", tc.name, loc.label)
				require.Equal(t, domain.ChunkID("variant"), diverseOut[1].Chunk.ID, "%s (%s): diverse dropped meaning-bearing variant", tc.name, loc.label)
				require.Contains(t, diverseOut[1].Chunk.Text, tc.want, "%s (%s)", tc.name, loc.label)
			}
		}

		// Cut variant with a new number in its kept prefix is still retained over a lower-scored distractor;
		// a cut paraphrase without a new signal is not falsely promoted.
		leadCutBase := "Domestic travel is reimbursed up to 500 dollars per trip for staff with a receipt submitted within thirty days. " + strings.Repeat("Standard policy details apply. ", 6)
		cutVariant := "Domestic travel is reimbursed up to 1500 dollars per trip for staff with a receipt submitted within thirty days. " + strings.Repeat("Additional appendix detail. ", 25)
		cutParaphrase := "Domestic travel is reimbursed up to 500 dollars per trip for staff with a receipt submitted within thirty days. " + strings.Repeat("Standard policy details apply! ", 6)
		outCutVar, truncCutVar, err := trimChunksForSynthesis(t.Context(), []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "lead", DocumentID: "doc-a", Index: 0, Text: leadCutBase}, Score: .90},
			{Chunk: domain.Chunk{ID: "variant", DocumentID: "doc-a", Index: 1, Text: cutVariant}, Score: .88},
			{Chunk: domain.Chunk{ID: "unrelated", DocumentID: "doc-b", Index: 0, Text: unrelated}, Score: .60},
		}, 256, .55, config.SynthesisContextPolicyDiverse)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(outCutVar), 2)
		require.Equal(t, domain.ChunkID("variant"), outCutVar[1].Chunk.ID)
		require.True(t, truncCutVar[1])
		require.Contains(t, outCutVar[1].Chunk.Text, "1500 dollars")

		require.False(t, meaningBearingDifference(
			"Remote work is approved for engineering staff.",
			"Remote work is not approved for engineering staff.",
			true,
		))
		require.True(t, meaningBearingDifference(
			"Remote work is approved for engineering staff.",
			"Remote work is not approved for engineering staff.",
			false,
		))
		require.False(t, meaningBearingDifference(cutParaphrase[:200], leadCutBase, true))
		require.True(t, meaningBearingDifference(
			"Contract expires on 2026-03-15 with 15% retention fee.",
			"Contract expires on 2026-04-15 with 15% retention fee.",
			false,
		))
		require.True(t, meaningBearingDifference(
			"Contract expires on 2026-03-15 with 20% retention fee.",
			"Contract expires on 2026-03-15 with 15% retention fee.",
			false,
		))
		require.False(t, meaningBearingDifference(
			"This node accepts another request from engineering.",
			"This node accepts other requests from engineering.",
			false,
		))
	})

	t.Run("adjacent continuations are retained with overlap, without overlap, and under reranking", func(t *testing.T) {
		sent := []string{
			"The contractor onboarding policy requires a signed agreement before any access is granted.",
			"Managers review each request and confirm the cost center within two business days.",
			"Badge issuance follows the review and is recorded by the security office in the register.",
			"Equipment checkout is handled by the service desk after the badge has been issued.",
			"Remote access tokens are provisioned only after equipment checkout is complete.",
			"Contractors working more than ninety days must complete annual security training before renewal.",
			"Training completion is tracked in the learning portal and reported monthly to managers.",
			"Expired training blocks contract renewal until the course is completed again.",
		}
		unrelated := "Visitor parking permits validate for level three north garage only during weekday business hours. Garage gates close at midnight and reopen at six in the morning. Lost permits are replaced at the security office for a small fee." +
			strings.Repeat(" Garage rules are posted at each entrance.", 7)
		for _, unrelatedScore := range []float64{.60, .70} {
			for _, tc := range []struct {
				name   string
				a1Text string
				rerank bool
			}{
				{"with-overlap", strings.Join(sent[4:8], " "), false},
				{"no-overlap", strings.Join(sent[5:8], " "), false},
				{"reranked-with-overlap", strings.Join(sent[4:8], " "), true},
			} {
				in := []domain.ScoredChunk{
					{Chunk: domain.Chunk{ID: "a0", DocumentID: "doc-a", Index: 0, Text: strings.Join(sent[0:5], " ")}, Score: .91},
					{Chunk: domain.Chunk{ID: "a1", DocumentID: "doc-a", Index: 1, Text: tc.a1Text}, Score: .89},
					{Chunk: domain.Chunk{ID: "b0", DocumentID: "doc-b", Index: 0, Text: unrelated}, Score: unrelatedScore},
				}
				if tc.rerank {
					r0, r1, r2 := 0.99, 0.95, 0.90
					in[0].RerankScore, in[1].RerankScore, in[2].RerankScore = &r0, &r1, &r2
				}
				out, _, err := trimChunksForSynthesis(t.Context(), in, 256, .55, config.SynthesisContextPolicyDiverse)
				require.NoError(t, err)
				require.GreaterOrEqual(t, len(out), 2, "%s unrelated=%.2f", tc.name, unrelatedScore)
				require.Equal(t, domain.ChunkID("a1"), out[1].Chunk.ID, "%s unrelated=%.2f", tc.name, unrelatedScore)
				require.Contains(t, out[1].Chunk.Text, sent[5], "%s unrelated=%.2f", tc.name, unrelatedScore)
			}
		}
	})

	t.Run("non-space-delimited CJK and Thai scripts detect overlap and keep distinct facts", func(t *testing.T) {
		jp1 := "東京支社の営業時間は午前9時から午後6時までです。" + strings.Repeat("あ", 160)
		jpDup := "東京支社の営業時間は午前9時から午後6時までです。" + strings.Repeat("あ", 160)
		thDistinct := "สำนักงานกรุงเทพเปิดให้บริการตั้งแต่เวลาแปดโมงเช้าถึงห้าโมงเย็นทุกวันจันทร์ถึงศุกร์" + strings.Repeat("ก", 160)
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "jp1", DocumentID: "doc-jp", Index: 0, Text: jp1}, Score: .95},
			{Chunk: domain.Chunk{ID: "jp2", DocumentID: "doc-jp", Index: 1, Text: jpDup}, Score: .92},
			{Chunk: domain.Chunk{ID: "th1", DocumentID: "doc-th", Index: 0, Text: thDistinct}, Score: .85},
		}
		qc, _ := runDiverse(t, 450, .55, chunks)
		require.Len(t, qc.Excerpts, 2)
		require.Equal(t, domain.ChunkID("jp1"), qc.Excerpts[0].Chunk.ID)
		require.Equal(t, domain.ChunkID("th1"), qc.Excerpts[1].Chunk.ID)
	})

	t.Run("zero, negative, and tied cosine scores have defined deterministic treatment", func(t *testing.T) {
		// Zero cosine scores: all pass floor (0 >= 0), diverse prefers distinct body.
		zeroChunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "z2", DocumentID: "doc-a", Index: 0, Text: "Zero score fact alpha. " + strings.Repeat("a", 160)}, Score: 0},
			{Chunk: domain.Chunk{ID: "z1", DocumentID: "doc-a", Index: 1, Text: "Zero score fact alpha. " + strings.Repeat("a", 160)}, Score: 0},
			{Chunk: domain.Chunk{ID: "z3", DocumentID: "doc-b", Index: 0, Text: "Zero score fact beta. " + strings.Repeat("b", 160)}, Score: 0},
		}
		qcZero, _ := runDiverse(t, 120, .55, zeroChunks)
		require.Len(t, qcZero.Excerpts, 2)
		require.Equal(t, domain.ChunkID("z2"), qcZero.Excerpts[0].Chunk.ID)
		require.Equal(t, domain.ChunkID("z3"), qcZero.Excerpts[1].Chunk.ID)

		// Negative cosine scores: first-candidate exception retains leading passage;
		// weaker negative scores below top*floor are excluded just like ranked mode.
		negChunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "n1", DocumentID: "doc-a", Index: 0, Text: "Negative best fact."}, Score: -0.5},
			{Chunk: domain.Chunk{ID: "n2", DocumentID: "doc-b", Index: 0, Text: "Weaker negative fact."}, Score: -0.6},
		}
		qcNeg, _ := runDiverse(t, 256, .55, negChunks)
		require.Len(t, qcNeg.Excerpts, 1)
		require.Equal(t, domain.ChunkID("n1"), qcNeg.Excerpts[0].Chunk.ID)

		// Tied cosine scores: deterministic across repeated runs and breaks ties by rank/ID.
		tiedChunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "t-first", DocumentID: "doc-a", Index: 0, Text: "Lead anchor fact."}, Score: .8},
			{Chunk: domain.Chunk{ID: "t-b", DocumentID: "doc-b", Index: 0, Text: "Unique tied clause."}, Score: .8},
			{Chunk: domain.Chunk{ID: "t-c", DocumentID: "doc-c", Index: 0, Text: "Unique tied clause."}, Score: .8},
		}
		qcTied1, _ := runDiverse(t, 256, .55, tiedChunks)
		qcTied2, _ := runDiverse(t, 256, .55, tiedChunks)
		require.Equal(t, qcTied1.Excerpts, qcTied2.Excerpts)
		require.Equal(t, domain.ChunkID("t-b"), qcTied1.Excerpts[1].Chunk.ID)
	})

	t.Run("first-candidate exception after reranking retains top reranked passage and filters below-floor", func(t *testing.T) {
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "rerank-top", DocumentID: "doc-a", Index: 0, Text: "Top reranked passage."}, Score: .30},
			{Chunk: domain.Chunk{ID: "weak-mid", DocumentID: "doc-b", Index: 0, Text: "Below floor passage."}, Score: .40},
			{Chunk: domain.Chunk{ID: "strong-vec", DocumentID: "doc-c", Index: 0, Text: "Strong vector passage."}, Score: .99},
		}
		r := rerankerFunc(func(_ context.Context, _ string, in []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
			out := []domain.ScoredChunk{in[0], in[1], in[2]}
			for i, rs := range []float64{.98, .80, .60} {
				score := rs
				out[i].RerankScore = &score
			}
			return out, nil
		})
		cfg := &config.Config{
			BedrockFallbackModel:        "fallback",
			SynthesisContextTokens:      256,
			SynthesisRelativeScoreFloor: .55,
			SynthesisContextPolicy:      config.SynthesisContextPolicyDiverse,
		}
		amb := llm.NewAmbassador(cfg)
		amb.SetClient(&citationPromptRecorder{answer: "Grounded [1] [2]."})
		qc := &QueryContext{OriginalQuery: "question", RetrievedChunks: chunks, TopScore: .99}
		_, err := RunRetrievalPipeline(t.Context(), qc, []QueryStage{NewRerankStage(r, time.Second), NewSynthesisStage(amb, cfg)})
		require.NoError(t, err)
		require.Len(t, qc.Excerpts, 2)
		require.Equal(t, domain.ChunkID("rerank-top"), qc.Excerpts[0].Chunk.ID)
		require.Equal(t, domain.ChunkID("strong-vec"), qc.Excerpts[1].Chunk.ID)
	})

	t.Run("reranker failure falls back while parent cancellation stops selection", func(t *testing.T) {
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "c1", DocumentID: "doc-a", Index: 0, Text: "Alpha rule. " + strings.Repeat("a", 160)}, Score: .95},
			{Chunk: domain.Chunk{ID: "c2", DocumentID: "doc-a", Index: 1, Text: "Alpha rule. " + strings.Repeat("a", 160)}, Score: .90},
			{Chunk: domain.Chunk{ID: "c3", DocumentID: "doc-b", Index: 0, Text: "Beta exception. " + strings.Repeat("b", 160)}, Score: .85},
		}
		failingReranker := rerankerFunc(func(context.Context, string, []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
			return nil, errors.New("reranker down")
		})
		cfg := &config.Config{
			BedrockFallbackModel:        "fallback",
			SynthesisContextTokens:      120,
			SynthesisRelativeScoreFloor: .55,
			SynthesisContextPolicy:      config.SynthesisContextPolicyDiverse,
		}
		amb := llm.NewAmbassador(cfg)
		amb.SetClient(&citationPromptRecorder{answer: "Alpha [1] Beta [2]."})
		qc := &QueryContext{OriginalQuery: "rules", RetrievedChunks: chunks, TopScore: .95}
		_, err := RunRetrievalPipeline(t.Context(), qc, []QueryStage{NewRerankStage(failingReranker, time.Second), NewSynthesisStage(amb, cfg)})
		require.NoError(t, err)
		require.Len(t, qc.Excerpts, 2)
		require.Equal(t, domain.ChunkID("c1"), qc.Excerpts[0].Chunk.ID)
		require.Equal(t, domain.ChunkID("c3"), qc.Excerpts[1].Chunk.ID)

		// Parent cancellation stops selection with zero synthesis calls.
		canceledCtx, cancel := context.WithCancel(t.Context())
		cancel()
		client := &synthesisDeadlineClient{call: func(context.Context) (*bedrockruntime.ConverseOutput, error) {
			t.Fatal("canceled context must not call synthesis")
			return nil, nil
		}}
		ambCanceled := llm.NewAmbassador(cfg)
		ambCanceled.SetClient(client)
		qcCanceled := &QueryContext{OriginalQuery: "rules", RetrievedChunks: chunks}
		err = NewSynthesisStage(ambCanceled, cfg).Run(canceledCtx, qcCanceled)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, client.calls)
	})

	t.Run("renumbered excerpts map citations to exact selected bodies and match evidence mode", func(t *testing.T) {
		lead := "Lead policy states badges expire yearly. " + strings.Repeat("p", 160)
		dup := "Lead policy states badges expire yearly. " + strings.Repeat("p", 160)
		trail := "Visitor passes expire after twenty four hours."
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "c1", DocumentID: "doc-a", Index: 0, Text: lead}, Score: .95},
			{Chunk: domain.Chunk{ID: "c2", DocumentID: "doc-a", Index: 1, Text: dup}, Score: .92},
			{Chunk: domain.Chunk{ID: "c3", DocumentID: "doc-b", Index: 7, Text: trail}, Score: .85},
		}
		qcGen, _ := runDiverse(t, 120, .55, chunks)
		require.Len(t, qcGen.Excerpts, 2)
		require.Equal(t, domain.ChunkID("c3"), qcGen.Excerpts[1].Chunk.ID)
		require.Len(t, qcGen.Citations, 2)
		require.Equal(t, 2, qcGen.Citations[1].Excerpt)
		require.Equal(t, domain.DocumentID("doc-b"), qcGen.Citations[1].DocumentID)
		require.Equal(t, 7, qcGen.Citations[1].ChunkIndex)

		cfgEvid := &config.Config{
			SynthesisContextTokens:      120,
			SynthesisRelativeScoreFloor: .55,
			SynthesisContextPolicy:      config.SynthesisContextPolicyDiverse,
		}
		ambEvid := llm.NewAmbassador(cfgEvid)
		ambEvid.SetGenerator(unexpectedEvidenceGenerator{t})
		qcEvid := &QueryContext{
			OriginalQuery:   "Compare retention and exceptions",
			AnswerMode:      domain.AnswerModeEvidence,
			RetrievedChunks: chunks,
		}
		require.NoError(t, NewSynthesisStage(ambEvid, cfgEvid).Run(t.Context(), qcEvid))
		require.Equal(t, qcGen.Excerpts, qcEvid.Excerpts)
		require.Equal(t, qcGen.Truncated, qcEvid.Truncated)
	})

	t.Run("strict profile and NaN score handling remain safe under diverse policy", func(t *testing.T) {
		out, trunc, err := trimChunksForSynthesis(t.Context(), []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "nan-lead", DocumentID: "doc-a", Index: 0, Text: "Lead passage."}, Score: math.NaN()},
			{Chunk: domain.Chunk{ID: "valid-2", DocumentID: "doc-b", Index: 0, Text: "Valid passage."}, Score: .8},
			{Chunk: domain.Chunk{ID: "weak-3", DocumentID: "doc-c", Index: 0, Text: "Weak passage."}, Score: .2},
		}, 256, .55, config.SynthesisContextPolicyDiverse)
		require.NoError(t, err)
		require.Len(t, out, 2)
		require.Equal(t, []bool{false, false}, trunc)
		require.Equal(t, domain.ChunkID("nan-lead"), out[0].Chunk.ID)
		require.Equal(t, domain.ChunkID("valid-2"), out[1].Chunk.ID)

		// Strict input profile preserves diverse selection order and exact token counting.
		cfg := &config.Config{
			LLMProvider:                 "bedrock",
			AWSRegion:                   "us-east-1",
			BedrockFallbackModel:        testBedrockFallback,
			SynthesisInputProfile:       "strict",
			SynthesisContextTokens:      120,
			SynthesisRelativeScoreFloor: .55,
			SynthesisContextPolicy:      config.SynthesisContextPolicyDiverse,
			SynthesisTimeout:            5 * time.Second,
		}
		double := &calibratedBedrockDouble{
			countFn: func(_ context.Context, _ string, _ string, _ []brtypes.Message, _ int) (llm.CountResult, error) {
				return llm.CountResult{InputTokens: 512, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
			},
		}
		amb := llm.NewAmbassador(cfg)
		amb.SetClient(double)
		lead := "Strict lead retention policy. " + strings.Repeat("a", 160)
		dup := "Strict lead retention policy. " + strings.Repeat("a", 160)
		trail := "Strict complementary exception."
		qc := &QueryContext{
			OriginalQuery: "retention?",
			RetrievedChunks: []domain.ScoredChunk{
				{Chunk: domain.Chunk{ID: "s1", DocumentID: "doc-a", Index: 0, Text: lead}, Score: .95},
				{Chunk: domain.Chunk{ID: "s2", DocumentID: "doc-a", Index: 1, Text: dup}, Score: .90},
				{Chunk: domain.Chunk{ID: "s3", DocumentID: "doc-b", Index: 0, Text: trail}, Score: .85},
			},
		}
		require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
		require.Len(t, qc.Excerpts, 2)
		require.Equal(t, domain.ChunkID("s3"), qc.Excerpts[1].Chunk.ID)
		require.Equal(t, 512, qc.ModelInputTokens)
	})
}
