package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queryReranker func(context.Context, string, []domain.ScoredChunk) ([]domain.ScoredChunk, error)

func (f queryReranker) Rerank(ctx context.Context, question string, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	return f(ctx, question, chunks)
}

func TestQueryRerankScoresAreOptionalAndPreserveVectorScore(t *testing.T) {
	for _, mode := range []string{"enabled", "disabled", "fallback"} {
		t.Run(mode, func(t *testing.T) {
			cfg := deadlineQueryConfig(t)
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(llm.NewFakeConverser())
			ret := retriever.NewRetriever(deadlineQueryStore{}, &deadlineEmbedder{release: closedQueryChannel()}, .7, 10)
			var r retriever.Reranker
			if mode != "disabled" {
				r = queryReranker(func(_ context.Context, _ string, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
					if mode == "fallback" {
						return nil, errors.New("unavailable")
					}
					score := 0.0
					chunks[0].RerankScore = &score
					return chunks, nil
				})
			}
			rec := httptest.NewRecorder()
			QueryHandler(ret, amb, cfg, nil, WithReranker(r)).ServeHTTP(rec, deadlineQueryRequest(t.Context()))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var got struct {
				ScoredChunks []map[string]json.RawMessage `json:"scored_chunks"`
				StageTimings map[string]int64             `json:"stage_timings_ms"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			_, timed := got.StageTimings["rerank"]
			require.Equal(t, mode != "disabled", timed)
			require.Len(t, got.ScoredChunks, 1)
			require.JSONEq(t, "0.9", string(got.ScoredChunks[0]["score"]))
			if mode == "enabled" {
				require.Equal(t, "0", string(got.ScoredChunks[0]["rerank_score"]))
			} else {
				require.NotContains(t, got.ScoredChunks[0], "rerank_score")
			}
		})
	}
}

func closedQueryChannel() chan struct{} { ch := make(chan struct{}); close(ch); return ch }

type deadlineQueryStore struct{ domain.VectorStore }

func (deadlineQueryStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	return []domain.ScoredChunk{{Chunk: domain.Chunk{DocumentID: "document", Text: "Archive evidence"}, Score: .9}}, nil
}

type deadlineConverser func(context.Context) (*bedrockruntime.ConverseOutput, error)

func (f deadlineConverser) Converse(ctx context.Context, _ []brtypes.Message) (*bedrockruntime.ConverseOutput, error) {
	return f(ctx)
}

type deadlineEmbedder struct {
	domain.EmbeddingProvider
	entered chan struct{}
	release chan struct{}
}

func (e deadlineEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	if e.entered != nil {
		e.entered <- struct{}{}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.release:
		return []float32{1}, nil
	}
}

func deadlineQueryConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{MaxChunksPerQuery: 10, BedrockFallbackModel: "fallback", QueryTimeout: time.Hour, SynthesisTimeout: time.Hour, QueryConcurrency: 1, QueryQueueTimeout: time.Hour}
}
func deadlineQueryHandler(cfg *config.Config, emb domain.EmbeddingProvider, client llm.Converser) http.HandlerFunc {
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(client)
	return QueryHandler(retriever.NewRetriever(deadlineQueryStore{}, emb, .7, 10), amb, cfg, nil)
}
func deadlineQueryRequest(ctx context.Context) *http.Request {
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/query", strings.NewReader(`{"query":"archive"}`))
	req.Header.Set("Content-Type", "application/json")
	return req
}
func deadlineOutput(text string, reason brtypes.StopReason) *bedrockruntime.ConverseOutput {
	return &bedrockruntime.ConverseOutput{StopReason: reason, Output: &brtypes.ConverseOutputMemberMessage{Value: brtypes.Message{Role: brtypes.ConversationRoleAssistant, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: text}}}}}
}
func deadlineAssertError(t *testing.T, rec *httptest.ResponseRecorder, code int, kind string) {
	t.Helper()
	require.Equal(t, code, rec.Code, rec.Body.String())
	var got APIError
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, kind, got.Error)
}

type citationQueryStore struct {
	domain.VectorStore
	chunks []domain.ScoredChunk
}

func (s citationQueryStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	return s.chunks, nil
}

func TestQueryReportsSynthesisContextAndOptionalCitations(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		include, evidence, markers bool
	}{
		{"default", false, true, true},
		{"requested", true, true, true},
		{"without markers", true, true, false},
		{"no evidence", true, false, true},
		{"no evidence by default", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := streamQueryConfig()
			cfg.SynthesisContextTokens, cfg.SynthesisRelativeScoreFloor = 256, .55
			var chunks []domain.ScoredChunk
			if tc.evidence {
				for i := range 7 {
					text := "Archive evidence"
					if i == 0 {
						text += strings.Repeat(" record", 200)
					}
					chunks = append(chunks, domain.ScoredChunk{Chunk: domain.Chunk{ID: domain.ChunkID(fmt.Sprintf("chunk-%d", i)), DocumentID: "document", Index: i + 10, Text: text}, Score: 1})
				}
			}
			answer := "Archive evidence"
			if tc.markers {
				answer += " [6] [1]."
			}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(deadlineConverser(func(context.Context) (*bedrockruntime.ConverseOutput, error) {
				require.True(t, tc.evidence, "no-evidence response called synthesis")
				return deadlineOutput(answer, brtypes.StopReasonEndTurn), nil
			}))
			h := QueryHandler(retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10), amb, cfg, nil)
			body := fmt.Sprintf(`{"query":"archive","mode":"global","include_citations":%t}`, tc.include)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var got map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			if !tc.include {
				require.NotContains(t, got, "citations")
			}
			if !tc.evidence {
				if tc.include {
					require.JSONEq(t, `[]`, string(got["citations"]))
				}
				return
			}
			var scored []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(got["scored_chunks"], &scored))
			require.Len(t, scored, 7)
			for i, chunk := range scored {
				if i == 0 {
					require.Equal(t, fmt.Sprint(i+1), string(chunk["excerpt"]))
				} else {
					require.NotContains(t, chunk, "excerpt")
				}
				if i == 0 {
					require.Equal(t, "true", string(chunk["truncated"]))
				} else {
					require.NotContains(t, chunk, "truncated")
				}
			}
			if tc.include && tc.markers {
				require.JSONEq(t, `"Archive evidence [1]."`, string(got["answer"]))
				require.JSONEq(t, `[{"excerpt":1,"doc_id":"document","chunk_index":10,"offset":17,"verified":true,"overlap":1}]`, string(got["citations"]))
			} else {
				encoded, err := json.Marshal(answer)
				require.NoError(t, err)
				require.JSONEq(t, string(encoded), string(got["answer"]))
				if tc.include {
					require.JSONEq(t, `[]`, string(got["citations"]))
				}
			}
		})
	}
}

func TestQueryTimesOutWith504(t *testing.T) {
	ctx := testBudget(t)
	cfg := deadlineQueryConfig(t)
	cfg.QueryTimeout = 50 * time.Millisecond
	rec := httptest.NewRecorder()
	deadlineQueryHandler(cfg, deadlineEmbedder{}, nil)(rec, deadlineQueryRequest(ctx))
	deadlineAssertError(t, rec, 504, "timeout")
	require.NoError(t, ctx.Err(), "query exhausted the caller deadline")
}
func TestQueryOverloadReturns503(t *testing.T) {
	entered, release := make(chan struct{}, 2), make(chan struct{})
	ctx, cancel := context.WithCancel(testBudget(t))
	defer cancel()
	cfg := deadlineQueryConfig(t)
	cfg.QueryQueueTimeout = 20 * time.Millisecond
	h := deadlineQueryHandler(cfg, deadlineEmbedder{entered: entered, release: release}, llm.NewFakeConverser())
	done := make(chan struct{})
	go func() { defer close(done); h(httptest.NewRecorder(), deadlineQueryRequest(ctx)) }()
	<-entered
	rec := httptest.NewRecorder()
	h(rec, deadlineQueryRequest(ctx))
	cancel()
	<-done
	deadlineAssertError(t, rec, 503, "overloaded")
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	close(release)
	rec = httptest.NewRecorder()
	h(rec, deadlineQueryRequest(t.Context()))
	require.Equal(t, http.StatusOK, rec.Code, "canceled query did not release its slot")
}
func TestThrottlingMapsTo503(t *testing.T) {
	for _, err := range []error{
		&smithy.GenericAPIError{Code: "ThrottlingException", Message: "secret-host:443"},
		&smithy.GenericAPIError{Code: "ServiceQuotaExceededException", Message: "secret-host:443"},
		&smithy.GenericAPIError{Code: "TooManyRequestsException", Message: "secret-host:443"},
		status.Error(codes.ResourceExhausted, "secret-host:443"),
	} {
		t.Run(fmt.Sprintf("%T/%s", err, err.Error()), func(t *testing.T) {
			rec := httptest.NewRecorder()
			h := deadlineQueryHandler(deadlineQueryConfig(t), embedder.NewFakeEmbedder(256), deadlineConverser(func(context.Context) (*bedrockruntime.ConverseOutput, error) {
				return nil, fmt.Errorf("upstream: %w", err)
			}))
			h(rec, deadlineQueryRequest(testBudget(t)))
			deadlineAssertError(t, rec, 503, "upstream_throttled")
			require.Equal(t, "2", rec.Header().Get("Retry-After"))
			require.NotContains(t, rec.Body.String(), "secret-host:443")
		})
	}
}
func TestInternalErrorsAreNotEchoed(t *testing.T) {
	rec := httptest.NewRecorder()
	deadlineQueryHandler(deadlineQueryConfig(t), embedder.NewFakeEmbedder(256), deadlineConverser(func(context.Context) (*bedrockruntime.ConverseOutput, error) {
		return nil, errors.New("secret-host:443 failed")
	}))(rec, deadlineQueryRequest(t.Context()))
	deadlineAssertError(t, rec, 500, "pipeline")
	require.NotContains(t, rec.Body.String(), "secret-host:443")
	require.Contains(t, rec.Body.String(), "query processing failed")
}
func TestEmptySynthesisReturns502(t *testing.T) {
	for _, output := range []*bedrockruntime.ConverseOutput{nil, {}, deadlineOutput(" \n", brtypes.StopReasonEndTurn)} {
		rec := httptest.NewRecorder()
		deadlineQueryHandler(deadlineQueryConfig(t), embedder.NewFakeEmbedder(256), deadlineConverser(func(context.Context) (*bedrockruntime.ConverseOutput, error) { return output, nil }))(rec, deadlineQueryRequest(t.Context()))
		deadlineAssertError(t, rec, 502, "synthesis")
	}
}
func TestFinishReasonIsReported(t *testing.T) {
	for _, tc := range []struct {
		reason brtypes.StopReason
		want   string
	}{
		{"end_turn", "stop"}, {"stop_sequence", "stop"}, {"max_tokens", "max_tokens"}, {"content_filtered", "filtered"}, {"guardrail_intervened", "filtered"}, {"model_context_window_exceeded", "context_exceeded"},
	} {
		t.Run(string(tc.reason), func(t *testing.T) {
			rec := httptest.NewRecorder()
			deadlineQueryHandler(deadlineQueryConfig(t), embedder.NewFakeEmbedder(256), deadlineConverser(func(context.Context) (*bedrockruntime.ConverseOutput, error) {
				return deadlineOutput("answer", tc.reason), nil
			}))(rec, deadlineQueryRequest(t.Context()))
			require.Equal(t, 200, rec.Code, rec.Body.String())
			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			require.Equal(t, tc.want, got["finish_reason"])
		})
	}
}
func TestCanceledQueryWritesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rec := httptest.NewRecorder()
	deadlineQueryHandler(deadlineQueryConfig(t), nil, nil)(rec, deadlineQueryRequest(ctx))
	require.Empty(t, rec.Body.String())
	require.Empty(t, rec.Header())
}

func TestQueryReportsNoEvidence(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(`{"query":"archive","allowed_document_ids":[]}`))
	req.Header.Set("Content-Type", "application/json")
	deadlineQueryHandler(deadlineQueryConfig(t), nil, nil)(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got QueryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "no_evidence", got.FinishReason)
	require.Zero(t, got.ChunksUsed)
	require.Empty(t, got.ScoredChunks)
}

func TestQueryNoEvidenceLanguage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
		tag    string
		answer string
	}{
		{"missing", "", "en", "No relevant information was found to answer this question."},
		{"spanish", "es-MX", "es", "No se encontró información relevante para responder esta pregunta."},
		{"portuguese", "pt-BR", "pt", "Não foi encontrada informação relevante para responder a esta pergunta."},
		{"french", "fr-CA", "fr", "Aucune information pertinente n'a été trouvée pour répondre à cette question."},
		{"german", "de", "de", "Es wurden keine relevanten Informationen gefunden, um diese Frage zu beantworten."},
		{"italian", "it", "it", "Non sono state trovate informazioni pertinenti per rispondere a questa domanda."},
		{"unsupported", "zh", "en", "No relevant information was found to answer this question."},
		{"malformed", "es;q=broken", "en", "No relevant information was found to answer this question."},
		{"weighted", "en;q=0.2, es-MX;q=0.9", "es", "No se encontró información relevante para responder esta pregunta."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(`{"query":"archive","allowed_document_ids":[]}`))
			req.Header.Set("Content-Type", "application/json")
			if tc.header != "" {
				req.Header.Set("Accept-Language", tc.header)
			}
			deadlineQueryHandler(deadlineQueryConfig(t), nil, nil)(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var got QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			require.Equal(t, tc.answer, got.Answer)
			require.Equal(t, tc.tag, rec.Header().Get("Content-Language"))
			require.Equal(t, "no_evidence", got.FinishReason)
			require.True(t, got.EarlyExit)
			require.Zero(t, got.ChunksUsed)
			require.Zero(t, got.ContextChunks)
			require.Zero(t, got.ContextTokens)
			require.Equal(t, domain.TokenUsage{}, got.TokenUsage)
			require.Empty(t, got.ScoredChunks)
		})
	}
}

func TestQuerySynthesisDoesNotSetContentLanguage(t *testing.T) {
	rec := httptest.NewRecorder()
	req := deadlineQueryRequest(t.Context())
	req.Header.Set("Accept-Language", "es")
	deadlineQueryHandler(deadlineQueryConfig(t), embedder.NewFakeEmbedder(256), deadlineConverser(func(context.Context) (*bedrockruntime.ConverseOutput, error) {
		return deadlineOutput("answer", brtypes.StopReasonEndTurn), nil
	}))(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Empty(t, rec.Header().Get("Content-Language"))
}

func TestQueryNoEvidenceLanguageCombinesHeaderLines(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(`{"query":"archive","allowed_document_ids":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Accept-Language", "en;q=0.2")
	req.Header.Add("Accept-Language", "es-MX;q=0.9")
	deadlineQueryHandler(deadlineQueryConfig(t), nil, nil)(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "es", rec.Header().Get("Content-Language"))
}

func TestQueryQueueHonorsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{}, 2)
		h := deadlineQueryHandler(deadlineQueryConfig(t), deadlineEmbedder{entered: entered}, nil)
		firstCtx, firstCancel := context.WithCancel(t.Context())
		defer firstCancel()
		go h(httptest.NewRecorder(), deadlineQueryRequest(firstCtx))
		<-entered

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		rec := httptest.NewRecorder()
		go func() { defer close(done); h(rec, deadlineQueryRequest(ctx)) }()
		synctest.Wait()
		require.Empty(t, entered, "queued request entered the pipeline")
		select {
		case <-done:
			t.Fatal("the second request returned before cancellation")
		default:
		}

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("the queued request did not stop after cancellation")
		}
		require.Empty(t, rec.Body.String())
		require.Empty(t, rec.Header())
		require.Empty(t, entered, "queued request entered the pipeline")
	})
}

func TestQueryRejectsImmediatelyWithZeroQueueTimeout(t *testing.T) {
	entered := make(chan struct{}, 2)
	cfg := deadlineQueryConfig(t)
	cfg.QueryQueueTimeout = 0
	h := deadlineQueryHandler(cfg, deadlineEmbedder{entered: entered}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); h(httptest.NewRecorder(), deadlineQueryRequest(ctx)) }()
	<-entered
	rec := httptest.NewRecorder()
	h(rec, deadlineQueryRequest(t.Context()))
	cancel()
	<-done
	deadlineAssertError(t, rec, 503, "overloaded")
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.Empty(t, entered, "rejected request entered the pipeline")
}

type routingHandlerStore struct {
	domain.VectorStore
	probe []domain.ScoredChunk
}

func (s routingHandlerStore) QueryFiltered(_ context.Context, _ []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	out := make([]domain.ScoredChunk, 0, len(s.probe))
	for _, sc := range s.probe {
		if filter.DocumentIDs != nil {
			matched := false
			for _, id := range filter.DocumentIDs {
				if string(sc.Chunk.DocumentID) == id {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		out = append(out, sc)
		if len(out) >= topK {
			break
		}
	}
	return out, nil
}

func TestQueryHandlerHonorsRoutingPoliciesAndCalibration(t *testing.T) {
	store := routingHandlerStore{
		probe: []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "a1", DocumentID: "doc-a", Text: "Alpha vault retention policy."}, Score: 0.90},
			{Chunk: domain.Chunk{ID: "b1", DocumentID: "doc-b", Text: "Beta vault retention policy."}, Score: 0.84},
		},
	}
	emb := &deadlineEmbedder{release: closedQueryChannel()}

	runQuery := func(t *testing.T, cfg *config.Config, body string) QueryResponse {
		t.Helper()
		amb := llm.NewAmbassador(cfg)
		amb.SetClient(llm.NewFakeConverser())
		ret := retriever.NewRetriever(store, emb, 0.70, 6)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		QueryHandler(ret, amb, cfg, nil).ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var got QueryResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		return got
	}

	// Legacy ignores routing margin even if set in config struct.
	legacyMargin := 0.50
	legacyCfg := deadlineQueryConfig(t)
	legacyCfg.RoutingPolicy = config.RoutingPolicyLegacy
	legacyCfg.RoutingMargin = &legacyMargin
	legacyResp := runQuery(t, legacyCfg, `{"query":"retention policy"}`)
	require.Equal(t, "LOCAL", legacyResp.Mode)

	// Evidence policy with margin 0.15 catches 0.06 lead and switches to GLOBAL.
	evidMargin := 0.15
	evidCfg := deadlineQueryConfig(t)
	evidCfg.RoutingPolicy = config.RoutingPolicyEvidence
	evidCfg.RoutingMargin = &evidMargin
	evidCfg.RoutingCalibrationMatched = true
	evidCfg.EffectiveRoutingMargin = evidMargin
	evidAmb := runQuery(t, evidCfg, `{"query":"retention policy"}`)
	require.Equal(t, "GLOBAL", evidAmb.Mode)
	require.Len(t, evidAmb.ScoredChunks, 2)

	// Explicit mode overrides remain unchanged under evidence policy.
	explicitLocal := runQuery(t, evidCfg, `{"query":"retention policy","mode":"local"}`)
	require.Equal(t, "LOCAL", explicitLocal.Mode)

	// Narrower margin (0.04 < 0.06 lead) allows strong-lead LOCAL.
	narrowCfg := deadlineQueryConfig(t)
	narrowCfg.RoutingPolicy = config.RoutingPolicyEvidence
	narrowCfg.RoutingCalibrationMatched = true
	narrowCfg.EffectiveRoutingMargin = 0.04
	narrowResp := runQuery(t, narrowCfg, `{"query":"retention policy"}`)
	require.Equal(t, "LOCAL", narrowResp.Mode)

	// Missing calibration in evidence mode conservatively selects GLOBAL even with large lead.
	uncalCfg := deadlineQueryConfig(t)
	uncalCfg.RoutingPolicy = config.RoutingPolicyEvidence
	uncalCfg.RoutingCalibrationMatched = false
	uncalResp := runQuery(t, uncalCfg, `{"query":"retention policy"}`)
	require.Equal(t, "GLOBAL", uncalResp.Mode)
}

type capturePromptConverser struct {
	prompts []string
	answer  string
}

func (c *capturePromptConverser) Converse(ctx context.Context, messages []brtypes.Message) (*bedrockruntime.ConverseOutput, error) {
	return c.ConverseWithModelAndSystem(ctx, "", "", messages, 0, 0)
}

func (c *capturePromptConverser) ConverseWithModelAndSystem(_ context.Context, _, _ string, messages []brtypes.Message, _ float32, _ int) (*bedrockruntime.ConverseOutput, error) {
	var b strings.Builder
	for _, m := range messages {
		for _, blk := range m.Content {
			if txt, ok := llm.BlockText(blk); ok {
				b.WriteString(txt)
			}
		}
	}
	c.prompts = append(c.prompts, b.String())
	return deadlineOutput(c.answer, brtypes.StopReasonEndTurn), nil
}

func (c *capturePromptConverser) ConverseStreamWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temp float32, maxTok int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	out, err := c.ConverseWithModelAndSystem(ctx, model, system, messages, temp, maxTok)
	if err != nil {
		return nil, err
	}
	if err := delta(c.answer); err != nil {
		return nil, err
	}
	return out, nil
}

func TestQueryDiversePolicyJSONAndSSEIdentity(t *testing.T) {
	firstHead := strings.Repeat("Primary archive retention lasts five years ", 10) + "."
	firstFull := firstHead + " " + strings.Repeat("appendix detail ", 40)
	dupChunk := firstFull
	trailChunk := "Exception: security incidents extend retention by ninety days."
	chunks := []domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: "c1", DocumentID: "00000000-0000-4000-8000-000000000001", Index: 0, Text: firstFull}, Score: .95},
		{Chunk: domain.Chunk{ID: "c2", DocumentID: "00000000-0000-4000-8000-000000000001", Index: 1, Text: dupChunk}, Score: .92},
		{Chunk: domain.Chunk{ID: "c3", DocumentID: "00000000-0000-4000-8000-000000000002", Index: 3, Text: trailChunk}, Score: .85},
	}

	cfg := streamQueryConfig()
	cfg.SynthesisContextTokens = 140
	cfg.SynthesisRelativeScoreFloor = .55
	cfg.SynthesisContextPolicy = config.SynthesisContextPolicyDiverse

	conv := &capturePromptConverser{answer: "Primary archive retention lasts five years [1]. Security incidents extend retention by ninety days [2]."}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(conv)
	ret := retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10)
	h := QueryHandler(ret, amb, cfg, nil)

	reqBody := `{"query":"retention","mode":"global","include_citations":true}`
	jsonReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(reqBody))
	jsonReq.Header.Set("Content-Type", "application/json")
	jsonRec := httptest.NewRecorder()
	h.ServeHTTP(jsonRec, jsonReq)
	require.Equal(t, http.StatusOK, jsonRec.Code, jsonRec.Body.String())

	var jsonResp QueryResponse
	require.NoError(t, json.Unmarshal(jsonRec.Body.Bytes(), &jsonResp))
	require.Equal(t, 2, jsonResp.ContextChunks)
	require.Len(t, jsonResp.ScoredChunks, 3)
	require.Equal(t, 1, jsonResp.ScoredChunks[0].Excerpt)
	require.True(t, jsonResp.ScoredChunks[0].Truncated)
	require.Equal(t, 0, jsonResp.ScoredChunks[1].Excerpt)
	require.False(t, jsonResp.ScoredChunks[1].Truncated)
	require.Equal(t, 2, jsonResp.ScoredChunks[2].Excerpt)
	require.False(t, jsonResp.ScoredChunks[2].Truncated)
	require.Len(t, jsonResp.Citations, 2)
	require.Equal(t, 2, jsonResp.Citations[1].Excerpt)
	require.Equal(t, "00000000-0000-4000-8000-000000000002", jsonResp.Citations[1].DocID)
	require.Equal(t, 3, jsonResp.Citations[1].ChunkIndex)

	sseRec := httptest.NewRecorder()
	h.ServeHTTP(sseRec, streamQueryRequest(t.Context(), reqBody))
	require.Equal(t, http.StatusOK, sseRec.Code, sseRec.Body.String())
	events := recordedEvents(t, sseRec.Body.String())
	require.GreaterOrEqual(t, len(events), 3)
	require.Equal(t, "context", events[0].name)
	doneEvent := events[len(events)-1]
	require.Equal(t, "done", doneEvent.name)

	var sseScored []ScoredChunkResponse
	require.NoError(t, json.Unmarshal(events[0].data["scored_chunks"], &sseScored))
	require.Equal(t, jsonResp.ScoredChunks, sseScored)

	var sseDone QueryStreamDone
	require.NoError(t, json.Unmarshal(bytesOrRaw(doneEvent.data), &sseDone))
	require.Equal(t, jsonResp.ContextTokens, sseDone.ContextTokens)
	require.Len(t, sseDone.Citations, 2)
	require.Equal(t, jsonResp.Citations[1].Excerpt, sseDone.Citations[1].Excerpt)
	require.Equal(t, jsonResp.Citations[1].DocID, sseDone.Citations[1].DocID)
	require.Equal(t, jsonResp.Citations[1].ChunkIndex, sseDone.Citations[1].ChunkIndex)

	require.Len(t, conv.prompts, 2)
	excerptRe := regexp.MustCompile(`(?s)<excerpt-[0-9a-f]+ id="(\d+)">\n(.*?)\n</excerpt-[0-9a-f]+>`)
	jsonBodies := excerptRe.FindAllStringSubmatch(conv.prompts[0], -1)
	sseBodies := excerptRe.FindAllStringSubmatch(conv.prompts[1], -1)
	require.Len(t, jsonBodies, 2)
	require.Len(t, sseBodies, 2)
	for i := range jsonBodies {
		require.Equal(t, fmt.Sprint(i+1), jsonBodies[i][1])
		require.Equal(t, jsonBodies[i][1:], sseBodies[i][1:])
	}
	require.Equal(t, firstHead, jsonBodies[0][2])
	require.Equal(t, trailChunk, jsonBodies[1][2])

	// Evidence mode JSON and SSE match the exact selected bodies.
	evidBody := `{"query":"retention","mode":"global","answer_mode":"evidence"}`
	evidJSONReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(evidBody))
	evidJSONReq.Header.Set("Content-Type", "application/json")
	evidJSONRec := httptest.NewRecorder()
	h.ServeHTTP(evidJSONRec, evidJSONReq)
	require.Equal(t, http.StatusOK, evidJSONRec.Code, evidJSONRec.Body.String())
	var evidJSON QueryResponse
	require.NoError(t, json.Unmarshal(evidJSONRec.Body.Bytes(), &evidJSON))
	require.Equal(t, []ScoredChunkResponse{
		{DocID: "00000000-0000-4000-8000-000000000001", ChunkIndex: 0, Score: .95, Text: firstHead, Excerpt: 1, Truncated: true},
		{DocID: "00000000-0000-4000-8000-000000000002", ChunkIndex: 3, Score: .85, Text: trailChunk, Excerpt: 2, Truncated: false},
	}, evidJSON.ScoredChunks)

	evidSSERec := httptest.NewRecorder()
	h.ServeHTTP(evidSSERec, streamQueryRequest(t.Context(), evidBody))
	require.Equal(t, http.StatusOK, evidSSERec.Code, evidSSERec.Body.String())
	evidEvents := recordedEvents(t, evidSSERec.Body.String())
	require.Len(t, evidEvents, 2)
	var evidSSEChunks []ScoredChunkResponse
	require.NoError(t, json.Unmarshal(evidEvents[0].data["scored_chunks"], &evidSSEChunks))
	require.Equal(t, evidJSON.ScoredChunks, evidSSEChunks)
}
