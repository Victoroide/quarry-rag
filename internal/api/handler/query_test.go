package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	return &config.Config{MaxChunksPerQuery: 10, BedrockFallbackModel: "fallback", QueryTimeout: 50 * time.Millisecond, SynthesisTimeout: 40 * time.Millisecond, QueryConcurrency: 1, QueryQueueTimeout: 20 * time.Millisecond}
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
			cfg := &config.Config{MaxChunksPerQuery: 10, BedrockFallbackModel: "fallback", QueryTimeout: 5 * time.Second, SynthesisTimeout: 4 * time.Second}
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
				if i < 5 {
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
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	rec := httptest.NewRecorder()
	deadlineQueryHandler(deadlineQueryConfig(t), deadlineEmbedder{}, nil)(rec, deadlineQueryRequest(ctx))
	deadlineAssertError(t, rec, 504, "timeout")
	require.NoError(t, ctx.Err(), "query exhausted the caller deadline")
}
func TestQueryOverloadReturns503(t *testing.T) {
	entered, release := make(chan struct{}, 2), make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	cfg := deadlineQueryConfig(t)
	cfg.QueryTimeout = time.Second
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
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			h := deadlineQueryHandler(deadlineQueryConfig(t), embedder.NewFakeEmbedder(256), deadlineConverser(func(context.Context) (*bedrockruntime.ConverseOutput, error) {
				return nil, fmt.Errorf("upstream: %w", err)
			}))
			h(rec, deadlineQueryRequest(ctx))
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
	entered := make(chan struct{}, 2)
	cfg := deadlineQueryConfig(t)
	cfg.QueryTimeout, cfg.QueryQueueTimeout = time.Second, time.Second
	h := deadlineQueryHandler(cfg, deadlineEmbedder{entered: entered}, nil)
	firstCtx, firstCancel := context.WithCancel(t.Context())
	defer firstCancel()
	firstDone := make(chan struct{})
	go func() { defer close(firstDone); h(httptest.NewRecorder(), deadlineQueryRequest(firstCtx)) }()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	rec := httptest.NewRecorder()
	go func() { defer close(done); h(rec, deadlineQueryRequest(ctx)) }()
	cancel()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Error("queued request did not stop after cancellation")
	}
	firstCancel()
	<-firstDone
	<-done
	require.Empty(t, rec.Body.String())
	require.Empty(t, rec.Header())
	require.Empty(t, entered, "queued request entered the pipeline")
}

func TestQueryRejectsImmediatelyWithZeroQueueTimeout(t *testing.T) {
	entered := make(chan struct{}, 2)
	cfg := deadlineQueryConfig(t)
	cfg.QueryTimeout, cfg.QueryQueueTimeout = time.Second, 0
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
