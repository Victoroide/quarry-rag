package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

type queryStreamConverser struct {
	llm.FakeConverser
	call func(context.Context, func(string) error) (*bedrockruntime.ConverseOutput, error)
}

func (c *queryStreamConverser) ConverseStreamWithModelAndSystem(ctx context.Context, _, _ string, _ []brtypes.Message, _ float32, _ int, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
	return c.call(ctx, fn)
}

func streamQueryConfig() *config.Config {
	return &config.Config{MaxChunksPerQuery: 10, BedrockFallbackModel: "fallback", QueryTimeout: 5 * time.Second, SynthesisTimeout: 4 * time.Second}
}

func streamQueryHandler(cfg *config.Config, evidence bool, c llm.Converser) http.HandlerFunc {
	var chunks []domain.ScoredChunk
	if evidence {
		chunks = []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "chunk", DocumentID: "document", Index: 4, Text: "Archive evidence"}, Score: .9}}
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(c)
	return QueryHandler(retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10), amb, cfg, nil)
}

func streamQueryRequest(ctx context.Context, body string) *http.Request {
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/query", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "text/event-stream")
	return r
}

type recordedEvent struct {
	name string
	data map[string]json.RawMessage
}

func recordedEvents(t *testing.T, body string) []recordedEvent {
	t.Helper()
	var events []recordedEvent
	for _, frame := range strings.Split(strings.TrimSuffix(body, "\n\n"), "\n\n") {
		lines := strings.Split(frame, "\n")
		require.Len(t, lines, 2, body)
		require.True(t, strings.HasPrefix(lines[0], "event: "), body)
		require.True(t, strings.HasPrefix(lines[1], "data: "), body)
		var data map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &data))
		events = append(events, recordedEvent{strings.TrimPrefix(lines[0], "event: "), data})
	}
	return events
}

func TestQueryStreamFramingOrderAndCitations(t *testing.T) {
	rec := httptest.NewRecorder()
	answer := "Archive [99] evidence [1].\n"
	c := &queryStreamConverser{call: func(ctx context.Context, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
		require.True(t, rec.Flushed, "retrieval context must arrive before synthesis")
		require.Contains(t, rec.Body.String(), "event: context\n")
		for _, part := range []string{"Archive [99] ", "evidence [1].\n"} {
			if err := fn(part); err != nil {
				return nil, err
			}
		}
		out := deadlineOutput(answer, brtypes.StopReasonEndTurn)
		out.Usage = &brtypes.TokenUsage{InputTokens: aws.Int32(11), OutputTokens: aws.Int32(3), TotalTokens: aws.Int32(14)}
		return out, ctx.Err()
	}}
	h := streamQueryHandler(streamQueryConfig(), true, c)
	h(rec, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global","include_citations":true}`))
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
	events := recordedEvents(t, rec.Body.String())
	require.Len(t, events, 4)
	for i, name := range []string{"context", "delta", "delta", "done"} {
		require.Equal(t, name, events[i].name)
	}
	require.JSONEq(t, `"GLOBAL"`, string(events[0].data["mode"]))
	require.Equal(t, "1", string(events[0].data["context_chunks"]))
	var scored []ScoredChunkResponse
	require.NoError(t, json.Unmarshal(events[0].data["scored_chunks"], &scored))
	require.Equal(t, 1, scored[0].Excerpt)
	var text string
	for _, e := range events[1:3] {
		var part string
		require.NoError(t, json.Unmarshal(e.data["text"], &part))
		text += part
	}
	require.Equal(t, answer, text)
	done := events[3].data
	require.JSONEq(t, `"stop"`, string(done["finish_reason"]))
	require.JSONEq(t, `{"input_tokens":11,"output_tokens":3,"total_tokens":14}`, string(done["token_usage"]))
	require.Contains(t, done, "context_tokens")
	require.Contains(t, done, "stage_timings_ms")
	require.Contains(t, string(done["stage_timings_ms"]), "synthesis")
	require.JSONEq(t, `[{"excerpt":1,"doc_id":"document","chunk_index":4,"offset":22,"verified":true,"overlap":1}]`, string(done["citations"]))
}

func TestQueryStreamNoEvidence(t *testing.T) {
	for _, body := range []string{`{"query":"archive","mode":"global","include_citations":true}`, `{"query":"archive","allowed_document_ids":[],"include_citations":true}`} {
		rec := httptest.NewRecorder()
		r := streamQueryRequest(t.Context(), body)
		r.Header.Set("Accept-Language", "es-MX")
		h := streamQueryHandler(streamQueryConfig(), false, &queryStreamConverser{call: func(context.Context, func(string) error) (*bedrockruntime.ConverseOutput, error) {
			t.Error("no evidence called synthesis")
			return nil, nil
		}})
		h(rec, r)
		require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
		require.Equal(t, "es", rec.Header().Get("Content-Language"))
		events := recordedEvents(t, rec.Body.String())
		require.Len(t, events, 3)
		for i, name := range []string{"context", "delta", "done"} {
			require.Equal(t, name, events[i].name)
		}
		require.JSONEq(t, `"No se encontró información relevante para responder esta pregunta."`, string(events[1].data["text"]))
		require.JSONEq(t, `"no_evidence"`, string(events[2].data["finish_reason"]))
		require.JSONEq(t, `[]`, string(events[2].data["citations"]))
		require.Equal(t, "0", string(events[0].data["context_chunks"]))
		require.Equal(t, "0", string(events[2].data["context_tokens"]))
		require.Equal(t, "true", string(events[2].data["early_exit"]))
	}
}

func TestQueryStreamErrorsAfterContext(t *testing.T) {
	for _, tc := range []struct {
		name, kind string
		err        error
	}{
		{"failure", "pipeline", errors.New("private upstream detail")},
		{"timeout", "timeout", context.DeadlineExceeded},
		{"throttled", "upstream_throttled", &smithy.GenericAPIError{Code: "ThrottlingException", Message: "private upstream detail"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLeveledLogs(t)
			rec := httptest.NewRecorder()
			c := &queryStreamConverser{call: func(_ context.Context, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
				if err := fn("partial"); err != nil {
					return nil, err
				}
				return nil, tc.err
			}}
			streamQueryHandler(streamQueryConfig(), true, c)(rec, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`))
			require.Equal(t, http.StatusOK, rec.Code)
			events := recordedEvents(t, rec.Body.String())
			require.Len(t, events, 3)
			require.Equal(t, "error", events[2].name)
			require.JSONEq(t, `"`+tc.kind+`"`, string(events[2].data["error"]))
			require.NotContains(t, rec.Body.String(), "private upstream detail")
			require.Contains(t, logs.String(), `"msg":"query processing failed"`)
		})
	}
}

func TestQueryStreamPreEventErrorsRemainJSON(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		code       int
	}{
		{"validation", `{"query":""}`, 400},
		{"body limit", `{"query":"` + strings.Repeat("a", 1<<20) + `"}`, 413},
		{"required allowlist", `{"query":"archive"}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := streamQueryConfig()
			cfg.RequireDocumentAllowlist = tc.name == "required allowlist"
			rec := httptest.NewRecorder()
			streamQueryHandler(cfg, false, nil)(rec, streamQueryRequest(t.Context(), tc.body))
			require.Equal(t, tc.code, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		})
	}
	cfg := deadlineQueryConfig(t)
	rec := httptest.NewRecorder()
	h := deadlineQueryHandler(cfg, deadlineEmbedder{}, nil)
	h(rec, streamQueryRequest(t.Context(), `{"query":"archive"}`))
	deadlineAssertError(t, rec, 504, "timeout")
}

func TestQueryStreamDisconnectCancelsUpstream(t *testing.T) {
	canceled := make(chan struct{})
	c := &queryStreamConverser{call: func(ctx context.Context, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
		if err := fn("partial"); err != nil {
			return nil, err
		}
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}}
	srv := httptest.NewServer(streamQueryHandler(streamQueryConfig(), true, c))
	defer srv.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader(`{"query":"archive","mode":"global"}`))
	require.NoError(t, err)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "text/event-stream")
	resp, err := srv.Client().Do(r)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	scanner := bufio.NewScanner(resp.Body)
	require.True(t, scanner.Scan())
	require.Equal(t, "event: context", scanner.Text())
	cancel()
	require.NoError(t, resp.Body.Close())
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("disconnect did not cancel upstream")
	}
}

func TestQueryStreamThroughMiddlewareAndAuthentication(t *testing.T) {
	service := metrics.NewService(nil)
	called := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	c := &queryStreamConverser{call: func(ctx context.Context, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
		called <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
		text := "Archive evidence"
		if err := fn(text); err != nil {
			return nil, err
		}
		return deadlineOutput(text, brtypes.StopReasonEndTurn), nil
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /query", middleware.RequireServiceTokens([]string{"synthetic-token"}, streamQueryHandler(streamQueryConfig(), true, c)))
	h := middleware.RequestID(middleware.Metrics(middleware.Tracing(middleware.Logging(middleware.Recovery(mux)), false), service))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`))
	deadlineAssertError(t, rec, 401, "unauthorized")
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	completed := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r); close(completed) }))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/query", strings.NewReader(`{"query":"archive","mode":"global"}`))
	require.NoError(t, err)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "text/event-stream")
	r.Header.Set("X-Service-Token", "synthetic-token")
	resp, err := srv.Client().Do(r)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	scanner := bufio.NewScanner(resp.Body)
	require.True(t, scanner.Scan())
	require.Equal(t, "event: context", scanner.Text())
	require.NotEmpty(t, resp.Header.Get("X-Request-ID"))
	select {
	case <-called:
	case <-ctx.Done():
		t.Fatal("stream did not reach synthesis")
	}
	cancel()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("middleware did not complete")
	}
	var collected bytes.Buffer
	require.NoError(t, service.Registry.WriteText(&collected))
	require.Contains(t, collected.String(), `quarry_http_requests_total{route="POST /query",code="200"} 1`)
	require.Contains(t, collected.String(), `quarry_query_mode_total{mode="GLOBAL"} 1`)
	require.Contains(t, collected.String(), `quarry_pipeline_stage_duration_seconds_count{stage="synthesis"} 1`)
}

func TestQueryStreamAdmissionAndSynthesisTimeout(t *testing.T) {
	cfg := streamQueryConfig()
	cfg.QueryConcurrency = 1
	cfg.QueryQueueTimeout = 10 * time.Millisecond
	cfg.SynthesisTimeout = time.Second
	entered := make(chan struct{}, 1)
	c := &queryStreamConverser{call: func(ctx context.Context, _ func(string) error) (*bedrockruntime.ConverseOutput, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	h := streamQueryHandler(cfg, true, c)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		h(httptest.NewRecorder(), streamQueryRequest(ctx, `{"query":"archive","mode":"global"}`))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first query did not start")
	}
	rec := httptest.NewRecorder()
	h(rec, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`))
	deadlineAssertError(t, rec, 503, "overloaded")
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	cancel()
	<-finished
	cfg.SynthesisTimeout = 10 * time.Millisecond
	rec = httptest.NewRecorder()
	streamQueryHandler(cfg, true, c)(rec, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`))
	events := recordedEvents(t, rec.Body.String())
	require.Len(t, events, 2)
	require.Equal(t, "error", events[1].name)
	require.JSONEq(t, `"timeout"`, string(events[1].data["error"]))
}

func TestQueryStreamMinimumRelevanceAndNegotiation(t *testing.T) {
	for _, accept := range []string{"text/event-stream", "application/json, text/event-stream;q=0.5", "text/event-stream;q=0", "text/event-stream;q=invalid", "application/json", "*/*"} {
		t.Run(accept, func(t *testing.T) {
			cfg := streamQueryConfig()
			cfg.MinRelevanceScore = .95
			rec := httptest.NewRecorder()
			r := streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`)
			r.Header.Set("Accept", accept)
			streamQueryHandler(cfg, true, nil)(rec, r)
			if accept == "text/event-stream" || strings.Contains(accept, "q=0.5") {
				events := recordedEvents(t, rec.Body.String())
				require.Len(t, events, 3)
				require.JSONEq(t, `[]`, string(events[0].data["scored_chunks"]))
				require.NotContains(t, events[2].data, "citations")
			} else {
				require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			}
		})
	}
}

type failingQueryStreamWriter struct {
	*httptest.ResponseRecorder
	writes int
	err    error
}

func (w *failingQueryStreamWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.writes > 1 {
		return 0, w.err
	}
	return w.ResponseRecorder.Write(data)
}

func TestQueryStreamWriteFailureStopsSynthesis(t *testing.T) {
	failed := errors.New("connection closed")
	w := &failingQueryStreamWriter{ResponseRecorder: httptest.NewRecorder(), err: failed}
	var observed error
	c := &queryStreamConverser{call: func(_ context.Context, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
		observed = fn("partial")
		return nil, observed
	}}
	streamQueryHandler(streamQueryConfig(), true, c)(w, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`))
	require.ErrorIs(t, observed, failed)
	require.Equal(t, 2, w.writes, "failed connection should not receive another event")
	events := recordedEvents(t, w.Body.String())
	require.Len(t, events, 1)
	require.Equal(t, "context", events[0].name)
}

func TestQueryStreamLogsCompletion(t *testing.T) {
	logs := captureLeveledLogs(t)
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	rec := httptest.NewRecorder()
	streamQueryHandler(streamQueryConfig(), false, nil)(rec, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "event: done")
	require.Contains(t, logs.String(), `"msg":"query completed"`)
}
