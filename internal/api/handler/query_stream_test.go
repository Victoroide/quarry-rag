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
	call     func(context.Context, func(string) error) (*bedrockruntime.ConverseOutput, error)
	messages []brtypes.Message
}

func (c *queryStreamConverser) ConverseStreamWithModelAndSystem(ctx context.Context, _, _ string, messages []brtypes.Message, _ float32, _ int, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
	c.messages = messages
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

func requireQueryResponseHeaders(t *testing.T, rec *httptest.ResponseRecorder, status int, contentType string) http.Header {
	t.Helper()
	resp := rec.Result()
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, status, resp.StatusCode)
	require.Equal(t, contentType, resp.Header.Get("Content-Type"))
	if contentType == "text/event-stream" {
		require.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
		require.Equal(t, "no", resp.Header.Get("X-Accel-Buffering"))
	}
	return resp.Header
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

func TestQueryStreamRerankingPreservesScoresAndSynthesisOrder(t *testing.T) {
	for _, mode := range []string{"enabled", "disabled", "fallback", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			cfg := streamQueryConfig()
			cfg.RerankTimeout = 5 * time.Millisecond
			chunks := []domain.ScoredChunk{
				{Chunk: domain.Chunk{ID: "orchard-0", DocumentID: "orchard", Text: "Orchard archive."}, Score: .9},
				{Chunk: domain.Chunk{ID: "river-1", DocumentID: "river", Index: 1, Text: "River archive."}, Score: .8},
			}
			order := []domain.ScoredChunk{chunks[0], chunks[1]}
			if mode == "enabled" {
				order[0], order[1] = order[1], order[0]
			}
			calls := 0
			var reranker retriever.Reranker
			if mode != "disabled" {
				reranker = queryReranker(func(ctx context.Context, question string, in []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
					calls++
					require.Equal(t, "archive hours", question)
					require.Equal(t, chunks, in)
					if mode == "timeout" {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					if mode == "fallback" {
						return nil, errors.New("unavailable")
					}
					out := []domain.ScoredChunk{in[1], in[0]}
					for i, score := range []float64{.75, 0} {
						out[i].RerankScore = &score
					}
					return out, nil
				})
			}
			rec := httptest.NewRecorder()
			answer := order[0].Chunk.Text + " [1] " + order[1].Chunk.Text + " [2]"
			c := &queryStreamConverser{call: func(_ context.Context, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
				require.True(t, rec.Flushed)
				require.Contains(t, rec.Body.String(), "event: context\n")
				if err := delta(answer); err != nil {
					return nil, err
				}
				return deadlineOutput(answer, brtypes.StopReasonEndTurn), nil
			}}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(c)
			ret := retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10)
			QueryHandler(ret, amb, cfg, nil, WithReranker(reranker))(rec, streamQueryRequest(t.Context(), `{"query":"archive hours","mode":"global","include_citations":true}`))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
			events := recordedEvents(t, rec.Body.String())
			require.Len(t, events, 3)
			for i, name := range []string{"context", "delta", "done"} {
				require.Equal(t, name, events[i].name)
			}
			require.Equal(t, "2", string(events[0].data["chunks_used"]))
			require.Equal(t, "2", string(events[0].data["context_chunks"]))
			var scored []ScoredChunkResponse
			require.NoError(t, json.Unmarshal(events[0].data["scored_chunks"], &scored))
			require.Len(t, scored, 2)
			var rawChunks []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(events[0].data["scored_chunks"], &rawChunks))
			for i, chunk := range scored {
				require.Equal(t, string(order[i].Chunk.DocumentID), chunk.DocID)
				require.Equal(t, order[i].Score, chunk.Score)
				require.Equal(t, i+1, chunk.Excerpt)
				if mode == "enabled" {
					require.NotNil(t, chunk.RerankScore)
					require.Equal(t, []float64{.75, 0}[i], *chunk.RerankScore)
					require.Contains(t, rawChunks[i], "rerank_score")
				} else {
					require.Nil(t, chunk.RerankScore)
					require.NotContains(t, rawChunks[i], "rerank_score")
				}
			}
			require.Len(t, c.messages, 1)
			prompt := c.messages[0].Content[0].(*brtypes.ContentBlockMemberText).Value
			require.Contains(t, prompt, "id=\"1\">\n"+order[0].Chunk.Text+"\n")
			require.Contains(t, prompt, "id=\"2\">\n"+order[1].Chunk.Text+"\n")
			require.JSONEq(t, `"`+answer+`"`, string(events[1].data["text"]))
			var timings map[string]int64
			require.NoError(t, json.Unmarshal(events[2].data["stage_timings_ms"], &timings))
			_, timed := timings["rerank"]
			require.Equal(t, mode != "disabled", timed)
			require.JSONEq(t, `"stop"`, string(events[2].data["finish_reason"]))
			var citations []CitationResponse
			require.NoError(t, json.Unmarshal(events[2].data["citations"], &citations))
			require.Len(t, citations, 2)
			require.Equal(t, string(order[0].Chunk.DocumentID), citations[0].DocID)
			if mode == "disabled" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
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
	requireQueryResponseHeaders(t, rec, http.StatusOK, "text/event-stream")
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
		headers := requireQueryResponseHeaders(t, rec, http.StatusOK, "text/event-stream")
		require.Equal(t, "es", headers.Get("Content-Language"))
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
			streamQueryHandler(streamQueryConfig(), true, c)(rec, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global","include_citations":true}`))
			requireQueryResponseHeaders(t, rec, http.StatusOK, "text/event-stream")
			events := recordedEvents(t, rec.Body.String())
			require.Len(t, events, 3)
			require.Equal(t, "context", events[0].name)
			require.Equal(t, "delta", events[1].name)
			require.Equal(t, "error", events[2].name)
			require.JSONEq(t, `"`+tc.kind+`"`, string(events[2].data["error"]))
			require.NotContains(t, events[2].data, "citations")
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
			requireQueryResponseHeaders(t, rec, tc.code, "application/json")
		})
	}
	cfg := deadlineQueryConfig(t)
	rec := httptest.NewRecorder()
	h := deadlineQueryHandler(cfg, deadlineEmbedder{}, nil)
	h(rec, streamQueryRequest(t.Context(), `{"query":"archive"}`))
	deadlineAssertError(t, rec, 504, "timeout")
	requireQueryResponseHeaders(t, rec, http.StatusGatewayTimeout, "application/json")
}

var errTestEnded = errors.New("test ended before the upstream request was canceled")

// nextStreamEvent returns the name of the next event on the stream.
func nextStreamEvent(t *testing.T, scanner *bufio.Scanner) string {
	t.Helper()
	for scanner.Scan() {
		if name, ok := strings.CutPrefix(scanner.Text(), "event: "); ok {
			return name
		}
	}
	t.Fatalf("stream ended before the next event: %v", scanner.Err())
	return ""
}

// streamDisconnect serves a streamed query whose upstream is run, drops the
// client once it has read the named events and returns how the upstream
// ended. waitForDisconnect signals that the upstream is ready for the client
// to disconnect, then waits for cancellation. The test can also release the
// upstream during cleanup so that a missing cancellation cannot block shutdown.
func streamDisconnect(t *testing.T, events []string, run func(publish func(string) error, waitForDisconnect func() error) error) error {
	t.Helper()
	ready := make(chan struct{})
	ended := make(chan error, 1)
	testEnded := make(chan struct{})
	c := &queryStreamConverser{call: func(ctx context.Context, publish func(string) error) (*bedrockruntime.ConverseOutput, error) {
		err := run(publish, func() error {
			close(ready)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-testEnded:
				return errTestEnded
			}
		})
		ended <- err
		return nil, err
	}}
	cfg := streamQueryConfig()
	cfg.QueryTimeout, cfg.SynthesisTimeout = time.Hour, time.Hour
	srv := httptest.NewServer(streamQueryHandler(cfg, true, c))
	defer srv.Close()
	defer close(testEnded)
	ctx, cancel := context.WithCancel(testBudget(t))
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader(`{"query":"archive","mode":"global"}`))
	require.NoError(t, err)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "text/event-stream")
	resp, err := srv.Client().Do(r)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	scanner := bufio.NewScanner(resp.Body)
	for _, name := range events {
		require.Equal(t, name, nextStreamEvent(t, scanner))
	}
	awaitSignal(t, ready, "the upstream request to be ready for the disconnect")
	cancel()
	require.NoError(t, resp.Body.Close())
	return awaitSignal(t, ended, "the upstream request to end after the disconnect")
}

func TestQueryStreamDisconnectCancelsUpstream(t *testing.T) {
	t.Run("while synthesizing", func(t *testing.T) {
		err := streamDisconnect(t, []string{"context", "delta"}, func(publish func(string) error, waitForDisconnect func() error) error {
			if err := publish("partial"); err != nil {
				return err
			}
			return waitForDisconnect()
		})
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("before the next delta", func(t *testing.T) {
		err := streamDisconnect(t, []string{"context"}, func(publish func(string) error, waitForDisconnect func() error) error {
			if err := waitForDisconnect(); !errors.Is(err, context.Canceled) {
				return err
			}
			return publish("late")
		})
		require.ErrorIs(t, err, context.Canceled)
	})
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
	for _, accept := range []string{"application/json", "text/event-stream"} {
		rec := httptest.NewRecorder()
		r := streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`)
		r.Header.Set("Accept", accept)
		h.ServeHTTP(rec, r)
		deadlineAssertError(t, rec, http.StatusUnauthorized, "unauthorized")
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	}
	completed := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r); close(completed) }))
	defer srv.Close()
	ctx, cancel := context.WithCancel(testBudget(t))
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
	awaitSignal(t, called, "the stream to reach synthesis")
	cancel()
	awaitSignal(t, completed, "the middleware chain to complete")
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
	awaitSignal(t, entered, "the first query to start")
	rec := httptest.NewRecorder()
	h(rec, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`))
	deadlineAssertError(t, rec, 503, "overloaded")
	headers := requireQueryResponseHeaders(t, rec, http.StatusServiceUnavailable, "application/json")
	require.Equal(t, "1", headers.Get("Retry-After"))
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
	for _, tc := range []struct {
		name, accept, contentType string
	}{
		{"stream", "text/event-stream", "text/event-stream"},
		{"stream preferred", "application/json, text/event-stream;q=0.5", "text/event-stream"},
		{"zero weight", "text/event-stream;q=0", "application/json"},
		{"out of range weight", "text/event-stream;q=1.1", "application/json"},
		{"malformed weight", "text/event-stream;q=invalid", "application/json"},
		{"JSON", "application/json", "application/json"},
		{"wildcard", "*/*", "application/json"},
		{"omitted", "", "application/json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := streamQueryConfig()
			cfg.MinRelevanceScore = .95
			rec := httptest.NewRecorder()
			r := streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`)
			r.Header.Del("Accept")
			if tc.accept != "" {
				r.Header.Set("Accept", tc.accept)
			}
			streamQueryHandler(cfg, true, nil)(rec, r)
			requireQueryResponseHeaders(t, rec, http.StatusOK, tc.contentType)
			if tc.contentType == "text/event-stream" {
				events := recordedEvents(t, rec.Body.String())
				require.Len(t, events, 3)
				require.JSONEq(t, `[]`, string(events[0].data["scored_chunks"]))
				require.Equal(t, "done", events[2].name)
				require.NotContains(t, events[2].data, "citations")
				return
			}
			var response QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, "no_evidence", response.FinishReason)
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
