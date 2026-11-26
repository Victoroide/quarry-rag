package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryStreamParser(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "text/event-stream", r.Header.Get("Accept"))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "synthetic-token", r.Header.Get("X-Service-Token"))
		assert.Equal(t, "es", r.Header.Get("Accept-Language"))
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/query", r.URL.Path)
		var body map[string]json.RawMessage
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.JSONEq(t, `[]`, string(body["allowed_document_ids"]))
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_, err := fmt.Fprint(w, ": keepalive\r\n\r\nevent: context\r\ndata: {\"mode\":\"GLOBAL\",\r\ndata: \"chunks_used\":0}\r\n\r\nevent: delta\r\ndata: {\"text\":\"évidence\\n\"}\r\n\r\nevent: delta\ndata: {\"text\":\""+strings.Repeat("a", 70<<10)+"\"}\n\nevent: done\ndata: {\"finish_reason\":\"stop\",\"citations\":[]}\n\n")
		assert.NoError(t, err)
	})
	var events []client.Event
	err := c.QueryStream(t.Context(), client.QueryRequest{Query: "archive", AllowedDocumentIDs: []string{}}, func(event client.Event) error { events = append(events, event); return nil }, client.WithAcceptLanguage("es"))
	require.NoError(t, err)
	require.Len(t, events, 4)
	for i, name := range []string{"context", "delta", "delta", "done"} {
		require.Equal(t, name, events[i].Type)
	}
	var delta struct {
		Text string `json:"text"`
	}
	require.NoError(t, json.Unmarshal(events[1].Data, &delta))
	require.Equal(t, "évidence\n", delta.Text)
	require.JSONEq(t, `{"mode":"GLOBAL","chunks_used":0}`, string(events[0].Data))
}

func TestQueryResponseCompletion(t *testing.T) {
	const contextFields = `"mode":"GLOBAL","chunks_used":1,"context_chunks":1,` +
		`"scored_chunks":[{"doc_id":"00000000-0000-4000-8000-000000000001","doc_type":"guide","chunk_index":0,"score":0.9,"text":"Archive evidence","excerpt":1}]`
	const generationFields = `"token_usage":{"input_tokens":11,"output_tokens":3,"total_tokens":14},` +
		`"context_tokens":8,"stage_timings_ms":{"retrieval":2,"synthesis":5}`
	const contextFrame = "event: context\ndata: {" + contextFields + "}\n\n"
	const deltaFrame = "event: delta\ndata: {\"text\":\"Archive evidence\"}\n\n"
	const doneFrame = "event: done\ndata: {\"finish_reason\":\"stop\"," + generationFields + "}\n\n"
	const requestID = "request-123"
	input := client.QueryRequest{Query: "archive", Mode: "global"}
	for _, tc := range []struct {
		name, accept, contentType, body string
		status                          int
		events                          []string
		checkError                      func(*testing.T, error)
	}{
		{
			name: "JSON success", accept: "application/json", contentType: "application/json", status: http.StatusOK,
			body: `{"answer":"Archive evidence","finish_reason":"stop",` + contextFields + "," + generationFields + "}",
		},
		{
			name: "JSON failure before context", accept: "text/event-stream", contentType: "application/json", status: http.StatusServiceUnavailable,
			body: `{"error":"overloaded","message":"too many concurrent queries"}`,
			checkError: func(t *testing.T, err error) {
				var got *client.APIError
				require.ErrorAs(t, err, &got)
				require.Equal(t, &client.APIError{
					StatusCode: http.StatusServiceUnavailable, Code: "overloaded", Message: "too many concurrent queries",
					RequestID: requestID, RetryAfter: time.Second,
				}, got)
			},
		},
		{
			name: "stream completed", accept: "text/event-stream", contentType: "text/event-stream", status: http.StatusOK,
			body:   contextFrame + deltaFrame + doneFrame,
			events: []string{"context", "delta", "done"},
		},
		{
			name: "stream failure after context", accept: "text/event-stream", contentType: "text/event-stream", status: http.StatusOK,
			body:   contextFrame + deltaFrame + "event: error\ndata: {\"error\":\"timeout\",\"message\":\"query processing timed out\"}\n\n",
			events: []string{"context", "delta", "error"},
			checkError: func(t *testing.T, err error) {
				var got *client.StreamError
				require.ErrorAs(t, err, &got)
				require.Equal(t, &client.StreamError{
					Code: "timeout", Message: "query processing timed out", RequestID: requestID,
				}, got)
			},
		},
		{
			name: "stream interrupted without a terminal frame", accept: "text/event-stream", contentType: "text/event-stream", status: http.StatusOK,
			body:   contextFrame + deltaFrame,
			events: []string{"context", "delta"},
			checkError: func(t *testing.T, err error) {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/query", r.URL.Path)
				assert.Equal(t, tc.accept, r.Header.Get("Accept"))
				assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
				assert.Equal(t, "synthetic-token", r.Header.Get("X-Service-Token"))
				var got client.QueryRequest
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&got))
				assert.Equal(t, input, got)
				w.Header().Set("Content-Type", tc.contentType)
				w.Header().Set("X-Request-ID", requestID)
				if tc.status == http.StatusServiceUnavailable {
					w.Header().Set("Retry-After", "1")
				}
				w.WriteHeader(tc.status)
				_, err := io.WriteString(w, tc.body)
				assert.NoError(t, err)
			})
			if tc.accept == "application/json" {
				result, err := c.Query(t.Context(), input)
				require.NoError(t, err)
				require.Equal(t, "Archive evidence", result.Answer)
				require.Equal(t, "stop", result.FinishReason)
				require.Equal(t, 1, result.ChunksUsed)
				require.Len(t, result.ScoredChunks, 1)
				require.Equal(t, client.TokenUsage{InputTokens: 11, OutputTokens: 3, TotalTokens: 14}, result.TokenUsage)
				return
			}
			var events []string
			err := c.QueryStream(t.Context(), input, func(event client.Event) error {
				events = append(events, event.Type)
				return nil
			})
			require.Equal(t, tc.events, events)
			if tc.checkError == nil {
				require.NoError(t, err)
				return
			}
			tc.checkError(t, err)
		})
	}
}

func TestEvidenceQueryClientHandlesJSONAndContextOnlyStream(t *testing.T) {
	const id = "00000000-0000-4000-8000-000000000001"
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.JSONEq(t, `"evidence"`, string(body["answer_mode"]))
		assert.JSONEq(t, `["`+id+`"]`, string(body["allowed_document_ids"]))
		if r.Header.Get("Accept") == "text/event-stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, err := fmt.Fprint(w, "event: context\ndata: {\"scored_chunks\":[{\"doc_id\":\""+id+"\",\"text\":\"Paper folds.\"}]}\n\nevent: done\ndata: {\"finish_reason\":\"evidence_only\"}\n\n")
			assert.NoError(t, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, err := fmt.Fprint(w, `{"finish_reason":"evidence_only","answer":"","scored_chunks":[{"doc_id":"`+id+`","text":"Paper folds."}]}`)
		assert.NoError(t, err)
	})
	in := client.QueryRequest{Query: "Archive?", AnswerMode: client.AnswerModeEvidence, AllowedDocumentIDs: []string{id}}
	response, err := c.Query(t.Context(), in)
	require.NoError(t, err)
	require.Equal(t, client.FinishReasonEvidenceOnly, response.FinishReason)
	var names []string
	err = c.QueryStream(t.Context(), in, func(event client.Event) error {
		names = append(names, event.Type)
		if event.Type == "context" {
			var value client.QueryStreamContext
			require.NoError(t, json.Unmarshal(event.Data, &value))
			require.Equal(t, response.ScoredChunks, value.ScoredChunks)
		} else {
			var value client.QueryStreamDone
			require.NoError(t, json.Unmarshal(event.Data, &value))
			require.Equal(t, client.FinishReasonEvidenceOnly, value.FinishReason)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"context", "done"}, names)
}

func TestQueryStreamRejectsInvalidResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body, contentType string
		want                    string
	}{
		{"broken json", "event: context\ndata: {\n\n", "text/event-stream", "invalid"},
		{"wrong content type", `{}`, "application/json", "Content-Type"},
		{"missing context", "event: done\ndata: {}\n\n", "text/event-stream", "context"},
		{"empty error", "event: context\ndata: {}\n\nevent: error\ndata: {}\n\n", "text/event-stream", "error"},
		{"duplicate context", "event: context\ndata: {}\n\nevent: context\ndata: {}\n\n", "text/event-stream", "duplicate"},
		{"invalid delta", "event: context\ndata: {}\n\nevent: delta\ndata: {\"text\":null}\n\n", "text/event-stream", "delta"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				_, err := io.WriteString(w, tc.body)
				assert.NoError(t, err)
			})
			err := c.QueryStream(t.Context(), client.QueryRequest{Query: "archive"}, func(client.Event) error { return nil })
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestQueryStreamCallbackCancelsRequest(t *testing.T) {
	canceled := make(chan struct{})
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, err := fmt.Fprint(w, "event: context\ndata: {}\n\n")
		assert.NoError(t, err)
		assert.NoError(t, http.NewResponseController(w).Flush())
		<-r.Context().Done()
		close(canceled)
	})
	failed := errors.New("stop reading")
	err := c.QueryStream(t.Context(), client.QueryRequest{Query: "archive"}, func(client.Event) error { return failed })
	require.ErrorIs(t, err, failed)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("callback failure did not cancel request")
	}
}

func TestQueryStreamCanceledContextAndNilCallback(t *testing.T) {
	c := newClient(t, func(http.ResponseWriter, *http.Request) { t.Error("invalid request reached server") })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.QueryStream(ctx, client.QueryRequest{Query: "archive"}, func(client.Event) error { return nil }), context.Canceled)
	require.Error(t, c.QueryStream(t.Context(), client.QueryRequest{Query: "archive"}, nil))
	require.Error(t, c.QueryStream(t.Context(), client.QueryRequest{Query: "archive"}, func(client.Event) error { return nil }, nil))
}

func TestQueryStreamDoneDecodesStrictBudgetFields(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, err := io.WriteString(w, "event: context\ndata: {\"mode\":\"GLOBAL\",\"chunks_used\":1,\"context_chunks\":1,\"scored_chunks\":[]}\n\n"+
			"event: delta\ndata: {\"text\":\"Ok\"}\n\n"+
			"event: done\ndata: {\"finish_reason\":\"stop\",\"token_usage\":{\"input_tokens\":60,\"output_tokens\":2,\"total_tokens\":62},\"context_tokens\":45,\"count_method\":\"bedrock_count_tokens\",\"model_input_tokens\":60,\"stage_timings_ms\":{\"synthesis\":3}}\n\n")
		assert.NoError(t, err)
	})
	var done client.QueryStreamDone
	err := c.QueryStream(t.Context(), client.QueryRequest{Query: "archive"}, func(ev client.Event) error {
		if ev.Type == "done" {
			return json.Unmarshal(ev.Data, &done)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "bedrock_count_tokens", done.CountMethod)
	require.Equal(t, 60, done.ModelInputTokens)
	require.Equal(t, 45, done.ContextTokens)
}

func TestQueryStreamSendsTheDocumentSetReference(t *testing.T) {
	const set = "20000000-0000-4000-8000-000000000001"
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.JSONEq(t, `"`+set+`"`, string(body["document_set_id"]))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte(`{"error":"not_found","message":"document set not found"}`))
		assert.NoError(t, err)
	})
	err := c.QueryStream(t.Context(), client.QueryRequest{Query: "archive", DocumentSetID: set}, func(client.Event) error { return nil })
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusNotFound, apiErr.StatusCode)
	require.Equal(t, "not_found", apiErr.Code)
}
