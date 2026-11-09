package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestQueryStreamFailures(t *testing.T) {
	for _, tc := range []struct {
		name, body, contentType string
		status                  int
		want                    string
	}{
		{"http", `{"error":"overloaded","message":"too many concurrent queries"}`, "application/json", 503, "overloaded"},
		{"stream error", "event: context\ndata: {}\n\nevent: error\ndata: {\"error\":\"timeout\",\"message\":\"query processing timed out\"}\n\n", "text/event-stream", 200, "timeout"},
		{"truncated", "event: context\ndata: {}\n\nevent: delta\ndata: {\"text\":\"partial\"}\n\n", "text/event-stream", 200, "unexpected EOF"},
		{"broken json", "event: context\ndata: {\n\n", "text/event-stream", 200, "invalid"},
		{"wrong content type", `{}`, "application/json", 200, "Content-Type"},
		{"missing context", "event: done\ndata: {}\n\n", "text/event-stream", 200, "context"},
		{"empty error", "event: context\ndata: {}\n\nevent: error\ndata: {}\n\n", "text/event-stream", 200, "error"},
		{"duplicate context", "event: context\ndata: {}\n\nevent: context\ndata: {}\n\n", "text/event-stream", 200, "duplicate"},
		{"invalid delta", "event: context\ndata: {}\n\nevent: delta\ndata: {\"text\":null}\n\n", "text/event-stream", 200, "delta"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				w.Header().Set("Retry-After", "2")
				w.Header().Set("X-Request-ID", "request-123")
				w.WriteHeader(tc.status)
				_, err := fmt.Fprint(w, tc.body)
				assert.NoError(t, err)
			})
			err := c.QueryStream(t.Context(), client.QueryRequest{Query: "archive"}, func(client.Event) error { return nil })
			require.ErrorContains(t, err, tc.want)
			if tc.name == "http" {
				var apiErr *client.APIError
				require.ErrorAs(t, err, &apiErr)
				require.Equal(t, 2*time.Second, apiErr.RetryAfter)
			}
			if tc.name == "stream error" {
				var streamErr *client.StreamError
				require.ErrorAs(t, err, &streamErr)
				require.Equal(t, "timeout", streamErr.Code)
				require.Equal(t, "request-123", streamErr.RequestID)
			}
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
