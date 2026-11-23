package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
)

func TestRouterEnforcesEvidenceOnlyPolicy(t *testing.T) {
	router := NewRouter(&RouterDeps{Config: &config.Config{QueryAnswerPolicy: "evidence_only", ServiceToken: "token"}})
	for _, tc := range []struct {
		name, body, accept, contentType string
		status                          int
	}{
		{"omitted", `{"query":"Archive?","allowed_document_ids":[]}`, "application/json", "application/json", 200},
		{"SSE", `{"query":"Archive?","answer_mode":"evidence","allowed_document_ids":[]}`, "text/event-stream", "application/json; charset=utf-8", 200},
		{"generated JSON", `{"query":"Archive?","answer_mode":"generated"}`, "application/json", "application/json", 403},
		{"generated SSE", `{"query":"Archive?","answer_mode":"generated"}`, "text/event-stream", "application/json", 403},
		{"invalid enum", `{"query":"Archive?","answer_mode":"other"}`, "application/json", "application/json", 400},
		{"empty enum", `{"query":"Archive?","answer_mode":""}`, "application/json", "application/json", 400},
		{"null enum", `{"query":"Archive?","answer_mode":null}`, "application/json", "application/json", 400},
		{"wrong type", `{"query":"Archive?","answer_mode":true}`, "application/json", "application/json", 400},
		{"duplicate", `{"query":"Archive?","answer_mode":"evidence","ANSWER_MODE":"generated"}`, "text/event-stream", "application/json", 400},
		{"citations", `{"query":"Archive?","include_citations":true}`, "application/json", "application/json", 400},
		{"form", "query=Archive&answer_mode=generated", "text/event-stream", "application/x-www-form-urlencoded", 415},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(tc.body))
			req.Header.Set("X-Service-Token", "token")
			req.Header.Set("Content-Type", tc.contentType)
			req.Header.Set("Accept", tc.accept)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			if tc.status == 200 {
				require.Contains(t, rec.Body.String(), `"finish_reason":"no_evidence"`)
			}
			if tc.status == 403 {
				require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
				require.Contains(t, rec.Body.String(), `"error":"forbidden"`)
			}
		})
	}
}

type requestLogPanicStore struct{ domain.ChunkStore }

func (requestLogPanicStore) ListDocuments(context.Context) ([]domain.DocumentRef, error) {
	panic("listing unavailable")
}

func TestRouterCorrelatesPanicLogs(t *testing.T) {
	router := NewRouter(&RouterDeps{Config: &config.Config{ServiceTokens: []string{"secret"}}, ChunkStore: requestLogPanicStore{}})
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(slog.NewJSONHandler(&output, nil))))
	t.Cleanup(func() { slog.SetDefault(previous) })
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents", http.NoBody)
	req.Header.Set("X-Service-Token", "secret")
	req.Header.Set("X-Request-ID", "listing-123")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError || rec.Header().Get("X-Request-ID") != "listing-123" {
		t.Fatalf("invalid panic response: %d %v", rec.Code, rec.Header())
	}
	decoder := json.NewDecoder(&output)
	for _, level := range []string{"ERROR", "INFO"} {
		var entry map[string]any
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry["request_id"] != "listing-123" || entry["level"] != level {
			t.Fatalf("invalid correlated log: %v", entry)
		}
		if level == "INFO" && entry["status"] != float64(500) {
			t.Fatalf("wrong access status: %v", entry)
		}
	}
}

type probeStore struct{ domain.ChunkStore }

func (probeStore) Ping(context.Context) error { return nil }
func (probeStore) ListDocuments(context.Context) ([]domain.DocumentRef, error) {
	return []domain.DocumentRef{}, nil
}
func TestServiceRoutesRequireAuthentication(t *testing.T) {
	router := NewRouter(&RouterDeps{Config: &config.Config{ServiceToken: "secret"}, ChunkStore: probeStore{}, VectorStore: memory.NewFakeVectorStore()})
	for _, route := range []struct{ method, path string }{{http.MethodPost, "/ingest"}, {http.MethodPost, "/query"}, {http.MethodGet, "/documents"}, {http.MethodGet, "/documents/550e8400-e29b-41d4-a716-446655440000/file"}, {http.MethodDelete, "/documents/550e8400-e29b-41d4-a716-446655440000"}} {
		req := httptest.NewRequestWithContext(t.Context(), route.method, route.path, http.NoBody)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s: got %d", route.method, route.path, rec.Code)
		}
	}
	for _, route := range []struct{ path, token string }{{"/health", ""}, {"/documents", "secret"}} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, route.path, http.NoBody)
		req.Header.Set("X-Service-Token", route.token)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Errorf("%s: got %d", route.path, rec.Code)
		}
	}
}

func TestServiceRoutesAcceptRotationTokens(t *testing.T) {
	router := NewRouter(&RouterDeps{Config: &config.Config{ServiceTokens: []string{"old-secret", "new-secret"}}, ChunkStore: probeStore{}, VectorStore: memory.NewFakeVectorStore()})
	for _, route := range []struct{ method, path string }{
		{http.MethodPost, "/ingest"},
		{http.MethodPost, "/query"},
		{http.MethodGet, "/documents"},
		{http.MethodGet, "/documents/not-a-uuid"},
		{http.MethodGet, "/documents/not-a-uuid/file"},
		{http.MethodDelete, "/documents/not-a-uuid"},
	} {
		for _, token := range []string{"", "wrong-secret", "old-secret", "new-secret"} {
			req := httptest.NewRequestWithContext(t.Context(), route.method, route.path, http.NoBody)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Service-Token", token)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			want := http.StatusUnauthorized
			if token == "old-secret" || token == "new-secret" {
				want = http.StatusBadRequest
				if route.path == "/documents" {
					want = http.StatusOK
				}
			}
			if rec.Code != want {
				t.Errorf("%s %s with token %q: got %d, want %d", route.method, route.path, token, rec.Code, want)
			}
		}
	}
}
