package middleware

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

type corpusModeFunc func(context.Context) (string, error)

func (f corpusModeFunc) CorpusAuthzMode(ctx context.Context) (string, error) {
	return f(ctx)
}

type auditRecorderFunc func(context.Context, domain.SecurityAuditEvent) error

func (f auditRecorderFunc) RecordSecurityAudit(ctx context.Context, event domain.SecurityAuditEvent) error {
	return f(ctx, event)
}

func TestRequireAuthzModeFailsClosedOnMismatchOrUnavailable(t *testing.T) {
	called := 0
	okHandler := func(w http.ResponseWriter, _ *http.Request) {
		called++
		w.WriteHeader(http.StatusNoContent)
	}

	t.Run("grants matches", func(t *testing.T) {
		called = 0
		rec := httptest.NewRecorder()
		h := RequireAuthzMode("grants", corpusModeFunc(func(context.Context) (string, error) { return "grants", nil }), okHandler)
		h(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents", http.NoBody))
		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, 1, called)
		require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
	})

	t.Run("grants mismatches service corpus", func(t *testing.T) {
		called = 0
		rec := httptest.NewRecorder()
		h := RequireAuthzMode("grants", corpusModeFunc(func(context.Context) (string, error) { return "service", nil }), okHandler)
		h(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents", http.NoBody))
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Equal(t, 0, called)
	})

	t.Run("service mismatches grants corpus", func(t *testing.T) {
		called = 0
		rec := httptest.NewRecorder()
		h := RequireAuthzMode("service", corpusModeFunc(func(context.Context) (string, error) { return "grants", nil }), okHandler)
		h(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents", http.NoBody))
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Equal(t, 0, called)
	})

	t.Run("grants rejects nil reader or read error", func(t *testing.T) {
		for _, reader := range []domain.CorpusModeReader{
			nil,
			corpusModeFunc(func(context.Context) (string, error) { return "", errors.New("db down") }),
		} {
			called = 0
			rec := httptest.NewRecorder()
			h := RequireAuthzMode("grants", reader, okHandler)
			h(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents", http.NoBody))
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
			require.Equal(t, 0, called)
		}
	})
}

func TestLoggingWithRouteTemplatesAndTracingRedaction(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(slog.NewJSONHandler(&buf, nil))))
	t.Cleanup(func() { slog.SetDefault(prev) })

	const sampleDocUUID = "8f1d45e9-1111-4444-8888-abcdef123456"
	var sawCapture, sawRedact bool

	mux := http.NewServeMux()
	mux.HandleFunc("GET /documents/{doc_id}/file", func(w http.ResponseWriter, r *http.Request) {
		sawCapture = tracing.ContentCapture(r.Context())
		sawRedact = tracing.RedactIdentifiers(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	handler := RequestID(TracingWithRedaction(LoggingWithRouteTemplates(mux), true, true))

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents/"+sampleDocUUID+"/file", http.NoBody)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.False(t, sawCapture)
	require.True(t, sawRedact)

	recUnmatched := httptest.NewRecorder()
	reqUnmatched := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents/"+sampleDocUUID+"/unknown", http.NoBody)
	handler.ServeHTTP(recUnmatched, reqUnmatched)
	require.Equal(t, http.StatusNotFound, recUnmatched.Code)

	logOut := buf.String()
	require.NotContains(t, logOut, sampleDocUUID)
	var first, second map[string]any
	dec := json.NewDecoder(&buf)
	require.NoError(t, dec.Decode(&first))
	require.NoError(t, dec.Decode(&second))
	require.Equal(t, "/documents/{doc_id}/file", first["path"])
	require.Equal(t, "unmatched", second["path"])
}

func TestRequireSubjectWithAuditRecordsDenials(t *testing.T) {
	var recorded []domain.SecurityAuditEvent
	auditor := auditRecorderFunc(func(_ context.Context, event domain.SecurityAuditEvent) error {
		recorded = append(recorded, event)
		return nil
	})

	h := RequireSubjectWithAudit([]string{"service-secret"}, nil, nil, auditor, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", http.NoBody)
	req.Header.Set("X-Service-Token", "service-secret")
	req.Header.Set("Authorization", "Bearer raw-jwt-must-not-leak")
	h(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Len(t, recorded, 1)
	require.Equal(t, "subject_auth", recorded[0].Action)
	require.Equal(t, "error", recorded[0].Outcome)
	require.Equal(t, "authority_unavailable", recorded[0].Reason)
	require.NotContains(t, recorded[0].Reason, "raw-jwt-must-not-leak")
	require.NotContains(t, recorded[0].Reason, "service-secret")
}

func TestRequireSubjectWithAuditAuthenticatesServiceTokenBeforeAuditing(t *testing.T) {
	const serviceToken = "service-secret"
	issuer := (&url.URL{Scheme: "https", Host: "issuer.example"}).String()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	trust, err := json.Marshal(map[string]any{
		"expires_at": time.Now().Add(time.Hour),
		"keys":       []jose.JSONWebKey{{Key: key.Public(), KeyID: "first"}},
	})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "trust.json")
	require.NoError(t, os.WriteFile(path, trust, 0o600))
	verifier, err := subjectauth.NewVerifier(issuer, "quarry", path)
	require.NoError(t, err)

	var recorded []domain.SecurityAuditEvent
	auditor := auditRecorderFunc(func(_ context.Context, event domain.SecurityAuditEvent) error {
		recorded = append(recorded, event)
		return nil
	})
	resolver := subjectLookupFunc(func(context.Context, string, string) (domain.Principal, bool, error) {
		t.Fatal("principal lookup must not run for an unverified credential")
		return domain.Principal{}, false, nil
	})
	h := RequireSubjectWithAudit([]string{serviceToken}, verifier, resolver, auditor, func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run")
	})
	do := func(serviceHeaders []string, authorization string) int {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents", http.NoBody)
		for _, value := range serviceHeaders {
			req.Header.Add("X-Service-Token", value)
		}
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec.Code
	}

	for name, headers := range map[string][]string{
		"missing":    nil,
		"wrong":      {"other-secret"},
		"duplicated": {serviceToken, serviceToken},
		"joined":     {serviceToken + "," + serviceToken},
	} {
		require.Equal(t, http.StatusUnauthorized, do(headers, "Bearer anything"), name)
		require.Equal(t, http.StatusUnauthorized, do(headers, ""), name)
	}
	require.Empty(t, recorded, "unauthenticated service callers must not write audit rows")

	require.Equal(t, http.StatusUnauthorized, do([]string{serviceToken}, ""))
	require.Equal(t, http.StatusUnauthorized, do([]string{serviceToken}, "Bearer not-a-token"))
	require.Len(t, recorded, 2)
	require.Equal(t, "missing_subject_credential", recorded[0].Reason)
	require.Equal(t, "invalid_or_expired_token", recorded[1].Reason)
	for _, event := range recorded {
		require.Equal(t, "deny", event.Outcome)
	}
}
