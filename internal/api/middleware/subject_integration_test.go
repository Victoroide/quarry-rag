package middleware

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type subjectLookupFunc func(context.Context, string, string) (domain.Principal, bool, error)

func (f subjectLookupFunc) ResolvePrincipal(ctx context.Context, issuer, subject string) (domain.Principal, bool, error) {
	return f(ctx, issuer, subject)
}

func TestSubjectCredentialsWithGrantCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon or the test services")
	}
	address := pgtest.Start(t)
	owner, err := postgres.NewClient(t.Context(), address)
	require.NoError(t, err)
	t.Cleanup(owner.Pool.Close)
	require.NoError(t, owner.RunMigrations(t.Context()))

	issuer := (&url.URL{Scheme: "https", Host: "issuer.example", Path: "/identity"}).String()
	const audience, firstService, secondService = "quarry", "fixture-service-first", "fixture-service-second"
	tenant, foreignTenant := uuid.NewString(), uuid.NewString()
	people := map[string]domain.Principal{
		"First":    {ID: uuid.NewString(), TenantID: tenant},
		"first":    {ID: uuid.NewString(), TenantID: tenant},
		"Other":    {ID: uuid.NewString(), TenantID: foreignTenant},
		"Disabled": {ID: uuid.NewString(), TenantID: tenant},
	}
	type entry = map[string]any
	var principals []entry
	for subject, person := range people {
		principals = append(principals, entry{"tenant_id": person.TenantID, "principal_id": person.ID, "issuer": issuer, "subject": subject, "enabled": subject != "Disabled"})
	}
	batch, err := json.Marshal(entry{"batch_id": uuid.NewString(), "expected_revision": 0, "tenants": []entry{{"tenant_id": tenant}, {"tenant_id": foreignTenant}}, "principals": principals})
	require.NoError(t, err)
	_, err = owner.ImportGrantBatch(t.Context(), bytes.NewReader(batch), false)
	require.NoError(t, err)

	// The verifier only needs the active-principal view, as does a restricted serving login.
	role := pgtest.Role(t, address, "fixture_subject_reader")
	quotedRole := pgx.Identifier{role}.Sanitize()
	_, err = owner.Pool.Exec(t.Context(), `CREATE ROLE `+quotedRole+`;
		GRANT USAGE ON SCHEMA public TO `+quotedRole+`;
		GRANT SELECT ON grant_catalog_active_principals TO `+quotedRole)
	require.NoError(t, err)
	servingConfig, err := pgxpool.ParseConfig(owner.Pool.Config().ConnString())
	require.NoError(t, err)
	servingConfig.ConnConfig.RuntimeParams["role"] = role
	servingConfig.ConnConfig.Tracer = tracing.NewQueryTracer()
	pool, err := pgxpool.NewWithConfig(t.Context(), servingConfig)
	require.NoError(t, err)
	serving := &postgres.Client{Pool: pool}
	t.Cleanup(serving.Pool.Close)
	var inaccessible bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT NOT has_table_privilege(current_user, 'grant_catalog_principals', 'SELECT')`).Scan(&inaccessible))
	require.True(t, inaccessible)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	now := time.Now().Truncate(time.Second)
	expires := now.Add(3 * time.Minute)
	trustPath := filepath.Join(t.TempDir(), "trust.json")
	trust, err := json.Marshal(entry{"expires_at": now.Add(time.Hour), "keys": []jose.JSONWebKey{{Key: key.Public(), KeyID: "fixture-key"}}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(trustPath, trust, 0o600))
	verifier, err := subjectauth.NewVerifier(issuer, audience, trustPath)
	require.NoError(t, err)
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithType("at+jwt").WithHeader("kid", "fixture-key"))
	require.NoError(t, err)
	mint := func(sub, aud string, exp time.Time) string {
		data, err := json.Marshal(entry{"iss": issuer, "sub": sub, "aud": aud, "client_id": "fixture-app", "jti": "fixture-reusable", "iat": now.Add(-time.Minute).Unix(), "exp": exp.Unix(), "role": "admin", "email": "other-subject"})
		require.NoError(t, err)
		signed, err := signer.Sign(data)
		require.NoError(t, err)
		token, err := signed.CompactSerialize()
		require.NoError(t, err)
		return token
	}
	firstJWT, sameTenantJWT, foreignJWT := mint("First", audience, expires), mint("first", audience, expires), mint("Other", audience, expires)
	invalidJWT, expiredJWT := mint("First", "wrong-audience", expires), mint("First", audience, now)

	previousLogger, previousTracer := slog.Default(), otel.GetTracerProvider()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
		otel.SetTracerProvider(previousTracer)
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
	})

	var got VerifiedSubject
	var lookups, docCalls, providerCalls int
	var waitForExpiry bool
	resolve := subjectLookupFunc(func(ctx context.Context, iss, sub string) (domain.Principal, bool, error) {
		lookups++
		if waitForExpiry {
			<-ctx.Done()
			return domain.Principal{}, false, ctx.Err()
		}
		return serving.ResolvePrincipal(ctx, iss, sub)
	})
	next := func(w http.ResponseWriter, r *http.Request) {
		var ok bool
		got, ok = SubjectFromContext(r.Context())
		require.True(t, ok)
		require.Empty(t, r.Header.Get("Authorization"))
		require.Empty(t, r.Header.Get("X-Service-Token"))
		deadline, ok := r.Context().Deadline()
		require.True(t, ok)
		require.Equal(t, got.ExpiresAt, deadline)
		docCalls++
		providerCalls++
		w.WriteHeader(http.StatusNoContent)
	}
	tokens := []string{firstService, secondService}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /query", RequireSubject(tokens, verifier, resolve, next))
	handler := RequestID(Tracing(Logging(mux), false))
	request := func(service, authorization []string, asserted, document string) *httptest.ResponseRecorder {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", bytes.NewBufferString(fmt.Sprintf(`{"document_id":%q}`, document)))
		for _, value := range service {
			r.Header.Add("X-Service-Token", value)
		}
		for _, value := range authorization {
			r.Header.Add("Authorization", value)
		}
		r.Header.Set("X-Forwarded-User", asserted)
		r.Header.Set("X-Tenant-ID", foreignTenant)
		r.Header.Set("X-Role", "admin")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		require.Equal(t, "private, no-store", w.Header().Get("Cache-Control"))
		return w
	}
	for _, tc := range []struct{ subject, service, jwt, asserted string }{
		{"First", firstService, firstJWT, "first"},
		{"First", secondService, firstJWT, "Other"},
		{"first", firstService, sameTenantJWT, "First"},
		{"Other", secondService, foreignJWT, "First"},
	} {
		w := request([]string{tc.service}, []string{"bEaReR  " + tc.jwt}, tc.asserted, uuid.NewString())
		require.Equal(t, http.StatusNoContent, w.Code)
		require.Equal(t, people[tc.subject], got.Principal)
		require.True(t, got.ValidAt(expires.Add(-time.Nanosecond)))
		require.False(t, got.ValidAt(expires))
		require.False(t, got.ValidAt(expires.Add(time.Nanosecond)))
	}
	for _, tc := range []struct{ service, authorization []string }{
		{nil, []string{"Bearer " + firstJWT}},
		{[]string{"wrong"}, []string{"Bearer " + firstJWT}},
		{[]string{firstService, secondService}, []string{"Bearer " + firstJWT}},
		{[]string{firstService + ", " + secondService}, []string{"Bearer " + firstJWT}},
		{[]string{firstService}, nil},
		{[]string{firstService}, []string{"Bearer " + firstJWT, "Bearer " + firstJWT}},
		{[]string{firstService}, []string{"Bearer " + firstJWT + ", Bearer " + foreignJWT}},
		{[]string{firstService}, []string{"Basic " + firstJWT}},
		{[]string{firstService}, []string{"Bearer "}},
		{[]string{firstService}, []string{"Bearer " + invalidJWT}},
		{[]string{firstService}, []string{"Bearer " + expiredJWT}},
	} {
		w := request(tc.service, tc.authorization, "Other", uuid.NewString())
		require.Equal(t, http.StatusUnauthorized, w.Code)
		require.JSONEq(t, `{"error":"unauthorized","message":"invalid or missing credentials"}`, w.Body.String())
	}
	require.Equal(t, 4, lookups)
	for _, sub := range []string{"Disabled", "FIRST", "unknown"} {
		w := request([]string{firstService}, []string{"Bearer " + mint(sub, audience, expires)}, "Other", uuid.NewString())
		require.Equal(t, http.StatusForbidden, w.Code)
		require.JSONEq(t, `{"error":"forbidden","message":"access denied"}`, w.Body.String())
	}
	waitForExpiry = true
	delayed := mint("First", audience, time.Now().Add(2*time.Second))
	w := request([]string{firstService}, []string{"Bearer " + delayed}, "Other", uuid.NewString())
	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Equal(t, 8, lookups)
	waitForExpiry = false
	serving.Pool.Close()
	w = request([]string{firstService}, []string{"Bearer " + firstJWT}, "Other", uuid.NewString())
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.JSONEq(t, `{"error":"unavailable","message":"authorization unavailable"}`, w.Body.String())
	require.NoError(t, os.Remove(trustPath))
	w = request([]string{firstService}, []string{"Bearer " + firstJWT}, "Other", uuid.NewString())
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, 9, lookups)
	require.Equal(t, 4, docCalls)
	require.Equal(t, 4, providerCalls)
	require.NotEmpty(t, recorder.Ended())
	var traceOutput bytes.Buffer
	for _, span := range recorder.Ended() {
		_, err := fmt.Fprintln(&traceOutput, span.Name(), span.Attributes(), span.Events(), span.Status())
		require.NoError(t, err)
	}
	for _, secret := range []string{firstService, secondService, firstJWT, sameTenantJWT, foreignJWT, invalidJWT, expiredJWT, issuer, "fixture-key"} {
		require.NotContains(t, logs.String(), secret)
		require.NotContains(t, traceOutput.String(), secret)
		require.NotContains(t, w.Body.String(), secret)
	}
}

func TestSubjectGateWithoutAuthorityFailsClosed(t *testing.T) {
	next := func(http.ResponseWriter, *http.Request) { t.Error("request reached a protected handler") }
	for _, tokens := range [][]string{nil, {"fixture-service"}} {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents", http.NoBody)
		r.Header.Set("X-Service-Token", "fixture-service")
		w := httptest.NewRecorder()
		RequireSubject(tokens, nil, nil, next)(w, r)
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	}
}
