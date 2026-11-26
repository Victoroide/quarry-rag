package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestScopedDocumentOperationsEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	ctx := t.Context()
	store, err := pgstore.NewClient(ctx, startPostgres(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	require.NoError(t, store.RunMigrations(ctx))
	_, err = store.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)

	tenantA, tenantB := uuid.NewString(), uuid.NewString()
	ownerA := domain.Principal{ID: uuid.NewString(), TenantID: tenantA}
	peerA := domain.Principal{ID: uuid.NewString(), TenantID: tenantA}
	outsiderB := domain.Principal{ID: uuid.NewString(), TenantID: tenantB}

	issuer := (&url.URL{Scheme: "https", Host: "issuer.example.test"}).String()
	const audience = "quarry-api"
	const serviceToken = "integration-service-secret"

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwksDir := t.TempDir()
	jwksPath := filepath.Join(jwksDir, "jwks.json")
	jwkJSON, err := json.Marshal(map[string]any{
		"expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		"keys": []jose.JSONWebKey{{
			Key: &privKey.PublicKey, KeyID: "k1", Algorithm: string(jose.RS256), Use: "sig",
		}},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(jwksPath, jwkJSON, 0o600))
	verifier, err := subjectauth.NewVerifier(issuer, audience, jwksPath)
	require.NoError(t, err)

	importBatch := func(fill func(map[string]any)) {
		var rev int64
		require.NoError(t, store.Pool.QueryRow(ctx, `SELECT revision FROM grant_catalog_policy_revision`).Scan(&rev))
		batch := map[string]any{"batch_id": uuid.NewString(), "expected_revision": rev}
		fill(batch)
		raw, marshalErr := json.Marshal(batch)
		require.NoError(t, marshalErr)
		_, importErr := store.ImportGrantBatch(ctx, bytes.NewReader(raw), false)
		require.NoError(t, importErr)
	}
	importBatch(func(b map[string]any) {
		b["tenants"] = []map[string]any{{"tenant_id": tenantA}, {"tenant_id": tenantB}}
		b["collections"] = []map[string]any{
			{"tenant_id": tenantA, "collection": ""}, {"tenant_id": tenantA, "collection": "team"},
			{"tenant_id": tenantB, "collection": ""},
		}
		b["principals"] = []map[string]any{
			{"tenant_id": tenantA, "principal_id": ownerA.ID, "issuer": issuer, "subject": "owner-a", "enabled": true},
			{"tenant_id": tenantA, "principal_id": peerA.ID, "issuer": issuer, "subject": "peer-a", "enabled": true},
			{"tenant_id": tenantB, "principal_id": outsiderB.ID, "issuer": issuer, "subject": "outsider-b", "enabled": true},
		}
		b["create_permissions"] = []map[string]any{
			{"tenant_id": tenantA, "principal_id": ownerA.ID, "collection": "", "can_create": true},
			{"tenant_id": tenantA, "principal_id": ownerA.ID, "collection": "team", "can_create": true},
			{"tenant_id": tenantB, "principal_id": outsiderB.ID, "collection": "", "can_create": true},
		}
	})

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: privKey}, (&jose.SignerOptions{}).WithType("at+jwt").WithHeader("kid", "k1"))
	require.NoError(t, err)
	mint := func(sub string, lifetime time.Duration) string {
		now := time.Now()
		raw, signErr := json.Marshal(map[string]any{
			"iss": issuer, "sub": sub, "aud": audience, "client_id": "app", "jti": uuid.NewString(),
			"iat": now.Unix(), "exp": now.Add(lifetime).Unix(),
		})
		require.NoError(t, signErr)
		jws, jwsErr := signer.Sign(raw)
		require.NoError(t, jwsErr)
		compact, compErr := jws.CompactSerialize()
		require.NoError(t, compErr)
		return compact
	}

	var mu sync.Mutex
	var deletedObjects []string
	s3Endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			mu.Lock()
			deletedObjects = append(deletedObjects, strings.TrimPrefix(r.URL.Path, "/fixture-bucket/"))
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s3Endpoint.Close)
	t.Setenv("AWS_ENDPOINT_URL_S3", s3Endpoint.URL)
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
	t.Setenv("AWS_USE_FIPS_ENDPOINT", "false")
	t.Setenv("AWS_USE_DUALSTACK_ENDPOINT", "false")
	s3Client, err := s3store.New(ctx, &config.Config{
		AWSRegion: "us-east-1", AWSAccessKeyID: "fixture-key", AWSSecretKey: "fixture-secret", S3Bucket: "fixture-bucket",
	})
	require.NoError(t, err)

	vectors := memory.NewFakeVectorStore()
	tokens := []string{serviceToken}
	guard := func(next http.HandlerFunc) http.HandlerFunc {
		return middleware.RequireSubject(tokens, verifier, store, next)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest", guard(handler.IngestHandler(nil, store, s3Client, 1<<20,
		handler.WithOwnedIngestion(), handler.WithContentDedup(domain.ContentDedupOff),
		handler.WithExtractor(func(_ context.Context, _ extractor.FileType, b []byte) (string, error) { return string(b), nil }))))
	mux.HandleFunc("GET /documents", guard(handler.ListDocumentsHandler(store, handler.WithScopedListing())))
	mux.HandleFunc("GET /documents/{doc_id}", guard(handler.DocumentStatusHandler(store, handler.WithScopedStatus())))
	mux.HandleFunc("GET /documents/{doc_id}/file", guard(handler.FileHandler(store, s3Client, handler.WithScopedFile())))
	mux.HandleFunc("DELETE /documents/{doc_id}", guard(handler.DeleteHandler(store, vectors, s3Client,
		handler.WithScopedDeletion(), handler.WithCleanupTargets(cleanup.Targets{SourceBucket: "fixture-bucket", InMemory: true}))))

	doReq := func(method, path, sub string, lifetime time.Duration, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(body))
		req.Header.Set("X-Service-Token", serviceToken)
		req.Header.Set("Authorization", "Bearer "+mint(sub, lifetime))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	// Ingest one document for ownerA with stored object key and one for outsiderB.
	ingA := doReq(http.MethodPost, "/ingest", "owner-a", 2*time.Minute, `{"content":"tenant A private text","collection":"team"}`)
	require.Equal(t, http.StatusAccepted, ingA.Code, ingA.Body.String())
	var ingResp handler.IngestResponse
	require.NoError(t, json.Unmarshal(ingA.Body.Bytes(), &ingResp))
	docA := ingResp.DocumentID

	s3Key := "documents/document/" + docA + "/inc/att/report.pdf"
	_, err = store.Pool.Exec(ctx, `UPDATE documents SET s3_key=$2 WHERE id=$1`, docA, s3Key)
	require.NoError(t, err)
	var incA string
	require.NoError(t, store.Pool.QueryRow(ctx, `SELECT incarnation::text FROM documents WHERE id=$1`, docA).Scan(&incA))

	ingB := doReq(http.MethodPost, "/ingest", "outsider-b", 2*time.Minute, `{"content":"tenant B private text"}`)
	require.Equal(t, http.StatusAccepted, ingB.Code)

	// peerA cannot list, inspect, sign, or delete docA until granted.
	require.Equal(t, http.StatusNotFound, doReq(http.MethodGet, "/documents/"+docA, "peer-a", 2*time.Minute, "").Code)
	require.Equal(t, http.StatusNotFound, doReq(http.MethodGet, "/documents/"+docA+"/file", "peer-a", 2*time.Minute, "").Code)
	require.Equal(t, http.StatusNotFound, doReq(http.MethodDelete, "/documents/"+docA, "peer-a", 2*time.Minute, "").Code)

	// Grant peerA read+download; signing succeeds with TTL bounded by remaining assertion lifetime.
	importBatch(func(b map[string]any) {
		b["grants"] = []map[string]any{{
			"tenant_id": tenantA, "principal_id": peerA.ID, "document_id": docA, "incarnation": incA, "can_read": true, "can_download": true,
		}}
	})
	statusRec := doReq(http.MethodGet, "/documents/"+docA, "peer-a", 2*time.Minute, "")
	require.Equal(t, http.StatusOK, statusRec.Code)
	require.NotContains(t, statusRec.Body.String(), "s3_key")

	fileRec := doReq(http.MethodGet, "/documents/"+docA+"/file", "peer-a", 45*time.Second, "")
	require.Equal(t, http.StatusOK, fileRec.Code)
	var fileResp handler.FileResponse
	require.NoError(t, json.Unmarshal(fileRec.Body.Bytes(), &fileResp))
	require.LessOrEqual(t, fileResp.ExpiresIn, 45)
	require.GreaterOrEqual(t, fileResp.ExpiresIn, 43)
	parsedURL, err := url.Parse(fileResp.URL)
	require.NoError(t, err)
	require.NotEmpty(t, parsedURL.Query().Get("X-Amz-Expires"))

	// Grantee still cannot delete; ownerA deletes and removes s3Key.
	require.Equal(t, http.StatusNotFound, doReq(http.MethodDelete, "/documents/"+docA, "peer-a", 2*time.Minute, "").Code)
	delRec := doReq(http.MethodDelete, "/documents/"+docA, "owner-a", 2*time.Minute, "")
	require.Equal(t, http.StatusOK, delRec.Code, delRec.Body.String())
	mu.Lock()
	require.Equal(t, []string{s3Key}, deletedObjects)
	mu.Unlock()
	require.Equal(t, http.StatusNotFound, doReq(http.MethodGet, "/documents/"+docA, "owner-a", 2*time.Minute, "").Code)
}
