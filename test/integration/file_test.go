package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDocumentFileDownload(t *testing.T) {
	store, err := pgstore.NewClient(t.Context(), startPostgres(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	require.NoError(t, store.RunMigrations(t.Context()))

	withFile, withoutFile := domain.DocumentID(uuid.NewString()), domain.DocumentID(uuid.NewString())
	key := "documents/document/" + string(withFile) + "/report.pdf"
	require.NoError(t, store.InsertDocument(t.Context(), domain.Document{ID: withFile, S3Key: &key}))
	require.NoError(t, store.InsertDocument(t.Context(), domain.Document{ID: withoutFile}))

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("presigning contacted the S3 endpoint")
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(endpoint.Close)
	t.Setenv("AWS_ENDPOINT_URL_S3", endpoint.URL)
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
	t.Setenv("AWS_USE_FIPS_ENDPOINT", "false")
	t.Setenv("AWS_USE_DUALSTACK_ENDPOINT", "false")
	s3, err := s3store.New(t.Context(), &config.Config{
		AWSRegion:      "us-east-1",
		AWSAccessKeyID: "fixture-key",
		AWSSecretKey:   "fixture-secret",
		S3Bucket:       "fixture-bucket",
	})
	require.NoError(t, err)

	const oldToken, newToken = "fixture-old-token", "fixture-new-token"
	newRouter := func(token string, files *s3store.Client) http.Handler {
		return api.NewRouter(&api.RouterDeps{
			Config: &config.Config{ServiceToken: token}, ChunkStore: store, S3Client: files,
		})
	}
	requestFile := func(router http.Handler, id domain.DocumentID, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents/"+string(id)+"/file", http.NoBody)
		req.Header.Set("X-Service-Token", token)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	assertDownload := func(rec *httptest.ResponseRecorder) {
		t.Helper()
		require.Equal(t, http.StatusOK, rec.Code)
		var body struct {
			URL       string          `json:"url"`
			ExpiresIn json.RawMessage `json:"expires_in"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
		require.Equal(t, "900", string(body.ExpiresIn))
		signed, err := url.Parse(body.URL)
		require.True(t, err == nil, "expected a parseable download address")
		require.Equal(t, endpoint.Listener.Addr().String(), signed.Host)
		require.Equal(t, "900", signed.Query().Get("X-Amz-Expires"))
	}
	assertError := func(rec *httptest.ResponseRecorder, status int, category string) {
		t.Helper()
		require.Equal(t, status, rec.Code)
		var body struct {
			handler.APIError
			URL json.RawMessage `json:"url"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
		require.Equal(t, category, body.Error)
		require.Empty(t, body.URL)
	}

	router := newRouter(oldToken, s3)
	assertDownload(requestFile(router, withFile, oldToken))
	assertError(requestFile(router, withoutFile, oldToken), http.StatusNotFound, "not_found")
	assertError(requestFile(router, domain.DocumentID(uuid.NewString()), oldToken), http.StatusNotFound, "not_found")
	assertError(requestFile(newRouter(oldToken, nil), withFile, oldToken), http.StatusNotFound, "not_found")

	rotated := newRouter(newToken, s3)
	assertError(requestFile(rotated, withFile, oldToken), http.StatusUnauthorized, "unauthorized")
	assertDownload(requestFile(rotated, withFile, newToken))
}
