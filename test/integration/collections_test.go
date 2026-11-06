package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCollectionIngestionOverHTTP(t *testing.T) {
	c := durableDatabase(t)
	vectors := memory.NewFakeVectorStore()
	emb := embedder.NewFakeEmbedder(256)
	w := durableWorker(c, vectors, emb)
	t.Cleanup(w.Shutdown)
	cfg := &config.Config{
		FakeMode: true, BedrockFallbackModel: "offline", MaxUploadBytes: 1 << 20,
		MaxChunksPerQuery: 10, WorkerQueueSize: 10, ExtractConcurrency: 1,
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	server := httptest.NewServer(api.NewRouter(&api.RouterDeps{
		VectorStore: vectors, ChunkStore: c, Retriever: retriever.NewRetriever(vectors, emb, 0.7, 10),
		Ambassador: amb, Config: cfg, Worker: w, PineconeDriver: vectors, PGPool: c.Pool,
	}))
	t.Cleanup(server.Close)
	submit := func(collection, id string, status int, state string) string {
		t.Helper()
		body, err := json.Marshal(map[string]string{"content": "A synthetic archive retains reports for seven years.", "collection": collection, "document_id": id})
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/ingest", strings.NewReader(string(body)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		defer func() { require.NoError(t, resp.Body.Close()) }()
		require.Equal(t, status, resp.StatusCode)
		if status == http.StatusConflict {
			var apiErr handler.APIError
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&apiErr))
			require.Equal(t, "document_id already exists with different content", apiErr.Message)
			return ""
		}
		var result handler.IngestResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, state, result.Status)
		return result.DocumentID
	}
	a := submit("  cafe\u0301  ", "", 202, "processing")
	require.Equal(t, a, submit("café", "", 202, "processing"))
	b := submit("b", "", 202, "processing")
	require.NotEqual(t, a, b)
	defaultID := submit("", "", 202, "processing")
	require.NotEqual(t, a, defaultID)
	require.NotEqual(t, b, defaultID)
	callerID := uuid.NewString()
	require.Equal(t, callerID, submit("café", callerID, 202, "processing"))
	submit("b", callerID, 409, "")
	w.Start(t.Context())
	for _, id := range []string{a, b, defaultID, callerID} {
		durableStatus(t, c, id, "completed")
	}
	require.Equal(t, a, submit("café", "", 200, "already_ingested"))
	require.Equal(t, b, submit("b", "", 200, "already_ingested"))
	require.Equal(t, defaultID, submit("", "", 200, "already_ingested"))
	expected := map[string]string{a: "café", b: "b", defaultID: "", callerID: "café"}
	get := func(path string, target any) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, http.NoBody)
		require.NoError(t, err)
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		defer func() { require.NoError(t, resp.Body.Close()) }()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NoError(t, json.NewDecoder(resp.Body).Decode(target))
	}
	for id, collection := range expected {
		var summary map[string]any
		get("/documents/"+id, &summary)
		require.Contains(t, summary, "collection")
		require.Equal(t, collection, summary["collection"])
	}
	for _, path := range []string{"/documents", "/documents?limit=10"} {
		var listing struct {
			Documents []map[string]any `json:"documents"`
		}
		get(path, &listing)
		require.Len(t, listing.Documents, 4)
		for _, doc := range listing.Documents {
			require.Contains(t, doc, "collection")
			require.Equal(t, expected[doc["document_id"].(string)], doc["collection"])
		}
	}
}

func TestCollectionUploadsRetainIndependentSources(t *testing.T) {
	c := durableDatabase(t)
	objects := &durableObjectStore{}
	h := handler.IngestHandler(nil, c, objects, 1<<20)
	upload := func(collection string, status int) string {
		t.Helper()
		rec := httptest.NewRecorder()
		h(rec, durableFile(t, map[string]string{"collection": collection}))
		require.Equal(t, status, rec.Code, rec.Body.String())
		var result handler.IngestResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
		return result.DocumentID
	}
	defaultID := upload("", 202)
	a := upload("  a  ", 202)
	b := upload("b", 202)
	require.NotEqual(t, defaultID, a)
	require.NotEqual(t, a, b)
	require.Equal(t, a, upload("a", 202))
	require.Equal(t, 3, objects.uploads)
	require.Zero(t, objects.deletes)
	for _, id := range []string{defaultID, a, b} {
		key, err := c.GetDocumentS3Key(t.Context(), domain.DocumentID(id))
		require.NoError(t, err)
		require.NotNil(t, key)
	}
	for range 3 {
		job, err := c.ClaimJob(t.Context(), "collections", time.Minute, 5)
		require.NoError(t, err)
		require.NotNil(t, job)
		ok, err := c.CompleteJob(t.Context(), *job, nil)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Equal(t, a, upload("a", 200))
	require.Equal(t, 3, objects.uploads)
	_, err := c.Pool.Exec(t.Context(), `UPDATE documents SET status='failed' WHERE id=$1`, a)
	require.NoError(t, err)
	require.Equal(t, a, upload("a", 202))
	require.Equal(t, 3, objects.uploads)
}
