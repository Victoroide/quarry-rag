package integration

import (
	"encoding/json"
	"fmt"
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

func TestCollectionQueryAndListingOverHTTP(t *testing.T) {
	c := durableDatabase(t)
	vectors := memory.NewFakeVectorStore()
	emb := embedder.NewFakeEmbedder(256)
	w := durableWorker(c, vectors, emb)
	t.Cleanup(w.Shutdown)
	cfg := &config.Config{FakeMode: true, BedrockFallbackModel: "offline", MaxUploadBytes: 1 << 20, MaxChunksPerQuery: 10, WorkerQueueSize: 10, ExtractConcurrency: 1}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	server := httptest.NewServer(api.NewRouter(&api.RouterDeps{VectorStore: vectors, ChunkStore: c, Retriever: retriever.NewRetriever(vectors, emb, .7, 10), Ambassador: amb, Config: cfg, Worker: w, PineconeDriver: vectors, PGPool: c.Pool}))
	t.Cleanup(server.Close)
	ids := make([]string, 3)
	for i, collection := range []string{"archive", "private", ""} {
		body, err := json.Marshal(map[string]string{"content": "A synthetic archive retains reports for seven years.", "collection": collection})
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/ingest", strings.NewReader(string(body)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		var result handler.IngestResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.NoError(t, resp.Body.Close())
		require.Equal(t, 202, resp.StatusCode)
		ids[i] = result.DocumentID
	}
	w.Start(t.Context())
	for _, id := range ids {
		durableStatus(t, c, id, "completed")
	}
	for _, tc := range []struct {
		mode, collection string
		allowed          []string
		want             []string
	}{
		{"auto", "archive", nil, []string{ids[0]}},
		{"local", "archive", nil, []string{ids[0]}},
		{"global", "  archive  ", []string{ids[0], ids[1]}, []string{ids[0]}},
		{"global", "archive", []string{ids[1]}, []string{}},
		{"auto", "missing", nil, []string{}},
		{"global", "", nil, ids},
	} {
		body := map[string]any{"query": "archive", "mode": tc.mode, "collection": tc.collection}
		if tc.allowed != nil {
			body["allowed_document_ids"] = tc.allowed
		}
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/query", strings.NewReader(string(encoded)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		var result handler.QueryResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.NoError(t, resp.Body.Close())
		require.Equal(t, 200, resp.StatusCode)
		got := make([]string, 0, len(result.ScoredChunks))
		for _, sc := range result.ScoredChunks {
			got = append(got, sc.DocID)
		}
		require.ElementsMatch(t, tc.want, got)
	}
	for _, path := range []string{"/documents?collection=archive", "/documents?collection=archive&limit=1", "/documents?collection=archive&limit=1&after=" + ids[0], "/documents?collection=missing"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, http.NoBody)
		require.NoError(t, err)
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		var result struct {
			Documents []domain.DocumentSummary `json:"documents"`
			Count     int                      `json:"count"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.NoError(t, resp.Body.Close())
		require.Equal(t, 200, resp.StatusCode)
		want := 1
		if strings.Contains(path, "after=") || strings.Contains(path, "missing") {
			want = 0
		}
		require.Len(t, result.Documents, want)
		require.Equal(t, want, result.Count)
		for _, doc := range result.Documents {
			require.Equal(t, ids[0], doc.ID)
		}
	}
	// Stale vector labels must still be rejected by the document visibility check.
	_, err := c.Pool.Exec(t.Context(), `UPDATE documents SET collection='moved' WHERE id=$1`, ids[0])
	require.NoError(t, err)
	rec := scopeQuery(t, handler.QueryHandler(retriever.NewRetriever(vectors, emb, .7, 10), amb, cfg, c.Pool), `{"query":"archive","mode":"local","collection":"archive"}`)
	require.Equal(t, 200, rec.Code)
	var result handler.QueryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	require.Empty(t, result.ScoredChunks)
	require.Equal(t, "GLOBAL", result.Mode)
}

func TestCollectionIsAppliedInsideVectorQuery(t *testing.T) {
	cfg := &config.Config{BedrockFallbackModel: "fallback"}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	emb := embedder.NewFakeEmbedder(256)
	vector, err := emb.Embed(t.Context(), "archive")
	require.NoError(t, err)
	chunks := make([]domain.Chunk, 0, 31)
	for i := range 30 {
		chunks = append(chunks, domain.Chunk{ID: domain.ChunkID(fmt.Sprint(i)), DocumentID: domain.DocumentID(uuid.NewString()), Text: "private", Embedding: vector, Metadata: map[string]any{"collection": "private"}})
	}
	opposite := make([]float32, len(vector))
	for i, v := range vector {
		opposite[i] = -v
	}
	id := uuid.NewString()
	chunks = append(chunks, domain.Chunk{ID: "archived", DocumentID: domain.DocumentID(id), Text: "The archive opens at nine.", Embedding: opposite, Metadata: map[string]any{"collection": "archive"}})
	vs := memory.NewFakeVectorStore()
	require.NoError(t, vs.Upsert(t.Context(), "", chunks))
	rec := scopeQuery(t, handler.QueryHandler(retriever.NewRetriever(vs, emb, 0.7, 10), amb, cfg, nil), `{"query":"archive","mode":"global","max_chunks":10,"collection":"archive"}`)
	require.Equal(t, 200, rec.Code, rec.Body.String())
	var resp handler.QueryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, 1, resp.ChunksUsed)
	require.Equal(t, id, resp.ScoredChunks[0].DocID)
}
