package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type bufferedVectorSearch struct {
	*memory.FakeVectorStore
	afterSearch func()
}

func (s *bufferedVectorSearch) QueryFiltered(ctx context.Context, embedding []float32, k int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	hits, err := s.FakeVectorStore.QueryFiltered(ctx, embedding, k, filter)
	if s.afterSearch != nil {
		after := s.afterSearch
		s.afterSearch = nil
		after()
	}
	return hits, err
}

func TestUntaggedSearchRejectsBufferedHitAfterSameScopeReplacement(t *testing.T) {
	c := durableDatabase(t)
	for _, mode := range []string{"global", "auto"} {
		for _, transport := range []string{"json", "sse"} {
			t.Run(mode+"/"+transport, func(t *testing.T) {
				ctx := t.Context()
				id := domain.DocumentID(uuid.NewString())
				chunkID := domain.ChunkID(uuid.NewString())
				index := &bufferedVectorSearch{FakeVectorStore: memory.NewFakeVectorStore()}
				const body = "The archive opens on Monday."
				seed := func(embedding []float32) string {
					t.Helper()
					_, err := c.AcceptDocument(ctx, domain.AcceptRequest{
						Document: domain.Document{ID: id, Content: body}, CallerSupplied: true,
						Collection: "public", Fingerprint: chunker.Fingerprint(body), MaxPending: 2,
					})
					require.NoError(t, err)
					job, err := c.ClaimJob(ctx, "worker", time.Minute, 3)
					require.NoError(t, err)
					require.NotNil(t, job)
					chunks := []domain.Chunk{{
						ID: chunkID, DocumentID: id, Text: body, Embedding: embedding,
						Metadata: map[string]any{"collection": "public"},
					}}
					require.NoError(t, index.Upsert(ctx, "", chunks))
					published, err := c.CompleteJob(ctx, *job, chunks, domain.IndexObservation{})
					require.NoError(t, err)
					require.True(t, published)
					return job.Incarnation
				}
				old := seed([]float32{1, 0})
				index.afterSearch = func() {
					require.NoError(t, index.DeleteByDocID(ctx, string(id)))
					require.NoError(t, c.DeleteDocument(ctx, id))
					require.NotEqual(t, old, seed([]float32{0, 1}))
				}

				cfg := &config.Config{MaxChunksPerQuery: 10}
				query := handler.QueryHandler(retriever.NewRetriever(index, revisionEmbedder{1, 0}, .7, 10), nil, cfg, c.Pool)
				request, err := json.Marshal(map[string]string{
					"query": "When does it open?", "collection": "public", "mode": mode, "answer_mode": "evidence",
				})
				require.NoError(t, err)
				run := func() (handler.QueryResponse, string) {
					t.Helper()
					req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/query", strings.NewReader(string(request)))
					req.Header.Set("Content-Type", "application/json")
					if transport == "sse" {
						req.Header.Set("Accept", "text/event-stream")
					}
					rec := httptest.NewRecorder()
					query(rec, req)
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					return revisionQueryResponse(t, rec, transport == "sse"), rec.Body.String()
				}
				got, output := run()
				require.Nil(t, index.afterSearch)
				if mode == "auto" {
					require.Equal(t, "GLOBAL", got.Mode)
				}
				require.Empty(t, got.ScoredChunks)
				require.Empty(t, got.Citations)
				require.NotContains(t, output, body)
				require.Zero(t, got.TokenUsage)

				fresh, _ := run()
				require.Len(t, fresh.ScoredChunks, 1)
				require.Equal(t, string(id), fresh.ScoredChunks[0].DocID)
				require.Equal(t, body, fresh.ScoredChunks[0].Text)
				require.Zero(t, fresh.ScoredChunks[0].Score)
			})
		}
	}
}

func TestTwoOfflineIndexesUseTheCompletedIncarnation(t *testing.T) {
	c := durableDatabase(t)
	ctx := t.Context()
	id := domain.DocumentID(uuid.NewString())
	const body = "The archive opens on Monday."
	retiredIndex, currentIndex := memory.NewFakeVectorStore(), memory.NewFakeVectorStore()
	complete := func(index *memory.FakeVectorStore, embed revisionEmbedder) string {
		t.Helper()
		completeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		admission, err := c.AcceptDocument(completeCtx, domain.AcceptRequest{
			Document: domain.Document{ID: id, Content: body}, CallerSupplied: true,
			Collection: "archive", Fingerprint: chunker.ScopedFingerprint("archive", body), MaxPending: 2,
		})
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, admission.Outcome)
		claims := make(chan domain.Job, 1)
		completed := make(chan completionOutcome, 1)
		stages := []pipeline.Stage{
			pipeline.NewChunkStage(chunker.NewChunker(128, 16)), pipeline.NewEmbedStage(embed, 1), pipeline.NewPersistStage(index),
		}
		w := worker.New(observedCompletionStore{observeClaims(c, claims), completed}, stages, index,
			worker.Options{Workers: 1, Owner: "worker", Lease: time.Minute, JobTimeout: 20 * time.Second})
		w.Start(completeCtx)
		defer func() { require.NoError(t, w.ShutdownContext(completeCtx)) }()
		job := awaitRevisionJob(completeCtx, t, claims)
		require.False(t, job.ScopedVectors)
		select {
		case result := <-completed:
			require.NoError(t, result.err)
			require.True(t, result.ok)
		case <-completeCtx.Done():
			t.Fatal(completeCtx.Err())
		}
		return job.Incarnation
	}
	retired := complete(retiredIndex, revisionEmbedder{1, 0})
	doc, err := c.GetDocument(ctx, id)
	require.NoError(t, err)
	require.NoError(t, cleanup.Remove(ctx, c, currentIndex, nil, doc))
	current := complete(currentIndex, revisionEmbedder{0, 1})
	require.NotEqual(t, retired, current)

	oldHits, err := retiredIndex.Query(ctx, "", []float32{1, 0}, 1, "")
	require.NoError(t, err)
	newHits, err := currentIndex.Query(ctx, "", []float32{1, 0}, 1, "")
	require.NoError(t, err)
	require.Len(t, oldHits, 1)
	require.Len(t, newHits, 1)
	require.Equal(t, oldHits[0].Chunk.ID, newHits[0].Chunk.ID)
	require.Equal(t, oldHits[0].Chunk.Text, newHits[0].Chunk.Text)
	require.EqualValues(t, 1, oldHits[0].Score)
	require.Zero(t, newHits[0].Score)

	for _, mode := range []string{"global", "auto"} {
		for _, transport := range []string{"json", "sse"} {
			for _, instance := range []struct {
				name  string
				index *memory.FakeVectorStore
			}{{"current", currentIndex}, {"retired", retiredIndex}} {
				t.Run(mode+"/"+transport+"/"+instance.name, func(t *testing.T) {
					cfg := &config.Config{MaxChunksPerQuery: 10}
					query := handler.QueryHandler(retriever.NewRetriever(instance.index, revisionEmbedder{1, 0}, .7, 10), nil, cfg, c.Pool)
					request := `{"query":"When does it open?","answer_mode":"evidence","collection":"archive","mode":"` + mode + `"}`
					req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(request))
					req.Header.Set("Content-Type", "application/json")
					if transport == "sse" {
						req.Header.Set("Accept", "text/event-stream")
					}
					rec := httptest.NewRecorder()
					query(rec, req)
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					got := revisionQueryResponse(t, rec, transport == "sse")
					if instance.index == currentIndex {
						require.Len(t, got.ScoredChunks, 1)
						require.Equal(t, string(id), got.ScoredChunks[0].DocID)
						require.Equal(t, body, got.ScoredChunks[0].Text)
						require.Zero(t, got.ScoredChunks[0].Score)
					} else {
						require.Empty(t, got.ScoredChunks)
						require.Empty(t, got.Citations)
						require.NotContains(t, rec.Body.String(), body)
					}
					require.Equal(t, "GLOBAL", got.Mode)
					require.NotContains(t, rec.Body.String(), retired)
					require.NotContains(t, rec.Body.String(), current)
				})
			}
		}
	}
}

func TestRetiredIncarnationCannotRouteOrSupplyJSONAndSSEEvidence(t *testing.T) {
	c := durableDatabase(t)
	ctx := t.Context()
	id, otherID := uuid.NewString(), uuid.NewString()
	const body = "The Evergreen Archive opens on Monday."
	chunk := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(id), Text: body}
	insert := func(ch domain.Chunk, collection, preview string) string {
		t.Helper()
		var revision string
		require.NoError(t, c.Pool.QueryRow(ctx, `INSERT INTO documents (id,content,content_preview,status,collection,scoped_vectors)
			VALUES ($1,$2,$3,'completed',$4,true) RETURNING incarnation::text`, ch.DocumentID, body, preview, collection).Scan(&revision))
		require.NoError(t, c.InsertChunks(ctx, []domain.Chunk{ch}))
		return revision
	}
	old := insert(chunk, "public", body)
	require.NoError(t, c.DeleteDocument(ctx, chunk.DocumentID))
	current := insert(chunk, "public", body)
	other := chunk
	other.ID, other.DocumentID = domain.ChunkID(uuid.NewString()), domain.DocumentID(otherID)
	foreign := insert(other, "private", body)
	vector := func(chunk domain.Chunk, revision, collection string, embedding []float32) domain.Chunk {
		chunk.Metadata = map[string]any{domain.VectorRevisionKey: revision, "collection": collection, "doc_type": domain.DefaultDocType}
		chunk.Embedding = embedding
		return chunk
	}
	store := memory.NewFakeVectorStore()
	// The higher-scoring stale hit and the private hit share the current body.
	require.NoError(t, store.Upsert(ctx, "", []domain.Chunk{
		vector(chunk, old, "public", []float32{1, 0}), vector(other, foreign, "private", []float32{1, 0}),
	}))
	cfg := &config.Config{BedrockFallbackModel: "offline", MaxChunksPerQuery: 10}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	router := handler.QueryHandler(retriever.NewRetriever(store, revisionEmbedder{1, 0}, 0.7, 10), amb, cfg, c.Pool)
	for _, withCurrent := range []bool{false, true} {
		if withCurrent {
			require.NoError(t, store.Upsert(ctx, "", []domain.Chunk{vector(chunk, current, "public", []float32{0.8, 0.6})}))
		}
		for _, evidence := range []bool{false, true} {
			for _, stream := range []bool{false, true} {
				reqBody := map[string]any{
					"query": "What are the Evergreen Archive hours?", "collection": "public",
					"allowed_document_ids": []string{id}, "include_citations": true, "mode": "auto",
				}
				if evidence {
					reqBody["answer_mode"] = "evidence"
					delete(reqBody, "include_citations")
				}
				data, err := json.Marshal(reqBody)
				require.NoError(t, err)
				req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/query", strings.NewReader(string(data)))
				req.Header.Set("Content-Type", "application/json")
				if stream {
					req.Header.Set("Accept", "text/event-stream")
				}
				rec := httptest.NewRecorder()
				router(rec, req)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				out := rec.Body.String()
				require.NotContains(t, out, otherID)
				require.NotContains(t, out, old)
				require.NotContains(t, out, current)
				response := revisionQueryResponse(t, rec, stream)
				if withCurrent {
					require.Len(t, response.ScoredChunks, 1)
					require.Equal(t, id, response.ScoredChunks[0].DocID)
					require.Equal(t, body, response.ScoredChunks[0].Text)
					require.InDelta(t, 0.8, response.ScoredChunks[0].Score, 1e-6)
					require.Equal(t, "LOCAL", response.Mode)
				} else {
					require.Empty(t, response.ScoredChunks)
					require.NotContains(t, out, body)
					require.NotContains(t, out, id)
					require.Equal(t, "GLOBAL", response.Mode)
				}
				if evidence || !withCurrent {
					require.Zero(t, response.TokenUsage)
				}
			}
		}
	}
}

func revisionQueryResponse(t *testing.T, rec *httptest.ResponseRecorder, stream bool) handler.QueryResponse {
	t.Helper()
	var response handler.QueryResponse
	if !stream {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		return response
	}
	emitted := make(map[string]bool)
	for _, frame := range strings.Split(rec.Body.String(), "\n\n") {
		event, data, ok := strings.Cut(frame, "\ndata: ")
		if !ok {
			continue
		}
		switch event {
		case "event: context", "event: done":
			require.NoError(t, json.Unmarshal([]byte(data), &response))
			emitted[event] = true
		case "event: error":
			t.Fatalf("stream failed: %s", data)
		}
	}
	require.Equal(t, map[string]bool{"event: context": true, "event: done": true}, emitted)
	return response
}
