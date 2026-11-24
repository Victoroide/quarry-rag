package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pc "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pg "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type retiringGenerator struct{ beforeReply func() }

func (g retiringGenerator) Generate(context.Context, llm.Request) (*llm.Response, error) {
	g.beforeReply()
	return &llm.Response{Text: "The archive opens on Monday. [1]", StopReason: "end_turn"}, nil
}
func (g retiringGenerator) Stream(ctx context.Context, req llm.Request, delta func(string) error) (*llm.Response, error) {
	r, e := g.Generate(ctx, req)
	if e == nil {
		e = delta(r.Text)
	}
	return r, e
}
func insertRevisionFixture(ctx context.Context, t *testing.T, c *pg.Client, ch domain.Chunk, coll string) string {
	t.Helper()
	var rev string
	require.NoError(t, c.Pool.QueryRow(ctx, "INSERT INTO documents (id,content,content_preview,status,collection,scoped_vectors) VALUES ($1,$2,$2,'completed',$3,true) RETURNING incarnation::text", ch.DocumentID, ch.Text, coll).Scan(&rev))
	require.NoError(t, c.InsertChunks(ctx, []domain.Chunk{ch}))
	return rev
}
func TestGeneratedReleaseRejectsRetiredCitation(t *testing.T) {
	c := durableDatabase(t)
	for _, mode := range []string{"json", "sse"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			v := memory.NewFakeVectorStore()
			ch := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(uuid.NewString()), Text: "The archive opens on Monday.", Embedding: []float32{1}}
			old := insertRevisionFixture(ctx, t, c, ch, "public")
			ch.Metadata = map[string]any{domain.VectorRevisionKey: old, "collection": "public"}
			require.NoError(t, v.Upsert(ctx, "", []domain.Chunk{ch}))
			cfg := &config.Config{BedrockFallbackModel: "offline", MaxChunksPerQuery: 10}
			amb := llm.NewAmbassador(cfg)
			replaced := false
			amb.SetGenerator(retiringGenerator{func() {
				require.NoError(t, c.DeleteDocument(ctx, ch.DocumentID))
				require.NotEqual(t, old, insertRevisionFixture(ctx, t, c, ch, "private"))
				replaced = true
			}})
			h := handler.QueryHandler(retriever.NewRetriever(v, revisionEmbedder{1}, .7, 10), amb, cfg, c.Pool)
			req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/query", strings.NewReader("{\"query\":\"What day?\",\"mode\":\"global\",\"collection\":\"public\",\"include_citations\":true}"))
			req.Header.Set("Content-Type", "application/json")
			if mode == "sse" {
				req.Header.Set("Accept", "text/event-stream")
			}
			w := httptest.NewRecorder()
			h(w, req)
			require.True(t, replaced)
			if mode == "json" {
				require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
				require.NotContains(t, w.Body.String(), "citations")
			} else {
				body := w.Body.String()
				require.Contains(t, body, "event: context")
				require.Contains(t, body, "event: error")
				require.NotContains(t, body, "event: delta")
				require.NotContains(t, body, "event: done")
				require.NotContains(t, body, "citations")
			}
		})
	}
}
func TestMalformedProbePreservesCurrentEvidence(t *testing.T) {
	c := durableDatabase(t)
	v := memory.NewFakeVectorStore()
	ch := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(uuid.NewString()), Text: "The current public record.", Embedding: []float32{1}}
	rev := insertRevisionFixture(t.Context(), t, c, ch, "")
	ch.Metadata = map[string]any{domain.VectorRevisionKey: rev}
	bad := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), Text: "foreign legacy vector without a document ID", Embedding: []float32{1}}
	require.NoError(t, v.Upsert(t.Context(), "", []domain.Chunk{ch, bad}))
	cfg := &config.Config{BedrockFallbackModel: "offline", MaxChunksPerQuery: 10}
	h := handler.QueryHandler(retriever.NewRetriever(v, revisionEmbedder{1}, .7, 10), llm.NewAmbassador(cfg), cfg, c.Pool)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader("{\"query\":\"Current public record?\",\"answer_mode\":\"evidence\"}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var response handler.QueryResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, response.ScoredChunks, 1)
	require.Equal(t, ch.Text, response.ScoredChunks[0].Text)
	vis := pg.Visibility{Pool: c.Pool}
	_, previews, err := vis.VisibleEvidenceWithPreviews(t.Context(), []domain.ScoredChunk{{Chunk: bad}, {Chunk: ch}}, []string{"", string(ch.DocumentID)})
	require.NoError(t, err)
	require.Equal(t, map[string]string{string(ch.DocumentID): ch.Text}, previews)
}

type unacknowledgedCleanup struct {
	*memory.FakeVectorStore
	delayed func(context.Context) error
}

func (v *unacknowledgedCleanup) DeleteByAttempt(ctx context.Context, id, rev, attempt string) error {
	if v.delayed == nil {
		v.delayed = func(ctx context.Context) error { return v.FakeVectorStore.DeleteByAttempt(ctx, id, rev, attempt) }
		return context.DeadlineExceeded
	}
	return v.FakeVectorStore.DeleteByAttempt(ctx, id, rev, attempt)
}
func TestDelayedCleanupCannotEraseCompletedRetry(t *testing.T) {
	c := durableDatabase(t)
	ctx := t.Context()
	_, err := c.Pool.Exec(ctx, "UPDATE document_deletion_activation SET activated_at=clock_timestamp()")
	require.NoError(t, err)
	id := domain.DocumentID(uuid.NewString())
	const body = "The archive opens on Monday."
	_, err = c.AcceptDocument(ctx, domain.AcceptRequest{Document: domain.Document{ID: id, Content: body}, CallerSupplied: true, Fingerprint: chunker.Fingerprint(body), MaxPending: 3, SupportsScopedVectors: true})
	require.NoError(t, err)
	v := &unacknowledgedCleanup{FakeVectorStore: memory.NewFakeVectorStore()}
	w, claims := revisionWorker(c, v, revisionEmbedder{1, 0})
	bctx, stop := context.WithTimeout(ctx, 18*time.Second)
	defer stop()
	w.Start(bctx)
	defer w.Shutdown()
	first := awaitRevisionJob(bctx, t, claims)
	second := awaitRevisionJob(bctx, t, claims)
	require.Equal(t, first.Incarnation, second.Incarnation)
	require.NotEqual(t, first.LeaseToken, second.LeaseToken)
	durableStatus(t, c, string(id), "completed")
	w.Shutdown()
	hits, err := v.Query(ctx, "", []float32{1, 0}, 10, "")
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.NoError(t, v.delayed(ctx))
	hits, err = v.Query(ctx, "", []float32{1, 0}, 10, "")
	require.NoError(t, err)
	var state string
	var n int
	var unknown bool
	require.NoError(t, c.Pool.QueryRow(ctx, "SELECT status,chunk_count,vector_mutation_uncertain FROM documents WHERE id=$1", id).Scan(&state, &n, &unknown))
	t.Logf("after late cleanup from attempt %d: attempt %d status=%s chunks=%d physical hits=%d uncertain=%v", first.Attempts, second.Attempts, state, n, len(hits), unknown)
	if len(hits) != 1 {
		t.Error("late delete removed completed retry's vector")
	}
}

type blockedUntaggedUpsert struct {
	*memory.FakeVectorStore
	entered, release chan struct{}
}

func (v *blockedUntaggedUpsert) Upsert(ctx context.Context, ns string, ch []domain.Chunk) error {
	close(v.entered)
	<-v.release
	return v.FakeVectorStore.Upsert(ctx, ns, ch)
}

type countedLegacySweeps struct {
	*memory.FakeVectorStore
	namespaces int
	documents  int
}

func (s *countedLegacySweeps) DeleteAllVectorsInNamespace(ctx context.Context, namespace string) error {
	s.namespaces++
	return s.FakeVectorStore.DeleteAllVectorsInNamespace(ctx, namespace)
}

func (s *countedLegacySweeps) DeleteByDocID(ctx context.Context, id string) error {
	s.documents++
	return s.FakeVectorStore.DeleteByDocID(ctx, id)
}

func TestLegacyDeleteWaitsForDispatchedVectorMutation(t *testing.T) {
	c := durableDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	v := &blockedUntaggedUpsert{memory.NewFakeVectorStore(), make(chan struct{}), make(chan struct{})}
	w, claims := revisionWorker(c, v, revisionEmbedder{1, 0})
	const content = "The archive opens on Monday."
	source := "sources/archive.txt"
	id := domain.DocumentID(uuid.NewString())
	accepted, err := c.AcceptDocument(ctx, domain.AcceptRequest{
		Document: domain.Document{ID: id, Content: content, S3Key: &source}, CallerSupplied: true,
		Fingerprint: chunker.Fingerprint(content), MaxPending: 2,
	})
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	var sourceDeletes atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || !strings.HasSuffix(r.URL.Path, "/"+source) {
			t.Errorf("unexpected source request: %s %s", r.Method, r.URL.Path)
		}
		sourceDeletes.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(endpoint.Close)
	t.Setenv("AWS_ENDPOINT_URL_S3", endpoint.URL)
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
	t.Setenv("AWS_USE_FIPS_ENDPOINT", "false")
	t.Setenv("AWS_USE_DUALSTACK_ENDPOINT", "false")
	files, err := s3store.New(ctx, &config.Config{
		AWSRegion: "us-east-1", AWSAccessKeyID: "fixture-key", AWSSecretKey: "fixture-secret", S3Bucket: "fixture-bucket",
	})
	require.NoError(t, err)
	sweeps := &countedLegacySweeps{FakeVectorStore: v.FakeVectorStore}
	w.Start(ctx)
	unblock := sync.OnceFunc(func() { close(v.release) })
	defer func() {
		unblock()
		w.Shutdown()
	}()
	job := awaitRevisionJob(ctx, t, claims)
	require.False(t, job.ScopedVectors)
	select {
	case <-v.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	req := httptest.NewRequestWithContext(ctx, http.MethodDelete, "/documents/"+string(job.Document.ID), http.NoBody)
	req.SetPathValue("doc_id", string(job.Document.ID))
	dropped := httptest.NewRecorder()
	deleteDocument := handler.DeleteHandler(c, sweeps, files)
	deleteDocument(dropped, req)
	require.Equal(t, http.StatusInternalServerError, dropped.Code, dropped.Body.String())
	require.Zero(t, sweeps.namespaces)
	require.Zero(t, sweeps.documents)
	require.Zero(t, sourceDeletes.Load())
	exists, err := c.DocumentExists(ctx, job.Document.ID)
	require.NoError(t, err)
	require.True(t, exists)
	unblock()
	w.Shutdown()
	retried := httptest.NewRecorder()
	deleteDocument(retried, req)
	require.Equal(t, http.StatusOK, retried.Code, retried.Body.String())
	require.Equal(t, 1, sweeps.namespaces)
	require.Equal(t, 2, sweeps.documents)
	require.EqualValues(t, 1, sourceDeletes.Load())
	hits, err := v.Query(ctx, "", []float32{1, 0}, 10, "")
	require.NoError(t, err)
	require.Empty(t, hits)
	exists, err = c.DocumentExists(ctx, job.Document.ID)
	require.NoError(t, err)
	require.False(t, exists)
}
func TestRejectedUpsertAddsNoUncertainty(t *testing.T) {
	c := durableDatabase(t)
	ctx := t.Context()
	id := domain.DocumentID(uuid.NewString())
	_, err := c.AcceptDocument(ctx, domain.AcceptRequest{Document: domain.Document{ID: id, Content: "text"}, Fingerprint: chunker.Fingerprint("text"), MaxPending: 3})
	require.NoError(t, err)
	job, err := c.ClaimJob(ctx, "worker", time.Minute, 3)
	require.NoError(t, err)
	p := &pc.Client{}
	err = c.MutateJobVectors(ctx, *job, func(call context.Context) error {
		return p.Upsert(call, "", []domain.Chunk{{DocumentID: id, ID: "aa", Text: strings.Repeat("&", 7000), Embedding: []float32{1}}})
	})
	require.True(t, errors.Is(err, pc.ErrMetadataTooLarge))
	var unknown bool
	require.NoError(t, c.Pool.QueryRow(ctx, "SELECT vector_mutation_uncertain FROM documents WHERE id=$1", id).Scan(&unknown))
	t.Logf("pre-dispatch validation error=%v, durable uncertainty=%v", err, unknown)
	if unknown {
		t.Error("rejected validation without a provider connection is classified as uncertain")
	}
}
func TestPgvectorRejectsBufferedHitAfterUUIDReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("requires pgvector")
	}
	ctx := t.Context()
	c, err := pg.NewClient(ctx, pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(c.Pool.Close)
	require.NoError(t, c.RunMigrations(ctx))
	store, err := pg.NewVectorStore(ctx, c, 2)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, "UPDATE document_deletion_activation SET activated_at=clock_timestamp()")
	require.NoError(t, err)
	id := domain.DocumentID(uuid.NewString())
	chunkID := domain.ChunkID(uuid.NewString())
	const body = "Current hours."
	seed := func(emb []float32) string {
		res, acceptErr := c.AcceptDocument(ctx, domain.AcceptRequest{Document: domain.Document{ID: id, Content: body}, CallerSupplied: true, Fingerprint: chunker.Fingerprint(body), MaxPending: 5, SupportsScopedVectors: true})
		require.NoError(t, acceptErr)
		require.Equal(t, domain.AcceptAccepted, res.Outcome)
		job, claimErr := c.ClaimJob(ctx, "worker", time.Minute, 3)
		require.NoError(t, claimErr)
		require.False(t, job.ScopedVectors)
		ok, completeErr := c.CompleteJob(ctx, *job, []domain.Chunk{{ID: chunkID, DocumentID: id, Text: body, Embedding: emb}}, domain.IndexObservation{})
		require.NoError(t, completeErr)
		require.True(t, ok)
		return job.Incarnation
	}
	filter := domain.VectorFilter{DocumentIDs: []string{string(id)}}
	old := seed([]float32{1, 0})
	buffered, err := store.QueryFiltered(ctx, []float32{1, 0}, 5, filter)
	require.NoError(t, err)
	require.Len(t, buffered, 1)
	require.NoError(t, c.DeleteDocument(ctx, id))
	require.NotEqual(t, old, seed([]float32{0, 1}))
	fresh, err := store.QueryFiltered(ctx, []float32{1, 0}, 5, filter)
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	vis := pg.Visibility{Pool: c.Pool}
	permitted, err := vis.VisibleEvidence(ctx, buffered)
	require.NoError(t, err)
	t.Logf("pgvector after exact-ID/text reuse: previous score=%v current score=%v retired candidates accepted=%d", buffered[0].Score, fresh[0].Score, len(permitted))
	require.Empty(t, permitted)
	permitted, err = vis.VisibleEvidence(ctx, fresh)
	require.NoError(t, err)
	require.Equal(t, fresh, permitted)
}

func TestLegacyDeleteChecksForMutationAtFinalRemoval(t *testing.T) {
	c := durableDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	index := &blockedUntaggedUpsert{memory.NewFakeVectorStore(), make(chan struct{}), make(chan struct{})}
	w, jobs := revisionWorker(c, index, revisionEmbedder{1, 0})
	released := sync.OnceFunc(func() { close(index.release) })
	defer func() {
		released()
		w.Shutdown()
	}()
	key := "original-source"
	id := domain.DocumentID(uuid.NewString())
	_, err := c.AcceptDocument(ctx, domain.AcceptRequest{
		Document:    domain.Document{ID: id, Content: "the second ordering", S3Key: &key},
		Fingerprint: chunker.Fingerprint("the second ordering"), MaxPending: 2,
	})
	require.NoError(t, err)

	// Enter the worker while deletion is removing the original source, after
	// the early check and before the final SQL removal.
	objects := sourceDeleteFunc(func(ctx context.Context, observed string) error {
		if observed != key {
			return fmt.Errorf("unexpected key %q", observed)
		}
		w.Start(ctx)
		select {
		case <-jobs:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-index.entered:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	doc, err := c.GetDocument(ctx, id)
	require.NoError(t, err)
	require.ErrorIs(t, cleanup.Remove(ctx, c, index, objects, doc), domain.ErrLiveVectorMutation)
	exists, err := c.DocumentExists(ctx, id)
	require.NoError(t, err)
	require.True(t, exists)
	released()
	w.Shutdown()
	require.NoError(t, cleanup.Remove(ctx, c, index, sourceDeleteFunc(func(context.Context, string) error { return nil }), doc))
	hits, err := index.Query(ctx, "", []float32{1, 0}, 10, "")
	require.NoError(t, err)
	require.Empty(t, hits)
}

type sourceDeleteFunc func(context.Context, string) error

func (f sourceDeleteFunc) Delete(ctx context.Context, key string) error { return f(ctx, key) }
