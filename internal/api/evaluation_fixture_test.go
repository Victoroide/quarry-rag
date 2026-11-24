package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOrdinaryDeletePreservesIndexedEvaluationFixture(t *testing.T) {
	db, err := postgres.NewClient(t.Context(), pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))
	vectors, err := postgres.NewVectorStore(t.Context(), db, 3)
	require.NoError(t, err)
	ns := evaluation.Namespace{Prefix: "staging", ID: uuid.New()}
	spec := evaluation.Documents()[0]
	id, receipt := ns.DocumentID(spec.Key), uuid.NewString()
	router := NewRouter(&RouterDeps{FixtureVectorIdentity: "postgres", Config: &config.Config{VectorStore: "postgres", EvaluationCollectionPrefix: ns.Prefix}, ChunkStore: db, VectorStore: vectors, PineconeDriver: vectors})
	rev := acceptRouterFixture(t, router, ns, spec, receipt)
	job, err := db.ClaimJobForFixtureStore(t.Context(), "fixture-worker", time.Minute, 2, "postgres")
	require.NoError(t, err)
	require.NotNil(t, job)
	ok, err := db.CompleteJob(t.Context(), *job, []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(id), Text: spec.Text, Embedding: []float32{1, 0, 0}}}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, fixtureVectorCount(t, db, id))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+id, http.NoBody))
	assert.Equal(t, http.StatusPreconditionFailed, w.Code, w.Body.String())
	assert.Equal(t, 1, fixtureVectorCount(t, db, id))
	fixture, err := db.ReadEvaluationFixture(t.Context(), domain.DocumentID(id), receipt)
	require.NoError(t, err)
	require.Equal(t, domain.FixtureOwned, fixture.State)
	require.Equal(t, rev, fixture.Incarnation)
}

func TestFixtureDeletionRequiresOriginalVectorStore(t *testing.T) {
	db, err := postgres.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))
	ns := evaluation.Namespace{Prefix: "staging", ID: uuid.New()}
	spec := evaluation.Documents()[0]
	id, receipt := ns.DocumentID(spec.Key), uuid.NewString()
	originalVectors := memory.NewFakeVectorStore()
	makeRouter := func(backend, index string, store domain.VectorStore) http.Handler {
		return NewRouter(&RouterDeps{FixtureVectorIdentity: backend + ":" + index, Config: &config.Config{VectorStore: backend, PineconeIndexName: index, EvaluationCollectionPrefix: ns.Prefix}, ChunkStore: db, VectorStore: store, PineconeDriver: store.(domain.VectorDeleter)})
	}
	original := makeRouter("pinecone", "original", originalVectors)
	rev := acceptRouterFixture(t, original, ns, spec, receipt)
	for _, identity := range []string{"", "postgres", "pinecone:replacement"} {
		job, claimErr := db.ClaimJobForFixtureStore(t.Context(), "wrong-store", time.Minute, 2, identity)
		require.NoError(t, claimErr)
		require.Nil(t, job)
	}
	job, err := db.ClaimJobForFixtureStore(t.Context(), "original-store", time.Minute, 2, "pinecone:original")
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, domain.DocumentID(id), job.Document.ID)
	ok, err := db.CompleteJob(t.Context(), *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, originalVectors.Upsert(t.Context(), "", []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(id), Text: spec.Text, Embedding: []float32{1}}}))
	for _, alternative := range []http.Handler{
		makeRouter("postgres", "", memory.NewFakeVectorStore()),
		makeRouter("pinecone", "replacement", memory.NewFakeVectorStore()),
	} {
		w := deleteRouterFixture(t, alternative, ns, id, receipt, rev)
		require.Equal(t, http.StatusPreconditionFailed, w.Code, w.Body.String())
		receiptState, readErr := db.ReadEvaluationFixture(t.Context(), domain.DocumentID(id), receipt)
		require.NoError(t, readErr)
		require.Equal(t, domain.FixtureOwned, receiptState.State)
	}
	w := deleteRouterFixture(t, original, ns, id, receipt, rev)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, domain.FixtureDeleted, w.Header().Get(evaluation.StateHeader))
	hits, err := originalVectors.QueryFiltered(t.Context(), []float32{1}, 1, domain.VectorFilter{DocumentIDs: []string{id}})
	require.NoError(t, err)
	require.Empty(t, hits)
}

// sweepRecorder rejects per-document namespaces like an index whose credential
// covers only the default namespace and records every external deletion.
type sweepRecorder struct {
	domain.VectorStore
	mu       sync.Mutex
	named    []string
	filtered []string
}

func (s *sweepRecorder) DeleteAllVectorsInNamespace(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.named = append(s.named, name)
	return status.Error(codes.PermissionDenied, "namespace deletion is not permitted")
}

func (s *sweepRecorder) DeleteByDocID(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.filtered = append(s.filtered, id)
	return nil
}

func TestFixtureCleanupUsesOnlyBoundDefaultNamespace(t *testing.T) {
	db, err := postgres.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))
	vectors := &sweepRecorder{VectorStore: memory.NewFakeVectorStore()}
	ns := evaluation.Namespace{Prefix: "staging", ID: uuid.New()}
	spec := evaluation.Documents()[0]
	id, receipt := ns.DocumentID(spec.Key), uuid.NewString()
	router := NewRouter(&RouterDeps{FixtureVectorIdentity: "pinecone:index", Config: &config.Config{VectorStore: "pinecone", EvaluationCollectionPrefix: ns.Prefix}, ChunkStore: db, VectorStore: vectors, PineconeDriver: vectors})
	rev := acceptRouterFixture(t, router, ns, spec, receipt)
	w := deleteRouterFixture(t, router, ns, id, receipt, rev)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, domain.FixtureDeleted, w.Header().Get(evaluation.StateHeader))
	require.Equal(t, []string{id}, vectors.filtered, "cleanup removes the fixture from the bound default namespace")
	require.Empty(t, vectors.named, "admission never bound a namespace named after the document")
}

func fixtureRowCounts(t *testing.T, db *postgres.Client) (receipts, documents, jobs int) {
	t.Helper()
	require.NoError(t, db.Pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM evaluation_fixtures), (SELECT count(*) FROM documents), (SELECT count(*) FROM ingest_jobs)`).Scan(&receipts, &documents, &jobs))
	return receipts, documents, jobs
}

func TestFixtureAdmissionRequiresDurableVectorStore(t *testing.T) {
	db, err := postgres.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))
	ns := evaluation.Namespace{Prefix: "staging", ID: uuid.New()}
	spec := evaluation.Documents()[0]
	id, receipt := ns.DocumentID(spec.Key), uuid.NewString()
	process := func(fake bool, identity string) http.Handler {
		store := memory.NewFakeVectorStore()
		return NewRouter(&RouterDeps{FixtureVectorIdentity: identity, Config: &config.Config{FakeMode: fake, VectorStore: "postgres", EvaluationCollectionPrefix: ns.Prefix}, ChunkStore: db, VectorStore: store, PineconeDriver: store})
	}
	t.Run("offline fake mode keeps no receipt", func(t *testing.T) {
		// Memory vectors vanish with the process, so a receipt bound to them could
		// never be proved clean after a restart.
		body, err := json.Marshal(map[string]string{"document_id": id, "content": spec.Text, "collection": ns.Collection(spec.Distractor)})
		require.NoError(t, err)
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/evaluation/fixtures", bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set(evaluation.FixtureHeader, ns.ID.String())
		r.Header.Set(evaluation.ReceiptHeader, receipt)
		w := httptest.NewRecorder()
		process(true, "memory:"+uuid.NewString()).ServeHTTP(w, r)
		require.Equal(t, http.StatusPreconditionFailed, w.Code, w.Body.String())
		receipts, documents, jobs := fixtureRowCounts(t, db)
		require.Zero(t, receipts+documents+jobs, "a refused admission leaves nothing to strand")
	})
	t.Run("durable identity survives a restart", func(t *testing.T) {
		rev := acceptRouterFixture(t, process(false, "postgres"), ns, spec, receipt)
		w := deleteRouterFixture(t, process(false, "postgres"), ns, id, receipt, rev)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Equal(t, domain.FixtureDeleted, w.Header().Get(evaluation.StateHeader))
	})
}

func acceptRouterFixture(t *testing.T, router http.Handler, ns evaluation.Namespace, spec evaluation.Document, receipt string) string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"document_id": ns.DocumentID(spec.Key), "content": spec.Text, "collection": ns.Collection(spec.Distractor)})
	require.NoError(t, err)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/evaluation/fixtures", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(evaluation.FixtureHeader, ns.ID.String())
	r.Header.Set(evaluation.ReceiptHeader, receipt)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	return w.Header().Get(evaluation.IncarnationHeader)
}

func deleteRouterFixture(t *testing.T, router http.Handler, ns evaluation.Namespace, id, receipt, revision string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/evaluation/fixtures/"+id, http.NoBody)
	r.Header.Set(evaluation.FixtureHeader, ns.ID.String())
	r.Header.Set(evaluation.ReceiptHeader, receipt)
	r.Header.Set(evaluation.IncarnationHeader, revision)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func fixtureVectorCount(t *testing.T, db *postgres.Client, id string) int {
	t.Helper()
	var n int
	require.NoError(t, db.Pool.QueryRow(t.Context(), "SELECT count(*) FROM chunk_vectors WHERE document_id=$1", id).Scan(&n))
	return n
}
