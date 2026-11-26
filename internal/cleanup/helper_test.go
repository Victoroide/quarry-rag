package cleanup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fileCleanupStore struct {
	domain.ChunkStore
	deletions int
}

type blockingObjectDelete struct {
	started, finish chan struct{}
}

func (d blockingObjectDelete) Delete(ctx context.Context, _ string) error {
	close(d.started)
	select {
	case <-d.finish:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestFreezeWaitsForAnAdmittedCleanup(t *testing.T) {
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(context.Background())) })
	require.NoError(t, pg.RunMigrations(t.Context()))
	id := domain.DocumentID(uuid.NewString())
	key := "document-source"
	require.NoError(t, pg.InsertDocument(t.Context(), domain.Document{ID: id, S3Key: &key}))
	objects := blockingObjectDelete{make(chan struct{}), make(chan struct{})}
	cleaner := &Cleaner{metadata: pg, objects: objects}
	result := make(chan error, 1)
	go func() { result <- cleaner.Delete(t.Context(), string(id)) }()
	select {
	case <-objects.started:
	case err := <-result:
		t.Fatalf("cleanup finished without deleting the object: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not reach object deletion")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	require.Error(t, pg.FreezeWriters(ctx, "g2"))
	close(objects.finish)
	require.NoError(t, <-result)
	require.NoError(t, pg.FreezeWriters(t.Context(), "g2"))
}

func (s *fileCleanupStore) DeleteDocument(context.Context, domain.DocumentID) error {
	s.deletions++
	return nil
}

type fileCleanupVectors struct{ deletions int }

func (v *fileCleanupVectors) DeleteAllVectorsInNamespace(context.Context, string) error {
	v.deletions++
	return nil
}

func (v *fileCleanupVectors) DeleteByDocID(context.Context, string) error {
	v.deletions++
	return nil
}

func TestDeleteRefusesWhenStoredFileCannotBeRemoved(t *testing.T) {
	key := "documents/document/id/file.pdf"
	store := &fileCleanupStore{}
	vectors := &fileCleanupVectors{}
	err := Remove(t.Context(), store, vectors, nil, &domain.Document{ID: "id", S3Key: &key})
	if !errors.Is(err, ErrFileStorageUnavailable) {
		t.Errorf("expected unavailable file storage, got %v", err)
	}
	if vectors.deletions != 0 || store.deletions != 0 {
		t.Fatalf("deleted vectors %d times and metadata %d times before checking storage", vectors.deletions, store.deletions)
	}
}

func TestPostgresCleanupWithoutPinecone(t *testing.T) {
	databaseURL := pgtest.StartVector(t)
	metadata, err := pgstore.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metadata.Close(context.Background())) })
	require.NoError(t, metadata.RunMigrations(t.Context()))
	vectors, err := pgstore.NewVectorStore(t.Context(), metadata, 3)
	require.NoError(t, err)
	cfg := &config.Config{VectorStore: "postgres", DatabaseURL: databaseURL, EmbeddingDimensions: 3}
	require.NoError(t, metadata.PinVectorGeneration(t.Context(), pgstore.LegacyGeneration, "postgres", "chunk_vectors", "", cfg.EmbeddingSettings()))
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, metadata.InsertDocument(t.Context(), domain.Document{ID: id}))
	require.NoError(t, vectors.Upsert(t.Context(), "", []domain.Chunk{{ID: "synthetic-cleanup", DocumentID: id, Embedding: []float32{1, 0, 0}}}))
	cleaner, err := New(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleaner.Close(context.Background())) })
	require.NoError(t, cleaner.Delete(t.Context(), string(id)))
	doc, err := metadata.GetDocument(t.Context(), id)
	require.NoError(t, err)
	require.Nil(t, doc)
	var remaining int
	require.NoError(t, metadata.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors WHERE document_id=$1`, string(id)).Scan(&remaining))
	require.Zero(t, remaining)
}

type recordingVectors struct {
	calls []string
	err   error
}

func (v *recordingVectors) DeleteAllVectorsInNamespace(_ context.Context, namespace string) error {
	v.calls = append(v.calls, "namespace:"+namespace)
	return v.err
}

func (v *recordingVectors) DeleteByDocID(_ context.Context, id string) error {
	v.calls = append(v.calls, "document:"+id)
	return v.err
}

type revisionRecordingVectors struct{ recordingVectors }

func (v *revisionRecordingVectors) DeleteByRevision(_ context.Context, id, revision string) error {
	v.calls = append(v.calls, "revision:"+id+"/"+revision)
	return v.err
}

type manifestRecordingVectors struct{ recordingVectors }

func (v *manifestRecordingVectors) DeleteByManifestRevision(_ context.Context, id, revision string, ids []string) error {
	v.calls = append(v.calls, "manifest:"+id+"/"+revision+"/"+strings.Join(ids, ","))
	return v.err
}

type transactionalRecordingVectors struct{ recordingVectors }

func (*transactionalRecordingVectors) TransactionalVectors() {}

type recordingObjects struct{ keys []string }

func (o *recordingObjects) Delete(_ context.Context, key string) error {
	o.keys = append(o.keys, key)
	return nil
}

type recordingDeleter struct {
	id  domain.DocumentID
	ops domain.DeleteOperations
	err error
}

func (*recordingDeleter) DeletionActive(context.Context) (bool, error) { return true, nil }

func (d *recordingDeleter) DeleteWithCleanup(_ context.Context, id domain.DocumentID, ops domain.DeleteOperations) error {
	d.id, d.ops = id, ops
	return d.err
}

func TestRemoveIncarnationForwardsTheObservedLifetimeAndTargets(t *testing.T) {
	vectors, objects := &revisionRecordingVectors{}, &recordingObjects{}
	deleter := &recordingDeleter{}
	targets := Targets{SourceBucket: "originals", VectorIndex: "index", InMemory: true}
	require.NoError(t, RemoveIncarnation(t.Context(), deleter, vectors, objects, targets, "document", "incarnation"))
	ops := deleter.ops
	require.Equal(t, domain.DocumentID("document"), deleter.id)
	require.Equal(t, "incarnation", ops.ExpectedIncarnation)
	require.Equal(t, "originals", ops.SourceBucket)
	require.Equal(t, "index", ops.VectorIndex)
	require.True(t, ops.InMemory)
	require.NoError(t, ops.LegacyNamespace(t.Context(), "document"))
	require.NoError(t, ops.LegacyVectors(t.Context(), "document"))
	require.NoError(t, ops.RevisionVectors(t.Context(), "document", "incarnation"))
	require.NoError(t, ops.Source(t.Context(), "source-key"))
	require.Equal(t, []string{"namespace:document", "document:document", "revision:document/incarnation"}, vectors.calls)
	require.Equal(t, []string{"source-key"}, objects.keys)
}

func TestRemoveIncarnationReturnsTheCoordinatorError(t *testing.T) {
	deleter := &recordingDeleter{err: domain.ErrDeletionBusy}
	require.ErrorIs(t, RemoveIncarnation(t.Context(), deleter, nil, nil, Targets{}, "document", ""), domain.ErrDeletionBusy)
}

func TestOperationsMatchTheConfiguredStores(t *testing.T) {
	for _, tc := range []struct {
		name             string
		vectors          domain.VectorDeleter
		objects          ObjectDeleter
		legacy, revision bool
		manifest, source bool
	}{
		{name: "no stores"},
		{name: "objects only", objects: &recordingObjects{}, source: true},
		{name: "legacy hosted vectors", vectors: &recordingVectors{}, legacy: true},
		{name: "revision hosted vectors", vectors: &revisionRecordingVectors{}, legacy: true, revision: true, source: true, objects: &recordingObjects{}},
		{name: "manifested hosted vectors", vectors: &manifestRecordingVectors{}, legacy: true, manifest: true},
		{name: "transactional vectors", vectors: &transactionalRecordingVectors{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := operations(tc.vectors, tc.objects, Targets{})
			require.Equal(t, tc.legacy, ops.LegacyNamespace != nil)
			require.Equal(t, tc.legacy, ops.LegacyVectors != nil)
			require.Equal(t, tc.revision, ops.RevisionVectors != nil)
			require.Equal(t, tc.manifest, ops.ManifestVectors != nil)
			require.Equal(t, tc.source, ops.Source != nil)
		})
	}
}

func TestOperationsTreatMissingVectorsAsDeleted(t *testing.T) {
	vectors := &revisionRecordingVectors{recordingVectors{err: status.Error(codes.NotFound, "namespace not found")}}
	ops := operations(vectors, nil, Targets{})
	require.NoError(t, ops.LegacyNamespace(t.Context(), "document"))
	require.NoError(t, ops.LegacyVectors(t.Context(), "document"))
	require.NoError(t, ops.RevisionVectors(t.Context(), "document", "incarnation"))
	vectors.err = status.Error(codes.Unavailable, "provider unavailable")
	require.Error(t, ops.LegacyVectors(t.Context(), "document"))
	require.Error(t, ops.RevisionVectors(t.Context(), "document", "incarnation"))
}

func TestManifestOperationDeletesExactIDsAndTreatsMissingVectorsAsDeleted(t *testing.T) {
	vectors := &manifestRecordingVectors{}
	ops := operations(vectors, nil, Targets{})
	require.NoError(t, ops.ManifestVectors(t.Context(), "document", "revision", []string{"document#revision#0", "document#revision#1"}))
	require.Equal(t, []string{"manifest:document/revision/document#revision#0,document#revision#1"}, vectors.calls)
	vectors.err = status.Error(codes.NotFound, "namespace not found")
	require.NoError(t, ops.ManifestVectors(t.Context(), "document", "revision", nil))
	vectors.err = status.Error(codes.Unavailable, "provider unavailable")
	require.Error(t, ops.ManifestVectors(t.Context(), "document", "revision", nil))
}

func TestPostgresCleanupOnAnActiveDatabaseUsesTheCoordinator(t *testing.T) {
	databaseURL := pgtest.StartVector(t)
	metadata, err := pgstore.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metadata.Close(context.Background())) })
	require.NoError(t, metadata.RunMigrations(t.Context()))
	vectors, err := pgstore.NewVectorStore(t.Context(), metadata, 3)
	require.NoError(t, err)
	cleaner, err := New(t.Context(), &config.Config{VectorStore: "postgres", DatabaseURL: databaseURL})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleaner.Close(context.Background())) })
	seed := func() (domain.DocumentID, string) {
		id := domain.DocumentID(uuid.NewString())
		require.NoError(t, metadata.InsertDocument(t.Context(), domain.Document{ID: id}))
		require.NoError(t, vectors.Upsert(t.Context(), "", []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), DocumentID: id, Embedding: []float32{1, 0, 0}}}))
		var incarnation string
		require.NoError(t, metadata.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, string(id)).Scan(&incarnation))
		return id, incarnation
	}
	id, incarnation := seed()
	require.ErrorIs(t, cleaner.DeleteIncarnation(t.Context(), string(id), incarnation), domain.ErrDeletionInactive, "a premature qualifier is refused")
	exists, err := metadata.DocumentExists(t.Context(), id)
	require.NoError(t, err)
	require.True(t, exists)

	_, err = metadata.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	require.ErrorIs(t, cleaner.Delete(t.Context(), string(id)), domain.ErrDeletionPreconditionRequired)
	require.ErrorIs(t, cleaner.DeleteIncarnation(t.Context(), string(id), uuid.NewString()), domain.ErrDeletionPreconditionFailed)
	require.NoError(t, cleaner.DeleteIncarnation(t.Context(), string(id), incarnation))
	var remaining int
	require.NoError(t, metadata.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors WHERE document_id=$1`, string(id)).Scan(&remaining))
	require.Zero(t, remaining, "transactional vectors go with the document")
	require.NoError(t, cleaner.DeleteIncarnation(t.Context(), string(id), incarnation), "the receipt acknowledges a retry")
	require.ErrorIs(t, cleaner.Delete(t.Context(), string(id)), ErrDocumentNotFound)
	var held bool
	require.NoError(t, metadata.Pool.QueryRow(t.Context(), `SELECT held FROM document_deletions WHERE document_id=$1`, string(id)).Scan(&held))
	require.False(t, held, "a transactional delete permits reuse")
}

func TestLegacyPostgresCleanupWithSingleConnection(t *testing.T) {
	databaseURL := pgtest.StartVector(t)
	metadata, err := pgstore.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metadata.Close(context.Background())) })
	require.NoError(t, metadata.RunMigrations(t.Context()))
	vectors, err := pgstore.NewVectorStore(t.Context(), metadata, 3)
	require.NoError(t, err)
	separator := "?"
	if strings.Contains(databaseURL, "?") {
		separator = "&"
	}
	// One connection must serve the guard, the receipt statements and the metadata removal.
	cleaner, err := New(t.Context(), &config.Config{VectorStore: "postgres", DatabaseURL: databaseURL + separator + "pool_max_conns=1"}, WithLegacyReservations())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleaner.Close(context.Background())) })
	seed := func() domain.DocumentID {
		id := domain.DocumentID(uuid.NewString())
		require.NoError(t, metadata.InsertDocument(t.Context(), domain.Document{ID: id}))
		require.NoError(t, vectors.Upsert(t.Context(), "", []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), DocumentID: id, Embedding: []float32{1, 0, 0}}}))
		return id
	}

	id := seed()
	require.NoError(t, cleaner.Delete(t.Context(), string(id)))
	require.ErrorIs(t, cleaner.Delete(t.Context(), string(id)), ErrDocumentNotFound, "a repeat reports absence")
	var remaining int
	require.NoError(t, metadata.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors WHERE document_id=$1`, string(id)).Scan(&remaining))
	require.Zero(t, remaining)
	var receipts int
	require.NoError(t, metadata.Pool.QueryRow(t.Context(), `SELECT count(*) FROM document_deletions`).Scan(&receipts))
	require.Zero(t, receipts, "transactional vectors need no reservation")

	live := seed()
	var incarnation string
	require.NoError(t, metadata.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, string(live)).Scan(&incarnation))
	require.ErrorIs(t, cleaner.DeleteIncarnation(t.Context(), string(live), incarnation), domain.ErrDeletionInactive)
	require.ErrorIs(t, cleaner.DeleteIncarnation(t.Context(), string(live), ""), domain.ErrDeletionPreconditionRequired, "an empty qualifier is never an ID-only request")
	exists, err := metadata.DocumentExists(t.Context(), live)
	require.NoError(t, err)
	require.True(t, exists)

	_, err = metadata.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	require.ErrorIs(t, cleaner.Delete(t.Context(), string(live)), domain.ErrDeletionPreconditionRequired, "the guard yields to the activated contract")
	require.NoError(t, cleaner.DeleteIncarnation(t.Context(), string(live), incarnation))
}

type scopedRecordingDeleter struct {
	incarnation  string
	complete     bool
	beginErr     error
	resumeErr    error
	beginCalls   int
	resumeCalls  int
	beginCtxErr  error
	resumeCtxErr error
	resumeTrace  any
	resumedInc   string
}

type scopedCtxKey struct{}

func (d *scopedRecordingDeleter) BeginScopedDeletion(ctx context.Context, _ domain.Principal, _ time.Time, _ domain.DocumentID, _ domain.DeleteOperations) (incarnation string, complete bool, err error) {
	d.beginCalls++
	d.beginCtxErr = ctx.Err()
	if d.beginCtxErr != nil && d.beginErr == nil {
		return "", false, d.beginCtxErr
	}
	return d.incarnation, d.complete, d.beginErr
}

func (d *scopedRecordingDeleter) ResumeScopedDeletion(ctx context.Context, _ domain.DocumentID, incarnation string, _ domain.DeleteOperations) error {
	d.resumeCalls++
	d.resumeCtxErr = ctx.Err()
	d.resumeTrace = ctx.Value(scopedCtxKey{})
	d.resumedInc = incarnation
	return d.resumeErr
}

func TestRemoveScopedDetachesOnlyAfterIntentCommits(t *testing.T) {
	principal := domain.Principal{ID: uuid.NewString(), TenantID: uuid.NewString()}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	deleter := &scopedRecordingDeleter{incarnation: "inc-1"}
	err := RemoveScoped(canceled, deleter, nil, nil, Targets{}, principal, time.Now().Add(time.Minute), "doc-1", "")
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, ErrScopedDeletionCanceled)
	require.Equal(t, 1, deleter.beginCalls)
	require.Zero(t, deleter.resumeCalls, "pre-commit cancellation must never start detached cleanup")

	// Pre-commit deadline expiration also returns ErrScopedDeletionCanceled.
	deleter = &scopedRecordingDeleter{incarnation: "inc-1b", beginErr: fmt.Errorf("begin tx: %w", context.DeadlineExceeded)}
	err = RemoveScoped(t.Context(), deleter, nil, nil, Targets{}, principal, time.Now().Add(time.Minute), "doc-1b", "")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, ErrScopedDeletionCanceled)
	require.Zero(t, deleter.resumeCalls)

	// Post-commit cleanup timeout does NOT match ErrScopedDeletionCanceled.
	deleter = &scopedRecordingDeleter{incarnation: "inc-1c", resumeErr: fmt.Errorf("delete vectors: %w", context.DeadlineExceeded)}
	err = RemoveScoped(t.Context(), deleter, nil, nil, Targets{}, principal, time.Now().Add(time.Minute), "doc-1c", "")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrScopedDeletionCanceled)
	require.Equal(t, 1, deleter.resumeCalls)

	// When the intent succeeds, detached cleanup ignores caller cancellation.
	liveCtx, liveCancel := context.WithCancel(context.WithValue(t.Context(), scopedCtxKey{}, "trace"))
	deleter = &scopedRecordingDeleter{incarnation: "inc-2"}
	err = RemoveScoped(liveCtx, &cancelAfterBegin{scopedRecordingDeleter: deleter, cancel: liveCancel}, nil, nil, Targets{}, principal, time.Now().Add(time.Minute), "doc-2", "")
	require.NoError(t, err)
	require.Equal(t, 1, deleter.resumeCalls)
	require.Equal(t, "inc-2", deleter.resumedInc)
	require.NoError(t, deleter.resumeCtxErr)
	require.Equal(t, "trace", deleter.resumeTrace)
}

type cancelAfterBegin struct {
	*scopedRecordingDeleter
	cancel context.CancelFunc
}

func (c *cancelAfterBegin) BeginScopedDeletion(ctx context.Context, p domain.Principal, until time.Time, id domain.DocumentID, ops domain.DeleteOperations) (incarnation string, complete bool, err error) {
	incarnation, complete, err = c.scopedRecordingDeleter.BeginScopedDeletion(ctx, p, until, id, ops)
	c.cancel()
	return incarnation, complete, err
}

func TestCleanerPausesDuringAVectorMigration(t *testing.T) {
	databaseURL := pgtest.StartVector(t)
	metadata, err := pgstore.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metadata.Close(context.Background())) })
	require.NoError(t, metadata.RunMigrations(t.Context()))
	_, err = pgstore.NewVectorStore(t.Context(), metadata, 3)
	require.NoError(t, err)
	cfg := &config.Config{VectorStore: "postgres", DatabaseURL: databaseURL, EmbeddingDimensions: 3}
	require.NoError(t, metadata.PinVectorGeneration(t.Context(), pgstore.LegacyGeneration, "postgres", "chunk_vectors", "", cfg.EmbeddingSettings()))
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, metadata.InsertDocument(t.Context(), domain.Document{ID: id}))

	cleaner, err := New(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleaner.Close(context.Background())) })
	require.NoError(t, metadata.FreezeWriters(t.Context(), "g2"))
	require.ErrorIs(t, cleaner.Delete(t.Context(), string(id)), domain.ErrWritersFrozen)
	doc, err := metadata.GetDocument(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, doc, "a paused deletion leaves the document")

	_, err = New(t.Context(), &config.Config{VectorStore: "postgres", DatabaseURL: databaseURL, VectorGeneration: "g2"})
	require.ErrorIs(t, err, domain.ErrGenerationMismatch, "a tool configured for another generation cannot open for deletion")
	require.NoError(t, metadata.ThawWriters(t.Context()))

	require.NoError(t, cleaner.Delete(t.Context(), string(id)))
}

func TestCheckpointedCleanerPausesDuringAVectorMigration(t *testing.T) {
	databaseURL := pgtest.StartVector(t)
	metadata, err := pgstore.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metadata.Close(context.Background())) })
	require.NoError(t, metadata.RunMigrations(t.Context()))
	_, err = pgstore.NewVectorStore(t.Context(), metadata, 3)
	require.NoError(t, err)
	cfg := &config.Config{VectorStore: "postgres", DatabaseURL: databaseURL, EmbeddingDimensions: 3}
	require.NoError(t, metadata.PinVectorGeneration(t.Context(), pgstore.LegacyGeneration, "postgres", "chunk_vectors", "", cfg.EmbeddingSettings()))
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, metadata.InsertDocument(t.Context(), domain.Document{ID: id}))
	var incarnation string
	require.NoError(t, metadata.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, string(id)).Scan(&incarnation))
	_, err = metadata.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)

	cleaner, err := New(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleaner.Close(context.Background())) })
	require.NoError(t, metadata.FreezeWriters(t.Context(), "g2"))
	require.ErrorIs(t, cleaner.DeleteIncarnation(t.Context(), string(id), incarnation), domain.ErrWritersFrozen)
	doc, err := metadata.GetDocument(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, doc, "a paused checkpointed deletion leaves the document")
	var begun int
	require.NoError(t, metadata.Pool.QueryRow(t.Context(), `SELECT count(*) FROM document_deletions WHERE document_id=$1`, string(id)).Scan(&begun))
	require.Zero(t, begun, "no checkpoint is recorded while writers are paused")

	require.NoError(t, metadata.ThawWriters(t.Context()))
	require.NoError(t, cleaner.DeleteIncarnation(t.Context(), string(id), incarnation))
}

func TestLegacyReservationCleanerAcquiresWriterBeforeGuard(t *testing.T) {
	databaseURL := pgtest.StartVector(t)
	metadata, err := pgstore.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metadata.Close(context.Background())) })
	require.NoError(t, metadata.RunMigrations(t.Context()))
	_, err = pgstore.NewVectorStore(t.Context(), metadata, 3)
	require.NoError(t, err)
	cfg := &config.Config{VectorStore: "postgres", DatabaseURL: databaseURL, EmbeddingDimensions: 3}
	require.NoError(t, metadata.PinVectorGeneration(t.Context(), pgstore.LegacyGeneration, "postgres", "chunk_vectors", "", cfg.EmbeddingSettings()))
	id := domain.DocumentID(uuid.NewString())
	key := "document-source"
	require.NoError(t, metadata.InsertDocument(t.Context(), domain.Document{ID: id, S3Key: &key}))

	cleaner, err := New(t.Context(), cfg, WithLegacyReservations())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleaner.Close(context.Background())) })
	require.NoError(t, metadata.FreezeWriters(t.Context(), "g2"))
	require.ErrorIs(t, cleaner.Delete(t.Context(), string(id)), domain.ErrWritersFrozen)
	doc, err := metadata.GetDocument(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, doc, "a paused deletion returns before the legacy guard")

	require.NoError(t, metadata.ThawWriters(t.Context()))
	objects := blockingObjectDelete{make(chan struct{}), make(chan struct{})}
	cleaner.objects = objects
	result := make(chan error, 1)
	go func() { result <- cleaner.Delete(t.Context(), string(id)) }()
	select {
	case <-objects.started:
	case delErr := <-result:
		t.Fatalf("cleanup finished without deleting the object: %v", delErr)
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not reach object deletion")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	require.Error(t, metadata.FreezeWriters(ctx, "g2"))
	close(objects.finish)
	require.NoError(t, <-result)
	require.NoError(t, metadata.FreezeWriters(t.Context(), "g2"))
}

type manifestedCleanupStore struct {
	domain.ChunkStore
	manifests []domain.VectorManifest
	deleted   int
}

func (s *manifestedCleanupStore) DeleteDocument(context.Context, domain.DocumentID) error {
	s.deleted++
	return nil
}
func (*manifestedCleanupStore) PersistVectorManifest(context.Context, domain.Job, domain.VectorManifest) error {
	return nil
}
func (*manifestedCleanupStore) GetVectorManifest(context.Context, string) (*domain.VectorManifest, error) {
	return nil, nil
}
func (s *manifestedCleanupStore) ListDocumentVectorManifests(context.Context, domain.DocumentID, string) ([]domain.VectorManifest, error) {
	return s.manifests, nil
}
func (*manifestedCleanupStore) ClaimVectorCleanup(context.Context, string, time.Duration, time.Duration) (*domain.VectorCleanupTask, error) {
	return nil, nil
}
func (*manifestedCleanupStore) RenewVectorCleanup(context.Context, domain.VectorCleanupTask, time.Duration) (bool, error) {
	return true, nil
}
func (*manifestedCleanupStore) AdvanceVectorCleanup(context.Context, domain.VectorCleanupTask, int) (bool, error) {
	return true, nil
}
func (*manifestedCleanupStore) DeferVectorCleanup(context.Context, domain.VectorCleanupTask, string, time.Duration, int, string) (bool, error) {
	return true, nil
}
func (*manifestedCleanupStore) CompleteVectorCleanup(context.Context, domain.VectorCleanupTask) (bool, error) {
	return true, nil
}
func (*manifestedCleanupStore) ReleaseVectorCleanup(context.Context, domain.VectorCleanupTask) error {
	return nil
}

type revisionCleanupVectors struct {
	fileCleanupVectors
	revDeletes []string
}

func (v *revisionCleanupVectors) DeleteByManifestRevision(_ context.Context, _, revisionID string, _ []string) error {
	v.revDeletes = append(v.revDeletes, revisionID)
	return nil
}

func TestRemoveTargetsManifestedRevisionWithoutDocWideDelete(t *testing.T) {
	store := &manifestedCleanupStore{
		manifests: []domain.VectorManifest{
			{RevisionID: "rev-retired", DocumentID: "doc-1", State: "retired"},
			{RevisionID: "rev-active", DocumentID: "doc-1", State: "active", VectorIDs: []string{"doc-1#rev-active#0"}},
		},
	}
	vecs := &revisionCleanupVectors{}
	require.NoError(t, Remove(t.Context(), store, vecs, nil, &domain.Document{ID: "doc-1"}))
	require.Equal(t, 1, store.deleted)
	require.Equal(t, 1, vecs.deletions, "only legacy namespace delete may run; DeleteByDocID must not run")
	require.Equal(t, []string{"rev-active", "rev-active"}, vecs.revDeletes)
}

func TestRemoveOfAManifestedDocumentHonorsTheDrainWindowAtClaim(t *testing.T) {
	metadata, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metadata.Close(context.Background())) })
	require.NoError(t, metadata.RunMigrations(t.Context()))
	metadata.EnableHostedVectorManifests(true)

	id, revision := uuid.NewString(), uuid.NewString()
	vectorID := domain.FormatRevisionVectorID(domain.DocumentID(id), revision, 0)
	_, err = metadata.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status) VALUES ($1,'manifested','completed')`, id)
	require.NoError(t, err)
	_, err = metadata.Pool.Exec(t.Context(), `INSERT INTO hosted_vector_manifests(revision_id,document_id,incarnation,attempt,id_prefix,vector_ids,chunk_ids,state)
		SELECT $1,id,COALESCE(incarnation,id),1,$3,ARRAY[$4],ARRAY[]::uuid[],'active' FROM documents WHERE id=$2`,
		revision, id, domain.RevisionVectorPrefix(domain.DocumentID(id), revision), vectorID)
	require.NoError(t, err)
	_, err = metadata.Pool.Exec(t.Context(), `INSERT INTO hosted_vector_revisions(document_id,incarnation,active_revision_id)
		SELECT id,COALESCE(incarnation,id),$2 FROM documents WHERE id=$1`, id, revision)
	require.NoError(t, err)

	vectors := &revisionCleanupVectors{}
	doc, err := metadata.GetDocument(t.Context(), domain.DocumentID(id))
	require.NoError(t, err)
	require.NoError(t, Remove(t.Context(), metadata, vectors, nil, doc))
	require.Equal(t, []string{revision, revision}, vectors.revDeletes, "exact deletes run before and after the metadata removal")
	require.Equal(t, 1, vectors.deletions, "only the namespace delete runs; the document-wide delete is skipped")

	held, err := metadata.ClaimVectorCleanup(t.Context(), "sweeper", time.Minute, time.Hour)
	require.NoError(t, err)
	require.Nil(t, held, "a writer that was mid-batch at removal may still land inside the drain window")
	_, err = metadata.Pool.Exec(t.Context(), `UPDATE hosted_vector_cleanups SET retired_at=retired_at-interval '2 hours' WHERE revision_id=$1`, revision)
	require.NoError(t, err)
	claimed, err := metadata.ClaimVectorCleanup(t.Context(), "sweeper", time.Minute, time.Hour)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, revision, claimed.RevisionID)
}

func TestHeldReleaseHonorsTheWriterGate(t *testing.T) {
	firstDatabaseURL := pgtest.StartVector(t)
	for i, terminatedOffline := range []bool{false, true} {
		name := "release_held"
		if terminatedOffline {
			name = "release_terminated_offline_held"
		}
		t.Run(name, func(t *testing.T) {
			databaseURL := firstDatabaseURL
			if i > 0 {
				databaseURL = pgtest.Sibling(t, firstDatabaseURL)
			}
			metadata, err := pgstore.NewClient(t.Context(), databaseURL)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, metadata.Close(context.Background())) })
			require.NoError(t, metadata.RunMigrations(t.Context()))
			_, err = pgstore.NewVectorStore(t.Context(), metadata, 3)
			require.NoError(t, err)
			cfg := &config.Config{VectorStore: "postgres", DatabaseURL: databaseURL, EmbeddingDimensions: 3}
			require.NoError(t, metadata.PinVectorGeneration(t.Context(), pgstore.LegacyGeneration, "postgres", "chunk_vectors", "", cfg.EmbeddingSettings()))

			id := domain.DocumentID(uuid.NewString())
			inc := uuid.NewString()
			owner := uuid.NewString()
			mode := "legacy"
			var ownerArg any
			if terminatedOffline {
				mode = "memory"
				ownerArg = owner
			}
			_, err = metadata.Pool.Exec(t.Context(), `INSERT INTO document_deletions
				(document_id, incarnation, kind, source_key, source_bucket, vector_mode, memory_owner, step, held, finalized)
				VALUES ($1, $2, 'legacy_reservation', 'document-source', 'originals', $3, $4, 0, TRUE, FALSE)`,
				string(id), inc, mode, ownerArg)
			require.NoError(t, err)

			var opts []Option
			if terminatedOffline {
				opts = append(opts, WithDirectRecovery(true))
			}
			cleaner, err := New(t.Context(), cfg, opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cleaner.Close(context.Background())) })
			cleaner.targets.SourceBucket = "originals"

			runRelease := func() error {
				if terminatedOffline {
					return cleaner.ReleaseTerminatedOfflineHeld(t.Context(), string(id), inc, owner)
				}
				return cleaner.ReleaseHeld(t.Context(), string(id), inc)
			}

			require.NoError(t, metadata.FreezeWriters(t.Context(), "g2"))
			require.ErrorIs(t, runRelease(), domain.ErrWritersFrozen)
			held, err := metadata.ListHeldDeletions(t.Context())
			require.NoError(t, err)
			require.Len(t, held, 1, "a paused release leaves the receipt held")

			require.NoError(t, metadata.ThawWriters(t.Context()))
			objects := blockingObjectDelete{make(chan struct{}), make(chan struct{})}
			cleaner.objects = objects
			result := make(chan error, 1)
			go func() { result <- runRelease() }()
			select {
			case <-objects.started:
			case relErr := <-result:
				t.Fatalf("release finished without deleting the object: %v", relErr)
			case <-time.After(5 * time.Second):
				t.Fatal("release did not reach object deletion")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			defer cancel()
			require.Error(t, metadata.FreezeWriters(ctx, "g2"))
			close(objects.finish)
			require.NoError(t, <-result)
			require.NoError(t, metadata.FreezeWriters(t.Context(), "g2"))
		})
	}
}
