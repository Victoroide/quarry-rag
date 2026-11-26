package cleanup

import (
	"context"
	"errors"
	"strings"
	"testing"

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
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	databaseURL := pgtest.StartVector(t)
	metadata, err := pgstore.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metadata.Close(context.Background())) })
	require.NoError(t, metadata.RunMigrations(t.Context()))
	vectors, err := pgstore.NewVectorStore(t.Context(), metadata, 3)
	require.NoError(t, err)
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, metadata.InsertDocument(t.Context(), domain.Document{ID: id}))
	require.NoError(t, vectors.Upsert(t.Context(), "", []domain.Chunk{{ID: "synthetic-cleanup", DocumentID: id, Embedding: []float32{1, 0, 0}}}))
	cleaner, err := New(t.Context(), &config.Config{VectorStore: "postgres", DatabaseURL: databaseURL})
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
		source           bool
	}{
		{name: "no stores"},
		{name: "objects only", objects: &recordingObjects{}, source: true},
		{name: "legacy hosted vectors", vectors: &recordingVectors{}, legacy: true},
		{name: "revision hosted vectors", vectors: &revisionRecordingVectors{}, legacy: true, revision: true, source: true, objects: &recordingObjects{}},
		{name: "transactional vectors", vectors: &transactionalRecordingVectors{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := operations(tc.vectors, tc.objects, Targets{})
			require.Equal(t, tc.legacy, ops.LegacyNamespace != nil)
			require.Equal(t, tc.legacy, ops.LegacyVectors != nil)
			require.Equal(t, tc.revision, ops.RevisionVectors != nil)
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

func TestPostgresCleanupOnAnActiveDatabaseUsesTheCoordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
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
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
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
