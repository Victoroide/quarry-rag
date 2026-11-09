package cleanup

import (
	"context"
	"errors"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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
