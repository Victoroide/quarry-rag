package cleanup

import (
	"context"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pg "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestFirstCleanupRequiresExistingPhysicalTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("requires the PostgreSQL integration harness")
	}
	ctx := t.Context()
	dsn := pgtest.Start(t)
	owner, err := pg.NewClient(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close(context.Background())) })
	require.NoError(t, owner.RunMigrations(ctx))
	var missing bool
	require.NoError(t, owner.Pool.QueryRow(ctx, "SELECT to_regclass('chunk_vectors') IS NULL").Scan(&missing))
	require.True(t, missing)
	cfg := &config.Config{VectorStore: "postgres", EmbeddingDimensions: 256, DatabaseURL: dsn}
	c, openErr := New(ctx, cfg)
	if c != nil {
		require.NoError(t, c.Close(ctx))
	}
	state, err := owner.VectorState(ctx)
	require.NoError(t, err)
	retryErr := owner.PinVectorGeneration(ctx, "legacy", "pinecone", "corpus", "corpus-physical-host", cfg.EmbeddingSettings())
	t.Logf("cleanup=%v recorded=%q/%q retry=%v", openErr, state.LegacyStore, state.LegacyTarget, retryErr)
	require.ErrorIs(t, openErr, pg.ErrGenerationDrift)
	require.Empty(t, state.LegacyStore)
	require.Empty(t, state.LegacyTarget)
	require.Empty(t, state.LegacyHost)
	require.NoError(t, retryErr)
}

func TestFirstPostgresCleanupBindsExistingChunkVectorsTable(t *testing.T) {
	if testing.Short() {
		t.Skip("requires the PostgreSQL integration harness")
	}
	ctx := t.Context()
	dsn := pgtest.StartVector(t)
	owner, err := pg.NewClient(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close(context.Background())) })
	require.NoError(t, owner.RunMigrations(ctx))

	vectors, err := pg.NewVectorStore(ctx, owner, 2)
	require.NoError(t, err)
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, owner.InsertDocument(ctx, domain.Document{ID: id, Content: "offline source"}))
	require.NoError(t, vectors.Upsert(ctx, "", []domain.Chunk{{ID: "stored", DocumentID: id, Embedding: []float32{1, 0}}}))

	c, err := New(ctx, &config.Config{VectorStore: "postgres", DatabaseURL: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })

	state, err := owner.VectorState(ctx)
	require.NoError(t, err)
	require.Equal(t, "postgres", state.LegacyStore)
	require.Equal(t, "chunk_vectors", state.LegacyTarget)
	require.Empty(t, state.LegacyHost)

	require.NoError(t, c.Delete(ctx, string(id)))
	exists, err := owner.DocumentExists(ctx, id)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestFirstPostgresCleanupRefusesNonVectorTableAndForeignBinding(t *testing.T) {
	if testing.Short() {
		t.Skip("requires the PostgreSQL integration harness")
	}
	ctx := t.Context()
	dsn := pgtest.StartVector(t)
	owner, err := pg.NewClient(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close(context.Background())) })
	require.NoError(t, owner.RunMigrations(ctx))

	// A table named chunk_vectors without a typed vector embedding column must not bind.
	_, err = owner.Pool.Exec(ctx, `CREATE TABLE chunk_vectors (chunk_id TEXT PRIMARY KEY, embedding TEXT NOT NULL)`)
	require.NoError(t, err)
	cfg := &config.Config{VectorStore: "postgres", DatabaseURL: dsn}
	c, openErr := New(ctx, cfg)
	if c != nil {
		require.NoError(t, c.Close(ctx))
	}
	require.ErrorIs(t, openErr, pg.ErrGenerationDrift)
	state, err := owner.VectorState(ctx)
	require.NoError(t, err)
	require.Empty(t, state.LegacyStore)

	// Even with a valid chunk_vectors table, an existing Pinecone binding is preserved and rejected.
	_, err = owner.Pool.Exec(ctx, `DROP TABLE chunk_vectors`)
	require.NoError(t, err)
	_, err = pg.NewVectorStore(ctx, owner, 2)
	require.NoError(t, err)
	require.NoError(t, owner.PinVectorGeneration(ctx, pg.LegacyGeneration, "pinecone", "corpus", "corpus-physical-host", cfg.EmbeddingSettings()))
	c, openErr = New(ctx, cfg)
	if c != nil {
		require.NoError(t, c.Close(ctx))
	}
	require.ErrorIs(t, openErr, pg.ErrGenerationDrift)
	state, err = owner.VectorState(ctx)
	require.NoError(t, err)
	require.Equal(t, "pinecone", state.LegacyStore)
	require.Equal(t, "corpus", state.LegacyTarget)
}
