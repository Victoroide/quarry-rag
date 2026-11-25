package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDocumentLookupPreservesRetryJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(t.Context())) })
	require.NoError(t, pg.RunMigrations(t.Context()))
	_, err = pg.Pool.Exec(t.Context(), `INSERT INTO documents(id, content, status) VALUES
        ($1, 'retryable', 'failed'), ($2, 'terminal', 'failed')`, storedID, missingID)
	require.NoError(t, err)
	_, err = pg.Pool.Exec(t.Context(), `INSERT INTO ingest_jobs(document_id) VALUES ($1)`, storedID)
	require.NoError(t, err)
	states, err := lookupDocuments(t.Context(), pg, []string{storedID, missingID, "invalid", "00000000-0000-4000-8000-000000000099"})
	require.NoError(t, err)
	require.Equal(t, map[string]documentState{storedID: {Status: "failed", HasJob: true}, missingID: {Status: "failed"}}, states)
	require.Equal(t, []string{"terminal"}, selectOrphanVectors([]vectorRef{{"retry", storedID}, {"terminal", missingID}}, states, 100))
}

func TestCollectionBackfillSelectsLabeledDocuments(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(t.Context())) })
	require.NoError(t, pg.RunMigrations(t.Context()))
	_, err = pg.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,collection,status) VALUES($1,'synthetic','archive','completed'),($2,'synthetic','','completed'),('00000000-0000-4000-8000-000000000003','synthetic','private','processing')`, storedID, missingID)
	require.NoError(t, err)
	docs, err := listCollectionDocuments(t.Context(), pg, "", 10)
	require.NoError(t, err)
	require.Equal(t, []collectionDocument{{ID: storedID, Collection: "archive"}, {ID: "00000000-0000-4000-8000-000000000003", Collection: "private"}}, docs)
	docs, err = listCollectionDocuments(t.Context(), pg, "", 1)
	require.NoError(t, err)
	require.Equal(t, []collectionDocument{{ID: storedID, Collection: "archive"}}, docs)
	docs, err = listCollectionDocuments(t.Context(), pg, storedID, 10)
	require.NoError(t, err)
	require.Equal(t, []collectionDocument{{ID: "00000000-0000-4000-8000-000000000003", Collection: "private"}}, docs)
}

func TestMakeCollectionBackfillDryRun(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(t.Context())) })
	require.NoError(t, pg.RunMigrations(t.Context()))
	_, err = pg.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,collection,status) VALUES($1,'synthetic','archive','completed')`, storedID)
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), "make", "-s", "reconcile", "ARGS=-backfill-collections -limit=1")
	command.Dir = "../.."
	command.Env = append(os.Environ(), "GO_RAG_FAKE_MODE=true", "DATABASE_URL="+pg.Pool.Config().ConnString())
	var output, logs bytes.Buffer
	command.Stdout = &output
	command.Stderr = &logs
	require.NoError(t, command.Run(), logs.String())
	var result collectionReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, []collectionDocument{{ID: storedID, Collection: "archive"}}, result.Documents)
	require.Zero(t, result.Updated)
}

type blockingScopedVectors struct {
	started chan struct{}
	finish  chan struct{}
}

func (b *blockingScopedVectors) Verify(ctx context.Context, _ pgstore.BackfillDocument) (bool, error) {
	close(b.started)
	select {
	case <-b.finish:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func TestScopedBackfillApplyAcquiresWriterAndRefusesGenerationMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	ctx := t.Context()
	pg, err := pgstore.NewClient(ctx, pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(context.Background())) })
	require.NoError(t, pg.RunMigrations(ctx))
	_, err = pgstore.NewVectorStore(ctx, pg, 3)
	require.NoError(t, err)
	cfg := &config.Config{
		VectorStore:            "postgres",
		EmbeddingProvider:      domain.EmbeddingProviderOpenAI,
		OpenAIEmbeddingModel:   "text-embedding-3-small",
		EmbeddingModelRevision: "rev-1",
		ExtractionChunkVersion: "chunk-1",
		ChunkTokenSize:         512,
		ChunkOverlap:           64,
		OCRProvider:            "none",
		EmbeddingDimensions:    3,
	}
	require.NoError(t, pg.PinVectorGeneration(ctx, pgstore.LegacyGeneration, "postgres", "chunk_vectors", "", cfg.EmbeddingSettings()))

	tenant, owner, docID, chunkID := uuid.NewString(), uuid.NewString(), uuid.NewString(), strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = pg.Pool.Exec(ctx, `INSERT INTO documents(id,content,status,chunk_count) VALUES($1,'legacy text','completed',1)`, docID)
	require.NoError(t, err)
	_, err = pg.Pool.Exec(ctx, `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,'chunk',1)`, chunkID, docID)
	require.NoError(t, err)

	var rev int64
	var incarnation, sourceVersion string
	require.NoError(t, pg.Pool.QueryRow(ctx, `SELECT revision FROM grant_catalog_policy_revision`).Scan(&rev))
	require.NoError(t, pg.Pool.QueryRow(ctx, `SELECT incarnation::text, source_version FROM grant_catalog_source_snapshot($1)`, docID).Scan(&incarnation, &sourceVersion))
	batch, err := json.Marshal(map[string]any{
		"batch_id":          uuid.NewString(),
		"expected_revision": rev,
		"tenants":           []map[string]any{{"tenant_id": tenant}},
		"principals":        []map[string]any{{"tenant_id": tenant, "principal_id": owner, "issuer": "test", "subject": "owner", "enabled": true}},
		"collections":       []map[string]any{{"tenant_id": tenant, "collection": ""}},
		"assignments":       []map[string]any{{"tenant_id": tenant, "principal_id": owner, "collection": "", "document_id": docID, "source_version": sourceVersion}},
	})
	require.NoError(t, err)
	_, err = pg.Pool.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1,false)`, string(batch))
	require.NoError(t, err)

	blocker := &blockingScopedVectors{started: make(chan struct{}), finish: make(chan struct{})}
	done := make(chan error, 1)
	opts := options{Apply: true, BackfillScoped: true, Limit: 10}
	go func() {
		var out bytes.Buffer
		done <- runScopedLocal(ctx, cfg, pg, blocker, opts, &out)
	}()
	select {
	case <-blocker.started:
	case runErr := <-done:
		t.Fatalf("scoped backfill finished before verification: %v", runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("scoped backfill did not enter verification")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	require.Error(t, pg.FreezeWriters(waitCtx, "g2"), "freeze must wait for an in-flight applied scoped backfill")
	close(blocker.finish)
	require.NoError(t, <-done)

	require.NoError(t, pg.FreezeWriters(ctx, "g2"))
	var out bytes.Buffer
	err = runScopedLocal(ctx, cfg, pg, pgvectorVectors{pg}, opts, &out)
	require.ErrorIs(t, err, domain.ErrWritersFrozen)

	g2Store, err := pgstore.CreateGenerationVectorStore(ctx, pg, 3, "g2")
	require.NoError(t, err)
	_, err = pg.RegisterGeneration(ctx, pgstore.Generation{
		Name: "g2", Store: "postgres", Target: pgstore.VectorTable("g2"), Profile: cfg.EmbeddingSettings(), Origin: pgstore.OriginRecorded,
	})
	require.NoError(t, err)
	require.NoError(t, pg.RecordVerification(ctx, "g2", "digest", json.RawMessage(`{}`)))
	require.NoError(t, pg.ActivateGeneration(ctx, "g2", "digest", 0))
	require.NoError(t, pg.ThawWriters(ctx))

	err = runScopedLocal(ctx, cfg, pg, pgvectorVectors{pg}, opts, &out)
	require.ErrorIs(t, err, domain.ErrGenerationMismatch)

	require.NoError(t, g2Store.Upsert(ctx, "", []domain.Chunk{{ID: domain.ChunkID(chunkID), DocumentID: domain.DocumentID(docID), Index: 0, Embedding: []float32{1, 0, 0}}}))
	cfg.VectorGeneration = "g2"
	out.Reset()
	require.NoError(t, runScopedLocal(ctx, cfg, pg, pgvectorVectors{pg}, opts, &out))
	var report scopedReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.Equal(t, scopedReport{Examined: 1, Verified: 1, ResumeAfter: docID}, report)
}
