package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"sync"
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
	require.Equal(t, []string{"terminal"}, selectOrphanVectors([]vectorRef{{ID: "retry", DocID: storedID}, {ID: "terminal", DocID: missingID}}, states, 100))
}

func TestCollectionBackfillSelectsLabeledDocuments(t *testing.T) {
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

func TestDocumentKeyListingFollowsUUIDOrder(t *testing.T) {
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(t.Context())) })
	require.NoError(t, pg.RunMigrations(t.Context()))

	id1 := "00000000-0000-4000-8000-000000000001"
	id2 := "00000000-0000-4000-8000-000000000002"
	id3 := "00000000-0000-4000-8000-000000000003"
	id4 := "00000000-0000-4000-8000-000000000004"
	_, err = pg.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status) VALUES
		($3,'three','failed'),($1,'one','processing'),($4,'four','deleting'),($2,'two','completed')`,
		id1, id2, id3, id4)
	require.NoError(t, err)

	ids, err := listDocumentIDs(t.Context(), pg, "", 10)
	require.NoError(t, err)
	require.Equal(t, []string{id1, id2, id3, id4}, ids)

	ids, err = listDocumentIDs(t.Context(), pg, "", 2)
	require.NoError(t, err)
	require.Equal(t, []string{id1, id2}, ids)

	ids, err = listDocumentIDs(t.Context(), pg, id2, 10)
	require.NoError(t, err)
	require.Equal(t, []string{id3, id4}, ids)
}

func TestMakeCollectionBackfillDryRun(t *testing.T) {
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

func TestLookupDocumentsIncludesManifestedRevisions(t *testing.T) {
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(t.Context())) })
	require.NoError(t, pg.RunMigrations(t.Context()))

	ctx := t.Context()
	activeRev, pendingRev, trackedRev, untrackedRev := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	_, err = pg.Pool.Exec(ctx, `INSERT INTO documents(id, content, status) VALUES ($1, 'manifested', 'completed')`, storedID)
	require.NoError(t, err)
	for _, m := range []struct {
		rev, doc, state string
	}{
		{activeRev, storedID, "active"},
		{pendingRev, storedID, "writing"},
		{trackedRev, storedID, "retired"},
	} {
		_, err = pg.Pool.Exec(ctx, `INSERT INTO hosted_vector_manifests(revision_id, document_id, incarnation, attempt, id_prefix, vector_ids, chunk_ids, state)
			VALUES($1, $2, $2, 1, $3, ARRAY[$4], ARRAY[]::uuid[], $5)`,
			m.rev, m.doc, domain.RevisionVectorPrefix(domain.DocumentID(m.doc), m.rev), domain.FormatRevisionVectorID(domain.DocumentID(m.doc), m.rev, 0), m.state)
		require.NoError(t, err)
	}
	_, err = pg.Pool.Exec(ctx, `INSERT INTO hosted_vector_revisions(document_id, incarnation, active_revision_id) VALUES($1, $1, $2)`, storedID, activeRev)
	require.NoError(t, err)
	_, err = pg.Pool.Exec(ctx, `INSERT INTO hosted_vector_cleanups(revision_id, document_id, incarnation, id_prefix, vector_ids) VALUES($1, $2, $2, $3, ARRAY[$4])`,
		trackedRev, storedID, domain.RevisionVectorPrefix(domain.DocumentID(storedID), trackedRev), domain.FormatRevisionVectorID(domain.DocumentID(storedID), trackedRev, 0))
	require.NoError(t, err)

	states, err := lookupDocuments(ctx, pg, []string{storedID})
	require.NoError(t, err)
	refs := []vectorRef{
		{ID: storedID + "#" + activeRev + "#0", DocID: storedID, RevisionID: activeRev},
		{ID: storedID + "#" + pendingRev + "#0", DocID: storedID, RevisionID: pendingRev},
		{ID: storedID + "#" + trackedRev + "#0", DocID: storedID, RevisionID: trackedRev},
		{ID: storedID + "#" + untrackedRev + "#0", DocID: storedID, RevisionID: untrackedRev},
	}
	require.Equal(t, []string{storedID + "#" + untrackedRev + "#0"}, selectOrphanVectors(refs, states, 10))
}

type blockingSweepVectors struct {
	started chan struct{}
	finish  chan struct{}
	deleted []string
	once    sync.Once
}

func (b *blockingSweepVectors) DeleteVectorIDs(ctx context.Context, _ string, ids []string) error {
	if b.started != nil {
		b.once.Do(func() { close(b.started) })
		select {
		case <-b.finish:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	b.deleted = append(b.deleted, ids...)
	return nil
}

func TestBlockingSweepVectorsSignalsOnce(t *testing.T) {
	b := &blockingSweepVectors{started: make(chan struct{}), finish: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- b.DeleteVectorIDs(t.Context(), "", []string{"first"})
	}()
	<-b.started
	close(b.finish)
	require.NoError(t, <-done)
	require.NoError(t, b.DeleteVectorIDs(t.Context(), "", []string{"second"}))
	require.Equal(t, []string{"first", "second"}, b.deleted)
}

func (*blockingSweepVectors) FetchPresentVectorIDs(context.Context, string, []string) ([]string, error) {
	return nil, nil
}

func (*blockingSweepVectors) ListPrefixVectorIDs(context.Context, string, string, int, string) (ids []string, nextToken string, supported bool, err error) {
	return nil, "", true, nil
}

func TestRevisionSweepApplyPinsGenerationAndHoldsWriterBarrier(t *testing.T) {
	ctx := t.Context()
	pg, err := pgstore.NewClient(ctx, pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(context.Background())) })
	require.NoError(t, pg.RunMigrations(ctx))

	cfg := &config.Config{
		VectorStore:            "pinecone",
		PineconeIndexName:      "quarry-g1",
		EmbeddingProvider:      domain.EmbeddingProviderOpenAI,
		OpenAIEmbeddingModel:   "text-embedding-3-small",
		EmbeddingModelRevision: "rev-1",
		ExtractionChunkVersion: "chunk-1",
		ChunkTokenSize:         512,
		ChunkOverlap:           64,
		OCRProvider:            "none",
		EmbeddingDimensions:    3,
	}
	require.NoError(t, pg.PinVectorGeneration(ctx, pgstore.LegacyGeneration, "pinecone", "quarry-g1", "quarry-g1-host", cfg.EmbeddingSettings()))

	revID := uuid.NewString()
	vecID := domain.FormatRevisionVectorID(domain.DocumentID(storedID), revID, 0)
	_, err = pg.Pool.Exec(ctx, `INSERT INTO hosted_vector_manifests(revision_id, document_id, incarnation, attempt, id_prefix, vector_ids, chunk_ids, state, retired_at) VALUES($1,$2,$2,1,$3,ARRAY[$4],ARRAY[]::uuid[],'retired',NOW()-interval '2 hours')`, revID, storedID, domain.RevisionVectorPrefix(domain.DocumentID(storedID), revID), vecID)
	require.NoError(t, err)
	_, err = pg.Pool.Exec(ctx, `INSERT INTO hosted_vector_cleanups(revision_id, document_id, incarnation, id_prefix, vector_ids, retired_at)
		VALUES($1, $2, $2, $3, ARRAY[$4], NOW() - interval '2 hours')`,
		revID, storedID, domain.RevisionVectorPrefix(domain.DocumentID(storedID), revID), vecID)
	require.NoError(t, err)

	blocker := &blockingSweepVectors{started: make(chan struct{}), finish: make(chan struct{})}
	idx := &reconcileFakeIndex{}
	opts := options{Apply: true, Limit: 10, Page: 5}
	done := make(chan error, 1)
	go func() {
		done <- pinAndRunMaintenance(ctx, ctx, cfg, pg, "quarry-g1-host", opts, func() error {
			_, runErr := reconcileRevisions(ctx, pg, blocker, idx, idx.deleteNamespace, noDocuments, opts, sweepSettings{DrainDelay: time.Minute}, &bytes.Buffer{})
			return runErr
		})
	}()

	select {
	case <-blocker.started:
	case runErr := <-done:
		t.Fatalf("sweep returned before entering delete barrier: %v", runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("sweep did not enter delete barrier")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	require.Error(t, pg.FreezeWriters(waitCtx, "g2"), "freeze must wait for an in-flight applied revision sweep")
	close(blocker.finish)
	require.NoError(t, <-done)

	var remaining int
	require.NoError(t, pg.Pool.QueryRow(ctx, `SELECT count(*) FROM hosted_vector_cleanups WHERE revision_id=$1`, revID).Scan(&remaining))
	require.Zero(t, remaining)
	require.Equal(t, []string{vecID}, blocker.deleted)

	rev2 := uuid.NewString()
	vec2 := domain.FormatRevisionVectorID(domain.DocumentID(storedID), rev2, 0)
	_, err = pg.Pool.Exec(ctx, `INSERT INTO hosted_vector_manifests(revision_id, document_id, incarnation, attempt, id_prefix, vector_ids, chunk_ids, state, retired_at) VALUES($1,$2,$2,1,$3,ARRAY[$4],ARRAY[]::uuid[],'retired',NOW()-interval '2 hours')`, rev2, storedID, domain.RevisionVectorPrefix(domain.DocumentID(storedID), rev2), vec2)
	require.NoError(t, err)
	_, err = pg.Pool.Exec(ctx, `INSERT INTO hosted_vector_cleanups(revision_id, document_id, incarnation, id_prefix, vector_ids, retired_at)
		VALUES($1, $2, $2, $3, ARRAY[$4], NOW() - interval '2 hours')`,
		rev2, storedID, domain.RevisionVectorPrefix(domain.DocumentID(storedID), rev2), vec2)
	require.NoError(t, err)

	require.NoError(t, pg.FreezeWriters(ctx, "g2"))
	sweepG2 := func() (report, error) {
		var rep report
		var sweepErr error
		sweepErr = pinAndRunMaintenance(ctx, ctx, cfg, pg, cfg.PineconeIndexName+"-host", opts, func() error {
			rep, sweepErr = reconcileRevisions(ctx, pg, &blockingSweepVectors{}, idx, idx.deleteNamespace, noDocuments, opts, sweepSettings{DrainDelay: time.Minute}, &bytes.Buffer{})
			return sweepErr
		})
		return rep, sweepErr
	}
	_, err = sweepG2()
	require.ErrorIs(t, err, domain.ErrWritersFrozen)

	_, err = pg.RegisterGeneration(ctx, pgstore.Generation{Name: "g2", Store: "pinecone", Target: "quarry-g2", Host: "quarry-g2-host", Profile: cfg.EmbeddingSettings(), Origin: pgstore.OriginRecorded})
	require.NoError(t, err)
	require.NoError(t, pg.RecordVerification(ctx, "g2", "digest", json.RawMessage(`{}`)))
	require.NoError(t, pg.ActivateGeneration(ctx, "g2", "digest", 0))
	require.NoError(t, pg.ThawWriters(ctx))

	_, err = sweepG2()
	require.ErrorIs(t, err, domain.ErrGenerationMismatch)

	cfg.VectorGeneration = "g2"
	cfg.PineconeIndexName = "quarry-g2"
	rep, err := sweepG2()
	require.NoError(t, err)
	require.Equal(t, 1, rep.SweptRevisions)
	require.Equal(t, 1, rep.SweptVectors)
}
