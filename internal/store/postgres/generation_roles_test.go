package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	reindexRolePlaceholder       = "quarry_reindex"
	reindexBaseGrantMarker       = "GRANT USAGE ON SCHEMA public TO quarry_reindex"
	reindexLegacyGrantMarker     = "ON public.chunk_vectors TO quarry_reindex"
	reindexPrecreatedGrantMarker = "ON public.chunk_vectors_<generation> TO quarry_reindex"
	reindexSelfCreateGrantMarker = "GRANT CREATE ON SCHEMA public TO quarry_reindex"
)

type reindexGrantBlocks struct {
	base       string
	legacy     string
	precreated string
	selfCreate string
}

func readPublishedReindexGrantBlocks(t *testing.T) reindexGrantBlocks {
	t.Helper()
	return reindexGrantBlocks{
		base:       publishedSQL(t, "deployment.md", reindexBaseGrantMarker),
		legacy:     publishedSQL(t, "deployment.md", reindexLegacyGrantMarker),
		precreated: publishedSQL(t, "deployment.md", reindexPrecreatedGrantMarker),
		selfCreate: publishedSQL(t, "deployment.md", reindexSelfCreateGrantMarker),
	}
}

func replaceReindexRole(sqlBlock, role, generation string) string {
	return strings.NewReplacer(
		reindexRolePlaceholder, pgx.Identifier{role}.Sanitize(),
		"<generation>", generation,
	).Replace(sqlBlock)
}

func provisionReindexRole(t *testing.T, owner *Client) (reindex *Client, roleName string) {
	t.Helper()
	dsn := owner.Pool.Config().ConnString()
	roles := newGrantRoles(t, dsn, reindexRolePlaceholder)
	roleName = roles.name(reindexRolePlaceholder)
	ident := pgx.Identifier{roleName}.Sanitize()
	grantExec(t, owner, `REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
	grantExec(t, owner, `CREATE ROLE `+ident+` LOGIN PASSWORD 'fixture-password'`)
	grantExec(t, owner, `ALTER ROLE `+ident+` SET search_path = public`)

	reindex, err := NewClient(t.Context(), dsn, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.User = roleName
		cfg.ConnConfig.Password = "fixture-password"
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reindex.Close(context.Background())) })
	return reindex, roleName
}

func TestPublishedReindexMaintenanceGrants(t *testing.T) {
	blocks := readPublishedReindexGrantBlocks(t)
	owner, _ := vectorDatabase(t)
	_, roleName := provisionReindexRole(t, owner)
	ctx := t.Context()

	var inherited bool
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT has_function_privilege($1, 'grant_catalog_document_tenant(uuid, uuid)', 'EXECUTE')
		OR has_schema_privilege($1, 'public', 'CREATE')`, roleName).Scan(&inherited))
	require.False(t, inherited, "the fresh maintenance role starts with neither schema CREATE nor catalog function EXECUTE")

	ddl := strings.NewReplacer("<generation>", "g2", "<dimensions>", "3").Replace(publishedSQL(t, "deployment.md", "CREATE TABLE public.chunk_vectors_<generation>"))
	_, err := owner.Pool.Exec(ctx, ddl)
	require.NoError(t, err)

	grantExec(t, owner, replaceReindexRole(blocks.base, roleName, ""))
	grantExec(t, owner, replaceReindexRole(blocks.legacy, roleName, ""))
	grantExec(t, owner, replaceReindexRole(blocks.precreated, roleName, "g2"))

	var ok bool
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT
		has_schema_privilege($1, 'public', 'USAGE')
		AND NOT has_schema_privilege($1, 'public', 'CREATE')
		AND has_function_privilege($1, 'grant_catalog_document_tenant(uuid, uuid)', 'EXECUTE')
		AND has_table_privilege($1, 'documents', 'SELECT')
		AND has_table_privilege($1, 'chunks', 'SELECT')
		AND has_table_privilege($1, 'document_pdf_extractions', 'SELECT')
		AND has_table_privilege($1, 'document_deletions', 'SELECT')
		AND has_table_privilege($1, 'ingest_jobs', 'SELECT')
		AND has_table_privilege($1, 'evaluation_fixtures', 'SELECT')
		AND has_table_privilege($1, 'vector_generations', 'SELECT')
		AND has_table_privilege($1, 'vector_generations', 'INSERT')
		AND has_table_privilege($1, 'vector_generations', 'UPDATE')
		AND NOT has_table_privilege($1, 'vector_generations', 'DELETE')
		AND has_table_privilege($1, 'vector_generation_documents', 'SELECT')
		AND has_table_privilege($1, 'vector_generation_documents', 'INSERT')
		AND has_table_privilege($1, 'vector_generation_documents', 'UPDATE')
		AND has_table_privilege($1, 'vector_generation_documents', 'DELETE')
		AND has_table_privilege($1, 'vector_state', 'SELECT')
		AND has_table_privilege($1, 'vector_state', 'UPDATE')
		AND NOT has_table_privilege($1, 'vector_state', 'INSERT')
		AND NOT has_table_privilege($1, 'vector_state', 'DELETE')
		AND has_table_privilege($1, 'chunk_vectors', 'SELECT')
		AND has_table_privilege($1, 'chunk_vectors', 'INSERT')
		AND has_table_privilege($1, 'chunk_vectors', 'UPDATE')
		AND has_table_privilege($1, 'chunk_vectors', 'DELETE')
		AND has_table_privilege($1, 'chunk_vectors_g2', 'SELECT')
		AND has_table_privilege($1, 'chunk_vectors_g2', 'INSERT')
		AND has_table_privilege($1, 'chunk_vectors_g2', 'UPDATE')
		AND has_table_privilege($1, 'chunk_vectors_g2', 'DELETE')
		AND NOT has_table_privilege($1, 'grant_catalog_document_ownership', 'SELECT')`,
		roleName).Scan(&ok))
	require.True(t, ok)

	grantExec(t, owner, replaceReindexRole(blocks.selfCreate, roleName, ""))
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT
		has_schema_privilege($1, 'public', 'CREATE')
		AND has_table_privilege($1, 'documents', 'REFERENCES')`,
		roleName).Scan(&ok))
	require.True(t, ok)
}

func TestReindexMaintenanceRoleFailsClosedWithoutCatalogTenantGrant(t *testing.T) {
	blocks := readPublishedReindexGrantBlocks(t)
	owner, legacy := vectorDatabase(t)
	reindex, roleName := provisionReindexRole(t, owner)
	ctx := t.Context()

	grantExec(t, owner, replaceReindexRole(blocks.base, roleName, ""))
	grantExec(t, owner, `REVOKE EXECUTE ON FUNCTION public.grant_catalog_document_tenant(uuid, uuid) FROM `+pgx.Identifier{roleName}.Sanitize())

	// Even on an empty corpus, PostgreSQL checks function EXECUTE during statement initialization.
	_, err := reindex.DocumentRevisions(ctx, "", 10)
	requireSQLState(t, err, "42501")
	var denied *pgconn.PgError
	require.ErrorAs(t, err, &denied)
	require.Contains(t, denied.Message, "grant_catalog_document_tenant")

	unassignedID := uuid.NewString()
	grantExec(t, owner, `INSERT INTO documents(id, content, status, collection, doc_type, chunk_count) VALUES($1, 'unassigned body', 'completed', 'c', 'note', 1)`, unassignedID)
	grantExec(t, owner, `INSERT INTO chunks(id, document_id, chunk_index, text, token_count) VALUES($1, $2, 0, 'unassigned chunk', 2)`, uuid.NewString(), unassignedID)
	grantExec(t, owner, `INSERT INTO document_pdf_extractions(document_id, profile, manifest, coverage, page_results) VALUES($1, $2, '{}', 'complete', '[{"number":1,"state":"complete"}]')`, unassignedID, domain.LayoutPDFProfile)

	_, err = reindex.DocumentRevisions(ctx, "", 10)
	requireSQLState(t, err, "42501")
	require.ErrorAs(t, err, &denied)
	require.Contains(t, denied.Message, "grant_catalog_document_tenant")
	_, err = reindex.LoadDocumentSnapshotBounded(ctx, unassignedID, 10, 4096)
	requireSQLState(t, err, "42501")
	require.ErrorAs(t, err, &denied)
	require.Contains(t, denied.Message, "grant_catalog_document_tenant")

	f := provisionOwnedFixture(t, owner)
	legacy.RouteCompletion()
	req := ownedRequest(f.owner, "team", "assigned body")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	job := f.claim(t)
	assignedChunk := chunkFor(req.Document.ID, 0)
	assignedChunk.ID = domain.ChunkID(strings.ReplaceAll(uuid.NewString(), "-", ""))
	assignedChunk.Embedding = []float32{1, 0, 0}
	done, err := owner.CompleteJob(ctx, *job, []domain.Chunk{assignedChunk}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)

	_, err = reindex.DocumentRevisions(ctx, "", 10)
	requireSQLState(t, err, "42501")
	require.ErrorAs(t, err, &denied)
	require.Contains(t, denied.Message, "grant_catalog_document_tenant")
	_, err = reindex.LoadDocumentSnapshotBounded(ctx, string(req.Document.ID), 10, 4096)
	requireSQLState(t, err, "42501")
	require.ErrorAs(t, err, &denied)
	require.Contains(t, denied.Message, "grant_catalog_document_tenant")

	grantExec(t, owner, replaceReindexRole(blocks.base, roleName, ""))
	revs, err := reindex.DocumentRevisions(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, revs, 2)
	snap, err := reindex.LoadDocumentSnapshotBounded(ctx, unassignedID, 10, 4096)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Empty(t, snap.TenantID)
	assignedSnap, err := reindex.LoadDocumentSnapshotBounded(ctx, string(req.Document.ID), 10, 4096)
	require.NoError(t, err)
	require.NotNil(t, assignedSnap)
	require.Equal(t, f.tenant, assignedSnap.TenantID)
}

func TestReindexMaintenanceRoleWithDocumentedGrants(t *testing.T) {
	blocks := readPublishedReindexGrantBlocks(t)
	owner, legacy := vectorDatabase(t)
	reindex, roleName := provisionReindexRole(t, owner)
	ctx := t.Context()

	grantExec(t, owner, replaceReindexRole(blocks.base, roleName, ""))
	grantExec(t, owner, replaceReindexRole(blocks.legacy, roleName, ""))
	ddl := strings.NewReplacer("<generation>", "g2", "<dimensions>", "3").Replace(publishedSQL(t, "deployment.md", "CREATE TABLE public.chunk_vectors_<generation>"))
	_, err := owner.Pool.Exec(ctx, ddl)
	require.NoError(t, err)
	grantExec(t, owner, replaceReindexRole(blocks.precreated, roleName, "g2"))

	// Unassigned and assigned documents together with an evaluation fixture row.
	unassignedID := uuid.NewString()
	unassignedChunkID := uuid.NewString()
	grantExec(t, owner, `INSERT INTO documents(id, content, status, collection, doc_type, chunk_count) VALUES($1, 'unassigned', 'completed', 'c1', 'note', 1)`, unassignedID)
	grantExec(t, owner, `INSERT INTO chunks(id, document_id, chunk_index, text, token_count) VALUES($1, $2, 0, 'unassigned text', 2)`, unassignedChunkID, unassignedID)

	f := provisionOwnedFixture(t, owner)
	legacy.RouteCompletion()
	req := ownedRequest(f.owner, "team", "assigned")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	job := f.claim(t)
	assignedChunk := chunkFor(req.Document.ID, 0)
	assignedChunk.ID = domain.ChunkID(strings.ReplaceAll(uuid.NewString(), "-", ""))
	assignedChunk.Embedding = []float32{0.5, 0.5, 0}
	done, err := owner.CompleteJob(ctx, *job, []domain.Chunk{assignedChunk}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)

	// An active fixture bound elsewhere blocks FreezeWriters toward g2 via evaluation_fixtures SELECT.
	fixtureID, receipt := fixtureShapedID(), uuid.NewString()
	const boundElsewhere = "pinecone:bound"
	acceptedFixture, err := owner.AcceptDocument(ctx, fixtureRequest(fixtureID, receipt, boundElsewhere))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, acceptedFixture.Outcome)
	fixtureJob, err := owner.ClaimJobForFixtureStore(ctx, "worker", time.Minute, 5, boundElsewhere)
	require.NoError(t, err)
	require.NotNil(t, fixtureJob)
	require.Equal(t, fixtureID, fixtureJob.Document.ID)
	fixtureChunk := chunkFor(fixtureID, 0)
	fixtureChunk.ID = domain.ChunkID(strings.ReplaceAll(uuid.NewString(), "-", ""))
	fixtureChunk.Embedding = []float32{0, 1, 0}
	done, err = owner.CompleteJob(ctx, *fixtureJob, []domain.Chunk{fixtureChunk}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)

	require.NoError(t, owner.PinVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", generationProfile()))

	_, err = reindex.RegisterGeneration(ctx, Generation{Name: LegacyGeneration, Store: "postgres", Target: "chunk_vectors", Profile: generationProfile(), Origin: OriginAttested})
	require.NoError(t, err)
	_, err = reindex.RegisterGeneration(ctx, stagingGeneration("g2"))
	require.NoError(t, err)

	err = reindex.FreezeWriters(ctx, "g2")
	require.ErrorIs(t, err, ErrFixtureReceipts)

	state, err := owner.DeleteEvaluationFixture(ctx, fixtureID, receipt, acceptedFixture.Incarnation, boundElsewhere, func(context.Context) error { return nil })
	require.NoError(t, err)
	require.Equal(t, domain.FixtureDeleted, state)

	revs, err := reindex.DocumentRevisions(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, revs, 2)

	fence, err := reindex.FenceGeneration(ctx, "g2")
	require.NoError(t, err)
	providerErr := errors.New("target write response lost")
	require.ErrorIs(t, fence.Write(ctx, func() error { return providerErr }), providerErr)
	pending, err := reindex.Generation(ctx, "g2")
	require.NoError(t, err)
	require.True(t, pending.PendingWriteID.Valid)
	require.NoError(t, fence.RequireAcknowledged(ctx, pending.PendingWriteID.UUID))
	_, err = reindex.Pool.Exec(ctx, `ALTER TABLE public.vector_generations ADD COLUMN forbidden BOOLEAN`)
	requireSQLState(t, err, "42501")

	ledger := fence.Ledger()
	for _, rev := range revs {
		snap, loadErr := reindex.LoadDocumentSnapshotBounded(ctx, rev.ID, 10, 4096)
		require.NoError(t, loadErr)
		require.NotNil(t, snap)
		chunkIDs := []string{string(snap.Chunks[0].ID)}
		_, beginErr := ledger.BeginGenerationDocument(ctx, "g2", rev.ID, chunkIDs)
		require.NoError(t, beginErr)
		require.NoError(t, fence.Write(ctx, func() error { return nil }))
		require.NoError(t, ledger.CompleteGenerationDocument(ctx, "g2", rev.ID, snap.Revision, snap.Incarnation, chunkIDs, 1, 0))
	}

	orphanedID := uuid.NewString()
	_, err = ledger.BeginGenerationDocument(ctx, "g2", orphanedID, []string{"deadkey"})
	require.NoError(t, err)
	orphans, err := ledger.OrphanedGenerationDocuments(ctx, "g2", 10)
	require.NoError(t, err)
	require.Len(t, orphans, 1)
	require.NoError(t, ledger.ForgetGenerationDocument(ctx, "g2", orphanedID))
	hasOrphans, err := ledger.HasOrphanedGenerationDocuments(ctx, "g2")
	require.NoError(t, err)
	require.False(t, hasOrphans)

	require.NoError(t, reindex.FreezeWriters(ctx, "g2"))
	quiesce, err := reindex.WriterQuiesce(ctx, 0)
	require.NoError(t, err)
	require.True(t, quiesce.Quiet())
	frozenAt, err := fence.BeginVerification(ctx)
	require.NoError(t, err)
	require.NoError(t, fence.RecordVerification(ctx, frozenAt, "corpus-digest", json.RawMessage(`{"passed":true}`)))
	readiness, err := reindex.ReadActivationReadiness(ctx, "g2", 0)
	require.NoError(t, err)
	require.Equal(t, LegacyGeneration, readiness.Active)
	require.Equal(t, "g2", readiness.FreezeGeneration)
	require.NotNil(t, readiness.FrozenAt)
	require.True(t, readiness.FrozenAt.Equal(frozenAt))
	require.Equal(t, StateVerified, readiness.TargetState)
	require.False(t, readiness.PendingWrite)
	require.Equal(t, "corpus-digest", readiness.CorpusDigest)
	require.True(t, readiness.Quiesce.Quiet())
	require.NoError(t, fence.Activate(ctx, "corpus-digest", 0))
	fence.Close(ctx)
	require.NoError(t, reindex.ThawWriters(ctx))

	// Rollback staging requires UnexpectedChunkKeys and DELETE/UPSERT on legacy chunk_vectors.
	extra, err := reindex.UnexpectedChunkKeys(ctx, []string{strings.ReplaceAll(unassignedChunkID, "-", ""), "00000000000000000000000000000001"}, false)
	require.NoError(t, err)
	require.Equal(t, []string{"00000000000000000000000000000001"}, extra)
	legacyStore, err := OpenGenerationVectorStore(ctx, reindex, 3, LegacyGeneration, WithExactScopeMax(10))
	require.NoError(t, err)
	require.NoError(t, legacyStore.Upsert(ctx, "", []domain.Chunk{{ID: domain.ChunkID(strings.ReplaceAll(unassignedChunkID, "-", "")), DocumentID: domain.DocumentID(unassignedID), Index: 0, Text: "unassigned text", TokenCount: 2, Embedding: []float32{1, 0, 0}}}))
	require.NoError(t, legacyStore.DeleteIDs(ctx, extra))

	// Source metadata and catalog tables remain non-writable by the maintenance login.
	for _, stmt := range []string{
		`DELETE FROM documents`,
		`DELETE FROM chunks`,
		`DELETE FROM evaluation_fixtures`,
		`SELECT 1 FROM grant_catalog_document_ownership`,
	} {
		_, err = reindex.Pool.Exec(ctx, stmt)
		requireSQLState(t, err, "42501")
	}
}

func TestReindexMaintenanceRoleUsesPrecreatedVectorTableWithoutSchemaCreate(t *testing.T) {
	blocks := readPublishedReindexGrantBlocks(t)
	owner, _ := vectorDatabase(t)
	reindex, roleName := provisionReindexRole(t, owner)
	ctx := t.Context()

	grantExec(t, owner, replaceReindexRole(blocks.base, roleName, ""))
	ddl := strings.NewReplacer("<generation>", "g2", "<dimensions>", "3").Replace(publishedSQL(t, "deployment.md", "CREATE TABLE public.chunk_vectors_<generation>"))
	_, err := owner.Pool.Exec(ctx, ddl)
	require.NoError(t, err)
	grantExec(t, owner, replaceReindexRole(blocks.precreated, roleName, "g2"))

	var canCreate bool
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT has_schema_privilege($1, 'public', 'CREATE')`, roleName).Scan(&canCreate))
	require.False(t, canCreate)

	// Without pre-creation, CreateGenerationVectorStore fails closed with 42501.
	_, err = CreateGenerationVectorStore(ctx, reindex, 3, "g3")
	requireSQLState(t, err, "42501")

	// With pre-created table and matching indexes, CreateGenerationVectorStore succeeds without schema CREATE.
	g2, err := CreateGenerationVectorStore(ctx, reindex, 3, "g2", WithExactScopeMax(10))
	require.NoError(t, err)

	docID := uuid.NewString()
	chunkID := strings.ReplaceAll(uuid.NewString(), "-", "")
	grantExec(t, owner, `INSERT INTO documents(id, content, status, collection, doc_type, chunk_count) VALUES($1, 'body', 'completed', 'alpha', 'note', 1)`, docID)
	require.NoError(t, g2.Upsert(ctx, "", []domain.Chunk{{
		ID: domain.ChunkID(chunkID), DocumentID: domain.DocumentID(docID), Index: 0, Text: "body", TokenCount: 1,
		Metadata: map[string]any{"doc_type": "note", "collection": "alpha"}, Embedding: []float32{1, 0, 0},
	}}))
	fetched, err := g2.Fetch(ctx, []string{chunkID})
	require.NoError(t, err)
	require.Contains(t, fetched, chunkID)
	ids, next, err := g2.ListIDs(ctx, "", 10)
	require.NoError(t, err)
	require.Empty(t, next)
	require.Equal(t, []string{chunkID}, ids)
	hits, exact, err := g2.QueryFilteredWithExactness(ctx, []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "alpha", DocType: "note", DocumentIDs: []string{docID}})
	require.NoError(t, err)
	require.True(t, exact)
	require.Len(t, hits, 1)
	require.NoError(t, g2.DeleteIDs(ctx, []string{chunkID}))
}

func TestReindexMaintenanceRoleCreatingTableRequiresDocumentReferences(t *testing.T) {
	blocks := readPublishedReindexGrantBlocks(t)
	owner, _ := vectorDatabase(t)
	reindex, roleName := provisionReindexRole(t, owner)
	ctx := t.Context()

	grantExec(t, owner, replaceReindexRole(blocks.base, roleName, ""))
	grantExec(t, owner, `GRANT CREATE ON SCHEMA public TO `+pgx.Identifier{roleName}.Sanitize())

	// Schema CREATE alone cannot create chunk_vectors_g2 because of REFERENCES documents(id).
	_, err := CreateGenerationVectorStore(ctx, reindex, 3, "g2")
	requireSQLState(t, err, "42501")

	grantExec(t, owner, replaceReindexRole(blocks.selfCreate, roleName, ""))
	g2, err := CreateGenerationVectorStore(ctx, reindex, 3, "g2", WithExactScopeMax(10))
	require.NoError(t, err)
	require.NoError(t, g2.Ping(ctx))

	docID := uuid.NewString()
	chunkID := strings.ReplaceAll(uuid.NewString(), "-", "")
	grantExec(t, owner, `INSERT INTO documents(id, content, status, collection, doc_type, chunk_count) VALUES($1, 'self created', 'completed', 'beta', 'memo', 1)`, docID)
	require.NoError(t, g2.Upsert(ctx, "", []domain.Chunk{{
		ID: domain.ChunkID(chunkID), DocumentID: domain.DocumentID(docID), Index: 0, Text: "self created", TokenCount: 2,
		Metadata: map[string]any{"doc_type": "memo", "collection": "beta"}, Embedding: []float32{0, 1, 0},
	}}))
	hits, err := g2.QueryFiltered(ctx, []float32{0, 1, 0}, 5, domain.VectorFilter{Collection: "beta", DocType: "memo", DocumentIDs: []string{docID}})
	require.NoError(t, err)
	require.Len(t, hits, 1)
}
