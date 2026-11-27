package postgres

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	grantSourceVersionMigration = 43
	grantGoldenID               = "10000000-0000-4000-8000-000000000004"
	grantGoldenVersion          = "6757dbd0c1457106e979b029f214954ea6f8092545a17d48c3b910655f92d0a3"
	grantGoldenInsert           = `INSERT INTO documents
		(id,incarnation,content,metadata,doc_type,file_type,status,chunk_count,created_at,completed_at)
		VALUES ($1,$2,'Café 雪','{"aa":"雪","z":null}','candidate','text/plain','completed',1,
			'2024-01-01 00:00:00.123456+00','2024-01-01 00:00:01.654321+00')`
)

func TestGrantSourceVersionStableAcrossAdditiveMigration(t *testing.T) {
	c := migrationTestClient(t, pgtest.StartVector(t))
	// Simulate the other supported merge order for the untagged stamp.
	applyMigrationsExcept(t, c, 27)
	f := provisionOwnedFixture(t, c)
	grantExec(t, c, grantGoldenInsert, grantGoldenID, nil)
	chunk := chunkFor(domain.DocumentID(grantGoldenID), 0)
	chunk.Embedding = []float32{1, 0, 0}
	grantExec(t, c, `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,$3,$4)`,
		string(chunk.ID), grantGoldenID, chunk.Text, chunk.TokenCount)
	batch := testGrantBatch(f.revision)
	batch["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.owner.ID, grantGoldenID)}
	require.Equal(t, grantGoldenVersion, grantSource(t, c, grantGoldenID))
	_, err := importTestBatch(t.Context(), c, batch, true)
	require.NoError(t, err)

	grantExec(t, c, `ALTER TABLE documents ADD COLUMN supplemental_note text,
		ADD COLUMN supplemental_flag boolean NOT NULL DEFAULT false`)
	require.NoError(t, c.RunMigrations(t.Context()))
	require.Equal(t, grantGoldenVersion, grantSource(t, c, grantGoldenID), "nullable and defaulted columns are outside the projection")
	grantExec(t, c, `ALTER TABLE documents DISABLE TRIGGER documents_stamp_untagged;
		UPDATE documents SET supplemental_note='annotation', supplemental_flag=true,
			last_error='historical error', updated_at=clock_timestamp(),
			index_visibility='sample_seen', index_visibility_observed_at=clock_timestamp(),
			untagged_since=pg_current_xact_id(), vector_mutation_claim=gen_random_uuid(),
			vector_mutation_until=clock_timestamp();
		ALTER TABLE documents ENABLE TRIGGER documents_stamp_untagged`)
	require.Equal(t, grantGoldenVersion, grantSource(t, c, grantGoldenID))

	tx, err := c.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	_, err = tx.Exec(t.Context(), `CREATE SCHEMA alternate;
		CREATE TABLE alternate.documents (id uuid);
		SET LOCAL search_path TO alternate, public;
		SET LOCAL timezone TO 'Pacific/Chatham';
		SET LOCAL datestyle TO 'SQL, DMY'`)
	require.NoError(t, err)
	for _, query := range []string{
		`SELECT source_version FROM public.grant_catalog_source_documents WHERE document_id=$1`,
		`SELECT source_version FROM public.grant_catalog_source_snapshot($1)`,
	} {
		var version string
		require.NoError(t, tx.QueryRow(t.Context(), query, grantGoldenID).Scan(&version))
		require.Equal(t, grantGoldenVersion, version)
	}
	require.NoError(t, tx.Rollback(t.Context()))

	_, err = importTestBatch(t.Context(), c, batch, false)
	require.NoError(t, err)
	require.Equal(t, grantGoldenVersion, grantSource(t, c, grantGoldenID), "the assignment marker is outside the projection")
	verifyGrantSourceBackfill(t, c, grantGoldenID, chunk)
	assertDocumentAccess(t, c, grantGoldenID, f.owner.ID, true, true, true)
}

func TestGrantSourceVersionBoundProjection(t *testing.T) {
	f := newOwnedFixture(t)
	c := f.c
	ctx := t.Context()
	grantExec(t, c, grantGoldenInsert, grantGoldenID, nil)
	version := grantSource(t, c, grantGoldenID)
	require.Equal(t, grantGoldenVersion, version, "the opposite migration order has the same digest")
	for _, tc := range []struct{ name, change, collection string }{
		{name: "collection", change: "collection='team'", collection: "team"},
		{name: "type", change: "doc_type='job'"},
		{name: "fingerprint", change: "fingerprint='new'"},
		{name: "deduplication", change: "content_dedup='off'"},
		{name: "large Unicode content", change: "content=repeat('Café 雪', 131072)"},
		{name: "metadata", change: `metadata='{"aa":"雨","z":null}'`},
		{name: "empty versus null preview", change: "content_preview=''"},
		{name: "text source", change: "text_source='ocr'"},
		{name: "file name", change: "file_name='source.txt'"},
		{name: "file type", change: "file_type='application/pdf'"},
		{name: "object key", change: "s3_key='corpus/source.txt'"},
		{name: "status", change: "status='processing'"},
		{name: "chunk count", change: "chunk_count=2"},
		{name: "creation timestamp", change: "created_at=created_at+interval '1 microsecond'"},
		{name: "recompletion after retry", change: "completed_at=completed_at+interval '1 microsecond'"},
		{name: "scoped vectors", change: "scoped_vectors=true"},
		{name: "vector attempt", change: "vector_attempt=gen_random_uuid()"},
		{name: "incarnation required", change: "vector_incarnation_required=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := c.Pool.Begin(ctx)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
			_, err = tx.Exec(ctx, `UPDATE documents SET `+tc.change+` WHERE id=$1`, grantGoldenID)
			require.NoError(t, err)
			assertStaleSourceProjection(ctx, t, tx, f, grantGoldenID, tc.collection, version)
		})
	}
	grantExec(t, c, `UPDATE documents SET metadata='{"z":null,"aa":"雪"}' WHERE id=$1`, grantGoldenID)
	require.Equal(t, version, grantSource(t, c, grantGoldenID), "JSONB key order is normalized")

	t.Run("document identity", func(t *testing.T) {
		tx, err := c.Pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		alternateID := uuid.NewString()
		// Keep the effective incarnation fixed when testing the document ID.
		_, err = tx.Exec(ctx, grantGoldenInsert, alternateID, grantGoldenID)
		require.NoError(t, err)
		assertStaleSourceProjection(ctx, t, tx, f, alternateID, "", version)
	})
	t.Run("legacy and explicit incarnations and UUID reuse", func(t *testing.T) {
		for _, incarnation := range []*string{new(grantGoldenID), new(uuid.NewString()), nil} {
			tx, err := c.Pool.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(ctx)) })
			_, err = tx.Exec(ctx, `DELETE FROM documents WHERE id=$1`, grantGoldenID)
			require.NoError(t, err)
			_, err = tx.Exec(ctx, grantGoldenInsert, grantGoldenID, incarnation)
			require.NoError(t, err)
			switch {
			case incarnation == nil:
				_, err = tx.Exec(ctx, `UPDATE documents SET created_at=created_at+interval '1 microsecond' WHERE id=$1`, grantGoldenID)
				require.NoError(t, err)
				assertStaleSourceProjection(ctx, t, tx, f, grantGoldenID, "", version)
			case *incarnation == grantGoldenID:
				var explicit string
				require.NoError(t, tx.QueryRow(ctx, `SELECT source_version FROM grant_catalog_source_snapshot($1)`, grantGoldenID).Scan(&explicit))
				require.Equal(t, version, explicit)
			default:
				assertStaleSourceProjection(ctx, t, tx, f, grantGoldenID, "", version)
			}
			require.NoError(t, tx.Rollback(ctx))
		}
	})
}

func assertStaleSourceProjection(ctx context.Context, t *testing.T, tx pgx.Tx, f *ownedFixture, id, collection, stale string) {
	t.Helper()
	var version string
	require.NoError(t, tx.QueryRow(ctx, `SELECT source_version FROM grant_catalog_source_snapshot($1)`, id).Scan(&version))
	require.NotEqual(t, stale, version)
	batch := testGrantBatch(f.revision)
	assignment := grantEntry{
		"tenant_id": f.tenant, "principal_id": f.owner.ID, "collection": collection,
		"document_id": id, "source_version": version,
	}
	batch["assignments"] = []grantEntry{assignment}
	fresh, err := json.Marshal(batch)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1,true)`, string(fresh))
	require.NoError(t, err)
	assignment["source_version"] = stale
	body, err := json.Marshal(batch)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1,true)`, string(body))
	require.ErrorContains(t, err, "invalid or conflicting assignments entry")
}

func TestGrantSourceVersionUpgradePreservesReadinessAndReceipts(t *testing.T) {
	ctx := t.Context()
	c := migrationTestClient(t, pgtest.StartVector(t))
	applyMigrationsExcept(t, c, grantSourceVersionMigration)
	f := provisionOwnedFixture(t, c)
	pending, ready, unassigned := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, id := range []string{pending, ready, unassigned} {
		grantExec(t, c, `INSERT INTO documents(id,content,status,chunk_count) VALUES ($1,'shared source','completed',1)`, id)
	}
	pendingChunk := chunkFor(domain.DocumentID(pending), 0)
	pendingChunk.Embedding = []float32{1, 0, 0}
	grantExec(t, c, `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,$3,$4),($5,$6,0,$3,$4)`,
		string(pendingChunk.ID), pending, pendingChunk.Text, pendingChunk.TokenCount, uuid.NewString(), ready)
	oldPending, oldReady := grantSource(t, c, pending), grantSource(t, c, ready)
	readyReader := uuid.NewString()
	committed := testGrantBatch(f.revision)
	committed["principals"] = []grantEntry{grantPrincipal(f.otherTenant, readyReader, "ready reader", true)}
	committed["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.owner.ID, pending),
		grantAssignment(t, c, f.otherTenant, f.outsider.ID, ready)}
	committed["grants"] = []grantEntry{
		{"tenant_id": f.tenant, "principal_id": f.peer.ID, "document_id": pending,
			"incarnation": grantIncarnation(t, c, pending), "can_read": true, "can_download": false},
		{"tenant_id": f.otherTenant, "principal_id": readyReader, "document_id": ready,
			"incarnation": grantIncarnation(t, c, ready), "can_read": true, "can_download": false},
	}
	body, err := json.Marshal(committed)
	require.NoError(t, err)
	var receipt GrantImportResult
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT * FROM grant_catalog_import_batch($1,false)`, string(body)).
		Scan(&receipt.Revision, &receipt.Replayed))
	require.False(t, receipt.Replayed)
	legacyCandidates, err := c.BackfillCandidates(ctx, "", 10)
	require.NoError(t, err)
	var pendingCandidate BackfillDocument
	for _, candidate := range legacyCandidates {
		if candidate.DocumentID == pending {
			pendingCandidate = candidate
			continue
		}
		require.Equal(t, ready, candidate.DocumentID)
		ok, publishErr := c.PublishVerifiedDocument(ctx, candidate)
		require.NoError(t, publishErr)
		require.True(t, ok)
	}
	require.Equal(t, oldPending, pendingCandidate.SourceVersion)
	req := ownedRequest(f.owner, "", "accepted source")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	job := f.claim(t)
	ok, err := c.CompleteJob(ctx, *job, []domain.Chunk{chunkFor(req.Document.ID, 0)}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	accepted := string(req.Document.ID)
	oldAccepted := storedGrantSourceVersion(t, c, accepted)
	unapplied := testGrantBatch(receipt.Revision)
	unapplied["tenants"] = []grantEntry{{"tenant_id": uuid.NewString()}}
	unapplied["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.owner.ID, unassigned)}
	_, err = importTestBatch(ctx, c, unapplied, true)
	require.NoError(t, err)

	require.NoError(t, c.RunMigrations(ctx))
	assertDocumentAccess(t, c, ready, f.outsider.ID, true, true, true)
	assertDocumentAccess(t, c, ready, readyReader, true, false, false)
	assertDocumentAccess(t, c, ready, f.owner.ID, false, false, false)
	assertDocumentAccess(t, c, accepted, f.owner.ID, true, true, true)
	assertDocumentAccess(t, c, pending, f.peer.ID, false, false, false)
	require.NotEqual(t, oldPending, grantSource(t, c, pending))
	require.Equal(t, oldPending, storedGrantSourceVersion(t, c, pending))
	for _, dryRun := range []bool{true, false} {
		_, err = importTestBatch(ctx, c, unapplied, dryRun)
		require.ErrorContains(t, err, "invalid or conflicting assignments entry")
		replay, replayErr := importTestBatch(ctx, c, committed, dryRun)
		require.NoError(t, replayErr)
		require.Equal(t, GrantImportResult{Revision: receipt.Revision, Replayed: true, DryRun: dryRun}, replay)
	}
	// Exercise the same SQL call and normalized receipt with reformatted input.
	formatted, err := json.MarshalIndent(committed, "", "  ")
	require.NoError(t, err)
	var direct GrantImportResult
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT * FROM grant_catalog_import_batch($1,false)`, string(formatted)).
		Scan(&direct.Revision, &direct.Replayed))
	require.Equal(t, GrantImportResult{Revision: receipt.Revision, Replayed: true}, direct)
	assertGrantRevision(t, c, receipt.Revision)
	assertGrantCount(t, c, "policy_audit", int(receipt.Revision))
	assertGrantCount(t, c, "document_ownership", 3)
	assertGrantCount(t, c, "tenants", 2)
	ok, err = c.PublishVerifiedDocument(ctx, pendingCandidate)
	require.NoError(t, err)
	require.False(t, ok)

	revision := refreshPendingGrantSource(t, f, pending, ready, receipt.Revision)
	verifyGrantSourceBackfill(t, c, pending, pendingChunk)
	require.Equal(t, grantSource(t, c, pending), storedGrantSourceVersion(t, c, pending))
	require.Equal(t, oldReady, storedGrantSourceVersion(t, c, ready))
	require.Equal(t, oldAccepted, storedGrantSourceVersion(t, c, accepted))
	assertDocumentAccess(t, c, pending, f.peer.ID, true, false, false)
	assertDocumentAccess(t, c, pending, f.outsider.ID, false, false, false)

	grantsOnly := testGrantBatch(revision)
	grantsOnly["grants"] = []grantEntry{{"tenant_id": f.otherTenant, "principal_id": readyReader,
		"document_id": ready, "incarnation": grantIncarnation(t, c, ready), "can_read": true, "can_download": true}}
	granted, err := importTestBatch(ctx, c, grantsOnly, false)
	require.NoError(t, err)
	assertDocumentAccess(t, c, ready, readyReader, true, true, false)

	recaptured := testGrantBatch(granted.Revision)
	recaptured["tenants"] = unapplied["tenants"]
	recaptured["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.owner.ID, unassigned)}
	_, err = importTestBatch(ctx, c, recaptured, false)
	require.NoError(t, err)
	assertGrantCount(t, c, "tenants", 3)
	require.Equal(t, grantSource(t, c, unassigned), storedGrantSourceVersion(t, c, unassigned))
	assertDocumentAccess(t, c, unassigned, f.owner.ID, false, false, false)
}

func refreshPendingGrantSource(t *testing.T, f *ownedFixture, pending, ready string, revision int64) int64 {
	t.Helper()
	ctx := t.Context()
	c := f.c
	for _, tc := range []struct{ tenant, principal, id string }{
		{f.tenant, f.peer.ID, pending},
		{f.otherTenant, f.outsider.ID, pending},
		{f.otherTenant, f.outsider.ID, ready},
	} {
		batch := testGrantBatch(revision)
		batch["assignments"] = []grantEntry{grantAssignment(t, c, tc.tenant, tc.principal, tc.id)}
		_, err := importTestBatch(ctx, c, batch, false)
		require.ErrorContains(t, err, "invalid or conflicting assignments entry")
	}
	wrongIncarnation := testGrantBatch(revision)
	wrongIncarnation["grants"] = []grantEntry{{"tenant_id": f.tenant, "principal_id": f.peer.ID,
		"document_id": pending, "incarnation": uuid.NewString(), "can_read": true, "can_download": false}}
	_, wrongIncarnationErr := importTestBatch(ctx, c, wrongIncarnation, false)
	require.ErrorContains(t, wrongIncarnationErr, "invalid or conflicting grants entry")
	t.Run("pending ownership cannot follow another incarnation", func(t *testing.T) {
		tx, err := c.Pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		// Create an inconsistent document/ownership pair inside the test transaction.
		_, err = tx.Exec(ctx, `ALTER TABLE documents DISABLE TRIGGER keep_document_identity`)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, `UPDATE documents SET incarnation=gen_random_uuid() WHERE id=$1`, pending)
		require.NoError(t, err)
		var version string
		require.NoError(t, tx.QueryRow(ctx, `SELECT source_version FROM grant_catalog_source_snapshot($1)`, pending).Scan(&version))
		batch := testGrantBatch(revision)
		batch["assignments"] = []grantEntry{{"tenant_id": f.tenant, "principal_id": f.owner.ID,
			"collection": "", "document_id": pending, "source_version": version}}
		body, err := json.Marshal(batch)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1,false)`, string(body))
		require.ErrorContains(t, err, "invalid or conflicting assignments entry")
	})
	assertGrantCount(t, c, "policy_audit", int(revision))

	batch := testGrantBatch(revision)
	batch["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.owner.ID, pending)}
	_, err := importTestBatch(ctx, c, batch, true)
	require.NoError(t, err)
	result, err := importTestBatch(ctx, c, batch, false)
	require.NoError(t, err)
	require.Equal(t, revision+1, result.Revision)
	assertDocumentAccess(t, c, pending, f.owner.ID, false, false, false)
	return result.Revision
}

func storedGrantSourceVersion(t *testing.T, c *Client, id string) string {
	t.Helper()
	var version string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT source_version FROM grant_catalog_document_ownership WHERE document_id=$1`, id).Scan(&version))
	return version
}

func verifyGrantSourceBackfill(t *testing.T, c *Client, id string, chunk domain.Chunk) {
	t.Helper()
	ctx := t.Context()
	v, err := NewVectorStore(ctx, c, 3)
	require.NoError(t, err)
	chunk.ID = domain.ChunkID(strings.ReplaceAll(string(chunk.ID), "-", ""))
	require.NoError(t, v.Upsert(ctx, "", []domain.Chunk{chunk}))
	candidates, err := c.BackfillCandidates(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	candidate := candidates[0]
	require.Equal(t, id, candidate.DocumentID)
	require.Equal(t, []string{string(chunk.ID)}, candidate.ChunkIDs)
	verified, err := c.VectorRowsMatch(ctx, candidate)
	require.NoError(t, err)
	require.True(t, verified)
	published, err := c.PublishVerifiedDocument(ctx, candidate)
	require.NoError(t, err)
	require.True(t, published)
}
