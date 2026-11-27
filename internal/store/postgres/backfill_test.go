package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// legacyDocument stores a completed document with chunks and assigns it to the owner.
func (f *ownedFixture) legacyDocument(t *testing.T, chunks int) (id string, chunkIDs []string) {
	t.Helper()
	return f.legacyDocumentContext(t.Context(), t, chunks)
}

func (f *ownedFixture) legacyDocumentContext(ctx context.Context, t *testing.T, chunks int) (id string, chunkIDs []string) {
	t.Helper()
	id = uuid.NewString()
	grantExecContext(ctx, t, f.c, `INSERT INTO documents(id,content,status,chunk_count) VALUES($1,'legacy text','completed',$2)`, id, chunks)
	ids := make([]string, chunks)
	for i := range ids {
		ids[i] = strings.ReplaceAll(uuid.NewString(), "-", "")
		grantExecContext(ctx, t, f.c, `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,$3,'chunk',1)`, ids[i], id, i)
	}
	f.importBatchContext(ctx, t, func(b map[string]any) {
		b["assignments"] = []grantEntry{grantAssignmentContext(ctx, t, f.c, f.tenant, f.owner.ID, id)}
	})
	return id, ids
}

func TestBackfillCandidatesListQuarantinedHistoricalDocumentsInOrder(t *testing.T) {
	f := newOwnedFixture(t)
	first, firstChunks := f.legacyDocument(t, 3)
	second, _ := f.legacyDocument(t, 1)
	third, _ := f.legacyDocument(t, 2)
	accepted := ownedRequest(f.owner, "", "accepted text")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, accepted).Outcome)
	grantExec(t, f.c, `INSERT INTO documents(id,content,status) VALUES($1,'unassigned','completed')`, uuid.NewString())

	all, err := f.c.BackfillCandidates(t.Context(), "", 50)
	require.NoError(t, err)
	var order []string
	for _, doc := range all {
		order = append(order, doc.DocumentID)
		require.Equal(t, f.tenant, doc.TenantID)
		require.NotEmpty(t, doc.SourceVersion)
		require.NotEmpty(t, doc.ChunkDigest)
		require.False(t, doc.Scoped, "historical documents were written in the untagged format")
		require.Empty(t, doc.Attempt)
	}
	want := []string{first, second, third}
	sort.Strings(want)
	require.Equal(t, want, order, "only quarantined, completed, assigned historical documents are candidates")
	for _, doc := range all {
		if doc.DocumentID != first {
			continue
		}
		require.Equal(t, firstChunks, doc.ChunkIDs, "chunk identities follow chunk order in the vector stores' spelling")
		sum := sha256.Sum256([]byte("chunk"))
		wantDigest := hex.EncodeToString(sum[:])
		require.Equal(t, []string{wantDigest, wantDigest, wantDigest}, doc.ChunkTextDigests)
		require.Equal(t, []int32{1, 1, 1}, doc.ChunkTokenCounts)
	}

	page, err := f.c.BackfillCandidates(t.Context(), want[0], 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, want[1], page[0].DocumentID)
	page, err = f.c.BackfillCandidates(t.Context(), want[2], 50)
	require.NoError(t, err)
	require.Empty(t, page)
	_, err = f.c.BackfillCandidates(t.Context(), "not-a-uuid", 1)
	require.Error(t, err)
	capped, err := f.c.BackfillCandidates(t.Context(), "", 10_000)
	require.NoError(t, err)
	require.LessOrEqual(t, len(capped), MaxBackfillBatch)
}

func TestPublishVerifiedDocumentMakesOnlyTheVerifiedSnapshotReadable(t *testing.T) {
	f := newOwnedFixture(t)
	id, _ := f.legacyDocument(t, 2)
	candidates, err := f.c.BackfillCandidates(t.Context(), "", 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	assertDocumentAccess(t, f.c, id, f.owner.ID, false, false, false)

	published, err := f.c.PublishVerifiedDocument(t.Context(), candidates[0])
	require.NoError(t, err)
	require.True(t, published)
	assertDocumentAccess(t, f.c, id, f.owner.ID, true, true, true)
	again, err := f.c.BackfillCandidates(t.Context(), "", 10)
	require.NoError(t, err)
	require.Empty(t, again, "a verified document leaves the candidate list")
	published, err = f.c.PublishVerifiedDocument(t.Context(), candidates[0])
	require.NoError(t, err)
	require.True(t, published, "publishing twice is idempotent")
}

func TestGrantSourceVersionRetainsPublicationGuards(t *testing.T) {
	for name, change := range map[string]string{
		"chunk replaced":      `DELETE FROM chunks WHERE document_id=$1 AND chunk_index=1`,
		"chunk added":         `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES(gen_random_uuid(),$1,2,'new',1)`,
		"chunk text changed":  `UPDATE chunks SET text='changed chunk text' WHERE document_id=$1 AND chunk_index=0`,
		"chunk size changed":  `UPDATE chunks SET token_count=99 WHERE document_id=$1 AND chunk_index=0`,
		"row changed":         `UPDATE documents SET content='changed content', updated_at=NOW() WHERE id=$1`,
		"document reset":      `UPDATE documents SET status='processing' WHERE id=$1`,
		"document moved":      `UPDATE documents SET collection='team' WHERE id=$1`,
		"retry completed":     `UPDATE documents SET completed_at=statement_timestamp() WHERE id=$1`,
		"chunk count changed": `UPDATE documents SET chunk_count=1 WHERE id=$1`,
	} {
		t.Run(name, func(t *testing.T) {
			f := newOwnedFixture(t)
			id, _ := f.legacyDocument(t, 2)
			candidates, err := f.c.BackfillCandidates(t.Context(), "", 10)
			require.NoError(t, err)
			require.Len(t, candidates, 1)
			grantExec(t, f.c, change, id)
			published, err := f.c.PublishVerifiedDocument(t.Context(), candidates[0])
			require.NoError(t, err)
			require.False(t, published)
			assertDocumentAccess(t, f.c, id, f.owner.ID, false, false, false)
		})
	}
	t.Run("wrong identity or digest", func(t *testing.T) {
		f := newOwnedFixture(t)
		id, _ := f.legacyDocument(t, 1)
		candidates, err := f.c.BackfillCandidates(t.Context(), "", 10)
		require.NoError(t, err)
		for _, mutate := range []func(*BackfillDocument){
			func(d *BackfillDocument) { d.ChunkDigest = strings.Repeat("0", 64) },
			func(d *BackfillDocument) { d.SourceVersion = strings.Repeat("0", 64) },
			func(d *BackfillDocument) { d.Incarnation = uuid.NewString() },
		} {
			doc := candidates[0]
			mutate(&doc)
			published, publishErr := f.c.PublishVerifiedDocument(t.Context(), doc)
			require.NoError(t, publishErr)
			require.False(t, published)
		}
		assertDocumentAccess(t, f.c, id, f.owner.ID, false, false, false)
	})
	t.Run("unresolved vector mutations block listing and publication", func(t *testing.T) {
		c, _ := vectorDatabase(t)
		f := provisionOwnedFixture(t, c)
		for _, column := range []string{"vector_mutation_pending", "vector_mutation_uncertain"} {
			id, ids := f.legacyDocument(t, 1)
			version := grantSource(t, f.c, id)
			candidates, err := f.c.BackfillCandidates(t.Context(), "", 10)
			require.NoError(t, err)
			require.Len(t, candidates, 1)
			writer, err := f.c.Pool.Acquire(t.Context())
			require.NoError(t, err)
			t.Cleanup(writer.Release)
			tx, err := writer.Begin(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) })
			_, err = tx.Exec(t.Context(), `UPDATE documents SET `+column+`=TRUE WHERE id=$1`, id)
			require.NoError(t, err)
			require.NoError(t, tx.Commit(t.Context()))
			require.Equal(t, version, grantSource(t, f.c, id), column)
			listed, err := f.c.BackfillCandidates(t.Context(), "", 10)
			require.NoError(t, err)
			require.Empty(t, listed, column)
			published, err := f.c.PublishVerifiedDocument(t.Context(), candidates[0])
			require.NoError(t, err)
			require.False(t, published, column)
			assertDocumentAccess(t, f.c, id, f.owner.ID, false, false, false)
			grantExec(t, f.c, `UPDATE documents SET `+column+`=FALSE WHERE id=$1`, id)
			chunk := domain.Chunk{ID: domain.ChunkID(ids[0]), DocumentID: domain.DocumentID(id), Embedding: []float32{1, 0, 0}}
			verifyGrantSourceBackfill(t, f.c, id, chunk)
			assertDocumentAccess(t, f.c, id, f.owner.ID, true, true, true)
		}
	})
	t.Run("count guard after the assignment is refreshed", func(t *testing.T) {
		f := newOwnedFixture(t)
		id, _ := f.legacyDocument(t, 1)
		grantExec(t, f.c, `UPDATE documents SET chunk_count=2 WHERE id=$1`, id)
		f.importBatch(t, func(b map[string]any) {
			b["assignments"] = []grantEntry{grantAssignment(t, f.c, f.tenant, f.owner.ID, id)}
		})
		candidates, err := f.c.BackfillCandidates(t.Context(), "", 10)
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		published, err := f.c.PublishVerifiedDocument(t.Context(), candidates[0])
		require.NoError(t, err)
		require.False(t, published)
		assertDocumentAccess(t, f.c, id, f.owner.ID, false, false, false)
	})
	t.Run("accepted documents publish through completion only", func(t *testing.T) {
		f := newOwnedFixture(t)
		req := ownedRequest(f.owner, "", "accepted text")
		require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
		published, err := f.c.PublishVerifiedDocument(t.Context(), BackfillDocument{
			DocumentID: string(req.Document.ID), Incarnation: req.Owner.Incarnation, SourceVersion: strings.Repeat("0", 64), ChunkDigest: strings.Repeat("0", 64),
		})
		require.NoError(t, err)
		require.False(t, published)
	})
	t.Run("assignment waits for the source writer", testGrantSourceImportLock)
}

func testGrantSourceImportLock(t *testing.T) {
	f := newOwnedFixture(t)
	c := f.c
	ctx := t.Context()
	id := uuid.NewString()
	insertGrantSource(t, c, id)
	batch := testGrantBatch(f.revision)
	batch["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.owner.ID, id)}

	for _, commit := range []bool{false, true} {
		writer, err := c.Pool.Acquire(ctx)
		require.NoError(t, err)
		t.Cleanup(writer.Release)
		tx, err := writer.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(ctx)) })
		_, err = tx.Exec(ctx, `UPDATE documents SET content='changed source' WHERE id=$1`, id)
		require.NoError(t, err)

		importCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		t.Cleanup(cancel)
		result := make(chan error, 1)
		go func() {
			_, importErr := importTestBatch(importCtx, c, batch, false)
			result <- importErr
		}()
		waitForGrantSourceBlocker(t, c, writer.Conn().PgConn().PID())
		if commit {
			require.NoError(t, tx.Commit(ctx))
			require.ErrorContains(t, <-result, "invalid or conflicting assignments entry")
		} else {
			cancel()
			require.ErrorIs(t, <-result, context.Canceled)
			require.NoError(t, tx.Rollback(ctx))
			_, err = importTestBatch(ctx, c, batch, true)
			require.NoError(t, err)
		}
		cancel()
		assertGrantRevision(t, c, f.revision)
		assertGrantCount(t, c, "document_ownership", 0)
	}
}

func waitForGrantSourceBlocker(t *testing.T, c *Client, blocker uint32) {
	t.Helper()
	require.Eventually(t, func() bool {
		var blocked bool
		err := c.Pool.QueryRow(t.Context(), `SELECT EXISTS (
			SELECT FROM pg_stat_activity WHERE datname=current_database()
				AND $1::bigint = ANY(pg_blocking_pids(pid)))`, blocker).Scan(&blocked)
		return err == nil && blocked
	}, 4*time.Second, 20*time.Millisecond)
}

func TestVectorRowsMatchRequiresExactlyTheCurrentChunks(t *testing.T) {
	c, v := vectorDatabase(t)
	f := provisionOwnedFixture(t, c)
	id, ids := f.legacyDocument(t, 2)
	doc := BackfillDocument{DocumentID: id, ChunkIDs: ids}
	matches, err := c.VectorRowsMatch(t.Context(), doc)
	require.NoError(t, err)
	require.False(t, matches, "no vector rows exist yet")

	chunks := make([]domain.Chunk, len(ids))
	for i, chunkID := range ids {
		chunks[i] = domain.Chunk{ID: domain.ChunkID(chunkID), DocumentID: domain.DocumentID(id), Index: i, Embedding: []float32{1, 0, 0}}
	}
	require.NoError(t, v.Upsert(t.Context(), "", chunks[:1]))
	matches, err = c.VectorRowsMatch(t.Context(), doc)
	require.NoError(t, err)
	require.False(t, matches, "a missing chunk vector is not verified")
	require.NoError(t, v.Upsert(t.Context(), "", chunks))
	matches, err = c.VectorRowsMatch(t.Context(), doc)
	require.NoError(t, err)
	require.True(t, matches)
	require.NoError(t, v.Upsert(t.Context(), "", []domain.Chunk{{ID: "stale-vector", DocumentID: domain.DocumentID(id), Index: 5, Embedding: []float32{1, 0, 0}}}))
	matches, err = c.VectorRowsMatch(t.Context(), doc)
	require.NoError(t, err)
	require.False(t, matches, "a stale vector of the document blocks verification")
	matches, err = c.VectorRowsMatch(t.Context(), BackfillDocument{DocumentID: id})
	require.NoError(t, err)
	require.False(t, matches, "a document without chunks cannot be verified")
}

func hostedCandidate(ctx context.Context, t *testing.T, c *Client, id string) BackfillDocument {
	t.Helper()
	docs, err := c.BackfillHostedCandidates(ctx, "", MaxBackfillBatch)
	require.NoError(t, err)
	for _, doc := range docs {
		if doc.DocumentID == id {
			require.NotNil(t, doc.Hosted)
			return doc
		}
	}
	t.Fatalf("missing hosted candidate %s", id)
	return BackfillDocument{}
}

func addBackfillManifest(ctx context.Context, t *testing.T, c *Client, doc BackfillDocument, state string, pointer bool) string {
	t.Helper()
	revision := uuid.NewString()
	prefix := domain.RevisionVectorPrefix(domain.DocumentID(doc.DocumentID), revision)
	keys := make([]string, len(doc.ChunkIDs))
	for i := range keys {
		keys[i] = domain.FormatRevisionVectorID(domain.DocumentID(doc.DocumentID), revision, i)
	}
	grantExecContext(ctx, t, c, `INSERT INTO hosted_vector_manifests
		(revision_id,document_id,incarnation,attempt,id_prefix,vector_ids,chunk_ids,state)
		VALUES($1,$2,$3,1,$4,$5,$6,$7)`, revision, doc.DocumentID, doc.Incarnation, prefix, keys, doc.ChunkIDs, state)
	if pointer {
		grantExecContext(ctx, t, c, `INSERT INTO hosted_vector_revisions(document_id,incarnation,active_revision_id)
			VALUES($1,$2,$3)`, doc.DocumentID, doc.Incarnation, revision)
	}
	return revision
}

func TestHostedBackfillCandidateBindings(t *testing.T) {
	f := newOwnedFixture(t)
	healthy, ids := f.legacyDocument(t, 2)
	current := hostedCandidate(t.Context(), t, f.c, healthy)
	require.True(t, current.Hosted.Valid)
	require.False(t, current.Hosted.Present)
	require.NotEmpty(t, current.Hosted.Digest)
	legacyDigest := current.Hosted.Digest
	addBackfillManifest(t.Context(), t, f.c, current, "retired", false)
	require.Equal(t, legacyDigest, hostedCandidate(t.Context(), t, f.c, healthy).Hosted.Digest)
	active := addBackfillManifest(t.Context(), t, f.c, current, "active", true)
	grantExec(t, f.c, `UPDATE hosted_vector_manifests SET vector_namespace='__default__' WHERE revision_id=$1`, active)
	current = hostedCandidate(t.Context(), t, f.c, healthy)
	require.True(t, current.Hosted.Valid)
	require.True(t, current.Hosted.Present)
	require.True(t, current.Hosted.SourceCurrent)
	require.Equal(t, grantSource(t, f.c, healthy), current.SourceVersion)
	require.Equal(t, active, current.Hosted.RevisionID)
	require.Equal(t, ids, current.Hosted.ChunkIDs)
	require.Equal(t, []int32{0, 1}, current.Hosted.ChunkIndexes)
	require.Equal(t, 2, current.Hosted.DeclaredChunks)
	require.Len(t, current.Hosted.Digest, 64)
	require.NotEqual(t, legacyDigest, current.Hosted.Digest)
	grantExec(t, f.c, `UPDATE hosted_vector_manifests SET activated_at=now() WHERE revision_id=$1`, active)
	require.Equal(t, current.Hosted.Digest, hostedCandidate(t.Context(), t, f.c, healthy).Hosted.Digest)

	for name, tc := range map[string]struct {
		state   string
		pointer bool
		change  string
	}{
		"orphan active":             {"active", false, ""},
		"orphan writing":            {"writing", false, ""},
		"pointed retired":           {"retired", true, ""},
		"pointed writing":           {"writing", true, ""},
		"wrong owner":               {"active", true, `UPDATE hosted_vector_manifests SET document_id=gen_random_uuid() WHERE revision_id=$1`},
		"wrong incarnation":         {"active", true, `UPDATE hosted_vector_manifests SET incarnation=gen_random_uuid() WHERE revision_id=$1`},
		"wrong pointer incarnation": {"active", true, `UPDATE hosted_vector_revisions SET incarnation=gen_random_uuid() WHERE active_revision_id=$1`},
		"wrong collection":          {"active", true, `UPDATE hosted_vector_manifests SET collection='other' WHERE revision_id=$1`},
		"wrong namespace":           {"active", true, `UPDATE hosted_vector_manifests SET vector_namespace='other' WHERE revision_id=$1`},
		"wrong prefix":              {"active", true, `UPDATE hosted_vector_manifests SET id_prefix='wrong' WHERE revision_id=$1`},
		"cleanup attached": {"active", true, `INSERT INTO hosted_vector_cleanups(revision_id,document_id,incarnation,id_prefix,vector_ids)
			SELECT revision_id,document_id,incarnation,id_prefix,vector_ids FROM hosted_vector_manifests WHERE revision_id=$1`},
		"null element": {"active", true, `UPDATE hosted_vector_manifests SET chunk_ids[1]=NULL WHERE revision_id=$1`},
		"null key":     {"active", true, `UPDATE hosted_vector_manifests SET vector_ids[1]=NULL WHERE revision_id=$1`},
		"empty":        {"active", true, `UPDATE hosted_vector_manifests SET vector_ids='{}',chunk_ids='{}' WHERE revision_id=$1`},
		"dimensions":   {"active", true, `UPDATE hosted_vector_manifests SET chunk_ids=ARRAY[chunk_ids,chunk_ids] WHERE revision_id=$1`},
		"lower bound":  {"active", true, `UPDATE hosted_vector_manifests SET chunk_ids=array_fill(chunk_ids[1],ARRAY[2],ARRAY[0]) WHERE revision_id=$1`},
		"length":       {"active", true, `UPDATE hosted_vector_manifests SET vector_ids=vector_ids[1:1] WHERE revision_id=$1`},
	} {
		t.Run(name, func(t *testing.T) {
			id, _ := f.legacyDocument(t, 2)
			revision := addBackfillManifest(t.Context(), t, f.c, hostedCandidate(t.Context(), t, f.c, id), tc.state, tc.pointer)
			if tc.change != "" {
				grantExec(t, f.c, tc.change, revision)
			}
			got := hostedCandidate(t.Context(), t, f.c, id).Hosted
			require.True(t, got.Present)
			require.False(t, got.Valid)
			require.Empty(t, got.Digest)
			require.Empty(t, got.VectorIDs)
			require.Empty(t, got.ChunkIDs)
		})
	}

	for _, held := range []bool{false, true} {
		id, _ := f.legacyDocument(t, 1)
		doc := hostedCandidate(t.Context(), t, f.c, id)
		grantExec(t, f.c, `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,held,finalized)
			VALUES($1,$2,'legacy_reservation','legacy',$3,$3)`, id, doc.Incarnation, held)
		require.True(t, hostedCandidate(t.Context(), t, f.c, id).Hosted.DeletionReserved)
		ok, err := f.c.PublishHostedBackfill(t.Context(), doc)
		require.NoError(t, err)
		require.False(t, ok)
	}

	stale, _ := f.legacyDocument(t, 1)
	grantExec(t, f.c, `UPDATE documents SET content='changed source' WHERE id=$1`, stale)
	require.False(t, hostedCandidate(t.Context(), t, f.c, stale).Hosted.SourceCurrent)
	f.importBatch(t, func(b map[string]any) {
		b["assignments"] = []grantEntry{grantAssignment(t, f.c, f.tenant, f.owner.ID, stale)}
	})
	require.True(t, hostedCandidate(t.Context(), t, f.c, stale).Hosted.SourceCurrent)

	docs, err := f.c.BackfillHostedCandidates(t.Context(), "", 50)
	require.NoError(t, err)
	require.Greater(t, len(docs), 2)
	var ordered []string
	for _, doc := range docs {
		ordered = append(ordered, doc.DocumentID)
	}
	require.True(t, sort.StringsAreSorted(ordered))
	require.Contains(t, ordered, healthy)
	page, err := f.c.BackfillHostedCandidates(t.Context(), ordered[0], 1)
	require.NoError(t, err)
	require.Equal(t, docs[1:2], page)
	_, err = f.c.BackfillHostedCandidates(t.Context(), "bad uuid", 1)
	require.ErrorContains(t, err, "cursor")

	var extra []grantEntry
	for range MaxBackfillBatch {
		id := uuid.NewString()
		grantExec(t, f.c, `INSERT INTO documents(id,content,status) VALUES($1,'zero chunks','completed')`, id)
		extra = append(extra, grantAssignment(t, f.c, f.tenant, f.owner.ID, id))
	}
	f.importBatch(t, func(b map[string]any) { b["assignments"] = extra })
	capped, err := f.c.BackfillHostedCandidates(t.Context(), "", 1000)
	require.NoError(t, err)
	require.Len(t, capped, MaxBackfillBatch)
	var empty BackfillDocument
	for _, doc := range capped {
		if doc.Hosted.DeclaredChunks == 0 {
			empty = doc
			break
		}
	}
	require.NotNil(t, empty.Hosted)
	require.Empty(t, empty.ChunkIDs)
	require.Empty(t, empty.Hosted.ChunkIndexes)
	published, err := f.c.PublishHostedBackfill(t.Context(), empty)
	require.NoError(t, err)
	require.False(t, published, "an empty current chunk set cannot publish")
}

func TestPublishHostedBackfillFencesSnapshot(t *testing.T) {
	f := newOwnedFixture(t)
	publisher, publisherName := newHostedBackfillPublisher(t.Context(), t, f.c)
	for name, changes := range map[string][]string{
		"pointer added": {
			`UPDATE hosted_vector_manifests SET state='active' WHERE revision_id=$2 AND document_id=$1`,
			`INSERT INTO hosted_vector_revisions(document_id,incarnation,active_revision_id)
			 SELECT document_id,incarnation,revision_id FROM hosted_vector_manifests WHERE document_id=$1 AND revision_id=$2`,
		},
		"pointer removed": {
			`DELETE FROM hosted_vector_revisions WHERE document_id=$1 AND active_revision_id=$2`,
			`UPDATE hosted_vector_manifests SET state='retired' WHERE revision_id=$2 AND document_id=$1`,
		},
		"pointer swapped": {
			`UPDATE hosted_vector_manifests SET state='retired' WHERE document_id=$1 AND revision_id=$2`,
			`UPDATE hosted_vector_manifests SET state='active' WHERE document_id=$1 AND revision_id<>$2`,
			`UPDATE hosted_vector_revisions SET active_revision_id=(SELECT revision_id FROM hosted_vector_manifests
			 WHERE document_id=$1 AND revision_id<>$2) WHERE document_id=$1`,
		},
		"retired":             {`UPDATE hosted_vector_manifests SET state='retired' WHERE document_id=$1 AND revision_id=$2`},
		"changed order":       {`UPDATE hosted_vector_manifests SET vector_ids=ARRAY[vector_ids[2],vector_ids[1]] WHERE document_id=$1 AND revision_id=$2`},
		"wrong chunk binding": {`UPDATE hosted_vector_manifests SET chunk_ids=ARRAY[chunk_ids[2],chunk_ids[1]] WHERE document_id=$1 AND revision_id=$2`},
		"changed text":        {`UPDATE chunks SET text='different' WHERE document_id=$1 AND chunk_index=0 AND $2::uuid IS NOT NULL`},
		"missing chunks":      {`DELETE FROM chunks WHERE document_id=$1 AND $2::uuid IS NOT NULL`},
		"duplicate indexes":   {`UPDATE chunks SET chunk_index=0 WHERE document_id=$1 AND $2::uuid IS NOT NULL`},
		"sparse indexes":      {`UPDATE chunks SET chunk_index=2 WHERE document_id=$1 AND chunk_index=1 AND $2::uuid IS NOT NULL`},
		"reassigned tenant": {`UPDATE grant_catalog_document_ownership o
			SET (tenant_id,principal_id)=(SELECT p.tenant_id,p.principal_id FROM grant_catalog_principals p
			 WHERE p.tenant_id<>o.tenant_id)
			WHERE o.document_id=$1 AND $2::uuid IS NOT NULL`},
		"changed tokens": {`UPDATE chunks SET token_count=5 WHERE document_id=$1 AND chunk_index=0 AND $2::uuid IS NOT NULL`},
		"changed source": {`UPDATE documents SET content='changed source' WHERE id=$1 AND $2::uuid IS NOT NULL`},
		"declared count": {`UPDATE documents SET chunk_count=3 WHERE id=$1 AND $2::uuid IS NOT NULL`},
		"pending":        {`UPDATE documents SET vector_mutation_pending=true WHERE id=$1 AND $2::uuid IS NOT NULL`},
		"uncertain":      {`UPDATE documents SET vector_mutation_uncertain=true WHERE id=$1 AND $2::uuid IS NOT NULL`},
		"deleted":        {`DELETE FROM documents WHERE id=$1 AND $2::uuid IS NOT NULL`},
		"re-admitted": {
			`DELETE FROM documents WHERE id=$1 AND $2::uuid IS NOT NULL`,
			`INSERT INTO documents(id,content,status) SELECT $1,'re-admitted','completed' WHERE $2::uuid IS NOT NULL`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			id, _ := f.legacyDocumentContext(ctx, t, 2)
			initial := hostedCandidate(ctx, t, f.c, id)
			revision := addBackfillManifest(ctx, t, f.c, initial, "active", true)
			if name == "pointer added" {
				grantExecContext(ctx, t, f.c, `DELETE FROM hosted_vector_revisions WHERE document_id=$1`, id)
				grantExecContext(ctx, t, f.c, `UPDATE hosted_vector_manifests SET state='retired' WHERE revision_id=$1`, revision)
			}
			if name == "pointer swapped" {
				addBackfillManifest(ctx, t, f.c, initial, "retired", false)
			}
			doc := hostedCandidate(ctx, t, f.c, id)
			tx, err := f.c.Pool.Begin(ctx)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback(ctx) }()
			var blocker int32
			require.NoError(t, tx.QueryRow(ctx, `SELECT pg_backend_pid() FROM documents WHERE id=$1 FOR NO KEY UPDATE`, id).Scan(&blocker))
			for _, change := range changes {
				_, err = tx.Exec(ctx, change, id, revision)
				require.NoError(t, err)
			}
			done := startHostedBackfillPublish(ctx, publisher, doc)
			waitForHostedBackfillPublishers(ctx, t, f.c, blocker, publisherName, 1)
			require.NoError(t, tx.Commit(ctx))
			result := awaitHostedBackfillPublish(ctx, t, done)
			require.NoError(t, result.err)
			require.False(t, result.published, "stale snapshot published")
			switch name {
			case "missing chunks", "duplicate indexes", "sparse indexes", "wrong chunk binding":
				current := hostedCandidate(ctx, t, f.c, id)
				ok, err := publisher.PublishHostedBackfill(ctx, current)
				require.NoError(t, err)
				require.False(t, ok, "the current malformed rows are ineligible even with their own digest")
			}
			assertDocumentAccessContext(ctx, t, f.c, id, f.owner.ID, false, false, false)
		})
	}
	t.Run("valid concurrent publishers, replay and canceled lock", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		concurrentPublisher, concurrentName := newHostedBackfillPublisher(ctx, t, f.c)
		id, _ := f.legacyDocumentContext(ctx, t, 2)
		addBackfillManifest(ctx, t, f.c, hostedCandidate(ctx, t, f.c, id), "active", true)
		doc := hostedCandidate(ctx, t, f.c, id)
		for _, mutate := range []func(*BackfillDocument){
			func(d *BackfillDocument) { d.TenantID = f.otherTenant },
			func(d *BackfillDocument) { d.Incarnation = uuid.NewString() },
			func(d *BackfillDocument) { d.SourceVersion = "stale source" },
			func(d *BackfillDocument) { d.Hosted.Digest = "" },
		} {
			wrong := doc
			snapshot := *doc.Hosted
			wrong.Hosted = &snapshot
			mutate(&wrong)
			ok, err := publisher.PublishHostedBackfill(ctx, wrong)
			require.NoError(t, err)
			require.False(t, ok)
		}
		tx, err := f.c.Pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		var blocker int32
		require.NoError(t, tx.QueryRow(ctx, `SELECT pg_backend_pid() FROM documents WHERE id=$1 FOR NO KEY UPDATE`, id).Scan(&blocker))
		canceled, cancelPublish := context.WithCancel(ctx)
		defer cancelPublish()
		stopped := startHostedBackfillPublish(canceled, publisher, doc)
		waitForHostedBackfillPublishers(ctx, t, f.c, blocker, publisherName, 1)
		cancelPublish()
		canceledResult := awaitHostedBackfillPublish(ctx, t, stopped)
		require.ErrorIs(t, canceledResult.err, context.Canceled)
		require.False(t, canceledResult.published)
		assertDocumentAccessContext(ctx, t, f.c, id, f.owner.ID, false, false, false)

		first := startHostedBackfillPublish(ctx, concurrentPublisher, doc)
		second := startHostedBackfillPublish(ctx, concurrentPublisher, doc)
		waitForHostedBackfillPublishers(ctx, t, f.c, blocker, concurrentName, 2)
		require.NoError(t, tx.Commit(ctx))
		for _, done := range []<-chan hostedBackfillPublishResult{first, second} {
			result := awaitHostedBackfillPublish(ctx, t, done)
			require.NoError(t, result.err)
			require.True(t, result.published)
		}
		assertDocumentAccessContext(ctx, t, f.c, id, f.owner.ID, true, true, true)
		ok, err := publisher.PublishHostedBackfill(ctx, doc)
		require.NoError(t, err)
		require.True(t, ok)
		grantExecContext(ctx, t, f.c, `UPDATE hosted_vector_manifests SET state='retired' WHERE document_id=$1`, id)
		ok, err = publisher.PublishHostedBackfill(ctx, doc)
		require.NoError(t, err)
		require.False(t, ok, "ready ownership cannot validate a stale snapshot")
	})
}

func newHostedBackfillPublisher(ctx context.Context, t *testing.T, c *Client) (publisher *Client, name string) {
	t.Helper()
	name = "backfill-publisher-" + uuid.NewString()
	publisher, err := NewClient(ctx, c.Pool.Config().ConnString(), func(cfg *pgxpool.Config) {
		cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
		cfg.ConnConfig.RuntimeParams["application_name"] = name
	})
	require.NoError(t, err)
	t.Cleanup(publisher.Pool.Close)
	return publisher, name
}

type hostedBackfillPublishResult struct {
	published bool
	err       error
}

func startHostedBackfillPublish(ctx context.Context, publisher *Client, doc BackfillDocument) <-chan hostedBackfillPublishResult {
	done := make(chan hostedBackfillPublishResult, 1)
	go func() {
		published, err := publisher.PublishHostedBackfill(ctx, doc)
		done <- hostedBackfillPublishResult{published: published, err: err}
	}()
	return done
}

func awaitHostedBackfillPublish(ctx context.Context, t *testing.T, done <-chan hostedBackfillPublishResult) hostedBackfillPublishResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		t.Fatalf("waiting for hosted backfill publisher: %v", ctx.Err())
		return hostedBackfillPublishResult{}
	}
}

func waitForHostedBackfillPublishers(ctx context.Context, t *testing.T, c *Client, holder int32, publisherName string, want int) {
	t.Helper()
	// A second publisher can wait on the first publisher's tuple lock.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var waiting int
		err := c.Pool.QueryRow(ctx, `WITH RECURSIVE lock_edges AS MATERIALIZED (
			SELECT pid, application_name, unnest(pg_blocking_pids(pid)) AS blocker
			FROM pg_stat_activity WHERE datname=current_database()
		), lock_chain(pid, application_name) AS (
			SELECT pid, application_name FROM lock_edges WHERE blocker=$1
			UNION
			SELECT e.pid, e.application_name FROM lock_edges e JOIN lock_chain c ON e.blocker=c.pid
		)
		SELECT count(*) FROM lock_chain WHERE application_name=$2`, holder, publisherName).Scan(&waiting)
		if assert.NoError(collect, err) {
			assert.Equal(collect, want, waiting)
		}
	}, 4*time.Second, 20*time.Millisecond)
}

func TestPublishVerifiedDocumentWriterSessionLoss(t *testing.T) {
	f := newOwnedFixture(t)
	id, _ := f.legacyDocument(t, 1)
	ctx := t.Context()
	docs, err := f.c.BackfillCandidates(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, docs, 1)

	begin := func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		return f.c.Pool.BeginTx(ctx, opts)
	}
	errLostCommit := errors.New("publication commit reply lost")
	t.Run("backend termination", func(t *testing.T) {
		check := func(ctx context.Context, tx pgx.Tx) error {
			if err := f.c.gateWriter(ctx, tx); err != nil {
				return err
			}
			var terminated bool
			return f.c.Pool.QueryRow(ctx, `SELECT pg_terminate_backend($1,5000)`, tx.Conn().PgConn().PID()).Scan(&terminated)
		}
		ok, err := publishVerifiedDocument(ctx, docs[0], begin, check)
		require.Error(t, err)
		require.False(t, ok)
		assertDocumentAccessContext(ctx, t, f.c, id, f.owner.ID, false, false, false)
	})
	t.Run("idle transaction timeout", func(t *testing.T) {
		check := func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SET LOCAL idle_in_transaction_session_timeout='100ms'`); err != nil {
				return err
			}
			if err := f.c.gateWriter(ctx, tx); err != nil {
				return err
			}
			pid := tx.Conn().PgConn().PID()
			require.Eventually(t, func() bool {
				var gone bool
				err := f.c.Pool.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)`, pid).Scan(&gone)
				return err == nil && gone
			}, 5*time.Second, 25*time.Millisecond)
			return nil
		}
		ok, err := publishVerifiedDocument(ctx, docs[0], begin, check)
		require.Error(t, err)
		require.False(t, ok)
		assertDocumentAccessContext(ctx, t, f.c, id, f.owner.ID, false, false, false)
	})
	t.Run("commit reply lost", func(t *testing.T) {
		beginWithLostReply := func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
			tx, err := begin(ctx, opts)
			if err != nil {
				return nil, err
			}
			return backfillLostCommit{Tx: tx, reply: errLostCommit}, nil
		}
		ok, err := publishVerifiedDocument(ctx, docs[0], beginWithLostReply, f.c.gateWriter)
		require.ErrorIs(t, err, errLostCommit)
		require.False(t, ok)
		assertDocumentAccessContext(ctx, t, f.c, id, f.owner.ID, true, true, true)
		ok, err = f.c.PublishVerifiedDocument(ctx, docs[0])
		require.NoError(t, err)
		require.True(t, ok)
	})
}

type backfillLostCommit struct {
	pgx.Tx
	reply error
}

func (tx backfillLostCommit) Commit(ctx context.Context) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	return tx.reply
}

func TestPublishVerifiedDocumentSerializesWithFreeze(t *testing.T) {
	f := newOwnedFixture(t)
	first, _ := f.legacyDocument(t, 1)
	second, _ := f.legacyDocument(t, 1)
	ctx := t.Context()
	docs, err := f.c.BackfillCandidates(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, docs, 2)
	if docs[0].DocumentID != first {
		docs[0], docs[1] = docs[1], docs[0]
	}

	tx, err := beginFencedWriterTx(ctx, f.c.Pool.BeginTx, f.c.gateWriter)
	require.NoError(t, err)
	defer rollbackFencedWriterTx(ctx, tx)
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	require.Error(t, f.c.FreezeWriters(short, "g2"))
	var published bool
	require.NoError(t, tx.QueryRow(ctx, publishVerifiedDocumentSQL, first, docs[0].Incarnation,
		docs[0].SourceVersion, docs[0].ChunkDigest).Scan(&published))
	require.True(t, published)
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, f.c.FreezeWriters(ctx, "g2"))
	published, err = f.c.PublishVerifiedDocument(ctx, docs[1])
	require.ErrorIs(t, err, domain.ErrWritersFrozen)
	require.False(t, published)
	assertDocumentAccess(t, f.c, first, f.owner.ID, true, true, true)
	assertDocumentAccess(t, f.c, second, f.owner.ID, false, false, false)
}
