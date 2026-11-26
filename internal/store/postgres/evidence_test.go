package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const utf8ClientOptions = "client_encoding=UTF8"

func TestEvidenceVisibilityMatchesUTF8BytesOnLatin1Database(t *testing.T) {
	ctx := t.Context()
	dsn := withFixtureOptions(t, pgtest.SiblingLatin1(t, startTestDatabase(t)), utf8ClientOptions)
	c, err := NewClient(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(ctx))
	var serverEncoding, clientEncoding string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT current_setting('server_encoding'), current_setting('client_encoding')`).Scan(&serverEncoding, &clientEncoding))
	require.Equal(t, "LATIN1", serverEncoding)
	require.Equal(t, "UTF8", clientEncoding)
	docID := domain.DocumentID(uuid.NewString())
	require.NoError(t, c.InsertDocument(ctx, domain.Document{ID: docID}))
	require.NoError(t, c.UpdateDocumentStatus(ctx, docID, "completed"))
	visibility := Visibility{Pool: c.Pool}

	for i, word := range []string{"cafe", "café"} {
		t.Run(word, func(t *testing.T) {
			ch := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: docID, Index: i, Text: word}
			require.NoError(t, c.InsertChunks(t.Context(), []domain.Chunk{ch}))
			var stored string
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT text FROM chunks WHERE id=$1`, string(ch.ID)).Scan(&stored))
			require.Equal(t, word, stored)
			visible, err := visibility.VisibleEvidence(t.Context(), []domain.ScoredChunk{{Chunk: ch}})
			require.NoError(t, err)
			require.Len(t, visible, 1)
			require.Equal(t, word, visible[0].Chunk.Text)
		})
	}
}

func TestEvidenceVisibilityMatchesCurrentChunkAndScope(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	archiveIncarnation := uuid.NewString()
	_, err := c.Pool.Exec(ctx, `INSERT INTO documents(id, content, status, doc_type, collection, incarnation, metadata) VALUES
		($1, 'private text', 'completed', 'report', 'archive', $3, '{"title":"Shared title"}'),
		($2, 'private text', 'completed', 'report', '', NULL, '{"title":"Shared title"}')`,
		listingFirstID, listingSecondID, archiveIncarnation)
	require.NoError(t, err)
	chunks := []domain.Chunk{
		{ID: domain.ChunkID(strings.ReplaceAll(uuid.NewString(), "-", "")), DocumentID: listingFirstID, Index: 4, Text: "Shared text.", Metadata: map[string]any{"doc_type": "report"}},
		{ID: domain.ChunkID(uuid.NewString()), DocumentID: listingSecondID, Index: 4, Text: "Shared text.", Metadata: map[string]any{"doc_type": "report"}},
	}
	require.NoError(t, c.InsertChunks(ctx, chunks))
	candidates := []domain.ScoredChunk{{Chunk: chunks[1], Score: .99}, {Chunk: chunks[0], Score: .9}}
	expected := []domain.ScoredChunk{candidates[0], candidates[1]}
	expected[0].ObservedIncarnation = listingSecondID
	expected[1].ObservedIncarnation = archiveIncarnation
	visibility := Visibility{Pool: c.Pool, Collection: "archive", DocType: "report"}
	got, err := visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, expected[1:], got)

	// Aliases match the stored UUID and keep the vector store's spelling, which a
	// byte-for-byte metadata filter needs to find the same document again.
	for _, spelling := range []string{"urn:uuid:" + string(candidates[1].Chunk.DocumentID), strings.ToUpper(string(candidates[1].Chunk.DocumentID))} {
		alias := candidates[1]
		alias.Chunk.DocumentID = domain.DocumentID(spelling)
		got, err = visibility.VisibleEvidence(ctx, []domain.ScoredChunk{alias})
		require.NoError(t, err)
		aliased := alias
		aliased.ObservedIncarnation = archiveIncarnation
		require.Equal(t, []domain.ScoredChunk{aliased}, got)
	}

	visibility.Collection = ""
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, expected, got)
	visibility.Collection = "archive"

	_, err = c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, listingFirstID)
	require.NoError(t, err)
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = c.Pool.Exec(ctx, `INSERT INTO documents(id,content,status,collection,doc_type)
		VALUES ($1,'Replacement','completed','archive','report')`, listingFirstID)
	require.NoError(t, err)
	replacement := chunks[0]
	replacement.Text = "Replacement."
	require.NoError(t, c.InsertChunks(ctx, []domain.Chunk{replacement}))
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Empty(t, got)
	got, err = visibility.VisibleEvidence(ctx, []domain.ScoredChunk{{Chunk: replacement}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotEmpty(t, got[0].ObservedIncarnation)
	require.NotEqual(t, archiveIncarnation, got[0].ObservedIncarnation)

	_, err = c.Pool.Exec(ctx, `ALTER TABLE chunks RENAME TO unavailable_chunks`)
	require.NoError(t, err)
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.ErrorContains(t, err, "visible evidence")
	require.Nil(t, got)
}

func TestPgvectorEvidenceRejectsStalePublishedIdentity(t *testing.T) {
	c, vs := vectorDatabase(t)
	ctx := t.Context()
	id, incarnation, attempt := uuid.NewString(), uuid.NewString(), uuid.NewString()
	insertPublishedDocument := func(inc, att string) {
		t.Helper()
		_, err := c.Pool.Exec(ctx, `INSERT INTO documents (id, incarnation, content, status, doc_type, collection, scoped_vectors, vector_attempt)
 VALUES ($1,$2,'Unchanged text.','completed','report','archive',TRUE,$3)`, id, inc, att)
		require.NoError(t, err)
	}
	insertPublishedDocument(incarnation, attempt)
	chunk := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(id),
		Text: "Unchanged text.", Embedding: retrievalQuery, Metadata: map[string]any{"doc_type": "report", "collection": "archive"}}
	require.NoError(t, c.InsertChunks(ctx, []domain.Chunk{chunk}))
	require.NoError(t, vs.Upsert(ctx, "", []domain.Chunk{chunk}))
	rank := func() []domain.ScoredChunk {
		t.Helper()
		got, err := vs.QueryFiltered(ctx, retrievalQuery, 1, domain.VectorFilter{Collection: "archive"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		return got
	}
	vis := Visibility{Pool: c.Pool, Collection: "archive"}
	check := func(hits []domain.ScoredChunk, count int) {
		t.Helper()
		visible, err := vis.VisibleEvidence(ctx, hits)
		require.NoError(t, err)
		require.Len(t, visible, count)
	}
	original := rank()
	check(original, 1)
	nextAttempt := uuid.NewString()
	_, err := c.Pool.Exec(ctx, `UPDATE documents SET vector_attempt=$2 WHERE id=$1`, id, nextAttempt)
	require.NoError(t, err)
	check(original, 0)
	next := rank()
	check(next, 1)
	_, err = c.Pool.Exec(ctx, `UPDATE documents SET vector_mutation_pending=TRUE WHERE id=$1`, id)
	require.NoError(t, err)
	check(rank(), 0)
	_, err = c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, id)
	require.NoError(t, err)
	insertPublishedDocument(uuid.NewString(), nextAttempt)
	require.NoError(t, c.InsertChunks(ctx, []domain.Chunk{chunk}))
	require.NoError(t, vs.Upsert(ctx, "", []domain.Chunk{chunk}))
	check(next, 0)
	replacement := rank()
	require.NotEqual(t, next[0].ObservedIncarnation, replacement[0].ObservedIncarnation)
	check(replacement, 1)

	ret := retriever.NewRetriever(vs, nil, 0.1, 1)
	qc := &pipeline.QueryContext{
		EvidenceVisibility: vis, Collection: "archive", MaxChunks: 1,
		QueryEmbedding: retrievalQuery, RetrievalMode: string(retriever.RetrievalModeGlobal),
	}
	require.NoError(t, pipeline.NewRetrievalStage(ret).Run(ctx, qc))
	require.Len(t, qc.RetrievedChunks, 1)
	require.Equal(t, chunk.ID, qc.RetrievedChunks[0].Chunk.ID)
}

func TestHistoricalHitsNeedAStampBeforeTheSnapshot(t *testing.T) {
	ctx := t.Context()
	c := migrationTestClient(t, startTestDatabase(t))
	applyMigrationPrefix(t, c, 26)

	newHit := func() domain.ScoredChunk {
		return domain.ScoredChunk{Chunk: domain.Chunk{
			ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(uuid.NewString()),
			Text: "Identical chunk text.", Index: 0,
		}}
	}
	old, foreign := newHit(), newHit()
	committed, replaced, pending, flipped := newHit(), newHit(), newHit(), newHit()
	tagged, manifested, late := newHit(), newHit(), newHit()
	insert := func(hit domain.ScoredChunk, inc string, scoped, required bool) {
		t.Helper()
		_, err := c.Pool.Exec(ctx, `
INSERT INTO documents(id, content, status, incarnation, scoped_vectors, vector_incarnation_required)
VALUES ($1, $2, 'completed', $3, $4, $5)`, hit.Chunk.DocumentID, hit.Chunk.Text, inc, scoped, required)
		require.NoError(t, err)
		require.NoError(t, c.InsertChunks(ctx, []domain.Chunk{hit.Chunk}))
	}
	insert(old, uuid.NewString(), false, false)
	insert(foreign, uuid.NewString(), false, false)
	require.NoError(t, c.RunMigrations(ctx))
	insert(replaced, uuid.NewString(), false, false)
	insert(flipped, uuid.NewString(), false, true)

	// Logical restores load document data with user triggers inactive.
	_, err := c.Pool.Exec(ctx, `ALTER TABLE documents DISABLE TRIGGER documents_stamp_untagged`)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `
UPDATE documents SET untagged_since=(pg_snapshot_xmax(pg_current_snapshot())::text::bigint + 1000000)::text::xid8
WHERE id=$1`, foreign.Chunk.DocumentID)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `ALTER TABLE documents ENABLE TRIGGER documents_stamp_untagged`)
	require.NoError(t, err)

	writer, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Rollback(context.Background()) })
	var pendingXID string
	require.NoError(t, writer.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&pendingXID))
	savepoint, err := writer.Begin(ctx)
	require.NoError(t, err)
	_, err = savepoint.Exec(ctx, `INSERT INTO documents(id,content,status) VALUES ($1,$2,'completed')`, pending.Chunk.DocumentID, pending.Chunk.Text)
	require.NoError(t, err)
	_, err = savepoint.Exec(ctx, `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES ($1,$2,$3,$4,1)`,
		pending.Chunk.ID, pending.Chunk.DocumentID, pending.Chunk.Index, pending.Chunk.Text)
	require.NoError(t, err)
	require.NoError(t, savepoint.Commit(ctx))

	// Commit a later transaction so the pending writer is inside the snapshot
	// range and appears in its list of in-progress transaction IDs.
	insert(committed, uuid.NewString(), false, false)
	visibility := Visibility{Pool: c.Pool}
	snapshot, err := visibility.ObserveEvidenceSnapshot(ctx)
	require.NoError(t, err)
	var pendingInSnapshot bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT EXISTS (
    SELECT 1 FROM pg_snapshot_xip($1::text::pg_snapshot) AS xip WHERE xip::text=$2
)`, snapshot, pendingXID).Scan(&pendingInSnapshot))
	require.True(t, pendingInSnapshot)
	before, err := visibility.VisibleEvidence(ctx, []domain.ScoredChunk{replaced})
	require.NoError(t, err)
	require.Len(t, before, 1)

	require.NoError(t, writer.Commit(ctx))
	require.Equal(t, &pendingXID, untaggedStamp(t, c, pending.Chunk.DocumentID))
	_, err = c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, replaced.Chunk.DocumentID)
	require.NoError(t, err)
	replacementIncarnation := uuid.NewString()
	insert(replaced, replacementIncarnation, false, false)
	_, err = c.Pool.Exec(ctx, `UPDATE documents SET vector_incarnation_required=false WHERE id=$1`, flipped.Chunk.DocumentID)
	require.NoError(t, err)
	insert(late, uuid.NewString(), false, false)

	taggedIncarnation := uuid.NewString()
	tagged.Chunk.Metadata = map[string]any{domain.VectorRevisionKey: taggedIncarnation}
	insert(tagged, taggedIncarnation, true, false)

	manifestedIncarnation := uuid.NewString()
	insert(manifested, manifestedIncarnation, false, false)
	manifested.Chunk.RevisionID = uuid.NewString()
	_, err = c.Pool.Exec(ctx, `
INSERT INTO hosted_vector_manifests(revision_id,document_id,incarnation,attempt,id_prefix,vector_ids,chunk_ids,state)
VALUES ($1,$2,$3,1,$1::uuid::text,ARRAY[$1::uuid::text],ARRAY[$4::uuid],'active')`,
		manifested.Chunk.RevisionID, manifested.Chunk.DocumentID, manifestedIncarnation, manifested.Chunk.ID)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `INSERT INTO hosted_vector_revisions(document_id,incarnation,active_revision_id) VALUES ($1,$2,$3)`,
		manifested.Chunk.DocumentID, manifestedIncarnation, manifested.Chunk.RevisionID)
	require.NoError(t, err)

	pinned := replaced
	pinned.ObservedIncarnation = replacementIncarnation
	writeTime := replaced
	writeTime.Chunk.VectorIncarnation = replacementIncarnation
	for _, tc := range []struct {
		name string
		hit  domain.ScoredChunk
		want bool
	}{
		{"committed before search", committed, true},
		{"replacement during search", replaced, false},
		{"insert in flight during observation", pending, false},
		{"became untagged during search", flipped, false},
		{"inserted during search", late, false},
		{"existing before migration", old, true},
		{"restored stamp beyond current xmax", foreign, true},
		{"already observed incarnation", pinned, true},
		{"write-time incarnation", writeTime, true},
		{"tagged revision", tagged, true},
		{"active manifested revision", manifested, true},
		{"release recheck of old incarnation", before[0], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.hit.ObservedSnapshot = snapshot
			visible, checkErr := visibility.VisibleEvidence(t.Context(), []domain.ScoredChunk{tc.hit})
			require.NoError(t, checkErr)
			if tc.want {
				require.Len(t, visible, 1)
				require.NotEmpty(t, visible[0].ObservedIncarnation)
				return
			}
			require.Empty(t, visible)
		})
	}
	visible, err := visibility.VisibleEvidence(ctx, []domain.ScoredChunk{replaced})
	require.NoError(t, err)
	require.Len(t, visible, 1, "an empty snapshot leaves a hit unpinned")
	replaced.ObservedSnapshot, err = visibility.ObserveEvidenceSnapshot(ctx)
	require.NoError(t, err)
	visible, err = visibility.VisibleEvidence(ctx, []domain.ScoredChunk{replaced})
	require.NoError(t, err)
	require.Len(t, visible, 1, "a fresh query can see the replacement")
}
