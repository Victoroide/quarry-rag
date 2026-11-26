package postgres

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// legacyDocument stores a completed document with chunks and assigns it to the owner.
func (f *ownedFixture) legacyDocument(t *testing.T, chunks int) (id string, chunkIDs []string) {
	t.Helper()
	id = uuid.NewString()
	grantExec(t, f.c, `INSERT INTO documents(id,content,status,chunk_count) VALUES($1,'legacy text','completed',$2)`, id, chunks)
	ids := make([]string, chunks)
	for i := range ids {
		ids[i] = strings.ReplaceAll(uuid.NewString(), "-", "")
		grantExec(t, f.c, `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,$3,'chunk',1)`, ids[i], id, i)
	}
	f.importBatch(t, func(b map[string]any) {
		b["assignments"] = []grantEntry{grantAssignment(t, f.c, f.tenant, f.owner.ID, id)}
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

func TestPublishVerifiedDocumentRefusesChangedData(t *testing.T) {
	for name, change := range map[string]string{
		"chunk replaced":     `DELETE FROM chunks WHERE document_id=$1 AND chunk_index=1`,
		"chunk added":        `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES(gen_random_uuid(),$1,2,'new',1)`,
		"chunk text changed": `UPDATE chunks SET text='changed chunk text' WHERE document_id=$1 AND chunk_index=0`,
		"chunk size changed": `UPDATE chunks SET token_count=99 WHERE document_id=$1 AND chunk_index=0`,
		"row changed":        `UPDATE documents SET content='changed content', updated_at=NOW() WHERE id=$1`,
		"document reset":     `UPDATE documents SET status='processing' WHERE id=$1`,
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
		for _, column := range []string{"vector_mutation_pending", "vector_mutation_uncertain"} {
			f := newOwnedFixture(t)
			id, _ := f.legacyDocument(t, 1)
			candidates, err := f.c.BackfillCandidates(t.Context(), "", 10)
			require.NoError(t, err)
			require.Len(t, candidates, 1)
			grantExec(t, f.c, `UPDATE documents SET `+column+`=TRUE WHERE id=$1`, id)
			listed, err := f.c.BackfillCandidates(t.Context(), "", 10)
			require.NoError(t, err)
			require.Empty(t, listed, column)
			published, err := f.c.PublishVerifiedDocument(t.Context(), candidates[0])
			require.NoError(t, err)
			require.False(t, published, column)
		}
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
