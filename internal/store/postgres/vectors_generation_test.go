package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func completedDocument(t *testing.T, c *Client) domain.DocumentID {
	t.Helper()
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, c.InsertDocument(t.Context(), domain.Document{ID: id}))
	require.NoError(t, c.UpdateDocumentStatus(t.Context(), id, "completed"))
	return id
}

func tableExists(t *testing.T, c *Client, table string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists))
	return exists
}

func TestGenerationVectorTableIsSeparate(t *testing.T) {
	c, legacy := vectorDatabase(t)
	generation, err := CreateGenerationVectorStore(t.Context(), c, 4, "g2")
	require.NoError(t, err)
	require.True(t, tableExists(t, c, "chunk_vectors"))
	require.True(t, tableExists(t, c, "chunk_vectors_g2"))
	var indexes int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_indexes WHERE tablename='chunk_vectors_g2' AND (indexdef LIKE '%USING hnsw%' OR indexdef LIKE '%(document_id)%' OR indexdef LIKE '%(collection)%')`).Scan(&indexes))
	require.Equal(t, 3, indexes)

	doc := completedDocument(t, c)
	require.NoError(t, legacy.Upsert(t.Context(), "", []domain.Chunk{{ID: "legacy-1", DocumentID: doc, Text: "t", TokenCount: 1, Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report"}}}))
	require.NoError(t, generation.Upsert(t.Context(), "", []domain.Chunk{{ID: "gen-1", DocumentID: doc, Text: "t", TokenCount: 1, Embedding: []float32{1, 0, 0, 0}, Metadata: map[string]any{"doc_type": "report"}}}))

	got, err := legacy.QueryFiltered(t.Context(), []float32{1, 0, 0}, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, domain.ChunkID("legacy-1"), got[0].Chunk.ID)
	got, err = generation.QueryFiltered(t.Context(), []float32{1, 0, 0, 0}, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, domain.ChunkID("gen-1"), got[0].Chunk.ID)

	// Deleting a document through either store touches only its own table.
	require.NoError(t, generation.DeleteByDocID(t.Context(), string(doc)))
	got, err = generation.QueryFiltered(t.Context(), []float32{1, 0, 0, 0}, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Empty(t, got)
	got, err = legacy.QueryFiltered(t.Context(), []float32{1, 0, 0}, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, generation.Ping(t.Context()))
}

func TestGenerationVectorStoreRejectsUnsafeNames(t *testing.T) {
	c, _ := vectorDatabase(t)
	for _, name := range []string{"", "G2", "g2; DROP TABLE documents", "g-2"} {
		_, err := CreateGenerationVectorStore(t.Context(), c, 3, name)
		require.Error(t, err, name)
		_, err = OpenGenerationVectorStore(t.Context(), c, 3, name)
		require.Error(t, err, name)
	}
	// The legacy name selects the fixed table.
	store, err := CreateGenerationVectorStore(t.Context(), c, 3, LegacyGeneration)
	require.NoError(t, err)
	require.NoError(t, store.Ping(t.Context()))
}

func TestExactScopeUsesTheSelectedGeneration(t *testing.T) {
	c, legacy := vectorDatabase(t)
	generation := "abcdefghijklmnopqrstuvwx"
	named, err := CreateGenerationVectorStore(t.Context(), c, 3, generation, WithExactScopeMax(2))
	require.NoError(t, err)
	doc := completedDocument(t, c)
	for store, id := range map[*VectorStore]domain.ChunkID{legacy: "legacy", named: "named"} {
		require.NoError(t, store.Upsert(t.Context(), "", []domain.Chunk{{ID: id, DocumentID: doc, Embedding: []float32{1, 0, 0}}}))
	}
	reopened, err := OpenGenerationVectorStore(t.Context(), c, 3, generation, WithExactScopeMax(2))
	require.NoError(t, err)
	require.NoError(t, reopened.EnsureExactScopeIndexes(t.Context()))
	hits, exact, err := reopened.QueryFilteredWithExactness(t.Context(), []float32{1, 0, 0}, 3, domain.VectorFilter{DocumentIDs: []string{string(doc)}})
	require.NoError(t, err)
	require.True(t, exact)
	require.Len(t, hits, 1)
	require.Equal(t, domain.ChunkID("named"), hits[0].Chunk.ID)
}

func TestOpenGenerationVectorStoreNeverCreatesSchema(t *testing.T) {
	c, _ := vectorDatabase(t)
	_, err := OpenGenerationVectorStore(t.Context(), c, 3, "g2")
	require.ErrorContains(t, err, "does not exist")
	require.False(t, tableExists(t, c, "chunk_vectors_g2"))
	_, err = CreateGenerationVectorStore(t.Context(), c, 3, "g2")
	require.NoError(t, err)
	opened, err := OpenGenerationVectorStore(t.Context(), c, 3, "g2")
	require.NoError(t, err)
	require.NoError(t, opened.Ping(t.Context()))
	_, err = OpenGenerationVectorStore(t.Context(), c, 4, "g2")
	require.ErrorContains(t, err, "dimension")
	_, err = CreateGenerationVectorStore(t.Context(), c, 4, "g2")
	require.ErrorContains(t, err, "dimension")
}

func TestStagingAStoreDoesNotRouteJobCompletion(t *testing.T) {
	c, _ := vectorDatabase(t)
	_, err := CreateGenerationVectorStore(t.Context(), c, 3, "g2")
	require.NoError(t, err)
	_, err = OpenGenerationVectorStore(t.Context(), c, 3, "g2")
	require.NoError(t, err)
	// A migration tool staging a table must not redirect the job path of the process.
	require.Equal(t, "chunk_vectors", c.vectorTargetTable())
}

func TestJobCompletionWritesToTheSelectedGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	c, err := NewClient(t.Context(), pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(t.Context()))
	_, err = CreateGenerationVectorStore(t.Context(), c, 3, "g2")
	require.NoError(t, err)
	selected, err := OpenGenerationVectorStore(t.Context(), c, 3, "g2")
	require.NoError(t, err)
	selected.RouteCompletion()
	c.SetVectorGeneration("g2")
	_, err = c.Pool.Exec(t.Context(), `UPDATE vector_state SET active_generation='g2'`)
	require.NoError(t, err)
	require.Equal(t, "chunk_vectors_g2", c.vectorTargetTable())

	accepted, err := c.AcceptDocument(t.Context(), jobsRequest("generation job"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	job, err := c.ClaimJob(t.Context(), "worker", 60e9, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	chunk := domain.Chunk{ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", DocumentID: job.Document.ID, Text: "generation job", TokenCount: 2, Embedding: []float32{0, 1, 0}}
	ok, err := c.CompleteJob(t.Context(), *job, []domain.Chunk{chunk}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	var inGeneration int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors_g2 WHERE document_id=$1`, string(job.Document.ID)).Scan(&inGeneration))
	require.Equal(t, 1, inGeneration)
	require.False(t, tableExists(t, c, "chunk_vectors"), "the fixed table is never created for a selected generation")
}

func TestVectorFetchListAndDeleteIDs(t *testing.T) {
	c, v := vectorDatabase(t)
	doc := completedDocument(t, c)
	chunks := []domain.Chunk{
		{ID: "k1", DocumentID: doc, Index: 0, Text: "one", TokenCount: 1, Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "a", "kind": "paragraph", "quality": "high"}},
		{ID: "k2", DocumentID: doc, Index: 1, Text: "two", TokenCount: 2, Embedding: []float32{0, 1, 0}, Metadata: map[string]any{"doc_type": "report"}},
		{ID: "k3", DocumentID: doc, Index: 2, Text: "three", TokenCount: 3, Embedding: []float32{0, 0, 0.5}, Metadata: map[string]any{"doc_type": "report"}},
	}
	require.NoError(t, v.Upsert(t.Context(), "", chunks))

	got, err := v.Fetch(t.Context(), []string{"k1", "k3", "absent"})
	require.NoError(t, err)
	require.Len(t, got, 2, "a missing key is simply absent")
	require.Equal(t, []float32{1, 0, 0}, got["k1"].Embedding)
	require.Equal(t, "one", got["k1"].Text)
	require.Equal(t, doc, got["k1"].DocumentID)
	require.Equal(t, 0, got["k1"].Index)
	require.Equal(t, "a", got["k1"].Metadata["collection"])
	require.Equal(t, "report", got["k1"].Metadata["doc_type"])
	require.Equal(t, "paragraph", got["k1"].Metadata["kind"])
	require.Equal(t, []float32{0, 0, 0.5}, got["k3"].Embedding)
	empty, err := v.Fetch(t.Context(), nil)
	require.NoError(t, err)
	require.Empty(t, empty)

	var all []string
	cursor := ""
	for {
		page, next, listErr := v.ListIDs(t.Context(), cursor, 2)
		require.NoError(t, listErr)
		require.LessOrEqual(t, len(page), 2)
		all = append(all, page...)
		if next == "" {
			break
		}
		cursor = next
	}
	require.Equal(t, []string{"k1", "k2", "k3"}, all)
	_, _, err = v.ListIDs(t.Context(), "", 0)
	require.Error(t, err)

	require.NoError(t, v.DeleteIDs(t.Context(), []string{"k2", "absent"}))
	require.NoError(t, v.DeleteIDs(t.Context(), nil))
	got, err = v.Fetch(t.Context(), []string{"k1", "k2", "k3"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.NotContains(t, got, "k2")
}

func TestAuthorizedSQLUsesSelectedGenerationTable(t *testing.T) {
	tenant := uuid.NewString()
	principal := uuid.NewString()
	docID := uuid.NewString()
	filter := domain.VectorFilter{
		DocType:     "guide",
		Collection:  "policies",
		DocumentIDs: []string{docID},
		Authorized:  &domain.AuthorizedScope{TenantID: tenant, PrincipalID: principal},
	}

	probeQuery, probeArgs, ok := probeSQL("chunk_vectors_g2", 10, filter)
	require.True(t, ok)
	exactQuery, exactArgs, ok := exactSQL("chunk_vectors_g2", "[1,0,0]", 5, 10, filter)
	require.True(t, ok)
	searchQuery, searchArgs, ok := searchSQL("chunk_vectors_g2", "[1,0,0]", 5, filter, true)
	require.True(t, ok)

	for name, q := range map[string]string{"probe": probeQuery, "exact": exactQuery, "search": searchQuery} {
		require.Contains(t, q, `"chunk_vectors_g2"`, name)
		require.Contains(t, q, "JOIN grant_catalog_readable_documents g", name)
		stripped := strings.ReplaceAll(q, `"chunk_vectors_g2"`, "")
		require.NotContains(t, stripped, "chunk_vectors", "%s must not reference the fixed table", name)
	}
	require.Equal(t, []any{"guide", "policies", []string{docID}, tenant, principal, 10}, probeArgs)
	require.Equal(t, []any{"[1,0,0]", "guide", "policies", []string{docID}, tenant, principal, 10, 5}, exactArgs)
	require.Equal(t, []any{"[1,0,0]", "guide", "policies", []string{docID}, tenant, principal, 5}, searchArgs)

	invalid := domain.VectorFilter{Authorized: &domain.AuthorizedScope{TenantID: tenant, PrincipalID: "not-a-uuid"}}
	_, _, ok = probeSQL("chunk_vectors_g2", 10, invalid)
	require.False(t, ok)
	_, _, ok = exactSQL("chunk_vectors_g2", "[1,0,0]", 5, 10, invalid)
	require.False(t, ok)
	_, _, ok = searchSQL("chunk_vectors_g2", "[1,0,0]", 5, invalid, true)
	require.False(t, ok)
}

func TestAuthorizedSearchUsesGenerationTable(t *testing.T) {
	f := newRetrievalFixture(t)
	ctx := t.Context()
	g2, err := CreateGenerationVectorStore(ctx, f.c, 3, "g2")
	require.NoError(t, err)
	for id, spec := range map[string]struct {
		chunkID   domain.ChunkID
		embedding []float32
	}{
		f.own:      {"g2-own", []float32{.8, .6, 0}},
		f.peerOnly: {"g2-peer", []float32{1, 0, 0}},
		f.shared:   {"g2-shared", []float32{.6, .8, 0}},
		f.other:    {"g2-other", []float32{1, 0, 0}},
	} {
		require.NoError(t, g2.Upsert(ctx, "", []domain.Chunk{{
			ID:         spec.chunkID,
			DocumentID: domain.DocumentID(id),
			Text:       "text",
			TokenCount: 1,
			Embedding:  spec.embedding,
			Metadata:   map[string]any{"doc_type": "guide"},
		}}))
	}

	filter := domain.VectorFilter{Authorized: &domain.AuthorizedScope{TenantID: f.owner.TenantID, PrincipalID: f.owner.ID}}
	for name, limit := range map[string]int{"exact": 100, "approximate": 0} {
		t.Run(name, func(t *testing.T) {
			g2.exactScopeMax = limit
			chunks, exact, searchErr := g2.QueryFilteredWithExactness(ctx, retrievalQuery, 10, filter)
			require.NoError(t, searchErr)
			require.Equal(t, limit > 0, exact)
			require.Equal(t, []string{f.own, f.shared}, documentIDs(chunks))
			require.Equal(t, []domain.ChunkID{"g2-own", "g2-shared"}, []domain.ChunkID{chunks[0].Chunk.ID, chunks[1].Chunk.ID})
			for _, sc := range chunks {
				require.NotEmpty(t, sc.ObservedIncarnation)
			}
		})
	}
	g2.exactScopeMax = 0
	f.grantRead(t, f.peer, f.shared, f.owner, false)
	chunks, err := g2.QueryFiltered(ctx, retrievalQuery, 10, filter)
	require.NoError(t, err)
	require.Equal(t, []string{f.own}, documentIDs(chunks))
	require.Equal(t, domain.ChunkID("g2-own"), chunks[0].Chunk.ID)
}
