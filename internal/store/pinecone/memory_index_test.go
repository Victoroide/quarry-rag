package pinecone

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func memoryClient(dims int) (*Client, *MemoryIndex) {
	index := NewMemoryIndex(dims)
	return NewClientForIndex(index, &config.Config{PineconeUpsertBatchSize: 100, PineconeTimeout: 1e9, PineconeMaxAttempts: 1, EmbeddingDimensions: dims}, ""), index
}

func memoryChunk(id, doc string, vector []float32, docType, collection string) domain.Chunk {
	return domain.Chunk{ID: domain.ChunkID(id), DocumentID: domain.DocumentID(doc), Index: len(id), Text: "text " + id, TokenCount: 2, Embedding: vector,
		Metadata: map[string]any{"doc_type": docType, "collection": collection, "kind": "paragraph", "quality": "high"}}
}

func TestMemoryIndexRoundTripsThroughTheClient(t *testing.T) {
	client, index := memoryClient(3)
	require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{
		memoryChunk("a", "doc-1", []float32{1, 0, 0}, "report", "x"),
		memoryChunk("b", "doc-1", []float32{0, 1, 0}, "report", "x"),
		memoryChunk("c", "doc-2", []float32{1, 1, 0}, "note", "y"),
	}))
	require.Equal(t, 3, index.Len())
	got, err := client.Fetch(t.Context(), []string{"a", "c", "absent"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, []float32{1, 0, 0}, got["a"].Embedding)
	require.Equal(t, "x", got["a"].Metadata["collection"])
	require.Equal(t, domain.DocumentID("doc-2"), got["c"].DocumentID)

	// Upsert replaces a record's values and metadata.
	require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{memoryChunk("a", "doc-1", []float32{0, 0, 1}, "memo", "z")}))
	got, err = client.Fetch(t.Context(), []string{"a"})
	require.NoError(t, err)
	require.Equal(t, []float32{0, 0, 1}, got["a"].Embedding)
	require.Equal(t, "memo", got["a"].Metadata["doc_type"])
	require.Equal(t, 3, index.Len())

	var all []string
	token := ""
	for {
		ids, next, listErr := client.ListIDs(t.Context(), token, 2)
		require.NoError(t, listErr)
		all = append(all, ids...)
		if next == "" {
			break
		}
		token = next
	}
	require.Equal(t, []string{"a", "b", "c"}, all)

	require.NoError(t, client.Delete(t.Context(), "", []string{"b", "absent"}))
	require.Equal(t, 2, index.Len())
	require.NoError(t, client.DeleteByDocID(t.Context(), "doc-2"))
	require.Equal(t, []string{"a"}, index.IDs())
}

func TestMemoryIndexRejectsDimensionMismatch(t *testing.T) {
	client, index := memoryClient(3)
	err := client.Upsert(t.Context(), "", []domain.Chunk{memoryChunk("a", "doc", []float32{1, 0}, "report", "")})
	require.ErrorContains(t, err, "dimension")
	require.Zero(t, index.Len())
}

func TestMemoryIndexQueryAppliesFiltersBeforeRanking(t *testing.T) {
	client, _ := memoryClient(2)
	require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{
		memoryChunk("best", "doc-out", []float32{1, 0}, "report", "other"),
		memoryChunk("near", "doc-in", []float32{0.9, 0.1}, "report", "mine"),
		memoryChunk("far", "doc-in", []float32{0, 1}, "report", "mine"),
		memoryChunk("typed", "doc-in", []float32{0.8, 0.2}, "note", "mine"),
	}))
	got, err := client.QueryFiltered(t.Context(), []float32{1, 0}, 2, domain.VectorFilter{DocumentIDs: []string{"doc-in"}, Collection: "mine", DocType: "report"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, domain.ChunkID("near"), got[0].Chunk.ID)
	require.Equal(t, domain.ChunkID("far"), got[1].Chunk.ID)
	require.Greater(t, got[0].Score, got[1].Score)
	for _, sc := range got {
		require.Equal(t, domain.DocumentID("doc-in"), sc.Chunk.DocumentID)
	}
	got, err = client.QueryFiltered(t.Context(), []float32{1, 0}, 10, domain.VectorFilter{DocumentIDs: []string{}})
	require.NoError(t, err)
	require.Empty(t, got, "an empty allowlist matches nothing")
	got, err = client.QueryFiltered(t.Context(), []float32{1, 0}, 1, domain.VectorFilter{})
	require.NoError(t, err)
	require.Equal(t, domain.ChunkID("best"), got[0].Chunk.ID)
}

func TestMemoryIndexFilterHonorsTheExistenceOperator(t *testing.T) {
	tagged, err := structpb.NewStruct(map[string]any{"doc_id": "doc-1", domain.VectorRevisionKey: "revision"})
	require.NoError(t, err)
	untagged, err := structpb.NewStruct(map[string]any{"doc_id": "doc-1"})
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		exists   bool
		metadata *structpb.Struct
		want     bool
	}{
		{"a tagged record satisfies exists", true, tagged, true},
		{"an untagged record fails exists", true, untagged, false},
		{"a tagged record fails not exists", false, tagged, false},
		{"an untagged record satisfies not exists", false, untagged, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter, filterErr := structpb.NewStruct(map[string]any{domain.VectorRevisionKey: map[string]any{"$exists": tc.exists}})
			require.NoError(t, filterErr)
			got, matchErr := matchesFilter(tc.metadata, filter)
			require.NoError(t, matchErr)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMemoryIndexRanksOnlyScopedManifestAndTaggedMatches(t *testing.T) {
	client, _ := memoryClient(2)
	scope, ids := memberScope(2)
	var chunks []domain.Chunk
	for _, tc := range []struct {
		id, doc, tenant, collection, docType, tag string
		manifest                                  bool
	}{
		{"manifest", ids[0], scopeTenant, "archive", "report", "", true},
		{"tagged", ids[1], scopeTenant, "archive", "report", memberIncarnation(1), false},
		{"unlisted", memberID(2), scopeTenant, "archive", "report", "", true},
		{"foreign", ids[0], "foreign-tenant", "archive", "report", "", true},
		{"wrong-collection", ids[0], scopeTenant, "private", "report", "", true},
		{"wrong-type", ids[0], scopeTenant, "archive", "note", "", true},
		{"stale-tag", ids[1], scopeTenant, "archive", "report", memberIncarnation(2), false},
		{"legacy", ids[0], scopeTenant, "archive", "report", "", false},
	} {
		ch := memoryChunk(tc.id, tc.doc, []float32{1, 0}, tc.docType, tc.collection)
		ch.Metadata[domain.VectorTenantKey] = tc.tenant
		if tc.manifest {
			ch.RevisionID = uuid.NewString()
			ch.VectorID = domain.FormatRevisionVectorID(ch.DocumentID, ch.RevisionID, ch.Index)
		}
		if tc.tag != "" {
			ch.Metadata[domain.VectorRevisionKey] = tc.tag
		}
		if tc.id == "manifest" || tc.id == "tagged" {
			ch.Embedding = []float32{1, 1}
		}
		chunks = append(chunks, ch)
	}
	require.NoError(t, client.Upsert(t.Context(), "", chunks))
	driver := NewDriver(client)
	filter := domain.VectorFilter{DocumentIDs: ids, Authorized: scope, Collection: "archive", DocType: "report"}
	check := func(hits []domain.ScoredChunk, err error) {
		t.Helper()
		require.NoError(t, err)
		var got []domain.ChunkID
		for _, hit := range hits {
			got = append(got, hit.Chunk.ID)
		}
		require.ElementsMatch(t, []domain.ChunkID{"manifest", "tagged"}, got)
	}
	query := []float32{1, 0}
	check(driver.QueryFiltered(t.Context(), query, 2, filter))
	check(driver.QueryCandidates(t.Context(), query, 2, filter))
	hits, used, err := driver.QueryFilteredBudgeted(t.Context(), query, 2, filter, 1)
	check(hits, err)
	require.Equal(t, 1, used)
}

func TestMemoryIndexUpdateVectorsByMetadata(t *testing.T) {
	client, index := memoryClient(2)
	require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{
		memoryChunk("a", "DOC-1", []float32{1, 0}, "report", "mine"),
		memoryChunk("b", "doc-1", []float32{0, 1}, "report", "mine"),
		memoryChunk("c", "DOC-2", []float32{1, 1}, "note", "other"),
	}))

	filter, err := structpb.NewStruct(map[string]any{"doc_id": map[string]any{"$in": []any{"DOC-1"}}})
	require.NoError(t, err)
	update, err := structpb.NewStruct(map[string]any{"doc_id": "doc-1"})
	require.NoError(t, err)

	dryRun := true
	res, err := index.UpdateVectorsByMetadata(t.Context(), &pinecone.UpdateVectorsByMetadataRequest{
		Filter: filter, Metadata: update, DryRun: &dryRun,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), res.MatchedRecords)
	fetched, err := client.Fetch(t.Context(), []string{"a", "b", "c"})
	require.NoError(t, err)
	require.Equal(t, domain.DocumentID("DOC-1"), fetched["a"].DocumentID, "dry run leaves metadata unchanged")

	apply := false
	res, err = index.UpdateVectorsByMetadata(t.Context(), &pinecone.UpdateVectorsByMetadataRequest{
		Filter: filter, Metadata: update, DryRun: &apply,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), res.MatchedRecords)
	fetched, err = client.Fetch(t.Context(), []string{"a", "b", "c"})
	require.NoError(t, err)
	require.Equal(t, domain.DocumentID("doc-1"), fetched["a"].DocumentID)
	require.Equal(t, "mine", fetched["a"].Metadata["collection"])
	require.Equal(t, "report", fetched["a"].Metadata["doc_type"])
	require.Equal(t, domain.DocumentID("DOC-2"), fetched["c"].DocumentID)

	res, err = index.UpdateVectorsByMetadata(t.Context(), &pinecone.UpdateVectorsByMetadataRequest{
		Filter: filter, Metadata: update, DryRun: &dryRun,
	})
	require.NoError(t, err)
	require.Zero(t, res.MatchedRecords)

	_, err = index.UpdateVectorsByMetadata(t.Context(), nil)
	require.Error(t, err)
	badFilter, err := structpb.NewStruct(map[string]any{"doc_id": map[string]any{"$regex": ".*"}})
	require.NoError(t, err)
	_, err = index.UpdateVectorsByMetadata(t.Context(), &pinecone.UpdateVectorsByMetadataRequest{Filter: badFilter, Metadata: update})
	require.Error(t, err)
}

func TestMemoryIndexListsOnlyKeysUnderThePrefix(t *testing.T) {
	index := NewMemoryIndex(2)
	for _, id := range []string{"doc-1/rev-1/a", "doc-1/rev-1/b", "doc-1/rev-1/c", "doc-1/rev-2/a", "doc-10/rev-1/a", "bare"} {
		_, err := index.UpsertVectors(t.Context(), []*pinecone.Vector{{Id: id, Values: &[]float32{1, 0}}})
		require.NoError(t, err)
	}
	list := func(prefix string, token *string, limit uint32) ([]string, *string) {
		res, err := index.ListVectors(t.Context(), &pinecone.ListVectorsRequest{Prefix: &prefix, Limit: &limit, PaginationToken: token})
		require.NoError(t, err)
		var ids []string
		for _, id := range res.VectorIds {
			ids = append(ids, *id)
		}
		return ids, res.NextPaginationToken
	}

	var paged []string
	var token *string
	for {
		ids, next := list("doc-1/rev-1/", token, 2)
		paged = append(paged, ids...)
		if next == nil {
			break
		}
		token = next
	}
	require.Equal(t, []string{"doc-1/rev-1/a", "doc-1/rev-1/b", "doc-1/rev-1/c"}, paged)

	ids, _ := list("doc-1/", nil, 100)
	require.Equal(t, []string{"doc-1/rev-1/a", "doc-1/rev-1/b", "doc-1/rev-1/c", "doc-1/rev-2/a"}, ids)
	ids, _ = list("absent/", nil, 100)
	require.Empty(t, ids)
	ids, _ = list("", nil, 100)
	require.Len(t, ids, 6, "an empty prefix lists every key")
}

// A cleanup of one revision, or one attempt of it, never lists or deletes the
// records of another document, revision or attempt that shares the index.
func TestRevisionCleanupLeavesOtherTaggedRecords(t *testing.T) {
	taggedChunk := func(id, doc, revision, attempt string) domain.Chunk {
		chunk := memoryChunk(id, doc, []float32{1, 0}, "report", "x")
		chunk.Metadata[domain.VectorRevisionKey], chunk.Metadata[domain.VectorAttemptKey] = revision, attempt
		return chunk
	}
	seed := func() (*Client, *MemoryIndex) {
		client, index := memoryClient(2)
		require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{
			taggedChunk("a", "doc-1", "rev-1", "att-1"),
			taggedChunk("b", "doc-1", "rev-1", "att-1"),
			taggedChunk("a", "doc-2", "rev-1", "att-1"),
			taggedChunk("a", "doc-1", "rev-2", "att-1"),
			taggedChunk("a", "doc-1", "rev-1", "att-2"),
			taggedChunk("b", "doc-1", "rev-1", "att-2"),
		}))
		return client, index
	}

	client, index := seed()
	require.NoError(t, client.DeleteByAttempt(t.Context(), "doc-1", "rev-1", "att-1"))
	require.Equal(t, []string{"doc-1/rev-1/att-2/a", "doc-1/rev-1/att-2/b", "doc-1/rev-2/att-1/a", "doc-2/rev-1/att-1/a"}, index.IDs())

	client, index = seed()
	require.NoError(t, client.DeleteByRevision(t.Context(), "doc-1", "rev-1"))
	require.ElementsMatch(t, []string{"doc-1/rev-2/att-1/a", "doc-2/rev-1/att-1/a"}, index.IDs())
}
