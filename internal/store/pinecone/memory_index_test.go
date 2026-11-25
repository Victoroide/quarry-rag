package pinecone

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func memoryClient(dims int) (*Client, *MemoryIndex) {
	index := NewMemoryIndex(dims)
	return NewClientForIndex(index, &config.Config{PineconeUpsertBatchSize: 100, PineconeTimeout: 1e9, PineconeMaxAttempts: 1, EmbeddingDimensions: dims}), index
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
