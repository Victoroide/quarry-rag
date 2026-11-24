package memory

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestDelayedWriteAndCleanupKeepReplacementPhysicalVector(t *testing.T) {
	ctx := t.Context()
	store := NewFakeVectorStore()
	chunk := func(revision string, embedding []float32) domain.Chunk {
		metadata := map[string]any{}
		if revision != "" {
			metadata[domain.VectorRevisionKey] = revision
		}
		return domain.Chunk{ID: "same-id", DocumentID: "same-document", Text: "same text", Embedding: embedding, Metadata: metadata}
	}
	require.NoError(t, store.Upsert(ctx, "", []domain.Chunk{chunk("current", []float32{1, 0})}))
	require.NoError(t, store.Upsert(ctx, "", []domain.Chunk{chunk("retired", []float32{0, 1}), chunk("", []float32{0, 1})}))
	hits, err := store.Query(ctx, "", []float32{1, 0}, 3, "")
	require.NoError(t, err)
	require.Len(t, hits, 3)
	require.NoError(t, store.DeleteByRevision(ctx, "same-document", "retired"))
	require.NoError(t, store.DeleteByDocID(ctx, "same-document"))
	hits, err = store.Query(ctx, "", []float32{1, 0}, 3, "")
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, domain.ChunkID("same-id"), hits[0].Chunk.ID)
	require.Equal(t, "current", hits[0].Chunk.Metadata[domain.VectorRevisionKey])
	require.EqualValues(t, 1, hits[0].Score)
}
