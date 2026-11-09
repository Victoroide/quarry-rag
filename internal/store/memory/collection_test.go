package memory

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestCollectionFilterBeforeTopK(t *testing.T) {
	s := NewFakeVectorStore()
	require.NoError(t, s.Upsert(t.Context(), "", []domain.Chunk{
		{ID: "private", DocumentID: "b", Embedding: []float32{1, 0}, Metadata: map[string]any{"collection": "b", "doc_type": "report"}},
		{ID: "allowed", DocumentID: "a", Embedding: []float32{0, 1}, Metadata: map[string]any{"collection": "a", "doc_type": "report"}},
		{ID: "legacy", DocumentID: "legacy", Embedding: []float32{1, 0}},
	}))
	for _, tc := range []struct {
		filter domain.VectorFilter
		want   []string
	}{
		{domain.VectorFilter{Collection: "a"}, []string{"a"}},
		{domain.VectorFilter{Collection: "a", DocType: "report", DocumentIDs: []string{"a", "b"}}, []string{"a"}},
		{domain.VectorFilter{Collection: "a", DocumentIDs: []string{"b"}}, []string{}},
		{domain.VectorFilter{Collection: "a", DocType: "manual"}, []string{}},
		{domain.VectorFilter{Collection: "missing"}, []string{}},
		{domain.VectorFilter{Collection: "a", DocumentIDs: []string{}}, []string{}},
	} {
		got, err := s.QueryFiltered(t.Context(), []float32{1, 0}, 1, tc.filter)
		require.NoError(t, err)
		ids := make([]string, 0, len(got))
		for _, sc := range got {
			ids = append(ids, string(sc.Chunk.DocumentID))
		}
		require.Equal(t, tc.want, ids)
	}
	got, err := s.QueryFiltered(t.Context(), []float32{1, 0}, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Len(t, got, 3)
}
