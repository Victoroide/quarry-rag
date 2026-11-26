package chunker

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestChunkLayoutCarriesPageTableAndRowProvenance(t *testing.T) {
	c := NewChunker(14, 2)
	sections := []domain.LayoutSection{
		{Page: 1, Text: "Introduction paragraph with enough prose to stand cleanly in its own window."},
		{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: `Region: EMEA | Revenue: 120`},
		{Page: 2, TableID: "p2-t1", RowID: "p2-t1-r1", Text: `Region: APAC | Revenue: 90`},
		{Page: 2, TableID: "p2-t1", RowID: "p2-t1-r2", Text: `Region: AMER | Revenue: 200`},
	}

	chunks, err := c.ChunkLayout(t.Context(), "doc-a", sections, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(chunks), 2)
	require.Equal(t, []int{1}, chunks[0].Metadata["pages"])

	var sawTable1, sawRow2 bool
	for _, ch := range chunks {
		if tids, ok := ch.Metadata["table_ids"].([]string); ok {
			for _, tid := range tids {
				if tid == "p1-t1" {
					sawTable1 = true
				}
			}
		}
		if rids, ok := ch.Metadata["row_ids"].([]string); ok {
			for _, rid := range rids {
				if rid == "p2-t1-r2" {
					sawRow2 = true
				}
			}
		}
	}
	require.True(t, sawTable1)
	require.True(t, sawRow2)

	// Distinct documents get distinct chunk IDs and never share coordinates.
	other, err := c.ChunkLayout(t.Context(), "doc-b", sections[:1], nil)
	require.NoError(t, err)
	require.NotEqual(t, chunks[0].ID, other[0].ID)
	require.Nil(t, other[0].Metadata["table_ids"])
}

func TestChunkLayoutWithEmbeddingProfile(t *testing.T) {
	profile := testutil.OpenAIProfile(t, "../embeddinginput/testdata/cl100k_base.tiktoken.gz", 16)
	c := NewChunker(10, 2)
	sections := []domain.LayoutSection{
		{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "Header: Alpha | Value: Beta"},
		{Page: 2, Text: "Second page conclusion paragraph with extra descriptive words for separation."},
	}
	chunks, err := c.ChunkLayout(t.Context(), "doc-profiled", sections, profile)
	require.NoError(t, err)
	require.NotEmpty(t, chunks)
	require.Equal(t, []int{1}, chunks[0].Metadata["pages"])
	require.Equal(t, []string{"p1-t1"}, chunks[0].Metadata["table_ids"])
	require.Equal(t, []string{"p1-t1-r1"}, chunks[0].Metadata["row_ids"])
}

func TestAttachLayoutProvenanceToChunksRestoresProvenance(t *testing.T) {
	c := NewChunker(14, 2)
	sections := []domain.LayoutSection{
		{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "Region: EMEA | Revenue: 120"},
		{Page: 2, TableID: "p2-t1", RowID: "p2-t1-r1", Text: "Region: EMEA | Revenue: 120"},
		{Page: 2, TableID: "p2-t1", RowID: "p2-t1-r2", Text: "Closing note on the second page."},
	}
	expected, err := c.ChunkLayout(t.Context(), "doc-restore", sections, nil)
	require.NoError(t, err)
	require.Len(t, expected, 2)

	reloaded := make([]domain.Chunk, len(expected))
	for i, ch := range expected {
		reloaded[i] = domain.Chunk{ID: ch.ID, DocumentID: ch.DocumentID, Index: ch.Index, Text: ch.Text, TokenCount: ch.TokenCount}
	}
	AttachLayoutProvenanceToChunks(reloaded, sections)
	for i := range expected {
		require.Equal(t, expected[i].Metadata["pages"], reloaded[i].Metadata["pages"])
		require.Equal(t, expected[i].Metadata["table_ids"], reloaded[i].Metadata["table_ids"])
		require.Equal(t, expected[i].Metadata["row_ids"], reloaded[i].Metadata["row_ids"])
	}
}
