package pipeline

import (
	"testing"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestStreamCitationsKeepOriginalOffsets(t *testing.T) {
	excerpts := []domain.ScoredChunk{{Chunk: domain.Chunk{DocumentID: "document", Index: 4, Text: "Archive evidence"}}}
	answer := "é Archive [99] evidence [1, 1, 99]. Archive evidence [001]."
	citations := VerifyStreamCitations(answer, excerpts)
	require.Len(t, citations, 2)
	require.Equal(t, utf8.RuneCountInString("é Archive [99] evidence "), citations[0].Offset)
	require.Equal(t, utf8.RuneCountInString("é Archive [99] evidence [1, 1, 99]. Archive evidence "), citations[1].Offset)
	for _, citation := range citations {
		require.Equal(t, 1, citation.Excerpt)
		require.True(t, citation.Verified)
	}
}
