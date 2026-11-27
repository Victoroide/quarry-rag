package domain

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceLineBoundaryCoordinates(t *testing.T) {
	const text = "a b\n\nc d"
	marks := []SourceLineBoundary{
		{Offset: 1, Kind: SourceLineBreak},
		{Offset: 3, Kind: SourceParagraphBreak},
		{Offset: 6, Kind: SourceLineBreak},
	}
	require.True(t, ValidateSourceLineBoundaries(text, marks))
	for _, tc := range []struct {
		left, right int
		kind        SourceLineBoundaryKind
	}{
		{0, 1, 0}, {1, 2, SourceLineBreak}, {1, 3, SourceLineBreak},
		{1, 4, SourceParagraphBreak}, {3, 4, SourceParagraphBreak},
		{4, 6, 0}, {6, 6, 0}, {7, 5, 0}, {6, 7, SourceLineBreak},
	} {
		kind, found := SourceBoundaryBetween(marks, tc.left, tc.right)
		require.Equal(t, tc.kind, kind)
		require.Equal(t, tc.kind != 0, found)
	}
	require.False(t, ValidateSourceLineBoundaries(text, []SourceLineBoundary{{Offset: 1, Kind: 255}}))
	require.False(t, ValidateSourceLineBoundaries("a  b", []SourceLineBoundary{{Offset: 1, Kind: SourceLineBreak}}))
	require.False(t, ValidateSourceLineBoundaries("a\n\n\n\nb", []SourceLineBoundary{{Offset: 2, Kind: SourceParagraphBreak}}))
	require.True(t, ValidateSourceLineBoundaries("a\n\n\n\nb", []SourceLineBoundary{
		{Offset: 1, Kind: SourceParagraphBreak}, {Offset: 3, Kind: SourceParagraphBreak},
	}))
	for _, ch := range []Chunk{{Text: text}, {Text: text, SourceLineBoundaries: marks}} {
		encoded, err := json.Marshal(ch)
		require.NoError(t, err)
		require.JSONEq(t, `{"id":"","document_id":"","chunk_index":0,"text":"a b\n\nc d","token_count":0}`, string(encoded))
	}
}

func TestSourceLineBoundaryWhitespaceEdges(t *testing.T) {
	for _, tc := range []struct {
		text string
		mark SourceLineBoundary
	}{
		{"a\n\n\n", SourceLineBoundary{Offset: 1, Kind: SourceParagraphBreak}},
		{"a \u2001", SourceLineBoundary{Offset: 1, Kind: SourceLineBreak}},
		{"\t a", SourceLineBoundary{Offset: 1, Kind: SourceLineBreak}},
		{"\u2001\n\n\n\na", SourceLineBoundary{Offset: 5, Kind: SourceParagraphBreak}},
		{"a\n\n\n\n\t\u2001", SourceLineBoundary{Offset: 3, Kind: SourceParagraphBreak}},
	} {
		require.False(t, ValidateSourceLineBoundaries(tc.text, []SourceLineBoundary{tc.mark}), "%q", tc.text)
	}
	require.True(t, ValidateSourceLineBoundaries("\t a\n\n\n\nb c\u2001", []SourceLineBoundary{
		{Offset: 3, Kind: SourceParagraphBreak}, {Offset: 5, Kind: SourceParagraphBreak}, {Offset: 8, Kind: SourceLineBreak},
	}))
}
