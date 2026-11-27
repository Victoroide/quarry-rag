package chunker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestChunkLineBoundariesWindows(t *testing.T) {
	profile := chunkInputProfile(t, `{"provider":"openai","model":"embed","revision":"r1","max_characters":33}`, embeddinginput.Target{Provider: "openai", Model: "embed"})
	for _, p := range []*embeddinginput.Profile{nil, profile} {
		for _, overlap := range []int{0, 4} {
			c := NewChunker(9, overlap)
			for _, raw := range []string{"", "one\ntwo\n\nthree", strings.Repeat("cafe\u0301。\u0301 repeated\nwords repeated.\n\n", 12), "one\u2001 \n\ttwo three four five six seven eight nine", strings.Repeat("東", 12000) + "\nend"} {
				assertChunkBoundaryParity(t, c, raw, p)
			}
		}
		const raw = "aa\nbb. cc\ndd. ee\nff. gg\nhh. ii\njj. kk\nll. mm\nnn."
		c := NewChunker(12, 6)
		chunks, err := c.ChunkForEmbeddingWithLineBoundaries(t.Context(), "id", raw, p)
		require.NoError(t, err)
		require.Greater(t, len(chunks), 1)
		// The final sentence of the first chunk survives in the next
		// window, with its separator represented in both coordinate spaces.
		previous, next := chunks[0], chunks[1]
		retained := previous.SourceLineBoundaries[len(previous.SourceLineBoundaries)-1]
		start := strings.Index(previous.Text, next.Text[:next.SourceLineBoundaries[0].Offset])
		require.Greater(t, start, 0)
		require.Contains(t, next.SourceLineBoundaries,
			domain.SourceLineBoundary{Offset: retained.Offset - start, Kind: retained.Kind})
	}
	c := NewChunker(9, 0)
	chunks, err := c.ChunkForEmbeddingWithLineBoundaries(t.Context(), "id", "one\u2001 \n\ttwo three four five six seven eight nine", nil)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(chunks[0].Text, "one two"))
	require.Equal(t, []domain.SourceLineBoundary{{Offset: 3, Kind: domain.SourceLineBreak}}, chunks[0].SourceLineBoundaries)
	chunks, err = NewChunker(2, 0).ChunkForEmbeddingWithLineBoundaries(t.Context(), "id", "one\ntwo", nil)
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	for _, ch := range chunks {
		require.Empty(t, ch.SourceLineBoundaries)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	chunks, err = c.ChunkForEmbeddingWithLineBoundaries(ctx, "id", "one\ntwo", profile)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, chunks)
}

func assertChunkBoundaryParity(t *testing.T, c *Chunker, raw string, p *embeddinginput.Profile) {
	t.Helper()
	base, baseErr := c.ChunkForEmbedding(t.Context(), "id", raw, p)
	got, err := c.ChunkForEmbeddingWithLineBoundaries(t.Context(), "id", raw, p)
	if baseErr != nil {
		require.EqualError(t, err, baseErr.Error())
	} else {
		require.NoError(t, err)
	}
	for i := range got {
		require.NotNil(t, got[i].SourceLineBoundaries)
		require.True(t, domain.ValidateSourceLineBoundaries(got[i].Text, got[i].SourceLineBoundaries))
		got[i].SourceLineBoundaries = nil
	}
	require.Equal(t, base, got)
}

func TestChunkLineBoundariesDropOnlyClippedEdgeMarks(t *testing.T) {
	chunk := domain.Chunk{Text: "\n\n\n\na b\n\n\n\nc d \u2001"}
	internal := []domain.SourceLineBoundary{
		{Offset: 5, Kind: domain.SourceLineBreak},
		{Offset: 7, Kind: domain.SourceParagraphBreak},
		{Offset: 9, Kind: domain.SourceParagraphBreak},
		{Offset: 12, Kind: domain.SourceLineBreak},
	}
	marks := []domain.SourceLineBoundary{
		{Offset: 0, Kind: domain.SourceParagraphBreak},
		{Offset: 2, Kind: domain.SourceParagraphBreak},
	}
	marks = append(marks, internal...)
	marks = append(marks, domain.SourceLineBoundary{Offset: 14, Kind: domain.SourceLineBreak})
	attachSourceLineBoundaries(&chunk, marks)
	require.Equal(t, internal, chunk.SourceLineBoundaries)

	chunk = domain.Chunk{Text: "\na\n\n\n"}
	attachSourceLineBoundaries(&chunk, nil)
	require.Nil(t, chunk.SourceLineBoundaries)
	attachSourceLineBoundaries(&chunk, []domain.SourceLineBoundary{{Offset: 2, Kind: domain.SourceParagraphBreak}})
	require.Equal(t, []domain.SourceLineBoundary{}, chunk.SourceLineBoundaries)
}

func TestChunkLineBoundariesExtractedSources(t *testing.T) {
	c := NewChunker(512, 64)
	const raw = "Page 3\r\nOf 48 pallet positions marked, six remained empty."
	var native struct {
		Text string `json:"text"`
	}
	require.NoError(t, json.Unmarshal([]byte(`{"text":"Page 3\r\nOf 48 pallet positions marked, six remained empty."}`), &native))
	inputs := []string{native.Text}
	wide := []byte{0xff, 0xfe}
	for _, unit := range utf16.Encode([]rune(raw)) {
		wide = binary.LittleEndian.AppendUint16(wide, unit)
	}
	for _, upload := range [][]byte{[]byte(raw), wide} {
		ex := extractor.New(extractor.FileTypeText)
		body, err := ex.Extract(t.Context(), bytes.NewReader(upload))
		require.NoError(t, err)
		inputs = append(inputs, body)
	}
	for _, body := range inputs {
		assertChunkBoundaryParity(t, c, body, nil)
		chunks, err := c.ChunkForEmbeddingWithLineBoundaries(t.Context(), "id", body, nil)
		require.NoError(t, err)
		require.Len(t, chunks, 1)
		require.Equal(t, []domain.SourceLineBoundary{{Offset: 6, Kind: domain.SourceLineBreak}}, chunks[0].SourceLineBoundaries)
	}
	for _, body := range []string{"Page 3 Of 48", "docu-\nment"} {
		chunks, err := c.ChunkForEmbeddingWithLineBoundaries(t.Context(), "id", body, nil)
		require.NoError(t, err)
		require.Empty(t, chunks[0].SourceLineBoundaries)
	}
	pdf, err := os.ReadFile("../../test/integration/testdata/mixed-scans.pdf")
	require.NoError(t, err)
	ex := extractor.New(extractor.FileTypePDF)
	text, err := ex.Extract(t.Context(), bytes.NewReader(pdf))
	require.NoError(t, err)
	assertChunkBoundaryParity(t, c, text, nil)
	const first = `BT /F1 12 Tf (First page) Tj ET`
	const second = `BT /F1 12 Tf (Second page) Tj ET`
	text, err = ex.Extract(t.Context(), bytes.NewReader(testutil.PDFWithPageStreams(first, second)))
	require.NoError(t, err)
	assertChunkBoundaryParity(t, c, text, nil)
	pages, err := c.ChunkForEmbeddingWithLineBoundaries(t.Context(), "id", text, nil)
	require.NoError(t, err)
	require.Len(t, pages, 1)
	require.Equal(t, "First page\n\nSecond page", pages[0].Text)
	require.Equal(t, []domain.SourceLineBoundary{{Offset: 10, Kind: domain.SourceParagraphBreak}}, pages[0].SourceLineBoundaries)
	sections := []domain.LayoutSection{
		{Text: "<p>First<br>line</p>", Page: 1},
		{Text: "", Page: 2},
		{Text: "row text", Page: 3, TableID: "table", RowID: "row"},
	}
	for _, profile := range []*embeddinginput.Profile{nil, chunkInputProfile(t, `{"provider":"openai","model":"embed","revision":"r1","max_characters":150}`, embeddinginput.Target{Provider: "openai", Model: "embed"})} {
		base, err := c.ChunkLayout(t.Context(), "id", sections, profile)
		require.NoError(t, err)
		chunks, err := c.ChunkLayoutWithLineBoundaries(t.Context(), "id", sections, profile)
		require.NoError(t, err)
		require.Len(t, chunks, 1)
		require.Equal(t, []domain.SourceLineBoundary{{Offset: 5, Kind: domain.SourceLineBreak}, {Offset: 10, Kind: domain.SourceParagraphBreak}}, chunks[0].SourceLineBoundaries)
		chunks[0].SourceLineBoundaries = nil
		require.Equal(t, base, chunks)
	}
}

func FuzzChunkLineBoundaries(f *testing.F) {
	profile := chunkInputProfile(f, `{"provider":"openai","model":"embed","revision":"r1","max_characters":96}`, embeddinginput.Target{Provider: "openai", Model: "embed"})
	for _, seed := range []string{"", "Page 3\nOf 48", "a\u2029\u2001\n\nb", "<p>a<br>b</p>", "co-\noperate ✓ cafe\u0301", "aaaa。\u0301aaaa"} {
		f.Add(seed, uint8(8), true)
	}
	f.Fuzz(func(t *testing.T, raw string, size uint8, overlap bool) {
		if len(raw) > 4096 {
			t.Skip()
		}
		n := int(size) + 2
		ov := 0
		if overlap {
			ov = n / 2
		}
		c := NewChunker(n, ov)
		assertChunkBoundaryParity(t, c, raw, nil)
		assertChunkBoundaryParity(t, c, raw, profile)
	})
}

func BenchmarkChunkLineBoundaryCapture(b *testing.B) {
	raw := strings.Repeat("repeated paragraph\nwrapped line\n\n", 30000)
	c := NewChunker(512, 256)
	for _, capture := range []bool{false, true} {
		name, chunk := "disabled", c.ChunkForEmbedding
		if capture {
			name, chunk = "enabled", c.ChunkForEmbeddingWithLineBoundaries
		}
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for b.Loop() {
				_, err := chunk(b.Context(), "id", raw, nil)
				require.NoError(b, err)
			}
		})
	}
}
