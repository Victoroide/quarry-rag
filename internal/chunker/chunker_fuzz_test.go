package chunker

import (
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/textutil"
)

func FuzzChunk(f *testing.F) {
	for _, text := range []string{"", "Archive opening hours.\n\nRecords remain available.", "<p>Readable <b>document</b> text.</p>", "世界 🌍 e\u0301 café", "a\x00b\xff", "longwordwithoutspaces"} {
		for _, limits := range [][2]uint16{{0, 0}, {1, 65535}, {512, 64}, {65535, 65535}} {
			f.Add(text, limits[0], limits[1])
		}
	}
	f.Fuzz(func(t *testing.T, text string, size, overlap uint16) {
		if len(text) > 64<<10 {
			t.Skip()
		}
		budget := 2 + int(size)%8191
		c := NewChunker(budget, int(overlap)%budget)
		chunks := c.Chunk(domain.DocumentID("fuzz-document"), text)
		again := c.Chunk(domain.DocumentID("fuzz-document"), text)
		if len(chunks) != len(again) {
			t.Fatal("chunk count is not deterministic")
		}
		want, covered := 0, 0
		for _, r := range textutil.NormalizeForIngest(stripHTML(text)) {
			if !unicode.IsSpace(r) {
				want++
			}
		}
		seen := make(map[domain.ChunkID]bool)
		for i, chunk := range chunks {
			for _, r := range chunk.Text {
				if !unicode.IsSpace(r) {
					covered++
				}
			}
			if chunk.Text == "" || !utf8.ValidString(chunk.Text) || len(chunk.Text) > MaxChunkBytes {
				t.Fatalf("chunk %d is empty, invalid UTF-8 or exceeds the byte limit", i)
			}
			if chunk.TokenCount > budget {
				t.Fatalf("chunk %d exceeds the token budget: %d > %d", i, chunk.TokenCount, budget)
			}
			if chunk.Index != i || seen[chunk.ID] {
				t.Fatalf("chunk %d has an invalid index or duplicate ID", i)
			}
			seen[chunk.ID] = true
			if chunk.ID != again[i].ID || chunk.Text != again[i].Text {
				t.Fatalf("chunk %d is not deterministic", i)
			}
		}
		if covered < want {
			t.Fatalf("chunks hold %d of %d non-space runes", covered, want)
		}
	})
}
