package chunker

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/textutil"
)

func TestNewChunkerClampsOverlap(t *testing.T) {
	for _, tc := range []struct {
		size, overlap, wantSize, wantOverlap int
	}{
		{0, 65535, 2, 1},
		{1, -1, 2, 0},
		{2, 1, 2, 1},
		{3, 2, 3, 1},
		{5, 3, 5, 2},
		{512, 256, 512, 256},
		{512, 511, 512, 256},
		{2048, 2047, 2048, 1024},
		{8192, 8191, 8192, 4096},
	} {
		t.Run(fmt.Sprintf("%d/%d", tc.size, tc.overlap), func(t *testing.T) {
			c := NewChunker(tc.size, tc.overlap)
			if c.chunkSize != tc.wantSize || c.overlap != tc.wantOverlap {
				t.Fatalf("window = %d/%d, want %d/%d", c.chunkSize, c.overlap, tc.wantSize, tc.wantOverlap)
			}
		})
	}
}

func TestChunkOutputBoundAtMaximumOverlap(t *testing.T) {
	text := strings.Repeat("ab ", (64<<10)/3+1)[:64<<10]
	for _, size := range []int{2048, 8192} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			chunks := NewChunker(size, size-1).Chunk("document", text)
			assertChunkOutputBound(t, text, chunks)
			// Rounding can underfill a window by one token before at least half is discarded.
			limit := 1 + (2*EstimateTokens(text)+size-3)/(size-2)
			if len(chunks) > limit {
				t.Fatalf("chunk count = %d, limit = %d", len(chunks), limit)
			}
			t.Logf("chunks = %d, aggregate bytes = %d", len(chunks), chunkTextBytes(chunks))
		})
	}
}

func TestChunkOutputBoundAcrossWindows(t *testing.T) {
	for _, size := range []int{2, 3, 4, 5, 8, 31, 64, 511, 512, 2048, 8191, 8192} {
		overlaps := []int{0, 1, size / 4, size / 2, size/2 + 1, size - 1, size, 65535}
		slices.Sort(overlaps)
		for _, overlap := range slices.Compact(overlaps) {
			t.Run(fmt.Sprintf("%d/%d", size, overlap), func(t *testing.T) {
				for _, tc := range []struct{ name, text string }{
					{"words", strings.Repeat("ab ", 2048)},
					{"long word", strings.Repeat("Abc123xy", 1024)},
					{"dense", strings.Repeat("漢字", 2048)},
					{"sentences", strings.Repeat("Archive records remain. Longer preservation notes follow.\n\n", 128)},
					{"mixed estimates", strings.Repeat("a ", 200) + strings.Repeat("😀", 2000) + " " + strings.Repeat("b ", 200)},
				} {
					t.Run(tc.name, func(t *testing.T) {
						assertChunkOutputBound(t, tc.text, NewChunker(size, overlap).Chunk("document", tc.text))
					})
				}
			})
		}
	}
	for _, tc := range []struct {
		name          string
		text          string
		size, overlap int
	}{
		{"byte capped emoji", strings.Repeat("😀", 32768), 8192, 4096},
		{"dense sentence separators", strings.Repeat("a。", 32768), 512, 256},
		{"minimum size clamp", strings.Repeat("ab ", 2048), 0, 65535},
		{"negative overlap clamp", strings.Repeat("ab ", 2048), 512, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertChunkOutputBound(t, tc.text, NewChunker(tc.size, tc.overlap).Chunk("document", tc.text))
		})
	}
}

func TestChunkTransitionsDiscardHalfWindow(t *testing.T) {
	for _, tc := range []struct {
		name          string
		text          string
		size, overlap int
	}{
		{"uneven sentences", "head. " + strings.Repeat("b", 155) + ". " + strings.Repeat("c", 239) + ".", 100, 50},
		{"uneven token density", strings.Repeat("😀", 100) + ". " + strings.Repeat("漢", 99) + "。 " + strings.Repeat("字", 197) + "。", 300, 150},
		{"byte capped sentences", strings.Repeat("漢", 2500) + "。 " + strings.Repeat("😀", 3500) + ". " + strings.Repeat("😁", 3999) + ".", 8192, 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunks := NewChunker(tc.size, tc.overlap).Chunk("document", tc.text)
			if len(chunks) < 2 {
				t.Fatal("document was not split")
			}
			for i := 1; i < len(chunks); i++ {
				start := strings.Index(tc.text, chunks[i-1].Text)
				next := strings.Index(tc.text, chunks[i].Text)
				if start < 0 || next <= start {
					t.Fatalf("chunk %d does not advance through the source", i)
				}
				discarded := tc.text[start:next]
				if 2*EstimateTokens(discarded) < chunks[i-1].TokenCount || 2*len(discarded) < len(chunks[i-1].Text) {
					t.Fatalf("transition %d discards %d tokens/%d bytes from %d tokens/%d bytes", i,
						EstimateTokens(discarded), len(discarded), chunks[i-1].TokenCount, len(chunks[i-1].Text))
				}
			}
		})
	}
}

func assertChunkOutputBound(t *testing.T, text string, chunks []domain.Chunk) {
	t.Helper()
	cleaned := textutil.NormalizeForIngest(stripHTML(text))
	// Each chunk starts at least halfway through the previous one and the
	// last chunk ends with the input, so the total is at most twice the input.
	if total, limit := chunkTextBytes(chunks), 2*len(cleaned); total > limit {
		t.Fatalf("aggregate chunk bytes = %d, linear limit = %d", total, limit)
	}
	// Every chunk consumes a previously unemitted non-space rune.
	if limit := len([]rune(cleaned)); len(chunks) > limit {
		t.Fatalf("chunk count = %d, normalized rune limit = %d", len(chunks), limit)
	}
}

func chunkTextBytes(chunks []domain.Chunk) int {
	total := 0
	for _, chunk := range chunks {
		total += len(chunk.Text)
	}
	return total
}
