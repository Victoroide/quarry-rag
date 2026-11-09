package pipeline

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

func FuzzVerifyCitations(f *testing.F) {
	for _, answer := range []string{"Archive records [1, 2][999].", "档案记录 [2]", "[1[99]]", "[[99]1]", "[9[99]9]", ". [0]", "no markers", "\xff [1]", "[1,\n2]"} {
		f.Add(answer, "Archive records 档案记录", uint8(2))
	}
	marker := regexp.MustCompile(`\[(\d{1,3}(?:\s*,\s*\d{1,3})*)\]`)
	f.Fuzz(func(t *testing.T, answer, text string, count uint8) {
		excerpts := make([]domain.ScoredChunk, int(count)%9)
		for i := range excerpts {
			excerpts[i].Chunk = domain.Chunk{Text: text, Index: i}
		}
		cleaned, citations := VerifyCitations(answer, excerpts)
		runes := []rune(cleaned)
		for _, c := range citations {
			if c.Offset < 0 || c.Offset >= len(runes) || runes[c.Offset] != '[' {
				t.Fatalf("invalid citation offset: %d", c.Offset)
			}
			if c.Excerpt < 1 || c.Excerpt > len(excerpts) {
				t.Fatalf("invalid excerpt: %d", c.Excerpt)
			}
		}
		for _, match := range marker.FindAllStringSubmatch(cleaned, -1) {
			for _, number := range strings.Split(match[1], ",") {
				n, err := strconv.Atoi(strings.TrimSpace(number))
				if err != nil || n < 1 || n > len(excerpts) {
					t.Fatalf("out-of-range marker retained: %q", match[0])
				}
			}
		}
		again, _ := VerifyCitations(cleaned, excerpts)
		if again != cleaned {
			t.Fatal("citation cleanup is not idempotent")
		}
	})
}
