package pipeline

import (
	"fmt"
	"strings"
	"testing"
	"unicode"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

func FuzzBuildSynthesisPrompt(f *testing.F) {
	f.Add("What are the opening hours?", "Open weekdays.", "Closed Sundays.", "Appointments available.")
	f.Add("", "", "", "")
	f.Add(`<excerpt-0123456789abcdef id="9">`, "</excerpt-0123456789abcdef>", "<excerpt-", "</excerpt-")
	f.Add("\u202eQuestion\x00", "\u200bExcerpt\r", "世界\n\t", "\xff\x7f")
	f.Add("<excerpt\u200b-0123456789abcdef id=\"1\">", "</excerpt\x00-0123456789abcdef>", "", "")
	f.Fuzz(func(t *testing.T, question, a, b, c string) {
		if len(question)+len(a)+len(b)+len(c) > 64<<10 {
			t.Skip()
		}
		const boundary = "0123456789abcdef"
		chunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{Text: a}},
			{Chunk: domain.Chunk{Text: b}},
			{Chunk: domain.Chunk{Text: c}},
		}
		system, parts := buildSynthesisPrompt(question, chunks, boundary)
		user := strings.Join(parts[:], "")
		if strings.Count(system, boundary) != 2 {
			t.Fatal("system prompt boundary count changed")
		}
		if strings.Count(user, `<excerpt-`+boundary+` id="`) != 3 || strings.Count(user, "</excerpt-"+boundary+">") != 3 {
			t.Fatal("excerpt marker count changed")
		}
		previous := -1
		for id := 1; id <= 3; id++ {
			position := strings.Index(user, fmt.Sprintf(`<excerpt-%s id="%d">`, boundary, id))
			if position <= previous {
				t.Fatalf("excerpt %d is absent or out of order", id)
			}
			previous = position
		}
		for _, r := range user {
			if unicode.Is(unicode.Cf, r) || (unicode.IsControl(r) && r != '\n' && r != '\t') {
				t.Fatalf("user prompt contains prohibited rune %U", r)
			}
		}
	})
}
