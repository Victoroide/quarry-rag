package pipeline

import (
	"math"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
)

func FuzzTrimChunksForSynthesis(f *testing.F) {
	f.Add(256, 0.55, true, "Lead policy.\u200b", "Lead policy.\x00", "Distinct exception.", "東京営業時間", 0.95, 0.85, 0.50)
	f.Add(256, 0.0, true, "\x00\u200b \t", "x", "日本語テスト。続き。", "العربية", 0.0, 0.0, -0.5)
	f.Add(1, 1.0, true, strings.Repeat("界", 200), strings.Repeat("a", 500), "tiny", "end", 1.0, 1.0, 1.0)
	f.Add(300, 0.55, false, "First.\nSecond.\nThird.", "Duplicate.", "Short.", "Tail.", 0.9, 0.8, 0.7)

	f.Fuzz(func(t *testing.T, budget int, floor float64, diverse bool, a, b, c, d string, s1, s2, s3 float64) {
		if len(a)+len(b)+len(c)+len(d) > 32<<10 {
			t.Skip()
		}
		if math.IsNaN(floor) || math.IsInf(floor, 0) || floor < 0 || floor > 1 {
			floor = 0.55
		}
		if budget > 100000 {
			budget = 100000
		}
		policy := config.SynthesisContextPolicyRanked
		if diverse {
			policy = config.SynthesisContextPolicyDiverse
		}
		in := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "c1", DocumentID: "doc-a", Index: 0, Text: a}, Score: s1},
			{Chunk: domain.Chunk{ID: "c2", DocumentID: "doc-a", Index: 1, Text: b}, Score: s2},
			{Chunk: domain.Chunk{ID: "c3", DocumentID: "doc-b", Index: 0, Text: c}, Score: s3},
			{Chunk: domain.Chunk{ID: "c4", DocumentID: "doc-c", Index: 2, Text: d}, Score: s1},
		}
		out1, trunc1, err1 := trimChunksForSynthesis(t.Context(), in, budget, floor, policy)
		if err1 != nil {
			t.Fatalf("unexpected error: %v", err1)
		}
		out2, trunc2, err2 := trimChunksForSynthesis(t.Context(), in, budget, floor, policy)
		if err2 != nil {
			t.Fatalf("unexpected second error: %v", err2)
		}
		if len(out1) != len(trunc1) || len(out1) != len(out2) || len(trunc1) != len(trunc2) {
			t.Fatalf("non-deterministic output lengths")
		}
		effBudget := budget
		if effBudget <= 0 {
			effBudget = defaultSynthesisContextBudget
		}
		maxExcerpts := min(maxSynthesisExcerpts, max(1, effBudget))
		maxBytes := (effBudget*4 + maxExcerpts*3) * utf8.UTFMax
		if len(out1) > maxExcerpts {
			t.Fatalf("selected %d chunks exceeds cap %d", len(out1), maxExcerpts)
		}

		totalTokens, totalBytes := 0, 0
		for i, sc := range out1 {
			if sc.Chunk.Text != out2[i].Chunk.Text || sc.Chunk.ID != out2[i].Chunk.ID || trunc1[i] != trunc2[i] {
				t.Fatalf("non-deterministic selection at index %d", i)
			}
			if !utf8.ValidString(sc.Chunk.Text) {
				t.Fatalf("selected excerpt %d has invalid UTF-8", i)
			}
			if strings.TrimSpace(sc.Chunk.Text) == "" {
				t.Fatalf("selected empty/whitespace excerpt %d", i)
			}
			for _, r := range sc.Chunk.Text {
				if unicode.Is(unicode.Cf, r) || (unicode.IsControl(r) && r != '\n' && r != '\t') {
					t.Fatalf("selected excerpt %d contains prohibited rune %U", i, r)
				}
			}
			totalTokens += llm.EstimateTokens(sc.Chunk.Text)
			totalBytes += len(sc.Chunk.Text)
		}
		if totalTokens > effBudget {
			t.Fatalf("total tokens %d exceeds budget %d", totalTokens, effBudget)
		}
		if totalBytes > maxBytes {
			t.Fatalf("total bytes %d exceeds cap %d", totalBytes, maxBytes)
		}
	})
}
