package chunker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/Victoroide/quarry-rag/internal/textutil"
	"github.com/stretchr/testify/require"
)

func TestProfiledChunksRespectSourceGraphemeBoundaries(t *testing.T) {
	target := embeddinginput.Target{Provider: "openai", Model: "embed"}
	load := func(limit int) *embeddinginput.Profile {
		t.Helper()
		definition := fmt.Sprintf(`{"provider":"openai","model":"embed","revision":"r1","max_characters":%d}`, limit)
		return chunkInputProfile(t, definition, target)
	}
	const raw = "apple。\u0301bananas"
	chunks, err := NewChunker(2, 0).ChunkForEmbedding(t.Context(), "id", raw, load(64))
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Equal(t, "apple。\u0301", chunks[0].Text)
	requireProfiledCoverage(t, raw, []int{0, 1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 16, 17}, chunks, func(_, _ int) bool { return true })
}

func TestProfiledChunksKeepIndicConjunct(t *testing.T) {
	target := embeddinginput.Target{Provider: "openai", Model: "embed"}
	load := func(limit int) *embeddinginput.Profile {
		t.Helper()
		definition := fmt.Sprintf(`{"provider":"openai","model":"embed","revision":"r1","max_characters":%d}`, limit)
		return chunkInputProfile(t, definition, target)
	}
	const conjunct = "क्ष"
	chunks, err := NewChunker(512, 0).ChunkForEmbedding(t.Context(), "id", conjunct, load(3))
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	require.Equal(t, conjunct, chunks[0].Text)
	chunks, err = NewChunker(512, 0).ChunkForEmbedding(t.Context(), "id", conjunct, load(2))
	require.ErrorIs(t, err, embeddinginput.ErrInputLimit)
	require.Empty(t, chunks)
}

type cancelAfterProfileChecks struct {
	context.Context
	cancel    context.CancelFunc
	remaining int
}

func (c *cancelAfterProfileChecks) Err() error {
	c.remaining--
	if c.remaining == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestProfiledFragmentationObservesCancellationDuringLongWord(t *testing.T) {
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &cancelAfterProfileChecks{Context: base, cancel: cancel, remaining: 220}
	p := chunkInputProfile(t, `{"provider":"openai","model":"embed","revision":"r1","max_characters":1000000}`,
		embeddinginput.Target{Provider: "openai", Model: "embed"})
	chunks, err := NewChunker(8192, 4096).ChunkForEmbedding(ctx, "id", strings.Repeat("x", 950_000), p)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, chunks)
}

func TestExactProfileFitsMultiGraphemeToken(t *testing.T) {
	profile := testutil.OpenAIProfile(t, "../embeddinginput/testdata/cl100k_base.tiktoken.gz", 1)
	for _, size := range []int{2, 3, 512} {
		chunks, err := NewChunker(size, size/2).ChunkForEmbedding(t.Context(), "id", "测试测试", profile)
		require.NoError(t, err)
		require.Len(t, chunks, 2)
		require.Equal(t, "测试", chunks[0].Text)
		require.Equal(t, "测试", chunks[1].Text)
	}
	// The reference profile counts ل, لا and اء as one token each,
	// but ء costs two. The longest prefix لا strands the final grapheme.
	chunks, err := NewChunker(512, 0).ChunkForEmbedding(t.Context(), "id", "لاء", profile)
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Equal(t, "ل", chunks[0].Text)
	require.Equal(t, "اء", chunks[1].Text)
}

func chunkInputProfile(t testing.TB, definition string, target embeddinginput.Target) *embeddinginput.Profile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "embedding.json")
	require.NoError(t, os.WriteFile(path, []byte(definition), 0o600))
	profile, err := embeddinginput.Load(path, target)
	require.NoError(t, err)
	return profile
}

func TestChunkEmbeddingProfileBoundaries(t *testing.T) {
	c := NewChunker(512, 64)
	target := embeddinginput.Target{Provider: "openai", Model: "embed"}
	for _, tc := range []struct {
		name, limits, raw string
		want              []string
	}{
		{"multilingual code points", `"max_characters":3`, "🙂漢字กขคεφγ", []string{"🙂漢字", "กขค", "εφγ"}},
		{"combining marks", `"max_characters":2`, "x\u0301y\u0301z\u0301", []string{"x\u0301", "ýź"}},
		{"byte and character limits", `"max_characters":4,"max_bytes":4`, "🙂漢字é", []string{"🙂", "漢", "字", "é"}},
		{"emoji flags", `"max_characters":2`, "🇧🇴🇫🇷", []string{"🇧🇴", "🇫🇷"}},
		{"word boundary", `"max_characters":8`, "archive notes", []string{"archive", "notes"}},
		{"separators", `"max_characters":1`, " \t\n\n▪▪ ", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile := chunkInputProfile(t, `{"provider":"openai","model":"embed","revision":"r1",`+tc.limits+`}`, target)
			chunks, err := c.ChunkForEmbedding(t.Context(), "document", tc.raw, profile)
			require.NoError(t, err)
			require.Len(t, chunks, len(tc.want))
			for i, ch := range chunks {
				require.Equal(t, tc.want[i], strings.TrimSpace(ch.Text))
				require.Equal(t, i, ch.Index)
				require.Equal(t, domain.DocumentID("document"), ch.DocumentID)
				require.NotEqual(t, chunkID("document", i), string(ch.ID))
				_, err := profile.Check(t.Context(), ch.Text)
				require.NoError(t, err)
			}
		})
	}
}

func TestChunkEmbeddingProfilePreservesBudgetsAndVersion(t *testing.T) {
	c := NewChunker(8, 4)
	target := embeddinginput.Target{Provider: "openai", Model: "embed"}
	const text = "A b. C d. E f. Oak profiles. Pine copies."
	load := func(revision string, limit int) *embeddinginput.Profile {
		definition := fmt.Sprintf(`{"provider":"openai","model":"embed","revision":%q,"max_characters":%d}`, revision, limit)
		return chunkInputProfile(t, definition, target)
	}
	p := load("one", 50000)
	initial, err := c.ChunkForEmbedding(t.Context(), "document", text, p)
	require.NoError(t, err)
	again, err := c.ChunkForEmbedding(t.Context(), "document", text, p)
	require.NoError(t, err)
	require.Equal(t, initial, again)
	changed, err := c.ChunkForEmbedding(t.Context(), "document", text, load("two", 50000))
	require.NoError(t, err)
	require.NotEqual(t, initial[0].ID, changed[0].ID)
	legacy := c.Chunk("document", text)
	require.Len(t, initial, len(legacy))
	for i, ch := range initial {
		require.Equal(t, legacy[i].Text, ch.Text)
	}
	require.Len(t, initial, 2)
	require.Contains(t, initial[0].Text, "E f.")
	require.Contains(t, initial[1].Text, "E f.")

	limited, err := c.ChunkForEmbedding(t.Context(), "document", text, load("one", 20))
	require.NoError(t, err)
	require.Len(t, limited, 3)
	require.Equal(t, text, limited[0].Text+" "+limited[1].Text+" "+limited[2].Text)
	assertChunkOutputBound(t, text, limited)

	oversized := strings.Repeat("🙂", MaxChunkBytes/2)
	byteCapped, err := NewChunker(8192, 4096).ChunkForEmbedding(t.Context(), "document", oversized, p)
	require.NoError(t, err)
	require.Greater(t, len(byteCapped), 1)
	for _, ch := range byteCapped {
		require.LessOrEqual(t, len(ch.Text), MaxChunkBytes)
	}
	assertChunkOutputBound(t, oversized, byteCapped)
}

func TestChunkEmbeddingProfileFailsWithNoPartialResult(t *testing.T) {
	target := embeddinginput.Target{Provider: "openai", Model: "embed"}
	profile := chunkInputProfile(t, `{"provider":"openai","model":"embed","revision":"r1","max_characters":1}`, target)
	c := NewChunker(512, 64)
	for _, tc := range []struct {
		text string
		want error
	}{
		{"abc🇧🇴", embeddinginput.ErrInputLimit},
		{strings.Repeat("a", maxProfileChunks+1), errEmbeddingChunkCount},
	} {
		chunks, err := c.ChunkForEmbedding(t.Context(), "document", tc.text, profile)
		require.ErrorIs(t, err, tc.want)
		require.Empty(t, chunks)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	chunks, err := c.ChunkForEmbedding(ctx, "document", "archive", profile)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, chunks)
}

func FuzzChunkEmbeddingProfile(f *testing.F) {
	var exact, character [4]*embeddinginput.Profile
	for i := range exact {
		exact[i] = testutil.OpenAIProfile(f, "../embeddinginput/testdata/cl100k_base.tiktoken.gz", i+1)
		definition := fmt.Sprintf(`{"provider":"openai","model":"embed","revision":"r1","max_characters":%d}`, i+3)
		character[i] = chunkInputProfile(f, definition, embeddinginput.Target{Provider: "openai", Model: "embed"})
	}
	var fixture struct {
		ByStart [2][65]int `json:"by_start"`
	}
	data, err := os.ReadFile("../embeddinginput/testdata/cl100k_test_phrase.json")
	require.NoError(f, err)
	require.NoError(f, json.Unmarshal(data, &fixture))
	require.Equal(f, 1, fixture.ByStart[0][2])
	require.Equal(f, 2, fixture.ByStart[0][1])
	f.Add("\x00")
	f.Add("archive")
	f.Add("🇧🇴क्षx\u0301")
	f.Fuzz(func(t *testing.T, seed string) {
		if seed == "" {
			seed = "\x00"
		}
		seed = seed[:min(32, len(seed))]
		variant := int(seed[0]) % len(exact)
		size := 2 + int(seed[len(seed)-1])%11
		overlap := int(seed[0]) % (size/2 + 1)
		c := NewChunker(size, overlap)

		// Each 测试 pair is a provider-counted single token and fits the
		// estimated budget. Count every possible emitted substring using the
		// independent reference table, including substrings that start at 试.
		text := strings.Repeat("测试", len(seed))
		offsets := make([]int, 2*len(seed)+1)
		for j := range offsets {
			offsets[j] = j * len("测")
		}
		chunks, err := c.ChunkForEmbedding(t.Context(), "fuzz-exact", text, exact[variant])
		require.NoError(t, err)
		requireProfiledCoverage(t, text, offsets, chunks, func(start, end int) bool {
			return fixture.ByStart[start%2][end-start] <= variant+1
		})

		// These are complete source graphemes of at most three code points.
		// A text formed from them is feasible even at the smallest limit.
		clusters := []string{"x\u0301", "क्ष", "🇧🇴", "ก้", "。\u0301", "a", "🐱", "字"}
		var joined strings.Builder
		offsets = []int{0}
		for i := range len(seed) {
			if i > 0 && seed[i]&8 != 0 {
				joined.WriteByte(' ')
				offsets = append(offsets, joined.Len())
			}
			joined.WriteString(clusters[int(seed[i])%len(clusters)])
			offsets = append(offsets, joined.Len())
		}
		text = joined.String()
		require.Equal(t, text, textutil.NormalizeForIngest(text))
		chunks, err = c.ChunkForEmbedding(t.Context(), "fuzz-characters", text, character[variant])
		require.NoError(t, err)
		requireProfiledCoverage(t, text, offsets, chunks, func(start, end int) bool {
			return utf8.RuneCountInString(text[offsets[start]:offsets[end]]) <= variant+3
		})
	})
}

func requireProfiledCoverage(t *testing.T, source string, offsets []int, chunks []domain.Chunk, withinProfile func(int, int) bool) {
	t.Helper()
	frontiers := map[int]bool{0: true}
	for i, ch := range chunks {
		require.Equal(t, i, ch.Index)
		require.True(t, utf8.ValidString(ch.Text))
		require.NotEmpty(t, strings.TrimSpace(ch.Text))
		require.LessOrEqual(t, len(ch.Text), MaxChunkBytes)
		next := map[int]bool{}
		for start, offset := range offsets {
			for end := start + 1; end < len(offsets) && offsets[end]-offset <= len(ch.Text); end++ {
				if source[offset:offsets[end]] != ch.Text || !withinProfile(start, end) {
					continue
				}
				for prev := range frontiers {
					if offsets[end] > prev && (offset <= prev || strings.TrimSpace(source[prev:offset]) == "") {
						next[offsets[end]] = true
					}
				}
			}
		}
		require.NotEmpty(t, next, "no feasible source grapheme span for %q in %q", ch.Text, source)
		frontiers = next
	}
	require.True(t, frontiers[len(source)], "chunks do not cover %q", source)
}
