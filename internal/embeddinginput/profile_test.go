package embeddinginput

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const openAIVocabularyDigest = "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7"

func writeProfile(t testing.TB, dir, definition string) string {
	t.Helper()
	path := filepath.Join(dir, "profile.json")
	require.NoError(t, os.WriteFile(path, []byte(definition), 0o600))
	return path
}

func TestCharacterProfileUsesCodePointsOnly(t *testing.T) {
	profile, err := Load(writeProfile(t, t.TempDir(), `{"provider":"bedrock","model":"amazon.titan-embed-text-v2:0","revision":"0","max_characters":3}`),
		Target{Provider: "bedrock", Model: "amazon.titan-embed-text-v2:0"})
	require.NoError(t, err)
	for _, input := range []string{"🙂漢", "🙂漢a", "e\u0301🙂", " \n "} {
		usage, checkErr := profile.Check(t.Context(), input)
		require.NoError(t, checkErr, "%q", input)
		require.Nil(t, usage.ExactTokens)
	}
	usage, err := profile.Check(t.Context(), "🙂漢aé")
	require.ErrorIs(t, err, ErrInputLimit)
	require.Nil(t, usage.ExactTokens)
}

func TestTitanByteOnlyProfileAppliesIntrinsicCharacterCeiling(t *testing.T) {
	profile, err := Load(writeProfile(t, t.TempDir(), `{"provider":"bedrock","model":"amazon.titan-embed-text-v2:0","revision":"0","max_bytes":100000}`),
		Target{Provider: "bedrock", Model: "amazon.titan-embed-text-v2:0"})
	require.NoError(t, err)
	usage, err := profile.Check(t.Context(), strings.Repeat("é", 50_000))
	require.NoError(t, err)
	require.Nil(t, usage.ExactTokens)
	usage, err = profile.Check(t.Context(), strings.Repeat("x", 50_001))
	require.ErrorIs(t, err, ErrInputLimit)
	require.Nil(t, usage.ExactTokens)
}

func openAIVocabulary(t testing.TB) string {
	t.Helper()
	in, err := os.Open("testdata/cl100k_base.tiktoken.gz")
	require.NoError(t, err)
	defer func() { require.NoError(t, in.Close()) }()
	z, err := gzip.NewReader(in)
	require.NoError(t, err)
	defer func() { require.NoError(t, z.Close()) }()
	const maxFixtureBytes = 2 << 20
	data, err := io.ReadAll(io.LimitReader(z, maxFixtureBytes+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(data), maxFixtureBytes)
	path := filepath.Join(t.TempDir(), "ranks.tiktoken")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func exactDefinition(model, digest string, window int) string {
	return fmt.Sprintf(`{"provider":"openai","model":%q,"revision":%q,"max_tokens":%d,"tokenizer":{"type":"tiktoken","version":"cl100k_base","sha256":%q,"artifact":"ranks.tiktoken"}}`,
		model, model, window, digest)
}

func TestOpenAITokenCountFixtures(t *testing.T) {
	vocab := openAIVocabulary(t)
	const model = "text-embedding-3-small"
	profile, err := Load(writeProfile(t, filepath.Dir(vocab), exactDefinition(model, openAIVocabularyDigest, 2)),
		Target{Provider: "openai", Model: model, BaseURL: OpenAIEndpoint})
	require.NoError(t, err)
	// OpenAI tiktoken test_encoding.py and the OpenAI Cookbook counting example.
	for _, tc := range []struct {
		input string
		count int
	}{
		{"rer", 1},
		{"hello world", 2},
		{"today\n ", 3},
		{"today\n \n", 2},
		{"today\n  \n", 2},
		{" \u00850", 4},
		{"お誕生日おめでとう", 9},
		{"2 + 2 = 4", 7},
		{"antidisestablishmentarianism", 6},
		{"👩🏽‍💻", 10},
		{"गाड़ी", 7},
		{"Cafe\u0301", 3},
		{"字漢", 3},
		{"Today is\n \n \tTomorrow", 7},
		{"A\x1cB", 3},
		{"\u0085\t𐐀", 7},
		{strings.Repeat("antidisestablishmentarianism", 333), 1998},
	} {
		usage, err := profile.Check(t.Context(), tc.input)
		require.NotNil(t, usage.ExactTokens)
		require.Equal(t, tc.count, *usage.ExactTokens, "input prefix %q", tc.input[:min(40, len(tc.input))])
		if tc.count > 2 {
			require.ErrorIs(t, err, ErrInputLimit)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestOpenAIModelWindowBoundaries(t *testing.T) {
	vocab := openAIVocabulary(t)
	const model = "text-embedding-3-small"
	profile, err := Load(writeProfile(t, filepath.Dir(vocab), exactDefinition(model, openAIVocabularyDigest, 8192)),
		Target{Provider: "openai", Model: model, BaseURL: OpenAIEndpoint})
	require.NoError(t, err)
	// These repeated inputs were counted with OpenAI tiktoken 0.12.0.
	for _, count := range []int{8191, 8192, 8193} {
		usage, err := profile.Check(t.Context(), strings.Repeat(" hello", count))
		require.NotNil(t, usage.ExactTokens)
		require.Equal(t, count, *usage.ExactTokens)
		if count == 8193 {
			require.ErrorIs(t, err, ErrInputLimit)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestOpenAIUnicodeCountsMatchProvider(t *testing.T) {
	var fixture struct {
		Cases []struct {
			Text   string
			Tokens int
		}
	}
	data, err := os.ReadFile("testdata/cl100k_unicode.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.NotEmpty(t, fixture.Cases)
	vocab := openAIVocabulary(t)
	const model = "text-embedding-3-small"
	profile, err := Load(writeProfile(t, filepath.Dir(vocab), exactDefinition(model, openAIVocabularyDigest, 4)),
		Target{Provider: "openai", Model: model, BaseURL: OpenAIEndpoint})
	require.NoError(t, err)
	for _, tc := range fixture.Cases {
		usage, checkErr := profile.Check(t.Context(), tc.Text)
		require.NotNil(t, usage.ExactTokens)
		require.Equal(t, tc.Tokens, *usage.ExactTokens, "%+q", tc.Text)
		if tc.Tokens > 4 {
			require.ErrorIs(t, checkErr, ErrInputLimit)
		} else {
			require.NoError(t, checkErr)
		}
	}
}

func TestExactProfileIdentityIncludesPretokenizerVersion(t *testing.T) {
	vocab := openAIVocabulary(t)
	const model = "text-embedding-3-small"
	definition := exactDefinition(model, openAIVocabularyDigest, 4)
	profile, err := Load(writeProfile(t, filepath.Dir(vocab), definition),
		Target{Provider: "openai", Model: model, BaseURL: OpenAIEndpoint})
	require.NoError(t, err)
	spec, err := decodeProfile([]byte(definition))
	require.NoError(t, err)
	spec.Tokenizer.Artifact = ""
	canonical, err := json.Marshal(spec)
	require.NoError(t, err)
	canonical = append(canonical, "\x00cl100k-base-pretokenizer-unicode16-v1"...)
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(canonical)), profile.Fingerprint())
}

func TestProfileRejectsUnverifiedOrInvalidSettings(t *testing.T) {
	const model = "text-embedding-3-small"
	target := Target{Provider: "openai", Model: model, BaseURL: OpenAIEndpoint}
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ranks.tiktoken"), []byte("invalid ranks"), 0o600))
	valid := exactDefinition(model, openAIVocabularyDigest, 8192)
	for _, tc := range []struct {
		name, definition string
		target           Target
		want             string
	}{
		{"zero", exactDefinition(model, openAIVocabularyDigest, 0), target, "positive integer"},
		{"negative", exactDefinition(model, openAIVocabularyDigest, -1), target, "positive integer"},
		{"explicit null", strings.Replace(valid, "8192", "null", 1), target, "positive integer"},
		{"unknown setting", strings.Replace(valid, "max_tokens", "max_token", 1), target, "invalid profile JSON"},
		{"unknown model", exactDefinition("custom", openAIVocabularyDigest, 8192), Target{Provider: "openai", Model: "custom", BaseURL: target.BaseURL}, "unverified"},
		{"compatible server", valid, Target{Provider: "openai", Model: model, BaseURL: (&url.URL{Scheme: "https", Host: "localhost", Path: "/v1"}).String()}, "unverified"},
		{"model mismatch", valid, Target{Provider: "openai", Model: "other", BaseURL: target.BaseURL}, "does not match"},
		{"window too large", exactDefinition(model, openAIVocabularyDigest, 8193), target, "8192"},
		{"invalid digest", exactDefinition(model, strings.Repeat("0", 64), 8192), target, "digest"},
		{"invalid artifact", valid, target, "digest"},
		{"token limit without a tokenizer", `{"provider":"openai","model":"text-embedding-3-small","revision":"1","max_tokens":8}`, target, "tokenizer"},
		{"no allowances", `{"provider":"openai","model":"text-embedding-3-small","revision":"1"}`, target, "allowance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeProfile(t, dir, tc.definition), tc.target)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func FuzzDecodeProfile(f *testing.F) {
	f.Add([]byte(`{"provider":"openai","model":"embed","revision":"r1","max_characters":4}`))
	f.Add([]byte(`{"max_characters":null}`))
	f.Add([]byte(`{"max_bytes":-1}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxProfileBytes {
			return
		}
		spec, err := decodeProfile(data)
		if err == nil {
			require.GreaterOrEqual(t, spec.MaxBytes, allowance(0))
			require.GreaterOrEqual(t, spec.MaxCharacters, allowance(0))
			require.GreaterOrEqual(t, spec.MaxTokens, allowance(0))
		}
	})
}
