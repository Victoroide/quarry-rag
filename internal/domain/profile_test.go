package domain

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func validProfile() EmbeddingProfile {
	return EmbeddingProfile{
		Provider: EmbeddingProviderBedrock, Model: "amazon.titan-embed-text-v2:0", ModelRevision: "2024-04",
		Dimensions: 1024, Metric: MetricCosine, ExtractionChunkVersion: "chunk-1",
	}
}

func TestEmbeddingProfileValidate(t *testing.T) {
	require.NoError(t, validProfile().Validate())
	for name, mutate := range map[string]func(*EmbeddingProfile){
		"provider":        func(p *EmbeddingProfile) { p.Provider = "offline" },
		"empty model":     func(p *EmbeddingProfile) { p.Model = "" },
		"empty revision":  func(p *EmbeddingProfile) { p.ModelRevision = "" },
		"empty version":   func(p *EmbeddingProfile) { p.ExtractionChunkVersion = "" },
		"zero dimensions": func(p *EmbeddingProfile) { p.Dimensions = 0 },
		"huge dimensions": func(p *EmbeddingProfile) { p.Dimensions = MaxProfileDimensions + 1 },
		"dot product":     func(p *EmbeddingProfile) { p.Metric = "dotproduct" },
		"padded":          func(p *EmbeddingProfile) { p.Model = " model" },
		"control":         func(p *EmbeddingProfile) { p.ModelRevision = "rev\x00" },
		"long":            func(p *EmbeddingProfile) { p.Model = strings.Repeat("m", MaxProfileFieldBytes+1) },
		"invalid utf8":    func(p *EmbeddingProfile) { p.Model = "\xff" },
	} {
		t.Run(name, func(t *testing.T) {
			p := validProfile()
			mutate(&p)
			require.Error(t, p.Validate())
		})
	}
}

func TestEmbeddingProfileIdentity(t *testing.T) {
	base := validProfile()
	require.Equal(t, base.Hash(), validProfile().Hash())
	require.Len(t, base.Hash(), 64)
	// Every field contributes to the hash, and concatenation cannot alias fields.
	seen := map[string]string{base.Hash(): "base"}
	for name, mutate := range map[string]func(*EmbeddingProfile){
		"provider":   func(p *EmbeddingProfile) { p.Provider = EmbeddingProviderOpenAI },
		"model":      func(p *EmbeddingProfile) { p.Model += "x" },
		"revision":   func(p *EmbeddingProfile) { p.ModelRevision += "x" },
		"dimensions": func(p *EmbeddingProfile) { p.Dimensions = 512 },
		"version":    func(p *EmbeddingProfile) { p.ExtractionChunkVersion += "x" },
		"shifted":    func(p *EmbeddingProfile) { p.Model, p.ModelRevision = p.Model+p.ModelRevision[:1], p.ModelRevision[1:] },
	} {
		p := validProfile()
		mutate(&p)
		other, dup := seen[p.Hash()]
		require.False(t, dup, "%s collides with %s", name, other)
		seen[p.Hash()] = name
	}
}

func TestEmbeddingProfileComparison(t *testing.T) {
	a, b := validProfile(), validProfile()
	require.True(t, a.SameEmbeddingSpace(b))
	require.True(t, a.SameTextVersion(b))
	b.ExtractionChunkVersion = "chunk-2"
	require.True(t, a.SameEmbeddingSpace(b), "a text version change keeps the vector space")
	require.False(t, a.SameTextVersion(b))
	for name, mutate := range map[string]func(*EmbeddingProfile){
		"model":      func(p *EmbeddingProfile) { p.Model = "other" },
		"revision":   func(p *EmbeddingProfile) { p.ModelRevision = "2025-01" },
		"dimensions": func(p *EmbeddingProfile) { p.Dimensions = 512 },
		"provider":   func(p *EmbeddingProfile) { p.Provider = EmbeddingProviderOpenAI },
	} {
		t.Run(name, func(t *testing.T) {
			c := validProfile()
			mutate(&c)
			require.False(t, a.SameEmbeddingSpace(c))
		})
	}
	// Equal dimensions never imply the same representation.
	same := validProfile()
	same.Model = "another-1024-dimension-model"
	require.Equal(t, a.Dimensions, same.Dimensions)
	require.False(t, a.SameEmbeddingSpace(same))
}

func TestParseEmbeddingProfile(t *testing.T) {
	encoded, err := json.Marshal(validProfile())
	require.NoError(t, err)
	got, err := ParseEmbeddingProfile(encoded)
	require.NoError(t, err)
	require.Equal(t, validProfile(), got)
	for name, body := range map[string]string{
		"unknown field": `{"provider":"bedrock","model":"m","model_revision":"r","dimensions":8,"metric":"cosine","extraction_chunk_version":"v","extra":1}`,
		"missing field": `{"provider":"bedrock","model":"m","dimensions":8,"metric":"cosine","extraction_chunk_version":"v"}`,
		"duplicate":     `{"provider":"bedrock","provider":"openai","model":"m","model_revision":"r","dimensions":8,"metric":"cosine","extraction_chunk_version":"v"}`,
		"trailing":      string(encoded) + ` {}`,
		"array":         `[]`,
		"null":          `null`,
		"string number": `{"provider":"bedrock","model":"m","model_revision":"r","dimensions":"8","metric":"cosine","extraction_chunk_version":"v"}`,
		"empty":         ``,
		"oversized":     `{"provider":"bedrock","model":"` + strings.Repeat("m", MaxProfileBytes) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, parseErr := ParseEmbeddingProfile([]byte(body))
			require.Error(t, parseErr)
		})
	}
}

func FuzzParseEmbeddingProfile(f *testing.F) {
	encoded, err := json.Marshal(validProfile())
	require.NoError(f, err)
	for _, seed := range []string{string(encoded), `{}`, `null`, `[]`, `{"provider":"openai"}`, string(encoded) + `{}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		profile, parseErr := ParseEmbeddingProfile(body)
		if parseErr != nil {
			return
		}
		require.NoError(t, profile.Validate())
		again, err := json.Marshal(profile)
		require.NoError(t, err)
		reparsed, err := ParseEmbeddingProfile(again)
		require.NoError(t, err)
		require.Equal(t, profile, reparsed)
		require.Equal(t, profile.Hash(), reparsed.Hash())
	})
}
