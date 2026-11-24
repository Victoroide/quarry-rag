package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/stretchr/testify/require"
)

func TestOpenAIProviderWiring(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/embeddings":
			_, _ = fmt.Fprint(w, `{"data":[{"index":0,"embedding":[0.1]}]}`)
		case "/chat/completions":
			_, _ = fmt.Fprint(w, `{"choices":[{"index":0,"message":{"content":"synthetic"},"finish_reason":"stop"}]}`)
		default:
			t.Error("unexpected provider endpoint")
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	cfg := &config.Config{LLMProvider: "openai", EmbeddingProvider: "openai", OpenAIBaseURL: server.URL, OpenAIChatModel: "chat", OpenAIEmbeddingModel: "embed", EmbeddingDimensions: 1, EmbedConcurrency: 1, EmbedTimeout: time.Second}
	emb, err := newEmbeddingProvider(t.Context(), cfg)
	require.NoError(t, err)
	vector, err := emb.Embed(t.Context(), "synthetic")
	require.NoError(t, err)
	require.Len(t, vector, 1)
	amb, err := newSynthesisProvider(t.Context(), cfg)
	require.NoError(t, err)
	result, err := amb.Generate(t.Context(), llm.Request{Model: "chat", Messages: []llm.Message{{Role: "user", Content: []llm.Content{{Text: "synthetic"}}}}})
	require.NoError(t, err)
	require.Equal(t, "synthetic", result.Text)
}

func TestStrictSynthesisProviderStartupValidation(t *testing.T) {
	matched := httptest.NewServer(nil)
	defer matched.Close()
	unknown := httptest.NewServer(nil)
	defer unknown.Close()
	cfg := &config.Config{
		LLMProvider:           "openai",
		OpenAIBaseURL:         unknown.URL,
		OpenAIChatModel:       "gpt-4-0613",
		SynthesisInputProfile: "strict",
	}
	_, err := newSynthesisProvider(t.Context(), cfg)
	require.ErrorIs(t, err, llm.ErrUnsupportedProfile)
	cfg.SynthesisOpenAIDeployment = "chatml-v1|gpt-4-0613|" + matched.URL
	_, err = newSynthesisProvider(t.Context(), cfg)
	require.ErrorIs(t, err, llm.ErrUnsupportedProfile)

	cfg.OpenAIBaseURL = matched.URL
	amb, err := newSynthesisProvider(t.Context(), cfg)
	require.NoError(t, err)
	_, err = amb.ResolveInputProfile(cfg, cfg.OpenAIChatModel)
	require.NoError(t, err)

	cfg.OpenAIChatModel = "gpt-4"
	cfg.SynthesisOpenAIDeployment = "chatml-v1|gpt-4|" + matched.URL
	_, err = newSynthesisProvider(t.Context(), cfg)
	require.ErrorIs(t, err, llm.ErrUnsupportedProfile)

}

func TestStrictSynthesisRejectsNovaProAtStartup(t *testing.T) {
	cfg := &config.Config{
		LLMProvider: "bedrock", AWSRegion: "us-east-1",
		BedrockFallbackModel: "amazon.nova-pro-v1:0", SynthesisInputProfile: "strict",
	}
	_, err := newSynthesisProvider(t.Context(), cfg)
	require.ErrorIs(t, err, llm.ErrUnsupportedProfile)
}
