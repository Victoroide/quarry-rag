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
