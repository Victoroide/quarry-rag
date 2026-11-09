package tracing

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/stretchr/testify/require"
)

func TestOpenAITracingMetadataAndPrivacy(t *testing.T) {
	for _, capture := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("capture=%t/stream=%t", capture, stream), func(t *testing.T) {
				recorder := recordSpans(t)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/embeddings" {
						_, _ = fmt.Fprint(w, `{"data":[{"index":0,"embedding":[0.1]}]}`)
						return
					}
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"private-answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
						return
					}
					_, _ = fmt.Fprint(w, `{"choices":[{"index":0,"message":{"content":"private-answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`)
				}))
				defer server.Close()
				cfg := &config.Config{OpenAIBaseURL: server.URL, OpenAIAPIKey: "private-key", OpenAIChatModel: "chat", OpenAIEmbeddingModel: "embed", EmbeddingDimensions: 1, EmbedConcurrency: 1, EmbedTimeout: time.Second}
				client, err := llm.NewOpenAIClient(cfg)
				require.NoError(t, err)
				generator := WrapGenerator(client, "openai")
				ctx := WithContentCapture(t.Context(), capture)
				request := llm.Request{Model: "chat", System: "private-system", Messages: []llm.Message{{Role: "user", Content: []llm.Content{{Text: "private-question"}}}}}
				if stream {
					_, err = generator.Stream(ctx, request, func(string) error { return nil })
				} else {
					_, err = generator.Generate(ctx, request)
				}
				require.NoError(t, err)
				emb, err := embedder.NewOpenAIEmbedder(cfg)
				require.NoError(t, err)
				_, err = WrapEmbedder(emb, "openai", "embed").Embed(ctx, "private-document")
				require.NoError(t, err)
				spans := recorder.Ended()
				require.Len(t, spans, 2)
				for _, span := range spans {
					attrs := spanAttrs(span)
					require.Equal(t, "openai", attrs["gen_ai.system"].AsString())
					require.Equal(t, "openai", attrs["gen_ai.provider.name"].AsString())
					for _, attr := range span.Attributes() {
						require.False(t, strings.Contains(attr.Value.String(), "private-key"))
						if !capture {
							require.False(t, strings.Contains(attr.Value.String(), "private-"))
						}
					}
				}
				if capture {
					t.Run("embedding", func(t *testing.T) {
						require.Equal(t, "private-document", spanAttrs(spans[1])["input.value"].AsString())
					})
					t.Run("chat", func(t *testing.T) {
						chat := spanAttrs(spans[0])
						require.Equal(t, "system", chat["gen_ai.prompt.0.role"].AsString())
						require.Equal(t, "private-system", chat["gen_ai.prompt.0.content"].AsString())
						require.Equal(t, "user", chat["gen_ai.prompt.1.role"].AsString())
						require.Equal(t, "private-question", chat["gen_ai.prompt.1.content"].AsString())
						require.Equal(t, "assistant", chat["gen_ai.completion.0.role"].AsString())
						require.Equal(t, "private-answer", chat["gen_ai.completion.0.content"].AsString())
					})
				}
				if stream {
					require.True(t, spanAttrs(spans[0])["gen_ai.request.streaming"].AsBool())
				} else {
					require.EqualValues(t, 7, spanAttrs(spans[0])["gen_ai.usage.input_tokens"].AsInt64())
				}
			})
		}
	}
}
