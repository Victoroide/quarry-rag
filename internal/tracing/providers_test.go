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
	"go.opentelemetry.io/otel/trace"
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
				cfg := &config.Config{OpenAIBaseURL: server.URL, OpenAIAPIKey: "private-key", OpenAIChatModel: "gpt-4o-2024-08-06", OpenAIEmbeddingModel: "embed", EmbeddingDimensions: 1, EmbedConcurrency: 1, EmbedTimeout: time.Second}
				client, err := llm.NewOpenAIClient(cfg)
				require.NoError(t, err)
				generator := WrapGenerator(client, "openai")
				counter, ok := generator.(llm.OpenAIRequestCounter)
				require.True(t, ok)
				ctx := WithContentCapture(t.Context(), capture)
				request := llm.Request{Model: "gpt-4o-2024-08-06", System: "private-system", Messages: []llm.Message{{Role: "user", Content: []llm.Content{{Text: "private-question"}}}}}
				countRes, err := counter.CountRequestInput(ctx, request)
				require.NoError(t, err)
				require.Positive(t, countRes.InputTokens)
				_, err = counter.CountRequestInput(ctx, request)
				require.NoError(t, err)
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
				require.Len(t, spans, 4)
				for _, span := range spans[:2] {
					require.Equal(t, trace.SpanKindInternal, span.SpanKind())
					attrs := spanAttrs(span)
					require.NotContains(t, attrs, "gen_ai.usage.input_tokens")
					require.EqualValues(t, countRes.InputTokens, attrs["quarry.synthesis.count_input_tokens"].AsInt64())
				}
				for _, span := range spans[2:] {
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
						require.Equal(t, "private-document", spanAttrs(spans[3])["input.value"].AsString())
					})
					t.Run("chat", func(t *testing.T) {
						chat := spanAttrs(spans[2])
						require.Equal(t, "system", chat["gen_ai.prompt.0.role"].AsString())
						require.Equal(t, "private-system", chat["gen_ai.prompt.0.content"].AsString())
						require.Equal(t, "user", chat["gen_ai.prompt.1.role"].AsString())
						require.Equal(t, "private-question", chat["gen_ai.prompt.1.content"].AsString())
						require.Equal(t, "assistant", chat["gen_ai.completion.0.role"].AsString())
						require.Equal(t, "private-answer", chat["gen_ai.completion.0.content"].AsString())
					})
				}
				if stream {
					require.True(t, spanAttrs(spans[2])["gen_ai.request.streaming"].AsBool())
				} else {
					require.EqualValues(t, 7, spanAttrs(spans[2])["gen_ai.usage.input_tokens"].AsInt64())
				}
			})
		}
	}
}
