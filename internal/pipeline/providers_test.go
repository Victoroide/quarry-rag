package pipeline

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/stretchr/testify/require"
)

func TestOpenAISynthesisPaths(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Synthetic answer [1].\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n")
				} else {
					_, _ = fmt.Fprint(w, `{"choices":[{"index":0,"message":{"content":"Synthetic answer [1]."},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`)
				}
			}))
			defer server.Close()
			cfg := &config.Config{LLMProvider: "openai", OpenAIBaseURL: server.URL, OpenAIChatModel: "chat", SynthesisTimeout: time.Second, SynthesisContextTokens: 2400}
			client, err := llm.NewOpenAIClient(cfg)
			require.NoError(t, err)
			amb := llm.NewAmbassador(cfg)
			amb.SetGenerator(client)
			qc := &QueryContext{OriginalQuery: "synthetic question", IncludeCitations: true, RetrievedChunks: []domain.ScoredChunk{{Chunk: domain.Chunk{Text: "synthetic evidence"}, Score: .9}}}
			var deltas strings.Builder
			if stream {
				qc.Stream = &QueryStream{Context: func() error { return nil }, Delta: func(s string) error { deltas.WriteString(s); return nil }}
			}
			require.NoError(t, NewSynthesisStage(amb, cfg).Run(t.Context(), qc))
			require.Equal(t, "stop", qc.FinishReason)
			require.EqualValues(t, 10, qc.TokenUsage.TotalTokens)
			require.Contains(t, qc.FinalAnswer, "Synthetic answer")
			require.Len(t, qc.Citations, 1)
			if stream {
				require.Equal(t, qc.FinalAnswer, deltas.String())
			}
		})
	}
}
