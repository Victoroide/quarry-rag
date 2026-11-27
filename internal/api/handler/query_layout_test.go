package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/require"
)

func TestLayoutQueryResponseOmitsAbsentProvenance(t *testing.T) {
	const (
		docWithLayout  = "00000000-0000-4000-8000-000000000011"
		docPlainNull   = "00000000-0000-4000-8000-000000000012"
		docEmptySlices = "00000000-0000-4000-8000-000000000013"
	)

	store := evidenceQueryVector{search: func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
		return []domain.ScoredChunk{
			{
				Chunk: domain.Chunk{
					ID:         "c1",
					DocumentID: docWithLayout,
					Index:      0,
					Text:       "Region: EMEA | Revenue: 120",
					Metadata: map[string]any{
						"doc_type":  "report",
						"pages":     []int{2, 5},
						"table_ids": []string{"p2-t1"},
						"row_ids":   []string{"p2-t1-r3"},
					},
				},
				Score: 0.95,
			},
			{
				Chunk: domain.Chunk{
					ID:         "c2",
					DocumentID: docPlainNull,
					Index:      0,
					Text:       "Plain legacy PDF text without layout provenance.",
					Metadata: map[string]any{
						"doc_type":  "report",
						"pages":     nil,
						"table_ids": nil,
						"row_ids":   nil,
					},
				},
				Score: 0.90,
			},
			{
				Chunk: domain.Chunk{
					ID:         "c3",
					DocumentID: docEmptySlices,
					Index:      0,
					Text:       "Empty layout arrays must omit JSON keys.",
					Metadata: map[string]any{
						"doc_type":  "report",
						"pages":     []int{},
						"table_ids": []string{},
						"row_ids":   []any{},
					},
				},
				Score: 0.85,
			},
		}, nil
	}}

	cfg := streamQueryConfig()
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(&queryStreamConverser{call: func(_ context.Context, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
		if fn != nil {
			if err := fn("Synthesized answer."); err != nil {
				return nil, err
			}
		}
		return &bedrockruntime.ConverseOutput{
			Output:     &brtypes.ConverseOutputMemberMessage{Value: brtypes.Message{Role: brtypes.ConversationRoleAssistant, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "Synthesized answer."}}}},
			StopReason: brtypes.StopReasonEndTurn,
			Usage:      &brtypes.TokenUsage{},
		}, nil
	}})

	ret := retriever.NewRetriever(store, &deadlineEmbedder{release: closedQueryChannel()}, 0.7, 10)
	qh := QueryHandler(ret, amb, cfg, nil)

	assertScoredChunksRaw := func(t *testing.T, raw json.RawMessage) {
		t.Helper()
		var items []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &items))
		require.Len(t, items, 3)

		require.JSONEq(t, "[2,5]", string(items[0]["pages"]))
		require.JSONEq(t, `["p2-t1"]`, string(items[0]["table_ids"]))
		require.JSONEq(t, `["p2-t1-r3"]`, string(items[0]["row_ids"]))

		for _, idx := range []int{1, 2} {
			require.NotContains(t, items[idx], "pages")
			require.NotContains(t, items[idx], "table_ids")
			require.NotContains(t, items[idx], "row_ids")
		}
	}

	for _, answerMode := range []string{"generated", "evidence"} {
		body := `{"query":"Revenue?","mode":"global","answer_mode":"` + answerMode + `","max_chunks":3}`

		t.Run(answerMode+"/json", func(t *testing.T) {
			req := streamQueryRequest(t.Context(), body)
			req.Header.Set("Accept", "application/json")
			rec := httptest.NewRecorder()
			qh.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
			assertScoredChunksRaw(t, raw["scored_chunks"])
		})

		t.Run(answerMode+"/sse", func(t *testing.T) {
			req := streamQueryRequest(t.Context(), body)
			rec := httptest.NewRecorder()
			qh.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			events := recordedEvents(t, rec.Body.String())
			require.GreaterOrEqual(t, len(events), 2)
			require.Equal(t, "context", events[0].name)
			assertScoredChunksRaw(t, events[0].data["scored_chunks"])
			last := events[len(events)-1]
			require.Equal(t, "done", last.name)
			if answerMode == "evidence" {
				require.JSONEq(t, `"evidence_only"`, string(last.data["finish_reason"]))
			} else {
				require.JSONEq(t, `"stop"`, string(last.data["finish_reason"]))
			}
		})
	}
}
