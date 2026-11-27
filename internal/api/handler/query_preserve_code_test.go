package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const preserveCodeAnswer = "The first station is `stations[0]` [1]."

func preserveCodeHandler(answer string, providerErr error) http.HandlerFunc {
	cfg := streamQueryConfig()
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(&preserveCodeConverser{citationSupportConverser: citationSupportConverser{answer: answer}, err: providerErr})
	chunks := []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "chunk", DocumentID: "document", Text: "The first station is open"}, Score: 1}}
	return QueryHandler(retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10), amb, cfg, nil)
}

type preserveCodeConverser struct {
	citationSupportConverser
	err error
}

func (c *preserveCodeConverser) ConverseWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int) (*bedrockruntime.ConverseOutput, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.citationSupportConverser.ConverseWithModelAndSystem(ctx, model, system, messages, temperature, maxTokens)
}

func (c *preserveCodeConverser) ConverseStreamWithModelAndSystem(_ context.Context, _, _ string, _ []brtypes.Message, _ float32, _ int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	// Split fence runs and markers, with valid UTF-8 in each provider delta.
	for _, part := range c.answer {
		if err := delta(string(part)); err != nil {
			return nil, err
		}
	}
	if c.err != nil {
		return nil, c.err
	}
	return deadlineOutput(c.answer, brtypes.StopReasonEndTurn), nil
}

func TestQueryPreserveCodeValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, message string
	}{
		{"without citations", `{"query":"station","preserve_code":true}`, "preserve_code requires include_citations"},
		{"citations disabled explicitly", `{"query":"station","preserve_code":true,"include_citations":false}`, "preserve_code requires include_citations"},
		{"evidence answers", `{"query":"station","preserve_code":true,"include_citations":true,"answer_mode":"evidence"}`, "include_citations requires generated answers"},
		{"not a boolean", `{"query":"station","preserve_code":"yes","include_citations":true}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := streamQueryRequest(t.Context(), tc.body)
			r.Header.Del("Accept")
			rec := httptest.NewRecorder()
			preserveCodeHandler(preserveCodeAnswer, nil)(rec, r)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			var resp APIError
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Equal(t, "validation", resp.Error)
			if tc.message != "" {
				require.Equal(t, tc.message, resp.Message)
			}
		})
	}
	// Streaming requests fail validation before the first event.
	r := streamQueryRequest(t.Context(), `{"query":"station","preserve_code":true}`)
	rec := httptest.NewRecorder()
	preserveCodeHandler(preserveCodeAnswer, nil)(rec, r)
	requireQueryResponseHeaders(t, rec, http.StatusBadRequest, "application/json")
}

func TestQueryPreserveCodeResponses(t *testing.T) {
	const (
		legacyInline = "The first station is `stations` [1]."
		literalCode  = "```\n档案 [01]\n```"
		legacyCode   = "```\n档案 [1]\n```"
	)
	for _, tc := range []struct {
		name, input, legacyJSON                      string
		preservedOffsets, jsonOffsets, streamOffsets []int
	}{
		{"inline cleanup", preserveCodeAnswer, legacyInline, []int{35}, []int{32}, []int{35}},
		{"inline valid marker", "Use `items[1]` for the second item [1].", "", []int{35}, []int{10, 35}, []int{10, 35}},
		{"dedent to sibling", "1. Install:\n    ```bash\n    npm i x\n2. Run:\n```\nrun\n```\nThen restart [1].", "", []int{69}, []int{69}, []int{69}},
		{"dedent after blank", "1. Install:\n    ```bash\n    npm i x\n\n```\nrun\n```\nThen restart [1].", "", []int{62}, []int{62}, []int{62}},
		{"dedent from indented block", "Example:\n\n    ```\n    raw\n\n```\nx\n```\nThen [1].", "", []int{42}, []int{42}, []int{42}},
		{"over-indented top-level closer", "  ```\n  a\n     ```\n  b\n  ```\nThen [1].", "", []int{34}, []int{34}, []int{34}},
		{"thematic break before top-level fence", "- - -\n  ```\nliteral\n```\nThen [1].", "", []int{29}, []int{29}, []int{29}},
		{"ordered paragraph continuation before top-level fence", "Paragraph text\n2. continuation\n   ```\nliteral\n```\nThen [1].", "", []int{55}, []int{55}, []int{55}},
		{"quote before ordered list", "> Quote\n2. Second\n   ```\n   literal [01]\n    ```\nThen [1].", "> Quote\n2. Second\n   ```\n   literal [1]\n    ```\nThen [1].", []int{54}, []int{36, 53}, []int{36, 54}},
		{"lazy quote continuation before ordered list", "> Quote\nLazy continuation\n2) Item\n   ~~~\n   literal [01]\n    ~~~\nThen [1].", "> Quote\nLazy continuation\n2) Item\n   ~~~\n   literal [1]\n    ~~~\nThen [1].", []int{70}, []int{52, 69}, []int{52, 70}},
		{"terminated comment before ordered list", "<!-- comment\n -->\n2. Item\n   ```\n   literal [01]\n    ```\nThen [1].", "<!-- comment\n -->\n2. Item\n   ```\n   literal [1]\n    ```\nThen [1].", []int{62}, []int{44, 61}, []int{44, 62}},
		{"literal marker and utf8", "1. Setup\n    ```\n    raw\n" + literalCode + "\nThen [1].", "1. Setup\n    ```\n    raw\n" + legacyCode + "\nThen [1].", []int{46}, []int{32, 45}, []int{32, 46}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, flag := range []struct {
				name, field string
			}{
				{"omitted", ""},
				{"false", `,"preserve_code":false`},
				{"true", `,"preserve_code":true`},
			} {
				t.Run(flag.name, func(t *testing.T) {
					handler := preserveCodeHandler(tc.input, nil)
					body := `{"query":"station","mode":"global","include_citations":true` + flag.field + `}`
					r := streamQueryRequest(t.Context(), body)
					r.Header.Del("Accept")
					rec := httptest.NewRecorder()
					handler(rec, r)
					require.Equal(t, http.StatusOK, rec.Code)
					var resp struct {
						Answer    string             `json:"answer"`
						Citations []CitationResponse `json:"citations"`
					}
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

					rec = httptest.NewRecorder()
					handler(rec, streamQueryRequest(t.Context(), body))
					require.Equal(t, http.StatusOK, rec.Code)
					events := recordedEvents(t, rec.Body.String())
					require.Equal(t, "context", events[0].name)
					done := events[len(events)-1]
					require.Equal(t, "done", done.name)
					var streamed strings.Builder
					for _, e := range events[1 : len(events)-1] {
						require.Equal(t, "delta", e.name)
						var delta string
						require.NoError(t, json.Unmarshal(e.data["text"], &delta))
						streamed.WriteString(delta)
					}
					require.Equal(t, tc.input, streamed.String())
					var citations []CitationResponse
					require.NoError(t, json.Unmarshal(done.data["citations"], &citations))

					wantAnswer, wantJSON, wantSSE := tc.input, tc.jsonOffsets, tc.streamOffsets
					if flag.name == "true" {
						wantJSON, wantSSE = tc.preservedOffsets, tc.preservedOffsets
					} else if tc.legacyJSON != "" {
						wantAnswer = tc.legacyJSON
					}
					assert.Equal(t, wantAnswer, resp.Answer)
					assert.Equal(t, wantJSON, responseCitationOffsets(resp.Citations), "JSON citation offsets")
					assert.Equal(t, wantSSE, responseCitationOffsets(citations), "SSE citation offsets")
				})
			}
		})
	}
}

func responseCitationOffsets(citations []CitationResponse) []int {
	offsets := make([]int, 0, len(citations))
	for _, c := range citations {
		offsets = append(offsets, c.Offset)
	}
	return offsets
}

func TestQueryPreserveCodeUpstreamFailure(t *testing.T) {
	const detail = "private upstream detail"
	h := preserveCodeHandler("partial", errors.New(detail))
	for _, flag := range []string{`false`, `true`} {
		t.Run(flag, func(t *testing.T) {
			body := `{"query":"station","mode":"global","include_citations":true,"preserve_code":` + flag + `}`
			rec := httptest.NewRecorder()
			r := streamQueryRequest(t.Context(), body)
			r.Header.Del("Accept")
			h(rec, r)
			requireQueryResponseHeaders(t, rec, http.StatusInternalServerError, "application/json")
			require.JSONEq(t, `{"error":"pipeline","message":"query processing failed"}`, rec.Body.String())

			rec = httptest.NewRecorder()
			h(rec, streamQueryRequest(t.Context(), body))
			requireQueryResponseHeaders(t, rec, http.StatusOK, "text/event-stream")
			events := recordedEvents(t, rec.Body.String())
			last := events[len(events)-1]
			require.Equal(t, "error", last.name)
			require.JSONEq(t, `"pipeline"`, string(last.data["error"]))
			require.JSONEq(t, `"query processing failed"`, string(last.data["message"]))
			require.NotContains(t, last.data, "citations")
			require.NotContains(t, rec.Body.String(), detail)
		})
	}
}
