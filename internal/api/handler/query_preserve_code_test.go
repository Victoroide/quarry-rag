package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/stretchr/testify/require"
)

const preserveCodeAnswer = "The first station is `stations[0]` [1]."

func preserveCodeHandler(answer string) http.HandlerFunc {
	cfg := streamQueryConfig()
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(&citationSupportConverser{answer: answer})
	chunks := []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "chunk", DocumentID: "document", Text: "The first station is open"}, Score: 1}}
	return QueryHandler(retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10), amb, cfg, nil)
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
			preserveCodeHandler(preserveCodeAnswer)(rec, r)
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
	preserveCodeHandler(preserveCodeAnswer)(rec, r)
	requireQueryResponseHeaders(t, rec, http.StatusBadRequest, "application/json")
}

func TestQueryPreserveCodeJSON(t *testing.T) {
	for _, tc := range []struct {
		name, body, answer string
		offset             int
	}{
		{"field omitted", `{"query":"station","mode":"global","include_citations":true}`, "The first station is `stations` [1].", 32},
		{"field false", `{"query":"station","mode":"global","include_citations":true,"preserve_code":false}`, "The first station is `stations` [1].", 32},
		{"field true", `{"query":"station","mode":"global","include_citations":true,"preserve_code":true}`, preserveCodeAnswer, 35},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := streamQueryRequest(t.Context(), tc.body)
			r.Header.Del("Accept")
			rec := httptest.NewRecorder()
			preserveCodeHandler(preserveCodeAnswer)(rec, r)
			require.Equal(t, http.StatusOK, rec.Code)
			var resp struct {
				Answer    string             `json:"answer"`
				Citations []CitationResponse `json:"citations"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Equal(t, tc.answer, resp.Answer)
			require.Len(t, resp.Citations, 1)
			require.Equal(t, tc.offset, resp.Citations[0].Offset)
		})
	}
}

func TestQueryPreserveCodeStream(t *testing.T) {
	const answer = "Use `items[1]` for the second item [1]."
	for _, tc := range []struct {
		name, body string
		offsets    []int
	}{
		{"field omitted", `{"query":"station","mode":"global","include_citations":true}`, []int{10, 35}},
		{"field true", `{"query":"station","mode":"global","include_citations":true,"preserve_code":true}`, []int{35}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := streamQueryRequest(t.Context(), tc.body)
			rec := httptest.NewRecorder()
			preserveCodeHandler(answer)(rec, r)
			require.Equal(t, http.StatusOK, rec.Code)
			events := recordedEvents(t, rec.Body.String())
			require.Equal(t, "done", events[len(events)-1].name)
			require.JSONEq(t, `"`+answer+`"`, string(events[1].data["text"]))
			var citations []CitationResponse
			require.NoError(t, json.Unmarshal(events[len(events)-1].data["citations"], &citations))
			var offsets []int
			for _, c := range citations {
				offsets = append(offsets, c.Offset)
			}
			require.Equal(t, tc.offsets, offsets)
		})
	}
}
