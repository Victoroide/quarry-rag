package handler

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/stretchr/testify/require"
)

type evidenceQueryVector struct {
	domain.VectorStore
	search func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error)
}

func (s evidenceQueryVector) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.search(ctx, embedding, topK, filter)
}

func TestEvidenceQueryReturnsSameSelectedTextInJSONAndSSE(t *testing.T) {
	const id = "00000000-0000-4000-8000-000000000001"
	const other = "00000000-0000-4000-8000-000000000002"
	prefix := "<b>Fold the paper.</b> "
	text := prefix + strings.Repeat("x", 650)
	searches := 0
	store := evidenceQueryVector{search: func(_ context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
		searches++
		require.Equal(t, []float32{1}, embedding)
		require.Equal(t, 3, topK)
		require.Equal(t, domain.VectorFilter{Collection: "archive", DocType: "guide", DocumentIDs: []string{id}}, filter)
		return []domain.ScoredChunk{
			{Chunk: domain.Chunk{DocumentID: other, Index: 0, Text: "Out of scope.", Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}}, Score: .99},
			{Chunk: domain.Chunk{DocumentID: id, Index: 2, Text: prefix + "\u202e" + strings.Repeat("x", 650), Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}}, Score: .9},
			{Chunk: domain.Chunk{DocumentID: id, Index: 3, Text: "Unselected low score.", Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}}, Score: .1},
		}, nil
	}}
	ret := retriever.NewRetriever(store, &deadlineEmbedder{release: closedQueryChannel()}, .7, 10)
	qh := QueryHandler(ret, nil, streamQueryConfig(), nil, WithReranker(queryReranker(func(context.Context, string, []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		t.Fatal("evidence query called reranker")
		return nil, nil
	})))
	body := `{"query":"How do I fold paper?","answer_mode":"evidence","mode":"global","filter_doc_type":"guide","collection":"archive","max_chunks":3,"allowed_document_ids":["` + id + `"]}`
	req := streamQueryRequest(t.Context(), body)
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()
	qh.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "<b>")
	require.NotContains(t, rec.Body.String(), "Unselected")
	require.NotContains(t, rec.Body.String(), "Out of scope")
	var response QueryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, "evidence_only", response.FinishReason)
	require.Empty(t, response.Answer)
	require.Zero(t, response.TokenUsage)
	require.Nil(t, response.Citations)
	require.Equal(t, []ScoredChunkResponse{{DocID: id, DocType: "guide", ChunkIndex: 2, Score: .9, Text: text, Excerpt: 1}}, response.ScoredChunks)

	rec = httptest.NewRecorder()
	qh.ServeHTTP(rec, streamQueryRequest(t.Context(), body))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "<b>")
	events := recordedEvents(t, rec.Body.String())
	require.Len(t, events, 2)
	require.Equal(t, "context", events[0].name)
	require.Equal(t, "done", events[1].name)
	var chunks []ScoredChunkResponse
	require.NoError(t, json.Unmarshal(events[0].data["scored_chunks"], &chunks))
	require.Equal(t, response.ScoredChunks, chunks)
	require.JSONEq(t, `"evidence_only"`, string(events[1].data["finish_reason"]))
	require.JSONEq(t, `{"input_tokens":0,"output_tokens":0,"total_tokens":0}`, string(events[1].data["token_usage"]))
	require.Equal(t, 2, searches)
}

func TestBothAnswerModesKeepLocalizedNoEvidence(t *testing.T) {
	const id = "00000000-0000-4000-8000-000000000001"
	for _, mode := range []string{"generated", "evidence"} {
		for _, allowlist := range []string{`[]`, `["` + id + `"]`} {
			t.Run(mode+"/"+allowlist, func(t *testing.T) {
				searches := 0
				store := evidenceQueryVector{search: func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
					searches++
					return []domain.ScoredChunk{{Chunk: domain.Chunk{DocumentID: "outside", Text: "Forbidden text."}, Score: .9}}, nil
				}}
				var emb domain.EmbeddingProvider
				if allowlist != "[]" {
					emb = &deadlineEmbedder{release: closedQueryChannel()}
				}
				ret := retriever.NewRetriever(store, emb, .7, 10)
				qh := QueryHandler(ret, nil, streamQueryConfig(), nil)
				body := `{"query":"Archive?","mode":"global","answer_mode":"` + mode + `","allowed_document_ids":` + allowlist + `}`
				for _, accept := range []string{"application/json", "text/event-stream"} {
					rec := httptest.NewRecorder()
					req := streamQueryRequest(t.Context(), body)
					req.Header.Set("Accept", accept)
					req.Header.Set("Accept-Language", "es-MX")
					qh.ServeHTTP(rec, req)
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					require.Equal(t, "es", rec.Header().Get("Content-Language"))
					require.Contains(t, rec.Body.String(), `"finish_reason":"no_evidence"`)
					require.Contains(t, rec.Body.String(), `"scored_chunks":[]`)
					require.Contains(t, rec.Body.String(), `"token_usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}`)
					require.NotContains(t, rec.Body.String(), "Forbidden text")
					if accept == "text/event-stream" {
						events := recordedEvents(t, rec.Body.String())
						require.Len(t, events, 3)
						require.Equal(t, "delta", events[1].name)
					}
				}
				if allowlist == "[]" {
					require.Zero(t, searches)
				} else {
					require.Equal(t, 2, searches)
				}
			})
		}
	}
}

type evidenceFlushWriter struct {
	*httptest.ResponseRecorder
	afterContext func()
}

func (w *evidenceFlushWriter) Flush() {
	w.ResponseRecorder.Flush()
	if w.afterContext != nil {
		afterContext := w.afterContext
		w.afterContext = nil
		afterContext()
	}
}

func TestEvidenceStreamStopsBeforeCompletionOnCancellationOrTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "disconnect"
		if timeout {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cfg := streamQueryConfig()
			if timeout {
				cfg.QueryTimeout = 50 * time.Millisecond
			}
			retrievalCtx := make(chan context.Context, 1)
			store := evidenceQueryVector{search: func(ctx context.Context, _ []float32, _ int, _ domain.VectorFilter) ([]domain.ScoredChunk, error) {
				retrievalCtx <- ctx
				return []domain.ScoredChunk{{Chunk: domain.Chunk{DocumentID: "document", Text: "Archive."}, Score: .9}}, nil
			}}
			ret := retriever.NewRetriever(store, &deadlineEmbedder{release: closedQueryChannel()}, .7, 10)
			w := &evidenceFlushWriter{ResponseRecorder: httptest.NewRecorder(), afterContext: func() {
				if timeout {
					<-(<-retrievalCtx).Done()
				} else {
					cancel()
				}
			}}
			QueryHandler(ret, nil, cfg, nil).ServeHTTP(w, streamQueryRequest(ctx, `{"query":"Archive?","mode":"global","answer_mode":"evidence"}`))
			events := recordedEvents(t, w.Body.String())
			require.Equal(t, "context", events[0].name)
			if timeout {
				require.Len(t, events, 2)
				require.Equal(t, "error", events[1].name)
				require.JSONEq(t, `"timeout"`, string(events[1].data["error"]))
			} else {
				require.Len(t, events, 1)
			}
		})
	}
}

func TestInvalidEvidencePayloadFailsBeforeResponseStarts(t *testing.T) {
	store := evidenceQueryVector{search: func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
		return []domain.ScoredChunk{{Chunk: domain.Chunk{DocumentID: "document", Text: "Archive."}, Score: math.NaN()}}, nil
	}}
	ret := retriever.NewRetriever(store, &deadlineEmbedder{release: closedQueryChannel()}, .7, 10)
	query := QueryHandler(ret, nil, streamQueryConfig(), nil)
	for _, accept := range []string{"application/json", "text/event-stream"} {
		req := streamQueryRequest(t.Context(), `{"query":"Archive?","mode":"global","answer_mode":"evidence"}`)
		req.Header.Set("Accept", accept)
		rec := httptest.NewRecorder()
		query.ServeHTTP(rec, req)
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.Contains(t, rec.Body.String(), `"error":"pipeline"`)
		require.NotContains(t, rec.Body.String(), "Archive.")
	}
}
