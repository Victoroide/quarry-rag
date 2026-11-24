package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"

	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/stretchr/testify/require"
)

type stageDeadlineVisibility struct {
	blockAt int
	calls   int
	failure error
}

func (v *stageDeadlineVisibility) VisibleEvidenceWithPreviews(ctx context.Context, rows []domain.ScoredChunk, _ []string) ([]domain.ScoredChunk, map[string]string, error) {
	v.calls++
	if v.failure != nil {
		return nil, nil, v.failure
	}
	if v.calls >= v.blockAt {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	return rows, nil, nil
}

func (v *stageDeadlineVisibility) VisibleEvidence(ctx context.Context, rows []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	visible, _, err := v.VisibleEvidenceWithPreviews(ctx, rows, nil)
	return visible, err
}

type stageDeadlineReranker struct{ calls int }

func (r *stageDeadlineReranker) Rerank(_ context.Context, _ string, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	r.calls++
	return chunks, nil
}

func stageDeadlineQuery(visibility *stageDeadlineVisibility) *pipeline.QueryContext {
	return &pipeline.QueryContext{
		OriginalQuery: "When?", RetrievalMode: "GLOBAL", RoutingPolicy: "evidence", EvidenceVisibility: visibility,
		RetrievedChunks: []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "c", DocumentID: "d", Text: "Sunday."}, Score: 1}},
	}
}

func TestQueryStageDeadlineBeforeFrameOrProvider(t *testing.T) {
	const budget = 100 * time.Millisecond
	for _, tc := range []struct {
		name    string
		blockAt int
	}{
		{"context frame", 1},
		{"first delta", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			cfg := &config.Config{SynthesisTimeout: budget, BedrockFallbackModel: "test"}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(llm.NewFakeConverser())
			resp := httptest.NewRecorder()
			qc := stageDeadlineQuery(&stageDeadlineVisibility{blockAt: tc.blockAt})
			stream := newQueryStream(parent, resp, httptest.NewRequestWithContext(parent, http.MethodPost, "/query", nil), qc)
			err := pipeline.NewSynthesisStage(amb, cfg).Run(parent, qc)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NoError(t, parent.Err())
			stream.finish(parent, err)
			if tc.blockAt == 1 {
				require.Equal(t, http.StatusGatewayTimeout, resp.Code)
				require.NotContains(t, resp.Body.String(), "event: context")
			} else {
				require.Equal(t, http.StatusOK, resp.Code)
				require.Contains(t, resp.Body.String(), "event: context")
				require.Contains(t, resp.Body.String(), "event: error")
			}
			require.NotContains(t, resp.Body.String(), "event: delta")
			require.NotContains(t, resp.Body.String(), "event: done")
		})
	}

	reranker := &stageDeadlineReranker{}
	qc := stageDeadlineQuery(&stageDeadlineVisibility{blockAt: 1})
	require.NoError(t, pipeline.NewRerankStage(reranker, budget).Run(t.Context(), qc))
	require.Zero(t, reranker.calls)
	require.Equal(t, "Sunday.", qc.RetrievedChunks[0].Chunk.Text)
	failure := errors.New("visibility unavailable")
	qc.EvidenceVisibility = &stageDeadlineVisibility{failure: failure}
	require.ErrorIs(t, pipeline.NewRerankStage(reranker, budget).Run(t.Context(), qc), failure)
	require.Zero(t, reranker.calls)
}
