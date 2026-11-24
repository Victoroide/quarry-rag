package pipeline

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/stretchr/testify/require"
)

type hostedSearch struct {
	filter domain.VectorFilter
	topK   int
	budget int
}

type refillIndex struct {
	domain.VectorStore
	search func(hostedSearch) ([]domain.ScoredChunk, int, error)
}

func (s refillIndex) QueryFiltered(_ context.Context, _ []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	chunks, _, err := s.search(hostedSearch{filter: filter, topK: topK})
	return chunks, err
}

func (s refillIndex) QueryCandidates(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.QueryFiltered(ctx, embedding, topK, filter)
}

func (s refillIndex) QueryFilteredBudgeted(_ context.Context, _ []float32, topK int, filter domain.VectorFilter, budget int) ([]domain.ScoredChunk, int, error) {
	return s.search(hostedSearch{filter: filter, topK: topK, budget: budget})
}

func (refillIndex) VisibilityRefillLimit() int { return 5 }

type currentChunks struct {
	check func([]domain.ScoredChunk) ([]domain.ScoredChunk, error)
}

func (v currentChunks) VisibleEvidence(_ context.Context, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	return v.check(chunks)
}

func (v currentChunks) VisibleEvidenceWithPreviews(ctx context.Context, chunks []domain.ScoredChunk, _ []string) ([]domain.ScoredChunk, map[string]string, error) {
	got, err := v.VisibleEvidence(ctx, chunks)
	return got, nil, err
}

func refillHit(id, doc, text string, score float64) domain.ScoredChunk {
	return domain.ScoredChunk{Chunk: domain.Chunk{
		ID: domain.ChunkID(id), DocumentID: domain.DocumentID(doc), Text: text,
		Metadata: map[string]any{"collection": "archive", "doc_type": "guide"},
	}, Score: score}
}

func TestHostedVisibilityRefillLocalUsesVisibleCountAndOriginalScope(t *testing.T) {
	local := domain.VectorFilter{Collection: "archive", DocType: "guide", DocumentIDs: []string{"local"}}
	scope := domain.VectorFilter{Collection: "archive", DocType: "guide", DocumentIDs: []string{"local", "other"}}
	first := refillHit("z", "local", "current", .8)
	extra := refillHit("a", "other", "current", .8)
	stale := refillHit("z", "local", "superseded", 1)
	stale2 := refillHit("s", "local", "superseded", .9)
	stale3 := refillHit("t", "local", "superseded", .85)
	crossStep := refillHit("x", "other", "current", 1)

	for _, budget := range []int{1, 2} {
		t.Run(strconv.Itoa(budget), func(t *testing.T) {
			var calls []hostedSearch
			index := refillIndex{search: func(call hostedSearch) ([]domain.ScoredChunk, int, error) {
				calls = append(calls, call)
				switch len(calls) {
				case 1:
					return []domain.ScoredChunk{first, stale2, stale3}, 0, nil
				case 2:
					return []domain.ScoredChunk{stale, stale2, first, crossStep}, 1, nil
				case 3:
					return []domain.ScoredChunk{first, extra}, 1, nil
				default:
					t.Fatal("unexpected query")
					return nil, 0, nil
				}
			}}
			vis := currentChunks{check: func(hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
				var visible []domain.ScoredChunk
				for _, hit := range hits {
					if hit.Chunk.Text == "current" {
						visible = append(visible, hit)
					}
				}
				return visible, nil
			}}
			service := metrics.NewService(nil)
			ctx := metrics.WithService(t.Context(), service)
			qc := &QueryContext{RetrievalMode: "LOCAL", LocalDocumentID: "local", QueryEmbedding: []float32{1},
				MaxChunks: 3, AllowedDocumentIDs: scope.DocumentIDs, Collection: scope.Collection, FilterDocType: scope.DocType,
				EvidenceVisibility: vis, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: budget,
			}
			stage := NewRetrievalStage(retriever.NewRetriever(index, nil, .7, 10))
			require.NoError(t, stage.Run(ctx, qc))
			require.Equal(t, "LOCAL", qc.RetrievalMode)
			wantCalls := []hostedSearch{{filter: local, topK: 3}, {filter: local, topK: 5, budget: budget}}
			wantChunks := []domain.ScoredChunk{first}
			if budget == 2 {
				wantCalls = append(wantCalls, hostedSearch{filter: scope, topK: 3, budget: 1})
				wantChunks = []domain.ScoredChunk{extra, first}
			} else {
				require.Contains(t, visibilityMetrics(t, service), `quarry_hosted_visibility_refill_total{result="budget_exhausted"} 1`)
			}
			require.Equal(t, wantCalls, calls)
			require.Equal(t, wantChunks, qc.RetrievedChunks)
			require.Contains(t, visibilityMetrics(t, service), `quarry_hosted_visibility_refill_total{result="underfill"} 1`)
		})
	}
}

func TestHostedVisibilityRefillOptionalFailureAndThreshold(t *testing.T) {
	for _, tt := range []struct {
		name      string
		first     []domain.ScoredChunk
		refill    bool
		wantCount int
	}{
		{name: "failed optional query keeps current hit", first: []domain.ScoredChunk{
			refillHit("first", "doc", "current", .8), refillHit("stale", "doc", "stale", .99),
		}, refill: true, wantCount: 1},
		{name: "relevance threshold alone triggers no refill", first: []domain.ScoredChunk{refillHit("first", "doc", "current", .1)}},
		{name: "repeated stale hits leave no live evidence", first: []domain.ScoredChunk{refillHit("stale", "doc", "stale", .99)}, refill: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			index := refillIndex{search: func(call hostedSearch) ([]domain.ScoredChunk, int, error) {
				if call.budget == 0 {
					return tt.first, 0, nil
				}
				require.True(t, tt.refill, "unexpected refill")
				if tt.wantCount == 0 {
					return tt.first, 1, nil
				}
				return nil, 1, errors.New("optional request failed")
			}}
			checked := 0
			vis := currentChunks{check: func(hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
				checked++
				var visible []domain.ScoredChunk
				for _, hit := range hits {
					if hit.Chunk.Text == "current" {
						visible = append(visible, hit)
					}
				}
				return visible, nil
			}}
			service := metrics.NewService(nil)
			ctx := metrics.WithService(t.Context(), service)
			qc := &QueryContext{RetrievalMode: "GLOBAL", QueryEmbedding: []float32{1}, MaxChunks: 2,
				EvidenceVisibility: vis, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1,
			}
			require.NoError(t, NewRetrievalStage(retriever.NewRetriever(index, nil, .7, 10)).Run(ctx, qc))
			if tt.refill {
				require.Equal(t, 2, checked)
				require.Len(t, qc.RetrievedChunks, tt.wantCount)
				require.Contains(t, visibilityMetrics(t, service), `quarry_hosted_visibility_refill_total{result="underfill"} 1`)
				if tt.wantCount > 0 {
					require.Contains(t, visibilityMetrics(t, service), `quarry_hosted_visibility_refill_total{result="upstream_error"} 1`)
				}
			} else {
				require.Equal(t, 1, checked)
				require.NoError(t, NewSynthesisStage(nil, &config.Config{MinRelevanceScore: .5}).Run(ctx, qc))
				require.Empty(t, qc.RetrievedChunks)
				require.True(t, qc.EarlyExit)
				require.NotContains(t, visibilityMetrics(t, service), `quarry_hosted_visibility_refill_total{`)
			}
		})
	}
}
