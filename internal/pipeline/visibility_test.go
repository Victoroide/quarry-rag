package pipeline

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type visibilityLookup func(context.Context, []string) (map[string]struct{}, error)

func (f visibilityLookup) VisibleDocumentIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	return f(ctx, ids)
}

func visibilityRetriever(t *testing.T) *retriever.Retriever {
	t.Helper()
	vs := memory.NewFakeVectorStore()
	require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
		{ID: "failed", DocumentID: "failed", Text: "failed text", Embedding: []float32{1, 0}},
		{ID: "missing", DocumentID: "missing", Text: "orphan text", Embedding: []float32{1, 1}},
		{ID: "completed", DocumentID: "completed", Text: "completed text", Embedding: []float32{0, 1}},
	}))
	return retriever.NewRetriever(vs, nil, 0.7, 10)
}

func TestRetrievalDropsChunksOfUnavailableDocuments(t *testing.T) {
	for _, tt := range []struct {
		name    string
		allowed []string
		visible map[string]struct{}
		wantIDs []string
	}{
		{"completed only", nil, map[string]struct{}{"completed": {}}, []string{"completed"}},
		{"none visible", nil, map[string]struct{}{}, []string{}},
		{"allowlist still applies", []string{"failed"}, map[string]struct{}{"completed": {}}, []string{}},
		{"score order", nil, map[string]struct{}{"missing": {}, "completed": {}}, []string{"missing", "completed"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			qc := &QueryContext{QueryEmbedding: []float32{1, 0}, RetrievalMode: "GLOBAL", AllowedDocumentIDs: tt.allowed}
			vis := visibilityLookup(func(context.Context, []string) (map[string]struct{}, error) {
				return tt.visible, nil
			})
			require.NoError(t, NewRetrievalStageWithVisibility(visibilityRetriever(t), vis).Run(t.Context(), qc))
			require.Equal(t, tt.wantIDs, docIDs(qc.RetrievedChunks))
			if len(qc.RetrievedChunks) == 0 {
				require.Zero(t, qc.TopScore)
			} else {
				require.Equal(t, qc.RetrievedChunks[0].Score, qc.TopScore)
				require.Less(t, qc.TopScore, 1.0)
			}
		})
	}
}

func TestVisibilityLookupFailureFailsClosed(t *testing.T) {
	lookupErr := errors.New("database unavailable")
	vis := visibilityLookup(func(context.Context, []string) (map[string]struct{}, error) {
		return nil, lookupErr
	})
	qc := &QueryContext{QueryEmbedding: []float32{1, 0}, RetrievalMode: "GLOBAL"}
	err := NewRetrievalStageWithVisibility(visibilityRetriever(t), vis).Run(t.Context(), qc)
	require.ErrorIs(t, err, lookupErr)
	require.ErrorContains(t, err, "retriever: visibility:")
	require.Empty(t, qc.RetrievedChunks)
	require.Zero(t, qc.TopScore)
}

func TestNilVisibilityKeepsChunks(t *testing.T) {
	ret := visibilityRetriever(t)
	for _, stage := range []*RetrievalStage{NewRetrievalStage(ret), NewRetrievalStageWithVisibility(ret, nil)} {
		qc := &QueryContext{QueryEmbedding: []float32{1, 0}, RetrievalMode: "GLOBAL"}
		require.NoError(t, stage.Run(t.Context(), qc))
		require.Equal(t, []string{"failed", "missing", "completed"}, docIDs(qc.RetrievedChunks))
		require.Equal(t, 1.0, qc.TopScore)
	}
}

type snapshotVisibility struct {
	currentChunks
	observe func(context.Context) (string, error)
}

func (v snapshotVisibility) ObserveEvidenceSnapshot(ctx context.Context) (string, error) {
	return v.observe(ctx)
}

type snapshotReadingIndex struct{ refillIndex }

func (snapshotReadingIndex) ObservesVectorIncarnation() bool { return true }

func TestQueryObservesOneSnapshot(t *testing.T) {
	const beforeSearch = "10:11:10"
	historical := refillHit("historical", uuid.NewString(), "historical body", .9)
	tagged := refillHit("tagged", uuid.NewString(), "tagged body", .8)
	tagged.Chunk.Metadata[domain.VectorRevisionKey] = uuid.NewString()
	writeTime := refillHit("write-time", uuid.NewString(), "write-time body", .7)
	writeTime.Chunk.VectorIncarnation = uuid.NewString()
	manifested := refillHit("manifested", uuid.NewString(), "manifested body", .6)
	manifested.Chunk.RevisionID = uuid.NewString()
	observed := refillHit("observed", uuid.NewString(), "observed body", .5)
	observed.ObservedIncarnation = uuid.NewString()
	malformed := refillHit("malformed", "invalid-uuid", "malformed hit", .4)
	hits := []domain.ScoredChunk{historical, tagged, writeTime, manifested, observed, malformed}

	observations, checks := 0, 0
	checksStore := snapshotVisibility{
		observe: func(context.Context) (string, error) {
			observations++
			return beforeSearch, nil
		},
		currentChunks: currentChunks{check: func(candidates []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
			checks++
			require.Len(t, candidates, len(hits)-1)
			want := slices.Clone(hits[:len(hits)-1])
			want[0].ObservedSnapshot = beforeSearch
			require.Equal(t, want, candidates)
			return candidates, nil
		}},
	}
	searches := 0
	index := refillIndex{search: func(hostedSearch) ([]domain.ScoredChunk, int, error) {
		searches++
		require.Equal(t, 1, observations, "the snapshot precedes vector I/O")
		return hits, 1, nil
	}}
	ret := retriever.NewRetriever(index, &scopeCountingEmbedder{}, .7, 10)
	qc := &QueryContext{
		OriginalQuery: "compare the archive and reading room",
		SubQueries:    []string{"archive", "reading room"}, EvidenceVisibility: checksStore,
	}
	require.NoError(t, NewRoutingStage(ret).Run(t.Context(), qc))
	require.Equal(t, string(retriever.RetrievalModeGlobal), qc.RetrievalMode)
	require.NoError(t, NewRetrievalStage(ret).Run(t.Context(), qc))
	require.GreaterOrEqual(t, searches, 2)
	require.GreaterOrEqual(t, checks, 2)
	require.Equal(t, 1, observations)
	require.Len(t, qc.RetrievedChunks, len(hits)-1)

	readsIncarnation := retriever.NewRetriever(snapshotReadingIndex{index}, nil, .7, 10)
	require.NoError(t, qc.observeUntaggedEvidence(t.Context(), readsIncarnation))
	require.Equal(t, 1, observations, "a store that observes its own incarnations skips the snapshot")
	require.Equal(t, hits, qc.matchObservedEvidence(hits), "retrieval clears its snapshot")
}

func TestQueryStopsBeforeSearchWhenSnapshotFails(t *testing.T) {
	snapshotErr := errors.New("snapshot unavailable")
	vis := snapshotVisibility{
		observe: func(context.Context) (string, error) { return "", snapshotErr },
		currentChunks: currentChunks{check: func([]domain.ScoredChunk) ([]domain.ScoredChunk, error) {
			t.Fatal("visibility called after observation failed")
			return nil, nil
		}},
	}
	index := refillIndex{search: func(hostedSearch) ([]domain.ScoredChunk, int, error) {
		t.Fatal("search called after observation failed")
		return nil, 0, nil
	}}
	ret := retriever.NewRetriever(index, &scopeCountingEmbedder{}, .7, 10)
	qc := &QueryContext{OriginalQuery: "archive", EvidenceVisibility: vis}
	require.ErrorIs(t, NewRoutingStage(ret).Run(t.Context(), qc), snapshotErr)
	require.ErrorIs(t, NewRetrievalStage(ret).Run(t.Context(), qc), snapshotErr)
}
