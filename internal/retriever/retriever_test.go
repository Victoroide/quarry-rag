package retriever

import (
	"context"
	"errors"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
)

type localScopedStore struct {
	*memory.FakeVectorStore
	filters []domain.VectorFilter
}

func (s *localScopedStore) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.filters = append(s.filters, filter)
	return s.FakeVectorStore.QueryFiltered(ctx, embedding, topK, filter)
}

func TestLocalRetrievalStaysWithinAllowlist(t *testing.T) {
	for _, tt := range []struct {
		name, target string
		allowed      []string
		calls        int
	}{
		{name: "authorized target", target: "allowed", allowed: []string{"allowed", "other"}, calls: 2},
		{name: "unauthorized target", target: "private", allowed: []string{"allowed", "other"}, calls: 1},
		{name: "empty allowlist", target: "private", allowed: []string{}, calls: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := &localScopedStore{FakeVectorStore: memory.NewFakeVectorStore()}
			require.NoError(t, store.Upsert(t.Context(), "", []domain.Chunk{
				{ID: "allowed", DocumentID: "allowed", Embedding: []float32{.8, .6}, Metadata: map[string]any{"doc_type": "report"}},
				{ID: "other", DocumentID: "other", Embedding: []float32{.9, .1}, Metadata: map[string]any{"doc_type": "report"}},
				{ID: "private", DocumentID: "private", Embedding: []float32{1, 0}, Metadata: map[string]any{"doc_type": "report"}},
				{ID: "wrong-type", DocumentID: "allowed", Embedding: []float32{1, 0}, Metadata: map[string]any{"doc_type": "note"}},
			}))
			filter := domain.VectorFilter{DocType: "report", DocumentIDs: tt.allowed}
			result, err := NewRetriever(store, nil, .7, 10).RetrieveWithEmbedding(t.Context(), []float32{1, 0}, RetrievalModeLocal, tt.target, 10, filter)
			require.NoError(t, err)
			require.Len(t, store.filters, tt.calls)
			if tt.calls == 0 {
				require.Empty(t, result.Chunks)
				return
			}
			require.Len(t, result.Chunks, 2)
			for _, sc := range result.Chunks {
				require.Contains(t, tt.allowed, string(sc.Chunk.DocumentID))
				require.Equal(t, "report", sc.Chunk.Metadata["doc_type"])
			}
			require.Equal(t, filter, store.filters[len(store.filters)-1])
			require.Equal(t, []string{"allowed", "other"}, tt.allowed)
		})
	}
}

type localSequenceStore struct {
	domain.VectorStore
	results [][]domain.ScoredChunk
	filters []domain.VectorFilter
	err     error
}

func (s *localSequenceStore) QueryFiltered(_ context.Context, _ []float32, _ int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.filters = append(s.filters, filter)
	if len(s.filters) == 2 && s.err != nil {
		return nil, s.err
	}
	return s.results[len(s.filters)-1], nil
}

func TestLocalTopUpSortsDeduplicatesAndTrims(t *testing.T) {
	chunk := func(id string, score float64) domain.ScoredChunk {
		return domain.ScoredChunk{Chunk: domain.Chunk{ID: domain.ChunkID(id)}, Score: score}
	}
	store := &localSequenceStore{results: [][]domain.ScoredChunk{
		{chunk("local", .6)},
		{chunk("last", .3), chunk("local", .8), chunk("first", 1), chunk("second", .9)},
	}}
	result, err := NewRetriever(store, nil, .7, 3).RetrieveWithEmbedding(t.Context(), []float32{1, 0}, RetrievalModeLocal, "target", 0, domain.VectorFilter{})
	require.NoError(t, err)
	require.Len(t, result.Chunks, 3)
	require.Equal(t, []domain.ScoredChunk{chunk("first", 1), chunk("second", .9), chunk("local", .8)}, result.Chunks)
	require.Equal(t, 1.0, result.TopScore)
	require.Equal(t, RetrievalModeLocal, result.Mode)
}

func TestLocalTopUpFailureIsReturned(t *testing.T) {
	want := errors.New("search unavailable")
	store := &localSequenceStore{results: [][]domain.ScoredChunk{nil}, err: want}
	_, err := NewRetriever(store, nil, .7, 10).RetrieveWithEmbedding(t.Context(), []float32{1, 0}, RetrievalModeLocal, "target", 10, domain.VectorFilter{})
	require.ErrorIs(t, err, want)
	require.Len(t, store.filters, 2)
}

func TestLocalWithoutTargetQueriesScopeOnce(t *testing.T) {
	store := &localSequenceStore{results: [][]domain.ScoredChunk{nil}}
	result, err := NewRetriever(store, nil, .7, 10).RetrieveWithEmbedding(t.Context(), []float32{1, 0}, RetrievalModeLocal, "", 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Equal(t, RetrievalModeGlobal, result.Mode)
	require.Len(t, store.filters, 1)
}

type legacyVectorStore struct{ domain.VectorStore }

func (legacyVectorStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	return []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "private", Metadata: map[string]any{"collection": "private"}}}}, nil
}
func TestCollectionRequiresFilteringStore(t *testing.T) {
	_, err := NewRetriever(legacyVectorStore{}, nil, .7, 10).RetrieveWithEmbedding(t.Context(), []float32{1, 0}, RetrievalModeGlobal, "", 10, domain.VectorFilter{Collection: "archive"})
	require.ErrorContains(t, err, "collection filtering")
}

func TestLocalTopUpPreservesDefaultScoreTieOrder(t *testing.T) {
	chunk := func(id string, docID string, score float64) domain.ScoredChunk {
		return domain.ScoredChunk{
			Chunk: domain.Chunk{ID: domain.ChunkID(id), DocumentID: domain.DocumentID(docID)},
			Score: score,
		}
	}
	store := &localSequenceStore{results: [][]domain.ScoredChunk{
		{chunk("003", "doc-local", 0.9)},
		{chunk("001", "doc-other", 0.9), chunk("002", "doc-other", 0.9), chunk("003", "doc-local", 0.9)},
	}}
	result, err := NewRetriever(store, nil, .7, 2).RetrieveWithEmbedding(t.Context(), []float32{1, 0}, RetrievalModeLocal, "doc-local", 2, domain.VectorFilter{})
	require.NoError(t, err)
	require.Equal(t, RetrievalModeLocal, result.Mode)
	require.Len(t, result.Chunks, 2)
	require.Equal(t, []domain.ChunkID{"003", "001"}, []domain.ChunkID{result.Chunks[0].Chunk.ID, result.Chunks[1].Chunk.ID})
}

func TestAggregateDocumentScoresDeduplicatesAndOrdersDeterministically(t *testing.T) {
	chunks := []domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: "a-1", DocumentID: "doc-a"}, Score: 0.88},
		{Chunk: domain.Chunk{ID: "a-2", DocumentID: "doc-a"}, Score: 0.84},
		{Chunk: domain.Chunk{ID: "b-1", DocumentID: "doc-b"}, Score: 0.88},
		{Chunk: domain.Chunk{ID: "a-3", DocumentID: "doc-a"}, Score: 0.90},
		{Chunk: domain.Chunk{ID: "c-1", DocumentID: "doc-c"}, Score: 0.65},
	}
	docs := AggregateDocumentScores(chunks)
	require.Equal(t, []DocumentCandidate{
		{DocID: "doc-a", BestScore: 0.90, ChunkCount: 3},
		{DocID: "doc-b", BestScore: 0.88, ChunkCount: 1},
		{DocID: "doc-c", BestScore: 0.65, ChunkCount: 1},
	}, docs)

	tied := AggregateDocumentScores([]domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: "z-1", DocumentID: "doc-z"}, Score: 0.85},
		{Chunk: domain.Chunk{ID: "a-1", DocumentID: "doc-a"}, Score: 0.85},
	})
	require.Equal(t, []DocumentCandidate{
		{DocID: "doc-a", BestScore: 0.85, ChunkCount: 1},
		{DocID: "doc-z", BestScore: 0.85, ChunkCount: 1},
	}, tied)
}

func TestDetermineEvidenceRouteProperties(t *testing.T) {
	ret := NewRetriever(nil, nil, 0.70, 10)
	cfg := RoutingConfig{Policy: RoutingPolicyEvidence, Margin: 0.08, Calibrated: true}

	t.Run("missing calibration forces global", func(t *testing.T) {
		decision := ret.DetermineEvidenceRoute(RoutingConfig{Policy: RoutingPolicyEvidence, Calibrated: false}, QuerySignals{}, []DocumentCandidate{{DocID: "a", BestScore: 0.95}}, EntityDecision{DocumentID: "a"})
		require.Equal(t, RetrievalModeGlobal, decision.Mode)
		require.Equal(t, RouteReasonMissingCalibration, decision.Reason)
		require.Empty(t, decision.LocalDocumentID)
		require.True(t, ValidRouteReason(decision.Reason))
	})

	t.Run("exact tie selects global even with zero margin", func(t *testing.T) {
		zeroCfg := RoutingConfig{Policy: RoutingPolicyEvidence, Margin: 0, Calibrated: true}
		decision := ret.DetermineEvidenceRoute(zeroCfg, QuerySignals{}, []DocumentCandidate{
			{DocID: "a", BestScore: 0.88},
			{DocID: "b", BestScore: 0.88},
		}, EntityDecision{})
		require.Equal(t, RetrievalModeGlobal, decision.Mode)
		require.Equal(t, RouteReasonAmbiguousMargin, decision.Reason)
		require.Empty(t, decision.LocalDocumentID)
	})

	t.Run("a name resolves a score tie regardless of document ID order", func(t *testing.T) {
		zeroCfg := RoutingConfig{Policy: RoutingPolicyEvidence, Margin: 0, Calibrated: true}
		docs := []DocumentCandidate{{DocID: "a", BestScore: 0.88}, {DocID: "b", BestScore: 0.88}}
		for _, focus := range []string{"a", "b"} {
			decision := ret.DetermineEvidenceRoute(zeroCfg, QuerySignals{}, docs, EntityDecision{DocumentID: focus})
			require.Equal(t, RetrievalModeLocal, decision.Mode)
			require.Equal(t, focus, decision.LocalDocumentID)
		}
	})

	t.Run("small lead selects global and strong lead selects local", func(t *testing.T) {
		closeDocs := []DocumentCandidate{{DocID: "a", BestScore: 0.85}, {DocID: "b", BestScore: 0.79}}
		d := ret.DetermineEvidenceRoute(cfg, QuerySignals{}, closeDocs, EntityDecision{})
		require.Equal(t, RetrievalModeGlobal, d.Mode)
		require.Equal(t, RouteReasonAmbiguousMargin, d.Reason)

		clearDocs := []DocumentCandidate{{DocID: "a", BestScore: 0.85}, {DocID: "b", BestScore: 0.76}}
		d = ret.DetermineEvidenceRoute(cfg, QuerySignals{}, clearDocs, EntityDecision{})
		require.Equal(t, RetrievalModeLocal, d.Mode)
		require.Equal(t, RouteReasonStrongLead, d.Reason)
		require.Equal(t, "a", d.LocalDocumentID)
	})

	t.Run("single visible document above threshold selects local", func(t *testing.T) {
		d := ret.DetermineEvidenceRoute(cfg, QuerySignals{}, []DocumentCandidate{{DocID: "a", BestScore: 0.75, ChunkCount: 4}}, EntityDecision{})
		require.Equal(t, RetrievalModeLocal, d.Mode)
		require.Equal(t, RouteReasonStrongLead, d.Reason)
		require.Equal(t, "a", d.LocalDocumentID)
	})

	t.Run("low score below threshold selects global", func(t *testing.T) {
		d := ret.DetermineEvidenceRoute(cfg, QuerySignals{}, []DocumentCandidate{{DocID: "a", BestScore: 0.68}, {DocID: "b", BestScore: 0.40}}, EntityDecision{})
		require.Equal(t, RetrievalModeGlobal, d.Mode)
		require.Equal(t, RouteReasonLowScore, d.Reason)
	})

	t.Run("negated and ambiguous entities select global", func(t *testing.T) {
		docs := []DocumentCandidate{{DocID: "a", BestScore: 0.90}, {DocID: "b", BestScore: 0.50}}
		d := ret.DetermineEvidenceRoute(cfg, QuerySignals{}, docs, EntityDecision{Negated: true})
		require.Equal(t, RetrievalModeGlobal, d.Mode)
		require.Equal(t, RouteReasonNegatedEntity, d.Reason)

		d = ret.DetermineEvidenceRoute(cfg, QuerySignals{}, docs, EntityDecision{Ambiguous: true})
		require.Equal(t, RetrievalModeGlobal, d.Mode)
		require.Equal(t, RouteReasonAmbiguousEntities, d.Reason)
	})

	t.Run("sparse local top-up preserves collection and doc_type filters", func(t *testing.T) {
		store := &localScopedStore{FakeVectorStore: memory.NewFakeVectorStore()}
		require.NoError(t, store.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "a-1", DocumentID: "a", Embedding: []float32{1, 0}, Metadata: map[string]any{"collection": "tenant-1", "doc_type": "guide"}},
			{ID: "b-1", DocumentID: "b", Embedding: []float32{.9, .1}, Metadata: map[string]any{"collection": "tenant-1", "doc_type": "guide"}},
			{ID: "other-col", DocumentID: "c", Embedding: []float32{1, 0}, Metadata: map[string]any{"collection": "tenant-2", "doc_type": "guide"}},
		}))
		filter := domain.VectorFilter{Collection: "tenant-1", DocType: "guide", DocumentIDs: []string{"a", "b"}}
		res, err := NewRetriever(store, nil, .7, 10).RetrieveWithEmbedding(t.Context(), []float32{1, 0}, RetrievalModeLocal, "a", 10, filter)
		require.NoError(t, err)
		require.Len(t, store.filters, 2)
		require.Equal(t, filter, store.filters[1])
		for _, sc := range res.Chunks {
			require.Equal(t, "tenant-1", sc.Chunk.Metadata["collection"])
			require.NotEqual(t, domain.DocumentID("c"), sc.Chunk.DocumentID)
		}
	})
}

func TestEvidenceLocalSearchesEveryStoredSpelling(t *testing.T) {
	const target, upper = "aaaaaaaa-1111-4222-8333-444444444444", "AAAAAAAA-1111-4222-8333-444444444444"
	for _, tt := range []struct {
		name    string
		keys    []string
		allowed []string
		want    []string
		mode    RetrievalMode
	}{
		{name: "canonical only", want: []string{target}, mode: RetrievalModeLocal},
		{name: "one stored spelling", keys: []string{upper}, want: []string{upper}, mode: RetrievalModeLocal},
		{name: "both spellings in one filter", keys: []string{upper, target}, want: []string{upper, target}, mode: RetrievalModeLocal},
		{name: "authorized target", keys: []string{target}, allowed: []string{target}, want: []string{target}, mode: RetrievalModeLocal},
		{name: "target outside the allowlist", keys: []string{upper}, allowed: []string{"other"}, want: nil, mode: RetrievalModeGlobal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := &localSequenceStore{results: [][]domain.ScoredChunk{nil, nil}}
			filter := domain.VectorFilter{Collection: "archive", DocType: "guide", DocumentIDs: tt.allowed}
			result, err := NewRetriever(store, nil, .7, 3).RetrieveEvidenceLocal(t.Context(), []float32{1, 0}, target, tt.keys, 3, filter)
			require.NoError(t, err)
			require.Equal(t, tt.mode, result.Mode)
			require.Len(t, store.filters, 1, "a targeted evidence search never tops up from the scope")
			require.Equal(t, "archive", store.filters[0].Collection)
			require.Equal(t, "guide", store.filters[0].DocType)
			if tt.mode == RetrievalModeLocal {
				require.Equal(t, tt.want, store.filters[0].DocumentIDs)
			} else {
				require.Equal(t, tt.allowed, store.filters[0].DocumentIDs)
			}
		})
	}
}
