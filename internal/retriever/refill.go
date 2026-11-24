package retriever

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
)

// SupportsVisibilityRefill reports whether the store can bound optional hosted searches.
func (r *Retriever) SupportsVisibilityRefill() bool {
	_, ok := r.vectorStore.(domain.VisibilityRefillStore)
	return ok
}

type visibilitySearch struct {
	store     domain.VisibilityRefillStore
	embedding []float32
	verify    func(context.Context, []domain.ScoredChunk, domain.VectorFilter) ([]domain.ScoredChunk, bool, error)
	topK      int
	budget    int
	expanded  bool
	removed   bool
	exhausted bool
	failed    bool
}

type visibilityBatch struct {
	candidates []domain.ScoredChunk
	visible    []domain.ScoredChunk
}

// RetrieveVisibleWithEmbedding retrieves current evidence and optionally widens
// one hosted search. The verifier applies the scope of each search before
// candidate counts or ranking can influence subsequent retrieval. It reports
// whether the authoritative visibility check removed any in-scope hits.
func (r *Retriever) RetrieveVisibleWithEmbedding(ctx context.Context, embedding []float32, mode RetrievalMode, localDocID string, topK int, filter domain.VectorFilter, budget int, verify func(context.Context, []domain.ScoredChunk, domain.VectorFilter) ([]domain.ScoredChunk, bool, error)) (*Result, error) {
	store, ok := r.vectorStore.(domain.VisibilityRefillStore)
	if !ok {
		return nil, errors.New("retriever: vector store does not support visibility refill")
	}
	if filter.MatchesNothing() {
		return &Result{Mode: mode}, nil
	}
	if topK <= 0 {
		topK = r.topK
	}
	search := visibilitySearch{store: store, embedding: embedding, verify: verify, topK: topK, budget: budget}
	mode, firstFilter := retrievalScope(mode, localDocID, filter)
	raw, err := store.QueryCandidates(ctx, embedding, topK, firstFilter)
	if err != nil {
		return nil, fmt.Errorf("retriever: query: %w", err)
	}
	current, removed, err := search.check(ctx, raw, firstFilter)
	if err != nil {
		return nil, err
	}
	batch, err := search.recover(ctx, visibilityBatch{candidates: raw, visible: current}, removed, firstFilter)
	if err != nil {
		return nil, err
	}
	chunks := mergeChunks(batch.visible)
	if mode == RetrievalModeLocal && len(chunks) < 3 {
		baseline := min(topK, len(mergeChunks(raw))) < 3
		batch, err = search.topUpVisible(ctx, batch, localDocID, baseline, filter)
		if err != nil {
			return nil, err
		}
		chunks = mergeChunks(batch.visible)
	}
	sortVisibilityHits(chunks)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	search.record(ctx, len(chunks))
	return retrievalResult(ctx, mode, chunks, topK), nil
}

// RoutedProbe holds the original scope search and the optional routed target.
type RoutedProbe struct {
	Candidates []domain.ScoredChunk
	SearchK    int
	Target     string
	TargetKeys []string
}

// RetrieveVisibleFromProbe reuses the evidence route's scoped search and shares
// one refill allowance between that search and any selected target search.
func (r *Retriever) RetrieveVisibleFromProbe(ctx context.Context, embedding []float32, probe RoutedProbe, topK int, filter domain.VectorFilter, budget int, verify func(context.Context, []domain.ScoredChunk, domain.VectorFilter) ([]domain.ScoredChunk, bool, error)) (*Result, error) {
	store, ok := r.vectorStore.(domain.VisibilityRefillStore)
	if !ok {
		return nil, errors.New("retriever: vector store does not support visibility refill")
	}
	topK = r.ChunkLimit(topK)
	search := visibilitySearch{store: store, embedding: embedding, verify: verify, topK: topK, budget: budget}
	mode := RetrievalModeGlobal
	var local []domain.ScoredChunk
	if probe.Target != "" {
		var targetFilter domain.VectorFilter
		mode, targetFilter = retrievalScopeWithKeys(RetrievalModeLocal, probe.Target, probe.TargetKeys, filter)
		if mode == RetrievalModeLocal {
			var err error
			local, err = search.retrieveRoutedTarget(ctx, targetFilter)
			if err != nil {
				return nil, err
			}
		} else {
			probe.Target = ""
		}
	}
	visible, removed, err := search.check(ctx, probe.Candidates, filter)
	if err != nil {
		return nil, err
	}
	chunks := SelectRoutedChunks(slices.Concat(local, visible), probe.Target, topK)
	if removed && len(chunks) < topK && routedScopeCanFill(chunks, probe.Target, topK) {
		search.topK = probe.SearchK
		batch, err := search.recover(ctx, visibilityBatch{candidates: probe.Candidates, visible: visible}, true, filter)
		if err != nil {
			return nil, err
		}
		sortVisibilityHits(batch.visible)
		chunks = SelectRoutedChunks(slices.Concat(local, batch.visible), probe.Target, topK)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	search.topK = topK
	search.record(ctx, len(chunks))
	return retrievalResult(ctx, mode, chunks, topK), nil
}

func (s *visibilitySearch) retrieveRoutedTarget(ctx context.Context, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	raw, err := s.store.QueryCandidates(ctx, s.embedding, s.topK, filter)
	if err != nil {
		return nil, fmt.Errorf("retriever: query: %w", err)
	}
	visible, removed, err := s.check(ctx, raw, filter)
	if err != nil {
		return nil, err
	}
	batch, err := s.recover(ctx, visibilityBatch{candidates: raw, visible: visible}, removed, filter)
	if err != nil {
		return nil, err
	}
	sortVisibilityHits(batch.visible)
	return batch.visible, nil
}

func routedScopeCanFill(chunks []domain.ScoredChunk, target string, limit int) bool {
	minimum := min(minLocalChunks, limit)
	return target == "" || len(chunks) < minimum || string(chunks[minimum-1].Chunk.DocumentID) != target
}

func sortVisibilityHits(chunks []domain.ScoredChunk) {
	slices.SortStableFunc(chunks, func(a, b domain.ScoredChunk) int {
		if rank := cmp.Compare(b.Score, a.Score); rank != 0 {
			return rank
		}
		return cmp.Compare(a.Chunk.ID, b.Chunk.ID)
	})
}

func (s *visibilitySearch) topUpVisible(ctx context.Context, local visibilityBatch, localDocID string, baseline bool, filter domain.VectorFilter) (visibilityBatch, error) {
	var raw []domain.ScoredChunk
	var err error
	completed := true
	if baseline {
		raw, err = s.store.QueryCandidates(ctx, s.embedding, s.topK, filter)
		if err != nil {
			return visibilityBatch{}, fmt.Errorf("retriever: top up: %w", err)
		}
	} else {
		raw, completed, err = s.optionalQuery(ctx, min(s.topK, s.store.VisibilityRefillLimit()), filter)
		if err != nil {
			return visibilityBatch{}, err
		}
	}
	_, extraRemoved, err := s.check(ctx, raw, filter)
	if err != nil {
		return visibilityBatch{}, err
	}
	candidates := local.visible
	if completed {
		candidates = slices.Clone(raw)
		// Keep the LOCAL restriction when checking the combined candidates
		// against the broader top-up scope.
		for _, candidate := range local.candidates {
			if candidate.Chunk.DocumentID == domain.DocumentID(localDocID) {
				candidates = append(candidates, candidate)
			}
		}
	}
	current, removed, err := s.check(ctx, candidates, filter)
	if err != nil {
		return visibilityBatch{}, err
	}
	return s.recover(ctx, visibilityBatch{candidates: candidates, visible: current}, removed || extraRemoved, filter)
}

func (s *visibilitySearch) recover(ctx context.Context, batch visibilityBatch, removed bool, filter domain.VectorFilter) (visibilityBatch, error) {
	limit := s.store.VisibilityRefillLimit()
	if !removed || s.expanded || limit <= s.topK {
		return batch, nil
	}
	s.expanded = true
	expandedK := s.topK + min(s.topK, limit-s.topK)
	extra, completed, err := s.optionalQuery(ctx, expandedK, filter)
	if err != nil {
		return visibilityBatch{}, err
	}
	candidates := batch.visible
	if completed {
		// Retain the first-pass bodies in case a document completed while
		// the wider search ran. Verify both batches before merging IDs.
		candidates = slices.Concat(batch.candidates, extra)
	}
	visible, _, err := s.check(ctx, candidates, filter)
	return visibilityBatch{candidates: candidates, visible: visible}, err
}

func (s *visibilitySearch) check(ctx context.Context, chunks []domain.ScoredChunk, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if len(chunks) == 0 {
		return chunks, false, nil
	}
	visible, removed, err := s.verify(ctx, chunks, filter)
	if err != nil {
		return nil, false, fmt.Errorf("retriever: visibility: %w", err)
	}
	s.removed = s.removed || len(visible) < len(chunks)
	return visible, removed, nil
}

func (s *visibilitySearch) optionalQuery(ctx context.Context, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.budget == 0 {
		s.exhausted = true
		return nil, false, nil
	}
	chunks, used, err := s.store.QueryFilteredBudgeted(ctx, s.embedding, topK, filter, s.budget)
	s.budget -= used
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, false, ctxErr
	}
	if err != nil {
		if errors.Is(err, domain.ErrVectorQueryBudget) {
			s.exhausted = true
		} else {
			s.failed = true
		}
		return nil, false, nil
	}
	return chunks, true, nil
}

func (s *visibilitySearch) record(ctx context.Context, count int) {
	counter := metrics.FromContext(ctx).HostedRefill
	if s.exhausted {
		counter.WithLabelValues("budget_exhausted").Inc()
	}
	if s.failed {
		counter.WithLabelValues("upstream_error").Inc()
	}
	if s.removed && count < s.topK {
		counter.WithLabelValues("underfill").Inc()
	}
}
