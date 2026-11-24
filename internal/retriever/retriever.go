// Package retriever implements LOCAL/GLOBAL adaptive retrieval
// using scoped vector search.
package retriever

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
)

const (
	defaultTopK    = 10
	minLocalChunks = 3
)

// RetrievalMode selects between local and global retrieval strategies.
type RetrievalMode string

// Retrieval modes control the search scope.
const (
	RetrievalModeLocal  RetrievalMode = "LOCAL"
	RetrievalModeGlobal RetrievalMode = "GLOBAL"
)

// RoutingPolicy selects how automatic retrieval routing decides LOCAL vs GLOBAL.
type RoutingPolicy string

// Supported automatic routing policies.
const (
	RoutingPolicyLegacy   RoutingPolicy = "legacy"
	RoutingPolicyEvidence RoutingPolicy = "evidence"
)

// RouteReason is a bounded enum describing why a retrieval route was selected.
type RouteReason string

// Bounded route-reason constants recorded for diagnostics and metrics.
const (
	RouteReasonEmptyScope         RouteReason = "empty_scope"
	RouteReasonExplicitGlobal     RouteReason = "explicit_global"
	RouteReasonExplicitLocal      RouteReason = "explicit_local"
	RouteReasonProbeFailed        RouteReason = "probe_failed"
	RouteReasonEmptyProbe         RouteReason = "empty_probe"
	RouteReasonMissingCalibration RouteReason = "missing_calibration"
	RouteReasonMultiNeed          RouteReason = "multi_need"
	RouteReasonAmbiguousEntities  RouteReason = "ambiguous_entities"
	RouteReasonNegatedEntity      RouteReason = "negated_entity"
	RouteReasonNamedEntity        RouteReason = "named_entity"
	RouteReasonAmbiguousMargin    RouteReason = "ambiguous_margin"
	RouteReasonScoreThreshold     RouteReason = "score_threshold"
	RouteReasonStrongLead         RouteReason = "strong_lead"
	RouteReasonLowScore           RouteReason = "low_score"
)

// ValidRouteReason reports whether r belongs to the bounded RouteReason enum.
func ValidRouteReason(r RouteReason) bool {
	switch r {
	case RouteReasonEmptyScope, RouteReasonExplicitGlobal, RouteReasonExplicitLocal,
		RouteReasonProbeFailed, RouteReasonEmptyProbe, RouteReasonMissingCalibration,
		RouteReasonMultiNeed, RouteReasonAmbiguousEntities, RouteReasonNegatedEntity,
		RouteReasonNamedEntity, RouteReasonAmbiguousMargin, RouteReasonScoreThreshold,
		RouteReasonStrongLead, RouteReasonLowScore:
		return true
	default:
		return false
	}
}

// RoutingConfig configures policy and calibration state for evidence routing.
type RoutingConfig struct {
	Policy     RoutingPolicy
	Margin     float64
	Calibrated bool
}

// DocumentCandidate summarizes visible probe evidence for one distinct document.
type DocumentCandidate struct {
	DocID      string
	BestScore  float64
	ChunkCount int
}

// QuerySignals captures structural query cues without calling external providers.
type QuerySignals struct {
	ExplicitMultiRequest bool
	StrongMultiNeedCue   bool
	DistinctSubQueries   bool
	ProbeSaturated       bool
	ProbeCutoff          *float64
}

// EntityDecision summarizes preview entity matching over visible probe documents.
type EntityDecision struct {
	DocumentID string
	Ambiguous  bool
	Negated    bool
}

// RouteDecision records the selected retrieval mode, bounded reason and optional target document.
type RouteDecision struct {
	Mode            RetrievalMode
	Reason          RouteReason
	LocalDocumentID string
}

// Retriever implements pure-vector adaptive local/global retrieval.
type Retriever struct {
	vectorStore domain.VectorStore
	embedder    domain.EmbeddingProvider
	threshold   float64
	topK        int
	profile     *embeddinginput.Profile
}

// Option configures a Retriever.
type Option func(*Retriever)

// WithEmbeddingInputProfile validates a query before calling the embedding provider.
func WithEmbeddingInputProfile(profile *embeddinginput.Profile) Option {
	return func(r *Retriever) { r.profile = profile }
}

// NewRetriever creates a Retriever with the given dependencies.
func NewRetriever(vs domain.VectorStore, emb domain.EmbeddingProvider, threshold float64, topK int, opts ...Option) *Retriever {
	if topK <= 0 {
		topK = defaultTopK
	}
	r := &Retriever{
		vectorStore: vs,
		embedder:    emb,
		threshold:   threshold,
		topK:        topK,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Result holds the retrieval output.
type Result struct {
	Chunks   []domain.ScoredChunk
	Mode     RetrievalMode
	TopScore float64
}

// Retrieve executes the retrieval pipeline based on the determined mode.
//
// If the caller already produced an embedding for the query (e.g. the
// routing stage), it can be passed via RetrieveWithEmbedding to avoid
// a redundant Bedrock embed call. This wrapper embeds on demand.
func (r *Retriever) Retrieve(ctx context.Context, query string, mode RetrievalMode, topK int, filter domain.VectorFilter) (*Result, error) {
	if filter.MatchesNothing() {
		return &Result{Mode: mode}, nil
	}
	embedding, err := r.embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("retriever: embed query: %w", err)
	}
	return r.RetrieveWithEmbedding(ctx, embedding, mode, "", topK, filter)
}

// RetrieveWithEmbedding runs the retrieval pipeline against a caller-
// supplied query embedding. Used by the query stage chain to avoid
// embedding the same text twice (once in routing, once here).
// LOCAL searches localDocID first and tops up sparse results from filter.
func (r *Retriever) RetrieveWithEmbedding(ctx context.Context, embedding []float32, mode RetrievalMode, localDocID string, topK int, filter domain.VectorFilter) (*Result, error) {
	return r.retrieveWithEmbedding(ctx, embedding, mode, localDocID, nil, topK, filter, true)
}

// RetrieveEvidenceLocal searches only the selected document. localKeys lists
// the spellings under which the vector store holds that document, which can
// differ from the canonical localDocID; empty means localDocID alone. The caller
// can fill sparse results from the previously fetched scope after visibility checks.
func (r *Retriever) RetrieveEvidenceLocal(ctx context.Context, embedding []float32, localDocID string, localKeys []string, topK int, filter domain.VectorFilter) (*Result, error) {
	return r.retrieveWithEmbedding(ctx, embedding, RetrievalModeLocal, localDocID, localKeys, topK, filter, false)
}

// ChunkLimit applies the retriever default when no explicit limit was supplied.
func (r *Retriever) ChunkLimit(topK int) int {
	if topK > 0 {
		return topK
	}
	return r.topK
}

func (r *Retriever) retrieveWithEmbedding(ctx context.Context, embedding []float32, mode RetrievalMode, localDocID string, localKeys []string, topK int, filter domain.VectorFilter, topUp bool) (*Result, error) {
	topK = r.ChunkLimit(topK)
	if filter.MatchesNothing() {
		return &Result{Mode: mode}, nil
	}
	mode, queryFilter := retrievalScopeWithKeys(mode, localDocID, localKeys, filter)
	chunks, exact, err := r.queryFiltered(ctx, embedding, topK, queryFilter)
	if err != nil {
		return nil, fmt.Errorf("retriever: query: %w", err)
	}
	if topUp && mode == RetrievalModeLocal && len(chunks) < minLocalChunks {
		extra, scopeExact, err := r.queryFiltered(ctx, embedding, topK, filter)
		if err != nil {
			return nil, fmt.Errorf("retriever: top up: %w", err)
		}
		exact = exact && scopeExact
		if exact {
			// The broader scope includes the local document and already has the database's top K in order.
			chunks = extra
		} else {
			chunks = mergeChunks(chunks, extra)
		}
	}

	if !exact {
		sort.Slice(chunks, func(i, j int) bool { return chunks[i].Score > chunks[j].Score })
	}

	return retrievalResult(ctx, mode, chunks, topK), nil
}

func retrievalScope(mode RetrievalMode, localDocID string, filter domain.VectorFilter) (RetrievalMode, domain.VectorFilter) {
	if mode == RetrievalModeLocal {
		if localDocID == "" || (filter.DocumentIDs != nil && !slices.Contains(filter.DocumentIDs, localDocID)) {
			return RetrievalModeGlobal, filter
		}
		filter.DocumentIDs = []string{localDocID}
	}
	return mode, filter
}

func retrievalScopeWithKeys(mode RetrievalMode, localDocID string, localKeys []string, filter domain.VectorFilter) (RetrievalMode, domain.VectorFilter) {
	mode, filter = retrievalScope(mode, localDocID, filter)
	if mode == RetrievalModeLocal && len(localKeys) != 0 {
		filter.DocumentIDs = localKeys
	}
	return mode, filter
}

// SelectRoutedChunks deduplicates verified evidence before applying the limit.
// With a target, it keeps that document first and fills only a sparse result.
func SelectRoutedChunks(verified []domain.ScoredChunk, target string, limit int) []domain.ScoredChunk {
	seen := make(map[domain.ChunkID]struct{}, len(verified))
	var primary, fill []domain.ScoredChunk
	for _, sc := range verified {
		if _, duplicate := seen[sc.Chunk.ID]; duplicate {
			continue
		}
		seen[sc.Chunk.ID] = struct{}{}
		if target == "" || string(sc.Chunk.DocumentID) == target {
			primary = append(primary, sc)
		} else {
			fill = append(fill, sc)
		}
	}
	if len(primary) != 0 && len(primary) < min(minLocalChunks, limit) {
		primary = append(primary, fill...)
	}
	return primary[:min(len(primary), limit)]
}

func mergeChunks(batches ...[]domain.ScoredChunk) []domain.ScoredChunk {
	size := 0
	for _, batch := range batches {
		size += len(batch)
	}
	merged := make([]domain.ScoredChunk, 0, size)
	byID := make(map[domain.ChunkID]int, size)
	for _, batch := range batches {
		for _, sc := range batch {
			if i, ok := byID[sc.Chunk.ID]; ok {
				if sc.Score > merged[i].Score {
					merged[i] = sc
				}
			} else {
				byID[sc.Chunk.ID] = len(merged)
				merged = append(merged, sc)
			}
		}
	}
	return merged
}

func retrievalResult(ctx context.Context, mode RetrievalMode, chunks []domain.ScoredChunk, topK int) *Result {
	if len(chunks) > topK {
		chunks = chunks[:topK]
	}

	topScore := 0.0
	if len(chunks) > 0 {
		topScore = chunks[0].Score
	}

	slog.DebugContext(ctx, "retrieval completed", "mode", mode, "chunks", len(chunks), "similarity", topScore)

	return &Result{
		Chunks:   chunks,
		Mode:     mode,
		TopScore: topScore,
	}
}

// DetermineModeWithQuery decides LOCAL or GLOBAL mode using structural signals only.
func (r *Retriever) DetermineModeWithQuery(subQueryCount int, topScore float64, namedEntity string) RetrievalMode {
	return r.DetermineLegacyRoute(subQueryCount, topScore, namedEntity, "", "").Mode
}

// DetermineLegacyRoute preserves legacy routing decisions and reports the bounded RouteReason.
func (r *Retriever) DetermineLegacyRoute(subQueryCount int, topScore float64, namedEntity, topDocID, entityDocID string) RouteDecision {
	if subQueryCount > 1 {
		return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonMultiNeed}
	}
	target := entityDocID
	if target == "" {
		target = topDocID
	}
	if namedEntity != "" {
		return RouteDecision{Mode: RetrievalModeLocal, Reason: RouteReasonNamedEntity, LocalDocumentID: target}
	}
	if topScore >= r.threshold {
		return RouteDecision{Mode: RetrievalModeLocal, Reason: RouteReasonScoreThreshold, LocalDocumentID: target}
	}
	return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonLowScore}
}

// AggregateDocumentScores aggregates scoped visible probe hits by distinct document.
// Each document's score is its strongest visible chunk score; ties break deterministically by document ID.
func AggregateDocumentScores(chunks []domain.ScoredChunk) []DocumentCandidate {
	if len(chunks) == 0 {
		return nil
	}
	byDoc := make(map[string]DocumentCandidate, len(chunks))
	for _, sc := range chunks {
		if math.IsNaN(sc.Score) || math.IsInf(sc.Score, 0) {
			continue
		}
		id := string(sc.Chunk.DocumentID)
		cur, ok := byDoc[id]
		if !ok {
			byDoc[id] = DocumentCandidate{DocID: id, BestScore: sc.Score, ChunkCount: 1}
			continue
		}
		cur.ChunkCount++
		if sc.Score > cur.BestScore {
			cur.BestScore = sc.Score
		}
		byDoc[id] = cur
	}
	if len(byDoc) == 0 {
		return nil
	}
	docs := make([]DocumentCandidate, 0, len(byDoc))
	for _, cand := range byDoc {
		docs = append(docs, cand)
	}
	sort.Slice(docs, func(i, j int) bool {
		if docs[i].BestScore == docs[j].BestScore {
			return docs[i].DocID < docs[j].DocID
		}
		return docs[i].BestScore > docs[j].BestScore
	})
	return docs
}

// DetermineEvidenceRoute selects LOCAL or GLOBAL using aggregated visible document evidence,
// calibrated margin, entity ambiguity/negation and explicit multi-request signals.
func (r *Retriever) DetermineEvidenceRoute(cfg RoutingConfig, signals QuerySignals, docs []DocumentCandidate, entity EntityDecision) RouteDecision {
	if len(docs) == 0 {
		return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonEmptyProbe}
	}
	if !cfg.Calibrated || !(cfg.Margin >= 0 && cfg.Margin <= 1) {
		return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonMissingCalibration}
	}
	if signals.ExplicitMultiRequest || signals.StrongMultiNeedCue || signals.DistinctSubQueries {
		return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonMultiNeed}
	}
	if entity.Negated {
		return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonNegatedEntity}
	}
	if entity.Ambiguous {
		return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonAmbiguousEntities}
	}
	if entity.DocumentID != "" {
		entityScore := 0.0
		foundEntityDoc := false
		for _, doc := range docs {
			if doc.DocID == entity.DocumentID {
				entityScore = doc.BestScore
				foundEntityDoc = true
				break
			}
		}
		lead := docs[0].BestScore - entityScore
		if !foundEntityDoc || (docs[0].BestScore >= r.threshold && lead > 0 && lead >= cfg.Margin) {
			return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonAmbiguousMargin}
		}
		return RouteDecision{Mode: RetrievalModeLocal, Reason: RouteReasonNamedEntity, LocalDocumentID: entity.DocumentID}
	}
	topScore := docs[0].BestScore
	if math.IsNaN(topScore) || topScore < r.threshold {
		return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonLowScore}
	}
	runnerUp := math.Inf(-1)
	if len(docs) > 1 {
		runnerUp = docs[1].BestScore
	}
	if signals.ProbeSaturated {
		if signals.ProbeCutoff == nil || math.IsNaN(*signals.ProbeCutoff) || math.IsInf(*signals.ProbeCutoff, 0) {
			return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonAmbiguousMargin}
		}
		runnerUp = max(runnerUp, *signals.ProbeCutoff)
	}
	if lead := topScore - runnerUp; math.IsNaN(lead) || lead <= 0 || lead < cfg.Margin {
		return RouteDecision{Mode: RetrievalModeGlobal, Reason: RouteReasonAmbiguousMargin}
	}
	return RouteDecision{Mode: RetrievalModeLocal, Reason: RouteReasonStrongLead, LocalDocumentID: docs[0].DocID}
}

// EmbedQuery embeds the query and optionally probes the authorized scope.
func (r *Retriever) EmbedQuery(ctx context.Context, query string, filter domain.VectorFilter, probeK int) (embedding []float32, candidates []domain.ScoredChunk, probeFailed bool, err error) {
	return r.embedQuery(ctx, query, filter, probeK, func(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
		chunks, _, err := r.queryFiltered(ctx, embedding, topK, filter)
		return chunks, err
	})
}

// EmbedQueryForVisibilityRefill preserves candidate versions across hosted
// partitions so the scoped search can be reused after authoritative checks.
func (r *Retriever) EmbedQueryForVisibilityRefill(ctx context.Context, query string, filter domain.VectorFilter, probeK int) (embedding []float32, candidates []domain.ScoredChunk, probeFailed bool, err error) {
	store, ok := r.vectorStore.(domain.VisibilityRefillStore)
	if !ok {
		return nil, nil, false, errors.New("retriever: vector store does not support visibility refill")
	}
	embedding, candidates, probeFailed, err = r.embedQuery(ctx, query, filter, probeK, store.QueryCandidates)
	sortVisibilityHits(candidates)
	return embedding, candidates, probeFailed, err
}

func (r *Retriever) embedQuery(ctx context.Context, query string, filter domain.VectorFilter, probeK int, search func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error)) (embedding []float32, candidates []domain.ScoredChunk, probeFailed bool, err error) {
	if filter.MatchesNothing() {
		return nil, nil, false, nil
	}
	embedding, err = r.embed(ctx, query)
	if err != nil {
		return nil, nil, false, fmt.Errorf("retriever: embed query: %w", err)
	}
	if probeK <= 0 {
		return embedding, nil, false, nil
	}
	candidates, err = search(ctx, embedding, probeK, filter)
	if err != nil {
		slog.WarnContext(ctx, "routing probe failed", "mode", RetrievalModeGlobal, "error", err)
		return embedding, nil, true, nil
	}
	return embedding, candidates, false, nil
}

func (r *Retriever) embed(ctx context.Context, text string) ([]float32, error) {
	if _, err := r.profile.Check(ctx, text); err != nil {
		return nil, err
	}
	return r.embedder.Embed(ctx, text)
}

func (r *Retriever) queryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	if filter.MatchesNothing() {
		return []domain.ScoredChunk{}, false, nil
	}
	if store, ok := r.vectorStore.(domain.FilteredVectorStoreWithExactness); ok {
		return store.QueryFilteredWithExactness(ctx, embedding, topK, filter)
	}
	if store, ok := r.vectorStore.(domain.FilteredVectorStore); ok {
		chunks, err := store.QueryFiltered(ctx, embedding, topK, filter)
		return chunks, false, err
	}
	if filter.Collection != "" {
		return nil, false, fmt.Errorf("retriever: vector store does not support collection filtering")
	}
	chunks, err := r.vectorStore.QueryAllNamespaces(ctx, embedding, topK, filter.DocType)
	if err != nil {
		return nil, false, err
	}
	allowed := make(map[string]struct{}, len(filter.DocumentIDs))
	for _, id := range filter.DocumentIDs {
		allowed[id] = struct{}{}
	}
	filtered := make([]domain.ScoredChunk, 0, len(chunks))
	for _, sc := range chunks {
		if filter.DocType != "" {
			if docType, _ := sc.Chunk.Metadata["doc_type"].(string); docType != filter.DocType {
				continue
			}
		}
		if filter.DocumentIDs != nil {
			if _, ok := allowed[string(sc.Chunk.DocumentID)]; !ok {
				continue
			}
		}
		filtered = append(filtered, sc)
	}
	sort.SliceStable(filtered, func(i, j int) bool { return filtered[i].Score > filtered[j].Score })
	if len(filtered) > topK {
		filtered = filtered[:topK]
	}
	return filtered, false, nil
}
