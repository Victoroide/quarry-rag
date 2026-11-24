package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"golang.org/x/text/unicode/norm"
	"golang.org/x/text/width"
)

const answerSynthesisMaxOutput = 1024

// DecomposeStage identifies multiple information needs without database access.
type DecomposeStage struct{}

// NewDecomposeStage creates a DecomposeStage.
func NewDecomposeStage() *DecomposeStage { return &DecomposeStage{} }

// ID returns the stage identifier.
func (s *DecomposeStage) ID() StageID { return StageQueryDecomposition }

// Multi-need words must stand alone so single-topic words do not broaden retrieval.
var multiNeedPattern = regexp.MustCompile(`(?i)(?:^|[^\p{L}\p{N}_])(?:and|or|all|which|compare|compares|comparing|comparison|list|listing|summarize|y|e|o|todos|todas|cuáles|cuales|compara|comparar|comparación|listar|lista|resumen|diferencias|versus|vs)(?:$|[^\p{L}\p{N}_])`)

func hasMultiNeedSignal(query string) bool { return multiNeedPattern.MatchString(query) }

// Run sets the sub-query count from surface cues without external calls.
func (s *DecomposeStage) Run(ctx context.Context, qc *QueryContext) error {
	slog.DebugContext(ctx, "decompose_stage", "query_bytes", len(qc.OriginalQuery), "query_digest", queryDigest(qc.OriginalQuery))
	if hasMultiNeedSignal(qc.OriginalQuery) {
		qc.SubQueries = []string{qc.OriginalQuery, qc.OriginalQuery}
	} else {
		qc.SubQueries = []string{qc.OriginalQuery}
	}
	return nil
}

func queryDigest(query string) string {
	digest := sha256.Sum256([]byte(query))
	return hex.EncodeToString(digest[:6])
}

// RoutingConfig aliases retriever.RoutingConfig for pipeline routing construction.
type RoutingConfig = retriever.RoutingConfig

// RoutingConfigFromConfig converts service configuration into pipeline routing settings.
func RoutingConfigFromConfig(cfg *config.Config) RoutingConfig {
	if cfg == nil || cfg.RoutingPolicy != config.RoutingPolicyEvidence {
		return RoutingConfig{Policy: retriever.RoutingPolicyLegacy}
	}
	return RoutingConfig{
		Policy:     retriever.RoutingPolicyEvidence,
		Margin:     cfg.EffectiveRoutingMargin,
		Calibrated: cfg.RoutingCalibrationMatched,
	}
}

// RoutingStage determines LOCAL or GLOBAL retrieval mode.
type RoutingStage struct {
	ret        *retriever.Retriever
	visibility domain.DocumentVisibility
	previews   domain.DocumentPreviewer
	cfg        RoutingConfig
}

type queryRoute struct {
	mode                 string
	reason               retriever.RouteReason
	localDocumentID      string
	namedEntity          string
	entityDocumentID     string
	previewNegativeMatch bool
}

type previewRouting struct {
	stage       *RoutingStage
	documentIDs []string
	original    queryRoute
}

// NewRoutingStage creates a RoutingStage with the default legacy routing policy.
func NewRoutingStage(ret *retriever.Retriever) *RoutingStage {
	return NewRoutingStageWithScope(ret, nil, nil)
}

// NewRoutingStageWithScope restricts routing signals to visible candidates.
func NewRoutingStageWithScope(ret *retriever.Retriever, vis domain.DocumentVisibility, previews domain.DocumentPreviewer) *RoutingStage {
	return NewRoutingStageWithConfig(ret, vis, previews, RoutingConfig{Policy: retriever.RoutingPolicyLegacy})
}

// NewRoutingStageWithConfig creates a RoutingStage with explicit visibility, previews and routing policy.
func NewRoutingStageWithConfig(ret *retriever.Retriever, vis domain.DocumentVisibility, previews domain.DocumentPreviewer, cfg RoutingConfig) *RoutingStage {
	if cfg.Policy == "" {
		cfg.Policy = retriever.RoutingPolicyLegacy
	}
	return &RoutingStage{ret: ret, visibility: vis, previews: previews, cfg: cfg}
}

// ID returns the stage identifier.
func (s *RoutingStage) ID() StageID { return StageRetrievalRouting }

// Run evaluates query signals and sets RetrievalMode on the QueryContext.
func (s *RoutingStage) Run(ctx context.Context, qc *QueryContext) error {
	filter := qc.vectorFilter()
	qc.NamedEntity, qc.EntityDocumentID = "", ""
	qc.LocalDocumentID = ""
	qc.Candidates, qc.QueryEmbedding = nil, nil
	qc.TopScore, qc.SkipRetrieval = 0, false
	qc.previewRouting = nil
	qc.previewNegativeMatch = false
	qc.scopedSearch, qc.scopedSearchReady = nil, false
	qc.probeSaturated, qc.probeCutoff = false, nil
	qc.localDocumentKeys = nil
	policy := s.cfg.Policy
	if policy == "" {
		policy = retriever.RoutingPolicyLegacy
	}
	qc.RoutingPolicy = string(policy)
	qc.RetrievalMode = string(retriever.RetrievalModeGlobal)
	qc.RouteReason = ""
	defer func() {
		slog.DebugContext(ctx, "routing completed",
			"policy", qc.RoutingPolicy,
			"mode", qc.RetrievalMode,
			"reason", string(qc.RouteReason),
			"candidates", len(qc.Candidates),
		)
	}()
	if filter.MatchesNothing() {
		qc.SkipRetrieval = true
		qc.RouteReason = retriever.RouteReasonEmptyScope
		return nil
	}
	probeK := 5
	if strings.EqualFold(qc.RequestedMode, "global") {
		probeK = 0
		qc.RouteReason = retriever.RouteReasonExplicitGlobal
	}
	searchK := probeK
	if qc.evidenceAutoRouting() {
		searchK = max(probeK, s.ret.ChunkLimit(qc.MaxChunks))
	}
	embedding, candidates, probeFailed, err := s.ret.EmbedQuery(ctx, qc.OriginalQuery, filter, searchK)
	if err != nil {
		return fmt.Errorf("routing_stage: %w", err)
	}
	qc.QueryEmbedding = embedding
	if probeK == 0 {
		return nil
	}
	if probeFailed {
		qc.RouteReason = retriever.RouteReasonProbeFailed
		return nil
	}
	var providerKeys documentKeys
	if qc.evidenceAutoRouting() {
		providerKeys = providerDocumentKeys(candidates)
		candidates = canonicalEvidenceIDs(candidates)
		qc.scopedSearch, qc.scopedSearchReady = candidates, true
		qc.probeSaturated = len(candidates) >= probeK && len(filter.DocumentIDs) != 1
		candidates = candidates[:min(len(candidates), probeK)]
	}
	var previews map[string]string
	if qc.EvidenceVisibility != nil && (qc.evidenceAutoRouting() || qc.HostedVisibilityRefill && s.previews != nil) {
		candidates = filterEvidenceScope(filter, candidates)
		candidates, previews, err = qc.EvidenceVisibility.VisibleEvidenceWithPreviews(ctx, candidates, candidateDocumentIDs(candidates))
	} else {
		candidates, err = scopedChunks(ctx, qc, candidates, s.visibility)
	}
	if err != nil {
		return fmt.Errorf("routing_stage: visibility: %w", err)
	}
	qc.Candidates = candidates
	if qc.probeSaturated && len(candidates) == probeK {
		score := candidates[probeK-1].Score
		qc.probeCutoff = &score
	}
	if len(candidates) == 0 {
		qc.RouteReason = retriever.RouteReasonEmptyProbe
		return nil
	}
	qc.TopScore = candidates[0].Score
	ids := candidateDocumentIDs(candidates)
	if previews == nil && s.previews != nil {
		previews, err = s.previews.DocumentPreviews(ctx, ids)
		if err != nil {
			return fmt.Errorf("routing_stage: previews: %w", err)
		}
	}
	route := s.selectRoute(qc, previews, ids)
	qc.RetrievalMode, qc.LocalDocumentID = route.mode, route.localDocumentID
	qc.NamedEntity, qc.EntityDocumentID = route.namedEntity, route.entityDocumentID
	qc.RouteReason = route.reason
	qc.previewNegativeMatch = route.previewNegativeMatch
	if qc.evidenceAutoRouting() && route.localDocumentID != "" {
		qc.localDocumentKeys = providerKeys.spellings(candidates, route.localDocumentID)
	}
	if qc.checksCurrentEvidence() && (s.previews != nil || previews != nil) {
		qc.previewRouting = &previewRouting{stage: s, documentIDs: ids, original: route}
	}
	return nil
}

func (s *RoutingStage) selectRoute(qc *QueryContext, previews map[string]string, ids []string) queryRoute {
	first := qc.Candidates[0]
	if qc.RequestedMode != "" && !strings.EqualFold(qc.RequestedMode, "auto") {
		var route queryRoute
		route.mode = strings.ToUpper(qc.RequestedMode)
		route.namedEntity, route.entityDocumentID = MatchNamedEntity(qc.OriginalQuery, previews, ids)
		if route.mode == string(retriever.RetrievalModeGlobal) {
			route.reason = retriever.RouteReasonExplicitGlobal
			return route
		}
		route.reason = retriever.RouteReasonExplicitLocal
		route.localDocumentID = route.entityDocumentID
		if route.localDocumentID == "" {
			route.localDocumentID = string(first.Chunk.DocumentID)
		}
		return route
	}

	if s.cfg.Policy == retriever.RoutingPolicyEvidence {
		return s.selectEvidenceRoute(qc, previews, ids)
	}

	var route queryRoute
	route.namedEntity, route.entityDocumentID = MatchNamedEntity(qc.OriginalQuery, previews, ids)
	decision := s.ret.DetermineLegacyRoute(len(qc.SubQueries), first.Score, route.namedEntity, string(first.Chunk.DocumentID), route.entityDocumentID)
	route.mode = string(decision.Mode)
	route.reason = decision.Reason
	route.localDocumentID = decision.LocalDocumentID
	return route
}

func (s *RoutingStage) selectEvidenceRoute(qc *QueryContext, previews map[string]string, ids []string) queryRoute {
	docs := retriever.AggregateDocumentScores(qc.Candidates)
	signals := analyzeQuerySignals(qc.OriginalQuery, qc.SubQueries)
	signals.ProbeSaturated, signals.ProbeCutoff = qc.probeSaturated, qc.probeCutoff
	em := MatchNamedEntityEvidence(qc.OriginalQuery, previews, ids)
	entity := retriever.EntityDecision{
		DocumentID: em.DocumentID,
		Ambiguous:  em.Ambiguous,
		Negated:    em.Negated,
	}
	decision := s.ret.DetermineEvidenceRoute(s.cfg, signals, docs, entity)
	route := queryRoute{
		mode:                 string(decision.Mode),
		reason:               decision.Reason,
		localDocumentID:      decision.LocalDocumentID,
		previewNegativeMatch: em.NegativeChecked,
	}
	if decision.Mode == retriever.RetrievalModeLocal && decision.Reason == retriever.RouteReasonNamedEntity {
		route.namedEntity = em.Entity
		route.entityDocumentID = em.DocumentID
	}
	return route
}

var (
	strongMultiCueRe       = regexp.MustCompile(`(?i)(?:^|[^\p{L}\p{N}_-])(?:compare|compares|comparing|comparison|versus|vs|differences|difference|compara|comparar|comparacion|diferencia|diferencias)(?:$|[^\p{L}\p{N}_-])`)
	pluralScopeRequestRe   = regexp.MustCompile(`(?i)(?:^|[^\p{L}\p{N}_])(?:which|what|list|listing|find|show|cuales|que|lista|listar)(?:[^\p{L}\p{N}_]+(?:\p{L}+)){0,2}[^\p{L}\p{N}_]+(?:documents|reports|files|guides|manuals|records|policies|archives|documentos|informes|archivos|guias|expedientes|normas)(?:$|[^\p{L}\p{N}_])`)
	followUpRequestCueRe   = regexp.MustCompile(`^(?:what|who|where|when|why|how|which|is|are|was|were|do|does|did|can|could|will|would|has|have|should|must|may|describe|explain|list|tell|give|summarize|compare|check|find|que|cual|cuales|quien|donde|cuando|como|explica|dime|lista|compara|resume)(?:$|[^\p{L}\p{N}_])`)
	coordinatedRequestRe   = regexp.MustCompile(`(?:^|[^\p{L}\p{N}_])(?:and|or|y|o|e)\s+(?:what|who|where|when|why|how|which|que|cual|cuales|quien|donde|cuando|como)(?:$|[^\p{L}\p{N}_])`)
	numberedMultiRequestRe = regexp.MustCompile(`(?s)(?:^|\s)1[.)][ \t]+\S.*?\s2[.)][ \t]+\S`)
)

func analyzeQuerySignals(query string, subQueries []string) retriever.QuerySignals {
	normalized := foldLatinMarks(strings.ToLower(stripFormatChars(width.Fold.String(norm.NFC.String(query)))))
	return retriever.QuerySignals{
		ExplicitMultiRequest: hasExplicitMultiRequestPunctuation(normalized) || coordinatedRequestRe.MatchString(normalized),
		StrongMultiNeedCue:   strongMultiCueRe.MatchString(normalized) || pluralScopeRequestRe.MatchString(normalized),
		DistinctSubQueries:   hasDistinctSubQueries(subQueries),
	}
}

func hasDistinctSubQueries(subQueries []string) bool {
	if len(subQueries) <= 1 {
		return false
	}
	first := strings.TrimSpace(subQueries[0])
	for _, sq := range subQueries[1:] {
		if strings.TrimSpace(sq) != first {
			return true
		}
	}
	return false
}

func hasExplicitMultiRequestPunctuation(normalized string) bool {
	if numberedMultiRequestRe.MatchString(normalized) {
		return true
	}
	// Multiple closing question marks (ignoring repeated runs like "???").
	qRuns := 0
	inRun := false
	firstQEnd := -1
	for i, r := range normalized {
		isQ := r == '?' || r == '？' || r == '؟'
		if isQ {
			if !inRun {
				qRuns++
				if qRuns == 1 {
					firstQEnd = i + utf8.RuneLen(r)
				}
			}
			inRun = true
			continue
		}
		if inRun {
			firstQEnd = i
			inRun = false
		}
	}
	if qRuns >= 2 {
		return true
	}
	if qRuns == 1 && firstQEnd > 0 && firstQEnd < len(normalized) {
		tail := strings.TrimLeft(strings.TrimSpace(normalized[firstQEnd:]), "¿¡.,;:!?？؟\"'()[] ")
		if tail != "" && followUpRequestCueRe.MatchString(tail) {
			return true
		}
	}
	// Semicolon separating non-trivial word-bearing clauses.
	for _, sep := range []string{";", "；"} {
		if strings.Contains(normalized, sep) {
			parts := strings.Split(normalized, sep)
			wordClauses := 0
			for _, part := range parts {
				if strings.ContainsFunc(part, unicode.IsLetter) || strings.ContainsFunc(part, unicode.IsDigit) {
					wordClauses++
				}
			}
			if wordClauses >= 2 {
				return true
			}
		}
	}
	return false
}

func candidateDocumentIDs(chunks []domain.ScoredChunk) []string {
	ids := make([]string, 0, len(chunks))
	seen := make(map[string]struct{}, len(chunks))
	for _, sc := range chunks {
		id := string(sc.Chunk.DocumentID)
		if _, ok := seen[id]; !ok {
			ids = append(ids, id)
			seen[id] = struct{}{}
		}
	}
	return ids
}

// documentKeys records the document keys the vector store returned for each
// canonical chunk. A metadata filter compares keys byte for byte, so a targeted
// search must use the spelling the store holds, not the canonical UUID.
type documentKeys map[domain.ChunkID][]string

func providerDocumentKeys(chunks []domain.ScoredChunk) documentKeys {
	keys := make(documentKeys, len(chunks))
	for _, sc := range chunks {
		id := sc.Chunk.ID
		if parsed, err := uuid.Parse(string(id)); err == nil {
			id = domain.ChunkID(parsed.String())
		}
		if key := string(sc.Chunk.DocumentID); !slices.Contains(keys[id], key) {
			keys[id] = append(keys[id], key)
		}
	}
	return keys
}

// spellings lists the stored spellings of target among verified chunks, in
// score order without repeats. Without a recorded key the canonical ID stands.
func (k documentKeys) spellings(verified []domain.ScoredChunk, target string) []string {
	var spellings []string
	for _, sc := range verified {
		if string(sc.Chunk.DocumentID) != target {
			continue
		}
		for _, key := range k[sc.Chunk.ID] {
			if !slices.Contains(spellings, key) {
				spellings = append(spellings, key)
			}
		}
	}
	if len(spellings) == 0 {
		return []string{target}
	}
	return spellings
}

// canonicalEvidenceIDs makes UUID aliases comparable within automatic evidence
// routing while keeping the vector store's original identifiers intact.
func canonicalEvidenceIDs(chunks []domain.ScoredChunk) []domain.ScoredChunk {
	normalized := slices.Clone(chunks)
	for i := range normalized {
		ch := &normalized[i].Chunk
		if id, err := uuid.Parse(string(ch.DocumentID)); err == nil {
			ch.DocumentID = domain.DocumentID(id.String())
		}
		if id, err := uuid.Parse(string(ch.ID)); err == nil {
			ch.ID = domain.ChunkID(id.String())
		}
	}
	return normalized
}

func scopedChunks(ctx context.Context, qc *QueryContext, chunks []domain.ScoredChunk, visibility domain.DocumentVisibility) ([]domain.ScoredChunk, error) {
	if !qc.checksCurrentEvidence() && !qc.evidenceAutoRouting() {
		return visibleChunks(ctx, filterChunksByAllowedDocIDs(chunks, qc.AllowedDocumentIDs), visibility)
	}
	chunks = filterEvidenceScope(qc.vectorFilter(), chunks)
	if qc.EvidenceVisibility != nil && len(chunks) != 0 {
		return qc.EvidenceVisibility.VisibleEvidence(ctx, chunks)
	}
	return visibleChunks(ctx, chunks, visibility)
}

func (qc *QueryContext) vectorFilter() domain.VectorFilter {
	return domain.VectorFilter{DocType: qc.FilterDocType, DocumentIDs: qc.AllowedDocumentIDs, Collection: qc.Collection}
}

func (qc *QueryContext) evidenceAutoRouting() bool {
	return qc.RoutingPolicy == string(retriever.RoutingPolicyEvidence) &&
		(qc.RequestedMode == "" || strings.EqualFold(qc.RequestedMode, "auto"))
}

func (qc *QueryContext) checksCurrentEvidence() bool {
	return qc.HostedVisibilityRefill || qc.AnswerMode == domain.AnswerModeEvidence ||
		qc.evidenceAutoRouting() && qc.EvidenceVisibility != nil
}

func filterEvidenceScope(filter domain.VectorFilter, chunks []domain.ScoredChunk) []domain.ScoredChunk {
	chunks = filterChunksByAllowedDocIDs(chunks, filter.DocumentIDs)
	if filter.Collection != "" || filter.DocType != "" {
		filtered := make([]domain.ScoredChunk, 0, len(chunks))
		for _, sc := range chunks {
			collection, _ := sc.Chunk.Metadata["collection"].(string)
			docType, _ := sc.Chunk.Metadata["doc_type"].(string)
			if (filter.Collection == "" || collection == filter.Collection) && (filter.DocType == "" || docType == filter.DocType) {
				filtered = append(filtered, sc)
			}
		}
		chunks = filtered
	}
	return chunks
}

// checkStrictEvidence revalidates the hosted candidate scope and the current
// rows behind the excerpts that a strict request would send.
func (qc *QueryContext) checkStrictEvidence(ctx context.Context, excerpts []domain.ScoredChunk) error {
	if err := qc.CheckEvidenceScope(ctx); err != nil {
		return err
	}
	if qc.EvidenceVisibility == nil {
		return nil
	}
	return qc.checkSynthesisEvidence(ctx, excerpts)
}

func (qc *QueryContext) checkSynthesisEvidence(ctx context.Context, excerpts []domain.ScoredChunk) error {
	originals := make([]domain.ScoredChunk, 0, len(excerpts))
	for _, excerpt := range excerpts {
		i := slices.IndexFunc(qc.RetrievedChunks, func(original domain.ScoredChunk) bool {
			return original.Chunk.ID == excerpt.Chunk.ID && original.Chunk.DocumentID == excerpt.Chunk.DocumentID &&
				original.Chunk.Index == excerpt.Chunk.Index
		})
		if i < 0 {
			return ErrEvidenceScopeChanged
		}
		originals = append(originals, qc.RetrievedChunks[i])
	}
	if len(filterEvidenceScope(qc.vectorFilter(), originals)) != len(originals) {
		return ErrEvidenceScopeChanged
	}
	current, err := qc.EvidenceVisibility.VisibleEvidence(ctx, originals)
	if err != nil {
		return fmt.Errorf("synthesis evidence: %w", err)
	}
	if len(current) != len(originals) {
		return ErrEvidenceScopeChanged
	}
	for i, original := range originals {
		row := current[i].Chunk
		if row.ID != original.Chunk.ID || row.DocumentID != original.Chunk.DocumentID ||
			row.Index != original.Chunk.Index || row.Text != original.Chunk.Text {
			return ErrEvidenceScopeChanged
		}
	}
	return ctx.Err()
}

// CheckEvidenceScope verifies the original candidate bodies before a response
// releases evidence or diagnostics derived from it.
func (qc *QueryContext) CheckEvidenceScope(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !qc.checksCurrentEvidence() {
		return nil
	}
	// Probe candidates can influence the public retrieval mode even when
	// retrieval returns no evidence.
	candidates := slices.Concat(qc.Candidates, qc.RetrievedChunks)
	if len(candidates) == 0 {
		return nil
	}
	visible := filterEvidenceScope(qc.vectorFilter(), candidates)
	var previews map[string]string
	if qc.EvidenceVisibility != nil && len(visible) != 0 {
		var probeIDs []string
		if qc.previewRouting != nil {
			probeIDs = qc.previewRouting.documentIDs
		}
		var err error
		visible, previews, err = qc.EvidenceVisibility.VisibleEvidenceWithPreviews(ctx, visible, probeIDs)
		if err != nil {
			return fmt.Errorf("evidence response: %w", err)
		}
	}
	if len(visible) != len(candidates) {
		return ErrEvidenceScopeChanged
	}
	for i, candidate := range candidates {
		docType, _ := candidate.Chunk.Metadata["doc_type"].(string)
		currentDocType, _ := visible[i].Chunk.Metadata["doc_type"].(string)
		if docType != currentDocType {
			return ErrEvidenceScopeChanged
		}
	}
	if routing := qc.previewRouting; routing != nil {
		if previews == nil && routing.stage.previews != nil {
			var err error
			previews, err = routing.stage.previews.DocumentPreviews(ctx, routing.documentIDs)
			if err != nil {
				return fmt.Errorf("evidence response: previews: %w", err)
			}
		}
		current := routing.stage.selectRoute(qc, previews, routing.documentIDs)
		if current.mode != routing.original.mode || current.localDocumentID != routing.original.localDocumentID {
			return ErrEvidenceScopeChanged
		}
	}
	return ctx.Err()
}

func visibleChunks(ctx context.Context, chunks []domain.ScoredChunk, visibility domain.DocumentVisibility) ([]domain.ScoredChunk, error) {
	if visibility == nil || len(chunks) == 0 {
		return chunks, nil
	}
	visible, err := visibility.VisibleDocumentIDs(ctx, candidateDocumentIDs(chunks))
	if err != nil {
		return nil, err
	}
	filtered := make([]domain.ScoredChunk, 0, len(chunks))
	for _, sc := range chunks {
		if _, ok := visible[string(sc.Chunk.DocumentID)]; ok {
			filtered = append(filtered, sc)
		}
	}
	return filtered, nil
}

// RetrievalStage retrieves chunks from Pinecone using the determined mode.
type RetrievalStage struct {
	ret        *retriever.Retriever
	visibility domain.DocumentVisibility
}

// NewRetrievalStage creates a RetrievalStage.
func NewRetrievalStage(ret *retriever.Retriever) *RetrievalStage {
	return &RetrievalStage{ret: ret}
}

// NewRetrievalStageWithVisibility creates a stage that checks document availability.
func NewRetrievalStageWithVisibility(ret *retriever.Retriever, vis domain.DocumentVisibility) *RetrievalStage {
	return &RetrievalStage{ret: ret, visibility: vis}
}

// ID returns the stage identifier.
func (s *RetrievalStage) ID() StageID { return StageRetrieval }

// Run retrieves chunks based on the routing decision. Reuses the
// embedding the routing stage already produced for the same query
// to skip one Bedrock embed call per request.
func (s *RetrievalStage) Run(ctx context.Context, qc *QueryContext) error {
	if qc.SkipRetrieval {
		qc.RetrievedChunks = nil
		qc.TopScore = 0
		return nil
	}
	filter := qc.vectorFilter()
	mode := retriever.RetrievalMode(qc.RetrievalMode)
	if len(qc.QueryEmbedding) == 0 {
		embedding, _, _, err := s.ret.EmbedQuery(ctx, qc.OriginalQuery, filter, 0)
		if err != nil {
			return fmt.Errorf("retrieval_stage: %w", err)
		}
		qc.QueryEmbedding = embedding
	}
	qc.RetrievedChunks = nil
	qc.TopScore = 0
	result, err := s.retrieve(ctx, qc, mode, filter)
	if err != nil {
		return fmt.Errorf("retrieval_stage: %w", err)
	}
	qc.RetrievedChunks = result.Chunks
	qc.RetrievalMode = string(result.Mode)
	if len(result.Chunks) > 0 {
		qc.TopScore = result.Chunks[0].Score
		for _, sc := range result.Chunks[1:] {
			qc.TopScore = max(qc.TopScore, sc.Score)
		}
	}
	return nil
}

func (s *RetrievalStage) retrieve(ctx context.Context, qc *QueryContext, mode retriever.RetrievalMode, filter domain.VectorFilter) (*retriever.Result, error) {
	if qc.evidenceAutoRouting() {
		return s.retrieveRoutedEvidence(ctx, qc, mode, filter)
	}
	if qc.HostedVisibilityRefill {
		return s.ret.RetrieveVisibleWithEmbedding(ctx, qc.QueryEmbedding, mode, qc.LocalDocumentID, qc.MaxChunks, filter, qc.HostedVisibilityExtraRequests, qc.verifyHostedEvidence)
	}
	result, err := s.ret.RetrieveWithEmbedding(ctx, qc.QueryEmbedding, mode, qc.LocalDocumentID, qc.MaxChunks, filter)
	if err != nil {
		return nil, err
	}
	result.Chunks, err = scopedChunks(ctx, qc, result.Chunks, s.visibility)
	if err != nil {
		return nil, fmt.Errorf("visibility: %w", err)
	}
	return result, nil
}

func (qc *QueryContext) verifyHostedEvidence(ctx context.Context, chunks []domain.ScoredChunk, scope domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	chunks = filterEvidenceScope(scope, chunks)
	if len(chunks) == 0 {
		return chunks, false, nil
	}
	visible, err := qc.EvidenceVisibility.VisibleEvidence(ctx, chunks)
	return visible, len(visible) < len(chunks), err
}

func (s *RetrievalStage) retrieveRoutedEvidence(ctx context.Context, qc *QueryContext, mode retriever.RetrievalMode, filter domain.VectorFilter) (*retriever.Result, error) {
	var result *retriever.Result
	var err error
	local := mode == retriever.RetrievalModeLocal
	switch {
	case local:
		result, err = s.ret.RetrieveEvidenceLocal(ctx, qc.QueryEmbedding, qc.LocalDocumentID, qc.localDocumentKeys, qc.MaxChunks, filter)
	case qc.scopedSearchReady:
		result = &retriever.Result{Mode: mode, Chunks: qc.scopedSearch}
	default:
		result, err = s.ret.RetrieveWithEmbedding(ctx, qc.QueryEmbedding, mode, qc.LocalDocumentID, qc.MaxChunks, filter)
	}
	if err != nil {
		return nil, err
	}
	input := result.Chunks
	if local || !qc.scopedSearchReady {
		input = canonicalEvidenceIDs(input)
	}
	if local {
		input = slices.Concat(filterChunksByAllowedDocIDs(input, []string{qc.LocalDocumentID}), qc.scopedSearch)
	}
	chunks, err := scopedChunks(ctx, qc, input, s.visibility)
	if err != nil {
		return nil, fmt.Errorf("visibility: %w", err)
	}
	chunks = uniqueEvidenceChunks(chunks)
	if qc.scopedSearchReady && !local {
		// The cached search is verified in score order before K applies, so a
		// stale or aliased leading hit cannot hide a current chunk fetched with it.
		chunks = chunks[:min(len(chunks), s.ret.ChunkLimit(qc.MaxChunks))]
	}
	if local {
		chunks = selectLocalEvidence(chunks, qc.LocalDocumentID, s.ret.ChunkLimit(qc.MaxChunks))
		if len(chunks) == 0 {
			return nil, ErrEvidenceScopeChanged
		}
	}
	result.Chunks = chunks
	return result, nil
}

func uniqueEvidenceChunks(chunks []domain.ScoredChunk) []domain.ScoredChunk {
	seen := make(map[domain.ChunkID]struct{}, len(chunks))
	unique := make([]domain.ScoredChunk, 0, len(chunks))
	for _, sc := range chunks {
		if _, duplicate := seen[sc.Chunk.ID]; duplicate {
			continue
		}
		seen[sc.Chunk.ID] = struct{}{}
		unique = append(unique, sc)
	}
	return unique
}

// selectLocalEvidence keeps target chunks ahead of any scope-wide fill.
func selectLocalEvidence(chunks []domain.ScoredChunk, target string, limit int) []domain.ScoredChunk {
	var primary, fill []domain.ScoredChunk
	for _, sc := range chunks {
		if string(sc.Chunk.DocumentID) == target {
			primary = append(primary, sc)
		} else {
			fill = append(fill, sc)
		}
	}
	if len(primary) == 0 {
		return nil
	}
	if len(primary) < min(3, limit) {
		primary = append(primary, fill...)
	}
	return primary[:min(len(primary), limit)]
}

// filterChunksByAllowedDocIDs restricts chunks to a caller-supplied document-id
// allowlist. Nil means no restriction; a non-nil empty list matches nothing.
// Chunks retain their original score order.
func filterChunksByAllowedDocIDs(chunks []domain.ScoredChunk, allowed []string) []domain.ScoredChunk {
	if allowed == nil {
		return chunks
	}
	allowSet := make(map[string]struct{}, len(allowed))
	for _, id := range allowed {
		allowSet[id] = struct{}{}
	}
	out := make([]domain.ScoredChunk, 0, len(chunks))
	for _, sc := range chunks {
		if _, ok := allowSet[string(sc.Chunk.DocumentID)]; ok {
			out = append(out, sc)
		}
	}
	return out
}

// RerankStage optionally reorders retrieved evidence before context selection.
type RerankStage struct {
	reranker retriever.Reranker
	timeout  time.Duration
}

// NewRerankStage creates a stage with a separate budget for optional reranking.
func NewRerankStage(r retriever.Reranker, timeout time.Duration) *RerankStage {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &RerankStage{reranker: r, timeout: timeout}
}

// ID returns the stage identifier.
func (s *RerankStage) ID() StageID { return StageRerank }

// Run preserves vector order on an upstream rerank failure and propagates
// evidence-check failures.
func (s *RerankStage) Run(ctx context.Context, qc *QueryContext) error {
	if qc.AnswerMode == domain.AnswerModeEvidence || s.reranker == nil || len(qc.RetrievedChunks) == 0 {
		return nil
	}
	rerankCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rerankCtx = qc.withProviderEvidenceCheck(rerankCtx)
	if scopeErr := domain.CheckEvidenceBeforeSend(rerankCtx); scopeErr != nil &&
		(qc.HostedVisibilityRefill || ctx.Err() != nil || rerankCtx.Err() == nil || !errors.Is(scopeErr, context.DeadlineExceeded)) {
		return fmt.Errorf("rerank_stage: %w", scopeErr)
	}
	var chunks []domain.ScoredChunk
	err := rerankCtx.Err()
	if err == nil {
		chunks, err = s.reranker.Rerank(rerankCtx, qc.OriginalQuery, slices.Clone(qc.RetrievedChunks))
		// The SDK can report cancellation in place of the visibility error.
		if guardErr := domain.RecordedEvidenceCheckError(rerankCtx); guardErr != nil {
			return fmt.Errorf("rerank_stage: %w", guardErr)
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("rerank_stage: %w", ctxErr)
	}
	if errors.Is(err, domain.ErrEvidenceCheck) {
		return fmt.Errorf("rerank_stage: %w", err)
	}
	if err == nil {
		err = rerankCtx.Err()
	}
	if err != nil {
		attrs := []any{"reason", "upstream"}
		if rerankCtx.Err() != nil {
			attrs[1] = "timeout"
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			attrs = append(attrs, "error_code", apiErr.ErrorCode())
		}
		slog.WarnContext(ctx, "reranking failed; retaining vector order", attrs...)
		metrics.FromContext(ctx).Rerank.WithLabelValues("fallback").Inc()
		return nil
	}
	qc.RetrievedChunks = chunks
	metrics.FromContext(ctx).Rerank.WithLabelValues("reranked").Inc()
	return nil
}

// SynthesisStage synthesizes a final answer from retrieved chunks.
// Model selection follows the configured synthesis routing policy.
type SynthesisStage struct {
	ambassador        *llm.Ambassador
	cfg               *config.Config
	noEvidenceCatalog *NoEvidenceCatalog
}

// NewSynthesisStage creates a SynthesisStage.
func NewSynthesisStage(amb *llm.Ambassador, cfg *config.Config) *SynthesisStage {
	return NewSynthesisStageWithCatalog(amb, cfg, nil)
}

// NewSynthesisStageWithCatalog creates a synthesis stage with fixed no-evidence translations.
func NewSynthesisStageWithCatalog(amb *llm.Ambassador, cfg *config.Config, catalog *NoEvidenceCatalog) *SynthesisStage {
	return &SynthesisStage{ambassador: amb, cfg: cfg, noEvidenceCatalog: catalog}
}

// ID returns the stage identifier.
func (s *SynthesisStage) ID() StageID { return StageSynthesis }

// trimChunksForSynthesis budgets sanitized excerpts in candidate order.
// A partial final excerpt needs a sentence boundary and at least 64 tokens;
// the first excerpt is always retained, with a hard cut if necessary.
func trimChunksForSynthesis(in []domain.ScoredChunk, budget int, relativeFloor float64, requireEvidence bool) (out []domain.ScoredChunk, truncated []bool) {
	if requireEvidence {
		usable := make([]domain.ScoredChunk, 0, len(in))
		for _, sc := range in {
			sc.Chunk.Text = sanitizeExcerpt(sc.Chunk.Text)
			if strings.TrimSpace(sc.Chunk.Text) != "" {
				usable = append(usable, sc)
			}
		}
		in = usable
	}
	if len(in) == 0 {
		return in, nil
	}
	if budget <= 0 {
		budget, relativeFloor = 2400, 0.55
	}
	// Reranking may reorder candidates: the floor follows the best vector score
	// and skips weaker candidates instead of ending selection.
	top := in[0].Score
	for _, sc := range in[1:] {
		top = max(top, sc.Score)
	}
	floor := top * relativeFloor
	out = make([]domain.ScoredChunk, 0, len(in))
	truncated = make([]bool, 0, len(in))
	remaining := budget
	for _, sc := range in {
		if sc.Score < floor && len(out) > 0 {
			continue
		}
		if !requireEvidence {
			sc.Chunk.Text = sanitizeExcerpt(sc.Chunk.Text)
		}
		tokens := llm.EstimateTokens(sc.Chunk.Text)
		if tokens > remaining {
			if remaining < 64 && len(out) > 0 {
				break
			}
			runes := []rune(sc.Chunk.Text)
			cut := sort.Search(len(runes)+1, func(n int) bool {
				return llm.EstimateTokens(string(runes[:n])) > remaining
			}) - 1
			body := string(runes[:cut])
			if end := strings.LastIndexAny(body, ".\n"); end >= 0 && llm.EstimateTokens(body[:end+1]) >= 64 {
				body = body[:end+1]
			} else if len(out) > 0 {
				break
			}
			if requireEvidence && strings.TrimSpace(body) == "" {
				continue
			}
			sc.Chunk.Text = body
			out, truncated = append(out, sc), append(truncated, true)
			break
		}
		out, truncated = append(out, sc), append(truncated, false)
		remaining -= tokens
	}
	return out, truncated
}

// Run synthesizes a grounded answer in the language of the question.
func (s *SynthesisStage) Run(ctx context.Context, qc *QueryContext) error {
	timeout := 45 * time.Second
	if s.cfg != nil && s.cfg.SynthesisTimeout > 0 {
		timeout = s.cfg.SynthesisTimeout
	}
	synthesisCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	qc.Excerpts, qc.Truncated = nil, nil
	qc.Citations = nil
	if qc.IncludeCitations {
		qc.Citations = make([]Citation, 0)
	}
	minScore := 0.0
	if s.cfg != nil {
		minScore = s.cfg.MinRelevanceScore
	}
	if minScore > 0 {
		filtered := make([]domain.ScoredChunk, 0, len(qc.RetrievedChunks))
		for _, sc := range qc.RetrievedChunks {
			if sc.Score >= minScore {
				filtered = append(filtered, sc)
			}
		}
		qc.RetrievedChunks = filtered
		qc.TopScore = 0
		if len(filtered) > 0 {
			qc.TopScore = filtered[0].Score
			for _, sc := range filtered[1:] {
				qc.TopScore = max(qc.TopScore, sc.Score)
			}
		}
	}
	if len(qc.RetrievedChunks) == 0 {
		return s.finishWithoutEvidence(synthesisCtx, qc)
	}

	strict := s.cfg.SynthesisInputProfile != "" && qc.AnswerMode != domain.AnswerModeEvidence
	chunks, truncated := trimChunksForSynthesis(qc.RetrievedChunks, s.cfg.SynthesisContextTokens, s.cfg.SynthesisRelativeScoreFloor, strict)
	if strict && len(chunks) == 0 {
		return s.finishWithoutEvidence(synthesisCtx, qc)
	}
	qc.ContextChunks = len(chunks)
	qc.Truncated = truncated
	qc.Excerpts = chunks
	qc.CountMethod = ""
	qc.ModelInputTokens = 0
	if qc.AnswerMode == domain.AnswerModeEvidence {
		qc.FinishReason = domain.FinishReasonEvidenceOnly
		qc.FinalAnswer = ""
		qc.TokenUsage = domain.TokenUsage{}
		qc.ContentLanguage = ""
		qc.ContextTokens = 0
		for _, sc := range chunks {
			qc.ContextTokens += llm.EstimateTokens(sc.Chunk.Text)
		}
		if qc.Stream != nil {
			return qc.Stream.start(synthesisCtx)
		}
		return synthesisCtx.Err()
	}

	var boundary string
	switch {
	case !strict:
		boundary = newBoundary()
	case s.cfg.LLMProvider == "openai":
		boundary = newCountingBoundary()
	default:
		boundary = bedrockCountingBoundary(s.cfg.ServiceTokens, qc.OriginalQuery, chunks)
	}
	builder := synthesisPromptBuilder{
		question: qc.OriginalQuery, includeCitations: qc.IncludeCitations,
		boundary: boundary, guardrail: s.cfg.BedrockGuardrailID != "",
		maxTokens: answerSynthesisMaxOutput,
	}
	request, system, prompt := builder.build(chunks)

	tokenEst := llm.EstimateTokens(prompt) + llm.EstimateTokens(system)
	qc.ContextTokens = tokenEst

	// SubQueries length > 1 is our cheap multi-need / aggregation
	// signal (set by the decomposition heuristic). The hybrid router
	// uses it to send aggregation queries to the stronger model.
	multiNeed := len(qc.SubQueries) > 1
	modelID, _ := llm.Route(s.cfg, llm.RoutingRequest{
		TaskType:      llm.TaskSynthesis,
		ContextTokens: tokenEst,
		MultiNeed:     multiNeed,
	})

	request.Model = modelID

	// Synthesis is free-form text — never JSON. Use the plain path so
	// the ambassador's JSON-correction retry loop is bypassed even when
	// SYNTHESIS_MODEL=primary routes us through Nova Micro.
	if strict {
		profile, err := s.ambassador.ResolveInputProfile(s.cfg, modelID)
		if err != nil {
			return fmt.Errorf("synthesis_stage: %w", translatePipelineCountError(err))
		}
		if transportErr := s.ambassador.ValidateSynthesisTransport(profile, qc.Stream != nil); transportErr != nil {
			return fmt.Errorf("synthesis_stage: %w", translatePipelineCountError(transportErr))
		}
		reservedOutput, err := resolveReservedOutput(profile, answerSynthesisMaxOutput)
		if err != nil {
			return fmt.Errorf("synthesis_stage: %w", err)
		}
		inputBudget := profile.ContextWindow - reservedOutput
		if inputBudget <= 0 {
			return fmt.Errorf("synthesis_stage: %w: output reserve %d exceeds window %d", ErrSynthesisInputTooLarge, reservedOutput, profile.ContextWindow)
		}
		builder.modelID, builder.maxTokens = modelID, reservedOutput
		if qc.EvidenceVisibility != nil || qc.HostedVisibilityRefill {
			builder.evidenceCheck = qc.checkStrictEvidence
		}
		outcome, err := budgetStrictSynthesisInput(synthesisCtx, s.ambassador, s.cfg, builder, profile, inputBudget, chunks, truncated)
		if err != nil {
			return fmt.Errorf("synthesis_stage: %w", err)
		}
		qc.Excerpts = outcome.chunks
		qc.Truncated = outcome.truncated
		qc.ContextChunks = len(outcome.chunks)
		qc.ContextTokens = llm.EstimateTokens(outcome.prompt) + llm.EstimateTokens(outcome.system)
		qc.CountMethod = string(outcome.count.Method)
		qc.ModelInputTokens = outcome.count.InputTokens
		request = outcome.request
		synthesisCtx = builder.disclosureContext(synthesisCtx, outcome.chunks)
		if err := domain.CheckEvidenceBeforeSend(synthesisCtx); err != nil {
			return fmt.Errorf("synthesis_stage: %w", err)
		}
	}

	result, err := s.converse(synthesisCtx, qc, request)
	if err != nil {
		return fmt.Errorf("synthesis_stage: %w", err)
	}

	if err := synthesisCtx.Err(); err != nil {
		return fmt.Errorf("synthesis_stage: %w", err)
	}
	qc.FinalAnswer = result.Text
	if strings.TrimSpace(qc.FinalAnswer) == "" {
		return ErrSynthesisEmpty
	}
	if qc.IncludeCitations {
		if qc.Stream == nil {
			qc.FinalAnswer, qc.Citations = VerifyCitations(qc.FinalAnswer, qc.Excerpts)
		} else {
			qc.Citations = VerifyStreamCitations(qc.FinalAnswer, qc.Excerpts)
		}
	}
	switch result.StopReason {
	case "end_turn", "stop_sequence":
		qc.FinishReason = "stop"
	case "max_tokens":
		qc.FinishReason = "max_tokens"
	case "content_filtered", "guardrail_intervened":
		qc.FinishReason = "filtered"
	case "model_context_window_exceeded":
		qc.FinishReason = "context_exceeded"
	}

	// Track token usage.
	if result.Usage != nil {
		qc.TokenUsage.InputTokens = result.Usage.InputTokens
		qc.TokenUsage.OutputTokens = result.Usage.OutputTokens
		qc.TokenUsage.TotalTokens = qc.TokenUsage.InputTokens + qc.TokenUsage.OutputTokens
	}

	slog.DebugContext(ctx, "synthesis completed", "mode", qc.RetrievalMode, "chunks", len(qc.RetrievedChunks), "answer_bytes", len(qc.FinalAnswer))
	return nil
}
