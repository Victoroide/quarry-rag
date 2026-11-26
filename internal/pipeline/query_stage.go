package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
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
	qc.scopedSearch, qc.scopedSearchK = nil, 0
	qc.probeSaturated, qc.probeCutoff = false, nil
	qc.localDocumentKeys = nil
	policy := s.cfg.Policy
	if policy == "" {
		policy = retriever.RoutingPolicyLegacy
	}
	qc.RoutingPolicy = string(policy)
	qc.untaggedIncarnations = nil
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
	if searchK > 0 {
		if err := qc.observeUntaggedEvidence(ctx, s.ret); err != nil {
			return fmt.Errorf("routing_stage: %w", err)
		}
	}
	embedQuery := s.ret.EmbedQuery
	if qc.EvidenceVisibility != nil {
		embedQuery = s.ret.EmbedQueryWithVisibility
	}
	if qc.evidenceAutoRouting() && qc.HostedVisibilityRefill {
		embedQuery = s.ret.EmbedQueryForVisibilityRefill
	}
	embedding, candidates, probeFailed, err := embedQuery(ctx, qc.OriginalQuery, filter, searchK)
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
		rawProbe := candidates[:min(len(candidates), probeK)]
		candidates = canonicalEvidenceIDs(candidates)
		qc.scopedSearch, qc.scopedSearchK = candidates, searchK
		qc.probeSaturated = len(candidates) >= probeK && len(filter.DocumentIDs) != 1
		candidates = candidates[:len(rawProbe)]
		providerKeys = providerDocumentKeys(rawProbe, candidates)
	}
	candidates, previews, err := s.visibleProbes(ctx, qc, candidates)
	if err != nil {
		return fmt.Errorf("routing_stage: %w", err)
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })
	candidates = candidates[:min(len(candidates), probeK)]
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

func (s *RoutingStage) visibleProbes(ctx context.Context, qc *QueryContext, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, map[string]string, error) {
	if qc.EvidenceVisibility != nil {
		chunks = filterEvidenceScope(qc.vectorFilter(), chunks)
		chunks = qc.matchObservedEvidence(chunks)
		var ids []string
		if s.previews != nil || qc.evidenceAutoRouting() {
			ids = candidateDocumentIDs(chunks)
		}
		return qc.EvidenceVisibility.VisibleEvidenceWithPreviews(ctx, chunks, ids)
	}
	visible, err := scopedChunks(ctx, qc, chunks, s.visibility)
	return visible, nil, err
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

type chunkDocument struct {
	chunkID    domain.ChunkID
	documentID domain.DocumentID
}

// documentKeys retains the byte-for-byte store keys by canonical chunk and
// document so stale attributions cannot add another document to a target search.
type documentKeys map[chunkDocument][]string

func providerDocumentKeys(raw, canonical []domain.ScoredChunk) documentKeys {
	keys := make(documentKeys, len(raw))
	for i, sc := range canonical {
		identity := chunkDocument{chunkID: sc.Chunk.ID, documentID: sc.Chunk.DocumentID}
		keys[identity] = append(keys[identity], string(raw[i].Chunk.DocumentID))
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
		identity := chunkDocument{chunkID: sc.Chunk.ID, documentID: sc.Chunk.DocumentID}
		for _, key := range k[identity] {
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

// canonicalDocumentIDs spells every UUID in an allowlist canonically. A nil
// allowlist stays nil, so it keeps meaning no restriction.
func canonicalDocumentIDs(ids []string) []string {
	if ids == nil {
		return nil
	}
	canonical := make([]string, len(ids))
	for i, raw := range ids {
		canonical[i] = raw
		if id, err := uuid.Parse(raw); err == nil {
			canonical[i] = id.String()
		}
	}
	return canonical
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
		return qc.EvidenceVisibility.VisibleEvidence(ctx, qc.matchObservedEvidence(chunks))
	}
	return visibleChunks(ctx, chunks, visibility)
}

func (qc *QueryContext) vectorFilter() domain.VectorFilter {
	filter := domain.VectorFilter{DocType: qc.FilterDocType, DocumentIDs: qc.AllowedDocumentIDs, Collection: qc.Collection}
	if qc.Authorization != nil {
		scope := qc.Authorization.Scope
		filter.Authorized = &scope
	}
	return filter
}

func (qc *QueryContext) evidenceAutoRouting() bool {
	return qc.RoutingPolicy == string(retriever.RoutingPolicyEvidence) &&
		(qc.RequestedMode == "" || strings.EqualFold(qc.RequestedMode, "auto"))
}

func (qc *QueryContext) checksCurrentEvidence() bool {
	return qc.Authorization != nil || qc.EvidenceVisibility != nil || qc.HostedVisibilityRefill || qc.RequireCitationScope || qc.AnswerMode == domain.AnswerModeEvidence
}

func (qc *QueryContext) observeUntaggedEvidence(ctx context.Context, ret *retriever.Retriever) error {
	observer, ok := qc.EvidenceVisibility.(domain.EvidenceIncarnationObserver)
	if !ok || qc.untaggedIncarnations != nil || ret.ObservesVectorIncarnation() {
		return nil
	}
	incarnations, err := observer.ObserveUntaggedIncarnations(ctx, qc.AllowedDocumentIDs)
	if err != nil {
		return err
	}
	qc.untaggedIncarnations = incarnations
	return nil
}

func (qc *QueryContext) matchObservedEvidence(chunks []domain.ScoredChunk) []domain.ScoredChunk {
	if qc.untaggedIncarnations == nil || len(chunks) == 0 {
		return chunks
	}
	// Historical external hits lack a write-time identity and are checked
	// against the SQL lifetime observed before searching.
	matched := make([]domain.ScoredChunk, 0, len(chunks))
	for _, hit := range chunks {
		if hit.Chunk.RevisionID != "" {
			if domain.ManifestIdentity(hit.Chunk) {
				matched = append(matched, hit)
			}
			continue
		}
		_, tagged := hit.Chunk.Metadata[domain.VectorRevisionKey]
		if hit.ObservedIncarnation == "" && hit.Chunk.VectorIncarnation == "" && !tagged {
			id, err := uuid.Parse(string(hit.Chunk.DocumentID))
			if err != nil {
				continue
			}
			hit.ObservedIncarnation = qc.untaggedIncarnations[id.String()]
			if hit.ObservedIncarnation == "" {
				continue
			}
		}
		matched = append(matched, hit)
	}
	return matched
}

func filterEvidenceScope(filter domain.VectorFilter, chunks []domain.ScoredChunk) []domain.ScoredChunk {
	chunks = filterChunksByAllowedDocIDs(chunks, filter.DocumentIDs)
	if filter.Authorized != nil {
		chunks = filterAuthorizedHits(filter.Authorized, chunks)
	}
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

// filterAuthorizedHits discards candidates whose own metadata contradicts the
// authorized scope. A hit naming another tenant is never usable. Under an
// explicit membership, a hit must name the tenant and carry either the listed
// incarnation or a hosted revision. Database checks still decide what is current.
func filterAuthorizedHits(scope *domain.AuthorizedScope, chunks []domain.ScoredChunk) []domain.ScoredChunk {
	kept := make([]domain.ScoredChunk, 0, len(chunks))
	for _, sc := range chunks {
		if scope.Incarnations == nil {
			if tenant, labeled := sc.Chunk.Metadata[domain.VectorTenantKey].(string); labeled && !strings.EqualFold(tenant, scope.TenantID) {
				continue
			}
		} else {
			incarnation, member := scope.IncarnationOf(string(sc.Chunk.DocumentID))
			revision, _ := sc.Chunk.Metadata[domain.VectorRevisionKey].(string)
			tagged := sc.Chunk.RevisionID == "" && revision == incarnation
			identityMatches := tagged || domain.ManifestIdentity(sc.Chunk)
			if !member || !scope.SameTenant(sc.Chunk.Metadata) || !identityMatches {
				continue
			}
		}
		kept = append(kept, sc)
	}
	return kept
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
	return qc.authorizeEvidenceError(ctx, qc.checkSynthesisEvidence(ctx, excerpts))
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
			row.Index != original.Chunk.Index || row.Text != original.Chunk.Text ||
			row.RevisionID != original.Chunk.RevisionID {
			return ErrEvidenceScopeChanged
		}
	}
	return ctx.Err()
}

// CheckEvidenceScope verifies the original candidate bodies before a response
// releases evidence or diagnostics derived from it. Under a verified principal
// it first checks the credential lifetime, key trust, principal and policy
// revision, so even a response without evidence is bounded by that check.
func (qc *QueryContext) CheckEvidenceScope(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if qc.Authorization == nil {
		return qc.checkCandidateEvidence(ctx)
	}
	if err := qc.Authorization.Check(ctx); err != nil {
		return err
	}
	if err := qc.authorizeEvidenceError(ctx, qc.checkCandidateEvidence(ctx)); err != nil {
		return err
	}
	// The evidence read can outlast the credential. A disclosure that follows
	// must not start at or after the exact expiry.
	if qc.Authorization.Expired() {
		return ErrCredentialExpired
	}
	return nil
}

// authorizeEvidenceError names a failed evidence check by its consequence for a
// verified principal. A changed document, grant or preview is one uniform
// denial. An unreadable database is unavailable authority, never a fallback.
func (qc *QueryContext) authorizeEvidenceError(ctx context.Context, err error) error {
	switch {
	case err == nil || qc.Authorization == nil:
		return err
	case errors.Is(err, ErrEvidenceScopeChanged):
		return ErrReadAuthorityChanged
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, ErrReadAuthorityChanged), errors.Is(err, ErrCredentialExpired), errors.Is(err, ErrAuthorizationUnavailable):
		return err
	}
	return fmt.Errorf("%w: %w", ErrAuthorizationUnavailable, err)
}

func (qc *QueryContext) checkCandidateEvidence(ctx context.Context) error {
	if !qc.checksCurrentEvidence() {
		return nil
	}
	// Probe candidates can influence the public retrieval mode even when
	// retrieval returns no evidence.
	retrieved := qc.releaseCandidates
	if retrieved == nil {
		retrieved = qc.RetrievedChunks
	}
	candidates := slices.Concat(qc.Candidates, retrieved)
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
	if revVis, ok := visibility.(domain.RevisionVisibility); ok {
		return revVis.VisibleRevisionChunks(ctx, chunks)
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
	if err := qc.observeUntaggedEvidence(ctx, s.ret); err != nil {
		return fmt.Errorf("retrieval_stage: %w", err)
	}
	result, err := s.retrieve(ctx, qc, mode, filter)
	qc.untaggedIncarnations = nil
	if err != nil {
		return fmt.Errorf("retrieval_stage: %w", err)
	}
	qc.RetrievedChunks = result.Chunks
	qc.releaseCandidates = slices.Clone(result.Chunks)
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
	check := func(ctx context.Context, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		return scopedChunks(ctx, qc, chunks, s.visibility)
	}
	return s.ret.RetrieveWithVisibility(ctx, qc.QueryEmbedding, mode, qc.LocalDocumentID, qc.MaxChunks, filter, check)
}

func (qc *QueryContext) verifyHostedEvidence(ctx context.Context, chunks []domain.ScoredChunk, scope domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	chunks = filterEvidenceScope(scope, chunks)
	if len(chunks) == 0 {
		return chunks, false, nil
	}
	visible, err := qc.EvidenceVisibility.VisibleEvidence(ctx, qc.matchObservedEvidence(chunks))
	return visible, len(visible) < len(chunks), err
}

// verifyRoutedHostedEvidence applies the logical scope to canonical IDs on both
// sides. A provider filter keeps the spellings under which the store holds its
// keys, so rows that an earlier check already canonicalized must still satisfy
// the same filter when a fallback checks them again.
func (qc *QueryContext) verifyRoutedHostedEvidence(ctx context.Context, chunks []domain.ScoredChunk, scope domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	scope.DocumentIDs = canonicalDocumentIDs(scope.DocumentIDs)
	return qc.verifyHostedEvidence(ctx, canonicalEvidenceIDs(chunks), scope)
}

func (s *RetrievalStage) retrieveRoutedEvidence(ctx context.Context, qc *QueryContext, mode retriever.RetrievalMode, filter domain.VectorFilter) (*retriever.Result, error) {
	if qc.HostedVisibilityRefill {
		if qc.scopedSearchK == 0 {
			return s.ret.RetrieveVisibleWithEmbedding(ctx, qc.QueryEmbedding, mode, qc.LocalDocumentID, qc.MaxChunks, filter, qc.HostedVisibilityExtraRequests, qc.verifyRoutedHostedEvidence)
		}
		probe := retriever.RoutedProbe{Candidates: qc.scopedSearch, SearchK: qc.scopedSearchK, Target: qc.LocalDocumentID, TargetKeys: qc.localDocumentKeys}
		result, err := s.ret.RetrieveVisibleFromProbe(ctx, qc.QueryEmbedding, probe, qc.MaxChunks, filter, qc.HostedVisibilityExtraRequests, qc.verifyRoutedHostedEvidence)
		if err == nil && result.Mode == retriever.RetrievalModeLocal && len(result.Chunks) == 0 {
			return nil, ErrEvidenceScopeChanged
		}
		return result, err
	}

	var result *retriever.Result
	var err error
	local := mode == retriever.RetrievalModeLocal
	switch {
	case local:
		result, err = s.ret.RetrieveEvidenceLocal(ctx, qc.QueryEmbedding, qc.LocalDocumentID, qc.localDocumentKeys, qc.MaxChunks, filter)
	case qc.scopedSearchK > 0:
		result = &retriever.Result{Mode: mode, Chunks: qc.scopedSearch}
	default:
		result, err = s.ret.RetrieveWithEmbedding(ctx, qc.QueryEmbedding, mode, qc.LocalDocumentID, qc.MaxChunks, filter)
	}
	if err != nil {
		return nil, err
	}
	input := result.Chunks
	if local || qc.scopedSearchK == 0 {
		input = canonicalEvidenceIDs(input)
	}
	if local {
		input = slices.Concat(filterChunksByAllowedDocIDs(input, []string{qc.LocalDocumentID}), qc.scopedSearch)
	}
	chunks, err := scopedChunks(ctx, qc, input, s.visibility)
	if err != nil {
		return nil, fmt.Errorf("visibility: %w", err)
	}
	// Apply K after verification and deduplication so the cache can retain a
	// current chunk that ranked behind a stale or aliased hit.
	result.Chunks = retriever.SelectRoutedChunks(chunks, qc.LocalDocumentID, s.ret.ChunkLimit(qc.MaxChunks))
	if local && len(result.Chunks) == 0 {
		return nil, ErrEvidenceScopeChanged
	}
	return result, nil
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
	citationVerifier  CitationVerifier
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

const (
	defaultSynthesisContextBudget = 2400
	defaultRelativeScoreFloor     = 0.55
	minPartialExcerptTokens       = 64
	maxSynthesisExcerpts          = 100
	contextShingleWidth           = 5
	maxShinglesPerChunk           = 1024
)

// trimChunksForSynthesis budgets sanitized excerpts using the configured policy.
// Empty or control-only bodies are dropped before selecting the leading usable
// passage. That leading passage is always retained under the first-candidate
// exception to the relative score floor, including after reranking, while later
// candidates must satisfy the vector floor and output chunk/byte caps.
func trimChunksForSynthesis(ctx context.Context, in []domain.ScoredChunk, budget int, relativeFloor float64, policy string) (out []domain.ScoredChunk, truncated []bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	usable := make([]domain.ScoredChunk, 0, len(in))
	for _, sc := range in {
		sc.Chunk.Text = sanitizeExcerpt(sc.Chunk.Text)
		if strings.TrimSpace(sc.Chunk.Text) != "" {
			usable = append(usable, sc)
		}
	}
	if len(usable) == 0 {
		return nil, nil, nil
	}
	if budget <= 0 {
		budget, relativeFloor = defaultSynthesisContextBudget, defaultRelativeScoreFloor
	}
	// Reranking may reorder candidates: the floor follows the best vector score
	// and skips weaker candidates instead of ending selection.
	top := 0.0
	hasTop := false
	for _, sc := range usable {
		if !math.IsNaN(sc.Score) && (!hasTop || sc.Score > top) {
			top = sc.Score
			hasTop = true
		}
	}
	floor := top * relativeFloor
	maxExcerpts := min(maxSynthesisExcerpts, max(1, budget))
	maxBytes := (budget*4 + maxExcerpts*3) * utf8.UTFMax

	if policy == config.SynthesisContextPolicyDiverse {
		return selectDiverseChunks(ctx, usable, budget, top, floor, maxExcerpts, maxBytes)
	}
	return selectRankedChunks(ctx, usable, budget, floor, maxExcerpts, maxBytes)
}

func selectRankedChunks(ctx context.Context, usable []domain.ScoredChunk, budget int, floor float64, maxExcerpts, maxBytes int) (out []domain.ScoredChunk, truncated []bool, err error) {
	out = make([]domain.ScoredChunk, 0, min(len(usable), maxExcerpts))
	truncated = make([]bool, 0, min(len(usable), maxExcerpts))
	remaining, usedBytes := budget, 0
	for _, sc := range usable {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if len(out) >= maxExcerpts {
			break
		}
		if len(out) > 0 && (math.IsNaN(sc.Score) || sc.Score < floor) {
			continue
		}
		tokens := llm.EstimateTokens(sc.Chunk.Text)
		if tokens > remaining || usedBytes+len(sc.Chunk.Text) > maxBytes {
			if (remaining < minPartialExcerptTokens || maxBytes-usedBytes <= 0) && len(out) > 0 {
				break
			}
			if len(out) > 0 && remaining <= 0 {
				break
			}
			body, ok := cutExcerptToBudget(sc.Chunk.Text, remaining, maxBytes-usedBytes, len(out) == 0)
			if !ok {
				if len(out) == 0 {
					continue
				}
				break
			}
			sc.Chunk.Text = body
			out, truncated = append(out, sc), append(truncated, true)
			break
		}
		if len(out) > 0 && remaining == 0 {
			break
		}
		out, truncated = append(out, sc), append(truncated, false)
		remaining -= tokens
		usedBytes += len(sc.Chunk.Text)
	}
	return out, truncated, nil
}

type diverseCandidate struct {
	chunk            domain.ScoredChunk
	rank             int
	tokens           int
	shingles         []uint64
	signals          meaningSignals
	signalsDone      bool
	lastCutRemTokens int
	lastCutRemBytes  int
	lastCutValid     bool
	lastCutBody      string
	lastCutTokens    int
	lastCutShingles  []uint64
	lastCutSignals   meaningSignals
	lastCutSigDone   bool
	selected         bool
	unfitting        bool
}

type selectedExcerpt struct {
	docID       domain.DocumentID
	index       int
	text        string
	shingles    map[uint64]struct{}
	signals     meaningSignals
	signalsDone bool
}

type meaningSignals struct {
	numbers map[string]struct{}
	terms   map[string]struct{}
}

const (
	meaningBearingOverlapThreshold = 0.50
	crossDocOverlapWeight          = 0.95
)

// meaningBearingWordTerms lists space-delimited negations, exceptions, and
// qualifiers across the built-in response languages (en, es, pt, fr, de, it),
// normalized via NFC, width folding, lowercasing, and apostrophe folding.
var meaningBearingWordTerms = []string{
	// en
	"no", "not", "never", "none", "neither", "nor", "nothing", "nowhere", "nobody",
	"cannot", "can't", "don't", "doesn't", "didn't", "isn't", "aren't", "wasn't", "weren't",
	"won't", "wouldn't", "couldn't", "shouldn't", "mustn't", "hasn't", "haven't", "hadn't",
	"needn't", "mightn't", "shan't", "ain't",
	"only", "solely", "exclusively", "merely",
	"except", "excepting", "excluding", "unless", "until", "till", "without",
	"at least", "at most", "no more than", "no less than",
	"prohibited", "forbidden", "disallowed",
	// es
	"nunca", "jamás", "jamas", "ningún", "ningun", "ninguno", "ninguna", "ningunos", "ningunas",
	"nada", "nadie", "ni", "tampoco", "prohibido", "prohibida", "prohibidos", "prohibidas",
	"solo", "sólo", "solamente", "únicamente", "unicamente", "exclusivamente",
	"excepto", "salvo", "a menos que", "a no ser que", "salvo que", "excepto que",
	"hasta", "sin", "al menos", "por lo menos", "como mínimo", "como minimo",
	"como máximo", "como maximo", "a lo sumo", "como mucho",
	// pt
	"não", "nao", "jamais", "nenhum", "nenhuma", "nenhuns", "nenhumas",
	"ninguém", "ninguem", "nem", "proibido", "proibida", "proibidos", "proibidas",
	"apenas", "somente", "só", "exceto", "a não ser que", "a nao ser que",
	"até", "sem", "pelo menos", "ao menos", "no mínimo", "no minimo",
	"no máximo", "no maximo", "quando muito",
	// fr
	"ne", "n'est", "n'a", "n'ont", "n'était", "n'étaient", "n'aura", "n'auront", "n'y", "n'en",
	"pas", "non", "aucun", "aucune", "aucuns", "aucunes", "rien", "personne",
	"nul", "nulle", "nullement", "interdit", "interdite", "interdits", "interdites",
	"seulement", "uniquement", "sauf", "excepté", "excepte", "hormis",
	"à moins que", "a moins que", "à moins de", "a moins de",
	"jusqu'à", "jusqu'a", "jusqu'au", "jusqu'aux", "jusque", "sans",
	"au moins", "tout au moins", "au plus", "tout au plus",
	// de
	"nicht", "kein", "keine", "keinen", "keinem", "keiner", "keines",
	"nie", "niemals", "niemand", "nichts", "weder", "noch", "nein", "verboten", "untersagt",
	"nur", "lediglich", "ausschließlich", "ausschliesslich", "einzig",
	"außer", "ausser", "ausgenommen", "es sei denn", "sofern nicht", "wenn nicht",
	"bis", "ohne", "mindestens", "wenigstens", "zumindest", "höchstens", "hoechstens",
	// it
	"mai", "nessun", "nessuno", "nessuna", "nessuni", "nessune",
	"niente", "nulla", "né", "neppure", "nemmeno", "neanche",
	"vietato", "vietata", "vietati", "vietate", "proibito", "proibita",
	"soltanto", "eccetto", "tranne", "a meno che", "fino", "finché", "finche",
	"senza", "almeno", "tutt'al più", "tutt'al piu", "al massimo", "al più", "al piu",
}

// meaningBearingDenseTerms lists negators and qualifiers matched as substrings
// in non-space-delimited scripts (Han, Kana, Hangul, Thai).
var meaningBearingDenseTerms = []string{
	"不", "没", "無", "无", "未", "非",
	"ない", "ません",
	"않", "안", "못", "없",
	"ไม่",
	"禁止", "のみ", "だけ", "以外",
	"금지", "제외",
	"ห้าม", "เท่านั้น", "ยกเว้น", "จนกว่า", "อย่างน้อย", "อย่างมาก",
}

func selectDiverseChunks(ctx context.Context, usable []domain.ScoredChunk, budget int, top, floor float64, maxExcerpts, maxBytes int) (out []domain.ScoredChunk, truncated []bool, err error) {
	candidates := make([]diverseCandidate, 0, len(usable))
	hasRerank := false
	for rank, sc := range usable {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		// Keep the leading usable passage regardless of relative floor; later
		// candidates must meet the vector floor.
		if rank > 0 && (math.IsNaN(sc.Score) || sc.Score < floor) {
			continue
		}
		if sc.RerankScore != nil {
			hasRerank = true
		}
		candidates = append(candidates, diverseCandidate{
			chunk:  sc,
			rank:   rank,
			tokens: llm.EstimateTokens(sc.Chunk.Text),
		})
	}
	if len(candidates) == 0 {
		return nil, nil, nil
	}

	out = make([]domain.ScoredChunk, 0, min(len(candidates), maxExcerpts))
	truncated = make([]bool, 0, min(len(candidates), maxExcerpts))
	coveredAll := make(map[uint64]struct{})
	coveredByDoc := make(map[domain.DocumentID]map[uint64]struct{})
	selectedExcerpts := make([]selectedExcerpt, 0, min(len(candidates), maxExcerpts))
	remaining, usedBytes := budget, 0

	// Always retain the leading usable passage first.
	for i := range candidates {
		c := &candidates[i]
		effBody := c.chunk.Chunk.Text
		effTokens := c.tokens
		effTrunc := false
		if effTokens > remaining || len(effBody) > maxBytes {
			cutBody, ok := cutExcerptToBudget(effBody, remaining, maxBytes, true)
			if !ok {
				c.unfitting = true
				continue
			}
			effBody = cutBody
			effTokens = llm.EstimateTokens(cutBody)
			effTrunc = true
		}
		c.selected = true
		chosen := c.chunk
		chosen.Chunk.Text = effBody
		out = append(out, chosen)
		truncated = append(truncated, effTrunc)
		remaining -= effTokens
		usedBytes += len(effBody)
		recordSelectedShingles(chosen.Chunk.DocumentID, chosen.Chunk.Index, effBody, extractContextShingles(effBody), coveredAll, coveredByDoc, &selectedExcerpts)
		break
	}
	if len(out) == 0 {
		return nil, nil, nil
	}

	for i := range candidates {
		if !candidates[i].selected && !candidates[i].unfitting {
			candidates[i].shingles = extractContextShingles(candidates[i].chunk.Chunk.Text)
		}
	}

	for len(out) < maxExcerpts && remaining > 0 && usedBytes < maxBytes {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		remBytes := maxBytes - usedBytes
		bestIdx := -1
		bestScore := math.Inf(-1)
		var bestBody string
		var bestTokens int
		var bestTrunc bool
		var bestShingles []uint64

		for i := range candidates {
			c := &candidates[i]
			if c.selected || c.unfitting {
				continue
			}
			effBody := c.chunk.Chunk.Text
			effTokens := c.tokens
			effTrunc := false
			shingles := c.shingles
			if effTokens > remaining || len(effBody) > remBytes {
				if remaining < minPartialExcerptTokens || remBytes <= 0 {
					c.unfitting = true
					continue
				}
				if c.lastCutValid && (c.lastCutRemTokens == remaining && c.lastCutRemBytes == remBytes ||
					(c.lastCutTokens <= remaining && len(c.lastCutBody) <= remBytes)) {
					effBody = c.lastCutBody
					effTokens = c.lastCutTokens
					shingles = c.lastCutShingles
				} else {
					cutBody, ok := cutExcerptToBudget(effBody, remaining, remBytes, false)
					if !ok {
						c.unfitting = true
						continue
					}
					effBody = cutBody
					effTokens = llm.EstimateTokens(cutBody)
					shingles = extractContextShingles(cutBody)
					c.lastCutRemTokens = remaining
					c.lastCutRemBytes = remBytes
					c.lastCutValid = true
					c.lastCutBody = cutBody
					c.lastCutTokens = effTokens
					c.lastCutShingles = shingles
					c.lastCutSigDone = false
				}
				effTrunc = true
			}
			score, skipSameDocDup := scoreDiverseCandidate(c, effBody, effTrunc, shingles, effTokens, remaining, top, hasRerank, coveredAll, coveredByDoc[c.chunk.Chunk.DocumentID], selectedExcerpts)
			if skipSameDocDup {
				c.unfitting = true
				continue
			}
			if bestIdx < 0 || diverseCandidateBetter(score, c, bestScore, &candidates[bestIdx]) {
				bestIdx = i
				bestScore = score
				bestBody = effBody
				bestTokens = effTokens
				bestTrunc = effTrunc
				bestShingles = shingles
			}
		}
		if bestIdx < 0 {
			break
		}
		chosen := &candidates[bestIdx]
		chosen.selected = true
		selectedChunk := chosen.chunk
		selectedChunk.Chunk.Text = bestBody
		out = append(out, selectedChunk)
		truncated = append(truncated, bestTrunc)
		remaining -= bestTokens
		usedBytes += len(bestBody)
		recordSelectedShingles(chosen.chunk.Chunk.DocumentID, chosen.chunk.Chunk.Index, bestBody, bestShingles, coveredAll, coveredByDoc, &selectedExcerpts)
	}
	return out, truncated, nil
}

func recordSelectedShingles(
	docID domain.DocumentID,
	index int,
	text string,
	shingles []uint64,
	coveredAll map[uint64]struct{},
	coveredByDoc map[domain.DocumentID]map[uint64]struct{},
	selected *[]selectedExcerpt,
) {
	docSet := coveredByDoc[docID]
	if docSet == nil {
		docSet = make(map[uint64]struct{}, len(shingles))
		coveredByDoc[docID] = docSet
	}
	excerptSet := make(map[uint64]struct{}, len(shingles))
	for _, h := range shingles {
		coveredAll[h] = struct{}{}
		docSet[h] = struct{}{}
		excerptSet[h] = struct{}{}
	}
	*selected = append(*selected, selectedExcerpt{
		docID:    docID,
		index:    index,
		text:     text,
		shingles: excerptSet,
	})
}

func scoreDiverseCandidate(
	c *diverseCandidate,
	effBody string,
	effTrunc bool,
	shingles []uint64,
	effTokens, remaining int,
	top float64,
	hasRerank bool,
	coveredAll map[uint64]struct{},
	coveredSameDoc map[uint64]struct{},
	selected []selectedExcerpt,
) (score float64, pureSameDocDuplicate bool) {
	total := len(shingles)
	if total == 0 {
		total = 1
	}
	allDup, sameDocDup := 0, 0
	for _, h := range shingles {
		if _, ok := coveredAll[h]; ok {
			allDup++
			if _, same := coveredSameDoc[h]; same {
				sameDocDup++
			}
		}
	}
	crossDocDup := allDup - sameDocDup
	novel := total - allDup
	if novel == 0 && sameDocDup == total {
		return 0, true
	}

	novelRatio := float64(novel) / float64(total)
	sameDocDupRatio := float64(sameDocDup) / float64(total)
	crossDocDupRatio := float64(crossDocDup) / float64(total)
	dupRatio := float64(allDup) / float64(total)

	if dupRatio >= meaningBearingOverlapThreshold && len(selected) > 0 {
		bestIdx := bestOverlappingExcerpt(shingles, selected)
		if bestIdx >= 0 {
			candSignals := candidateMeaningSignals(c, effBody, effTrunc)
			excSignals := excerptMeaningSignals(&selected[bestIdx])
			if hasMeaningBearingSignalDiff(candSignals, excSignals, effTrunc) {
				novelRatio = 1
				sameDocDupRatio = 0
				crossDocDupRatio = 0
			}
		}
	}

	rel := 1.0 / (1.0 + 0.20*float64(c.rank))
	if !hasRerank && top > 0 && c.chunk.Score > 0 {
		rel = min(1.0, c.chunk.Score/top)
	}

	effDupRatio := 1.0 - novelRatio
	if sameDocDupRatio > 0 || crossDocDupRatio > 0 {
		effDupRatio = sameDocDupRatio + crossDocOverlapWeight*crossDocDupRatio
	}
	excessDup := max(0.0, effDupRatio-0.50)
	dupPenalty := 4.5 * excessDup * excessDup

	cost := max(1, effTokens)
	costRatio := float64(cost) / float64(max(1, remaining))

	return rel - dupPenalty - 0.10*costRatio, false
}

func bestOverlappingExcerpt(shingles []uint64, selected []selectedExcerpt) int {
	bestIdx := -1
	bestHits := -1
	for i := range selected {
		hits := 0
		set := selected[i].shingles
		for _, h := range shingles {
			if _, ok := set[h]; ok {
				hits++
			}
		}
		if hits > bestHits {
			bestHits = hits
			bestIdx = i
		}
	}
	return bestIdx
}

func candidateMeaningSignals(c *diverseCandidate, effBody string, effTrunc bool) meaningSignals {
	if effTrunc {
		if !c.lastCutSigDone {
			c.lastCutSignals = extractMeaningSignals(effBody)
			c.lastCutSigDone = true
		}
		return c.lastCutSignals
	}
	if !c.signalsDone {
		c.signals = extractMeaningSignals(effBody)
		c.signalsDone = true
	}
	return c.signals
}

func excerptMeaningSignals(s *selectedExcerpt) meaningSignals {
	if !s.signalsDone {
		s.signals = extractMeaningSignals(s.text)
		s.signalsDone = true
	}
	return s.signals
}

func meaningBearingDifference(candidate, excerpt string, candidateCut bool) bool {
	return hasMeaningBearingSignalDiff(extractMeaningSignals(candidate), extractMeaningSignals(excerpt), candidateCut)
}

func hasMeaningBearingSignalDiff(cand, exc meaningSignals, candidateCut bool) bool {
	for n := range cand.numbers {
		if _, ok := exc.numbers[n]; !ok {
			return true
		}
	}
	for t := range cand.terms {
		if _, ok := exc.terms[t]; !ok {
			return true
		}
	}
	if !candidateCut {
		for n := range exc.numbers {
			if _, ok := cand.numbers[n]; !ok {
				return true
			}
		}
		for t := range exc.terms {
			if _, ok := cand.terms[t]; !ok {
				return true
			}
		}
	}
	return false
}

func extractMeaningSignals(text string) meaningSignals {
	runes := normalizeContextRunes(text)
	for i, r := range runes {
		if r == '\u2019' || r == '\u02bc' {
			runes[i] = '\''
		}
	}
	var numbers map[string]struct{}
	var digitBuf []rune
	flushDigits := func() {
		if len(digitBuf) == 0 {
			return
		}
		if numbers == nil {
			numbers = make(map[string]struct{})
		}
		numbers[string(digitBuf)] = struct{}{}
		digitBuf = digitBuf[:0]
	}
	for _, r := range runes {
		if unicode.IsDigit(r) {
			digitBuf = append(digitBuf, r)
		} else {
			flushDigits()
		}
	}
	flushDigits()

	normStr := string(runes)
	var terms map[string]struct{}
	for _, term := range meaningBearingWordTerms {
		if containsWordTerm(normStr, term) {
			if terms == nil {
				terms = make(map[string]struct{})
			}
			terms[term] = struct{}{}
		}
	}
	for _, term := range meaningBearingDenseTerms {
		if strings.Contains(normStr, term) {
			if terms == nil {
				terms = make(map[string]struct{})
			}
			terms[term] = struct{}{}
		}
	}
	return meaningSignals{numbers: numbers, terms: terms}
}

func containsWordTerm(normStr, term string) bool {
	pos := 0
	for pos <= len(normStr)-len(term) {
		relIdx := strings.Index(normStr[pos:], term)
		if relIdx < 0 {
			return false
		}
		idx := pos + relIdx
		beforeOK := idx == 0
		if !beforeOK {
			prev, _ := utf8.DecodeLastRuneInString(normStr[:idx])
			beforeOK = !isContextWordRune(prev)
		}
		afterIdx := idx + len(term)
		afterOK := afterIdx == len(normStr)
		if !afterOK {
			next, _ := utf8.DecodeRuneInString(normStr[afterIdx:])
			afterOK = !isContextWordRune(next)
		}
		if beforeOK && afterOK {
			return true
		}
		_, size := utf8.DecodeRuneInString(normStr[idx:])
		pos = idx + max(1, size)
	}
	return false
}

func isContextWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r)
}

// diverseCandidateBetter breaks ties deterministically; rank is unique among
// candidates, while the remaining fields keep the fallback order explicit.
func diverseCandidateBetter(scoreA float64, a *diverseCandidate, scoreB float64, b *diverseCandidate) bool {
	const eps = 1e-12
	if math.Abs(scoreA-scoreB) > eps {
		return scoreA > scoreB
	}
	if a.rank != b.rank {
		return a.rank < b.rank
	}
	if a.chunk.Chunk.ID != b.chunk.Chunk.ID {
		return a.chunk.Chunk.ID < b.chunk.Chunk.ID
	}
	if a.chunk.Chunk.DocumentID != b.chunk.Chunk.DocumentID {
		return a.chunk.Chunk.DocumentID < b.chunk.Chunk.DocumentID
	}
	return a.chunk.Chunk.Index < b.chunk.Chunk.Index
}

func cutExcerptToBudget(text string, remTokens, remBytes int, allowHardCut bool) (string, bool) {
	if remTokens < 0 || remBytes <= 0 {
		return "", false
	}
	runes := []rune(text)
	byteEnds := make([]int, len(runes)+1)
	for i, r := range runes {
		byteEnds[i+1] = byteEnds[i] + utf8.RuneLen(r)
	}
	cut := sort.Search(len(runes)+1, func(n int) bool {
		return n/4 > remTokens || byteEnds[n] > remBytes
	}) - 1
	if cut <= 0 {
		return "", false
	}
	body := string(runes[:cut])
	if end := strings.LastIndexAny(body, ".\n"); end >= 0 && llm.EstimateTokens(body[:end+1]) >= minPartialExcerptTokens {
		body = body[:end+1]
	} else if !allowHardCut {
		return "", false
	}
	if strings.TrimSpace(body) == "" {
		return "", false
	}
	return body, true
}

func normalizeContextRunes(text string) []rune {
	normalized := strings.ToLower(width.Fold.String(norm.NFC.String(text)))
	runes := make([]rune, 0, len(normalized))
	prevSpace := true
	for _, r := range normalized {
		if unicode.Is(unicode.Cf, r) {
			continue
		}
		if unicode.IsSpace(r) {
			if !prevSpace {
				runes = append(runes, ' ')
				prevSpace = true
			}
			continue
		}
		runes = append(runes, r)
		prevSpace = false
	}
	if len(runes) > 0 && runes[len(runes)-1] == ' ' {
		runes = runes[:len(runes)-1]
	}
	return runes
}

func extractContextShingles(text string) []uint64 {
	runes := normalizeContextRunes(text)
	if len(runes) == 0 {
		return nil
	}
	if len(runes) <= contextShingleWidth {
		return []uint64{hashRuneShingle(runes)}
	}
	windows := len(runes) - contextShingleWidth + 1
	if windows <= maxShinglesPerChunk {
		seen := make(map[uint64]struct{}, windows)
		for i := range windows {
			seen[hashRuneShingle(runes[i:i+contextShingleWidth])] = struct{}{}
		}
		out := make([]uint64, 0, len(seen))
		for h := range seen {
			out = append(out, h)
		}
		slices.Sort(out)
		return out
	}
	// Bounded bottom-k min-hash sketch retains <= maxShinglesPerChunk unique hashes
	// across the whole body so tails remain represented without unbounded memory.
	seen := make(map[uint64]struct{}, maxShinglesPerChunk)
	heap := make([]uint64, 0, maxShinglesPerChunk)
	for i := range windows {
		h := hashRuneShingle(runes[i : i+contextShingleWidth])
		if _, ok := seen[h]; ok {
			continue
		}
		if len(heap) < maxShinglesPerChunk {
			seen[h] = struct{}{}
			heap = append(heap, h)
			siftUpMaxHeap(heap, len(heap)-1)
			continue
		}
		if h < heap[0] {
			delete(seen, heap[0])
			seen[h] = struct{}{}
			heap[0] = h
			siftDownMaxHeap(heap, 0)
		}
	}
	slices.Sort(heap)
	return heap
}

func hashRuneShingle(runes []rune) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	var buf [utf8.UTFMax]byte
	h := uint64(offset64)
	for _, r := range runes {
		n := utf8.EncodeRune(buf[:], r)
		for _, b := range buf[:n] {
			h ^= uint64(b)
			h *= prime64
		}
	}
	h += 0x9e3779b97f4a7c15
	h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
	h = (h ^ (h >> 27)) * 0x94d049bb133111eb
	return h ^ (h >> 31)
}

func siftUpMaxHeap(h []uint64, i int) {
	for i > 0 {
		p := (i - 1) / 2
		if h[p] >= h[i] {
			break
		}
		h[p], h[i] = h[i], h[p]
		i = p
	}
}

func siftDownMaxHeap(h []uint64, i int) {
	n := len(h)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		largest := left
		if right := left + 1; right < n && h[right] > h[left] {
			largest = right
		}
		if h[i] >= h[largest] {
			break
		}
		h[i], h[largest] = h[largest], h[i]
		i = largest
	}
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
	qc.CitationCoverage = nil
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
	if err := synthesisCtx.Err(); err != nil {
		return fmt.Errorf("synthesis_stage: %w", err)
	}
	if len(qc.RetrievedChunks) == 0 {
		return s.finishWithoutEvidence(synthesisCtx, qc)
	}

	var policy, llmProvider, guardrailID string
	var budget int
	var floor float64
	var strict bool
	var serviceTokens []string
	if s.cfg != nil {
		strict = s.cfg.SynthesisInputProfile != "" && qc.AnswerMode != domain.AnswerModeEvidence
		budget = s.cfg.SynthesisContextTokens
		floor = s.cfg.SynthesisRelativeScoreFloor
		policy = s.cfg.SynthesisContextPolicy
		llmProvider = s.cfg.LLMProvider
		guardrailID = s.cfg.BedrockGuardrailID
		serviceTokens = s.cfg.ServiceTokens
	}
	chunks, truncated, trimErr := trimChunksForSynthesis(synthesisCtx, qc.RetrievedChunks, budget, floor, policy)
	if trimErr != nil {
		return fmt.Errorf("synthesis_stage: %w", trimErr)
	}
	if len(chunks) == 0 {
		qc.RetrievedChunks = nil
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
	case llmProvider == "openai":
		boundary = newCountingBoundary()
	default:
		boundary = bedrockCountingBoundary(serviceTokens, qc.OriginalQuery, chunks)
	}
	builder := synthesisPromptBuilder{
		question: qc.OriginalQuery, includeCitations: qc.IncludeCitations,
		boundary: boundary, guardrail: guardrailID != "",
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
		if qc.Authorization != nil || qc.EvidenceVisibility != nil || qc.HostedVisibilityRefill || qc.RequireCitationScope {
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
		cancel()
		if err := s.finalizeCitations(ctx, qc); err != nil {
			return fmt.Errorf("citation support: %w", err)
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
