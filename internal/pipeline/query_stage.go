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

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/smithy-go"
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

// RoutingStage determines LOCAL or GLOBAL retrieval mode.
type RoutingStage struct {
	ret        *retriever.Retriever
	visibility domain.DocumentVisibility
	previews   domain.DocumentPreviewer
}

// NewRoutingStage creates a RoutingStage.
func NewRoutingStage(ret *retriever.Retriever) *RoutingStage {
	return NewRoutingStageWithScope(ret, nil, nil)
}

// NewRoutingStageWithScope restricts routing signals to visible candidates.
func NewRoutingStageWithScope(ret *retriever.Retriever, vis domain.DocumentVisibility, previews domain.DocumentPreviewer) *RoutingStage {
	return &RoutingStage{ret: ret, visibility: vis, previews: previews}
}

// ID returns the stage identifier.
func (s *RoutingStage) ID() StageID { return StageRetrievalRouting }

// Run evaluates query signals and sets RetrievalMode on the QueryContext.
func (s *RoutingStage) Run(ctx context.Context, qc *QueryContext) error {
	filter := domain.VectorFilter{DocType: qc.FilterDocType, DocumentIDs: qc.AllowedDocumentIDs, Collection: qc.Collection}
	qc.NamedEntity, qc.EntityDocumentID = "", ""
	qc.LocalDocumentID = ""
	qc.Candidates, qc.QueryEmbedding = nil, nil
	qc.TopScore, qc.SkipRetrieval = 0, false
	explicit := qc.RequestedMode != "" && !strings.EqualFold(qc.RequestedMode, "auto")
	qc.RetrievalMode = string(retriever.RetrievalModeGlobal)
	if filter.MatchesNothing() {
		qc.SkipRetrieval = true
		return nil
	}
	probeK := 5
	if strings.EqualFold(qc.RequestedMode, "global") {
		probeK = 0
	}
	embedding, candidates, probeFailed, err := s.ret.EmbedQuery(ctx, qc.OriginalQuery, filter, probeK)
	if err != nil {
		return fmt.Errorf("routing_stage: %w", err)
	}
	qc.QueryEmbedding = embedding
	if probeK == 0 || probeFailed {
		return nil
	}
	candidates = filterChunksByAllowedDocIDs(candidates, qc.AllowedDocumentIDs)
	candidates, err = visibleChunks(ctx, candidates, s.visibility)
	if err != nil {
		return fmt.Errorf("routing_stage: visibility: %w", err)
	}
	qc.Candidates = candidates
	if len(candidates) == 0 {
		return nil
	}
	qc.TopScore = candidates[0].Score
	if s.previews != nil {
		ids := candidateDocumentIDs(candidates)
		previews, err := s.previews.DocumentPreviews(ctx, ids)
		if err != nil {
			return fmt.Errorf("routing_stage: previews: %w", err)
		}
		qc.NamedEntity, qc.EntityDocumentID = MatchNamedEntity(qc.OriginalQuery, previews, ids)
	}
	qc.RetrievalMode = string(s.ret.DetermineModeWithQuery(len(qc.SubQueries), qc.TopScore, qc.NamedEntity))
	if explicit {
		qc.RetrievalMode = strings.ToUpper(qc.RequestedMode)
	}
	if qc.RetrievalMode == string(retriever.RetrievalModeLocal) {
		qc.LocalDocumentID = qc.EntityDocumentID
		if qc.LocalDocumentID == "" {
			qc.LocalDocumentID = string(candidates[0].Chunk.DocumentID)
		}
	}
	return nil
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
	filter := domain.VectorFilter{DocType: qc.FilterDocType, DocumentIDs: qc.AllowedDocumentIDs, Collection: qc.Collection}
	mode := retriever.RetrievalMode(qc.RetrievalMode)
	if len(qc.QueryEmbedding) == 0 {
		embedding, _, _, err := s.ret.EmbedQuery(ctx, qc.OriginalQuery, filter, 0)
		if err != nil {
			return fmt.Errorf("retrieval_stage: %w", err)
		}
		qc.QueryEmbedding = embedding
	}
	result, err := s.ret.RetrieveWithEmbedding(ctx, qc.QueryEmbedding, mode, qc.LocalDocumentID, qc.MaxChunks, filter)
	if err != nil {
		return fmt.Errorf("retrieval_stage: %w", err)
	}
	// Enforce the caller's tenant/role allowlist BEFORE the synthesis stage
	// runs, so privileged content never reaches the LLM context (and so can
	// never leak into the generated answer). A nil allowlist is a no-op.
	chunks := filterChunksByAllowedDocIDs(result.Chunks, qc.AllowedDocumentIDs)
	qc.RetrievedChunks = nil
	qc.TopScore = 0
	chunks, err = visibleChunks(ctx, chunks, s.visibility)
	if err != nil {
		return fmt.Errorf("retrieval_stage: visibility: %w", err)
	}
	qc.RetrievedChunks = chunks
	qc.RetrievalMode = string(result.Mode)
	if len(chunks) > 0 {
		qc.TopScore = chunks[0].Score
	}
	return nil
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

// Run preserves vector order when reranking fails or its deadline expires.
func (s *RerankStage) Run(ctx context.Context, qc *QueryContext) error {
	if s.reranker == nil || len(qc.RetrievedChunks) == 0 {
		return nil
	}
	rerankCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	chunks, err := s.reranker.Rerank(rerankCtx, qc.OriginalQuery, slices.Clone(qc.RetrievedChunks))
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("rerank_stage: %w", ctxErr)
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
	ambassador *llm.Ambassador
	cfg        *config.Config
}

// NewSynthesisStage creates a SynthesisStage.
func NewSynthesisStage(amb *llm.Ambassador, cfg *config.Config) *SynthesisStage {
	return &SynthesisStage{ambassador: amb, cfg: cfg}
}

// ID returns the stage identifier.
func (s *SynthesisStage) ID() StageID { return StageSynthesis }

// trimChunksForSynthesis budgets sanitized excerpts in candidate order.
// A partial final excerpt needs a sentence boundary and at least 64 tokens;
// the first excerpt is always retained, with a hard cut if necessary.
func trimChunksForSynthesis(in []domain.ScoredChunk, budget int, relativeFloor float64) (out []domain.ScoredChunk, truncated []bool) {
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
		sc.Chunk.Text = sanitizeExcerpt(sc.Chunk.Text)
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
		qc.FinishReason = "no_evidence"
		qc.FinalAnswer, qc.ContentLanguage = noEvidenceAnswer(qc.AcceptLanguage)
		qc.ContextTokens, qc.ContextChunks = 0, 0
		qc.TokenUsage = domain.TokenUsage{}
		qc.EarlyExit = true
		return streamNoEvidence(ctx, qc)
	}

	chunks, truncated := trimChunksForSynthesis(qc.RetrievedChunks, s.cfg.SynthesisContextTokens, s.cfg.SynthesisRelativeScoreFloor)
	qc.ContextChunks = len(chunks)
	qc.Truncated = truncated
	qc.Excerpts = chunks

	system, parts := buildSynthesisPrompt(qc.OriginalQuery, chunks, newBoundary())
	if qc.IncludeCitations {
		system += "\n" + synthesisCitationInstruction
	}
	prompt := strings.Join(parts[:], "")
	content := []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: prompt}}
	if s.cfg.BedrockGuardrailID != "" {
		content = guardedSynthesisContent(parts)
	}

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

	messages := []brtypes.Message{
		{
			Role:    brtypes.ConversationRoleUser,
			Content: content,
		},
	}

	// Synthesis is free-form text — never JSON. Use the plain path so
	// the ambassador's JSON-correction retry loop is bypassed even when
	// SYNTHESIS_MODEL=primary routes us through Nova Micro.
	timeout := s.cfg.SynthesisTimeout
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	synthesisCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := s.converse(
		synthesisCtx, qc, modelID, system, messages, 0.2, answerSynthesisMaxOutput,
	)
	if err != nil {
		return fmt.Errorf("synthesis_stage: %w", err)
	}

	if err := synthesisCtx.Err(); err != nil {
		return fmt.Errorf("synthesis_stage: %w", err)
	}
	qc.FinalAnswer = llm.ExtractText(result.Output)
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
	switch result.Output.StopReason {
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
	if result.Output != nil && result.Output.Usage != nil {
		if result.Output.Usage.InputTokens != nil {
			qc.TokenUsage.InputTokens = *result.Output.Usage.InputTokens
		}
		if result.Output.Usage.OutputTokens != nil {
			qc.TokenUsage.OutputTokens = *result.Output.Usage.OutputTokens
		}
		qc.TokenUsage.TotalTokens = qc.TokenUsage.InputTokens + qc.TokenUsage.OutputTokens
	}

	slog.DebugContext(ctx, "synthesis completed", "mode", qc.RetrievalMode, "chunks", len(qc.RetrievedChunks), "answer_bytes", len(qc.FinalAnswer))
	return nil
}
