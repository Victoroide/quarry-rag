package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/citationsupport"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// QueryRequest is the POST /query request body.
//
// AllowedDocumentIDs is an optional tenant/role allowlist. When the caller
// supplies it (non-null), retrieval is restricted to those document ids BEFORE
// synthesis, so the LLM can never read — let alone summarize — documents the
// caller is not permitted to see. A null value means "no restriction"; an empty
// array matches nothing. The trusted caller supplies the authorization scope.
type QueryRequest struct {
	Query              string              `json:"query"`
	Mode               string              `json:"mode,omitempty"`
	AnswerMode         requestedAnswerMode `json:"answer_mode,omitempty"`
	FilterDocType      string              `json:"filter_doc_type,omitempty"`
	MaxChunks          int                 `json:"max_chunks,omitempty"`
	IncludeCitations   bool                `json:"include_citations,omitempty"`
	PreserveCode       bool                `json:"preserve_code,omitempty"`
	AllowedDocumentIDs []string            `json:"allowed_document_ids,omitzero"`

	Collection string `json:"collection,omitempty"`
}

// ScoredChunkResponse is a single scored chunk in the query response.
// “kind“ and “quality“ are populated for chunks ingested with the
// classifier (body/contact/header × high/low) so downstream consumers
// can de-prioritize low-quality evidence without re-implementing the
// heuristics. Older chunks ingested before the classifier was added
// will leave these fields empty; callers must treat that as "unknown".
type ScoredChunkResponse struct {
	DocID       string   `json:"doc_id"`
	DocType     string   `json:"doc_type"`
	ChunkIndex  int      `json:"chunk_index"`
	Score       float64  `json:"score"`
	Text        string   `json:"text"`
	Kind        string   `json:"kind,omitempty"`
	Quality     string   `json:"quality,omitempty"`
	Pages       []int    `json:"pages,omitzero"`
	TableIDs    []string `json:"table_ids,omitzero"`
	RowIDs      []string `json:"row_ids,omitzero"`
	Excerpt     int      `json:"excerpt,omitempty"`
	Truncated   bool     `json:"truncated,omitempty"`
	RerankScore *float64 `json:"rerank_score,omitempty"`
}

// CitationResponse links an answer marker to an excerpt and its lexical check.
type CitationResponse struct {
	Excerpt    int              `json:"excerpt"`
	DocID      string           `json:"doc_id"`
	ChunkIndex int              `json:"chunk_index"`
	Offset     int              `json:"offset"`
	Verified   bool             `json:"verified"`
	Overlap    float64          `json:"overlap"`
	Support    *CitationSupport `json:"support,omitempty"`
}

// CitationSupport is the semantic assessment of a claim against its evidence.
type CitationSupport citationsupport.Assessment

// CitationCoverage counts generated sentences and those that have valid markers.
type CitationCoverage citationsupport.Coverage

// QueryResponse is the POST /query response body.
type QueryResponse struct {
	FinishReason     string                `json:"finish_reason,omitempty"`
	Answer           string                `json:"answer"`
	Mode             string                `json:"mode"`
	ChunksUsed       int                   `json:"chunks_used"`
	ContextChunks    int                   `json:"context_chunks,omitempty"`
	ContextTokens    int                   `json:"context_tokens,omitempty"`
	CountMethod      string                `json:"count_method,omitempty"`
	ModelInputTokens int                   `json:"model_input_tokens,omitempty"`
	EarlyExit        bool                  `json:"early_exit,omitempty"`
	StageTimings     map[string]int64      `json:"stage_timings_ms,omitempty"`
	TokenUsage       domain.TokenUsage     `json:"token_usage"`
	ScoredChunks     []ScoredChunkResponse `json:"scored_chunks"`
	Citations        []CitationResponse    `json:"citations,omitzero"`
	CitationCoverage *CitationCoverage     `json:"citation_coverage,omitempty"`
}

func citationResponses(citations []pipeline.Citation) []CitationResponse {
	out := make([]CitationResponse, 0, len(citations))
	for _, c := range citations {
		out = append(out, CitationResponse{Excerpt: c.Excerpt, DocID: string(c.DocumentID), ChunkIndex: c.ChunkIndex, Offset: c.Offset, Verified: c.Verified, Overlap: c.Overlap, Support: (*CitationSupport)(c.Support)})
	}
	return out
}

// APIError is a structured JSON error response.
type APIError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func validateQueryRequest(req *QueryRequest) *APIError {
	if strings.TrimSpace(req.Query) == "" {
		return &APIError{Error: "validation", Message: "query is required"}
	}
	if len(req.Query) > 10000 {
		return &APIError{Error: "validation", Message: "query exceeds maximum length of 10000 bytes"}
	}
	if req.PreserveCode && !req.IncludeCitations {
		return &APIError{Error: "validation", Message: "preserve_code requires include_citations"}
	}
	switch strings.ToLower(req.Mode) {
	case "", "auto", "local", "global":
	default:
		return &APIError{Error: "validation", Message: "mode must be auto, local or global"}
	}
	if req.MaxChunks < 0 || req.MaxChunks > 100 {
		return &APIError{Error: "validation", Message: "max_chunks must be between 0 and 100"}
	}
	var apiErr *APIError
	req.FilterDocType, apiErr = validateDocType(req.FilterDocType)
	if apiErr != nil {
		return apiErr
	}
	req.Collection, apiErr = validateLabel("collection", req.Collection)
	return apiErr
}

// QueryHandler returns an HTTP handler for POST /query.
func QueryHandler(ret *retriever.Retriever, amb *llm.Ambassador, cfg *config.Config, pool *pgxpool.Pool, opts ...QueryOption) http.HandlerFunc {
	options := queryOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	concurrency := cfg.QueryConcurrency
	if concurrency <= 0 {
		concurrency = 32
	}
	slots := make(chan struct{}, concurrency)
	queryTimeout := cfg.QueryTimeout
	if queryTimeout <= 0 {
		queryTimeout = time.Minute
	}
	synthesis := pipeline.NewSynthesisStageWithCatalog(amb, cfg, options.noEvidenceCatalog).WithCitationSupport(options.citationVerifier)
	var authz *authorizedQuery
	if options.authority != nil {
		var capable bool
		if authz, capable = newAuthorizedQuery(options.authority, ret); !capable {
			slog.Error("authorized retrieval requires a vector store that enforces read scopes")
			return func(w http.ResponseWriter, r *http.Request) {
				recordSecurityAuditBestEffort(r.Context(), options.audit, nil, nil, "query", "error", "authorization_unavailable", "", "")
				refuseAuthorizedQuery(w)
			}
		}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		var subject middleware.VerifiedSubject
		if authz != nil {
			w.Header().Set("Cache-Control", "private, no-store")
			var verified bool
			subject, verified = middleware.SubjectFromContext(r.Context())
			if !subjectValid(subject, verified) {
				var auditSubject *middleware.VerifiedSubject
				if verified {
					auditSubject = &subject
				}
				recordSecurityAuditBestEffort(r.Context(), options.audit, auditSubject, nil, "query", "deny", "credential_expired", "", "")
				writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials")
				return
			}
		}
		if !isJSONRequest(r) {
			writeJSONError(w, http.StatusUnsupportedMediaType, "validation", "Content-Type must be application/json")
			return
		}
		start := time.Now()
		var req QueryRequest
		if !decodeJSON(w, r, &req, 1<<20) {
			return
		}

		if apiErr := validateQueryRequest(&req); apiErr != nil {
			writeJSONError(w, http.StatusBadRequest, apiErr.Error, apiErr.Message)
			return
		}
		if apiErr := validateQueryAllowlist(&req, cfg); apiErr != nil {
			writeJSONError(w, http.StatusBadRequest, apiErr.Error, apiErr.Message)
			return
		}
		answerMode, status, apiErr := resolveAnswerMode(req, cfg.QueryAnswerPolicy)
		if apiErr != nil {
			writeJSONError(w, status, apiErr.Error, apiErr.Message)
			return
		}
		if err := acquireQuerySlot(r.Context(), slots, cfg.QueryQueueTimeout); err != nil {
			writeQueryError(w, r, err)
			return
		}
		defer func() { <-slots }()
		ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
		defer cancel()

		routingCfg := pipeline.RoutingConfigFromConfig(cfg)
		retrieval := pipeline.NewRetrievalStage(ret)
		routing := pipeline.NewRoutingStageWithConfig(ret, nil, nil, routingCfg)
		var authorization *pipeline.ReadAuthorization
		var evidence domain.EvidenceVisibility
		if authz != nil {
			var staged bool
			if routing, retrieval, evidence, staged = authz.stages(ret, subject.Principal, &req, routingCfg); !staged {
				recordSecurityAuditBestEffort(ctx, options.audit, &subject, nil, "query", "error", "authorization_unavailable", "", "")
				refuseAuthorizedQuery(w)
				return
			}
			var err error
			if authorization, req.AllowedDocumentIDs, err = authz.derive(ctx, subject, &req, cfg); err != nil {
				mappedErr := credentialFailure(err, subject)
				recordQueryFailureAudit(ctx, options.audit, &subject, nil, mappedErr)
				authorizedFailure(w, r, mappedErr)
				return
			}
			rev := authorization.PolicyRevision()
			if !requireAuditedAllow(ctx, w, options.audit, &subject, &rev, "query", "authorized", "", "") {
				return
			}
		}

		qc := &pipeline.QueryContext{
			OriginalQuery:        req.Query,
			MaxChunks:            effectiveMaxChunks(req.MaxChunks, cfg.MaxChunksPerQuery),
			RequestedMode:        req.Mode,
			AnswerMode:           answerMode,
			FilterDocType:        req.FilterDocType,
			IncludeCitations:     req.IncludeCitations,
			PreserveCode:         req.PreserveCode,
			AllowedDocumentIDs:   req.AllowedDocumentIDs,
			AcceptLanguage:       strings.Join(r.Header.Values("Accept-Language"), ","),
			Collection:           req.Collection,
			RequireCitationScope: req.IncludeCitations && options.citationVerifier != nil,
			Authorization:        authorization,
		}

		var stream *queryStream
		if acceptsQueryStream(r) {
			stream = newQueryStream(ctx, w, r, qc)
		}

		if authz != nil {
			qc.EvidenceVisibility = evidence
			qc.HostedVisibilityRefill = cfg.HostedVisibilityRefill && ret.SupportsVisibilityRefill()
			qc.HostedVisibilityExtraRequests = cfg.HostedVisibilityExtraRequests
		} else if pool != nil {
			visibility := pgstore.Visibility{Pool: pool, Collection: req.Collection, DocType: req.FilterDocType}
			retrieval = pipeline.NewRetrievalStageWithVisibility(ret, visibility)
			previews := pgstore.Previews{Pool: pool, Collection: req.Collection, DocType: req.FilterDocType}
			routing = pipeline.NewRoutingStageWithConfig(ret, visibility, previews, routingCfg)
			qc.EvidenceVisibility = visibility
			qc.HostedVisibilityRefill = cfg.HostedVisibilityRefill && ret.SupportsVisibilityRefill()
			qc.HostedVisibilityExtraRequests = cfg.HostedVisibilityExtraRequests
		}
		stages := []pipeline.QueryStage{
			pipeline.NewDecomposeStage(),
			routing,
			retrieval,
		}
		if answerMode != domain.AnswerModeEvidence && options.reranker != nil {
			stages = append(stages, pipeline.NewRerankStage(options.reranker, cfg.RerankTimeout))
		}
		stages = append(stages, synthesis)

		var upstreams *evaluation.UpstreamProof
		_, scopedEvaluation := evaluationScope(r, cfg.EvaluationCollectionPrefix, qc)
		if scopedEvaluation && !cfg.FakeMode && stream == nil {
			ctx, upstreams = evaluation.TrackUpstreams(ctx)
		}
		_, err := pipeline.RunRetrievalPipeline(ctx, qc, stages)
		if err == nil {
			err = ctx.Err()
		}
		err = boundedFailure(err, authorization)
		if err != nil && authorization != nil {
			rev := authorization.PolicyRevision()
			recordQueryPostAdmissionAudit(r.Context(), options.audit, &subject, &rev, err)
		}
		if err == nil {
			slog.DebugContext(r.Context(), "query completed", "mode", qc.RetrievalMode, "chunks", len(qc.RetrievedChunks), "duration", time.Since(start))
		}
		if stream != nil {
			stream.finish(ctx, err)
			return
		}
		if err != nil {
			writeQueryError(w, r, err)
			return
		}

		scoredChunks := queryScoredChunks(qc)
		resp := QueryResponse{
			Answer:           qc.FinalAnswer,
			FinishReason:     qc.FinishReason,
			Mode:             qc.RetrievalMode,
			ChunksUsed:       len(qc.RetrievedChunks),
			ContextChunks:    qc.ContextChunks,
			ContextTokens:    qc.ContextTokens,
			CountMethod:      qc.CountMethod,
			ModelInputTokens: qc.ModelInputTokens,
			EarlyExit:        qc.EarlyExit,
			StageTimings:     qc.StageTimingsMS,
			TokenUsage:       qc.TokenUsage,
			ScoredChunks:     scoredChunks,
		}
		if req.IncludeCitations {
			resp.Citations = citationResponses(qc.Citations)
			resp.CitationCoverage = (*CitationCoverage)(qc.CitationCoverage)
		}

		data, encodeErr := json.Marshal(resp)
		if encodeErr != nil {
			writeQueryError(w, r, encodeErr)
			return
		}
		if scopeErr := qc.CheckEvidenceScope(ctx); scopeErr != nil {
			if authorization != nil {
				rev := authorization.PolicyRevision()
				recordQueryFailureAudit(r.Context(), options.audit, &subject, &rev, scopeErr)
			}
			writeQueryError(w, r, scopeErr)
			return
		}
		proof := evaluationProof(ctx, r, cfg.EvaluationCollectionPrefix, qc)
		if err := ctx.Err(); err != nil {
			writeQueryError(w, r, err)
			return
		}
		if proof != "" {
			w.Header().Set(evaluation.MatchesHeader, proof)
		}
		if scopedEvaluation {
			if execution := evaluationExecution(cfg, qc); execution != "" {
				w.Header().Set(evaluation.ExecutionHeader, execution)
				if upstreams != nil {
					if hosts := upstreams.Header(); evaluation.ValidUpstreams(hosts, cfg.EmbeddingProvider, cfg.LLMProvider) {
						w.Header().Set(evaluation.UpstreamsHeader, hosts)
					}
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		setQueryLanguageHeaders(w.Header(), qc)
		w.WriteHeader(http.StatusOK)
		if _, writeErr := w.Write(append(data, '\n')); writeErr != nil {
			slog.ErrorContext(r.Context(), "write query response", "error", writeErr)
		}
	}
}

func setQueryLanguageHeaders(headers http.Header, qc *pipeline.QueryContext) {
	if qc.EarlyExit && qc.ContentLanguage != "" {
		headers.Set("Content-Language", qc.ContentLanguage)
		headers.Add("Vary", "Accept-Language")
	}
}

func writeJSONError(w http.ResponseWriter, code int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	resp := APIError{Error: errType, Message: message}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("encode error response", "error", err)
	}
}

func recordQueryFailureAudit(ctx context.Context, recorder domain.SecurityAuditRecorder, subject *middleware.VerifiedSubject, rev *int64, err error) {
	outcome, reason := "error", "authorization_failed"
	switch {
	case errors.Is(err, errScopeTooLarge):
		outcome, reason = "deny", "scope_too_large"
	case errors.Is(err, domain.ErrReadDenied):
		outcome, reason = "deny", "access_denied"
	case errors.Is(err, pipeline.ErrCredentialExpired):
		outcome, reason = "deny", "credential_expired"
	case errors.Is(err, pipeline.ErrReadAuthorityChanged), errors.Is(err, pipeline.ErrEvidenceScopeChanged):
		outcome, reason = "deny", "scope_changed"
	case errors.Is(err, pipeline.ErrAuthorizationUnavailable):
		outcome, reason = "error", "authorization_unavailable"
	case errors.Is(err, domain.ErrAuthzModeMismatch):
		reason = "authz_mode_mismatch"
	case errors.Is(err, context.Canceled):
		outcome, reason = "deny", "request_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		outcome, reason = "deny", "request_timed_out"
	}
	recordSecurityAuditBestEffort(ctx, recorder, subject, rev, "query", outcome, reason, "", "")
}

func recordQueryPostAdmissionAudit(ctx context.Context, recorder domain.SecurityAuditRecorder, subject *middleware.VerifiedSubject, rev *int64, err error) {
	switch {
	case errors.Is(err, pipeline.ErrCredentialExpired),
		errors.Is(err, domain.ErrReadDenied),
		errors.Is(err, pipeline.ErrReadAuthorityChanged),
		errors.Is(err, pipeline.ErrEvidenceScopeChanged),
		errors.Is(err, pipeline.ErrAuthorizationUnavailable),
		errors.Is(err, domain.ErrAuthzModeMismatch):
		recordQueryFailureAudit(ctx, recorder, subject, rev, err)
	}
}
