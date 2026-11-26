package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
)

type requestedAnswerMode domain.AnswerMode

func (m *requestedAnswerMode) UnmarshalJSON(body []byte) error {
	var mode domain.AnswerMode
	if err := json.Unmarshal(body, &mode); err != nil {
		return err
	}
	switch mode {
	case domain.AnswerModeGenerated, domain.AnswerModeEvidence:
		*m = requestedAnswerMode(mode)
		return nil
	default:
		return errors.New("answer_mode must be generated or evidence")
	}
}

func resolveAnswerMode(req QueryRequest, policy string) (domain.AnswerMode, int, *APIError) {
	mode := domain.AnswerMode(req.AnswerMode)
	if policy == config.QueryAnswerPolicyEvidenceOnly {
		if mode == domain.AnswerModeGenerated {
			return "", http.StatusForbidden, &APIError{Error: "forbidden", Message: "generated answers are disabled"}
		}
		mode = domain.AnswerModeEvidence
	}
	if mode == "" {
		mode = domain.AnswerModeGenerated
	}
	if mode == domain.AnswerModeEvidence && req.IncludeCitations {
		return "", http.StatusBadRequest, &APIError{Error: "validation", Message: "include_citations requires generated answers"}
	}
	return mode, 0, nil
}

type queryOptions struct {
	authority         domain.ReadAuthority
	reranker          retriever.Reranker
	noEvidenceCatalog *pipeline.NoEvidenceCatalog
	citationVerifier  pipeline.CitationVerifier
	audit             domain.SecurityAuditRecorder
}

// QueryOption configures optional query dependencies.
type QueryOption func(*queryOptions)

// WithQueryAudit records bounded security audit events for authorized query decisions.
func WithQueryAudit(recorder domain.SecurityAuditRecorder) QueryOption {
	return func(opts *queryOptions) { opts.audit = recorder }
}

// WithReranker selects the reranker used before synthesis context selection.
func WithReranker(r retriever.Reranker) QueryOption {
	return func(opts *queryOptions) { opts.reranker = r }
}

// WithNoEvidenceCatalog selects the fixed translations for queries without evidence.
func WithNoEvidenceCatalog(catalog *pipeline.NoEvidenceCatalog) QueryOption {
	return func(opts *queryOptions) { opts.noEvidenceCatalog = catalog }
}

// WithCitationSupport selects the verifier used for requested citations.
func WithCitationSupport(verifier pipeline.CitationVerifier) QueryOption {
	return func(opts *queryOptions) { opts.citationVerifier = verifier }
}

// WithAuthorizedRetrieval binds every query to the verified principal in its
// request context. Scope, routing previews, vector searches, model inputs and
// every response are then limited to what that principal may read now, and the
// responses are private and uncacheable. Only a route behind subject
// verification may use it.
func WithAuthorizedRetrieval(authority domain.ReadAuthority) QueryOption {
	return func(opts *queryOptions) { opts.authority = authority }
}
