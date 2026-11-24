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
	reranker          retriever.Reranker
	noEvidenceCatalog *pipeline.NoEvidenceCatalog
}

// QueryOption configures optional query dependencies.
type QueryOption func(*queryOptions)

// WithReranker selects the reranker used before synthesis context selection.
func WithReranker(r retriever.Reranker) QueryOption {
	return func(opts *queryOptions) { opts.reranker = r }
}

// WithNoEvidenceCatalog selects the fixed translations for queries without evidence.
func WithNoEvidenceCatalog(catalog *pipeline.NoEvidenceCatalog) QueryOption {
	return func(opts *queryOptions) { opts.noEvidenceCatalog = catalog }
}
