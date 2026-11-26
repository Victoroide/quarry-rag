package handler

import (
	"encoding/json"
	"log/slog"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/google/uuid"
)

const (
	defaultDocumentSetMaxMembers = config.DocumentSetMembersLimit
	defaultDocumentSetMaxBatches = config.DocumentSetBatchesLimit
	documentSetNotFoundMessage   = "document set not found"
)

// documentSets serves queries that select documents through a subject-bound
// set. It exists only when the deployment enabled sets under verified subjects.
type documentSets struct {
	authority  domain.DocumentSetAuthority
	maxMembers int
	maxBatches int
	// capable reports that the authority and the vector store can both confine a
	// search to a set. Without it every query that names a set is refused.
	capable bool
}

// newDocumentSets returns nil unless sets are enabled. It logs, and then
// refuses every set query, when the authority or the store cannot serve them.
func newDocumentSets(cfg *config.Config, authz *authorizedQuery, ret *retriever.Retriever) *documentSets {
	if !cfg.DocumentSetsEnabled || authz == nil {
		return nil
	}
	sets := &documentSets{maxMembers: cfg.DocumentSetMaxMembers, maxBatches: cfg.DocumentSetMaxBatches}
	if sets.maxMembers <= 0 {
		sets.maxMembers = defaultDocumentSetMaxMembers
	}
	if sets.maxBatches <= 0 {
		sets.maxBatches = defaultDocumentSetMaxBatches
	}
	authority, ok := authz.authority.(domain.DocumentSetAuthority)
	sets.authority = authority
	sets.capable = ok && ret != nil && ret.SupportsDocumentSets()
	if !sets.capable {
		slog.Error("document sets require a read authority and a vector store that confine a search to a set")
	}
	return sets
}

// documentSetReference is the optional reference of a request. Decoding
// accepts only a JSON string holding a UUID and stores it canonically, so an
// absent field is the one empty value. A null, a number, an empty string or
// any other spelling is a malformed reference, which is never the same
// failure as a reference that does not resolve.
type documentSetReference string

var errMalformedReference = fieldError{"document_set_id must be a UUID"}

// UnmarshalJSON implements json.Unmarshaler.
func (r *documentSetReference) UnmarshalJSON(data []byte) error {
	var text string
	if len(data) == 0 || data[0] != '"' || json.Unmarshal(data, &text) != nil || len(text) != 36 {
		return errMalformedReference
	}
	parsed, err := uuid.Parse(text)
	if err != nil {
		return errMalformedReference
	}
	*r = documentSetReference(parsed.String())
	return nil
}

// requestedDocumentSet returns the reference a request supplied. A reference
// sent where sets are not enabled is refused, so a deployment that has not
// enabled them never reads or resolves one.
func requestedDocumentSet(req *QueryRequest, enabled bool) (id string, supplied bool, apiErr *APIError) {
	if req.DocumentSetID == "" {
		return "", false, nil
	}
	if !enabled {
		return "", true, &APIError{Error: "validation", Message: "document_set_id is not supported"}
	}
	return string(req.DocumentSetID), true, nil
}

// planSearch prices the hosted searches of a set query before anything is
// embedded. A relation store runs one statement and needs no plan. A hosted
// store searches the intersection in partitions, within the request's budget.
func (s *documentSets) planSearch(ret *retriever.Retriever, kind domain.GrantScopeKind, members int, mode string) (plan retriever.SearchPlan, budget *domain.SearchBudget, err error) {
	if kind != domain.GrantScopeMembership {
		return retriever.SearchPlan{}, nil, nil
	}
	partitions, priced := ret.SearchPartitions(members)
	if !priced {
		return retriever.SearchPlan{}, nil, domain.ErrGrantScopeUnsupported
	}
	if plan, err = retriever.PlanHostedSearch(partitions, s.maxBatches, mode); err != nil {
		return retriever.SearchPlan{}, nil, errScopeTooLarge
	}
	if partitions == 0 {
		return plan, nil, nil
	}
	return plan, domain.NewSearchBudget(s.maxBatches, plan.Planned), nil
}
