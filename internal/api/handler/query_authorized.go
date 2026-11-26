package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
)

// errScopeTooLarge reports a valid scope that a hosted search cannot carry.
var errScopeTooLarge = errors.New("authorized document scope exceeds capacity")

// authorizedQuery is the part of a query that exists only under a verified principal.
type authorizedQuery struct {
	authority domain.ReadAuthority
	kind      domain.GrantScopeKind
}

// newAuthorizedQuery fails closed when the store cannot confine a search to a
// principal's grants. The handler then refuses every request instead of ever
// using an unscoped search.
func newAuthorizedQuery(authority domain.ReadAuthority, ret *retriever.Retriever) (*authorizedQuery, bool) {
	if authority == nil || ret == nil {
		return nil, false
	}
	kind, capable := ret.GrantScope()
	if !capable {
		return nil, false
	}
	return &authorizedQuery{authority: authority, kind: kind}, true
}

func refuseAuthorizedQuery(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "private, no-store")
	writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "authorization unavailable")
}

// derive intersects the principal's current readable documents with the inline
// IDs, collection and type before anything is embedded or searched. Foreign and
// unknown UUIDs simply drop out. An explicit empty list never widens the scope.
//
// A document set reference also intersects the scope with that immutable
// membership. Missing, foreign, deleted and expired references share one error.
func (a *authorizedQuery) derive(ctx context.Context, subject middleware.VerifiedSubject, req *QueryRequest, cfg *config.Config, sets *documentSets, setID string) (*pipeline.ReadAuthorization, []string, error) {
	// A membership store needs the listing. A relation store joins the live
	// relation, so it lists only to narrow an inline allowlist to what survives.
	materialize := a.kind == domain.GrantScopeMembership || req.AllowedDocumentIDs != nil
	request := domain.ReadScopeRequest{DocumentIDs: req.AllowedDocumentIDs, Collection: req.Collection, DocType: req.FilterDocType, DocumentSetID: setID}
	if materialize {
		request.Membership = maxAllowedDocumentIDs(cfg)
		if setID != "" && a.kind == domain.GrantScopeMembership {
			// A set can be larger than an inline list. Its ceiling bounds what a
			// hosted search carries, and the request budget prices the rest.
			request.Membership = sets.maxMembers
		}
	}
	scope, err := a.authority.DeriveReadScope(ctx, subject.Principal, request)
	if err != nil {
		return nil, nil, err
	}
	if scope.Exceeded {
		return nil, nil, errScopeTooLarge
	}
	authorized := domain.AuthorizedScope{TenantID: subject.Principal.TenantID, PrincipalID: subject.Principal.ID}
	allowed := req.AllowedDocumentIDs
	switch {
	case scope.Empty:
		allowed = []string{}
	case materialize:
		allowed = make([]string, 0, len(scope.Documents))
		for _, doc := range scope.Documents {
			allowed = append(allowed, doc.ID)
		}
		slices.Sort(allowed)
	}
	if a.kind == domain.GrantScopeMembership {
		authorized.Incarnations = make(map[string]string, len(scope.Documents))
		for _, doc := range scope.Documents {
			authorized.Incarnations[doc.ID] = doc.Incarnation
		}
	}
	authorized.DocumentSet = scope.DocumentSet
	authorization := pipeline.NewReadAuthorization(subject.Principal, subject.ExpiresAt, a.authority, authorized, scope.Revision)
	authorization.DocumentSet = scope.DocumentSet
	authorization.RecheckTrust = subject.RecheckTrust
	return authorization, allowed, nil
}

// stages builds the routing and retrieval stages over checks that join the
// principal's readable relation. Previews and visibility never see a document
// the principal cannot read.
func (a *authorizedQuery) stages(ret *retriever.Retriever, principal domain.Principal, req *QueryRequest, routingCfg pipeline.RoutingConfig, setID string) (routing *pipeline.RoutingStage, retrieval *pipeline.RetrievalStage, evidence domain.EvidenceVisibility, ok bool) {
	checks := a.authority.Evidence(principal, req.Collection, req.FilterDocType)
	if setID != "" {
		// Candidates and previews must also belong to the set, so a document
		// outside it can never be read even if a store returned it.
		sets, capable := a.authority.(domain.DocumentSetAuthority)
		if !capable {
			return nil, nil, nil, false
		}
		checks = sets.EvidenceInSet(principal, domain.DocumentSetRef{ID: setID}, req.Collection, req.FilterDocType)
	}
	if checks.Visibility == nil || checks.Documents == nil || checks.Previews == nil {
		return nil, nil, nil, false
	}
	checks = pipeline.FailClosedChecks(checks)
	return pipeline.NewRoutingStageWithConfig(ret, checks.Documents, checks.Previews, routingCfg),
		pipeline.NewRetrievalStageWithVisibility(ret, checks.Documents), checks.Visibility, true
}

// authorizedFailure names the response for a failure that happened while
// deriving the scope. Nothing here depends on document content.
func authorizedFailure(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, errScopeTooLarge):
		writeJSONError(w, http.StatusUnprocessableEntity, "scope_too_large", "authorized document scope exceeds the supported size")
	case errors.Is(err, domain.ErrDocumentSetNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", documentSetNotFoundMessage)
	case errors.Is(err, domain.ErrReadDenied):
		writeJSONError(w, http.StatusForbidden, "forbidden", "access denied")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, pipeline.ErrCredentialExpired):
		writeQueryError(w, r, err)
	default:
		// A lookup that failed never decides the scope, so nothing is searched.
		slog.ErrorContext(r.Context(), "derive read scope", "error", err)
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "authorization unavailable")
	}
}

// boundedFailure maps a failure after dispatch to the credential outcome when
// the request context ended because the credential did.
func boundedFailure(err error, authorization *pipeline.ReadAuthorization) error {
	if err == nil || authorization == nil {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) && authorization.Expired() {
		return pipeline.ErrCredentialExpired
	}
	return err
}

// credentialFailure reports the credential outcome when the request context
// ended at the credential's own deadline.
func credentialFailure(err error, subject middleware.VerifiedSubject) error {
	if errors.Is(err, context.DeadlineExceeded) && !subject.ValidAt(time.Now()) {
		return pipeline.ErrCredentialExpired
	}
	return err
}

func subjectValid(subject middleware.VerifiedSubject, ok bool) bool {
	return ok && subject.ValidAt(time.Now())
}
