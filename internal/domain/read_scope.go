package domain

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
)

// GrantScopeKind says how a vector store enforces an authorized read scope.
type GrantScopeKind string

// Grant scope enforcement kinds.
const (
	// GrantScopeRelation joins the readable-document relation inside the
	// store's own ranking statement.
	GrantScopeRelation GrantScopeKind = "relation"
	// GrantScopeMembership sends the tenant and the allowed document
	// incarnations with every request, in bounded predicates.
	GrantScopeMembership GrantScopeKind = "membership"
)

// ErrGrantScopeUnsupported marks a search that carries an authorized scope the
// vector store cannot enforce. The caller must fail instead of searching without it.
var ErrGrantScopeUnsupported = errors.New("vector store cannot enforce an authorized read scope")

// ErrReadDenied marks a principal that is no longer active or no longer holds
// the read authority a request was admitted with.
var ErrReadDenied = errors.New("read access denied")

// GrantScopedVectorStore marks a store that applies VectorFilter.Authorized to
// every ranking and candidate request before results leave the store.
type GrantScopedVectorStore interface {
	GrantScope() GrantScopeKind
}

// AuthorizedScope is the server-derived read authority of one request. It is
// attached to every search so no store method can return an unscoped result.
type AuthorizedScope struct {
	TenantID    string
	PrincipalID string
	// Incarnations maps each canonical document ID the principal may read to
	// the incarnation it may read. Membership stores require it. Relation
	// stores ignore it because their SQL joins the live relation.
	Incarnations map[string]string
}

// IncarnationOf returns the readable incarnation for a document ID in any UUID
// spelling. Identifiers that are not UUIDs have none.
func (s *AuthorizedScope) IncarnationOf(documentID string) (string, bool) {
	if s == nil {
		return "", false
	}
	parsed, err := uuid.Parse(documentID)
	if err != nil {
		return "", false
	}
	incarnation, ok := s.Incarnations[parsed.String()]
	return incarnation, ok
}

// SameTenant reports whether hosted metadata names the scope's tenant. Absent
// metadata is not a match, because an owned vector always carries its tenant.
func (s *AuthorizedScope) SameTenant(metadata map[string]any) bool {
	tenant, _ := metadata[VectorTenantKey].(string)
	return s != nil && tenant != "" && strings.EqualFold(tenant, s.TenantID)
}

// ReadableDocument is a completed document incarnation a principal may read now.
type ReadableDocument struct {
	ID          string
	Incarnation string
}

// ReadScopeRequest asks which readable documents satisfy the caller's narrowing.
type ReadScopeRequest struct {
	// DocumentIDs narrows the scope. Nil means the caller supplied none; an
	// empty non-nil list matches nothing.
	DocumentIDs []string
	Collection  string
	DocType     string
	// Membership is the largest number of documents to list. Zero asks only
	// whether any document qualifies.
	Membership int
}

// ReadScope is the current intersection of the principal's grants and the request.
type ReadScope struct {
	// Revision is the policy revision the decision was made at.
	Revision int64
	// Empty reports that no document qualifies.
	Empty bool
	// Exceeded reports that more than ReadScopeRequest.Membership documents
	// qualify. Documents is then not a complete listing.
	Exceeded  bool
	Documents []ReadableDocument
}

// ReadState is the principal's current standing in the policy catalog.
type ReadState struct {
	// Active reports that the principal exists, is enabled and has a tenant.
	Active   bool
	Revision int64
}

// SubjectEvidence checks candidates and previews against one principal's current grants.
type SubjectEvidence struct {
	Visibility EvidenceVisibility
	Documents  DocumentVisibility
	Previews   DocumentPreviewer
}

// ReadAuthority derives and rechecks what a verified principal may read.
type ReadAuthority interface {
	// DeriveReadScope reads the intersection in one snapshot, so the revision
	// and the listing describe the same policy.
	DeriveReadScope(ctx context.Context, principal Principal, req ReadScopeRequest) (ReadScope, error)
	// ReadState reports the principal's standing and the policy revision.
	ReadState(ctx context.Context, principal Principal) (ReadState, error)
	// Evidence returns visibility and preview checks that require every
	// document to be readable by the principal at its current incarnation.
	Evidence(principal Principal, collection, docType string) SubjectEvidence
}
