package domain

import (
	"context"
	"time"
)

// Principal identifies a provisioned subject and its immutable tenant.
type Principal struct {
	ID       string
	TenantID string
}

// OwnerBinding is the server-verified owner of a document acceptance. The
// incarnation is the one the caller was authorized to use, so acceptance can
// refuse a document that was replaced after authorization.
type OwnerBinding struct {
	Principal   Principal
	Incarnation string
	ValidUntil  time.Time
}

// OwnedDecision is the outcome of authorizing a private upload before any
// extraction, object write or shared lookup.
type OwnedDecision int

// Owned authorization decisions. An existing, reserved and new target all
// authorize the caller; missing identities never reveal why they are unusable.
const (
	OwnedNew OwnedDecision = iota
	OwnedExisting
	OwnedReserved
	OwnedForbidden
	OwnedNotFound
)

// OwnedTarget names the document incarnation an authorized upload may use.
type OwnedTarget struct {
	Decision    OwnedDecision
	Incarnation string
}

// OwnedIngestStore authorizes and durably accepts private uploads for a verified principal.
type OwnedIngestStore interface {
	AuthorizeOwnedIngest(ctx context.Context, principal Principal, collection string, supplied DocumentID) (OwnedTarget, error)
	DocumentAcceptor
}
