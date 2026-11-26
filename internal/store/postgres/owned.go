package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
)

var _ domain.OwnedIngestStore = (*Client)(nil)

// AuthorizeOwnedIngest decides what a private upload may use before any
// extraction, object write or shared lookup. A supplied identity that the
// principal cannot use yields the same decision whatever the reason.
func (c *Client) AuthorizeOwnedIngest(ctx context.Context, principal domain.Principal, collection string, supplied domain.DocumentID) (domain.OwnedTarget, error) {
	return authorizeOwned(ctx, c.Pool, principal, collection, supplied)
}

type ownedQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func authorizeOwned(ctx context.Context, db ownedQuerier, principal domain.Principal, collection string, supplied domain.DocumentID) (domain.OwnedTarget, error) {
	var document any
	if supplied != "" {
		document = string(supplied)
	}
	var outcome string
	var incarnation *string
	err := db.QueryRow(ctx, `SELECT outcome, target_incarnation::text FROM grant_catalog_authorize_ingest($1::uuid,$2::uuid,$3,$4::uuid)`,
		principal.TenantID, principal.ID, collection, document).Scan(&outcome, &incarnation)
	if err != nil {
		return domain.OwnedTarget{}, fmt.Errorf("postgres: authorize owned ingest: %w", err)
	}
	target := domain.OwnedTarget{}
	if incarnation != nil {
		target.Incarnation = *incarnation
	}
	switch outcome {
	case "new":
		target.Decision = domain.OwnedNew
	case "existing":
		target.Decision = domain.OwnedExisting
	case "reserved":
		target.Decision = domain.OwnedReserved
	case "forbidden":
		target.Decision = domain.OwnedForbidden
	case "missing":
		target.Decision = domain.OwnedNotFound
	default:
		return domain.OwnedTarget{}, fmt.Errorf("postgres: unexpected owned ingest decision %q", outcome)
	}
	return target, nil
}

var errOwnedPolicy = errors.New("postgres: owned acceptance requires private identity and no shared lookup")

// validateOwnedRequest rejects requests that could consult or publish a shared match.
func validateOwnedRequest(req domain.AcceptRequest, existingPDFOnly bool) error {
	owner := req.Owner
	if owner == nil {
		return nil
	}
	if req.ContentDedup != domain.ContentDedupOff || existingPDFOnly || req.TryLegacyPDFText ||
		req.EvaluationReceipt != "" || owner.Principal.ID == "" || owner.Principal.TenantID == "" ||
		owner.Incarnation == "" || owner.ValidUntil.IsZero() {
		return errOwnedPolicy
	}
	return nil
}

// authorizeOwnedAcceptance holds the principal's create permission until the
// transaction ends, then checks the target again under that lock. A revocation
// either waits for this commit or is already visible here.
func authorizeOwnedAcceptance(ctx context.Context, tx pgx.Tx, req domain.AcceptRequest) (domain.OwnedTarget, error) {
	owner := req.Owner
	var permitted bool
	err := tx.QueryRow(ctx, `SELECT grant_catalog_lock_ingest_permission($1::uuid,$2::uuid,$3)`,
		owner.Principal.TenantID, owner.Principal.ID, req.Collection).Scan(&permitted)
	if err != nil {
		return domain.OwnedTarget{}, err
	}
	if !permitted {
		return domain.OwnedTarget{Decision: domain.OwnedForbidden}, nil
	}
	supplied := domain.DocumentID("")
	if req.CallerSupplied {
		supplied = req.Document.ID
	}
	target, err := authorizeOwned(ctx, tx, owner.Principal, req.Collection, supplied)
	if err != nil {
		return target, err
	}
	switch target.Decision {
	case domain.OwnedForbidden, domain.OwnedNotFound:
		return target, nil
	case domain.OwnedNew:
		if req.CallerSupplied {
			return domain.OwnedTarget{Decision: domain.OwnedNotFound}, nil
		}
		return target, nil
	}
	if !req.CallerSupplied || target.Incarnation != owner.Incarnation {
		return domain.OwnedTarget{Decision: domain.OwnedNotFound}, nil
	}
	return target, nil
}

func bindAcceptedDocument(ctx context.Context, tx pgx.Tx, req domain.AcceptRequest, reserved bool) (bool, error) {
	owner := req.Owner
	var bound bool
	err := tx.QueryRow(ctx, `SELECT grant_catalog_bind_accepted_document($1::uuid,$2::uuid,$3,$4::uuid,$5::uuid,$6)`,
		owner.Principal.TenantID, owner.Principal.ID, req.Collection, string(req.Document.ID), owner.Incarnation, reserved).Scan(&bound)
	return bound, err
}

// documentTenant returns the tenant that owns one incarnation, or empty when no ownership remains.
func (c *Client) documentTenant(ctx context.Context, id, incarnation string) (string, error) {
	var tenant *string
	err := c.Pool.QueryRow(ctx, `SELECT grant_catalog_document_tenant($1::uuid,$2::uuid)::text`, id, incarnation).Scan(&tenant)
	if err != nil {
		return "", fmt.Errorf("postgres: look up document tenant: %w", err)
	}
	if tenant == nil {
		return "", nil
	}
	return *tenant, nil
}
