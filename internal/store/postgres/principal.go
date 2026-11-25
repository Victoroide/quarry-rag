package postgres

import (
	"context"
	"errors"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
)

// ResolvePrincipal looks up the exact delegated identity in the active catalog.
// Missing and disabled identities are indistinguishable to the caller.
func (c *Client) ResolvePrincipal(ctx context.Context, issuer, subject string) (domain.Principal, bool, error) {
	var principal domain.Principal
	err := c.Pool.QueryRow(ctx, `SELECT principal_id, tenant_id
		FROM grant_catalog_active_principals WHERE issuer = $1 AND subject = $2`, issuer, subject).
		Scan(&principal.ID, &principal.TenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Principal{}, false, nil
	}
	if err != nil {
		return domain.Principal{}, false, err
	}
	return principal, true, nil
}
