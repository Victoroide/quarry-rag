package postgres

import (
	"context"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GrantReader derives and rechecks a verified principal's read authority from
// the authoritative primary database. It reads only the owner-defined readable
// relation and revision function, never the catalog tables.
type GrantReader struct {
	Pool *pgxpool.Pool
}

var _ domain.ReadAuthority = GrantReader{}

// DeriveReadScope lists the readable documents that satisfy the request in the
// same snapshot as the policy revision. The relation already excludes
// disabled principals, revoked grants and replaced or unfinished documents.
func (g GrantReader) DeriveReadScope(ctx context.Context, principal domain.Principal, req domain.ReadScopeRequest) (domain.ReadScope, error) {
	tx, err := g.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.ReadScope{}, fmt.Errorf("postgres: derive read scope: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	revision, active, err := readRevision(ctx, tx, principal)
	if err != nil {
		return domain.ReadScope{}, err
	}
	if !active {
		return domain.ReadScope{}, domain.ErrReadDenied
	}
	var ids any
	if req.DocumentIDs != nil {
		ids = canonicalDocumentIDs(req.DocumentIDs)
	}
	limit := 1
	if req.Membership > 0 {
		limit = req.Membership + 1
	}
	rows, err := tx.Query(ctx, `
SELECT g.document_id::text, g.incarnation::text
FROM grant_catalog_readable_documents g
WHERE g.tenant_id=$1::uuid AND g.principal_id=$2::uuid
  AND ($3::uuid[] IS NULL OR g.document_id=ANY($3::uuid[]))
  AND ($4='' OR g.collection=$4) AND ($5='' OR g.doc_type=$5)
ORDER BY g.document_id
LIMIT $6`, principal.TenantID, principal.ID, ids, req.Collection, req.DocType, limit)
	if err != nil {
		return domain.ReadScope{}, fmt.Errorf("postgres: derive read scope: %w", err)
	}
	defer rows.Close()
	scope := domain.ReadScope{Revision: revision}
	for rows.Next() {
		var doc domain.ReadableDocument
		if err := rows.Scan(&doc.ID, &doc.Incarnation); err != nil {
			return domain.ReadScope{}, fmt.Errorf("postgres: scan read scope: %w", err)
		}
		scope.Documents = append(scope.Documents, doc)
	}
	if err := rows.Err(); err != nil {
		return domain.ReadScope{}, fmt.Errorf("postgres: derive read scope: %w", err)
	}
	scope.Empty = len(scope.Documents) == 0
	if req.Membership <= 0 {
		scope.Documents = nil
	} else if len(scope.Documents) > req.Membership {
		scope.Exceeded, scope.Documents = true, nil
	}
	return scope, nil
}

// ReadState reports whether the principal is still active and the policy revision.
func (g GrantReader) ReadState(ctx context.Context, principal domain.Principal) (domain.ReadState, error) {
	revision, active, err := readRevision(ctx, g.Pool, principal)
	if err != nil {
		return domain.ReadState{}, err
	}
	return domain.ReadState{Active: active, Revision: revision}, nil
}

// Evidence returns checks that join the principal's readable relation.
func (g GrantReader) Evidence(principal domain.Principal, collection, docType string) domain.SubjectEvidence {
	subject := principal
	visibility := Visibility{Pool: g.Pool, Collection: collection, DocType: docType, Subject: &subject}
	return domain.SubjectEvidence{
		Visibility: visibility,
		Documents:  visibility,
		Previews:   Previews{Pool: g.Pool, Collection: collection, DocType: docType, Subject: &subject},
	}
}

type revisionQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readRevision(ctx context.Context, db revisionQuerier, principal domain.Principal) (revision int64, active bool, err error) {
	var current *int64
	if err = db.QueryRow(ctx, `SELECT grant_catalog_read_revision($1::uuid,$2::uuid)`, principal.TenantID, principal.ID).Scan(&current); err != nil {
		return 0, false, fmt.Errorf("postgres: read policy revision: %w", err)
	}
	if current == nil {
		return 0, false, nil
	}
	return *current, true, nil
}
