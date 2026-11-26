package postgres

import (
	"context"
	"errors"
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

var (
	_ domain.ReadAuthority        = GrantReader{}
	_ domain.DocumentSetAuthority = GrantReader{}
)

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
	args := []any{principal.TenantID, principal.ID, ids, req.Collection, req.DocType, limit}
	relation, setPredicate := "grant_catalog_readable_documents", ""
	var set *domain.DocumentSetRef
	if req.DocumentSetID != "" {
		// Resolution and listing share one snapshot, so a set that expires or
		// loses a member between them cannot yield a listing that mixes both.
		if set, err = resolveDocumentSet(ctx, tx, principal, req.DocumentSetID); err != nil {
			return domain.ReadScope{}, err
		}
		relation, setPredicate = "grant_catalog_document_set_readable", " AND g.set_id=$7::uuid"
		args = append(args, set.ID)
	}
	rows, err := tx.Query(ctx, `
SELECT g.document_id::text, g.incarnation::text
FROM `+relation+` g
WHERE g.tenant_id=$1::uuid AND g.principal_id=$2::uuid`+setPredicate+`
  AND ($3::uuid[] IS NULL OR g.document_id=ANY($3::uuid[]))
  AND ($4='' OR g.collection=$4) AND ($5='' OR g.doc_type=$5)
ORDER BY g.document_id
LIMIT $6`, args...)
	if err != nil {
		return domain.ReadScope{}, fmt.Errorf("postgres: derive read scope: %w", err)
	}
	defer rows.Close()
	scope := domain.ReadScope{Revision: revision, DocumentSet: set}
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

// resolveDocumentSet finds a live set owned by the principal in its tenant.
// DeriveReadScope checks whether the principal is active before resolving a set.
func resolveDocumentSet(ctx context.Context, db revisionQuerier, principal domain.Principal, id string) (*domain.DocumentSetRef, error) {
	var ref domain.DocumentSetRef
	err := db.QueryRow(ctx, `
SELECT s.set_id::text, s.revision::text, s.expires_at
FROM grant_catalog_document_sets_live s
WHERE s.set_id=$1::uuid AND s.tenant_id=$2::uuid AND s.owner_principal_id=$3::uuid`,
		id, principal.TenantID, principal.ID).Scan(&ref.ID, &ref.Revision, &ref.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrDocumentSetNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: resolve document set: %w", err)
	}
	return &ref, nil
}

// DocumentSetState reads the principal's standing, the policy revision and the
// set's liveness in one statement, so they describe the same instant.
func (g GrantReader) DocumentSetState(ctx context.Context, principal domain.Principal, set domain.DocumentSetRef) (domain.ReadState, bool, error) {
	var current *int64
	var live bool
	err := g.Pool.QueryRow(ctx, `
SELECT grant_catalog_read_revision($1::uuid,$2::uuid),
  EXISTS (SELECT 1 FROM grant_catalog_document_sets_live s
          WHERE s.set_id=$3::uuid AND s.tenant_id=$1::uuid AND s.owner_principal_id=$2::uuid AND s.revision=$4::uuid)`,
		principal.TenantID, principal.ID, set.ID, set.Revision).Scan(&current, &live)
	if err != nil {
		return domain.ReadState{}, false, fmt.Errorf("postgres: read document set state: %w", err)
	}
	if current == nil {
		return domain.ReadState{}, live, nil
	}
	return domain.ReadState{Active: true, Revision: *current}, live, nil
}

// EvidenceInSet returns checks that join the readable members of one live set.
func (g GrantReader) EvidenceInSet(principal domain.Principal, set domain.DocumentSetRef, collection, docType string) domain.SubjectEvidence {
	subject := principal
	visibility := Visibility{Pool: g.Pool, Collection: collection, DocType: docType, Subject: &subject, DocumentSetID: set.ID}
	return domain.SubjectEvidence{
		Visibility: visibility,
		Documents:  visibility,
		Previews:   Previews{Pool: g.Pool, Collection: collection, DocType: docType, Subject: &subject, DocumentSetID: set.ID},
	}
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
