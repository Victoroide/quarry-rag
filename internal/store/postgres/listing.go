package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const summaryColumns = `id::text, doc_type, collection, s3_key, status, chunk_count, file_name, file_type, created_at, updated_at, completed_at, last_error, COALESCE(index_visibility,'unknown'), index_visibility_observed_at,
	COALESCE((SELECT coverage FROM document_pdf_extractions WHERE document_id=documents.id),'')`

var (
	_ domain.DocumentLister       = (*Client)(nil)
	_ domain.ScopedDocumentLister = (*Client)(nil)
)

type documentLister struct {
	pool               *pgxpool.Pool
	includeIncarnation bool
}

// WithDocumentIncarnations returns a lister with the historical UUID fallback.
func (c *Client) WithDocumentIncarnations() domain.DocumentLister {
	return documentLister{pool: c.Pool, includeIncarnation: true}
}

func (c documentLister) columns() string {
	if c.includeIncarnation {
		return summaryColumns + `, COALESCE(incarnation,id)::text`
	}
	return summaryColumns
}

func (c documentLister) scanDocumentSummary(row pgx.Row) (*domain.DocumentSummary, error) {
	var doc domain.DocumentSummary
	dest := []any{&doc.ID, &doc.DocType, &doc.Collection, &doc.S3Key, &doc.Status, &doc.ChunkCount, &doc.FileName, &doc.FileType, &doc.CreatedAt, &doc.UpdatedAt, &doc.CompletedAt, &doc.LastError, &doc.IndexVisibility, &doc.IndexVisibilityObservedAt, &doc.PDFCoverage}
	if c.includeIncarnation {
		dest = append(dest, &doc.Incarnation)
	}
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	return &doc, nil
}

// StreamDocuments visits document summaries in ascending identifier order.
func (c *Client) StreamDocuments(ctx context.Context, visit func(domain.DocumentSummary) error) error {
	return c.StreamDocumentsInCollection(ctx, "", visit)
}

// StreamDocumentsInCollection visits summaries in one collection, or all collections when empty.
func (c *Client) StreamDocumentsInCollection(ctx context.Context, collection string, visit func(domain.DocumentSummary) error) error {
	return (documentLister{pool: c.Pool}).StreamDocumentsInCollection(ctx, collection, visit)
}

func (c documentLister) StreamDocuments(ctx context.Context, visit func(domain.DocumentSummary) error) error {
	return c.StreamDocumentsInCollection(ctx, "", visit)
}

func (c documentLister) StreamDocumentsInCollection(ctx context.Context, collection string, visit func(domain.DocumentSummary) error) error {
	query := `SELECT ` + c.columns() + ` FROM documents WHERE status <> 'deleting'`
	args := []any{}
	if collection != "" {
		query += ` AND collection = $1`
		args = append(args, collection)
	}
	rows, err := c.pool.Query(ctx, query+` ORDER BY documents.id`, args...)
	if err != nil {
		return fmt.Errorf("postgres: stream documents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		doc, err := c.scanDocumentSummary(rows)
		if err != nil {
			return fmt.Errorf("postgres: scan document summary: %w", err)
		}
		if err := visit(*doc); err != nil {
			return fmt.Errorf("postgres: visit document summary: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: stream documents: %w", err)
	}
	return nil
}

// ListDocumentsPage returns summaries strictly after the supplied identifier.
func (c *Client) ListDocumentsPage(ctx context.Context, after string, limit int) ([]domain.DocumentSummary, error) {
	return c.ListDocumentsPageInCollection(ctx, after, limit, "")
}

// ListDocumentsPageInCollection returns a page within a collection, or all collections when empty.
func (c *Client) ListDocumentsPageInCollection(ctx context.Context, after string, limit int, collection string) ([]domain.DocumentSummary, error) {
	return (documentLister{pool: c.Pool}).ListDocumentsPageInCollection(ctx, after, limit, collection)
}

func (c documentLister) ListDocumentsPage(ctx context.Context, after string, limit int) ([]domain.DocumentSummary, error) {
	return c.ListDocumentsPageInCollection(ctx, after, limit, "")
}

func (c documentLister) ListDocumentsPageInCollection(ctx context.Context, after string, limit int, collection string) ([]domain.DocumentSummary, error) {
	query := `SELECT ` + c.columns() + ` FROM documents WHERE status <> 'deleting' AND ($1::text = '' OR id > NULLIF($1::text, '')::uuid)`
	args := []any{after, limit}
	if collection != "" {
		query += ` AND collection = $3`
		args = append(args, collection)
	}
	rows, err := c.pool.Query(ctx, query+` ORDER BY documents.id LIMIT $2`, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: list documents page: %w", err)
	}
	defer rows.Close()
	docs := make([]domain.DocumentSummary, 0)
	for rows.Next() {
		doc, err := c.scanDocumentSummary(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan document summary: %w", err)
		}
		docs = append(docs, *doc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list documents page: %w", err)
	}
	return docs, nil
}

// GetDocumentSummary returns nil when the document does not exist.
func (c *Client) GetDocumentSummary(ctx context.Context, id domain.DocumentID) (*domain.DocumentSummary, error) {
	return (documentLister{pool: c.Pool}).GetDocumentSummary(ctx, id)
}

func (c documentLister) GetDocumentSummary(ctx context.Context, id domain.DocumentID) (*domain.DocumentSummary, error) {
	doc, err := c.scanDocumentSummary(c.pool.QueryRow(ctx, `SELECT `+c.columns()+` FROM documents WHERE id = $1 AND status <> 'deleting'`, string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get document summary: %w", err)
	}
	return doc, nil
}

// ScopedPolicyState reports whether the principal is active and the current policy revision.
func (c *Client) ScopedPolicyState(ctx context.Context, principal domain.Principal) (domain.ScopedPolicyState, error) {
	var state domain.ScopedPolicyState
	if err := c.Pool.QueryRow(ctx, `SELECT active, policy_revision FROM grant_catalog_policy_state($1::uuid,$2::uuid)`,
		principal.TenantID, principal.ID).Scan(&state.Active, &state.Revision); err != nil {
		return domain.ScopedPolicyState{}, fmt.Errorf("postgres: read scoped policy state: %w", err)
	}
	return state, nil
}

// ListScopedDocumentsPage returns one page of summaries visible to the verified
// principal, together with the snapshot policy revision.
func (c *Client) ListScopedDocumentsPage(ctx context.Context, principal domain.Principal, after string, limit int, collection string, includeIncarnation bool) (domain.ScopedDocumentPage, error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.ScopedDocumentPage{}, fmt.Errorf("postgres: begin scoped listing: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	state, err := readScopedPolicyStateTx(ctx, tx, principal)
	if err != nil {
		return domain.ScopedDocumentPage{}, err
	}
	if !state.Active {
		return domain.ScopedDocumentPage{Active: false, Revision: state.Revision}, nil
	}
	var cursor any
	if after != "" {
		cursor = after
	}
	rows, err := tx.Query(ctx, `SELECT id, doc_type, collection, status, chunk_count, file_name, file_type,
		created_at, updated_at, completed_at, last_error, index_visibility, index_visibility_observed_at, pdf_coverage, incarnation
		FROM grant_catalog_list_documents($1::uuid,$2::uuid,$3::uuid,$4,$5,$6)`,
		principal.TenantID, principal.ID, cursor, limit, collection, includeIncarnation)
	if err != nil {
		return domain.ScopedDocumentPage{}, fmt.Errorf("postgres: list scoped documents: %w", err)
	}
	defer rows.Close()
	docs := make([]domain.DocumentSummary, 0)
	for rows.Next() {
		doc, scanErr := scanScopedDocumentSummary(rows, includeIncarnation)
		if scanErr != nil {
			return domain.ScopedDocumentPage{}, fmt.Errorf("postgres: scan scoped document summary: %w", scanErr)
		}
		docs = append(docs, *doc)
	}
	if err := rows.Err(); err != nil {
		return domain.ScopedDocumentPage{}, fmt.Errorf("postgres: list scoped documents: %w", err)
	}
	return domain.ScopedDocumentPage{Active: true, Revision: state.Revision, Documents: docs}, tx.Commit(ctx)
}

// GetScopedDocumentSummary returns one readable summary for the verified principal,
// or nil when the document is missing, unassigned or denied.
func (c *Client) GetScopedDocumentSummary(ctx context.Context, principal domain.Principal, id domain.DocumentID, includeIncarnation bool) (domain.ScopedDocumentSummary, error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.ScopedDocumentSummary{}, fmt.Errorf("postgres: begin scoped status: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	state, err := readScopedPolicyStateTx(ctx, tx, principal)
	if err != nil {
		return domain.ScopedDocumentSummary{}, err
	}
	if !state.Active {
		return domain.ScopedDocumentSummary{Active: false, Revision: state.Revision}, nil
	}
	row := tx.QueryRow(ctx, `SELECT id, doc_type, collection, status, chunk_count, file_name, file_type,
		created_at, updated_at, completed_at, last_error, index_visibility, index_visibility_observed_at, pdf_coverage, incarnation
		FROM grant_catalog_get_document($1::uuid,$2::uuid,$3::uuid,$4)`,
		principal.TenantID, principal.ID, string(id), includeIncarnation)
	doc, err := scanScopedDocumentSummary(row, includeIncarnation)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ScopedDocumentSummary{Active: true, Revision: state.Revision}, tx.Commit(ctx)
	}
	if err != nil {
		return domain.ScopedDocumentSummary{}, fmt.Errorf("postgres: get scoped document summary: %w", err)
	}
	return domain.ScopedDocumentSummary{Active: true, Revision: state.Revision, Document: doc}, tx.Commit(ctx)
}

func readScopedPolicyStateTx(ctx context.Context, tx pgx.Tx, principal domain.Principal) (domain.ScopedPolicyState, error) {
	var state domain.ScopedPolicyState
	if err := tx.QueryRow(ctx, `SELECT active, policy_revision FROM grant_catalog_policy_state($1::uuid,$2::uuid)`,
		principal.TenantID, principal.ID).Scan(&state.Active, &state.Revision); err != nil {
		return domain.ScopedPolicyState{}, fmt.Errorf("postgres: read scoped policy state: %w", err)
	}
	return state, nil
}

func scanScopedDocumentSummary(row pgx.Row, includeIncarnation bool) (*domain.DocumentSummary, error) {
	var doc domain.DocumentSummary
	var incarnation string
	if err := row.Scan(
		&doc.ID, &doc.DocType, &doc.Collection, &doc.Status, &doc.ChunkCount,
		&doc.FileName, &doc.FileType, &doc.CreatedAt, &doc.UpdatedAt, &doc.CompletedAt,
		&doc.LastError, &doc.IndexVisibility, &doc.IndexVisibilityObservedAt, &doc.PDFCoverage, &incarnation,
	); err != nil {
		return nil, err
	}
	if includeIncarnation {
		doc.Incarnation = incarnation
	}
	return &doc, nil
}
