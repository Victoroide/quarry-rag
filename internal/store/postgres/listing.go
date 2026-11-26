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

var _ domain.DocumentLister = (*Client)(nil)

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
