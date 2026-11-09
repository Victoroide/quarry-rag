package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
)

const summaryColumns = `id::text, doc_type, collection, s3_key, status, chunk_count, file_name, file_type, created_at, updated_at, completed_at, last_error`

var _ domain.DocumentLister = (*Client)(nil)

func scanDocumentSummary(row pgx.Row) (*domain.DocumentSummary, error) {
	var doc domain.DocumentSummary
	if err := row.Scan(&doc.ID, &doc.DocType, &doc.Collection, &doc.S3Key, &doc.Status, &doc.ChunkCount, &doc.FileName, &doc.FileType, &doc.CreatedAt, &doc.UpdatedAt, &doc.CompletedAt, &doc.LastError); err != nil {
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
	query := `SELECT ` + summaryColumns + ` FROM documents`
	args := []any{}
	if collection != "" {
		query += ` WHERE collection = $1`
		args = append(args, collection)
	}
	rows, err := c.Pool.Query(ctx, query+` ORDER BY documents.id`, args...)
	if err != nil {
		return fmt.Errorf("postgres: stream documents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		doc, err := scanDocumentSummary(rows)
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
	query := `SELECT ` + summaryColumns + ` FROM documents WHERE ($1::text = '' OR id > NULLIF($1::text, '')::uuid)`
	args := []any{after, limit}
	if collection != "" {
		query += ` AND collection = $3`
		args = append(args, collection)
	}
	rows, err := c.Pool.Query(ctx, query+` ORDER BY documents.id LIMIT $2`, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: list documents page: %w", err)
	}
	defer rows.Close()
	docs := make([]domain.DocumentSummary, 0)
	for rows.Next() {
		doc, err := scanDocumentSummary(rows)
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
	doc, err := scanDocumentSummary(c.Pool.QueryRow(ctx, `SELECT `+summaryColumns+` FROM documents WHERE id = $1`, string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get document summary: %w", err)
	}
	return doc, nil
}
