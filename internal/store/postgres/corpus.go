package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
)

// DocumentRevision identifies the stored content of one completed document
// that is not being deleted. The revision changes when the document is
// recreated or when its chunks, type or collection change.
type DocumentRevision struct {
	ID          string
	Incarnation string
	DocType     string
	Collection  string
	// DeclaredChunks is the count recorded on the document row; Chunks is the
	// number of stored chunk rows. A difference marks an inconsistent document.
	DeclaredChunks int
	Chunks         int
	Revision       string
}

// DocumentSnapshot is one completed document read from a single snapshot.
type DocumentSnapshot struct {
	DocumentRevision
	Chunks []domain.Chunk
}

// revisionSQL hashes the identity that a target copy depends on. Chunk keys use
// the undashed form under which vector stores record them.
const revisionSQL = `SELECT d.id::text, COALESCE(d.incarnation, d.id)::text, d.doc_type, d.collection, d.chunk_count, count(c.id)::int,
 encode(sha256(convert_to(concat_ws('|', 'v1', COALESCE(d.incarnation, d.id)::text, d.doc_type, d.collection, count(c.id)::text,
  COALESCE(string_agg(replace(c.id::text, '-', '') || ':' || c.chunk_index::text || ':' || c.token_count::text || ':' || encode(sha256(convert_to(c.text, 'UTF8')), 'hex'), ',' ORDER BY c.chunk_index, c.id), '')), 'UTF8')), 'hex')
 FROM documents d LEFT JOIN chunks c ON c.document_id = d.id
 WHERE d.status = 'completed'
 AND NOT EXISTS (SELECT 1 FROM document_deletions x WHERE x.document_id = d.id AND (x.held OR NOT x.finalized))`

func scanRevision(rows pgx.Row, r *DocumentRevision) error {
	return rows.Scan(&r.ID, &r.Incarnation, &r.DocType, &r.Collection, &r.DeclaredChunks, &r.Chunks, &r.Revision)
}

// DocumentRevisions lists completed documents after a document ID in ascending order.
func (c *Client) DocumentRevisions(ctx context.Context, after string, limit int) ([]DocumentRevision, error) {
	if limit < 1 {
		return nil, errors.New("postgres: revision page size must be positive")
	}
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, revisionSQL+` AND ($1::text = '' OR d.id > NULLIF($1::text, '')::uuid) GROUP BY d.id ORDER BY d.id LIMIT $2`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list document revisions: %w", err)
	}
	defer rows.Close()
	var out []DocumentRevision
	for rows.Next() {
		var r DocumentRevision
		if err = scanRevision(rows, &r); err != nil {
			return nil, fmt.Errorf("postgres: read document revision: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// LoadDocumentSnapshot reads a completed document, its revision and its chunks
// from one snapshot. It returns nil when the document is not completed.
func (c *Client) LoadDocumentSnapshot(ctx context.Context, id string) (*DocumentSnapshot, error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var snapshot DocumentSnapshot
	err = scanRevision(tx.QueryRow(ctx, revisionSQL+` AND d.id = $1::uuid GROUP BY d.id`, id), &snapshot.DocumentRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: load document revision %s: %w", id, err)
	}
	rows, err := tx.Query(ctx, `SELECT replace(id::text, '-', ''), chunk_index, text, token_count FROM chunks WHERE document_id = $1::uuid ORDER BY chunk_index, id`, id)
	if err != nil {
		return nil, fmt.Errorf("postgres: load chunks of document %s: %w", id, err)
	}
	defer rows.Close()
	for rows.Next() {
		chunk := domain.Chunk{DocumentID: domain.DocumentID(id)}
		var key string
		if err = rows.Scan(&key, &chunk.Index, &chunk.Text, &chunk.TokenCount); err != nil {
			return nil, fmt.Errorf("postgres: read chunk of document %s: %w", id, err)
		}
		chunk.ID = domain.ChunkID(key)
		snapshot.Chunks = append(snapshot.Chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &snapshot, nil
}
