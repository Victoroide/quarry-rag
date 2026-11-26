package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
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
	TenantID    string
	// DeclaredChunks is the count recorded on the document row; Chunks is the
	// number of stored chunk rows. A difference marks an inconsistent document.
	DeclaredChunks int
	Chunks         int
	TextBytes      int64
	Revision       string
	// ScopedVectors marks a document whose vectors use the tagged format, with
	// revision-qualified physical keys that a copy by logical key cannot reproduce.
	ScopedVectors bool
}

// DocumentSnapshot is one completed document read from a single snapshot.
type DocumentSnapshot struct {
	DocumentRevision
	Chunks []domain.Chunk
}

// revisionSQL hashes the identity that a target copy depends on. Chunk keys use
// the undashed form under which vector stores record them.
const eligibleDocument = `d.status = 'completed'
 AND NOT EXISTS (SELECT 1 FROM document_deletions x WHERE x.document_id = d.id AND (x.held OR NOT x.finalized))`

const documentTenantSQL = `CASE WHEN d.grant_catalog_assigned THEN COALESCE(grant_catalog_document_tenant(d.id, COALESCE(d.incarnation, d.id))::text, '') ELSE '' END`

const revisionSQL = `SELECT d.id::text, COALESCE(d.incarnation, d.id)::text, d.doc_type, d.collection, ` + documentTenantSQL + `, d.chunk_count, c.n, c.bytes,
 encode(sha256(convert_to(jsonb_build_array('v3', COALESCE(d.incarnation, d.id)::text, d.doc_type, d.collection, ` + documentTenantSQL + `, c.n, c.fingerprint)::text, 'UTF8')), 'hex'),
 d.scoped_vectors
 FROM eligible d CROSS JOIN LATERAL (
  SELECT count(*)::int AS n, COALESCE(sum(octet_length(text)),0) AS bytes,
   string_agg(id::text || ':' || chunk_index::text || ':' || token_count::text || ':' ||
     encode(sha256(convert_to(text, 'UTF8')), 'hex'), ',' ORDER BY chunk_index, id) AS fingerprint
  FROM chunks WHERE document_id=d.id
 ) c`

func scanRevision(rows pgx.Row, r *DocumentRevision) error {
	return rows.Scan(&r.ID, &r.Incarnation, &r.DocType, &r.Collection, &r.TenantID, &r.DeclaredChunks, &r.Chunks, &r.TextBytes, &r.Revision, &r.ScopedVectors)
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
	rows, err := tx.Query(ctx, `WITH eligible AS MATERIALIZED (SELECT d.* FROM documents d WHERE `+eligibleDocument+
		` AND ($1::text = '' OR d.id > NULLIF($1::text, '')::uuid) ORDER BY d.id LIMIT $2) `+revisionSQL+` ORDER BY d.id`, after, limit)
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

// HasDocumentsAfter checks whether a bounded scan stopped before the end.
func (c *Client) HasDocumentsAfter(ctx context.Context, after string) (bool, error) {
	var more bool
	err := c.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM documents d WHERE `+eligibleDocument+
		` AND ($1::text = '' OR d.id > NULLIF($1::text, '')::uuid))`, after).Scan(&more)
	return more, err
}

// LoadDocumentSnapshot reads a completed document, its revision and its chunks
// from one snapshot. It returns nil when the document is not completed.
func (c *Client) LoadDocumentSnapshot(ctx context.Context, id string) (*DocumentSnapshot, error) {
	return c.LoadDocumentSnapshotBounded(ctx, id, math.MaxInt, math.MaxInt64)
}

// ErrSnapshotLimit means the document exceeded a load bound before text was read.
var ErrSnapshotLimit = errors.New("document snapshot exceeds its chunk or byte budget")

// LoadDocumentSnapshotBounded checks the bounds in the same snapshot before
// transferring chunk text to the caller.
func (c *Client) LoadDocumentSnapshotBounded(ctx context.Context, id string, maxChunks int, maxBytes int64) (*DocumentSnapshot, error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var snapshot DocumentSnapshot
	err = scanRevision(tx.QueryRow(ctx, `WITH eligible AS (SELECT d.* FROM documents d WHERE `+eligibleDocument+
		` AND d.id = $1::uuid) `+revisionSQL, id), &snapshot.DocumentRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: load document revision %s: %w", id, err)
	}
	if snapshot.DocumentRevision.Chunks > maxChunks || snapshot.TextBytes > maxBytes {
		return nil, fmt.Errorf("%w (%d chunks, %d bytes)", ErrSnapshotLimit, snapshot.DocumentRevision.Chunks, snapshot.TextBytes)
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
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, rowsErr
	}
	var pdfProfile *string
	var pageResults []byte
	if err = tx.QueryRow(ctx, `SELECT profile, page_results FROM document_pdf_extractions WHERE document_id = $1::uuid`, id).Scan(&pdfProfile, &pageResults); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("postgres: load layout provenance of document %s: %w", id, err)
	}
	if pdfProfile != nil && *pdfProfile == domain.LayoutPDFProfile && len(pageResults) > 0 {
		var pages []domain.PDFPageResult
		if err = json.Unmarshal(pageResults, &pages); err != nil {
			return nil, fmt.Errorf("postgres: decode layout provenance of document %s: %w", id, err)
		}
		var sections []domain.LayoutSection
		for _, page := range pages {
			sections = append(sections, page.Sections...)
		}
		chunker.AttachLayoutProvenanceToChunks(snapshot.Chunks, sections)
	}
	return &snapshot, nil
}

// UnexpectedChunkKeys returns only keys absent from the eligible source corpus.
func (c *Client) UnexpectedChunkKeys(ctx context.Context, keys []string) ([]string, error) {
	ids := make([]string, 0, len(keys))
	for _, key := range keys {
		id, err := uuid.Parse(key)
		if err == nil && key == fmt.Sprintf("%x", id[:]) {
			ids = append(ids, id.String())
		}
	}
	rows, err := c.Pool.Query(ctx, `SELECT replace(c.id::text, '-', '') FROM chunks c
 JOIN documents d ON c.document_id=d.id WHERE c.id=ANY($1::uuid[]) AND `+eligibleDocument, ids)
	if err != nil {
		return nil, err
	}
	present, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	eligible := make(map[string]bool, len(present))
	for _, key := range present {
		eligible[key] = true
	}
	extra := make([]string, 0, len(keys)-len(present))
	for _, key := range keys {
		if !eligible[key] {
			extra = append(extra, key)
		}
	}
	return extra, nil
}
