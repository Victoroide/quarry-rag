package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// MaxBackfillBatch bounds the documents read in one candidate query.
const MaxBackfillBatch = 50

// BackfillDocument is a quarantined historical assignment with the identities,
// content digests and token counts of its current chunks, read from the
// authoritative rows.
type BackfillDocument struct {
	DocumentID       string
	Incarnation      string
	TenantID         string
	SourceVersion    string
	ChunkDigest      string
	ChunkIDs         []string
	ChunkTextDigests []string
	ChunkTokenCounts []int32
	// Scoped reports that hosted vectors carry the revision and attempt tags and a
	// qualified identifier. Attempt is the tag of the attempt that completed the document.
	Scoped  bool
	Attempt string
}

// BackfillCandidates lists the next completed, assigned documents that are not
// yet verified, ordered by document so an interrupted run resumes after the
// last identity it finished. An empty cursor starts from the beginning.
func (c *Client) BackfillCandidates(ctx context.Context, after string, limit int) ([]BackfillDocument, error) {
	var cursor any
	if after != "" {
		parsed, err := uuid.Parse(after)
		if err != nil {
			return nil, errors.New("postgres: backfill cursor must be a document UUID")
		}
		cursor = parsed.String()
	}
	rows, err := c.Pool.Query(ctx, `SELECT document_id::text, incarnation::text, tenant_id::text, source_version,
		chunk_ids, chunk_text_digests, chunk_token_counts, chunk_digest, scoped_vectors, COALESCE(vector_attempt::text, '')
		FROM grant_catalog_backfill_candidates($1::uuid,$2)`, cursor, min(max(limit, 1), MaxBackfillBatch))
	if err != nil {
		return nil, fmt.Errorf("postgres: list backfill candidates: %w", err)
	}
	defer rows.Close()
	var documents []BackfillDocument
	for rows.Next() {
		var doc BackfillDocument
		if err = rows.Scan(&doc.DocumentID, &doc.Incarnation, &doc.TenantID, &doc.SourceVersion,
			&doc.ChunkIDs, &doc.ChunkTextDigests, &doc.ChunkTokenCounts, &doc.ChunkDigest, &doc.Scoped, &doc.Attempt); err != nil {
			return nil, fmt.Errorf("postgres: read backfill candidate: %w", err)
		}
		documents = append(documents, doc)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list backfill candidates: %w", err)
	}
	return documents, nil
}

// PublishVerifiedDocument marks one document readable. It reports false, and
// changes nothing, when the document row or its chunks differ from the snapshot
// that was verified, so changed data stays quarantined.
func (c *Client) PublishVerifiedDocument(ctx context.Context, doc BackfillDocument) (bool, error) {
	var published bool
	err := c.Pool.QueryRow(ctx, `SELECT grant_catalog_publish_verified_document($1::uuid,$2::uuid,$3,$4)`,
		doc.DocumentID, doc.Incarnation, doc.SourceVersion, doc.ChunkDigest).Scan(&published)
	if err != nil {
		return false, fmt.Errorf("postgres: publish verified document: %w", err)
	}
	return published, nil
}

// VectorRowsMatch reports whether the pgvector table holds exactly the
// document's current chunks: every current chunk has a row for this document
// and no row of the document is stale.
func (c *Client) VectorRowsMatch(ctx context.Context, doc BackfillDocument) (bool, error) {
	if len(doc.ChunkIDs) == 0 {
		return false, nil
	}
	var total, current int
	err := c.Pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE chunk_id = ANY($2::text[]))
		FROM chunk_vectors WHERE document_id=$1::uuid`, doc.DocumentID, doc.ChunkIDs).Scan(&total, &current)
	if err != nil {
		return false, fmt.Errorf("postgres: verify vector rows: %w", err)
	}
	return total == len(doc.ChunkIDs) && current == len(doc.ChunkIDs), nil
}
