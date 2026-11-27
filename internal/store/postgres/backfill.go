package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	Hosted  *HostedBackfillSnapshot
}

// HostedBackfillSnapshot binds hosted verification to the current catalog and
// ordered manifest. Present records manifest intent even if it is invalid.
type HostedBackfillSnapshot struct {
	Present, Valid, SourceCurrent, DeletionReserved bool
	RevisionID, Digest, Collection                  string
	DeclaredChunks                                  int
	ChunkIndexes                                    []int32
	VectorIDs, ChunkIDs                             []string
}

// BackfillCandidates lists the next completed, assigned documents that are not
// yet verified, ordered by document so an interrupted run resumes after the
// last identity it finished. An empty cursor starts from the beginning.
func (c *Client) BackfillCandidates(ctx context.Context, after string, limit int) ([]BackfillDocument, error) {
	return c.backfillCandidates(ctx, after, limit, false)
}

// BackfillHostedCandidates lists quarantined assignments with their hosted
// vector and chunk bindings from the same PostgreSQL snapshot.
func (c *Client) BackfillHostedCandidates(ctx context.Context, after string, limit int) ([]BackfillDocument, error) {
	return c.backfillCandidates(ctx, after, limit, true)
}

func (c *Client) backfillCandidates(ctx context.Context, after string, limit int, hosted bool) ([]BackfillDocument, error) {
	var cursor any
	if after != "" {
		parsed, err := uuid.Parse(after)
		if err != nil {
			return nil, errors.New("postgres: backfill cursor must be a document UUID")
		}
		cursor = parsed.String()
	}
	const columns = `document_id::text, incarnation::text, tenant_id::text, source_version,
		chunk_ids, chunk_text_digests, chunk_token_counts, chunk_digest, scoped_vectors, COALESCE(vector_attempt::text, '')`
	query := `SELECT ` + columns + ` FROM grant_catalog_backfill_candidates($1::uuid,$2) AS candidate`
	if hosted {
		query = `SELECT ` + columns + `,
			manifest_present, manifest_valid, COALESCE(revision_id::text, ''), vector_ids, manifest_chunk_ids,
			COALESCE(manifest_digest, ''), source_current, declared_chunks, chunk_indexes, deletion_reserved, collection
			FROM grant_catalog_hosted_backfill_candidates($1::uuid,$2) AS candidate`
	}
	rows, err := c.Pool.Query(ctx, query+` ORDER BY candidate.document_id`, cursor, min(max(limit, 1), MaxBackfillBatch))
	if err != nil {
		return nil, fmt.Errorf("postgres: list backfill candidates: %w", err)
	}
	defer rows.Close()
	var documents []BackfillDocument
	for rows.Next() {
		var doc BackfillDocument
		fields := []any{&doc.DocumentID, &doc.Incarnation, &doc.TenantID, &doc.SourceVersion,
			&doc.ChunkIDs, &doc.ChunkTextDigests, &doc.ChunkTokenCounts, &doc.ChunkDigest, &doc.Scoped, &doc.Attempt}
		if hosted {
			snapshot := &HostedBackfillSnapshot{}
			doc.Hosted = snapshot
			fields = append(fields, &snapshot.Present, &snapshot.Valid, &snapshot.RevisionID, &snapshot.VectorIDs, &snapshot.ChunkIDs,
				&snapshot.Digest, &snapshot.SourceCurrent, &snapshot.DeclaredChunks, &snapshot.ChunkIndexes, &snapshot.DeletionReserved, &snapshot.Collection)
		}
		if err = rows.Scan(fields...); err != nil {
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

// CheckHostedBackfillPrivileges verifies that both operator entry points are
// installed and executable before an applied backfill contacts the provider.
func (c *Client) CheckHostedBackfillPrivileges(ctx context.Context) error {
	var list, publish bool
	err := c.Pool.QueryRow(ctx, `SELECT
		COALESCE(has_function_privilege(to_regprocedure('grant_catalog_hosted_backfill_candidates(uuid,integer)'), 'EXECUTE'), false),
		COALESCE(has_function_privilege(to_regprocedure('grant_catalog_publish_hosted_backfill(uuid,uuid,uuid,text,text,text)'), 'EXECUTE'), false)`).
		Scan(&list, &publish)
	if err != nil {
		return fmt.Errorf("postgres: check hosted backfill privileges: %w", err)
	}
	if !list || !publish {
		return errors.New("postgres: hosted backfill requires migration 044 and the catalog operator's hosted backfill grants")
	}
	return nil
}

// PublishHostedBackfill marks the matching verified assignment ready only
// while the source, current chunks and hosted vector binding still agree.
func (c *Client) PublishHostedBackfill(ctx context.Context, doc BackfillDocument) (bool, error) {
	if doc.Hosted == nil {
		return false, errors.New("postgres: missing hosted backfill snapshot")
	}
	// Use a fresh snapshot after lock waits even when the connection defaults
	// to a stronger isolation level. A canceled statement never reaches commit.
	var published bool
	err := pgx.BeginTxFunc(ctx, c.Pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		queryErr := tx.QueryRow(ctx, `SELECT grant_catalog_publish_hosted_backfill($1::uuid,$2::uuid,$3::uuid,$4,$5,$6)`,
			doc.DocumentID, doc.Incarnation, doc.TenantID, doc.SourceVersion, doc.ChunkDigest, doc.Hosted.Digest).Scan(&published)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return queryErr
	})
	if err != nil {
		return false, fmt.Errorf("postgres: publish hosted backfill: %w", err)
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
	table := VectorTable(c.servingPin().generation)
	var total, current int
	err := c.Pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE chunk_id = ANY($2::text[]))
		FROM `+pgx.Identifier{table}.Sanitize()+` WHERE document_id=$1::uuid`, doc.DocumentID, doc.ChunkIDs).Scan(&total, &current)
	if err != nil {
		return false, fmt.Errorf("postgres: verify vector rows: %w", err)
	}
	return total == len(doc.ChunkIDs) && current == len(doc.ChunkIDs), nil
}
