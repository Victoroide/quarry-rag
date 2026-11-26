package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

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
	// ActiveRevisionID holds the active revision UUID when the document uses
	// hosted vector manifests, or empty otherwise. A document with an active
	// revision keeps its vectors under manifest keys, whether or not it is also
	// marked ScopedVectors.
	ActiveRevisionID string
	// VectorAttempt is the published vector attempt of a tagged document.
	VectorAttempt string
	// VectorIncarnationRequired marks a document whose vectors must carry the
	// separate incarnation tag in addition to any revision-qualified identity.
	VectorIncarnationRequired bool
	// MutationPending marks a document whose vector write has not settled.
	MutationPending bool
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
 encode(sha256(convert_to((CASE WHEN d.scoped_vectors AND r.active_revision_id IS NULL
  THEN jsonb_build_array('v4', COALESCE(d.incarnation, d.id)::text, d.doc_type, d.collection, ` + documentTenantSQL + `, '', c.n, c.fingerprint, 'tagged', COALESCE(d.vector_attempt::text, ''))
  ELSE jsonb_build_array('v4', COALESCE(d.incarnation, d.id)::text, d.doc_type, d.collection, ` + documentTenantSQL + `, COALESCE(r.active_revision_id::text, ''), c.n, c.fingerprint) END)::text, 'UTF8')), 'hex'),
 d.scoped_vectors, COALESCE(r.active_revision_id::text, ''), COALESCE(d.vector_attempt::text, ''), d.vector_incarnation_required, d.vector_mutation_pending
 FROM eligible d
 LEFT JOIN hosted_vector_revisions r ON r.document_id = d.id
 CROSS JOIN LATERAL (
  SELECT count(*)::int AS n, COALESCE(sum(octet_length(text)),0) AS bytes,
   string_agg(id::text || ':' || chunk_index::text || ':' || token_count::text || ':' ||
     encode(sha256(convert_to(text, 'UTF8')), 'hex'), ',' ORDER BY chunk_index, id) AS fingerprint
  FROM chunks WHERE document_id=d.id
 ) c`

func scanRevision(rows pgx.Row, r *DocumentRevision) error {
	return rows.Scan(&r.ID, &r.Incarnation, &r.DocType, &r.Collection, &r.TenantID, &r.DeclaredChunks, &r.Chunks, &r.TextBytes, &r.Revision, &r.ScopedVectors, &r.ActiveRevisionID, &r.VectorAttempt, &r.VectorIncarnationRequired, &r.MutationPending)
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

// UnexpectedChunkKeys returns the keys that are not current in the eligible
// source corpus: bare chunk keys and hosted manifest keys. On a hosted target a
// tagged document is current only under its tagged physical keys, so
// hostedTagged also reports the bare key of a tagged document. A PostgreSQL
// target keeps tagged documents under the bare key and passes false.
func (c *Client) UnexpectedChunkKeys(ctx context.Context, keys []string, hostedTagged bool) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(keys))
	var manifestDocs, manifestRevs []string
	var manifestIndices []int32
	for _, key := range keys {
		if id, err := uuid.Parse(key); err == nil && key == fmt.Sprintf("%x", id[:]) {
			ids = append(ids, id.String())
			continue
		}
		if hostedTagged {
			if docID, revID, idx, ok := parseManifestVectorID(key); ok {
				manifestDocs = append(manifestDocs, docID)
				manifestRevs = append(manifestRevs, revID)
				manifestIndices = append(manifestIndices, idx)
			}
		}
	}
	eligible := make(map[string]bool, len(keys))
	if len(ids) > 0 {
		rows, err := c.Pool.Query(ctx, `SELECT replace(c.id::text, '-', '') FROM chunks c
 JOIN documents d ON c.document_id=d.id
 LEFT JOIN hosted_vector_revisions r ON r.document_id=d.id
 WHERE c.id=ANY($1::uuid[]) AND r.active_revision_id IS NULL AND NOT ($2::boolean AND d.scoped_vectors) AND `+eligibleDocument, ids, hostedTagged)
		if err != nil {
			return nil, err
		}
		present, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil, err
		}
		for _, key := range present {
			eligible[key] = true
		}
	}
	if len(manifestDocs) > 0 {
		rows, err := c.Pool.Query(ctx, `SELECT d.id::text || '#' || r.active_revision_id::text || '#' || c.chunk_index::text
 FROM unnest($1::uuid[], $2::uuid[], $3::int[]) AS k(doc_id, rev_id, idx)
 JOIN documents d ON d.id = k.doc_id
 JOIN hosted_vector_revisions r ON r.document_id = d.id AND r.active_revision_id = k.rev_id
 JOIN chunks c ON c.document_id = d.id AND c.chunk_index = k.idx
 WHERE `+eligibleDocument, manifestDocs, manifestRevs, manifestIndices)
		if err != nil {
			return nil, err
		}
		present, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil, err
		}
		for _, key := range present {
			eligible[key] = true
		}
	}
	extra := make([]string, 0, len(keys))
	for _, key := range keys {
		if !eligible[key] {
			extra = append(extra, key)
		}
	}
	return extra, nil
}

func parseManifestVectorID(key string) (docID, revID string, index int32, ok bool) {
	parts := strings.Split(key, "#")
	if len(parts) != 3 {
		return "", "", 0, false
	}
	docUUID, err1 := uuid.Parse(parts[0])
	revUUID, err2 := uuid.Parse(parts[1])
	idx, err3 := strconv.ParseInt(parts[2], 10, 32)
	if err1 != nil || err2 != nil || err3 != nil || idx < 0 {
		return "", "", 0, false
	}
	canonical := domain.FormatRevisionVectorID(domain.DocumentID(docUUID.String()), revUUID.String(), int(idx))
	if canonical != key {
		return "", "", 0, false
	}
	return docUUID.String(), revUUID.String(), int32(idx), true
}

// TaggedVectorDocuments counts eligible completed documents stored in the tagged
// vector format and how many lack a determinable published vector attempt. A
// document with an active hosted revision keeps manifest keys and is not counted.
func (c *Client) TaggedVectorDocuments(ctx context.Context) (total, unresolved int, err error) {
	err = c.Pool.QueryRow(ctx, `SELECT count(*)::int, count(*) FILTER (WHERE d.vector_attempt IS NULL OR d.vector_mutation_pending)::int
 FROM documents d LEFT JOIN hosted_vector_revisions r ON r.document_id = d.id
 WHERE `+eligibleDocument+` AND d.scoped_vectors AND r.active_revision_id IS NULL`).Scan(&total, &unresolved)
	if err != nil {
		return 0, 0, fmt.Errorf("postgres: count tagged vector documents: %w", err)
	}
	return total, unresolved, nil
}

// TaggedVectorKey identifies one physical tagged vector key and its parsed parts.
type TaggedVectorKey struct {
	Key        string
	DocumentID string
	Revision   string
	Attempt    string
	ChunkID    string
}

// UnexpectedTaggedKeys returns only tagged keys absent from the eligible tagged
// corpus or whose revision, attempt or chunk no longer matches PostgreSQL. A
// document with an active hosted revision has no current tagged key.
func (c *Client) UnexpectedTaggedKeys(ctx context.Context, keys []TaggedVectorKey) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	rawKeys := make([]string, 0, len(keys))
	docIDs := make([]string, 0, len(keys))
	revisions := make([]string, 0, len(keys))
	attempts := make([]string, 0, len(keys))
	chunkIDs := make([]string, 0, len(keys))
	for _, k := range keys {
		docUUID, docErr := uuid.Parse(k.DocumentID)
		revUUID, revErr := uuid.Parse(k.Revision)
		attUUID, attErr := uuid.Parse(k.Attempt)
		chunkUUID, chunkErr := uuid.Parse(k.ChunkID)
		if docErr != nil || revErr != nil || attErr != nil || chunkErr != nil ||
			k.DocumentID != docUUID.String() || k.Revision != revUUID.String() || k.Attempt != attUUID.String() ||
			k.ChunkID != fmt.Sprintf("%x", chunkUUID[:]) {
			continue
		}
		rawKeys = append(rawKeys, k.Key)
		docIDs = append(docIDs, docUUID.String())
		revisions = append(revisions, revUUID.String())
		attempts = append(attempts, attUUID.String())
		chunkIDs = append(chunkIDs, chunkUUID.String())
	}
	present := make(map[string]bool, len(rawKeys))
	if len(rawKeys) > 0 {
		rows, err := c.Pool.Query(ctx, `SELECT k.key
 FROM unnest($1::text[], $2::uuid[], $3::text[], $4::text[], $5::uuid[]) AS k(key, doc_id, revision, attempt, chunk_id)
 JOIN documents d ON d.id = k.doc_id
 LEFT JOIN hosted_vector_revisions r ON r.document_id = d.id
 JOIN chunks c ON c.id = k.chunk_id AND c.document_id = d.id
 WHERE `+eligibleDocument+`
  AND d.scoped_vectors AND r.active_revision_id IS NULL
  AND COALESCE(d.incarnation, d.id)::text = k.revision
  AND d.vector_attempt IS NOT NULL
  AND d.vector_attempt::text = k.attempt`, rawKeys, docIDs, revisions, attempts, chunkIDs)
		if err != nil {
			return nil, fmt.Errorf("postgres: check tagged vector keys: %w", err)
		}
		matched, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil, fmt.Errorf("postgres: collect tagged vector keys: %w", err)
		}
		for _, key := range matched {
			present[key] = true
		}
	}
	extra := make([]string, 0, len(keys)-len(present))
	for _, k := range keys {
		if !present[k.Key] {
			extra = append(extra, k.Key)
		}
	}
	return extra, nil
}
