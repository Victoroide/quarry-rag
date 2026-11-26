package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Visibility uses document records to determine which evidence is available.
type Visibility struct {
	Pool *pgxpool.Pool

	Collection string
	DocType    string
	// Subject, when set, requires every document to be readable by this principal
	// at its current incarnation. Service mode leaves it nil and reads no grant
	// relation.
	Subject *domain.Principal
	// DocumentSetID, with Subject, also requires every document to belong to
	// that live set at its current incarnation.
	DocumentSetID string
}

// readableJoin restricts the documents relation aliased d to those the principal
// may read now. The relation itself requires an active principal, a ready
// ownership row for the live incarnation and a completed document. A set also
// requires the document to belong to that live set at the same incarnation.
func readableJoin(subject *domain.Principal, set string, args []any) (join string, next []any) {
	if subject == nil {
		return "", args
	}
	if set == "" {
		args = append(args, subject.TenantID, subject.ID)
		return fmt.Sprintf(` JOIN grant_catalog_readable_documents g ON g.tenant_id=$%d::uuid AND g.principal_id=$%d::uuid
  AND g.document_id=d.id AND g.incarnation=COALESCE(d.incarnation,d.id)`, len(args)-1, len(args)), args
	}
	args = append(args, subject.TenantID, subject.ID, set)
	return fmt.Sprintf(` JOIN grant_catalog_document_set_readable g ON g.tenant_id=$%d::uuid AND g.principal_id=$%d::uuid
  AND g.set_id=$%d::uuid AND g.document_id=d.id AND g.incarnation=COALESCE(d.incarnation,d.id)`, len(args)-2, len(args)-1, len(args)), args
}

// ObserveUntaggedIncarnations reads a consistent view of the SQL identities
// for historical untagged vectors, returning the connection before vector I/O.
func (v Visibility) ObserveUntaggedIncarnations(ctx context.Context, allowedIDs []string) (map[string]string, error) {
	incarnations := make(map[string]string)
	if allowedIDs != nil {
		allowedIDs = canonicalDocumentIDs(allowedIDs)
		if len(allowedIDs) == 0 {
			return incarnations, nil
		}
	}
	join, args := readableJoin(v.Subject, v.DocumentSetID, []any{v.Collection, v.DocType, allowedIDs})
	rows, err := v.Pool.Query(ctx, `
SELECT d.id::text, COALESCE(d.incarnation,d.id)::text
FROM documents d`+join+`
WHERE NOT d.scoped_vectors AND NOT d.vector_incarnation_required
  AND ($1='' OR d.collection=$1) AND ($2='' OR d.doc_type=$2)
  AND ($3::uuid[] IS NULL OR d.id=ANY($3::uuid[]))`, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: observe untagged incarnations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, incarnation string
		if err := rows.Scan(&id, &incarnation); err != nil {
			return nil, fmt.Errorf("postgres: scan untagged incarnation: %w", err)
		}
		incarnations[id] = incarnation
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: observe untagged incarnations: %w", err)
	}
	return incarnations, nil
}

var (
	_ domain.DocumentVisibility = Visibility{}
	_ domain.RevisionVisibility = Visibility{}
	_ domain.EvidenceVisibility = Visibility{}
)

// VisibleEvidence checks that vector hits match current completed chunks and
// reads the current document category.
// Chunk IDs are deterministic and can recur when a document ID is reused.
// Document and chunk IDs are matched as UUIDs but returned exactly as the
// vector store spelled them, because metadata filters compare keys byte for byte.
func (v Visibility) VisibleEvidence(ctx context.Context, candidates []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	visible, _, err := v.VisibleEvidenceWithPreviews(ctx, candidates, nil)
	return visible, err
}

// VisibleEvidenceWithPreviews checks current evidence and reads previews only
// for visible probe documents using the same statement snapshot.
func (v Visibility) VisibleEvidenceWithPreviews(ctx context.Context, candidates []domain.ScoredChunk, probeIDs []string) ([]domain.ScoredChunk, map[string]string, error) {
	return v.visibleEvidence(ctx, v.Pool, candidates, probeIDs)
}

type evidenceQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (v Visibility) visibleEvidence(ctx context.Context, source evidenceQueryer, candidates []domain.ScoredChunk, probeIDs []string) ([]domain.ScoredChunk, map[string]string, error) {
	probeIDs = canonicalDocumentIDs(probeIDs)
	chunkIDs := make([]string, 0, len(candidates))
	docIDs := make([]string, 0, len(candidates))
	indices := make([]int64, 0, len(candidates))
	digests := make([]string, 0, len(candidates))
	revisions := make([]string, 0, len(candidates))
	observed := make([]string, 0, len(candidates))
	attempts := make([]string, 0, len(candidates))
	vectorIncarnations := make([]string, 0, len(candidates))
	revIDs := make([]*string, 0, len(candidates))
	input := make([]domain.ScoredChunk, 0, len(candidates))
	for _, candidate := range candidates {
		ch := candidate.Chunk
		chunkID, chunkErr := uuid.Parse(string(ch.ID))
		docID, docErr := uuid.Parse(string(ch.DocumentID))
		if chunkErr != nil || docErr != nil {
			continue
		}
		var revision string
		if value, tagged := ch.Metadata[domain.VectorRevisionKey]; tagged {
			var valid bool
			revision, valid = value.(string)
			if !valid || revision == "" {
				continue
			}
		}
		var attempt string
		if value, hasAttempt := ch.Metadata[domain.VectorAttemptKey]; hasAttempt {
			var valid bool
			attempt, valid = value.(string)
			if revision == "" || !valid || attempt == "" {
				continue
			}
		}
		var revPtr *string
		if ch.RevisionID != "" {
			revID, revErr := uuid.Parse(ch.RevisionID)
			if revErr != nil {
				continue
			}
			canonicalRev := revID.String()
			revPtr = &canonicalRev
		}
		digest := sha256.Sum256([]byte(ch.Text))
		chunkIDs, docIDs = append(chunkIDs, chunkID.String()), append(docIDs, docID.String())
		indices, digests = append(indices, int64(ch.Index)), append(digests, hex.EncodeToString(digest[:]))
		revisions = append(revisions, revision)
		observed = append(observed, candidate.ObservedIncarnation)
		attempts = append(attempts, attempt)
		vectorIncarnations = append(vectorIncarnations, ch.VectorIncarnation)
		revIDs = append(revIDs, revPtr)
		input = append(input, candidate)
	}
	var previews map[string]string
	var previewIDs []string
	if len(probeIDs) != 0 {
		previews = make(map[string]string, len(probeIDs))
		requested := make(map[string]struct{}, len(probeIDs))
		for _, id := range probeIDs {
			if parsed, err := uuid.Parse(id); err == nil {
				requested[parsed.String()] = struct{}{}
			}
		}
		for _, id := range docIDs {
			if _, ok := requested[id]; ok {
				previewIDs = append(previewIDs, id)
				delete(requested, id)
			}
		}
	}
	if len(input) == 0 {
		return input, previews, nil
	}

	join, args := readableJoin(v.Subject, v.DocumentSetID, []any{chunkIDs, docIDs, indices, digests, revisions, observed, attempts, vectorIncarnations, v.Collection, v.DocType, previewIDs, revIDs})
	rows, err := source.Query(ctx, `
SELECT hit.ord, d.doc_type, COALESCE(d.incarnation,d.id)::text,
       CASE WHEN d.id=ANY($11::uuid[]) THEN d.content_preview END
FROM unnest($1::uuid[], $2::uuid[], $3::bigint[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $12::uuid[])
	WITH ORDINALITY AS hit(chunk_id, doc_id, idx, hash, revision, incarnation, attempt, vector_incarnation, rev_id, ord)
JOIN chunks c ON c.id=hit.chunk_id AND c.document_id=hit.doc_id AND c.chunk_index=hit.idx
JOIN documents d ON d.id=c.document_id AND d.status='completed'`+join+`
WHERE ($9='' OR d.collection=$9) AND ($10='' OR d.doc_type=$10)
  AND (hit.incarnation='' OR hit.incarnation=COALESCE(d.incarnation,d.id)::text)
  AND ((hit.vector_incarnation='' AND NOT d.vector_incarnation_required)
       OR hit.vector_incarnation=COALESCE(d.incarnation,d.id)::text)
  AND (CASE WHEN d.scoped_vectors THEN COALESCE(d.incarnation,d.id)::text ELSE '' END)=hit.revision
  AND COALESCE(d.vector_attempt::text,'')=hit.attempt
  AND hosted_vector_revision_visible(d.id, hit.rev_id)
  AND encode(digest(convert_to(c.text, 'UTF8'), 'sha256'), 'hex')=hit.hash
ORDER BY hit.ord`, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: visible evidence: %w", err)
	}
	defer rows.Close()
	visible := make([]domain.ScoredChunk, 0, len(input))
	for rows.Next() {
		var ord int
		var docType, incarnation string
		var preview *string
		if err := rows.Scan(&ord, &docType, &incarnation, &preview); err != nil {
			return nil, nil, fmt.Errorf("postgres: scan visible evidence: %w", err)
		}
		if preview != nil {
			previews[docIDs[ord-1]] = *preview
		}
		candidate := input[ord-1]
		candidate.ObservedIncarnation = incarnation
		if candidate.Chunk.Metadata["doc_type"] != docType {
			metadata := make(map[string]any, len(candidate.Chunk.Metadata)+1)
			maps.Copy(metadata, candidate.Chunk.Metadata)
			metadata["doc_type"] = docType
			candidate.Chunk.Metadata = metadata
		}
		visible = append(visible, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("postgres: visible evidence: %w", err)
	}
	return visible, previews, nil
}

// VerifyEvaluationEvidence uses the same snapshot for fixture receipts and
// chunk bodies so a concurrent delete cannot mix document incarnations.
func (v Visibility) VerifyEvaluationEvidence(ctx context.Context, receipt string, allowed []string, selected []domain.ScoredChunk) (bool, error) {
	tx, err := v.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var owned int
	err = tx.QueryRow(ctx, `
SELECT count(*)
FROM evaluation_fixtures f
JOIN documents d ON d.id=f.active_document_id AND d.incarnation=f.incarnation
WHERE d.id=ANY($1::uuid[]) AND f.receipt=$2 AND d.collection=$3
  AND f.vector_identity IS NOT NULL`, allowed, receipt, v.Collection).Scan(&owned)
	if err != nil || owned != len(allowed) {
		return false, err
	}
	visible, _, err := v.visibleEvidence(ctx, tx, selected, nil)
	return err == nil && len(visible) == len(selected), err
}

// VisibleDocumentIDs returns the visible documents among the supplied IDs.
func (v Visibility) VisibleDocumentIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	visible := make(map[string]struct{})
	queryIDs := canonicalDocumentIDs(ids)
	if len(queryIDs) == 0 {
		return visible, nil
	}

	join, args := readableJoin(v.Subject, v.DocumentSetID, []any{queryIDs})
	query := `SELECT d.id::text FROM documents d` + join + ` WHERE d.id = ANY($1::uuid[]) AND d.status = 'completed'`
	if v.Collection != "" {
		args = append(args, v.Collection)
		query += fmt.Sprintf(` AND d.collection = $%d`, len(args))
	}
	rows, err := v.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: visible documents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("postgres: scan visible document: %w", err)
		}
		visible[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: visible documents: %w", err)
	}
	return visible, nil
}

// VisibleRevisionChunks filters vector hits to completed documents whose active
// hosted revision matches each candidate's revision identity.
func (v Visibility) VisibleRevisionChunks(ctx context.Context, candidates []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	if len(candidates) == 0 {
		return candidates, nil
	}
	docIDs := make([]string, 0, len(candidates))
	revIDs := make([]*string, 0, len(candidates))
	input := make([]domain.ScoredChunk, 0, len(candidates))
	for _, candidate := range candidates {
		docID, docErr := uuid.Parse(string(candidate.Chunk.DocumentID))
		if docErr != nil {
			continue
		}
		var revPtr *string
		if candidate.Chunk.RevisionID != "" {
			revID, revErr := uuid.Parse(candidate.Chunk.RevisionID)
			if revErr != nil {
				continue
			}
			canonicalRev := revID.String()
			revPtr = &canonicalRev
		}
		docIDs = append(docIDs, docID.String())
		revIDs = append(revIDs, revPtr)
		input = append(input, candidate)
	}
	if len(input) == 0 {
		return []domain.ScoredChunk{}, nil
	}
	join, args := readableJoin(v.Subject, v.DocumentSetID, []any{docIDs, revIDs, v.Collection})
	rows, err := v.Pool.Query(ctx, `
SELECT hit.ord
FROM unnest($1::uuid[], $2::uuid[]) WITH ORDINALITY AS hit(doc_id, rev_id, ord)
JOIN documents d ON d.id=hit.doc_id AND d.status='completed'`+join+`
WHERE ($3='' OR d.collection=$3)
  AND hosted_vector_revision_visible(d.id, hit.rev_id)
ORDER BY hit.ord`, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: visible revision chunks: %w", err)
	}
	defer rows.Close()
	visible := make([]domain.ScoredChunk, 0, len(input))
	for rows.Next() {
		var ord int
		if err := rows.Scan(&ord); err != nil {
			return nil, fmt.Errorf("postgres: scan visible revision chunk: %w", err)
		}
		visible = append(visible, input[ord-1])
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: visible revision chunks: %w", err)
	}
	return visible, nil
}

func canonicalDocumentIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	queryIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		parsed, err := uuid.Parse(id)
		if err != nil {
			continue
		}
		canonical := parsed.String()
		if _, exists := seen[canonical]; exists {
			continue
		}
		seen[canonical] = struct{}{}
		queryIDs = append(queryIDs, canonical)
	}
	return queryIDs
}
