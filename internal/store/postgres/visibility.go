package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Visibility uses document records to determine which evidence is available.
type Visibility struct {
	Pool *pgxpool.Pool

	Collection string
	DocType    string
}

// VisibleEvidence checks that vector hits match current completed chunks and
// reads the current document category.
// Chunk IDs are deterministic and can recur when a document ID is reused.
func (v Visibility) VisibleEvidence(ctx context.Context, candidates []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	visible, _, err := v.VisibleEvidenceWithPreviews(ctx, candidates, nil)
	return visible, err
}

// VisibleEvidenceWithPreviews checks current evidence and reads previews only
// for visible probe documents using the same statement snapshot.
func (v Visibility) VisibleEvidenceWithPreviews(ctx context.Context, candidates []domain.ScoredChunk, probeIDs []string) ([]domain.ScoredChunk, map[string]string, error) {
	chunkIDs := make([]string, 0, len(candidates))
	docIDs := make([]string, 0, len(candidates))
	indices := make([]int64, 0, len(candidates))
	digests := make([]string, 0, len(candidates))
	input := make([]domain.ScoredChunk, 0, len(candidates))
	for _, candidate := range candidates {
		ch := candidate.Chunk
		chunkID, chunkErr := uuid.Parse(string(ch.ID))
		docID, docErr := uuid.Parse(string(ch.DocumentID))
		if chunkErr != nil || docErr != nil {
			continue
		}
		digest := sha256.Sum256([]byte(ch.Text))
		chunkIDs, docIDs = append(chunkIDs, chunkID.String()), append(docIDs, docID.String())
		indices, digests = append(indices, int64(ch.Index)), append(digests, hex.EncodeToString(digest[:]))
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

	rows, err := v.Pool.Query(ctx, `
SELECT hit.ord, d.doc_type, CASE WHEN d.id=ANY($7::uuid[]) THEN d.content_preview END
FROM unnest($1::uuid[], $2::uuid[], $3::bigint[], $4::text[]) WITH ORDINALITY AS hit(chunk_id, doc_id, idx, hash, ord)
JOIN chunks c ON c.id=hit.chunk_id AND c.document_id=hit.doc_id AND c.chunk_index=hit.idx
JOIN documents d ON d.id=c.document_id AND d.status='completed'
WHERE ($5='' OR d.collection=$5) AND ($6='' OR d.doc_type=$6)
  AND encode(digest(convert_to(c.text, 'UTF8'), 'sha256'), 'hex')=hit.hash
ORDER BY hit.ord`, chunkIDs, docIDs, indices, digests, v.Collection, v.DocType, previewIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: visible evidence: %w", err)
	}
	defer rows.Close()
	visible := make([]domain.ScoredChunk, 0, len(input))
	for rows.Next() {
		var ord int
		var docType string
		var preview *string
		if err := rows.Scan(&ord, &docType, &preview); err != nil {
			return nil, nil, fmt.Errorf("postgres: scan visible evidence: %w", err)
		}
		if preview != nil {
			previews[docIDs[ord-1]] = *preview
		}
		candidate := input[ord-1]
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

// VisibleDocumentIDs returns the visible documents among the supplied IDs.
func (v Visibility) VisibleDocumentIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	visible := make(map[string]struct{})
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
	if len(queryIDs) == 0 {
		return visible, nil
	}

	query := `SELECT d.id::text FROM documents d WHERE d.id = ANY($1::uuid[]) AND d.status = 'completed'`
	args := []any{queryIDs}
	if v.Collection != "" {
		query += ` AND d.collection = $2`
		args = append(args, v.Collection)
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
