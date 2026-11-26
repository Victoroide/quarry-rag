package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/protobuf/types/known/structpb"
)

// documentKeyIndex rewrites metadata of the vectors a filter selects.
type documentKeyIndex interface {
	UpdateVectorsByMetadata(context.Context, *pinecone.UpdateVectorsByMetadataRequest) (*pinecone.UpdateVectorsByMetadataResponse, error)
}

const (
	// maxMetadataUpdate is the provider's limit on records changed by one request.
	maxMetadataUpdate    = 100_000
	maxReportedDocuments = 100
)

type documentKeyMatch struct {
	ID      string `json:"document_id"`
	Matched int32  `json:"matched_vectors"`
	Capped  bool   `json:"capped,omitempty"`
}

type documentKeyReport struct {
	Mode           string             `json:"mode"` // "inventory" or "apply"
	Examined       int                `json:"examined_documents"`
	MatchedVectors int64              `json:"matched_vectors"`
	Updated        int                `json:"updated_documents"`
	Documents      []documentKeyMatch `json:"documents"` // the first 100 documents with matches
	ResumeAfter    string             `json:"resume_after,omitempty"`
}

// noncanonicalSpellings lists the uniform-case spellings of id that uuid.Parse
// accepts, other than the canonical one.
func noncanonicalSpellings(id uuid.UUID) []string {
	lower := id.String()
	upper := strings.ToUpper(lower)
	compact := strings.ReplaceAll(lower, "-", "")
	candidates := []string{
		upper,
		"{" + lower + "}", "{" + upper + "}",
		"urn:uuid:" + lower, "urn:uuid:" + upper, "URN:UUID:" + lower, "URN:UUID:" + upper,
		compact, strings.ToUpper(compact),
	}
	seen := map[string]struct{}{lower: {}}
	out := make([]string, 0, len(candidates))
	for _, s := range candidates {
		if _, exists := seen[s]; exists {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// listDocumentIDs returns document UUIDs in ascending order after the cursor,
// in every status.
func listDocumentIDs(ctx context.Context, pg *pgstore.Client, after string, limit int) ([]string, error) {
	if limit < 1 {
		return nil, errors.New("document listing limit must be greater than zero")
	}
	rows, err := pg.Pool.Query(ctx, `SELECT id::text FROM documents
		WHERE ($1::text = '' OR id > NULLIF($1::text, '')::uuid) ORDER BY id LIMIT $2`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list document IDs: %w", err)
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("read document ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list document IDs: %w", err)
	}
	return ids, nil
}

// canonicalizeDocumentKeys sets doc_id to the canonical UUID on vectors that
// spell it differently. Without Apply it only counts them.
func canonicalizeDocumentKeys(ctx context.Context, idx documentKeyIndex, ids []string, opts options) (documentKeyReport, error) {
	mode := "inventory"
	if opts.Apply {
		mode = "apply"
	}
	result := documentKeyReport{
		Mode:        mode,
		Documents:   []documentKeyMatch{},
		ResumeAfter: opts.After,
	}
	if opts.Limit < 1 {
		return result, errors.New("canonical document IDs limit must be greater than zero")
	}
	dryRun := !opts.Apply
	last := opts.After
	for _, rawID := range ids[:min(len(ids), opts.Limit)] {
		parsed, parseErr := uuid.Parse(rawID)
		if parseErr != nil {
			return result, fmt.Errorf("canonicalize document %s: invalid UUID: %w; resume with -after=%q", rawID, parseErr, last)
		}
		canonical := parsed.String()
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("canonical document IDs stopped before document %s: %w; resume with -after=%q", canonical, err, last)
		}
		spellings := noncanonicalSpellings(parsed)
		values := make([]*structpb.Value, len(spellings))
		for i, spelling := range spellings {
			values[i] = structpb.NewStringValue(spelling)
		}
		filter := &structpb.Struct{Fields: map[string]*structpb.Value{
			"doc_id": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
				"$in": structpb.NewListValue(&structpb.ListValue{Values: values}),
			}}),
		}}
		metadata := &structpb.Struct{Fields: map[string]*structpb.Value{
			"doc_id": structpb.NewStringValue(canonical),
		}}
		res, err := idx.UpdateVectorsByMetadata(ctx, &pinecone.UpdateVectorsByMetadataRequest{
			Filter:   filter,
			Metadata: metadata,
			DryRun:   &dryRun,
		})
		if err != nil {
			return result, fmt.Errorf("canonicalize document %s: %w; resume with -after=%q", canonical, err, last)
		}
		if res == nil {
			return result, fmt.Errorf("canonicalize document %s: empty provider response; resume with -after=%q", canonical, last)
		}
		if res.MatchedRecords < 0 {
			return result, fmt.Errorf("canonicalize document %s: negative matched count %d; resume with -after=%q", canonical, res.MatchedRecords, last)
		}
		result.Examined++
		if res.MatchedRecords > 0 {
			result.MatchedVectors += int64(res.MatchedRecords)
			if len(result.Documents) < maxReportedDocuments {
				result.Documents = append(result.Documents, documentKeyMatch{
					ID:      canonical,
					Matched: res.MatchedRecords,
					Capped:  res.MatchedRecords >= maxMetadataUpdate,
				})
			}
			if opts.Apply {
				result.Updated++
			}
		}
		last = canonical
		result.ResumeAfter = canonical
	}
	return result, nil
}
