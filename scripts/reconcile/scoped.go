package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pineconestore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/protobuf/types/known/structpb"
)

const hostedFetchBatch = 100

// scopedSource is the authoritative database side of a scoped backfill.
type scopedSource interface {
	BackfillCandidates(context.Context, string, int) ([]pgstore.BackfillDocument, error)
	PublishVerifiedDocument(context.Context, pgstore.BackfillDocument) (bool, error)
}

// scopedVectors checks the vectors of one document against its current chunks.
// A document is verified only when every current chunk has exactly the stored
// vector. Anything else stays quarantined.
type scopedVectors interface {
	Verify(context.Context, pgstore.BackfillDocument) (bool, error)
}

// scopedReport counts outcomes only. It names no tenant, owner or content.
type scopedReport struct {
	Examined    int    `json:"examined"`
	Verified    int    `json:"verified"`
	Quarantined int    `json:"quarantined"`
	Changed     int    `json:"changed"`
	ResumeAfter string `json:"resume_after,omitempty"`
}

type scopedIndex interface {
	FetchVectors(context.Context, []string) (*pinecone.FetchVectorsResponse, error)
	UpdateVector(context.Context, *pinecone.UpdateVectorRequest) error
}

// pgvectorVectors verifies documents whose vectors live in PostgreSQL. Its rows
// carry no tenant labels, so verification only proves the stored set is current.
type pgvectorVectors struct{ pg *pgstore.Client }

func (v pgvectorVectors) Verify(ctx context.Context, doc pgstore.BackfillDocument) (bool, error) {
	return v.pg.VectorRowsMatch(ctx, doc)
}

// hostedVectors labels the current chunks of a document in the hosted index.
// Vectors of replaced or stale chunks are never selected, so they keep no
// tenant label and no search can return them. A document written in the scoped
// format already carries its revision, so only the tenant is added. An untagged
// historical vector receives the tenant and its document incarnation.
type hostedVectors struct{ idx scopedIndex }

// physicalID spells a chunk's identifier as the hosted index stores it.
func physicalID(doc pgstore.BackfillDocument, chunkID string) (string, error) {
	if !doc.Scoped {
		return chunkID, nil
	}
	return pineconestore.TaggedVectorID(doc.DocumentID, doc.Incarnation, doc.Attempt, chunkID)
}

func physicalIDs(doc pgstore.BackfillDocument) ([]string, bool) {
	ids := make([]string, 0, len(doc.ChunkIDs))
	for _, chunkID := range doc.ChunkIDs {
		id, err := physicalID(doc, chunkID)
		if err != nil {
			return nil, false
		}
		ids = append(ids, id)
	}
	return ids, true
}

func (h hostedVectors) Verify(ctx context.Context, doc pgstore.BackfillDocument) (bool, error) {
	if len(doc.ChunkIDs) == 0 ||
		len(doc.ChunkTextDigests) != len(doc.ChunkIDs) ||
		len(doc.ChunkTokenCounts) != len(doc.ChunkIDs) ||
		(doc.Scoped && doc.Attempt == "") {
		return false, nil
	}
	ids, valid := physicalIDs(doc)
	if !valid {
		return false, nil
	}
	intact, err := h.matches(ctx, doc, ids, false)
	if err != nil || !intact {
		return false, err
	}
	fields := map[string]*structpb.Value{domain.VectorTenantKey: structpb.NewStringValue(doc.TenantID)}
	if !doc.Scoped {
		fields[domain.VectorIncarnationKey] = structpb.NewStringValue(doc.Incarnation)
	}
	labels := &structpb.Struct{Fields: fields}
	for _, targetID := range ids {
		if err = h.idx.UpdateVector(ctx, &pinecone.UpdateVectorRequest{Id: targetID, Metadata: labels}); err != nil {
			return false, fmt.Errorf("label vector: %w", err)
		}
	}
	return h.matches(ctx, doc, ids, true)
}

// matches fetches every current chunk and checks its identity, content and format.
// With labeled set it also requires the labels written by this backfill.
func (h hostedVectors) matches(ctx context.Context, doc pgstore.BackfillDocument, allIDs []string, labeled bool) (bool, error) {
	for start := 0; start < len(allIDs); start += hostedFetchBatch {
		batch := allIDs[start:min(start+hostedFetchBatch, len(allIDs))]
		res, err := h.idx.FetchVectors(ctx, batch)
		if err != nil {
			return false, fmt.Errorf("fetch vectors: %w", err)
		}
		for offset, id := range batch {
			if !h.matchesVector(res.Vectors[id], doc, start+offset, labeled) {
				return false, nil
			}
		}
	}
	return true, nil
}

func (hostedVectors) matchesVector(vector *pinecone.Vector, doc pgstore.BackfillDocument, index int, labeled bool) bool {
	if vector == nil || vector.Metadata == nil ||
		index < 0 || index >= len(doc.ChunkTextDigests) || index >= len(doc.ChunkTokenCounts) {
		return false
	}
	fields := vector.Metadata.GetFields()
	docVal, hasDoc := fields["doc_id"]
	docStr, isDocStr := docVal.GetKind().(*structpb.Value_StringValue)
	idxVal, hasIdx := fields["chunk_index"]
	idxNum, isIdxNum := idxVal.GetKind().(*structpb.Value_NumberValue)
	if !hasDoc || !isDocStr || docStr.StringValue != doc.DocumentID ||
		!hasIdx || !isIdxNum || idxNum.NumberValue != float64(index) {
		return false
	}
	textVal, hasText := fields["text"]
	textStr, isTextStr := textVal.GetKind().(*structpb.Value_StringValue)
	tokenVal, hasToken := fields["token_count"]
	tokenNum, isTokenNum := tokenVal.GetKind().(*structpb.Value_NumberValue)
	if !hasText || !isTextStr || !hasToken || !isTokenNum || tokenNum.NumberValue != float64(doc.ChunkTokenCounts[index]) {
		return false
	}
	sum := sha256.Sum256([]byte(textStr.StringValue))
	if hex.EncodeToString(sum[:]) != doc.ChunkTextDigests[index] {
		return false
	}
	if inc, hasInc := fields[domain.VectorIncarnationKey]; hasInc && inc.GetStringValue() != doc.Incarnation {
		return false
	}
	revision, tagged := fields[domain.VectorRevisionKey]
	attempt, hasAttempt := fields[domain.VectorAttemptKey]
	if doc.Scoped {
		if !tagged || revision.GetStringValue() != doc.Incarnation || !hasAttempt || attempt.GetStringValue() != doc.Attempt {
			return false
		}
	} else if tagged || hasAttempt {
		return false
	}
	if !labeled {
		return true
	}
	if fields[domain.VectorTenantKey].GetStringValue() != doc.TenantID {
		return false
	}
	return doc.Scoped || fields[domain.VectorIncarnationKey].GetStringValue() == doc.Incarnation
}

// backfillScoped verifies quarantined historical documents in order and
// publishes each one only if it is unchanged since it was read. It stops at the
// first infrastructure error and reports where to resume. A document whose
// vectors or rows changed stays quarantined and is counted, not retried here.
func backfillScoped(ctx context.Context, source scopedSource, vectors scopedVectors, opts options) (scopedReport, error) {
	var report scopedReport
	if opts.Limit < 1 {
		return report, errors.New("backfill limit must be greater than zero")
	}
	cursor := opts.After
	report.ResumeAfter = cursor
	for report.Examined < opts.Limit {
		batch, err := source.BackfillCandidates(ctx, cursor, min(opts.Limit-report.Examined, pgstore.MaxBackfillBatch))
		if err != nil {
			return report, fmt.Errorf("%w; resume with -after=%q", err, report.ResumeAfter)
		}
		if len(batch) == 0 {
			return report, nil
		}
		for _, doc := range batch {
			if err = ctx.Err(); err != nil {
				return report, fmt.Errorf("backfill stopped: %w; resume with -after=%q", err, report.ResumeAfter)
			}
			if opts.Apply {
				if err = verifyAndPublish(ctx, source, vectors, doc, &report); err != nil {
					return report, fmt.Errorf("%w; resume with -after=%q", err, report.ResumeAfter)
				}
			}
			report.Examined++
			cursor, report.ResumeAfter = doc.DocumentID, doc.DocumentID
		}
	}
	return report, nil
}

func verifyAndPublish(ctx context.Context, source scopedSource, vectors scopedVectors, doc pgstore.BackfillDocument, report *scopedReport) error {
	verified, err := vectors.Verify(ctx, doc)
	if err != nil {
		return err
	}
	if !verified {
		report.Quarantined++
		return nil
	}
	published, err := source.PublishVerifiedDocument(ctx, doc)
	if err != nil {
		return err
	}
	if published {
		report.Verified++
	} else {
		report.Changed++
	}
	return nil
}
