package pipeline

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

const (
	maxSourceLineBoundaryLookupTime = 100 * time.Millisecond
	minSourceLineBoundaryStageTime  = 10 * time.Millisecond
)

type boundaryChunkKey struct {
	chunkDocument
	index int
}

func sourceLineBoundaryKey(ch domain.Chunk) boundaryChunkKey {
	return boundaryChunkKey{chunkDocument{chunkID: ch.ID, documentID: ch.DocumentID}, ch.Index}
}

func hydrateSourceLineBoundaries(ctx context.Context, qc *QueryContext, selected []domain.ScoredChunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Only the current scoped lookup may supply provenance on these candidates.
	owned := false
	for i := range qc.RetrievedChunks {
		if qc.RetrievedChunks[i].Chunk.SourceLineBoundaries != nil {
			if !owned {
				qc.RetrievedChunks = slices.Clone(qc.RetrievedChunks)
				owned = true
			}
			qc.RetrievedChunks[i].Chunk.SourceLineBoundaries = nil
		}
	}
	reader, ok := qc.EvidenceVisibility.(domain.SourceLineBoundaryVisibility)
	if !ok || boundaryCredentialExpired(qc) {
		return ctx.Err()
	}

	byKey := make(map[boundaryChunkKey]int, len(qc.RetrievedChunks))
	for i, hit := range qc.RetrievedChunks {
		key := sourceLineBoundaryKey(hit.Chunk)
		if _, exists := byKey[key]; !exists {
			byKey[key] = i
		}
	}
	n := min(maxSynthesisExcerpts, len(selected))
	lookups := make([]domain.ScoredChunk, 0, n)
	pending := make(map[boundaryChunkKey]int, n)
	for _, excerpt := range selected[:n] {
		key := sourceLineBoundaryKey(excerpt.Chunk)
		i, ok := byKey[key]
		if _, duplicate := pending[key]; duplicate || !ok {
			continue
		}
		hit := qc.RetrievedChunks[i]
		if hasSourceLineBoundaryIdentity(qc, hit) || hit.ObservedSnapshot != "" {
			pending[key] = i
			lookups = append(lookups, hit)
		}
	}
	lookups = filterEvidenceScope(qc.vectorFilter(), lookups)
	if len(lookups) == 0 {
		return ctx.Err()
	}
	eligible := make(map[boundaryChunkKey]int, len(lookups))
	for _, hit := range lookups {
		key := sourceLineBoundaryKey(hit.Chunk)
		eligible[key] = pending[key]
	}

	// Scope membership filtering needs the original observation. For a legacy
	// hit the SQL check must instead prove its lifetime against the presearch
	// snapshot, even if an earlier visibility check already annotated the hit.
	for i, lookup := range lookups {
		if !hasSourceLineBoundaryIdentity(qc, lookup) {
			lookups[i].ObservedIncarnation = ""
		}
	}
	// Reserve most of the remaining stage budget for emitting evidence or
	// calling the model. Bound pool acquisition as well as the SQL statement.
	budget := maxSourceLineBoundaryLookupTime
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < minSourceLineBoundaryStageTime {
			return ctx.Err()
		}
		budget = min(budget, remaining/4)
	}
	lookupCtx, cancel := context.WithTimeout(ctx, budget)
	current, err := reader.VisibleEvidenceWithLineBoundaries(lookupCtx, lookups)
	lookupErr := lookupCtx.Err()
	cancel()
	if err == nil && lookupErr == nil {
		if ctx.Err() == nil && !boundaryCredentialExpired(qc) {
			qc.RetrievedChunks = attachRetrievedSourceLineBoundaries(qc.RetrievedChunks, current, eligible, owned)
		}
	} else if ctx.Err() == nil {
		slog.DebugContext(ctx, "source line boundaries unavailable")
	}
	return ctx.Err()
}

func attachRetrievedSourceLineBoundaries(retrieved, current []domain.ScoredChunk, eligible map[boundaryChunkKey]int, owned bool) []domain.ScoredChunk {
	for _, row := range current {
		key := sourceLineBoundaryKey(row.Chunk)
		i, ok := eligible[key]
		if !ok || row.Chunk.SourceLineBoundaries == nil {
			continue
		}
		original := retrieved[i]
		if row.Chunk.Text != original.Chunk.Text || row.Chunk.RevisionID != original.Chunk.RevisionID ||
			(original.ObservedIncarnation != "" && row.ObservedIncarnation != original.ObservedIncarnation) ||
			!domain.ValidateSourceLineBoundaries(row.Chunk.Text, row.Chunk.SourceLineBoundaries) {
			continue
		}
		if !owned {
			retrieved = slices.Clone(retrieved)
			owned = true
		}
		retrieved[i].Chunk.SourceLineBoundaries = slices.Clone(row.Chunk.SourceLineBoundaries)
	}
	return retrieved
}

func hasSourceLineBoundaryIdentity(qc *QueryContext, hit domain.ScoredChunk) bool {
	attempt, _ := hit.Chunk.Metadata[domain.VectorAttemptKey].(string)
	revision, _ := hit.Chunk.Metadata[domain.VectorRevisionKey].(string)
	return hit.Chunk.VectorIncarnation != "" || hit.Chunk.RevisionID != "" ||
		(revision != "" && attempt != "") || (qc.vectorObservesIncarnation && hit.ObservedIncarnation != "")
}

func boundaryCredentialExpired(qc *QueryContext) bool {
	return qc.Authorization != nil && qc.Authorization.Expired()
}
