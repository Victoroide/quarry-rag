package domain

import (
	"context"
	"errors"
)

// VectorFilter restricts a search by collection, document type and document IDs.
// A nil ID list is unrestricted; an empty non-nil list matches nothing.
type VectorFilter struct {
	DocType     string
	DocumentIDs []string

	Collection string
}

// MatchesNothing reports whether the filter excludes every document.
func (f VectorFilter) MatchesNothing() bool { return f.DocumentIDs != nil && len(f.DocumentIDs) == 0 }

// FilteredVectorStore applies document scope before selecting the top results.
type FilteredVectorStore interface {
	QueryFiltered(ctx context.Context, embedding []float32, topK int, filter VectorFilter) ([]ScoredChunk, error)
}

// ErrVectorQueryBudget indicates an optional search could not fit its request budget.
var ErrVectorQueryBudget = errors.New("vector query request budget exhausted")

// VisibilityRefillStore supports optional hosted searches with a shared request budget.
type VisibilityRefillStore interface {
	FilteredVectorStore
	VisibilityRefillLimit() int
	// QueryCandidates returns the candidates from each partition before global
	// truncation or deduplication so a visibility check can run first.
	QueryCandidates(ctx context.Context, embedding []float32, topK int, filter VectorFilter) ([]ScoredChunk, error)
	// QueryFilteredBudgeted returns the number of provider attempts used,
	// including retries, and the unmerged candidates from all partitions on
	// success. It skips the search if all partitions cannot fit.
	QueryFilteredBudgeted(ctx context.Context, embedding []float32, topK int, filter VectorFilter, budget int) ([]ScoredChunk, int, error)
}

// FilteredVectorStoreWithExactness also reports whether a search used exact ranking.
type FilteredVectorStoreWithExactness interface {
	FilteredVectorStore
	QueryFilteredWithExactness(ctx context.Context, embedding []float32, topK int, filter VectorFilter) ([]ScoredChunk, bool, error)
}

// DocumentPreviewer loads previews for the supplied document IDs only.
type DocumentPreviewer interface {
	DocumentPreviews(ctx context.Context, ids []string) (map[string]string, error)
}

// VectorCandidateStore returns the bounded hits per partition for visibility checks.
type VectorCandidateStore interface {
	QueryCandidates(ctx context.Context, embedding []float32, perPartition int, filter VectorFilter) ([]ScoredChunk, error)
}
