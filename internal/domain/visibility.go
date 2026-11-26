package domain

import "context"

// DocumentVisibility identifies documents whose chunks may be used as evidence.
type DocumentVisibility interface {
	VisibleDocumentIDs(ctx context.Context, ids []string) (map[string]struct{}, error)
}

// RevisionVisibility filters scored chunks against completed documents and
// their active hosted vector revisions when present.
type RevisionVisibility interface {
	VisibleRevisionChunks(ctx context.Context, candidates []ScoredChunk) ([]ScoredChunk, error)
}

// EvidenceVisibility identifies current chunks within the requested scope.
// It returns a subset of the candidates in their original order with the
// current document category.
type EvidenceVisibility interface {
	VisibleEvidence(ctx context.Context, candidates []ScoredChunk) ([]ScoredChunk, error)
	// VisibleEvidenceWithPreviews also reads previews for the supplied probe IDs
	// from the same snapshot as the evidence check.
	VisibleEvidenceWithPreviews(ctx context.Context, candidates []ScoredChunk, probeIDs []string) ([]ScoredChunk, map[string]string, error)
}

// EvaluationEvidenceVisibility checks the ownership of the entire fixture
// scope and the currency of selected evidence in one storage snapshot.
type EvaluationEvidenceVisibility interface {
	VerifyEvaluationEvidence(ctx context.Context, receipt string, allowed []string, selected []ScoredChunk) (bool, error)
}

// EvidenceSnapshotObserver records the database snapshot before a search whose
// historical untagged hits carry no document lifetime.
type EvidenceSnapshotObserver interface {
	ObserveEvidenceSnapshot(ctx context.Context) (string, error)
}

// VectorIncarnationReader identifies stores that attach the document
// incarnation to hits from the same snapshot as their scores.
type VectorIncarnationReader interface {
	ObservesVectorIncarnation() bool
}
