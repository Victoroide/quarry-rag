package domain

import (
	"context"
	"time"
)

// IndexVisibility describes the evidence recorded during one ingestion attempt.
type IndexVisibility string

// Index visibility outcomes reported in document summaries.
const (
	VisibilityUnknown       IndexVisibility = "unknown"
	VisibilityDisabled      IndexVisibility = "disabled"
	VisibilityTimeout       IndexVisibility = "timeout"
	VisibilitySampleSeen    IndexVisibility = "sample_seen"
	VisibilityWritesIndexed IndexVisibility = "writes_indexed"
	VisibilityTransactional IndexVisibility = "transactional"
)

// IndexObservation is an ingestion outcome and the time it was observed.
type IndexObservation struct {
	Visibility IndexVisibility
	ObservedAt *time.Time
}

// VisibilityProbe checks progress through one attempt's acknowledged writes.
// An empty result with no error means more progress is needed.
type VisibilityProbe func(context.Context, []float32) (IndexVisibility, error)

// VisibilityWriter can return a probe bound to the original writes, or nil
// if the writes succeeded without enough evidence to verify them.
type VisibilityWriter interface {
	UpsertWithVisibility(context.Context, string, []Chunk) (VisibilityProbe, error)
}
