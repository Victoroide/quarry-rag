package domain

import (
	"context"
	"errors"
)

// VectorRevisionKey identifies vectors written for a document incarnation.
const VectorRevisionKey = "document_revision"

// VectorAttemptKey identifies the tagged worker attempt that wrote a vector.
const VectorAttemptKey = "vector_attempt"

// VectorIncarnationKey records read provenance on an untagged hosted vector.
const VectorIncarnationKey = "document_incarnation"

type vectorIncarnationKey struct{}

// WithVectorIncarnation binds an untagged vector write to its claimed document.
func WithVectorIncarnation(ctx context.Context, incarnation string) context.Context {
	return context.WithValue(ctx, vectorIncarnationKey{}, incarnation)
}

// VectorIncarnation returns the document identity bound to an untagged write.
func VectorIncarnation(ctx context.Context) string {
	incarnation, _ := ctx.Value(vectorIncarnationKey{}).(string)
	return incarnation
}

// RevisionVectorDeleter removes vectors for one document incarnation.
type RevisionVectorDeleter interface {
	DeleteByRevision(ctx context.Context, documentID, revision string) error
}

type trackedVectorMutation struct{}

// TrackVectorMutation lets the caller checkpoint provider mutation outcomes.
func TrackVectorMutation(ctx context.Context) context.Context {
	return context.WithValue(ctx, trackedVectorMutation{}, true)
}

// IsTrackedVectorMutation reports whether a durable caller owns mutation retries.
func IsTrackedVectorMutation(ctx context.Context) bool {
	return ctx.Value(trackedVectorMutation{}) != nil
}

// AttemptVectorDeleter removes only the vectors written by one worker claim.
type AttemptVectorDeleter interface {
	DeleteByAttempt(ctx context.Context, documentID, revision, attempt string) error
}

// VectorDispatchStore checkpoints individual provider requests.
type VectorDispatchStore interface {
	ManagesVectorDispatch() bool
}

type vectorDispatchKey struct{}

// WithVectorDispatch binds a durable claim to each physical mutation request.
func WithVectorDispatch(ctx context.Context, dispatch func(context.Context, bool, func(context.Context) error) error) context.Context {
	return context.WithValue(ctx, vectorDispatchKey{}, dispatch)
}

// DispatchVectorMutation runs a physical mutation under its bound claim, if any.
func DispatchVectorMutation(ctx context.Context, deletes bool, call func(context.Context) error) error {
	if dispatch, ok := ctx.Value(vectorDispatchKey{}).(func(context.Context, bool, func(context.Context) error) error); ok {
		return dispatch(ctx, deletes, call)
	}
	return call(ctx)
}

type vectorRejectedError struct{ error }

func (e vectorRejectedError) Unwrap() error { return e.error }

// RejectedVectorMutation marks a definite rejection before any write.
func RejectedVectorMutation(err error) error { return vectorRejectedError{err} }

// VectorMutationUncertain reports whether a failed dispatch could have executed.
func VectorMutationUncertain(err error) bool {
	var rejected vectorRejectedError
	return err != nil && !errors.As(err, &rejected)
}

// ErrLiveVectorMutation indicates legacy removal could not retire the observed row.
var ErrLiveVectorMutation = errors.New("document changed or has a pending vector mutation")
