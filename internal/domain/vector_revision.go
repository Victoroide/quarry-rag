package domain

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/google/uuid"
)

// VectorRevisionKey identifies vectors written for a document incarnation.
const VectorRevisionKey = "document_revision"

// VectorTenantKey records the owning tenant on vectors of an owned document.
const VectorTenantKey = "tenant_id"

// VectorAttemptKey identifies the tagged worker attempt that wrote a vector.
const VectorAttemptKey = "vector_attempt"

// VectorIncarnationKey records read provenance on an untagged hosted vector.
const VectorIncarnationKey = "document_incarnation"

// ValidManifestIncarnation reports whether a manifest stamp is a nonzero,
// canonical UUID, as used by the claim and the authorization scope.
func ValidManifestIncarnation(incarnation string) bool {
	id, err := uuid.Parse(incarnation)
	return err == nil && id != uuid.Nil && id.String() == incarnation
}

// ManifestIdentity reports whether a chunk carries only a hosted revision as
// its publication identity. Its active revision still needs SQL verification.
func ManifestIdentity(chunk Chunk) bool {
	return chunk.RevisionID != "" && emptyVectorTag(chunk.Metadata, VectorRevisionKey) &&
		emptyVectorTag(chunk.Metadata, VectorAttemptKey)
}

func emptyVectorTag(metadata map[string]any, key string) bool {
	value, exists := metadata[key]
	return !exists || value == ""
}

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

// VectorOutcome records whether any attempt of an observed provider mutation
// may have taken effect, including one that a later retry replaced with success.
type VectorOutcome struct{ uncertain atomic.Bool }

// Uncertain reports whether some attempt could have executed without an acknowledgment.
func (o *VectorOutcome) Uncertain() bool { return o.uncertain.Load() }

type vectorOutcomeKey struct{}

// ObserveVectorOutcome lets an adapter that retries internally report an
// attempt whose outcome stayed unknown.
func ObserveVectorOutcome(ctx context.Context) (context.Context, *VectorOutcome) {
	outcome := new(VectorOutcome)
	return context.WithValue(ctx, vectorOutcomeKey{}, outcome), outcome
}

// ObservingVectorOutcome reports whether the caller wants attempt outcomes.
func ObservingVectorOutcome(ctx context.Context) bool {
	_, ok := ctx.Value(vectorOutcomeKey{}).(*VectorOutcome)
	return ok
}

// NoteVectorAttempt records the failure of one attempt for the observer, if any.
func NoteVectorAttempt(ctx context.Context, err error) {
	if outcome, ok := ctx.Value(vectorOutcomeKey{}).(*VectorOutcome); ok && VectorMutationUncertain(err) {
		outcome.uncertain.Store(true)
	}
}
