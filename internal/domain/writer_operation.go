package domain

import (
	"context"
	"errors"
)

// ErrWriterEvidenceUnavailable means the recovery stream could not confirm a record.
var ErrWriterEvidenceUnavailable = errors.New("writer recovery evidence is unavailable")

type externalWriterDispatchKey struct{}

// ExternalWriterAttempt reports whether the attempt definitely did not mutate
// the provider, or whether the provider confirmed settlement.
type ExternalWriterAttempt func(context.Context) (settled bool, err error)

// WithExternalWriterDispatch binds provider attempts to an enrolled operation.
func WithExternalWriterDispatch(ctx context.Context, dispatch func(context.Context, ExternalWriterAttempt) error) context.Context {
	return context.WithValue(ctx, externalWriterDispatchKey{}, dispatch)
}

// HasExternalWriterDispatch reports whether provider attempts need settlement.
func HasExternalWriterDispatch(ctx context.Context) bool {
	return ctx.Value(externalWriterDispatchKey{}) != nil
}

// DispatchExternalWriter records a physical provider attempt when enrolled.
func DispatchExternalWriter(ctx context.Context, attempt ExternalWriterAttempt) error {
	if dispatch, ok := ctx.Value(externalWriterDispatchKey{}).(func(context.Context, ExternalWriterAttempt) error); ok {
		return dispatch(ctx, attempt)
	}
	_, err := attempt(ctx)
	return err
}

// DispatchExternalWriterResult records an attempt and transfers its result
// to the caller. The caller owns any resources in the returned value.
func DispatchExternalWriterResult[T any](ctx context.Context, attempt func(context.Context) (T, bool, error)) (T, error) {
	var result T
	err := DispatchExternalWriter(ctx, func(ctx context.Context) (bool, error) {
		value, settled, err := attempt(ctx)
		result = value
		return settled, err
	})
	return result, err
}

type externalWriterEvidenceKey struct{}

// ExternalWriterEvidence describes one stage of a provider transport attempt.
// Identifiers must come from provider response metadata, never the payload.
type ExternalWriterEvidence struct {
	Provider, Namespace, Outcome, RequestID, RequestSequence string
}

// WithExternalWriterEvidence attaches a reporter to a single physical attempt.
func WithExternalWriterEvidence(ctx context.Context, report func(context.Context, ExternalWriterEvidence) error) context.Context {
	return context.WithValue(ctx, externalWriterEvidenceKey{}, report)
}

// RecordExternalWriterEvidence records internal recovery evidence when enrolled.
func RecordExternalWriterEvidence(ctx context.Context, evidence ExternalWriterEvidence) error {
	if report, ok := ctx.Value(externalWriterEvidenceKey{}).(func(context.Context, ExternalWriterEvidence) error); ok {
		return report(ctx, evidence)
	}
	return nil
}

type writerEvidenceFailureKey struct{}

// WithWriterEvidenceFailureObserver reports a failed recovery record to the caller.
func WithWriterEvidenceFailureObserver(ctx context.Context, failed func()) context.Context {
	return context.WithValue(ctx, writerEvidenceFailureKey{}, failed)
}

// NotifyWriterEvidenceFailure allows callers to avoid logging to a stalled stream.
func NotifyWriterEvidenceFailure(ctx context.Context) {
	if failed, ok := ctx.Value(writerEvidenceFailureKey{}).(func()); ok {
		failed()
	}
}
