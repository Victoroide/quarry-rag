package domain

import "context"

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
