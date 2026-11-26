package tracing

import "context"

type captureKey struct{}
type redactIdentifiersKey struct{}

// WithContentCapture selects whether this operation may record message content.
func WithContentCapture(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, captureKey{}, enabled)
}

// ContentCapture reports whether message content capture is enabled for this operation.
func ContentCapture(ctx context.Context) bool {
	enabled, _ := ctx.Value(captureKey{}).(bool)
	return enabled
}

// WithRedactIdentifiers selects whether telemetry should omit document and object identifiers and bound error details.
func WithRedactIdentifiers(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, redactIdentifiersKey{}, enabled)
}

// RedactIdentifiers reports whether telemetry must omit identifiers and bound error details.
func RedactIdentifiers(ctx context.Context) bool {
	enabled, _ := ctx.Value(redactIdentifiersKey{}).(bool)
	return enabled
}
