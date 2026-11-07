package tracing

import "context"

type captureKey struct{}

// WithContentCapture selects whether this operation may record message content.
func WithContentCapture(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, captureKey{}, enabled)
}

// ContentCapture reports whether message content capture is enabled for this operation.
func ContentCapture(ctx context.Context) bool {
	enabled, _ := ctx.Value(captureKey{}).(bool)
	return enabled
}
