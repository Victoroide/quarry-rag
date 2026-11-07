package tracing

import (
	"context"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type embeddingProvider struct {
	inner           domain.EmbeddingProvider
	provider, model string
}

// WrapEmbedder traces embeddings without recording source text by default.
func WrapEmbedder(inner domain.EmbeddingProvider, provider, model string) domain.EmbeddingProvider {
	return &embeddingProvider{inner, provider, model}
}

func (e *embeddingProvider) Embed(ctx context.Context, text string) ([]float32, error) {
	ctx, span := start(ctx, "embeddings "+e.model, "embedding", trace.SpanKindClient, modelAttributes(e.provider, e.model, "embeddings")...)
	defer span.End()
	if span.IsRecording() && ContentCapture(ctx) {
		span.SetAttributes(attribute.String("input.value", text))
	}
	vector, err := e.inner.Embed(ctx, text)
	RecordError(span, err)
	return vector, err
}
