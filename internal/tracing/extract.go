package tracing

import (
	"context"

	"github.com/Victoroide/quarry-rag/internal/extractor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// WrapExtract traces extraction without recording document bytes or text.
func WrapExtract(inner func(context.Context, extractor.FileType, []byte) (string, error)) func(context.Context, extractor.FileType, []byte) (string, error) {
	return func(ctx context.Context, fileType extractor.FileType, data []byte) (string, error) {
		ctx, span := start(ctx, "extract "+string(fileType), "parser", trace.SpanKindInternal,
			attribute.String("quarry.extract.file_type", string(fileType)), attribute.Int("quarry.extract.bytes", len(data)))
		defer span.End()
		text, err := inner(ctx, fileType, data)
		RecordError(span, err)
		return text, err
	}
}
