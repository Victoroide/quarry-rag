package tracing

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// LangSmithKind maps spans to LangSmith operation types.
const LangSmithKind = "langsmith.span.kind"

// DocumentAttributes correlates ingestion acceptance and processing by document.
func DocumentAttributes(id string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("quarry.document_id", id),
		attribute.String("langsmith.metadata.document_id", id),
	}
}

func start(ctx context.Context, name, kind string, spanKind trace.SpanKind, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if kind != "" {
		attrs = append(attrs, attribute.String(LangSmithKind, kind))
	}
	return otel.Tracer("github.com/Victoroide/quarry-rag/internal/tracing").Start(ctx, name, trace.WithSpanKind(spanKind), trace.WithAttributes(attrs...))
}

var (
	errOperationFailed   = errors.New("operation failed")
	errOperationCanceled = errors.New("operation canceled")
	errOperationTimedOut = errors.New("operation timed out")
)

// ErrorForTelemetry returns a bounded error for logs and spans when redaction is enabled.
func ErrorForTelemetry(ctx context.Context, err error) error {
	if err == nil || !RedactIdentifiers(ctx) {
		return err
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return errOperationTimedOut
	case errors.Is(err, context.Canceled):
		return errOperationCanceled
	default:
		return errOperationFailed
	}
}

// RecordError records an operation failure without changing its returned error.
func RecordError(ctx context.Context, span trace.Span, err error) {
	if safe := ErrorForTelemetry(ctx, err); safe != nil {
		span.RecordError(safe)
		span.SetStatus(codes.Error, safe.Error())
	}
}

func modelAttributes(provider, model, operation string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("gen_ai.operation.name", operation),
		attribute.String("gen_ai.system", provider),
		attribute.String("gen_ai.provider.name", provider),
		attribute.String("gen_ai.request.model", model),
	}
}
