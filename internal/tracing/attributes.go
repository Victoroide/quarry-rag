package tracing

import (
	"context"

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

// RecordError records an operation failure without changing its returned error.
func RecordError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
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
