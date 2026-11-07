package tracing

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type queryTracer struct{}
type querySpanKey struct{}

// NewQueryTracer instruments SQL statements only inside an existing trace.
func NewQueryTracer() pgx.QueryTracer { return queryTracer{} }

func (queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	operation := ""
	if fields := strings.Fields(data.SQL); len(fields) > 0 {
		operation = strings.ToUpper(fields[0])
	}
	ctx, span := start(ctx, "db.query", "", trace.SpanKindClient,
		attribute.String("db.system.name", "postgresql"), attribute.String("db.operation.name", operation), attribute.String("db.query.text", data.SQL))
	return context.WithValue(ctx, querySpanKey{}, span)
}

func (queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if span, ok := ctx.Value(querySpanKey{}).(trace.Span); ok {
		RecordError(span, data.Err)
		span.End()
	}
}
