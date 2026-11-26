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
	redact := RedactIdentifiers(ctx)
	operation := ""
	for token := range strings.FieldsSeq(data.SQL) {
		operation = token
		break
	}
	attrs := []attribute.KeyValue{attribute.String("db.system.name", "postgresql")}
	if redact {
		operation = boundedSQLOperation(operation)
	} else {
		operation = strings.ToUpper(operation)
		attrs = append(attrs, attribute.String("db.query.text", data.SQL))
	}
	attrs = append(attrs, attribute.String("db.operation.name", operation))
	ctx, span := start(ctx, "db.query", "", trace.SpanKindClient, attrs...)
	return context.WithValue(ctx, querySpanKey{}, span)
}

func (queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if span, ok := ctx.Value(querySpanKey{}).(trace.Span); ok {
		RecordError(ctx, span, data.Err)
		span.End()
	}
}

func boundedSQLOperation(token string) string {
	if len(token) <= len("TRUNCATE") {
		switch verb := strings.ToUpper(token); verb {
		case "SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "VALUES", "MERGE", "COPY",
			"BEGIN", "COMMIT", "ROLLBACK", "CREATE", "ALTER", "DROP", "TRUNCATE", "SET", "SHOW",
			"LOCK", "EXPLAIN", "ANALYZE", "VACUUM":
			return verb
		}
	}
	return "OTHER"
}
