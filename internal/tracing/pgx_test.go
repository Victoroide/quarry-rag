package tracing

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func TestQueryTracerRequiresParentAndOmitsArguments(t *testing.T) {
	recorder := recordSpans(t)
	tracer := NewQueryTracer()
	query := pgx.TraceQueryStartData{SQL: "SELECT content FROM documents WHERE id=$1", Args: []any{"fixture-secret"}}
	ctx := tracer.TraceQueryStart(t.Context(), nil, query)
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{CommandTag: pgconn.NewCommandTag("SELECT 1")})
	require.Empty(t, recorder.Ended())
	ctx, parent := otel.Tracer("test").Start(t.Context(), "request")
	queryCtx := tracer.TraceQueryStart(ctx, nil, query)
	tracer.TraceQueryEnd(queryCtx, nil, pgx.TraceQueryEndData{Err: errors.New("database unavailable")})
	require.Len(t, recorder.Ended(), 1)
	span := recorder.Ended()[0]
	require.Equal(t, "db.query", span.Name())
	require.Equal(t, trace.SpanKindClient, span.SpanKind())
	require.Equal(t, parent.SpanContext().SpanID(), span.Parent().SpanID())
	attrs := spanAttrs(span)
	require.Equal(t, "postgresql", attrs["db.system.name"].AsString())
	require.Equal(t, "SELECT", attrs["db.operation.name"].AsString())
	require.Equal(t, query.SQL, attrs["db.query.text"].AsString())
	for _, attr := range span.Attributes() {
		require.NotContains(t, attr.Value.String(), "fixture-secret")
	}
	require.Equal(t, codes.Error, span.Status().Code)
	parent.End()
}
