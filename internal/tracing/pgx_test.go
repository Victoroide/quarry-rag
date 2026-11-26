package tracing

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

func TestRedactedPGXSpanOmitsStatementAndDatabaseDetail(t *testing.T) {
	const canary = "fixture-document-chunk-object-content"
	failure := &pgconn.PgError{Code: "22000", Message: canary, Detail: "private " + canary}
	recorder := recordSpans(t)
	tracer := NewQueryTracer()
	for _, tc := range []struct {
		sql, operation string
	}{
		{"  select '" + canary + "'", "SELECT"},
		{canary + " SELECT 1", "OTHER"},
		{"/* " + canary + " */ SELECT 1", "OTHER"},
		{"", "OTHER"},
	} {
		ctx := WithRedactIdentifiers(t.Context(), true)
		data := pgx.TraceQueryStartData{SQL: tc.sql, Args: []any{canary}}
		before := len(recorder.Ended())
		tracer.TraceQueryEnd(tracer.TraceQueryStart(ctx, nil, data), nil, pgx.TraceQueryEndData{Err: failure})
		require.Len(t, recorder.Ended(), before)

		ctx, parent := otel.Tracer("test").Start(ctx, "request")
		tracer.TraceQueryEnd(tracer.TraceQueryStart(ctx, nil, data), nil, pgx.TraceQueryEndData{Err: failure})
		ended := recorder.Ended()
		child := ended[len(ended)-1]
		require.Equal(t, "db.query", child.Name())
		require.Equal(t, parent.SpanContext().SpanID(), child.Parent().SpanID())
		attrs := spanAttrs(child)
		require.NotContains(t, attrs, "db.query.text")
		require.Equal(t, tc.operation, attrs["db.operation.name"].AsString())
		require.Equal(t, codes.Error, child.Status().Code)
		require.Equal(t, "operation failed", child.Status().Description)
		require.Len(t, child.Events(), 1)
		require.Contains(t, child.Events()[0].Attributes, attribute.String("exception.message", "operation failed"))
		for _, attr := range child.Events()[0].Attributes {
			require.NotContains(t, attr.Value.String(), canary)
		}
		parent.End()
	}

	ctx, parent := otel.Tracer("test").Start(t.Context(), "service")
	defer parent.End()
	sql := "SELECT '" + canary + "'"
	tracer.TraceQueryEnd(tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql}), nil, pgx.TraceQueryEndData{Err: failure})
	ended := recorder.Ended()
	child := ended[len(ended)-1]
	require.Equal(t, sql, spanAttrs(child)["db.query.text"].AsString())
	require.Contains(t, child.Status().Description, canary)
}
