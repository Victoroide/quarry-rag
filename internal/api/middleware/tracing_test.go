package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTracingRequestShapeAndPropagation(t *testing.T) {
	previous, propagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(propagator)
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("POST /query", func(w http.ResponseWriter, r *http.Request) {
		require.True(t, tracing.ContentCapture(r.Context()))
		require.Equal(t, "fixture-baggage", baggage.FromContext(r.Context()).Member("fixture").Value())
		w.WriteHeader(500)
	})
	handler := RequestID(Tracing(mux, true))
	probe := httptest.NewRecorder()
	handler.ServeHTTP(probe, httptest.NewRequestWithContext(t.Context(), "GET", "/readyz", http.NoBody))
	require.Empty(t, recorder.Ended())
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), "FIXTURE", "/missing", http.NoBody))
	require.Len(t, recorder.Ended(), 1)
	unmatched := recorder.Ended()[0]
	require.Equal(t, "_OTHER unmatched", unmatched.Name())
	require.Contains(t, unmatched.Attributes(), attribute.String("http.request.method", "_OTHER"))
	for _, attr := range unmatched.Attributes() {
		require.NotEqual(t, attribute.Key("http.route"), attr.Key)
	}
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/query", http.NoBody)
	req.Header.Set("X-Request-ID", "fixture-request")
	req.Header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	req.Header.Set("Baggage", "fixture=fixture-baggage")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	require.Equal(t, 500, response.Code)
	require.Len(t, recorder.Ended(), 2)
	span := recorder.Ended()[1]
	require.Equal(t, "POST /query", span.Name())
	require.Equal(t, trace.SpanKindServer, span.SpanKind())
	require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", span.SpanContext().TraceID().String())
	require.Equal(t, "00f067aa0ba902b7", span.Parent().SpanID().String())
	require.Equal(t, codes.Error, span.Status().Code)
	attrs := map[string]attribute.Value{}
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value
	}
	require.Equal(t, "/query", attrs["http.route"].AsString())
	require.Equal(t, "POST", attrs["http.request.method"].AsString())
	require.EqualValues(t, 500, attrs["http.response.status_code"].AsInt64())
	require.Equal(t, "fixture-request", attrs["quarry.request_id"].AsString())
	require.Equal(t, "fixture-request", attrs["langsmith.metadata.request_id"].AsString())
	require.Equal(t, "chain", attrs["langsmith.span.kind"].AsString())
}
