package middleware

import (
	"errors"
	"net/http"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Tracing continues incoming traces and records requests by matched route.
func Tracing(next http.Handler, captureContent bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/livez", "/readyz", "/health":
			next.ServeHTTP(w, r)
			return
		}
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx = tracing.WithContentCapture(ctx, captureContent)
		requestID := logctx.RequestID(ctx)
		method := spanMethod(r.Method)
		ctx, span := otel.Tracer("github.com/Victoroide/quarry-rag/internal/api/middleware").Start(ctx, method+" unmatched", trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(
			attribute.String(tracing.LangSmithKind, "chain"), attribute.String("http.request.method", method),
			attribute.String("quarry.request_id", requestID), attribute.String("langsmith.metadata.request_id", requestID)))
		defer span.End()
		rt := r.WithContext(ctx)
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, rt)
		r.Pattern = rt.Pattern
		route := rt.Pattern
		if route == "" {
			route = method + " unmatched"
		}
		span.SetName(route)
		span.SetAttributes(attribute.Int("http.response.status_code", rw.statusCode))
		if i := strings.IndexByte(rt.Pattern, '/'); i >= 0 {
			span.SetAttributes(attribute.String("http.route", rt.Pattern[i:]))
		}
		if rw.statusCode >= http.StatusInternalServerError {
			tracing.RecordError(span, errors.New(http.StatusText(rw.statusCode)))
		}
	})
}

func spanMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	}
	return "_OTHER"
}
