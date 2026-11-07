package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestTracingPreservesRouterMetricPatterns(t *testing.T) {
	previous := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
		otel.SetTracerProvider(previous)
	})
	m := metrics.NewService(nil)
	router := NewRouter(&RouterDeps{Config: &config.Config{ServiceToken: "secret"}, ChunkStore: requestLogPanicStore{}, Metrics: m})
	for _, request := range []struct{ method, path string }{{"POST", "/query"}, {"GET", "/documents"}} {
		req := httptest.NewRequestWithContext(t.Context(), request.method, request.path, http.NoBody)
		if request.path == "/documents" {
			req.Header.Set("X-Service-Token", "secret")
		}
		router.ServeHTTP(httptest.NewRecorder(), req)
	}
	var output strings.Builder
	require.NoError(t, m.Registry.WriteText(&output))
	require.Contains(t, output.String(), `quarry_http_requests_total{route="POST /query",code="401"} 1`)
	require.NotContains(t, output.String(), `route="unmatched"`)
	require.Len(t, recorder.Ended(), 2)
	require.Equal(t, "POST /query", recorder.Ended()[0].Name())
	require.Equal(t, "GET /documents", recorder.Ended()[1].Name())
}
