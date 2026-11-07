package tracing

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Default LangSmith API address for the US region.
const langSmithDefaultEndpoint = "https://api.smith.langchain.com"

func newLangSmithExporter(ctx context.Context, cfg config.LangSmithConfig) (sdktrace.SpanExporter, error) {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = langSmithDefaultEndpoint
	}
	headers := map[string]string{"x-api-key": cfg.APIKey}
	if cfg.Project != "" {
		headers["Langsmith-Project"] = cfg.Project
	}
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(strings.TrimRight(endpoint, "/")+"/otel/v1/traces"),
		otlptracehttp.WithHeaders(headers),
		otlptracehttp.WithEncoding(otlptracehttp.EncodingProtobuf),
		otlptracehttp.WithCompression(otlptracehttp.NoCompression),
		otlptracehttp.WithTimeout(10*time.Second),
		otlptracehttp.WithHTTPClient(&http.Client{Transport: langSmithStatusTransport{}, Timeout: 10 * time.Second}),
		otlptracehttp.WithTLSClientConfig(nil))
	if err != nil {
		return nil, err
	}
	return localRootExporter{exporter}, nil
}

// localRootExporter exports spans that continue a remote trace as roots,
// because LangSmith drops spans whose parent it never receives.
type localRootExporter struct{ sdktrace.SpanExporter }

func (e localRootExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	rooted := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, span := range spans {
		if span.Parent().IsRemote() {
			span = remoteRootSpan{span}
		}
		rooted[i] = span
	}
	var status atomic.Int64
	err := e.SpanExporter.ExportSpans(context.WithValue(ctx, langSmithStatusKey{}, &status), rooted)
	if code := int(status.Load()); err != nil && code >= http.StatusBadRequest {
		return langSmithExportError{err, code}
	}
	return err
}

type remoteRootSpan struct{ sdktrace.ReadOnlySpan }

func (remoteRootSpan) Parent() trace.SpanContext { return trace.SpanContext{} }

type langSmithStatusKey struct{}

type langSmithStatusTransport struct{}

func (langSmithStatusTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	status, _ := r.Context().Value(langSmithStatusKey{}).(*atomic.Int64)
	if status != nil {
		status.Store(0)
	}
	response, err := http.DefaultTransport.RoundTrip(r)
	if status != nil && response != nil {
		status.Store(int64(response.StatusCode))
	}
	return response, err
}

type langSmithExportError struct {
	error
	status int
}

func (e langSmithExportError) Unwrap() error { return e.error }
