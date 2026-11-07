// Package tracing configures optional trace export and instruments service operations.
package tracing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Victoroide/quarry-rag/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// Setup installs trace export only when a destination is explicitly enabled.
func Setup(ctx context.Context, cfg config.TracingConfig, version string) (func(context.Context) error, error) {
	if cfg.Disabled || (cfg.Exporter != "otlp" && !cfg.LangSmith.Enabled) {
		return func(context.Context) error { return nil }, nil
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		var exportErr langSmithExportError
		if errors.As(err, &exportErr) {
			slog.Warn("trace export failed", "http.response.status_code", exportErr.status)
			return
		}
		slog.Warn("trace export failed")
	}))
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("quarry-rag"), semconv.ServiceVersion(version)),
		resource.WithFromEnv(), resource.WithTelemetrySDK())
	if err != nil {
		return nil, fmt.Errorf("tracing: resource configuration: %w", err)
	}
	var exporters []sdktrace.SpanExporter
	var destinations []string
	if cfg.Exporter == "otlp" {
		exporter, exportErr := otlptracehttp.New(ctx)
		if exportErr != nil {
			return nil, fmt.Errorf("tracing: OTLP exporter initialization failed")
		}
		exporters = append(exporters, exporter)
		destinations = append(destinations, "otlp")
	}
	if cfg.LangSmith.Enabled {
		exporter, exportErr := newLangSmithExporter(ctx, cfg.LangSmith)
		if exportErr != nil {
			for _, previous := range exporters {
				_ = previous.Shutdown(ctx)
			}
			return nil, fmt.Errorf("tracing: LangSmith exporter initialization failed")
		}
		exporters = append(exporters, exporter)
		destinations = append(destinations, "langsmith")
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	for _, exporter := range exporters {
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}
	provider := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	slog.InfoContext(ctx, "trace export enabled", "destinations", destinations)
	return func(ctx context.Context) error {
		err := errors.Join(provider.ForceFlush(ctx), provider.Shutdown(ctx))
		if err != nil {
			otel.Handle(err)
		}
		return err
	}, nil
}
