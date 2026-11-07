package tracing

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace/noop"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type receivedExport struct {
	path    string
	headers http.Header
	body    []byte
}

func exportServer(t *testing.T) (collectorServer *httptest.Server, exports <-chan receivedExport) {
	t.Helper()
	requests := make(chan receivedExport, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		requests <- receivedExport{r.URL.Path, r.Header.Clone(), body}
		w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
		if r.Header.Get("Content-Type") == "application/json" {
			_, _ = w.Write([]byte("{}"))
		}
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func exportAttribute(attrs []*common.KeyValue, key string) string {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value.GetStringValue()
		}
	}
	return ""
}

func resetGlobals(t *testing.T) {
	t.Helper()
	tp, propagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagator)
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { t.Log(err) }))
	})
	otel.SetTracerProvider(noop.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	for _, key := range []string{"OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES", "OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS", "OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "OTEL_EXPORTER_OTLP_COMPRESSION", "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION"} {
		t.Setenv(key, "")
	}
}

func TestExportDestinations(t *testing.T) {
	for _, tc := range []struct {
		name                                       string
		generic, langsmith, disabled, custom, json bool
		selfHosted                                 bool
	}{
		{name: "langsmith", langsmith: true},
		{name: "generic", generic: true},
		{name: "both", generic: true, langsmith: true},
		{name: "disabled", generic: true, langsmith: true, disabled: true},
		{name: "default"},
		{name: "custom_resource", langsmith: true, custom: true},
		{name: "json", generic: true, langsmith: true, json: true},
		{name: "self_hosted", langsmith: true, selfHosted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetGlobals(t)
			generic, genericRequests := exportServer(t)
			langsmith, langsmithRequests := exportServer(t)
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", generic.URL+"/custom/traces")
			t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer%20t")
			if tc.custom {
				t.Setenv("OTEL_SERVICE_NAME", "custom")
				t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=test")
			}
			if tc.json {
				t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/json")
			}
			cfg := config.TracingConfig{Exporter: "none", Disabled: tc.disabled, LangSmith: config.LangSmithConfig{Enabled: tc.langsmith, APIKey: "test-key", Project: "quarry-test", Endpoint: langsmith.URL}}
			langSmithPath := "/otel/v1/traces"
			if tc.selfHosted {
				cfg.LangSmith.Endpoint += "/api/v1/"
				langSmithPath = "/api/v1/otel/v1/traces"
			}
			if tc.generic {
				cfg.Exporter = "otlp"
			}
			shutdown, err := Setup(t.Context(), cfg, "test-version")
			require.NoError(t, err)
			_, span := otel.Tracer("export-test").Start(t.Context(), "query.synthesis")
			span.SetAttributes(attribute.String("langsmith.span.kind", "chain"))
			span.End()
			require.NoError(t, shutdown(t.Context()))
			for _, destination := range []struct {
				name              string
				enabled           bool
				requests          <-chan receivedExport
				path, contentType string
			}{
				{"generic", tc.generic, genericRequests, "/custom/traces", map[bool]string{true: "application/json", false: "application/x-protobuf"}[tc.json]},
				{"langsmith", tc.langsmith, langsmithRequests, langSmithPath, "application/x-protobuf"},
			} {
				if !destination.enabled || tc.disabled {
					require.Empty(t, destination.requests)
					continue
				}
				require.Len(t, destination.requests, 1)
				received := <-destination.requests
				require.Equal(t, destination.path, received.path)
				require.Equal(t, destination.contentType, received.headers.Get("Content-Type"))
				if destination.name == "langsmith" {
					require.Equal(t, "test-key", received.headers.Get("X-Api-Key"))
					require.Equal(t, "quarry-test", received.headers.Get("Langsmith-Project"))
					require.Empty(t, received.headers.Get("Authorization"))
				} else {
					require.Equal(t, "Bearer t", received.headers.Get("Authorization"))
					require.Empty(t, received.headers.Get("X-Api-Key"))
				}
				var decoded collector.ExportTraceServiceRequest
				if destination.contentType == "application/json" {
					require.NoError(t, protojson.Unmarshal(received.body, &decoded))
				} else {
					require.NoError(t, proto.Unmarshal(received.body, &decoded))
				}
				require.Len(t, decoded.ResourceSpans, 1)
				resource := decoded.ResourceSpans[0]
				wantName := "quarry-rag"
				if tc.custom {
					wantName = "custom"
					require.Equal(t, "test", exportAttribute(resource.Resource.Attributes, "deployment.environment.name"))
				}
				require.Equal(t, wantName, exportAttribute(resource.Resource.Attributes, "service.name"))
				require.Equal(t, "test-version", exportAttribute(resource.Resource.Attributes, "service.version"))
				require.Len(t, resource.ScopeSpans, 1)
				require.Len(t, resource.ScopeSpans[0].Spans, 1)
				require.Equal(t, "chain", exportAttribute(resource.ScopeSpans[0].Spans[0].Attributes, "langsmith.span.kind"))
			}
		})
	}
}

func TestExportSampling(t *testing.T) {
	resetGlobals(t)
	server, requests := exportServer(t)
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	shutdown, err := Setup(t.Context(), config.TracingConfig{LangSmith: config.LangSmithConfig{Enabled: true, APIKey: "test-key", Endpoint: server.URL}}, "test")
	require.NoError(t, err)
	_, span := otel.Tracer("export-test").Start(context.WithoutCancel(t.Context()), "sampled-out")
	span.End()
	require.NoError(t, shutdown(t.Context()))
	require.Empty(t, requests)
}

func TestLangSmithExportsContinuedRequestsAsRoots(t *testing.T) {
	resetGlobals(t)
	generic, genericRequests := exportServer(t)
	langsmith, langsmithRequests := exportServer(t)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", generic.URL+"/v1/traces")
	shutdown, err := Setup(t.Context(), config.TracingConfig{Exporter: "otlp", LangSmith: config.LangSmithConfig{Enabled: true, APIKey: "test-key", Endpoint: langsmith.URL}}, "test")
	require.NoError(t, err)
	carrier := propagation.HeaderCarrier{}
	carrier.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	ctx, server := otel.Tracer("test").Start(otel.GetTextMapPropagator().Extract(t.Context(), carrier), "POST /query")
	_, child := otel.Tracer("test").Start(ctx, "query.retrieval")
	child.End()
	server.End()
	require.NoError(t, shutdown(t.Context()))
	for _, destination := range []struct {
		requests   <-chan receivedExport
		wantParent string
	}{{genericRequests, "00f067aa0ba902b7"}, {langsmithRequests, ""}} {
		require.Len(t, destination.requests, 1)
		var decoded collector.ExportTraceServiceRequest
		require.NoError(t, proto.Unmarshal((<-destination.requests).body, &decoded))
		spans := decoded.ResourceSpans[0].ScopeSpans[0].Spans
		require.Len(t, spans, 2)
		require.Equal(t, "query.retrieval", spans[0].Name)
		require.Equal(t, spans[1].SpanId, spans[0].ParentSpanId)
		require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", hex.EncodeToString(spans[1].TraceId))
		require.Equal(t, destination.wantParent, hex.EncodeToString(spans[1].ParentSpanId))
	}
}

func TestLangSmithExportPathWithoutProject(t *testing.T) {
	resetGlobals(t)
	server, requests := exportServer(t)
	shutdown, err := Setup(t.Context(), config.TracingConfig{LangSmith: config.LangSmithConfig{Enabled: true, APIKey: "test-key", Endpoint: server.URL}}, "test")
	require.NoError(t, err)
	_, span := otel.Tracer("test").Start(t.Context(), "request")
	span.End()
	require.NoError(t, shutdown(t.Context()))
	require.Len(t, requests, 1)
	request := <-requests
	require.Equal(t, "/otel/v1/traces", request.path)
	require.Empty(t, request.headers.Get("Langsmith-Project"))
}

func TestExportErrorsDoNotLogCredentialsOrResponseBody(t *testing.T) {
	for _, status := range []int{400, 401, 404, 413} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			resetGlobals(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte("fixture-sensitive-response"))
			}))
			defer server.Close()
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			shutdown, err := Setup(t.Context(), config.TracingConfig{LangSmith: config.LangSmithConfig{Enabled: true, APIKey: "fixture-secret-key", Endpoint: server.URL}}, "test")
			require.NoError(t, err)
			_, span := otel.Tracer("test").Start(t.Context(), "request")
			span.End()
			require.Error(t, shutdown(t.Context()))
			require.Equal(t, 1, strings.Count(logs.String(), "trace export failed"))
			require.Contains(t, logs.String(), fmt.Sprintf(`"http.response.status_code":%d`, status))
			require.NotContains(t, logs.String(), "fixture-secret-key")
			require.NotContains(t, logs.String(), "fixture-sensitive-response")
			require.NotContains(t, logs.String(), server.URL)
		})
	}
}

func TestResourceConfigurationRetainsCause(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "invalid-resource-attribute")
	_, err := Setup(t.Context(), config.TracingConfig{Exporter: "otlp"}, "test")
	require.ErrorContains(t, err, "tracing: resource configuration:")
	require.ErrorIs(t, err, sdkresource.ErrPartialResource)
}
