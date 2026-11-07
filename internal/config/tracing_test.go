package config

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func clearTracingEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"OTEL_TRACES_EXPORTER", "OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG", "OTEL_SDK_DISABLED", "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "LANGSMITH_TRACING", "LANGSMITH_API_KEY", "LANGSMITH_PROJECT", "LANGSMITH_ENDPOINT"} {
		t.Setenv(key, "")
	}
}

func TestTracingDefaultsAndValidation(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"defaults", nil, ""},
		{"exporter", map[string]string{"OTEL_TRACES_EXPORTER": "zipkin"}, "OTEL_TRACES_EXPORTER"},
		{"grpc", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, "OTEL_EXPORTER_OTLP_PROTOCOL"},
		{"trace_grpc", map[string]string{"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc"}, "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"},
		{"json", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_EXPORTER_OTLP_PROTOCOL": "http/json"}, ""},
		{"missing_key", map[string]string{"LANGSMITH_TRACING": "true"}, "LANGSMITH_API_KEY"},
		{"key_whitespace", map[string]string{"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test key"}, "LANGSMITH_API_KEY"},
		{"key_control", map[string]string{"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test\x7fkey"}, "LANGSMITH_API_KEY"},
		{"endpoint", map[string]string{"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key", "LANGSMITH_ENDPOINT": "localhost"}, "LANGSMITH_ENDPOINT"},
		{"endpoint_valid", map[string]string{"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key", "LANGSMITH_ENDPOINT": server.URL + "/api/v1"}, ""},
		{"disabled", map[string]string{"OTEL_SDK_DISABLED": "true", "LANGSMITH_TRACING": "true", "OTEL_TRACES_EXPORTER": "otlp"}, ""},
		{"disabled_protocol", map[string]string{"OTEL_SDK_DISABLED": "true", "OTEL_TRACES_EXPORTER": "otlp", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, ""},
		{"unknown_sampler", map[string]string{"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key", "OTEL_TRACES_SAMPLER": "bogus"}, "OTEL_TRACES_SAMPLER"},
		{"decimal_comma", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0,1"}, "OTEL_TRACES_SAMPLER_ARG"},
		{"out_of_range", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": "parentbased_traceidratio", "OTEL_TRACES_SAMPLER_ARG": "1.5"}, "OTEL_TRACES_SAMPLER_ARG"},
		{"negative_ratio", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "-0.1"}, "OTEL_TRACES_SAMPLER_ARG"},
		{"not_a_number", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "NaN"}, "OTEL_TRACES_SAMPLER_ARG"},
		{"infinite_ratio", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "+Inf"}, "OTEL_TRACES_SAMPLER_ARG"},
		{"valid_ratio", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0.25"}, ""},
		{"ratio_zero", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0"}, ""},
		{"ratio_one", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": "parentbased_traceidratio", "OTEL_TRACES_SAMPLER_ARG": "1"}, ""},
		{"normalized_sampler", map[string]string{"OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": " TRACEIDRATIO ", "OTEL_TRACES_SAMPLER_ARG": " 0.25 "}, ""},
		{"disabled_sampler", map[string]string{"OTEL_SDK_DISABLED": "true", "OTEL_TRACES_EXPORTER": "otlp", "OTEL_TRACES_SAMPLER": "bogus"}, ""},
		{"no_destination_sampler", map[string]string{"OTEL_TRACES_SAMPLER": "bogus"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearTracingEnv(t)
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			cfg, err := loadTracing()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			if tc.name == "defaults" {
				require.Equal(t, "none", cfg.Exporter)
				require.False(t, cfg.CaptureContent)
				require.False(t, cfg.LangSmith.Enabled)
			}
			if tc.name == "disabled" {
				require.True(t, cfg.Disabled)
			}
		})
	}
	for _, key := range []string{"OTEL_SDK_DISABLED", "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "LANGSMITH_TRACING"} {
		t.Run(key, func(t *testing.T) {
			clearTracingEnv(t)
			t.Setenv(key, "invalid")
			_, err := loadTracing()
			require.ErrorContains(t, err, key)
		})
	}
}

func TestLoadIncludesTracing(t *testing.T) {
	clearTracingEnv(t)
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
	t.Setenv("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "true")
	cfg, err := Load()
	require.NoError(t, err)
	require.True(t, cfg.Tracing.CaptureContent)
	t.Setenv("OTEL_TRACES_EXPORTER", "zipkin")
	_, err = Load()
	require.ErrorContains(t, err, "OTEL_TRACES_EXPORTER")
}
