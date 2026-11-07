package config

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"unicode"
)

// TracingConfig selects optional trace destinations and content capture.
type TracingConfig struct {
	Exporter                 string
	Disabled, CaptureContent bool
	LangSmith                LangSmithConfig
}

// LangSmithConfig configures LangSmith's OTLP destination.
type LangSmithConfig struct {
	Enabled                   bool
	APIKey, Project, Endpoint string
}

func loadTracing() (TracingConfig, error) {
	cfg := TracingConfig{Exporter: envOrDefault("OTEL_TRACES_EXPORTER", "none")}
	for _, setting := range []struct {
		key string
		dst *bool
	}{
		{"OTEL_SDK_DISABLED", &cfg.Disabled},
		{"OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", &cfg.CaptureContent},
		{"LANGSMITH_TRACING", &cfg.LangSmith.Enabled},
	} {
		value, err := envBoolOrDefault(setting.key, false)
		if err != nil {
			return cfg, err
		}
		*setting.dst = value
	}
	if cfg.Exporter != "none" && cfg.Exporter != "otlp" {
		return cfg, fmt.Errorf("config: OTEL_TRACES_EXPORTER must be none or otlp")
	}
	cfg.LangSmith.APIKey = os.Getenv("LANGSMITH_API_KEY")
	cfg.LangSmith.Project = os.Getenv("LANGSMITH_PROJECT")
	cfg.LangSmith.Endpoint = os.Getenv("LANGSMITH_ENDPOINT")
	if cfg.Disabled || (cfg.Exporter == "none" && !cfg.LangSmith.Enabled) {
		return cfg, nil
	}
	for _, key := range []string{"OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"} {
		value := os.Getenv(key)
		if value != "" && value != "http/protobuf" && value != "http/json" {
			return cfg, fmt.Errorf("config: %s must be http/protobuf or http/json", key)
		}
	}
	switch sampler := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER"))); sampler {
	case "", "always_on", "always_off", "parentbased_always_on", "parentbased_always_off":
	case "traceidratio", "parentbased_traceidratio":
		if arg := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG")); arg != "" {
			ratio, err := strconv.ParseFloat(arg, 64)
			if err != nil || math.IsNaN(ratio) || ratio < 0 || ratio > 1 {
				return cfg, fmt.Errorf("config: OTEL_TRACES_SAMPLER_ARG must be a ratio from 0 to 1")
			}
		}
	default:
		return cfg, fmt.Errorf("config: OTEL_TRACES_SAMPLER must be a supported sampler")
	}
	if cfg.LangSmith.Enabled {
		if cfg.LangSmith.APIKey == "" || strings.ContainsFunc(cfg.LangSmith.APIKey, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return cfg, fmt.Errorf("config: LANGSMITH_API_KEY is required and must not contain whitespace or control characters")
		}
		if strings.ContainsFunc(cfg.LangSmith.Project, unicode.IsControl) {
			return cfg, fmt.Errorf("config: LANGSMITH_PROJECT must not contain control characters")
		}
		if cfg.LangSmith.Endpoint != "" {
			u, err := url.Parse(cfg.LangSmith.Endpoint)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return cfg, fmt.Errorf("config: LANGSMITH_ENDPOINT must be an absolute http or https API address without credentials, query or fragment")
			}
		}
	}
	return cfg, nil
}
