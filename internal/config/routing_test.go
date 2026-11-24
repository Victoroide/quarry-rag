package config

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func routingConfigEnvironment(t *testing.T) {
	t.Helper()
	awsConfigEnvironment(t)
	for _, key := range []string{"ROUTING_POLICY", "ROUTING_MARGIN", "ROUTING_PROFILE_FILE", "ROUTING_EMBEDDING_REVISION"} {
		t.Setenv(key, "")
	}
}

func TestRoutingProfileBindsOpenAIDeployment(t *testing.T) {
	routingConfigEnvironment(t)
	a, b := httptest.NewServer(http.NotFoundHandler()), httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)
	t.Setenv("ROUTING_POLICY", "evidence")
	t.Setenv("EMBEDDING_PROVIDER", "openai")
	t.Setenv("OPENAI_EMBEDDING_MODEL", "local-embed")
	t.Setenv("OPENAI_BASE_URL", a.URL)
	unbound := `{"schema_version":1,"profiles":[{"provider":"openai","model":"local-embed","dimensions":1024,"metric":"cosine","margin":0.09,"version":"v1"}]}`
	t.Setenv("ROUTING_PROFILE_FILE", writeRoutingProfileFile(t, unbound))
	cfg, err := Load()
	require.NoError(t, err)
	require.False(t, cfg.RoutingCalibrationMatched)

	digest := sha256.Sum256([]byte(a.URL))
	bound := fmt.Sprintf(`{"schema_version":1,"profiles":[{"provider":"openai","model":"local-embed","dimensions":1024,"metric":"cosine","margin":0.09,"version":"v1","endpoint_sha256":"%x","model_revision":"release-1"}]}`, digest)
	t.Setenv("ROUTING_PROFILE_FILE", writeRoutingProfileFile(t, bound))
	for _, tc := range []struct {
		endpoint, revision string
		matches            bool
	}{
		{a.URL, "", false},
		{a.URL + "/", "release-1", true},
		{a.URL, "release-2", false},
		{b.URL, "release-1", false},
	} {
		t.Setenv("OPENAI_BASE_URL", tc.endpoint)
		t.Setenv("ROUTING_EMBEDDING_REVISION", tc.revision)
		cfg, err = Load()
		require.NoError(t, err)
		require.Equal(t, tc.matches, cfg.RoutingCalibrationMatched, tc)
	}
}

func writeRoutingProfileFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routing-profiles.json")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestRoutingConfigDefaultsAndLegacyIgnoresMargin(t *testing.T) {
	routingConfigEnvironment(t)
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, RoutingPolicyLegacy, cfg.RoutingPolicy)
	require.Nil(t, cfg.RoutingMargin)
	require.Empty(t, cfg.RoutingProfileFile)
	require.False(t, cfg.RoutingCalibrationMatched)

	t.Setenv("ROUTING_MARGIN", "unused in legacy")
	t.Setenv("ROUTING_PROFILE_FILE", "unused in legacy")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, RoutingPolicyLegacy, cfg.RoutingPolicy)
	require.Nil(t, cfg.RoutingMargin)
	require.Zero(t, cfg.EffectiveRoutingMargin)
}

func TestRoutingConfigRejectsInvalidValues(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"ROUTING_POLICY", "adaptive"},
		{"ROUTING_POLICY", "LEGACY"},
		{"ROUTING_MARGIN", "-0.01"},
		{"ROUTING_MARGIN", "1.01"},
		{"ROUTING_MARGIN", "NaN"},
		{"ROUTING_MARGIN", "+Inf"},
		{"ROUTING_MARGIN", "-Inf"},
		{"ROUTING_MARGIN", "invalid"},
		{"ROUTING_PROFILE_FILE", "   "},
		{"ROUTING_PROFILE_FILE", "bad\npath.json"},
		{"ROUTING_PROFILE_FILE", filepath.Join(t.TempDir(), "missing.json")},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			routingConfigEnvironment(t)
			t.Setenv("ROUTING_POLICY", "evidence")
			t.Setenv(tc.key, tc.value)
			_, err := Load()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.key)
		})
	}
}

func TestRoutingProfileFileValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body string
	}{
		{"empty json", `{}`},
		{"bad schema version", `{"schema_version":2,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":0.08}]}`},
		{"unknown field", `{"schema_version":1,"extra":true,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":0.08}]}`},
		{"trailing data", `{"schema_version":1,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":0.08}]} {}`},
		{"empty profiles", `{"schema_version":1,"profiles":[]}`},
		{"negative margin", `{"schema_version":1,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":-0.1}]}`},
		{"missing margin", `{"schema_version":1,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine"}]}`},
		{"null margin", `{"schema_version":1,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":null}]}`},
		{"margin above 1", `{"schema_version":1,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":1.1}]}`},
		{"unsupported metric", `{"schema_version":1,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"dot","margin":0.08}]}`},
		{"zero dimensions", `{"schema_version":1,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":0,"metric":"cosine","margin":0.08}]}`},
		{"missing version", `{"schema_version":1,"profiles":[{"version":"","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":0.08}]}`},
		{"duplicate profile", `{"schema_version":1,"profiles":[{"version":"1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":0.08},{"version":"2","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":0.10}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routingConfigEnvironment(t)
			t.Setenv("ROUTING_POLICY", "evidence")
			t.Setenv("ROUTING_PROFILE_FILE", writeRoutingProfileFile(t, tc.body))
			_, err := Load()
			require.Error(t, err)
			require.Contains(t, err.Error(), "ROUTING_PROFILE_FILE")
		})
	}
}

func TestRoutingProfileCalibrationMatchingAndFallbacks(t *testing.T) {
	t.Run("missing calibration in evidence mode", func(t *testing.T) {
		routingConfigEnvironment(t)
		t.Setenv("ROUTING_POLICY", "evidence")
		cfg, err := Load()
		require.NoError(t, err)
		require.Equal(t, RoutingPolicyEvidence, cfg.RoutingPolicy)
		require.False(t, cfg.RoutingCalibrationMatched)
	})

	t.Run("matched profile defaults margin and allows override", func(t *testing.T) {
		routingConfigEnvironment(t)
		t.Setenv("ROUTING_POLICY", "evidence")
		path := writeRoutingProfileFile(t, `{"schema_version":1,"profiles":[{"version":"v1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":1024,"metric":"cosine","margin":0.09}]}`)
		t.Setenv("ROUTING_PROFILE_FILE", path)
		cfg, err := Load()
		require.NoError(t, err)
		require.True(t, cfg.RoutingCalibrationMatched)
		require.Equal(t, 0.09, cfg.EffectiveRoutingMargin)
		require.Equal(t, "v1", cfg.RoutingProfileVersion)

		t.Setenv("ROUTING_MARGIN", "0.14")
		cfg, err = Load()
		require.NoError(t, err)
		require.True(t, cfg.RoutingCalibrationMatched)
		require.Equal(t, 0.14, cfg.EffectiveRoutingMargin)

		t.Setenv("ROUTING_MARGIN", "0")
		cfg, err = Load()
		require.NoError(t, err)
		require.True(t, cfg.RoutingCalibrationMatched)
		require.Zero(t, cfg.EffectiveRoutingMargin)
	})

	t.Run("mismatched dimensions or model falls back conservatively", func(t *testing.T) {
		routingConfigEnvironment(t)
		t.Setenv("ROUTING_POLICY", "evidence")
		path := writeRoutingProfileFile(t, `{"schema_version":1,"profiles":[{"version":"v1","provider":"bedrock","model":"amazon.titan-embed-text-v2:0","dimensions":512,"metric":"cosine","margin":0.09}]}`)
		t.Setenv("ROUTING_PROFILE_FILE", path)
		t.Setenv("ROUTING_MARGIN", "0.12")
		cfg, err := Load()
		require.NoError(t, err)
		require.False(t, cfg.RoutingCalibrationMatched)
		require.True(t, slices.ContainsFunc(cfg.Warnings, func(w string) bool { return strings.Contains(w, "calibration") }))
	})

	t.Run("explicit margin without profile file", func(t *testing.T) {
		routingConfigEnvironment(t)
		t.Setenv("ROUTING_POLICY", "evidence")
		t.Setenv("ROUTING_MARGIN", "0.07")
		cfg, err := Load()
		require.NoError(t, err)
		require.False(t, cfg.RoutingCalibrationMatched)
		require.Zero(t, cfg.EffectiveRoutingMargin)
	})

	t.Run("published calibration artifact contains valid replay profiles", func(t *testing.T) {
		for _, dims := range []int{256, 1024} {
			profile, err := LookupRoutingProfile("", EmbeddingProfile{
				Provider:   "replay",
				Model:      "quarry-char3-bow-v1",
				Dimensions: dims,
			})
			require.NoError(t, err)
			require.NotNil(t, profile)
			require.NotEmpty(t, profile.Version)
			require.Equal(t, "cosine", profile.Metric)
			require.GreaterOrEqual(t, *profile.Margin, 0.0)
			require.LessOrEqual(t, *profile.Margin, 1.0)
		}
	})
}

func FuzzRoutingProfileSet(f *testing.F) {
	f.Add(embeddedRoutingProfilesJSON)
	f.Add([]byte(`{"schema_version":1,"profiles":[{"margin":null}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxRoutingProfileFileBytes {
			t.Skip()
		}
		set, err := parseRoutingProfileSet(data)
		if err != nil {
			return
		}
		require.Equal(t, routingProfileSchemaVersion, set.SchemaVersion)
		require.NotEmpty(t, set.Profiles)
		for _, profile := range set.Profiles {
			require.NotNil(t, profile.Margin)
			require.True(t, validRoutingMargin(*profile.Margin))
		}
	})
}
