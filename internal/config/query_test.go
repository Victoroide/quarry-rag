package config

import (
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func queryConfigEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
	for _, key := range []string{"QUERY_TIMEOUT", "QUERY_ANSWER_POLICY", "NO_EVIDENCE_CATALOG_FILE", "SYNTHESIS_TIMEOUT", "QUERY_CONCURRENCY", "LEGACY_DELETION_CONCURRENCY", "QUERY_QUEUE_TIMEOUT", "MIN_RELEVANCE_SCORE", "HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT", "HTTP_IDLE_TIMEOUT", "EVALUATION_COLLECTION_PREFIX"} {
		t.Setenv(key, "")
	}
}
func TestWriteTimeoutMustExceedQueryTimeout(t *testing.T) {
	for _, v := range []string{"59s", "60s"} {
		t.Run(v, func(t *testing.T) {
			queryConfigEnvironment(t)
			t.Setenv("HTTP_WRITE_TIMEOUT", v)
			_, err := Load()
			require.Error(t, err)
			require.Contains(t, err.Error(), "HTTP_WRITE_TIMEOUT")
		})
	}
}
func TestQueryConfigDefaults(t *testing.T) {
	queryConfigEnvironment(t)
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, time.Minute, cfg.QueryTimeout)
	require.Equal(t, "both", cfg.QueryAnswerPolicy)
	require.Empty(t, cfg.EvaluationCollectionPrefix)
	require.Empty(t, cfg.NoEvidenceCatalogFile)
	require.Equal(t, 45*time.Second, cfg.SynthesisTimeout)
	require.Equal(t, 32, cfg.QueryConcurrency)
	require.Equal(t, 2*time.Second, cfg.QueryQueueTimeout)
	require.Zero(t, cfg.MinRelevanceScore)
	require.Equal(t, time.Minute, cfg.HTTPReadTimeout)
	require.Equal(t, 75*time.Second, cfg.HTTPWriteTimeout)
	require.Equal(t, time.Minute, cfg.HTTPIdleTimeout)
	t.Setenv("QUERY_TIMEOUT", "2m")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, 135*time.Second, cfg.HTTPWriteTimeout)
}

func TestNoEvidenceCatalogPathConfiguration(t *testing.T) {
	queryConfigEnvironment(t)
	t.Setenv("NO_EVIDENCE_CATALOG_FILE", "config/no evidence.json")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "config/no evidence.json", cfg.NoEvidenceCatalogFile)
}

func TestQueryAnswerPolicyConfiguration(t *testing.T) {
	queryConfigEnvironment(t)
	t.Setenv("QUERY_ANSWER_POLICY", "evidence_only")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "evidence_only", cfg.QueryAnswerPolicy)
	t.Setenv("QUERY_ANSWER_POLICY", "generated")
	_, err = Load()
	require.ErrorContains(t, err, "QUERY_ANSWER_POLICY")
}

func TestSynthesisInputProfileConfiguration(t *testing.T) {
	awsConfigEnvironment(t)
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "", cfg.SynthesisInputProfile)

	t.Setenv("SYNTHESIS_INPUT_PROFILE", "strict")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, "strict", cfg.SynthesisInputProfile)

	for _, bad := range []string{"best_effort", "STRICT", "true", "on"} {
		t.Run("invalid/"+bad, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("SYNTHESIS_INPUT_PROFILE", bad)
			_, err := Load()
			require.ErrorContains(t, err, "SYNTHESIS_INPUT_PROFILE")
		})
	}

	t.Run("fake mode rejects strict", func(t *testing.T) {
		queryConfigEnvironment(t)
		t.Setenv("SYNTHESIS_INPUT_PROFILE", "strict")
		_, err := Load()
		require.ErrorContains(t, err, "SYNTHESIS_INPUT_PROFILE")
	})
}
func TestQueryConfigRejectsInvalidBounds(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"QUERY_TIMEOUT", "4s"}, {"QUERY_TIMEOUT", "601s"}, {"QUERY_TIMEOUT", "bad"}, {"SYNTHESIS_TIMEOUT", "0s"}, {"SYNTHESIS_TIMEOUT", "60s"}, {"SYNTHESIS_TIMEOUT", "bad"},
		{"QUERY_CONCURRENCY", "0"}, {"QUERY_CONCURRENCY", "1025"}, {"QUERY_CONCURRENCY", "bad"}, {"LEGACY_DELETION_CONCURRENCY", "-1"}, {"LEGACY_DELETION_CONCURRENCY", "65"}, {"LEGACY_DELETION_CONCURRENCY", "abc"}, {"QUERY_QUEUE_TIMEOUT", "-1s"}, {"QUERY_QUEUE_TIMEOUT", "61s"}, {"QUERY_QUEUE_TIMEOUT", "bad"},
		{"MIN_RELEVANCE_SCORE", "-0.1"}, {"MIN_RELEVANCE_SCORE", "1.1"}, {"MIN_RELEVANCE_SCORE", "NaN"}, {"MIN_RELEVANCE_SCORE", "Inf"}, {"MIN_RELEVANCE_SCORE", "bad"},
		{"HTTP_READ_TIMEOUT", "4s"}, {"HTTP_READ_TIMEOUT", "31m"}, {"HTTP_READ_TIMEOUT", "bad"}, {"HTTP_WRITE_TIMEOUT", "bad"}, {"HTTP_IDLE_TIMEOUT", "0s"}, {"HTTP_IDLE_TIMEOUT", "-1s"}, {"HTTP_IDLE_TIMEOUT", "bad"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			queryConfigEnvironment(t)
			t.Setenv(tc.key, tc.value)
			_, err := Load()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.key)
		})
	}
}

func TestLegacyDeletionConcurrencyConfig(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{{"", 0}, {"0", 0}, {"1", 1}, {"4", 4}, {"64", 64}} {
		t.Run(tc.value, func(t *testing.T) {
			queryConfigEnvironment(t)
			t.Setenv("LEGACY_DELETION_CONCURRENCY", tc.value)
			cfg, err := Load()
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.LegacyDeletionConcurrency)
		})
	}
}

func TestEvaluationCollectionPrefixConfiguration(t *testing.T) {
	queryConfigEnvironment(t)
	t.Setenv("EVALUATION_COLLECTION_PREFIX", "evidence-qa")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "evidence-qa", cfg.EvaluationCollectionPrefix)
	t.Setenv("EVALUATION_COLLECTION_PREFIX", "wrong prefix")
	_, err = Load()
	require.ErrorContains(t, err, "EVALUATION_COLLECTION_PREFIX")
}

func TestQueryConfigAcceptsBoundaries(t *testing.T) {
	for _, tc := range []struct{ query, synthesis, concurrency, queue, score, read string }{
		{"5s", "1s", "1", "0s", "0", "5s"},
		{"10m", "9m", "1024", "60s", "1", "30m"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			queryConfigEnvironment(t)
			t.Setenv("QUERY_TIMEOUT", tc.query)
			t.Setenv("SYNTHESIS_TIMEOUT", tc.synthesis)
			t.Setenv("QUERY_CONCURRENCY", tc.concurrency)
			t.Setenv("QUERY_QUEUE_TIMEOUT", tc.queue)
			t.Setenv("MIN_RELEVANCE_SCORE", tc.score)
			t.Setenv("HTTP_READ_TIMEOUT", tc.read)
			t.Setenv("HTTP_WRITE_TIMEOUT", "11m")
			t.Setenv("HTTP_IDLE_TIMEOUT", "30s")
			_, err := Load()
			require.NoError(t, err)
		})
	}
}

func TestCitationSupportConfiguration(t *testing.T) {
	host := uuid.NewString() + ".example"
	endpoint := func(scheme string) string { return (&url.URL{Scheme: scheme, Host: host, Path: "/verify"}).String() }
	for _, tc := range []struct {
		name, key, value, token, wantError string
	}{
		{"local defaults", "", "", "", ""},
		{"mutable model", "CITATION_SUPPORT_MODEL", "example:latest", "", "CITATION_SUPPORT_MODEL"},
		{"timeout", "CITATION_SUPPORT_TIMEOUT", "0s", "", "CITATION_SUPPORT_TIMEOUT"},
		{"total timeout", "CITATION_SUPPORT_TIMEOUT", "60s", "", "CITATION_SUPPORT_TIMEOUT"},
		{"pair limit", "CITATION_SUPPORT_MAX_PAIRS", "33", "", "CITATION_SUPPORT_MAX_PAIRS"},
		{"pair minimum", "CITATION_SUPPORT_MAX_PAIRS", "0", "", "CITATION_SUPPORT_MAX_PAIRS"},
		{"plaintext external", "CITATION_SUPPORT_ENDPOINT", endpoint("http"), "fixture-token", "requires HTTPS"},
		{"unauthenticated external", "CITATION_SUPPORT_ENDPOINT", endpoint("https"), "", "TOKEN is required"},
		{"authenticated external", "CITATION_SUPPORT_ENDPOINT", endpoint("https"), "fixture-token", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queryConfigEnvironment(t)
			t.Setenv("GO_RAG_FAKE_MODE", "false")
			t.Setenv("AWS_REGION", "us-east-1")
			t.Setenv("VECTOR_STORE", "postgres")
			t.Setenv("SERVICE_TOKEN", "fixture-service-token")
			t.Setenv("CITATION_SUPPORT_MODEL", "example@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
			for _, env := range []string{"CITATION_SUPPORT_TIMEOUT", "CITATION_SUPPORT_MAX_PAIRS", "CITATION_SUPPORT_ENDPOINT", "CITATION_SUPPORT_TOKEN", "CITATION_SUPPORT_CALIBRATION_FILE"} {
				t.Setenv(env, "")
			}
			t.Setenv("CITATION_SUPPORT_TOKEN", tc.token)
			if tc.key != "" {
				t.Setenv(tc.key, tc.value)
			}
			cfg, err := Load()
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.value, cfg.CitationSupportEndpoint)
			require.Equal(t, 2*time.Second, cfg.CitationSupportTimeout)
			require.Equal(t, 8, cfg.CitationSupportMaxPairs)
			t.Setenv("CITATION_SUPPORT_MODEL", "")
			t.Setenv("CITATION_SUPPORT_TIMEOUT", "ignored")
			t.Setenv("CITATION_SUPPORT_ENDPOINT", "ignored")
			cfg, err = Load()
			require.NoError(t, err)
			require.Empty(t, cfg.CitationSupportModel)
		})
	}
}

func TestCitationSupportConfigurationIgnoresInheritedSettingsOffline(t *testing.T) {
	queryConfigEnvironment(t)
	t.Setenv("CITATION_SUPPORT_MODEL", "unusable")
	t.Setenv("CITATION_SUPPORT_ENDPOINT", "not an address")
	t.Setenv("CITATION_SUPPORT_TOKEN", "not a token")
	t.Setenv("CITATION_SUPPORT_CALIBRATION_FILE", "missing.json")
	t.Setenv("CITATION_SUPPORT_MAX_PAIRS", "-1")
	t.Setenv("CITATION_SUPPORT_TIMEOUT", "bad")
	cfg, err := Load()
	require.NoError(t, err)
	require.Empty(t, cfg.CitationSupportModel)
	require.Empty(t, cfg.CitationSupportEndpoint)
	require.Empty(t, cfg.CitationSupportToken)
	require.Empty(t, cfg.CitationSupportCalibrationFile)
}
