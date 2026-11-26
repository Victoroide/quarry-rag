package config

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

func TestSubjectAuthoritySettingsDoNotActivateGrants(t *testing.T) {
	awsConfigEnvironment(t)
	for _, key := range []string{"AUTHZ_MODE", "AUTHZ_ISSUER", "AUTHZ_AUDIENCE", "AUTHZ_JWKS_FILE"} {
		t.Setenv(key, "")
	}
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, AuthzModeService, cfg.AuthzMode)
	require.Zero(t, cfg.SubjectAuthority)

	t.Setenv("AUTHZ_MODE", "service")
	t.Setenv("AUTHZ_ISSUER", "staged-issuer")
	t.Setenv("AUTHZ_AUDIENCE", "staged-audience")
	t.Setenv("AUTHZ_JWKS_FILE", "unmounted-trust")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, AuthzModeService, cfg.AuthzMode)
	require.Equal(t, SubjectAuthorityConfig{Issuer: "staged-issuer", Audience: "staged-audience", JWKSFile: "unmounted-trust"}, cfg.SubjectAuthority)

	t.Setenv("AUTHZ_MODE", "grants")
	_, err = Load()
	require.ErrorContains(t, err, "AUTHZ_MODE")
}

func writeValidJWKSFixture(t *testing.T, expiresAt time.Time) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	data, err := json.Marshal(map[string]any{
		"expires_at": expiresAt,
		"keys":       []jose.JSONWebKey{{Key: key.Public(), KeyID: "k1"}},
	})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "jwks.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func configureValidGrantsEnv(t *testing.T, jwksPath string) {
	t.Helper()
	awsConfigEnvironment(t)
	t.Setenv("AUTHZ_MODE", AuthzModeGrants)
	t.Setenv("GO_RAG_FAKE_MODE", "false")
	t.Setenv("SERVICE_TOKEN", "")
	t.Setenv("SERVICE_TOKENS", "0123456789abcdef0123456789abcdef")
	t.Setenv("INGEST_CONTENT_DEDUP", "off")
	t.Setenv("VECTOR_STORE", "postgres")
	t.Setenv("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "false")
	t.Setenv("EVALUATION_COLLECTION_PREFIX", "")
	t.Setenv("AUTHZ_ISSUER", "https://issuer.example/delegated")
	t.Setenv("AUTHZ_AUDIENCE", "quarry-documents")
	t.Setenv("AUTHZ_JWKS_FILE", jwksPath)
}

func TestGrantsModeConfigValidation(t *testing.T) {
	validJWKS := writeValidJWKSFixture(t, time.Now().UTC().Add(time.Hour))
	configureValidGrantsEnv(t, validJWKS)
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, AuthzModeGrants, cfg.AuthzMode)

	expiredJWKS := writeValidJWKSFixture(t, time.Now().UTC().Add(-time.Minute))
	badJWKS := filepath.Join(t.TempDir(), "bad-jwks.json")
	require.NoError(t, os.WriteFile(badJWKS, []byte(`{`), 0o600))

	for _, tc := range []struct {
		name, key, value, want string
	}{
		{"invalid mode", "AUTHZ_MODE", "partial", "AUTHZ_MODE"},
		{"fake mode", "GO_RAG_FAKE_MODE", "true", "GO_RAG_FAKE_MODE"},
		{"missing service token", "SERVICE_TOKENS", "", "SERVICE_TOKEN"},
		{"shared dedup", "INGEST_CONTENT_DEDUP", "collection", "INGEST_CONTENT_DEDUP"},
		{"default dedup", "INGEST_CONTENT_DEDUP", "", "INGEST_CONTENT_DEDUP"},
		{"content capture", "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "true", "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"},
		{"evaluation fixtures", "EVALUATION_COLLECTION_PREFIX", "eval-prefix", "EVALUATION_COLLECTION_PREFIX"},
		{"http issuer", "AUTHZ_ISSUER", "http://issuer.example", "AUTHZ_ISSUER"},
		{"missing audience", "AUTHZ_AUDIENCE", "", "AUTHZ_AUDIENCE"},
		{"missing jwks file", "AUTHZ_JWKS_FILE", filepath.Join(t.TempDir(), "absent.json"), "AUTHZ_JWKS_FILE"},
		{"expired jwks file", "AUTHZ_JWKS_FILE", expiredJWKS, "AUTHZ_JWKS_FILE"},
		{"malformed jwks file", "AUTHZ_JWKS_FILE", badJWKS, "AUTHZ_JWKS_FILE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configureValidGrantsEnv(t, validJWKS)
			t.Setenv(tc.key, tc.value)
			_, loadErr := Load()
			require.Error(t, loadErr)
			require.ErrorContains(t, loadErr, tc.want)
			if tc.key == "SERVICE_TOKENS" {
				// Empty SERVICE_TOKENS triggers service token requirement.
				require.True(t, errors.New(loadErr.Error()) != nil)
			}
		})
	}
}
