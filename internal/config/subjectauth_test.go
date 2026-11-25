package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubjectAuthoritySettingsDoNotActivateGrants(t *testing.T) {
	awsConfigEnvironment(t)
	for _, key := range []string{"AUTHZ_MODE", "AUTHZ_ISSUER", "AUTHZ_AUDIENCE", "AUTHZ_JWKS_FILE"} {
		t.Setenv(key, "")
	}
	cfg, err := Load()
	require.NoError(t, err)
	require.Zero(t, cfg.SubjectAuthority)

	t.Setenv("AUTHZ_MODE", "service")
	t.Setenv("AUTHZ_ISSUER", "staged-issuer")
	t.Setenv("AUTHZ_AUDIENCE", "staged-audience")
	t.Setenv("AUTHZ_JWKS_FILE", "unmounted-trust")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, SubjectAuthorityConfig{Issuer: "staged-issuer", Audience: "staged-audience", JWKSFile: "unmounted-trust"}, cfg.SubjectAuthority)

	t.Setenv("AUTHZ_MODE", "grants")
	_, err = Load()
	require.ErrorContains(t, err, "AUTHZ_MODE")
}
