package middleware

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

func TestVerifiedSubjectRechecksTrustAfterAdmission(t *testing.T) {
	const audience, serviceToken = "quarry", "fixture-service"
	issuer := (&url.URL{Scheme: "https", Host: "issuer.example"}).String()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	signing := jose.JSONWebKey{Key: key.Public(), KeyID: "first"}
	unrelated := jose.JSONWebKey{Key: other.Public(), KeyID: "second"}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithType("at+jwt").WithHeader("kid", signing.KeyID))
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "trust.json")
	publish := func(expires time.Time, keys ...jose.JSONWebKey) {
		t.Helper()
		data, marshalErr := json.Marshal(map[string]any{"expires_at": expires, "keys": keys})
		require.NoError(t, marshalErr)
		require.NoError(t, os.WriteFile(path+".new", data, 0o600))
		require.NoError(t, os.Rename(path+".new", path))
	}
	publish(time.Now().Add(time.Hour), signing)
	verifier, err := subjectauth.NewVerifier(issuer, audience, path)
	require.NoError(t, err)
	now := time.Now()
	claims, err := json.Marshal(map[string]any{
		"iss": issuer, "aud": audience, "sub": "fixture-subject", "client_id": "fixture-app", "jti": "fixture-token",
		"iat": now.Unix(), "exp": now.Add(2 * time.Minute).Unix(),
	})
	require.NoError(t, err)
	signed, err := signer.Sign(claims)
	require.NoError(t, err)
	token, err := signed.CompactSerialize()
	require.NoError(t, err)

	var recheck func() error
	next := func(w http.ResponseWriter, r *http.Request) {
		subject, ok := SubjectFromContext(r.Context())
		require.True(t, ok)
		recheck = subject.RecheckTrust
		w.WriteHeader(http.StatusNoContent)
	}
	resolver := subjectLookupFunc(func(context.Context, string, string) (domain.Principal, bool, error) {
		return domain.Principal{ID: "principal", TenantID: "tenant"}, true, nil
	})
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", http.NoBody)
	req.Header.Set("X-Service-Token", serviceToken)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	RequireSubject([]string{serviceToken}, verifier, resolver, next)(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.NotNil(t, recheck)
	require.NoError(t, recheck())

	publish(time.Now().Add(time.Hour), signing, unrelated)
	require.NoError(t, recheck(), "key rotation overlap keeps the token usable")
	publish(time.Now().Add(time.Hour), unrelated)
	require.ErrorIs(t, recheck(), subjectauth.ErrInvalidCredential, "removing the signing key ends later disclosures")
	publish(time.Now().Add(-time.Second), signing)
	require.ErrorIs(t, recheck(), subjectauth.ErrAuthorityUnavailable, "expired trust data cannot authorize")
	require.NoError(t, os.Remove(path))
	require.ErrorIs(t, recheck(), subjectauth.ErrAuthorityUnavailable, "a missing trust file cannot authorize")
}
