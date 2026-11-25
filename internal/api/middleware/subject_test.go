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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubjectTrustChangesDuringPrincipalLookup(t *testing.T) {
	const audience, serviceToken = "quarry", "fixture-service"
	issuer := (&url.URL{Scheme: "https", Host: "issuer.example"}).String()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	replacement, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	oldKey := jose.JSONWebKey{Key: key.Public(), KeyID: "first"}
	newKey := jose.JSONWebKey{Key: replacement.Public(), KeyID: "second"}
	sameID := jose.JSONWebKey{Key: replacement.Public(), KeyID: oldKey.KeyID}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithType("at+jwt").WithHeader("kid", oldKey.KeyID))
	require.NoError(t, err)

	for _, tc := range []struct {
		name         string
		replacement  []jose.JSONWebKey
		wantStatus   int
		wantResponse string
	}{
		{"expires", nil, http.StatusServiceUnavailable, `{"error":"unavailable","message":"authorization unavailable"}`},
		{"removes signing key", []jose.JSONWebKey{newKey}, http.StatusUnauthorized, `{"error":"unauthorized","message":"invalid or missing credentials"}`},
		{"replaces signing key under the same ID", []jose.JSONWebKey{sameID}, http.StatusUnauthorized, `{"error":"unauthorized","message":"invalid or missing credentials"}`},
		{"adds signing key during overlap", []jose.JSONWebKey{oldKey, newKey}, http.StatusNoContent, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "trust.json")
			replace := func(expires time.Time, keys ...jose.JSONWebKey) {
				t.Helper()
				data, err := json.Marshal(map[string]any{"expires_at": expires, "keys": keys})
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path+".new", data, 0o600))
				require.NoError(t, os.Rename(path+".new", path))
			}
			verifier, err := subjectauth.NewVerifier(issuer, audience, path)
			require.NoError(t, err)
			now := time.Now()
			tokenExpiry := now.Add(2 * time.Minute).Truncate(time.Second)
			claims, err := json.Marshal(map[string]any{
				"iss": issuer, "aud": audience, "sub": "fixture-subject",
				"client_id": "fixture-app", "jti": "fixture-token", "iat": now.Unix(), "exp": tokenExpiry.Unix(),
			})
			require.NoError(t, err)
			signed, err := signer.Sign(claims)
			require.NoError(t, err)
			token, err := signed.CompactSerialize()
			require.NoError(t, err)

			trustExpiry := time.Now().Add(2 * time.Second)
			replace(trustExpiry, oldKey)
			lookups, dispatches := 0, 0
			resolver := subjectLookupFunc(func(ctx context.Context, iss, sub string) (domain.Principal, bool, error) {
				lookups++
				require.Equal(t, issuer, iss)
				require.Equal(t, "fixture-subject", sub)
				if tc.replacement == nil {
					require.True(t, time.Now().Before(trustExpiry))
					time.Sleep(time.Until(trustExpiry))
				} else {
					replace(time.Now().Add(time.Hour), tc.replacement...)
				}
				require.NoError(t, ctx.Err())
				return domain.Principal{ID: "fixture-principal", TenantID: "fixture-tenant"}, true, nil
			})
			next := func(w http.ResponseWriter, _ *http.Request) {
				dispatches++
				w.WriteHeader(http.StatusNoContent)
			}
			w := httptest.NewRecorder()
			r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents", http.NoBody)
			r.Header.Set("X-Service-Token", serviceToken)
			r.Header.Set("Authorization", "Bearer "+token)
			RequireSubject([]string{serviceToken}, verifier, resolver, next)(w, r)
			require.Equal(t, 1, lookups)
			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantResponse == "" {
				assert.Equal(t, 1, dispatches)
			} else {
				assert.Zero(t, dispatches)
				assert.JSONEq(t, tc.wantResponse, w.Body.String())
			}
		})
	}
}
