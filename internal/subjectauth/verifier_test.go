package subjectauth

import (
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

var fixtureTime = time.Date(2026, time.November, 25, 16, 0, 0, 0, time.UTC)

const fixtureHeader = `{"alg":"RS256","typ":"at+jwt","kid":"first"}`
const fixtureAudience = "quarry-documents"

func fixtureIssuer() string {
	return (&url.URL{Scheme: "https", Host: "issuer.example", Path: "/delegated"}).String()
}

func fixtureClaims(t testing.TB, overrides map[string]any) string {
	t.Helper()
	claims := map[string]any{
		"iss": fixtureIssuer(), "aud": fixtureAudience, "sub": "Synthetic-Subject",
		"client_id": "application", "jti": "token-id", "iat": fixtureTime.Unix(), "exp": fixtureTime.Add(time.Minute).Unix(),
	}
	for field, value := range overrides {
		claims[field] = value
	}
	raw, err := json.Marshal(claims)
	require.NoError(t, err)
	return string(raw)
}

func fixtureToken(t testing.TB, key *rsa.PrivateKey, header, payload string) string {
	t.Helper()
	encode := base64.RawURLEncoding.EncodeToString
	input := encode([]byte(header)) + "." + encode([]byte(payload))
	digest := sha256.Sum256([]byte(input))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	require.NoError(t, err)
	return input + "." + encode(signature)
}

func trustFixture(t testing.TB, expiry time.Time, keys ...jose.JSONWebKey) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{"expires_at": expiry, "keys": keys})
	require.NoError(t, err)
	return data
}

func replaceTrust(t testing.TB, path string, data []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path+".new", data, 0o600))
	require.NoError(t, os.Rename(path+".new", path))
}

func newFixtureVerifier(t testing.TB) (*Verifier, *rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "trust.json")
	replaceTrust(t, path, trustFixture(t, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: key.Public(), KeyID: "first"}))
	verifier, err := NewVerifier(fixtureIssuer(), fixtureAudience, path)
	require.NoError(t, err)
	verifier.now = func() time.Time { return fixtureTime }
	return verifier, key, path
}

func TestSubjectTokenProfile(t *testing.T) {
	verifier, key, _ := newFixtureVerifier(t)
	claims := fixtureClaims(t, nil)
	for _, tc := range []struct{ name, header, claims string }{
		{"issuer", fixtureHeader, fixtureClaims(t, map[string]any{"iss": strings.ToUpper(fixtureIssuer())})},
		{"other audience", fixtureHeader, fixtureClaims(t, map[string]any{"aud": "other"})},
		{"multiple audiences", fixtureHeader, fixtureClaims(t, map[string]any{"aud": []string{fixtureAudience, "other"}})},
		{"empty audience array", fixtureHeader, fixtureClaims(t, map[string]any{"aud": []string{}})},
		{"generic ID token", strings.Replace(fixtureHeader, "at+jwt", "JWT", 1), claims},
		{"missing type", `{"alg":"RS256","kid":"first"}`, claims},
		{"wrong algorithm", strings.Replace(fixtureHeader, "RS256", "PS256", 1), claims},
		{"missing key id", `{"alg":"RS256","typ":"at+jwt"}`, claims},
		{"unknown key id", strings.Replace(fixtureHeader, "first", "other", 1), claims},
		{"duplicate escaped key", `{"alg":"RS256","typ":"at+jwt","kid":"other","k\u0069d":"first"}`, claims},
		{"conflicting header case", `{"alg":"RS256","ALG":"HS256","typ":"at+jwt","kid":"first"}`, claims},
		{"duplicate subject", fixtureHeader, `{"sub":"other",` + claims[1:]},
		{"conflicting claim case", fixtureHeader, `{"SUB":"other",` + claims[1:]},
		{"empty subject", fixtureHeader, fixtureClaims(t, map[string]any{"sub": ""})},
		{"NUL subject", fixtureHeader, fixtureClaims(t, map[string]any{"sub": "synthetic\x00subject"})},
		{"overlong subject", fixtureHeader, fixtureClaims(t, map[string]any{"sub": strings.Repeat("x", 1025)})},
		{"missing token id", fixtureHeader, strings.Replace(claims, `"jti":"token-id",`, "", 1)},
		{"null client id", fixtureHeader, fixtureClaims(t, map[string]any{"client_id": nil})},
		{"nonstring client id", fixtureHeader, fixtureClaims(t, map[string]any{"client_id": 1})},
		{"duplicate nested field", fixtureHeader, `{"ignored":{"id":1,"id":2},` + claims[1:]},
		{"deep claims", fixtureHeader, `{"ignored":` + strings.Repeat("[", 24) + `null` + strings.Repeat("]", 24) + "," + claims[1:]},
		{"deep header", `{"ignored":` + strings.Repeat("[", 24) + `null` + strings.Repeat("]", 24) + "," + fixtureHeader[1:], claims},
		{"malformed claims", fixtureHeader, `{"sub":`},
		{"trailing claims", fixtureHeader, claims + `{}`},
		{"nonobject claims", fixtureHeader, `[]`},
		{"oversized", fixtureHeader, fixtureClaims(t, map[string]any{"ignored": strings.Repeat("x", 8192)})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := verifier.Verify(fixtureToken(t, key, tc.header, tc.claims))
			require.ErrorIs(t, err, ErrInvalidCredential)
		})
	}
	for _, field := range []string{`"jwk":{}`, `"jku":"untrusted"`, `"x5u":"untrusted"`, `"x5c":[]`, `"x5t":"untrusted"`, `"x5t#S256":"untrusted"`, `"crit":["unrecognized"]`, `"crit":[]`, `"b64":false`} {
		t.Run(field, func(t *testing.T) {
			_, err := verifier.Verify(fixtureToken(t, key, "{"+field+","+fixtureHeader[1:], claims))
			require.ErrorIs(t, err, ErrInvalidCredential)
		})
	}
	valid := fixtureToken(t, key, fixtureHeader, claims)
	parts := strings.Split(valid, ".")
	for _, malformed := range []string{"", "a.b", "a.b.c.d", "a=.b.c", valid + "\r\n", parts[0] + "." + parts[1] + ".", parts[0] + "." + parts[1] + ".YWJj"} {
		_, err := verifier.Verify(malformed)
		require.ErrorIs(t, err, ErrInvalidCredential)
	}
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(strings.Replace(fixtureHeader, "RS256", "none", 1))) + "." + parts[1] + "."
	_, err := verifier.Verify(unsigned)
	require.ErrorIs(t, err, ErrInvalidCredential)
	publicKey, err := x509.MarshalPKIXPublicKey(key.Public())
	require.NoError(t, err)
	confused := base64.RawURLEncoding.EncodeToString([]byte(strings.Replace(fixtureHeader, "RS256", "HS256", 1))) + "." + parts[1]
	mac := hmac.New(sha256.New, publicKey)
	_, err = mac.Write([]byte(confused))
	require.NoError(t, err)
	_, err = verifier.Verify(confused + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))
	require.ErrorIs(t, err, ErrInvalidCredential)
	for _, typ := range []string{"at+jwt", "at+JWT", "application/at+jwt"} {
		token := fixtureToken(t, key, strings.Replace(fixtureHeader, "at+jwt", typ, 1), fixtureClaims(t, map[string]any{"aud": []string{fixtureAudience}, "email": "untrusted", "role": "untrusted"}))
		for range 2 {
			got, err := verifier.Verify(token)
			require.NoError(t, err)
			require.Equal(t, fixtureIssuer(), got.Issuer)
			require.Equal(t, "Synthetic-Subject", got.Subject)
			require.Equal(t, fixtureTime.Add(time.Minute), got.ExpiresAt)
		}
	}
}

func TestSubjectTokenClockBoundaries(t *testing.T) {
	verifier, key, _ := newFixtureVerifier(t)
	now := fixtureTime.Unix()
	for _, tc := range []struct {
		name       string
		overrides  map[string]any
		wantReject bool
	}{
		{"maximum lifetime", map[string]any{"exp": now + 300}, false},
		{"excess lifetime", map[string]any{"exp": now + 301}, true},
		{"exact fractional bounds", map[string]any{"iat": json.Number(fmt.Sprintf("%d.000000001", now)), "exp": json.Number(fmt.Sprintf("%d.000000001", now+300))}, false},
		{"fraction over lifetime", map[string]any{"exp": json.Number(fmt.Sprintf("%d.000000001", now+300))}, true},
		{"zero lifetime", map[string]any{"exp": now}, true},
		{"negative lifetime", map[string]any{"exp": now - 1}, true},
		{"future issue within skew", map[string]any{"iat": now + 30}, false},
		{"future issue past skew", map[string]any{"iat": now + 31}, true},
		{"not before within skew", map[string]any{"nbf": now + 30}, false},
		{"not before past skew", map[string]any{"nbf": now + 31}, true},
		{"not before after expiry", map[string]any{"nbf": now + 30, "exp": now + 20}, true},
		{"null not before", map[string]any{"nbf": nil}, true},
		{"null issue time", map[string]any{"iat": nil}, true},
		{"numeric string", map[string]any{"exp": fmt.Sprint(now + 1)}, true},
		{"out of range", map[string]any{"exp": json.Number("1e1000000000")}, true},
		{"exponent notation", map[string]any{"exp": json.Number(fmt.Sprintf("%de-1", (now+60)*10))}, false},
		{"expiry in future", map[string]any{"iat": now - 120, "exp": now + 30}, false},
		{"expiry now within admission skew", map[string]any{"iat": now - 120, "exp": now}, false},
		{"expiry one second ago within admission skew", map[string]any{"iat": now - 120, "exp": now - 1}, false},
		{"expiry at limit", map[string]any{"iat": now - 120, "exp": now - 30}, true},
		{"expiry past limit", map[string]any{"iat": now - 120, "exp": now - 31}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := verifier.Verify(fixtureToken(t, key, fixtureHeader, fixtureClaims(t, tc.overrides)))
			if tc.wantReject {
				require.ErrorIs(t, err, ErrInvalidCredential)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSubjectTrustRotationAndFailures(t *testing.T) {
	verifier, first, path := newFixtureVerifier(t)
	replica, err := NewVerifier(fixtureIssuer(), fixtureAudience, path)
	require.NoError(t, err)
	replica.now = verifier.now
	second, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	oldKey := jose.JSONWebKey{Key: first.Public(), KeyID: "first", Algorithm: "RS256", Use: "sig"}
	newKey := jose.JSONWebKey{Key: second.Public(), KeyID: "second"}
	claims := fixtureClaims(t, nil)
	oldToken := fixtureToken(t, first, fixtureHeader, claims)
	newToken := fixtureToken(t, second, strings.Replace(fixtureHeader, "first", "second", 1), claims)
	expiry := fixtureTime.Add(time.Hour)
	for _, instance := range []*Verifier{verifier, replica} {
		_, verifyErr := instance.Verify(oldToken)
		require.NoError(t, verifyErr)
	}
	replaceTrust(t, path, trustFixture(t, expiry, oldKey, newKey))
	for _, instance := range []*Verifier{verifier, replica} {
		for _, token := range []string{oldToken, newToken} {
			_, verifyErr := instance.Verify(token)
			require.NoError(t, verifyErr)
		}
	}
	replaceTrust(t, path, trustFixture(t, expiry, newKey))
	for _, instance := range []*Verifier{verifier, replica} {
		_, verifyErr := instance.Verify(oldToken)
		require.ErrorIs(t, verifyErr, ErrInvalidCredential)
		_, verifyErr = instance.Verify(newToken)
		require.NoError(t, verifyErr)
	}
	verifier.now = func() time.Time { return expiry }
	_, err = verifier.Verify(newToken)
	require.ErrorIs(t, err, ErrAuthorityUnavailable)
	verifier.now = func() time.Time { return fixtureTime }

	manyKeys := make([]jose.JSONWebKey, 17)
	for i := range manyKeys {
		manyKeys[i] = jose.JSONWebKey{Key: first.Public(), KeyID: fmt.Sprint(i)}
	}
	weak := *first.Public().(*rsa.PublicKey)
	weak.N = new(big.Int).Rsh(weak.N, 1024)
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"missing expiry", []byte(`{"keys":[]}`)},
		{"invalid expiry", []byte(`{"keys":[],"expires_at":"never"}`)},
		{"missing keys", []byte(fmt.Sprintf(`{"expires_at":%q}`, expiry.Format(time.RFC3339)))},
		{"null keys", trustFixture(t, expiry)},
		{"duplicate top-level field", []byte(`{"keys":[],"keys":[]}`)},
		{"conflicting trust fields", []byte(fmt.Sprintf(`{"expires_at":%q,"keys":[],"Keys":[]}`, expiry.Format(time.RFC3339)))},
		{"duplicate kid", trustFixture(t, expiry, oldKey, oldKey)},
		{"private key", trustFixture(t, expiry, jose.JSONWebKey{Key: first, KeyID: "first"})},
		{"symmetric key", trustFixture(t, expiry, jose.JSONWebKey{Key: []byte("synthetic-key"), KeyID: "first"})},
		{"weak RSA key", trustFixture(t, expiry, jose.JSONWebKey{Key: &weak, KeyID: "first"})},
		{"encryption use", trustFixture(t, expiry, jose.JSONWebKey{Key: first.Public(), KeyID: "first", Use: "enc"})},
		{"wrong algorithm", trustFixture(t, expiry, jose.JSONWebKey{Key: first.Public(), KeyID: "first", Algorithm: "PS256"})},
		{"too many keys", trustFixture(t, expiry, manyKeys...)},
		{"too many bytes", []byte(strings.Repeat(" ", 65537))},
		{"malformed", []byte(`{"expires_at":`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replaceTrust(t, path, tc.data)
			_, verifyErr := verifier.Verify(newToken)
			require.ErrorIs(t, verifyErr, ErrAuthorityUnavailable)
		})
	}
	replaceTrust(t, path, trustFixture(t, expiry, newKey))
	require.NoError(t, os.Remove(path))
	_, err = verifier.Verify(newToken)
	require.ErrorIs(t, err, ErrAuthorityUnavailable)
	require.NoError(t, os.Mkdir(path, 0o700))
	_, err = verifier.Verify(newToken)
	require.ErrorIs(t, err, ErrAuthorityUnavailable)
}

func TestSubjectVerifierRequiresConfiguredAuthority(t *testing.T) {
	for _, tc := range []struct{ issuer, audience, file, setting string }{
		{"", fixtureAudience, "keys.json", "AUTHZ_ISSUER"},
		{(&url.URL{Scheme: "http", Host: "issuer.example"}).String(), fixtureAudience, "keys.json", "AUTHZ_ISSUER"},
		{fixtureIssuer() + "?", fixtureAudience, "keys.json", "AUTHZ_ISSUER"},
		{fixtureIssuer(), "", "keys.json", "AUTHZ_AUDIENCE"},
		{fixtureIssuer(), fixtureAudience, "", "AUTHZ_JWKS_FILE"},
	} {
		_, err := NewVerifier(tc.issuer, tc.audience, tc.file)
		require.ErrorContains(t, err, tc.setting)
	}
}

func TestTrustObserverRecordsLoadRotationFailureAndFailsClosed(t *testing.T) {
	first, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	second, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "trust.json")
	expiry := fixtureTime.Add(time.Hour)
	replaceTrust(t, path, trustFixture(t, expiry, jose.JSONWebKey{Key: first.Public(), KeyID: "first"}))

	var events []TrustEvent
	var failAudit bool
	verifier, err := NewVerifierWithObserver(fixtureIssuer(), fixtureAudience, path, func(ev TrustEvent) error {
		events = append(events, ev)
		if failAudit {
			return fmt.Errorf("audit unavailable")
		}
		return nil
	})
	require.NoError(t, err)
	verifier.now = func() time.Time { return fixtureTime }

	require.NoError(t, verifier.ValidateTrust())
	require.Equal(t, []TrustEvent{{Action: "trust_key_load", Outcome: "allow", Reason: "trust_loaded"}}, events)

	token := fixtureToken(t, first, fixtureHeader, fixtureClaims(t, nil))
	_, err = verifier.Verify(token)
	require.NoError(t, err)
	require.Len(t, events, 1, "unchanged digest must not re-emit audit load event")

	// Rotate to second key while audit fails: must reject rotation and not cache it.
	replaceTrust(t, path, trustFixture(t, expiry, jose.JSONWebKey{Key: second.Public(), KeyID: "second"}))
	failAudit = true
	secondHeader := strings.Replace(fixtureHeader, "first", "second", 1)
	secondToken := fixtureToken(t, second, secondHeader, fixtureClaims(t, nil))
	_, err = verifier.Verify(secondToken)
	require.ErrorIs(t, err, ErrAuthorityUnavailable)

	failAudit = false
	_, err = verifier.Verify(secondToken)
	require.NoError(t, err)
	require.Equal(t, TrustEvent{Action: "trust_key_load", Outcome: "allow", Reason: "trust_rotated"}, events[len(events)-1])

	// Remove trust file -> emits trust_unavailable once while unavailable.
	require.NoError(t, os.Remove(path))
	_, err = verifier.Verify(secondToken)
	require.ErrorIs(t, err, ErrAuthorityUnavailable)
	_, err = verifier.Verify(secondToken)
	require.ErrorIs(t, err, ErrAuthorityUnavailable)
	require.Equal(t, TrustEvent{Action: "trust_key_load", Outcome: "error", Reason: "trust_unavailable"}, events[len(events)-1])
	unavailableCount := 0
	for _, ev := range events {
		if ev.Reason == "trust_unavailable" {
			unavailableCount++
		}
	}
	require.Equal(t, 1, unavailableCount)
}

func FuzzSubjectTrustAndClaims(f *testing.F) {
	verifier, key, _ := newFixtureVerifier(f)
	trust := trustFixture(f, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: key.Public(), KeyID: "first"})
	for _, seed := range [][]byte{[]byte(fixtureClaims(f, nil)), trust, []byte(fixtureHeader), []byte(`{"aud":["quarry-documents"],"exp":1e999999}`), []byte(`{"exp":1,"exp":2}`)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		accessTokenKey(raw)
		identity, err := verifier.verifyClaims(raw, fixtureTime)
		if err == nil {
			require.Equal(t, fixtureIssuer(), identity.Issuer)
			require.NotEmpty(t, identity.Subject)
			require.True(t, identity.ExpiresAt.After(fixtureTime.Add(-30*time.Second)))
		}
		snapshot, ok := parseTrust(raw)
		if ok {
			require.LessOrEqual(t, len(snapshot.keys), 16)
			require.False(t, snapshot.expiresAt.IsZero())
		}
	})
}

func FuzzSubjectCredentialParsing(f *testing.F) {
	verifier, key, _ := newFixtureVerifier(f)
	valid := fixtureToken(f, key, fixtureHeader, fixtureClaims(f, nil))
	for _, raw := range []string{valid, "", "x.y.z", valid + "=", strings.Repeat("a", 8193)} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		identity, err := verifier.Verify(raw)
		if err == nil {
			require.Equal(t, valid, raw)
			require.Equal(t, "Synthetic-Subject", identity.Subject)
		} else {
			require.ErrorIs(t, err, ErrInvalidCredential)
		}
	})
}
