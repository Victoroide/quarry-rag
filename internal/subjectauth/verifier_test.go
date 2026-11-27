package subjectauth

import (
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/credential"
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
	dir, err := os.OpenRoot(filepath.Dir(path))
	require.NoError(t, err)
	defer func() { require.NoError(t, dir.Close()) }()
	name := filepath.Base(path)
	require.NoError(t, dir.WriteFile(name+".new", data, 0o600))
	require.NoError(t, dir.Rename(name+".new", name))
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

func awaitTrustResult[T any](t *testing.T, results <-chan T) T {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for trust operation")
		var zero T
		return zero
	}
}

func startTrustCall(call func() error) <-chan error {
	result := make(chan error, 1)
	go func() { result <- call() }()
	return result
}

func observeTrustFixture(t *testing.T, verifier *Verifier) (events <-chan TrustEvent, blockNext func(), release chan<- error) {
	t.Helper()
	seen := make(chan TrustEvent, 16)
	gate := make(chan error, 1)
	var block atomic.Bool
	ctx := t.Context()
	verifier.trust.observer = func(event TrustEvent) error {
		pending := block.Swap(false)
		seen <- event
		if pending {
			select {
			case err := <-gate:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	return seen, func() { block.Store(true) }, gate
}

func requireTrustEvent(t *testing.T, events <-chan TrustEvent, outcome, reason string) {
	t.Helper()
	require.Equal(t, TrustEvent{Action: "trust_key_load", Outcome: outcome, Reason: reason}, awaitTrustResult(t, events))
}

// gateTrustRead pauses after taking a real file sample, before the loader can
// use it. The read count includes any checks that unexpectedly enter the reader.
func gateTrustRead(t *testing.T, verifier *Verifier, readErr error) (
	count *atomic.Int32, blockRead func(int32), entered <-chan struct{}, release func(),
) {
	t.Helper()
	count = new(atomic.Int32)
	var blockedRead atomic.Int32
	sampled, released := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(released) }) }
	t.Cleanup(release)
	verifier.trust.readFile = func(path string, limit int64, denied fs.FileMode) ([]byte, error) {
		attempt := count.Add(1)
		data, err := credential.ReadFile(path, limit, denied)
		if attempt == blockedRead.Load() {
			close(sampled)
			<-released
			if readErr != nil {
				return nil, readErr
			}
		}
		return data, err
	}
	return count, func(offset int32) { blockedRead.Store(count.Load() + offset) }, sampled, release
}

func TestTrustUnchangedReadsDoNotBlockEachOther(t *testing.T) {
	verifier, key, _ := newFixtureVerifier(t)
	events, _, _ := observeTrustFixture(t, verifier)
	reads, blockRead, entered, release := gateTrustRead(t, verifier, nil)
	token := fixtureToken(t, key, fixtureHeader, fixtureClaims(t, nil))
	identity, err := verifier.Verify(token)
	require.NoError(t, err)
	requireTrustEvent(t, events, "allow", "trust_loaded")
	initialReads := reads.Load()
	blockRead(1)

	stalled := startTrustCall(verifier.ValidateTrust)
	awaitTrustResult(t, entered)
	for _, check := range []func() error{
		func() error { _, err := verifier.Verify(token); return err },
		func() error { return verifier.CheckTrust(identity) },
		verifier.ValidateTrust,
	} {
		require.NoError(t, awaitTrustResult(t, startTrustCall(check)))
	}
	require.Equal(t, initialReads+4, reads.Load())
	require.Empty(t, events)

	release()
	require.NoError(t, awaitTrustResult(t, stalled))
	require.Equal(t, initialReads+4, reads.Load())
	require.Empty(t, events)
}

func TestTrustPendingReadDoesNotHoldStateMutex(t *testing.T) {
	for _, tc := range []struct {
		name string
		read int32
	}{
		{"pre-audit", 2},
		{"post-audit", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verifier, key, path := newFixtureVerifier(t)
			sibling, _, _ := newFixtureVerifier(t)
			events, _, _ := observeTrustFixture(t, verifier)
			reads, blockRead, entered, release := gateTrustRead(t, verifier, nil)
			claims := fixtureClaims(t, nil)
			oldToken := fixtureToken(t, key, fixtureHeader, claims)
			identity, err := verifier.Verify(oldToken)
			require.NoError(t, err)
			requireTrustEvent(t, events, "allow", "trust_loaded")
			initialReads := reads.Load()
			blockRead(tc.read)

			replacement, err := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, err)
			newToken := fixtureToken(t, replacement, fixtureHeader, claims)
			replaceTrust(t, path, trustFixture(t, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: replacement.Public(), KeyID: "first"}))
			rotation := startTrustCall(verifier.ValidateTrust)
			awaitTrustResult(t, entered)
			if tc.name == "post-audit" {
				requireTrustEvent(t, events, "allow", "trust_rotated")
			}

			for _, check := range []func() error{
				func() error { _, verifyErr := verifier.Verify(oldToken); return verifyErr },
				func() error { _, verifyErr := verifier.Verify(newToken); return verifyErr },
				func() error { return verifier.CheckTrust(identity) },
				verifier.ValidateTrust,
			} {
				require.ErrorIs(t, awaitTrustResult(t, startTrustCall(check)), ErrAuthorityUnavailable)
			}
			require.NoError(t, awaitTrustResult(t, startTrustCall(sibling.ValidateTrust)))
			require.Equal(t, initialReads+tc.read, reads.Load())
			require.Empty(t, events)
			require.True(t, verifier.trust.last.keys["first"].Equal(key.Public()))

			release()
			require.NoError(t, awaitTrustResult(t, rotation))
			if tc.name == "pre-audit" {
				requireTrustEvent(t, events, "allow", "trust_rotated")
			}
			require.Equal(t, initialReads+3, reads.Load())
			_, err = verifier.Verify(newToken)
			require.NoError(t, err)
			require.ErrorIs(t, verifier.CheckTrust(identity), ErrInvalidCredential)
			require.Empty(t, events)
		})
	}
}

func TestTrustRejectsDelayedReadAcrossTransition(t *testing.T) {
	for _, tc := range []struct {
		name           string
		failAudit      bool
		staleReadError error
	}{
		{name: "published same-kid replacement"},
		{name: "failed audit leaves original published", failAudit: true},
		{name: "stale read error cannot start an episode", failAudit: true, staleReadError: fs.ErrPermission},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verifier, key, path := newFixtureVerifier(t)
			events, blockNext, finishAudit := observeTrustFixture(t, verifier)
			// Open a failure episode before holding a successful old sample in
			// the failed-audit case. A stale success must leave it open.
			_, blockRead, entered, release := gateTrustRead(t, verifier, tc.staleReadError)
			claims := fixtureClaims(t, nil)
			oldToken := fixtureToken(t, key, fixtureHeader, claims)
			identity, err := verifier.Verify(oldToken)
			require.NoError(t, err)
			requireTrustEvent(t, events, "allow", "trust_loaded")
			oldBytes, err := credential.ReadFile(path, maxTrustBytes, 0o022)
			require.NoError(t, err)
			episodeOpen := tc.failAudit && tc.staleReadError == nil
			if episodeOpen {
				require.NoError(t, os.Remove(path))
				require.ErrorIs(t, verifier.ValidateTrust(), ErrAuthorityUnavailable)
				requireTrustEvent(t, events, "error", "trust_unavailable")
				replaceTrust(t, path, oldBytes)
			}

			blockRead(1)
			stale := startTrustCall(func() error { return verifier.CheckTrust(identity) })
			awaitTrustResult(t, entered)
			replacement, err := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, err)
			newToken := fixtureToken(t, replacement, fixtureHeader, claims)
			newBytes := trustFixture(t, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: replacement.Public(), KeyID: "first"})
			require.Len(t, newBytes, len(oldBytes))
			replaceTrust(t, path, newBytes)
			if tc.failAudit {
				blockNext()
				rotation := startTrustCall(verifier.ValidateTrust)
				requireTrustEvent(t, events, "allow", "trust_rotated")
				finishAudit <- context.Canceled
				require.ErrorIs(t, awaitTrustResult(t, rotation), ErrAuthorityUnavailable)
				replaceTrust(t, path, oldBytes)
			} else {
				require.NoError(t, awaitTrustResult(t, startTrustCall(verifier.ValidateTrust)))
				requireTrustEvent(t, events, "allow", "trust_rotated")
			}

			release()
			require.ErrorIs(t, awaitTrustResult(t, stale), ErrAuthorityUnavailable)
			require.Equal(t, episodeOpen, verifier.trust.unavailable)
			require.Empty(t, events)
			if tc.failAudit {
				require.NoError(t, verifier.CheckTrust(identity))
			} else {
				require.ErrorIs(t, verifier.CheckTrust(identity), ErrInvalidCredential)
				_, err = verifier.Verify(newToken)
				require.NoError(t, err)
			}
			require.Empty(t, events)
		})
	}
}

func TestTrustRereadRestoresPublishedSnapshot(t *testing.T) {
	verifier, key, path := newFixtureVerifier(t)
	events, _, _ := observeTrustFixture(t, verifier)
	original, err := credential.ReadFile(path, maxTrustBytes, 0o022)
	require.NoError(t, err)
	restore := false
	verifier.trust.readFile = func(path string, limit int64, denied fs.FileMode) ([]byte, error) {
		data, readErr := credential.ReadFile(path, limit, denied)
		if restore {
			restore = false
			replaceTrust(t, path, original)
		}
		return data, readErr
	}
	token := fixtureToken(t, key, fixtureHeader, fixtureClaims(t, nil))
	identity, err := verifier.Verify(token)
	require.NoError(t, err)
	requireTrustEvent(t, events, "allow", "trust_loaded")
	require.NoError(t, os.Remove(path))
	require.ErrorIs(t, verifier.ValidateTrust(), ErrAuthorityUnavailable)
	requireTrustEvent(t, events, "error", "trust_unavailable")

	empty := make([]jose.JSONWebKey, 0)
	replaceTrust(t, path, trustFixture(t, fixtureTime.Add(time.Hour), empty...))
	restore = true
	require.NoError(t, verifier.CheckTrust(identity))
	require.Empty(t, events)

	require.NoError(t, os.Remove(path))
	require.ErrorIs(t, verifier.ValidateTrust(), ErrAuthorityUnavailable)
	requireTrustEvent(t, events, "error", "trust_unavailable")
}

func TestTrustReadLimitsAndRecovery(t *testing.T) {
	verifier, key, path := newFixtureVerifier(t)
	var denyRead bool
	verifier.trust.readFile = func(path string, limit int64, denied fs.FileMode) ([]byte, error) {
		if denyRead {
			return nil, fs.ErrPermission
		}
		return credential.ReadFile(path, limit, denied)
	}
	events, _, _ := observeTrustFixture(t, verifier)
	token := fixtureToken(t, key, fixtureHeader, fixtureClaims(t, nil))
	identity, err := verifier.Verify(token)
	require.NoError(t, err)
	requireTrustEvent(t, events, "allow", "trust_loaded")

	keys := make([]jose.JSONWebKey, maxTrustKeys+1)
	for i := range keys {
		keys[i] = jose.JSONWebKey{Key: key.Public(), KeyID: fmt.Sprintf("key-%02d", i)}
	}
	expiry := fixtureTime.Add(time.Hour)
	valid := trustFixture(t, expiry, keys[:maxTrustKeys]...)
	atLimit := []byte(strings.Repeat(" ", maxTrustBytes))
	copy(atLimit, valid)
	for _, tc := range []struct {
		name      string
		data      []byte
		deny      bool
		want      error
		newOutage bool
	}{
		{name: "empty key set", data: trustFixture(t, expiry, keys[:0]...)},
		{name: "empty file", want: ErrAuthorityUnavailable, newOutage: true},
		{name: "byte and key limits", data: atLimit},
		{name: "byte limit exceeded", data: append(atLimit, ' '), want: ErrAuthorityUnavailable, newOutage: true},
		{name: "key limit exceeded", data: trustFixture(t, expiry, keys...), want: ErrAuthorityUnavailable},
		{name: "unreadable", data: valid, deny: true, want: ErrAuthorityUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replaceTrust(t, path, tc.data)
			denyRead = tc.deny
			defer func() { denyRead = false }()
			err := verifier.ValidateTrust()
			if tc.want == nil {
				require.NoError(t, err)
				requireTrustEvent(t, events, "allow", "trust_rotated")
				require.ErrorIs(t, verifier.CheckTrust(identity), ErrInvalidCredential)
			} else {
				require.ErrorIs(t, err, tc.want)
				require.ErrorIs(t, verifier.CheckTrust(identity), ErrAuthorityUnavailable)
				if tc.newOutage {
					requireTrustEvent(t, events, "error", "trust_unavailable")
				}
			}
			require.Empty(t, events)
		})
	}
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o700))
	require.ErrorIs(t, verifier.ValidateTrust(), ErrAuthorityUnavailable)
	require.Empty(t, events)
	require.NoError(t, os.Remove(path))
	replaceTrust(t, path, trustFixture(t, expiry, jose.JSONWebKey{Key: key.Public(), KeyID: "first"}))
	require.NoError(t, verifier.CheckTrust(identity))
	requireTrustEvent(t, events, "allow", "trust_rotated")
	require.Empty(t, events)
}

func TestPendingTrustAuditCannotAuthenticate(t *testing.T) {
	replacement, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, kid string
		rotation  bool
		auditErr  error
	}{
		{name: "initial success", kid: "second"},
		{name: "initial error", kid: "second", auditErr: context.DeadlineExceeded},
		{name: "rotation removes a key", kid: "second", rotation: true},
		{name: "rotation replaces a key with the same kid", kid: "first", rotation: true, auditErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verifier, first, path := newFixtureVerifier(t)
			events, blockNext, release := observeTrustFixture(t, verifier)
			oldToken := fixtureToken(t, first, fixtureHeader, fixtureClaims(t, nil))
			newToken := fixtureToken(t, replacement, strings.Replace(fixtureHeader, "first", tc.kid, 1),
				fixtureClaims(t, map[string]any{"sub": "Replacement-Subject"}))
			var prior Identity
			reason := "trust_loaded"
			if tc.rotation {
				var verifyErr error
				prior, verifyErr = verifier.Verify(oldToken)
				require.NoError(t, verifyErr)
				requireTrustEvent(t, events, "allow", "trust_loaded")
				reason = "trust_rotated"
			}
			data := trustFixture(t, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: replacement.Public(), KeyID: tc.kid})
			replaceTrust(t, path, data)
			verifyNew := func() error {
				_, verifyErr := verifier.Verify(newToken)
				return verifyErr
			}
			assertPending := func() {
				checks := []func() error{
					verifyNew,
					func() error { _, verifyErr := verifier.Verify(oldToken); return verifyErr },
					verifier.ValidateTrust,
					func() error { return verifier.CheckTrust(prior) },
				}
				pending := make([]<-chan error, 0, len(checks))
				for _, check := range checks {
					pending = append(pending, startTrustCall(check))
				}
				for _, result := range pending {
					require.ErrorIs(t, awaitTrustResult(t, result), ErrAuthorityUnavailable)
				}
				require.Empty(t, events)
			}

			blockNext()
			result := startTrustCall(verifyNew)
			requireTrustEvent(t, events, "allow", reason)
			assertPending()
			release <- tc.auditErr
			if tc.auditErr != nil {
				require.ErrorIs(t, awaitTrustResult(t, result), ErrAuthorityUnavailable)
				blockNext()
				result = startTrustCall(verifyNew)
				requireTrustEvent(t, events, "allow", reason)
				assertPending()
				release <- nil
			}
			require.NoError(t, awaitTrustResult(t, result))

			identity, verifyErr := verifier.Verify(newToken)
			require.NoError(t, verifyErr)
			require.Equal(t, "Replacement-Subject", identity.Subject)
			require.NoError(t, verifier.CheckTrust(identity))
			require.NoError(t, verifier.ValidateTrust())
			_, verifyErr = verifier.Verify(oldToken)
			require.ErrorIs(t, verifyErr, ErrInvalidCredential)
			require.ErrorIs(t, verifier.CheckTrust(prior), ErrInvalidCredential)
			require.Equal(t, sha256.Sum256(data), verifier.trust.last.digest)
			require.Empty(t, events)
		})
	}
}

func TestTrustTransitionRejectsSupersededOrExpiredCandidate(t *testing.T) {
	t.Run("superseded initial load", func(t *testing.T) {
		verifier, key, path := newFixtureVerifier(t)
		events, blockNext, release := observeTrustFixture(t, verifier)
		claims := fixtureClaims(t, nil)
		oldToken := fixtureToken(t, key, fixtureHeader, claims)
		currentToken := fixtureToken(t, key, strings.Replace(fixtureHeader, "first", "current", 1), claims)
		blockNext()
		result := startTrustCall(func() error { _, err := verifier.Verify(oldToken); return err })
		requireTrustEvent(t, events, "allow", "trust_loaded")
		current := trustFixture(t, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: key.Public(), KeyID: "current"})
		replaceTrust(t, path, current)
		release <- nil
		require.ErrorIs(t, awaitTrustResult(t, result), ErrAuthorityUnavailable)
		require.Nil(t, verifier.trust.last.keys)
		require.Empty(t, events)

		identity, err := verifier.Verify(currentToken)
		require.NoError(t, err)
		require.NoError(t, verifier.CheckTrust(identity))
		_, err = verifier.Verify(oldToken)
		require.ErrorIs(t, err, ErrInvalidCredential)
		requireTrustEvent(t, events, "allow", "trust_loaded")
		require.Equal(t, sha256.Sum256(current), verifier.trust.last.digest)
		require.Empty(t, events)
	})

	t.Run("return to published bytes", func(t *testing.T) {
		verifier, key, path := newFixtureVerifier(t)
		events, blockNext, release := observeTrustFixture(t, verifier)
		prior := trustFixture(t, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: key.Public(), KeyID: "first"})
		replaceTrust(t, path, prior)
		token := fixtureToken(t, key, fixtureHeader, fixtureClaims(t, nil))
		_, err := verifier.Verify(token)
		require.NoError(t, err)
		requireTrustEvent(t, events, "allow", "trust_loaded")
		candidate := trustFixture(t, fixtureTime.Add(time.Hour), []jose.JSONWebKey{}...)
		replaceTrust(t, path, candidate)
		blockNext()
		result := startTrustCall(verifier.ValidateTrust)
		requireTrustEvent(t, events, "allow", "trust_rotated")
		replaceTrust(t, path, prior)
		release <- nil
		require.ErrorIs(t, awaitTrustResult(t, result), ErrAuthorityUnavailable)
		require.Equal(t, sha256.Sum256(prior), verifier.trust.last.digest)
		_, err = verifier.Verify(token)
		require.NoError(t, err)
		require.Empty(t, events)

		replaceTrust(t, path, candidate)
		require.NoError(t, verifier.ValidateTrust())
		requireTrustEvent(t, events, "allow", "trust_rotated")
		require.Equal(t, sha256.Sum256(candidate), verifier.trust.last.digest)
	})

	t.Run("expiry before and during audit", func(t *testing.T) {
		verifier, key, path := newFixtureVerifier(t)
		events, blockNext, release := observeTrustFixture(t, verifier)
		var ticks atomic.Int64
		verifier.now = func() time.Time { return fixtureTime.Add(time.Duration(ticks.Load())) }
		jwk := jose.JSONWebKey{Key: key.Public(), KeyID: "first"}
		token := fixtureToken(t, key, fixtureHeader, fixtureClaims(t, nil))
		verify := func() error { _, err := verifier.Verify(token); return err }
		replaceTrust(t, path, trustFixture(t, fixtureTime, jwk))
		require.ErrorIs(t, verify(), ErrAuthorityUnavailable)
		requireTrustEvent(t, events, "error", "trust_unavailable")
		require.Empty(t, events)

		replaceTrust(t, path, trustFixture(t, fixtureTime.Add(time.Second), jwk))
		blockNext()
		result := startTrustCall(verify)
		requireTrustEvent(t, events, "allow", "trust_loaded")
		ticks.Store(int64(time.Second))
		release <- nil
		require.ErrorIs(t, awaitTrustResult(t, result), ErrAuthorityUnavailable)
		require.ErrorIs(t, verify(), ErrAuthorityUnavailable)
		require.Nil(t, verifier.trust.last.keys)
		require.Empty(t, events, "failed transitions do not close the availability episode")

		replaceTrust(t, path, trustFixture(t, fixtureTime.Add(time.Hour), jwk))
		require.NoError(t, verify())
		requireTrustEvent(t, events, "allow", "trust_loaded")
		require.NoError(t, os.Remove(path))
		require.ErrorIs(t, verify(), ErrAuthorityUnavailable)
		requireTrustEvent(t, events, "error", "trust_unavailable")
	})

	t.Run("nil observer allows fresh file", func(t *testing.T) {
		verifier, key, _ := newFixtureVerifier(t)
		_, err := verifier.Verify(fixtureToken(t, key, fixtureHeader, fixtureClaims(t, nil)))
		require.NoError(t, err)
	})
}

func testTrustTransitionInvalidReread(t *testing.T, invalidate func(*testing.T, string)) {
	t.Helper()
	verifier, key, path := newFixtureVerifier(t)
	events, blockNext, release := observeTrustFixture(t, verifier)
	token := fixtureToken(t, key, fixtureHeader, fixtureClaims(t, nil))
	verify := func() error { _, err := verifier.Verify(token); return err }
	blockNext()
	result := startTrustCall(verify)
	requireTrustEvent(t, events, "allow", "trust_loaded")
	invalidate(t, path)
	release <- nil
	require.ErrorIs(t, awaitTrustResult(t, result), ErrAuthorityUnavailable)
	requireTrustEvent(t, events, "error", "trust_unavailable")
	require.ErrorIs(t, verify(), ErrAuthorityUnavailable)
	require.Nil(t, verifier.trust.last.keys)
	require.Empty(t, events)
	replaceTrust(t, path, trustFixture(t, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: key.Public(), KeyID: "first"}))
	require.NoError(t, verify())
	requireTrustEvent(t, events, "allow", "trust_loaded")
}

func TestTrustTransitionRejectsInvalidReread(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		testTrustTransitionInvalidReread(t, func(t *testing.T, path string) {
			require.NoError(t, os.Remove(path))
		})
	})
	t.Run("malformed", func(t *testing.T) {
		testTrustTransitionInvalidReread(t, func(t *testing.T, path string) {
			replaceTrust(t, path, []byte(`{"keys":`))
		})
	})
}

func TestTrustTransitionRecoversAfterObserverCancellation(t *testing.T) {
	verifier, key, _ := newFixtureVerifier(t)
	events := make(chan TrustEvent, 3)
	reentrant := make(chan error, 3)
	observerErr := context.DeadlineExceeded
	verifier.trust.observer = func(event TrustEvent) error {
		events <- event
		reentrant <- verifier.ValidateTrust()
		return observerErr
	}
	token := fixtureToken(t, key, fixtureHeader, fixtureClaims(t, nil))
	verify := func() error { _, err := verifier.Verify(token); return err }
	require.ErrorIs(t, awaitTrustResult(t, startTrustCall(verify)), ErrAuthorityUnavailable)
	require.ErrorIs(t, awaitTrustResult(t, reentrant), ErrAuthorityUnavailable)
	requireTrustEvent(t, events, "allow", "trust_loaded")
	require.Nil(t, verifier.trust.last.keys)

	observerErr = nil
	require.NoError(t, awaitTrustResult(t, startTrustCall(verify)))
	require.ErrorIs(t, awaitTrustResult(t, reentrant), ErrAuthorityUnavailable)
	requireTrustEvent(t, events, "allow", "trust_loaded")
	require.NoError(t, verify())
	require.Empty(t, events)
}

func TestTrustUnavailableObservationSerializesRecovery(t *testing.T) {
	verifier, key, path := newFixtureVerifier(t)
	events, blockNext, release := observeTrustFixture(t, verifier)
	require.NoError(t, os.Remove(path))
	blockNext()
	result := startTrustCall(verifier.ValidateTrust)
	requireTrustEvent(t, events, "error", "trust_unavailable")
	require.ErrorIs(t, awaitTrustResult(t, startTrustCall(verifier.ValidateTrust)), ErrAuthorityUnavailable)
	replaceTrust(t, path, trustFixture(t, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: key.Public(), KeyID: "first"}))
	require.ErrorIs(t, awaitTrustResult(t, startTrustCall(verifier.ValidateTrust)), ErrAuthorityUnavailable)
	require.Empty(t, events)

	release <- context.DeadlineExceeded
	require.ErrorIs(t, awaitTrustResult(t, result), ErrAuthorityUnavailable)
	require.NoError(t, verifier.ValidateTrust())
	requireTrustEvent(t, events, "allow", "trust_loaded")
}

func TestTrustTransitionRecoversAfterObserverPanic(t *testing.T) {
	verifier, _, _ := newFixtureVerifier(t)
	fail := true
	verifier.trust.observer = func(TrustEvent) error {
		if fail {
			panic("observer failure")
		}
		return nil
	}
	require.PanicsWithValue(t, "observer failure", func() { _ = verifier.ValidateTrust() })
	require.Nil(t, verifier.trust.last.keys)
	fail = false
	require.NoError(t, awaitTrustResult(t, startTrustCall(verifier.ValidateTrust)))
}

func FuzzSubjectTrustAndClaims(f *testing.F) {
	verifier, key, _ := newFixtureVerifier(f)
	trust := trustFixture(f, fixtureTime.Add(time.Hour), jose.JSONWebKey{Key: key.Public(), KeyID: "first"})
	for _, seed := range [][]byte{
		[]byte(fixtureClaims(f, nil)), trust, []byte(fixtureHeader),
		[]byte(`{"aud":["quarry-documents"],"exp":1e999999}`), []byte(`{"exp":1,"exp":2}`),
		[]byte(`{"keys":[`),
		[]byte(`{"keys":[],"keys":[],"expires_at":"2026-11-25T17:00:00Z"}`),
		[]byte(`{"keys":{},"expires_at":"2026-11-25T17:00:00Z"}`),
	} {
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
