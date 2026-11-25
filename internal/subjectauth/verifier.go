// Package subjectauth verifies delegated access tokens against local trust data.
package subjectauth

import (
	"crypto/rsa"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/credential"
	jose "github.com/go-jose/go-jose/v4"
)

const (
	maxIdentityBytes = 1024 // Matches the grant catalog issuer and subject columns.
	maxClockSkew     = 30 * time.Second
	maxTokenLifetime = 5 * time.Minute
)

// ErrInvalidCredential indicates that the supplied access token failed verification.
var ErrInvalidCredential = errors.New("invalid subject credential")

// ErrAuthorityUnavailable indicates that local trust data cannot be used.
var ErrAuthorityUnavailable = errors.New("subject authority unavailable")

// Identity is the verified issuer, foreign subject and exact expiry used to look
// up the internal request principal. It must never be logged or sent to callers.
type Identity struct {
	Issuer    string
	Subject   string
	ExpiresAt time.Time

	keyID      string
	signingKey *rsa.PublicKey
}

// Verifier verifies access tokens for one issuer and one audience. It is safe
// for concurrent use and performs no issuer discovery or network key retrieval.
type Verifier struct {
	issuer, audience string
	trust            trustFile
	now              func() time.Time
}

// NewVerifier validates the authority settings. Trust is loaded at verification
// time so an unavailable or replaced file is reflected at each admission.
func NewVerifier(issuer, audience, path string) (*Verifier, error) {
	u, err := url.Parse(issuer)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || strings.ContainsAny(issuer, "?#") || len(issuer) > maxIdentityBytes {
		return nil, errors.New("AUTHZ_ISSUER must be an HTTPS issuer without credentials, query or fragment")
	}
	if strings.TrimSpace(audience) == "" {
		return nil, errors.New("AUTHZ_AUDIENCE is required")
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("AUTHZ_JWKS_FILE is required")
	}
	return &Verifier{issuer: issuer, audience: audience, trust: trustFile{path: path}, now: time.Now}, nil
}

// Verify authenticates a single compact bearer token. The caller checks the
// returned expiry without clock skew immediately before any disclosure.
func (v *Verifier) Verify(token string) (Identity, error) {
	header, _, ok := credential.CompactJWT(token)
	if !ok {
		return Identity{}, ErrInvalidCredential
	}
	kid, ok := accessTokenKey(header)
	if !ok {
		return Identity{}, ErrInvalidCredential
	}
	jws, err := jose.ParseSignedCompact(token, []jose.SignatureAlgorithm{jose.RS256})
	if err != nil {
		return Identity{}, ErrInvalidCredential
	}
	trust, err := v.trust.load(v.now)
	if err != nil {
		return Identity{}, err
	}
	key, ok := trust.keys[kid]
	if !ok {
		return Identity{}, ErrInvalidCredential
	}
	payload, err := jws.Verify(key)
	now := v.now()
	if !now.Before(trust.expiresAt) {
		return Identity{}, ErrAuthorityUnavailable
	}
	if err != nil {
		return Identity{}, ErrInvalidCredential
	}
	identity, err := v.verifyClaims(payload, now)
	if err != nil {
		return Identity{}, err
	}
	identity.keyID = kid
	identity.signingKey = key
	return identity, nil
}

// CheckTrust confirms that an identity returned by Verify was signed by a key
// still present in the current trust file. Call it after the principal lookup.
func (v *Verifier) CheckTrust(identity Identity) error {
	trust, err := v.trust.load(v.now)
	if err != nil {
		return err
	}
	key := trust.keys[identity.keyID]
	if identity.signingKey == nil || key == nil || !identity.signingKey.Equal(key) {
		return ErrInvalidCredential
	}
	return nil
}

func accessTokenKey(raw []byte) (string, bool) {
	var fields map[string]jsontext.Value
	if err := credential.DecodeObject(raw, &fields); err != nil {
		return "", false
	}
	for name := range fields {
		switch lower := strings.ToLower(name); lower {
		case "jwk", "jku", "x5u", "x5c", "x5t", "x5t#s256", "crit", "b64":
			return "", false
		case "typ", "alg", "kid":
			if name != lower {
				return "", false
			}
		}
	}
	typ := stringValue(fields["typ"])
	kid := stringValue(fields["kid"])
	return kid, kid != "" && stringValue(fields["alg"]) == string(jose.RS256) &&
		(strings.EqualFold(typ, "at+jwt") || strings.EqualFold(typ, "application/at+jwt"))
}

func (v *Verifier) verifyClaims(raw []byte, now time.Time) (Identity, error) {
	var fields map[string]jsontext.Value
	if err := credential.DecodeObject(raw, &fields); err != nil {
		return Identity{}, ErrInvalidCredential
	}
	for name := range fields {
		switch lower := strings.ToLower(name); lower {
		case "iss", "sub", "aud", "client_id", "jti", "iat", "exp", "nbf":
			if name != lower {
				return Identity{}, ErrInvalidCredential
			}
		}
	}
	issuer, subject := stringValue(fields["iss"]), stringValue(fields["sub"])
	if issuer != v.issuer || !matchesAudience(fields["aud"], v.audience) ||
		subject == "" || len(subject) > maxIdentityBytes || strings.ContainsRune(subject, 0) ||
		stringValue(fields["client_id"]) == "" || stringValue(fields["jti"]) == "" {
		return Identity{}, ErrInvalidCredential
	}
	issued, hasIssue := credential.NumericDate(fields["iat"])
	expires, hasExpiry := credential.NumericDate(fields["exp"])
	if !hasIssue || !hasExpiry || !expires.After(issued) || expires.Sub(issued) > maxTokenLifetime ||
		issued.After(now.Add(maxClockSkew)) || !now.Before(expires.Add(maxClockSkew)) {
		return Identity{}, ErrInvalidCredential
	}
	if value, exists := fields["nbf"]; exists {
		notBefore, ok := credential.NumericDate(value)
		if !ok || notBefore.After(now.Add(maxClockSkew)) || !notBefore.Before(expires) {
			return Identity{}, ErrInvalidCredential
		}
	}
	return Identity{Issuer: issuer, Subject: subject, ExpiresAt: expires}, nil
}

func matchesAudience(raw jsontext.Value, expected string) bool {
	if raw.Kind() == '"' {
		return stringValue(raw) == expected
	}
	var audience []string
	return json.Unmarshal(raw, &audience) == nil && len(audience) == 1 && audience[0] == expected
}

func stringValue(raw jsontext.Value) string {
	var value string
	if raw.Kind() != '"' || json.Unmarshal(raw, &value) != nil || strings.TrimSpace(value) == "" {
		return ""
	}
	return value
}
