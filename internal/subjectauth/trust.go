package subjectauth

import (
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"strings"
	"sync"
	"time"

	"github.com/Victoroide/quarry-rag/internal/credential"
	jose "github.com/go-jose/go-jose/v4"
)

const (
	maxTrustBytes = 64 << 10
	maxTrustKeys  = 16
)

type trustSnapshot struct {
	digest    [sha256.Size]byte
	expiresAt time.Time
	keys      map[string]*rsa.PublicKey
}

type trustFile struct {
	path string
	mu   sync.Mutex
	last trustSnapshot
}

func (f *trustFile) load(now func() time.Time) (trustSnapshot, error) {
	// Read and hash the current file even when the previous snapshot is fresh.
	// A replacement may remove a key without changing size or timestamps.
	data, err := credential.ReadFile(f.path, maxTrustBytes, 0o022)
	if err != nil {
		return trustSnapshot{}, ErrAuthorityUnavailable
	}
	digest := sha256.Sum256(data)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.last.keys == nil || f.last.digest != digest {
		parsed, ok := parseTrust(data)
		if !ok {
			return trustSnapshot{}, ErrAuthorityUnavailable
		}
		parsed.digest = digest
		f.last = parsed
	}
	if !now().Before(f.last.expiresAt) {
		return trustSnapshot{}, ErrAuthorityUnavailable
	}
	return f.last, nil
}

func parseTrust(raw []byte) (trustSnapshot, bool) {
	var document map[string]jsontext.Value
	if err := credential.DecodeObject(raw, &document); err != nil {
		return trustSnapshot{}, false
	}
	for name := range document {
		switch lower := strings.ToLower(name); lower {
		case "keys", "expires_at":
			if name != lower {
				return trustSnapshot{}, false
			}
		}
	}
	var expiresAt time.Time
	var entries []jsontext.Value
	if json.Unmarshal(document["expires_at"], &expiresAt) != nil || expiresAt.IsZero() ||
		document["keys"].Kind() != '[' || json.Unmarshal(document["keys"], &entries) != nil || len(entries) > maxTrustKeys {
		return trustSnapshot{}, false
	}
	keys := make(map[string]*rsa.PublicKey, len(entries))
	for _, entry := range entries {
		kid, key := parseTrustKey(entry)
		if key == nil || keys[kid] != nil {
			return trustSnapshot{}, false
		}
		keys[kid] = key
	}
	return trustSnapshot{expiresAt: expiresAt, keys: keys}, true
}

func parseTrustKey(raw jsontext.Value) (string, *rsa.PublicKey) {
	var fields map[string]jsontext.Value
	if raw.Kind() != '{' || json.Unmarshal(raw, &fields) != nil {
		return "", nil
	}
	for name, value := range fields {
		switch name {
		case "kid", "kty", "n", "e":
		case "use":
			if stringValue(value) != "sig" {
				return "", nil
			}
		case "alg":
			if stringValue(value) != string(jose.RS256) {
				return "", nil
			}
		case "key_ops":
			var methods []string
			if json.Unmarshal(value, &methods) != nil || len(methods) != 1 || methods[0] != "verify" {
				return "", nil
			}
		default:
			return "", nil
		}
	}
	kid := stringValue(fields["kid"])
	if kid == "" || stringValue(fields["kty"]) != "RSA" || !base64UInt(fields["n"]) || !base64UInt(fields["e"]) {
		return "", nil
	}
	var jwk jose.JSONWebKey
	if err := jwk.UnmarshalJSON(raw); err != nil {
		return "", nil
	}
	key, ok := jwk.Key.(*rsa.PublicKey)
	if !ok || key.N.BitLen() < 2048 || key.N.Bit(0) != 1 || key.E < 3 || int64(key.E) >= 1<<31 || key.E%2 != 1 {
		return "", nil
	}
	return kid, key
}

func base64UInt(raw jsontext.Value) bool {
	value := stringValue(raw)
	data, err := base64.RawURLEncoding.Strict().DecodeString(value)
	return err == nil && len(data) > 0 && data[0] != 0 && !strings.ContainsAny(value, "\r\n")
}
