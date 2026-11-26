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

// TrustEvent describes a trust-file load, key rotation, or availability failure
// without exposing key material or file paths.
type TrustEvent struct {
	Action  string
	Outcome string
	Reason  string
}

// TrustObserver records trust-file transitions. Returning an error on a key
// load or rotation rejects the transition so unaudited keys cannot be used.
type TrustObserver func(TrustEvent) error

type trustFile struct {
	path                 string
	observer             TrustObserver
	mu                   sync.Mutex
	last                 trustSnapshot
	unavailable          bool
	transitionInProgress bool
}

func (f *trustFile) load(now func() time.Time) (trustSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.transitionInProgress {
		return trustSnapshot{}, ErrAuthorityUnavailable
	}
	candidate, ok := f.readFresh(now, f.last)
	if !ok {
		f.notifyUnavailable()
		return trustSnapshot{}, ErrAuthorityUnavailable
	}
	if f.last.keys == nil || candidate.digest != f.last.digest {
		reason := "trust_loaded"
		if f.last.keys != nil {
			reason = "trust_rotated"
		}
		if err := f.observe(TrustEvent{Action: "trust_key_load", Outcome: "allow", Reason: reason}); err != nil {
			return trustSnapshot{}, ErrAuthorityUnavailable
		}

		// The file may have been replaced or expired while the audit ran.
		current, ok := f.readFresh(now, candidate)
		if !ok {
			f.notifyUnavailable()
			return trustSnapshot{}, ErrAuthorityUnavailable
		}
		if current.digest != candidate.digest {
			return trustSnapshot{}, ErrAuthorityUnavailable
		}
		f.last = candidate
	}
	f.unavailable = false
	return f.last, nil
}

// readFresh runs under mu so a caller cannot publish an older read after a
// newer one. Every read checks the file policy, even on a digest match.
func (f *trustFile) readFresh(now func() time.Time, cached trustSnapshot) (trustSnapshot, bool) {
	data, err := credential.ReadFile(f.path, maxTrustBytes, 0o022)
	if err != nil {
		return trustSnapshot{}, false
	}
	digest := sha256.Sum256(data)
	if cached.keys != nil && cached.digest == digest {
		return cached, now().Before(cached.expiresAt)
	}
	parsed, ok := parseTrust(data)
	parsed.digest = digest
	return parsed, ok && now().Before(parsed.expiresAt)
}

// notifyUnavailable records one best-effort event until a successful load.
func (f *trustFile) notifyUnavailable() {
	if !f.unavailable {
		f.unavailable = true
		_ = f.observe(TrustEvent{Action: "trust_key_load", Outcome: "error", Reason: "trust_unavailable"})
	}
}

// observe releases mu during the callback and returns with it held, keeping
// other loads out of the transition without waiting for the audit write.
func (f *trustFile) observe(event TrustEvent) error {
	if f.observer == nil {
		return nil
	}
	f.transitionInProgress = true
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.transitionInProgress = false
	}()
	return f.observer(event)
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
