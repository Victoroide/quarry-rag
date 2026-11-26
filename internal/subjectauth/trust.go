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
	path        string
	observer    TrustObserver
	mu          sync.Mutex
	last        trustSnapshot
	unavailable bool
}

func (f *trustFile) load(now func() time.Time) (trustSnapshot, error) {
	// Read and hash the current file even when the previous snapshot is fresh.
	// A replacement may remove a key without changing size or timestamps.
	data, err := credential.ReadFile(f.path, maxTrustBytes, 0o022)
	if err != nil {
		f.notifyUnavailable()
		return trustSnapshot{}, ErrAuthorityUnavailable
	}
	digest := sha256.Sum256(data)
	var event *TrustEvent
	var prev trustSnapshot
	f.mu.Lock()
	if f.last.keys == nil || f.last.digest != digest {
		parsed, ok := parseTrust(data)
		if !ok {
			alreadyUnavailable := f.unavailable
			f.unavailable = true
			f.mu.Unlock()
			if !alreadyUnavailable {
				_ = f.emit(TrustEvent{Action: "trust_key_load", Outcome: "error", Reason: "trust_unavailable"})
			}
			return trustSnapshot{}, ErrAuthorityUnavailable
		}
		reason := "trust_loaded"
		if f.last.keys != nil {
			reason = "trust_rotated"
		}
		prev = f.last
		parsed.digest = digest
		f.last = parsed
		event = &TrustEvent{Action: "trust_key_load", Outcome: "allow", Reason: reason}
	}
	if !now().Before(f.last.expiresAt) {
		alreadyUnavailable := f.unavailable
		f.unavailable = true
		f.mu.Unlock()
		if !alreadyUnavailable {
			_ = f.emit(TrustEvent{Action: "trust_key_load", Outcome: "error", Reason: "trust_unavailable"})
		}
		return trustSnapshot{}, ErrAuthorityUnavailable
	}
	f.unavailable = false
	snap := f.last
	f.mu.Unlock()
	if event != nil {
		if obsErr := f.emit(*event); obsErr != nil {
			// Restore the prior snapshot so the rotation is not accepted until
			// the audit event succeeds.
			f.mu.Lock()
			if f.last.digest == snap.digest {
				f.last = prev
			}
			f.mu.Unlock()
			return trustSnapshot{}, ErrAuthorityUnavailable
		}
	}
	return snap, nil
}

func (f *trustFile) notifyUnavailable() {
	f.mu.Lock()
	already := f.unavailable
	f.unavailable = true
	f.mu.Unlock()
	if !already {
		_ = f.emit(TrustEvent{Action: "trust_key_load", Outcome: "error", Reason: "trust_unavailable"})
	}
}

func (f *trustFile) emit(event TrustEvent) error {
	if f.observer == nil {
		return nil
	}
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
