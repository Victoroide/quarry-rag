package subjectauth

import (
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"io/fs"
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
	readFile             func(string, int64, fs.FileMode) ([]byte, error)
	mu                   sync.Mutex
	last                 trustSnapshot
	unavailable          bool
	transitionInProgress bool
	generation           uint64
}

func (f *trustFile) load(now func() time.Time) (trustSnapshot, error) {
	f.mu.Lock()
	if f.transitionInProgress {
		f.mu.Unlock()
		return trustSnapshot{}, ErrAuthorityUnavailable
	}
	generation := f.generation
	f.mu.Unlock()

	_, digest, err := f.readTrustBytes()

	f.mu.Lock()
	// A completed transition also invalidates samples taken before it began.
	if f.transitionInProgress || generation != f.generation {
		f.mu.Unlock()
		return trustSnapshot{}, ErrAuthorityUnavailable
	}
	if err != nil {
		return f.failUnavailableLocked()
	}
	last := f.last
	if last.keys != nil && digest == last.digest {
		if !now().Before(last.expiresAt) {
			return f.failUnavailableLocked()
		}
		f.unavailable = false
		f.mu.Unlock()
		return last, nil
	}

	f.reserveTransitionLocked()
	f.mu.Unlock()
	return f.loadTransition(now, last)
}

// loadTransition owns the reservation through both rereads and the audit.
func (f *trustFile) loadTransition(now func() time.Time, last trustSnapshot) (trustSnapshot, error) {
	defer f.releaseTransition()

	candidate, ok := f.freshTrust(now, last)
	if !ok {
		f.mu.Lock()
		return f.failUnavailableLocked()
	}
	if last.keys != nil && candidate.digest == last.digest {
		return f.publishFresh(now, last)
	}

	reason := "trust_loaded"
	if last.keys != nil {
		reason = "trust_rotated"
	}
	if f.observer != nil {
		if err := f.observer(TrustEvent{Action: "trust_key_load", Outcome: "allow", Reason: reason}); err != nil {
			return trustSnapshot{}, ErrAuthorityUnavailable
		}
	}

	current, ok := f.freshTrust(now, candidate)
	if !ok {
		f.mu.Lock()
		return f.failUnavailableLocked()
	}
	if current.digest != candidate.digest {
		return trustSnapshot{}, ErrAuthorityUnavailable
	}
	return f.publishFresh(now, current)
}

func (f *trustFile) publishFresh(now func() time.Time, snapshot trustSnapshot) (trustSnapshot, error) {
	f.mu.Lock()
	if !now().Before(snapshot.expiresAt) {
		return f.failUnavailableLocked()
	}
	f.last = snapshot
	f.unavailable = false
	f.mu.Unlock()
	return snapshot, nil
}

// readTrustBytes checks the file policy on every read, including cache hits.
func (f *trustFile) readTrustBytes() ([]byte, [sha256.Size]byte, error) {
	readFile := f.readFile
	if readFile == nil {
		readFile = credential.ReadFile
	}
	data, err := readFile(f.path, maxTrustBytes, 0o022)
	if err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	return data, sha256.Sum256(data), nil
}

func (f *trustFile) freshTrust(now func() time.Time, cached trustSnapshot) (trustSnapshot, bool) {
	data, digest, err := f.readTrustBytes()
	if err != nil {
		return trustSnapshot{}, false
	}
	if cached.keys != nil && cached.digest == digest {
		return cached, now().Before(cached.expiresAt)
	}
	parsed, ok := parseTrust(data)
	parsed.digest = digest
	return parsed, ok && now().Before(parsed.expiresAt)
}

// failUnavailableLocked releases mu and records one best-effort event per
// episode. A transition caller already owns the reservation.
func (f *trustFile) failUnavailableLocked() (trustSnapshot, error) {
	notify := !f.unavailable && f.observer != nil
	f.unavailable = true
	if notify && !f.transitionInProgress {
		f.reserveTransitionLocked()
		defer f.releaseTransition()
	}
	f.mu.Unlock()
	if notify {
		_ = f.observer(TrustEvent{Action: "trust_key_load", Outcome: "error", Reason: "trust_unavailable"})
	}
	return trustSnapshot{}, ErrAuthorityUnavailable
}

func (f *trustFile) reserveTransitionLocked() {
	f.transitionInProgress = true
	f.generation++
}

func (f *trustFile) releaseTransition() {
	f.mu.Lock()
	f.transitionInProgress = false
	f.mu.Unlock()
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
