// Package embeddinginput validates inputs against a locally configured embedding model profile.
package embeddinginput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ErrInputLimit indicates that input exceeds a configured local allowance.
var ErrInputLimit = errors.New("embedding input exceeds profile limit")

var errAllowance = errors.New("embedding input allowance must be a positive integer")

const maxProfileBytes = 16 << 10

// Target identifies the configured embedding endpoint and model.
type Target struct {
	Provider string
	Model    string
	BaseURL  string
}

type allowance int

func (a *allowance) UnmarshalJSON(raw []byte) error {
	var value int
	if err := json.Unmarshal(raw, &value); err != nil || value <= 0 {
		return errAllowance
	}
	*a = allowance(value)
	return nil
}

type tokenizerSpec struct {
	Type     string `json:"type"`
	Version  string `json:"version"`
	Digest   string `json:"sha256"`
	Artifact string `json:"artifact"`
}

type profileSpec struct {
	Provider      string         `json:"provider"`
	Model         string         `json:"model"`
	Revision      string         `json:"revision"`
	MaxBytes      allowance      `json:"max_bytes,omitempty"`
	MaxCharacters allowance      `json:"max_characters,omitempty"`
	MaxTokens     allowance      `json:"max_tokens,omitempty"`
	Tokenizer     *tokenizerSpec `json:"tokenizer,omitempty"`
}

// Profile is immutable and safe for concurrent validation.
type Profile struct {
	maxBytes, maxCharacters, maxTokens int
	tokenizer                          *cl100kTokenizer
	fingerprint                        string
	chunkingFingerprint                string
}

// Usage carries an exact token count when a verified tokenizer ran.
type Usage struct {
	ExactTokens *int
}

// Load validates the declared profile and any tokenizer artifact without network calls.
// An empty path disables the profile.
func Load(path string, target Target) (*Profile, error) {
	if path == "" {
		return nil, nil
	}
	data, err := readLocalFile(path, maxProfileBytes)
	if err != nil {
		return nil, fmt.Errorf("read embedding input profile: %w", err)
	}
	spec, err := decodeProfile(data)
	if err != nil {
		return nil, err
	}
	if spec.Provider != target.Provider || spec.Model == "" || spec.Model != target.Model {
		return nil, errors.New("profile provider or model does not match embedding configuration")
	}
	if spec.Revision == "" || strings.ContainsFunc(spec.Revision, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) {
		return nil, errors.New("profile requires a model revision without whitespace or control characters")
	}
	profile := &Profile{maxBytes: int(spec.MaxBytes), maxCharacters: int(spec.MaxCharacters), maxTokens: int(spec.MaxTokens)}
	if spec.Model == "amazon.titan-embed-text-v2:0" && spec.Provider == "bedrock" {
		const titanCharacters = 50_000
		if spec.Revision != "0" || spec.MaxCharacters > titanCharacters {
			return nil, errors.New("the Titan Text Embeddings V2 profile requires revision 0 and a character allowance at most 50000")
		}
		if profile.maxCharacters == 0 {
			profile.maxCharacters = titanCharacters
		}
	}
	if spec.Tokenizer != nil || spec.MaxTokens != 0 {
		profile.tokenizer, err = loadExactTokenizer(path, spec, target)
		if err != nil {
			return nil, err
		}
	} else if spec.MaxCharacters == 0 && spec.MaxBytes == 0 {
		return nil, errors.New("profile requires at least one input allowance")
	}

	// The artifact path may move without changing the tokenizer or chunk IDs.
	spec.MaxCharacters = allowance(profile.maxCharacters)
	if spec.Tokenizer != nil {
		spec.Tokenizer.Artifact = ""
	}
	canonical, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("encode embedding input profile: %w", err)
	}
	if profile.tokenizer != nil {
		canonical = append(append(canonical, 0), cl100kSplitVersion...)
	}
	digest := sha256.Sum256(canonical)
	profile.fingerprint = hex.EncodeToString(digest[:])
	// Chunk boundaries depend on the allowances and tokenizer, independent of
	// the name of the embedding model that uses them.
	chunking := sha256.New()
	_, _ = fmt.Fprintf(chunking, "%d/%d/%d", profile.maxBytes, profile.maxCharacters, profile.maxTokens)
	if spec.Tokenizer != nil {
		_, _ = fmt.Fprintf(chunking, "/%q/%q/%q/%q", spec.Tokenizer.Type, spec.Tokenizer.Version, spec.Tokenizer.Digest, cl100kSplitVersion)
	}
	profile.chunkingFingerprint = hex.EncodeToString(chunking.Sum(nil))
	return profile, nil
}

func decodeProfile(data []byte) (profileSpec, error) {
	var spec profileSpec
	if !utf8.Valid(data) {
		return spec, errors.New("invalid profile JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&spec)
	if errors.Is(err, errAllowance) {
		return spec, err
	}
	if err != nil || bytes.Equal(bytes.TrimSpace(data), []byte("null")) || decoder.Decode(new(json.RawMessage)) != io.EOF {
		return spec, errors.New("invalid profile JSON")
	}
	return spec, nil
}

func readLocalFile(path string, limit int64) ([]byte, error) {
	cleanPath := filepath.Clean(path)
	info, err := os.Stat(cleanPath)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("expected a readable, regular local file")
	}
	file, err := os.Open(cleanPath)
	if err != nil {
		return nil, errors.New("cannot open local file")
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errors.New("cannot read local file within its size limit")
	}
	return data, nil
}

// Fingerprint identifies the model revision, allowances and tokenizer version and digest.
func (p *Profile) Fingerprint() string { return p.fingerprint }

// ChunkingFingerprint identifies the input limits and tokenizer that affect chunk boundaries.
func (p *Profile) ChunkingFingerprint() string { return p.chunkingFingerprint }

// SplitLimits supplies necessary byte and character bounds for candidate chunks.
// Zero means unbounded. Exact token limits also bound bytes by vocabulary width.
func (p *Profile) SplitLimits() (byteLimit, characterLimit int) {
	byteLimit = p.maxBytes
	if p.tokenizer != nil {
		tokenBytes := p.maxTokens * p.tokenizer.maxTokenBytes
		if byteLimit == 0 || tokenBytes < byteLimit {
			byteLimit = tokenBytes
		}
	}
	return byteLimit, p.maxCharacters
}

// Check validates the complete text that would be sent to the embedding endpoint.
// A nil profile only checks for cancellation.
func (p *Profile) Check(ctx context.Context, text string) (Usage, error) {
	if err := p.checkText(ctx, text); err != nil || p == nil || p.tokenizer == nil {
		return Usage{}, err
	}
	count, err := p.tokenizer.count(ctx, text, 0)
	if err != nil {
		return Usage{}, err
	}
	usage := Usage{ExactTokens: &count}
	if count > p.maxTokens {
		return usage, ErrInputLimit
	}
	return usage, nil
}

// Allows checks a candidate without counting tokens after its limit is exceeded.
func (p *Profile) Allows(ctx context.Context, text string) error {
	if err := p.checkText(ctx, text); err != nil || p == nil || p.tokenizer == nil {
		return err
	}
	if p.maxTokens == 1 && text != "" {
		if _, ok := p.tokenizer.ranks[text]; !ok {
			return ErrInputLimit
		}
		n, err := cl100kPiece(ctx, text)
		if err != nil {
			return err
		}
		if n != len(text) {
			return ErrInputLimit
		}
		return nil
	}
	count, err := p.tokenizer.count(ctx, text, p.maxTokens)
	if err != nil {
		return err
	}
	if count > p.maxTokens {
		return ErrInputLimit
	}
	return nil
}

func (p *Profile) checkText(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil || p == nil {
		return err
	}
	if p.maxBytes > 0 && len(text) > p.maxBytes {
		return ErrInputLimit
	}
	if !utf8.ValidString(text) {
		return errors.New("embedding input is not valid UTF-8")
	}
	if p.maxCharacters > 0 && utf8.RuneCountInString(text) > p.maxCharacters {
		return ErrInputLimit
	}
	return ctx.Err()
}

func artifactPath(profilePath, artifact string) string {
	if filepath.IsAbs(artifact) {
		return artifact
	}
	return filepath.Join(filepath.Dir(profilePath), artifact)
}
