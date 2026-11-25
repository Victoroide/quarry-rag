package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Embedding provider and distance metric names recorded in a profile.
const (
	EmbeddingProviderBedrock = "bedrock"
	EmbeddingProviderOpenAI  = "openai"
	MetricCosine             = "cosine"
)

// LegacyGeneration names the vector target used before generations existed:
// the fixed chunk_vectors table or the configured Pinecone index.
const LegacyGeneration = "legacy"

var generationName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,23}$`)

// ValidGenerationName reports whether name can identify a vector generation
// and, for PostgreSQL, name its table.
func ValidGenerationName(name string) bool { return generationName.MatchString(name) }

// Profile size limits.
const (
	MaxProfileBytes      = 4096
	MaxProfileFieldBytes = 200
	MaxProfileDimensions = 4096
)

// EmbeddingProfile names the vector representation a target stores. Vectors
// with equal dimensions but different models or revisions are not comparable.
type EmbeddingProfile struct {
	Provider      string `json:"provider"`
	Model         string `json:"model"`
	ModelRevision string `json:"model_revision"`
	Dimensions    int    `json:"dimensions"`
	Metric        string `json:"metric"`
	// ExtractionChunkVersion names the text extraction and chunking that
	// produced the stored chunks. Changing it requires re-extraction.
	ExtractionChunkVersion string `json:"extraction_chunk_version"`
}

func profileText(name, value string) error {
	if value == "" || len(value) > MaxProfileFieldBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return fmt.Errorf("profile %s must be 1 to %d trimmed UTF-8 bytes", name, MaxProfileFieldBytes)
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("profile %s must not contain control characters", name)
	}
	return nil
}

// Validate rejects incomplete or unsupported profiles.
func (p EmbeddingProfile) Validate() error {
	if p.Provider != EmbeddingProviderBedrock && p.Provider != EmbeddingProviderOpenAI {
		return fmt.Errorf("profile provider must be %q or %q", EmbeddingProviderBedrock, EmbeddingProviderOpenAI)
	}
	for _, field := range []struct{ name, value string }{
		{"model", p.Model}, {"model_revision", p.ModelRevision}, {"extraction_chunk_version", p.ExtractionChunkVersion},
	} {
		if err := profileText(field.name, field.value); err != nil {
			return err
		}
	}
	if p.Dimensions < 1 || p.Dimensions > MaxProfileDimensions {
		return fmt.Errorf("profile dimensions must be between 1 and %d", MaxProfileDimensions)
	}
	if p.Metric != MetricCosine {
		return fmt.Errorf("profile metric must be %q", MetricCosine)
	}
	return nil
}

// Hash returns a stable identity for the complete profile.
func (p EmbeddingProfile) Hash() string {
	h := sha256.New()
	for _, part := range []string{"quarry.profile.v1", p.Provider, p.Model, p.ModelRevision, strconv.Itoa(p.Dimensions), p.Metric, p.ExtractionChunkVersion} {
		// A length prefix keeps neighboring fields from aliasing each other.
		_, _ = fmt.Fprintf(h, "%d:%s\x00", len(part), part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// SameEmbeddingSpace reports whether vectors from either profile are interchangeable.
func (p EmbeddingProfile) SameEmbeddingSpace(o EmbeddingProfile) bool {
	return p.Provider == o.Provider && p.Model == o.Model && p.ModelRevision == o.ModelRevision &&
		p.Dimensions == o.Dimensions && p.Metric == o.Metric
}

// SameTextVersion reports whether both profiles describe the same stored chunks.
func (p EmbeddingProfile) SameTextVersion(o EmbeddingProfile) bool {
	return p.ExtractionChunkVersion == o.ExtractionChunkVersion
}

// ParseEmbeddingProfile decodes one strictly typed profile object.
func ParseEmbeddingProfile(data []byte) (EmbeddingProfile, error) {
	var profile EmbeddingProfile
	if len(data) > MaxProfileBytes {
		return profile, fmt.Errorf("profile exceeds %d bytes", MaxProfileBytes)
	}
	if err := rejectDuplicateFields(data); err != nil {
		return profile, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return EmbeddingProfile{}, fmt.Errorf("decode profile: %w", err)
	}
	if err := profile.Validate(); err != nil {
		return EmbeddingProfile{}, err
	}
	return profile, nil
}

// rejectDuplicateFields requires a flat object with unique case-insensitive keys.
func rejectDuplicateFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode profile: %w", err)
	}
	if token != json.Delim('{') {
		return errors.New("profile must be a JSON object")
	}
	seen := make(map[string]struct{})
	for decoder.More() {
		key, keyErr := decoder.Token()
		if keyErr != nil {
			return fmt.Errorf("decode profile: %w", keyErr)
		}
		name, _ := key.(string)
		folded := strings.ToLower(name)
		if _, dup := seen[folded]; dup {
			return fmt.Errorf("duplicate profile field %q", name)
		}
		seen[folded] = struct{}{}
		value, valueErr := decoder.Token()
		if valueErr != nil {
			return fmt.Errorf("decode profile: %w", valueErr)
		}
		if _, nested := value.(json.Delim); nested {
			return errors.New("profile fields must be scalar values")
		}
	}
	if _, err = decoder.Token(); err != nil {
		return fmt.Errorf("decode profile: %w", err)
	}
	if _, err = decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after profile")
	}
	return nil
}
