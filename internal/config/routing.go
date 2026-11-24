package config

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

// Routing policies select how automatic retrieval routing decides LOCAL vs GLOBAL.
const (
	RoutingPolicyLegacy   = "legacy"
	RoutingPolicyEvidence = "evidence"
)

const (
	routingProfileSchemaVersion = 1
	maxRoutingProfileFileBytes  = 1 << 20
)

//go:embed routing_profiles.json
var embeddedRoutingProfilesJSON []byte

// EmbeddingProfile identifies the active embedding provider, model and dimensions.
type EmbeddingProfile struct {
	Provider       string `json:"provider"`
	Model          string `json:"model"`
	Dimensions     int    `json:"dimensions"`
	EndpointSHA256 string `json:"endpoint_sha256,omitempty"`
	ModelRevision  string `json:"model_revision,omitempty"`
}

// RoutingCalibrationStats records the development-split measurements used to fit a profile.
type RoutingCalibrationStats struct {
	CorpusVersion           string  `json:"corpus_version"`
	DocumentCount           int     `json:"document_count"`
	QueryCount              int     `json:"query_count"`
	SingleDocRecall         float64 `json:"single_doc_recall"`
	SingleDocRecallLoss     float64 `json:"single_doc_recall_loss"`
	AmbiguousRecall         float64 `json:"ambiguous_recall"`
	AmbiguousFalseLocalRate float64 `json:"ambiguous_false_local_rate"`
}

// RoutingProfile defines a versioned, calibrated margin for one embedding profile.
type RoutingProfile struct {
	Version        string                   `json:"version"`
	Provider       string                   `json:"provider"`
	Model          string                   `json:"model"`
	Dimensions     int                      `json:"dimensions"`
	EndpointSHA256 string                   `json:"endpoint_sha256,omitempty"`
	ModelRevision  string                   `json:"model_revision,omitempty"`
	Metric         string                   `json:"metric"`
	Margin         *float64                 `json:"margin"`
	DevSplit       *RoutingCalibrationStats `json:"dev_split,omitempty"`
}

type routingProfileSet struct {
	SchemaVersion int              `json:"schema_version"`
	Profiles      []RoutingProfile `json:"profiles"`
}

// ActiveEmbeddingProfile returns the embedding profile configured for runtime use.
func (c *Config) ActiveEmbeddingProfile() EmbeddingProfile {
	if c == nil {
		return EmbeddingProfile{}
	}
	if c.FakeMode {
		return EmbeddingProfile{
			Provider:   "offline",
			Model:      "fake",
			Dimensions: c.EmbeddingDimensions,
		}
	}
	if c.EmbeddingProvider == "openai" {
		profile := EmbeddingProfile{
			Provider:   "openai",
			Model:      c.OpenAIEmbeddingModel,
			Dimensions: c.EmbeddingDimensions,
		}
		if c.OpenAIBaseURL != "" && validModelRevision(c.RoutingEmbeddingRevision) {
			digest := sha256.Sum256([]byte(strings.TrimRight(c.OpenAIBaseURL, "/")))
			profile.EndpointSHA256 = hex.EncodeToString(digest[:])
			profile.ModelRevision = c.RoutingEmbeddingRevision
		}
		return profile
	}
	provider := c.EmbeddingProvider
	if provider == "" {
		provider = "bedrock"
	}
	return EmbeddingProfile{
		Provider:   provider,
		Model:      c.BedrockEmbedModel,
		Dimensions: c.EmbeddingDimensions,
	}
}

// LookupRoutingProfile loads and validates a calibration profile set (from file
// when non-empty, otherwise from the embedded published artifact) and returns
// the matching versioned profile, or nil when no entry matches.
func LookupRoutingProfile(file string, target EmbeddingProfile) (*RoutingProfile, error) {
	var data []byte
	var err error
	if file == "" {
		data = embeddedRoutingProfilesJSON
	} else {
		data, err = readRoutingProfileFile(file)
		if err != nil {
			return nil, err
		}
	}
	set, err := parseRoutingProfileSet(data)
	if err != nil {
		return nil, err
	}
	if target.Provider == "openai" && (target.EndpointSHA256 == "" || target.ModelRevision == "") {
		return nil, nil
	}
	for i := range set.Profiles {
		p := &set.Profiles[i]
		if p.embeddingProfile() == target {
			cp := *p
			return &cp, nil
		}
	}
	return nil, nil
}

func (p RoutingProfile) embeddingProfile() EmbeddingProfile {
	return EmbeddingProfile{Provider: p.Provider, Model: p.Model, Dimensions: p.Dimensions,
		EndpointSHA256: p.EndpointSHA256, ModelRevision: p.ModelRevision}
}

func readRoutingProfileFile(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "" || strings.ContainsFunc(path, unicode.IsControl) {
		return nil, errors.New("path must be non-empty without control characters")
	}
	clean := filepath.Clean(path)
	abs, err := filepath.Abs(clean)
	if err != nil {
		return nil, fmt.Errorf("resolve path: %w", err)
	}
	dir := filepath.Dir(abs)
	base := filepath.Base(abs)
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open profile directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	info, err := root.Stat(base)
	if err != nil {
		return nil, fmt.Errorf("stat profile file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("profile path must be a regular file")
	}
	f, err := root.Open(base)
	if err != nil {
		return nil, fmt.Errorf("open profile file: %w", err)
	}
	defer func() { _ = f.Close() }()
	info, err = f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat profile file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("profile path must be a regular file")
	}
	if info.Size() > maxRoutingProfileFileBytes {
		return nil, fmt.Errorf("profile file exceeds %d bytes", maxRoutingProfileFileBytes)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxRoutingProfileFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read profile file: %w", err)
	}
	if len(data) > maxRoutingProfileFileBytes {
		return nil, fmt.Errorf("profile file exceeds %d bytes", maxRoutingProfileFileBytes)
	}
	return data, nil
}

func parseRoutingProfileSet(data []byte) (*routingProfileSet, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var set routingProfileSet
	if err := dec.Decode(&set); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected trailing JSON data")
	}
	if set.SchemaVersion != routingProfileSchemaVersion {
		return nil, fmt.Errorf("unsupported schema_version %d", set.SchemaVersion)
	}
	if len(set.Profiles) == 0 {
		return nil, errors.New("profiles must not be empty")
	}
	seen := make(map[EmbeddingProfile]struct{}, len(set.Profiles))
	for i, p := range set.Profiles {
		if err := validateRoutingProfile(p); err != nil {
			return nil, fmt.Errorf("profiles[%d]: %w", i, err)
		}
		key := p.embeddingProfile()
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("duplicate profile for %s/%s/%d", p.Provider, p.Model, p.Dimensions)
		}
		seen[key] = struct{}{}
	}
	return &set, nil
}

func validateRoutingProfile(p RoutingProfile) error {
	if strings.TrimSpace(p.Version) == "" || strings.ContainsFunc(p.Version, unicode.IsControl) {
		return errors.New("version must be non-empty")
	}
	switch p.Provider {
	case "bedrock", "openai", "offline", "replay":
	default:
		return fmt.Errorf("unsupported provider %q", p.Provider)
	}
	if p.EndpointSHA256 != "" || p.ModelRevision != "" {
		value, err := hex.DecodeString(p.EndpointSHA256)
		if err != nil || len(value) != sha256.Size || strings.ToLower(p.EndpointSHA256) != p.EndpointSHA256 ||
			p.Provider != "openai" || !validModelRevision(p.ModelRevision) {
			return errors.New("model_revision and lowercase endpoint_sha256 must identify an OpenAI deployment together")
		}
	}
	if strings.TrimSpace(p.Model) == "" || strings.ContainsFunc(p.Model, unicode.IsControl) {
		return errors.New("model must be non-empty")
	}
	if p.Dimensions < 1 || p.Dimensions > 4096 {
		return fmt.Errorf("dimensions must be between 1 and 4096, got %d", p.Dimensions)
	}
	if p.Metric != "cosine" {
		return fmt.Errorf("unsupported metric %q", p.Metric)
	}
	if p.Margin == nil || !validRoutingMargin(*p.Margin) {
		return errors.New("margin must be present, finite and between 0 and 1")
	}
	return nil
}

func (c *Config) loadRoutingConfig() error {
	c.RoutingPolicy = envOrDefault("ROUTING_POLICY", RoutingPolicyLegacy)
	if c.RoutingPolicy != RoutingPolicyEvidence {
		return nil
	}
	c.RoutingProfileFile = os.Getenv("ROUTING_PROFILE_FILE")
	c.RoutingEmbeddingRevision = os.Getenv("ROUTING_EMBEDDING_REVISION")
	if rawMargin := os.Getenv("ROUTING_MARGIN"); rawMargin != "" {
		margin, err := envFloat64OrDefault("ROUTING_MARGIN", 0)
		if err != nil {
			return err
		}
		c.RoutingMargin = &margin
	}
	return nil
}

func validModelRevision(value string) bool {
	return value != "" && len(value) <= 256 && !strings.ContainsFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	})
}

func (c *Config) validateAndResolveRouting() error {
	if c.RoutingPolicy == "" {
		c.RoutingPolicy = RoutingPolicyLegacy
	}
	switch c.RoutingPolicy {
	case RoutingPolicyLegacy:
		return nil
	case RoutingPolicyEvidence:
	default:
		return fmt.Errorf("config: ROUTING_POLICY must be %s or %s", RoutingPolicyLegacy, RoutingPolicyEvidence)
	}
	if c.RoutingMargin != nil && !validRoutingMargin(*c.RoutingMargin) {
		return fmt.Errorf("config: ROUTING_MARGIN must be finite and between 0 and 1")
	}
	matched, err := LookupRoutingProfile(c.RoutingProfileFile, c.ActiveEmbeddingProfile())
	if err != nil {
		if c.RoutingProfileFile != "" {
			return fmt.Errorf("config: invalid ROUTING_PROFILE_FILE: %w", err)
		}
		return fmt.Errorf("config: invalid embedded routing profile: %w", err)
	}
	if matched == nil {
		c.Warnings = append(c.Warnings, "no routing calibration matched the active embedding profile; evidence routing will use conservative GLOBAL fallback")
		return nil
	}

	c.RoutingCalibrationMatched = true
	c.RoutingProfileVersion = matched.Version
	c.EffectiveRoutingMargin = *matched.Margin
	if c.RoutingMargin != nil {
		c.EffectiveRoutingMargin = *c.RoutingMargin
	}
	return nil
}

func validRoutingMargin(m float64) bool {
	return m >= 0 && m <= 1
}
