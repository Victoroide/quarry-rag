package citationsupport

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// Method and budget constants define the verifier protocol and default limits.
const (
	Method          = "multilingual_nli_v1"
	DefaultTimeout  = 2 * time.Second
	DefaultMaxPairs = 8
	MaxPairs        = 32
	MaxClaims       = 32
	MaxBatchBytes   = 32 << 10
	defaultEndpoint = "http://127.0.0.1:8091/citation-support/v1"
)

var bearerToken = regexp.MustCompile(`^[a-zA-Z0-9._~+/-]+=*$`)

// Assessment reports a sentence-level status and its cited excerpt numbers.
type Assessment struct {
	Status   Status `json:"status"`
	Method   string `json:"method"`
	Version  string `json:"version"`
	Evidence []int  `json:"evidence"`
}

// Coverage counts generated sentences and those with at least one valid marker.
type Coverage struct {
	Sentences      int `json:"sentences"`
	CitedSentences int `json:"cited_sentences"`
}

// Pair contains only a generated claim and an exact synthesis excerpt body.
type Pair struct {
	Claim string `json:"claim"`
	Body  string `json:"body"`
}

// Batch references unique pairs by zero-based index, grouping joint evidence.
type Batch struct {
	Pairs  []Pair  `json:"pairs"`
	Claims [][]int `json:"claims"`
}

type verifierRequest struct {
	Model string `json:"model"`
	Batch
}

// Prediction is the language identification and scores returned for one evidence group.
type Prediction struct {
	ClaimLanguage     string   `json:"claim_language"`
	EvidenceLanguages []string `json:"evidence_languages"`
	Scores            *Scores  `json:"scores"`
}

type verifierResponse struct {
	Model   string       `json:"model"`
	Results []Prediction `json:"results"`
}

// Verifier is a deployment-owned, model-pinned verifier client.
type Verifier struct {
	model, endpoint, token string
	client                 *http.Client
	profiles               map[profileKey]Scores
}

// ValidateSettings checks the immutable model pin and verifier address.
func ValidateSettings(model, endpoint, token string) error {
	if !modelPattern.MatchString(model) {
		return errors.New("CITATION_SUPPORT_MODEL must be pinned as name@sha256:<digest>")
	}
	if token != "" && !bearerToken.MatchString(token) {
		return errors.New("CITATION_SUPPORT_TOKEN is not a valid bearer token")
	}
	if endpoint == "" {
		return nil
	}
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil || parsed.User != nil || parsed.Hostname() == "" || parsed.ForceQuery || parsed.RawQuery != "" || strings.Contains(endpoint, "#") {
		return errors.New("CITATION_SUPPORT_ENDPOINT must be an absolute verifier address without credentials, query or fragment")
	}
	address, _ := netip.ParseAddr(parsed.Hostname())
	loopback := address.Unmap().IsLoopback()
	if parsed.Scheme == "http" && loopback {
		return nil
	}
	if parsed.Scheme != "https" {
		return errors.New("CITATION_SUPPORT_ENDPOINT requires HTTPS outside literal loopback")
	}
	if !loopback && token == "" {
		return errors.New("CITATION_SUPPORT_TOKEN is required for an external verifier")
	}
	return nil
}

// New opens the configured calibration once and initializes a verifier.
// Without a model it returns a nil verifier.
func New(model, endpoint, token, calibrationPath string) (*Verifier, error) {
	if model == "" {
		return nil, nil
	}
	var calibration *Calibration
	if calibrationPath != "" {
		var err error
		calibration, err = ReadCalibration(calibrationPath)
		if err != nil {
			return nil, err
		}
	}
	return newVerifier(model, endpoint, token, calibration, nil)
}

func newVerifier(model, endpoint, token string, calibration *Calibration, transport http.RoundTripper) (*Verifier, error) {
	if err := ValidateSettings(model, endpoint, token); err != nil {
		return nil, err
	}
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	profiles := make(map[profileKey]Scores)
	if calibration != nil {
		if calibration.Model != model {
			return nil, errors.New("citation support: calibration model does not match configured digest")
		}
		reports, err := calibration.Evaluate()
		if err != nil {
			return nil, err
		}
		for i, report := range reports {
			if report.Enabled {
				p := calibration.Pairs[i]
				profiles[p.key()] = *p.Thresholds
			}
		}
	}
	if transport == nil {
		transport = &http.Transport{MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second, ForceAttemptHTTP2: true}
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &Verifier{model: model, endpoint: endpoint, token: token, client: client, profiles: profiles}, nil
}

// Version returns the immutable verifier identity.
func (v *Verifier) Version() string { return v.model }

// FitsBatch checks the encoded payload size, including JSON escaping and framing.
func FitsBatch(model string, batch Batch) bool {
	_, err := encodeBatch(model, batch)
	return err == nil
}

func encodeBatch(model string, batch Batch) ([]byte, error) {
	if len(batch.Pairs) == 0 || len(batch.Pairs) > MaxPairs || len(batch.Claims) == 0 || len(batch.Claims) > MaxClaims {
		return nil, errors.New("citation support: invalid batch dimensions")
	}
	rawBytes := 0
	for _, pair := range batch.Pairs {
		if pair.Claim == "" || pair.Body == "" || !utf8.ValidString(pair.Claim) || !utf8.ValidString(pair.Body) {
			return nil, errors.New("citation support: invalid claim or body")
		}
		rawBytes += len(pair.Claim) + len(pair.Body)
		if rawBytes > MaxBatchBytes {
			return nil, errors.New("citation support: batch exceeds byte limit")
		}
	}
	for _, group := range batch.Claims {
		if len(group) == 0 || len(group) > len(batch.Pairs) {
			return nil, errors.New("citation support: invalid evidence set")
		}
		for i, index := range group {
			if index < 0 || index >= len(batch.Pairs) || slices.Contains(group[:i], index) || batch.Pairs[index].Claim != batch.Pairs[group[0]].Claim {
				return nil, errors.New("citation support: invalid pair reference")
			}
		}
	}
	payload, err := json.Marshal(verifierRequest{Model: model, Batch: batch})
	if err != nil {
		return nil, fmt.Errorf("citation support: encode batch: %w", err)
	}
	if len(payload) > MaxBatchBytes {
		return nil, errors.New("citation support: batch exceeds byte limit")
	}
	return payload, nil
}

// Verify assesses one bounded batch using the request's deadline and calibration.
func (v *Verifier) Verify(ctx context.Context, batch Batch) ([]Status, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	payload, err := encodeBatch(v.model, batch)
	if err != nil {
		return nil, err
	}
	results := make([]Status, len(batch.Claims))
	for i := range results {
		results[i] = Unchecked
	}
	if len(v.profiles) == 0 {
		return results, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, errors.New("citation support: create verifier request")
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	if v.token != "" {
		req.Header.Set("Authorization", "Bearer "+v.token)
	}
	resp, err := v.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("citation support: verifier unavailable")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("citation support: verifier HTTP status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBatchBytes+1))
	if err != nil || len(body) > MaxBatchBytes {
		return nil, errors.New("citation support: unreadable or oversized verifier response")
	}
	var response verifierResponse
	if err := json.Unmarshal(body, &response, json.RejectUnknownMembers(true)); err != nil || response.Model != v.model || len(response.Results) != len(results) {
		return nil, errors.New("citation support: invalid verifier response")
	}
	for i, result := range response.Results {
		_, results[i] = assessPrediction(v.profiles, &result, len(batch.Claims[i]))
	}
	return results, ctx.Err()
}

func evidenceMode(count int) string {
	if count == 1 {
		return Single
	}
	return Set
}

func assessPrediction(profiles map[profileKey]Scores, result *Prediction, evidenceCount int) (profileKey, Status) {
	if result == nil || evidenceCount < 1 || len(result.EvidenceLanguages) != evidenceCount {
		return profileKey{}, Unchecked
	}
	source := result.EvidenceLanguages[0]
	if !validLanguage(result.ClaimLanguage) || !validLanguage(source) {
		return profileKey{}, Unchecked
	}
	for _, lang := range result.EvidenceLanguages[1:] {
		if lang != source {
			return profileKey{}, Unchecked
		}
	}
	key := profileKey{result.ClaimLanguage, source, evidenceMode(evidenceCount)}
	thresholds, ok := profiles[key]
	if !ok {
		return key, Unchecked
	}
	return key, classify(result.Scores, thresholds)
}
