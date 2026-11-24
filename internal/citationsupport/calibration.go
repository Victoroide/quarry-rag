// Package citationsupport assesses claims against the exact excerpts used for synthesis.
package citationsupport

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/text/language"
)

// Status describes the verifier's assessment of the claim against its evidence.
type Status string

// Status values distinguish the three assessed outcomes and abstention.
const (
	Supported    Status = "supported"
	Contradicted Status = "contradicted"
	Insufficient Status = "insufficient"
	Unchecked    Status = "unchecked"
)

// Evidence modes and sample categories identify the calibration conditions.
const (
	Single              = "single"
	Set                 = "set"
	BenignContrast      = "benign_contrast"
	BilingualParaphrase = "bilingual_paraphrase"
	maxCalibrationBytes = 8 << 20
)

var modelPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/-]{0,127}@sha256:[a-f0-9]{64}$`)

// Scores holds the three mutually exclusive outcomes in verifier protocol order.
type Scores struct {
	Supported    float64 `json:"supported"`
	Contradicted float64 `json:"contradicted"`
	Insufficient float64 `json:"insufficient"`
}

// UnmarshalJSON requires every score, including explicit zeros.
func (s *Scores) UnmarshalJSON(data []byte) error {
	var fields struct {
		Supported    *float64 `json:"supported"`
		Contradicted *float64 `json:"contradicted"`
		Insufficient *float64 `json:"insufficient"`
	}
	if err := json.Unmarshal(data, &fields, json.RejectUnknownMembers(true)); err != nil {
		return err
	}
	if fields.Supported == nil || fields.Contradicted == nil || fields.Insufficient == nil {
		return errors.New("citation support: incomplete scores")
	}
	*s = Scores{*fields.Supported, *fields.Contradicted, *fields.Insufficient}
	return nil
}

func (s Scores) values() [3]float64 {
	return [3]float64{s.Supported, s.Contradicted, s.Insufficient}
}

func (s Scores) valid() bool {
	for _, value := range s.values() {
		if !(value >= 0 && value <= 1) {
			return false
		}
	}
	return math.Abs(s.Supported+s.Contradicted+s.Insufficient-1) <= .001
}

var statuses = [...]Status{Supported, Contradicted, Insufficient}

func classify(scores *Scores, thresholds Scores) Status {
	if scores == nil || !scores.valid() {
		return Unchecked
	}
	values := scores.values()
	winner := 0
	for i, value := range values {
		if value > values[winner] {
			winner = i
		}
	}
	for i, value := range values {
		if i != winner && value == values[winner] {
			return Unchecked
		}
	}
	if values[winner] < thresholds.values()[winner] {
		return Unchecked
	}
	return statuses[winner]
}

// Calibration contains the operator's frozen thresholds and held-out replays.
type Calibration struct {
	Model                string           `json:"model"`
	BlindedNativeLabels  bool             `json:"blinded_native_labels"`
	DevelopmentDocuments []string         `json:"development_documents"`
	Pairs                []PairEvaluation `json:"pairs"`
}

// PairEvaluation groups held-out samples by reference languages and evidence mode.
// Thresholds are optional for reference pairs that are not activation candidates.
type PairEvaluation struct {
	ClaimLanguage  string   `json:"claim_language"`
	SourceLanguage string   `json:"source_language"`
	EvidenceMode   string   `json:"evidence_mode"`
	Thresholds     *Scores  `json:"thresholds,omitempty"`
	HeldOut        []Sample `json:"held_out"`
}

// Sample represents one eligible held-out example, including unlabeled examples.
type Sample struct {
	Documents     []string    `json:"documents"`
	EvidenceCount int         `json:"evidence_count"`
	Label         Status      `json:"label,omitempty"`
	Prediction    *Prediction `json:"prediction,omitempty"`
	Category      string      `json:"category,omitempty"`
}

// Count preserves the numerator and denominator of an evaluation metric.
type Count struct {
	Numerator   int `json:"numerator"`
	Denominator int `json:"denominator"`
}

// PairReport publishes all denominators used to decide whether to enable a pair.
type PairReport struct {
	ClaimLanguage               string           `json:"claim_language"`
	SourceLanguage              string           `json:"source_language"`
	EvidenceMode                string           `json:"evidence_mode"`
	Enabled                     bool             `json:"enabled"`
	Eligible                    int              `json:"eligible"`
	Labeled                     int              `json:"labeled"`
	Assessed                    int              `json:"assessed_labeled"`
	ProfileAssessed             int              `json:"profile_assessed_labeled"`
	SupportedLabeled            int              `json:"supported_labeled"`
	SupportedPrecision          Count            `json:"confirmed_supported_precision"`
	PrecisionLower95            float64          `json:"precision_lower_95"`
	ReferenceSupportedPrecision Count            `json:"reference_confirmed_supported_precision"`
	ReferencePrecisionLower95   float64          `json:"reference_precision_lower_95"`
	Recall                      map[Status]Count `json:"recall"`
}

type profileKey struct{ claimLanguage, sourceLanguage, evidenceMode string }

func (p PairEvaluation) key() profileKey {
	return profileKey{p.ClaimLanguage, p.SourceLanguage, p.EvidenceMode}
}

func validLanguage(code string) bool {
	tag, err := language.Parse(code)
	if err != nil || tag.String() != code {
		return false
	}
	base, _, _ := tag.Raw()
	switch base.String() {
	case "und", "mul", "zxx", "mis":
		return false
	default:
		return true
	}
}

// ReadCalibration loads a bounded local replay, without reading any source text.
func ReadCalibration(path string) (*Calibration, error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("citation support: open calibration directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(filepath.Base(path))
	if err != nil {
		return nil, fmt.Errorf("citation support: open calibration: %w", err)
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxCalibrationBytes+1))
	if err != nil {
		return nil, fmt.Errorf("citation support: read calibration: %w", err)
	}
	return decodeCalibration(data)
}

func decodeCalibration(data []byte) (*Calibration, error) {
	if len(data) > maxCalibrationBytes {
		return nil, errors.New("citation support: calibration exceeds byte limit")
	}
	var c Calibration
	if err := json.Unmarshal(data, &c, json.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("citation support: invalid calibration: %w", err)
	}
	if _, err := c.Evaluate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Evaluate applies the same gate as the live verifier.
func (c Calibration) Evaluate() ([]PairReport, error) {
	if !modelPattern.MatchString(c.Model) || len(c.DevelopmentDocuments) == 0 {
		return nil, errors.New("citation support: calibration requires a digest-pinned model and development document IDs")
	}
	development := make(map[string]struct{}, len(c.DevelopmentDocuments))
	for _, doc := range c.DevelopmentDocuments {
		if doc == "" || strings.TrimSpace(doc) != doc {
			return nil, errors.New("citation support: invalid development document ID")
		}
		development[doc] = struct{}{}
	}
	byKey := make(map[profileKey]*PairReport, len(c.Pairs))
	candidates := make(map[profileKey]Scores, len(c.Pairs))
	reports := make([]PairReport, len(c.Pairs))
	for i, p := range c.Pairs {
		if err := p.validate(); err != nil {
			return nil, fmt.Errorf("citation support: calibration profile %d: %w", i+1, err)
		}
		key := p.key()
		if _, exists := byKey[key]; exists {
			return nil, fmt.Errorf("citation support: duplicate calibration profile %d", i+1)
		}
		reports[i] = PairReport{ClaimLanguage: p.ClaimLanguage, SourceLanguage: p.SourceLanguage, EvidenceMode: p.EvidenceMode,
			Eligible: len(p.HeldOut), Recall: map[Status]Count{Supported: {}, Contradicted: {}, Insufficient: {}}}
		byKey[key] = &reports[i]
		if p.Thresholds != nil {
			candidates[key] = *p.Thresholds
		}
	}

	// Replay every reference group against the frozen candidate routing table.
	// Incoming predictions contribute to the profile that actually issued them.
	heldOut := make(map[string]struct{})
	qualification := make([]bool, len(c.Pairs))
	for i, p := range c.Pairs {
		var err error
		qualification[i], err = p.countSamples(&reports[i], byKey, candidates, development, heldOut)
		if err != nil {
			return nil, fmt.Errorf("citation support: calibration profile %d: %w", i+1, err)
		}
	}
	enabled := make(map[profileKey]Scores)
	for i, p := range c.Pairs {
		report := &reports[i]
		precision := report.SupportedPrecision
		report.PrecisionLower95 = precisionLower(precision.Numerator, precision.Denominator)
		referencePrecision := report.ReferenceSupportedPrecision
		report.ReferencePrecisionLower95 = precisionLower(referencePrecision.Numerator, referencePrecision.Denominator)
		report.Enabled = p.Thresholds != nil && c.BlindedNativeLabels && qualification[i] &&
			report.Labeled*2 >= report.Eligible && report.ProfileAssessed*2 >= report.Eligible &&
			report.PrecisionLower95 >= .90 && report.ReferencePrecisionLower95 >= .90
		if report.Enabled {
			enabled[p.key()] = *p.Thresholds
		}
	}
	// Reference coverage and recall describe the qualified live policy.
	for i, p := range c.Pairs {
		reports[i].countLiveOutcomes(p.HeldOut, enabled)
	}
	return reports, nil
}

func (p PairEvaluation) validate() error {
	if !validLanguage(p.ClaimLanguage) || !validLanguage(p.SourceLanguage) || (p.EvidenceMode != Single && p.EvidenceMode != Set) {
		return errors.New("invalid language pair or evidence mode")
	}
	if p.Thresholds == nil {
		return nil
	}
	for _, threshold := range p.Thresholds.values() {
		if !(threshold > 0 && threshold <= 1) {
			return errors.New("thresholds must be in (0, 1]")
		}
	}
	return nil
}

func (p PairEvaluation) countSamples(report *PairReport, byKey map[profileKey]*PairReport, candidates map[profileKey]Scores, development, heldOut map[string]struct{}) (bool, error) {
	reference := p.key()
	contrast, bilingual := false, false
	for _, example := range p.HeldOut {
		if err := validSample(example, p.EvidenceMode, development, heldOut); err != nil {
			return false, err
		}
		routed, predicted := assessPrediction(candidates, example.Prediction, example.EvidenceCount)
		label := example.Label
		if label != "" {
			report.Labeled++
			contrast = contrast || (example.Category == BenignContrast && label == Contradicted)
			bilingual = bilingual || (example.Category == BilingualParaphrase && label == Supported)
			recall := report.Recall[label]
			recall.Denominator++
			report.Recall[label] = recall
			if routed == reference && predicted != Unchecked {
				report.ProfileAssessed++
			}
		}
		if predicted == Supported {
			issuer := byKey[routed]
			issuer.SupportedPrecision.Denominator++
			if routed == reference {
				report.ReferenceSupportedPrecision.Denominator++
			}
			if label != "" {
				issuer.SupportedLabeled++
			}
			if label == Supported {
				issuer.SupportedPrecision.Numerator++
				if routed == reference {
					report.ReferenceSupportedPrecision.Numerator++
				}
			}
		}
	}
	balanced := true
	for _, class := range statuses {
		balanced = balanced && report.Recall[class].Denominator*len(statuses) == report.Labeled
	}
	return balanced && contrast && (p.ClaimLanguage == p.SourceLanguage || bilingual), nil
}

func (r *PairReport) countLiveOutcomes(samples []Sample, profiles map[profileKey]Scores) {
	for _, sample := range samples {
		if sample.Label == "" {
			continue
		}
		_, status := assessPrediction(profiles, sample.Prediction, sample.EvidenceCount)
		if status != Unchecked {
			r.Assessed++
		}
		if status == sample.Label {
			recall := r.Recall[status]
			recall.Numerator++
			r.Recall[status] = recall
		}
	}
}

func validSample(s Sample, mode string, development, heldOut map[string]struct{}) error {
	if s.EvidenceCount < 1 || s.EvidenceCount > MaxPairs || mode != evidenceMode(s.EvidenceCount) {
		return errors.New("held-out sample has an invalid evidence count for its mode")
	}
	if len(s.Documents) == 0 || (mode == Single && len(s.Documents) != 1) {
		return errors.New("held-out sample has invalid document IDs for its evidence mode")
	}
	for _, doc := range s.Documents {
		_, dev := development[doc]
		_, test := heldOut[doc]
		if doc == "" || strings.TrimSpace(doc) != doc || dev || test {
			return errors.New("held-out document is missing or reused across samples or splits")
		}
		heldOut[doc] = struct{}{}
	}
	switch s.Label {
	case "", Supported, Contradicted, Insufficient:
	default:
		return errors.New("invalid held-out label")
	}
	return nil
}

// precisionLower inverts the binomial upper tail for an exact one-sided
// Clopper-Pearson 95% lower confidence bound.
func precisionLower(success, count int) float64 {
	if success == 0 {
		return 0
	}
	if success == count {
		return math.Pow(.05, 1/float64(count))
	}
	logN, _ := math.Lgamma(float64(count + 1))
	logK, _ := math.Lgamma(float64(success + 1))
	logNK, _ := math.Lgamma(float64(count - success + 1))
	logChoose := logN - logK - logNK
	lo, hi := 0.0, float64(success)/float64(count)
	for range 48 {
		p := (lo + hi) / 2
		logP, logQ := math.Log(p), math.Log1p(-p)
		logTerm := logChoose + float64(success)*logP + float64(count-success)*logQ
		tail := math.Exp(logTerm)
		for k := success; k < count && tail < .05; k++ {
			logTerm += math.Log(float64(count-k)/float64(k+1)) + logP - logQ
			tail += math.Exp(logTerm)
		}
		if tail >= .05 {
			hi = p
		} else {
			lo = p
		}
	}
	return lo
}
