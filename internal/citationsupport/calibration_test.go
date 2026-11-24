package citationsupport

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const testModel = "verifier@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func testCalibration() Calibration {
	p := PairEvaluation{
		ClaimLanguage: "ja", SourceLanguage: "en", EvidenceMode: Single,
		Thresholds: &Scores{Supported: .85, Contradicted: .85, Insufficient: .85},
	}
	prediction := func(scores Scores) *Prediction {
		return &Prediction{ClaimLanguage: "ja", EvidenceLanguages: []string{"en"}, Scores: &scores}
	}
	for i := range 30 {
		p.HeldOut = append(p.HeldOut,
			Sample{Documents: []string{fmt.Sprintf("paraphrase-%d", i)}, EvidenceCount: 1, Label: Supported, Prediction: prediction(Scores{Supported: .98, Contradicted: .01, Insufficient: .01}), Category: BilingualParaphrase},
			Sample{Documents: []string{fmt.Sprintf("contrast-%d", i)}, EvidenceCount: 1, Label: Contradicted, Prediction: prediction(Scores{Supported: .02, Contradicted: .96, Insufficient: .02}), Category: BenignContrast},
			Sample{Documents: []string{fmt.Sprintf("missing-%d", i)}, EvidenceCount: 1, Label: Insufficient, Prediction: prediction(Scores{Supported: .01, Contradicted: .01, Insufficient: .98})},
		)
	}
	return Calibration{Model: testModel, BlindedNativeLabels: true, DevelopmentDocuments: []string{"development-1"}, Pairs: []PairEvaluation{p}}
}

func TestCalibrationGateAndDenominators(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Calibration)
		on   bool
	}{
		{"eligible and labeled are separate", func(c *Calibration) {
			p := &c.Pairs[0]
			p.HeldOut = append(p.HeldOut, Sample{Documents: []string{"unlabeled"}, EvidenceCount: 1})
		}, true},
		{"inadequate sample", func(c *Calibration) { c.Pairs[0].HeldOut = c.Pairs[0].HeldOut[:28*3] }, false},
		{"zero supported predictions", func(c *Calibration) { c.Pairs[0].Thresholds.Supported = .99 }, false},
		{"one false positive defeats interval", func(c *Calibration) {
			c.Pairs[0].HeldOut[1].Prediction.Scores = c.Pairs[0].HeldOut[0].Prediction.Scores
		}, false},
		{"low labeled coverage", func(c *Calibration) {
			p := &c.Pairs[0]
			for i := range 91 {
				p.HeldOut = append(p.HeldOut, Sample{Documents: []string{fmt.Sprintf("unlabeled-%d", i)}, EvidenceCount: 1})
			}
		}, false},
		{"low assessment coverage", func(c *Calibration) {
			c.Pairs[0].Thresholds.Contradicted = .99
			c.Pairs[0].Thresholds.Insufficient = .99
		}, false},
		{"unbalanced labels", func(c *Calibration) { c.Pairs[0].HeldOut = c.Pairs[0].HeldOut[:89] }, false},
		{"unblinded labels", func(c *Calibration) { c.BlindedNativeLabels = false }, false},
		{"missing bilingual paraphrases", func(c *Calibration) {
			for i := range c.Pairs[0].HeldOut {
				if c.Pairs[0].HeldOut[i].Label == Supported {
					c.Pairs[0].HeldOut[i].Category = ""
				}
			}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testCalibration()
			tc.edit(&c)
			reports, err := c.Evaluate()
			require.NoError(t, err)
			require.Len(t, reports, 1)
			require.Equal(t, tc.on, reports[0].Enabled)
			if tc.on {
				got := reports[0]
				require.Equal(t, 91, got.Eligible)
				require.Equal(t, 90, got.Labeled)
				require.Equal(t, 90, got.Assessed)
				require.Equal(t, 90, got.ProfileAssessed)
				require.Equal(t, 30, got.SupportedLabeled)
				require.Equal(t, Count{Numerator: 30, Denominator: 30}, got.SupportedPrecision)
				require.Equal(t, got.SupportedPrecision, got.ReferenceSupportedPrecision)
				require.Equal(t, got.PrecisionLower95, got.ReferencePrecisionLower95)
				for _, recall := range got.Recall {
					require.Equal(t, Count{Numerator: 30, Denominator: 30}, recall)
				}
			}
		})
	}
	require.InDelta(t, .9018553723, precisionLower(29, 29), 1e-9)
	require.InDelta(t, .8985342644, precisionLower(28, 28), 1e-9)
	require.Zero(t, precisionLower(0, 0))
}

func TestCalibrationRejectsSplitLeakageAndDependentSamples(t *testing.T) {
	for _, doc := range []string{"development-1", "paraphrase-0"} {
		t.Run(doc, func(t *testing.T) {
			c := testCalibration()
			c.Pairs[0].HeldOut[1].Documents = []string{doc}
			_, err := c.Evaluate()
			require.Error(t, err)
		})
	}
	c := testCalibration()
	c.Pairs = append(c.Pairs, PairEvaluation{ClaimLanguage: "fr", SourceLanguage: "en", EvidenceMode: Single, HeldOut: c.Pairs[0].HeldOut[:1]})
	_, err := c.Evaluate()
	require.ErrorContains(t, err, "reused across samples")
}

func TestCalibrationCountsUnlabeledSupportedPredictions(t *testing.T) {
	c := testCalibration()
	p := &c.Pairs[0]
	for i := range 90 {
		p.HeldOut = append(p.HeldOut, Sample{Documents: []string{fmt.Sprintf("unlabeled-supported-%d", i)}, EvidenceCount: 1, Prediction: p.HeldOut[0].Prediction})
	}
	reports, err := c.Evaluate()
	require.NoError(t, err)
	report := reports[0]
	require.Equal(t, 180, report.Eligible)
	require.Equal(t, 90, report.Labeled)
	require.Equal(t, 90, report.ProfileAssessed)
	require.Equal(t, 30, report.SupportedLabeled)
	require.Zero(t, report.Assessed)
	require.Equal(t, Count{Numerator: 30, Denominator: 120}, report.SupportedPrecision)
	require.Equal(t, report.SupportedPrecision, report.ReferenceSupportedPrecision)
	require.False(t, report.Enabled)
	require.Less(t, report.PrecisionLower95, .25)
}

func TestCalibrationChargesLanguageMisroutesToIssuingProfile(t *testing.T) {
	c := testCalibration()
	reference := testCalibration().Pairs[0]
	reference.ClaimLanguage, reference.SourceLanguage, reference.Thresholds = "en", "ja", nil
	for i := range reference.HeldOut {
		row := &reference.HeldOut[i]
		row.Documents = []string{"other-" + row.Documents[0]}
		row.Prediction.ClaimLanguage = "en"
		row.Prediction.EvidenceLanguages = []string{"ja"}
	}
	c.Pairs = append(c.Pairs, reference)
	reports, err := c.Evaluate()
	require.NoError(t, err)
	require.True(t, reports[0].Enabled)
	require.Equal(t, 90, reports[0].Assessed)
	for i := range c.Pairs[1].HeldOut {
		row := &c.Pairs[1].HeldOut[i]
		if row.Label == Contradicted {
			row.Prediction = c.Pairs[0].HeldOut[0].Prediction
		}
	}
	reports, err = c.Evaluate()
	require.NoError(t, err)
	require.Equal(t, Count{Numerator: 30, Denominator: 60}, reports[0].SupportedPrecision)
	require.Equal(t, Count{Numerator: 30, Denominator: 30}, reports[0].ReferenceSupportedPrecision)
	require.Equal(t, 60, reports[0].SupportedLabeled)
	require.Less(t, reports[0].PrecisionLower95, .5)
	require.False(t, reports[0].Enabled)
	require.False(t, reports[1].Enabled)
	for _, report := range reports {
		require.Equal(t, 90, report.Eligible)
		require.Equal(t, 90, report.Labeled)
		require.Zero(t, report.Assessed)
	}
	require.Equal(t, 90, reports[0].ProfileAssessed)
	require.Zero(t, reports[1].ProfileAssessed)
	require.Zero(t, reports[1].SupportedPrecision.Denominator)
}

func TestCalibrationRequiresPrecisionOnReferencePair(t *testing.T) {
	c := testCalibration()
	target := &c.Pairs[0]
	for i := range target.HeldOut {
		sample := &target.HeldOut[i]
		switch sample.Label {
		case Supported:
			sample.Prediction.Scores = &Scores{Supported: .01, Contradicted: .98, Insufficient: .01}
		case Contradicted:
			sample.Prediction.Scores = &Scores{Supported: .98, Contradicted: .01, Insufficient: .01}
		}
	}
	incoming := PairEvaluation{ClaimLanguage: "en", SourceLanguage: "ja", EvidenceMode: Single}
	misrouted := &Prediction{ClaimLanguage: target.ClaimLanguage, EvidenceLanguages: []string{target.SourceLanguage},
		Scores: &Scores{Supported: .98, Contradicted: .01, Insufficient: .01}}
	for i := range 600 {
		incoming.HeldOut = append(incoming.HeldOut,
			Sample{Documents: []string{fmt.Sprintf("incoming-paraphrase-%d", i)}, EvidenceCount: 1, Label: Supported, Prediction: misrouted, Category: BilingualParaphrase},
			Sample{Documents: []string{fmt.Sprintf("incoming-contrast-%d", i)}, EvidenceCount: 1, Label: Contradicted, Category: BenignContrast},
			Sample{Documents: []string{fmt.Sprintf("incoming-insufficient-%d", i)}, EvidenceCount: 1, Label: Insufficient},
		)
	}
	c.Pairs = append(c.Pairs, incoming)
	reports, err := c.Evaluate()
	require.NoError(t, err)
	report := reports[0]
	require.Equal(t, 90, report.ProfileAssessed)
	require.Equal(t, Count{Numerator: 600, Denominator: 630}, report.SupportedPrecision)
	require.GreaterOrEqual(t, report.PrecisionLower95, .90)
	require.False(t, report.Enabled, "all 30 supported predictions on the reference pair were wrong")
	require.Equal(t, Count{Denominator: 30}, report.ReferenceSupportedPrecision)
	require.Zero(t, report.ReferencePrecisionLower95)
}

func TestCalibrationRequiresConcreteClaimAndSourceLanguages(t *testing.T) {
	for _, tag := range []string{"und", "und-Latn", "mul", "mul-Latn", "zxx", "zxx-Latn", "mis", "mis-Latn", "x-test"} {
		t.Run(tag, func(t *testing.T) {
			c := testCalibration()
			c.Pairs[0].ClaimLanguage = tag
			_, err := c.Evaluate()
			require.Error(t, err)
			c = testCalibration()
			c.Pairs[0].SourceLanguage = tag
			_, err = c.Evaluate()
			require.Error(t, err)
		})
	}
	c := testCalibration()
	c.Pairs[0].SourceLanguage = "en-GB"
	_, err := c.Evaluate()
	require.NoError(t, err)
}

func FuzzReadCalibration(f *testing.F) {
	for _, seed := range []string{"{}", "{", "null", `{"pairs":[{"held_out":[null]}]}`} {
		f.Add([]byte(seed))
	}
	valid, err := json.Marshal(testCalibration())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Fuzz(func(t *testing.T, body []byte) {
		calibration, err := decodeCalibration(body)
		if err == nil {
			_, err = calibration.Evaluate()
			require.NoError(t, err)
		}
	})
}
