package pipeline

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMeaningBearingPairs(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "meaning_bearing_pairs.json"))
	require.NoError(t, err)
	require.Equal(t, "a81a4dbd5cb270c10071ad9b6bb3eb55e7d933e1085f8a899299bc34fc4562b4", fmt.Sprintf("%x", sha256.Sum256(data)))
	var fixture struct {
		Version string `json:"version"`
		Pairs   []struct {
			ID             string `json:"id"`
			Category       string `json:"category"`
			Candidate      string `json:"candidate"`
			Excerpt        string `json:"excerpt"`
			CandidateCut   bool   `json:"candidate_cut"`
			MeaningBearing bool   `json:"meaning_bearing"`
		} `json:"pairs"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Equal(t, "meaning-bearing-pairs-v1", fixture.Version)
	require.NotEmpty(t, fixture.Pairs)

	type result struct{ total, correct, falsePositives, misses int }
	counts := make(map[string]result)
	for _, pair := range fixture.Pairs {
		got := meaningBearingDifference(pair.Candidate, pair.Excerpt, pair.CandidateCut)
		r := counts[pair.Category]
		r.total++
		switch {
		case got == pair.MeaningBearing:
			r.correct++
		case got:
			r.falsePositives++
		default:
			r.misses++
		}
		counts[pair.Category] = r
		assert.Equal(t, pair.MeaningBearing, got, pair.ID)
		if !pair.CandidateCut {
			assert.Equal(t, pair.MeaningBearing, meaningBearingDifference(pair.Excerpt, pair.Candidate, false), "%s reversed", pair.ID)
		}
	}
	for category, r := range counts {
		t.Logf("%s: %d/%d correct, %d false positives, %d misses", category, r.correct, r.total, r.falsePositives, r.misses)
	}
}
