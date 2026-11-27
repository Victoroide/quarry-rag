package postgres

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestScopedRecallMetrics(t *testing.T) {
	exact := []recallHit{{"a", .1, true}, {"b", .2, true}, {"c", .2, true}}
	candidates := []recallHit{
		{"b", .2, true}, {"d", .2, true}, {"e", .2, true}, {"d", .2, true},
		{"foreign", .05, false}, {"absent", 0, false}, {"worse", .3, true},
	}
	got := scoreRecall(exact, candidates, 3, 6)
	require.Equal(t, 7, got.Returned)
	require.Equal(t, 4, got.EligibleHits)
	require.Equal(t, 2, got.Unauthorized)
	require.Equal(t, 1.0/3, *got.Strict)
	require.Equal(t, 2.0/3, *got.TieAware, "ties cannot fill the missing strictly closer slot")
	require.Equal(t, 1.0, *got.FillRate)
	roundedTie := []recallHit{{"different", .20000000000000004, true}}
	require.Zero(t, *scoreRecall(exact, roundedTie, 3, 4).TieAware)
	short := scoreRecall(exact[:1], exact[:1], 5, 1)
	require.Equal(t, 1.0, *short.Strict)
	require.Equal(t, 1.0/5, *short.FillRate)
	empty := scoreRecall(nil, candidates[4:5], 5, 0)
	require.Nil(t, empty.Strict)
	require.Nil(t, empty.TieAware)
	require.Zero(t, empty.EligibleHits)
	require.Equal(t, 1, empty.Unauthorized)

	require.Nil(t, recallQuantile(nil, 95))
	values := []time.Duration{9, 1, 3, 7, 5}
	require.Equal(t, 5*time.Nanosecond, *recallQuantile(values, 50))
	require.Equal(t, 9*time.Nanosecond, *recallQuantile(values, 95))
	require.Equal(t, []time.Duration{9, 1, 3, 7, 5}, values)

	good := recallThresholdEvidence{Threshold: 100, Eligible: 100, K: 50, Samples: 31, Exact: 31,
		P95: recallQuantile([]time.Duration{50 * time.Millisecond}, 95), MeanExact: new(1.0), DefaultStrict: new(.94)}
	bad := good
	bad.Threshold, bad.TimedOut = 1000, 1
	require.Equal(t, 100, recommendRecallThreshold([]recallThresholdEvidence{bad, good}))
	require.Zero(t, recommendRecallThreshold([]recallThresholdEvidence{bad}))
	require.Zero(t, recommendRecallThreshold(nil))
	good.DefaultStrict = new(.95)
	require.Zero(t, recommendRecallThreshold([]recallThresholdEvidence{good}))
	good.DefaultStrict, good.Eligible = new(.94), 0
	require.Zero(t, recommendRecallThreshold([]recallThresholdEvidence{good}))
	good.Eligible = 101
	require.Zero(t, recommendRecallThreshold([]recallThresholdEvidence{good}))
}

type recallHit struct {
	ID       string
	Distance float64
	Eligible bool
}

type recallScore struct {
	Returned     int      `json:"returned"`
	EligibleHits int      `json:"eligible_hits"`
	Unauthorized int      `json:"unauthorized_hits"`
	Strict       *float64 `json:"strict_recall"`
	TieAware     *float64 `json:"tie_aware_recall"`
	FillRate     *float64 `json:"fill_rate"`
}

func scoreRecall(exact, candidates []recallHit, k, eligible int) recallScore {
	out := recallScore{Returned: len(candidates)}
	seen := make(map[string]struct{}, len(candidates))
	want := make(map[string]struct{}, len(exact))
	cutoffSlots := 0
	cutoff := 0.0
	if len(exact) > 0 {
		cutoff = exact[len(exact)-1].Distance
	}
	for _, hit := range exact {
		want[hit.ID] = struct{}{}
		if hit.Distance == cutoff {
			cutoffSlots++
		}
	}
	var strict, closer, tied int
	for _, hit := range candidates {
		if _, duplicate := seen[hit.ID]; duplicate {
			continue
		}
		seen[hit.ID] = struct{}{}
		if !hit.Eligible {
			out.Unauthorized++
			continue
		}
		out.EligibleHits++
		_, expected := want[hit.ID]
		if expected {
			strict++
		}
		switch {
		case expected && hit.Distance < cutoff:
			closer++
		case hit.Distance == cutoff:
			tied++
		}
	}
	if k > 0 {
		out.FillRate = new(float64(min(out.EligibleHits, k)) / float64(k))
	}
	if denom := min(k, eligible); denom > 0 {
		out.Strict = new(float64(strict) / float64(denom))
		out.TieAware = new(float64(closer+min(tied, cutoffSlots)) / float64(denom))
	}
	return out
}

func recallQuantile(values []time.Duration, percent int) *time.Duration {
	if len(values) == 0 {
		return nil
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	rank := (len(sorted)*percent + 99) / 100
	return new(sorted[max(1, rank)-1])
}

type recallThresholdEvidence struct {
	Threshold     int
	Eligible      int
	K             int
	Samples       int
	Exact         int
	TimedOut      int
	P95           *time.Duration
	MeanExact     *float64
	DefaultStrict *float64
}

func recommendRecallThreshold(rows []recallThresholdEvidence) int {
	type verdict struct{ failed, reason bool }
	verdicts := make(map[int]verdict)
	for _, row := range rows {
		if row.Threshold <= 0 || row.K != 50 || row.Eligible == 0 || row.Eligible > row.Threshold {
			continue
		}
		v := verdicts[row.Threshold]
		v.failed = v.failed || row.Samples != recallQueryRows-1 || row.Exact != row.Samples || row.TimedOut != 0 ||
			row.P95 == nil || *row.P95 > 50*time.Millisecond || row.MeanExact == nil || *row.MeanExact != 1
		v.reason = v.reason || row.DefaultStrict != nil && *row.DefaultStrict < .95
		verdicts[row.Threshold] = v
	}
	largest := 0
	for threshold, v := range verdicts {
		if !v.failed && v.reason {
			largest = max(largest, threshold)
		}
	}
	return largest
}
