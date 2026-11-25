package main

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func parse(t *testing.T, args ...string) (options, error) {
	t.Helper()
	return parseOptions(args, io.Discard)
}

func TestDefaultIsAReadOnlyInventory(t *testing.T) {
	opts, err := parse(t, "-generation", "g2", "-profile", "profile.json")
	require.NoError(t, err)
	require.Equal(t, actionInventory, opts.action())
	require.False(t, opts.Apply)
	require.Equal(t, 1000, opts.MaxDocuments)
	require.Zero(t, opts.MaxEmbedCalls, "model calls are opt in")
	require.Equal(t, 30*time.Minute, opts.Timeout)
}

func TestApplyStagesAndEveryWriteNeedsIt(t *testing.T) {
	opts, err := parse(t, "-generation", "g2", "-profile", "profile.json", "-apply")
	require.NoError(t, err)
	require.Equal(t, actionStage, opts.action())
	for _, flag := range []string{"-freeze", "-thaw", "-activate", "-verify"} {
		args := []string{flag}
		switch flag {
		case "-freeze", "-activate":
			args = append(args, "-generation", "g2")
		case "-verify":
			args = append(args, "-generation", "g2", "-sample-size", "10", "-min-recall", "0.9", "-max-p95", "1s")
		}
		got, parseErr := parse(t, args...)
		require.NoError(t, parseErr, flag)
		require.False(t, got.Apply, "%s previews unless -apply is given", flag)
	}
}

func TestModesAreExclusive(t *testing.T) {
	for _, args := range [][]string{
		{"-verify", "-freeze"}, {"-freeze", "-thaw"}, {"-activate", "-verify"}, {"-attest-source", "-thaw"}, {"-freeze", "-activate"},
	} {
		_, err := parse(t, append(args, "-generation", "g2", "-profile", "p.json")...)
		require.ErrorContains(t, err, "one of", args)
	}
}

func TestAppliedVerificationRequiresFiniteRecallThreshold(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"1", true}, {"0.5", true}, {"0", false}, {"1.2", false},
		{"+Inf", false}, {"-Inf", false}, {"NaN", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			opts, err := parse(t, "-generation", "g2", "-verify", "-apply", "-sample-size", "1", "-min-recall", tc.value, "-max-p95", "1s")
			if !tc.valid {
				require.ErrorContains(t, err, "-min-recall")
				return
			}
			require.NoError(t, err)
			v := verifier{opts: opts}
			report := verifyReport{Expected: expectedCounts{Chunks: 1}, Sample: sampleReport{Requested: 1, Queries: 1, MinRecall: opts.MinRecall}}
			v.judge(&report)
			require.False(t, report.Passed)
		})
	}
}

func TestRequiredSelectors(t *testing.T) {
	for name, args := range map[string][]string{
		"inventory without profile":    {"-generation", "g2"},
		"inventory without generation": {"-profile", "p.json"},
		"bad generation":               {"-generation", "G2", "-profile", "p.json"},
		"positional":                   {"-generation", "g2", "-profile", "p.json", "extra"},
		"pinecone without index":       {"-generation", "g2", "-profile", "p.json", "-target-store", "pinecone"},
		"postgres with index":          {"-generation", "g2", "-profile", "p.json", "-target-store", "postgres", "-target-index", "idx"},
		"unknown store":                {"-generation", "g2", "-profile", "p.json", "-target-store", "memory"},
		"freeze without generation":    {"-freeze"},
		"activate without generation":  {"-activate"},
		"verify without generation":    {"-verify", "-sample-size", "1", "-min-recall", "0.5", "-max-p95", "1s"},
		"attest without profile":       {"-attest-source"},
		"attest with generation":       {"-attest-source", "-profile", "p.json", "-generation", "g2"},
		"bad cursor":                   {"-generation", "g2", "-profile", "p.json", "-after", "not-a-uuid"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parse(t, args...)
			require.Error(t, err)
		})
	}
	opts, err := parse(t, "-thaw")
	require.NoError(t, err)
	require.Equal(t, actionThaw, opts.action())
	opts, err = parse(t, "-generation", "g2", "-profile", "p.json", "-target-store", "pinecone", "-target-index", "quarry-g2")
	require.NoError(t, err)
	require.Equal(t, "pinecone", opts.TargetStore)
	opts, err = parse(t, "-attest-source", "-profile", "p.json")
	require.NoError(t, err)
	require.Equal(t, actionAttest, opts.action())
}

func TestBoundsAreValidated(t *testing.T) {
	base := []string{"-generation", "g2", "-profile", "p.json"}
	for _, bad := range [][]string{
		{"-max-documents", "0"}, {"-max-chunks", "0"}, {"-max-bytes", "0"}, {"-max-embed-calls", "-1"},
		{"-batch", "0"}, {"-batch", "1001"}, {"-timeout", "0s"}, {"-timeout", "-1s"}, {"-page", "0"}, {"-page", "1001"},
	} {
		_, err := parse(t, append(append([]string{}, base...), bad...)...)
		require.Error(t, err, bad)
	}
	opts, err := parse(t, append(append([]string{}, base...), "-max-embed-calls", "500", "-batch", "1000", "-after", "8a3e792c-5df6-4b6e-a612-4f7db4959db2")...)
	require.NoError(t, err)
	require.Equal(t, 500, opts.MaxEmbedCalls)
	require.Equal(t, "8a3e792c-5df6-4b6e-a612-4f7db4959db2", opts.After)
}

func TestVerificationThresholdsMustBeDeclared(t *testing.T) {
	base := []string{"-verify", "-generation", "g2"}
	for name, args := range map[string][]string{
		"none":          {},
		"no recall":     {"-sample-size", "10", "-max-p95", "1s"},
		"no latency":    {"-sample-size", "10", "-min-recall", "0.9"},
		"no sample":     {"-min-recall", "0.9", "-max-p95", "1s"},
		"recall zero":   {"-sample-size", "10", "-min-recall", "0", "-max-p95", "1s"},
		"recall high":   {"-sample-size", "10", "-min-recall", "1.1", "-max-p95", "1s"},
		"latency zero":  {"-sample-size", "10", "-min-recall", "0.9", "-max-p95", "0s"},
		"sample large":  {"-sample-size", "100001", "-min-recall", "0.9", "-max-p95", "1s"},
		"top-k zero":    {"-sample-size", "10", "-min-recall", "0.9", "-max-p95", "1s", "-top-k", "0"},
		"scope zero":    {"-sample-size", "10", "-min-recall", "0.9", "-max-p95", "1s", "-scope-documents", "0"},
		"top-k too big": {"-sample-size", "10", "-min-recall", "0.9", "-max-p95", "1s", "-top-k", "101"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parse(t, append(append([]string{}, base...), args...)...)
			require.Error(t, err)
		})
	}
	opts, err := parse(t, append(base, "-sample-size", "50", "-min-recall", "0.95", "-max-p95", "250ms")...)
	require.NoError(t, err)
	require.Equal(t, 50, opts.SampleSize)
	require.InDelta(t, 0.95, opts.MinRecall, 1e-9)
	require.Equal(t, 250*time.Millisecond, opts.MaxP95)
	require.Equal(t, 10, opts.TopK)
	require.Equal(t, 5, opts.ScopeDocuments)
}

func TestQuiesceCannotBeShorterThanTheDeleteDeadline(t *testing.T) {
	opts, err := parse(t, "-freeze", "-generation", "g2")
	require.NoError(t, err)
	require.Equal(t, domain.WriteRequestTimeout, opts.Quiesce)
	_, err = parse(t, "-freeze", "-generation", "g2", "-quiesce", "10s")
	require.ErrorContains(t, err, "quiesce")
	opts, err = parse(t, "-activate", "-generation", "g2", "-quiesce", "5m")
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, opts.Quiesce)
}

func FuzzParseOptions(f *testing.F) {
	for _, seed := range []string{
		"-generation g2 -profile p.json",
		"-generation g2 -profile p.json -apply -max-embed-calls 10",
		"-verify -generation g2 -sample-size 5 -min-recall 0.9 -max-p95 1s",
		"-freeze -generation g2 -quiesce 2m",
		"-thaw",
		"-attest-source -profile p.json -apply",
		"-generation G2 -profile p.json",
		"-generation g2 -profile p.json -after 8a3e792c-5df6-4b6e-a612-4f7db4959db2",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		if len(line) > 512 {
			t.Skip()
		}
		opts, err := parseOptions(strings.Fields(line), io.Discard)
		if err != nil {
			return
		}
		// Anything accepted is bounded and names a valid generation when it names one.
		require.Positive(t, opts.MaxDocuments)
		require.Positive(t, opts.MaxChunks)
		require.GreaterOrEqual(t, opts.MaxEmbedCalls, 0)
		require.Positive(t, opts.Timeout)
		require.LessOrEqual(t, opts.Batch, 1000)
		if opts.Generation != "" {
			require.True(t, domain.ValidGenerationName(opts.Generation))
		}
		if !opts.Apply {
			require.NotEqual(t, actionStage, opts.action(), "staging needs -apply")
		}
	})
}
