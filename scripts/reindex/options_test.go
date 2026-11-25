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
	opts, err = parse(t, "-attest-source", "-profile", "profile.json")
	require.NoError(t, err)
	require.Equal(t, actionAttest, opts.action())
	require.False(t, opts.Apply, "an attestation is only reported unless -apply is given")
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
		"attest without profile":       {"-attest-source"},
		"attest with generation":       {"-attest-source", "-profile", "p.json", "-generation", "g2"},
		"bad cursor":                   {"-generation", "g2", "-profile", "p.json", "-after", "not-a-uuid"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parse(t, args...)
			require.Error(t, err)
		})
	}
	opts, err := parse(t, "-generation", "g2", "-profile", "p.json", "-target-store", "pinecone", "-target-index", "quarry-g2")
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

func FuzzParseOptions(f *testing.F) {
	for _, seed := range []string{
		"-generation g2 -profile p.json",
		"-generation g2 -profile p.json -apply -max-embed-calls 10",
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
