package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiveOptionsRequireExplicitCapsAndMutations(t *testing.T) {
	base := []string{"--live", "--collection", "evaluation", "--targets", "targets.json", "--ledger-dir", "ledgers"}
	for _, tc := range [][]string{
		base, append([]string{"--create"}, base...), append([]string{"--max-requests=1"}, base...),
		append([]string{"--create", "--max-requests=1", "--poll-deadline=24h"}, base...),
		{"--create"}, {"--delete"}, {"--cleanup=ledger.json", "--live", "--max-requests=1"},
		{"--max-spend=-1"}, {"--max-spend=NaN"},
	} {
		_, err := parseOptions(tc, "1")
		require.Error(t, err, tc)
	}
	args := append([]string{"--create", "--max-requests=1"}, base...)
	_, err := parseOptions(args, "")
	require.Error(t, err)
	opts, err := parseOptions(args, "1")
	require.NoError(t, err)
	require.Equal(t, 1, opts.MaxRequests)
}

func FuzzDecodeTargetSettings(f *testing.F) {
	f.Add("[]")
	f.Add("[{}]")
	f.Add(`[{"name":"staging","dimensions":128}]`)
	f.Add(`[{"service_token":"private-value"}]`)
	f.Fuzz(func(_ *testing.T, data string) {
		var cfg []targetConfig
		err := decodeLimited(strings.NewReader(data), &cfg)
		if err == nil {
			for _, target := range cfg {
				_ = target.validate()
			}
		}
	})
}
