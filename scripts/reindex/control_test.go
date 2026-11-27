package main

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/stretchr/testify/require"
)

func TestSetActivationReadiness(t *testing.T) {
	rep := controlReport{Generation: "g2", Reasons: []string{"previous reason"}}
	snapshot := pgstore.ActivationReadiness{
		Active: "g1", FreezeGeneration: "g3", TargetState: pgstore.StateVerified, PendingWrite: true,
		Quiesce: pgstore.Quiesce{Frozen: true, RunningJobs: 2, OpenDeletions: 1},
	}
	setActivationReadiness(&rep, snapshot, time.Minute)
	require.Equal(t, "g1", rep.Active)
	require.Equal(t, "g3", rep.FreezeGeneration)
	require.True(t, rep.Frozen)
	require.Equal(t, snapshot.Quiesce, *rep.Quiesce)
	require.Equal(t, []string{
		"the generation has an unconfirmed target write",
		"writers are not frozen for this generation",
		"2 in-flight jobs must finish before activation",
		"1 open deletions must finish before activation",
	}, rep.Reasons)

	snapshot.Active = rep.Generation
	setActivationReadiness(&rep, snapshot, time.Minute)
	require.Equal(t, []string{`generation "g2" is already active`}, rep.Reasons)
}

func TestCorpusDigest(t *testing.T) {
	first := pgstore.DocumentSnapshot{DocumentRevision: pgstore.DocumentRevision{ID: "a", Revision: "one"}}
	second := pgstore.DocumentSnapshot{DocumentRevision: pgstore.DocumentRevision{ID: "b", Revision: "two"}}
	source := newSnapshotsCorpus(second, first)
	opts := defaultOptions()
	opts.MaxDocuments = 2
	for _, page := range []int{1, 1000} {
		opts.Page = page
		got, err := corpusDigest(t.Context(), source, opts)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte("a|one\nb|two\n"))), got)

		got, err = corpusDigest(t.Context(), newSnapshotsCorpus(), opts)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(nil)), got)
	}

	opts.MaxDocuments = 1
	got, err := corpusDigest(t.Context(), source, opts)
	require.ErrorIs(t, err, errActivationCorpusLimit)
	require.Empty(t, got)
}
