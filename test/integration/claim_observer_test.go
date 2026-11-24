package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type leasedClaims struct{ job domain.Job }

func (s leasedClaims) ClaimJob(context.Context, string, time.Duration, int) (*domain.Job, error) {
	return &s.job, nil
}

func (s leasedClaims) ClaimJobForFixtureStore(context.Context, string, time.Duration, int, string) (*domain.Job, error) {
	return &s.job, nil
}

// The worker leases work through the fixture-aware method when the store
// offers it, so that is the call the observer must record.
func TestClaimObserverRecordsFixtureAwareLease(t *testing.T) {
	want := domain.Job{LeaseOwner: "same-worker", Attempts: 1, LeaseToken: "token", Incarnation: "incarnation"}
	claimed := make(chan domain.Job, 1)
	var store domain.JobStore = observedRevisionJobs{claimed: claimed, source: leasedClaims{want}}
	fixture, ok := store.(domain.FixtureJobStore)
	require.True(t, ok)
	job, err := fixture.ClaimJobForFixtureStore(t.Context(), "same-worker", time.Second, 3, "")
	require.NoError(t, err)
	require.Equal(t, want, *job)
	require.Equal(t, []domain.Job{want}, []domain.Job{<-claimed})

	job, err = store.ClaimJob(t.Context(), "same-worker", time.Second, 3)
	require.NoError(t, err)
	require.Equal(t, want, *job)
	require.Equal(t, want, <-claimed)
}

func TestClaimObserverYieldsToCancellation(t *testing.T) {
	claimed := make(chan domain.Job)
	store := observedRevisionJobs{claimed: claimed, source: leasedClaims{}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	job, err := store.ClaimJobForFixtureStore(ctx, "same-worker", time.Second, 3, "")
	require.NoError(t, err)
	require.NotNil(t, job)
}
