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

func (s leasedClaims) ClaimPDFProfilesForFixtureStore(context.Context, string, time.Duration, int, string, bool, bool) (*domain.Job, error) {
	return &s.job, nil
}

type profileClaimSource struct {
	leasedClaims
	claim func(context.Context, string, time.Duration, int, string, bool, bool) (*domain.Job, error)
}

func (s profileClaimSource) ClaimPDFProfilesForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string, mixed, layout bool) (*domain.Job, error) {
	return s.claim(ctx, owner, lease, maxAttempts, vectorIdentity, mixed, layout)
}

func TestClaimObserverRecordsProfileAwareLeaseOnce(t *testing.T) {
	want := domain.Job{LeaseOwner: "same-worker", Attempts: 1, LeaseToken: "token", Incarnation: "incarnation"}
	claimed := make(chan domain.Job, 1)
	ctx := t.Context()
	calls := 0
	source := profileClaimSource{
		leasedClaims: leasedClaims{want},
		claim: func(gotCtx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string, mixed, layout bool) (*domain.Job, error) {
			calls++
			require.Equal(t, ctx, gotCtx)
			require.Equal(t, want.LeaseOwner, owner)
			require.Equal(t, time.Second, lease)
			require.Equal(t, 3, maxAttempts)
			require.Equal(t, "fixture-target", vectorIdentity)
			require.False(t, mixed)
			require.True(t, layout)
			return &want, nil
		},
	}
	store := observedRevisionJobs{claimed: claimed, source: source, claimedOnce: new(bool)}
	job, err := store.ClaimPDFProfilesForFixtureStore(ctx, want.LeaseOwner, time.Second, 3, "fixture-target", false, true)
	require.NoError(t, err)
	require.Equal(t, &want, job)
	require.Len(t, claimed, 1)
	require.Equal(t, want, <-claimed)

	job, err = store.ClaimPDFProfilesForFixtureStore(ctx, want.LeaseOwner, time.Second, 3, "fixture-target", false, true)
	require.NoError(t, err)
	require.Nil(t, job)
	require.Equal(t, 1, calls)
	require.Empty(t, claimed)
}

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
	job, err := store.ClaimPDFProfilesForFixtureStore(ctx, "same-worker", time.Second, 3, "", false, false)
	require.NoError(t, err)
	require.NotNil(t, job)
}
