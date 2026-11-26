package main

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// retiredStore holds one tombstone and applies the transitions a sweep makes.
type retiredStore struct {
	task      domain.VectorCleanupTask
	claimErr  error
	claimed   bool
	drainSeen time.Duration
}

func (*retiredStore) PersistVectorManifest(context.Context, domain.Job, domain.VectorManifest) error {
	return nil
}
func (*retiredStore) GetVectorManifest(context.Context, string) (*domain.VectorManifest, error) {
	return nil, nil
}
func (*retiredStore) ListDocumentVectorManifests(context.Context, domain.DocumentID, string) ([]domain.VectorManifest, error) {
	return nil, nil
}

func (s *retiredStore) ClaimVectorCleanup(_ context.Context, owner string, _, drain time.Duration) (*domain.VectorCleanupTask, error) {
	s.drainSeen = drain
	if s.claimErr != nil || s.claimed {
		return nil, s.claimErr
	}
	s.claimed = true
	token := "token"
	claimed := s.task
	claimed.LeaseOwner, claimed.LeaseToken = owner, &token
	return &claimed, nil
}
func (*retiredStore) RenewVectorCleanup(context.Context, domain.VectorCleanupTask, time.Duration) (bool, error) {
	return true, nil
}
func (s *retiredStore) AdvanceVectorCleanup(_ context.Context, _ domain.VectorCleanupTask, offset int) (bool, error) {
	s.task.NextOffset = offset
	return true, nil
}
func (s *retiredStore) DeferVectorCleanup(_ context.Context, _ domain.VectorCleanupTask, state string, _ time.Duration, _ int, message string) (bool, error) {
	s.task.State, s.task.LastError = state, message
	return true, nil
}
func (*retiredStore) CompleteVectorCleanup(context.Context, domain.VectorCleanupTask) (bool, error) {
	return true, nil
}
func (*retiredStore) ReleaseVectorCleanup(context.Context, domain.VectorCleanupTask) error {
	return nil
}
func (s *retiredStore) ListPendingVectorCleanups(context.Context, int) ([]domain.VectorCleanupTask, error) {
	return []domain.VectorCleanupTask{s.task}, nil
}

// stubbornVectors reports every ID as still present after a delete.
type stubbornVectors struct{ ids []string }

func (*stubbornVectors) DeleteVectorIDs(context.Context, string, []string) error { return nil }
func (v *stubbornVectors) FetchPresentVectorIDs(context.Context, string, []string) ([]string, error) {
	return slices.Clone(v.ids), nil
}
func (*stubbornVectors) ListPrefixVectorIDs(context.Context, string, string, int, string) (ids []string, nextToken string, supported bool, err error) {
	return nil, "", false, nil
}

func revisionRun(t *testing.T, store *retiredStore, apply bool) (report, error) {
	t.Helper()
	idx := &reconcileFakeIndex{refs: []vectorRef{{ID: "legacy-orphan", DocID: missingID}}}
	return reconcileRevisions(t.Context(), store, &stubbornVectors{ids: store.task.VectorIDs}, idx, idx.deleteNamespace, noDocuments,
		options{Apply: apply, Limit: 10, Page: 5}, sweepSettings{DrainDelay: 45 * time.Second}, &bytes.Buffer{})
}

func TestApplyReportsResidueAndStillScansLegacyOrphans(t *testing.T) {
	store := &retiredStore{task: domain.VectorCleanupTask{RevisionID: "revision", DocumentID: storedID, VectorIDs: []string{"vector"}, State: "pending"}}
	result, err := revisionRun(t, store, true)
	require.NoError(t, err, "unresolved residue is an outcome to report, not a reason to stop")
	require.Equal(t, 1, result.UnresolvedRevisions)
	require.Equal(t, 1, result.UnresolvedVectors, "the cursor sits at the end, but the recorded vector was never verified gone")
	require.Contains(t, result.SweepError, "unresolved residue")
	require.Equal(t, 1, result.OrphanVectors, "the legacy scan still runs")
	require.Contains(t, result.Sample, "vector:legacy-orphan")
	require.Contains(t, result.Sample, "retired:revision")
	require.Equal(t, 45*time.Second, store.drainSeen, "the configured drain window reaches the claim")
}

func TestApplyStopsOnlyOnAFailureThatAbortsTheSweep(t *testing.T) {
	store := &retiredStore{task: domain.VectorCleanupTask{RevisionID: "revision", VectorIDs: []string{"vector"}}, claimErr: errors.New("claim unavailable")}
	_, err := revisionRun(t, store, true)
	require.ErrorContains(t, err, "claim unavailable")

	store = &retiredStore{task: domain.VectorCleanupTask{RevisionID: "revision", VectorIDs: []string{"vector"}}, claimErr: &pgconn.PgError{Code: "42P01"}}
	result, err := revisionRun(t, store, true)
	require.NoError(t, err, "a database that predates the tables has nothing to sweep")
	require.Equal(t, 1, result.OrphanVectors)
}

func TestDryRunReportsRetiredRevisionsWithoutSweeping(t *testing.T) {
	store := &retiredStore{task: domain.VectorCleanupTask{RevisionID: "revision", VectorIDs: []string{"a", "b"}, NextOffset: 1, State: "unresolved"}}
	result, err := revisionRun(t, store, false)
	require.NoError(t, err)
	require.False(t, store.claimed)
	require.Equal(t, 1, result.RetiredRevisions)
	require.Equal(t, 1, result.RetiredVectors)
	require.Equal(t, 1, result.UnresolvedRevisions)
	require.Equal(t, 2, result.UnresolvedVectors)
	require.Empty(t, result.SweepError)
}

type trackingGate struct {
	err      error
	acquired int
	released int
}

func (g *trackingGate) AcquireWriter(context.Context) (func(), error) {
	if g.err != nil {
		return nil, g.err
	}
	g.acquired++
	return func() { g.released++ }, nil
}

func TestRunMaintenanceGatesRevisionApplySweep(t *testing.T) {
	applyOpts := options{Apply: true, Limit: 10, Page: 5}
	for _, blockedErr := range []error{domain.ErrWritersFrozen, domain.ErrGenerationMismatch} {
		store := &retiredStore{task: domain.VectorCleanupTask{RevisionID: "revision", VectorIDs: []string{"vector"}, State: "pending"}}
		err := runMaintenance(t.Context(), &trackingGate{err: blockedErr}, applyOpts, func() error {
			_, runErr := revisionRun(t, store, true)
			return runErr
		})
		require.ErrorIs(t, err, blockedErr)
		require.False(t, store.claimed, "blocked gate must stop the revision sweep before claiming work")
	}

	gate := &trackingGate{}
	store := &retiredStore{task: domain.VectorCleanupTask{RevisionID: "revision", VectorIDs: []string{"vector"}, State: "pending"}, claimErr: errors.New("claim failed")}
	err := runMaintenance(t.Context(), gate, applyOpts, func() error {
		_, runErr := revisionRun(t, store, true)
		return runErr
	})
	require.ErrorContains(t, err, "claim failed")
	require.Equal(t, 1, gate.acquired)
	require.Equal(t, 1, gate.released, "writer gate releases even when the sweep aborts")
}
