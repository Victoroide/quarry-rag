package worker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/stretchr/testify/require"
)

type fakeManifestStore struct {
	mu      sync.Mutex
	tasks   map[string]*domain.VectorCleanupTask
	leaseOK bool
}

func newFakeManifestStore(tasks ...domain.VectorCleanupTask) *fakeManifestStore {
	m := make(map[string]*domain.VectorCleanupTask, len(tasks))
	for i := range tasks {
		cp := tasks[i]
		m[cp.RevisionID] = &cp
	}
	return &fakeManifestStore{tasks: m, leaseOK: true}
}

func (*fakeManifestStore) PersistVectorManifest(context.Context, domain.Job, domain.VectorManifest) error {
	return nil
}
func (*fakeManifestStore) GetVectorManifest(context.Context, string) (*domain.VectorManifest, error) {
	return nil, nil
}
func (*fakeManifestStore) ListDocumentVectorManifests(context.Context, domain.DocumentID, string) ([]domain.VectorManifest, error) {
	return nil, nil
}

func (s *fakeManifestStore) ClaimVectorCleanup(_ context.Context, owner string, _, drain time.Duration) (*domain.VectorCleanupTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var keys []string
	for k, task := range s.tasks {
		if task.LeaseOwner == "" && !task.RetiredAt.Add(drain).After(now) && !task.NextAttemptAt.After(now) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	if len(keys) == 0 {
		return nil, nil
	}
	task := s.tasks[keys[0]]
	tok := "tok-" + task.RevisionID
	task.LeaseOwner = owner
	task.LeaseToken = &tok
	cp := *task
	cp.VectorIDs = slices.Clone(task.VectorIDs)
	return &cp, nil
}

// makeDue moves a deferred task's retry time into the past, so the next pass
// takes exactly one more transition without waiting on a clock.
func (s *fakeManifestStore) makeDue(revisionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[revisionID].NextAttemptAt = time.Now().Add(-time.Second)
}

func (s *fakeManifestStore) RenewVectorCleanup(context.Context, domain.VectorCleanupTask, time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaseOK, nil
}

func (s *fakeManifestStore) AdvanceVectorCleanup(_ context.Context, task domain.VectorCleanupTask, nextOffset int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.leaseOK {
		return false, nil
	}
	stored, ok := s.tasks[task.RevisionID]
	if !ok || stored.LeaseOwner != task.LeaseOwner {
		return false, nil
	}
	stored.NextOffset = nextOffset
	return true, nil
}

func (s *fakeManifestStore) DeferVectorCleanup(_ context.Context, task domain.VectorCleanupTask, state string, delay time.Duration, checks int, lastError string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.leaseOK {
		return false, nil
	}
	stored, ok := s.tasks[task.RevisionID]
	if !ok || stored.LeaseOwner != task.LeaseOwner {
		return false, nil
	}
	stored.State = state
	stored.NextAttemptAt = time.Now().Add(delay)
	stored.ConvergenceChecks = checks
	stored.Attempts++
	stored.LastError = lastError
	stored.LeaseOwner = ""
	stored.LeaseToken = nil
	return true, nil
}

func (s *fakeManifestStore) CompleteVectorCleanup(_ context.Context, task domain.VectorCleanupTask) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.leaseOK {
		return false, nil
	}
	stored, ok := s.tasks[task.RevisionID]
	if !ok || stored.LeaseOwner != task.LeaseOwner {
		return false, nil
	}
	delete(s.tasks, task.RevisionID)
	return true, nil
}

func (s *fakeManifestStore) ReleaseVectorCleanup(_ context.Context, task domain.VectorCleanupTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stored, ok := s.tasks[task.RevisionID]; ok && stored.LeaseOwner == task.LeaseOwner {
		stored.LeaseOwner = ""
		stored.LeaseToken = nil
	}
	return nil
}

type fakePresenceVectors struct {
	mu            sync.Mutex
	present       map[string]bool
	deleteErr     error
	fetchErr      error
	listSupported bool
	deletedCalls  [][]string
	reappear      map[string]int
}

func (v *fakePresenceVectors) DeleteVectorIDs(_ context.Context, _ string, ids []string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.deleteErr != nil {
		return v.deleteErr
	}
	v.deletedCalls = append(v.deletedCalls, slices.Clone(ids))
	for _, id := range ids {
		if v.reappear[id] > 0 {
			v.reappear[id]--
			continue
		}
		delete(v.present, id)
	}
	return nil
}

func (v *fakePresenceVectors) FetchPresentVectorIDs(_ context.Context, _ string, ids []string) ([]string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.fetchErr != nil {
		return nil, v.fetchErr
	}
	var out []string
	for _, id := range ids {
		if v.present[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (v *fakePresenceVectors) ListPrefixVectorIDs(_ context.Context, _, prefix string, _ int, _ string) (ids []string, nextToken string, supported bool, err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.listSupported {
		return nil, "", false, nil
	}
	var out []string
	for id := range v.present {
		if len(id) >= len(prefix) && id[:len(prefix)] == prefix {
			out = append(out, id)
		}
	}
	return out, "", true, nil
}

func TestSweeperRespectsPageBudgetAndPersistsCursor(t *testing.T) {
	task := domain.VectorCleanupTask{
		RevisionID: "rev-1",
		DocumentID: "doc-1",
		IDPrefix:   "doc-1#rev-1#",
		VectorIDs:  []string{"doc-1#rev-1#0", "doc-1#rev-1#1", "doc-1#rev-1#2", "doc-1#rev-1#3", "doc-1#rev-1#4"},
		State:      "pending",
	}
	store := newFakeManifestStore(task)
	vecs := &fakePresenceVectors{present: map[string]bool{
		"doc-1#rev-1#0": true, "doc-1#rev-1#1": true, "doc-1#rev-1#2": true,
		"doc-1#rev-1#3": true, "doc-1#rev-1#4": true,
	}}
	sweeper := NewSweeper(store, vecs, SweepOptions{
		Owner: "sweep-1", PageSize: 2, Budget: 3, ConvergenceDelay: 0, HasWriteFence: true,
	})

	rep1, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, rep1.VectorsDeleted)
	require.Zero(t, rep1.TasksCompleted)
	require.Equal(t, 3, store.tasks["rev-1"].NextOffset)

	rep2, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, rep2.VectorsDeleted)
	require.Empty(t, store.tasks)
	require.Len(t, vecs.deletedCalls, 3) // pages of 2, 1, 2
	require.Empty(t, vecs.present)

	// Repeated cleanup is idempotent.
	rep3, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, rep3.TasksClaimed)
	require.Zero(t, rep3.VectorsDeleted)
}

func TestSweeperRetainsTombstoneOnUnresolvedResidueAndPodIndex(t *testing.T) {
	task := domain.VectorCleanupTask{
		RevisionID: "rev-pod",
		DocumentID: "doc-pod",
		IDPrefix:   "doc-pod#rev-pod#",
		VectorIDs:  []string{"doc-pod#rev-pod#0", "doc-pod#rev-pod#1"},
		State:      "pending",
	}
	store := newFakeManifestStore(task)
	vecs := &fakePresenceVectors{
		present:       map[string]bool{"doc-pod#rev-pod#0": true, "doc-pod#rev-pod#1": true},
		listSupported: false, // pod index without List
		reappear:      map[string]int{"doc-pod#rev-pod#1": 1},
	}
	svc := metrics.NewService(nil)
	// Delays far beyond the test make every pass take exactly one transition.
	sweeper := NewSweeper(store, vecs, SweepOptions{
		Owner: "sweep-pod", PageSize: 10, Budget: 10,
		ConvergenceDelay: time.Hour, RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour,
		HasWriteFence: false, Metrics: svc,
	})

	// Pass 1: deletes both IDs, transitions to verifying.
	rep1, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep1.TasksDeferred)
	require.Equal(t, "verifying", store.tasks["rev-pod"].State)

	// A pass before the delay elapses leaves the task alone.
	idle, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, idle.TasksClaimed)

	// Pass 2: the convergence check finds residue, deletes it again and marks the task unresolved.
	store.makeDue("rev-pod")
	rep2, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep2.TasksUnresolved)
	require.Equal(t, 1, rep2.UnresolvedVectors)
	require.Contains(t, rep2.LastError, "unresolved residue")
	require.Equal(t, "unresolved", store.tasks["rev-pod"].State)

	// Once the backoff elapses the provider has settled: the tombstone verifies, then completes.
	store.makeDue("rev-pod")
	rep3, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep3.TasksDeferred)
	require.Equal(t, "verifying", store.tasks["rev-pod"].State)

	store.makeDue("rev-pod")
	rep4, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep4.TasksCompleted)
	require.Empty(t, store.tasks)
}

func TestSweeperClaimsOnlyAfterTheConfiguredDrain(t *testing.T) {
	task := domain.VectorCleanupTask{
		RevisionID: "rev-drain",
		DocumentID: "doc-drain",
		VectorIDs:  []string{"doc-drain#rev-drain#0"},
		State:      "pending",
		RetiredAt:  time.Now(),
	}
	store := newFakeManifestStore(task)
	vecs := &fakePresenceVectors{present: map[string]bool{"doc-drain#rev-drain#0": true}}
	sweeper := NewSweeper(store, vecs, SweepOptions{
		Owner: "sweep-drain", PageSize: 10, Budget: 10, DrainDelay: time.Hour, HasWriteFence: true,
	})

	held, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, held.TasksClaimed, "a write that was in flight may still land inside the drain window")
	require.Empty(t, vecs.deletedCalls)

	store.mu.Lock()
	store.tasks["rev-drain"].RetiredAt = time.Now().Add(-2 * time.Hour)
	store.mu.Unlock()
	done, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, done.TasksCompleted)
	require.Empty(t, vecs.present)
}

func TestSweeperBacksOffOnOutageAndStopsOnLeaseLoss(t *testing.T) {
	task := domain.VectorCleanupTask{
		RevisionID: "rev-err",
		DocumentID: "doc-err",
		VectorIDs:  []string{"doc-err#rev-err#0"},
		State:      "pending",
	}
	store := newFakeManifestStore(task)
	vecs := &fakePresenceVectors{present: map[string]bool{"doc-err#rev-err#0": true}, deleteErr: errors.New("pinecone outage")}
	sweeper := NewSweeper(store, vecs, SweepOptions{
		Owner: "sweep-err", PageSize: 10, Budget: 10, RetryBaseDelay: time.Minute,
	})

	rep, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksDeferred)
	require.Contains(t, rep.LastError, "pinecone outage")
	require.Equal(t, 1, store.tasks["rev-err"].Attempts)

	// Lease loss stops without advancing or completing.
	store2 := newFakeManifestStore(task)
	store2.leaseOK = false
	vecs2 := &fakePresenceVectors{present: map[string]bool{"doc-err#rev-err#0": true}}
	sweeper2 := NewSweeper(store2, vecs2, SweepOptions{Owner: "sweep-lost", PageSize: 10, Budget: 10})
	rep2, err := sweeper2.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, rep2.TasksCompleted)
	require.Zero(t, store2.tasks["rev-err"].NextOffset)
}

type telemetrySweepStore struct {
	*fakeManifestStore
	err       error
	lastError string
}

func (s *telemetrySweepStore) RenewVectorCleanup(context.Context, domain.VectorCleanupTask, time.Duration) (bool, error) {
	return false, s.err
}
func (s *telemetrySweepStore) ReleaseVectorCleanup(context.Context, domain.VectorCleanupTask) error {
	return s.err
}
func (s *telemetrySweepStore) DeferVectorCleanup(_ context.Context, _ domain.VectorCleanupTask, _ string, _ time.Duration, _ int, lastError string) (bool, error) {
	s.lastError = lastError
	return false, s.err
}

type telemetrySweepVectors struct{ fakePresenceVectors }

func (*telemetrySweepVectors) DeleteVectorIDs(ctx context.Context, _ string, _ []string) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestHostedCleanupErrorsRedactRevisionAndProviderText(t *testing.T) {
	logs := captureTelemetryLogs(t)
	task := domain.VectorCleanupTask{RevisionID: "synthetic-retired-revision", VectorIDs: []string{"vector-to-delete"}}
	store := &telemetrySweepStore{fakeManifestStore: newFakeManifestStore(task), err: errors.New("synthetic-provider-text")}
	sweeper := NewSweeper(store, &telemetrySweepVectors{}, SweepOptions{Lease: 30 * time.Millisecond})
	bounded, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	ctx := tracing.WithRedactIdentifiers(bounded, true)
	_, err := sweeper.SweepOnce(ctx)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	sweeper.releaseTask(canceled, task)
	sweeper.deferOnError(canceled, task, "retired", store.err.Error(), &SweepReport{})
	require.Equal(t, store.err.Error(), store.lastError)
	for _, message := range []string{"renew vector cleanup lease", "release vector cleanup lease", "defer vector cleanup"} {
		require.Contains(t, logs.String(), message)
	}
	require.Equal(t, 3, strings.Count(logs.String(), "operation failed"))
	require.NotContains(t, logs.String(), task.RevisionID)
	require.NotContains(t, logs.String(), store.err.Error())
}
