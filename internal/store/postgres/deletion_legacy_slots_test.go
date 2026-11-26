package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestLegacyBoundDerivation(t *testing.T) {
	for _, tc := range []struct{ poolMax, configured, want int }{
		{1, 0, 1}, {2, 0, 1}, {3, 0, 1}, {4, 0, 1}, {8, 0, 2}, {16, 0, 4}, {64, 0, 16},
		{16, -3, 4}, {0, 0, 1},
		{16, 1, 1}, {16, 3, 3}, {16, 16, 16}, {16, 64, 16}, {1, 5, 1}, {2, 5, 2},
	} {
		require.Equal(t, tc.want, legacyBound(tc.poolMax, tc.configured), "pool %d configured %d", tc.poolMax, tc.configured)
	}
}

func TestLegacySlotsNeverWait(t *testing.T) {
	slots := legacySlots{limit: 2}
	require.True(t, slots.acquire())
	require.True(t, slots.acquire())
	done := make(chan bool, 1)
	go func() { done <- slots.acquire() }()
	select {
	case got := <-done:
		require.False(t, got, "a full set refuses instead of waiting")
	case <-time.After(5 * time.Second):
		t.Fatal("acquire waited for a free slot")
	}
	slots.release()
	require.True(t, slots.acquire())
}

func TestLegacySlotsAreSafeUnderContention(t *testing.T) {
	slots := legacySlots{limit: 3}
	var mu sync.Mutex
	var running, peak int
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			if !slots.acquire() {
				return
			}
			mu.Lock()
			running++
			peak = max(peak, running)
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			running--
			mu.Unlock()
			slots.release()
		})
	}
	wg.Wait()
	require.LessOrEqual(t, peak, 3)
	require.Zero(t, slots.active)
}

func TestReserveLegacyDeletionClaimsOneSlotForTheGuard(t *testing.T) {
	c := &Client{legacy: legacySlots{limit: 1}}
	ctx, release, err := c.ReserveLegacyDeletion(t.Context())
	require.NoError(t, err)
	_, _, err = c.ReserveLegacyDeletion(t.Context())
	require.ErrorIs(t, err, domain.ErrTooManyLegacyDeletions)

	// The guard runs in the claimed slot, and does not give it back.
	giveBack, err := c.claimLegacySlot(ctx)
	require.NoError(t, err)
	giveBack()
	_, _, err = c.ReserveLegacyDeletion(t.Context())
	require.ErrorIs(t, err, domain.ErrTooManyLegacyDeletions, "the reservation outlives the guard call")

	release()
	release()
	again, releaseAgain, err := c.ReserveLegacyDeletion(t.Context())
	require.NoError(t, err, "a repeated release frees exactly one slot")
	_, _, err = c.ReserveLegacyDeletion(t.Context())
	require.ErrorIs(t, err, domain.ErrTooManyLegacyDeletions)
	releaseAgain()

	// A released reservation that is still in a context claims a slot of its own.
	taken, err := c.claimLegacySlot(ctx)
	require.NoError(t, err)
	_, _, err = c.ReserveLegacyDeletion(again)
	require.ErrorIs(t, err, domain.ErrTooManyLegacyDeletions)
	taken()
}

func TestReservationOfAnotherClientIsNotHonored(t *testing.T) {
	first := &Client{legacy: legacySlots{limit: 1}}
	second := &Client{legacy: legacySlots{limit: 1}}
	ctx, release, err := first.ReserveLegacyDeletion(t.Context())
	require.NoError(t, err)
	defer release()
	giveBack, err := second.claimLegacySlot(ctx)
	require.NoError(t, err)
	_, _, err = second.ReserveLegacyDeletion(t.Context())
	require.ErrorIs(t, err, domain.ErrTooManyLegacyDeletions, "the second client claimed a slot of its own")
	giveBack()
}

// heldCleanup is a guarded deletion that stays inside remove until released,
// the way a slow provider call does.
type heldCleanup struct {
	release chan struct{}
	done    chan error
}

func holdGuardedDeletion(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID) *heldCleanup {
	t.Helper()
	held := &heldCleanup{release: make(chan struct{}), done: make(chan error, 1)}
	entered := make(chan struct{})
	go func() {
		held.done <- c.WithLegacyDeletion(ctx, id, func(domain.LegacyDocumentStore) error {
			close(entered)
			select {
			case <-held.release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-entered:
	case err := <-held.done:
		t.Fatalf("the guarded deletion ended before its cleanup ran: %v", err)
	case <-ctx.Done():
		t.Fatal("the guarded deletion never reached its cleanup")
	}
	return held
}

func (h *heldCleanup) finish(t *testing.T) {
	t.Helper()
	close(h.release)
	require.NoError(t, <-h.done)
}

// advisoryLockCount counts held and waiting advisory locks in the fixture database.
func advisoryLockCount(ctx context.Context, t *testing.T, observer *pgx.Conn) int {
	t.Helper()
	var n int
	require.NoError(t, observer.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype='advisory'
		AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&n))
	return n
}

func TestGuardedDeletionProbesIgnoreUnrelatedSessions(t *testing.T) {
	c, observer := sessionTestClient(t)
	ctx := t.Context()
	connect := func(config *pgx.ConnConfig) *pgx.Conn {
		t.Helper()
		conn, err := pgx.ConnectConfig(ctx, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			require.NoError(t, conn.Close(cleanup))
		})
		return conn
	}

	unrelated := observer.Config()
	unrelated.RuntimeParams["application_name"] = "unrelated-deletion-probe"
	connect(unrelated)

	siblingURL := pgtest.Sibling(t, observer.Config().ConnString())
	siblingConfig, err := pgx.ParseConfig(siblingURL)
	require.NoError(t, err)
	siblingConfig.RuntimeParams["application_name"] = observer.Config().RuntimeParams["application_name"]
	sibling := connect(siblingConfig)
	_, err = sibling.Exec(ctx, `SELECT pg_advisory_lock_shared($1)`, DeletionActivationLockID)
	require.NoError(t, err)

	t.Run("advisory locks", func(t *testing.T) {
		require.Zero(t, advisoryLockCount(t.Context(), t, observer))
	})
	t.Run("dedicated sessions", func(t *testing.T) {
		requireDedicatedSessions(t, c, observer, 0)
	})
}

func TestGuardedDeletionHoldsNoPoolConnection(t *testing.T) {
	c, observer := sessionTestClient(t)
	ctx := legacyContext(t)
	held := holdGuardedDeletion(ctx, t, c, domain.DocumentID(uuid.NewString()))
	requireDedicatedSessions(t, c, observer, 1)
	require.Zero(t, c.Pool.Stat().AcquiredConns(), "the cleanup keeps no pooled connection while it waits on a provider")
	require.Equal(t, 1, advisoryLockCount(ctx, t, observer), "the shared activation lock lives on the dedicated session")

	// Every pooled connection is still available to other traffic.
	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	first, err := c.Pool.Acquire(short)
	require.NoError(t, err)
	second, err := c.Pool.Acquire(short)
	require.NoError(t, err)
	first.Release()
	second.Release()

	held.finish(t)
	requireDedicatedSessions(t, c, observer, 0)
	require.Zero(t, advisoryLockCount(ctx, t, observer))
}

func TestSlowGuardedDeletionsDoNotExhaustSmallPool(t *testing.T) {
	c, observer := sessionTestClientWithPool(t, 2)
	c.SetLegacyDeletionConcurrency(2)
	ctx := legacyContext(t)
	first := holdGuardedDeletion(ctx, t, c, domain.DocumentID(uuid.NewString()))
	second := holdGuardedDeletion(ctx, t, c, domain.DocumentID(uuid.NewString()))
	requireDedicatedSessions(t, c, observer, 2)

	// With every guarded slot busy, other traffic completes inside its normal deadline.
	traffic, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	started := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for range 2 {
		wg.Go(func() {
			result, err := c.AcceptDocument(traffic, jobsRequest("admitted during slow deletions "+uuid.NewString()))
			if err == nil && result.Outcome != domain.AcceptAccepted {
				err = domain.ErrDeletionBusy
			}
			errs <- err
		})
		wg.Go(func() {
			_, err := c.GetDocument(traffic, domain.DocumentID(uuid.NewString()))
			errs <- err
		})
		wg.Go(func() {
			var n int
			errs <- c.Pool.QueryRow(traffic, `SELECT count(*) FROM documents`).Scan(&n)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Less(t, time.Since(started), 2*time.Second)

	first.finish(t)
	second.finish(t)
	requireDedicatedSessions(t, c, observer, 0)
}

func TestSlowGuardedDeletionLeavesSinglePoolSlotFree(t *testing.T) {
	c, observer := sessionTestClientWithPool(t, 1)
	require.Equal(t, 1, c.legacy.limit, "one pooled connection derives one cleanup slot")
	ctx := legacyContext(t)
	held := holdGuardedDeletion(ctx, t, c, domain.DocumentID(uuid.NewString()))
	requireDedicatedSessions(t, c, observer, 1)

	traffic, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	result, err := c.AcceptDocument(traffic, jobsRequest("admitted on the only pooled connection"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	var active bool
	require.NoError(t, c.Pool.QueryRow(traffic, `SELECT document_deletion_active()`).Scan(&active))

	held.finish(t)
}

func TestGuardedDeletionOverTheBoundTakesNoLockOrConnection(t *testing.T) {
	c, observer := sessionTestClient(t)
	sibling, err := pgx.Connect(t.Context(), pgtest.Sibling(t, observer.Config().ConnString()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sibling.Close(context.Background())) })
	_, err = sibling.Exec(t.Context(), `SELECT pg_advisory_lock_shared($1)`, DeletionActivationLockID)
	require.NoError(t, err)

	ctx := legacyContext(t)
	held := holdGuardedDeletion(ctx, t, c, domain.DocumentID(uuid.NewString()))
	requireDedicatedSessions(t, c, observer, 1)
	locks := advisoryLockCount(ctx, t, observer)
	require.Equal(t, 1, locks)

	called := false
	err = c.WithLegacyDeletion(ctx, domain.DocumentID(uuid.NewString()), func(domain.LegacyDocumentStore) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, domain.ErrTooManyLegacyDeletions)
	require.False(t, called)
	requireDedicatedSessions(t, c, observer, 1)
	require.Equal(t, locks, advisoryLockCount(ctx, t, observer), "the refused call took no lock")

	_, _, err = c.ReserveLegacyDeletion(ctx)
	require.ErrorIs(t, err, domain.ErrTooManyLegacyDeletions)

	held.finish(t)
	require.NoError(t, c.WithLegacyDeletion(ctx, domain.DocumentID(uuid.NewString()), func(domain.LegacyDocumentStore) error { return nil }), "the slot is free again")
}

// requireLegacySlotFree checks that a finished cleanup returned its slot and closed its session.
func requireLegacySlotFree(t *testing.T, c *Client, observer *pgx.Conn, step string) {
	t.Helper()
	_, release, err := c.ReserveLegacyDeletion(t.Context())
	require.NoError(t, err, "the slot is free after %s", step)
	release()
	requireDedicatedSessions(t, c, observer, 0)
}

func TestLegacyDeletionSlotReleasedAfterEveryOutcome(t *testing.T) {
	c, observer := sessionTestClient(t)
	ctx := legacyContext(t)
	id := domain.DocumentID(uuid.NewString())
	free := func(step string) { requireLegacySlotFree(t, c, observer, step) }

	require.ErrorIs(t, c.WithLegacyDeletion(ctx, id, func(domain.LegacyDocumentStore) error { return domain.ErrDeletionBusy }), domain.ErrDeletionBusy)
	free("a failed cleanup")

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, c.WithLegacyDeletion(canceled, id, func(domain.LegacyDocumentStore) error { return nil }))
	free("a request canceled before its session opened")

	canceledRequest, cancelNow := context.WithCancel(ctx)
	err := c.WithLegacyDeletion(canceledRequest, id, func(domain.LegacyDocumentStore) error {
		cancelNow()
		return canceledRequest.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
	free("a request canceled during the cleanup")

	err = legacySession(ctx, t, c, id, func(store *legacyDeletion) {
		var terminated bool
		require.NoError(t, observer.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, int32(store.conn.PgConn().PID())).Scan(&terminated)) // #nosec G115 -- PostgreSQL process IDs are small.
		require.True(t, terminated)
		require.Eventually(t, func() bool { return advisoryLockCount(ctx, t, observer) == 0 }, 10*time.Second, 25*time.Millisecond)
	})
	require.NoError(t, err)
	free("a lost session")
}

func TestLegacyDeletionOnTheActivatedDatabaseReleasesItsSlot(t *testing.T) {
	c := activatedDatabase(t)
	c.SetLegacyDeletionConcurrency(1)
	ctx := legacyContext(t)
	for range 3 {
		err := c.WithLegacyDeletion(ctx, domain.DocumentID(uuid.NewString()), func(domain.LegacyDocumentStore) error { return nil })
		require.ErrorIs(t, err, domain.ErrDeletionActivated)
	}
	require.True(t, lockIsFree(ctx, t, c))
}

func TestConfiguredLegacyBoundAppliesToTheClient(t *testing.T) {
	c, _ := sessionTestClientWithPool(t, 8)
	require.Equal(t, 2, c.legacy.limit)
	c.SetLegacyDeletionConcurrency(5)
	require.Equal(t, 5, c.legacy.limit)
	c.SetLegacyDeletionConcurrency(100)
	require.Equal(t, 8, c.legacy.limit, "the bound never exceeds the pool")
	c.SetLegacyDeletionConcurrency(0)
	require.Equal(t, 2, c.legacy.limit)
}
