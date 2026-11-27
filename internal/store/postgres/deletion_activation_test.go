package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func recordedMarker(ctx context.Context, t *testing.T, c *Client) *time.Time {
	t.Helper()
	var marker *time.Time
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT activated_at FROM document_deletion_activation`).Scan(&marker))
	return marker
}

func TestActivationSetsTheMarkerOnceAndReportsTheRecordedValue(t *testing.T) {
	parallelDatabaseTest(t)
	c := jobsDatabase(t)
	other := otherConnection(t, c)
	ctx := t.Context()
	require.Nil(t, recordedMarker(ctx, t, c), "a migrated database starts inactive")

	first, err := c.ActivateDeletion(ctx)
	require.NoError(t, err)
	require.False(t, first.AlreadyActive)
	require.False(t, first.ActivatedAt.IsZero())
	require.Equal(t, time.UTC, first.ActivatedAt.Location())
	marker := recordedMarker(ctx, t, c)
	require.NotNil(t, marker)
	require.True(t, marker.Equal(first.ActivatedAt))

	// A later explicit activation, from this or another process, even after the
	// clock has moved on, changes nothing and reports the same instant.
	require.Eventually(t, func() bool {
		var later bool
		readErr := c.Pool.QueryRow(ctx, `SELECT clock_timestamp() > $1::timestamptz`, first.ActivatedAt).Scan(&later)
		return readErr == nil && later
	}, 5*time.Second, 10*time.Millisecond, "the database clock must move past the recorded marker")
	for _, client := range []*Client{c, other} {
		again, activateErr := client.ActivateDeletion(ctx)
		require.NoError(t, activateErr)
		require.True(t, again.AlreadyActive)
		require.True(t, again.ActivatedAt.Equal(first.ActivatedAt))
	}
	require.True(t, recordedMarker(ctx, t, c).Equal(first.ActivatedAt))
	active, err := other.DeletionActive(ctx)
	require.NoError(t, err)
	require.True(t, active, "a stale replica reads the marker from the database")
	require.True(t, lockIsFree(ctx, t, c), "activation releases its lock after every outcome")
}

func TestConcurrentActivationsRecordOneValue(t *testing.T) {
	parallelDatabaseTest(t)
	c := jobsDatabase(t)
	ctx := t.Context()
	const callers = 6
	clients := make([]*Client, callers)
	for i := range clients {
		clients[i] = otherConnection(t, c)
	}
	results := make([]domain.DeletionActivation, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range clients {
		wg.Go(func() { results[i], errs[i] = clients[i].ActivateDeletion(ctx) })
	}
	wg.Wait()
	first := 0
	for i := range results {
		require.NoError(t, errs[i])
		require.True(t, results[i].ActivatedAt.Equal(results[0].ActivatedAt))
		if !results[i].AlreadyActive {
			first++
		}
	}
	require.Equal(t, 1, first, "exactly one caller sets the marker")
}

func TestActivationWaitsForAHealthyCleanupAndSetsTheMarkerAfterIt(t *testing.T) {
	parallelDatabaseTest(t)
	c := jobsDatabase(t)
	activator := otherConnection(t, c)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "in-flight compatible request", false, false)
	activated := make(chan error, 1)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(*legacyDeletion) {
		go func() {
			_, err := activator.ActivateDeletion(ctx)
			activated <- err
		}()
		waitForAdvisoryKey(ctx, t, c, DeletionActivationLockID)
		require.Nil(t, recordedMarker(ctx, t, c), "the marker stays unset while the cleanup runs")
	}))
	require.NoError(t, <-activated)
	require.NotNil(t, recordedMarker(ctx, t, c))
	err := c.WithLegacyDeletion(ctx, doc.id, func(domain.LegacyDocumentStore) error {
		t.Fatal("an ID-only cleanup must not start on an activated database")
		return nil
	})
	require.ErrorIs(t, err, domain.ErrDeletionActivated)
}

func TestActivationThatTimesOutLeavesTheMarkerUnset(t *testing.T) {
	parallelDatabaseTest(t)
	c := jobsDatabase(t)
	activator := otherConnection(t, c)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "slow cleanup", false, false)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(*legacyDeletion) {
		short, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer cancel()
		_, err := activator.ActivateDeletion(short)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err), "the wait ended at the caller's deadline: %v", err)
		require.Nil(t, recordedMarker(ctx, t, c))
	}))
	result, err := activator.ActivateDeletion(ctx)
	require.NoError(t, err, "a retry succeeds once the cleanup has returned")
	require.False(t, result.AlreadyActive)
}

func TestActivationLosesItsConnectionWithoutSettingTheMarker(t *testing.T) {
	parallelDatabaseTest(t)
	c := jobsDatabase(t)
	activator := otherConnection(t, c)
	killer := otherConnection(t, c)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "connection loss", false, false)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(*legacyDeletion) {
		done := make(chan error, 1)
		go func() {
			_, err := activator.ActivateDeletion(ctx)
			done <- err
		}()
		require.Eventually(t, func() bool {
			var terminated bool
			err := killer.Pool.QueryRow(ctx, `SELECT COALESCE(bool_or(pg_terminate_backend(pid)), FALSE)
				FROM pg_stat_activity
				WHERE pid <> pg_backend_pid() AND datname = current_database()
				  AND wait_event_type = 'Lock' AND wait_event = 'advisory'
				  AND query LIKE '%pg_advisory_lock(%' AND query NOT LIKE '%pg_advisory_lock_shared%'`).Scan(&terminated)
			return err == nil && terminated
		}, 15*time.Second, 25*time.Millisecond, "the waiting activation session")
		require.Error(t, <-done)
		require.Nil(t, recordedMarker(ctx, t, killer))
	}))
	result, err := activator.ActivateDeletion(ctx)
	require.NoError(t, err, "a repeated activation succeeds after the lost session")
	require.False(t, result.AlreadyActive)
	require.True(t, lockIsFree(ctx, t, c))
}

func TestActivationRequiresTheMigratedMarker(t *testing.T) {
	parallelDatabaseTest(t)
	c := jobsDatabase(t)
	ctx := t.Context()
	_, err := c.Pool.Exec(ctx, `DELETE FROM document_deletion_activation`)
	require.NoError(t, err)
	_, err = c.ActivateDeletion(ctx)
	require.ErrorIs(t, err, errActivationMarkerMissing)
	require.True(t, lockIsFree(ctx, t, c))
}

func TestActivationKeepsAnUnfinishedReservationProtectingItsIdentity(t *testing.T) {
	parallelDatabaseTest(t)
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "held across activation", false, false)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err)
		require.NoError(t, store.RetireLegacyDocument(ctx, doc.id, target))
	}))
	before := readLegacyReceipt(ctx, t, c, doc.id)
	_, err := c.ActivateDeletion(ctx)
	require.NoError(t, err)
	require.Equal(t, before, readLegacyReceipt(ctx, t, c, doc.id), "activation never edits a receipt")
	require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, doc.id, "replacement"))
	require.Equal(t, domain.AcceptAccepted, admit(ctx, t, c, domain.DocumentID(uuid.NewString()), "unrelated"))
}

func TestRestrictedServingLoginCannotActivate(t *testing.T) {
	parallelDatabaseTest(t)
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	_, err := serving.ActivateDeletion(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "schema owner")
	requireSQLState(t, err, "42501")
	require.Nil(t, recordedMarker(ctx, t, owner), "a serving login never sets the marker")
	require.True(t, lockIsFree(ctx, t, owner))

	result, err := owner.ActivateDeletion(ctx)
	require.NoError(t, err)
	require.False(t, result.AlreadyActive)
	active, err := serving.DeletionActive(ctx)
	require.NoError(t, err)
	require.True(t, active, "the serving login observes the owner's activation without any table privilege")
}
