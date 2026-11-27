package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const deletionProbeTestDeadline = 100 * time.Millisecond

func TestDeletionLockProbeCancellation(t *testing.T) {
	address := startPostgres(t)
	client := openDeletionLockClient(t, address, func(config *pgxpool.Config) {
		config.MaxConns = 1
	})

	for _, tc := range []struct {
		name  string
		probe func(context.Context, *pgxpool.Pool) (int, error)
	}{
		{name: "count", probe: countDeletionAdvisoryLocks},
		{name: "termination", probe: endDeletionActivationHolders},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
			held, err := client.Pool.Acquire(ctx)
			cancel()
			require.NoError(t, err, "acquire the only local pool connection")
			release := sync.OnceFunc(held.Release)
			defer release()
			// Unblock even if a probe stops passing its context to the pool.
			watchdog := time.AfterFunc(5*time.Second, release)
			defer watchdog.Stop()

			deadlineCtx, cancelDeadline := context.WithTimeout(t.Context(), deletionProbeTestDeadline)
			_, err = tc.probe(deadlineCtx, client.Pool)
			cancelDeadline()
			require.ErrorIs(t, err, context.DeadlineExceeded)

			canceledCtx, cancelParent := context.WithCancel(t.Context())
			cancelParent()
			_, err = tc.probe(canceledCtx, client.Pool)
			require.ErrorIs(t, err, context.Canceled)
			require.True(t, watchdog.Stop(), "the probes returned before the watchdog released the connection")
			release()

			freshCtx, cancelFresh := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
			n, err := tc.probe(freshCtx, client.Pool)
			cancelFresh()
			require.NoError(t, err, "the probe works after the connection is released")
			require.Zero(t, n)
		})
	}

	t.Run("sibling_lock", func(t *testing.T) {
		siblingAddress := pgtest.Sibling(t, address)
		t.Run("blocked", func(t *testing.T) {
			blocker := openDeletionLockConn(t, siblingAddress, "sibling blocker")
			ctx, cancel := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
			_, err := blocker.Exec(ctx, `SELECT pg_advisory_lock($1)`, pgstore.DeletionActivationLockID)
			cancel()
			require.NoError(t, err, "hold the exclusive activation lock in the sibling fixture")

			waiter := openDeletionLockConn(t, siblingAddress, "sibling waiter")
			blockedCtx, cancelBlocked := context.WithTimeout(t.Context(), time.Second)
			tx, err := beginSiblingActivationLock(blockedCtx, waiter)
			cancelBlocked()
			require.Nil(t, tx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.ErrorContains(t, err, "acquire shared activation lock in the sibling fixture")
			if !waiter.IsClosed() {
				require.Equal(t, byte('I'), waiter.PgConn().TxStatus(), "the failed holder rolled back")
			}
		})

		fresh := openDeletionLockConn(t, siblingAddress, "sibling verifier")
		_, err := beginSiblingActivationLock(t.Context(), fresh)
		require.NoError(t, err, "a fresh transaction can hold the activation lock after the blocker closes")
	})

	t.Run("cleanup", func(t *testing.T) {
		siblingAddress := pgtest.Sibling(t, address)
		t.Run("holder", func(t *testing.T) {
			holder := openDeletionLockConn(t, siblingAddress, "sibling holder")
			ctx, cancel := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
			_, err := holder.Exec(ctx, `SELECT pg_advisory_lock_shared($1)`, pgstore.DeletionActivationLockID)
			cancel()
			require.NoError(t, err, "hold the session lock until subtest cleanup")
		})

		fresh := openDeletionLockConn(t, siblingAddress, "sibling verifier")
		ctx, cancel := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
		_, err := fresh.Exec(ctx, `SELECT pg_advisory_lock($1)`, pgstore.DeletionActivationLockID)
		cancel()
		require.NoError(t, err, "the sibling session lock was released during subtest cleanup")
	})
}

func TestDeletionLockConnectErrorRedactsCredentials(t *testing.T) {
	parseErr := pgconn.NewParseConfigError("user=fixture-user password=fixture-secret", "invalid setting", nil)
	pgErr := &pgconn.PgError{Code: "28P01", Message: "password authentication failed for user fixture-user"}
	for _, tc := range []struct {
		name  string
		cause error
		want  string
	}{
		{name: "parse", cause: parseErr, want: "invalid connection configuration"},
		{name: "authentication", cause: pgErr, want: "SQLSTATE 28P01"},
		{name: "deadline", cause: context.DeadlineExceeded, want: "context deadline exceeded"},
		{name: "canceled", cause: context.Canceled, want: "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := deletionLockConnectError{cause: tc.cause}
			require.ErrorIs(t, err, tc.cause)
			require.ErrorContains(t, err, tc.want)
			require.NotContains(t, err.Error(), "fixture-user")
			require.NotContains(t, err.Error(), "fixture-secret")
		})
	}
}
