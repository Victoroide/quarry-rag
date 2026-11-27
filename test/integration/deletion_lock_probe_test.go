package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const deletionProbeTestDeadline = 100 * time.Millisecond

func TestDeletionLockProbeCancellation(t *testing.T) {
	address := startPostgres(t)
	ctx, cancel := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	client, err := pgstore.NewClient(ctx, address, func(config *pgxpool.Config) {
		config.MaxConns = 1
	})
	cancel()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.Background())) })

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
}
