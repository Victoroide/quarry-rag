package postgres

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type ingestPoolReleaseTracer struct {
	ingestTxTracer
	closedPID atomic.Uint32
}

func (tracer *ingestPoolReleaseTracer) TraceRelease(_ *pgxpool.Pool, data pgxpool.TraceReleaseData) {
	if data.Conn.IsClosed() {
		tracer.closedPID.Store(data.Conn.PgConn().PID())
	}
}

func requireRecoveredIngestPool(ctx context.Context, t *testing.T, c *Client, observer *pgx.Conn, pid uint32, replaced bool) {
	t.Helper()
	if replaced {
		// pgxpool releases the checkout synchronously, then destroys a closed
		// resource asynchronously. The pool counts it until destruction ends.
		require.Eventually(t, func() bool {
			return c.Pool.Stat().AcquiredConns() == 0
		}, 3*time.Second, 10*time.Millisecond, "the pool must remove the discarded connection")
	} else {
		require.Zero(t, c.Pool.Stat().AcquiredConns())
	}
	require.NoError(t, c.Pool.Ping(ctx))
	var recoveredPID uint32
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&recoveredPID))
	require.NotEqual(t, observer.PgConn().PID(), recoveredPID)
	if replaced {
		require.NotEqual(t, pid, recoveredPID)
		// PostgreSQL can finish handling the disconnect after the pool has
		// removed the connection. Wait before probing the released locks.
		pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			var disconnected bool
			require.NoError(t, observer.QueryRow(pollCtx, `SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE pid=$1)`, pid).Scan(&disconnected))
			if disconnected {
				break
			}
			select {
			case <-ticker.C:
			case <-pollCtx.Done():
				t.Fatal("PostgreSQL did not release the discarded session before the watchdog")
			}
		}
	} else {
		require.Equal(t, pid, recoveredPID)
	}
}
