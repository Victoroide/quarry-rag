package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type ingestRollbackTx struct {
	pgx.Tx
	rollback func(context.Context) error
}

func (tx ingestRollbackTx) Rollback(ctx context.Context) error {
	return tx.rollback(ctx)
}

func TestRollbackIngestTxDetachesFromExpiredCaller(t *testing.T) {
	type valueKey struct{}
	parent, cancel := context.WithDeadline(context.WithValue(t.Context(), valueKey{}, "ingest"), time.Now().Add(-time.Second))
	defer cancel()
	require.ErrorIs(t, parent.Err(), context.DeadlineExceeded)
	var calls int
	var cleanup context.Context
	rollbackIngestTx(parent, ingestRollbackTx{rollback: func(ctx context.Context) error {
		calls++
		cleanup = ctx
		require.NoError(t, ctx.Err())
		require.Equal(t, "ingest", ctx.Value(valueKey{}))
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		remaining := time.Until(deadline)
		require.Positive(t, remaining)
		require.LessOrEqual(t, remaining, 3*time.Second)
		return errors.New("rollback failed")
	}})
	require.Equal(t, 1, calls)
	require.ErrorIs(t, cleanup.Err(), context.Canceled)
}
