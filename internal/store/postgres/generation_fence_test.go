package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type activationValueKey struct{}

type activationRollbackTx struct {
	pgx.Tx
	rollback func(context.Context) error
}

func (tx activationRollbackTx) Rollback(ctx context.Context) error {
	return tx.rollback(ctx)
}

func TestActivationRollbackUsesIndependentContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "confirmed rollback"},
		{name: "closed transaction", err: fmt.Errorf("already ended: %w", pgx.ErrTxClosed)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, cancel := context.WithDeadline(
				context.WithValue(t.Context(), activationValueKey{}, "activation"),
				time.Now().Add(-time.Second),
			)
			defer cancel()
			require.ErrorIs(t, parent.Err(), context.DeadlineExceeded)
			var cleanup context.Context
			tx := activationRollbackTx{rollback: func(ctx context.Context) error {
				require.Nil(t, cleanup, "rollback called twice")
				cleanup = ctx
				require.NoError(t, ctx.Err())
				require.Equal(t, "activation", ctx.Value(activationValueKey{}))
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.WithinDuration(t, time.Now().Add(3*time.Second), deadline, 100*time.Millisecond)
				return tc.err
			}}
			// An acknowledged rollback or ended transaction needs no transport.
			(&GenerationFence{}).rollbackActivation(parent, tx)
			require.NotNil(t, cleanup)
			require.ErrorIs(t, cleanup.Err(), context.Canceled)
		})
	}
}
