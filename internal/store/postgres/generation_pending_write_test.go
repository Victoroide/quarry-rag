package postgres

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cancelAfterGenerationReserveKey struct{}

type cancelAfterGenerationReserve struct{}

func (cancelAfterGenerationReserve) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (cancelAfterGenerationReserve) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if cancel, ok := ctx.Value(cancelAfterGenerationReserveKey{}).(context.CancelFunc); ok && data.Err == nil && data.CommandTag.Update() {
		cancel()
	}
}

func TestGenerationPendingWriteIdentity(t *testing.T) {
	c := jobsDatabase(t, WithQueryTracer(cancelAfterGenerationReserve{}))
	ctx := t.Context()
	const name = "g2"
	_, err := c.RegisterGeneration(ctx, stagingGeneration(name))
	require.NoError(t, err)

	open := func(t *testing.T) *GenerationFence {
		t.Helper()
		fence, err := c.FenceGeneration(t.Context(), name)
		require.NoError(t, err)
		t.Cleanup(func() { fence.Close(t.Context()) })
		return fence
	}
	read := func(t *testing.T) uuid.UUID {
		t.Helper()
		row, err := c.Generation(t.Context(), name)
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, row.PendingWrite, row.PendingWriteID.Valid)
		if row.PendingWrite {
			require.NotEqual(t, uuid.Nil, row.PendingWriteID.UUID)
		}
		return row.PendingWriteID.UUID
	}
	terminate := func(t *testing.T, pid uint32) {
		t.Helper()
		var terminated bool
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT pg_terminate_backend($1, 5000)`, int64(pid)).Scan(&terminated))
		require.True(t, terminated)
	}
	settlePendingWrite := func(t *testing.T, id uuid.UUID) {
		t.Helper()
		fence := open(t)
		require.NoError(t, fence.RequireAcknowledged(t.Context(), id))
		require.Equal(t, uuid.Nil, read(t))
	}

	t.Run("pre-reserve cancellation and competing session", func(t *testing.T) {
		fence := open(t)
		_, err := c.FenceGeneration(t.Context(), name)
		require.ErrorContains(t, err, "another operation owns")
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		calls := 0
		require.ErrorIs(t, fence.Write(canceled, func() error { calls++; return nil }), context.Canceled)
		require.Zero(t, calls)
		require.Equal(t, uuid.Nil, read(t))
	})

	t.Run("backend terminated before reservation response", func(t *testing.T) {
		fence := open(t)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		tx, err := c.Pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(t.Context()) }()
		_, err = tx.Exec(ctx, `SELECT 1 FROM vector_generations WHERE name=$1 FOR UPDATE`, name)
		require.NoError(t, err)
		pid := fence.conn.PgConn().PID()
		dispatched := make(chan struct{}, 1)
		result := make(chan error, 1)
		go func() {
			result <- fence.Write(ctx, func() error { dispatched <- struct{}{}; return nil })
		}()
		defer func() {
			cancel()
			if result != nil {
				// Join the canceled write when setup fails before the assertion.
				assert.Error(t, <-result, "the canceled write must fail")
			}
		}()
		require.Eventually(t, func() bool {
			var blocked bool
			err := c.Pool.QueryRow(ctx, `SELECT cardinality(pg_blocking_pids($1)) > 0`, int64(pid)).Scan(&blocked)
			return err == nil && blocked
		}, 5*time.Second, 10*time.Millisecond)
		terminate(t, pid)
		writeErr := <-result
		result = nil
		require.ErrorContains(t, writeErr, "record target write")
		require.Empty(t, dispatched)
		require.Equal(t, uuid.Nil, read(t))
	})

	t.Run("cancel after reservation before dispatch", func(t *testing.T) {
		fence := open(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		err := fence.Write(context.WithValue(ctx, cancelAfterGenerationReserveKey{}, cancel), func() error {
			calls++
			return nil
		})
		id := read(t)
		require.NotEqual(t, uuid.Nil, id)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, id.String())
		require.Zero(t, calls)
		fence.Close(t.Context())
		settlePendingWrite(t, id)
	})

	for _, failure := range []string{"provider error after reservation", "backend lost after reservation", "backend lost after provider success", "cancel after provider success", "provider panic"} {
		t.Run(failure, func(t *testing.T) {
			fence := open(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			providerErr := errors.New("write response lost")
			pid := fence.conn.PgConn().PID()
			calls := 0
			var id uuid.UUID
			dispatch := func() error {
				calls++
				id = read(t)
				require.NotEqual(t, uuid.Nil, id)
				switch failure {
				case "provider error after reservation":
					return providerErr
				case "backend lost after reservation":
					terminate(t, pid)
					return providerErr
				case "backend lost after provider success":
					terminate(t, pid)
				case "cancel after provider success":
					cancel()
				case "provider panic":
					panic(providerErr)
				}
				return nil
			}
			if failure == "provider panic" {
				require.PanicsWithValue(t, providerErr, func() { _ = fence.Write(ctx, dispatch) })
			} else {
				err := fence.Write(ctx, dispatch)
				require.ErrorContains(t, err, id.String())
				require.ErrorContains(t, err, name)
				switch failure {
				case "provider error after reservation", "backend lost after reservation":
					require.ErrorIs(t, err, providerErr)
				case "cancel after provider success":
					require.ErrorIs(t, err, context.Canceled)
				}
			}
			require.Equal(t, 1, calls)
			require.Equal(t, id, read(t))
			fence.Close(t.Context())
			settlePendingWrite(t, id)
		})
	}

	t.Run("only the bound nonactive write can be cleared", func(t *testing.T) {
		fence := open(t)
		providerErr := errors.New("unknown result")
		require.ErrorIs(t, fence.Write(t.Context(), func() error { return providerErr }), providerErr)
		id := read(t)
		require.ErrorContains(t, fence.RequireAcknowledged(t.Context(), uuid.Nil), "-confirm-target-drained="+id.String())
		wrong := uuid.New()
		require.ErrorContains(t, fence.RequireAcknowledged(t.Context(), wrong), "stale")
		require.Error(t, fence.acknowledge(t.Context(), wrong))
		_, err := c.Pool.Exec(t.Context(), `UPDATE vector_state SET active_generation=$1`, name)
		require.NoError(t, err)
		require.Error(t, fence.RequireAcknowledged(t.Context(), id))
		require.Equal(t, id, read(t))
		_, err = c.Pool.Exec(t.Context(), `UPDATE vector_state SET active_generation='legacy'`)
		require.NoError(t, err)
		require.NoError(t, fence.RequireAcknowledged(t.Context(), id))
		require.ErrorContains(t, fence.RequireAcknowledged(t.Context(), id), "stale")
		require.NoError(t, fence.RequireAcknowledged(t.Context(), uuid.Nil))
		calls := 0
		require.NoError(t, fence.Write(t.Context(), func() error {
			calls++
			next := read(t)
			require.NotEqual(t, uuid.Nil, next)
			require.NotEqual(t, id, next)
			return nil
		}))
		require.Equal(t, 1, calls)
		require.Equal(t, uuid.Nil, read(t))

		missing, err := c.FenceGeneration(t.Context(), "missing")
		require.NoError(t, err)
		defer missing.Close(t.Context())
		require.NoError(t, missing.RequireAcknowledged(t.Context(), uuid.Nil))
		require.ErrorContains(t, missing.RequireAcknowledged(t.Context(), id), "stale")
	})
}

type generationResponseLossKey struct{}

type generationResponseLoss struct {
	armed   atomic.Bool
	release chan struct{}
}

type generationResponseConn struct {
	net.Conn
	loss *generationResponseLoss
}

func (c generationResponseConn) Read(p []byte) (int, error) {
	if c.loss.armed.Swap(false) {
		<-c.loss.release
		return 0, io.ErrUnexpectedEOF
	}
	return c.Conn.Read(p)
}

func TestGenerationPendingWriteLostCommittedResponses(t *testing.T) {
	ctx := t.Context()
	const name = "g2"
	databaseURL := startTestDatabase(t)
	observer := migrationTestClient(t, databaseURL)
	require.NoError(t, observer.RunMigrations(ctx))
	_, registerErr := observer.RegisterGeneration(ctx, stagingGeneration(name))
	require.NoError(t, registerErr)

	faultClient, clientErr := NewClient(ctx, databaseURL, func(config *pgxpool.Config) {
		// Send an extended query and its parameters together so losing the
		// response cannot stop a separate prepare before the UPDATE is sent.
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
		dial := config.ConnConfig.DialFunc
		config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, dialErr := dial(ctx, network, address)
			if loss, ok := ctx.Value(generationResponseLossKey{}).(*generationResponseLoss); dialErr == nil && ok {
				return generationResponseConn{Conn: conn, loss: loss}, nil
			}
			return conn, dialErr
		}
	})
	require.NoError(t, clientErr)
	t.Cleanup(faultClient.Pool.Close)

	// Wait for a different connection to observe the commit before
	// discarding the original connection's response.
	lose := func(command func(context.Context, *GenerationFence) error, pending bool) {
		t.Helper()
		loss := &generationResponseLoss{release: make(chan struct{})}
		fence, fenceErr := faultClient.FenceGeneration(context.WithValue(ctx, generationResponseLossKey{}, loss), name)
		require.NoError(t, fenceErr)
		defer fence.Close(ctx)
		pid := fence.conn.PgConn().PID()
		result := make(chan error, 1)
		loss.armed.Store(true)
		go func() { result <- command(ctx, fence) }()
		defer func() {
			if result != nil {
				close(loss.release)
				// Unblock and join the faulted command if observation fails.
				assert.Error(t, <-result, "the faulted command must fail")
			}
		}()
		require.Eventually(t, func() bool {
			row, readErr := observer.Generation(ctx, name)
			return readErr == nil && row != nil && row.PendingWrite == pending && row.PendingWriteID.Valid == pending
		}, 5*time.Second, 10*time.Millisecond)
		close(loss.release)
		commandErr := <-result
		result = nil
		require.Error(t, commandErr)
		require.Eventually(t, func() bool {
			var exited bool
			pollErr := observer.Pool.QueryRow(ctx, `SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1)`, int64(pid)).Scan(&exited)
			return pollErr == nil && exited
		}, 5*time.Second, 10*time.Millisecond)
	}

	calls := 0
	lose(func(ctx context.Context, fence *GenerationFence) error {
		return fence.Write(ctx, func() error { calls++; return nil })
	}, true)
	require.Zero(t, calls)
	a, err := observer.Generation(ctx, name)
	require.NoError(t, err)
	require.True(t, a.PendingWriteID.Valid)
	require.NotEqual(t, uuid.Nil, a.PendingWriteID.UUID)

	lose(func(ctx context.Context, fence *GenerationFence) error {
		return fence.acknowledge(ctx, a.PendingWriteID.UUID)
	}, false)

	fence, err := observer.FenceGeneration(ctx, name)
	require.NoError(t, err)
	defer fence.Close(ctx)
	require.ErrorContains(t, fence.RequireAcknowledged(ctx, a.PendingWriteID.UUID), "stale")
	providerErr := errors.New("response lost again")
	require.ErrorIs(t, fence.Write(ctx, func() error { return providerErr }), providerErr)
	b, err := observer.Generation(ctx, name)
	require.NoError(t, err)
	require.True(t, b.PendingWriteID.Valid)
	require.NotEqual(t, a.PendingWriteID, b.PendingWriteID)
	require.ErrorContains(t, fence.RequireAcknowledged(ctx, a.PendingWriteID.UUID), "stale")
	stillPending, err := observer.Generation(ctx, name)
	require.NoError(t, err)
	require.Equal(t, b.PendingWriteID, stillPending.PendingWriteID)
	require.NoError(t, fence.RequireAcknowledged(ctx, b.PendingWriteID.UUID))
}

func TestMigrateGenerationPendingWriteIdentity(t *testing.T) {
	ctx := t.Context()
	c := migrationTestClient(t, startTestDatabase(t))
	applyMigrationPrefix(t, c, 40)
	_, err := c.Pool.Exec(ctx, `INSERT INTO vector_generations(name, store, target, profile, profile_hash, profile_origin, pending_write)
 VALUES ('g2', 'postgres', 'chunk_vectors_g2', '{}', 'profile', 'recorded', TRUE),
 ('g3', 'postgres', 'chunk_vectors_g3', '{}', 'profile', 'recorded', TRUE),
 ('g4', 'postgres', 'chunk_vectors_g4', '{}', 'profile', 'recorded', FALSE);
 CREATE SCHEMA sibling;
 CREATE TABLE sibling.vector_generations AS TABLE vector_generations`)
	require.NoError(t, err)

	read := func(name string) uuid.NullUUID {
		t.Helper()
		var id uuid.NullUUID
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT pending_write_id FROM vector_generations WHERE name=$1`, name).Scan(&id))
		return id
	}
	require.NoError(t, c.RunMigrations(ctx))
	requireRecordedMigrations(t, c)
	first, second := read("g2"), read("g3")
	for _, id := range []uuid.NullUUID{first, second} {
		require.True(t, id.Valid)
		require.NotEqual(t, uuid.Nil, id.UUID)
	}
	require.NotEqual(t, first, second)
	require.Equal(t, uuid.NullUUID{}, read("g4"))
	for _, stmt := range []string{
		`UPDATE vector_generations SET pending_write=FALSE WHERE name='g2' AND pending_write`,
		`UPDATE vector_generations SET pending_write=TRUE WHERE name='g4' AND NOT pending_write`,
		`UPDATE vector_generations SET pending_write=TRUE,pending_write_id='00000000-0000-0000-0000-000000000000' WHERE name='g4'`,
	} {
		_, updateErr := c.Pool.Exec(ctx, stmt)
		requireSQLState(t, updateErr, "23514")
	}
	require.NoError(t, c.RunMigrations(ctx))
	require.Equal(t, first, read("g2"))
	require.Equal(t, second, read("g3"))
	require.Equal(t, uuid.NullUUID{}, read("g4"))
	var siblingPending, siblingHasID, unchanged bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT pending_write,
 EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='sibling' AND table_name='vector_generations' AND column_name='pending_write_id'),
 (SELECT to_jsonb(g)-'pending_write_id' = to_jsonb(b) FROM vector_generations g JOIN sibling.vector_generations b USING (name) WHERE g.name='g4')
 FROM sibling.vector_generations AS s WHERE name='g2'`).Scan(&siblingPending, &siblingHasID, &unchanged))
	require.True(t, siblingPending)
	require.False(t, siblingHasID)
	require.True(t, unchanged)

	// The updated maintenance binary can still settle a backfilled write
	// when rolling application binaries back with migration 041 retained.
	fence, err := c.FenceGeneration(ctx, "g2")
	require.NoError(t, err)
	defer fence.Close(ctx)
	require.NoError(t, fence.RequireAcknowledged(ctx, first.UUID))
	require.Equal(t, uuid.NullUUID{}, read("g2"))
	require.Equal(t, second, read("g3"))
}
