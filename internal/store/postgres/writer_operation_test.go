package postgres

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestWriterRecoveryEvidenceIgnoresServiceVerbosity(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, level := range []slog.Level{slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
		t.Run(level.String(), func(t *testing.T) {
			var retained, ordinary bytes.Buffer
			slog.SetDefault(slog.New(slog.NewJSONHandler(&ordinary, &slog.HandlerOptions{Level: level})))
			ctx := t.Context()
			sink := newWriterRecoveryEvidence(&retained).withAttrs([]slog.Attr{
				slog.String("writer_operation", "operation"), slog.String("writer_sender", "sender"),
			})
			require.NoError(t, writeWriterEvidence(ctx, sink, "writer operation enrollment started"))
			state := externalWriterState{evidence: sink}
			require.NoError(t, state.dispatch(ctx, func(ctx context.Context) (bool, error) {
				event := domain.ExternalWriterEvidence{Provider: "pinecone", Namespace: "default", Outcome: "started"}
				if err := domain.RecordExternalWriterEvidence(ctx, event); err != nil {
					return false, err
				}
				event.Outcome = "acknowledged_pending_indexing"
				return false, domain.RecordExternalWriterEvidence(ctx, event)
			}))
			require.Panics(t, func() {
				_ = state.dispatch(ctx, func(ctx context.Context) (bool, error) {
					event := domain.ExternalWriterEvidence{Provider: "pinecone", Namespace: "fragment", Outcome: "started"}
					require.NoError(t, domain.RecordExternalWriterEvidence(ctx, event))
					panic("interrupted before the response")
				})
			})
			require.False(t, state.close())
			require.Empty(t, ordinary.String())
			lines := bytes.Split(bytes.TrimSpace(retained.Bytes()), []byte("\n"))
			require.Len(t, lines, 4)
			for _, line := range lines {
				require.Contains(t, string(line), `"stream":"writer_recovery"`)
				require.Contains(t, string(line), `"writer_operation":"operation","writer_sender":"sender"`)
			}
			require.Contains(t, string(lines[0]), "writer operation enrollment started")
			require.Contains(t, string(lines[1]), `"attempt":1`)
			require.Contains(t, string(lines[2]), `"outcome":"acknowledged_pending_indexing","provider_request_id":"","provider_request_sequence":""`)
			require.Contains(t, string(lines[3]), `"attempt":2,"provider":"pinecone","namespace":"fragment","outcome":"started"`)
		})
	}
}

type failedRecoveryOutput struct{}

func (failedRecoveryOutput) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestWriterOperationUnavailableEvidenceStopsAdmission(t *testing.T) {
	c, _ := sessionTestClientWithPool(t, 2)
	grantExec(t, c, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	c.writerEvidence = newWriterRecoveryEvidence(failedRecoveryOutput{})
	called := false
	err := c.RunWriterOperation(t.Context(), domain.WriterOperationReconcile, "scoped", func(context.Context) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.False(t, called)
	require.NoError(t, c.FreezeWriters(t.Context(), "next"))
}

func TestExternalWriterSettlementIsMonotone(t *testing.T) {
	lost := errors.New("transport reply lost")
	for _, test := range []struct {
		name     string
		cancel   bool
		mark     error
		outcomes []bool
		settled  bool
	}{
		{name: "canceled before mark", cancel: true, outcomes: []bool{true}, settled: true},
		{name: "mark commit unknown", mark: lost, outcomes: []bool{true}},
		{name: "acknowledged", outcomes: []bool{true, true}, settled: true},
		{name: "unknown then acknowledged", outcomes: []bool{false, true}},
		{name: "acknowledged then unknown", outcomes: []bool{true, false}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var marks, sends int
			state := externalWriterState{mark: func(context.Context) error {
				marks++
				return test.mark
			}}
			ctx = domain.WithExternalWriterDispatch(ctx, state.dispatch)
			if test.cancel {
				cancel()
			}
			for _, settled := range test.outcomes {
				err := domain.DispatchExternalWriter(ctx, func(context.Context) (bool, error) {
					sends++
					return settled, nil
				})
				switch {
				case test.cancel:
					require.ErrorIs(t, err, context.Canceled)
				case test.mark != nil:
					require.ErrorIs(t, err, test.mark)
				default:
					require.NoError(t, err)
				}
			}
			if test.cancel {
				require.Zero(t, marks)
			} else {
				require.Equal(t, 1, marks)
			}
			if test.cancel || test.mark != nil {
				require.Zero(t, sends)
			} else {
				require.Equal(t, len(test.outcomes), sends)
			}
			require.Equal(t, test.settled, state.close())
			require.Error(t, state.dispatch(t.Context(), func(context.Context) (bool, error) {
				t.Fatal("closed reservation dispatched")
				return true, nil
			}))
		})
	}
}

func TestWriterOperationsFreezeAfterGuardBackendLoss(t *testing.T) {
	c, observer := sessionTestClientWithPool(t, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := c.Pool.Exec(ctx, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	require.NoError(t, err)
	err = c.RunWriterOperation(ctx, domain.WriterOperationReconcile, "scoped", func(ctx context.Context) error {
		requireDedicatedSessionsContext(ctx, t, c, observer, 0)
		var pid uint32
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
		var terminated bool
		require.NoError(t, observer.QueryRow(ctx, `SELECT pg_terminate_backend($1,5000)`, pid).Scan(&terminated))
		require.True(t, terminated)
		c.Pool.Reset()
		freezeErr := c.FreezeWriters(ctx, "g2")
		var pending *ErrWriterOperationsPending
		require.ErrorAs(t, freezeErr, &pending)
		require.EqualValues(t, 1, pending.Count)
		require.Len(t, pending.OperationIDs, 1)
		state, stateErr := c.VectorState(ctx)
		require.NoError(t, stateErr)
		require.False(t, state.Frozen)
		for range 2 {
			require.NoError(t, domain.DispatchExternalWriter(ctx, func(ctx context.Context) (bool, error) {
				var marked bool
				require.NoError(t, c.Pool.QueryRow(ctx, `SELECT pending_provider FROM vector_writer_operations`).Scan(&marked))
				require.True(t, marked, "the mark is committed before dispatch")
				return true, nil
			}))
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, c.FreezeWriters(ctx, "g2"))
	require.NoError(t, c.FreezeWriters(ctx, "g2"))
	require.Error(t, c.FreezeWriters(ctx, "g3"))
	require.ErrorIs(t, c.RunWriterOperation(ctx, domain.WriterOperationReconcile, "scoped", func(context.Context) error {
		t.Fatal("freeze allowed callback")
		return nil
	}), domain.ErrWritersFrozen)
	require.NoError(t, c.ThawWriters(ctx))
}

func TestWriterOperationFreezeSeesReservationCommittedDuringLockWait(t *testing.T) {
	c, observer := sessionTestClientWithPool(t, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	grantExec(t, c, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	conn, err := pgx.ConnectConfig(ctx, c.Pool.Config().ConnConfig)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close(context.Background())) }()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	require.NoError(t, err)
	defer rollbackFencedWriterTx(ctx, tx)
	operation, sender := uuid.New(), uuid.New()
	var enrolled bool
	require.NoError(t, tx.QueryRow(ctx, `SELECT vector_writer_operation_begin($1,$2,$3,$4,$5)`,
		operation, sender, LegacyGeneration, "reconcile", "scoped").Scan(&enrolled))
	require.True(t, enrolled)

	freeze := make(chan error, 1)
	go func() { freeze <- c.FreezeWriters(ctx, "g2") }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := observer.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
			AND wait_event_type='Lock' AND query LIKE '%vector_state s WHERE singleton FOR UPDATE%')`).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 25*time.Millisecond)
	require.NoError(t, tx.Commit(ctx))
	var pending *ErrWriterOperationsPending
	require.ErrorAs(t, <-freeze, &pending)
	require.Equal(t, []uuid.UUID{operation}, pending.OperationIDs)
	require.Empty(t, c.writerSlots)
}

func TestWriterOperationRightsAndExactResolution(t *testing.T) {
	ctx := t.Context()
	dsn := startTestDatabase(t)
	c := migrationTestClient(t, dsn)
	applyMigrationPrefix(t, c, 43)
	called := 0
	work := func(context.Context) error { called++; return nil }
	require.NoError(t, c.RunWriterOperation(ctx, domain.WriterOperationReconcile, "scoped", work))
	roles := newGrantRoles(t, dsn, "maintenance", "other", "public_only")
	grantExec(t, c, roles.sql(`CREATE ROLE {maintenance} LOGIN PASSWORD 'fixture-password';
		CREATE ROLE {other} LOGIN PASSWORD 'fixture-password';
		CREATE ROLE {public_only} LOGIN PASSWORD 'fixture-password';
		GRANT EXECUTE ON FUNCTION vector_writer_state() TO {maintenance}, {other};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {public_only};
		ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {public_only}`))
	require.NoError(t, c.RunMigrations(ctx))
	grantExec(t, c, roles.sql(`GRANT UPDATE (writer_operations_enabled) ON vector_state TO {maintenance}`))
	require.NoError(t, c.RunWriterOperation(ctx, domain.WriterOperationReconcile, "scoped", work))
	require.Equal(t, 2, called)
	var count int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM vector_writer_operations`).Scan(&count))
	require.Zero(t, count)
	grantExec(t, c, roles.sql(`GRANT EXECUTE ON FUNCTION vector_writer_operations_enabled(),
		vector_writer_operation_begin(uuid,uuid,text,text,text), vector_writer_operation_mark(uuid,uuid),
		vector_writer_operation_finish(uuid,uuid) TO {maintenance}, {other}`))
	writer := writerLogin(t, dsn, roles.name("maintenance"))
	other := writerLogin(t, dsn, roles.name("other"))
	restricted := writerLogin(t, dsn, roles.name("public_only"))
	var enabled bool
	require.NoError(t, writer.Pool.QueryRow(ctx, `SELECT vector_writer_operations_enabled()`).Scan(&enabled))
	require.False(t, enabled)
	_, err := restricted.Pool.Exec(ctx, `SELECT vector_writer_operations_enabled()`)
	requireSQLState(t, err, "42501")
	_, err = restricted.Pool.Exec(ctx, `SELECT * FROM vector_writer_operations`)
	requireSQLState(t, err, "42501")
	_, err = writer.Pool.Exec(ctx, `SELECT * FROM vector_writer_operation_drain_status()`)
	requireSQLState(t, err, "42501")
	_, err = writer.Pool.Exec(ctx, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	requireSQLState(t, err, "42501")
	require.ErrorContains(t, err, "requires the metadata schema owner")
	grantExec(t, c, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	id, sender := uuid.New(), uuid.New()
	const begin = `SELECT vector_writer_operation_begin($1,$2,$3,$4,$5)`
	for _, invalid := range []struct {
		id                          uuid.UUID
		generation, kind, reference string
	}{
		{uuid.Nil, LegacyGeneration, "delete", "document"},
		{id, LegacyGeneration, "reconcile'; SELECT 1", "scope"},
		{id, LegacyGeneration, "reconcile", "credential=untrusted"},
	} {
		_, err = writer.Pool.Exec(ctx, begin, invalid.id, sender, invalid.generation, invalid.kind, invalid.reference)
		require.Error(t, err)
	}
	for _, generation := range []string{"invalid generation", "different"} {
		var admitted bool
		require.NoError(t, writer.Pool.QueryRow(ctx, begin, id, sender, generation, "reconcile", "scoped").Scan(&admitted))
		require.False(t, admitted)
	}
	_, err = writer.Pool.Exec(ctx, begin, id, sender, LegacyGeneration, "reconcile", "scoped")
	require.NoError(t, err)
	_, err = writer.Pool.Exec(ctx, begin, id, sender, LegacyGeneration, "reconcile", "scoped")
	require.Error(t, err)
	var resolved bool
	require.NoError(t, other.Pool.QueryRow(ctx, `SELECT vector_writer_operation_finish($1,$2)`, id, sender).Scan(&resolved))
	require.False(t, resolved)
	require.NoError(t, other.Pool.QueryRow(ctx, `SELECT vector_writer_operation_mark($1,$2)`, id, sender).Scan(&resolved))
	require.False(t, resolved)
	require.NoError(t, writer.Pool.QueryRow(ctx, `SELECT vector_writer_operation_mark($1,$2)`, id, sender).Scan(&resolved))
	require.True(t, resolved)
	require.NoError(t, writer.Pool.QueryRow(ctx, `SELECT vector_writer_operation_finish($1,$2)`, id, uuid.New()).Scan(&resolved))
	require.False(t, resolved)
	for _, update := range []string{
		`UPDATE vector_state SET frozen=TRUE, frozen_at=clock_timestamp(), freeze_generation='g2'`,
		`UPDATE vector_state SET writer_operations_enabled=FALSE`,
	} {
		_, err = c.Pool.Exec(ctx, update)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "23514", pgErr.Code)
		require.Equal(t, "vector_writer_operations_drained", pgErr.ConstraintName)
	}
	require.NoError(t, writer.Pool.QueryRow(ctx, `SELECT vector_writer_operation_finish($1,$2)`, id, sender).Scan(&resolved))
	require.True(t, resolved)
	require.NoError(t, writer.Pool.QueryRow(ctx, `SELECT vector_writer_operation_finish($1,$2)`, id, sender).Scan(&resolved))
	require.False(t, resolved)
}

func TestWriterOperationCancellationDuringSendKeepsReservation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	state := externalWriterState{mark: func(context.Context) error { return nil }}
	go func() {
		done <- state.dispatch(ctx, func(ctx context.Context) (bool, error) {
			close(entered)
			<-release
			return false, ctx.Err()
		})
	}()
	<-entered
	cancel()
	require.False(t, state.close())
	close(release)
	require.ErrorIs(t, <-done, context.Canceled)
	require.False(t, state.close())
}

func TestWriterOperationFinishFailurePreservesWorkErrorAndReservation(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	grantExec(t, c, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	workErr := errors.New("receipt checkpoint failed")
	err := c.RunWriterOperation(ctx, domain.WriterOperationDelete, uuid.NewString(), func(ctx context.Context) error {
		require.NoError(t, domain.DispatchExternalWriter(ctx, func(context.Context) (bool, error) { return true, nil }))
		grantExecContext(ctx, t, c, `DROP FUNCTION vector_writer_operation_finish(uuid,uuid)`)
		return workErr
	})
	require.ErrorIs(t, err, workErr)
	var finish *WriterOperationFinishError
	require.ErrorAs(t, err, &finish)
	var operation uuid.UUID
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT operation_id FROM vector_writer_operations`).Scan(&operation))
	require.Equal(t, operation, finish.OperationID)
	var pending *ErrWriterOperationsPending
	require.ErrorAs(t, c.FreezeWriters(ctx, "g2"), &pending)
}

func TestWriterOperationMissingReservationPreventsDispatch(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	grantExec(t, c, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	err := c.RunWriterOperation(ctx, domain.WriterOperationDelete, uuid.NewString(), func(ctx context.Context) error {
		// Model a replacement database missing the acknowledged enrollment.
		grantExecContext(ctx, t, c, `DELETE FROM vector_writer_operations`)
		return domain.DispatchExternalWriter(ctx, func(context.Context) (bool, error) {
			t.Fatal("missing reservation allowed provider dispatch")
			return false, nil
		})
	})
	require.ErrorContains(t, err, "reservation missing")
}

func TestWriterOperationDispatchWithFixtureRemovalAndOneConnection(t *testing.T) {
	c, observer := sessionTestClientWithPool(t, 1)
	c.EnableHostedVectorManifests(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	grantExec(t, c, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	id := domain.DocumentID(uuid.NewSHA1(uuid.NameSpaceOID, []byte("fixture-removal")).String())
	sends := 0
	require.NoError(t, c.RunWriterOperation(ctx, domain.WriterOperationDelete, string(id), func(ctx context.Context) error {
		return c.WithOrdinaryRemoval(ctx, id, func(ctx context.Context) error {
			manifests, err := c.ListDocumentVectorManifests(ctx, id, "")
			require.NoError(t, err)
			require.Empty(t, manifests)
			return domain.DispatchExternalWriter(ctx, func(ctx context.Context) (bool, error) {
				var marked bool
				require.NoError(t, observer.QueryRow(ctx, `SELECT pending_provider FROM vector_writer_operations`).Scan(&marked))
				require.True(t, marked)
				sends++
				return true, nil
			})
		})
	}))
	require.Equal(t, 1, sends)
	require.NoError(t, c.FreezeWriters(ctx, "g2"))
}

func TestWriterSQLSettlementIsMonotoneThroughDetachedCleanup(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		settled bool
	}{
		{"acknowledged rollback", pgx.ErrTxCommitRollback, true},
		{"acknowledged rejection", &pgconn.PgError{Code: "23514", SeverityUnlocalized: "ERROR"}, true},
		{"acknowledged statement cancellation", &pgconn.PgError{Code: "57014", SeverityUnlocalized: "ERROR"}, true},
		{"no matching row", pgx.ErrNoRows, true},
		{"client timeout", context.DeadlineExceeded, false},
		{"connection reset", &pgconn.PgError{Code: "08006", SeverityUnlocalized: "FATAL"}, false},
		{"completion unknown", &pgconn.PgError{Code: "40003", SeverityUnlocalized: "ERROR"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			state := &externalWriterState{}
			ctx = context.WithValue(ctx, writerSQLSettlementKey{}, state)
			cancel()
			detached := context.WithoutCancel(ctx)
			db := heldClaimTestDB{exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, test.err
			}}
			err := clearHeldDeletionClaim(detached, db, domain.DocumentID(uuid.NewString()), uuid.NewString(), uuid.NewString())
			require.ErrorIs(t, err, test.err)
			require.Equal(t, test.settled, retryWriterSQL(detached, err))
			require.NoError(t, observeWriterSQL(detached, nil))
			require.NoError(t, state.dispatch(detached, func(context.Context) (bool, error) { return true, nil }))
			require.Equal(t, test.settled, state.close())
		})
	}
}
