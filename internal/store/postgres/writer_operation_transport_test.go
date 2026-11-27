package postgres

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// writerFaultTrace arms the socket fault only on the exact connection sending
// the statement under test. Observer and finish connections stay healthy.
type writerFaultTrace struct {
	match  func(string) bool
	before func(context.Context, *pgx.Conn)
	armed  atomic.Bool
}

func (f *writerFaultTrace) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if f.match(data.SQL) && f.armed.CompareAndSwap(false, true) {
		f.before(ctx, conn)
	}
	return ctx
}

func (*writerFaultTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type writerReplyFault struct {
	net.Conn
	inject       atomic.Bool
	failed       atomic.Bool
	allowFailure <-chan struct{}
}

func (f *writerReplyFault) Read(p []byte) (int, error) {
	if f.inject.Load() {
		<-f.allowFailure
		f.failed.Store(true)
		return 0, os.ErrDeadlineExceeded
	}
	return f.Conn.Read(p)
}

func (f *writerReplyFault) Write(p []byte) (int, error) {
	if f.failed.Load() {
		return 0, net.ErrClosed
	}
	return f.Conn.Write(p)
}

func (f *writerReplyFault) Close() error {
	if f.failed.Load() {
		return nil // The test closes the underlying socket after checking settlement.
	}
	return f.Conn.Close()
}

type writerCancelAddress struct{}

func (writerCancelAddress) Network() string { return "tcp" }

func (writerCancelAddress) String() string { return "deferred writer test cancellation" }

func (f *writerReplyFault) RemoteAddr() net.Addr {
	if f.failed.Load() {
		return writerCancelAddress{}
	}
	return f.Conn.RemoteAddr()
}

func writerFaultClient(t *testing.T, owner *Client, trace pgx.QueryTracer, allowFailure <-chan struct{}, opts ...Option) *Client {
	t.Helper()
	dsn := withFixtureOptions(t, owner.Pool.Config().ConnString(), "sslmode=disable")
	configureFault := func(config *pgxpool.Config) {
		config.ConnConfig.Tracer = trace
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
		dial := config.ConnConfig.DialFunc
		config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == (writerCancelAddress{}).String() {
				return nil, errors.New("cancel request held away from the backend")
			}
			conn, dialErr := dial(ctx, network, address)
			if dialErr != nil {
				return nil, dialErr
			}
			t.Cleanup(func() { _ = conn.Close() })
			return &writerReplyFault{Conn: conn, allowFailure: allowFailure}, nil
		}
	}
	c, err := NewClient(t.Context(), dsn, append(opts, configureFault)...)
	require.NoError(t, err)
	t.Cleanup(c.Pool.Close)
	return c
}

func TestWriterSQLTimeoutCannotOutrunFreeze(t *testing.T) {
	for _, test := range []struct {
		name     string
		deferred bool
		legacy   bool
	}{
		{name: "delete statement"},
		{name: "commit with deferred trigger", deferred: true},
		{name: "legacy local commit", deferred: true, legacy: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner, observer := sessionTestClientWithPool(t, 2)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			doc := acceptDocument(t, owner, test.name, false, false)
			var later *domain.Document
			if test.legacy {
				other := acceptDocument(t, owner, "later cleanup", false, false)
				later = &domain.Document{ID: other.id, Incarnation: other.incarnation}
			}
			grantExec(t, owner, `UPDATE vector_state SET writer_operations_enabled=TRUE;
				CREATE FUNCTION pause_writer_test_delete() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN PERFORM pg_advisory_xact_lock(71612623); RETURN OLD; END $$`)
			switch {
			case test.legacy:
				seedLegacyLocalPendingMutation(t, owner, doc.id)
				grantExec(t, owner, `CREATE CONSTRAINT TRIGGER pause_writer_test_delete AFTER UPDATE ON documents
					DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
					WHEN (OLD.vector_mutation_pending AND NOT NEW.vector_mutation_pending)
					EXECUTE FUNCTION pause_writer_test_delete()`)
			case test.deferred:
				grantExec(t, owner, `CREATE CONSTRAINT TRIGGER pause_writer_test_delete AFTER DELETE ON documents
					DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pause_writer_test_delete()`)
			default:
				grantExec(t, owner, `CREATE TRIGGER pause_writer_test_delete BEFORE DELETE ON documents
					FOR EACH ROW EXECUTE FUNCTION pause_writer_test_delete()`)
			}
			_, err := observer.Exec(ctx, `SELECT pg_advisory_lock(71612623)`)
			require.NoError(t, err)
			pid := make(chan uint32, 1)
			allowFailure := make(chan struct{})
			fail := sync.OnceFunc(func() { close(allowFailure) })
			defer fail()
			target := &writerFaultTrace{
				match: func(query string) bool {
					return !test.deferred && strings.HasPrefix(query, "DELETE FROM documents WHERE id=$1") ||
						test.deferred && query == "commit"
				},
				before: func(_ context.Context, conn *pgx.Conn) {
					conn.PgConn().Conn().(*writerReplyFault).inject.Store(true)
					pid <- conn.PgConn().PID()
				},
			}
			// Install the fault after the enrollment commit so only work is affected.
			productionDB := writerFaultClient(t, owner, target, allowFailure)
			result := make(chan error, 1)
			go func() {
				result <- owner.RunWriterOperation(ctx, domain.WriterOperationDelete, string(doc.id), func(ctx context.Context) error {
					if test.legacy {
						workErr := prepareLegacyLocalCleanup(ctx, productionDB, doc.id)
						// A different, successful SQL mutation must not settle the earlier COMMIT.
						return errors.Join(workErr, owner.RemoveLiveDocument(ctx, later))
					}
					if !test.deferred {
						return productionDB.RemoveLiveDocument(ctx, &domain.Document{ID: doc.id, Incarnation: doc.incarnation})
					}
					tx, beginErr := productionDB.Pool.Begin(ctx)
					if beginErr != nil {
						return beginErr
					}
					defer rollbackFencedWriterTx(ctx, tx)
					if _, deleteErr := tx.Exec(ctx, `DELETE FROM documents WHERE id=$1`, string(doc.id)); deleteErr != nil {
						return deleteErr
					}
					return commitWriterSQL(ctx, tx)
				})
			}()
			var backend uint32
			select {
			case backend = <-pid:
			case err = <-result:
				t.Fatalf("mutation ended before reaching the fault: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			writerTestWaitBlocked(ctx, t, observer, backend)
			fail()
			select {
			case err = <-result:
				require.ErrorIs(t, err, os.ErrDeadlineExceeded)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			writerTestWaitBlocked(ctx, t, observer, backend)
			if test.legacy {
				var exists bool
				require.NoError(t, observer.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM documents WHERE id=$1)`, string(later.ID)).Scan(&exists))
				require.False(t, exists, "the later mutation committed despite the earlier lost reply")
			}
			var pending *ErrWriterOperationsPending
			require.ErrorAs(t, owner.FreezeWriters(ctx, "next"), &pending)
			require.Len(t, pending.OperationIDs, 1)
			_, err = observer.Exec(ctx, `SELECT pg_advisory_unlock(71612623)`)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				if test.legacy {
					var stopped bool
					err := observer.QueryRow(ctx, `SELECT NOT vector_mutation_pending AND vector_mutation_uncertain
						FROM documents WHERE id=$1`, string(doc.id)).Scan(&stopped)
					return err == nil && stopped
				}
				var live bool
				err := observer.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM documents WHERE id=$1)`, string(doc.id)).Scan(&live)
				return err == nil && !live
			}, 5*time.Second, 20*time.Millisecond, "the original request can still commit after the timeout")
		})
	}
}

func seedLegacyLocalPendingMutation(t *testing.T, owner *Client, id domain.DocumentID) {
	t.Helper()
	grantExec(t, owner, `UPDATE documents SET status='failed',vector_mutation_pending=TRUE,
		vector_mutation_until=clock_timestamp()-interval '1 minute' WHERE id=$1`, string(id))
}

func prepareLegacyLocalCleanup(ctx context.Context, c *Client, id domain.DocumentID) error {
	return c.WithLegacyDeletion(ctx, id, func(store domain.LegacyDocumentStore) error {
		_, err := store.PrepareLegacyDeletion(ctx, id, domain.LegacyDeletionSpec{})
		return err
	})
}

func TestWriterLegacyLocalCommitKnownOutcomesAllowFreeze(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "acknowledged commit"
		if reject {
			name = "proven rollback"
		}
		t.Run(name, func(t *testing.T) {
			owner, _ := sessionTestClientWithPool(t, 2)
			ctx := t.Context()
			doc := acceptDocument(t, owner, name, false, false)
			seedLegacyLocalPendingMutation(t, owner, doc.id)
			grantExec(t, owner, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
			if reject {
				grantExec(t, owner, `CREATE FUNCTION reject_writer_test_commit() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN RAISE check_violation USING MESSAGE='reject local cleanup commit'; END $$;
					CREATE CONSTRAINT TRIGGER reject_writer_test_commit AFTER UPDATE ON documents
					DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
					WHEN (OLD.vector_mutation_pending AND NOT NEW.vector_mutation_pending)
					EXECUTE FUNCTION reject_writer_test_commit()`)
			}
			err := owner.RunWriterOperation(ctx, domain.WriterOperationDelete, string(doc.id), func(ctx context.Context) error {
				return prepareLegacyLocalCleanup(ctx, owner, doc.id)
			})
			if reject {
				requireSQLState(t, err, "23514")
			} else {
				require.NoError(t, err)
			}
			var pending, uncertain bool
			require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT vector_mutation_pending,vector_mutation_uncertain
				FROM documents WHERE id=$1`, string(doc.id)).Scan(&pending, &uncertain))
			require.Equal(t, reject, pending)
			require.Equal(t, !reject, uncertain)
			require.NoError(t, owner.FreezeWriters(ctx, "next"))
		})
	}
}

func writerTestWaitBlocked(ctx context.Context, t *testing.T, observer *pgx.Conn, backend uint32) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting bool
		err := observer.QueryRow(ctx, `SELECT pg_backend_pid()=ANY(pg_blocking_pids($1::integer))`, backend).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 20*time.Millisecond)
}

func TestWriterEnrollmentAndMarkCommitLoss(t *testing.T) {
	for _, mark := range []bool{false, true} {
		phase := "enrollment"
		commitNumber := int32(1)
		if mark {
			phase = "mark"
			commitNumber = 2
		}
		for _, mode := range []string{"terminate", "idle timeout", "lost reply"} {
			t.Run(phase+"/"+mode, func(t *testing.T) {
				owner, observer := sessionTestClientWithPool(t, 2)
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				grantExec(t, owner, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
				allowFailure := make(chan struct{})
				fail := sync.OnceFunc(func() { close(allowFailure) })
				defer fail()
				var commits atomic.Int32
				pids := make(chan uint32, 1)
				innerErrors := make(chan error, 1)
				trace := &writerFaultTrace{
					match: func(query string) bool {
						return query == "commit" && commits.Add(1) == commitNumber
					},
					before: func(ctx context.Context, conn *pgx.Conn) {
						pid := conn.PgConn().PID()
						pids <- pid
						switch mode {
						case "terminate":
							var killed bool
							err := observer.QueryRow(ctx, `SELECT pg_terminate_backend($1, 5000)`, pid).Scan(&killed)
							if err == nil && !killed {
								err = errors.New("the intended transaction backend was not terminated")
							}
							innerErrors <- err
						case "idle timeout":
							var timeout string
							if err := conn.QueryRow(ctx, `SHOW idle_in_transaction_session_timeout`).Scan(&timeout); err != nil {
								innerErrors <- err
								return
							}
							if timeout != "600ms" {
								innerErrors <- errors.New("unexpected idle transaction timeout on target backend: " + timeout)
								return
							}
							for ctx.Err() == nil {
								var gone bool
								err := observer.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)`, pid).Scan(&gone)
								if err != nil || gone {
									innerErrors <- err
									return
								}
								time.Sleep(25 * time.Millisecond)
							}
							innerErrors <- ctx.Err()
						case "lost reply":
							conn.PgConn().Conn().(*writerReplyFault).inject.Store(true)
							innerErrors <- nil
						}
					},
				}
				var opts []Option
				if mode == "idle timeout" {
					opts = append(opts, func(config *pgxpool.Config) {
						config.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "600ms"
					})
				}
				c := writerFaultClient(t, owner, trace, allowFailure, opts...)
				var retained bytes.Buffer
				c.writerEvidence = newWriterRecoveryEvidence(&retained)
				var callbacks, sends atomic.Int32
				result := make(chan error, 1)
				go func() {
					result <- c.RunWriterOperation(ctx, domain.WriterOperationReconcile, "scoped", func(ctx context.Context) error {
						callbacks.Add(1)
						return domain.DispatchExternalWriter(ctx, func(context.Context) (bool, error) {
							sends.Add(1)
							return true, nil
						})
					})
				}()
				select {
				case <-pids:
				case err := <-result:
					t.Fatalf("%s ended before the target commit: %v", phase, err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case err := <-innerErrors:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if mode == "lost reply" {
					require.Eventually(t, func() bool {
						var found bool
						err := owner.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vector_writer_operations WHERE pending_provider=$1)`, mark).Scan(&found)
						return err == nil && found
					}, 5*time.Second, 20*time.Millisecond, "the server committed before the client loses the reply")
				}
				fail()
				select {
				case err := <-result:
					require.Error(t, err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				require.Zero(t, sends.Load())
				require.Contains(t, retained.String(), "writer operation enrollment started")
				require.NotContains(t, retained.String(), "writer provider attempt")
				if mark {
					require.EqualValues(t, 1, callbacks.Load())
				} else {
					require.Zero(t, callbacks.Load())
				}
				var pending *ErrWriterOperationsPending
				if mark || mode == "lost reply" {
					require.ErrorAs(t, owner.FreezeWriters(ctx, "next"), &pending)
					require.Len(t, pending.OperationIDs, 1)
					require.Contains(t, retained.String(), pending.OperationIDs[0].String())
				} else {
					require.NoError(t, owner.FreezeWriters(ctx, "next"))
				}
			})
		}
	}
}

func TestHostedPublicationLostReplyKeepsWriterReservation(t *testing.T) {
	f := newOwnedFixture(t)
	id, _ := f.legacyDocument(t, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	doc := hostedCandidate(ctx, t, f.c, id)
	grantExec(t, f.c, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	started := make(chan struct{})
	allowFailure := make(chan struct{})
	fail := sync.OnceFunc(func() { close(allowFailure) })
	defer fail()
	trace := &writerFaultTrace{
		match: func(query string) bool { return query == "commit" },
		before: func(_ context.Context, conn *pgx.Conn) {
			conn.PgConn().Conn().(*writerReplyFault).inject.Store(true)
			close(started)
		},
	}
	publisher := writerFaultClient(t, f.c, trace, allowFailure)
	done := make(chan error, 1)
	go func() {
		done <- f.c.RunWriterOperation(ctx, domain.WriterOperationReconcile, "scoped", func(ctx context.Context) error {
			if err := domain.DispatchExternalWriter(ctx, func(context.Context) (bool, error) { return true, nil }); err != nil {
				return err
			}
			_, err := publisher.PublishHostedBackfill(ctx, doc)
			return err
		})
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("hosted publication ended before COMMIT: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Eventually(t, func() bool {
		var ready bool
		err := f.c.Pool.QueryRow(ctx, `SELECT ready FROM grant_catalog_document_ownership WHERE document_id=$1`, id).Scan(&ready)
		return err == nil && ready
	}, 5*time.Second, 20*time.Millisecond)
	fail()
	select {
	case err := <-done:
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var pending *ErrWriterOperationsPending
	require.ErrorAs(t, f.c.FreezeWriters(ctx, "next"), &pending)
	require.Len(t, pending.OperationIDs, 1)
}
