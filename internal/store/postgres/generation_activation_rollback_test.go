package postgres

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	activationTarget = "g2"
	activationDigest = "digest"
	wrongFreezeError = `postgres: writers are frozen for generation "g3", not "g2"`
)

type activationSocketKey struct{}
type activationQueryKey struct{}

type activationFaultMode int32

const (
	activationNoFault activationFaultMode = iota
	activationBlockWrite
	activationBlockRead
	activationLoseRead
)

type activationSocket struct {
	net.Conn
	mode         atomic.Int32
	failed       atomic.Bool
	laterWrites  atomic.Int32
	started      time.Time
	blocked      chan time.Time
	deadline     chan time.Time
	releaseRead  chan struct{}
	watchdog     <-chan struct{}
	stopWatchdog context.CancelFunc
	closed       chan struct{}
	closeOnce    sync.Once
}

func (s *activationSocket) Close() error {
	err := s.Conn.Close()
	s.closeOnce.Do(func() { close(s.closed) })
	return err
}

func (s *activationSocket) SetDeadline(deadline time.Time) error {
	err := s.Conn.SetDeadline(deadline)
	if !deadline.IsZero() {
		select {
		case s.deadline <- deadline:
		default:
		}
	}
	return err
}

func (s *activationSocket) awaitDeadline() (int, error) {
	s.blocked <- s.started
	select {
	case deadline := <-s.deadline:
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case <-timer.C:
			s.failed.Store(true)
			return 0, os.ErrDeadlineExceeded
		case <-s.watchdog:
		}
	case <-s.watchdog:
	}
	return 0, net.ErrClosed
}

func (s *activationSocket) Write(p []byte) (int, error) {
	if s.mode.CompareAndSwap(int32(activationBlockWrite), 0) {
		return s.awaitDeadline()
	}
	if s.failed.Load() {
		s.laterWrites.Add(1)
		return 0, net.ErrClosed
	}
	return s.Conn.Write(p)
}

func (s *activationSocket) Read(p []byte) (int, error) {
	switch {
	case s.mode.CompareAndSwap(int32(activationBlockRead), 0):
		return s.awaitDeadline()
	case s.mode.CompareAndSwap(int32(activationLoseRead), 0):
		s.blocked <- s.started
		select {
		case <-s.releaseRead:
			return 0, io.ErrUnexpectedEOF
		case <-s.watchdog:
			return 0, net.ErrClosed
		}
	default:
		return s.Conn.Read(p)
	}
}

type activationQuery struct {
	sql      string
	pid      uint32
	started  time.Time
	deadline time.Time
	value    any
	entryErr error
	tag      string
	err      error
}

type activationTrace struct {
	queries       []activationQuery
	cancelOnLock  context.CancelFunc
	faultSQL      string
	mode          activationFaultMode
	parkCancel    atomic.Bool
	releaseCancel <-chan struct{}
}

func (tr *activationTrace) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	deadline, _ := ctx.Deadline()
	query := activationQuery{
		sql: data.SQL, pid: conn.PgConn().PID(), started: time.Now(), deadline: deadline,
		value: ctx.Value(activationValueKey{}), entryErr: ctx.Err(),
	}
	if tr.faultSQL != "" && data.SQL == tr.faultSQL {
		transport := conn.PgConn().Conn()
		if secure, ok := transport.(*tls.Conn); ok {
			transport = secure.NetConn()
		}
		if socket, ok := transport.(*activationSocket); ok {
			// Ignore deadlines set during connection establishment.
			select {
			case <-socket.deadline:
			default:
			}
			tr.parkCancel.Store(true)
			socket.started = query.started
			socket.mode.Store(int32(tr.mode))
		}
	}
	return context.WithValue(ctx, activationQueryKey{}, query)
}

func (tr *activationTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	query, _ := ctx.Value(activationQueryKey{}).(activationQuery)
	query.tag = data.CommandTag.String()
	query.err = data.Err
	tr.queries = append(tr.queries, query)
	if tr.cancelOnLock != nil && data.Err == nil &&
		strings.Contains(query.sql, "FROM vector_generations") && strings.HasSuffix(query.sql, "FOR UPDATE") {
		tr.cancelOnLock()
	}
}

func (tr *activationTrace) controls() []string {
	var result []string
	for _, query := range tr.queries {
		switch {
		case strings.HasPrefix(query.sql, "begin"):
			result = append(result, "begin")
		case query.sql == "commit" || query.sql == "rollback":
			result = append(result, query.sql)
		case strings.Contains(query.sql, "pg_advisory_unlock"):
			result = append(result, "unlock")
		}
	}
	return result
}

func (tr *activationTrace) query(t *testing.T, sql string) activationQuery {
	t.Helper()
	for _, query := range tr.queries {
		if query.sql == sql {
			return query
		}
	}
	t.Fatalf("missing query %q", sql)
	return activationQuery{}
}

func activationFaultClient(t *testing.T, dsn string, trace *activationTrace, secure *tls.Config) *Client {
	t.Helper()
	client, err := NewClient(t.Context(), dsn, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.Tracer = trace
		if secure != nil {
			cfg.ConnConfig.TLSConfig = secure
			cfg.ConnConfig.Fallbacks = nil
		}
		dial := cfg.ConnConfig.DialFunc
		cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			if socket, ok := ctx.Value(activationSocketKey{}).(*activationSocket); ok {
				conn, err := dial(ctx, network, address)
				if err != nil {
					return nil, err
				}
				socket.Conn = conn
				return socket, nil
			}
			if trace.parkCancel.Load() {
				// Keep pgx's asynchronous cleanup from disposing the primary
				// socket until the test has observed activation's disposal.
				select {
				case <-trace.releaseCancel:
					return nil, net.ErrClosed
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return dial(ctx, network, address)
		}
	})
	require.NoError(t, err)
	t.Cleanup(client.Pool.Close)
	return client
}

func activationDatabase(t *testing.T, dsn string) *Client {
	t.Helper()
	owner := migrationTestClient(t, dsn)
	require.NoError(t, owner.RunMigrations(t.Context()))
	verifiedGeneration(t, owner, activationTarget, activationDigest)
	return owner
}

func wrongActivationFreeze(t *testing.T, owner *Client) {
	t.Helper()
	require.NoError(t, owner.ThawWriters(t.Context()))
	require.NoError(t, owner.FreezeWriters(t.Context(), "g3"))
}

func openActivationFence(ctx context.Context, t *testing.T, client *Client, name string) *GenerationFence {
	t.Helper()
	fence, err := client.FenceGeneration(ctx, name)
	require.NoError(t, err)
	t.Cleanup(func() { fence.Close(ctx) })
	return fence
}

func activationFaultFixture(t *testing.T, sql string, mode activationFaultMode) (*activationSocket, *activationTrace) {
	t.Helper()
	watchdog, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 12*time.Second)
	t.Cleanup(cancel)
	socket := &activationSocket{
		blocked: make(chan time.Time, 1), deadline: make(chan time.Time, 1),
		releaseRead: make(chan struct{}), watchdog: watchdog.Done(), closed: make(chan struct{}), stopWatchdog: cancel,
	}
	return socket, &activationTrace{faultSQL: sql, mode: mode, releaseCancel: watchdog.Done()}
}

func startFaultedActivation(ctx context.Context, t *testing.T, fence *GenerationFence, socket *activationSocket) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- fence.Activate(ctx, activationDigest, 0)
	}()
	t.Cleanup(func() {
		// A failed assertion must still release the transport and join the
		// activation before the earlier fence cleanup touches the connection.
		socket.stopWatchdog()
		_ = socket.Close()
		<-done
		// Join pgx's asynchronous cleanup after the watchdog releases its dial.
		if fence.conn.IsClosed() {
			<-fence.conn.PgConn().CleanupDone()
		}
	})
	return result
}

func awaitActivationSocket(t *testing.T, socket *activationSocket) time.Time {
	t.Helper()
	select {
	case started := <-socket.blocked:
		return started
	case <-socket.watchdog:
		t.Fatal("activation did not enter the transport fault")
		return time.Time{}
	}
}

func activationResult(t *testing.T, result <-chan error, deadline time.Time) error {
	t.Helper()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		t.Fatal("activation did not return before the deadline")
		return nil
	}
}

func assertActivationRowLocks(ctx context.Context, t *testing.T, owner *Client, held bool) {
	t.Helper()
	for _, sql := range []string{
		`SELECT 1 FROM vector_state WHERE singleton FOR UPDATE NOWAIT`,
		`SELECT 1 FROM vector_generations WHERE name='g2' FOR UPDATE NOWAIT`,
	} {
		var sentinel int
		err := owner.Pool.QueryRow(ctx, sql).Scan(&sentinel)
		if held {
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, "55P03", pgErr.Code)
		} else {
			require.NoError(t, err)
		}
	}
}

func activationAdvisoryLocks(ctx context.Context, t *testing.T, owner *Client, pid uint32) int {
	t.Helper()
	var count int
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks
 WHERE pid=$1 AND locktype='advisory' AND classid=$2 AND granted`, pid, uint32(reindexLockNamespace)).Scan(&count))
	return count
}

func awaitActivationBackendExit(t *testing.T, owner *Client, pid uint32, deadline time.Time) {
	t.Helper()
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	require.Eventually(t, func() bool {
		var gone bool
		err := owner.Pool.QueryRow(ctx, `SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1)
 AND NOT EXISTS (SELECT 1 FROM pg_locks WHERE pid=$1)`, pid).Scan(&gone)
		return err == nil && gone
	}, time.Until(deadline), 20*time.Millisecond)
}

func assertActivationRollbackContext(methodCtx context.Context, t *testing.T, trace *activationTrace) {
	t.Helper()
	rollback := trace.query(t, "rollback")
	methodDeadline, _ := methodCtx.Deadline()
	require.NoError(t, rollback.entryErr)
	require.Equal(t, methodCtx.Value(activationValueKey{}), rollback.value)
	require.NotEqual(t, methodDeadline, rollback.deadline)
	require.WithinDuration(t, rollback.started.Add(3*time.Second), rollback.deadline, 100*time.Millisecond)
}

func assertActivationSocketClosed(t *testing.T, socket *activationSocket, fence *GenerationFence) {
	t.Helper()
	require.True(t, fence.conn.IsClosed())
	select {
	case <-socket.closed:
	default:
		t.Fatal("activation returned before closing the transport")
	}
}

func TestGenerationActivationRollbackDeadlineDiscardsFence(t *testing.T) {
	dsn := startTestDatabase(t)
	owner := activationDatabase(t, dsn)
	wrongActivationFreeze(t, owner)
	socket, trace := activationFaultFixture(t, "rollback", activationBlockWrite)
	client := activationFaultClient(t, dsn, trace, nil)
	fence := openActivationFence(context.WithValue(t.Context(), activationSocketKey{}, socket), t, client, activationTarget)
	oldPID := fence.conn.PgConn().PID()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	result := startFaultedActivation(ctx, t, fence, socket)
	started := awaitActivationSocket(t, socket)

	observerCtx, stopObserver := context.WithDeadline(t.Context(), started.Add(2*time.Second))
	defer stopObserver()
	assertActivationRowLocks(observerCtx, t, owner, true)
	require.Equal(t, 1, activationAdvisoryLocks(observerCtx, t, owner, oldPID))
	require.EqualError(t, activationResult(t, result, started.Add(5*time.Second)), wrongFreezeError)
	assertActivationRollbackContext(ctx, t, trace)
	require.Error(t, trace.query(t, "rollback").err)
	assertActivationSocketClosed(t, socket, fence)
	fence.Close(ctx)
	require.Equal(t, []string{"begin", "rollback"}, trace.controls())
	awaitActivationBackendExit(t, owner, oldPID, started.Add(6*time.Second))
	assertActivationRowLocks(ctx, t, owner, false)

	require.NoError(t, owner.ThawWriters(ctx))
	verifiedGeneration(t, owner, activationTarget, activationDigest)
	retryTrace := &activationTrace{}
	retry := activationFaultClient(t, dsn, retryTrace, nil)
	require.NoError(t, retry.ActivateGeneration(ctx, activationTarget, activationDigest, 0))
	require.NotEqual(t, oldPID, retryTrace.query(t, "commit").pid)
	require.Equal(t, "COMMIT", retryTrace.query(t, "commit").tag)
	require.Equal(t, []string{"begin", "commit", "unlock"}, retryTrace.controls())
}

func TestGenerationActivationCanceledParentStillRollsBack(t *testing.T) {
	dsn := startTestDatabase(t)
	owner := activationDatabase(t, dsn)
	for _, stalled := range []bool{false, true} {
		name := "prompt"
		mode := activationNoFault
		if stalled {
			name = "stalled"
			mode = activationBlockWrite
		}
		t.Run(name, func(t *testing.T) {
			socket, trace := activationFaultFixture(t, "rollback", mode)
			if !stalled {
				trace.faultSQL = ""
			}
			client := activationFaultClient(t, dsn, trace, nil)
			fence := openActivationFence(context.WithValue(t.Context(), activationSocketKey{}, socket), t, client, activationTarget)
			pid := fence.conn.PgConn().PID()
			ctx, cancel := context.WithCancel(context.WithValue(t.Context(), activationValueKey{}, "sentinel"))
			defer cancel()
			trace.cancelOnLock = cancel
			result := startFaultedActivation(ctx, t, fence, socket)
			deadline := time.Now().Add(5 * time.Second)
			if stalled {
				deadline = awaitActivationSocket(t, socket).Add(5 * time.Second)
			}
			err := activationResult(t, result, deadline)
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorContains(t, err, "postgres: read vector state: ")
			assertActivationRollbackContext(ctx, t, trace)
			state, err := owner.VectorState(t.Context())
			require.NoError(t, err)
			require.Equal(t, LegacyGeneration, state.Active)
			if stalled {
				require.Error(t, trace.query(t, "rollback").err)
				assertActivationSocketClosed(t, socket, fence)
			} else {
				require.Equal(t, "ROLLBACK", trace.query(t, "rollback").tag)
				require.False(t, fence.conn.IsClosed())
				require.Equal(t, pid, fence.conn.PgConn().PID())
				require.Equal(t, 1, activationAdvisoryLocks(t.Context(), t, owner, pid))
				assertActivationRowLocks(t.Context(), t, owner, false)
			}
			fence.Close(ctx)
			if stalled {
				require.Equal(t, []string{"begin", "rollback"}, trace.controls())
			} else {
				require.Equal(t, []string{"begin", "rollback", "unlock"}, trace.controls())
			}
			awaitActivationBackendExit(t, owner, pid, deadline.Add(time.Second))
		})
	}
}

func TestGenerationActivationRollbackDoesNotChangeOutcome(t *testing.T) {
	dsn := startTestDatabase(t)
	owner := activationDatabase(t, dsn)

	t.Run("refusal on a healthy fence", func(t *testing.T) {
		ctx := t.Context()
		trace := &activationTrace{}
		client := activationFaultClient(t, dsn, trace, nil)
		fence := openActivationFence(ctx, t, client, activationTarget)
		before, err := owner.VectorState(ctx)
		require.NoError(t, err)
		target, err := owner.Generation(ctx, activationTarget)
		require.NoError(t, err)

		require.EqualError(t, fence.Activate(ctx, "different", 0),
			`postgres: corpus changed since verification of generation "g2"`)
		after, err := owner.VectorState(ctx)
		require.NoError(t, err)
		require.Equal(t, before, after)
		unchanged, err := owner.Generation(ctx, activationTarget)
		require.NoError(t, err)
		require.Equal(t, target, unchanged)
		require.Equal(t, "ROLLBACK", trace.query(t, "rollback").tag)
		require.False(t, fence.conn.IsClosed())
		require.Equal(t, []string{"begin", "rollback"}, trace.controls())
	})

	t.Run("server abort at commit", func(t *testing.T) {
		ctx := t.Context()
		grantExec(t, owner, `CREATE FUNCTION abort_generation_activation() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION 'fixture rejected activation at commit'; END $$`)
		grantExec(t, owner, `CREATE CONSTRAINT TRIGGER abort_generation_activation AFTER UPDATE ON vector_state
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (OLD.active_generation <> NEW.active_generation)
 EXECUTE FUNCTION abort_generation_activation()`)
		t.Cleanup(func() {
			_, err := owner.Pool.Exec(context.WithoutCancel(ctx), `DROP TRIGGER abort_generation_activation ON vector_state`)
			require.NoError(t, err)
		})

		trace := &activationTrace{}
		client := activationFaultClient(t, dsn, trace, nil)
		fence := openActivationFence(ctx, t, client, activationTarget)
		err := fence.Activate(ctx, activationDigest, 0)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "fixture rejected activation at commit", pgErr.Message)
		require.Same(t, pgErr, trace.query(t, "commit").err)
		state, err := owner.VectorState(ctx)
		require.NoError(t, err)
		require.Equal(t, LegacyGeneration, state.Active)
		fence.Close(ctx)
		require.Equal(t, []string{"begin", "commit", "unlock"}, trace.controls())
	})

	t.Run("lost commit response", func(t *testing.T) {
		ctx := t.Context()
		socket, trace := activationFaultFixture(t, "commit", activationLoseRead)
		client := activationFaultClient(t, dsn, trace, nil)
		fence := openActivationFence(context.WithValue(ctx, activationSocketKey{}, socket), t, client, activationTarget)
		pid := fence.conn.PgConn().PID()
		result := startFaultedActivation(ctx, t, fence, socket)
		started := awaitActivationSocket(t, socket)

		observerCtx, cancel := context.WithDeadline(ctx, started.Add(4*time.Second))
		defer cancel()
		require.Eventually(t, func() bool {
			state, err := owner.VectorState(observerCtx)
			return err == nil && state.Active == activationTarget && state.Frozen
		}, time.Until(started.Add(4*time.Second)), 20*time.Millisecond)
		close(socket.releaseRead)
		err := activationResult(t, result, started.Add(5*time.Second))
		// pgx closes the session when the commit response read fails.
		// Its simple-query reader reports ErrConnClosed for that failure.
		require.ErrorIs(t, err, pgconn.ErrConnClosed)
		require.Same(t, trace.query(t, "commit").err, err)
		require.False(t, pgconn.SafeToRetry(err))

		fence.Close(ctx)
		fence.Close(ctx)
		require.Equal(t, []string{"begin", "commit"}, trace.controls())
		socket.stopWatchdog()
		awaitActivationBackendExit(t, owner, pid, started.Add(8*time.Second))

		retryTrace := &activationTrace{}
		retry := activationFaultClient(t, dsn, retryTrace, nil)
		require.ErrorContains(t, retry.ActivateGeneration(ctx, activationTarget, activationDigest, 0), "already active")
		require.NotEqual(t, pid, retryTrace.query(t, "rollback").pid)
		require.Equal(t, []string{"begin", "rollback", "unlock"}, retryTrace.controls())
	})

	t.Run("confirmed commit", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, owner.ThawWriters(ctx))
		verifiedGeneration(t, owner, "g3", activationDigest)
		trace := &activationTrace{}
		client := activationFaultClient(t, dsn, trace, nil)
		require.NoError(t, client.ActivateGeneration(ctx, "g3", activationDigest, 0))
		require.Equal(t, "COMMIT", trace.query(t, "commit").tag)
		require.Equal(t, []string{"begin", "commit", "unlock"}, trace.controls())
		state, err := owner.VectorState(ctx)
		require.NoError(t, err)
		require.Equal(t, "g3", state.Active)
	})
}

func TestGenerationActivationRollbackPermissions(t *testing.T) {
	dsn := startTestDatabase(t)
	owner := activationDatabase(t, dsn)
	wrongActivationFreeze(t, owner)
	_, err := owner.RegisterGeneration(t.Context(), stagingGeneration("g3"))
	require.NoError(t, err)
	reindex, role := provisionReindexRole(t, owner)
	grants := readPublishedReindexGrantBlocks(t)
	grantExec(t, owner, replaceReindexRole(grants.base, role, ""))

	ctx := t.Context()
	targetFence := openActivationFence(ctx, t, reindex, activationTarget)
	otherFence := openActivationFence(ctx, t, reindex, "g3")
	targetPID := targetFence.conn.PgConn().PID()
	otherPID := otherFence.conn.PgConn().PID()
	require.NotEqual(t, targetPID, otherPID)
	require.EqualError(t, targetFence.Activate(ctx, activationDigest, 0), wrongFreezeError)
	require.Equal(t, 1, activationAdvisoryLocks(ctx, t, owner, targetPID))
	require.Equal(t, 1, activationAdvisoryLocks(ctx, t, owner, otherPID))
	targetFence.Close(ctx)
	awaitActivationBackendExit(t, owner, targetPID, time.Now().Add(3*time.Second))
	require.Equal(t, 1, activationAdvisoryLocks(ctx, t, owner, otherPID))
	require.ErrorContains(t, reindex.ThawWriters(ctx), "another operation owns")
	otherFence.Close(ctx)
	awaitActivationBackendExit(t, owner, otherPID, time.Now().Add(3*time.Second))

	require.NoError(t, reindex.ThawWriters(ctx))
	verifiedGeneration(t, reindex, activationTarget, activationDigest)
	require.NoError(t, reindex.ActivateGeneration(ctx, activationTarget, activationDigest, 0))
	state, err := owner.VectorState(ctx)
	require.NoError(t, err)
	require.Equal(t, activationTarget, state.Active)
}
