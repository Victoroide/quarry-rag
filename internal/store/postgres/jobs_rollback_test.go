package postgres

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type ingestTraceKey struct{}
type ingestRollbackQueryKey struct{}

type ingestQueryObservation struct {
	ctx       context.Context
	startedAt time.Time
	entryErr  error
	pid       uint32
	args      []any
}

type ingestTxTrace struct {
	statement string
	operation ingestQueryObservation

	cancelOperation context.CancelFunc
	operationPaused chan ingestQueryObservation
	continueQuery   chan struct{}
	rollbackEntered chan time.Time
	watchdog        context.Context

	rollbacks   []ingestQueryObservation
	rollbackErr error
	rollbackTag string
	commits     int
}

type ingestTxTracer struct{}

func (ingestTxTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	trace, ok := ctx.Value(ingestTraceKey{}).(*ingestTxTrace)
	if !ok {
		return ctx
	}
	observation := ingestQueryObservation{
		ctx: ctx, startedAt: time.Now(), entryErr: ctx.Err(), pid: conn.PgConn().PID(), args: data.Args,
	}
	switch {
	case data.SQL == "rollback":
		trace.rollbacks = append(trace.rollbacks, observation)
		if trace.rollbackEntered != nil {
			select {
			case trace.rollbackEntered <- observation.startedAt:
			default:
			}
		}
		if trace.watchdog != nil {
			select {
			case <-ctx.Done():
			case <-trace.watchdog.Done():
			}
		}
		return context.WithValue(ctx, ingestRollbackQueryKey{}, trace)
	case data.SQL == "commit":
		trace.commits++
	case trace.statement != "" && strings.Contains(data.SQL, trace.statement):
		observation.args = append([]any(nil), data.Args...)
		trace.operation = observation
		if trace.cancelOperation != nil {
			// Interrupt before pgx dispatches the statement. The transaction
			// keeps its locks while the separate observer checks them.
			trace.cancelOperation()
			trace.operationPaused <- observation
			select {
			case <-trace.continueQuery:
			case <-trace.watchdog.Done():
			}
		}
	}
	return ctx
}

func (ingestTxTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if trace, ok := ctx.Value(ingestRollbackQueryKey{}).(*ingestTxTrace); ok {
		trace.rollbackErr = data.Err
		trace.rollbackTag = data.CommandTag.String()
	}
}

func ingestRollbackDatabase(t *testing.T, tracer pgx.QueryTracer) (*Client, *pgx.Conn, context.Context) {
	t.Helper()
	c := jobsDatabase(t, WithQueryTracer(tracer), configureIngestRollbackPool)
	observer, ctx := ingestRollbackObserver(t, c)
	return c, observer, ctx
}

func configureIngestRollbackPool(cfg *pgxpool.Config) {
	cfg.MaxConns = 1
}

func ingestRollbackObserver(t *testing.T, c *Client) (*pgx.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	cfg := c.Pool.Config().ConnConfig.Copy()
	cfg.Tracer = nil
	observer, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		require.NoError(t, observer.Close(closeCtx))
	})
	return observer, ctx
}

const (
	ingestRollbackOwner = "ingest-rollback-worker"
	ingestMaxAttempts   = 5
)

func acceptIngestRollbackJob(ctx context.Context, t *testing.T, c *Client, req domain.AcceptRequest) *domain.Job {
	t.Helper()
	accepted, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	job, err := c.ClaimJob(ctx, ingestRollbackOwner, time.Minute, ingestMaxAttempts)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, accepted.DocumentID, job.Document.ID)
	return job
}

func requireIngestRollback(parent context.Context, t *testing.T, trace *ingestTxTrace) ingestQueryObservation {
	t.Helper()
	require.Len(t, trace.rollbacks, 1)
	rollback := trace.rollbacks[0]
	require.NoError(t, rollback.entryErr)
	require.Positive(t, rollback.pid)
	require.Empty(t, rollback.args)
	require.Same(t, trace, rollback.ctx.Value(ingestTraceKey{}))
	deadline, bounded := rollback.ctx.Deadline()
	require.True(t, bounded)
	require.Positive(t, deadline.Sub(rollback.startedAt))
	require.LessOrEqual(t, deadline.Sub(rollback.startedAt), 3*time.Second)
	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)
	require.NotEqual(t, parentDeadline, deadline)
	return rollback
}

func requireIngestRowLocks(ctx context.Context, t *testing.T, observer *pgx.Conn, id domain.DocumentID, held bool) {
	t.Helper()
	for _, query := range []string{
		`SELECT id::text FROM documents WHERE id=$1 FOR UPDATE NOWAIT`,
		`SELECT document_id::text FROM ingest_jobs WHERE document_id=$1 FOR UPDATE NOWAIT`,
	} {
		var lockedID string
		err := observer.QueryRow(ctx, query, id).Scan(&lockedID)
		if held {
			requireSQLState(t, err, "55P03")
		} else {
			require.NoError(t, err)
			require.Equal(t, string(id), lockedID)
		}
	}
}

func TestIngestCompletionRollbackDeadlineAfterCorpusCutover(t *testing.T) {
	releaseTrace := &ingestPoolReleaseTracer{}
	service, observer, ctx := ingestRollbackDatabase(t, releaseTrace)
	service.SetAuthzMode("service")
	job := acceptIngestRollbackJob(ctx, t, service, jobsRequest("leased before grants activation"))
	_, err := service.ActivateDeletion(ctx)
	require.NoError(t, err)
	_, err = service.ActivateGrants(ctx, false)
	require.NoError(t, err)
	before := publicationStateContext(ctx, t, service, job.Document.ID)
	releaseTrace.closedPID.Store(0)

	watchdog, stopWatchdog := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer stopWatchdog()
	trace := &ingestTxTrace{statement: sharedCorpusAuthzModeQuery, watchdog: watchdog}
	opCtx := context.WithValue(ctx, ingestTraceKey{}, trace)
	chunks := []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), Text: "fenced chunk", TokenCount: 2}}
	ok, err := service.CompleteJob(opCtx, *job, chunks, domain.IndexObservation{})
	require.NoError(t, watchdog.Err())
	require.NoError(t, opCtx.Err())
	require.False(t, ok)
	require.ErrorIs(t, err, domain.ErrAuthzModeMismatch)
	rollback := requireIngestRollback(opCtx, t, trace)
	require.Equal(t, trace.operation.pid, rollback.pid)
	require.ErrorIs(t, trace.rollbackErr, context.DeadlineExceeded)
	require.Equal(t, rollback.pid, releaseTrace.closedPID.Load(), "rollback must release the closed connection before returning")
	requireRecoveredIngestPool(ctx, t, service, observer, rollback.pid, true)
	require.Equal(t, before, publicationStateContext(ctx, t, service, job.Document.ID))
	requireIngestRowLocks(ctx, t, observer, job.Document.ID, false)

	require.NoError(t, service.ReleaseJob(ctx, *job))
	grants, err := NewClient(ctx, service.Pool.Config().ConnString())
	require.NoError(t, err)
	t.Cleanup(grants.Pool.Close)
	grants.SetAuthzMode("grants")
	requireIngestReclaim(ctx, t, grants, job, job.Attempts)
}

func TestIngestTransitionRollbackDeadlineAfterLeaseLocks(t *testing.T) {
	const retryDelay = 2 * time.Minute
	const failure = "lease transition fixture"
	for _, tc := range []struct {
		name, statement string
		transition      func(*Client, context.Context, domain.Job) error
	}{
		{"release", "attempts=GREATEST(attempts-1,0)", (*Client).ReleaseJob},
		{"retry", "next_attempt_at=clock_timestamp()+make_interval(secs=>$2)", func(c *Client, ctx context.Context, j domain.Job) error {
			return c.RetryJob(ctx, j, retryDelay, failure)
		}},
		{"fail", "DELETE FROM ingest_jobs", func(c *Client, ctx context.Context, j domain.Job) error {
			return c.FailJob(ctx, j, failure)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			releaseTrace := &ingestPoolReleaseTracer{}
			c, observer, ctx := ingestRollbackDatabase(t, releaseTrace)
			c.EnableHostedVectorManifests(true)
			job := acceptIngestRollbackJob(ctx, t, c, jobsRequest("hosted lease transition "+tc.name))
			attempt := persistManifestAttemptWithChunk(ctx, t, c, job.Document.ID, *job, uuid.NewString())
			before := publicationStateContext(ctx, t, c, job.Document.ID)
			releaseTrace.closedPID.Store(0)

			watchdog, stopWatchdog := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			parent, cancel := context.WithCancel(ctx)
			trace := &ingestTxTrace{
				statement: tc.statement, cancelOperation: cancel, operationPaused: make(chan ingestQueryObservation, 1),
				continueQuery: make(chan struct{}), watchdog: watchdog,
			}
			continueQuery := sync.OnceFunc(func() { close(trace.continueQuery) })
			opCtx := context.WithValue(parent, ingestTraceKey{}, trace)
			done := make(chan struct{})
			var transitionErr error
			go func() {
				defer close(done)
				transitionErr = tc.transition(c, opCtx, attempt.job)
			}()
			defer func() {
				continueQuery()
				cancel()
				stopWatchdog()
				<-done
			}()
			select {
			case observed := <-trace.operationPaused:
				require.NoError(t, observed.entryErr)
				require.NotEqual(t, observer.PgConn().PID(), observed.pid)
				require.NotEmpty(t, observed.args)
				require.Equal(t, job.Document.ID, observed.args[0])
				require.ErrorIs(t, observed.ctx.Err(), context.Canceled)
			case <-done:
				t.Fatalf("transition returned before its write: %v", transitionErr)
			case <-watchdog.Done():
				t.Fatal("transition did not reach its write before the watchdog")
			}
			requireIngestRowLocks(watchdog, t, observer, job.Document.ID, true)
			continueQuery()
			select {
			case <-done:
			case <-watchdog.Done():
				t.Fatal("transition did not return before the watchdog")
			}
			require.NoError(t, watchdog.Err())
			require.ErrorIs(t, transitionErr, context.Canceled)
			rollback := requireIngestRollback(opCtx, t, trace)
			require.Equal(t, trace.operation.pid, rollback.pid)
			require.ErrorIs(t, trace.rollbackErr, context.DeadlineExceeded)
			require.Equal(t, rollback.pid, releaseTrace.closedPID.Load(), "rollback must release the closed connection before returning")
			requireRecoveredIngestPool(ctx, t, c, observer, rollback.pid, true)
			require.Equal(t, before, publicationStateContext(ctx, t, c, job.Document.ID))
			requireIngestRowLocks(ctx, t, observer, job.Document.ID, false)

			var earliest, latest time.Time
			require.NoError(t, observer.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&earliest))
			require.NoError(t, tc.transition(c, ctx, attempt.job))
			require.NoError(t, observer.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&latest))
			require.Equal(t, "retired", manifestStateContext(ctx, t, c, attempt.revision))
			cleanupReason := cleanupReasonContext(ctx, t, c, attempt.revision)
			switch tc.name {
			case "release":
				require.Equal(t, "abandoned", cleanupReason)
				requireIngestReclaim(ctx, t, c, job, job.Attempts)
			case "retry":
				require.Equal(t, "abandoned", cleanupReason)
				var due time.Time
				var attempts int
				require.NoError(t, observer.QueryRow(ctx, `SELECT next_attempt_at,attempts FROM ingest_jobs
					WHERE document_id=$1 AND state='pending' AND lease_owner IS NULL AND lease_until IS NULL AND lease_token IS NULL`,
					job.Document.ID).Scan(&due, &attempts))
				require.False(t, due.Before(earliest.Add(retryDelay)))
				require.False(t, due.After(latest.Add(retryDelay)))
				require.Equal(t, job.Attempts, attempts)
				requireNoIngestClaim(ctx, t, c)
				_, err := observer.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=clock_timestamp() WHERE document_id=$1`, job.Document.ID)
				require.NoError(t, err)
				requireIngestReclaim(ctx, t, c, job, job.Attempts+1)
			case "fail":
				require.Equal(t, "failed", cleanupReason)
				var status, message string
				var hasJob bool
				require.NoError(t, observer.QueryRow(ctx, `SELECT status,last_error,
					EXISTS(SELECT FROM ingest_jobs WHERE document_id=$1) FROM documents WHERE id=$1`,
					job.Document.ID).Scan(&status, &message, &hasJob))
				require.Equal(t, "failed", status)
				require.Equal(t, failure, message)
				require.False(t, hasJob)
				requireNoIngestClaim(ctx, t, c)
			}
		})
	}
}

func requireIngestReclaim(ctx context.Context, t *testing.T, c *Client, original *domain.Job, attempts int) {
	t.Helper()
	reclaimed, err := c.ClaimJob(ctx, original.LeaseOwner, time.Minute, ingestMaxAttempts)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	require.Equal(t, original.Document.ID, reclaimed.Document.ID)
	require.Equal(t, original.Incarnation, reclaimed.Incarnation)
	require.Equal(t, attempts, reclaimed.Attempts)
	require.NotEmpty(t, reclaimed.LeaseToken)
	require.NotEqual(t, original.LeaseToken, reclaimed.LeaseToken)
}

func requireNoIngestClaim(ctx context.Context, t *testing.T, c *Client) {
	t.Helper()
	job, err := c.ClaimJob(ctx, ingestRollbackOwner, time.Minute, ingestMaxAttempts)
	require.NoError(t, err)
	require.Nil(t, job)
}

func TestIngestRollbackPreservesLeaseRefusals(t *testing.T) {
	c, observer, ctx := ingestRollbackDatabase(t, ingestTxTracer{})
	const content = "same content in distinct collections"
	job := acceptIngestRollbackJob(ctx, t, c, jobsRequest(content))
	otherReq := jobsRequest(content)
	otherReq.Collection = "other-collection"
	otherReq.Fingerprint = chunker.ScopedFingerprint(otherReq.Collection, otherReq.Document.Content)
	other := acceptIngestRollbackJob(ctx, t, c, otherReq)
	otherBefore := publicationStateContext(ctx, t, c, other.Document.ID)
	before := publicationStateContext(ctx, t, c, job.Document.ID)

	t.Run("pre-canceled begin", func(t *testing.T) {
		trace := &ingestTxTrace{}
		opCtx, cancel := context.WithCancel(context.WithValue(ctx, ingestTraceKey{}, trace))
		cancel()
		ok, err := c.CompleteJob(opCtx, *job, nil, domain.IndexObservation{})
		require.False(t, ok)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, c.ReleaseJob(opCtx, *job), context.Canceled)
		require.Empty(t, trace.rollbacks)
	})
	for _, tc := range []struct {
		name   string
		change func(*domain.Job)
	}{
		{"missing token", func(j *domain.Job) { j.LeaseToken = "" }},
		{"missing incarnation", func(j *domain.Job) { j.Incarnation = "" }},
		{"stale token", func(j *domain.Job) { j.LeaseToken = uuid.NewString() }},
		{"wrong owner", func(j *domain.Job) { j.LeaseOwner = "different-worker" }},
		{"wrong incarnation", func(j *domain.Job) { j.Incarnation = uuid.NewString() }},
		{"wrong attempt", func(j *domain.Job) { j.Attempts++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := *job
			tc.change(&invalid)
			requireIngestLeaseRefused(ctx, t, c, observer, invalid, before)
		})
	}
	t.Run("denied write", func(t *testing.T) {
		roles := newGrantRoles(t, c.Pool.Config().ConnString(), "ingest_rollback")
		role := roles.name("ingest_rollback")
		ident := pgx.Identifier{role}.Sanitize()
		grantExecContext(ctx, t, c, `CREATE ROLE `+ident)
		grantExecContext(ctx, t, c, `GRANT USAGE ON SCHEMA public TO `+ident)
		grantExecContext(ctx, t, c, `GRANT SELECT, UPDATE ON documents, ingest_jobs TO `+ident)
		grantExecContext(ctx, t, c, `GRANT SELECT ON chunks TO `+ident)
		limited := roleClientContext(ctx, t, c, role)

		requireIngestWriteDenied(ctx, t, limited, observer, "DELETE FROM chunks", job.Document.ID, func(opCtx context.Context) error {
			ok, err := limited.CompleteJob(opCtx, *job, nil, domain.IndexObservation{})
			require.False(t, ok)
			return err
		})
		requireIngestWriteDenied(ctx, t, limited, observer, "DELETE FROM ingest_jobs", job.Document.ID, func(opCtx context.Context) error {
			return limited.FailJob(opCtx, *job, "denied failure")
		})
	})
	require.Equal(t, before, publicationStateContext(ctx, t, c, job.Document.ID))
	t.Run("expired lease", func(t *testing.T) {
		expireJobLeaseContext(ctx, t, c, job.Document.ID)
		expired := publicationStateContext(ctx, t, c, job.Document.ID)
		requireIngestLeaseRefused(ctx, t, c, observer, *job, expired)
	})
	t.Run("post-commit rollback is a no-op", func(t *testing.T) {
		fresh, err := c.ClaimJob(ctx, job.LeaseOwner, time.Minute, ingestMaxAttempts)
		require.NoError(t, err)
		require.NotNil(t, fresh)
		require.Equal(t, job.Document.ID, fresh.Document.ID)
		transition := &ingestTxTrace{}
		require.NoError(t, c.ReleaseJob(context.WithValue(ctx, ingestTraceKey{}, transition), *fresh))
		require.Equal(t, 1, transition.commits)
		require.Empty(t, transition.rollbacks)
		fresh, err = c.ClaimJob(ctx, job.LeaseOwner, time.Minute, ingestMaxAttempts)
		require.NoError(t, err)
		require.NotNil(t, fresh)
		require.Equal(t, job.Document.ID, fresh.Document.ID)
		completion := &ingestTxTrace{}
		ok, err := c.CompleteJob(context.WithValue(ctx, ingestTraceKey{}, completion), *fresh, nil, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 1, completion.commits)
		require.Empty(t, completion.rollbacks)
	})
	require.Equal(t, otherBefore, publicationStateContext(ctx, t, c, other.Document.ID))
	requireIngestRowLocks(ctx, t, observer, other.Document.ID, false)
}

func requireIngestLeaseRefused(ctx context.Context, t *testing.T, c *Client, observer *pgx.Conn, job domain.Job, before string) {
	t.Helper()
	ok, err := c.CompleteJob(ctx, job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, ok)
	trace := &ingestTxTrace{}
	opCtx := context.WithValue(ctx, ingestTraceKey{}, trace)
	require.ErrorIs(t, c.ReleaseJob(opCtx, job), domain.ErrJobNotOwned)
	rollback := requireIngestRollback(opCtx, t, trace)
	require.NoError(t, trace.rollbackErr)
	require.Equal(t, "ROLLBACK", trace.rollbackTag)
	requireRecoveredIngestPool(ctx, t, c, observer, rollback.pid, false)
	require.Equal(t, before, publicationStateContext(ctx, t, c, job.Document.ID))
	requireIngestRowLocks(ctx, t, observer, job.Document.ID, false)
}

func requireIngestWriteDenied(ctx context.Context, t *testing.T, c *Client, observer *pgx.Conn, statement string, id domain.DocumentID, operation func(context.Context) error) {
	t.Helper()
	trace := &ingestTxTrace{statement: statement}
	opCtx := context.WithValue(ctx, ingestTraceKey{}, trace)
	requireSQLState(t, operation(opCtx), "42501")
	rollback := requireIngestRollback(opCtx, t, trace)
	require.Equal(t, trace.operation.pid, rollback.pid)
	require.NoError(t, trace.rollbackErr)
	require.Equal(t, "ROLLBACK", trace.rollbackTag)
	requireRecoveredIngestPool(ctx, t, c, observer, rollback.pid, false)
	requireIngestRowLocks(ctx, t, observer, id, false)
}
