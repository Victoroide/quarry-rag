package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// waitForJobLockWait blocks until a statement on ingest_jobs is waiting for a row lock.
func waitForJobLockWait(t *testing.T, c *Client) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting bool
		err := c.Pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database() AND wait_event_type='Lock' AND query LIKE '%ingest_jobs%')`).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond, "the operation must be waiting for the job row lock")
}

// waitForLeaseExpiry blocks until the database clock has passed the lease.
func waitForLeaseExpiry(t *testing.T, c *Client, id domain.DocumentID, within time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		var expired bool
		err := c.Pool.QueryRow(t.Context(), `SELECT lease_until<=clock_timestamp() FROM ingest_jobs WHERE document_id=$1`, string(id)).Scan(&expired)
		return err == nil && expired
	}, within, 15*time.Millisecond, "observe the lease expiring while the row is locked")
}

func hasAdvisoryWaiter(ctx context.Context, pool *pgxpool.Pool, advisoryKey int) (bool, error) {
	var waiting bool
	err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT FROM pg_locks WHERE locktype='advisory' AND objid=$1 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, advisoryKey).Scan(&waiting)
	return waiting, err
}

func TestAdvisoryWaiterIsScopedToFixtureDatabase(t *testing.T) {
	address := startTestDatabase(t)
	home, err := NewClient(t.Context(), address)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, home.Close(context.Background())) })
	foreign, err := NewClient(t.Context(), pgtest.Sibling(t, address))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, foreign.Close(context.Background())) })

	const advisoryKey = 731152
	ctx := t.Context()
	holder, err := foreign.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback(context.WithoutCancel(ctx)) }()
	_, err = holder.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryKey)
	require.NoError(t, err)

	waitCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = foreign.Pool.Exec(waitCtx, `SELECT pg_advisory_xact_lock($1)`, advisoryKey)
	}()
	defer func() {
		cancel()
		<-done
	}()

	require.Eventually(t, func() bool {
		waiting, queryErr := hasAdvisoryWaiter(ctx, foreign.Pool, advisoryKey)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond, "the sibling database must have a waiter")
	waiting, err := hasAdvisoryWaiter(ctx, home.Pool, advisoryKey)
	require.NoError(t, err)
	require.False(t, waiting, "a waiter in the sibling database must not satisfy the lease test's probe")
}

// A healthy worker must keep renewing while completion is still writing chunks
// and the fingerprint, otherwise slow persistence loses the document.
func TestHeartbeatRenewsWhileCompletionPersists(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	id := jobsAccept(t, c, "slow but healthy completion")
	const advisoryKey = 731152
	gate, err := c.Pool.Acquire(ctx)
	require.NoError(t, err)
	defer gate.Release()
	_, err = gate.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey)
	require.NoError(t, err)
	unlock := func() { _, _ = gate.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey) }
	defer unlock()
	// The fingerprint insert is the last write before completion finishes.
	_, err = c.Pool.Exec(ctx, `CREATE FUNCTION delay_fingerprint_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(731152);
    RETURN NEW;
END $$;
CREATE TRIGGER delay_fingerprint_insert BEFORE INSERT ON document_fingerprints FOR EACH ROW EXECUTE FUNCTION delay_fingerprint_insert()`)
	require.NoError(t, err)

	const lease = 3 * time.Second
	job, err := c.ClaimJob(ctx, "healthy-worker", lease, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	var originalDeadline time.Time
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT lease_until FROM ingest_jobs WHERE document_id=$1`, string(id)).Scan(&originalDeadline))
	observedAt := time.Now().UTC()
	type outcome struct {
		ok  bool
		err error
	}
	complete := make(chan outcome, 1)
	go func() {
		ok, completeErr := c.CompleteJob(ctx, *job, []domain.Chunk{{ID: domain.ChunkID(uuid.NewString())}}, domain.IndexObservation{Visibility: domain.VisibilitySampleSeen, ObservedAt: &observedAt})
		complete <- outcome{ok, completeErr}
	}()
	require.Eventually(t, func() bool {
		waiting, queryErr := hasAdvisoryWaiter(ctx, c.Pool, advisoryKey)
		return queryErr == nil && waiting
	}, 2*time.Second, 10*time.Millisecond, "completion reaches the fingerprint insert while the lease is live")

	heartbeatCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	renewed, err := c.HeartbeatJob(heartbeatCtx, *job, 15*time.Second)
	require.NoError(t, err)
	require.True(t, renewed, "the heartbeat must not wait for the completion transaction")

	// Hold completion past the original expiry; only the renewal keeps the claim alive.
	require.Eventually(t, func() bool {
		var expired bool
		queryErr := c.Pool.QueryRow(ctx, `SELECT $1::timestamptz<=clock_timestamp()`, originalDeadline).Scan(&expired)
		return queryErr == nil && expired
	}, lease, 15*time.Millisecond, "the original lease expires while persistence is still waiting")
	unlock()
	done := <-complete
	require.NoError(t, done.err)
	require.True(t, done.ok, "a renewed claim completes after slow persistence")
	summary, err := c.GetDocumentSummary(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "completed", summary.Status)
	require.Equal(t, domain.VisibilitySampleSeen, summary.IndexVisibility)
}

// A statement that qualified the row before waiting for another transaction's
// lock must not act on a lease that expired during the wait.
func TestExpiredLeaseIsRejectedAfterRowLockWait(t *testing.T) {
	c := jobsDatabase(t)
	for _, operation := range []string{"heartbeat", "fail", "retry", "complete"} {
		t.Run(operation, func(t *testing.T) {
			ctx := t.Context()
			id := jobsAccept(t, c, "contended "+operation)
			// Later cases must not claim this document's expired job.
			t.Cleanup(func() { _ = c.DeleteDocument(context.Background(), id) })
			const lease = 2 * time.Second
			job, err := c.ClaimJob(ctx, "late-worker", lease, 3)
			require.NoError(t, err)
			require.NotNil(t, job)
			holder, err := c.Pool.Begin(ctx)
			require.NoError(t, err)
			defer func() { _ = holder.Rollback(context.WithoutCancel(ctx)) }()
			var held string
			require.NoError(t, holder.QueryRow(ctx, `SELECT document_id::text FROM ingest_jobs WHERE document_id=$1 FOR UPDATE`, string(id)).Scan(&held))

			observedAt := time.Now().UTC()
			result := make(chan error, 1)
			var renewed, completed bool
			go func() {
				var opErr error
				switch operation {
				case "heartbeat":
					renewed, opErr = c.HeartbeatJob(ctx, *job, 15*time.Second)
				case "fail":
					opErr = c.FailJob(ctx, *job, "late failure")
				case "retry":
					opErr = c.RetryJob(ctx, *job, time.Second, "late retry")
				case "complete":
					completed, opErr = c.CompleteJob(ctx, *job, nil, domain.IndexObservation{Visibility: domain.VisibilityWritesIndexed, ObservedAt: &observedAt})
				}
				result <- opErr
			}()
			waitForJobLockWait(t, c)
			waitForLeaseExpiry(t, c, id, 2*lease)
			require.NoError(t, holder.Rollback(ctx))
			opErr := <-result

			switch operation {
			case "heartbeat":
				require.NoError(t, opErr)
				require.False(t, renewed, "an expired claim cannot be renewed after waiting")
			case "complete":
				require.NoError(t, opErr)
				require.False(t, completed, "an expired claim cannot complete after waiting")
			default:
				require.True(t, errors.Is(opErr, domain.ErrJobNotOwned), "got %v", opErr)
			}
			summary, err := c.GetDocumentSummary(ctx, id)
			require.NoError(t, err)
			require.Equal(t, "processing", summary.Status, "the expired owner must not change the document")
			require.Equal(t, domain.VisibilityUnknown, summary.IndexVisibility)
			var state string
			require.NoError(t, c.Pool.QueryRow(ctx, `SELECT state FROM ingest_jobs WHERE document_id=$1`, string(id)).Scan(&state))
			require.Equal(t, "running", state, "the row stays for the next lease reclaim")
			reclaimed, err := c.ClaimJob(ctx, "next-worker", time.Minute, 3)
			require.NoError(t, err)
			require.NotNil(t, reclaimed, "the expired job is reclaimable")
			require.Equal(t, 2, reclaimed.Attempts)
		})
	}
}
