package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const repeatableReadDefault = "repeatable read"

func setRepeatableReadDefault(config *pgxpool.Config) {
	config.ConnConfig.RuntimeParams["default_transaction_isolation"] = repeatableReadDefault
}

func failAdmissionDocument(t *testing.T, c *Client, req domain.AcceptRequest, failure string) domain.DocumentID {
	t.Helper()
	accepted, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	job, err := c.ClaimJob(t.Context(), "initial-attempt", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, accepted.DocumentID, job.Document.ID)
	require.NoError(t, c.FailJob(t.Context(), *job, failure))
	return accepted.DocumentID
}

func assertAdmissionState(t *testing.T, c *Client, id domain.DocumentID, status string, lastError *string, jobs int) {
	t.Helper()
	var storedStatus string
	var storedError *string
	var storedJobs int
	err := c.Pool.QueryRow(t.Context(), `SELECT status,last_error,(SELECT count(*) FROM ingest_jobs WHERE document_id=$1) FROM documents WHERE id=$1`, string(id)).Scan(&storedStatus, &storedError, &storedJobs)
	require.NoError(t, err)
	require.Equal(t, status, storedStatus)
	require.Equal(t, lastError, storedError)
	require.Equal(t, jobs, storedJobs)
}

func TestAcceptFailedRetryAtQueueCapacity(t *testing.T) {
	for _, state := range []string{"pending", "delayed", "running"} {
		t.Run(state, func(t *testing.T) {
			c := jobsDatabase(t)
			failure := "terminal processing error"
			failedID := failAdmissionDocument(t, c, jobsRequest("failed content"), failure)
			occupant := jobsRequest("outstanding work")
			occupant.MaxPending = 1
			accepted, err := c.AcceptDocument(t.Context(), occupant)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
			if state != "pending" {
				job, claimErr := c.ClaimJob(t.Context(), "occupant", time.Minute, 5)
				require.NoError(t, claimErr)
				require.NotNil(t, job)
				require.Equal(t, accepted.DocumentID, job.Document.ID)
				if state == "delayed" {
					require.NoError(t, c.RetryJob(t.Context(), *job, time.Hour, "try later"))
				}
			}

			retry := jobsRequest("failed content")
			retry.MaxPending = 1
			rejected, err := c.AcceptDocument(t.Context(), retry)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptQueueFull, rejected.Outcome)
			require.Equal(t, failedID, rejected.DocumentID)
			assertAdmissionState(t, c, failedID, "failed", &failure, 0)

			observed, err := c.AcceptDocument(t.Context(), occupant)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptResult{DocumentID: accepted.DocumentID, Outcome: domain.AcceptInProgress}, observed)
			require.NoError(t, c.DeleteDocument(t.Context(), accepted.DocumentID))

			requeued, err := c.AcceptDocument(t.Context(), retry)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptResult{DocumentID: failedID, Outcome: domain.AcceptRequeued}, requeued)
			observed, err = c.AcceptDocument(t.Context(), retry)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptResult{DocumentID: failedID, Outcome: domain.AcceptInProgress}, observed)
			assertAdmissionState(t, c, failedID, "processing", nil, 1)
		})
	}
}

func TestAcceptAfterQueueLimitDecrease(t *testing.T) {
	c := jobsDatabase(t)
	failure := "terminal processing error"
	retry := jobsRequest("failed content")
	retry.CallerSupplied = true
	retry.Collection = "archive"
	retry.Fingerprint = chunker.ScopedFingerprint(retry.Collection, retry.Document.Content)
	failedID := failAdmissionDocument(t, c, retry, failure)

	completed := jobsRequest("completed content")
	accepted, err := c.AcceptDocument(t.Context(), completed)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	job, err := c.ClaimJob(t.Context(), "completed", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	ok, err := c.CompleteJob(t.Context(), *job, nil)
	require.NoError(t, err)
	require.True(t, ok)

	first := jobsAccept(t, c, "first occupant")
	second := jobsAccept(t, c, "second occupant")
	retry.MaxPending = 1
	completed.MaxPending = 1
	conflict := retry
	conflict.Collection = "other"
	conflict.Fingerprint = chunker.ScopedFingerprint(conflict.Collection, conflict.Document.Content)
	observed, err := c.AcceptDocument(t.Context(), completed)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptResult{DocumentID: accepted.DocumentID, Outcome: domain.AcceptDuplicate}, observed)
	observed, err = c.AcceptDocument(t.Context(), conflict)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptResult{DocumentID: failedID, Outcome: domain.AcceptConflict}, observed)

	fresh := jobsRequest("new content")
	fresh.MaxPending = 1
	for _, occupant := range []domain.DocumentID{first, second} {
		for _, req := range []domain.AcceptRequest{retry, fresh} {
			rejected, acceptErr := c.AcceptDocument(t.Context(), req)
			require.NoError(t, acceptErr)
			require.Equal(t, domain.AcceptQueueFull, rejected.Outcome)
		}
		assertAdmissionState(t, c, failedID, "failed", &failure, 0)
		exists, existsErr := c.DocumentExists(t.Context(), fresh.Document.ID)
		require.NoError(t, existsErr)
		require.False(t, exists)
		require.NoError(t, c.DeleteDocument(t.Context(), occupant))
	}
	requeued, err := c.AcceptDocument(t.Context(), retry)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptResult{DocumentID: failedID, Outcome: domain.AcceptRequeued}, requeued)
	assertAdmissionState(t, c, failedID, "processing", nil, 1)
}

func TestConcurrentFailedRetryAdmission(t *testing.T) {
	t.Run("retry and new document compete for one slot", func(t *testing.T) {
		c := jobsDatabase(t, setRepeatableReadDefault)
		failure := "terminal processing error"
		retry := jobsRequest("failed content")
		retry.CallerSupplied = true
		failedID := failAdmissionDocument(t, c, retry, failure)
		retry.MaxPending = 1
		fresh := jobsRequest("new content")
		fresh.MaxPending = 1

		left, right := raceAdmissions(t, c, retry, fresh)
		switch left.Outcome {
		case domain.AcceptRequeued:
			require.Equal(t, domain.AcceptQueueFull, right.Outcome)
			assertAdmissionState(t, c, failedID, "processing", nil, 1)
		case domain.AcceptQueueFull:
			require.Equal(t, domain.AcceptAccepted, right.Outcome)
			assertAdmissionState(t, c, failedID, "failed", &failure, 0)
		default:
			t.Fatalf("unexpected retry outcome: %v", left.Outcome)
		}
		var count int
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM ingest_jobs`).Scan(&count))
		require.Equal(t, 1, count)
	})

	t.Run("simultaneous retries of the same ID coalesce", func(t *testing.T) {
		c := jobsDatabase(t, setRepeatableReadDefault)
		retry := jobsRequest("failed content")
		retry.CallerSupplied = true
		failedID := failAdmissionDocument(t, c, retry, "terminal processing error")
		retry.MaxPending = 1

		left, right := raceAdmissions(t, c, retry, retry)
		require.Equal(t, failedID, left.DocumentID)
		require.Equal(t, failedID, right.DocumentID)
		require.ElementsMatch(t, []domain.AcceptOutcome{domain.AcceptRequeued, domain.AcceptInProgress}, []domain.AcceptOutcome{left.Outcome, right.Outcome})
		assertAdmissionState(t, c, failedID, "processing", nil, 1)
	})
}

func raceAdmissions(t *testing.T, c *Client, first, second domain.AcceptRequest) (firstResult, secondResult domain.AcceptResult) {
	t.Helper()
	ctx := t.Context()
	other, err := NewClient(ctx, c.Pool.Config().ConnString(), setRepeatableReadDefault)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, other.Close(context.Background())) })
	for _, client := range []*Client{c, other} {
		var isolation string
		err = client.Pool.QueryRow(ctx, `SHOW default_transaction_isolation`).Scan(&isolation)
		require.NoError(t, err)
		require.Equal(t, repeatableReadDefault, isolation)
	}

	lock, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(ctx) }()
	_, err = lock.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock)
	require.NoError(t, err)

	type response struct {
		result domain.AcceptResult
		err    error
	}
	left, right := make(chan response, 1), make(chan response, 1)
	accept := func(client *Client, req domain.AcceptRequest, replies chan<- response) {
		result, acceptErr := client.AcceptDocument(ctx, req)
		replies <- response{result: result, err: acceptErr}
	}
	go accept(c, first, left)
	go accept(other, second, right)
	require.Eventually(t, func() bool {
		var waiting int
		readErr := c.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted`).Scan(&waiting)
		return readErr == nil && waiting == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, lock.Commit(ctx))
	a, b := <-left, <-right
	require.NoError(t, a.err)
	require.NoError(t, b.err)
	return a.result, b.result
}

func TestFailedRetryRollsBackOnJobInsertError(t *testing.T) {
	c := jobsDatabase(t)
	failure := "terminal processing error"
	retry := jobsRequest("failed content")
	failedID := failAdmissionDocument(t, c, retry, failure)
	retry.MaxPending = 1

	_, err := c.Pool.Exec(t.Context(), `ALTER TABLE ingest_jobs ADD CONSTRAINT reject_job_insert CHECK (false) NOT VALID`)
	require.NoError(t, err)
	_, err = c.AcceptDocument(t.Context(), retry)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "reject_job_insert", pgErr.ConstraintName)
	assertAdmissionState(t, c, failedID, "failed", &failure, 0)
}

func TestFailedRetryRollsBackOnCancellation(t *testing.T) {
	c := jobsDatabase(t)
	failure := "terminal processing error"
	retry := jobsRequest("failed content")
	failedID := failAdmissionDocument(t, c, retry, failure)
	retry.MaxPending = 1

	lock, err := c.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(t.Context()) }()
	// Allow the capacity read, then block the insert after the document update.
	_, err = lock.Exec(t.Context(), `LOCK TABLE ingest_jobs IN SHARE MODE`)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, acceptErr := c.AcceptDocument(ctx, retry)
		done <- acceptErr
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		readErr := c.Pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='ingest_jobs'::regclass AND mode='RowExclusiveLock' AND NOT granted)`).Scan(&waiting)
		return readErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, lock.Rollback(t.Context()))
	assertAdmissionState(t, c, failedID, "failed", &failure, 0)

	requeued, err := c.AcceptDocument(t.Context(), retry)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptResult{DocumentID: failedID, Outcome: domain.AcceptRequeued}, requeued)
}
