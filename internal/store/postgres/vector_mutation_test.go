package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestAdmissionRecordsBackendFormatAndPreservesItOnRetry(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	req := jobsRequest("same body")
	req.CallerSupplied = true
	req.SupportsScopedVectors = true
	first, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, first.Outcome)
	job, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.False(t, job.ScopedVectors)
	require.NotEmpty(t, job.Incarnation)

	require.NoError(t, c.FailJob(ctx, *job, "test retry"))
	_, err = c.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	requeued, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptRequeued, requeued.Outcome)
	retry, err := c.ClaimJob(ctx, job.LeaseOwner, time.Minute, 5)
	require.NoError(t, err)
	require.False(t, retry.ScopedVectors)
	require.Equal(t, job.Incarnation, retry.Incarnation)
	require.Equal(t, job.Attempts, retry.Attempts)
	requireStaleJobRejected(t, c, *job)

	require.NoError(t, c.DeleteDocument(ctx, first.DocumentID))
	_, err = c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	replacement, err := c.ClaimJob(ctx, job.LeaseOwner, time.Minute, 5)
	require.NoError(t, err)
	require.True(t, replacement.ScopedVectors)
	require.NotEqual(t, job.Incarnation, replacement.Incarnation)
	requireStaleJobRejected(t, c, *retry)

	ordinary := jobsRequest("ordinary production admission")
	_, err = c.AcceptDocument(ctx, ordinary)
	require.NoError(t, err)
	legacy, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.False(t, legacy.ScopedVectors)
}

func requireStaleJobRejected(t *testing.T, c *Client, stale domain.Job) {
	t.Helper()
	ctx := t.Context()
	owned, err := c.HeartbeatJob(ctx, stale, time.Minute)
	require.NoError(t, err)
	require.False(t, owned)
	require.ErrorIs(t, c.StoreOCRText(ctx, stale, "stale OCR"), domain.ErrJobNotOwned)
	require.ErrorIs(t, c.RetryJob(ctx, stale, 0, "stale retry"), domain.ErrJobNotOwned)
	require.ErrorIs(t, c.FailJob(ctx, stale, "stale failure"), domain.ErrJobNotOwned)
	require.ErrorIs(t, c.ReleaseJob(ctx, stale), domain.ErrJobNotOwned)
	complete, err := c.CompleteJob(ctx, stale, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, complete)
	require.ErrorIs(t, c.mutateJobVectors(ctx, stale, false, func(context.Context) error {
		t.Error("stale job reached the vector store")
		return nil
	}), domain.ErrJobNotOwned)
}

func TestExpiredLeaseRejectsEveryMutationAndTransition(t *testing.T) {
	c := jobsDatabase(t)
	id := jobsAccept(t, c, "expired lease")
	job, err := c.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	_, err = c.Pool.Exec(t.Context(), `UPDATE ingest_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, id)
	require.NoError(t, err)
	requireStaleJobRejected(t, c, *job)
}

func TestVectorMutationClaimUsesDatabaseClockAndReleasesConnection(t *testing.T) {
	observer := jobsDatabase(t)
	ctx := t.Context()
	_, err := observer.Pool.Exec(ctx, `CREATE SCHEMA test_clock;
		CREATE FUNCTION test_clock.clock_timestamp() RETURNS timestamptz LANGUAGE SQL VOLATILE AS
		'SELECT pg_catalog.clock_timestamp() + current_setting(''quarry.clock_offset'')::interval'`)
	require.NoError(t, err)
	for _, shift := range []int{-90, 90} {
		t.Run(fmt.Sprint(shift), func(t *testing.T) {
			c, err := NewClient(t.Context(), observer.Pool.Config().ConnString(), func(cfg *pgxpool.Config) {
				cfg.MaxConns = 1
				cfg.ConnConfig.RuntimeParams["search_path"] = "test_clock,pg_catalog,public"
				cfg.ConnConfig.RuntimeParams["quarry.clock_offset"] = fmt.Sprintf("%d seconds", shift)
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
			id := jobsAccept(t, c, fmt.Sprintf("clock shifted %d seconds", shift))
			// Column defaults use their original built-in clock. Move this queued
			// job onto the shifted clock before claiming it.
			_, err = c.Pool.Exec(t.Context(), `UPDATE ingest_jobs SET next_attempt_at=clock_timestamp() WHERE document_id=$1`, id)
			require.NoError(t, err)
			const lease = 1200 * time.Millisecond
			job, err := c.ClaimJob(t.Context(), "worker", lease, 5)
			require.NoError(t, err)
			require.NotNil(t, job)
			callCtx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			started, finish := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(finish) })
			defer unblock()
			result := make(chan error, 1)
			go func() {
				result <- c.mutateJobVectors(callCtx, *job, false, func(ctx context.Context) error {
					close(started)
					select {
					case <-finish:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}()
			select {
			case <-started:
			case mutationErr := <-result:
				t.Fatalf("mutation did not start: %v", mutationErr)
			case <-callCtx.Done():
				t.Fatal(callCtx.Err())
			}
			var remaining float64
			require.NoError(t, observer.Pool.QueryRow(callCtx, `SELECT extract(epoch FROM
				vector_mutation_until-pg_catalog.clock_timestamp()) FROM documents WHERE id=$1`, id).Scan(&remaining))
			require.InDelta(t, float64(8+shift), remaining, 1.5)
			for range 3 {
				renewCtx, stop := context.WithTimeout(callCtx, lease/2)
				owned, renewErr := c.HeartbeatJob(renewCtx, *job, lease)
				stop()
				require.NoError(t, renewErr)
				require.True(t, owned)
				<-time.After(lease / 2)
			}
			unblock()
			require.NoError(t, <-result)
			requireMutationState(callCtx, t, observer, id, false, false)
			ok, err := c.CompleteJob(t.Context(), *job, nil, domain.IndexObservation{})
			require.NoError(t, err)
			require.True(t, ok)
		})
	}
}

func TestVectorMutationDistinguishesConfirmationInterruptionAndUnknownReply(t *testing.T) {
	c := jobsDatabase(t)
	for _, scenario := range []string{"confirmed", "interrupted", "unknown reply", "expired acknowledgment"} {
		t.Run(scenario, func(t *testing.T) {
			id := jobsAccept(t, c, scenario)
			ctx := t.Context()
			job, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
			require.NoError(t, err)
			confirmed := func(context.Context) error { return nil }
			switch scenario {
			case "confirmed":
				require.NoError(t, c.mutateJobVectors(ctx, *job, false, confirmed))
				requireMutationState(ctx, t, c, id, false, false)
			case "interrupted":
				require.Panics(t, func() {
					_ = c.mutateJobVectors(ctx, *job, false, func(context.Context) error { panic("interrupted before checkpoint") })
				})
				requireMutationState(ctx, t, c, id, true, false)
				require.ErrorIs(t, c.mutateJobVectors(ctx, *job, false, confirmed), domain.ErrJobNotOwned)
				_, err = c.Pool.Exec(ctx, `UPDATE documents SET vector_mutation_until=clock_timestamp() WHERE id=$1`, id)
				require.NoError(t, err)
			case "unknown reply":
				err = c.mutateJobVectors(ctx, *job, false, func(context.Context) error { return context.DeadlineExceeded })
				require.ErrorIs(t, err, context.DeadlineExceeded)
				requireMutationState(ctx, t, c, id, false, true)
			case "expired acknowledgment":
				err = c.mutateJobVectors(ctx, *job, false, func(ctx context.Context) error {
					_, updateErr := c.Pool.Exec(ctx, `UPDATE documents SET vector_mutation_until=clock_timestamp() WHERE id=$1`, id)
					return updateErr
				})
				require.ErrorIs(t, err, domain.ErrJobNotOwned)
				requireMutationState(ctx, t, c, id, false, true)
			}
			require.NoError(t, c.mutateJobVectors(ctx, *job, false, confirmed))
			requireMutationState(ctx, t, c, id, false, scenario != "confirmed")
			require.NoError(t, c.FailJob(ctx, *job, "fixture complete"))
		})
	}
}

func TestExpiredMutationAtAttemptExhaustion(t *testing.T) {
	c := jobsDatabase(t)
	for _, mutation := range []struct {
		name    string
		deletes bool
	}{{name: "upsert"}, {name: "delete", deletes: true}} {
		t.Run(mutation.name, func(t *testing.T) {
			ctx := t.Context()
			id := jobsAccept(t, c, "interrupted "+mutation.name)
			job, err := c.ClaimJob(ctx, "worker", time.Minute, 1)
			require.NoError(t, err)
			require.NotNil(t, job)
			require.False(t, job.ScopedVectors)
			require.Panics(t, func() {
				_ = c.mutateJobVectors(ctx, *job, mutation.deletes, func(context.Context) error {
					panic("worker interrupted before checkpoint")
				})
			})
			requireMutationState(ctx, t, c, id, true, false)
			_, err = c.Pool.Exec(ctx, `UPDATE documents SET vector_mutation_until=clock_timestamp()-interval '1 second' WHERE id=$1`, id)
			require.NoError(t, err)
			_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, id)
			require.NoError(t, err)

			next, err := c.ClaimJob(ctx, "replacement worker", time.Minute, 1)
			require.NoError(t, err)
			require.Nil(t, next)
			requireFailedJobRemoved(ctx, t, c, id)
			requireSettledMutation(ctx, t, c, id, mutation.deletes)
			doc, err := c.GetDocument(ctx, id)
			require.NoError(t, err)
			require.NotNil(t, doc)
			require.NoError(t, c.CheckLiveVectorRemoval(ctx, doc))
			require.NoError(t, c.RemoveLiveDocument(ctx, doc))
		})
	}
}

func TestLegacyRemovalRetriesAfterFailedJobMutationExpires(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	id := jobsAccept(t, c, "terminal failure with an in-flight delete")
	job, err := c.ClaimJob(ctx, "worker", time.Minute, 1)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.False(t, job.ScopedVectors)
	doc, err := c.GetDocument(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, doc)

	err = c.mutateJobVectors(ctx, *job, true, func(ctx context.Context) error {
		require.NoError(t, c.FailJob(ctx, *job, "terminal failure"))
		requireFailedJobRemoved(ctx, t, c, id)
		requireMutationState(ctx, t, c, id, true, false)
		require.ErrorIs(t, c.CheckLiveVectorRemoval(ctx, doc), domain.ErrLiveVectorMutation)
		require.ErrorIs(t, c.RemoveLiveDocument(ctx, doc), domain.ErrLiveVectorMutation)

		_, expireErr := c.Pool.Exec(ctx, `UPDATE documents SET vector_mutation_until=clock_timestamp()-interval '1 second' WHERE id=$1`, id)
		require.NoError(t, expireErr)
		require.NoError(t, c.CheckLiveVectorRemoval(ctx, doc))
		requireSettledMutation(ctx, t, c, id, true)
		return nil
	})
	require.ErrorIs(t, err, domain.ErrJobNotOwned, "the late checkpoint used the retired claim token")
	requireSettledMutation(ctx, t, c, id, true)
	require.NoError(t, c.RemoveLiveDocument(ctx, doc))
	exists, err := c.DocumentExists(ctx, id)
	require.NoError(t, err)
	require.False(t, exists)
}

func requireFailedJobRemoved(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID) {
	t.Helper()
	var status string
	var jobs int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT status,
		(SELECT count(*) FROM ingest_jobs WHERE document_id=d.id) FROM documents d WHERE id=$1`, id).Scan(&status, &jobs))
	require.Equal(t, "failed", status)
	require.Zero(t, jobs)
}

func requireSettledMutation(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID, deletes bool) {
	t.Helper()
	requireMutationState(ctx, t, c, id, false, true)
	var deleteUncertain, pendingDelete bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT vector_delete_uncertain,vector_mutation_deletes
		FROM documents WHERE id=$1`, id).Scan(&deleteUncertain, &pendingDelete))
	require.Equal(t, deletes, deleteUncertain)
	require.False(t, pendingDelete)
}

func requireMutationState(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID, pending, uncertain bool) {
	t.Helper()
	var gotPending, gotUncertain, hasClaim, hasExpiry bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT vector_mutation_pending,vector_mutation_uncertain,
		vector_mutation_claim IS NOT NULL,vector_mutation_until IS NOT NULL FROM documents WHERE id=$1`, id).
		Scan(&gotPending, &gotUncertain, &hasClaim, &hasExpiry))
	require.Equal(t, pending, gotPending)
	require.Equal(t, uncertain, gotUncertain)
	require.Equal(t, pending, hasClaim)
	require.Equal(t, pending, hasExpiry)
}

func TestUnresolvedUntaggedDeleteBlocksPublication(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	id := jobsAccept(t, c, "shared untagged physical target")
	first, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, first)
	err = c.MutateJobVectors(ctx, *first, func(ctx context.Context) error {
		return domain.DispatchVectorMutation(ctx, true, func(context.Context) error {
			return context.DeadlineExceeded
		})
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, c.RetryJob(ctx, *first, 0, "provider reply lost"))
	second, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.NoError(t, c.mutateJobVectors(ctx, *second, false, func(context.Context) error { return nil }))
	completed, err := c.CompleteJob(ctx, *second, nil, domain.IndexObservation{})
	require.ErrorContains(t, err, "mutation is unresolved")
	require.False(t, completed)
	var status string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT status FROM documents WHERE id=$1`, id).Scan(&status))
	require.Equal(t, "processing", status)
}
