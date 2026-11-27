package postgres

import (
	"context"
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

var errProviderReplyLost = errors.New("provider reply lost")

func TestWriterJournalsSurviveSynchronousPromotion(t *testing.T) {
	cluster := pgtest.StartReplication(t, func(ctx context.Context, dsn string) {
		c := durabilityClient(ctx, t, dsn)
		require.NoError(t, c.RunMigrations(ctx))
		_, err := c.Pool.Exec(ctx, "ALTER DATABASE quarry SET synchronous_commit=off")
		require.NoError(t, err)
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, "ALTER ROLE "+pgx.Identifier{u.User.Username()}.Sanitize()+" SET synchronous_commit=off")
		require.NoError(t, err)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	writer := durabilityClient(ctx, t, durabilityDSN(t, cluster.PrimaryDSN, "on"))
	requireDurabilitySettings(ctx, t, writer.Pool, "on")

	if !t.Run("client deadline before standby acknowledgment", func(t *testing.T) {
		testDurabilityDeadline(t, cluster, writer)
	}) {
		return
	}
	if !t.Run("operator cancel after local commit", func(t *testing.T) {
		testDurabilityOperatorCancel(t, cluster)
	}) {
		return
	}

	active := pineconeGeneration(LegacyGeneration)
	require.NoError(t, writer.PinVectorGeneration(ctx, LegacyGeneration, active.Store, active.Target, active.Host, active.Profile))
	writer.EnableHostedVectorManifests(true)

	upload := journalUpload()
	var uploadCalls, stagingCalls, cleanupCalls, deletionCalls, mutationCalls atomic.Int32
	require.NoError(t, writer.StageSourceUpload(ctx, upload))
	require.NoError(t, writer.BeginSourceUpload(ctx, upload.ID))
	uploadCalls.Add(1) // PUT attempted, outcome still uncertain.

	target := pineconeGeneration("promotion_staging")
	stagingFence := durabilityFence(ctx, t, writer, target)
	requireDurabilitySettings(ctx, t, stagingFence.conn, "on")
	require.ErrorIs(t, stagingFence.Write(ctx, func() error {
		stagingCalls.Add(1)
		return errProviderReplyLost
	}), errProviderReplyLost)
	pending, err := writer.Generation(ctx, target.Name)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.True(t, pending.PendingWrite)
	require.True(t, pending.PendingWriteID.Valid)

	docID := jobsAccept(t, writer, "hosted journal survives promotion")
	job, err := writer.ClaimJob(ctx, uuid.NewString(), 10*time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, docID, job.Document.ID)
	attempt := persistManifestAttemptWithChunk(ctx, t, writer, docID, *job, uuid.NewString())

	// Hold the production worker at the provider boundary through promotion.
	// The old connection cannot checkpoint its outcome on the new primary.
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var workerErr error
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go func() {
		defer close(finished)
		workerErr = writer.MutateJobVectors(workerCtx, attempt.job, func(dispatchCtx context.Context) error {
			return domain.DispatchVectorMutation(dispatchCtx, false, func(callCtx context.Context) error {
				mutationCalls.Add(1)
				close(started)
				select {
				case <-release:
					return errProviderReplyLost
				case <-callCtx.Done():
					return callCtx.Err()
				}
			})
		})
	}()
	t.Cleanup(func() {
		// Any early fatal assertion still releases and joins the worker.
		stopWorker()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Error("worker did not leave the provider boundary")
		}
	})
	select {
	case <-started:
	case <-finished:
		t.Fatalf("worker failed before dispatch: %v", workerErr)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	claim := readPromotionClaim(ctx, t, writer, docID)

	owned := provisionOwnedFixtureContext(ctx, t, writer)
	deletedID := domain.DocumentID(owned.acceptOwned(t, owned.owner, "team", ""))
	incarnation := storedIncarnation(t, writer, deletedID)
	grantExec(t, writer, "UPDATE document_deletion_activation SET activated_at=clock_timestamp()")
	ops := memoryOps(incarnation, uuid.NewString())
	ops.RevisionVectors = func(context.Context, string, string) error {
		deletionCalls.Add(1)
		return errProviderReplyLost
	}
	resolved, complete, err := writer.BeginScopedDeletion(ctx, owned.owner, time.Now().Add(time.Minute), deletedID, ops)
	require.NoError(t, err)
	require.Equal(t, incarnation, resolved)
	require.False(t, complete)
	wantDeletion := readReceipt(t, writer, deletedID)
	require.Equal(t, "qualified_intent", wantDeletion.kind)
	require.Zero(t, wantDeletion.step)
	require.False(t, wantDeletion.finalized)

	require.NoError(t, writer.FreezeWriters(ctx, LegacyGeneration))
	state, err := writer.VectorState(ctx)
	require.NoError(t, err)
	require.NotNil(t, state.FrozenAt)
	epoch := *state.FrozenAt
	activeFence, err := writer.FenceGeneration(ctx, LegacyGeneration)
	require.NoError(t, err)
	defer activeFence.Close(ctx)
	ids := []string{uuid.NewString(), uuid.NewString()}
	sender := uuid.New()
	reservation, err := activeFence.ReserveActiveCleanup(ctx, epoch, active.Target, active.Host, active.Profile, ids, sender)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, reservation)
	cleanupCalls.Add(1) // Exact cleanup attempted, outcome still uncertain.

	cluster.CrashPrimary(t)
	cluster.Promote(t)
	close(release)
	select {
	case <-finished:
		require.ErrorIs(t, workerErr, errProviderReplyLost)
	case <-ctx.Done():
		t.Fatal("old worker did not finish after failover")
	}

	// The isolated promoted node has no synchronous peer. Any later test
	// mutations use primary-only durability, with dispatch fenced.
	promoted := durabilityClient(ctx, t, durabilityDSN(t, cluster.StandbyDSN, "local"))
	promoted.EnableHostedVectorManifests(true)
	requireDurabilitySettings(ctx, t, promoted.Pool, "local")
	requireUploadJournal(ctx, t, promoted, upload, "uncertain")
	require.ErrorContains(t, promoted.BeginSourceUpload(ctx, upload.ID), "cannot begin")
	recoveredStaging, err := promoted.FenceGeneration(ctx, target.Name)
	require.NoError(t, err)
	defer recoveredStaging.Close(ctx)
	requireDurabilitySettings(ctx, t, recoveredStaging.conn, "local")
	generation, err := readGeneration(ctx, recoveredStaging.conn, target.Name, "")
	require.NoError(t, err)
	require.NotNil(t, generation)
	require.Equal(t, target.Target, generation.Target)
	require.Equal(t, StateStaging, generation.State)
	require.True(t, generation.PendingWrite)
	require.Equal(t, pending.PendingWriteID, generation.PendingWriteID)
	require.ErrorContains(t, recoveredStaging.RequireAcknowledged(ctx, uuid.New()), "stale confirmation")
	require.Error(t, recoveredStaging.Write(ctx, func() error {
		stagingCalls.Add(1)
		return nil
	}))

	require.Equal(t, claim, readPromotionClaim(ctx, t, promoted, docID))
	manifest, err := promoted.GetVectorManifest(ctx, attempt.revision)
	require.NoError(t, err)
	require.Equal(t, &domain.VectorManifest{
		RevisionID: attempt.revision, DocumentID: docID, Incarnation: job.Incarnation,
		Attempt: job.Attempts, LeaseToken: job.LeaseToken, IDPrefix: domain.RevisionVectorPrefix(docID, attempt.revision),
		VectorIDs: []string{attempt.vectorID}, ChunkIDs: []string{attempt.chunkID}, State: "writing",
	}, manifest)
	require.ErrorIs(t, promoted.MutateJobVectors(ctx, attempt.job, func(dispatchCtx context.Context) error {
		return domain.DispatchVectorMutation(dispatchCtx, false, func(context.Context) error {
			mutationCalls.Add(1)
			return nil
		})
	}), domain.ErrJobNotOwned)

	require.Equal(t, wantDeletion, readReceipt(t, promoted, deletedID))
	var recordedIncarnation, recordedOwner, recordedTenant, recordedPrincipal string
	err = promoted.Pool.QueryRow(ctx, `SELECT d.incarnation::text, d.memory_owner::text, o.tenant_id::text, o.principal_id::text
		FROM document_deletions d JOIN grant_catalog_deletion_receipts o USING (document_id, incarnation)
		WHERE d.document_id=$1`, deletedID).Scan(&recordedIncarnation, &recordedOwner, &recordedTenant, &recordedPrincipal)
	require.NoError(t, err)
	require.Equal(t, []string{incarnation, ops.MemoryOwner, owned.tenant, owned.owner.ID},
		[]string{recordedIncarnation, recordedOwner, recordedTenant, recordedPrincipal})
	_, _, err = promoted.BeginScopedDeletion(ctx, owned.outsider, time.Now().Add(time.Minute), deletedID, ops)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)
	resolved, complete, err = promoted.BeginScopedDeletion(ctx, owned.owner, time.Now().Add(time.Minute), deletedID, ops)
	require.NoError(t, err)
	require.Equal(t, incarnation, resolved)
	require.False(t, complete)

	recoveredActive, err := promoted.FenceGeneration(ctx, LegacyGeneration)
	require.NoError(t, err)
	defer recoveredActive.Close(ctx)
	batch, err := recoveredActive.ActiveCleanupBatch(ctx, epoch)
	require.NoError(t, err)
	require.Equal(t, &ActiveCleanupBatch{Reservation: reservation, Sender: sender, IDs: ids}, batch)
	_, err = recoveredActive.ReserveActiveCleanup(ctx, epoch, active.Target, active.Host, active.Profile, ids, uuid.New())
	require.Error(t, err)
	require.Error(t, recoveredActive.ClearActiveCleanup(ctx, epoch, reservation, ids, uuid.New()))
	recoveredActive.Close(ctx)
	require.ErrorContains(t, promoted.ThawWriters(ctx), "pending active vector cleanup")
	require.Equal(t, []int32{1, 1, 1, 0, 1},
		[]int32{uploadCalls.Load(), stagingCalls.Load(), cleanupCalls.Load(), deletionCalls.Load(), mutationCalls.Load()})

	// After modeled provider settlement, only the exact reservation can
	// discard the staging barrier.
	require.NoError(t, recoveredStaging.RequireAcknowledged(ctx, pending.PendingWriteID.UUID))
	generation, err = readGeneration(ctx, recoveredStaging.conn, target.Name, "")
	require.NoError(t, err)
	require.NotNil(t, generation)
	require.False(t, generation.PendingWrite)
	require.False(t, generation.PendingWriteID.Valid)
}

func TestWriterJournalPromotionExcludesUnreplicatedLocalCommit(t *testing.T) {
	cluster := pgtest.StartReplication(t, func(ctx context.Context, dsn string) {
		require.NoError(t, durabilityClient(ctx, t, dsn).RunMigrations(ctx))
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	writer := durabilityClient(ctx, t, durabilityDSN(t, cluster.PrimaryDSN, "on"))
	baseline, lost := journalUpload(), journalUpload()
	require.NoError(t, writer.StageSourceUpload(ctx, baseline))
	cluster.WaitForReplay(t)
	cluster.StopStandby(t)

	local := durabilityClient(ctx, t, durabilityDSN(t, cluster.PrimaryDSN, "local"))
	requireDurabilitySettings(ctx, t, local.Pool, "local")
	require.NoError(t, local.StageSourceUpload(ctx, lost))
	require.NoError(t, local.BeginSourceUpload(ctx, lost.ID))
	requireUploadJournal(ctx, t, local, lost, "uncertain")

	cluster.CrashPrimary(t)
	cluster.StartStandby(t)
	cluster.Promote(t)
	promoted := durabilityClient(ctx, t, cluster.StandbyDSN)
	requireUploadJournal(ctx, t, promoted, baseline, "not_started")
	var exists bool
	err := promoted.Pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM source_uploads WHERE id=$1 OR document_id=$2 OR object_key=$3)",
		lost.ID, lost.DocumentID, lost.Key).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "absence after promotion cannot justify resending a previously attempted PUT")
}

func testDurabilityDeadline(t *testing.T, cluster *pgtest.Replication, writer *Client) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	target := pineconeGeneration("promotion_deadline")
	fence := durabilityFence(ctx, t, writer, target)
	var pid int32
	require.NoError(t, fence.conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))
	cluster.StopStandby(t)

	deadline, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	var calls atomic.Int32
	done := make(chan struct{})
	var writeErr error
	go func() {
		defer close(done)
		writeErr = fence.Write(deadline, func() error {
			calls.Add(1)
			return errProviderReplyLost
		})
	}()
	defer func() { stop(); <-done }()
	require.Eventually(t, func() bool {
		return waitingForSyncRep(deadline, writer, pid)
	}, 4*time.Second, 25*time.Millisecond)
	select {
	case <-done:
		require.Error(t, writeErr)
		require.ErrorIs(t, deadline.Err(), context.DeadlineExceeded)
	case <-ctx.Done():
		t.Fatal("timed out waiting for the writer deadline")
	}
	require.Zero(t, calls.Load())
	cluster.StartStandby(t)
	cluster.WaitForReplay(t)
	recovered, err := writer.FenceGeneration(ctx, target.Name)
	require.NoError(t, err)
	defer recovered.Close(ctx)
	record, err := readGeneration(ctx, recovered.conn, target.Name, "")
	require.NoError(t, err)
	require.NotNil(t, record)
	require.True(t, record.PendingWrite)
	require.True(t, record.PendingWriteID.Valid)
	require.ErrorContains(t, recovered.RequireAcknowledged(ctx, uuid.Nil), "unconfirmed target write")
	require.Error(t, recovered.Write(ctx, func() error {
		calls.Add(1)
		return nil
	}))
	require.Zero(t, calls.Load())
}

func testDurabilityOperatorCancel(t *testing.T, cluster *pgtest.Replication) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	dsn := durabilityDSN(t, cluster.PrimaryDSN, "on")
	writer := durabilityClient(ctx, t, dsn+"&pool_max_conns=1")
	observer := durabilityClient(ctx, t, cluster.PrimaryDSN)
	cluster.StopStandby(t)
	conn, err := writer.Pool.Acquire(ctx)
	require.NoError(t, err)
	var pid int32
	pidErr := conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
	conn.Release()
	require.NoError(t, pidErr)
	upload := journalUpload()
	writeCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	var writeErr error
	go func() {
		defer close(done)
		writeErr = writer.StageSourceUpload(writeCtx, upload)
	}()
	defer func() { stop(); <-done }()
	require.Eventually(t, func() bool {
		return waitingForSyncRep(ctx, observer, pid)
	}, 10*time.Second, 25*time.Millisecond)
	var canceled bool
	require.NoError(t, observer.Pool.QueryRow(ctx, "SELECT pg_catalog.pg_cancel_backend($1)", pid).Scan(&canceled))
	require.True(t, canceled)
	select {
	case <-done:
		require.NoError(t, writeErr, "PostgreSQL can return success with a warning after canceling a synchronous wait")
	case <-ctx.Done():
		t.Fatal("operator cancel did not interrupt the synchronous wait")
	}
	requireUploadJournal(ctx, t, observer, upload, "not_started")
	var connected bool
	require.NoError(t, observer.Pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_replication)").Scan(&connected))
	require.False(t, connected, "the local commit had no standby acknowledgment")
	// Keep provider dispatch fenced until the original standby has replayed
	// the canceled commit and the journal can be inspected safely.
	cluster.StartStandby(t)
	cluster.WaitForReplay(t)
	standby := durabilityClient(ctx, t, cluster.StandbyDSN)
	requireUploadJournal(ctx, t, standby, upload, "not_started")
}

func durabilityClient(ctx context.Context, t *testing.T, dsn string) *Client {
	t.Helper()
	c, err := NewClient(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to journal fixture (%T)", err)
	}
	t.Cleanup(c.Pool.Close)
	return c
}

func durabilityDSN(t *testing.T, dsn, mode string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("synchronous_commit", mode)
	q.Set("statement_timeout", "0")
	u.RawQuery = q.Encode()
	return u.String()
}

func durabilityFence(ctx context.Context, t *testing.T, c *Client, target Generation) *GenerationFence {
	t.Helper()
	_, err := c.RegisterGeneration(ctx, target)
	require.NoError(t, err)
	fence, err := c.FenceGeneration(ctx, target.Name)
	require.NoError(t, err)
	t.Cleanup(func() { fence.Close(ctx) })
	return fence
}

func requireDurabilitySettings(ctx context.Context, t *testing.T, query queryRower, mode string) {
	t.Helper()
	var commit, timeout string
	var fsync, fullPageWrites, recovery bool
	err := query.QueryRow(ctx, `SELECT current_setting('synchronous_commit'), current_setting('statement_timeout'),
		current_setting('fsync')::boolean, current_setting('full_page_writes')::boolean, pg_is_in_recovery()`).
		Scan(&commit, &timeout, &fsync, &fullPageWrites, &recovery)
	require.NoError(t, err)
	require.Equal(t, mode, commit)
	require.Equal(t, "0", timeout)
	require.True(t, fsync && fullPageWrites)
	require.False(t, recovery)
}

func journalUpload() domain.SourceUpload {
	id := domain.DocumentID(uuid.NewString())
	return domain.SourceUpload{
		ID: uuid.NewString(), DocumentID: id, Collection: "team",
		Bucket: "journal-originals", Key: "documents/" + string(id) + "/original.pdf",
	}
}

func requireUploadJournal(ctx context.Context, t *testing.T, c *Client, upload domain.SourceUpload, writeState string) {
	t.Helper()
	var docID, collection, bucket, key, state, outcome string
	var unacknowledged bool
	err := c.Pool.QueryRow(ctx, `SELECT document_id::text,collection,bucket,object_key,state,write_state,
		acknowledged_at IS NULL AND linked_document_id IS NULL AND version_id IS NULL FROM source_uploads WHERE id=$1`, upload.ID).
		Scan(&docID, &collection, &bucket, &key, &state, &outcome, &unacknowledged)
	require.NoError(t, err)
	require.Equal(t, []string{string(upload.DocumentID), upload.Collection, upload.Bucket, upload.Key, "staged", writeState},
		[]string{docID, collection, bucket, key, state, outcome})
	require.True(t, unacknowledged)
}

type promotionClaim struct {
	token, incarnation          string
	until                       time.Time
	pending, deletes, uncertain bool
}

func readPromotionClaim(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID) promotionClaim {
	t.Helper()
	var claim promotionClaim
	err := c.Pool.QueryRow(ctx, `SELECT vector_mutation_claim::text, incarnation::text, vector_mutation_until,
		vector_mutation_pending, vector_mutation_deletes, vector_mutation_uncertain
		FROM documents WHERE id=$1`, id).
		Scan(&claim.token, &claim.incarnation, &claim.until, &claim.pending, &claim.deletes, &claim.uncertain)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil.String(), claim.token)
	require.True(t, claim.pending)
	require.False(t, claim.deletes || claim.uncertain)
	return claim
}

func waitingForSyncRep(ctx context.Context, c *Client, pid int32) bool {
	var waiting bool
	err := c.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity
		WHERE pid=$1 AND wait_event_type='IPC' AND wait_event='SyncRep')`, pid).Scan(&waiting)
	return err == nil && waiting
}
