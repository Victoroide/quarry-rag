package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func requirePromptIngestRollback(ctx context.Context, t *testing.T, c *Client, observer *pgx.Conn, trace *ingestTxTrace) {
	t.Helper()
	rollback := requireIngestRollback(ctx, t, trace)
	require.NoError(t, trace.rollbackErr)
	require.Equal(t, "ROLLBACK", trace.rollbackTag)
	requireRecoveredIngestPool(ctx, t, c, observer, rollback.pid, false)
}

func requireIngestCommit(t *testing.T, trace *ingestTxTrace) {
	t.Helper()
	require.Equal(t, 1, trace.commits)
	require.Empty(t, trace.rollbacks)
}

func testIngestClaimReturnsWithoutWork(t *testing.T) {
	c, observer, ctx := ingestRollbackDatabase(t, ingestTxTracer{})
	noClaim := func(t *testing.T) {
		t.Helper()
		trace := &ingestTxTrace{}
		opCtx := context.WithValue(ctx, ingestTraceKey{}, trace)
		requireNoIngestClaim(opCtx, t, c)
		requirePromptIngestRollback(opCtx, t, c, observer, trace)
		require.Zero(t, trace.commits)
	}
	t.Run("empty", noClaim)
	id := jobsAccept(t, c, "still queued after a frozen claim")
	require.NoError(t, c.FreezeWriters(ctx, "g2"))
	before := ingestAdmissionState(ctx, t, observer, id)
	t.Run("frozen", noClaim)
	require.Equal(t, before, ingestAdmissionState(ctx, t, observer, id))
	require.NoError(t, c.ThawWriters(ctx))
	found, err := c.ClaimJob(ctx, ingestRollbackOwner, time.Minute, ingestMaxAttempts)
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, id, found.Document.ID)
	require.Equal(t, 1, found.Attempts)
}

func testIngestClaimRollbackBeforeLayoutMigration(t *testing.T) {
	dsn := startTestDatabase(t)
	c, err := NewClient(t.Context(), dsn, WithQueryTracer(ingestTxTracer{}), configureIngestRollbackPool)
	require.NoError(t, err)
	t.Cleanup(c.Pool.Close)
	applyMigrationPrefix(t, c, 30)
	observer, ctx := ingestRollbackObserver(t, c)

	// The old schema supports mixed PDF work but lacks the layout claim column.
	req := mixedPDFRequest()
	accepted, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	trace := &ingestTxTrace{statement: claimStatement(false, true)}
	opCtx := context.WithValue(ctx, ingestTraceKey{}, trace)
	claimed, err := c.ClaimPDFProfilesForFixtureStore(opCtx, ingestRollbackOwner, time.Minute, ingestMaxAttempts, "", true, true)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, accepted.DocumentID, claimed.Document.ID)
	require.Equal(t, 1, claimed.Attempts)
	require.Equal(t, req.PDF, claimed.PDF)
	requirePromptIngestRollback(opCtx, t, c, observer, trace)
	require.Equal(t, trace.rollbacks[0].pid, trace.operation.pid)
	require.Equal(t, 1, trace.commits)
}

func testIngestOCROutcomes(ctx context.Context, t *testing.T, c *Client, observer *pgx.Conn, req domain.AcceptRequest, job *domain.Job) {
	t.Helper()
	// Give both collections identical input and keep the unrelated lease live.
	otherReq := jobsRequest(req.Document.Content)
	otherReq.Collection = "other-collection"
	otherReq.Fingerprint = chunker.ScopedFingerprint(otherReq.Collection, otherReq.Document.Content)
	other := acceptIngestRollbackJob(ctx, t, c, otherReq)
	otherBefore := ingestAdmissionState(ctx, t, observer, other.Document.ID)

	trace := &ingestTxTrace{}
	require.NoError(t, c.StoreOCRText(context.WithValue(ctx, ingestTraceKey{}, trace), *job, ""))
	requireIngestCommit(t, trace)
	var content, preview, source string
	require.NoError(t, observer.QueryRow(ctx, `SELECT content,content_preview,text_source FROM documents WHERE id=$1`, job.Document.ID).
		Scan(&content, &preview, &source))
	require.Empty(t, content)
	require.Empty(t, preview)
	require.Equal(t, "ocr", source)

	refuse := func(t *testing.T, stale domain.Job) {
		t.Helper()
		before := ingestAdmissionState(ctx, t, observer, stale.Document.ID)
		trace := &ingestTxTrace{}
		opCtx := context.WithValue(ctx, ingestTraceKey{}, trace)
		require.ErrorIs(t, c.StoreOCRText(opCtx, stale, "refused OCR text"), domain.ErrJobNotOwned)
		requirePromptIngestRollback(opCtx, t, c, observer, trace)
		require.Zero(t, trace.commits)
		require.Equal(t, before, ingestAdmissionState(ctx, t, observer, stale.Document.ID))
	}
	t.Run("stale token", func(t *testing.T) {
		stale := *job
		stale.LeaseToken = uuid.NewString()
		refuse(t, stale)
	})
	t.Run("expired lease", func(t *testing.T) {
		expireJobLeaseContext(ctx, t, c, job.Document.ID)
		refuse(t, *job)
	})
	t.Run("replaced incarnation", func(t *testing.T) {
		require.NoError(t, c.DeleteDocument(ctx, job.Document.ID))
		refuse(t, *job)
		req.CallerSupplied = true
		replacement := acceptIngestRollbackJob(ctx, t, c, req)
		require.Equal(t, job.Document.ID, replacement.Document.ID)
		require.NotEqual(t, job.Incarnation, replacement.Incarnation)
		stale := *replacement
		stale.Incarnation = job.Incarnation
		refuse(t, stale)
	})
	require.Equal(t, otherBefore, ingestAdmissionState(ctx, t, observer, other.Document.ID))
	requireIngestRowLocks(ctx, t, observer, other.Document.ID, false)
}

func TestIngestRollbackPreservesAdmissionClaimAndOCROutcomes(t *testing.T) {
	c, observer, ctx := ingestRollbackDatabase(t, ingestTxTracer{})
	req := jobsRequest("committed document duplicate")
	trace := &ingestTxTrace{}
	accepted, err := c.AcceptDocument(context.WithValue(ctx, ingestTraceKey{}, trace), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	requireIngestCommit(t, trace)

	trace = &ingestTxTrace{}
	job, err := c.ClaimJob(context.WithValue(ctx, ingestTraceKey{}, trace), ingestRollbackOwner, time.Minute, ingestMaxAttempts)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, accepted.DocumentID, job.Document.ID)
	requireIngestCommit(t, trace)

	trace = &ingestTxTrace{}
	require.NoError(t, c.StoreOCRText(context.WithValue(ctx, ingestTraceKey{}, trace), *job, "committed OCR text"))
	requireIngestCommit(t, trace)
	complete, err := c.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, complete)
	before := ingestAdmissionState(ctx, t, observer, job.Document.ID)

	t.Run("duplicate", func(t *testing.T) {
		trace := &ingestTxTrace{}
		opCtx := context.WithValue(ctx, ingestTraceKey{}, trace)
		duplicate, err := c.AcceptDocument(opCtx, req)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptDuplicate, duplicate.Outcome)
		require.Equal(t, accepted.DocumentID, duplicate.DocumentID)
		requirePromptIngestRollback(opCtx, t, c, observer, trace)
		require.Zero(t, trace.commits)
	})
	t.Run("PDF needs source", func(t *testing.T) {
		trace := &ingestTxTrace{}
		opCtx := context.WithValue(ctx, ingestTraceKey{}, trace)
		missing := mixedPDFRequest()
		missing.ContentDedup = domain.ContentDedupCollection
		missing.Document.S3Key = nil
		lookup, err := c.AcceptExistingPDF(opCtx, missing, true)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptNeedsSource, lookup.Outcome)
		requirePromptIngestRollback(opCtx, t, c, observer, trace)
		require.Zero(t, trace.commits)
		requireIngestAdmissionLock(ctx, t, observer, false)
	})
	t.Run("restricted role", func(t *testing.T) {
		roles := newGrantRoles(t, c.Pool.Config().ConnString(), "admission_rollback")
		role := roles.name("admission_rollback")
		ident := pgx.Identifier{role}.Sanitize()
		grantExecContext(ctx, t, c, "CREATE ROLE "+ident)
		grantExecContext(ctx, t, c, "GRANT USAGE ON SCHEMA public TO "+ident)
		limited := roleClientContext(ctx, t, c, role)
		trace := &ingestTxTrace{statement: "SELECT active, frozen, profile_hash FROM vector_writer_state()"}
		opCtx := context.WithValue(ctx, ingestTraceKey{}, trace)
		_, err := limited.AcceptDocument(opCtx, req)
		requireSQLState(t, err, "42501")
		requirePromptIngestRollback(opCtx, t, limited, observer, trace)
		require.Equal(t, trace.rollbacks[0].pid, trace.operation.pid)
		require.Zero(t, trace.commits)
		requireIngestAdmissionLock(ctx, t, observer, false)
	})
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			trace := &ingestTxTrace{}
			var parent context.Context
			var cancel context.CancelFunc
			if errors.Is(failure, context.Canceled) {
				parent, cancel = context.WithCancel(ctx)
				cancel()
			} else {
				parent, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			}
			opCtx := context.WithValue(parent, ingestTraceKey{}, trace)
			_, err := c.AcceptDocument(opCtx, req)
			require.ErrorIs(t, err, failure)
			_, err = c.ClaimJob(opCtx, ingestRollbackOwner, time.Minute, ingestMaxAttempts)
			require.ErrorIs(t, err, failure)
			require.ErrorIs(t, c.StoreOCRText(opCtx, *job, "expired input"), failure)
			require.Empty(t, trace.rollbacks)
			require.Zero(t, trace.commits)
			require.Zero(t, c.Pool.Stat().AcquiredConns())
		})
	}
	require.Equal(t, before, ingestAdmissionState(ctx, t, observer, job.Document.ID))
}
