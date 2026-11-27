package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// runCanceledIngestRollback pauses before dispatch, lets the observer check the
// live transaction, then stalls rollback until its own deadline.
func runCanceledIngestRollback(ctx context.Context, t *testing.T, c *Client, observer *pgx.Conn, releaseTrace *ingestPoolReleaseTracer, statement string,
	observeLocks func(context.Context), operation func(context.Context) error) {
	t.Helper()
	releaseTrace.closedPID.Store(0)
	watchdog, stopWatchdog := context.WithCancel(context.WithoutCancel(ctx))
	entryWatchdog, stopEntryWatchdog := context.WithTimeout(watchdog, 10*time.Second)
	defer stopEntryWatchdog()
	parent, cancel := context.WithCancel(ctx)
	trace := &ingestTxTrace{
		statement: statement, cancelOperation: cancel, operationPaused: make(chan ingestQueryObservation, 1),
		continueQuery: make(chan struct{}), rollbackEntered: make(chan time.Time, 1), watchdog: watchdog,
	}
	opCtx := context.WithValue(parent, ingestTraceKey{}, trace)
	done := make(chan struct{})
	var operationErr error
	go func() {
		defer close(done)
		operationErr = operation(opCtx)
	}()
	defer func() {
		cancel()
		stopWatchdog()
		<-done
	}()

	select {
	case observed := <-trace.operationPaused:
		require.NoError(t, observed.entryErr)
		require.ErrorIs(t, observed.ctx.Err(), context.Canceled)
		require.NotEqual(t, observer.PgConn().PID(), observed.pid)
	case <-done:
		t.Fatalf("operation returned before the traced statement: %v", operationErr)
	case <-entryWatchdog.Done():
		t.Fatal("operation did not reach the traced statement before the watchdog")
	}
	observeLocks(entryWatchdog)
	close(trace.continueQuery)

	var rollbackStart time.Time
	select {
	case rollbackStart = <-trace.rollbackEntered:
	case <-done:
		t.Fatalf("operation returned without a wire rollback: %v", operationErr)
	case <-entryWatchdog.Done():
		t.Fatal("operation did not start rollback before the watchdog")
	}
	finishWatchdog, stopFinishWatchdog := context.WithDeadline(watchdog, rollbackStart.Add(6*time.Second))
	defer stopFinishWatchdog()
	select {
	case <-done:
	case <-finishWatchdog.Done():
		t.Fatal("operation did not return within six seconds of starting rollback")
	}
	require.NoError(t, finishWatchdog.Err())
	require.ErrorIs(t, operationErr, context.Canceled)
	rollback := requireIngestRollback(opCtx, t, trace)
	require.Equal(t, trace.operation.pid, rollback.pid)
	require.ErrorIs(t, trace.rollbackErr, context.DeadlineExceeded)
	require.Zero(t, trace.commits)
	require.Equal(t, rollback.pid, releaseTrace.closedPID.Load(), "rollback must release the closed connection before returning")
	requireRecoveredIngestPool(ctx, t, c, observer, rollback.pid, true)
}

func ingestAdmissionState(ctx context.Context, t *testing.T, observer *pgx.Conn, id domain.DocumentID) string {
	t.Helper()
	var state string
	require.NoError(t, observer.QueryRow(ctx, `SELECT jsonb_build_object(
		'document', (SELECT to_jsonb(d) FROM documents d WHERE id=$1),
		'job', (SELECT to_jsonb(j) FROM ingest_jobs j WHERE document_id=$1),
		'queue_count', (SELECT count(*) FROM ingest_jobs),
		'fingerprints', (SELECT jsonb_agg(fingerprint ORDER BY fingerprint) FROM document_fingerprints WHERE document_id=$1),
		'pdf', (SELECT to_jsonb(p) FROM document_pdf_extractions p WHERE document_id=$1),
		'uploads', (SELECT jsonb_agg(to_jsonb(u) ORDER BY id) FROM source_uploads u WHERE document_id=$1),
		'ownership', (SELECT to_jsonb(o) FROM grant_catalog_document_ownership o WHERE document_id=$1),
		'reservation', (SELECT to_jsonb(r) FROM grant_catalog_document_reservations r WHERE document_id=$1)
	)::text`, id).Scan(&state))
	return state
}

func requireIngestAdmissionLock(ctx context.Context, t *testing.T, observer *pgx.Conn, held bool) {
	t.Helper()
	var acquired bool
	require.NoError(t, observer.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, ingestAdmissionLock).Scan(&acquired))
	require.Equal(t, !held, acquired)
}

func TestIngestAdmissionRollbackDeadlineAfterAdvisoryLock(t *testing.T) {
	parallelDatabaseTest(t)
	for _, tc := range []struct {
		name   string
		setup  func(context.Context, *testing.T, *Client) domain.AcceptRequest
		accept func(*Client, context.Context, domain.AcceptRequest) (domain.AcceptResult, error)
		want   domain.AcceptOutcome
	}{
		{
			name: "ordinary",
			setup: func(_ context.Context, _ *testing.T, _ *Client) domain.AcceptRequest {
				return jobsRequest("document admitted after rollback")
			},
			accept: (*Client).AcceptDocument,
			want:   domain.AcceptAccepted,
		},
		{
			name: "caller-supplied owned PDF with staged source",
			setup: func(ctx context.Context, t *testing.T, c *Client) domain.AcceptRequest {
				f := provisionOwnedFixtureContext(ctx, t, c)
				id, incarnation := f.reserveContext(ctx, t, f.owner, "")
				req := suppliedOwnedRequest(f.owner, "", "", id, incarnation)
				pdf := mixedPDFRequest()
				req.PDF, req.Fingerprint, req.RequireSource = pdf.PDF, pdf.Fingerprint, pdf.RequireSource
				req.Document.S3Key = pdf.Document.S3Key
				req.MaxPending = 1
				upload := domain.SourceUpload{ID: uuid.NewString(), DocumentID: req.Document.ID, Bucket: uploadTestBucket, Key: *req.Document.S3Key}
				require.NoError(t, c.StageSourceUpload(ctx, upload))
				req.SourceUpload = &domain.SourceUploadLink{ID: upload.ID, VersionID: "v1"}
				return req
			},
			accept: (*Client).AcceptDocument,
			want:   domain.AcceptAccepted,
		},
		{
			name: "existing PDF",
			setup: func(ctx context.Context, t *testing.T, c *Client) domain.AcceptRequest {
				req := mixedPDFRequest()
				req.CallerSupplied = true
				accepted, err := c.AcceptDocument(ctx, req)
				require.NoError(t, err)
				require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
				return req
			},
			accept: func(c *Client, ctx context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
				return c.AcceptExistingPDF(ctx, req, true)
			},
			want: domain.AcceptInProgress,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			releaseTrace := &ingestPoolReleaseTracer{}
			c, observer, ctx := ingestRollbackDatabase(t, releaseTrace)
			req := tc.setup(ctx, t, c)
			require.NoError(t, c.WritersOpen(ctx))
			before := ingestAdmissionState(ctx, t, observer, req.Document.ID)
			runCanceledIngestRollback(ctx, t, c, observer, releaseTrace, "SELECT active, frozen, profile_hash FROM vector_writer_state()",
				func(opCtx context.Context) { requireIngestAdmissionLock(opCtx, t, observer, true) },
				func(opCtx context.Context) error {
					_, err := tc.accept(c, opCtx, req)
					return err
				})
			requireIngestAdmissionLock(ctx, t, observer, false)
			require.Equal(t, before, ingestAdmissionState(ctx, t, observer, req.Document.ID))
			retried, err := tc.accept(c, ctx, req)
			require.NoError(t, err)
			require.Equal(t, tc.want, retried.Outcome)
			require.Equal(t, req.Document.ID, retried.DocumentID)
			var queued int
			require.NoError(t, observer.QueryRow(ctx, `SELECT count(*) FROM ingest_jobs`).Scan(&queued))
			require.Equal(t, 1, queued)
			if req.SourceUpload != nil {
				var linkedID, incarnation string
				require.NoError(t, observer.QueryRow(ctx, `SELECT linked_document_id::text FROM source_uploads WHERE id=$1 AND state='linked'`,
					req.SourceUpload.ID).Scan(&linkedID))
				require.Equal(t, string(req.Document.ID), linkedID)
				require.NoError(t, observer.QueryRow(ctx, `SELECT incarnation::text FROM grant_catalog_document_ownership WHERE document_id=$1`,
					req.Document.ID).Scan(&incarnation))
				require.Equal(t, req.Owner.Incarnation, retried.Incarnation)
				require.Equal(t, req.Owner.Incarnation, incarnation)
			}
		})
	}
}

func requireIngestWriterLocks(ctx context.Context, t *testing.T, observer *pgx.Conn, held bool) {
	t.Helper()
	for _, query := range []string{
		`SELECT singleton FROM vector_state WHERE singleton FOR UPDATE NOWAIT`,
		`SELECT singleton FROM grant_catalog_corpus_mode WHERE singleton FOR UPDATE NOWAIT`,
	} {
		var singleton bool
		err := observer.QueryRow(ctx, query).Scan(&singleton)
		if held {
			requireSQLState(t, err, "55P03")
		} else {
			require.NoError(t, err)
			require.True(t, singleton)
		}
	}
}

func TestIngestClaimRollbackDeadlineAfterWriterGate(t *testing.T) {
	parallelDatabaseTest(t)
	for _, profile := range []string{"ordinary", domain.LayoutPDFProfile} {
		t.Run(profile, func(t *testing.T) {
			releaseTrace := &ingestPoolReleaseTracer{}
			c, observer, ctx := ingestRollbackDatabase(t, releaseTrace)
			c.SetAuthzMode("service")
			req := jobsRequest("claim after rollback")
			claim := func(opCtx context.Context) (*domain.Job, error) {
				return c.ClaimJob(opCtx, ingestRollbackOwner, time.Minute, ingestMaxAttempts)
			}
			if profile == domain.LayoutPDFProfile {
				req = layoutPDFRequest("", "claim rollback layout")
				claim = func(opCtx context.Context) (*domain.Job, error) {
					return claimLayoutForCorpusTest(opCtx, c, ingestRollbackOwner, ingestMaxAttempts)
				}
			}
			accepted, err := c.AcceptDocument(ctx, req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
			before := ingestAdmissionState(ctx, t, observer, accepted.DocumentID)

			var canceledJob *domain.Job
			runCanceledIngestRollback(ctx, t, c, observer, releaseTrace, claimStatement(true, true),
				func(opCtx context.Context) { requireIngestWriterLocks(opCtx, t, observer, true) },
				func(opCtx context.Context) error {
					var claimErr error
					canceledJob, claimErr = claim(opCtx)
					return claimErr
				})
			require.Nil(t, canceledJob)
			requireIngestWriterLocks(ctx, t, observer, false)
			require.Equal(t, before, ingestAdmissionState(ctx, t, observer, accepted.DocumentID))
			retried, err := claim(ctx)
			require.NoError(t, err)
			require.NotNil(t, retried)
			require.Equal(t, accepted.DocumentID, retried.Document.ID)
			require.Equal(t, 1, retried.Attempts)
			require.NotEmpty(t, retried.LeaseToken)
			if req.PDF != nil {
				require.Equal(t, req.PDF, retried.PDF)
			}
			next, err := claim(ctx)
			require.NoError(t, err)
			require.Nil(t, next)
		})
	}
	t.Run("empty and frozen queues", testIngestClaimReturnsWithoutWork)
	t.Run("pre-layout schema", testIngestClaimRollbackBeforeLayoutMigration)
}

func TestIngestOCRRollbackDeadlineAfterLeaseLocks(t *testing.T) {
	parallelDatabaseTest(t)
	releaseTrace := &ingestPoolReleaseTracer{}
	c, observer, ctx := ingestRollbackDatabase(t, releaseTrace)
	req := jobsRequest("")
	req.Document.TextSource = "ocr_pending"
	job := acceptIngestRollbackJob(ctx, t, c, req)
	before := ingestAdmissionState(ctx, t, observer, job.Document.ID)

	text := strings.Repeat("界", 120) + " recuperación"
	runCanceledIngestRollback(ctx, t, c, observer, releaseTrace, "UPDATE documents d SET content=$2,content_preview=$3,text_source='ocr'",
		func(opCtx context.Context) { requireIngestRowLocks(opCtx, t, observer, job.Document.ID, true) },
		func(opCtx context.Context) error { return c.StoreOCRText(opCtx, *job, text) })
	require.Equal(t, before, ingestAdmissionState(ctx, t, observer, job.Document.ID))
	requireIngestRowLocks(ctx, t, observer, job.Document.ID, false)

	require.NoError(t, c.StoreOCRText(ctx, *job, text))
	var content, preview, source string
	require.NoError(t, observer.QueryRow(ctx, `SELECT content,content_preview,text_source FROM documents WHERE id=$1`, job.Document.ID).
		Scan(&content, &preview, &source))
	require.Equal(t, text, content)
	require.Equal(t, strings.Repeat("界", 120), preview)
	require.Equal(t, "ocr", source)
	t.Run("lease and text outcomes", func(t *testing.T) {
		testIngestOCROutcomes(ctx, t, c, observer, req, job)
	})
}
