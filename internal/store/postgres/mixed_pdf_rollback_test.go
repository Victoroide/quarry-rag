package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	beginPDFOCRWrite  = "UPDATE document_pdf_extractions p SET started_at="
	recordPDFOCRWrite = "UPDATE document_pdf_extractions p SET accepted_at="
	storePDFOCRWrite  = "UPDATE document_pdf_extractions p SET page_results="
	storePDFTextWrite = "UPDATE documents SET content="
)

type pdfLeaseFixture struct {
	client   *Client
	observer *pgx.Conn
	ctx      context.Context
	job      *domain.Job
}

func newPDFLeaseFixture(t *testing.T, tracer pgx.QueryTracer, layout bool) pdfLeaseFixture {
	t.Helper()
	c, observer, ctx := ingestRollbackDatabase(t, tracer)
	req := mixedPDFRequest()
	if layout {
		req = layoutPDFRequest("", "layout rollback fixture")
	}
	job := acceptPDFLeaseFixtureJob(ctx, t, c, req)
	return pdfLeaseFixture{client: c, observer: observer, ctx: ctx, job: job}
}

func acceptPDFLeaseFixtureJob(ctx context.Context, t *testing.T, c *Client, req domain.AcceptRequest) *domain.Job {
	t.Helper()
	accepted, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	var job *domain.Job
	if req.PDF.Profile == domain.LayoutPDFProfile {
		job, err = claimLayoutForCorpusTest(ctx, c, ingestRollbackOwner, ingestMaxAttempts)
	} else {
		job, err = c.ClaimPDFJob(ctx, ingestRollbackOwner, time.Minute, ingestMaxAttempts)
	}
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, accepted.DocumentID, job.Document.ID)
	require.Equal(t, req.PDF, job.PDF)
	return job
}

func (f pdfLeaseFixture) begin(t *testing.T) domain.PDFOCRWindow {
	t.Helper()
	window, err := f.client.BeginPDFOCR(f.ctx, *f.job)
	require.NoError(t, err)
	require.NotEmpty(t, window.ID)
	return window
}

func (f pdfLeaseFixture) state(t *testing.T) string {
	t.Helper()
	return ingestAdmissionState(f.ctx, t, f.observer, f.job.Document.ID)
}

func (f pdfLeaseFixture) requireLiveLease(t *testing.T) {
	t.Helper()
	var token, owner, incarnation string
	var attempt int
	var live bool
	require.NoError(t, f.observer.QueryRow(f.ctx, `SELECT j.lease_token::text,j.lease_owner,
		j.attempts,COALESCE(d.incarnation,d.id)::text,j.lease_until>clock_timestamp()
		FROM ingest_jobs j JOIN documents d ON d.id=j.document_id
		WHERE j.document_id=$1 AND j.state='running'`, f.job.Document.ID).Scan(&token, &owner, &attempt, &incarnation, &live))
	require.Equal(t, f.job.LeaseToken, token)
	require.Equal(t, f.job.LeaseOwner, owner)
	require.Equal(t, f.job.Attempts, attempt)
	require.Equal(t, f.job.Incarnation, incarnation)
	require.True(t, live)
}

func (f pdfLeaseFixture) requireLocksAndPreimage(ctx context.Context, t *testing.T, before string) {
	t.Helper()
	requireIngestRowLocks(ctx, t, f.observer, f.job.Document.ID, true)
	require.Equal(t, before, ingestAdmissionState(ctx, t, f.observer, f.job.Document.ID))
}

func mixedPDFLeaseCheckpoint(text string) domain.PDFResult {
	return domain.PDFResult{Text: text, Coverage: domain.PDFComplete, Pages: []domain.PDFPageResult{
		{Number: 1, Source: "ocr", State: "text"},
		{Number: 2, Source: "parser", State: "text"},
	}}
}

func (f pdfLeaseFixture) requireCheckpoint(t *testing.T, result domain.PDFResult, source string) {
	t.Helper()
	var content, preview, storedSource string
	var coverage domain.PDFCoverage
	var rawPages []byte
	require.NoError(t, f.observer.QueryRow(f.ctx, `SELECT d.content,d.content_preview,d.text_source,p.coverage,p.page_results
		FROM documents d JOIN document_pdf_extractions p ON p.document_id=d.id WHERE d.id=$1`,
		f.job.Document.ID).Scan(&content, &preview, &storedSource, &coverage, &rawPages))
	var pages []domain.PDFPageResult
	require.NoError(t, json.Unmarshal(rawPages, &pages))
	runes := []rune(result.Text)
	require.Equal(t, result.Text, content)
	require.Equal(t, string(runes[:min(len(runes), 120)]), preview)
	require.Equal(t, source, storedSource)
	require.Equal(t, result.Coverage, coverage)
	require.Equal(t, result.Pages, pages)
}

type pdfLeaseOperation struct {
	call           func(context.Context) error
	requireSuccess func()
}

func beginPDFLeaseOperation(t *testing.T, f pdfLeaseFixture) pdfLeaseOperation {
	t.Helper()
	var originalID string
	require.NoError(t, f.observer.QueryRow(f.ctx, `SELECT window_id::text FROM document_pdf_extractions WHERE document_id=$1`,
		f.job.Document.ID).Scan(&originalID))
	var window domain.PDFOCRWindow
	return pdfLeaseOperation{
		call: func(ctx context.Context) error {
			var err error
			window, err = f.client.BeginPDFOCR(ctx, *f.job)
			return err
		},
		requireSuccess: func() {
			var started time.Time
			stmt := `SELECT started_at FROM document_pdf_extractions WHERE document_id=$1`
			require.NoError(t, f.observer.QueryRow(f.ctx, stmt, f.job.Document.ID).Scan(&started))
			require.Equal(t, originalID, window.ID)
			require.Equal(t, started.Add(domain.PDFOCRLifetime), window.Expires)
			require.Equal(t, window, f.begin(t))
			var repeated time.Time
			require.NoError(t, f.observer.QueryRow(f.ctx, stmt, f.job.Document.ID).Scan(&repeated))
			require.Equal(t, started, repeated)
		},
	}
}

func recordPDFLeaseOperation(t *testing.T, f pdfLeaseFixture) pdfLeaseOperation {
	t.Helper()
	window := f.begin(t)
	record := func(ctx context.Context) error {
		return f.client.RecordPDFOCRStart(ctx, *f.job, window.ID)
	}
	return pdfLeaseOperation{
		call: record,
		requireSuccess: func() {
			var accepted time.Time
			require.NoError(t, f.observer.QueryRow(f.ctx, `SELECT accepted_at FROM document_pdf_extractions WHERE document_id=$1`,
				f.job.Document.ID).Scan(&accepted))
			require.False(t, accepted.IsZero())
			before := f.state(t)
			require.NoError(t, record(f.ctx))
			require.Equal(t, before, f.state(t))
		},
	}
}

func storePDFLeaseOperation(t *testing.T, f pdfLeaseFixture) pdfLeaseOperation {
	t.Helper()
	window := f.begin(t)
	require.NoError(t, f.client.RecordPDFOCRStart(f.ctx, *f.job, window.ID))
	result := mixedPDFLeaseCheckpoint("Recovered line\n\nRepeated archive header")
	source := "pdf_mixed"
	if f.job.PDF.Profile == domain.LayoutPDFProfile {
		result = domain.PDFResult{Text: "Layout heading and table cell", Coverage: domain.PDFComplete, Pages: []domain.PDFPageResult{
			{Number: 1, Source: "textract", State: "text", Sections: []domain.LayoutSection{
				{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "Layout heading and table cell"},
			}},
		}}
		source = "pdf_layout"
	}
	store := func(ctx context.Context) error {
		return f.client.StorePDFOCR(ctx, *f.job, result)
	}
	return pdfLeaseOperation{
		call: store,
		requireSuccess: func() {
			f.requireCheckpoint(t, result, source)
			before := f.state(t)
			f.requireRejected(t, domain.ErrJobNotOwned, store)
			require.Equal(t, before, f.state(t))
		},
	}
}

var pdfLeaseWriteCases = []struct {
	name, statement    string
	prepare            func(*testing.T, pdfLeaseFixture) pdfLeaseOperation
	layoutDuringPrompt bool
}{
	{"begin window", beginPDFOCRWrite, beginPDFLeaseOperation, false},
	{"record provider start", recordPDFOCRWrite, recordPDFLeaseOperation, false},
	{"store extraction", storePDFOCRWrite, storePDFLeaseOperation, false},
	{"store document after extraction update", storePDFTextWrite, storePDFLeaseOperation, true},
}

func TestPDFLeaseRollbackDeadlineAfterRowLocks(t *testing.T) {
	for _, tc := range pdfLeaseWriteCases {
		t.Run(tc.name, func(t *testing.T) {
			tracer := &ingestPoolReleaseTracer{}
			f := newPDFLeaseFixture(t, tracer, false)
			op := tc.prepare(t, f)
			before := f.state(t)
			runCanceledIngestRollback(f.ctx, t, f.client, f.observer, tracer, tc.statement, func(ctx context.Context) {
				f.requireLocksAndPreimage(ctx, t, before)
			}, op.call)
			f.requireUnchangedAndRetry(t, before, op)
		})
	}
}

// runPromptPDFLeaseRollback releases the trace as soon as rollback starts so a
// healthy connection can execute the actual rollback.
func runPromptPDFLeaseRollback(t *testing.T, f pdfLeaseFixture, statement, before string, operation func(context.Context) error) {
	t.Helper()
	parent, cancel := context.WithCancel(f.ctx)
	traceWatchdog, releaseTrace := context.WithCancel(context.WithoutCancel(f.ctx))
	entryWatchdog, stopEntryWatchdog := context.WithTimeout(f.ctx, 10*time.Second)
	defer stopEntryWatchdog()

	trace := &ingestTxTrace{
		statement: statement, cancelOperation: cancel, operationPaused: make(chan ingestQueryObservation, 1),
		continueQuery: make(chan struct{}), rollbackEntered: make(chan time.Time, 1), watchdog: traceWatchdog,
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
		releaseTrace()
		cleanup, stopCleanup := context.WithTimeout(context.WithoutCancel(f.ctx), 6*time.Second)
		defer stopCleanup()
		select {
		case <-done:
		case <-cleanup.Done():
			t.Error("PDF operation still running after releasing the trace")
		}
	}()

	var queryPID uint32
	select {
	case observed := <-trace.operationPaused:
		require.NoError(t, observed.entryErr)
		require.ErrorIs(t, observed.ctx.Err(), context.Canceled)
		require.NotEqual(t, f.observer.PgConn().PID(), observed.pid)
		queryPID = observed.pid
	case <-done:
		t.Fatalf("PDF operation returned before the traced write: %v", operationErr)
	case <-entryWatchdog.Done():
		t.Fatal("PDF operation did not reach the traced write before the watchdog")
	}
	f.requireLocksAndPreimage(entryWatchdog, t, before)
	close(trace.continueQuery)

	var rollbackStart time.Time
	select {
	case rollbackStart = <-trace.rollbackEntered:
		releaseTrace()
	case <-done:
		t.Fatalf("PDF operation returned without a wire rollback: %v", operationErr)
	case <-entryWatchdog.Done():
		t.Fatal("PDF operation did not start rollback before the watchdog")
	}
	finishWatchdog, stopFinishWatchdog := context.WithDeadline(context.WithoutCancel(f.ctx), rollbackStart.Add(6*time.Second))
	defer stopFinishWatchdog()
	select {
	case <-done:
		require.ErrorIs(t, operationErr, context.Canceled)
	case <-finishWatchdog.Done():
		t.Fatal("PDF operation did not finish rollback before the watchdog")
	}
	require.NoError(t, finishWatchdog.Err())
	// The recovery probe needs a live context after the operation was canceled.
	requirePromptIngestRollback(f.ctx, t, f.client, f.observer, trace)
	require.Equal(t, queryPID, trace.rollbacks[0].pid)
	require.Zero(t, trace.commits)
}

func (f pdfLeaseFixture) requireUnchangedAndRetry(t *testing.T, before string, op pdfLeaseOperation) {
	t.Helper()
	require.Equal(t, before, f.state(t))
	requireIngestRowLocks(f.ctx, t, f.observer, f.job.Document.ID, false)
	f.requireLiveLease(t)

	trace := &ingestTxTrace{}
	require.NoError(t, op.call(context.WithValue(f.ctx, ingestTraceKey{}, trace)))
	requireIngestCommit(t, trace)
	op.requireSuccess()
	f.requireLiveLease(t)
}

func TestPDFLeaseRollbackPromptlyReusesConnection(t *testing.T) {
	for _, tc := range pdfLeaseWriteCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newPDFLeaseFixture(t, ingestTxTracer{}, tc.layoutDuringPrompt)
			op := tc.prepare(t, f)
			before := f.state(t)
			runPromptPDFLeaseRollback(t, f, tc.statement, before, op.call)
			f.requireUnchangedAndRetry(t, before, op)
		})
	}
	t.Run("already expired parent", func(t *testing.T) {
		f := newPDFLeaseFixture(t, ingestTxTracer{}, false)
		before := f.state(t)
		expired, cancel := context.WithDeadline(f.ctx, time.Now().Add(-time.Second))
		defer cancel()
		requireNoPDFLeaseTransaction(expired, t, beginPDFLeaseOperation(t, f).call, context.DeadlineExceeded)
		require.Equal(t, before, f.state(t))
		requireIngestRowLocks(f.ctx, t, f.observer, f.job.Document.ID, false)
	})
}

func (f pdfLeaseFixture) requireRejected(t *testing.T, want error, operation func(context.Context) error) {
	t.Helper()
	trace := &ingestTxTrace{}
	ctx := context.WithValue(f.ctx, ingestTraceKey{}, trace)
	require.ErrorIs(t, operation(ctx), want)
	requirePromptIngestRollback(ctx, t, f.client, f.observer, trace)
	require.Zero(t, trace.commits)
	requireIngestRowLocks(f.ctx, t, f.observer, f.job.Document.ID, false)
}

func requireNoPDFLeaseTransaction(ctx context.Context, t *testing.T, operation func(context.Context) error, want error) {
	t.Helper()
	trace := &ingestTxTrace{statement: "begin"}
	require.ErrorIs(t, operation(context.WithValue(ctx, ingestTraceKey{}, trace)), want)
	require.Zero(t, trace.operation.pid)
	require.Empty(t, trace.rollbacks)
	require.Zero(t, trace.commits)
}

func TestPDFLeaseRollbackPreservesOwnershipErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, pdfLeaseFixture) domain.Job
	}{
		{"wrong token", func(_ *testing.T, f pdfLeaseFixture) domain.Job {
			job := *f.job
			job.LeaseToken = uuid.NewString()
			return job
		}},
		{"wrong owner", func(_ *testing.T, f pdfLeaseFixture) domain.Job {
			job := *f.job
			job.LeaseOwner = "different-owner"
			return job
		}},
		{"wrong attempt", func(_ *testing.T, f pdfLeaseFixture) domain.Job {
			job := *f.job
			job.Attempts++
			return job
		}},
		{"wrong source key", func(_ *testing.T, f pdfLeaseFixture) domain.Job {
			job := *f.job
			key := "different/original.pdf"
			job.Document.S3Key = &key
			return job
		}},
		{"wrong incarnation", func(_ *testing.T, f pdfLeaseFixture) domain.Job {
			job := *f.job
			job.Incarnation = uuid.NewString()
			return job
		}},
		{"non-running job", func(t *testing.T, f pdfLeaseFixture) domain.Job {
			t.Helper()
			_, err := f.client.Pool.Exec(f.ctx, `UPDATE ingest_jobs SET state='pending' WHERE document_id=$1`, f.job.Document.ID)
			require.NoError(t, err)
			// Restore the token cleared by the queue trigger to isolate the
			// lease guard's running-state predicate.
			_, err = f.client.Pool.Exec(f.ctx, `UPDATE ingest_jobs SET lease_token=$2 WHERE document_id=$1`,
				f.job.Document.ID, f.job.LeaseToken)
			require.NoError(t, err)
			return *f.job
		}},
		{"expired lease", func(t *testing.T, f pdfLeaseFixture) domain.Job {
			t.Helper()
			expireJobLeaseContext(f.ctx, t, f.client, f.job.Document.ID)
			return *f.job
		}},
		{"released and reclaimed attempt", func(t *testing.T, f pdfLeaseFixture) domain.Job {
			t.Helper()
			require.NoError(t, f.client.ReleaseJob(f.ctx, *f.job))
			current, err := f.client.ClaimPDFJob(f.ctx, f.job.LeaseOwner, time.Minute, ingestMaxAttempts)
			require.NoError(t, err)
			require.NotNil(t, current)
			require.Equal(t, f.job.Document.ID, current.Document.ID)
			require.Equal(t, f.job.Attempts, current.Attempts)
			require.NotEqual(t, f.job.LeaseToken, current.LeaseToken)
			return *f.job
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPDFLeaseFixture(t, ingestTxTracer{}, false)
			otherReq := mixedPDFRequest()
			otherReq.Collection = "other-collection"
			otherReq.Fingerprint = chunker.ScopedFingerprint(otherReq.Collection, otherReq.Fingerprint)
			other := acceptPDFLeaseFixtureJob(f.ctx, t, f.client, otherReq)
			require.NotEqual(t, f.job.Collection, other.Collection)
			job := tc.mutate(t, f)
			before := f.state(t)
			otherBefore := ingestAdmissionState(f.ctx, t, f.observer, other.Document.ID)
			f.requireRejected(t, domain.ErrJobNotOwned, func(ctx context.Context) error {
				_, err := f.client.BeginPDFOCR(ctx, job)
				return err
			})
			require.Equal(t, before, f.state(t))
			require.Equal(t, otherBefore, ingestAdmissionState(f.ctx, t, f.observer, other.Document.ID))
			requireIngestRowLocks(f.ctx, t, f.observer, other.Document.ID, false)
		})
	}

	t.Run("errors after the lease guard", testPDFLeaseCallbackErrors)
	t.Run("short circuits", testPDFLeaseShortCircuits)
}

func testPDFLeaseCallbackErrors(t *testing.T) {
	f := newPDFLeaseFixture(t, ingestTxTracer{}, false)
	f.begin(t)
	before := f.state(t)
	f.requireRejected(t, domain.ErrJobNotOwned, func(ctx context.Context) error {
		return f.client.RecordPDFOCRStart(ctx, *f.job, uuid.NewString())
	})
	require.Equal(t, before, f.state(t))

	sentinel := errors.New("checkpoint callback failed")
	f.requireRejected(t, sentinel, func(ctx context.Context) error {
		err := f.client.withPDFLease(ctx, *f.job, func(pgx.Tx) error { return sentinel })
		require.Equal(t, sentinel, err)
		return err
	})
	require.Equal(t, before, f.state(t))

	_, err := f.client.Pool.Exec(f.ctx, `UPDATE document_pdf_extractions
		SET started_at=clock_timestamp()-make_interval(secs=>$2) WHERE document_id=$1`,
		f.job.Document.ID, (domain.PDFOCRLifetime + time.Hour).Seconds())
	require.NoError(t, err)
	before = f.state(t)
	f.requireRejected(t, domain.ErrPDFOCRExpired, func(ctx context.Context) error {
		_, beginErr := f.client.BeginPDFOCR(ctx, *f.job)
		require.ErrorIs(t, beginErr, domain.ErrPermanent)
		return beginErr
	})
	require.Equal(t, before, f.state(t))
}

func testPDFLeaseShortCircuits(t *testing.T) {
	f := newPDFLeaseFixture(t, ingestTxTracer{}, false)
	before := f.state(t)
	requireNoPDFLeaseTransaction(f.ctx, t, func(ctx context.Context) error {
		job := *f.job
		job.LeaseToken = ""
		_, err := f.client.BeginPDFOCR(ctx, job)
		return err
	}, domain.ErrJobNotOwned)
	requireNoPDFLeaseTransaction(f.ctx, t, func(ctx context.Context) error {
		job := *f.job
		job.PDF = nil
		return f.client.StorePDFOCR(ctx, job, mixedPDFLeaseCheckpoint("Recovered PDF text"))
	}, domain.ErrJobNotOwned)
	require.Equal(t, before, f.state(t))
	requireIngestRowLocks(f.ctx, t, f.observer, f.job.Document.ID, false)
}

func TestPDFLeaseCheckpointValidationAndRetry(t *testing.T) {
	t.Run("invalid before transaction", testPDFLeaseCheckpointValidation)
	for _, tc := range []struct {
		name, source string
		layout       bool
		result       domain.PDFResult
	}{
		{"mixed complete with minimum text", "pdf_mixed", false, mixedPDFLeaseCheckpoint(strings.Repeat("a", domain.MinPDFTextBytes))},
		{"mixed partial with empty text", "pdf_mixed", false, domain.PDFResult{Coverage: domain.PDFPartial, Pages: []domain.PDFPageResult{
			{Number: 1, State: "missing"}, {Number: 2, State: "missing"},
		}}},
		{"layout partial with multibyte preview", "pdf_layout", true, domain.PDFResult{
			Text: strings.Repeat("界", 120) + " truncation", Coverage: domain.PDFPartial, Pages: []domain.PDFPageResult{{
				Number: 1, Source: "textract", State: "partial", Sections: []domain.LayoutSection{
					{Page: 1, Text: "Partial layout extraction"},
				},
			}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPDFLeaseFixture(t, ingestTxTracer{}, tc.layout)
			window := f.begin(t)
			require.NoError(t, f.client.RecordPDFOCRStart(f.ctx, *f.job, window.ID))
			store := func(ctx context.Context) error {
				return f.client.StorePDFOCR(ctx, *f.job, tc.result)
			}
			trace := &ingestTxTrace{}
			require.NoError(t, store(context.WithValue(f.ctx, ingestTraceKey{}, trace)))
			requireIngestCommit(t, trace)
			f.requireCheckpoint(t, tc.result, tc.source)
			before := f.state(t)
			f.requireRejected(t, domain.ErrJobNotOwned, store)
			require.Equal(t, before, f.state(t))
		})
	}
}

func testPDFLeaseCheckpointValidation(t *testing.T) {
	f := newPDFLeaseFixture(t, ingestTxTracer{}, false)
	window := f.begin(t)
	require.NoError(t, f.client.RecordPDFOCRStart(f.ctx, *f.job, window.ID))
	before := f.state(t)
	for _, tc := range []struct {
		name, want string
		mutate     func(*domain.PDFResult)
	}{
		{"NUL", "invalid PDF detection checkpoint", func(r *domain.PDFResult) { r.Text += "\x00" }},
		{"invalid UTF-8", "invalid PDF detection checkpoint", func(r *domain.PDFResult) { r.Text += "\xff" }},
		{"oversize", "invalid PDF detection checkpoint", func(r *domain.PDFResult) { r.Text = strings.Repeat("a", domain.MaxContentBytes+1) }},
		{"out of order", "PDF detection checkpoint has inconsistent page coverage", func(r *domain.PDFResult) {
			r.Pages[0], r.Pages[1] = r.Pages[1], r.Pages[0]
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := mixedPDFLeaseCheckpoint("Recovered line\n\nRepeated archive header")
			tc.mutate(&result)
			trace := &ingestTxTrace{statement: "begin"}
			ctx := context.WithValue(f.ctx, ingestTraceKey{}, trace)
			require.EqualError(t, f.client.StorePDFOCR(ctx, *f.job, result), tc.want)
			require.Zero(t, trace.operation.pid)
			require.Empty(t, trace.rollbacks)
			require.Zero(t, trace.commits)
			require.Equal(t, before, f.state(t))
			requireIngestRowLocks(f.ctx, t, f.observer, f.job.Document.ID, false)
		})
	}
}
