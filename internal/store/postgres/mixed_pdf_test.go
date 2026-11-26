package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func mixedPDFRequest() domain.AcceptRequest {
	key := uuid.NewString() + "/original.pdf"
	req := jobsRequest("")
	req.Document.S3Key = &key
	req.RequireSource = true
	req.Fingerprint = chunker.Fingerprint("original profile bytes")
	req.PDF = &domain.PDFExtraction{Profile: domain.MixedPDFProfile, Coverage: domain.PDFPending, Pages: domain.PDFPages{
		{Number: 1, Evidence: domain.PDFImage},
		{Number: 2, Evidence: domain.PDFEmbedded, Text: "Repeated archive header"},
	}}
	return req
}

func TestMixedPDFServingRoleGrants(t *testing.T) {
	t.Run("fresh", func(t *testing.T) { testMixedPDFServingRoleGrants(t, false) })
	t.Run("upgrade from index visibility", func(t *testing.T) { testMixedPDFServingRoleGrants(t, true) })
}

// TestMixedPDFServingRolesAreCleanedUpBetweenFixtures repeats both cases on one
// shared cluster. Each fixture must own four distinct roles that exist while
// it runs and are gone once it ends.
func TestMixedPDFServingRolesAreCleanedUpBetweenFixtures(t *testing.T) {
	if _, configured := os.LookupEnv("QUARRY_TEST_POSTGRES_ADMIN_DSN"); !configured {
		t.Skip("requires a shared PostgreSQL test service")
	}
	observer := migrationTestClient(t, startTestDatabase(t))
	roleExists := func(name string) bool {
		var exists bool
		require.NoError(t, observer.Pool.QueryRow(t.Context(),
			`SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)`, name).Scan(&exists))
		return exists
	}
	seen := make(map[string]bool)
	for _, upgrade := range []bool{false, true, false} {
		var roles grantRoles
		t.Run("fixture", func(t *testing.T) {
			roles = testMixedPDFServingRoleGrants(t, upgrade)
			require.Len(t, roles, 1+len(servingRoles))
			for _, name := range roles {
				require.False(t, seen[name], "role %s was reused by a later fixture", name)
				seen[name] = true
				require.True(t, roleExists(name), "fixture role %s was not created", name)
			}
		})
		for _, name := range roles {
			require.False(t, roleExists(name), "fixture role %s survived cleanup", name)
		}
	}
}

func testMixedPDFServingRoleGrants(t *testing.T, upgrade bool) grantRoles {
	t.Helper()
	databaseURL := startTestDatabase(t)
	roles := newGrantRoles(t, databaseURL, append([]string{"quarry_metadata_owner"}, servingRoles...)...)
	quoted := func(purpose string) string { return pgx.Identifier{roles.name(purpose)}.Sanitize() }
	admin := migrationTestClient(t, databaseURL)
	grantExec(t, admin, `CREATE EXTENSION IF NOT EXISTS pgcrypto`)
	for _, purpose := range append([]string{"quarry_metadata_owner"}, servingRoles...) {
		grantExec(t, admin, "CREATE ROLE "+quoted(purpose))
	}
	grantExec(t, admin, "ALTER SCHEMA public OWNER TO "+quoted("quarry_metadata_owner"))
	grantExec(t, admin, `REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
	owner := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+quoted("quarry_metadata_owner"))
	if upgrade {
		applyMigrationPrefix(t, owner, 12)
	}
	require.NoError(t, owner.RunMigrations(t.Context()))
	requireRecordedMigrations(t, owner)
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
	serving := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+quoted("quarry_service"))
	ctx := t.Context()
	require.NoError(t, serving.RunMigrations(ctx))
	legacy := jobsAccept(t, serving, "ordinary queued document")
	docs, err := serving.ListDocumentsPage(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, docs, 1)
	require.Equal(t, string(legacy), docs[0].ID)
	job, err := serving.ClaimJob(ctx, "image-only-serving", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, legacy, job.Document.ID)
	completed, err := serving.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, completed)

	req := mixedPDFRequest()
	accepted, err := serving.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	job, err = serving.ClaimPDFJob(ctx, "mixed-serving", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, accepted.DocumentID, job.Document.ID)
	_, err = serving.BeginPDFOCR(ctx, *job)
	require.NoError(t, err)
	checkpoint := domain.PDFResult{Text: "Recovered line\n\nRepeated archive header", Coverage: domain.PDFComplete, Pages: []domain.PDFPageResult{
		{Number: 1, Source: "ocr", State: "text"}, {Number: 2, Source: "parser", State: "text"},
	}}
	require.NoError(t, serving.StorePDFOCR(ctx, *job, checkpoint))
	summary, err := serving.GetDocumentSummary(ctx, accepted.DocumentID)
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.Equal(t, domain.PDFComplete, summary.PDFCoverage)
	return roles
}

func TestMixedPDFClaimAndCheckpoint(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	req := mixedPDFRequest()
	result, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET state='running',attempts=attempts+1,lease_owner='old-binary',lease_until=NOW()+interval '1 minute' WHERE document_id=$1`, string(result.DocumentID))
	requireIncompatiblePDFLease(t, err)
	legacy, err := c.ClaimJob(ctx, "without-ocr", time.Minute, 5)
	require.NoError(t, err)
	require.Nil(t, legacy)
	claimed := make(chan *domain.Job, 2)
	for _, owner := range []string{"first", "second"} {
		go func() {
			job, claimErr := c.ClaimPDFJob(ctx, owner, time.Minute, 5)
			if claimErr != nil {
				t.Error(claimErr)
			}
			claimed <- job
		}()
	}
	first, second := <-claimed, <-claimed
	if first == nil {
		first, second = second, first
	}
	require.NotNil(t, first)
	require.Nil(t, second)
	require.Equal(t, req.PDF, first.PDF)
	expiry, err := c.BeginPDFOCR(ctx, *first)
	require.NoError(t, err)
	var start time.Time
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT started_at FROM document_pdf_extractions WHERE document_id=$1`, string(result.DocumentID)).Scan(&start))

	checkpoint := domain.PDFResult{Text: "Recovered line\n\nRepeated archive header", Coverage: domain.PDFComplete, Pages: []domain.PDFPageResult{
		{Number: 1, Source: "ocr", State: "text"}, {Number: 2, Source: "parser", State: "text"},
	}}
	lock, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(ctx) }()
	_, err = lock.Exec(ctx, `SELECT id FROM documents WHERE id=$1 FOR UPDATE`, string(result.DocumentID))
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.StorePDFOCR(canceled, *first, checkpoint) }()
	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := c.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT TRUE FROM documents d JOIN ingest_jobs j%')`).Scan(&waiting)
		return queryErr == nil && waiting
	}, 2*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, lock.Commit(ctx))
	require.NoError(t, c.ReleaseJob(ctx, *first))
	// Run the pre-migration claim statement against the released job.
	var claimedID string
	var attempt int
	err = c.Pool.QueryRow(ctx, `UPDATE ingest_jobs j SET state='running',attempts=j.attempts+1,lease_owner=$1,lease_until=NOW()+make_interval(secs=>$2),updated_at=NOW()
		WHERE j.document_id=(SELECT document_id FROM ingest_jobs WHERE (state='pending' AND next_attempt_at<=NOW()) OR (state='running' AND lease_until<NOW()) ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1)
		RETURNING j.document_id::text,j.attempts`, "legacy-owner", 60.0).Scan(&claimedID, &attempt)
	requireIncompatiblePDFLease(t, err)
	current, err := c.ClaimPDFJob(ctx, "new-owner", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, current)
	require.ErrorIs(t, c.StorePDFOCR(ctx, *first, checkpoint), domain.ErrJobNotOwned)
	sameExpiry, err := c.BeginPDFOCR(ctx, *current)
	require.NoError(t, err)
	require.Equal(t, expiry, sameExpiry)
	var sameStart time.Time
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT started_at FROM document_pdf_extractions WHERE document_id=$1`, string(result.DocumentID)).Scan(&sameStart))
	require.Equal(t, start, sameStart)
	require.NoError(t, c.StorePDFOCR(ctx, *current, checkpoint))
	require.NoError(t, c.ReleaseJob(ctx, *current))
	resumed, err := c.ClaimPDFJob(ctx, "embedding-retry", time.Minute, 5)
	require.NoError(t, err)
	require.Equal(t, checkpoint.Text, resumed.Document.Content)
	require.Equal(t, domain.PDFComplete, resumed.PDF.Coverage)
	require.NoError(t, c.DeleteDocument(ctx, result.DocumentID))
	newKey := "replacement.pdf"
	req.Document.S3Key = &newKey
	_, err = c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	replacement, err := c.ClaimPDFJob(ctx, resumed.LeaseOwner, time.Minute, 5)
	require.NoError(t, err)
	require.Equal(t, resumed.Attempts, replacement.Attempts)
	require.ErrorIs(t, c.StorePDFOCR(ctx, *current, checkpoint), domain.ErrJobNotOwned)
	require.ErrorIs(t, c.FailJob(ctx, *resumed, "stale revision"), domain.ErrJobNotOwned)
	_, err = c.BeginPDFOCR(ctx, *replacement)
	require.NoError(t, err)
	incomplete := domain.PDFResult{Text: req.PDF.Pages[1].Text, Coverage: domain.PDFComplete, Pages: []domain.PDFPageResult{
		{Number: 1, State: "missing"}, {Number: 2, Source: "parser", State: "text"},
	}}
	require.ErrorContains(t, c.StorePDFOCR(ctx, *replacement, incomplete), "inconsistent page coverage")
	incomplete.Coverage = domain.PDFPartial
	require.NoError(t, c.StorePDFOCR(ctx, *replacement, incomplete))
	summary, err := c.GetDocumentSummary(ctx, replacement.Document.ID)
	require.NoError(t, err)
	require.Equal(t, domain.PDFPartial, summary.PDFCoverage)
}

func TestMixedPDFReplacementRejectsDelayedLegacyOwner(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	legacy := jobsRequest("")
	legacy.Document.TextSource = "ocr_pending"
	legacyKey := "legacy/original.pdf"
	legacy.Document.S3Key = &legacyKey
	old, err := c.AcceptDocument(ctx, legacy)
	require.NoError(t, err)
	stale, err := c.ClaimPDFJob(ctx, "shared-owner", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, stale)
	require.Nil(t, stale.PDF)
	require.NoError(t, c.DeleteDocument(ctx, old.DocumentID))
	request := mixedPDFRequest()
	request.Document.ID = old.DocumentID
	_, err = c.AcceptDocument(ctx, request)
	require.NoError(t, err)
	current, err := c.ClaimPDFJob(ctx, "shared-owner", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, current)
	require.Equal(t, stale.Attempts, current.Attempts)
	owned, err := c.HeartbeatJob(ctx, *stale, time.Minute)
	require.NoError(t, err)
	require.False(t, owned)
	require.ErrorIs(t, c.StoreOCRText(ctx, *stale, "Stale legacy text"), domain.ErrJobNotOwned)
	require.ErrorIs(t, c.RetryJob(ctx, *stale, 0, "stale retry"), domain.ErrJobNotOwned)
	require.ErrorIs(t, c.ReleaseJob(ctx, *stale), domain.ErrJobNotOwned)
	require.ErrorIs(t, c.FailJob(ctx, *stale, "stale failure"), domain.ErrJobNotOwned)
	completed, err := c.CompleteJob(ctx, *stale, []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), Text: "Stale legacy text"}}, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, completed)
	_, err = c.BeginPDFOCR(ctx, *current)
	require.NoError(t, err)
	summary, err := c.GetDocumentSummary(ctx, old.DocumentID)
	require.NoError(t, err)
	require.Equal(t, "processing", summary.Status)
	require.Equal(t, domain.PDFPending, summary.PDFCoverage)
}

func TestMixedPDFClaimCannotCheckpointReclaimedAttempt(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	for _, scenario := range []struct {
		name     string
		terminal bool
	}{
		{"release", false},
		{"terminal requeue", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			req := mixedPDFRequest()
			result, err := c.AcceptDocument(ctx, req)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, c.DeleteDocument(ctx, result.DocumentID)) })
			first, err := c.ClaimPDFJob(ctx, "shared-owner", time.Minute, 5)
			require.NoError(t, err)
			firstWindow, err := c.BeginPDFOCR(ctx, *first)
			require.NoError(t, err)
			if scenario.terminal {
				_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(result.DocumentID))
				require.NoError(t, err)
				second, claimErr := c.ClaimPDFJob(ctx, "second-owner", time.Minute, 5)
				require.NoError(t, claimErr)
				require.Equal(t, 2, second.Attempts)
				require.NoError(t, c.FailJob(ctx, *second, "terminal failure"))
				req.CallerSupplied = true
				resumed, retryErr := c.AcceptDocument(ctx, req)
				require.NoError(t, retryErr)
				require.Equal(t, domain.AcceptRequeued, resumed.Outcome)
			} else {
				require.NoError(t, c.ReleaseJob(ctx, *first))
			}
			current, err := c.ClaimPDFJob(ctx, "shared-owner", time.Minute, 5)
			require.NoError(t, err)
			require.Equal(t, first.Attempts, current.Attempts)
			require.Equal(t, first.Incarnation, current.Incarnation)
			require.NotEmpty(t, first.LeaseToken)
			require.NotEmpty(t, current.LeaseToken)
			require.NotEqual(t, first.LeaseToken, current.LeaseToken)
			checkpoint := domain.PDFResult{Text: "Old OCR result from stalled request", Coverage: domain.PDFComplete, Pages: []domain.PDFPageResult{
				{Number: 1, Source: "ocr", State: "text"}, {Number: 2, Source: "parser", State: "text"},
			}}
			require.ErrorIs(t, c.StorePDFOCR(ctx, *first, checkpoint), domain.ErrJobNotOwned)
			require.ErrorIs(t, c.RecordPDFOCRStart(ctx, *first, firstWindow.ID), domain.ErrJobNotOwned)
			owned, err := c.HeartbeatJob(ctx, *first, time.Minute)
			require.NoError(t, err)
			require.False(t, owned)
			require.ErrorIs(t, c.StoreOCRText(ctx, *first, "stale legacy text"), domain.ErrJobNotOwned)
			require.ErrorIs(t, c.RetryJob(ctx, *first, 0, "stale retry"), domain.ErrJobNotOwned)
			require.ErrorIs(t, c.ReleaseJob(ctx, *first), domain.ErrJobNotOwned)
			require.ErrorIs(t, c.FailJob(ctx, *first, "stale failure"), domain.ErrJobNotOwned)
			completed, err := c.CompleteJob(ctx, *first, nil, domain.IndexObservation{})
			require.NoError(t, err)
			require.False(t, completed)
			currentWindow, err := c.BeginPDFOCR(ctx, *current)
			require.NoError(t, err)
			require.Equal(t, firstWindow, currentWindow)
			require.NoError(t, c.RecordPDFOCRStart(ctx, *current, currentWindow.ID))
			checkpoint.Text = "Current OCR result from the replacement attempt"
			require.NoError(t, c.StorePDFOCR(ctx, *current, checkpoint))
		})
	}
}

func TestMixedPDFActivationRejectsIncompatibleClaims(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	legacyID := jobsAccept(t, c, "Legacy text in shared queue")
	legacy, err := c.ClaimJob(ctx, "legacy", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, legacy)
	req := mixedPDFRequest()
	_, err = c.AcceptDocument(ctx, req)
	require.ErrorContains(t, err, "requires compatible ingestion consumers")
	require.NoError(t, c.ReleaseJob(ctx, *legacy))
	mixed, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, mixed.Outcome)
	_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET state='running',attempts=attempts+1,lease_owner='old-binary',lease_until=NOW()+interval '1 minute' WHERE document_id=$1`, string(legacyID))
	requireIncompatiblePDFLease(t, err)
	legacy, err = c.ClaimJob(ctx, "legacy", time.Minute, 5)
	require.NoError(t, err)
	require.Nil(t, legacy)
	require.NoError(t, c.DeleteDocument(ctx, mixed.DocumentID))
	legacy, err = c.ClaimJob(ctx, "legacy", time.Minute, 5)
	require.NoError(t, err)
	require.Equal(t, legacyID, legacy.Document.ID)
}

func requireIncompatiblePDFLease(t *testing.T, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23514", pgErr.Code)
	require.Equal(t, "ingest_jobs_pdf_lease", pgErr.ConstraintName)
}

func TestMixedPDFExpiryRequiresExplicitRetry(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	req := mixedPDFRequest()
	result, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	job, err := c.ClaimPDFJob(ctx, "owner", time.Minute, 5)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `UPDATE document_pdf_extractions SET started_at=NOW()-interval '8 days' WHERE document_id=$1`, string(result.DocumentID))
	require.NoError(t, err)
	_, err = c.BeginPDFOCR(ctx, *job)
	require.ErrorIs(t, err, domain.ErrPDFOCRExpired)
	require.NoError(t, c.FailJob(ctx, *job, "ocr_result_expired"))
	summary, err := c.GetDocumentSummary(ctx, result.DocumentID)
	require.NoError(t, err)
	require.Equal(t, "failed", summary.Status)
	require.Equal(t, domain.PDFFailed, summary.PDFCoverage)
	result, err = c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptOCRRetryRequired, result.Outcome)
	req.CallerSupplied = true
	req.Document.S3Key = nil
	result, err = c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptRequeued, result.Outcome)
	next, err := c.ClaimPDFJob(ctx, "next", time.Minute, 5)
	require.NoError(t, err)
	require.Equal(t, job.Document.S3Key, next.Document.S3Key)
	_, err = c.BeginPDFOCR(ctx, *next)
	require.NoError(t, err)
}

func TestMixedPDFUnacknowledgedStartWaitsForBound(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	req := mixedPDFRequest()
	result, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	job, err := c.ClaimPDFJob(ctx, "owner", time.Minute, 5)
	require.NoError(t, err)
	requestCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	window, err := c.BeginPDFOCR(requestCtx, *job)
	require.NoError(t, err)
	var intent, bound time.Time
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT started_at,start_bound_at FROM document_pdf_extractions WHERE document_id=$1`, string(result.DocumentID)).Scan(&intent, &bound))
	require.Positive(t, bound.Sub(intent))
	require.LessOrEqual(t, bound.Sub(intent), time.Minute)
	require.NoError(t, c.FailJob(ctx, *job, "uncertain provider response"))
	_, err = c.Pool.Exec(ctx, `UPDATE document_pdf_extractions SET started_at=NOW()-interval '8 days' WHERE document_id=$1`, string(result.DocumentID))
	require.NoError(t, err)
	req.CallerSupplied = true
	req.Document.S3Key = nil
	retry, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptOCRRetryAfterExpiry, retry.Outcome)
	_, err = c.Pool.Exec(ctx, `UPDATE document_pdf_extractions SET start_bound_at=NOW()-interval '8 days' WHERE document_id=$1`, string(result.DocumentID))
	require.NoError(t, err)
	retry, err = c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptRequeued, retry.Outcome)
	next, err := c.ClaimPDFJob(ctx, "owner", time.Minute, 5)
	require.NoError(t, err)
	nextWindow, err := c.BeginPDFOCR(ctx, *next)
	require.NoError(t, err)
	require.NotEqual(t, window.ID, nextWindow.ID)
	require.ErrorIs(t, c.RecordPDFOCRStart(ctx, *next, window.ID), domain.ErrJobNotOwned)
}

func TestMixedPDFRetryReopensExpiredUnusableCheckpoint(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	for _, usable := range []bool{false, true} {
		req := mixedPDFRequest()
		req.Fingerprint = chunker.Fingerprint(string(req.Document.ID))
		_, err := c.AcceptDocument(ctx, req)
		require.NoError(t, err)
		job, err := c.ClaimPDFJob(ctx, "owner", time.Minute, 5)
		require.NoError(t, err)
		_, err = c.BeginPDFOCR(ctx, *job)
		require.NoError(t, err)
		result := domain.PDFResult{Coverage: domain.PDFPartial, Pages: []domain.PDFPageResult{{Number: 1, State: "unrecognized"}, {Number: 2, State: "missing"}}}
		if usable {
			result.Text = "Still usable archived text"
		}
		require.NoError(t, c.StorePDFOCR(ctx, *job, result))
		require.NoError(t, c.FailJob(ctx, *job, "indexing failed"))
		if !usable {
			explicit := req
			explicit.CallerSupplied = true
			beforeExpiry, retryErr := c.AcceptDocument(ctx, explicit)
			require.NoError(t, retryErr)
			require.Equal(t, domain.AcceptOCRRetryAfterExpiry, beforeExpiry.Outcome)
		}
		_, err = c.Pool.Exec(ctx, `UPDATE document_pdf_extractions SET started_at=NOW()-interval '8 days' WHERE document_id=$1`, string(req.Document.ID))
		require.NoError(t, err)
		if !usable {
			anonymous, anonymousErr := c.AcceptDocument(ctx, req)
			require.NoError(t, anonymousErr)
			require.Equal(t, domain.AcceptOCRRetryRequired, anonymous.Outcome)
		}
		req.CallerSupplied = true
		req.Document.S3Key = nil
		retry, err := c.AcceptDocument(ctx, req)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptRequeued, retry.Outcome)
		next, err := c.ClaimPDFJob(ctx, "retry", time.Minute, 5)
		require.NoError(t, err)
		if usable {
			require.Equal(t, domain.PDFPartial, next.PDF.Coverage)
			require.Equal(t, result.Text, next.Document.Content)
		} else {
			require.Equal(t, domain.PDFPending, next.PDF.Coverage)
			require.Equal(t, job.Document.S3Key, next.Document.S3Key)
			newExpiry, startErr := c.BeginPDFOCR(ctx, *next)
			require.NoError(t, startErr)
			require.WithinDuration(t, time.Now().Add(domain.PDFOCRLifetime), newExpiry.Expires, time.Minute)
		}
		require.NoError(t, c.DeleteDocument(ctx, req.Document.ID))
	}
}

func TestMixedPDFIdentityPreservesLegacyAndCollection(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	legacy := jobsRequest("Repeated archive header")
	legacy.Collection = "archive"
	legacy.Fingerprint = chunker.ScopedFingerprint(legacy.Collection, legacy.Document.Content)
	old, err := c.AcceptDocument(ctx, legacy)
	require.NoError(t, err)
	job, err := c.ClaimJob(ctx, "legacy", time.Minute, 5)
	require.NoError(t, err)
	done, err := c.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)
	req := mixedPDFRequest()
	req.Collection = legacy.Collection
	req.LegacyParserFingerprint = legacy.Fingerprint
	req.LegacyOCRFingerprint = chunker.ScopedFingerprint(legacy.Collection, "original bytes")
	req.CallerSupplied = true
	req.Document.ID = old.DocumentID
	result, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptDuplicate, result.Outcome)
	summary, err := c.GetDocumentSummary(ctx, old.DocumentID)
	require.NoError(t, err)
	require.Empty(t, summary.PDFCoverage)
	req.Document.ID = domain.DocumentID(uuid.NewString())
	req.CallerSupplied = false
	result, err = c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	different := req
	different.Collection = "other-collection"
	different.Fingerprint = chunker.ScopedFingerprint(different.Collection, "original profile bytes")
	different.Document.ID = domain.DocumentID(uuid.NewString())
	result, err = c.AcceptDocument(ctx, different)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	same := req
	same.Document.ID = domain.DocumentID(uuid.NewString())
	same.Document.S3Key = nil
	result, err = c.AcceptDocument(ctx, same)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptInProgress, result.Outcome)
	require.Equal(t, req.Document.ID, result.DocumentID)
	original := same
	original.PDF = nil
	original.ContentDedup = domain.ContentDedupCollection
	result, err = c.AcceptExistingPDF(ctx, original, true)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptInProgress, result.Outcome)
	require.Equal(t, req.Document.ID, result.DocumentID)
	original.Collection = different.Collection
	result, err = c.AcceptExistingPDF(ctx, original, true)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptNeedsSource, result.Outcome)
	original.Collection = same.Collection
	original.Fingerprint = legacy.Fingerprint
	original.LegacyOCRFingerprint = legacy.Fingerprint
	result, err = c.AcceptExistingPDF(ctx, original, true)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptNeedsSource, result.Outcome, "parser identities cannot satisfy an original-byte lookup")
	legacyOCR := jobsRequest("")
	legacyOCR.Collection = legacy.Collection
	legacyOCR.Document.TextSource = "ocr_pending"
	legacyOCR.Document.S3Key = req.Document.S3Key
	legacyOCR.Fingerprint = same.LegacyOCRFingerprint
	oldOCR, err := c.AcceptDocument(ctx, legacyOCR)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, oldOCR.Outcome)
	same.Fingerprint = chunker.ScopedFingerprint(same.Collection, "legacy OCR's new profile")
	result, err = c.AcceptDocument(ctx, same)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptInProgress, result.Outcome)
	require.Equal(t, oldOCR.DocumentID, result.DocumentID)
}

func TestLayoutPDFProfileAdmissionIsolationAndCheckpoint(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()

	mixed := mixedPDFRequest()
	mixedRes, err := c.AcceptDocument(ctx, mixed)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, mixedRes.Outcome)

	key := uuid.NewString() + "/layout.pdf"
	layoutReq := jobsRequest("")
	layoutReq.Document.S3Key = &key
	layoutReq.RequireSource = true
	layoutReq.PDFProfile = domain.LayoutPDFProfile
	layoutReq.Fingerprint = chunker.ScopedFingerprint(layoutReq.Collection, domain.LayoutPDFProfile+"\x00layout-bytes")
	layoutReq.PDF = &domain.PDFExtraction{
		Profile:  domain.LayoutPDFProfile,
		Coverage: domain.PDFPending,
		Pages:    domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Digital page"}},
	}

	// Same UUID ingested under mixed profile conflicts under layout profile.
	conflictReq := layoutReq
	conflictReq.Document.ID = mixedRes.DocumentID
	conflictReq.CallerSupplied = true
	conflictRes, err := c.AcceptDocument(ctx, conflictReq)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptConflict, conflictRes.Outcome)

	// Anonymous upload under layout profile does not match mixed or legacy rows.
	layoutRes, err := c.AcceptDocument(ctx, layoutReq)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, layoutRes.Outcome)
	require.NotEqual(t, mixedRes.DocumentID, layoutRes.DocumentID)

	// Claim and checkpoint layout sections, then verify retry preserves sections.
	for range 2 {
		job, claimErr := c.ClaimPDFJob(ctx, "layout-worker", time.Minute, 5)
		require.NoError(t, claimErr)
		require.NotNil(t, job)
		if job.Document.ID == mixedRes.DocumentID {
			_, err = c.BeginPDFOCR(ctx, *job)
			require.NoError(t, err)
			require.NoError(t, c.StorePDFOCR(ctx, *job, domain.PDFResult{
				Text:     "Recovered\n\nRepeated archive header",
				Coverage: domain.PDFComplete,
				Pages:    []domain.PDFPageResult{{Number: 1, Source: "ocr", State: "text"}, {Number: 2, Source: "parser", State: "text"}},
			}))
			ok, compErr := c.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
			require.NoError(t, compErr)
			require.True(t, ok)
			continue
		}
		require.Equal(t, domain.LayoutPDFProfile, job.PDF.Profile)
		_, err = c.BeginPDFOCR(ctx, *job)
		require.NoError(t, err)
		checkpoint := domain.PDFResult{
			Text:     "Column: Value",
			Coverage: domain.PDFComplete,
			Pages: []domain.PDFPageResult{{
				Number:   1,
				Source:   "textract",
				State:    "text",
				Sections: []domain.LayoutSection{{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "Column: Value"}},
			}},
		}
		require.NoError(t, c.StorePDFOCR(ctx, *job, checkpoint))
		require.NoError(t, c.ReleaseJob(ctx, *job))

		reclaimed, claimErr := c.ClaimPDFJob(ctx, "layout-worker", time.Minute, 5)
		require.NoError(t, claimErr)
		require.NotNil(t, reclaimed)
		require.Len(t, reclaimed.PDF.PageResults, 1)
		require.Equal(t, checkpoint.Pages[0].Sections, reclaimed.PDF.PageResults[0].Sections)
	}
}
