package postgres

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// preChangeMixedClaimSQL preserves the exact pre-change claimGuardedJob statement
// from before migration 040 so tests prove compatibility with deployed binaries.
const preChangeMixedClaimSQL = `WITH claimed AS (
	UPDATE ingest_jobs j SET state='running',attempts=j.attempts+1,lease_owner=$1,lease_until=clock_timestamp()+make_interval(secs=>$2),
		pdf_claim_attempt=CASE WHEN $3 THEN j.attempts+1 END,lease_token=gen_random_uuid(),updated_at=now()
	FROM documents d WHERE j.document_id=d.id AND d.status='processing'
	AND j.document_id=(SELECT ready.document_id FROM ingest_jobs ready
		JOIN documents candidate ON candidate.id=ready.document_id AND candidate.status='processing'
		WHERE ($3 OR NOT EXISTS(SELECT 1 FROM ingest_jobs WHERE pdf_required))
		AND ((ready.state='pending' AND ready.next_attempt_at<=(SELECT clock_timestamp()))
			OR (ready.state='running' AND ready.lease_until<=(SELECT clock_timestamp())))
		AND (candidate.vector_mutation_until IS NULL OR candidate.vector_mutation_until<=clock_timestamp())
		AND evaluation_fixture_claimable(ready.document_id, NULLIF($4::text, ''))
		AND NOT document_deletion_reserved(ready.document_id)
		ORDER BY ready.next_attempt_at,ready.created_at FOR UPDATE OF ready SKIP LOCKED LIMIT 1)
	RETURNING j.document_id,j.attempts,j.lease_token)
	SELECT d.id::text,j.attempts,d.content,d.metadata,d.file_name,d.file_type,d.s3_key,d.doc_type,d.fingerprint,d.collection,
		d.text_source,COALESCE(d.incarnation,d.id)::text,j.lease_token::text,d.scoped_vectors,p.profile,p.coverage,p.manifest,p.page_results,
		d.grant_catalog_assigned
	FROM claimed j JOIN documents d ON d.id=j.document_id
	LEFT JOIN document_pdf_extractions p ON p.document_id=d.id`

func layoutPDFRequest(collection, tag string) domain.AcceptRequest {
	key := uuid.NewString() + "/layout.pdf"
	req := jobsRequest("")
	req.Collection = collection
	req.Document.S3Key = &key
	req.RequireSource = true
	req.PDFProfile = domain.LayoutPDFProfile
	req.Fingerprint = chunker.ScopedFingerprint(collection, domain.LayoutPDFProfile+"\x00"+tag)
	req.LegacyOCRFingerprint = chunker.ScopedFingerprint(collection, tag)
	req.PDF = &domain.PDFExtraction{
		Profile:  domain.LayoutPDFProfile,
		Coverage: domain.PDFPending,
		Pages:    domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Embedded digital page"}},
	}
	return req
}

func claimLayoutForCorpusTest(ctx context.Context, c *Client, owner string, maxAttempts int) (*domain.Job, error) {
	return c.ClaimPDFProfilesForFixtureStore(ctx, owner, time.Minute, maxAttempts, "", false, true)
}

func seedLayoutDocumentPre040(t *testing.T, c *Client, collection, tag string) domain.DocumentID {
	t.Helper()
	req := layoutPDFRequest(collection, tag)
	seedLegacyLayoutRequest(t.Context(), t, c, req)
	return req.Document.ID
}

func seedLegacyLayoutRequest(ctx context.Context, t *testing.T, c *Client, req domain.AcceptRequest) {
	t.Helper()
	require.NoError(t, req.PDF.Pages.Validate())
	manifest, err := json.Marshal(req.PDF.Pages)
	require.NoError(t, err)
	id := string(req.Document.ID)
	_, err = c.Pool.Exec(ctx, `INSERT INTO documents(id,content,content_preview,metadata,chunk_count,file_name,file_type,s3_key,doc_type,status,fingerprint,collection,content_dedup)
		VALUES($1,'','','{}',0,'layout.pdf','pdf',$2,'document','processing',$3,$4,'collection')`,
		id, *req.Document.S3Key, req.Fingerprint, req.Collection)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `INSERT INTO document_fingerprints(fingerprint,document_id) VALUES($1,$2)`, req.Fingerprint, id)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `INSERT INTO document_pdf_extractions(document_id,profile,manifest,coverage) VALUES($1,$2,$3,'pending')`,
		id, domain.LayoutPDFProfile, manifest)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `INSERT INTO ingest_jobs(document_id,pdf_required) VALUES($1,TRUE)`, id)
	require.NoError(t, err)
}

func failCheckpointedLayout(ctx context.Context, t *testing.T, c *Client, job *domain.Job, coverage domain.PDFCoverage) {
	t.Helper()
	storeLayoutCheckpoint(ctx, t, c, job, coverage)
	require.NoError(t, c.FailJob(ctx, *job, "indexing failed"))
}

func storeLayoutCheckpoint(ctx context.Context, t *testing.T, c *Client, job *domain.Job, coverage domain.PDFCoverage) {
	t.Helper()
	const checkpoint = "Layout text saved before indexing failed"
	state := "text"
	if coverage == domain.PDFPartial {
		state = "partial"
	}
	_, err := c.BeginPDFOCR(ctx, *job)
	require.NoError(t, err)
	require.NoError(t, c.StorePDFOCR(ctx, *job, domain.PDFResult{
		Text:     checkpoint,
		Coverage: coverage,
		Pages: []domain.PDFPageResult{{
			Number: 1, Source: "textract", State: state,
			Sections: []domain.LayoutSection{{Page: 1, Text: checkpoint}},
		}},
	}))
}

// legacyRequeueLayoutCheckpoint uses the old explicit UUID retry writes for
// the matching profile and fingerprint when a usable checkpoint can be reused.
func legacyRequeueLayoutCheckpoint(ctx context.Context, tx pgx.Tx, req domain.AcceptRequest) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
		return err
	}
	var id string
	if err := tx.QueryRow(ctx, `SELECT d.id::text FROM documents d JOIN document_pdf_extractions p ON p.document_id=d.id
		WHERE d.id=$1 AND d.status='failed' AND d.fingerprint=$2 AND p.profile=$3
		AND p.coverage IN ('partial','complete') AND octet_length(d.content)>=$4 FOR UPDATE OF d`,
		req.Document.ID, req.Fingerprint, req.PDFProfile, domain.MinPDFTextBytes).Scan(&id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE documents SET status='processing',last_error=NULL,index_visibility=NULL,index_visibility_observed_at=NULL,updated_at=NOW() WHERE id=$1`, id); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO ingest_jobs(document_id) VALUES($1) ON CONFLICT DO NOTHING`, id)
	return err
}

func snapshotJobAndDoc(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID) (jobJSON, docJSON, pdfJSON string) {
	t.Helper()
	err := c.Pool.QueryRow(ctx, `SELECT to_jsonb(j)::text, to_jsonb(d)::text, COALESCE(to_jsonb(p)::text, '')
		FROM ingest_jobs j JOIN documents d ON d.id=j.document_id
		LEFT JOIN document_pdf_extractions p ON p.document_id=d.id
		WHERE j.document_id=$1`, string(id)).Scan(&jobJSON, &docJSON, &pdfJSON)
	require.NoError(t, err)
	return jobJSON, docJSON, pdfJSON
}

func TestLayoutClaimGuardMigrationWithOutstandingLeases(t *testing.T) {
	t.Run("live layout lease aborts migration without partial DDL and released lease allows it", func(t *testing.T) {
		ctx := t.Context()
		c := migrationTestClient(t, startTestDatabase(t))
		applyMigrationPrefix(t, c, 30)

		layoutID := seedLayoutDocumentPre040(t, c, "", "live-layout")
		pendingLayoutID := seedLayoutDocumentPre040(t, c, "", "pending-layout")
		failedLayoutID := seedLayoutDocumentPre040(t, c, "", "failed-layout")
		_, err := c.Pool.Exec(ctx, `DELETE FROM ingest_jobs WHERE document_id=$1`, string(failedLayoutID))
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, `UPDATE documents SET status='failed' WHERE id=$1`, string(failedLayoutID))
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, `UPDATE document_pdf_extractions SET coverage='failed' WHERE document_id=$1`, string(failedLayoutID))
		require.NoError(t, err)

		mixedReq := mixedPDFRequest()
		mixedRes, err := c.AcceptDocument(ctx, mixedReq)
		require.NoError(t, err)

		// Order ready timestamps deterministically so layoutID is claimed first, mixedRes second,
		// and pendingLayoutID remains pending.
		_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=now()-interval '20 seconds' WHERE document_id=$1`, string(layoutID))
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=now()-interval '10 seconds' WHERE document_id=$1`, string(mixedRes.DocumentID))
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=now()+interval '10 seconds' WHERE document_id=$1`, string(pendingLayoutID))
		require.NoError(t, err)

		liveLayout, err := c.claimJob(ctx, "old-layout-holder", 5, preChangeMixedClaimSQL, "old-layout-holder", 60.0, true, "")
		require.NoError(t, err)
		require.NotNil(t, liveLayout)
		require.Equal(t, layoutID, liveLayout.Document.ID)

		liveMixed, err := c.claimJob(ctx, "old-mixed", 5, preChangeMixedClaimSQL, "old-mixed", 60.0, true, "")
		require.NoError(t, err)
		require.NotNil(t, liveMixed)
		require.Equal(t, mixedRes.DocumentID, liveMixed.Document.ID)

		_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=now()-interval '1 second' WHERE document_id=$1`, string(pendingLayoutID))
		require.NoError(t, err)

		beforeJob, beforeDoc, beforePDF := snapshotJobAndDoc(ctx, t, c, layoutID)

		err = c.RunMigrations(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "drain")
		requireSQLState(t, err, "55000")

		var hasCol, hasTrigger, hasMig40 bool
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT
			EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='ingest_jobs' AND column_name='layout_claim_attempt'),
			EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='ingest_jobs_layout_capable_claim'),
			EXISTS(SELECT 1 FROM schema_migrations WHERE version=40)`).Scan(&hasCol, &hasTrigger, &hasMig40))
		require.False(t, hasCol)
		require.False(t, hasTrigger)
		require.False(t, hasMig40)

		afterJob, afterDoc, afterPDF := snapshotJobAndDoc(ctx, t, c, layoutID)
		require.Equal(t, beforeJob, afterJob)
		require.Equal(t, beforeDoc, afterDoc)
		require.Equal(t, beforePDF, afterPDF)

		// Releasing the live layout lease allows migration 040 to proceed (even with pending/failed layout and live mixed leases).
		require.NoError(t, c.ReleaseJob(ctx, *liveLayout))
		require.NoError(t, c.RunMigrations(ctx))
		requireRecordedMigrations(t, c)
	})

	t.Run("expired layout lease allows migration and blocks old token operations", func(t *testing.T) {
		ctx := t.Context()
		c := migrationTestClient(t, startTestDatabase(t))
		applyMigrationPrefix(t, c, 30)

		layoutID := seedLayoutDocumentPre040(t, c, "", "expired-layout")
		oldJob, err := c.claimJob(ctx, "pre040-worker", 5, preChangeMixedClaimSQL, "pre040-worker", 60.0, true, "")
		require.NoError(t, err)
		require.NotNil(t, oldJob)
		require.Equal(t, layoutID, oldJob.Document.ID)

		_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(layoutID))
		require.NoError(t, err)

		require.NoError(t, c.RunMigrations(ctx))
		requireRecordedMigrations(t, c)

		var layoutAttempt *int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT layout_claim_attempt FROM ingest_jobs WHERE document_id=$1`, string(layoutID)).Scan(&layoutAttempt))
		require.Nil(t, layoutAttempt)

		// Old token cannot heartbeat, checkpoint, complete, retry, release, or fail after expiry.
		renewed, err := c.HeartbeatJob(ctx, *oldJob, time.Minute)
		require.NoError(t, err)
		require.False(t, renewed)
		_, err = c.BeginPDFOCR(ctx, *oldJob)
		require.ErrorIs(t, err, domain.ErrJobNotOwned)
		require.ErrorIs(t, c.RecordPDFOCRStart(ctx, *oldJob, uuid.NewString()), domain.ErrJobNotOwned)
		require.ErrorIs(t, c.StorePDFOCR(ctx, *oldJob, domain.PDFResult{
			Text:     "Stale layout text",
			Coverage: domain.PDFComplete,
			Pages:    []domain.PDFPageResult{{Number: 1, Source: "textract", State: "text"}},
		}), domain.ErrJobNotOwned)
		completed, err := c.CompleteJob(ctx, *oldJob, nil, domain.IndexObservation{})
		require.NoError(t, err)
		require.False(t, completed)
		require.ErrorIs(t, c.RetryJob(ctx, *oldJob, time.Second, "stale"), domain.ErrJobNotOwned)
		require.ErrorIs(t, c.ReleaseJob(ctx, *oldJob), domain.ErrJobNotOwned)
		require.ErrorIs(t, c.FailJob(ctx, *oldJob, "stale"), domain.ErrJobNotOwned)

		// Reclaiming with an analyzer stamps layout_claim_attempt and takes over cleanly.
		reclaimed, err := c.ClaimPDFProfilesForFixtureStore(ctx, "new-analyzer", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, reclaimed)
		require.Equal(t, 2, reclaimed.Attempts)
		require.NotEqual(t, oldJob.LeaseToken, reclaimed.LeaseToken)
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT layout_claim_attempt FROM ingest_jobs WHERE document_id=$1`, string(layoutID)).Scan(&layoutAttempt))
		require.NotNil(t, layoutAttempt)
		require.Equal(t, 2, *layoutAttempt)

		// Old token still rejected after takeover.
		_, err = c.BeginPDFOCR(ctx, *oldJob)
		require.ErrorIs(t, err, domain.ErrJobNotOwned)
	})
}

func TestOldMixedClaimCannotLeaseLayout(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()

	// 1. Pending layout job cannot be claimed by old mixed SQL.
	pendingReq := layoutPDFRequest("", "pending-layout")
	pendingRes, err := serving.AcceptDocument(ctx, pendingReq)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, pendingRes.Outcome)

	beforeJob, beforeDoc, beforePDF := snapshotJobAndDoc(ctx, t, owner, pendingRes.DocumentID)
	got, err := serving.claimJob(ctx, "old-mixed-idle", 5, preChangeMixedClaimSQL, "old-mixed-idle", 60.0, true, "")
	require.NoError(t, err)
	require.Nil(t, got)
	afterJob, afterDoc, afterPDF := snapshotJobAndDoc(ctx, t, owner, pendingRes.DocumentID)
	require.Equal(t, beforeJob, afterJob)
	require.Equal(t, beforeDoc, afterDoc)
	require.Equal(t, beforePDF, afterPDF)

	// New analyzer claims pending layout job immediately.
	analyzerJob, err := serving.ClaimPDFProfilesForFixtureStore(ctx, "new-analyzer", time.Minute, 5, "", false, true)
	require.NoError(t, err)
	require.NotNil(t, analyzerJob)
	require.Equal(t, pendingRes.DocumentID, analyzerJob.Document.ID)

	// Checkpoint partial layout text, fail, and requeue (coverage='partial', pdf_required=TRUE).
	_, err = serving.BeginPDFOCR(ctx, *analyzerJob)
	require.NoError(t, err)
	require.NoError(t, serving.StorePDFOCR(ctx, *analyzerJob, domain.PDFResult{
		Text:     "Checkpointed partial layout text",
		Coverage: domain.PDFPartial,
		Pages:    []domain.PDFPageResult{{Number: 1, Source: "textract", State: "partial", Sections: []domain.LayoutSection{{Page: 1, Text: "Checkpointed partial layout text"}}}},
	}))
	require.NoError(t, serving.FailJob(ctx, *analyzerJob, "embedding failure"))

	retryReq := pendingReq
	retryReq.CallerSupplied = true
	retryReq.Document.S3Key = nil
	requeued, err := serving.AcceptDocument(ctx, retryReq)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptRequeued, requeued.Outcome)

	// 2. Checkpointed layout job cannot be claimed by old mixed SQL.
	beforeJob, beforeDoc, beforePDF = snapshotJobAndDoc(ctx, t, owner, pendingRes.DocumentID)
	got, err = serving.claimJob(ctx, "old-mixed-checkpointed", 5, preChangeMixedClaimSQL, "old-mixed-checkpointed", 60.0, true, "")
	require.NoError(t, err)
	require.Nil(t, got)
	afterJob, afterDoc, afterPDF = snapshotJobAndDoc(ctx, t, owner, pendingRes.DocumentID)
	require.Equal(t, beforeJob, afterJob)
	require.Equal(t, beforeDoc, afterDoc)
	require.Equal(t, beforePDF, afterPDF)

	// Analyzer claims checkpointed layout job and sees saved PageResults, then its lease expires.
	reclaimed, err := serving.ClaimPDFProfilesForFixtureStore(ctx, "new-analyzer-2", time.Minute, 5, "", false, true)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	require.Len(t, reclaimed.PDF.PageResults, 1)
	_, err = owner.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(pendingRes.DocumentID))
	require.NoError(t, err)

	// 3. Expired layout job cannot be reclaimed by old mixed SQL.
	beforeJob, beforeDoc, beforePDF = snapshotJobAndDoc(ctx, t, owner, pendingRes.DocumentID)
	got, err = serving.claimJob(ctx, "old-mixed-expired", 5, preChangeMixedClaimSQL, "old-mixed-expired", 60.0, true, "")
	require.NoError(t, err)
	require.Nil(t, got)
	afterJob, afterDoc, afterPDF = snapshotJobAndDoc(ctx, t, owner, pendingRes.DocumentID)
	require.Equal(t, beforeJob, afterJob)
	require.Equal(t, beforeDoc, afterDoc)
	require.Equal(t, beforePDF, afterPDF)

	// Also place a live ordinary/mixed lease and an expired mixed lease alongside the expired layout job.
	ordID := jobsAccept(t, serving, "live ordinary job")
	ordJob, err := serving.ClaimPDFJob(ctx, "mixed-live", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, ordJob)
	require.Equal(t, ordID, ordJob.Document.ID)

	mixedRes, err := serving.AcceptDocument(ctx, mixedPDFRequest())
	require.NoError(t, err)
	_, err = owner.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=clock_timestamp()-interval '1 minute' WHERE document_id=$1`, string(mixedRes.DocumentID))
	require.NoError(t, err)
	expMixed, err := serving.claimJob(ctx, "old-mixed", 5, preChangeMixedClaimSQL, "old-mixed", 60.0, true, "")
	require.NoError(t, err)
	require.NotNil(t, expMixed)
	require.Equal(t, mixedRes.DocumentID, expMixed.Document.ID)
	_, err = owner.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(mixedRes.DocumentID))
	require.NoError(t, err)

	// Old SQL reclaims the expired mixed job, still never touching the expired layout job.
	reclaimedMixed, err := serving.claimJob(ctx, "old-mixed-reclaim", 5, preChangeMixedClaimSQL, "old-mixed-reclaim", 60.0, true, "")
	require.NoError(t, err)
	require.NotNil(t, reclaimedMixed)
	require.Equal(t, mixedRes.DocumentID, reclaimedMixed.Document.ID)
	require.Equal(t, 2, reclaimedMixed.Attempts)
	afterJob, afterDoc, afterPDF = snapshotJobAndDoc(ctx, t, owner, pendingRes.DocumentID)
	require.Equal(t, beforeJob, afterJob)
	require.Equal(t, beforeDoc, afterDoc)
	require.Equal(t, beforePDF, afterPDF)

	// New analyzer claims the expired layout job immediately.
	analyzerReclaimed, err := serving.ClaimPDFProfilesForFixtureStore(ctx, "new-analyzer-3", time.Minute, 5, "", false, true)
	require.NoError(t, err)
	require.NotNil(t, analyzerReclaimed)
	require.Equal(t, pendingRes.DocumentID, analyzerReclaimed.Document.ID)
	require.Equal(t, 2, analyzerReclaimed.Attempts)
}

func TestHistoricalLayoutCheckpointClaims(t *testing.T) {
	for _, coverage := range []domain.PDFCoverage{domain.PDFPartial, domain.PDFComplete} {
		t.Run(string(coverage), func(t *testing.T) {
			ctx := t.Context()
			owner := migrationTestClient(t, startTestDatabase(t))
			applyMigrationPrefix(t, owner, 39)

			seedFailed := func(tag string) domain.AcceptRequest {
				req := layoutPDFRequest("", tag)
				seedLegacyLayoutRequest(ctx, t, owner, req)
				job, err := owner.claimJob(ctx, "old-layout", 5, preChangeMixedClaimSQL, "old-layout", 60.0, true, "")
				require.NoError(t, err)
				require.NotNil(t, job)
				require.Equal(t, req.Document.ID, job.Document.ID)
				require.NotNil(t, job.PDF)
				require.Equal(t, req.PDF.Pages, job.PDF.Pages)
				failCheckpointedLayout(ctx, t, owner, job, coverage)
				return req
			}
			requeue := func(c *Client, req domain.AcceptRequest) {
				tx, err := c.Pool.Begin(ctx)
				require.NoError(t, err)
				defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
				require.NoError(t, legacyRequeueLayoutCheckpoint(ctx, tx, req))
				require.NoError(t, tx.Commit(ctx))
			}
			expire := func(id domain.DocumentID) {
				tag, err := owner.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp()-interval '1 second'
					WHERE document_id=$1 AND state='running'`, id)
				require.NoError(t, err)
				require.EqualValues(t, 1, tag.RowsAffected())
			}

			beforePending := seedFailed("pre-migration pending retry")
			beforeExpired := seedFailed("pre-migration expired retry")
			afterPending := seedFailed("post-migration pending retry")
			afterExpired := seedFailed("post-migration expired retry")
			requeue(owner, beforeExpired)
			oldLease, err := owner.claimJob(ctx, "old-layout-retry", 5, preChangeMixedClaimSQL, "old-layout-retry", 60.0, true, "")
			require.NoError(t, err)
			require.NotNil(t, oldLease)
			require.Equal(t, beforeExpired.Document.ID, oldLease.Document.ID)
			expire(beforeExpired.Document.ID)
			_, err = owner.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=clock_timestamp()-interval '1 minute' WHERE document_id=$1`, beforeExpired.Document.ID)
			require.NoError(t, err)
			requeue(owner, beforePending)

			require.NoError(t, owner.RunMigrations(ctx))
			serving, _ := publishedServingClient(t, owner)

			verify := func(req domain.AcceptRequest, priorLease *domain.Job) {
				id := req.Document.ID
				var required bool
				require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT pdf_required FROM ingest_jobs WHERE document_id=$1`, id).Scan(&required))
				require.False(t, required, "the legacy retry must exercise the false-flag path")
				beforeJob, beforeDoc, beforePDF := snapshotJobAndDoc(ctx, t, owner, id)
				for _, mixed := range []bool{true, false} {
					job, claimErr := serving.claimJob(ctx, "old-incompatible", 5, preChangeMixedClaimSQL, "old-incompatible", 60.0, mixed, "")
					require.NoError(t, claimErr)
					require.Nil(t, job)

					job, claimErr = serving.ClaimPDFProfilesForFixtureStore(ctx, "current-incompatible", time.Minute, 5, "", mixed, false)
					require.NoError(t, claimErr)
					require.Nil(t, job)

					afterJob, afterDoc, afterPDF := snapshotJobAndDoc(ctx, t, owner, id)
					require.Equal(t, beforeJob, afterJob)
					require.Equal(t, beforeDoc, afterDoc)
					require.Equal(t, beforePDF, afterPDF)
				}

				analyzer, claimErr := serving.ClaimPDFProfilesForFixtureStore(ctx, "restored-analyzer", time.Minute, 5, "", false, true)
				require.NoError(t, claimErr)
				require.NotNil(t, analyzer)
				require.Equal(t, id, analyzer.Document.ID)
				require.NotEmpty(t, analyzer.LeaseToken)
				require.NotNil(t, analyzer.PDF)
				require.Equal(t, coverage, analyzer.PDF.Coverage)
				require.Len(t, analyzer.PDF.PageResults, 1)
				require.Len(t, analyzer.PDF.PageResults[0].Sections, 1)
				require.NotEmpty(t, analyzer.Document.Content)
				require.Equal(t, req.Document.S3Key, analyzer.Document.S3Key)
				var stamp int
				require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT layout_claim_attempt FROM ingest_jobs WHERE document_id=$1`, id).Scan(&stamp))
				require.Equal(t, analyzer.Attempts, stamp)
				if priorLease != nil {
					require.Equal(t, priorLease.Attempts+1, analyzer.Attempts)
					require.NotEqual(t, priorLease.LeaseToken, analyzer.LeaseToken)
					done, completeErr := serving.CompleteJob(ctx, *priorLease, nil, domain.IndexObservation{})
					require.NoError(t, completeErr)
					require.False(t, done)
				} else {
					require.Equal(t, 1, analyzer.Attempts)
				}
				done, completeErr := serving.CompleteJob(ctx, *analyzer, nil, domain.IndexObservation{})
				require.NoError(t, completeErr)
				require.True(t, done)
			}

			verify(beforeExpired, oldLease)
			verify(beforePending, nil)
			requeue(serving, afterPending)
			verify(afterPending, nil)
			requeue(serving, afterExpired)
			lease, err := serving.ClaimPDFProfilesForFixtureStore(ctx, "analyzer-before-expiry", time.Minute, 5, "", false, true)
			require.NoError(t, err)
			require.NotNil(t, lease)
			require.Equal(t, afterExpired.Document.ID, lease.Document.ID)
			expire(afterExpired.Document.ID)
			verify(afterExpired, lease)
		})
	}
}

func TestHistoricalPendingLayoutClaimAllowsOCR(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	id := seedLayoutDocumentPre040(t, owner, "", "historical pending")
	_, err := owner.Pool.Exec(ctx, `UPDATE ingest_jobs SET pdf_required=FALSE WHERE document_id=$1`, id)
	require.NoError(t, err)

	job, err := serving.ClaimPDFProfilesForFixtureStore(ctx, "layout-ocr", time.Minute, 5, "", false, true)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, id, job.Document.ID)
	var pdfRequired bool
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT pdf_required FROM ingest_jobs WHERE document_id=$1`, id).Scan(&pdfRequired))
	require.True(t, pdfRequired)
	failCheckpointedLayout(ctx, t, serving, job, domain.PDFComplete)
}

func TestProfileClaimsSkipIncompatibleHead(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()

	// Seed a queue where layout sits at the very head, followed by mixed, plain, unknown profile, and inconsistent pdf_required row.
	layoutHead, err := c.AcceptDocument(ctx, layoutPDFRequest("col-a", "head-layout"))
	require.NoError(t, err)
	mixedReq := mixedPDFRequest()
	mixedReq.Collection = "col-b"
	mixedReq.Fingerprint = chunker.ScopedFingerprint("col-b", "mixed-bytes")
	mixedRes, err := c.AcceptDocument(ctx, mixedReq)
	require.NoError(t, err)
	plainReq := jobsRequest("plain body")
	plainReq.Collection = "col-c"
	plainReq.Fingerprint = chunker.ScopedFingerprint("col-c", "plain body")
	plainRes, err := c.AcceptDocument(ctx, plainReq)
	require.NoError(t, err)

	unknownID := jobsAccept(t, c, "unknown profile doc")
	_, err = c.Pool.Exec(ctx, `INSERT INTO document_pdf_extractions(document_id,profile,manifest,coverage) VALUES($1,'quarry.pdf.future.v2','[]','pending')`, string(unknownID))
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET pdf_required=TRUE WHERE document_id=$1`, string(unknownID))
	require.NoError(t, err)

	inconsistentID := jobsAccept(t, c, "inconsistent pdf_required doc")
	_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET pdf_required=TRUE WHERE document_id=$1`, string(inconsistentID))
	require.NoError(t, err)

	base := time.Now().UTC().Add(-time.Minute)
	for i, id := range []domain.DocumentID{layoutHead.DocumentID, mixedRes.DocumentID, plainRes.DocumentID, unknownID, inconsistentID} {
		_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=$2 WHERE document_id=$1`, string(id), base.Add(time.Duration(i)*time.Second))
		require.NoError(t, err)
	}

	// "none" cannot claim while pdf_required jobs exist.
	noneJob, err := c.ClaimPDFProfilesForFixtureStore(ctx, "none", time.Minute, 5, "", false, false)
	require.NoError(t, err)
	require.Nil(t, noneJob)

	// "detector" skips layout head and claims ready mixed and plain jobs across collections.
	det1, err := c.ClaimPDFProfilesForFixtureStore(ctx, "detector", time.Minute, 5, "", true, false)
	require.NoError(t, err)
	require.NotNil(t, det1)
	require.Equal(t, mixedRes.DocumentID, det1.Document.ID)
	require.Equal(t, "col-b", det1.Collection)

	det2, err := c.ClaimPDFProfilesForFixtureStore(ctx, "detector", time.Minute, 5, "", true, false)
	require.NoError(t, err)
	require.NotNil(t, det2)
	require.Equal(t, plainRes.DocumentID, det2.Document.ID)
	require.Equal(t, "col-c", det2.Collection)

	// Detector cannot claim unknown or inconsistent rows or layout head.
	det3, err := c.ClaimPDFProfilesForFixtureStore(ctx, "detector", time.Minute, 5, "", true, false)
	require.NoError(t, err)
	require.Nil(t, det3)

	// "both" also skips unknown and inconsistent rows after releasing layout head or when layout is claimed.
	ana, err := claimLayoutForCorpusTest(ctx, c, "analyzer", 5)
	require.NoError(t, err)
	require.NotNil(t, ana)
	require.Equal(t, layoutHead.DocumentID, ana.Document.ID)
	bothSkip, err := c.ClaimPDFProfilesForFixtureStore(ctx, "both", time.Minute, 5, "", true, true)
	require.NoError(t, err)
	require.Nil(t, bothSkip)

	// Clean up unknown and inconsistent rows so remaining invariant checks run cleanly.
	require.NoError(t, c.DeleteDocument(ctx, unknownID))
	require.NoError(t, c.DeleteDocument(ctx, inconsistentID))
	done1, err := c.CompleteJob(ctx, *det1, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done1)
	done2, err := c.CompleteJob(ctx, *det2, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done2)
	require.NoError(t, c.ReleaseJob(ctx, *ana))

	// Checkpointed mixed job (pdf_required=false) can be claimed by analyzer and none (if no other pdf_required jobs exist).
	require.NoError(t, c.DeleteDocument(ctx, layoutHead.DocumentID))
	chkMixedReq := mixedPDFRequest()
	chkMixedReq.Document.Content = "Already extracted digital text"
	chkMixedReq.PDF.Coverage = domain.PDFComplete
	chkMixedReq.PDF.Pages = domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Already extracted digital text"}}
	chkMixed, err := c.AcceptDocument(ctx, chkMixedReq)
	require.NoError(t, err)
	noneMixed, err := c.ClaimPDFProfilesForFixtureStore(ctx, "none", time.Minute, 5, "", false, false)
	require.NoError(t, err)
	require.NotNil(t, noneMixed)
	require.Equal(t, chkMixed.DocumentID, noneMixed.Document.ID)
	require.NoError(t, c.ReleaseJob(ctx, *noneMixed))
	anaMixed, err := c.ClaimPDFProfilesForFixtureStore(ctx, "analyzer", time.Minute, 5, "", false, true)
	require.NoError(t, err)
	require.NotNil(t, anaMixed)
	require.Equal(t, chkMixed.DocumentID, anaMixed.Document.ID)
	require.NoError(t, c.DeleteDocument(ctx, chkMixed.DocumentID))

	// Preserve ready time ordering, fixture/store matching, deletion reservation, writer freeze, and max attempts.
	futureLayout, err := c.AcceptDocument(ctx, layoutPDFRequest("col", "future-layout"))
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE document_id=$1`, string(futureLayout.DocumentID))
	require.NoError(t, err)
	notReady, err := c.ClaimPDFProfilesForFixtureStore(ctx, "analyzer", time.Minute, 5, "", false, true)
	require.NoError(t, err)
	require.Nil(t, notReady)
	require.NoError(t, c.DeleteDocument(ctx, futureLayout.DocumentID))

	// Fixture store matching.
	fixtureID := fixtureShapedID()
	fixtureReq := layoutPDFRequest("eval", "fixture-layout")
	fixtureReq.Document.ID = fixtureID
	fixtureReq.CallerSupplied = true
	fixtureReq.EvaluationReceipt = uuid.NewString()
	fixtureReq.EvaluationVectorIdentity = "pinecone:bound"
	_, err = c.AcceptDocument(ctx, fixtureReq)
	require.NoError(t, err)
	wrongStore, err := c.ClaimPDFProfilesForFixtureStore(ctx, "wrong", time.Minute, 5, "postgres", false, true)
	require.NoError(t, err)
	require.Nil(t, wrongStore)
	rightStore, err := c.ClaimPDFProfilesForFixtureStore(ctx, "right", time.Minute, 5, "pinecone:bound", false, true)
	require.NoError(t, err)
	require.NotNil(t, rightStore)
	require.Equal(t, fixtureID, rightStore.Document.ID)
	_, err = c.DeleteEvaluationFixture(ctx, fixtureID, fixtureReq.EvaluationReceipt, rightStore.Incarnation, "pinecone:bound", func(context.Context) error { return nil })
	require.NoError(t, err)

	// Deletion reservation skips job.
	heldLayout, err := c.AcceptDocument(ctx, layoutPDFRequest("col", "held-layout"))
	require.NoError(t, err)
	for _, receipt := range []struct {
		kind, mode      string
		held, finalized bool
	}{
		{kind: "legacy_reservation", mode: "legacy", held: true},
		{kind: "qualified_intent", mode: "local"},
		{kind: "quarantine", mode: "local", held: true, finalized: true},
	} {
		t.Run(receipt.kind, func(t *testing.T) {
			tag, insertErr := c.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,held,finalized)
				SELECT id,COALESCE(incarnation,id),$2,$3,$4,$5 FROM documents WHERE id=$1`,
				heldLayout.DocumentID, receipt.kind, receipt.mode, receipt.held, receipt.finalized)
			require.NoError(t, insertErr)
			require.EqualValues(t, 1, tag.RowsAffected())
			heldClaim, claimErr := c.ClaimPDFProfilesForFixtureStore(ctx, "analyzer", time.Minute, 5, "", false, true)
			require.NoError(t, claimErr)
			require.Nil(t, heldClaim)
			_, removeErr := c.Pool.Exec(ctx, `DELETE FROM document_deletions WHERE document_id=$1`, heldLayout.DocumentID)
			require.NoError(t, removeErr)

			unheldClaim, claimErr := c.ClaimPDFProfilesForFixtureStore(ctx, "analyzer", time.Minute, 5, "", false, true)
			require.NoError(t, claimErr)
			require.NotNil(t, unheldClaim)
			require.Equal(t, heldLayout.DocumentID, unheldClaim.Document.ID)
			require.NoError(t, c.ReleaseJob(ctx, *unheldClaim))
		})
	}

	// Writer freeze blocks claims and unfreeze resumes.
	_, err = c.Pool.Exec(ctx, `UPDATE vector_state SET frozen=TRUE`)
	require.NoError(t, err)
	frozenClaim, err := c.ClaimPDFProfilesForFixtureStore(ctx, "analyzer", time.Minute, 5, "", false, true)
	require.NoError(t, err)
	require.Nil(t, frozenClaim)
	_, err = c.Pool.Exec(ctx, `UPDATE vector_state SET frozen=FALSE`)
	require.NoError(t, err)
	unfrozen, err := c.ClaimPDFProfilesForFixtureStore(ctx, "analyzer", time.Minute, 5, "", false, true)
	require.NoError(t, err)
	require.NotNil(t, unfrozen)
	require.Equal(t, heldLayout.DocumentID, unfrozen.Document.ID)
	require.NoError(t, c.ReleaseJob(ctx, *unfrozen))

	// Max attempts fails the exhausted job cleanly.
	_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET attempts=5 WHERE document_id=$1`, string(heldLayout.DocumentID))
	require.NoError(t, err)
	exhaustedClaim, err := c.ClaimPDFProfilesForFixtureStore(ctx, "analyzer", time.Minute, 5, "", false, true)
	require.NoError(t, err)
	require.Nil(t, exhaustedClaim)
	summary, err := c.GetDocumentSummary(ctx, heldLayout.DocumentID)
	require.NoError(t, err)
	require.Equal(t, "failed", summary.Status)
	require.Equal(t, domain.PDFFailed, summary.PDFCoverage)
}

func TestProfileLeaseAndAdmissionRace(t *testing.T) {
	c, serving, _ := restrictedDatabase(t)

	for _, transition := range []string{"admission", "explicit retry", "legacy checkpoint retry"} {
		for _, admissionFirst := range []bool{true, false} {
			order := "claim first"
			if admissionFirst {
				order = "admission first"
			}
			t.Run(transition+"/"+order, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				req := layoutPDFRequest("race", t.Name())
				t.Cleanup(func() {
					_, err := c.Pool.Exec(context.WithoutCancel(t.Context()), `DELETE FROM documents WHERE id=$1`, req.Document.ID)
					require.NoError(t, err)
				})
				want := domain.AcceptAccepted
				if transition != "admission" {
					accepted, err := serving.AcceptDocument(ctx, req)
					require.NoError(t, err)
					require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
					job, err := serving.ClaimPDFProfilesForFixtureStore(ctx, "checkpoint", time.Minute, 5, "", false, true)
					require.NoError(t, err)
					require.NotNil(t, job)
					failCheckpointedLayout(ctx, t, serving, job, domain.PDFPartial)
					req.CallerSupplied = true
					req.Document.S3Key = nil
					want = domain.AcceptRequeued
				}
				legacy := transition == "legacy checkpoint retry"
				admit := func() (domain.AcceptOutcome, error) {
					if !legacy {
						result, err := serving.AcceptDocument(ctx, req)
						return result.Outcome, err
					}
					tx, err := serving.Pool.Begin(ctx)
					if err != nil {
						return 0, err
					}
					defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
					if err := legacyRequeueLayoutCheckpoint(ctx, tx, req); err != nil {
						return 0, err
					}
					return domain.AcceptRequeued, tx.Commit(ctx)
				}
				type admissionResult struct {
					outcome domain.AcceptOutcome
					err     error
				}
				startAdmission := func() <-chan admissionResult {
					done := make(chan admissionResult, 1)
					go func() {
						outcome, err := admit()
						done <- admissionResult{outcome, err}
					}()
					return done
				}
				waitForQueueLock := func(mode string) {
					require.Eventually(t, func() bool {
						var blocked bool
						err := c.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT FROM pg_locks
							WHERE relation='ingest_jobs'::regclass AND NOT granted AND mode=$1
							AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, mode).Scan(&blocked)
						return err == nil && blocked
					}, 5*time.Second, 10*time.Millisecond, "expected a waiting %s on ingest_jobs", mode)
				}
				barrier, err := serving.Pool.Begin(ctx)
				require.NoError(t, err)
				defer func() { _ = barrier.Rollback(context.WithoutCancel(ctx)) }()
				_, err = barrier.Exec(ctx, `LOCK TABLE ingest_jobs IN SHARE ROW EXCLUSIVE MODE`)
				require.NoError(t, err)

				if admissionFirst {
					var admitted <-chan admissionResult
					if legacy {
						// Hold the legacy retry open after its false-flag insert.
						require.NoError(t, legacyRequeueLayoutCheckpoint(ctx, barrier, req))
					} else {
						// Queue the admission's lock request ahead of the old claim.
						admitted = startAdmission()
						waitForQueueLock("ShareRowExclusiveLock")
					}
					type claimResult struct {
						job *domain.Job
						err error
					}
					claimed := make(chan claimResult, 1)
					go func() {
						job, claimErr := serving.claimJob(ctx, "old-racer", 5, preChangeMixedClaimSQL, "old-racer", 60.0, true, "")
						claimed <- claimResult{job, claimErr}
					}()
					waitForQueueLock("RowExclusiveLock")
					require.NoError(t, barrier.Commit(ctx))
					if admitted != nil {
						result := <-admitted
						require.NoError(t, result.err)
						require.Equal(t, want, result.outcome)
					}
					result := <-claimed
					require.NoError(t, result.err)
					require.Nil(t, result.job)
				} else {
					// Keep the old claim transaction open until admission is waiting to write.
					tag, claimErr := barrier.Exec(ctx, preChangeMixedClaimSQL, "old-racer", 60.0, true, "")
					require.NoError(t, claimErr)
					require.Zero(t, tag.RowsAffected())
					admitted := startAdmission()
					mode := "ShareRowExclusiveLock"
					if legacy {
						mode = "RowExclusiveLock"
					}
					waitForQueueLock(mode)
					require.NoError(t, barrier.Commit(ctx))
					result := <-admitted
					require.NoError(t, result.err)
					require.Equal(t, want, result.outcome)
				}

				// A later claim from the same old statement must also leave the job intact.
				var required bool
				require.NoError(t, c.Pool.QueryRow(ctx, `SELECT pdf_required FROM ingest_jobs WHERE document_id=$1`, req.Document.ID).Scan(&required))
				require.Equal(t, !legacy, required)
				beforeJob, beforeDoc, beforePDF := snapshotJobAndDoc(ctx, t, c, req.Document.ID)
				job, err := serving.claimJob(ctx, "old-racer", 5, preChangeMixedClaimSQL, "old-racer", 60.0, true, "")
				require.NoError(t, err)
				require.Nil(t, job)
				afterJob, afterDoc, afterPDF := snapshotJobAndDoc(ctx, t, c, req.Document.ID)
				require.Equal(t, beforeJob, afterJob)
				require.Equal(t, beforeDoc, afterDoc)
				require.Equal(t, beforePDF, afterPDF)
			})
		}
	}

	ctx := t.Context()
	t.Run("competing profile claims have a single compatible winner", func(t *testing.T) {
		accepted, err := c.AcceptDocument(ctx, layoutPDFRequest("", "concurrent profile claims"))
		require.NoError(t, err)
		var workers sync.WaitGroup
		start := make(chan struct{})
		claimed := make(chan *domain.Job, 6)
		for _, caps := range []struct {
			owner      string
			mixed, lay bool
		}{
			{"det-1", true, false},
			{"det-2", true, false},
			{"none-1", false, false},
			{"ana-1", false, true},
			{"both-1", true, true},
			{"ana-2", false, true},
		} {
			workers.Go(func() {
				<-start
				job, claimErr := c.ClaimPDFProfilesForFixtureStore(ctx, caps.owner, time.Minute, 5, "", caps.mixed, caps.lay)
				if claimErr != nil {
					t.Error(claimErr)
				}
				claimed <- job
			})
		}
		close(start)
		workers.Wait()
		close(claimed)
		var winner *domain.Job
		for job := range claimed {
			if job != nil {
				require.Nil(t, winner)
				winner = job
			}
		}
		require.NotNil(t, winner)
		require.Equal(t, accepted.DocumentID, winner.Document.ID)
		require.Contains(t, []string{"ana-1", "ana-2", "both-1"}, winner.LeaseOwner)
		require.NoError(t, c.DeleteDocument(ctx, accepted.DocumentID))
	})
	// 2. Context cancellation while acquiring migration lock, writer lock, and queued job lock.
	t.Run("cancellation on migration lock, writer lock, and queued row lock", func(t *testing.T) {
		subCtx := t.Context()
		migClient := migrationTestClient(t, startTestDatabase(t))
		applyMigrationPrefix(t, migClient, 39)
		lockTx, err := migClient.Pool.Begin(subCtx)
		require.NoError(t, err)
		defer func() { _ = lockTx.Rollback(context.WithoutCancel(subCtx)) }()
		_, err = lockTx.Exec(subCtx, `LOCK TABLE ingest_jobs IN ACCESS SHARE MODE`)
		require.NoError(t, err)
		var migrationBlocker int32
		require.NoError(t, lockTx.QueryRow(subCtx, `SELECT pg_backend_pid()`).Scan(&migrationBlocker))

		cancelMigCtx, cancelMig := context.WithCancel(subCtx)
		migDone := make(chan struct{})
		var migrationErr error
		go func() {
			defer close(migDone)
			migrationErr = migClient.RunMigrations(cancelMigCtx)
		}()
		defer func() {
			cancelMig()
			<-migDone
		}()
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			var waiting bool
			qErr := migClient.Pool.QueryRow(subCtx, `SELECT EXISTS(SELECT FROM pg_locks
				WHERE locktype='relation' AND NOT granted AND relation='ingest_jobs'::regclass
				AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
				AND $1=ANY(pg_blocking_pids(pid)))`, migrationBlocker).Scan(&waiting)
			assert.NoError(collect, qErr)
			assert.True(collect, waiting)
		}, 3*time.Second, 10*time.Millisecond)
		cancelMig()
		<-migDone
		require.ErrorIs(t, migrationErr, context.Canceled)
		require.NoError(t, lockTx.Rollback(subCtx))
		require.NoError(t, migClient.RunMigrations(subCtx))

		// Writer/admission lock cancellation.
		writerLockTx, err := c.Pool.Begin(subCtx)
		require.NoError(t, err)
		defer func() { _ = writerLockTx.Rollback(context.WithoutCancel(subCtx)) }()
		_, err = writerLockTx.Exec(subCtx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock)
		require.NoError(t, err)
		canceledReq := layoutPDFRequest("cancel", "locked-admission")
		cancelAdmCtx, cancelAdm := context.WithCancel(subCtx)
		admDone := make(chan struct{})
		var admissionErr error
		go func() {
			defer close(admDone)
			_, admissionErr = c.AcceptDocument(cancelAdmCtx, canceledReq)
		}()
		defer func() {
			cancelAdm()
			<-admDone
		}()
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			waiting, qErr := hasAdvisoryWaiter(subCtx, c.Pool, ingestAdmissionLock)
			assert.NoError(collect, qErr)
			assert.True(collect, waiting)
		}, 3*time.Second, 10*time.Millisecond)
		cancelAdm()
		<-admDone
		require.ErrorIs(t, admissionErr, context.Canceled)
		require.NoError(t, writerLockTx.Rollback(subCtx))
		var queued bool
		require.NoError(t, c.Pool.QueryRow(subCtx, `SELECT EXISTS(SELECT FROM ingest_jobs WHERE document_id=$1)`, canceledReq.Document.ID).Scan(&queued))
		require.False(t, queued)

		// Queued row-lock cancellation on claim.
		queuedID := jobsAccept(t, c, "queued cancellation")
		jobLockTx, err := c.Pool.Begin(subCtx)
		require.NoError(t, err)
		defer func() { _ = jobLockTx.Rollback(context.WithoutCancel(subCtx)) }()
		_, err = jobLockTx.Exec(subCtx, `SELECT document_id FROM ingest_jobs WHERE document_id=$1 FOR UPDATE`, string(queuedID))
		require.NoError(t, err)
		cancelClaimCtx, cancelClaim := context.WithCancel(subCtx)
		cancelClaim()
		_, err = c.ClaimPDFProfilesForFixtureStore(cancelClaimCtx, "canceled", time.Minute, 5, "", true, true)
		require.ErrorIs(t, err, context.Canceled)
		require.NoError(t, jobLockTx.Rollback(subCtx))
		require.NoError(t, c.DeleteDocument(subCtx, queuedID))
	})

	// 3. Rollback guard from a pre-layout binary with both pending and expired layout jobs.
	t.Run("rollback to pre-layout binary protects pending and expired layout jobs", func(t *testing.T) {
		pendingRes, err := c.AcceptDocument(ctx, layoutPDFRequest("rollback", "pending"))
		require.NoError(t, err)
		expiredRes, err := c.AcceptDocument(ctx, layoutPDFRequest("rollback", "expired"))
		require.NoError(t, err)

		// Claim expiredRes with an analyzer and expire its lease.
		_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=clock_timestamp()+interval '1 minute' WHERE document_id=$1`, string(pendingRes.DocumentID))
		require.NoError(t, err)
		claimedExp, err := c.ClaimPDFProfilesForFixtureStore(ctx, "analyzer-before-rollback", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, claimedExp)
		require.Equal(t, expiredRes.DocumentID, claimedExp.Document.ID)
		_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(expiredRes.DocumentID))
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(pendingRes.DocumentID))
		require.NoError(t, err)

		// Rolled-back pre-layout binary runs preChangeMixedClaimSQL; neither job can be leased.
		got, err := c.claimJob(ctx, "rolled-back-mixed", 5, preChangeMixedClaimSQL, "rolled-back-mixed", 60.0, true, "")
		require.NoError(t, err)
		require.Nil(t, got)

		// Compatible layout worker restores progress without re-upload.
		for range 2 {
			j, claimErr := c.ClaimPDFProfilesForFixtureStore(ctx, "restored-analyzer", time.Minute, 5, "", false, true)
			require.NoError(t, claimErr)
			require.NotNil(t, j)
			require.Equal(t, domain.LayoutPDFProfile, j.PDF.Profile)
			ok, compErr := c.CompleteJob(ctx, *j, nil, domain.IndexObservation{})
			require.NoError(t, compErr)
			require.True(t, ok)
		}
	})
}

func TestLayoutQueueRestrictedRoleAndSchemaIsolation(t *testing.T) {
	t.Run("restricted serving role and schema shadowing isolation", func(t *testing.T) {
		ctx := t.Context()
		owner, serving, roles := restrictedDatabase(t)
		layoutRes, err := serving.AcceptDocument(ctx, layoutPDFRequest("", "restricted-layout"))
		require.NoError(t, err)

		// Create a shadow schema with a fake document_pdf_extractions claiming the document is mixed.
		grantExec(t, owner, roles.sql(`
			CREATE SCHEMA shadow;
			CREATE TABLE shadow.document_pdf_extractions (
				document_id UUID PRIMARY KEY,
				profile TEXT NOT NULL,
				coverage TEXT,
				manifest JSONB,
				page_results JSONB
			);
			GRANT USAGE ON SCHEMA shadow TO {quarry_service};
			GRANT SELECT ON shadow.document_pdf_extractions TO {quarry_service};`))
		_, err = owner.Pool.Exec(ctx, `INSERT INTO shadow.document_pdf_extractions(document_id, profile) VALUES($1, $2)`,
			string(layoutRes.DocumentID), domain.MixedPDFProfile)
		require.NoError(t, err)

		shadowServing := grantRoleClient(t, owner.Pool.Config().ConnString(),
			roles.sql(`SET SESSION AUTHORIZATION {quarry_service}; SET search_path TO shadow, public`))
		// Even with shadow ahead on search_path, the trigger resolves TG_TABLE_SCHEMA (public) and blocks old mixed SQL.
		got, err := shadowServing.claimJob(ctx, "shadow-old", 5, preChangeMixedClaimSQL, "shadow-old", 60.0, true, "")
		require.NoError(t, err)
		require.Nil(t, got)

		// Current analyzer on the restricted role claims and completes the layout job.
		claimed, err := serving.ClaimPDFProfilesForFixtureStore(ctx, "serving-analyzer", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.Equal(t, layoutRes.DocumentID, claimed.Document.ID)
		_, err = serving.BeginPDFOCR(ctx, *claimed)
		require.NoError(t, err)
		require.NoError(t, serving.StorePDFOCR(ctx, *claimed, domain.PDFResult{
			Text:     "Restricted layout text",
			Coverage: domain.PDFComplete,
			Pages:    []domain.PDFPageResult{{Number: 1, Source: "textract", State: "text"}},
		}))
		done, err := serving.CompleteJob(ctx, *claimed, nil, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, done)

		// Dedicated second corpus schema with its own migrations uses its own TG_TABLE_SCHEMA.
		corpusOwnerRoles := newGrantRoles(t, owner.Pool.Config().ConnString(), "corpus_owner", "corpus_service")
		grantExec(t, owner, corpusOwnerRoles.sql(`
			CREATE ROLE {corpus_owner};
			CREATE ROLE {corpus_service};
			CREATE SCHEMA "Layout Corpus" AUTHORIZATION {corpus_owner};
			GRANT USAGE ON SCHEMA "Layout Corpus" TO {corpus_service};`))
		corpusOwner := grantRoleClient(t, owner.Pool.Config().ConnString(),
			corpusOwnerRoles.sql(`SET SESSION AUTHORIZATION {corpus_owner}; SET search_path TO "Layout Corpus"`))
		require.NoError(t, corpusOwner.RunMigrations(ctx))
		grantExec(t, corpusOwner, corpusOwnerRoles.sql(`
			GRANT USAGE ON SCHEMA "Layout Corpus" TO {corpus_service};
			GRANT SELECT, INSERT, UPDATE, DELETE ON documents, chunks, ingest_jobs, document_fingerprints TO {corpus_service};
			GRANT SELECT, INSERT, UPDATE ON document_pdf_extractions, document_deletions TO {corpus_service};
			GRANT EXECUTE ON FUNCTION vector_bind_legacy(text,text,text), vector_writer_state(),
				evaluation_fixture_claimable(uuid,text), evaluation_fixture_reserved(uuid),
				document_deletion_active(), document_deletion_active_shared(), document_deletion_reserved(uuid) TO {corpus_service};`))
		corpusServing := grantRoleClient(t, owner.Pool.Config().ConnString(),
			corpusOwnerRoles.sql(`SET SESSION AUTHORIZATION {corpus_service}; SET search_path TO "Layout Corpus", public`))
		corpusLayout, err := corpusServing.AcceptDocument(ctx, layoutPDFRequest("", "corpus-layout"))
		require.NoError(t, err)
		// Poison public.document_pdf_extractions with a mixed profile for the same UUID; trigger must ignore public.
		_, err = owner.Pool.Exec(ctx, `INSERT INTO documents(id,content) VALUES($1,'')`, string(corpusLayout.DocumentID))
		require.NoError(t, err)
		_, err = owner.Pool.Exec(ctx, `INSERT INTO document_pdf_extractions(document_id,profile,manifest,coverage) VALUES($1,$2,'[]','pending')`,
			string(corpusLayout.DocumentID), domain.MixedPDFProfile)
		require.NoError(t, err)
		got, err = corpusServing.claimJob(ctx, "corpus-old-mixed", 5, preChangeMixedClaimSQL, "corpus-old-mixed", 60.0, true, "")
		require.NoError(t, err)
		require.Nil(t, got)
		corpusClaimed, err := corpusServing.ClaimPDFProfilesForFixtureStore(ctx, "corpus-analyzer", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, corpusClaimed)
		require.Equal(t, corpusLayout.DocumentID, corpusClaimed.Document.ID)
	})

	t.Run("privilege errors are returned distinctly and never converted to fallback or no rows", func(t *testing.T) {
		ctx := t.Context()
		owner, serving, roles := restrictedDatabase(t)
		_, err := serving.AcceptDocument(ctx, layoutPDFRequest("", "priv-layout"))
		require.NoError(t, err)

		grantExec(t, owner, `REVOKE SELECT ON public.document_pdf_extractions FROM `+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
		_, err = serving.ClaimPDFProfilesForFixtureStore(ctx, "no-priv", time.Minute, 5, "", true, true)
		requireSQLState(t, err, "42501")

		grantExec(t, owner, `GRANT SELECT ON public.document_pdf_extractions TO `+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
		grantExec(t, owner, `REVOKE UPDATE ON public.ingest_jobs FROM `+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
		_, err = serving.ClaimPDFProfilesForFixtureStore(ctx, "no-update", time.Minute, 5, "", true, true)
		requireSQLState(t, err, "42501")
	})

	t.Run("missing migration fails layout admission closed and retains profile exclusion on claims", func(t *testing.T) {
		ctx := t.Context()
		c := migrationTestClient(t, startTestDatabase(t))
		applyMigrationPrefix(t, c, 30)

		// Layout admission fails closed before inserting any document/job.
		_, err := c.AcceptDocument(ctx, layoutPDFRequest("", "blocked-layout"))
		require.ErrorContains(t, err, "migration 040")
		var docs, jobs int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM ingest_jobs)`).Scan(&docs, &jobs))
		require.Zero(t, docs)
		require.Zero(t, jobs)

		// Even if a pre-040 layout row already exists, current claim falls back ONLY to mixed/ordinary exclusion.
		preLayoutID := seedLayoutDocumentPre040(t, c, "", "pre040-layout")
		mixedRes, err := c.AcceptDocument(ctx, mixedPDFRequest())
		require.NoError(t, err)
		ordID := jobsAccept(t, c, "pre040 ordinary")

		// Analyzer-only cannot claim layout without migration 040, and cannot claim mixed.
		got, err := c.ClaimPDFProfilesForFixtureStore(ctx, "new-ana-pre040", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, ordID, got.Document.ID)
		got2, err := c.ClaimPDFProfilesForFixtureStore(ctx, "new-ana-pre040", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.Nil(t, got2)

		// Both-capable worker on pre-040 claims mixed, never layout.
		gotMixed, err := c.ClaimPDFProfilesForFixtureStore(ctx, "new-both-pre040", time.Minute, 5, "", true, true)
		require.NoError(t, err)
		require.NotNil(t, gotMixed)
		require.Equal(t, mixedRes.DocumentID, gotMixed.Document.ID)
		gotNone, err := c.ClaimPDFProfilesForFixtureStore(ctx, "new-both-pre040", time.Minute, 5, "", true, true)
		require.NoError(t, err)
		require.Nil(t, gotNone)

		var attempts int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT attempts FROM ingest_jobs WHERE document_id=$1`, string(preLayoutID)).Scan(&attempts))
		require.Zero(t, attempts)
	})

	t.Run("pre-fixture fallback preserves layout capability and profile rules", func(t *testing.T) {
		ctx := t.Context()
		c := migrationTestClient(t, startTestDatabase(t))
		applyMigrationsExcept(t, c, 17, 18, 19)

		layoutID := seedLayoutDocumentPre040(t, c, "", "pre-fixture-layout")
		detSkip, err := c.ClaimPDFProfilesForFixtureStore(ctx, "det", time.Minute, 5, "", true, false)
		require.NoError(t, err)
		require.Nil(t, detSkip)

		anaJob, err := c.ClaimPDFProfilesForFixtureStore(ctx, "ana", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, anaJob)
		require.Equal(t, layoutID, anaJob.Document.ID)

		var layoutAttempt *int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT layout_claim_attempt FROM ingest_jobs WHERE document_id=$1`, string(layoutID)).Scan(&layoutAttempt))
		require.NotNil(t, layoutAttempt)
		require.Equal(t, 1, *layoutAttempt)
	})
}

func TestLayoutRetryAndProfileIsolation(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()

	t.Run("page limit and admission profile checks", func(t *testing.T) {
		atLimit := layoutPDFRequest("limits", "maximum pages")
		pages := make(domain.PDFPages, domain.MaxPDFPages)
		for i := range pages {
			pages[i] = domain.PDFPage{Number: i + 1, Evidence: domain.PDFEmbedded, Text: "Readable embedded PDF page"}
		}
		atLimit.PDF.Pages = pages
		accepted, err := c.AcceptDocument(ctx, atLimit)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
		require.NoError(t, c.DeleteDocument(ctx, accepted.DocumentID))

		tooMany := layoutPDFRequest("limits", "over maximum pages")
		extraPage := pages[len(pages)-1]
		extraPage.Number++
		pages = append(pages, extraPage)
		tooMany.PDF.Pages = pages
		_, err = c.AcceptDocument(ctx, tooMany)
		require.ErrorContains(t, err, "invalid PDF manifest page count")
	})

	t.Run("frozen generation blocks accept and claim cleanly", func(t *testing.T) {
		_, err := c.Pool.Exec(ctx, `UPDATE vector_state SET frozen=TRUE`)
		require.NoError(t, err)
		frozenRes, err := c.AcceptDocument(ctx, layoutPDFRequest("freeze", "frozen"))
		require.NoError(t, err)
		require.Equal(t, domain.AcceptFrozen, frozenRes.Outcome)
		_, err = c.Pool.Exec(ctx, `UPDATE vector_state SET frozen=FALSE`)
		require.NoError(t, err)
	})

	t.Run("all ingestion entry points maintain pdf_required=TRUE before and after checkpoint", func(t *testing.T) {
		entryCtx := t.Context()
		requireLayoutJobPDFRequired := func(id domain.DocumentID) {
			t.Helper()
			var pdfReq bool
			require.NoError(t, c.Pool.QueryRow(entryCtx, `SELECT pdf_required FROM ingest_jobs WHERE document_id=$1`, string(id)).Scan(&pdfReq))
			require.True(t, pdfReq)
		}

		// 1. Anonymous AcceptDocument + AcceptExistingPDF before and after checkpoint.
		anonReq := layoutPDFRequest("entry", "anon-layout")
		anonRes, err := c.AcceptDocument(entryCtx, anonReq)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, anonRes.Outcome)
		requireLayoutJobPDFRequired(anonRes.DocumentID)

		// In-progress via AcceptDocument & AcceptExistingPDF.
		inProg, err := c.AcceptExistingPDF(entryCtx, domain.AcceptRequest{
			Document:             domain.Document{ID: domain.DocumentID(uuid.NewString())},
			Collection:           anonReq.Collection,
			ContentDedup:         domain.ContentDedupCollection,
			Fingerprint:          anonReq.Fingerprint,
			LegacyOCRFingerprint: anonReq.LegacyOCRFingerprint,
			PDFProfile:           domain.LayoutPDFProfile,
		}, true)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptInProgress, inProg.Outcome)
		require.Equal(t, anonRes.DocumentID, inProg.DocumentID)

		// Cross-profile lookup returns AcceptNeedsSource on anonymous, AcceptConflict on caller-supplied.
		wrongAnon, err := c.AcceptExistingPDF(entryCtx, domain.AcceptRequest{
			Document:             domain.Document{ID: domain.DocumentID(uuid.NewString())},
			Collection:           anonReq.Collection,
			ContentDedup:         domain.ContentDedupCollection,
			Fingerprint:          chunker.ScopedFingerprint(anonReq.Collection, domain.MixedPDFProfile+"\x00anon-layout"),
			LegacyOCRFingerprint: anonReq.LegacyOCRFingerprint,
			PDFProfile:           domain.MixedPDFProfile,
		}, true)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptNeedsSource, wrongAnon.Outcome)

		wrongUUID, err := c.AcceptExistingPDF(entryCtx, domain.AcceptRequest{
			Document:       domain.Document{ID: anonRes.DocumentID},
			CallerSupplied: true,
			Collection:     anonReq.Collection,
			ContentDedup:   domain.ContentDedupCollection,
			Fingerprint:    chunker.ScopedFingerprint(anonReq.Collection, domain.MixedPDFProfile+"\x00anon-layout"),
			PDFProfile:     domain.MixedPDFProfile,
		}, true)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptConflict, wrongUUID.Outcome)

		// Claim, fail without checkpoint -> anonymous returns AcceptOCRRetryRequired, UUID returns AcceptRequeued.
		job, err := c.ClaimPDFProfilesForFixtureStore(entryCtx, "ana", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, job)
		require.NoError(t, c.FailJob(entryCtx, *job, "provider failed"))

		anonRetry, err := c.AcceptExistingPDF(entryCtx, domain.AcceptRequest{
			Document:     domain.Document{ID: domain.DocumentID(uuid.NewString())},
			Collection:   anonReq.Collection,
			ContentDedup: domain.ContentDedupCollection,
			Fingerprint:  anonReq.Fingerprint,
			PDFProfile:   domain.LayoutPDFProfile,
		}, true)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptOCRRetryRequired, anonRetry.Outcome)

		uuidRetryReq := domain.AcceptRequest{
			Document:       domain.Document{ID: anonRes.DocumentID},
			CallerSupplied: true,
			Collection:     anonReq.Collection,
			ContentDedup:   domain.ContentDedupCollection,
			Fingerprint:    anonReq.Fingerprint,
			PDFProfile:     domain.LayoutPDFProfile,
			MaxPending:     1000,
		}
		uuidRetry, err := c.AcceptExistingPDF(entryCtx, uuidRetryReq, true)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptRequeued, uuidRetry.Outcome)
		requireLayoutJobPDFRequired(anonRes.DocumentID)

		// Claim, checkpoint usable partial text, then fail -> AcceptExistingPDF requeues with pdf_required=TRUE.
		job, err = c.ClaimPDFProfilesForFixtureStore(entryCtx, "ana", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, job)
		_, err = c.BeginPDFOCR(entryCtx, *job)
		require.NoError(t, err)
		require.NoError(t, c.StorePDFOCR(entryCtx, *job, domain.PDFResult{
			Text:     "Usable checkpointed layout text",
			Coverage: domain.PDFPartial,
			Pages:    []domain.PDFPageResult{{Number: 1, Source: "textract", State: "partial", Sections: []domain.LayoutSection{{Page: 1, Text: "Usable checkpointed layout text"}}}},
		}))
		require.NoError(t, c.FailJob(entryCtx, *job, "vector failure"))

		requeueAfterCheckpoint, err := c.AcceptExistingPDF(entryCtx, uuidRetryReq, true)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptRequeued, requeueAfterCheckpoint.Outcome)
		requireLayoutJobPDFRequired(anonRes.DocumentID)

		// Complete, then verify AcceptDuplicate.
		job, err = c.ClaimPDFProfilesForFixtureStore(entryCtx, "ana", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, job)
		done, err := c.CompleteJob(entryCtx, *job, nil, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, done)
		dupRes, err := c.AcceptExistingPDF(entryCtx, uuidRetryReq, true)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptDuplicate, dupRes.Outcome)

		// 2. Stale/unusable checkpoint after expiry returns AcceptOCRRetryAfterExpiry before window expiry, then AcceptRequeued.
		staleReq := layoutPDFRequest("entry", "stale-checkpoint")
		staleRes, err := c.AcceptDocument(entryCtx, staleReq)
		require.NoError(t, err)
		job, err = c.ClaimPDFProfilesForFixtureStore(entryCtx, "ana", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		_, err = c.BeginPDFOCR(entryCtx, *job)
		require.NoError(t, err)
		require.NoError(t, c.StorePDFOCR(entryCtx, *job, domain.PDFResult{
			Text:     "tiny",
			Coverage: domain.PDFPartial,
			Pages:    []domain.PDFPageResult{{Number: 1, State: "missing"}},
		}))
		require.NoError(t, c.FailJob(entryCtx, *job, "no_text"))

		staleUUID := staleReq
		staleUUID.CallerSupplied = true
		staleUUID.Document.S3Key = nil
		waitExpiry, err := c.AcceptDocument(entryCtx, staleUUID)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptOCRRetryAfterExpiry, waitExpiry.Outcome)

		_, err = c.Pool.Exec(entryCtx, `UPDATE document_pdf_extractions SET started_at=NOW()-interval '8 days' WHERE document_id=$1`, string(staleRes.DocumentID))
		require.NoError(t, err)
		reopened, err := c.AcceptDocument(entryCtx, staleUUID)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptRequeued, reopened.Outcome)
		requireLayoutJobPDFRequired(staleRes.DocumentID)
		require.NoError(t, c.DeleteDocument(entryCtx, staleRes.DocumentID))

		// 3. Owned ingestion entry point: new + retry after checkpoint + retry after failure.
		f := provisionOwnedFixture(t, c)
		inc := uuid.NewString()
		ownedReq := layoutPDFRequest("", "owned-layout")
		ownedReq.ContentDedup = domain.ContentDedupOff
		ownedReq.Owner = &domain.OwnerBinding{Principal: f.owner, Incarnation: inc, ValidUntil: time.Now().Add(time.Minute)}
		ownedRes, err := c.AcceptDocument(entryCtx, ownedReq)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, ownedRes.Outcome)
		requireLayoutJobPDFRequired(ownedRes.DocumentID)

		job, err = c.ClaimPDFProfilesForFixtureStore(entryCtx, "owned-ana", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, job)
		_, err = c.BeginPDFOCR(entryCtx, *job)
		require.NoError(t, err)
		require.NoError(t, c.StorePDFOCR(entryCtx, *job, domain.PDFResult{
			Text:     "Owned layout checkpoint text",
			Coverage: domain.PDFComplete,
			Pages:    []domain.PDFPageResult{{Number: 1, Source: "textract", State: "text", Sections: []domain.LayoutSection{{Page: 1, Text: "Owned layout checkpoint text"}}}},
		}))
		require.NoError(t, c.FailJob(entryCtx, *job, "vector timeout"))

		ownedRetry := ownedReq
		ownedRetry.CallerSupplied = true
		ownedRetry.Document.S3Key = nil
		ownedRetry.Owner.ValidUntil = time.Now().Add(time.Minute)
		ownedRequeued, err := c.AcceptDocument(entryCtx, ownedRetry)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptRequeued, ownedRequeued.Outcome)
		requireLayoutJobPDFRequired(ownedRes.DocumentID)

		// Missing original on reclaimed/accepted job fails cleanly in worker/store guard.
		_, err = c.Pool.Exec(entryCtx, `UPDATE documents SET s3_key=NULL WHERE id=$1`, string(ownedRes.DocumentID))
		require.NoError(t, err)
		missingOrig, err := c.ClaimPDFProfilesForFixtureStore(entryCtx, "owned-ana-2", time.Minute, 5, "", false, true)
		require.NoError(t, err)
		require.NotNil(t, missingOrig)
		require.Nil(t, missingOrig.Document.S3Key)
		// Force coverage pending with NULL s3_key -> BeginPDFOCR rejects missing source via withPDFLease.
		_, err = c.Pool.Exec(entryCtx, `UPDATE document_pdf_extractions SET coverage='pending' WHERE document_id=$1`, string(ownedRes.DocumentID))
		require.NoError(t, err)
		_, err = c.BeginPDFOCR(entryCtx, *missingOrig)
		require.ErrorIs(t, err, domain.ErrJobNotOwned)

		// Validate exact profile matching (no substring/loose parser).
		require.NoError(t, c.ReleaseJob(entryCtx, *missingOrig))
		_, err = c.Pool.Exec(entryCtx, `UPDATE document_pdf_extractions SET profile='quarry.pdf.layout.v1 ' WHERE document_id=$1`, string(ownedRes.DocumentID))
		require.NoError(t, err)
		skipped, err := c.ClaimPDFProfilesForFixtureStore(entryCtx, "exact-profile", time.Minute, 5, "", true, true)
		require.NoError(t, err)
		require.Nil(t, skipped)
	})
}
