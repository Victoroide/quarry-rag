package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func corpusWriterSession(t *testing.T, owner *Client, role string) (client *Client, pid int32) {
	t.Helper()
	client, err := NewClient(t.Context(), owner.Pool.Config().ConnString(), func(cfg *pgxpool.Config) {
		cfg.MaxConns = 1
		setRepeatableReadDefault(cfg)
		if role != "" {
			cfg.ConnConfig.RuntimeParams["role"] = role
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.Background())) })
	require.NoError(t, client.Pool.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&pid))
	return client, pid
}

func requireCorpusBlockedBy(t *testing.T, owner *Client, waiting int32, blockers ...int32) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		var blocked bool
		require.NoError(collect, owner.Pool.QueryRow(ctx, "SELECT pg_blocking_pids($1) && $2::int[]", waiting, blockers).Scan(&blocked))
		require.True(collect, blocked, "session %d must wait for one of %v", waiting, blockers)
	}, 4*time.Second, 10*time.Millisecond)
}

func requireCorpusPolicy(t *testing.T, owner *Client, mode string, revision, audits int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	requireCorpusPolicyContext(ctx, t, owner, mode, revision, audits)
}

func requireCorpusPolicyContext(ctx context.Context, t *testing.T, owner *Client, mode string, revision, audits int64) {
	t.Helper()
	var actual string
	var actualRevision, actualAudits int64
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT grant_catalog_corpus_authz_mode(),
		(SELECT revision FROM grant_catalog_policy_revision WHERE singleton),
		(SELECT count(*) FROM grant_catalog_security_audit WHERE action='corpus_cutover')`).
		Scan(&actual, &actualRevision, &actualAudits))
	require.Equal(t, mode, actual)
	require.Equal(t, revision, actualRevision)
	require.Equal(t, audits, actualAudits)
}

func activateDeletionForCorpusCutover(t *testing.T, owner *Client) {
	t.Helper()
	_, err := owner.ActivateDeletion(t.Context())
	require.NoError(t, err)
}

func corpusCommitBarrier(t *testing.T, owner *Client) (tx pgx.Tx, pid int32) {
	t.Helper()
	tx, err := owner.Pool.Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if rollbackErr := tx.Rollback(ctx); !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			require.NoError(t, rollbackErr)
		}
	})
	require.NoError(t, tx.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&pid))
	return tx, pid
}

func TestCorpusCutoverWaitsForWriterCommit(t *testing.T) {
	for _, tc := range []struct {
		name, table string
		rollback    bool
	}{
		{name: "acceptance", table: "documents"},
		{name: "claim", table: "ingest_jobs"},
		{name: "completion", table: "chunks"},
		{name: "admission rollback", table: "documents", rollback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := jobsDatabase(t)
			writer, writerPID := corpusWriterSession(t, owner, "")
			writer.SetAuthzMode("service")
			operator, operatorPID := corpusWriterSession(t, owner, "")
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			writeCtx, stopWrite := context.WithCancel(ctx)
			defer stopWrite()

			var write func(context.Context) error
			switch tc.table {
			case "documents":
				write = func(ctx context.Context) error {
					res, err := writer.AcceptDocument(ctx, jobsRequest("admission "+tc.name))
					if err == nil && res.Outcome != domain.AcceptAccepted {
						return errors.New("admission did not accept the document")
					}
					return err
				}
			case "ingest_jobs":
				jobsAccept(t, writer, "job to claim")
				write = func(ctx context.Context) error {
					job, err := writer.ClaimJob(ctx, "service-worker", time.Minute, 5)
					if err == nil && job == nil {
						return errors.New("claim did not lease the job")
					}
					return err
				}
			case "chunks":
				jobsAccept(t, writer, "job to complete")
				job, err := writer.ClaimJob(ctx, "service-worker", time.Minute, 5)
				require.NoError(t, err)
				require.NotNil(t, job)
				write = func(ctx context.Context) error {
					ok, err := writer.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
					if err == nil && !ok {
						return domain.ErrJobNotOwned
					}
					return err
				}
			}

			activateDeletionForCorpusCutover(t, owner)
			barrier, barrierPID := corpusCommitBarrier(t, owner)
			_, err := barrier.Exec(ctx, "LOCK TABLE "+pgx.Identifier{tc.table}.Sanitize()+" IN SHARE MODE")
			require.NoError(t, err)
			wrote := make(chan error, 1)
			go func() { wrote <- write(writeCtx) }()
			requireCorpusBlockedBy(t, owner, writerPID, barrierPID)

			activated := make(chan error, 1)
			go func() {
				_, activateErr := operator.ActivateGrants(ctx, false)
				activated <- activateErr
			}()
			requireCorpusBlockedBy(t, owner, operatorPID, writerPID)
			requireCorpusPolicy(t, owner, "service", 0, 0)
			if tc.rollback {
				stopWrite()
				require.Error(t, <-wrote)
				// Cutover can finish even though the statement barrier is still held.
				require.NoError(t, <-activated)
				var count int
				require.NoError(t, owner.Pool.QueryRow(ctx, "SELECT count(*) FROM documents").Scan(&count))
				require.Zero(t, count)
			} else {
				require.NoError(t, barrier.Commit(ctx))
				require.NoError(t, <-wrote)
				require.NoError(t, <-activated)
			}
			requireCorpusPolicy(t, owner, "grants", 1, 1)
			require.ErrorIs(t, write(ctx), domain.ErrAuthzModeMismatch)
		})
	}
}

func TestCorpusModeWriterWaitingOnCutover(t *testing.T) {
	for _, outcome := range []string{"commit", "rollback", "cancel writer"} {
		t.Run(outcome, func(t *testing.T) {
			owner := jobsDatabase(t)
			writer, writerPID := corpusWriterSession(t, owner, "")
			writer.SetAuthzMode("service")
			id := jobsAccept(t, writer, "job waiting for cutover")
			activateDeletionForCorpusCutover(t, owner)

			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cutover, cutoverPID := corpusCommitBarrier(t, owner)
			_, err := cutover.Exec(ctx, "SELECT * FROM grant_catalog_activate_grants(false)")
			require.NoError(t, err)

			writeCtx, stopWrite := context.WithCancel(ctx)
			defer stopWrite()
			claimed := make(chan error, 1)
			go func() {
				_, claimErr := writer.ClaimJob(writeCtx, "waiting-worker", time.Minute, 5)
				claimed <- claimErr
			}()
			requireCorpusBlockedBy(t, owner, writerPID, cutoverPID)
			switch outcome {
			case "commit":
				require.NoError(t, cutover.Commit(ctx))
				require.ErrorIs(t, <-claimed, domain.ErrAuthzModeMismatch)
				requireCorpusPolicy(t, owner, "grants", 1, 1)
			case "rollback":
				require.NoError(t, cutover.Rollback(ctx))
				require.NoError(t, <-claimed)
				requireCorpusPolicy(t, owner, "service", 0, 0)
				var state string
				require.NoError(t, owner.Pool.QueryRow(ctx, "SELECT state FROM ingest_jobs WHERE document_id=$1", id).Scan(&state))
				require.Equal(t, "running", state)
			case "cancel writer":
				stopWrite()
				require.ErrorIs(t, <-claimed, context.Canceled)
				require.NoError(t, cutover.Rollback(ctx))
				requireCorpusPolicy(t, owner, "service", 0, 0)
				got, claimErr := writer.ClaimJob(ctx, "retry-worker", time.Minute, 5)
				require.NoError(t, claimErr)
				require.NotNil(t, got)
				require.Equal(t, id, got.Document.ID)
				require.Equal(t, 1, got.Attempts)
			}
			retry, err := owner.ActivateGrants(ctx, false)
			require.NoError(t, err)
			require.Equal(t, outcome == "commit", retry.AlreadyActive)
			require.EqualValues(t, 1, retry.Revision)
			requireCorpusPolicy(t, owner, "grants", 1, 1)
		})
	}
}

func TestCorpusCutoverLockTimeoutLeavesPolicyUntouched(t *testing.T) {
	owner := activatedDatabase(t)
	operator, operatorPID := corpusWriterSession(t, owner, "")
	preview, _ := corpusWriterSession(t, owner, "")
	writer, writerPID := corpusCommitBarrier(t, owner)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	_, err := writer.Exec(ctx, "SELECT grant_catalog_corpus_authz_mode_shared()")
	require.NoError(t, err)

	activated := make(chan error, 1)
	go func() {
		_, activateErr := operator.ActivateGrants(ctx, false)
		activated <- activateErr
	}()
	requireCorpusBlockedBy(t, owner, operatorPID, writerPID)
	previewCtx, stopPreview := context.WithTimeout(ctx, 3*time.Second)
	defer stopPreview()
	dry, err := preview.ActivateGrants(previewCtx, true)
	require.NoError(t, err)
	require.True(t, dry.DryRun)
	require.False(t, dry.AlreadyActive)
	require.Zero(t, dry.Revision)

	require.ErrorContains(t, <-activated, "SQLSTATE 55P03")
	requireCorpusPolicy(t, owner, "service", 0, 0)
	require.NoError(t, writer.Rollback(ctx))
	result, err := operator.ActivateGrants(ctx, false)
	require.NoError(t, err)
	require.False(t, result.AlreadyActive)
	requireCorpusPolicy(t, owner, "grants", 1, 1)
}

func TestCorpusCutoverWaitsForAcquiredWriter(t *testing.T) {
	owner, _, roles := restrictedDatabase(t)
	activateDeletionForCorpusCutover(t, owner)
	writer, writerPoolPID := corpusWriterSession(t, owner, roles.name("quarry_service"))
	writer.SetAuthzMode("service")
	operator, operatorPID := corpusWriterSession(t, owner, "")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	release, err := writer.AcquireWriter(ctx)
	require.NoError(t, err)
	defer func() {
		if release != nil {
			release()
		}
	}()
	// The sole pooled connection stays available while the separate writer
	// session holds the marker lock.
	require.NoError(t, writer.WritersOpen(ctx))
	var writerPID int32
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT pid FROM pg_stat_activity
		WHERE datname=current_database() AND pid<>$1 AND state='idle in transaction'
		AND query='SELECT grant_catalog_corpus_authz_mode_shared()'`, writerPoolPID).Scan(&writerPID))

	activated := make(chan error, 1)
	go func() {
		_, activateErr := operator.ActivateGrants(ctx, false)
		activated <- activateErr
	}()
	requireCorpusBlockedBy(t, owner, operatorPID, writerPID)
	release()
	release = nil
	require.NoError(t, <-activated)

	// A failed check disposes of its lock session and returns the one slot.
	_, err = writer.AcquireWriter(ctx)
	require.ErrorIs(t, err, domain.ErrAuthzModeMismatch)
	writer.SetAuthzMode("grants")
	marker, markerPID := corpusCommitBarrier(t, owner)
	_, err = marker.Exec(ctx, "SELECT 1 FROM grant_catalog_corpus_mode WHERE singleton FOR UPDATE")
	require.NoError(t, err)
	waitCtx, stopWait := context.WithCancel(ctx)
	defer stopWait()
	stopped := make(chan error, 1)
	go func() {
		done, acquireErr := writer.AcquireWriter(waitCtx)
		if done != nil {
			done()
		}
		stopped <- acquireErr
	}()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var blocked bool
		require.NoError(collect, owner.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
			WHERE datname=current_database() AND query='SELECT grant_catalog_corpus_authz_mode_shared()'
			AND $1=ANY(pg_blocking_pids(pid)))`, markerPID).Scan(&blocked))
		require.True(collect, blocked)
	}, 4*time.Second, 10*time.Millisecond)
	stopWait()
	require.ErrorIs(t, <-stopped, context.Canceled)
	require.NoError(t, marker.Rollback(ctx))
	require.Empty(t, writer.writerSlots)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var open int
		require.NoError(collect, owner.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname=current_database() AND query='SELECT grant_catalog_corpus_authz_mode_shared()'
			AND pid<>$1`, writerPoolPID).Scan(&open))
		require.Zero(collect, open)
	}, 3*time.Second, 10*time.Millisecond)

	f := provisionOwnedFixture(t, owner)
	scopedRelease, err := writer.AcquireWriter(ctx)
	require.NoError(t, err)
	scopedRelease()
	result, err := writer.AcceptDocument(ctx, ownedRequest(f.owner, "", "scoped after cutover"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	job, err := writer.ClaimJob(ctx, "grants-worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, result.DocumentID, job.Document.ID)
	completed, err := writer.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, completed)
	page, err := writer.ListScopedDocumentsPage(ctx, f.owner, "", 100, "", false)
	require.NoError(t, err)
	require.Len(t, page.Documents, 1)
	require.Equal(t, string(result.DocumentID), page.Documents[0].ID)
}

func corpusPublicationDatabase(t *testing.T, backend string) *Client {
	t.Helper()
	switch backend {
	case "offline":
		return jobsDatabase(t)
	case "pgvector":
		c, _ := vectorDatabase(t)
		return c
	case "hosted manifest":
		return manifestDatabase(t)
	default:
		t.Fatalf("unknown publication backend %q", backend)
		return nil
	}
}

func TestCompleteJobCorpusCutoverFence(t *testing.T) {
	for _, backend := range []string{"offline", "pgvector", "hosted manifest"} {
		t.Run(backend, func(t *testing.T) {
			c := corpusPublicationDatabase(t, backend)
			c.SetAuthzMode("service")
			makeAttempt := func(content string) (domain.Job, []domain.Chunk) {
				t.Helper()
				id := jobsAccept(t, c, content)
				job, err := c.ClaimJob(t.Context(), "publication-worker", time.Minute, 5)
				require.NoError(t, err)
				require.NotNil(t, job)
				require.Equal(t, id, job.Document.ID)
				if backend == "hosted manifest" {
					a := persistManifestAttempt(t, c, id, *job)
					return a.job, a.chunks(id, content)
				}
				return *job, []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), Text: content, TokenCount: 3, Embedding: []float32{1, 0, 0}}}
			}
			approved, approvedChunks := makeAttempt("leased before freeze")
			blocked, blockedChunks := makeAttempt("leased before cutover")
			ctx := t.Context()
			// Exercise completion's fingerprint backfill for a legacy pending row.
			grantExec(t, c, `DELETE FROM document_fingerprints;
				UPDATE documents SET fingerprint=NULL WHERE status='processing'`)
			_, err := c.RegisterGeneration(ctx, Generation{Name: "g2", Store: "postgres", Target: VectorTable("g2"), Profile: generationProfile(), Origin: OriginRecorded})
			require.NoError(t, err)
			require.NoError(t, c.FreezeWriters(ctx, "g2"))
			for _, token := range []string{"", uuid.NewString()} {
				stale := approved
				stale.LeaseToken = token
				ok, completeErr := c.CompleteJob(ctx, stale, approvedChunks, domain.IndexObservation{})
				require.NoError(t, completeErr)
				require.False(t, ok)
				requirePublicationState(t, c, stale, backend, false)
			}
			ok, err := c.CompleteJob(ctx, approved, approvedChunks, domain.IndexObservation{})
			require.NoError(t, err)
			require.True(t, ok, "an approved lease can publish while the vector freeze drains")
			requirePublicationState(t, c, approved, backend, true)

			requirePublicationState(t, c, blocked, backend, false)
			activateDeletionForCorpusCutover(t, c)
			_, err = c.ActivateGrants(ctx, false)
			require.NoError(t, err)
			ok, err = c.CompleteJob(ctx, blocked, blockedChunks, domain.IndexObservation{})
			require.ErrorIs(t, err, domain.ErrAuthzModeMismatch)
			require.False(t, ok)
			requirePublicationState(t, c, blocked, backend, false)
		})
	}
}

type corpusLayoutLease struct {
	state                     string
	attempts                  int
	owner, token              *string
	until                     *time.Time
	pdfAttempt, layoutAttempt *int
	pdfRequired, due          bool
}

func readCorpusLayoutLease(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID) corpusLayoutLease {
	t.Helper()
	var lease corpusLayoutLease
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT state,attempts,lease_owner,lease_until,lease_token::text,
		pdf_claim_attempt,layout_claim_attempt,pdf_required,next_attempt_at<=clock_timestamp()
		FROM ingest_jobs WHERE document_id=$1`, id).Scan(&lease.state, &lease.attempts, &lease.owner, &lease.until,
		&lease.token, &lease.pdfAttempt, &lease.layoutAttempt, &lease.pdfRequired, &lease.due))
	return lease
}

func requireStampedCorpusLayoutLease(ctx context.Context, t *testing.T, c *Client, job *domain.Job) {
	t.Helper()
	lease := readCorpusLayoutLease(ctx, t, c, job.Document.ID)
	require.Equal(t, "running", lease.state)
	require.Equal(t, job.Attempts, lease.attempts)
	require.Equal(t, &job.LeaseOwner, lease.owner)
	require.NotEmpty(t, job.LeaseToken)
	require.Equal(t, &job.LeaseToken, lease.token)
	require.NotNil(t, lease.until)
	var serverNow time.Time
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&serverNow))
	require.True(t, lease.until.After(serverNow), "stamped lease must still be live")
	require.Equal(t, &job.Attempts, lease.pdfAttempt)
	require.Equal(t, &job.Attempts, lease.layoutAttempt)
	require.True(t, lease.pdfRequired)
}

func TestReleaseLayoutLeaseAfterCorpusCutover(t *testing.T) {
	for _, backend := range []string{"offline", "pgvector", "hosted manifest"} {
		t.Run(backend, func(t *testing.T) {
			service := corpusPublicationDatabase(t, backend)
			grants := otherConnection(t, service)
			service.SetAuthzMode("service")
			grants.SetAuthzMode("grants")
			hosted := backend == "hosted manifest"
			if hosted {
				grants.EnableHostedVectorManifests(true)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			req := layoutPDFRequest("archive", t.Name())
			accepted, err := service.AcceptDocument(ctx, req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
			id := accepted.DocumentID
			fingerprintCount := func() int {
				t.Helper()
				var count int
				require.NoError(t, service.Pool.QueryRow(ctx,
					`SELECT count(*) FROM document_fingerprints WHERE document_id=$1`, id).Scan(&count))
				return count
			}
			acceptedFingerprints := fingerprintCount()

			const owner = "layout-publication-worker"
			const maxAttempts = 2
			first, err := claimLayoutForCorpusTest(ctx, service, owner, maxAttempts)
			require.NoError(t, err)
			require.NotNil(t, first)
			require.Equal(t, id, first.Document.ID)
			require.Equal(t, 1, first.Attempts)
			requireStampedCorpusLayoutLease(ctx, t, service, first)
			storeLayoutCheckpoint(ctx, t, service, first, domain.PDFComplete)
			_, checkpointDoc, checkpointPDF := snapshotJobAndDoc(ctx, t, service, id)

			var earlier manifestAttempt
			var earlierManifest string
			if hosted {
				earlier = persistManifestAttemptWithChunk(ctx, t, service, id, *first, uuid.NewString())
				require.Equal(t, "writing", manifestStateContext(ctx, t, service, earlier.revision))
				earlierManifest = readCorpusManifestJSON(ctx, t, service, earlier.revision)
			}
			expireJobLeaseContext(ctx, t, service, id)
			leased, err := claimLayoutForCorpusTest(ctx, service, owner, maxAttempts)
			require.NoError(t, err)
			require.NotNil(t, leased)
			require.Equal(t, id, leased.Document.ID)
			require.Equal(t, first.Incarnation, leased.Incarnation)
			require.Equal(t, maxAttempts, leased.Attempts)
			require.NotEqual(t, first.LeaseToken, leased.LeaseToken)
			require.Equal(t, req.Collection, leased.Collection)
			require.Equal(t, req.Document.S3Key, leased.Document.S3Key)
			require.Equal(t, "pdf_layout", leased.Document.TextSource)
			require.NotNil(t, leased.PDF)
			require.Equal(t, domain.LayoutPDFProfile, leased.PDF.Profile)
			require.Equal(t, domain.PDFComplete, leased.PDF.Coverage)
			require.Len(t, leased.PDF.PageResults, 1)
			require.Len(t, leased.PDF.PageResults[0].Sections, 1)
			require.NotEmpty(t, leased.Document.Content)
			require.Equal(t, leased.Document.Content, leased.PDF.PageResults[0].Sections[0].Text)
			_, expiryDoc, expiryPDF := snapshotJobAndDoc(ctx, t, service, id)
			require.Equal(t, checkpointDoc, expiryDoc)
			require.Equal(t, checkpointPDF, expiryPDF)

			chunks := []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), DocumentID: id,
				Text: leased.Document.Content, TokenCount: 3, Embedding: []float32{1, 0, 0}}}
			var writing *domain.VectorManifest
			if hosted {
				attempt := persistManifestAttemptWithChunk(ctx, t, service, id, *leased, uuid.NewString())
				*leased = attempt.job
				chunks = attempt.chunks(id, leased.Document.Content)
				writing, err = service.GetVectorManifest(ctx, attempt.revision)
				require.NoError(t, err)
				require.NotNil(t, writing)
				require.Equal(t, "writing", writing.State)
			}

			_, err = service.RegisterGeneration(ctx, Generation{Name: "g2", Store: "postgres", Target: VectorTable("g2"), Profile: generationProfile(), Origin: OriginRecorded})
			require.NoError(t, err)
			require.NoError(t, service.FreezeWriters(ctx, "g2"))
			_, err = service.ActivateDeletion(ctx)
			require.NoError(t, err)
			_, err = service.ActivateGrants(ctx, false)
			require.NoError(t, err)
			requireCorpusPolicyContext(ctx, t, service, "grants", 1, 1)

			requireStampedCorpusLayoutLease(ctx, t, service, leased)
			type snapshot struct {
				job, doc, pdf, publication string
				fingerprints               int
			}
			readSnapshot := func() snapshot {
				t.Helper()
				job, doc, pdf := snapshotJobAndDoc(ctx, t, service, id)
				state := snapshot{job: job, doc: doc, pdf: pdf, fingerprints: fingerprintCount()}
				if hosted {
					state.publication = publicationStateContext(ctx, t, service, id)
				}
				return state
			}
			before := readSnapshot()
			ok, err := service.CompleteJob(ctx, *leased, chunks, domain.IndexObservation{})
			require.ErrorIs(t, err, domain.ErrAuthzModeMismatch)
			require.False(t, ok)
			require.Equal(t, before, readSnapshot(), "fenced publication must leave the stored state intact")
			require.Equal(t, acceptedFingerprints, before.fingerprints)
			requireUnpublishedLayout(ctx, t, service, id, backend)

			canceled, cancelRelease := context.WithCancel(ctx)
			cancelRelease()
			require.ErrorIs(t, service.ReleaseJob(canceled, *leased), context.Canceled)
			require.Equal(t, before, readSnapshot(), "canceled release must leave the stored state intact")

			require.NoError(t, service.ReleaseJob(ctx, *leased))
			pending := readCorpusLayoutLease(ctx, t, service, id)
			require.Equal(t, corpusLayoutLease{state: "pending", attempts: 1, pdfRequired: true, due: true}, pending)
			released := readSnapshot()
			require.Equal(t, before.doc, released.doc)
			require.Equal(t, before.pdf, released.pdf)
			require.Equal(t, acceptedFingerprints, released.fingerprints)
			requireUnpublishedLayout(ctx, t, service, id, backend)
			if hosted {
				requireAbandonedCorpusManifest(ctx, t, service, writing)
				require.Equal(t, earlierManifest, readCorpusManifestJSON(ctx, t, service, earlier.revision))
				require.Empty(t, cleanupReasonContext(ctx, t, service, earlier.revision))
			}
			require.ErrorIs(t, service.ReleaseJob(ctx, *leased), domain.ErrJobNotOwned)
			require.Equal(t, released, readSnapshot(), "duplicate release must leave the pending state intact")
			require.Equal(t, pending, readCorpusLayoutLease(ctx, t, service, id))

			require.NoError(t, service.ThawWriters(ctx))
			legacy, err := grants.ClaimPDFJob(ctx, owner, time.Minute, maxAttempts)
			require.NoError(t, err)
			require.Nil(t, legacy)
			require.Equal(t, released, readSnapshot(), "mixed-only claim must leave layout work pending")

			reclaimed, err := claimLayoutForCorpusTest(ctx, grants, owner, maxAttempts)
			require.NoError(t, err)
			require.NotNil(t, reclaimed)
			require.Equal(t, leased.Document, reclaimed.Document)
			require.Equal(t, leased.Incarnation, reclaimed.Incarnation)
			require.Equal(t, leased.Collection, reclaimed.Collection)
			require.Equal(t, leased.PDF, reclaimed.PDF)
			require.Equal(t, owner, reclaimed.LeaseOwner)
			require.Equal(t, leased.LeaseOwner, reclaimed.LeaseOwner)
			require.Equal(t, leased.Attempts, reclaimed.Attempts)
			require.NotEqual(t, leased.LeaseToken, reclaimed.LeaseToken)
			requireStampedCorpusLayoutLease(ctx, t, grants, reclaimed)
			live, err := grants.HeartbeatJob(ctx, *reclaimed, time.Minute)
			require.NoError(t, err)
			require.True(t, live)
			if hosted {
				newAttempt := persistManifestAttemptWithChunk(ctx, t, grants, id, *reclaimed, uuid.NewString())
				require.Equal(t, "writing", manifestStateContext(ctx, t, grants, newAttempt.revision))
				require.Empty(t, cleanupReasonContext(ctx, t, grants, newAttempt.revision))
				require.Equal(t, earlierManifest, readCorpusManifestJSON(ctx, t, service, earlier.revision))
				requireAbandonedCorpusManifest(ctx, t, service, writing)
			}
			beforeReplay := readSnapshot()
			require.Equal(t, released.doc, beforeReplay.doc)
			require.Equal(t, released.pdf, beforeReplay.pdf)
			require.Equal(t, acceptedFingerprints, beforeReplay.fingerprints)
			require.ErrorIs(t, service.ReleaseJob(ctx, *leased), domain.ErrJobNotOwned)
			require.Equal(t, beforeReplay, readSnapshot(), "stale release must leave the reclaimed lease and its writes intact")
		})
	}
}

func requireUnpublishedLayout(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID, backend string) {
	t.Helper()
	var status string
	var chunks int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT status,
		(SELECT count(*) FROM chunks WHERE document_id=$1)
		FROM documents WHERE id=$1`, id).Scan(&status, &chunks))
	require.Equal(t, "processing", status)
	require.Zero(t, chunks)
	switch backend {
	case "pgvector":
		var vectors int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM chunk_vectors WHERE document_id=$1`, id).Scan(&vectors))
		require.Zero(t, vectors)
	case "hosted manifest":
		var pointers int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM hosted_vector_revisions WHERE document_id=$1`, id).Scan(&pointers))
		require.Zero(t, pointers)
	}
}

func readCorpusManifestJSON(ctx context.Context, t *testing.T, c *Client, revision string) string {
	t.Helper()
	var snapshot string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT to_jsonb(m)::text FROM hosted_vector_manifests m
		WHERE revision_id=$1`, revision).Scan(&snapshot))
	return snapshot
}

func requireAbandonedCorpusManifest(ctx context.Context, t *testing.T, c *Client, writing *domain.VectorManifest) {
	t.Helper()
	var state string
	var retiredAt *time.Time
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT state,retired_at FROM hosted_vector_manifests WHERE revision_id=$1`,
		writing.RevisionID).Scan(&state, &retiredAt))
	require.Equal(t, "retired", state)
	require.NotNil(t, retiredAt)
	var documentID, incarnation, collection, namespace, prefix, reason string
	var vectorIDs []string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT document_id::text,incarnation::text,collection,
		vector_namespace,id_prefix,vector_ids,reason FROM hosted_vector_cleanups WHERE revision_id=$1`,
		writing.RevisionID).Scan(&documentID, &incarnation, &collection, &namespace, &prefix, &vectorIDs, &reason))
	require.Equal(t, "abandoned", reason)
	require.Equal(t, string(writing.DocumentID), documentID)
	require.Equal(t, writing.Incarnation, incarnation)
	require.Equal(t, writing.Collection, collection)
	require.Equal(t, writing.VectorNamespace, namespace)
	require.Equal(t, writing.IDPrefix, prefix)
	require.Equal(t, writing.VectorIDs, vectorIDs)
	var count int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM hosted_vector_cleanups WHERE document_id=$1`,
		writing.DocumentID).Scan(&count))
	require.Equal(t, 1, count)
}

func requirePublicationState(t *testing.T, c *Client, job domain.Job, backend string, published bool) {
	t.Helper()
	ctx := t.Context()
	status, count := "processing", 0
	if published {
		status, count = "completed", 1
	}
	var actualStatus string
	var chunks, fingerprints, leases int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT status,
		(SELECT count(*) FROM chunks WHERE document_id=$1),
		(SELECT count(*) FROM document_fingerprints WHERE document_id=$1),
		(SELECT count(*) FROM ingest_jobs WHERE document_id=$1 AND state='running')
		FROM documents WHERE id=$1`, job.Document.ID).Scan(&actualStatus, &chunks, &fingerprints, &leases))
	require.Equal(t, status, actualStatus)
	require.Equal(t, count, chunks)
	require.Equal(t, count, fingerprints)
	require.Equal(t, 1-count, leases)
	switch backend {
	case "pgvector":
		var vectors int
		require.NoError(t, c.Pool.QueryRow(ctx, "SELECT count(*) FROM chunk_vectors WHERE document_id=$1", job.Document.ID).Scan(&vectors))
		require.Equal(t, count, vectors)
	case "hosted manifest":
		state := "writing"
		if published {
			state = "active"
		}
		require.Equal(t, state, manifestState(t, c, job.RevisionID))
		require.Empty(t, cleanupReason(t, c, job.RevisionID))
	}
}

func TestCorpusModeWriterLockPrivileges(t *testing.T) {
	dsn := startTestDatabase(t)
	owner := migrationTestClient(t, dsn)
	applyMigrationsExcept(t, owner, 36)
	ctx := t.Context()
	owner.SetAuthzMode("service")

	// A lock-aware binary cannot admit work before migration 036, although
	// the existing unlocked read is still available.
	require.NoError(t, owner.WritersOpen(ctx))
	_, err := owner.AcceptDocument(ctx, jobsRequest("before writer lock migration"))
	requireSQLState(t, err, "42883")

	roles := newGrantRoles(t, dsn, append(append([]string{}, servingRoles...), "mode_default", "mode_unaffiliated")...)
	grantExec(t, owner, roles.sql(`
		CREATE ROLE {quarry_service} LOGIN;
		CREATE ROLE {quarry_operator} LOGIN;
		CREATE ROLE {quarry_auditor};
		CREATE ROLE {mode_default};
		CREATE ROLE {mode_unaffiliated} LOGIN;
		GRANT {mode_default} TO {mode_unaffiliated};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {quarry_service}, {quarry_operator};
		ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO {mode_default};
	`))
	require.NoError(t, owner.RunMigrations(ctx))
	service, _ := corpusWriterSession(t, owner, roles.name("quarry_service"))
	operator, _ := corpusWriterSession(t, owner, roles.name("quarry_operator"))
	unaffiliated, _ := corpusWriterSession(t, owner, roles.name("mode_unaffiliated"))
	service.SetAuthzMode("service")

	const sharedRead = "SELECT grant_catalog_corpus_authz_mode_shared()"
	for _, client := range []*Client{service, operator, unaffiliated} {
		_, execErr := client.Pool.Exec(ctx, sharedRead)
		requireSQLState(t, execErr, "42501")
	}
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
	for _, client := range []*Client{operator, unaffiliated} {
		_, execErr := client.Pool.Exec(ctx, sharedRead)
		requireSQLState(t, execErr, "42501")
	}
	_, err = service.Pool.Exec(ctx, "SELECT authz_mode FROM grant_catalog_corpus_mode FOR SHARE")
	requireSQLState(t, err, "42501")
	_, err = service.Pool.Exec(ctx, "SELECT * FROM grant_catalog_activate_grants(true)")
	requireSQLState(t, err, "42501")
	activateDeletionForCorpusCutover(t, owner)
	preview, err := operator.ActivateGrants(ctx, true)
	require.NoError(t, err)
	require.False(t, preview.AlreadyActive)

	grantExec(t, owner, "ALTER SCHEMA public RENAME TO corpus_renamed")
	var locked, unlocked string
	require.NoError(t, service.Pool.QueryRow(ctx, `SELECT corpus_renamed.grant_catalog_corpus_authz_mode_shared(),
		corpus_renamed.grant_catalog_corpus_authz_mode()`).Scan(&locked, &unlocked))
	require.Equal(t, "service", locked)
	require.Equal(t, locked, unlocked)

	// The constraint normally rules out an invalid marker. Corrupt it as
	// the owner to verify that neither writer nor readiness trusts it.
	grantExec(t, owner, `ALTER SCHEMA corpus_renamed RENAME TO public;
		ALTER TABLE grant_catalog_corpus_mode DROP CONSTRAINT grant_catalog_corpus_mode_authz_mode_check;
		UPDATE grant_catalog_corpus_mode SET authz_mode='invalid'`)
	for _, corrupt := range []string{"invalid", "missing"} {
		_, err = service.AcceptDocument(ctx, jobsRequest("with "+corrupt+" corpus marker"))
		require.ErrorContains(t, err, "corpus authz mode marker is missing or invalid")
		require.ErrorContains(t, service.WritersOpen(ctx), "corpus authz mode marker is missing or invalid")
		if corrupt == "invalid" {
			grantExec(t, owner, "DELETE FROM grant_catalog_corpus_mode")
		}
	}
}
