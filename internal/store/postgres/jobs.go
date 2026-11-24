package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// AcceptDocument atomically reserves document identity and queues ingestion.
func (c *Client) AcceptDocument(ctx context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	if req.ContentDedup == "" {
		req.ContentDedup = domain.ContentDedupCollection
	}
	if req.PDF != nil {
		if err := validatePDFAdmission(req); err != nil {
			return domain.AcceptResult{}, err
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		result, err := c.acceptDocument(ctx, req, false, false)
		var pgErr *pgconn.PgError
		if attempt == 0 && errors.As(err, &pgErr) && pgErr.Code == "23505" {
			continue
		}
		if err != nil {
			return result, fmt.Errorf("postgres: accept document: %w", err)
		}
		return result, nil
	}
	return domain.AcceptResult{}, errors.New("postgres: acceptance retry exhausted")
}

// AcceptExistingPDF resolves a UUID or a shared original without admitting a new document.
func (c *Client) AcceptExistingPDF(ctx context.Context, req domain.AcceptRequest, includeLegacy bool) (domain.AcceptResult, error) {
	if !req.CallerSupplied && (req.ContentDedup != domain.ContentDedupCollection || !includeLegacy) {
		return domain.AcceptResult{}, errors.New("postgres: anonymous PDF lookup requires collection deduplication in mixed mode")
	}
	return c.acceptDocument(ctx, req, true, includeLegacy)
}

func identityFingerprint(policy domain.ContentDedup, id domain.DocumentID, fingerprint string) string {
	if policy == domain.ContentDedupOff {
		return chunker.Fingerprint("quarry.document.v1\x00" + string(id) + "\x00" + fingerprint)
	}
	return fingerprint
}

func (c *Client) acceptDocument(ctx context.Context, req domain.AcceptRequest, existingPDFOnly, includeLegacy bool) (domain.AcceptResult, error) {
	result := domain.AcceptResult{DocumentID: req.Document.ID}
	// Reads after the advisory lock wait must see the previous holder's commit.
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// Serialize admission so concurrent requests cannot overfill the durable queue.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
		return result, err
	}
	if req.EvaluationReceipt != "" {
		if req.EvaluationVectorIdentity == "" {
			return result, errors.New("evaluation fixture vector store is unspecified")
		}
		if lockErr := lockFixtureDocument(ctx, tx, string(req.Document.ID)); lockErr != nil {
			return result, lockErr
		}
		var reserved bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM evaluation_fixtures WHERE document_id=$1)`, string(req.Document.ID)).Scan(&reserved)
		if err != nil {
			return result, err
		}
		if reserved {
			result.Outcome = domain.AcceptConflict
			return result, nil
		}
	}
	var id, state string
	if req.CallerSupplied {
		var fingerprint *string
		var content, collection string
		var policy domain.ContentDedup
		err = tx.QueryRow(ctx, `SELECT status,fingerprint,content,collection,content_dedup FROM documents WHERE id=$1 FOR UPDATE`, string(req.Document.ID)).Scan(&state, &fingerprint, &content, &collection, &policy)
		if err == nil {
			if req.EvaluationReceipt != "" || collection != req.Collection {
				result.Outcome = domain.AcceptConflict
				return result, nil
			}
			if fingerprint == nil {
				legacy := chunker.ScopedFingerprint(collection, content)
				fingerprint = &legacy
			}
			matches := *fingerprint == identityFingerprint(policy, req.Document.ID, req.Fingerprint)
			if existingPDFOnly {
				result.Outcome, err = matchExistingPDF(ctx, tx, req, policy, *fingerprint, includeLegacy)
				if err != nil || result.Outcome != domain.AcceptAccepted {
					return result, err
				}
				matches = true
			} else if !matches && req.PDF != nil {
				matches, err = matchesLegacyPDF(ctx, tx, req, policy, *fingerprint)
				if err != nil {
					return result, err
				}
			}
			if !matches {
				result.Outcome = domain.AcceptConflict
				return result, nil
			}
			id = string(req.Document.ID)
		}
	} else if req.ContentDedup == domain.ContentDedupCollection {
		if req.PDF != nil || existingPDFOnly {
			id, state, err = findSharedPDF(ctx, tx, req)
		} else {
			err = tx.QueryRow(ctx, `SELECT d.id::text,d.status FROM document_fingerprints f JOIN documents d ON d.id=f.document_id WHERE f.fingerprint=$1 FOR UPDATE OF d`, req.Fingerprint).Scan(&id, &state)
		}
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	if existingPDFOnly && id == "" {
		result.Outcome = domain.AcceptNeedsSource
		return result, nil
	}
	if id == "" && req.TryLegacyPDFText && !req.CallerSupplied && req.ContentDedup == domain.ContentDedupCollection && req.PDF != nil && req.PDF.Coverage == domain.PDFComplete {
		result.Outcome = domain.AcceptNeedsPDFText
		return result, nil
	}
	if id != "" {
		result.DocumentID = domain.DocumentID(id)
		switch state {
		case "completed":
			result.Outcome = domain.AcceptDuplicate
			return result, nil
		case "processing":
			result.Outcome = domain.AcceptInProgress
			return result, nil
		case "failed":
			// Terminal failure removed the job, so a retry needs queue capacity.
		default:
			return result, fmt.Errorf("unexpected document status %q", state)
		}
	}
	var reopenPDF, rotateWindow bool
	if id != "" && (req.PDF != nil || existingPDFOnly) {
		var pdfCoverage domain.PDFCoverage
		var unusable, expired, intentExpired bool
		err = tx.QueryRow(ctx, `SELECT p.coverage,octet_length(d.content)<$2,
			COALESCE(COALESCE(p.accepted_at,p.start_bound_at,p.started_at)+make_interval(secs=>$3)<=clock_timestamp(),FALSE),
			COALESCE(p.started_at+make_interval(secs=>$3)<=clock_timestamp(),FALSE)
			FROM document_pdf_extractions p JOIN documents d ON d.id=p.document_id WHERE d.id=$1`, id, domain.MinPDFTextBytes, domain.PDFOCRLifetime.Seconds()).Scan(&pdfCoverage, &unusable, &expired, &intentExpired)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return result, err
		}
		unusableCheckpoint := pdfCoverage == domain.PDFPartial && unusable
		reopenPDF = pdfCoverage == domain.PDFFailed || unusableCheckpoint
		if reopenPDF && !req.CallerSupplied {
			result.Outcome = domain.AcceptOCRRetryRequired
			return result, nil
		}
		if reopenPDF && (unusableCheckpoint || intentExpired) && !expired {
			result.Outcome = domain.AcceptOCRRetryAfterExpiry
			return result, nil
		}
		rotateWindow = expired
	}

	var count int
	if countErr := tx.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM ingest_jobs LIMIT $1) q`, req.MaxPending).Scan(&count); countErr != nil {
		return result, countErr
	}
	if count >= req.MaxPending {
		result.Outcome = domain.AcceptQueueFull
		return result, nil
	}
	if id != "" {
		if _, err = tx.Exec(ctx, `UPDATE documents SET status='processing',last_error=NULL,index_visibility=NULL,index_visibility_observed_at=NULL,updated_at=NOW() WHERE id=$1`, id); err != nil {
			return result, err
		}
		if reopenPDF {
			if requeueErr := requeuePDF(ctx, tx, id, rotateWindow); requeueErr != nil {
				return result, requeueErr
			}
		} else if _, err = tx.Exec(ctx, `INSERT INTO ingest_jobs(document_id) VALUES($1) ON CONFLICT DO NOTHING`, id); err != nil {
			return result, err
		}
		result.Outcome = domain.AcceptRequeued
		return result, tx.Commit(ctx)
	}
	if req.RequireSource && req.Document.S3Key == nil {
		result.Outcome = domain.AcceptNeedsSource
		return result, nil
	}
	metadata := req.Document.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["doc_type"] = req.DocType
	if req.DocType == "" {
		metadata["doc_type"] = domain.DefaultDocType
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return result, err
	}
	preview := []rune(req.Document.Content)
	if len(preview) > 120 {
		preview = preview[:120]
	}
	fingerprint := identityFingerprint(req.ContentDedup, req.Document.ID, req.Fingerprint)
	insert := `INSERT INTO documents(id,content,content_preview,metadata,chunk_count,file_name,file_type,s3_key,doc_type,status,fingerprint,collection,content_dedup) VALUES($1,$2,$3,$4,0,$5,$6,$7,$8,'processing',$9,$10,$11)`
	args := []any{string(req.Document.ID), req.Document.Content, string(preview), encoded, req.Document.FileName, req.Document.FileType, req.Document.S3Key, metadata["doc_type"], fingerprint, req.Collection, req.ContentDedup}
	if req.EvaluationReceipt == "" {
		_, err = tx.Exec(ctx, insert, args...)
	} else {
		err = tx.QueryRow(ctx, insert+` RETURNING incarnation::text`, args...).Scan(&result.Incarnation)
	}
	if err != nil {
		return result, err
	}
	if req.EvaluationReceipt != "" {
		if _, err = tx.Exec(ctx, `INSERT INTO evaluation_fixtures(document_id, receipt, incarnation, active_document_id, vector_identity)
			VALUES($1,$2,$3,$1,$4)`, string(req.Document.ID), req.EvaluationReceipt, result.Incarnation, req.EvaluationVectorIdentity); err != nil {
			return result, err
		}
	}
	if req.Document.TextSource == "ocr_pending" {
		if _, err = tx.Exec(ctx, `UPDATE documents SET text_source='ocr_pending' WHERE id=$1`, string(req.Document.ID)); err != nil {
			return result, err
		}
	}
	if req.ContentDedup == domain.ContentDedupCollection {
		query := `INSERT INTO document_fingerprints(fingerprint,document_id) VALUES($1,$2)`
		if req.CallerSupplied {
			query += ` ON CONFLICT(fingerprint) DO NOTHING`
		}
		if _, err = tx.Exec(ctx, query, fingerprint, string(req.Document.ID)); err != nil {
			return result, err
		}
	}
	if req.PDF != nil {
		err = insertPDFJob(ctx, tx, req)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO ingest_jobs(document_id) VALUES($1)`, string(req.Document.ID))
	}
	if err != nil {
		return result, err
	}
	result.Outcome = domain.AcceptAccepted
	return result, tx.Commit(ctx)
}

const claimJobPrefix = `WITH claimed AS (
	UPDATE ingest_jobs j SET state='running',attempts=j.attempts+1,lease_owner=$1,lease_until=NOW()+make_interval(secs=>$2),
		pdf_claim_attempt=CASE WHEN $3 THEN j.attempts+1 END,lease_token=gen_random_uuid(),updated_at=NOW()
	WHERE j.document_id=(SELECT ready.document_id FROM ingest_jobs ready
		WHERE ($3 OR NOT EXISTS(SELECT 1 FROM ingest_jobs WHERE pdf_required))
		AND ((ready.state='pending' AND ready.next_attempt_at<=NOW()) OR (ready.state='running' AND ready.lease_until<NOW()))
		AND `

const claimJobSuffix = `
		ORDER BY ready.next_attempt_at,ready.created_at FOR UPDATE SKIP LOCKED LIMIT 1)
	RETURNING j.document_id,j.attempts,j.lease_token)
	SELECT d.id::text,j.attempts,d.content,d.metadata,d.file_name,d.file_type,d.s3_key,d.doc_type,d.fingerprint,d.collection,
		d.text_source,COALESCE(d.incarnation,d.id)::text,j.lease_token::text,p.profile,p.coverage,p.manifest
	FROM claimed j JOIN documents d ON d.id=j.document_id
	LEFT JOIN document_pdf_extractions p ON p.document_id=d.id`

// The reservation predicate is owner-defined, so a serving login needs no
// privilege on the receipt table to lease ordinary work.
const claimGuardedJob = claimJobPrefix + `evaluation_fixture_claimable(ready.document_id, NULLIF($4::text, ''))` + claimJobSuffix

const claimBeforeFixtureMigration = claimJobPrefix + `to_regclass('evaluation_fixtures') IS NULL` + claimJobSuffix

// ClaimJob leases ordinary work on both current and pre-fixture schemas.
func (c *Client) ClaimJob(ctx context.Context, owner string, lease time.Duration, maxAttempts int) (*domain.Job, error) {
	return c.ClaimJobForFixtureStore(ctx, owner, lease, maxAttempts, "")
}

// ClaimJobForFixtureStore permits fixture jobs only for their reserved vector store.
func (c *Client) ClaimJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*domain.Job, error) {
	return c.claimGuarded(ctx, owner, lease, maxAttempts, false, vectorIdentity)
}

// ClaimPDFJob also leases jobs that require a mixed-PDF worker.
func (c *Client) ClaimPDFJob(ctx context.Context, owner string, lease time.Duration, maxAttempts int) (*domain.Job, error) {
	return c.ClaimPDFJobForFixtureStore(ctx, owner, lease, maxAttempts, "")
}

// ClaimPDFJobForFixtureStore applies both the PDF and fixture claim rules.
func (c *Client) ClaimPDFJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*domain.Job, error) {
	return c.claimGuarded(ctx, owner, lease, maxAttempts, true, vectorIdentity)
}

func (c *Client) claimGuarded(ctx context.Context, owner string, lease time.Duration, maxAttempts int, pdfCapable bool, vectorIdentity string) (*domain.Job, error) {
	job, err := c.claimJob(ctx, owner, maxAttempts, claimGuardedJob, owner, lease.Seconds(), pdfCapable, vectorIdentity)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42883" {
		// Before the fixture migration, only claim while the reservation table is absent.
		return c.claimJob(ctx, owner, maxAttempts, claimBeforeFixtureMigration, owner, lease.Seconds(), pdfCapable)
	}
	return job, err
}

func (c *Client) claimJob(ctx context.Context, owner string, maxAttempts int, statement string, args ...any) (*domain.Job, error) {
	for range 10 {
		j := &domain.Job{LeaseOwner: owner}
		var id string
		var metadata, manifest []byte
		var fingerprint, profile *string
		var coverage *domain.PDFCoverage
		err := c.Pool.QueryRow(ctx, statement, args...).
			Scan(&id, &j.Attempts, &j.Document.Content, &metadata, &j.Document.FileName, &j.Document.FileType,
				&j.Document.S3Key, &j.DocType, &fingerprint, &j.Collection, &j.Document.TextSource, &j.Incarnation, &j.LeaseToken, &profile, &coverage, &manifest)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("postgres: claim job: %w", err)
		}
		j.Document.ID = domain.DocumentID(id)
		if profile != nil && coverage != nil {
			j.PDF = &domain.PDFExtraction{Profile: *profile, Coverage: *coverage}
		}
		if j.Attempts > maxAttempts {
			if failErr := c.FailJob(ctx, *j, "exceeded maximum attempts"); failErr != nil && !errors.Is(failErr, domain.ErrJobNotOwned) {
				return nil, failErr
			}
			continue
		}
		if fingerprint != nil {
			j.Document.Fingerprint = *fingerprint
		}
		if j.PDF != nil && j.PDF.Coverage == domain.PDFPending {
			j.PDF.Pages, err = domain.DecodePDFPages(manifest)
			if err != nil {
				return nil, fmt.Errorf("postgres: load PDF job manifest: %w", err)
			}
		}
		if err = json.Unmarshal(metadata, &j.Document.Metadata); err != nil {
			return nil, fmt.Errorf("postgres: decode job metadata: %w", err)
		}
		return j, nil
	}
	return nil, nil
}

// jobClaim identifies one lease of a job by $1 document, $2 owner, $3 attempt
// and $4 lease token. Owner and attempt numbers repeat after release or
// replacement, so the token is part of the identity.
const jobClaim = `document_id=$1 AND lease_owner=$2 AND attempts=$3 AND lease_token IS NOT DISTINCT FROM $4 AND state='running'`

// jobLive also requires the database clock to be before the lease deadline.
const jobLive = jobClaim + ` AND lease_until>clock_timestamp()`

// lockClaimedJob takes the job row lock of a claim, waiting for any other
// transaction that holds it. A statement that qualifies the row before such a
// wait can still apply its effect afterwards if the holder released the row
// unchanged, so a lease deadline must be tested by a later statement.
func lockClaimedJob(ctx context.Context, tx pgx.Tx, j domain.Job) (bool, error) {
	var locked int
	err := tx.QueryRow(ctx, `SELECT 1 FROM ingest_jobs WHERE `+jobClaim+` AND EXISTS(SELECT 1 FROM documents WHERE id=$1 AND COALESCE(incarnation,id)=$5) FOR UPDATE`, string(j.Document.ID), j.LeaseOwner, j.Attempts, j.LeaseToken, j.Incarnation).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// HeartbeatJob renews a live lease without reviving an expired attempt.
func (c *Client) HeartbeatJob(ctx context.Context, j domain.Job, lease time.Duration) (bool, error) {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if held, lockErr := lockClaimedJob(ctx, tx, j); lockErr != nil || !held {
		return false, lockErr
	}
	tag, err := tx.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp()+make_interval(secs=>$5),updated_at=NOW() WHERE `+jobLive, string(j.Document.ID), j.LeaseOwner, j.Attempts, j.LeaseToken, lease.Seconds())
	if err != nil || tag.RowsAffected() != 1 {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// CompleteJob commits chunks and completion only while the document and lease remain valid.
func (c *Client) CompleteJob(ctx context.Context, j domain.Job, chunks []domain.Chunk, observation domain.IndexObservation) (bool, error) {
	if j.LeaseToken == nil && observation.Visibility != "" {
		return false, nil
	}
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var policy domain.ContentDedup
	var fingerprint *string
	var collection string
	err = tx.QueryRow(ctx, `SELECT content_dedup,fingerprint,collection FROM documents WHERE id=$1 AND COALESCE(incarnation,id)=$2 AND status='processing' FOR UPDATE`, string(j.Document.ID), j.Incarnation).Scan(&policy, &fingerprint, &collection)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	leaseArgs := []any{string(j.Document.ID), j.LeaseOwner, j.Attempts, j.LeaseToken}
	// Look without locking: persistence can be slow, and the heartbeat needs the
	// job row to renew the lease while it runs.
	var live int
	err = tx.QueryRow(ctx, `SELECT 1 FROM ingest_jobs WHERE `+jobLive, leaseArgs...).Scan(&live)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM chunks WHERE document_id=$1`, string(j.Document.ID)); err != nil {
		return false, err
	}
	batch := &pgx.Batch{}
	for _, ch := range chunks {
		batch.Queue(`INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,$3,$4,$5)`, string(ch.ID), string(j.Document.ID), ch.Index, ch.Text, ch.TokenCount)
	}
	if batchErr := tx.SendBatch(ctx, batch).Close(); batchErr != nil {
		return false, batchErr
	}
	if dimensions := int(c.vectorDimensions.Load()); dimensions != 0 {
		if _, err = tx.Exec(ctx, `DELETE FROM chunk_vectors WHERE document_id=$1`, string(j.Document.ID)); err != nil {
			return false, err
		}
		vectors := make([]domain.Chunk, len(chunks))
		for i, ch := range chunks {
			ch.DocumentID = j.Document.ID
			ch.Metadata = maps.Clone(ch.Metadata)
			if ch.Metadata == nil {
				ch.Metadata = make(map[string]any)
			}
			ch.Metadata["doc_type"] = j.DocType
			ch.Metadata["collection"] = j.Collection
			vectors[i] = ch
		}
		if err = insertVectors(ctx, tx, vectors, dimensions); err != nil {
			return false, fmt.Errorf("postgres: complete vectors: %w", err)
		}
		observation = domain.IndexObservation{Visibility: domain.VisibilityTransactional}
	}
	if policy == domain.ContentDedupCollection {
		if fingerprint == nil {
			legacy := chunker.ScopedFingerprint(collection, j.Document.Content)
			fingerprint = &legacy
			if _, err = tx.Exec(ctx, `UPDATE documents SET fingerprint=$2 WHERE id=$1`, string(j.Document.ID), *fingerprint); err != nil {
				return false, err
			}
		}
		if _, err = tx.Exec(ctx, `INSERT INTO document_fingerprints(fingerprint,document_id) VALUES($1,$2) ON CONFLICT(fingerprint) DO NOTHING`, *fingerprint, string(j.Document.ID)); err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE documents SET status='completed',chunk_count=$2,completed_at=statement_timestamp(),updated_at=statement_timestamp(),last_error=NULL,
        index_visibility=NULLIF($3,''),index_visibility_observed_at=CASE WHEN $3='transactional' THEN statement_timestamp() ELSE $4 END WHERE id=$1`, string(j.Document.ID), len(chunks), observation.Visibility, observation.ObservedAt); err != nil {
		return false, err
	}
	// Persistence can outlast the lease, and a renewal may have extended it.
	// Lock the job row only now, then test the lease in the next statement.
	if held, lockErr := lockClaimedJob(ctx, tx, j); lockErr != nil || !held {
		return false, lockErr
	}
	tag, err := tx.Exec(ctx, `DELETE FROM ingest_jobs WHERE `+jobLive, leaseArgs...)
	if err != nil || tag.RowsAffected() != 1 {
		return false, err
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return false, commitErr
	}
	return true, nil
}

func jobError(message string) string {
	r := []rune(message)
	if len(r) > 1000 {
		r = r[:1000]
	}
	return string(r)
}

func (c *Client) changeJob(ctx context.Context, j domain.Job, query string, args []any, message *string) error {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var id string
	err = tx.QueryRow(ctx, `SELECT id::text FROM documents WHERE id=$1 AND COALESCE(incarnation,id)=$2 FOR UPDATE`, string(j.Document.ID), j.Incarnation).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrJobNotOwned
	}
	if err != nil {
		return err
	}
	// Lock the claim before the guarded statement so that its lease check runs
	// after any wait for another transaction's lock on the row.
	if held, lockErr := lockClaimedJob(ctx, tx, j); lockErr != nil || !held {
		if lockErr != nil {
			return lockErr
		}
		return domain.ErrJobNotOwned
	}
	params := []any{id, j.LeaseOwner, j.Attempts, j.LeaseToken}
	params = append(params, args...)
	tag, err := tx.Exec(ctx, query, params...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrJobNotOwned
	}
	if message != nil {
		state := "processing"
		if args == nil {
			state = "failed"
			if j.PDF != nil {
				if _, err = tx.Exec(ctx, `UPDATE document_pdf_extractions SET coverage='failed' WHERE document_id=$1 AND coverage='pending'`, id); err != nil {
					return err
				}
			}
		}
		_, err = tx.Exec(ctx, `UPDATE documents SET status=$2,last_error=$3,updated_at=NOW() WHERE id=$1`, id, state, jobError(*message))
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// RetryJob reschedules an owned attempt and records its latest failure.
func (c *Client) RetryJob(ctx context.Context, j domain.Job, delay time.Duration, message string) error {
	return c.changeJob(ctx, j, `UPDATE ingest_jobs SET state='pending',lease_owner=NULL,lease_until=NULL,lease_token=NULL,next_attempt_at=NOW()+make_interval(secs=>$5),last_error=$6,updated_at=NOW() WHERE `+jobLive, []any{delay.Seconds(), jobError(message)}, &message)
}

// FailJob records a terminal failure and removes the owned work.
func (c *Client) FailJob(ctx context.Context, j domain.Job, message string) error {
	return c.changeJob(ctx, j, `DELETE FROM ingest_jobs WHERE `+jobLive, nil, &message)
}

// ReleaseJob returns interrupted work without consuming an attempt.
func (c *Client) ReleaseJob(ctx context.Context, j domain.Job) error {
	return c.changeJob(ctx, j, `UPDATE ingest_jobs SET state='pending',lease_owner=NULL,lease_until=NULL,lease_token=NULL,next_attempt_at=NOW(),attempts=GREATEST(attempts-1,0),updated_at=NOW() WHERE `+jobClaim, nil, nil)
}

// DocumentExists reports whether deletion has removed a document.
func (c *Client) DocumentExists(ctx context.Context, id domain.DocumentID) (bool, error) {
	var exists bool
	err := c.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM documents WHERE id=$1)`, string(id)).Scan(&exists)
	return exists, err
}

// StoreOCRText saves detected content only while its ingestion lease is live.
func (c *Client) StoreOCRText(ctx context.Context, j domain.Job, text string) error {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var id string
	err = tx.QueryRow(ctx, `SELECT d.id::text FROM documents d JOIN ingest_jobs j ON j.document_id=d.id WHERE d.id=$1 AND d.status='processing' AND COALESCE(d.incarnation,d.id)=$5 AND j.lease_owner=$2 AND j.attempts=$3 AND j.lease_token IS NOT DISTINCT FROM $4 AND j.state='running' AND j.lease_until>clock_timestamp() FOR UPDATE OF d,j`, string(j.Document.ID), j.LeaseOwner, j.Attempts, j.LeaseToken, j.Incarnation).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrJobNotOwned
	}
	if err != nil {
		return err
	}
	preview := []rune(text)
	if len(preview) > 120 {
		preview = preview[:120]
	}
	// Recheck the lease after any wait for the document and job row locks.
	tag, err := tx.Exec(ctx, `UPDATE documents d SET content=$2,content_preview=$3,text_source='ocr',updated_at=NOW() WHERE d.id=$1 AND EXISTS(SELECT 1 FROM ingest_jobs j WHERE j.document_id=d.id AND j.lease_owner=$4 AND j.attempts=$5 AND j.lease_token IS NOT DISTINCT FROM $6 AND j.state='running' AND j.lease_until>clock_timestamp())`, id, text, string(preview), j.LeaseOwner, j.Attempts, j.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrJobNotOwned
	}
	return tx.Commit(ctx)
}
