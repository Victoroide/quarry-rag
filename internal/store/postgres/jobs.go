package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
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
	if err := validateOwnedRequest(req, false); err != nil {
		return domain.AcceptResult{}, err
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
	if err := validateOwnedRequest(req, true); err != nil {
		return domain.AcceptResult{}, err
	}
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
	if req.SourceUpload != nil && req.Document.S3Key == nil {
		return result, errors.New("source upload link requires an object key")
	}
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
	if gateErr := c.gate(ctx, tx); gateErr != nil {
		if errors.Is(gateErr, domain.ErrWritersFrozen) {
			result.Outcome = domain.AcceptFrozen
			return result, nil
		}
		return result, gateErr
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
	owned := req.Owner != nil
	if !owned && c.configuredAuthzMode() == "grants" {
		return result, errors.New("postgres: grants mode requires an owner binding")
	}
	var ownedTarget domain.OwnedTarget
	if owned {
		if ownedTarget, err = authorizeOwnedAcceptance(ctx, tx, req); err != nil {
			return result, err
		}
		if !time.Now().Before(req.Owner.ValidUntil) {
			result.Outcome = domain.AcceptUnauthorized
			return result, nil
		}
		switch ownedTarget.Decision {
		case domain.OwnedForbidden:
			result.Outcome = domain.AcceptForbidden
			return result, nil
		case domain.OwnedNotFound:
			result.Outcome = domain.AcceptNotFound
			return result, nil
		}
		result.Incarnation = req.Owner.Incarnation
	}
	if req.CallerSupplied && (!owned || ownedTarget.Decision == domain.OwnedExisting) {
		var fingerprint *string
		var content, collection string
		var policy domain.ContentDedup
		lookup, lookupArgs := `SELECT status,fingerprint,content,collection,content_dedup FROM documents WHERE id=$1 FOR UPDATE`, []any{string(req.Document.ID)}
		if owned {
			lookup = `SELECT status,fingerprint,content,collection,content_dedup FROM documents WHERE id=$1 AND COALESCE(incarnation,id)=$2::uuid FOR UPDATE`
			lookupArgs = append(lookupArgs, req.Owner.Incarnation)
		}
		err = tx.QueryRow(ctx, lookup, lookupArgs...).Scan(&state, &fingerprint, &content, &collection, &policy)
		if errors.Is(err, pgx.ErrNoRows) && owned {
			// Deleted or replaced since the permission check.
			result.Outcome = domain.AcceptNotFound
			return result, nil
		}
		if err == nil {
			if req.EvaluationReceipt != "" || collection != req.Collection {
				result.Outcome = domain.AcceptConflict
				return result, nil
			}
			if state == "deleting" {
				result.Outcome = domain.AcceptDeletionConflict
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
				var needsParserText bool
				matches, needsParserText, err = matchesLegacyPDF(ctx, tx, req, policy, *fingerprint)
				if err != nil {
					return result, err
				}
				if !matches && needsParserText {
					// Release the transaction before the caller extracts the parser identity.
					result.Outcome = domain.AcceptNeedsPDFText
					return result, nil
				}
			}
			if !matches {
				result.Outcome = domain.AcceptConflict
				return result, nil
			}
			id = string(req.Document.ID)
		}
	} else if !owned && req.ContentDedup == domain.ContentDedupCollection {
		if req.PDF != nil || existingPDFOnly {
			id, state, err = findSharedPDF(ctx, tx, req)
		} else {
			err = tx.QueryRow(ctx, `SELECT d.id::text,d.status FROM document_fingerprints f JOIN documents d ON d.id=f.document_id
				WHERE f.fingerprint=$1 AND d.status<>'deleting' FOR UPDATE OF d`, req.Fingerprint).Scan(&id, &state)
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
		case "deleting":
			result.Outcome = domain.AcceptDeletionConflict
			return result, nil
		case "completed":
			result.Outcome = domain.AcceptDuplicate
			if owned {
				if auditErr := recordOwnedIngestAudit(ctx, tx, req, result.Outcome); auditErr != nil {
					return result, auditErr
				}
				return result, tx.Commit(ctx)
			}
			return result, nil
		case "processing":
			result.Outcome = domain.AcceptInProgress
			if owned {
				if auditErr := recordOwnedIngestAudit(ctx, tx, req, result.Outcome); auditErr != nil {
					return result, auditErr
				}
				return result, tx.Commit(ctx)
			}
			return result, nil
		case "failed":
			// Terminal failure removed the job, so a retry needs queue capacity.
		default:
			return result, fmt.Errorf("unexpected document status %q", state)
		}
	}
	var reopenPDF, rotateWindow bool
	var pdfProfile string
	if id != "" {
		var pdfCoverage domain.PDFCoverage
		var unusable, expired, intentExpired bool
		err = tx.QueryRow(ctx, `SELECT p.profile,p.coverage,octet_length(d.content)<$2,
			COALESCE(COALESCE(p.accepted_at,p.start_bound_at,p.started_at)+make_interval(secs=>$3)<=clock_timestamp(),FALSE),
			COALESCE(p.started_at+make_interval(secs=>$3)<=clock_timestamp(),FALSE)
			FROM document_pdf_extractions p JOIN documents d ON d.id=p.document_id WHERE d.id=$1`, id, domain.MinPDFTextBytes, domain.PDFOCRLifetime.Seconds()).Scan(&pdfProfile, &pdfCoverage, &unusable, &expired, &intentExpired)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return result, err
		}
		if err == nil && (req.PDF != nil || existingPDFOnly) {
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
	}

	// An unfinished or held deletion keeps its identity until it is released.
	var reserved bool
	if holdErr := tx.QueryRow(ctx, `SELECT document_deletion_reserved($1)`, string(req.Document.ID)).Scan(&reserved); holdErr != nil {
		return result, holdErr
	}
	if reserved {
		result.Outcome = domain.AcceptDeletionConflict
		return result, nil
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
		if reopenPDF {
			if requeueErr := requeuePDF(ctx, tx, id, pdfProfile, rotateWindow); requeueErr != nil {
				return result, requeueErr
			}
			if _, err = tx.Exec(ctx, `UPDATE documents SET status='processing',last_error=NULL,index_visibility=NULL,index_visibility_observed_at=NULL,updated_at=NOW() WHERE id=$1`, id); err != nil {
				return result, err
			}
		} else {
			if pdfProfile == domain.LayoutPDFProfile {
				if compatErr := requirePDFQueueCompatibility(ctx, tx, pdfProfile); compatErr != nil {
					return result, compatErr
				}
			}
			if _, err = tx.Exec(ctx, `UPDATE documents SET status='processing',last_error=NULL,index_visibility=NULL,index_visibility_observed_at=NULL,updated_at=NOW() WHERE id=$1`, id); err != nil {
				return result, err
			}
			if _, err = tx.Exec(ctx, `INSERT INTO ingest_jobs(document_id,pdf_required) VALUES($1,$2) ON CONFLICT DO NOTHING`, id, pdfProfile == domain.LayoutPDFProfile); err != nil {
				return result, err
			}
		}
		result.Outcome = domain.AcceptRequeued
		if owned {
			if auditErr := recordOwnedIngestAudit(ctx, tx, req, result.Outcome); auditErr != nil {
				return result, auditErr
			}
		}
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
	// A capable backend selects the tagged format only while it holds the
	// activation marker in share mode, through an owner-defined function that
	// needs no table privilege.
	scoped := false
	if req.SupportsScopedVectors && c.vectorDimensions.Load() == 0 {
		query := `SELECT document_deletion_active_shared()`
		if owned {
			query = `SELECT grant_catalog_corpus_authz_mode() = 'grants' OR document_deletion_active_shared()`
		}
		if err = tx.QueryRow(ctx, query).Scan(&scoped); err != nil {
			return result, fmt.Errorf("postgres: read deletion mode: %w", err)
		}
	}
	insert := `INSERT INTO documents(id,content,content_preview,metadata,chunk_count,
		file_name,file_type,s3_key,doc_type,status,fingerprint,collection,content_dedup,scoped_vectors)
		VALUES($1,$2,$3,$4,0,$5,$6,$7,$8,'processing',$9,$10,$11,$12)`
	args := []any{string(req.Document.ID), req.Document.Content, string(preview), encoded, req.Document.FileName, req.Document.FileType, req.Document.S3Key, metadata["doc_type"], fingerprint, req.Collection, req.ContentDedup, scoped}
	if owned {
		insert = `INSERT INTO documents(id,content,content_preview,metadata,chunk_count,
		file_name,file_type,s3_key,doc_type,status,fingerprint,collection,content_dedup,scoped_vectors,incarnation)
		VALUES($1,$2,$3,$4,0,$5,$6,$7,$8,'processing',$9,$10,$11,$12,$13::uuid)`
		args = append(args, req.Owner.Incarnation)
	}
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
	if owned {
		bound, bindErr := bindAcceptedDocument(ctx, tx, req, req.CallerSupplied)
		if bindErr != nil {
			return result, bindErr
		}
		if !bound {
			result.Outcome = domain.AcceptForbidden
			return result, nil
		}
	}
	if req.SourceUpload != nil {
		// Cleanup claims a staged upload under a row lock, so exactly one of
		// the claim and this adoption succeeds.
		tag, linkErr := tx.Exec(ctx, `UPDATE source_uploads SET state='linked',linked_document_id=$2,linked_at=NOW(),acknowledged_at=COALESCE(acknowledged_at,NOW()),version_id=COALESCE(NULLIF($5,''),version_id)
			WHERE id=$1 AND state='staged' AND document_id=$2 AND collection=$3 AND object_key=$4`,
			req.SourceUpload.ID, string(req.Document.ID), req.Collection, *req.Document.S3Key, req.SourceUpload.VersionID)
		if linkErr != nil {
			return result, linkErr
		}
		if tag.RowsAffected() != 1 {
			result.Outcome = domain.AcceptUploadExpired
			return result, nil
		}
	}
	result.Outcome = domain.AcceptAccepted
	if owned {
		if auditErr := recordOwnedIngestAudit(ctx, tx, req, result.Outcome); auditErr != nil {
			return result, auditErr
		}
	}
	return result, tx.Commit(ctx)
}

func recordOwnedIngestAudit(ctx context.Context, tx pgx.Tx, req domain.AcceptRequest, outcome domain.AcceptOutcome) error {
	reason := "accepted"
	switch outcome {
	case domain.AcceptDuplicate:
		reason = "already_ingested"
	case domain.AcceptInProgress:
		reason = "in_progress"
	case domain.AcceptRequeued:
		reason = "requeued"
	}
	return recordSecurityAuditTx(ctx, tx, domain.SecurityAuditEvent{
		RequestID:   logctx.RequestID(ctx),
		TenantID:    req.Owner.Principal.TenantID,
		PrincipalID: req.Owner.Principal.ID,
		Action:      "ingest",
		Outcome:     "allow",
		Reason:      reason,
		DocumentID:  string(req.Document.ID),
		Incarnation: req.Owner.Incarnation,
	})
}

const claimJobUpdateHeader = `WITH claimed AS (
	UPDATE ingest_jobs j SET state='running',attempts=j.attempts+1,lease_owner=$1,lease_until=clock_timestamp()+make_interval(secs=>$2),
		pdf_claim_attempt=CASE WHEN ($3 OR $4) THEN j.attempts+1 END`

const claimJobSetLayout = `,
		layout_claim_attempt=CASE WHEN $4 AND EXISTS(
			SELECT 1 FROM document_pdf_extractions lp WHERE lp.document_id=j.document_id AND lp.profile='` + domain.LayoutPDFProfile + `'
		) THEN j.attempts+1 END`

const claimJobTail = `,lease_token=gen_random_uuid(),updated_at=now()`

const claimReadyPrefix = `
	FROM documents d WHERE j.document_id=d.id AND d.status='processing'
	AND j.document_id=(SELECT ready.document_id FROM ingest_jobs ready
		JOIN documents candidate ON candidate.id=ready.document_id AND candidate.status='processing'
		LEFT JOIN document_pdf_extractions candidate_pdf ON candidate_pdf.document_id=ready.document_id
		WHERE (($3 OR $4) OR NOT EXISTS(SELECT 1 FROM ingest_jobs WHERE pdf_required))
		AND `

const claimProfileCurrent = `(
			(candidate_pdf.document_id IS NULL AND NOT ready.pdf_required)
			OR (candidate_pdf.profile='` + domain.MixedPDFProfile + `' AND (NOT ready.pdf_required OR $3))
			OR (candidate_pdf.profile='` + domain.LayoutPDFProfile + `' AND $4)
		)`

const claimProfileLegacy = `(
			(candidate_pdf.document_id IS NULL AND NOT ready.pdf_required)
			OR (candidate_pdf.profile='` + domain.MixedPDFProfile + `' AND (NOT ready.pdf_required OR $3))
		)`

const claimReadySchedule = `
		AND ((ready.state='pending' AND ready.next_attempt_at<=(SELECT clock_timestamp()))
			OR (ready.state='running' AND ready.lease_until<=(SELECT clock_timestamp())))
		AND (candidate.vector_mutation_until IS NULL OR candidate.vector_mutation_until<=clock_timestamp())
		AND `

const claimJobSuffix = `
		ORDER BY ready.next_attempt_at,ready.created_at FOR UPDATE OF ready SKIP LOCKED LIMIT 1)
	RETURNING j.document_id,j.attempts,j.lease_token)
	SELECT d.id::text,j.attempts,d.content,d.metadata,d.file_name,d.file_type,d.s3_key,d.doc_type,d.fingerprint,d.collection,
		d.text_source,COALESCE(d.incarnation,d.id)::text,j.lease_token::text,d.scoped_vectors,p.profile,p.coverage,p.manifest,p.page_results,
		d.grant_catalog_assigned
	FROM claimed j JOIN documents d ON d.id=j.document_id
	LEFT JOIN document_pdf_extractions p ON p.document_id=d.id`

// The reservation predicates are owner-defined, so a serving login needs no
// privilege on the receipt table to lease ordinary work. An unfinished or held
// deletion receipt keeps a worker from leasing the document, so no new broad
// vector work starts under a reservation.
const claimFixturePredicate = `evaluation_fixture_claimable(ready.document_id, NULLIF($5::text, ''))
		AND NOT document_deletion_reserved(ready.document_id)`

const claimBeforeFixturePredicate = `to_regclass('evaluation_fixtures') IS NULL`

func claimStatement(hasLayoutColumn, hasFixtureGuards bool) string {
	layoutSet := ""
	profileRule := claimProfileLegacy
	if hasLayoutColumn {
		layoutSet = claimJobSetLayout
		profileRule = claimProfileCurrent
	}
	guard := claimBeforeFixturePredicate
	if hasFixtureGuards {
		guard = claimFixturePredicate
	}
	return claimJobUpdateHeader + layoutSet + claimJobTail + claimReadyPrefix + profileRule + claimReadySchedule + guard + claimJobSuffix
}

// ClaimJob leases ordinary work on both current and pre-fixture schemas.
func (c *Client) ClaimJob(ctx context.Context, owner string, lease time.Duration, maxAttempts int) (*domain.Job, error) {
	return c.ClaimJobForFixtureStore(ctx, owner, lease, maxAttempts, "")
}

// ClaimJobForFixtureStore permits fixture jobs only for their reserved vector store.
func (c *Client) ClaimJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*domain.Job, error) {
	return c.ClaimPDFProfilesForFixtureStore(ctx, owner, lease, maxAttempts, vectorIdentity, false, false)
}

// ClaimPDFJob also leases jobs that require a mixed-PDF worker.
func (c *Client) ClaimPDFJob(ctx context.Context, owner string, lease time.Duration, maxAttempts int) (*domain.Job, error) {
	return c.ClaimPDFJobForFixtureStore(ctx, owner, lease, maxAttempts, "")
}

// ClaimPDFJobForFixtureStore applies both the PDF and fixture claim rules.
func (c *Client) ClaimPDFJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*domain.Job, error) {
	return c.ClaimPDFProfilesForFixtureStore(ctx, owner, lease, maxAttempts, vectorIdentity, true, false)
}

// ClaimPDFProfilesForFixtureStore leases jobs matching the worker's PDF OCR capabilities and fixture store binding.
func (c *Client) ClaimPDFProfilesForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string, mixed, layout bool) (*domain.Job, error) {
	return c.claimGuarded(ctx, owner, lease, maxAttempts, mixed, layout, vectorIdentity)
}

func (c *Client) isLayoutClaimColumnMissing(ctx context.Context) (bool, error) {
	_, err := c.Pool.Exec(ctx, `SELECT layout_claim_attempt FROM ingest_jobs WHERE FALSE`)
	if err == nil {
		return false, nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42703" {
		return true, nil
	}
	return false, fmt.Errorf("postgres: check layout claim column: %w", err)
}

func (c *Client) claimGuarded(ctx context.Context, owner string, lease time.Duration, maxAttempts int, mixed, layout bool, vectorIdentity string) (*domain.Job, error) {
	job, err := c.claimJob(ctx, owner, maxAttempts, claimStatement(true, true), owner, lease.Seconds(), mixed, layout, vectorIdentity)
	if err == nil {
		return job, nil
	}
	hasLayoutCol := true
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42703" {
		missing, checkErr := c.isLayoutClaimColumnMissing(ctx)
		if checkErr != nil {
			return nil, checkErr
		}
		if !missing {
			return nil, err
		}
		hasLayoutCol = false
		job, err = c.claimJob(ctx, owner, maxAttempts, claimStatement(false, true), owner, lease.Seconds(), mixed, layout, vectorIdentity)
		if err == nil {
			return job, nil
		}
	}
	if errors.As(err, &pgErr) && pgErr.Code == "42883" {
		// Before the fixture migration, only claim while the reservation table is absent.
		job, err = c.claimJob(ctx, owner, maxAttempts, claimStatement(hasLayoutCol, false), owner, lease.Seconds(), mixed, layout)
		if err != nil && hasLayoutCol && errors.As(err, &pgErr) && pgErr.Code == "42703" {
			missing, checkErr := c.isLayoutClaimColumnMissing(ctx)
			if checkErr != nil {
				return nil, checkErr
			}
			if !missing {
				return nil, err
			}
			return c.claimJob(ctx, owner, maxAttempts, claimStatement(false, false), owner, lease.Seconds(), mixed, layout)
		}
	}
	return job, err
}

func (c *Client) claimJob(ctx context.Context, owner string, maxAttempts int, statement string, args ...any) (*domain.Job, error) {
	for range 10 {
		j := &domain.Job{LeaseOwner: owner}
		var id string
		var metadata, manifest, pageResults []byte
		var fingerprint, profile *string
		var coverage *domain.PDFCoverage
		var assigned bool
		err := c.claimJobLease(ctx, statement, args, func(row pgx.Row) error {
			return row.Scan(&id, &j.Attempts, &j.Document.Content, &metadata, &j.Document.FileName, &j.Document.FileType,
				&j.Document.S3Key, &j.DocType, &fingerprint, &j.Collection, &j.Document.TextSource, &j.Incarnation, &j.LeaseToken, &j.ScopedVectors, &profile, &coverage, &manifest, &pageResults, &assigned)
		})
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, domain.ErrWritersFrozen) {
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
		if assigned {
			// Only an assigned document needs the catalog, so service-mode logins need no catalog privilege.
			tenant, tenantErr := c.documentTenant(ctx, id, j.Incarnation)
			if tenantErr != nil {
				return nil, tenantErr
			}
			if tenant == "" {
				// Deletion removed the ownership with the document, and the job went with it.
				continue
			}
			j.Tenant = tenant
		}
		if fingerprint != nil {
			j.Document.Fingerprint = *fingerprint
		}
		if j.PDF != nil && j.PDF.Coverage == domain.PDFPending {
			j.PDF.Pages, err = domain.DecodePDFPages(manifest)
			if err != nil {
				return nil, fmt.Errorf("postgres: load PDF job manifest: %w", err)
			}
		} else if j.PDF != nil && j.PDF.Profile == domain.LayoutPDFProfile && len(pageResults) > 0 {
			if err = json.Unmarshal(pageResults, &j.PDF.PageResults); err != nil {
				return nil, fmt.Errorf("postgres: load PDF layout sections: %w", err)
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
const jobClaim = `document_id=$1 AND lease_owner=$2 AND attempts=$3 AND lease_token=$4 AND state='running'`

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

// claimJobLease runs the claim statement in a transaction that holds the
// writer gate until the lease commits.
func (c *Client) claimJobLease(ctx context.Context, statement string, args []any, scan func(pgx.Row) error) error {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if gateErr := c.gate(ctx, tx); gateErr != nil {
		return gateErr
	}
	if scanErr := scan(tx.QueryRow(ctx, statement, args...)); scanErr != nil {
		return scanErr
	}
	return tx.Commit(ctx)
}

// HeartbeatJob renews a live lease without reviving an expired attempt.
func (c *Client) HeartbeatJob(ctx context.Context, j domain.Job, lease time.Duration) (bool, error) {
	if j.Incarnation == "" || j.LeaseToken == "" {
		return false, nil
	}
	tag, err := c.Pool.Exec(ctx, `WITH owned AS MATERIALIZED (
		SELECT j.document_id,j.lease_until FROM ingest_jobs j JOIN documents d ON d.id=j.document_id
		WHERE j.document_id=$1 AND j.lease_owner=$2 AND j.attempts=$3 AND j.lease_token=$4 AND j.state='running'
			AND COALESCE(d.incarnation,d.id)=$5 AND d.status='processing' FOR UPDATE OF j
	) UPDATE ingest_jobs j SET lease_until=clock_timestamp()+make_interval(secs=>$6),updated_at=now()
		FROM owned WHERE j.document_id=owned.document_id AND owned.lease_until>clock_timestamp()`,
		j.Document.ID, j.LeaseOwner, j.Attempts, j.LeaseToken, j.Incarnation, lease.Seconds())
	return tag.RowsAffected() == 1, err
}

// CompleteJob commits chunks and completion only while the document and lease remain valid.
func (c *Client) CompleteJob(ctx context.Context, j domain.Job, chunks []domain.Chunk, observation domain.IndexObservation) (bool, error) {
	if j.Incarnation == "" || j.LeaseToken == "" {
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
	var scoped, pending, uncertain, deleteUncertain, assigned bool
	err = tx.QueryRow(ctx, `SELECT content_dedup,fingerprint,collection,
		scoped_vectors,vector_mutation_pending,vector_mutation_uncertain,vector_delete_uncertain,grant_catalog_assigned
		FROM documents WHERE id=$1 AND status='processing' AND COALESCE(incarnation,id)=$2 FOR UPDATE`,
		j.Document.ID, j.Incarnation).Scan(&policy, &fingerprint, &collection, &scoped, &pending, &uncertain, &deleteUncertain, &assigned)
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
	safeToPublish := !pending && (scoped || !uncertain)
	if j.RevisionID != "" && !scoped {
		if pending {
			if _, settleErr := settleExpiredVectorMutation(ctx, tx, j.Document.ID); settleErr != nil {
				return false, settleErr
			}
			err = tx.QueryRow(ctx, `SELECT vector_mutation_pending,vector_delete_uncertain FROM documents WHERE id=$1`,
				j.Document.ID).Scan(&pending, &deleteUncertain)
			if err != nil {
				return false, err
			}
		}
		// Manifested revisions use disjoint keys from legacy upserts. A broad
		// legacy delete can still match their metadata.
		safeToPublish = !pending && !deleteUncertain
	}
	if !safeToPublish {
		return false, errors.New("vector mutation is unresolved for the publication target")
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
		table := c.vectorTargetTable()
		if _, err = tx.Exec(ctx, `DELETE FROM `+pgx.Identifier{table}.Sanitize()+` WHERE document_id=$1`, string(j.Document.ID)); err != nil {
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
		if err = insertVectors(ctx, tx, table, vectors, dimensions); err != nil {
			return false, fmt.Errorf("postgres: complete vectors: %w", err)
		}
		observation = domain.IndexObservation{Visibility: domain.VisibilityTransactional}
	}
	// A document assigned in the grant catalog never registers a shared match,
	// even when a historical row kept the collection policy.
	if policy == domain.ContentDedupCollection && !assigned && c.configuredAuthzMode() != "grants" {
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
	vectorIncarnationRequired := slices.ContainsFunc(chunks, func(ch domain.Chunk) bool { return ch.VectorIncarnation != "" })
	if _, err = tx.Exec(ctx, `UPDATE documents SET status='completed',chunk_count=$2,completed_at=statement_timestamp(),updated_at=statement_timestamp(),last_error=NULL,
        index_visibility=NULLIF($3,''),index_visibility_observed_at=CASE WHEN $3='transactional' THEN statement_timestamp() ELSE $4 END,
        vector_attempt=CASE WHEN scoped_vectors THEN $5::uuid ELSE NULL END,vector_incarnation_required=$6
        WHERE id=$1`, j.Document.ID, len(chunks), observation.Visibility, observation.ObservedAt, j.LeaseToken, vectorIncarnationRequired); err != nil {
		return false, err
	}
	if j.RevisionID != "" {
		if actErr := activateVectorManifest(ctx, tx, j); actErr != nil {
			return false, actErr
		}
	}
	if assigned {
		if _, err = tx.Exec(ctx, `SELECT grant_catalog_publish_accepted_document($1::uuid,$2::uuid)`, string(j.Document.ID), j.Incarnation); err != nil {
			return false, fmt.Errorf("postgres: publish accepted document: %w", err)
		}
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
	if lockErr := lockLiveJob(ctx, tx, j); lockErr != nil {
		return lockErr
	}
	params := append([]any{j.Document.ID}, args...)
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
			if _, settleErr := settleExpiredVectorMutation(ctx, tx, j.Document.ID); settleErr != nil {
				return settleErr
			}
			if j.PDF != nil {
				if _, err = tx.Exec(ctx, `UPDATE document_pdf_extractions SET coverage='failed' WHERE document_id=$1 AND coverage='pending'`, j.Document.ID); err != nil {
					return err
				}
			}
		}
		_, err = tx.Exec(ctx, `UPDATE documents SET status=$2,last_error=$3,updated_at=NOW() WHERE id=$1`, j.Document.ID, state, jobError(*message))
		if err != nil {
			return err
		}
	}
	// A failure retires every manifest of the document through the status
	// trigger. An attempt that is only retried or released abandons its own.
	if j.RevisionID != "" && (args != nil || message == nil) {
		if retErr := abandonAttemptManifest(ctx, tx, string(j.Document.ID), j.RevisionID); retErr != nil {
			return retErr
		}
	}
	return tx.Commit(ctx)
}

// activateVectorManifest publishes the attempt's revision. Every other
// unretired manifest of the document is retired first, because at most one
// manifest may be active, and the superseded revisions wait for the sweeper.
func activateVectorManifest(ctx context.Context, tx pgx.Tx, j domain.Job) error {
	if _, err := tx.Exec(ctx, `
		WITH retired AS (
			UPDATE hosted_vector_manifests
			   SET state = 'retired',
			       retired_at = COALESCE(retired_at, statement_timestamp())
			 WHERE document_id = $1
			   AND revision_id <> $2
			   AND state IN ('writing', 'active')
			RETURNING revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids, retired_at
		)
		INSERT INTO hosted_vector_cleanups (
			revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids,
			next_attempt_at, retired_at, reason
		)
		SELECT revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids,
		       statement_timestamp(), retired_at, 'superseded'
		FROM retired
		ON CONFLICT (revision_id) DO NOTHING`,
		string(j.Document.ID), j.RevisionID); err != nil {
		return fmt.Errorf("postgres: retire superseded vector manifests: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE hosted_vector_manifests
		   SET state = 'active',
		       activated_at = statement_timestamp()
		 WHERE revision_id = $1
		   AND document_id = $2
		   AND incarnation = $3
		   AND attempt = $4
		   AND lease_token IS NOT DISTINCT FROM NULLIF($5, '')::uuid
		   AND state = 'writing'`,
		j.RevisionID, string(j.Document.ID), j.Incarnation, j.Attempts, j.LeaseToken)
	if err != nil {
		return fmt.Errorf("postgres: activate vector manifest: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("postgres: vector manifest %s is not active for this attempt", j.RevisionID)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO hosted_vector_revisions (document_id, incarnation, active_revision_id, updated_at)
		VALUES ($1, $2, $3, statement_timestamp())
		ON CONFLICT (document_id) DO UPDATE
		  SET incarnation = EXCLUDED.incarnation,
		      active_revision_id = EXCLUDED.active_revision_id,
		      updated_at = EXCLUDED.updated_at`,
		string(j.Document.ID), j.Incarnation, j.RevisionID); err != nil {
		return fmt.Errorf("postgres: set active vector revision: %w", err)
	}
	return nil
}

// abandonAttemptManifest retires the revision of an attempt that stops without
// completing, so the sweeper removes whatever it wrote.
func abandonAttemptManifest(ctx context.Context, tx pgx.Tx, docID, revisionID string) error {
	_, err := tx.Exec(ctx, `
		WITH retired AS (
			UPDATE hosted_vector_manifests
			   SET state = 'retired',
			       retired_at = COALESCE(retired_at, statement_timestamp())
			 WHERE document_id = $1
			   AND revision_id = $2
			   AND state = 'writing'
			RETURNING revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids, retired_at
		)
		INSERT INTO hosted_vector_cleanups (
			revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids,
			next_attempt_at, retired_at, reason
		)
		SELECT revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids,
		       statement_timestamp(), retired_at, 'abandoned'
		FROM retired
		ON CONFLICT (revision_id) DO NOTHING`,
		docID, revisionID)
	if err != nil {
		return fmt.Errorf("postgres: retire attempt vector manifest: %w", err)
	}
	return nil
}

// RetryJob reschedules an owned attempt and records its latest failure.
func (c *Client) RetryJob(ctx context.Context, j domain.Job, delay time.Duration, message string) error {
	return c.changeJob(ctx, j, `UPDATE ingest_jobs SET state='pending',lease_owner=NULL,lease_until=NULL,lease_token=NULL,
		next_attempt_at=clock_timestamp()+make_interval(secs=>$2),last_error=$3,updated_at=now()
		WHERE document_id=$1 AND lease_until>clock_timestamp()`, []any{delay.Seconds(), jobError(message)}, &message)
}

// FailJob records a terminal failure and removes the owned work.
func (c *Client) FailJob(ctx context.Context, j domain.Job, message string) error {
	return c.changeJob(ctx, j, `DELETE FROM ingest_jobs WHERE document_id=$1 AND lease_until>clock_timestamp()`, nil, &message)
}

// ReleaseJob returns interrupted work without consuming an attempt.
func (c *Client) ReleaseJob(ctx context.Context, j domain.Job) error {
	return c.changeJob(ctx, j, `UPDATE ingest_jobs SET state='pending',lease_owner=NULL,lease_until=NULL,lease_token=NULL,
		next_attempt_at=clock_timestamp(),attempts=GREATEST(attempts-1,0),updated_at=now()
		WHERE document_id=$1 AND lease_until>clock_timestamp()`, nil, nil)
}

// lockLiveJob locks the document before the job and compares lease time after acquiring both.
func lockLiveJob(ctx context.Context, tx pgx.Tx, job domain.Job) error {
	if job.Incarnation == "" || job.LeaseToken == "" {
		return domain.ErrJobNotOwned
	}
	var id string
	err := tx.QueryRow(ctx, `SELECT id::text FROM documents WHERE id=$1 AND status='processing'
		AND COALESCE(incarnation,id)=$2 FOR UPDATE`, job.Document.ID, job.Incarnation).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrJobNotOwned
	}
	if err != nil {
		return err
	}
	var live bool
	err = tx.QueryRow(ctx, `WITH owned AS MATERIALIZED (
		SELECT lease_until FROM ingest_jobs WHERE document_id=$1 AND lease_owner=$2
			AND attempts=$3 AND lease_token=$4 AND state='running' FOR UPDATE
	) SELECT lease_until>clock_timestamp() FROM owned`, id, job.LeaseOwner, job.Attempts, job.LeaseToken).Scan(&live)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !live) {
		return domain.ErrJobNotOwned
	}
	return err
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
	if lockErr := lockLiveJob(ctx, tx, j); lockErr != nil {
		return lockErr
	}
	preview := []rune(text)
	if len(preview) > 120 {
		preview = preview[:120]
	}
	// Recheck the lease after any wait for the document and job row locks.
	tag, err := tx.Exec(ctx, `UPDATE documents d SET content=$2,content_preview=$3,text_source='ocr',updated_at=NOW() WHERE d.id=$1
		AND EXISTS(SELECT 1 FROM ingest_jobs j WHERE j.document_id=d.id AND j.lease_until>clock_timestamp())`, j.Document.ID, text, string(preview))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrJobNotOwned
	}
	return tx.Commit(ctx)
}
