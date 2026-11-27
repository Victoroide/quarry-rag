package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func validatePDFAdmission(req domain.AcceptRequest) error {
	if (req.PDF.Profile != domain.MixedPDFProfile && req.PDF.Profile != domain.LayoutPDFProfile) || !req.RequireSource {
		return errors.New("postgres: invalid PDF admission profile or source requirement")
	}
	if err := req.PDF.Pages.Validate(); err != nil {
		return err
	}
	if req.PDF.Profile == domain.LayoutPDFProfile {
		if req.PDF.Coverage != domain.PDFPending || req.Document.Content != "" {
			return errors.New("postgres: layout PDF admission requires pending coverage and empty content")
		}
		for _, page := range req.PDF.Pages {
			if page.Evidence != domain.PDFBlank {
				return nil
			}
		}
		return errors.New("postgres: layout PDF admission requires at least one non-blank page")
	}
	coverage := req.PDF.Pages.CoverageBeforeOCR()
	if req.PDF.Coverage == coverage && (coverage == domain.PDFPending && req.Document.Content == "" ||
		coverage != domain.PDFPending && len(req.Document.Content) >= domain.MinPDFTextBytes && req.Document.Content == req.PDF.Pages.Text()) {
		return nil
	}
	return errors.New("postgres: PDF admission coverage does not match page evidence")
}

func requestedPDFProfile(req domain.AcceptRequest) string {
	if req.PDF != nil && req.PDF.Profile != "" {
		return req.PDF.Profile
	}
	if req.PDFProfile != "" {
		return req.PDFProfile
	}
	return domain.MixedPDFProfile
}

// matchesLegacyPDF reports whether a historical identity matches and whether an
// owned retry of a historical parser PDF needs the parser-text fingerprint.
func matchesLegacyPDF(ctx context.Context, tx pgx.Tx, req domain.AcceptRequest, policy domain.ContentDedup, stored string) (matches, needsParserText bool, err error) {
	if requestedPDFProfile(req) == domain.LayoutPDFProfile {
		return false, false, nil
	}
	parser := req.LegacyParserFingerprint != "" && stored == identityFingerprint(policy, req.Document.ID, req.LegacyParserFingerprint)
	detection := req.LegacyOCRFingerprint != "" && stored == identityFingerprint(policy, req.Document.ID, req.LegacyOCRFingerprint)
	mayNeedParserText := req.Owner != nil && req.LegacyParserFingerprint == ""
	if !parser && !detection && !mayNeedParserText {
		return false, false, nil
	}
	var parserLegacy, detectionLegacy, pdf bool
	err = tx.QueryRow(ctx, `SELECT p.document_id IS NULL AND d.text_source='parser',
		p.document_id IS NULL AND d.text_source IN ('ocr_pending','ocr'), COALESCE(d.file_type='pdf',FALSE)
		FROM documents d LEFT JOIN document_pdf_extractions p ON p.document_id=d.id WHERE d.id=$1`,
		string(req.Document.ID)).Scan(&parserLegacy, &detectionLegacy, &pdf)
	return parserLegacy && parser || detectionLegacy && detection, mayNeedParserText && parserLegacy && pdf, err
}

func matchExistingPDF(ctx context.Context, tx pgx.Tx, req domain.AcceptRequest, policy domain.ContentDedup, stored string, includeLegacy bool) (domain.AcceptOutcome, error) {
	var source string
	var profile *string
	err := tx.QueryRow(ctx, `SELECT d.text_source,p.profile FROM documents d
		LEFT JOIN document_pdf_extractions p ON p.document_id=d.id WHERE d.id=$1`, string(req.Document.ID)).Scan(&source, &profile)
	if err != nil {
		return domain.AcceptConflict, err
	}
	expectedProfile := requestedPDFProfile(req)
	var candidate string
	switch {
	case profile != nil:
		if *profile == expectedProfile {
			candidate = req.Fingerprint
		}
	case expectedProfile == domain.LayoutPDFProfile:
		return domain.AcceptConflict, nil
	case !includeLegacy:
		return domain.AcceptNeedsSource, nil
	case source == "parser":
		if req.LegacyParserFingerprint == "" {
			return domain.AcceptNeedsPDFText, nil
		}
		candidate = req.LegacyParserFingerprint
	case source == "ocr_pending" || source == "ocr":
		candidate = req.LegacyOCRFingerprint
	}
	if candidate != "" && stored == identityFingerprint(policy, req.Document.ID, candidate) {
		return domain.AcceptAccepted, nil
	}
	return domain.AcceptConflict, nil
}

func findSharedPDF(ctx context.Context, tx pgx.Tx, req domain.AcceptRequest) (id, state string, err error) {
	expectedProfile := requestedPDFProfile(req)
	if expectedProfile == domain.LayoutPDFProfile {
		err = tx.QueryRow(ctx, `SELECT d.id::text,d.status FROM document_fingerprints f
			JOIN documents d ON d.id=f.document_id
			JOIN document_pdf_extractions p ON p.document_id=d.id
			WHERE d.collection=$2 AND d.content_dedup='collection' AND f.fingerprint=$1 AND p.profile=$3
			AND d.text_source IN ('parser','pdf_layout')
			LIMIT 1 FOR UPDATE OF d`, req.Fingerprint, req.Collection, domain.LayoutPDFProfile).Scan(&id, &state)
		return id, state, err
	}
	// A text-only identity cannot establish that a different original has the
	// same scanned pages. On unqualified requests it is safe only for clean PDFs.
	var parser string
	if req.PDF != nil && req.PDF.Coverage == domain.PDFComplete {
		parser = req.LegacyParserFingerprint
	}
	err = tx.QueryRow(ctx, `SELECT d.id::text,d.status FROM document_fingerprints f
		JOIN documents d ON d.id=f.document_id
		LEFT JOIN document_pdf_extractions p ON p.document_id=d.id
		WHERE d.collection=$2 AND d.content_dedup='collection' AND d.status<>'deleting' AND (
			(f.fingerprint=$1 AND p.profile=$5 AND d.text_source IN ('parser','pdf_mixed')) OR
			(p.document_id IS NULL AND
			((f.fingerprint=$3 AND d.text_source='parser') OR
			 (f.fingerprint=$4 AND d.text_source IN ('ocr_pending','ocr')))))
		ORDER BY CASE f.fingerprint WHEN $1 THEN 0 WHEN $4 THEN 1 ELSE 2 END
		LIMIT 1 FOR UPDATE OF d`, req.Fingerprint, req.Collection, parser, req.LegacyOCRFingerprint, domain.MixedPDFProfile).Scan(&id, &state)
	return id, state, err
}

func insertPDFJob(ctx context.Context, tx pgx.Tx, req domain.AcceptRequest) error {
	if req.PDF.Coverage == domain.PDFPending || req.PDF.Profile == domain.LayoutPDFProfile {
		if err := requirePDFQueueCompatibility(ctx, tx, req.PDF.Profile); err != nil {
			return err
		}
	}
	manifest, err := json.Marshal(req.PDF.Pages)
	if err != nil {
		return err
	}
	id := string(req.Document.ID)
	if _, err = tx.Exec(ctx, `INSERT INTO document_pdf_extractions(document_id,profile,manifest,coverage) VALUES($1,$2,$3,$4)`,
		id, req.PDF.Profile, manifest, req.PDF.Coverage); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ingest_jobs(document_id,pdf_required) VALUES($1,$2)`,
		id, req.PDF.Coverage == domain.PDFPending || req.PDF.Profile == domain.LayoutPDFProfile)
	return err
}

func requeuePDF(ctx context.Context, tx pgx.Tx, id, profile string, rotateWindow bool) error {
	if err := requirePDFQueueCompatibility(ctx, tx, profile); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE document_pdf_extractions SET coverage='pending',page_results=NULL,
		window_id=CASE WHEN $2 THEN gen_random_uuid() ELSE window_id END,
		started_at=CASE WHEN NOT $2 THEN started_at END,
		start_bound_at=CASE WHEN NOT $2 THEN start_bound_at END,
		accepted_at=CASE WHEN NOT $2 THEN accepted_at END
		WHERE document_id=$1`, id, rotateWindow); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE documents SET content='',content_preview='',text_source='parser' WHERE id=$1`, id); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO ingest_jobs(document_id,pdf_required) VALUES($1,TRUE)`, id)
	return err
}

func requirePDFQueueCompatibility(ctx context.Context, tx pgx.Tx, profile string) error {
	// Order the first mixed write against claims from older binaries as well as
	// current callers. After the insert, the claim trigger protects the queue.
	if _, err := tx.Exec(ctx, `LOCK TABLE ingest_jobs IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return err
	}
	if profile == domain.LayoutPDFProfile {
		if _, err := tx.Exec(ctx, `SELECT layout_claim_attempt FROM ingest_jobs WHERE FALSE`); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42703" {
				return errors.New("postgres: layout PDF work requires layout claim capability migration 040")
			}
			return err
		}
	}
	var incompatible bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ingest_jobs WHERE state='running' AND lease_until>clock_timestamp()
		AND pdf_claim_attempt IS DISTINCT FROM attempts)`).Scan(&incompatible)
	if err != nil {
		return err
	}
	if incompatible {
		return errors.New("postgres: mixed PDF work requires compatible ingestion consumers")
	}
	return nil
}

// BeginPDFOCR records when a provider request could be sent and the latest
// deadline of any request using this window.
func (c *Client) BeginPDFOCR(ctx context.Context, job domain.Job) (domain.PDFOCRWindow, error) {
	var window domain.PDFOCRWindow
	err := c.withPDFLease(ctx, job, func(tx pgx.Tx) error {
		var budget *float64
		if deadline, ok := ctx.Deadline(); ok {
			seconds := max(0, time.Until(deadline).Seconds())
			budget = &seconds
		}
		// Calculate timestamps on the database clock so clock skew between
		// the worker and database cannot shorten the restart wait.
		err := tx.QueryRow(ctx, `UPDATE document_pdf_extractions p SET started_at=COALESCE(started_at,statement_timestamp()),
			start_bound_at=CASE WHEN $4::double precision IS NULL THEN start_bound_at ELSE GREATEST(start_bound_at,LEAST(
				statement_timestamp()+make_interval(secs=>$4),COALESCE(started_at,statement_timestamp())+make_interval(secs=>$3))) END
			WHERE p.document_id=$1 AND p.coverage='pending'
			AND (started_at IS NULL OR started_at + make_interval(secs=>$3)>clock_timestamp())
			AND EXISTS(SELECT 1 FROM ingest_jobs j WHERE j.document_id=p.document_id AND j.lease_token=$2 AND j.lease_until>clock_timestamp())
			RETURNING window_id::text,started_at + make_interval(secs=>$3)`,
			string(job.Document.ID), job.LeaseToken, domain.PDFOCRLifetime.Seconds(), budget).Scan(&window.ID, &window.Expires)
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var expired bool
		if expiryErr := tx.QueryRow(ctx, `SELECT COALESCE(started_at + make_interval(secs=>$2)<=clock_timestamp(),FALSE)
			FROM document_pdf_extractions WHERE document_id=$1`, string(job.Document.ID), domain.PDFOCRLifetime.Seconds()).Scan(&expired); expiryErr != nil {
			return expiryErr
		}
		if expired {
			return domain.Permanent(domain.ErrPDFOCRExpired)
		}
		return domain.ErrJobNotOwned
	})
	return window, err
}

// RecordPDFOCRStart records the first acknowledged provider start for this window.
func (c *Client) RecordPDFOCRStart(ctx context.Context, job domain.Job, windowID string) error {
	return c.withPDFLease(ctx, job, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE document_pdf_extractions p SET accepted_at=COALESCE(accepted_at,clock_timestamp())
			WHERE p.document_id=$1 AND p.window_id=$2 AND p.coverage='pending' AND p.started_at IS NOT NULL
			AND EXISTS(SELECT 1 FROM ingest_jobs j WHERE j.document_id=p.document_id AND j.lease_token=$3 AND j.lease_until>clock_timestamp())`,
			string(job.Document.ID), windowID, job.LeaseToken)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return domain.ErrJobNotOwned
		}
		return nil
	})
}

// StorePDFOCR atomically checkpoints text, coverage and per-page sources.
func (c *Client) StorePDFOCR(ctx context.Context, job domain.Job, result domain.PDFResult) error {
	if job.PDF == nil {
		return domain.ErrJobNotOwned
	}
	if err := result.Validate(len(job.PDF.Pages)); err != nil {
		return err
	}
	pages, err := json.Marshal(result.Pages)
	if err != nil {
		return err
	}
	return c.withPDFLease(ctx, job, func(tx pgx.Tx) error {
		id := string(job.Document.ID)
		tag, err := tx.Exec(ctx, `UPDATE document_pdf_extractions p SET page_results=$2,coverage=$3
			WHERE p.document_id=$1 AND p.coverage='pending' AND p.started_at IS NOT NULL AND EXISTS(
			SELECT 1 FROM ingest_jobs j WHERE j.document_id=p.document_id AND j.lease_token=$4 AND j.lease_until>clock_timestamp())`,
			id, pages, result.Coverage, job.LeaseToken)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return domain.ErrJobNotOwned
		}
		preview := []rune(result.Text)
		if len(preview) > 120 {
			preview = preview[:120]
		}
		textSource := "pdf_mixed"
		if job.PDF.Profile == domain.LayoutPDFProfile {
			textSource = "pdf_layout"
		}
		_, err = tx.Exec(ctx, `UPDATE documents SET content=$2,content_preview=$3,text_source=$4,updated_at=NOW() WHERE id=$1`, id, result.Text, string(preview), textSource)
		return err
	})
}

func (c *Client) withPDFLease(ctx context.Context, job domain.Job, write func(pgx.Tx) error) error {
	if job.LeaseToken == "" {
		return domain.ErrJobNotOwned
	}
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var owned bool
	err = tx.QueryRow(ctx, `SELECT TRUE FROM documents d JOIN ingest_jobs j ON j.document_id=d.id
		WHERE d.id=$1 AND d.s3_key=$2 AND d.status='processing' AND j.lease_owner=$3 AND j.attempts=$4 AND COALESCE(d.incarnation,d.id)=$5
		AND j.state='running' AND j.pdf_required AND j.lease_token=$6 AND j.lease_until>clock_timestamp() FOR UPDATE OF d,j`,
		string(job.Document.ID), job.Document.S3Key, job.LeaseOwner, job.Attempts, job.Incarnation, job.LeaseToken).Scan(&owned)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrJobNotOwned
	}
	if err != nil {
		return err
	}
	if writeErr := write(tx); writeErr != nil {
		return writeErr
	}
	return tx.Commit(ctx)
}
