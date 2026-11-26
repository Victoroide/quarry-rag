package handler

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
)

const (
	defaultScopedStreamBatchSize = 100
	maxPolicyRecheckAttempts     = 4
	maxPresignTTLSeconds         = 900
)

var errScopedCredentialExpired = errors.New("subject credential expired")

// DocumentListOption customizes GET /documents.
type DocumentListOption func(*documentListOptions)

type documentListOptions struct {
	scoped          bool
	streamBatchSize int
	now             func() time.Time
	audit           domain.SecurityAuditRecorder
}

// WithScopedListing enforces tenant and read-grant checks inside inventory
// queries without exposing a partially protected public mode.
func WithScopedListing() DocumentListOption {
	return func(opts *documentListOptions) { opts.scoped = true }
}

// WithListingAudit records bounded security audit events for scoped document listing.
func WithListingAudit(recorder domain.SecurityAuditRecorder) DocumentListOption {
	return func(opts *documentListOptions) { opts.audit = recorder }
}

func withScopedStreamBatchSize(size int) DocumentListOption {
	return func(opts *documentListOptions) {
		if size > 0 {
			opts.streamBatchSize = size
		}
	}
}

func withListingClock(now func() time.Time) DocumentListOption {
	return func(opts *documentListOptions) { opts.now = now }
}

// DocumentStatusOption customizes GET /documents/{doc_id}.
type DocumentStatusOption func(*documentStatusOptions)

type documentStatusOptions struct {
	scoped bool
	now    func() time.Time
	audit  domain.SecurityAuditRecorder
}

// WithScopedStatus enforces tenant and read-grant checks on status lookups.
func WithScopedStatus() DocumentStatusOption {
	return func(opts *documentStatusOptions) { opts.scoped = true }
}

// WithStatusAudit records bounded security audit events for scoped document status lookups.
func WithStatusAudit(recorder domain.SecurityAuditRecorder) DocumentStatusOption {
	return func(opts *documentStatusOptions) { opts.audit = recorder }
}

func withStatusClock(now func() time.Time) DocumentStatusOption {
	return func(opts *documentStatusOptions) { opts.now = now }
}

// FileOption customizes GET /documents/{doc_id}/file.
type FileOption func(*fileOptions)

type fileOptions struct {
	scoped bool
	now    func() time.Time
	audit  domain.SecurityAuditRecorder
}

// WithScopedFile enforces owner or read-plus-download grants and assertion-bounded
// presigned lifetimes on original-file downloads.
func WithScopedFile() FileOption {
	return func(opts *fileOptions) { opts.scoped = true }
}

// WithFileAudit records bounded security audit events for scoped file signing denials and failures.
func WithFileAudit(recorder domain.SecurityAuditRecorder) FileOption {
	return func(opts *fileOptions) { opts.audit = recorder }
}

func withFileClock(now func() time.Time) FileOption {
	return func(opts *fileOptions) { opts.now = now }
}

// WithScopedDeletion enforces owner-only authorization before recording a
// checkpointed deletion intent.
func WithScopedDeletion() DeleteOption {
	return func(opts *deleteOptions) { opts.scoped = true }
}

// WithDeleteAudit records bounded security audit events for scoped deletion denials and failures.
func WithDeleteAudit(recorder domain.SecurityAuditRecorder) DeleteOption {
	return func(opts *deleteOptions) { opts.audit = recorder }
}

func withDeleteClock(now func() time.Time) DeleteOption {
	return func(opts *deleteOptions) { opts.now = now }
}

func clockOrDefault(now func() time.Time) func() time.Time {
	if now != nil {
		return now
	}
	return time.Now
}

func requireScopedSubject(w http.ResponseWriter, r *http.Request, now func() time.Time) (middleware.VerifiedSubject, bool) {
	w.Header().Set("Cache-Control", "private, no-store")
	subject, ok := middleware.SubjectFromContext(r.Context())
	if !ok || !subject.ValidAt(now()) {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials")
		return middleware.VerifiedSubject{}, false
	}
	return subject, true
}

func redactScopedSummary(doc domain.DocumentSummary) domain.DocumentSummary {
	doc.S3Key = nil
	if doc.Status != "failed" {
		doc.LastError = nil
		return doc
	}
	if doc.LastError != nil {
		category := publicFailureCategory(*doc.LastError)
		doc.LastError = &category
	}
	return doc
}

func publicFailureCategory(raw string) string {
	switch {
	case raw == "no_text":
		return "no_text"
	case raw == "ocr_result_expired":
		return "ocr_result_expired"
	case raw == "ocr_text_too_large" || strings.HasSuffix(raw, ": ocr_text_too_large"):
		return "ocr_text_too_large"
	case strings.Contains(raw, domain.ErrEmbeddingModelLimit.Error()):
		return "embedding_limit"
	case strings.Contains(raw, pipeline.ErrNoChunks.Error()):
		return "no_chunks"
	case strings.HasPrefix(raw, "ocr:"):
		return "ocr_failed"
	default:
		return "processing_failed"
	}
}

type scopedFetchError struct {
	status  int
	errCode string
	message string
	cause   error
}

func fetchVerifiedScopedPage(
	ctx context.Context,
	lister domain.ScopedDocumentLister,
	subject middleware.VerifiedSubject,
	now func() time.Time,
	after string,
	limit int,
	collection string,
	includeIncarnation bool,
) (domain.ScopedDocumentPage, *scopedFetchError) {
	for range maxPolicyRecheckAttempts {
		if !subject.ValidAt(now()) {
			return domain.ScopedDocumentPage{}, &scopedFetchError{http.StatusUnauthorized, "unauthorized", "invalid or missing credentials", nil}
		}
		page, err := lister.ListScopedDocumentsPage(ctx, subject.Principal, after, limit, collection, includeIncarnation)
		if err != nil {
			return domain.ScopedDocumentPage{}, &scopedFetchError{http.StatusInternalServerError, "internal", "failed to list documents", err}
		}
		if !page.Active {
			return domain.ScopedDocumentPage{}, &scopedFetchError{http.StatusForbidden, "forbidden", "access denied", nil}
		}
		if !subject.ValidAt(now()) {
			return domain.ScopedDocumentPage{}, &scopedFetchError{http.StatusUnauthorized, "unauthorized", "invalid or missing credentials", nil}
		}
		state, stateErr := lister.ScopedPolicyState(ctx, subject.Principal)
		if stateErr != nil {
			return domain.ScopedDocumentPage{}, &scopedFetchError{http.StatusInternalServerError, "internal", "failed to list documents", stateErr}
		}
		if !state.Active {
			return domain.ScopedDocumentPage{}, &scopedFetchError{http.StatusForbidden, "forbidden", "access denied", nil}
		}
		if !subject.ValidAt(now()) {
			return domain.ScopedDocumentPage{}, &scopedFetchError{http.StatusUnauthorized, "unauthorized", "invalid or missing credentials", nil}
		}
		if state.Revision == page.Revision {
			return page, nil
		}
	}
	return domain.ScopedDocumentPage{}, &scopedFetchError{http.StatusServiceUnavailable, "unavailable", "authorization policy changed concurrently", nil}
}

func fetchVerifiedScopedSummary(
	ctx context.Context,
	lister domain.ScopedDocumentLister,
	subject middleware.VerifiedSubject,
	now func() time.Time,
	id domain.DocumentID,
	includeIncarnation bool,
) (domain.ScopedDocumentSummary, *scopedFetchError) {
	for range maxPolicyRecheckAttempts {
		if !subject.ValidAt(now()) {
			return domain.ScopedDocumentSummary{}, &scopedFetchError{http.StatusUnauthorized, "unauthorized", "invalid or missing credentials", nil}
		}
		summary, err := lister.GetScopedDocumentSummary(ctx, subject.Principal, id, includeIncarnation)
		if err != nil {
			return domain.ScopedDocumentSummary{}, &scopedFetchError{http.StatusInternalServerError, "internal", "failed to look up document", err}
		}
		if !summary.Active {
			return domain.ScopedDocumentSummary{}, &scopedFetchError{http.StatusForbidden, "forbidden", "access denied", nil}
		}
		if !subject.ValidAt(now()) {
			return domain.ScopedDocumentSummary{}, &scopedFetchError{http.StatusUnauthorized, "unauthorized", "invalid or missing credentials", nil}
		}
		state, stateErr := lister.ScopedPolicyState(ctx, subject.Principal)
		if stateErr != nil {
			return domain.ScopedDocumentSummary{}, &scopedFetchError{http.StatusInternalServerError, "internal", "failed to look up document", stateErr}
		}
		if !state.Active {
			return domain.ScopedDocumentSummary{}, &scopedFetchError{http.StatusForbidden, "forbidden", "access denied", nil}
		}
		if !subject.ValidAt(now()) {
			return domain.ScopedDocumentSummary{}, &scopedFetchError{http.StatusUnauthorized, "unauthorized", "invalid or missing credentials", nil}
		}
		if state.Revision == summary.Revision {
			return summary, nil
		}
	}
	return domain.ScopedDocumentSummary{}, &scopedFetchError{http.StatusServiceUnavailable, "unavailable", "authorization policy changed concurrently", nil}
}

func remainingSigningSeconds(expiresAt, now time.Time) (int, bool) {
	if expiresAt.IsZero() {
		return 0, false
	}
	remaining := expiresAt.Sub(now)
	if remaining < time.Second {
		return 0, false
	}
	seconds := int(remaining / time.Second)
	if seconds > maxPresignTTLSeconds {
		seconds = maxPresignTTLSeconds
	}
	return seconds, true
}
