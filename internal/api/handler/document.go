package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/google/uuid"
)

// DeleteResponse is the DELETE /documents/{doc_id} response body.
type DeleteResponse struct {
	Deleted bool   `json:"deleted"`
	DocID   string `json:"doc_id"`
}

// DeleteOption customizes DELETE /documents/{doc_id}.
type DeleteOption func(*deleteOptions)

type deleteOptions struct {
	targets            cleanup.Targets
	legacyReservations bool
	scoped             bool
	now                func() time.Time
	audit              domain.SecurityAuditRecorder
}

// WithLegacyReservations makes an ID-only request commit a durable reservation
// under the activation lock before it mutates any external store, so activation
// and a replacement admission cannot overtake its last call. Enable it only in a
// deployment that can also quarantine, release and activate.
func WithLegacyReservations() DeleteOption {
	return func(opts *deleteOptions) { opts.legacyReservations = true }
}

// WithCleanupTargets names the stores the deletion acts on, so a checkpointed
// deletion records them and refuses to resume against different ones.
func WithCleanupTargets(targets cleanup.Targets) DeleteOption {
	return func(opts *deleteOptions) { opts.targets = targets }
}

// incarnationHeaderName selects the document lifetime a deletion addresses.
const incarnationHeaderName = "X-Document-Incarnation"

// DeleteHandler returns an HTTP handler for DELETE /documents/{doc_id}. The
// database selects the contract: before checkpointed deletion is active the
// request addresses the live UUID, and afterwards it must name its incarnation.
func DeleteHandler(cs domain.ChunkStore, pcDriver domain.VectorDeleter, objects cleanup.ObjectDeleter, opts ...DeleteOption) http.HandlerFunc {
	var options deleteOptions
	for _, opt := range opts {
		opt(&options)
	}
	if options.scoped {
		return scopedDeleteHandler(cs, pcDriver, objects, options)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		docIDStr := r.PathValue("doc_id")

		parsedID, err := uuid.Parse(docIDStr)
		if err != nil {
			writeJSONError(r.Context(), w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeJSONError(r.Context(), w, http.StatusBadRequest, "validation", "invalid query parameters")
			return
		}
		releaseHeld, ok := parseReleaseHeld(query)
		if !ok {
			writeJSONError(r.Context(), w, http.StatusBadRequest, "validation", "release_held must be a single true or false value")
			return
		}
		incarnation, qualified, valid := deletionIncarnation(r.Header)

		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), domain.WriteRequestTimeout)
		defer cancel()
		guard, guarded := cs.(domain.LegacyDeletionGuard)
		guarded = guarded && options.legacyReservations && !qualified
		legacyCtx, releaseSlot := ctx, func() {}
		if limiter, ok := cs.(domain.LegacyDeletionLimiter); ok && guarded {
			// A request over the replica's bound is refused before it takes a writer
			// session, a lock or a connection.
			if legacyCtx, releaseSlot, err = limiter.ReserveLegacyDeletion(ctx); err != nil {
				writeDeleteResult(ctx, w, docIDStr, err)
				return
			}
			defer releaseSlot()
		}
		release, gateErr := acquireDeleteWriter(ctx, cs)
		if gateErr != nil {
			writeDeleteGateError(ctx, w, docIDStr, gateErr)
			return
		}
		if release != nil {
			defer release()
		}
		docID := domain.DocumentID(docIDStr)
		if releaseHeld {
			if !valid {
				writeJSONError(r.Context(), w, http.StatusBadRequest, "validation", incarnationHeaderName+" must be exactly one canonical UUID")
				return
			}
			if !qualified {
				writeJSONError(r.Context(), w, http.StatusPreconditionRequired, "precondition", incarnationHeaderName+" is required to release a held deletion")
				return
			}
			canonical := parsedID.String()
			err = cleanup.ReleaseOfflineHold(ctx, cs, pcDriver, objects, options.targets, domain.DocumentID(canonical), incarnation)
			writeDeleteResult(ctx, w, canonical, err)
			return
		}
		if guarded {
			// The activation lock decides the contract. A request that loses the race
			// to an activation continues below as an activated ID-only request.
			err = cleanup.RemoveLegacy(legacyCtx, guard, pcDriver, objects, options.targets, docID)
			releaseSlot()
			if !errors.Is(err, domain.ErrDeletionActivated) {
				writeDeleteResult(ctx, w, docIDStr, err)
				return
			}
		}
		if deleter, ok := cs.(domain.DocumentDeleter); ok {
			active, modeErr := deleter.DeletionActive(ctx)
			if modeErr != nil {
				slog.ErrorContext(ctx, "read deletion mode", "document_id", docIDStr, "error", tracing.ErrorForTelemetry(ctx, modeErr))
				writeJSONError(r.Context(), w, http.StatusServiceUnavailable, "unavailable", "document deletion mode is unavailable")
				return
			}
			if active {
				if !valid {
					writeJSONError(r.Context(), w, http.StatusBadRequest, "validation", incarnationHeaderName+" must be exactly one canonical UUID")
					return
				}
				// External stores and receipts key the document by its canonical form.
				canonical := parsedID.String()
				err = cleanup.RemoveIncarnation(ctx, deleter, pcDriver, objects, options.targets, domain.DocumentID(canonical), incarnation)
				writeDeleteResult(ctx, w, canonical, err)
				return
			}
		}
		if qualified {
			writeJSONError(r.Context(), w, http.StatusPreconditionFailed, "precondition", "incarnation-qualified deletion and hold release are unavailable")
			return
		}

		// 1. Check document exists and get s3_key.
		doc, err := cs.GetDocument(ctx, docID)
		if err != nil {
			slog.ErrorContext(ctx, "look up document", "document_id", docIDStr, "error", tracing.ErrorForTelemetry(ctx, err))
			writeJSONError(r.Context(), w, http.StatusInternalServerError, "internal", "failed to look up document")
			return
		}
		if doc == nil {
			writeJSONError(r.Context(), w, http.StatusNotFound, "not_found", "document not found")
			return
		}
		err = cleanup.Remove(ctx, cs, pcDriver, objects, doc)
		writeDeleteResult(ctx, w, docIDStr, err)
	}
}

// parseReleaseHeld accepts zero or one literal "true" or "false" value.
// Empty values, other spellings and duplicates are rejected so validation
// failures never reach ordinary deletion.
func parseReleaseHeld(query url.Values) (release, ok bool) {
	values, present := query["release_held"]
	if !present {
		return false, true
	}
	if len(values) != 1 {
		return false, false
	}
	switch values[0] {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

// deletionIncarnation reads the optional incarnation header. It reports valid
// as false for an empty, repeated or non-canonical value, so a malformed
// qualifier can never select an unqualified deletion.
func deletionIncarnation(headers http.Header) (incarnation string, present, valid bool) {
	var values []string
	for name, headerValues := range headers {
		if strings.EqualFold(name, incarnationHeaderName) {
			present = true
			values = append(values, headerValues...)
		}
	}
	if !present {
		return "", false, true
	}
	if len(values) != 1 || len(values[0]) != len(uuid.UUID{}.String()) {
		return "", true, false
	}
	parsed, err := uuid.Parse(values[0])
	if err != nil {
		return "", true, false
	}
	return parsed.String(), true, true
}

func writeDeleteResult(ctx context.Context, w http.ResponseWriter, docID string, err error) {
	switch {
	case err == nil:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if encodeErr := json.NewEncoder(w).Encode(DeleteResponse{Deleted: true, DocID: docID}); encodeErr != nil {
			slog.ErrorContext(ctx, "encode delete response", "error", tracing.ErrorForTelemetry(ctx, encodeErr))
		}
	case errors.Is(err, domain.ErrDocumentNotFound):
		writeJSONError(ctx, w, http.StatusNotFound, "not_found", "document not found")
	case errors.Is(err, domain.ErrUnauthorized):
		writeJSONError(ctx, w, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials")
	case errors.Is(err, domain.ErrForbidden):
		writeJSONError(ctx, w, http.StatusForbidden, "forbidden", "access denied")
	case errors.Is(err, domain.ErrDeletionPreconditionRequired):
		writeJSONError(ctx, w, http.StatusPreconditionRequired, "precondition", incarnationHeaderName+" is required to delete a live document")
	case errors.Is(err, domain.ErrDeletionPreconditionFailed):
		writeJSONError(ctx, w, http.StatusPreconditionFailed, "precondition", "document incarnation does not match the stored document")
	case errors.Is(err, domain.ErrFixtureDeletionRequired):
		writeJSONError(ctx, w, http.StatusPreconditionFailed, "precondition", "evaluation fixture requires receipt-qualified deletion")
	case errors.Is(err, domain.ErrFileStorageUnavailable):
		writeJSONError(ctx, w, http.StatusServiceUnavailable, "storage", "stored file cannot be deleted because file storage is not configured")
	case errors.Is(err, domain.ErrVectorStorageUnavailable):
		writeJSONError(ctx, w, http.StatusServiceUnavailable, "storage", "document vectors cannot be deleted because vector storage is not configured")
	case errors.Is(err, domain.ErrDeletionBusy):
		w.Header().Set("Retry-After", "5")
		writeJSONError(ctx, w, http.StatusServiceUnavailable, "busy", "document cleanup is in progress; retry the same request")
	case errors.Is(err, domain.ErrTooManyLegacyDeletions):
		w.Header().Set("Retry-After", "5")
		writeJSONError(ctx, w, http.StatusServiceUnavailable, "busy", "too many concurrent deletions; retry later")
	case errors.Is(err, domain.ErrDeletionInactive):
		writeJSONError(ctx, w, http.StatusPreconditionFailed, "precondition", "incarnation-qualified deletion and hold release are unavailable")
	default:
		if tracing.RedactIdentifiers(ctx) {
			slog.ErrorContext(ctx, "delete document", "error", tracing.ErrorForTelemetry(ctx, err))
		} else {
			slog.ErrorContext(ctx, "delete document", "document_id", docID, "error", tracing.ErrorForTelemetry(ctx, err))
		}
		writeJSONError(ctx, w, http.StatusInternalServerError, "internal", "failed to delete document")
	}
}

func acquireDeleteWriter(ctx context.Context, cs domain.ChunkStore) (func(), error) {
	switch gate := cs.(type) {
	case domain.WriterLocker:
		return gate.AcquireWriter(ctx)
	case domain.WriterGate:
		return nil, gate.WritersOpen(ctx)
	default:
		return nil, nil
	}
}

func writeDeleteGateError(ctx context.Context, w http.ResponseWriter, docIDStr string, gateErr error) {
	switch {
	case errors.Is(gateErr, domain.ErrWritersFrozen):
		writeFrozen(ctx, w)
	case errors.Is(gateErr, domain.ErrGenerationMismatch):
		w.Header().Set("Retry-After", "30")
		writeJSONError(ctx, w, http.StatusServiceUnavailable, "unavailable", "this replica does not serve the active vector generation")
	default:
		if tracing.RedactIdentifiers(ctx) {
			slog.ErrorContext(ctx, "check document writers", "error", tracing.ErrorForTelemetry(ctx, gateErr))
		} else {
			slog.ErrorContext(ctx, "check document writers", "document_id", docIDStr, "error", tracing.ErrorForTelemetry(ctx, gateErr))
		}
		w.Header().Set("Retry-After", "30")
		writeJSONError(ctx, w, http.StatusServiceUnavailable, "unavailable", "document store unavailable")
	}
}

func scopedDeleteHandler(cs domain.ChunkStore, pcDriver domain.VectorDeleter, objects cleanup.ObjectDeleter, options deleteOptions) http.HandlerFunc {
	now := clockOrDefault(options.now)
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := tracing.WithRedactIdentifiers(r.Context(), true)
		r = r.WithContext(ctx)
		subject, ok := requireAuditedScopedSubject(w, r, now, options.audit, "document_delete")
		if !ok {
			return
		}
		docIDStr := r.PathValue("doc_id")
		parsedID, err := uuid.Parse(docIDStr)
		if err != nil {
			writeJSONError(ctx, w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeJSONError(ctx, w, http.StatusBadRequest, "validation", "invalid query parameters")
			return
		}
		if query.Has("release_held") {
			recordSecurityAuditBestEffort(ctx, options.audit, &subject, nil, "document_delete", "deny", "release_held_unsupported", parsedID.String(), "")
			writeJSONError(ctx, w, http.StatusPreconditionFailed, "precondition", "incarnation-qualified deletion and hold release are unavailable")
			return
		}
		incarnation, _, valid := deletionIncarnation(r.Header)
		if !valid || incarnation == uuid.Nil.String() {
			writeJSONError(ctx, w, http.StatusBadRequest, "validation", incarnationHeaderName+" must be exactly one canonical UUID")
			return
		}
		canonical := parsedID.String()
		deleter, capable := cs.(domain.ScopedDocumentDeleter)
		if !capable {
			recordSecurityAuditBestEffort(ctx, options.audit, &subject, nil, "document_delete", "error", "deletion_unavailable", canonical, incarnation)
			writeJSONError(ctx, w, http.StatusServiceUnavailable, "unavailable", "document deletion is unavailable")
			return
		}
		release, gateErr := acquireDeleteWriter(ctx, cs)
		if gateErr != nil {
			if (errors.Is(gateErr, context.Canceled) || errors.Is(gateErr, context.DeadlineExceeded)) && !subject.ValidAt(now()) {
				recordSecurityAuditBestEffort(ctx, options.audit, &subject, nil, "document_delete", "deny", "credential_expired", canonical, incarnation)
				writeDeleteResult(ctx, w, canonical, domain.ErrUnauthorized)
				return
			}
			recordSecurityAuditBestEffort(ctx, options.audit, &subject, nil, "document_delete", "error", "writer_gate_unavailable", canonical, incarnation)
			writeDeleteGateError(ctx, w, docIDStr, gateErr)
			return
		}
		if release != nil {
			defer release()
		}
		err = cleanup.RemoveScoped(ctx, deleter, pcDriver, objects, options.targets, subject.Principal, subject.ExpiresAt, domain.DocumentID(canonical), incarnation)
		if errors.Is(err, cleanup.ErrScopedDeletionCanceled) {
			if !subject.ValidAt(now()) {
				err = domain.ErrUnauthorized
			} else {
				recordSecurityAuditBestEffort(ctx, options.audit, &subject, nil, "document_delete", "error", "deletion_canceled", canonical, incarnation)
				writeJSONError(ctx, w, http.StatusServiceUnavailable, "unavailable", "document deletion canceled")
				return
			}
		}
		if err != nil {
			auditDeleteFailure(ctx, options.audit, &subject, canonical, incarnation, err)
		}
		writeDeleteResult(ctx, w, canonical, err)
	}
}

func auditDeleteFailure(ctx context.Context, recorder domain.SecurityAuditRecorder, subject *middleware.VerifiedSubject, docID, incarnation string, err error) {
	outcome, reason := "error", "delete_failed"
	switch {
	case errors.Is(err, domain.ErrUnauthorized):
		outcome, reason = "deny", "credential_expired"
	case errors.Is(err, domain.ErrForbidden):
		outcome, reason = "deny", "access_denied"
	case errors.Is(err, domain.ErrDocumentNotFound):
		outcome, reason = "deny", "not_found"
	case errors.Is(err, domain.ErrDeletionPreconditionRequired),
		errors.Is(err, domain.ErrDeletionPreconditionFailed),
		errors.Is(err, domain.ErrFixtureDeletionRequired):
		outcome, reason = "deny", "precondition_failed"
	case errors.Is(err, domain.ErrDeletionInactive):
		outcome, reason = "deny", "deletion_inactive"
	case errors.Is(err, domain.ErrAuthzModeMismatch):
		reason = "authz_mode_mismatch"
	case errors.Is(err, domain.ErrDeletionBusy):
		reason = "deletion_busy"
	case errors.Is(err, domain.ErrScopedDeletionCommitUnconfirmed):
		reason = "commit_unconfirmed"
	}
	recordSecurityAuditBestEffort(ctx, recorder, subject, nil, "document_delete", outcome, reason, docID, incarnation)
}

// ListDocumentsHandler returns document summaries or legacy references.
func ListDocumentsHandler(cs domain.ChunkStore, opts ...DocumentListOption) http.HandlerFunc {
	options := documentListOptions{streamBatchSize: defaultScopedStreamBatchSize}
	for _, opt := range opts {
		opt(&options)
	}
	if options.scoped {
		return scopedListDocumentsHandler(cs, options)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeJSONError(r.Context(), w, http.StatusBadRequest, "validation", "invalid query parameters")
			return
		}
		collection, apiErr := listingCollection(query)
		if apiErr != nil {
			writeJSONError(r.Context(), w, http.StatusBadRequest, apiErr.Error, apiErr.Message)
			return
		}
		projection := documentSummaryOptions(query)
		if lister, ok := selectDocumentLister(cs, projection.incarnation); ok {
			listDocumentSummaries(w, r, lister, query, collection, projection)
			return
		}
		if projection.incarnation {
			writeJSONError(r.Context(), w, http.StatusServiceUnavailable, "unavailable", "document incarnations are unavailable")
			return
		}
		if projection.indexVisibility {
			writeJSONError(r.Context(), w, http.StatusServiceUnavailable, "unavailable", "document index visibility is unavailable")
			return
		}
		if collection != "" {
			writeJSONError(r.Context(), w, http.StatusInternalServerError, "internal", "collection filtering is unavailable")
			return
		}
		docs, err := cs.ListDocuments(r.Context())
		if err != nil {
			slog.ErrorContext(r.Context(), "list documents", "error", tracing.ErrorForTelemetry(r.Context(), err))
			writeJSONError(r.Context(), w, http.StatusInternalServerError, "internal", "failed to list documents")
			return
		}
		if docs == nil {
			docs = []domain.DocumentRef{}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"documents": docs, "count": len(docs)}); err != nil {
			slog.ErrorContext(r.Context(), "encode document list", "error", tracing.ErrorForTelemetry(r.Context(), err))
		}
	}
}
