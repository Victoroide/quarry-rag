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

	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

// DeleteResponse is the DELETE /documents/{doc_id} response body.
type DeleteResponse struct {
	Deleted bool   `json:"deleted"`
	DocID   string `json:"doc_id"`
}

// DeleteOption customizes DELETE /documents/{doc_id}.
type DeleteOption func(*deleteOptions)

type deleteOptions struct{ targets cleanup.Targets }

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
	return func(w http.ResponseWriter, r *http.Request) {
		docIDStr := r.PathValue("doc_id")

		parsedID, err := uuid.Parse(docIDStr)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "invalid query parameters")
			return
		}
		if query.Has("release_held") {
			writeJSONError(w, http.StatusPreconditionFailed, "precondition", "incarnation-qualified deletion and hold release are unavailable")
			return
		}
		incarnation, qualified, valid := deletionIncarnation(r.Header)

		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 60*time.Second)
		defer cancel()
		docID := domain.DocumentID(docIDStr)
		if deleter, ok := cs.(domain.DocumentDeleter); ok {
			active, modeErr := deleter.DeletionActive(ctx)
			if modeErr != nil {
				slog.ErrorContext(ctx, "read deletion mode", "document_id", docIDStr, "error", modeErr)
				writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "document deletion mode is unavailable")
				return
			}
			if active {
				if !valid {
					writeJSONError(w, http.StatusBadRequest, "validation", incarnationHeaderName+" must be exactly one canonical UUID")
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
			writeJSONError(w, http.StatusPreconditionFailed, "precondition", "incarnation-qualified deletion and hold release are unavailable")
			return
		}

		// 1. Check document exists and get s3_key.
		doc, err := cs.GetDocument(ctx, docID)
		if err != nil {
			slog.ErrorContext(ctx, "look up document", "document_id", docIDStr, "error", err)
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to look up document")
			return
		}
		if doc == nil {
			writeJSONError(w, http.StatusNotFound, "not_found", "document not found")
			return
		}
		err = cleanup.Remove(ctx, cs, pcDriver, objects, doc)
		writeDeleteResult(ctx, w, docIDStr, err)
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
			slog.ErrorContext(ctx, "encode delete response", "error", encodeErr)
		}
	case errors.Is(err, domain.ErrDocumentNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", "document not found")
	case errors.Is(err, domain.ErrDeletionPreconditionRequired):
		writeJSONError(w, http.StatusPreconditionRequired, "precondition", incarnationHeaderName+" is required to delete a live document")
	case errors.Is(err, domain.ErrDeletionPreconditionFailed):
		writeJSONError(w, http.StatusPreconditionFailed, "precondition", "document incarnation does not match the stored document")
	case errors.Is(err, domain.ErrFixtureDeletionRequired):
		writeJSONError(w, http.StatusPreconditionFailed, "precondition", "evaluation fixture requires receipt-qualified deletion")
	case errors.Is(err, domain.ErrFileStorageUnavailable):
		writeJSONError(w, http.StatusServiceUnavailable, "storage", "stored file cannot be deleted because file storage is not configured")
	case errors.Is(err, domain.ErrVectorStorageUnavailable):
		writeJSONError(w, http.StatusServiceUnavailable, "storage", "document vectors cannot be deleted because vector storage is not configured")
	case errors.Is(err, domain.ErrDeletionBusy):
		w.Header().Set("Retry-After", "5")
		writeJSONError(w, http.StatusServiceUnavailable, "busy", "document cleanup is in progress; retry the same request")
	case errors.Is(err, domain.ErrDeletionInactive):
		writeJSONError(w, http.StatusPreconditionFailed, "precondition", "incarnation-qualified deletion and hold release are unavailable")
	default:
		slog.ErrorContext(ctx, "delete document", "document_id", docID, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to delete document")
	}
}

// ListDocumentsHandler returns document summaries or legacy references.
func ListDocumentsHandler(cs domain.ChunkStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "invalid query parameters")
			return
		}
		collection, apiErr := listingCollection(query)
		if apiErr != nil {
			writeJSONError(w, http.StatusBadRequest, apiErr.Error, apiErr.Message)
			return
		}
		projection := documentSummaryOptions(query)
		if lister, ok := selectDocumentLister(cs, projection.incarnation); ok {
			listDocumentSummaries(w, r, lister, query, collection, projection)
			return
		}
		if projection.incarnation {
			writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "document incarnations are unavailable")
			return
		}
		if projection.indexVisibility {
			writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "document index visibility is unavailable")
			return
		}
		if collection != "" {
			writeJSONError(w, http.StatusInternalServerError, "internal", "collection filtering is unavailable")
			return
		}
		docs, err := cs.ListDocuments(r.Context())
		if err != nil {
			slog.ErrorContext(r.Context(), "list documents", "error", err)
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to list documents")
			return
		}
		if docs == nil {
			docs = []domain.DocumentRef{}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"documents": docs, "count": len(docs)}); err != nil {
			slog.ErrorContext(r.Context(), "encode document list", "error", err)
		}
	}
}
