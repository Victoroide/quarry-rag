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
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/google/uuid"
)

// DeleteResponse is the DELETE /documents/{doc_id} response body.
type DeleteResponse struct {
	Deleted bool   `json:"deleted"`
	DocID   string `json:"doc_id"`
}

// DeleteHandler returns an HTTP handler for DELETE /documents/{doc_id}.
func DeleteHandler(cs domain.ChunkStore, pcDriver domain.VectorDeleter, s3Client *s3store.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		docIDStr := r.PathValue("doc_id")

		if _, err := uuid.Parse(docIDStr); err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "invalid query parameters")
			return
		}
		if query.Has("release_held") || hasIncarnationHeader(r.Header) {
			writeJSONError(w, http.StatusPreconditionFailed, "precondition", "incarnation-qualified deletion and hold release are unavailable")
			return
		}

		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 60*time.Second)
		defer cancel()
		docID := domain.DocumentID(docIDStr)

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

		var objects cleanup.ObjectDeleter
		if s3Client != nil {
			objects = tracing.WrapObjectStore(s3Client)
		}
		if err := cleanup.Remove(ctx, cs, pcDriver, objects, doc); err != nil {
			if errors.Is(err, cleanup.ErrFileStorageUnavailable) {
				writeJSONError(w, http.StatusServiceUnavailable, "storage", "stored file cannot be deleted because file storage is not configured")
				return
			}
			slog.ErrorContext(ctx, "delete document", "document_id", docIDStr, "error", err)
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to delete document")
			return
		}

		resp := DeleteResponse{
			Deleted: true,
			DocID:   docIDStr,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.ErrorContext(ctx, "encode delete response", "error", err)
		}
	}
}

func hasIncarnationHeader(headers http.Header) bool {
	for name := range headers {
		if strings.EqualFold(name, "X-Document-Incarnation") {
			return true
		}
	}
	return false
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
