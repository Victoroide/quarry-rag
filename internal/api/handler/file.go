package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

// FileSigner creates a short-lived source download address.
type FileSigner interface {
	PresignGet(context.Context, string) (string, error)
}

// FileResponse is the GET /documents/{doc_id}/file response body.
type FileResponse struct {
	URL       string `json:"url"`
	ExpiresIn int    `json:"expires_in"`
}

// FileHandler returns an HTTP handler for GET /documents/{doc_id}/file.
func FileHandler(cs domain.ChunkStore, signer FileSigner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		docIDStr := r.PathValue("doc_id")

		if _, err := uuid.Parse(docIDStr); err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}

		if signer == nil {
			writeJSONError(w, http.StatusNotFound, "not_found", "file storage is not configured")
			return
		}

		docID := domain.DocumentID(docIDStr)
		var presignedURL string
		var signErr error
		found, err := signDocumentFile(r.Context(), cs, docID, func(ctx context.Context, key string) error {
			presignedURL, signErr = signer.PresignGet(ctx, key)
			return signErr
		})
		if signErr != nil {
			slog.ErrorContext(r.Context(), "presign document file", "document_id", docID, "error", signErr)
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to generate download URL")
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "get document file", "document_id", docID, "error", err)
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to look up document")
			return
		}
		if !found {
			writeJSONError(w, http.StatusNotFound, "not_found", "no file stored for this document")
			return
		}

		resp := FileResponse{
			URL:       presignedURL,
			ExpiresIn: 900,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.ErrorContext(r.Context(), "encode file response", "error", err)
		}
	}
}

// signDocumentFile signs the stored source while the store keeps the document
// from entering deletion, when the store supports that guard.
func signDocumentFile(ctx context.Context, cs domain.ChunkStore, id domain.DocumentID, sign func(context.Context, string) error) (bool, error) {
	if reader, ok := cs.(domain.DocumentFileReader); ok {
		return reader.WithDocumentFile(ctx, id, sign)
	}
	key, err := cs.GetDocumentS3Key(ctx, id)
	if err != nil || key == nil {
		return false, err
	}
	return true, sign(ctx, *key)
}
