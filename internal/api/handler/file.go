package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

// FileSigner creates a short-lived source download address.
type FileSigner interface {
	PresignGet(context.Context, string) (string, error)
}

// ExpiringFileSigner creates a presigned GET URL with an explicit bounded TTL.
type ExpiringFileSigner interface {
	FileSigner
	PresignGetWithTTL(context.Context, string, time.Duration) (string, error)
}

// FileResponse is the GET /documents/{doc_id}/file response body.
type FileResponse struct {
	URL       string `json:"url"`
	ExpiresIn int    `json:"expires_in"`
}

// FileHandler returns an HTTP handler for GET /documents/{doc_id}/file.
func FileHandler(cs domain.ChunkStore, signer FileSigner, opts ...FileOption) http.HandlerFunc {
	var options fileOptions
	for _, opt := range opts {
		opt(&options)
	}
	if options.scoped {
		return scopedFileHandler(cs, signer, options)
	}
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

func scopedFileHandler(cs domain.ChunkStore, signer FileSigner, opts fileOptions) http.HandlerFunc {
	now := clockOrDefault(opts.now)
	return func(w http.ResponseWriter, r *http.Request) {
		subject, ok := requireScopedSubject(w, r, now)
		if !ok {
			return
		}
		parsedID, err := uuid.Parse(r.PathValue("doc_id"))
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		if _, remainingOK := remainingSigningSeconds(subject.ExpiresAt, now()); !remainingOK {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials")
			return
		}
		reader, capable := cs.(domain.ScopedDocumentFileReader)
		if !capable {
			writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "document file access is unavailable")
			return
		}
		if signer == nil {
			writeJSONError(w, http.StatusNotFound, "not_found", "document not found")
			return
		}
		expiring, supportsTTL := signer.(ExpiringFileSigner)
		if !supportsTTL {
			writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "file signing is unavailable")
			return
		}

		docID := domain.DocumentID(parsedID.String())
		var presignedURL string
		var signErr error
		var ttlSeconds int
		res, err := reader.WithScopedDocumentFile(r.Context(), subject.Principal, docID, func(ctx context.Context, key string) error {
			var validTTL bool
			ttlSeconds, validTTL = remainingSigningSeconds(subject.ExpiresAt, now())
			if !validTTL {
				return errScopedCredentialExpired
			}
			presignedURL, signErr = expiring.PresignGetWithTTL(ctx, key, time.Duration(ttlSeconds)*time.Second)
			return signErr
		})
		if errors.Is(err, errScopedCredentialExpired) {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials")
			return
		}
		if signErr != nil {
			slog.ErrorContext(r.Context(), "presign scoped document file", "document_id", docID, "error", signErr)
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to generate download URL")
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "get scoped document file", "document_id", docID, "error", err)
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to look up document")
			return
		}
		if !res.Active {
			writeJSONError(w, http.StatusForbidden, "forbidden", "access denied")
			return
		}
		if !res.Found {
			writeJSONError(w, http.StatusNotFound, "not_found", "document not found")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(FileResponse{URL: presignedURL, ExpiresIn: ttlSeconds}); err != nil {
			slog.ErrorContext(r.Context(), "encode scoped file response", "error", err)
		}
	}
}
