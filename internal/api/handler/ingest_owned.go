package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
)

const multipartMemoryBytes = 32 << 20

// ownedIngestHandler admits uploads for a verified principal. Every step that
// can read, write or reveal shared state runs only after the principal's create
// permission and the supplied identity are authorized, and it uses only
// private identity. A deployment that cannot guarantee that refuses all uploads.
func ownedIngestHandler(wk *worker.Worker, cs domain.ChunkStore, s3Client ObjectStore, maxUploadBytes int64, opts ingestOptions) http.HandlerFunc {
	store, capable := cs.(domain.OwnedIngestStore)
	if !capable || opts.contentDedup != domain.ContentDedupOff {
		slog.Error("owned ingestion requires private content identity and an owner-aware store")
		return func(rw http.ResponseWriter, _ *http.Request) {
			rw.Header().Set("Cache-Control", "private, no-store")
			writeJSONError(rw, http.StatusServiceUnavailable, "unavailable", "document store unavailable")
		}
	}
	return func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Cache-Control", "private, no-store")
		subject, ok := middleware.SubjectFromContext(r.Context())
		if !ok || !subject.ValidAt(time.Now()) {
			writeJSONError(rw, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials")
			return
		}
		if len(r.Header.Values(evaluation.ReceiptHeader)) != 0 {
			fixtureUnavailable(rw)
			return
		}
		ingest := ownedIngest{store: store, cs: cs, wk: wk, s3: s3Client, maxUpload: maxUploadBytes, opts: opts, principal: subject.Principal, validUntil: subject.ExpiresAt}
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err == nil && mediaType == "multipart/form-data" {
			ingest.file(rw, r)
			return
		}
		ingest.json(rw, r)
	}
}

type ownedIngest struct {
	store      domain.OwnedIngestStore
	cs         domain.ChunkStore
	wk         *worker.Worker
	s3         ObjectStore
	maxUpload  int64
	opts       ingestOptions
	principal  domain.Principal
	validUntil time.Time
}

// authorize decides, before any extraction or external write, whether the
// principal may use the supplied identity. An identity the principal cannot use
// receives one answer whatever the reason.
func (o ownedIngest) authorize(rw http.ResponseWriter, r *http.Request, collection, supplied string) (domain.OwnedTarget, *domain.OwnerBinding, bool) {
	target, err := o.store.AuthorizeOwnedIngest(r.Context(), o.principal, collection, domain.DocumentID(supplied))
	if err != nil {
		slog.ErrorContext(r.Context(), "authorize owned ingest", "error", err)
		writeJSONError(rw, http.StatusServiceUnavailable, "unavailable", "document store unavailable")
		return target, nil, false
	}
	switch {
	case target.Decision == domain.OwnedForbidden:
		writeJSONError(rw, http.StatusForbidden, "forbidden", "access denied")
		return target, nil, false
	case target.Decision == domain.OwnedNotFound, target.Decision == domain.OwnedNew && supplied != "",
		target.Decision != domain.OwnedNew && (supplied == "" || target.Incarnation == ""):
		writeJSONError(rw, http.StatusNotFound, "not_found", "document not found")
		return target, nil, false
	}
	incarnation := target.Incarnation
	if target.Decision == domain.OwnedNew {
		incarnation = uuid.NewString()
	}
	return target, &domain.OwnerBinding{Principal: o.principal, Incarnation: incarnation, ValidUntil: o.validUntil}, true
}

func (o ownedIngest) request(docID string, supplied bool, collection, docType string, owner *domain.OwnerBinding) domain.AcceptRequest {
	return domain.AcceptRequest{
		Document:       domain.Document{ID: domain.DocumentID(docID)},
		CallerSupplied: supplied,
		DocType:        docType,
		Collection:     collection,
		ContentDedup:   domain.ContentDedupOff,
		MaxPending:     o.opts.maxPending,
		Owner:          owner,
		// Hosted vectors written for an owned document carry its revision whenever the database allows it.
		SupportsScopedVectors: true,
	}
}

func (o ownedIngest) json(rw http.ResponseWriter, r *http.Request) {
	if !isJSONRequest(r) {
		writeJSONError(rw, http.StatusUnsupportedMediaType, "validation", "Content-Type must be application/json or multipart/form-data")
		return
	}
	var req IngestRequest
	if !decodeJSON(rw, r, &req, ingestJSONBodyLimit) {
		return
	}
	req.Content = strings.ReplaceAll(req.Content, "\x00", "")
	apiErr := validateIngestRequest(&req)
	if apiErr == nil {
		apiErr = validateOwnedMetadata(req.Metadata)
	}
	if apiErr != nil {
		writeJSONError(rw, http.StatusBadRequest, apiErr.Error, apiErr.Message)
		return
	}
	docID, ok := ingestDocumentID(rw, req.DocumentID)
	if !ok {
		return
	}
	_, owner, ok := o.authorize(rw, r, req.Collection, suppliedID(req.DocumentID, docID))
	if !ok {
		return
	}
	acceptReq := o.request(docID, req.DocumentID != "", req.Collection, req.DocType, owner)
	acceptReq.Document.Content = req.Content
	acceptReq.Document.Metadata = req.Metadata
	acceptReq.Fingerprint = chunker.ScopedFingerprint(req.Collection, req.Content)
	_, _ = acceptIngest(rw, r, o.wk, o.cs, acceptReq)
}

// formField returns the only value of a multipart field. Query parameters never
// supply a field, and a repeated field is rejected rather than resolved by order.
func formField(form *multipart.Form, name string) (value string, present bool, apiErr *APIError) {
	values, present := form.Value[name]
	if !present {
		return "", false, nil
	}
	if len(values) != 1 {
		return "", true, &APIError{Error: "validation", Message: name + " must be supplied at most once"}
	}
	return values[0], true, nil
}

type ownedFields struct {
	suppliedID string
	collection string
	docType    string
	metadata   map[string]any
}

// suppliedID is the canonical form of an identity the caller chose, or empty.
func suppliedID(raw, canonical string) string {
	if raw == "" {
		return ""
	}
	return canonical
}

// parseOwnedFields reads every field before any decision. The whole body has been
// parsed under the upload limit, so field order cannot change what is enforced.
func parseOwnedFields(form *multipart.Form) (ownedFields, *APIError) {
	fields := ownedFields{metadata: make(map[string]any)}
	for _, field := range []struct {
		name string
		dest *string
	}{{"document_id", &fields.suppliedID}, {"doc_type", &fields.docType}, {"collection", &fields.collection}} {
		value, _, apiErr := formField(form, field.name)
		if apiErr != nil {
			return fields, apiErr
		}
		*field.dest = value
	}
	raw, present, apiErr := formField(form, "metadata")
	if apiErr != nil {
		return fields, apiErr
	}
	if present {
		if decodeJSONObject([]byte(raw), &fields.metadata) != nil {
			return fields, &APIError{Error: "validation", Message: "metadata must be a JSON object without duplicate keys or trailing data"}
		}
		if metadataErr := validateMetadata(fields.metadata); metadataErr != nil {
			return fields, metadataErr
		}
		if reservedErr := validateOwnedMetadata(fields.metadata); reservedErr != nil {
			return fields, reservedErr
		}
	}
	req := IngestRequest{DocType: fields.docType, Collection: fields.collection, Metadata: fields.metadata}
	if fieldErr := validateIngestFields(&req); fieldErr != nil {
		return fields, fieldErr
	}
	fields.docType, fields.collection = req.DocType, req.Collection
	return fields, nil
}

func (o ownedIngest) file(rw http.ResponseWriter, r *http.Request) {
	if o.opts.limiter != nil {
		release, err := o.opts.limiter.Acquire(r.Context())
		if err != nil {
			rw.Header().Set("Retry-After", "5")
			writeJSONError(rw, http.StatusServiceUnavailable, "busy", "too many concurrent uploads")
			return
		}
		defer release()
	}
	r.Body = http.MaxBytesReader(rw, r.Body, o.maxUpload)
	defer func(ctx context.Context) {
		if r.MultipartForm != nil {
			if err := r.MultipartForm.RemoveAll(); err != nil {
				slog.WarnContext(ctx, "remove multipart temporary files", "error", err)
			}
		}
	}(r.Context())
	if err := r.ParseMultipartForm(multipartMemoryBytes); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSONError(rw, http.StatusRequestEntityTooLarge, "validation", "request body exceeds maximum upload size")
			return
		}
		writeJSONError(rw, http.StatusBadRequest, "validation", "file field is required")
		return
	}
	files := r.MultipartForm.File["file"]
	if len(files) != 1 {
		writeJSONError(rw, http.StatusBadRequest, "validation", "file field is required")
		return
	}
	fields, apiErr := parseOwnedFields(r.MultipartForm)
	if apiErr != nil {
		writeJSONError(rw, http.StatusBadRequest, apiErr.Error, apiErr.Message)
		return
	}
	docID, ok := ingestDocumentID(rw, fields.suppliedID)
	if !ok {
		return
	}
	target, owner, ok := o.authorize(rw, r, fields.collection, suppliedID(fields.suppliedID, docID))
	if !ok {
		return
	}
	header := files[0]
	file, err := header.Open()
	if err != nil {
		writeJSONError(rw, http.StatusBadRequest, "validation", "failed to read file")
		return
	}
	defer func(ctx context.Context) {
		if closeErr := file.Close(); closeErr != nil {
			slog.WarnContext(ctx, "close uploaded file", "error", closeErr)
		}
	}(r.Context())
	fileData, err := io.ReadAll(file)
	if err != nil {
		writeJSONError(rw, http.StatusBadRequest, "validation", "failed to read file")
		return
	}
	ft, extractCtx, err := extractor.ResolveForExtraction(r.Context(), header.Filename, fileData)
	if err != nil {
		writeExtractionError(rw, r, err)
		return
	}
	upload, apiErr, extractErr := extractUploadText(extractCtx, o.opts, ft, fileData)
	if extractErr != nil {
		writeExtractionError(rw, r, extractErr)
		return
	}
	if apiErr != nil {
		writeJSONError(rw, http.StatusBadRequest, apiErr.Error, apiErr.Message)
		return
	}
	ftStr, fileName := string(ft), header.Filename
	fields.metadata["source_file"] = fileName
	acceptReq := o.request(docID, fields.suppliedID != "", fields.collection, fields.docType, owner)
	acceptReq.Document.Metadata = fields.metadata
	acceptReq.Document.FileName, acceptReq.Document.FileType, acceptReq.Document.TextSource = &fileName, &ftStr, "parser"
	acceptReq.Document.Content = upload.text
	acceptReq.RequireSource = o.s3 != nil || upload.ocrPending || upload.pdf != nil
	switch {
	case upload.pdf != nil:
		setPDFOriginalFingerprints(&acceptReq, fileData)
		acceptReq.PDF = upload.pdf
	case upload.ocrPending:
		acceptReq.Document.TextSource = "ocr_pending"
		acceptReq.Fingerprint = chunker.ScopedFingerprint(fields.collection, string(fileData))
	default:
		acceptReq.Fingerprint = chunker.ScopedFingerprint(fields.collection, upload.text)
	}
	if target.Decision == domain.OwnedExisting {
		// The caller's own document already stores its original, so a retry writes no object.
		_, _ = acceptIngest(rw, r, o.wk, o.cs, acceptReq)
		return
	}
	if (upload.ocrPending || upload.pdf != nil) && o.s3 == nil {
		rw.Header().Set("Retry-After", "30")
		writeJSONError(rw, http.StatusServiceUnavailable, "storage", "source file storage is unavailable")
		return
	}
	var s3Key *string
	if o.s3 != nil {
		docType := fields.docType
		if docType == "" {
			docType = domain.DefaultDocType
		}
		// The object is named for the intended incarnation and this attempt alone.
		key, uploadErr := o.s3.Upload(r.Context(), docID+"/"+owner.Incarnation+"/"+uuid.NewString(), docType, fileName, ft, bytes.NewReader(fileData))
		if uploadErr != nil {
			slog.ErrorContext(r.Context(), "upload source file", "error", uploadErr)
			rw.Header().Set("Retry-After", "30")
			writeJSONError(rw, http.StatusServiceUnavailable, "storage", "source file storage is unavailable")
			return
		}
		s3Key = &key
		acceptReq.Document.S3Key = s3Key
	}
	outcome, acceptErr := acceptIngest(rw, r, o.wk, o.cs, acceptReq)
	if s3Key != nil && outcome != domain.AcceptAccepted {
		releaseAttemptObject(r.Context(), o.cs, o.s3, docID, *s3Key, acceptErr)
	}
}
