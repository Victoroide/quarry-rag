package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/Victoroide/quarry-rag/internal/worker"
)

// ObjectStore stores and removes original uploaded files.
type ObjectStore interface {
	Upload(context.Context, string, string, string, extractor.FileType, io.Reader) (string, error)
	Delete(context.Context, string) error
}

// JSON escaping can expand each content byte sixfold; reserve room for metadata.
const ingestJSONBodyLimit = 6*domain.MaxContentBytes + 2<<20

// IngestRequest is the POST /ingest request body.
//
// DocumentID is optional. The ingest policy determines the ID for omitted
// values. A supplied value must be a UUID.
type IngestRequest struct {
	DocumentID string         `json:"document_id,omitempty"`
	Content    string         `json:"content"`
	Metadata   map[string]any `json:"metadata,omitzero"`
	DocType    string         `json:"doc_type,omitempty"`
	Collection string         `json:"collection,omitempty"`
}

// IngestResponse is the POST /ingest response body.
type IngestResponse struct {
	DocumentID string `json:"document_id"`
	Status     string `json:"status"`
}

func validateIngestRequest(req *IngestRequest) *APIError {
	if apiErr := validateIngestContent(req.Content); apiErr != nil {
		return apiErr
	}
	return validateIngestFields(req)
}

func validateIngestContent(content string) *APIError {
	if strings.TrimSpace(content) == "" {
		return &APIError{Error: "validation", Message: "content is required"}
	}
	if len(content) > domain.MaxContentBytes {
		return &APIError{Error: "validation", Message: "content exceeds maximum length of 1000000 bytes"}
	}
	return nil
}

func validateIngestFields(req *IngestRequest) *APIError {
	var apiErr *APIError
	req.DocType, apiErr = validateDocType(req.DocType)
	if apiErr != nil {
		return apiErr
	}
	req.Collection, apiErr = validateLabel("collection", req.Collection)
	if apiErr != nil {
		return apiErr
	}
	return validateMetadata(req.Metadata)
}

// IngestHandler returns an HTTP handler for POST /ingest.
// Supports two content types:
//   - application/json: existing JSON body with "content" field
//   - multipart/form-data: file upload with text extraction
func IngestHandler(wk *worker.Worker, cs domain.ChunkStore, s3Client ObjectStore, maxUploadBytes int64, opts ...IngestOption) http.HandlerFunc {
	options := ingestOptions{maxPending: 1000, contentDedup: domain.ContentDedupCollection, extract: tracing.WrapExtract(func(ctx context.Context, ft extractor.FileType, data []byte) (string, error) {
		return extractor.New(ft).Extract(ctx, bytes.NewReader(data))
	})}
	for _, opt := range opts {
		opt(&options)
	}
	if options.owned {
		return ownedIngestHandler(wk, cs, s3Client, maxUploadBytes, options)
	}
	return func(rw http.ResponseWriter, r *http.Request) {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err == nil && mediaType == "multipart/form-data" {
			if len(r.Header.Values(evaluation.ReceiptHeader)) != 0 {
				fixtureUnavailable(rw)
				return
			}
			handleFileIngest(rw, r, wk, cs, s3Client, maxUploadBytes, options)
			return
		}
		handleJSONIngest(rw, r, wk, cs, options)
	}
}

func handleJSONIngest(rw http.ResponseWriter, r *http.Request, wk *worker.Worker, cs domain.ChunkStore, opts ingestOptions) {
	if !isJSONRequest(r) {
		writeJSONError(rw, http.StatusUnsupportedMediaType, "validation", "Content-Type must be application/json or multipart/form-data")
		return
	}
	var req IngestRequest
	if !decodeJSON(rw, r, &req, ingestJSONBodyLimit) {
		return
	}

	req.Content = strings.ReplaceAll(req.Content, "\x00", "")
	if apiErr := validateIngestRequest(&req); apiErr != nil {
		writeJSONError(rw, http.StatusBadRequest, apiErr.Error, apiErr.Message)
		return
	}

	docID, ok := ingestDocumentID(rw, req.DocumentID)
	if !ok {
		return
	}
	var receipt string
	if len(r.Header.Values(evaluation.ReceiptHeader)) != 0 {
		ns, supplied, fixture, valid := fixtureRequest(r, opts.fixturePrefix, docID)
		if _, supported := cs.(domain.EvaluationFixtureStore); !valid || !supported || opts.fixtureVectorIdentity == "" ||
			req.Collection != ns.Collection(fixture.Distractor) || req.Content != fixture.Text {
			fixtureUnavailable(rw)
			return
		}
		receipt = supplied
	}
	_, _ = acceptIngest(rw, r, wk, cs, domain.AcceptRequest{
		Document:                 domain.Document{ID: domain.DocumentID(docID), Content: req.Content, Metadata: req.Metadata},
		CallerSupplied:           req.DocumentID != "",
		Fingerprint:              chunker.ScopedFingerprint(req.Collection, req.Content),
		DocType:                  req.DocType,
		Collection:               req.Collection,
		ContentDedup:             opts.contentDedup,
		MaxPending:               opts.maxPending,
		EvaluationReceipt:        receipt,
		EvaluationVectorIdentity: opts.fixtureVectorIdentity,
		SupportsScopedVectors:    opts.scopedVectors,
	})
}

func handleFileIngest(rw http.ResponseWriter, r *http.Request, wk *worker.Worker, cs domain.ChunkStore, s3Client ObjectStore, maxUploadBytes int64, opts ingestOptions) {
	if opts.limiter != nil {
		release, err := opts.limiter.Acquire(r.Context())
		if err != nil {
			rw.Header().Set("Retry-After", "5")
			writeJSONError(rw, http.StatusServiceUnavailable, "busy", "too many concurrent uploads")
			return
		}
		defer release()
	}
	r.Body = http.MaxBytesReader(rw, r.Body, maxUploadBytes)

	defer func(ctx context.Context) {
		if r.MultipartForm != nil {
			if err := r.MultipartForm.RemoveAll(); err != nil {
				slog.WarnContext(ctx, "remove multipart temporary files", "error", err)
			}
		}
	}(r.Context())
	file, header, err := r.FormFile("file")
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSONError(rw, http.StatusRequestEntityTooLarge, "validation", "request body exceeds maximum upload size")
			return
		}
		writeJSONError(rw, http.StatusBadRequest, "validation", "file field is required")
		return
	}
	defer func(ctx context.Context) {
		if closeErr := file.Close(); closeErr != nil {
			slog.WarnContext(ctx, "close uploaded file", "error", closeErr)
		}
	}(r.Context())

	// Read the entire file into memory for extraction and S3 upload.
	fileData, err := io.ReadAll(file)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSONError(rw, http.StatusRequestEntityTooLarge, "validation", "request body exceeds maximum upload size")
			return
		}
		writeJSONError(rw, http.StatusBadRequest, "validation", "failed to read file")
		return
	}

	ft, extractCtx, err := extractor.ResolveForExtraction(r.Context(), header.Filename, fileData)
	if err != nil {
		writeExtractionError(rw, r, err)
		return
	}

	suppliedID := r.FormValue("document_id")
	docID, ok := ingestDocumentID(rw, suppliedID)
	if !ok {
		return
	}
	formDocType := r.FormValue("doc_type")
	ftStr, fileName := string(ft), header.Filename
	metadata := make(map[string]any)
	if values, present := r.MultipartForm.Value["metadata"]; present {
		if len(values) != 1 || decodeJSONObject([]byte(values[0]), &metadata) != nil {
			writeJSONError(rw, http.StatusBadRequest, "validation", "metadata must be a JSON object without duplicate keys or trailing data")
			return
		}
		if apiErr := validateMetadata(metadata); apiErr != nil {
			writeJSONError(rw, http.StatusBadRequest, apiErr.Error, apiErr.Message)
			return
		}
	}
	metadata["source_file"] = fileName
	collection := ""
	if values, present := r.MultipartForm.Value["collection"]; present {
		if len(values) != 1 {
			writeJSONError(rw, http.StatusBadRequest, "validation", "collection must be supplied at most once")
			return
		}
		collection = values[0]
	}
	req := IngestRequest{DocType: formDocType, Collection: collection, Metadata: metadata}
	if apiErr := validateIngestFields(&req); apiErr != nil {
		writeJSONError(rw, http.StatusBadRequest, apiErr.Error, apiErr.Message)
		return
	}
	formDocType = req.DocType
	acceptReq := domain.AcceptRequest{
		Document:              domain.Document{ID: domain.DocumentID(docID), Metadata: metadata, FileName: &fileName, FileType: &ftStr, TextSource: "parser"},
		CallerSupplied:        suppliedID != "",
		DocType:               formDocType,
		Collection:            req.Collection,
		ContentDedup:          opts.contentDedup,
		MaxPending:            opts.maxPending,
		SupportsScopedVectors: opts.scopedVectors,
	}
	if ft == extractor.FileTypePDF && (acceptReq.CallerSupplied || opts.extractPages != nil || opts.pdfLayout) {
		setPDFOriginalFingerprints(&acceptReq, fileData, activePDFProfile(opts))
		if (acceptReq.CallerSupplied || acceptReq.ContentDedup == domain.ContentDedupCollection) &&
			acceptExistingPDFUpload(rw, r, wk, cs, opts, fileData, acceptReq) {
			return
		}
	}

	upload, apiErr, extractErr := extractUploadText(extractCtx, opts, ft, fileData)
	if extractErr != nil {
		writeExtractionError(rw, r, extractErr)
		return
	}
	if apiErr != nil {
		writeJSONError(rw, http.StatusBadRequest, apiErr.Error, apiErr.Message)
		return
	}
	text, ocrPending, pdfExtraction := upload.text, upload.ocrPending, upload.pdf
	acceptReq.Document.Content = text
	acceptReq.RequireSource = s3Client != nil || ocrPending || pdfExtraction != nil
	if ocrPending {
		acceptReq.Document.TextSource = "ocr_pending"
		acceptReq.Fingerprint = acceptReq.LegacyOCRFingerprint
		if acceptReq.Fingerprint == "" {
			acceptReq.Fingerprint = chunker.ScopedFingerprint(req.Collection, string(fileData))
		}
	} else if pdfExtraction == nil {
		acceptReq.Fingerprint = chunker.ScopedFingerprint(req.Collection, text)
	}
	if pdfExtraction != nil {
		acceptReq.PDF = pdfExtraction
		acceptReq.TryLegacyPDFText = !acceptReq.CallerSupplied && opts.contentDedup == domain.ContentDedupCollection && pdfExtraction.Coverage == domain.PDFComplete
		if acceptMixedPDF(rw, r, wk, cs, opts, fileData, &acceptReq) {
			return
		}
	} else if acceptReq.RequireSource && opts.contentDedup == domain.ContentDedupCollection {
		checkIdentity := acceptReq.CallerSupplied
		if !checkIdentity && s3Client != nil {
			_, matches, lookupErr := cs.CheckFingerprint(r.Context(), acceptReq.Fingerprint)
			if lookupErr != nil {
				writeJSONError(rw, http.StatusServiceUnavailable, "unavailable", "document store unavailable")
				return
			}
			checkIdentity = matches
		}
		if checkIdentity {
			outcome, _ := acceptIngest(rw, r, wk, cs, acceptReq)
			if outcome != domain.AcceptNeedsSource {
				return
			}
		}
	}
	if (ocrPending || pdfExtraction != nil) && s3Client == nil {
		rw.Header().Set("Retry-After", "30")
		writeJSONError(rw, http.StatusServiceUnavailable, "storage", "source file storage is unavailable")
		return
	}
	var s3Key *string
	var written sourceWrite
	writer := sourceWriter{objects: s3Client, journal: opts.journal}
	if s3Client != nil {
		docType := formDocType
		if docType == "" {
			docType = domain.DefaultDocType
		}
		var uploadErr error
		written, uploadErr = writer.write(r.Context(), docID, req.Collection, docType, fileName, ft, "", fileData)
		if uploadErr != nil {
			slog.ErrorContext(r.Context(), "upload source file", "error", uploadErr)
			rw.Header().Set("Retry-After", "30")
			writeJSONError(rw, http.StatusServiceUnavailable, "storage", "source file storage is unavailable")
			return
		}
		s3Key = &written.key
		acceptReq.Document.S3Key = s3Key
		acceptReq.SourceUpload = written.link
	}
	outcome, acceptErr := acceptIngest(rw, r, wk, cs, acceptReq)
	// Reconciliation owns every unadopted journaled upload. It fences acceptance
	// and checks the full version history, including writes the SDK retried.
	if s3Key != nil && written.link == nil && outcome != domain.AcceptAccepted {
		releaseAttemptObject(r.Context(), cs, s3Client, docID, *s3Key, acceptErr)
	}
}

// releaseAttemptObject removes the object a refused attempt wrote. Each attempt
// owns a distinct key, so this never touches another acceptance's file. When
// acceptance is uncertain, a stored key equal to the attempt's proves it
// committed, and an unreadable state keeps the object for reconciliation.
func releaseAttemptObject(ctx context.Context, cs domain.ChunkStore, s3Client ObjectStore, docID, key string, acceptErr error) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if acceptErr != nil {
		storedKey, lookupErr := cs.GetDocumentS3Key(cleanupCtx, domain.DocumentID(docID))
		if lookupErr != nil || (storedKey != nil && *storedKey == key) {
			if tracing.RedactIdentifiers(cleanupCtx) {
				slog.WarnContext(cleanupCtx, "retain source file after uncertain acceptance", "error", lookupErr)
			} else {
				slog.WarnContext(cleanupCtx, "retain source file after uncertain acceptance", "document_id", docID, "error", lookupErr)
			}
			return
		}
	}
	if err := s3Client.Delete(cleanupCtx, key); err != nil {
		slog.ErrorContext(cleanupCtx, "remove unaccepted source file", "error", err)
	}
}

func inspectPDFUpload(ctx context.Context, extract func(context.Context, []byte) (domain.PDFPages, error), data []byte, layoutMode bool) (*domain.PDFExtraction, string, error) {
	pages, err := extract(ctx, data)
	if err != nil {
		return nil, "", err
	}
	if validationErr := pages.Validate(); validationErr != nil {
		return nil, "", validationErr
	}
	if layoutMode {
		hasContent := false
		for _, page := range pages {
			if page.Evidence != domain.PDFBlank {
				hasContent = true
				break
			}
		}
		if !hasContent {
			return nil, "", extractor.ErrNoText
		}
		return &domain.PDFExtraction{Profile: domain.LayoutPDFProfile, Pages: pages, Coverage: domain.PDFPending}, "", nil
	}
	profile := &domain.PDFExtraction{Profile: domain.MixedPDFProfile, Pages: pages, Coverage: pages.CoverageBeforeOCR()}
	if profile.Coverage == domain.PDFPending {
		return profile, "", nil
	}
	text := pages.Text()
	if len(text) < domain.MinPDFTextBytes {
		return nil, "", extractor.ErrNoText
	}
	return profile, text, nil
}
