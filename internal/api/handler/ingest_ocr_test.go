package handler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/stretchr/testify/require"
)

type ocrAcceptStore struct {
	validationAcceptStore
	lookup  string
	outcome domain.AcceptOutcome
}

func (s *ocrAcceptStore) CheckFingerprint(_ context.Context, fp string) (domain.DocumentID, bool, error) {
	s.lookup = fp
	return "", false, nil
}
func (s *ocrAcceptStore) AcceptDocument(ctx context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	_, err := s.validationAcceptStore.AcceptDocument(ctx, req)
	outcome := s.outcome
	if outcome == domain.AcceptNeedsSource && req.Document.S3Key != nil {
		outcome = domain.AcceptAccepted
	}
	return domain.AcceptResult{DocumentID: req.Document.ID, Outcome: outcome}, err
}

type ocrObjects struct {
	data      []byte
	deleted   bool
	uploadErr error
}

func (s *ocrObjects) Upload(_ context.Context, _, _, _ string, _ extractor.FileType, r io.Reader) (string, error) {
	var err error
	s.data, err = io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return "synthetic-object", s.uploadErr
}
func (s *ocrObjects) Delete(context.Context, string) error { s.deleted = true; return nil }
func ocrUpload(t *testing.T, filename, collection string, data []byte) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	f, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, mw.WriteField("collection", collection))
	require.NoError(t, mw.Close())
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", &body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	return r
}
func TestMixedPDFUploadClassifiesFreeTextAnnotations(t *testing.T) {
	const heading = "Operations register"
	const instruction = "Delivery requires refrigeration"
	const annotation = "<< /Type /Annot /Subtype /FreeText /Rect [12 40 330 125] /F %d /Contents (%s) /DA (/F1 15 Tf 0 g) /Border [0 0 0] >>"
	for _, tc := range []struct {
		name, annotation string
		evidence         domain.PDFPageEvidence
		coverage         domain.PDFCoverage
	}{
		{name: "printable", annotation: fmt.Sprintf(annotation, 4, instruction), evidence: domain.PDFUnreadable, coverage: domain.PDFPending},
		{name: "blank", evidence: domain.PDFBlank, coverage: domain.PDFComplete},
		{name: "hidden", annotation: fmt.Sprintf(annotation, 6, instruction), evidence: domain.PDFBlank, coverage: domain.PDFComplete},
		{name: "empty", annotation: fmt.Sprintf(annotation, 4, ""), evidence: domain.PDFBlank, coverage: domain.PDFComplete},
		{name: "whitespace", annotation: fmt.Sprintf(annotation, 4, "  "), evidence: domain.PDFBlank, coverage: domain.PDFComplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := testutil.PDFWithPageAnnotation(tc.annotation, "BT /F1 14 Tf 20 150 Td ("+heading+") Tj ET", "")
			s := &ocrAcceptStore{outcome: domain.AcceptNeedsSource}
			objects := &ocrObjects{}
			h := IngestHandler(nil, s, objects, 1<<20, WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
				return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
			}))
			rw := httptest.NewRecorder()
			h(rw, ocrUpload(t, "register.pdf", "archive", data))
			require.Equal(t, http.StatusAccepted, rw.Code, rw.Body.String())
			require.Equal(t, data, objects.data)
			require.Len(t, s.accepted, 2)
			req := s.accepted[1]
			require.NotNil(t, req.PDF)
			require.Equal(t, tc.coverage, req.PDF.Coverage)
			require.Len(t, req.PDF.Pages, 2)
			require.Equal(t, heading, req.PDF.Pages.Text())
			require.Equal(t, domain.PDFPage{Number: 2, Evidence: tc.evidence}, req.PDF.Pages[1])
			if tc.coverage == domain.PDFPending {
				require.Empty(t, req.Document.Content)
			} else {
				require.Equal(t, heading, req.Document.Content)
			}
		})
	}
}

func TestOCRUploadAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name             string
		enabled, storage bool
		extractErr       error
		status           int
	}{
		{"enabled", true, true, extractor.ErrNoText, 202},
		{"child error", true, true, errors.New("pdf: " + extractor.ErrNoText.Error()), 202},
		{"disabled", false, true, extractor.ErrNoText, 422},
		{"missing storage", true, false, extractor.ErrNoText, 503},
		{"invalid pdf", true, true, extractor.ErrExtractionFailed, 422},
		{"timeout", true, true, extractor.ErrExtractionTimeout, 422},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &ocrAcceptStore{}
			objects := &ocrObjects{}
			var objectStore ObjectStore
			if tc.storage {
				objectStore = objects
			}
			data := []byte("%PDF-1.4\nsynthetic image-only PDF")
			rw := httptest.NewRecorder()
			IngestHandler(nil, s, objectStore, 1<<20, WithOCR(tc.enabled), WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) { return "", tc.extractErr }))(rw, ocrUpload(t, "scanned.pdf", "archive", data))
			require.Equal(t, tc.status, rw.Code, rw.Body.String())
			if tc.status != 202 {
				require.Empty(t, s.accepted)
				require.Empty(t, objects.data)
				require.Empty(t, s.lookup)
				return
			}
			require.Len(t, s.accepted, 1)
			req := s.accepted[0]
			require.Empty(t, req.Document.Content)
			require.Equal(t, "ocr_pending", req.Document.TextSource)
			require.Equal(t, "archive", req.Collection)
			require.Equal(t, chunker.ScopedFingerprint("archive", string(data)), req.Fingerprint)
			require.Equal(t, req.Fingerprint, s.lookup)
			require.Equal(t, data, objects.data)
			require.Equal(t, "synthetic-object", *req.Document.S3Key)
		})
	}
}
func TestOCRUploadValidatesBeforeStorageAndRollsBack(t *testing.T) {
	for _, tc := range []struct {
		name, collection  string
		outcome           domain.AcceptOutcome
		uploadErr         error
		status            int
		uploaded, deleted bool
	}{
		{name: "invalid collection", collection: "bad\x00collection", status: 400},
		{name: "storage failure", collection: "archive", uploadErr: errors.New("unavailable"), status: 503, uploaded: true},
		{name: "queue full", collection: "archive", outcome: domain.AcceptQueueFull, status: 503, uploaded: true, deleted: true},
		{name: "duplicate race", collection: "archive", outcome: domain.AcceptInProgress, status: 202, uploaded: true, deleted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &ocrAcceptStore{outcome: tc.outcome}
			objects := &ocrObjects{uploadErr: tc.uploadErr}
			rw := httptest.NewRecorder()
			IngestHandler(nil, s, objects, 1<<20, WithOCR(true), WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) { return "", extractor.ErrNoText }))(rw, ocrUpload(t, "scan.pdf", tc.collection, []byte("%PDF-1.4\nscan")))
			require.Equal(t, tc.status, rw.Code, rw.Body.String())
			require.Equal(t, tc.uploaded, len(objects.data) > 0)
			require.Equal(t, tc.deleted, objects.deleted)
			if tc.outcome == domain.AcceptQueueFull {
				require.Equal(t, "30", rw.Header().Get("Retry-After"))
				require.JSONEq(t, `{"error":"queue","message":"ingest queue is full"}`, rw.Body.String())
			}
		})
	}
}

func TestPrivateOCRUploadSkipsSharedLookup(t *testing.T) {
	store := &ocrAcceptStore{}
	objects := &ocrObjects{}
	data := []byte("%PDF-1.4\nprivate image-only PDF")
	rw := httptest.NewRecorder()
	h := IngestHandler(nil, store, objects, 1<<20, WithContentDedup(domain.ContentDedupOff), WithOCR(true),
		WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) { return "", extractor.ErrNoText }))
	h(rw, ocrUpload(t, "scan.pdf", "archive", data))
	require.Equal(t, http.StatusAccepted, rw.Code, rw.Body.String())
	require.Empty(t, store.lookup)
	require.Len(t, store.accepted, 1)
	require.Equal(t, domain.ContentDedupOff, store.accepted[0].ContentDedup)
	require.Equal(t, chunker.ScopedFingerprint("archive", string(data)), store.accepted[0].Fingerprint)
	require.Equal(t, data, objects.data)
	require.False(t, objects.deleted)
}

func TestMixedPDFUploadChecksManifestBeforeObjectStorage(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pages    domain.PDFPages
		err      error
		status   int
		coverage domain.PDFCoverage
	}{
		{"mixed", domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}, {Number: 2, Evidence: domain.PDFEmbedded, Text: "Archive operating hours"}}, nil, http.StatusAccepted, domain.PDFPending},
		{"digital", domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Archive operating hours"}, {Number: 2, Evidence: domain.PDFBlank}}, nil, http.StatusAccepted, domain.PDFComplete},
		{"blank", domain.PDFPages{{Number: 1, Evidence: domain.PDFBlank}}, nil, http.StatusUnprocessableEntity, ""},
		{"invalid", nil, extractor.ErrExtractionFailed, http.StatusUnprocessableEntity, ""},
		{"missing", domain.PDFPages{{Number: 2, Evidence: domain.PDFImage}}, nil, http.StatusUnprocessableEntity, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &ocrAcceptStore{outcome: domain.AcceptNeedsSource}
			objects := &ocrObjects{}
			data := []byte("%PDF-1.4\nsynthetic original")
			h := IngestHandler(nil, s, objects, 1<<20, WithMixedPDFOCR(func(context.Context, []byte) (domain.PDFPages, error) { return tc.pages, tc.err }))
			rw := httptest.NewRecorder()
			h(rw, ocrUpload(t, "scan.pdf", "archive", data))
			require.Equal(t, tc.status, rw.Code, rw.Body.String())
			if tc.coverage == "" {
				require.Empty(t, objects.data)
				require.Empty(t, s.accepted)
				return
			}
			require.Equal(t, data, objects.data)
			require.Len(t, s.accepted, 2)
			require.Equal(t, tc.coverage, s.accepted[1].PDF.Coverage)
			require.Equal(t, s.accepted[0].Fingerprint, s.accepted[1].Fingerprint)
			require.NotEqual(t, chunker.ScopedFingerprint("archive", string(data)), s.accepted[1].Fingerprint)
			if tc.coverage == domain.PDFPending {
				require.Empty(t, s.accepted[1].Document.Content)
			}
		})
	}
}

// legacyLookupStore reports that a clean anonymous PDF needs its historical
// parser identity before the store can decide whether it is a duplicate.
type legacyLookupStore struct{ ocrAcceptStore }

func (s *legacyLookupStore) AcceptDocument(ctx context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	if req.TryLegacyPDFText {
		s.accepted = append(s.accepted, req)
		return domain.AcceptResult{DocumentID: req.Document.ID, Outcome: domain.AcceptNeedsPDFText}, nil
	}
	return s.ocrAcceptStore.AcceptDocument(ctx, req)
}

func TestMixedPDFLegacyIdentityLookupFailureStopsBeforeAdmission(t *testing.T) {
	clean := domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Archive operating hours"}}
	data := []byte("%PDF-1.4\nsynthetic original")
	for _, tc := range []struct {
		name        string
		text        string
		err         error
		status      int
		stored      bool
		fingerprint string
	}{
		{"extraction unavailable", "", extractor.ErrExtractionUnavailable, http.StatusServiceUnavailable, false, ""},
		{"wrapped extraction unavailable", "", fmt.Errorf("%w: child failed", extractor.ErrExtractionUnavailable), http.StatusServiceUnavailable, false, ""},
		{"extraction timeout", "", extractor.ErrExtractionTimeout, http.StatusUnprocessableEntity, false, ""},
		{"canceled", "", context.Canceled, http.StatusUnprocessableEntity, false, ""},
		{"deadline exceeded", "", context.DeadlineExceeded, http.StatusUnprocessableEntity, false, ""},
		{"parser failure", "", extractor.ErrExtractionFailed, http.StatusUnprocessableEntity, false, ""},
		{"conclusive no text", "", extractor.ErrNoText, http.StatusAccepted, true, ""},
		{"wrapped no text", "", fmt.Errorf("extractor: pdf: %w", extractor.ErrNoText), http.StatusAccepted, true, ""},
		{"readable legacy text", "Archive operating hours", nil, http.StatusAccepted, true, chunker.ScopedFingerprint("archive", "Archive operating hours")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &legacyLookupStore{ocrAcceptStore{outcome: domain.AcceptNeedsSource}}
			objects := &ocrObjects{}
			h := IngestHandler(nil, s, objects, 1<<20,
				WithMixedPDFOCR(func(context.Context, []byte) (domain.PDFPages, error) { return clean, nil }),
				WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) { return tc.text, tc.err }))
			rw := httptest.NewRecorder()
			h(rw, ocrUpload(t, "archive.pdf", "archive", data))
			require.Equal(t, tc.status, rw.Code, rw.Body.String())
			if !tc.stored {
				require.Empty(t, objects.data, "a source object was stored before the identity was known")
				require.Len(t, s.accepted, 1, "the document was admitted after an inconclusive identity lookup")
				return
			}
			require.Equal(t, data, objects.data)
			require.Len(t, s.accepted, 3)
			require.Equal(t, tc.fingerprint, s.accepted[1].LegacyParserFingerprint)
			require.Equal(t, tc.fingerprint, s.accepted[2].LegacyParserFingerprint)
		})
	}
}
