package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
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
	return domain.AcceptResult{DocumentID: req.Document.ID, Outcome: s.outcome}, err
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
		})
	}
}
