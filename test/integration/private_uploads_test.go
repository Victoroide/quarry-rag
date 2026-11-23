package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func privateJSON(t *testing.T, req handler.IngestRequest) *http.Request {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func privatePDF(t *testing.T, data string, fields map[string]string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, err := w.CreateFormFile("file", "sample.pdf")
	require.NoError(t, err)
	_, err = io.WriteString(part, data)
	require.NoError(t, err)
	for k, v := range fields {
		require.NoError(t, w.WriteField(k, v))
	}
	require.NoError(t, w.Close())
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", &body)
	r.Header.Set("Content-Type", w.FormDataContentType())
	return r
}

type privateUploadObjects struct {
	mu      sync.Mutex
	saved   map[string]string
	removed []string
	fail    bool
}

func (s *privateUploadObjects) Upload(_ context.Context, key, _, _ string, _ extractor.FileType, body io.Reader) (string, error) {
	if s.fail {
		return "", errors.New("source storage unavailable")
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved[key] = string(data)
	return key, nil
}

func (s *privateUploadObjects) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.saved, key)
	s.removed = append(s.removed, key)
	return nil
}

func privateResponse(t *testing.T, rec *httptest.ResponseRecorder, code int) handler.IngestResponse {
	t.Helper()
	require.Equal(t, code, rec.Code, rec.Body.String())
	var rsp handler.IngestResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rsp))
	_, err := uuid.Parse(rsp.DocumentID)
	require.NoError(t, err)
	return rsp
}

func parallelIngest(h http.HandlerFunc, requests ...*http.Request) []*httptest.ResponseRecorder {
	var wg sync.WaitGroup
	responses := make([]*httptest.ResponseRecorder, len(requests))
	for i, r := range requests {
		responses[i] = httptest.NewRecorder()
		wg.Go(func() { h(responses[i], r) })
	}
	wg.Wait()
	return responses
}

func TestPrivateUploadsUseDistinctIDs(t *testing.T) {
	c := durableDatabase(t)
	objects := &privateUploadObjects{saved: make(map[string]string)}
	const content = "identical sample content"
	const raw = "%PDF-1.4\nidentical sample bytes"
	extract := func(_ context.Context, ft extractor.FileType, _ []byte) (string, error) {
		if ft == extractor.FileTypePDF {
			return "", extractor.ErrNoText
		}
		return content, nil
	}
	h := handler.IngestHandler(nil, c, objects, 1<<20, handler.WithContentDedup(domain.ContentDedupOff),
		handler.WithOCR(true), handler.WithExtractor(extract))

	ids := make(map[string]bool)
	for _, collection := range []string{"", "archive"} {
		fields := map[string]string{"collection": collection}
		requests := []*http.Request{
			privateJSON(t, handler.IngestRequest{Content: content, Collection: collection}),
			privateJSON(t, handler.IngestRequest{Content: content, Collection: collection}),
			durableFile(t, fields), durableFile(t, fields),
			privatePDF(t, raw, fields), privatePDF(t, raw, fields),
		}
		for i, rec := range parallelIngest(h, requests...) {
			rsp := privateResponse(t, rec, http.StatusAccepted)
			require.Equal(t, "processing", rsp.Status)
			require.False(t, ids[rsp.DocumentID])
			ids[rsp.DocumentID] = true
			key, err := c.GetDocumentS3Key(t.Context(), domain.DocumentID(rsp.DocumentID))
			require.NoError(t, err)
			if i < 2 {
				require.Nil(t, key)
				continue
			}
			require.NotNil(t, key)
			require.True(t, strings.HasPrefix(*key, rsp.DocumentID+"/"))
			require.Contains(t, objects.saved, *key)
			if i >= 4 {
				require.Equal(t, raw, objects.saved[*key])
			}
		}
	}
	require.Len(t, objects.saved, 8)
	require.Empty(t, objects.removed)
	var shared, durable int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM document_fingerprints),(SELECT count(*) FROM ingest_jobs)`).Scan(&shared, &durable))
	require.Zero(t, shared)
	require.Equal(t, len(ids), durable)
}

func TestPrivateIngestThroughGoClient(t *testing.T) {
	c := durableDatabase(t)
	const token = "sample-service-token"
	server := httptest.NewServer(api.NewRouter(&api.RouterDeps{
		ChunkStore: c,
		Config:     &config.Config{ServiceToken: token, IngestContentDedup: domain.ContentDedupOff},
	}))
	t.Cleanup(server.Close)
	apiClient, err := client.New(server.URL, client.WithServiceToken(token))
	require.NoError(t, err)
	req := client.IngestRequest{Content: "matching client content"}
	first, err := apiClient.Ingest(t.Context(), req)
	require.NoError(t, err)
	second, err := apiClient.Ingest(t.Context(), req)
	require.NoError(t, err)
	require.NotEqual(t, first.DocumentID, second.DocumentID)
	req.DocumentID = strings.ToUpper(first.DocumentID)
	retry, err := apiClient.Ingest(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, first, retry)
}

func TestPrivateOCRRetryKeepsAcceptedSource(t *testing.T) {
	c := durableDatabase(t)
	ctx := t.Context()
	const raw = "%PDF-1.4\nsample"
	const text = "recognized document text"
	const collection = "archive"
	id := uuid.NewString()
	fields := map[string]string{"document_id": id, "collection": collection}
	objects := &privateUploadObjects{saved: make(map[string]string)}
	ocr := handler.WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) { return "", extractor.ErrNoText })
	h := handler.IngestHandler(nil, c, objects, 1<<20, handler.WithContentDedup(domain.ContentDedupOff), handler.WithOCR(true), ocr)
	for _, rec := range parallelIngest(h, privatePDF(t, raw, fields), privatePDF(t, raw, fields)) {
		rsp := privateResponse(t, rec, http.StatusAccepted)
		require.Equal(t, id, rsp.DocumentID)
	}
	key, err := c.GetDocumentS3Key(ctx, domain.DocumentID(id))
	require.NoError(t, err)
	require.NotNil(t, key)
	require.Equal(t, map[string]string{*key: raw}, objects.saved)
	require.Len(t, objects.removed, 1)
	require.NotEqual(t, *key, objects.removed[0])

	job, err := c.ClaimJob(ctx, "ocr", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, domain.DocumentID(id), job.Document.ID)
	require.NoError(t, c.StoreOCRText(ctx, *job, text))
	require.NoError(t, c.FailJob(ctx, *job, "later OCR processing failed"))

	rec := httptest.NewRecorder()
	h(rec, privateJSON(t, handler.IngestRequest{Content: "occupies the queue"}))
	queuedID := privateResponse(t, rec, http.StatusAccepted).DocumentID
	limited := handler.IngestHandler(nil, c, objects, 1<<20, handler.WithContentDedup(domain.ContentDedupOff),
		handler.WithOCR(true), ocr, handler.WithMaxPending(1))
	rec = httptest.NewRecorder()
	limited(rec, privatePDF(t, raw, fields))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"error":"queue"`)
	require.NoError(t, c.DeleteDocument(ctx, domain.DocumentID(queuedID)))

	rec = httptest.NewRecorder()
	h(rec, privatePDF(t, raw, fields))
	require.Equal(t, id, privateResponse(t, rec, http.StatusAccepted).DocumentID)
	require.Equal(t, map[string]string{*key: raw}, objects.saved)
	next, err := c.ClaimJob(ctx, "ocr-retry", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Equal(t, text, next.Document.Content)
	require.Equal(t, "ocr", next.Document.TextSource)
	done, err := c.CompleteJob(ctx, *next, nil)
	require.NoError(t, err)
	require.True(t, done)

	objects.fail = true
	shared := handler.IngestHandler(nil, c, objects, 1<<20, handler.WithOCR(true), ocr)
	t.Run("exact replay during storage outage", func(t *testing.T) {
		rec := httptest.NewRecorder()
		shared(rec, privatePDF(t, raw, fields))
		require.Equal(t, handler.IngestResponse{DocumentID: id, Status: "already_ingested"}, privateResponse(t, rec, http.StatusOK))
	})
	t.Run("exact replay without an object client", func(t *testing.T) {
		withoutStorage := handler.IngestHandler(nil, c, nil, 1<<20, handler.WithOCR(true), ocr)
		rec := httptest.NewRecorder()
		withoutStorage(rec, privatePDF(t, raw, fields))
		require.Equal(t, handler.IngestResponse{DocumentID: id, Status: "already_ingested"}, privateResponse(t, rec, http.StatusOK))
	})
	conflicts := []struct {
		name        string
		makeRequest func(*testing.T) *http.Request
	}{
		{"changed bytes during storage outage", func(t *testing.T) *http.Request { return privatePDF(t, raw+" changed", fields) }},
		{"changed collection during storage outage", func(t *testing.T) *http.Request {
			return privatePDF(t, raw, map[string]string{"document_id": id})
		}},
		{"recognized text is not the OCR input", func(t *testing.T) *http.Request {
			return privateJSON(t, handler.IngestRequest{DocumentID: id, Content: text, Collection: collection})
		}},
	}
	for _, conflict := range conflicts {
		t.Run(conflict.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			shared(rec, conflict.makeRequest(t))
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
		})
	}
	t.Run("new ID needs storage", func(t *testing.T) {
		newID := uuid.NewString()
		rec := httptest.NewRecorder()
		shared(rec, privatePDF(t, raw, map[string]string{"document_id": newID, "collection": collection}))
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Equal(t, "30", rec.Header().Get("Retry-After"))
		require.JSONEq(t, `{"error":"storage","message":"source file storage is unavailable"}`, rec.Body.String())
		exists, lookupErr := c.DocumentExists(t.Context(), domain.DocumentID(newID))
		require.NoError(t, lookupErr)
		require.False(t, exists)
	})
	storedKey, err := c.GetDocumentS3Key(ctx, domain.DocumentID(id))
	require.NoError(t, err)
	require.Equal(t, key, storedKey)
	require.Equal(t, map[string]string{*key: raw}, objects.saved)
	for _, content := range []string{raw, text} {
		_, found, err := c.CheckFingerprint(ctx, chunker.ScopedFingerprint(collection, content))
		require.NoError(t, err)
		require.False(t, found)
	}
}

type deleteAfterFingerprintLookup struct {
	*pgstore.Client
}

func (s deleteAfterFingerprintLookup) CheckFingerprint(ctx context.Context, fingerprint string) (domain.DocumentID, bool, error) {
	id, found, err := s.Client.CheckFingerprint(ctx, fingerprint)
	if found && err == nil {
		err = s.DeleteDocument(ctx, id)
	}
	return id, found, err
}

func TestCollectionUploadRetainsSourceAfterStalePreflight(t *testing.T) {
	c := durableDatabase(t)
	const content = "shared sample content"
	original := privateResponse(t, durableSubmit(t, nil, c, `{"content":"shared sample content"}`), http.StatusAccepted)
	extract := handler.WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) { return content, nil })
	objects := &privateUploadObjects{fail: true}
	h := handler.IngestHandler(nil, c, objects, 1<<20, extract)
	rec := httptest.NewRecorder()
	h(rec, durableFile(t))
	require.Equal(t, original, privateResponse(t, rec, http.StatusAccepted))

	objects = &privateUploadObjects{saved: make(map[string]string)}
	h = handler.IngestHandler(nil, deleteAfterFingerprintLookup{c}, objects, 1<<20, extract)
	rec = httptest.NewRecorder()
	h(rec, durableFile(t))
	replacement := privateResponse(t, rec, http.StatusAccepted)
	require.NotEqual(t, original.DocumentID, replacement.DocumentID)
	key, err := c.GetDocumentS3Key(t.Context(), domain.DocumentID(replacement.DocumentID))
	require.NoError(t, err)
	require.NotNil(t, key)
	require.Contains(t, objects.saved, *key)
}

func TestPrivateUploadFailureAndLimits(t *testing.T) {
	c := durableDatabase(t)
	objects := &privateUploadObjects{fail: true}
	extract := handler.WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) {
		return "sample content", nil
	})
	policy := handler.WithContentDedup(domain.ContentDedupOff)
	h := handler.IngestHandler(nil, c, objects, 1<<20, policy, extract)
	rec := httptest.NewRecorder()
	h(rec, durableFile(t))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "30", rec.Header().Get("Retry-After"))
	require.JSONEq(t, `{"error":"storage","message":"source file storage is unavailable"}`, rec.Body.String())
	var docs int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM documents`).Scan(&docs))
	require.Zero(t, docs)

	objects = &privateUploadObjects{saved: make(map[string]string)}
	h = handler.IngestHandler(nil, c, objects, 32, policy, extract)
	rec = httptest.NewRecorder()
	h(rec, durableFile(t))
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	rec = httptest.NewRecorder()
	h(rec, privateJSON(t, handler.IngestRequest{Content: strings.Repeat("a", domain.MaxContentBytes+1)}))
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Empty(t, objects.saved)

	h = handler.IngestHandler(nil, c, objects, 1<<20, policy, extract, handler.WithMaxPending(1))
	rec = httptest.NewRecorder()
	h(rec, privateJSON(t, handler.IngestRequest{Content: "sample content"}))
	privateResponse(t, rec, http.StatusAccepted)
	rec = httptest.NewRecorder()
	h(rec, durableFile(t))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Equal(t, "30", rec.Header().Get("Retry-After"))
	require.Contains(t, rec.Body.String(), `"error":"queue"`)
	require.Empty(t, objects.saved)
	require.Len(t, objects.removed, 1)
}
