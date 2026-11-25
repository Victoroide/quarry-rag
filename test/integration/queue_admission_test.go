package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type queueAdmissionObjects struct {
	data             map[string]string
	uploads, deletes []string
}

func (s *queueAdmissionObjects) Upload(_ context.Context, key, _, _ string, _ extractor.FileType, r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	s.data[key] = string(data)
	s.uploads = append(s.uploads, key)
	return key, nil
}

func (s *queueAdmissionObjects) Delete(_ context.Context, key string) error {
	delete(s.data, key)
	s.deletes = append(s.deletes, key)
	return nil
}

func queueAdmissionUpload(t *testing.T, id, source string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	if id != "" {
		require.NoError(t, form.WriteField("document_id", id))
	}
	file, err := form.CreateFormFile("file", "original.txt")
	require.NoError(t, err)
	_, err = io.WriteString(file, source)
	require.NoError(t, err)
	require.NoError(t, form.Close())
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", &body)
	req.Header.Set("Content-Type", form.FormDataContentType())
	return req
}

func TestFailedRetriesAtDurableQueueLimit(t *testing.T) {
	c := durableDatabase(t)
	objects := &queueAdmissionObjects{data: make(map[string]string)}
	seed := handler.IngestHandler(nil, c, objects, 1<<20, handler.WithMaxPending(2))
	ingest := handler.IngestHandler(nil, c, objects, 1<<20, handler.WithMaxPending(1))
	server := httptest.NewServer(ingest)
	t.Cleanup(server.Close)
	api, err := client.New(server.URL)
	require.NoError(t, err)
	const content = "Archived synthetic report."
	const original = "\n" + content + "\n"
	const failure = "terminal processing error"
	occupantID, failedID := uuid.NewString(), uuid.NewString()

	rec := httptest.NewRecorder()
	seed(rec, queueAdmissionUpload(t, occupantID, content))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	occupant, err := c.ClaimJob(t.Context(), "occupant", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, occupant)
	require.Equal(t, domain.DocumentID(occupantID), occupant.Document.ID)

	// An explicit ID may retain a second original for the same extracted text.
	rec = httptest.NewRecorder()
	seed(rec, queueAdmissionUpload(t, failedID, original))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	failed, err := c.ClaimJob(t.Context(), "failed", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, failed)
	require.Equal(t, domain.DocumentID(failedID), failed.Document.ID)
	require.NoError(t, c.FailJob(t.Context(), *failed, failure))
	require.Len(t, objects.uploads, 2)
	occupantKey, failedKey := objects.uploads[0], objects.uploads[1]

	retry := client.IngestRequest{DocumentID: failedID, Content: content}
	_, err = api.Ingest(t.Context(), retry)
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusServiceUnavailable, apiErr.StatusCode)
	require.Equal(t, "queue", apiErr.Code)
	require.Equal(t, "ingest queue is full", apiErr.Message)
	require.Equal(t, 30*time.Second, apiErr.RetryAfter)

	rec = httptest.NewRecorder()
	ingest(rec, queueAdmissionUpload(t, failedID, original))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Equal(t, "30", rec.Header().Get("Retry-After"))
	require.JSONEq(t, `{"error":"queue","message":"ingest queue is full"}`, rec.Body.String())
	require.Equal(t, []string{occupantKey, failedKey}, objects.uploads)
	require.Empty(t, objects.deletes)
	require.Equal(t, map[string]string{occupantKey: content, failedKey: original}, objects.data)
	for id, key := range map[string]string{occupantID: occupantKey, failedID: failedKey} {
		stored, lookupErr := c.GetDocumentS3Key(t.Context(), domain.DocumentID(id))
		require.NoError(t, lookupErr)
		require.NotNil(t, stored)
		require.Equal(t, key, *stored)
	}
	var state, lastError string
	var outstanding int
	err = c.Pool.QueryRow(t.Context(), `SELECT status,last_error,(SELECT count(*) FROM ingest_jobs) FROM documents WHERE id=$1`, failedID).Scan(&state, &lastError, &outstanding)
	require.NoError(t, err)
	require.Equal(t, "failed", state)
	require.Equal(t, failure, lastError)
	require.Equal(t, 1, outstanding)

	rec = httptest.NewRecorder()
	poll := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", strings.NewReader(`{"content":"`+content+`"}`))
	poll.Header.Set("Content-Type", "application/json")
	ingest(rec, poll)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	var observed handler.IngestResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &observed))
	require.Equal(t, handler.IngestResponse{DocumentID: occupantID, Status: "processing"}, observed)

	completed, err := c.CompleteJob(t.Context(), *occupant, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, completed)
	requeued, err := api.Ingest(t.Context(), retry)
	require.NoError(t, err)
	require.Equal(t, &client.IngestResponse{DocumentID: failedID, Status: "processing"}, requeued)
}
