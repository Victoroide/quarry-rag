package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/stretchr/testify/require"
)

const frozenBody = `{"error":"frozen","message":"document writes are paused for a vector migration"}`

func TestFrozenIngestAnswersServiceUnavailable(t *testing.T) {
	store := &ocrAcceptStore{outcome: domain.AcceptFrozen}
	rw := httptest.NewRecorder()
	IngestHandler(nil, store, nil, 1<<20)(rw, identityRequest(t, "Frozen document text.", "", false))
	require.Equal(t, http.StatusServiceUnavailable, rw.Code, rw.Body.String())
	require.Equal(t, "30", rw.Header().Get("Retry-After"))
	require.JSONEq(t, frozenBody, rw.Body.String())
}

func TestFrozenUploadRemovesTheSourceObject(t *testing.T) {
	store := &ocrAcceptStore{outcome: domain.AcceptFrozen}
	objects := &ocrObjects{}
	rw := httptest.NewRecorder()
	IngestHandler(nil, store, objects, 1<<20, WithOCR(true), WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) { return "", extractor.ErrNoText }))(rw, ocrUpload(t, "scan.pdf", "archive", []byte("%PDF-1.4\nscan")))
	require.Equal(t, http.StatusServiceUnavailable, rw.Code, rw.Body.String())
	require.JSONEq(t, frozenBody, rw.Body.String())
	require.NotEmpty(t, objects.data, "the object was uploaded before acceptance")
	require.True(t, objects.deleted, "a rejected acceptance must not keep the object")
}

func TestFrozenOwnedIngestAnswersServiceUnavailableAndReleasesObject(t *testing.T) {
	h := newOwnedHarness(t)
	h.rebuild(1 << 20)
	h.store.result = domain.AcceptFrozen
	rec := h.serve(ownedUpload(t, false, []byte(ownedBody)))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Equal(t, "30", rec.Header().Get("Retry-After"))
	require.JSONEq(t, frozenBody, rec.Body.String())
	require.Len(t, h.objects.uploads, 1)
	require.Equal(t, []string{h.objects.uploads[0]}, h.objects.deleted)
}

type gatedDeleteStore struct {
	fileDeleteStore
	gate error
}

func (s *gatedDeleteStore) WritersOpen(context.Context) error { return s.gate }

func TestDeleteHonorsTheWriterGate(t *testing.T) {
	const docID = "8a3e792c-5df6-4b6e-a612-4f7db4959db2"
	for _, tc := range []struct {
		name    string
		gate    error
		status  int
		body    string
		deleted bool
	}{
		{name: "open", status: http.StatusOK, deleted: true},
		{name: "frozen", gate: domain.ErrWritersFrozen, status: http.StatusServiceUnavailable, body: frozenBody},
		{name: "wrapped freeze", gate: fmt.Errorf("gate: %w", domain.ErrWritersFrozen), status: http.StatusServiceUnavailable, body: frozenBody},
		{name: "other generation", gate: domain.ErrGenerationMismatch, status: http.StatusServiceUnavailable, body: `{"error":"unavailable","message":"this replica does not serve the active vector generation"}`},
		{name: "gate failure", gate: errors.New("database unavailable"), status: http.StatusServiceUnavailable, body: `{"error":"unavailable","message":"document store unavailable"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &gatedDeleteStore{fileDeleteStore: fileDeleteStore{document: &domain.Document{ID: docID}}, gate: tc.gate}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+docID, http.NoBody)
			req.SetPathValue("doc_id", docID)
			rw := httptest.NewRecorder()
			DeleteHandler(store, nil, nil)(rw, req)
			require.Equal(t, tc.status, rw.Code, rw.Body.String())
			require.Equal(t, tc.deleted, store.deleted)
			if tc.body != "" {
				require.JSONEq(t, tc.body, rw.Body.String())
			}
			if tc.status == http.StatusServiceUnavailable {
				require.Equal(t, "30", rw.Header().Get("Retry-After"))
			}
		})
	}
}

type gatedCheckpointedStore struct {
	checkpointedStore
	gate error
}

func (s *gatedCheckpointedStore) WritersOpen(context.Context) error { return s.gate }

func TestCheckpointedDeletionHonorsTheWriterGate(t *testing.T) {
	header := http.Header{"X-Document-Incarnation": {"3f6c9a4e-2d1b-4c7a-9e55-0b8d1f2a7c10"}}
	for _, tc := range []struct {
		name    string
		gate    error
		status  int
		deleted int
	}{
		{name: "open", status: http.StatusOK, deleted: 1},
		{name: "frozen", gate: domain.ErrWritersFrozen, status: http.StatusServiceUnavailable},
		{name: "other generation", gate: domain.ErrGenerationMismatch, status: http.StatusServiceUnavailable},
		{name: "gate failure", gate: errors.New("database unavailable"), status: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &gatedCheckpointedStore{checkpointedStore: checkpointedStore{active: true}, gate: tc.gate}
			rec := serveDeletion(t, store, header.Clone())
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Len(t, store.deletes, tc.deleted, "the coordinator must not start while writers are paused")
			require.Zero(t, store.modeReads-tc.deleted, "the deletion mode is read only after the gate opens")
		})
	}
}
