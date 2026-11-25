package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/google/uuid"
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

type gatedLegacyStore struct {
	*guardedStore
	gate   error
	events []string
}

func (s *gatedLegacyStore) AcquireWriter(context.Context) (func(), error) {
	s.events = append(s.events, "acquire")
	if s.gate != nil {
		return nil, s.gate
	}
	return func() { s.events = append(s.events, "release") }, nil
}

func (s *gatedLegacyStore) WithLegacyDeletion(ctx context.Context, id domain.DocumentID, fn func(domain.LegacyDocumentStore) error) error {
	s.events = append(s.events, "legacy")
	return s.guardedStore.WithLegacyDeletion(ctx, id, fn)
}

func TestLegacyReservationDeletionHonorsTheWriterGate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		gate        error
		status      int
		wantEvents  []string
		wantGuarded int
	}{
		{name: "open", status: http.StatusOK, wantEvents: []string{"acquire", "legacy", "release"}, wantGuarded: 1},
		{name: "frozen", gate: domain.ErrWritersFrozen, status: http.StatusServiceUnavailable, wantEvents: []string{"acquire"}},
		{name: "other generation", gate: domain.ErrGenerationMismatch, status: http.StatusServiceUnavailable, wantEvents: []string{"acquire"}},
		{name: "gate failure", gate: errors.New("database unavailable"), status: http.StatusServiceUnavailable, wantEvents: []string{"acquire"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &gatedLegacyStore{guardedStore: &guardedStore{checkpointedStore: &checkpointedStore{}}, gate: tc.gate}
			rec := serveDeletion(t, store, http.Header{}, WithLegacyReservations())
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Equal(t, tc.wantEvents, store.events)
			require.Equal(t, tc.wantGuarded, store.guarded)
			require.Zero(t, store.legacy)
			require.Empty(t, store.deletes)
		})
	}
}

type gatedScopedStore struct {
	*scopedMockStore
	gate   error
	events []string
}

func (s *gatedScopedStore) AcquireWriter(context.Context) (func(), error) {
	s.events = append(s.events, "acquire")
	if s.gate != nil {
		return nil, s.gate
	}
	return func() { s.events = append(s.events, "release") }, nil
}

func (s *gatedScopedStore) BeginScopedDeletion(ctx context.Context, principal domain.Principal, validUntil time.Time, id domain.DocumentID, ops domain.DeleteOperations) (incarnation string, complete bool, err error) {
	s.events = append(s.events, "begin")
	return s.scopedMockStore.BeginScopedDeletion(ctx, principal, validUntil, id, ops)
}

func (s *gatedScopedStore) ResumeScopedDeletion(ctx context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	s.events = append(s.events, "resume")
	return s.scopedMockStore.ResumeScopedDeletion(ctx, id, incarnation, ops)
}

func TestScopedDeletionHonorsTheWriterGate(t *testing.T) {
	tenant := uuid.NewString()
	owner := domain.Principal{ID: uuid.NewString(), TenantID: tenant}
	for _, tc := range []struct {
		name       string
		gate       error
		status     int
		body       string
		wantEvents []string
	}{
		{name: "open", status: http.StatusOK, wantEvents: []string{"acquire", "begin", "resume", "release"}},
		{name: "frozen", gate: domain.ErrWritersFrozen, status: http.StatusServiceUnavailable, body: frozenBody, wantEvents: []string{"acquire"}},
		{name: "other generation", gate: domain.ErrGenerationMismatch, status: http.StatusServiceUnavailable, body: `{"error":"unavailable","message":"this replica does not serve the active vector generation"}`, wantEvents: []string{"acquire"}},
		{name: "gate failure", gate: errors.New("database unavailable"), status: http.StatusServiceUnavailable, body: `{"error":"unavailable","message":"document store unavailable"}`, wantEvents: []string{"acquire"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &gatedScopedStore{scopedMockStore: newScopedMockStore(owner), gate: tc.gate}
			docID := uuid.NewString()
			store.docs[docID] = &scopedMemoryEntry{
				owner:   owner,
				summary: domain.DocumentSummary{ID: docID, Incarnation: uuid.NewString(), Status: "completed"},
			}
			h := DeleteHandler(store, nil, nil, WithScopedDeletion())
			rec := serveScoped(h, http.MethodDelete, "/documents/"+docID, &owner, time.Now().Add(time.Minute))
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Equal(t, tc.wantEvents, store.events)
			if tc.body != "" {
				require.JSONEq(t, tc.body, rec.Body.String())
			}
			if tc.status == http.StatusServiceUnavailable {
				require.Equal(t, "30", rec.Header().Get("Retry-After"))
			}
		})
	}
}
