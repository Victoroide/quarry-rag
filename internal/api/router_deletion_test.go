package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// touchedStore counts every call that would reach storage.
type touchedStore struct {
	domain.ChunkStore
	touched atomic.Int32
	deleted atomic.Int32
}

func (s *touchedStore) DeletionActive(context.Context) (bool, error) {
	s.touched.Add(1)
	return true, nil
}

func (s *touchedStore) DeleteWithCleanup(context.Context, domain.DocumentID, domain.DeleteOperations) error {
	s.touched.Add(1)
	s.deleted.Add(1)
	return nil
}

func (s *touchedStore) WithDocumentFile(context.Context, domain.DocumentID, func(context.Context, string) error) (bool, error) {
	s.touched.Add(1)
	return false, nil
}

func (s *touchedStore) GetDocument(context.Context, domain.DocumentID) (*domain.Document, error) {
	s.touched.Add(1)
	return nil, nil
}

type vectorsSpy struct{ calls atomic.Int32 }

func (v *vectorsSpy) DeleteAllVectorsInNamespace(context.Context, string) error {
	v.calls.Add(1)
	return nil
}

func (v *vectorsSpy) DeleteByDocID(context.Context, string) error {
	v.calls.Add(1)
	return nil
}

func TestUnauthorizedDeletionTouchesNoStore(t *testing.T) {
	store, vectors := &touchedStore{}, &vectorsSpy{}
	router := NewRouter(&RouterDeps{ChunkStore: store, PineconeDriver: vectors, Config: &config.Config{ServiceToken: "permitted"}})
	id := uuid.NewString()
	for name, token := range map[string]string{"missing": "", "wrong": "denied"} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+id, http.NoBody)
			req.Header.Set("X-Document-Incarnation", uuid.NewString())
			if token != "" {
				req.Header.Set("X-Service-Token", token)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			file := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents/"+id+"/file", http.NoBody)
			fileRec := httptest.NewRecorder()
			router.ServeHTTP(fileRec, file)
			require.Equal(t, http.StatusUnauthorized, fileRec.Code)
		})
	}
	require.Zero(t, store.touched.Load(), "an unauthorized caller reached the database")
	require.Zero(t, vectors.calls.Load(), "an unauthorized caller reached the vector store")

	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+id, http.NoBody)
	req.Header.Set("X-Document-Incarnation", uuid.NewString())
	req.Header.Set("X-Service-Token", "permitted")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.EqualValues(t, 1, store.deleted.Load())
}

func TestDeletionTargetsFollowTheConfiguredStores(t *testing.T) {
	for _, tc := range []struct {
		name         string
		cfg          config.Config
		bucket, idx  string
		wantInMemory bool
	}{
		{name: "hosted", cfg: config.Config{S3Bucket: "originals", PineconeIndexName: "index"}, bucket: "originals", idx: "index"},
		{name: "pgvector", cfg: config.Config{VectorStore: "postgres", S3Bucket: "originals", PineconeIndexName: "ignored"}, bucket: "originals"},
		{name: "offline", cfg: config.Config{FakeMode: true, PineconeIndexName: "ignored"}, wantInMemory: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			targets := deletionTargets(&tc.cfg, nil)
			require.Equal(t, tc.bucket, targets.SourceBucket)
			require.Equal(t, tc.idx, targets.VectorIndex)
			require.Equal(t, tc.wantInMemory, targets.InMemory)
		})
	}
}

func TestDeletionTargetsExposeTheOwningInMemoryIndex(t *testing.T) {
	index := memory.NewFakeVectorStore()
	offline := deletionTargets(&config.Config{FakeMode: true}, index)
	require.Same(t, index, offline.Memory)
	require.Equal(t, index.MemoryOwner(), offline.Memory.MemoryOwner())
	// A hosted deployment never treats a process-local index as the owner.
	require.Nil(t, deletionTargets(&config.Config{PineconeIndexName: "index"}, index).Memory)
	require.Nil(t, deletionTargets(&config.Config{FakeMode: true}, nil).Memory)
}

func TestDeletionModeRouteIsAServiceRoute(t *testing.T) {
	store := &touchedStore{}
	router := NewRouter(&RouterDeps{ChunkStore: store, Config: &config.Config{ServiceToken: "permitted"}})
	for _, token := range []string{"", "other"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/deletion-mode", http.NoBody)
		if token != "" {
			req.Header.Set("X-Service-Token", token)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code, "token %q", token)
	}
	require.Zero(t, store.touched.Load(), "an unauthenticated mode check reads nothing")

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/deletion-mode", http.NoBody)
	req.Header.Set("X-Service-Token", "permitted")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.JSONEq(t, `{"deletion_mode":"active"}`, rec.Body.String())
	require.EqualValues(t, 1, store.touched.Load())
	require.Zero(t, store.deleted.Load())

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req = httptest.NewRequestWithContext(t.Context(), method, "/deletion-mode", http.NoBody)
		req.Header.Set("X-Service-Token", "permitted")
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code, method)
	}
	require.EqualValues(t, 1, store.touched.Load(), "no other method reads or writes the marker")
}

// acceptingStore records the admission request the router builds.
type acceptingStore struct {
	domain.ChunkStore
	accepted []domain.AcceptRequest
}

func (s *acceptingStore) AcceptDocument(_ context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	s.accepted = append(s.accepted, req)
	return domain.AcceptResult{Outcome: domain.AcceptAccepted, DocumentID: req.Document.ID}, nil
}

func TestRouterRequestsTaggedVectorsOnlyFromACapableBackend(t *testing.T) {
	for name, tc := range map[string]struct {
		driver   domain.VectorDeleter
		manifest bool
		want     bool
	}{
		"backend that deletes one incarnation": {memory.NewFakeVectorStore(), false, true},
		"backend limited to document deletion": {&vectorsSpy{}, false, false},
		"no backend":                           {nil, false, false},
		"manifested hosted vectors":            {memory.NewFakeVectorStore(), true, false},
	} {
		t.Run(name, func(t *testing.T) {
			store := &acceptingStore{}
			router := NewRouter(&RouterDeps{ChunkStore: store, PineconeDriver: tc.driver, Config: &config.Config{ServiceToken: "permitted", HostedVectorManifest: tc.manifest}})
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", strings.NewReader(`{"content":"router admission"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Service-Token", "permitted")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
			require.Len(t, store.accepted, 1)
			require.Equal(t, tc.want, store.accepted[0].SupportsScopedVectors, "the database marker, not the router, decides the format")
		})
	}
}
