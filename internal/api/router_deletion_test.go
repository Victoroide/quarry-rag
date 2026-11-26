package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
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
			targets := deletionTargets(&tc.cfg)
			require.Equal(t, tc.bucket, targets.SourceBucket)
			require.Equal(t, tc.idx, targets.VectorIndex)
			require.Equal(t, tc.wantInMemory, targets.InMemory)
		})
	}
}
