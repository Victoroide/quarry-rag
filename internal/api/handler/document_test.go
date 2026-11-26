package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type fileDeleteContextKey struct{}

type fileDeleteStore struct {
	domain.ChunkStore
	document *domain.Document
	deleted  bool
	check    func(context.Context) error
}

func (s *fileDeleteStore) GetDocument(ctx context.Context, _ domain.DocumentID) (*domain.Document, error) {
	if s.check != nil {
		if err := s.check(ctx); err != nil {
			return nil, err
		}
	}
	return s.document, nil
}

func (s *fileDeleteStore) DeleteDocument(ctx context.Context, _ domain.DocumentID) error {
	if s.check != nil {
		if err := s.check(ctx); err != nil {
			return err
		}
	}
	s.deleted = true
	return nil
}

func TestDeleteSurvivesClientDisconnect(t *testing.T) {
	const docID = "8a3e792c-5df6-4b6e-a612-4f7db4959db2"
	store := &fileDeleteStore{document: &domain.Document{ID: docID}}
	store.check = func(ctx context.Context) error {
		if ctx.Value(fileDeleteContextKey{}) != "request-value" {
			t.Error("deletion lost request context values")
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > time.Minute {
			t.Error("deletion has no bounded independent deadline")
		}
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), fileDeleteContextKey{}, "request-value"))
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodDelete, "/documents/"+docID, http.NoBody)
	req.SetPathValue("doc_id", docID)
	resp := httptest.NewRecorder()
	DeleteHandler(store, nil, nil)(resp, req)
	if resp.Code != http.StatusOK || !store.deleted {
		t.Fatalf("status = %d, deleted = %v, body = %s", resp.Code, store.deleted, resp.Body.String())
	}
}

func TestDeleteReturnsStorageUnavailable(t *testing.T) {
	const docID = "8a3e792c-5df6-4b6e-a612-4f7db4959db2"
	key := "documents/document/" + docID + "/file.pdf"
	store := &fileDeleteStore{document: &domain.Document{ID: docID, S3Key: &key}}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+docID, http.NoBody)
	req.SetPathValue("doc_id", docID)
	resp := httptest.NewRecorder()
	DeleteHandler(store, nil, nil)(resp, req)
	var body struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if resp.Code != http.StatusServiceUnavailable || body.Error != "storage" || body.Message != "stored file cannot be deleted because file storage is not configured" || store.deleted {
		t.Fatalf("status = %d, body = %+v, deleted = %v", resp.Code, body, store.deleted)
	}
}

func TestDeleteRejectsReservedIncarnationSelectors(t *testing.T) {
	incHeader := http.Header{"X-Document-Incarnation": {"e22e083c-1695-4f93-841f-829c5e9e0978"}}
	for _, tc := range []struct {
		name, query string
		headers     http.Header
		status      int
	}{
		{name: "header", headers: incHeader, status: http.StatusPreconditionFailed},
		{name: "empty header", headers: http.Header{"X-Document-Incarnation": {""}}, status: http.StatusPreconditionFailed},
		{name: "duplicate header", headers: http.Header{"X-Document-Incarnation": {"", "value"}}, status: http.StatusPreconditionFailed},
		{name: "release without incarnation", query: "release_held=true", status: http.StatusPreconditionRequired},
		{name: "release on non-offline store", query: "release_held=true", headers: incHeader, status: http.StatusPreconditionFailed},
		{name: "false release uses ordinary delete", query: "release_held=false", status: http.StatusOK},
		{name: "empty release", query: "release_held=", headers: incHeader, status: http.StatusBadRequest},
		{name: "noncanonical True release", query: "release_held=True", headers: incHeader, status: http.StatusBadRequest},
		{name: "duplicate false then true", query: "release_held=false&release_held=true", headers: incHeader, status: http.StatusBadRequest},
		{name: "duplicate true then false", query: "release_held=true&release_held=false", headers: incHeader, status: http.StatusBadRequest},
		{name: "duplicate true then true", query: "release_held=true&release_held=true", headers: incHeader, status: http.StatusBadRequest},
		{name: "duplicate false then false", query: "release_held=false&release_held=false", status: http.StatusBadRequest},
		{name: "encoded duplicate release", query: "release_held=false&release%5fheld=true", headers: incHeader, status: http.StatusBadRequest},
		{name: "malformed query", query: "release_held=%zz", headers: incHeader, status: http.StatusBadRequest},
		{name: "ambiguous separator", query: "flag=on;release_held=true", headers: incHeader, status: http.StatusBadRequest},
		{name: "bare control", query: "unrelated=value", status: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, accessed, deleted := exerciseDeletion(t, tc.query, tc.headers)
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Equal(t, tc.status == http.StatusOK, accessed)
			require.Equal(t, tc.status == http.StatusOK, deleted)
		})
	}
}

func FuzzDeleteReservedIncarnationSelectors(f *testing.F) {
	f.Add("release%5fheld=true", false, "")
	f.Add("release_held=false", false, "")
	f.Add("release_held=True", false, "")
	f.Add("release_held=false&release_held=true", false, "")
	f.Add("release_held=%", false, "")
	f.Add("", true, "")
	f.Add("unrelated=1", false, "")
	f.Fuzz(func(t *testing.T, query string, headerPresent bool, value string) {
		var headers http.Header
		if headerPresent {
			headers = http.Header{"x-document-incarnation": {value}}
		}
		params, err := url.ParseQuery(query)
		release, validRelease := parseReleaseHeld(params)
		wantCleanup := err == nil && validRelease && !release && !headerPresent
		rec, accessed, deleted := exerciseDeletion(t, query, headers)
		require.Equal(t, wantCleanup, rec.Code == http.StatusOK)
		require.Equal(t, wantCleanup, accessed)
		require.Equal(t, wantCleanup, deleted)
	})
}

func exerciseDeletion(t *testing.T, query string, headers http.Header) (*httptest.ResponseRecorder, bool, bool) {
	t.Helper()
	const id = "8a3e792c-5df6-4b6e-a612-4f7db4959db2"
	var accessed bool
	store := &fileDeleteStore{document: &domain.Document{ID: id}, check: func(context.Context) error {
		accessed = true
		return nil
	}}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+id, http.NoBody)
	req.SetPathValue("doc_id", id)
	req.URL.RawQuery = query
	req.Header = headers
	rec := httptest.NewRecorder()
	DeleteHandler(store, nil, nil)(rec, req)
	return rec, accessed, store.deleted
}
