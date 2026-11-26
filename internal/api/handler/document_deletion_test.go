package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const deletionTestDocument = "8a3e792c-5df6-4b6e-a612-4f7db4959db2"

// checkpointedStore records how a request reached the deletion coordinator.
type checkpointedStore struct {
	domain.ChunkStore
	active    bool
	modeErr   error
	result    error
	modeReads int
	deletes   []domain.DeleteOperations
	ids       []domain.DocumentID
	legacy    int
	deadline  time.Time
}

func (s *checkpointedStore) DeletionActive(context.Context) (bool, error) {
	s.modeReads++
	return s.active, s.modeErr
}

func (s *checkpointedStore) DeleteWithCleanup(ctx context.Context, id domain.DocumentID, ops domain.DeleteOperations) error {
	s.deletes = append(s.deletes, ops)
	s.ids = append(s.ids, id)
	s.deadline, _ = ctx.Deadline()
	return s.result
}

func (s *checkpointedStore) GetDocument(context.Context, domain.DocumentID) (*domain.Document, error) {
	s.legacy++
	return nil, nil
}

type releasableStore struct {
	*checkpointedStore
	released     []domain.DeleteOperations
	releasedIDs  []domain.DocumentID
	releasedIncs []string
	releaseErr   error
}

func (s *releasableStore) ReleaseHeldDeletion(_ context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	s.releasedIDs = append(s.releasedIDs, id)
	s.releasedIncs = append(s.releasedIncs, incarnation)
	s.released = append(s.released, ops)
	return s.releaseErr
}

type stubMemoryIndex struct{ owner string }

func (s stubMemoryIndex) MemoryOwner() string                { return s.owner }
func (s stubMemoryIndex) LockDocument(string) (func(), bool) { return func() {}, true }

func serveDeletion(t *testing.T, store domain.ChunkStore, header http.Header, opts ...DeleteOption) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+deletionTestDocument, http.NoBody)
	req.SetPathValue("doc_id", deletionTestDocument)
	req.Header = header
	rec := httptest.NewRecorder()
	DeleteHandler(store, nil, nil, opts...)(rec, req)
	return rec
}

func TestDeleteMapsCoordinatorOutcomes(t *testing.T) {
	incarnation := http.Header{"X-Document-Incarnation": {uuid.NewString()}}
	for _, tc := range []struct {
		name       string
		header     http.Header
		result     error
		status     int
		category   string
		retryAfter string
	}{
		{name: "completed", header: incarnation, status: http.StatusOK},
		{name: "absent", result: domain.ErrDocumentNotFound, status: http.StatusNotFound, category: "not_found"},
		{name: "live without qualifier", result: domain.ErrDeletionPreconditionRequired, status: http.StatusPreconditionRequired, category: "precondition"},
		{name: "replaced lifetime", header: incarnation, result: domain.ErrDeletionPreconditionFailed, status: http.StatusPreconditionFailed, category: "precondition"},
		{name: "fixture", header: incarnation, result: domain.ErrFixtureDeletionRequired, status: http.StatusPreconditionFailed, category: "precondition"},
		{name: "claimed elsewhere", header: incarnation, result: domain.ErrDeletionBusy, status: http.StatusServiceUnavailable, category: "busy", retryAfter: "5"},
		{name: "source storage missing", header: incarnation, result: domain.ErrFileStorageUnavailable, status: http.StatusServiceUnavailable, category: "storage"},
		{name: "vector storage missing", header: incarnation, result: domain.ErrVectorStorageUnavailable, status: http.StatusServiceUnavailable, category: "storage"},
		{name: "unfinished step", header: incarnation, result: errors.New("provider unavailable"), status: http.StatusInternalServerError, category: "internal"},
		{name: "deadline exceeded", header: incarnation, result: fmt.Errorf("delete vectors: %w", context.DeadlineExceeded), status: http.StatusInternalServerError, category: "internal"},
		{name: "canceled", header: incarnation, result: fmt.Errorf("delete vectors: %w", context.Canceled), status: http.StatusInternalServerError, category: "internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &checkpointedStore{active: true, result: tc.result}
			rec := serveDeletion(t, store, tc.header)
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Equal(t, tc.retryAfter, rec.Header().Get("Retry-After"))
			if tc.category != "" {
				require.Contains(t, rec.Body.String(), `"error":"`+tc.category+`"`)
			}
			require.Len(t, store.deletes, 1)
			require.Zero(t, store.legacy, "an active database must not use the compatibility path")
			if want := tc.header.Get("X-Document-Incarnation"); want != "" {
				require.Equal(t, want, store.deletes[0].ExpectedIncarnation)
			} else {
				require.Empty(t, store.deletes[0].ExpectedIncarnation)
			}
			require.False(t, strings.Contains(rec.Body.String(), "provider unavailable"), "internal errors must not reach the caller")
		})
	}
}

func TestDeleteNormalizesTheQualifierAndBoundsTheCleanup(t *testing.T) {
	id := uuid.NewString()
	store := &checkpointedStore{active: true}
	rec := serveDeletion(t, store, http.Header{"x-document-incarnation": {strings.ToUpper(id)}},
		WithCleanupTargets(cleanup.Targets{SourceBucket: "originals", VectorIndex: "index"}))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, id, store.deletes[0].ExpectedIncarnation)
	require.Equal(t, "originals", store.deletes[0].SourceBucket)
	require.Equal(t, "index", store.deletes[0].VectorIndex)
	require.False(t, store.deadline.IsZero(), "the cleanup needs its own deadline")
	require.Less(t, time.Until(store.deadline), time.Minute+time.Second)
}

func TestDeleteAddressesTheCanonicalUUID(t *testing.T) {
	store := &checkpointedStore{active: true}
	upper := strings.ToUpper(deletionTestDocument)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+upper, http.NoBody)
	req.SetPathValue("doc_id", upper)
	req.Header.Set("X-Document-Incarnation", uuid.NewString())
	rec := httptest.NewRecorder()
	DeleteHandler(store, nil, nil)(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, []domain.DocumentID{deletionTestDocument}, store.ids, "external stores key vectors by the canonical form")
	require.Contains(t, rec.Body.String(), `"doc_id":"`+deletionTestDocument+`"`)
}

func TestDeleteRejectsMalformedQualifiersOnAnActiveDatabase(t *testing.T) {
	valid := uuid.NewString()
	for name, values := range map[string][]string{
		"empty":         {""},
		"duplicate":     {valid, valid},
		"not a UUID":    {"incarnation"},
		"braces":        {"{" + valid + "}"},
		"no hyphens":    {strings.ReplaceAll(valid, "-", "")},
		"urn":           {"urn:uuid:" + valid},
		"padded":        {" " + valid},
		"trailing junk": {valid + "x"},
	} {
		t.Run(name, func(t *testing.T) {
			store := &checkpointedStore{active: true}
			rec := serveDeletion(t, store, http.Header{"X-Document-Incarnation": values})
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Empty(t, store.deletes)
			require.Zero(t, store.legacy)
		})
	}
}

func TestDeleteReleaseHeldValidationAndRouting(t *testing.T) {
	inc := uuid.NewString()
	offlineOpts := WithCleanupTargets(cleanup.Targets{InMemory: true, Memory: stubMemoryIndex{owner: "owner-1"}})

	for _, active := range []bool{false, true} {
		t.Run("true_routes_only_to_release_in_either_mode", func(t *testing.T) {
			store := &releasableStore{checkpointedStore: &checkpointedStore{active: active}}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+deletionTestDocument+"?release_held=true", http.NoBody)
			req.SetPathValue("doc_id", deletionTestDocument)
			req.Header.Set("X-Document-Incarnation", inc)
			rec := httptest.NewRecorder()
			DeleteHandler(store, nil, nil, offlineOpts)(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, []domain.DocumentID{deletionTestDocument}, store.releasedIDs)
			require.Equal(t, []string{inc}, store.releasedIncs)
			require.Empty(t, store.deletes)
			require.Zero(t, store.legacy)
			require.Zero(t, store.modeReads, "hold release must work without activating or checking mode")
		})
	}

	t.Run("false_uses_qualified_deletion_when_active", func(t *testing.T) {
		store := &releasableStore{checkpointedStore: &checkpointedStore{active: true}}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+deletionTestDocument+"?release_held=false", http.NoBody)
		req.SetPathValue("doc_id", deletionTestDocument)
		req.Header.Set("X-Document-Incarnation", inc)
		rec := httptest.NewRecorder()
		DeleteHandler(store, nil, nil, offlineOpts)(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Len(t, store.deletes, 1)
		require.Empty(t, store.released)
	})

	t.Run("hosted_target_refuses_offline_release_with_412", func(t *testing.T) {
		store := &releasableStore{checkpointedStore: &checkpointedStore{active: true}}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+deletionTestDocument+"?release_held=true", http.NoBody)
		req.SetPathValue("doc_id", deletionTestDocument)
		req.Header.Set("X-Document-Incarnation", inc)
		rec := httptest.NewRecorder()
		DeleteHandler(store, nil, nil)(rec, req)
		require.Equal(t, http.StatusPreconditionFailed, rec.Code)
		require.Empty(t, store.released)
		require.Empty(t, store.deletes)
	})

	for name, tc := range map[string]struct {
		query  string
		header http.Header
		status int
	}{
		"capitalized True":       {query: "release_held=True", header: http.Header{"X-Document-Incarnation": {inc}}, status: http.StatusBadRequest},
		"empty value":            {query: "release_held=", header: http.Header{"X-Document-Incarnation": {inc}}, status: http.StatusBadRequest},
		"false then true":        {query: "release_held=false&release_held=true", header: http.Header{"X-Document-Incarnation": {inc}}, status: http.StatusBadRequest},
		"true then false":        {query: "release_held=true&release_held=false", header: http.Header{"X-Document-Incarnation": {inc}}, status: http.StatusBadRequest},
		"duplicate true":         {query: "release_held=true&release_held=true", header: http.Header{"X-Document-Incarnation": {inc}}, status: http.StatusBadRequest},
		"malformed encoding":     {query: "release_held=%ZZ", header: http.Header{"X-Document-Incarnation": {inc}}, status: http.StatusBadRequest},
		"missing incarnation":    {query: "release_held=true", status: http.StatusPreconditionRequired},
		"malformed incarnation":  {query: "release_held=true", header: http.Header{"X-Document-Incarnation": {"bad"}}, status: http.StatusBadRequest},
		"empty incarnation":      {query: "release_held=true", header: http.Header{"X-Document-Incarnation": {""}}, status: http.StatusBadRequest},
		"duplicate incarnations": {query: "release_held=true", header: http.Header{"X-Document-Incarnation": {inc, inc}}, status: http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			store := &releasableStore{checkpointedStore: &checkpointedStore{active: true}}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+deletionTestDocument+"?"+tc.query, http.NoBody)
			req.SetPathValue("doc_id", deletionTestDocument)
			req.Header = tc.header
			rec := httptest.NewRecorder()
			DeleteHandler(store, nil, nil, offlineOpts)(rec, req)
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Zero(t, store.modeReads)
			require.Empty(t, store.deletes)
			require.Empty(t, store.released)
			require.Zero(t, store.legacy)
		})
	}
}

func TestDeleteWithoutActivationKeepsTheCompatibilityContract(t *testing.T) {
	store := &checkpointedStore{}
	rec := serveDeletion(t, store, http.Header{"X-Document-Incarnation": {uuid.NewString()}})
	require.Equal(t, http.StatusPreconditionFailed, rec.Code, "a premature qualifier is refused")
	require.Empty(t, store.deletes)
	require.Zero(t, store.legacy)

	rec = serveDeletion(t, store, nil)
	require.Equal(t, http.StatusNotFound, rec.Code, "an unqualified request addresses the live UUID")
	require.Empty(t, store.deletes)
	require.Equal(t, 1, store.legacy)
}

func TestDeleteFailsClosedWhenTheModeCannotBeRead(t *testing.T) {
	store := &checkpointedStore{modeErr: errors.New("connection refused")}
	for _, header := range []http.Header{nil, {"X-Document-Incarnation": {uuid.NewString()}}} {
		rec := serveDeletion(t, store, header)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		require.Empty(t, store.deletes)
		require.Zero(t, store.legacy, "a failed mode lookup must never select the compatibility path")
	}
}

func TestCheckpointedDeleteSurvivesClientDisconnect(t *testing.T) {
	store := &checkpointedStore{active: true}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodDelete, "/documents/"+deletionTestDocument, http.NoBody)
	req.SetPathValue("doc_id", deletionTestDocument)
	req.Header.Set("X-Document-Incarnation", uuid.NewString())
	rec := httptest.NewRecorder()
	DeleteHandler(&contextChecking{checkpointedStore: store}, nil, nil)(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, store.deletes, 1)
}

// contextChecking fails the coordinator if the client's cancellation reaches it.
type contextChecking struct{ *checkpointedStore }

func (s *contextChecking) DeletionActive(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return s.checkpointedStore.DeletionActive(ctx)
}

func (s *contextChecking) DeleteWithCleanup(ctx context.Context, id domain.DocumentID, ops domain.DeleteOperations) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.checkpointedStore.DeleteWithCleanup(ctx, id, ops)
}

func TestServiceDeleteTimeoutReturnsInternalWithRetryContract(t *testing.T) {
	store := &checkpointedStore{
		active: true,
		result: fmt.Errorf("delete vectors: %w", context.DeadlineExceeded),
	}
	rec := serveDeletion(t, store, http.Header{"X-Document-Incarnation": {uuid.NewString()}})
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	require.JSONEq(t, `{"error":"internal","message":"failed to delete document"}`, rec.Body.String())

	// Compatibility path (inactive / non-DocumentDeleter store) also keeps 500 on timeout.
	calls := 0
	compatStore := &fileDeleteStore{
		document: &domain.Document{ID: deletionTestDocument},
		check: func(ctx context.Context) error {
			calls++
			if calls == 1 {
				return ctx.Err()
			}
			return fmt.Errorf("delete document metadata: %w", context.DeadlineExceeded)
		},
	}
	compatRec := serveDeletion(t, compatStore, nil)
	require.Equal(t, http.StatusInternalServerError, compatRec.Code, compatRec.Body.String())
	require.JSONEq(t, `{"error":"internal","message":"failed to delete document"}`, compatRec.Body.String())
}

func FuzzDeletionIncarnation(f *testing.F) {
	valid := uuid.NewString()
	for _, seed := range []string{valid, strings.ToUpper(valid), "", "x", "{" + valid + "}", valid + " ", "urn:uuid:" + valid} {
		f.Add("X-Document-Incarnation", seed, "")
	}
	f.Add("x-document-incarnation", valid, valid)
	f.Fuzz(func(t *testing.T, name, first, second string) {
		headers := http.Header{}
		if name != "" {
			headers[name] = []string{first}
			if second != "" {
				headers[name] = append(headers[name], second)
			}
		}
		incarnation, present, ok := deletionIncarnation(headers)
		selected := strings.EqualFold(name, incarnationHeaderName)
		require.Equal(t, selected, present)
		if !present {
			require.True(t, ok)
			require.Empty(t, incarnation)
			return
		}
		if !ok {
			require.Empty(t, incarnation, "a malformed qualifier must not select a lifetime")
			return
		}
		require.Empty(t, second)
		parsed, err := uuid.Parse(incarnation)
		require.NoError(t, err)
		require.Equal(t, parsed.String(), incarnation)
		require.Len(t, first, len(incarnation))
		require.True(t, strings.EqualFold(first, incarnation))
	})
}
