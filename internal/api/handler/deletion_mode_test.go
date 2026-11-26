package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

// modeStore answers the mode lookup and fails any other store call.
type modeStore struct {
	domain.ChunkStore
	active bool
	err    error
	reads  atomic.Int32
}

func (s *modeStore) DeletionActive(context.Context) (bool, error) {
	s.reads.Add(1)
	return s.active, s.err
}

func (s *modeStore) DeleteWithCleanup(context.Context, domain.DocumentID, domain.DeleteOperations) error {
	panic("a mode check never deletes")
}

func serveDeletionMode(t *testing.T, store domain.ChunkStore) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	DeletionModeHandler(store)(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/deletion-mode", http.NoBody))
	return rec
}

func TestDeletionModeReportsTheDatabaseMarker(t *testing.T) {
	for _, tc := range []struct {
		active bool
		want   string
	}{{false, DeletionModeInactive}, {true, DeletionModeActive}} {
		store := &modeStore{active: tc.active}
		rec := serveDeletionMode(t, store)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
		var body map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Equal(t, map[string]string{"deletion_mode": tc.want}, body, "the response carries only the mode")
		require.EqualValues(t, 1, store.reads.Load())
	}
}

func TestDeletionModeFailsClosedWhenTheMarkerCannotBeRead(t *testing.T) {
	for name, store := range map[string]domain.ChunkStore{
		"lookup error":       &modeStore{err: errors.New("connection reset")},
		"store without mode": struct{ domain.ChunkStore }{},
	} {
		t.Run(name, func(t *testing.T) {
			rec := serveDeletionMode(t, store)
			require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
			require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
			var body APIError
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Equal(t, "unavailable", body.Error)
			require.NotContains(t, rec.Body.String(), "connection reset", "a driver error never reaches the caller")
			require.NotContains(t, rec.Body.String(), DeletionModeInactive, "an unreadable marker never reads as a mode")
		})
	}
}
