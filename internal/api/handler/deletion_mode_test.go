package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/tracing"
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

func TestDeletionModeErrorTelemetry(t *testing.T) {
	lookupErr := errors.New("connection reset while reading private-marker-canary")
	for _, tc := range []struct {
		name   string
		store  domain.ChunkStore
		status int
		logs   []string
		causes []string
	}{
		{"missing capability", struct{ domain.ChunkStore }{}, http.StatusServiceUnavailable,
			[]string{"encode error response"}, []string{"connection closed"}},
		{"lookup failure", &modeStore{err: lookupErr}, http.StatusServiceUnavailable,
			[]string{"read deletion mode", "encode error response"}, []string{lookupErr.Error(), "connection closed"}},
		{"success response write failure", &modeStore{}, http.StatusOK,
			[]string{"encode deletion mode response"}, []string{"connection closed"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, redacted := range []bool{false, true} {
				logs := captureLeveledLogs(t)
				ctx := logctx.WithRequestID(t.Context(), "deletion-mode-request")
				ctx = tracing.WithRedactIdentifiers(ctx, redacted)
				req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/deletion-mode", http.NoBody)
				writer := &leveledFailWriter{*httptest.NewRecorder()}
				DeletionModeHandler(tc.store)(writer, req)
				require.Equal(t, tc.status, writer.Code)
				lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
				require.Len(t, lines, len(tc.logs))
				for i, line := range lines {
					var record map[string]any
					require.NoError(t, json.Unmarshal([]byte(line), &record))
					require.Equal(t, tc.logs[i], record["msg"])
					require.Equal(t, "deletion-mode-request", record["request_id"])
					if redacted {
						require.Equal(t, "operation failed", record["error"])
						require.NotContains(t, line, tc.causes[i])
					} else {
						require.Equal(t, tc.causes[i], record["error"])
					}
				}
			}
		})
	}
}
