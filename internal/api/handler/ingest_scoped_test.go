package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

// capturingStore records the admission requests of one test.
type capturingStore struct {
	domain.ChunkStore
	mu       sync.Mutex
	accepted []domain.AcceptRequest
}

func (s *capturingStore) AcceptDocument(_ context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accepted = append(s.accepted, req)
	return domain.AcceptResult{Outcome: domain.AcceptAccepted, DocumentID: req.Document.ID}, nil
}

func TestScopedVectorsAreOnlyRequestedWhenTheBackendIsDeclaredCapable(t *testing.T) {
	jsonRequest := func() *http.Request {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", strings.NewReader(`{"content":"scoped admission"}`))
		r.Header.Set("Content-Type", "application/json")
		return r
	}
	for name, tc := range map[string]struct {
		opts []IngestOption
		want bool
	}{
		"default":         {nil, false},
		"declared":        {[]IngestOption{WithScopedVectors()}, true},
		"declared twice":  {[]IngestOption{WithScopedVectors(), WithScopedVectors()}, true},
		"other options":   {[]IngestOption{WithMaxPending(5)}, false},
		"declared, other": {[]IngestOption{WithMaxPending(5), WithScopedVectors()}, true},
	} {
		t.Run(name, func(t *testing.T) {
			for path, request := range map[string]*http.Request{
				"json":      jsonRequest(),
				"multipart": detectionUpload(t, "notes.txt", bytes.Repeat([]byte("scoped upload "), 8)),
			} {
				store := &capturingStore{}
				rec := httptest.NewRecorder()
				IngestHandler(nil, store, nil, 20<<20, tc.opts...)(rec, request)
				require.Equal(t, http.StatusAccepted, rec.Code, "%s: %s", path, rec.Body.String())
				require.Len(t, store.accepted, 1, path)
				require.Equal(t, tc.want, store.accepted[0].SupportsScopedVectors, path)
			}
		})
	}
}
