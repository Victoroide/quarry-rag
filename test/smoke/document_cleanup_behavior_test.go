package smoke

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	qclient "github.com/Victoroide/quarry-rag/client"
	serviceapi "github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// scriptedService answers each call from a script and records what it was asked.
type scriptedService struct {
	mu          sync.Mutex
	mode        func() (*qclient.DeletionModeResponse, error)
	incarnation func() (*qclient.DocumentIncarnationSummary, error)
	deletion    func() (*qclient.DeleteResponse, error)

	modeReads        int
	incarnationReads []string
	deletions        [][2]string
}

func (s *scriptedService) GetDeletionMode(context.Context, ...qclient.RequestOption) (*qclient.DeletionModeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modeReads++
	return s.mode()
}

func (s *scriptedService) GetDocumentWithIncarnation(_ context.Context, id string) (*qclient.DocumentIncarnationSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incarnationReads = append(s.incarnationReads, id)
	return s.incarnation()
}

func (s *scriptedService) DeleteDocumentIncarnation(_ context.Context, id, incarnation string) (*qclient.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletions = append(s.deletions, [2]string{id, incarnation})
	return s.deletion()
}

func fastRetries(t *testing.T) {
	t.Helper()
	previous := cleanupRetryDelay
	cleanupRetryDelay = time.Millisecond
	t.Cleanup(func() { cleanupRetryDelay = previous })
}

func apiFailure(status int) error {
	return &qclient.APIError{StatusCode: status, Code: "failure", Message: "scripted"}
}

func TestActivationCheckAcceptsOnlyAnExplicitlyActiveTarget(t *testing.T) {
	for name, tc := range map[string]struct {
		mode func() (*qclient.DeletionModeResponse, error)
		want error
	}{
		"active": {func() (*qclient.DeletionModeResponse, error) {
			return &qclient.DeletionModeResponse{DeletionMode: "active"}, nil
		}, nil},
		"inactive": {func() (*qclient.DeletionModeResponse, error) {
			return &qclient.DeletionModeResponse{DeletionMode: "inactive"}, nil
		}, errTargetInactive},
		"unknown": {func() (*qclient.DeletionModeResponse, error) {
			return &qclient.DeletionModeResponse{DeletionMode: "pending"}, nil
		}, errTargetInactive},
		"empty":      {func() (*qclient.DeletionModeResponse, error) { return &qclient.DeletionModeResponse{}, nil }, errTargetInactive},
		"unreadable": {func() (*qclient.DeletionModeResponse, error) { return nil, apiFailure(http.StatusServiceUnavailable) }, errors.New("read the deletion mode")},
	} {
		t.Run(name, func(t *testing.T) {
			api := &scriptedService{mode: tc.mode}
			err := checkActivatedTarget(t.Context(), api)
			switch {
			case tc.want == nil:
				require.NoError(t, err)
			case errors.Is(tc.want, errTargetInactive):
				require.ErrorIs(t, err, errTargetInactive)
			default:
				require.ErrorContains(t, err, tc.want.Error())
				require.NotErrorIs(t, err, errTargetInactive)
			}
			require.Equal(t, 1, api.modeReads)
			require.Empty(t, api.incarnationReads)
			require.Empty(t, api.deletions)
		})
	}
}

func TestIncarnationIsReadOnceFromAnAdmittedDocumentAndNeverGuessed(t *testing.T) {
	fastRetries(t)
	id, incarnation := uuid.NewString(), uuid.NewString()

	t.Run("transient failures repeat the read", func(t *testing.T) {
		calls := 0
		api := &scriptedService{incarnation: func() (*qclient.DocumentIncarnationSummary, error) {
			calls++
			if calls < 3 {
				return nil, apiFailure(http.StatusServiceUnavailable)
			}
			return &qclient.DocumentIncarnationSummary{Incarnation: incarnation}, nil
		}}
		got, err := readAdmittedIncarnation(t.Context(), api, id)
		require.NoError(t, err)
		require.Equal(t, incarnation, got)
		require.Equal(t, []string{id, id, id}, api.incarnationReads)
	})
	t.Run("an absent document is never cleanup", func(t *testing.T) {
		api := &scriptedService{incarnation: func() (*qclient.DocumentIncarnationSummary, error) { return nil, apiFailure(http.StatusNotFound) }}
		_, err := readAdmittedIncarnation(t.Context(), api, id)
		require.Error(t, err)
		require.Len(t, api.incarnationReads, 1, "a missing document is not retried")
		require.Empty(t, api.deletions)
	})
	t.Run("an unusable token is an error", func(t *testing.T) {
		for _, bad := range []string{"", "latest", "{" + incarnation + "}", "2d43ca1476bc4f72bf85e7a607a1197d", incarnation + " "} {
			api := &scriptedService{incarnation: func() (*qclient.DocumentIncarnationSummary, error) {
				return &qclient.DocumentIncarnationSummary{Incarnation: bad}, nil
			}}
			_, err := readAdmittedIncarnation(t.Context(), api, id)
			require.Error(t, err, "%q", bad)
			require.Len(t, api.incarnationReads, 1)
		}
	})
	t.Run("an invalid identifier makes no request", func(t *testing.T) {
		api := &scriptedService{}
		for _, bad := range []string{"", "not-a-uuid", "{" + id + "}"} {
			_, err := readAdmittedIncarnation(t.Context(), api, bad)
			require.Error(t, err, "%q", bad)
		}
		require.Empty(t, api.incarnationReads)
	})
	t.Run("a spent deadline stops the retries", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		api := &scriptedService{incarnation: func() (*qclient.DocumentIncarnationSummary, error) { return nil, apiFailure(http.StatusBadGateway) }}
		_, err := readAdmittedIncarnation(ctx, api, id)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestCleanupRepeatsOnlyTheSavedIncarnation(t *testing.T) {
	fastRetries(t)
	id, saved := uuid.NewString(), uuid.NewString()

	t.Run("lost replies and busy services repeat the same token", func(t *testing.T) {
		calls := 0
		api := &scriptedService{deletion: func() (*qclient.DeleteResponse, error) {
			calls++
			switch calls {
			case 1:
				return nil, errors.New("connection reset by peer")
			case 2:
				return nil, apiFailure(http.StatusServiceUnavailable)
			case 3:
				return nil, &qclient.APIError{StatusCode: http.StatusTooManyRequests, RetryAfter: time.Millisecond}
			}
			return &qclient.DeleteResponse{Deleted: true, DocID: id}, nil
		}}
		require.NoError(t, deleteSavedIncarnation(t.Context(), api, id, saved))
		require.Len(t, api.deletions, 4)
		for _, attempt := range api.deletions {
			require.Equal(t, [2]string{id, saved}, attempt, "every attempt sends the saved token")
		}
		require.Empty(t, api.incarnationReads, "cleanup never looks up a current token")
		require.Zero(t, api.modeReads)
	})
	t.Run("a replacement ends the cleanup", func(t *testing.T) {
		api := &scriptedService{deletion: func() (*qclient.DeleteResponse, error) { return nil, apiFailure(http.StatusPreconditionFailed) }}
		err := deleteSavedIncarnation(t.Context(), api, id, saved)
		var apiErr *qclient.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)
		require.Len(t, api.deletions, 1, "a different lifetime is never retried or re-read")
		require.Empty(t, api.incarnationReads)
	})
	t.Run("other client errors are final", func(t *testing.T) {
		for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusPreconditionRequired} {
			api := &scriptedService{deletion: func() (*qclient.DeleteResponse, error) { return nil, apiFailure(status) }}
			require.Error(t, deleteSavedIncarnation(t.Context(), api, id, saved), status)
			require.Len(t, api.deletions, 1, status)
		}
	})
	t.Run("an absent document needs no deletion", func(t *testing.T) {
		api := &scriptedService{deletion: func() (*qclient.DeleteResponse, error) { return nil, apiFailure(http.StatusNotFound) }}
		require.NoError(t, deleteSavedIncarnation(t.Context(), api, id, saved))
		require.Len(t, api.deletions, 1)
	})
	t.Run("the deadline bounds the retries", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		api := &scriptedService{deletion: func() (*qclient.DeleteResponse, error) { return nil, apiFailure(http.StatusServiceUnavailable) }}
		require.ErrorIs(t, deleteSavedIncarnation(ctx, api, id, saved), context.DeadlineExceeded)
		for _, attempt := range api.deletions {
			require.Equal(t, [2]string{id, saved}, attempt)
		}
	})
}

// observedRequest is one request a fixture service received.
type observedRequest struct {
	method, path, incarnation string
}

// fixtureService is the production router on a disposable database with an
// in-process vector index, reached over HTTP like a deployed service.
type fixtureService struct {
	db  *pgstore.Client
	api *qclient.Client

	mu       sync.Mutex
	observed []observedRequest
}

func newFixtureService(t *testing.T) *fixtureService {
	t.Helper()
	db, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))
	vectors := memory.NewFakeVectorStore()
	fx := &fixtureService{db: db}
	router := serviceapi.NewRouter(&serviceapi.RouterDeps{
		ChunkStore: db, VectorStore: vectors, PineconeDriver: vectors, LegacyDeletionReservations: true,
		Config: &config.Config{ServiceToken: "fixture-token"},
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fx.mu.Lock()
		fx.observed = append(fx.observed, observedRequest{r.Method, r.URL.Path, r.Header.Get("X-Document-Incarnation")})
		fx.mu.Unlock()
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	fx.api, err = qclient.New(server.URL, qclient.WithServiceToken("fixture-token"))
	require.NoError(t, err)
	return fx
}

func (fx *fixtureService) requests() []observedRequest {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return append([]observedRequest(nil), fx.observed...)
}

func (fx *fixtureService) reset() {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	fx.observed = nil
}

func (fx *fixtureService) admit(t *testing.T, id, content string) {
	t.Helper()
	response, err := fx.api.Ingest(t.Context(), qclient.IngestRequest{DocumentID: id, Content: content})
	require.NoError(t, err)
	require.Equal(t, id, response.DocumentID)
}

func (fx *fixtureService) incarnation(t *testing.T, id string) string {
	t.Helper()
	var stored string
	require.NoError(t, fx.db.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, id).Scan(&stored))
	return stored
}

func (fx *fixtureService) marker(t *testing.T) *time.Time {
	t.Helper()
	var activated *time.Time
	require.NoError(t, fx.db.Pool.QueryRow(t.Context(), `SELECT activated_at FROM document_deletion_activation`).Scan(&activated))
	return activated
}

func (fx *fixtureService) documents(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, fx.db.Pool.QueryRow(t.Context(), `SELECT count(*) FROM documents`).Scan(&n))
	return n
}

func TestWritingSmokeStopsBeforeAnyIngestOrDeleteOnAnInactiveTarget(t *testing.T) {
	fx := newFixtureService(t)
	// A local setting from before activation moved into the database. It has no
	// effect on the target, and nothing in the test process can set the marker.
	t.Setenv("RETRYABLE_DELETION", "true")
	unrelated := uuid.NewString()
	fx.admit(t, unrelated, "a document that belongs to an unrelated caller")
	fx.reset()

	err := checkActivatedTarget(t.Context(), fx.api)
	require.ErrorIs(t, err, errTargetInactive)
	require.Equal(t, []observedRequest{{http.MethodGet, "/deletion-mode", ""}}, fx.requests(), "the check reads once and sends no ingest or delete")
	require.Equal(t, 1, fx.documents(t))
	require.Nil(t, fx.marker(t), "the marker stays unset")

	// An unrelated bare caller keeps the behavior of an inactive database.
	deleted, err := fx.api.DeleteDocument(t.Context(), unrelated)
	require.NoError(t, err)
	require.True(t, deleted.Deleted)
	require.Zero(t, fx.documents(t))
	require.Nil(t, fx.marker(t))
}

func TestSmokeCleanupOnAnActiveTargetNeverTouchesAReplacementOrTheControl(t *testing.T) {
	fx := newFixtureService(t)
	_, err := fx.db.ActivateDeletion(t.Context())
	require.NoError(t, err)
	require.NoError(t, checkActivatedTarget(t.Context(), fx.api))

	// The preexisting smoke control stays read-only throughout.
	control := uuid.NewString()
	fx.admit(t, control, "the preexisting document of the smoke control")
	controlIncarnation := fx.incarnation(t, control)

	id := uuid.NewString()
	fx.admit(t, id, "a document admitted by this smoke run")
	saved, err := readAdmittedIncarnation(t.Context(), fx.api, id)
	require.NoError(t, err)
	require.Equal(t, fx.incarnation(t, id), saved)
	fx.reset()

	require.NoError(t, deleteSavedIncarnation(t.Context(), fx.api, id, saved))
	require.Zero(t, fx.documentsWithID(t, id))
	require.NoError(t, deleteSavedIncarnation(t.Context(), fx.api, id, saved), "a repeat acknowledges the completed deletion")

	// The identifier is reused by another lifetime. A late cleanup with the
	// saved token cannot remove it.
	fx.admit(t, id, "a replacement admitted after the first lifetime was removed")
	replacement := fx.incarnation(t, id)
	require.NotEqual(t, saved, replacement)
	err = deleteSavedIncarnation(t.Context(), fx.api, id, saved)
	var apiErr *qclient.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)
	require.Equal(t, replacement, fx.incarnation(t, id), "the replacement is intact")
	require.Equal(t, controlIncarnation, fx.incarnation(t, control), "the control is intact")

	for _, request := range fx.requests() {
		if request.method == http.MethodPost && request.path == "/ingest" {
			continue // the test itself reuses the identifier
		}
		require.Equal(t, observedRequest{http.MethodDelete, "/documents/" + id, saved}, request,
			"cleanup sends only qualified deletions of the saved lifetime and never looks anything up")
	}
}

func (fx *fixtureService) documentsWithID(t *testing.T, id string) int {
	t.Helper()
	var n int
	require.NoError(t, fx.db.Pool.QueryRow(t.Context(), `SELECT count(*) FROM documents WHERE id=$1`, id).Scan(&n))
	return n
}
