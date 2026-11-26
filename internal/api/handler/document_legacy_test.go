package handler

import (
	"context"
	"net/http"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// guardedStore adds the activation guard to the checkpointed double.
type guardedStore struct {
	*checkpointedStore
	guarded     int
	guardResult error
	lockedID    domain.DocumentID
}

func (s *guardedStore) WithLegacyDeletion(_ context.Context, id domain.DocumentID, _ func(domain.LegacyDocumentStore) error) error {
	s.guarded++
	s.lockedID = id
	return s.guardResult
}

func TestIDOnlyDeleteUsesTheGuardOnlyWhenReservationsAreEnabled(t *testing.T) {
	store := &guardedStore{checkpointedStore: &checkpointedStore{}}
	rec := serveDeletion(t, store, http.Header{})
	require.Equal(t, http.StatusNotFound, rec.Code, "the default path is unchanged")
	require.Zero(t, store.guarded)
	require.Equal(t, 1, store.legacy)

	store = &guardedStore{checkpointedStore: &checkpointedStore{}}
	rec = serveDeletion(t, store, http.Header{}, WithLegacyReservations())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, 1, store.guarded)
	require.Equal(t, domain.DocumentID(deletionTestDocument), store.lockedID)
	require.Zero(t, store.legacy, "the guarded path replaces the unguarded lookup")
	require.Empty(t, store.deletes)
}

func TestGuardedIDOnlyDeleteMapsLegacyOutcomes(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		status int
	}{
		"absent":           {domain.ErrDocumentNotFound, http.StatusNotFound},
		"busy":             {domain.ErrDeletionBusy, http.StatusServiceUnavailable},
		"fixture":          {domain.ErrFixtureDeletionRequired, http.StatusPreconditionFailed},
		"no file storage":  {domain.ErrFileStorageUnavailable, http.StatusServiceUnavailable},
		"no vector store":  {domain.ErrVectorStorageUnavailable, http.StatusServiceUnavailable},
		"pending mutation": {domain.ErrLiveVectorMutation, http.StatusInternalServerError},
	} {
		t.Run(name, func(t *testing.T) {
			store := &guardedStore{checkpointedStore: &checkpointedStore{}, guardResult: tc.err}
			rec := serveDeletion(t, store, http.Header{}, WithLegacyReservations())
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Empty(t, store.deletes, "a legacy outcome never reaches the coordinator")
		})
	}
}

func TestIDOnlyDeleteThatLosesTheActivationRaceGetsTheActivatedContract(t *testing.T) {
	// Without a qualifier a live document is refused, never deleted by ID alone.
	store := &guardedStore{checkpointedStore: &checkpointedStore{active: true, result: domain.ErrDeletionPreconditionRequired}, guardResult: domain.ErrDeletionActivated}
	rec := serveDeletion(t, store, http.Header{}, WithLegacyReservations())
	require.Equal(t, http.StatusPreconditionRequired, rec.Code, rec.Body.String())
	require.Equal(t, 1, store.guarded)
	require.Len(t, store.deletes, 1)
	require.Empty(t, store.deletes[0].ExpectedIncarnation)

	// An absent identifier still reports absence.
	store = &guardedStore{checkpointedStore: &checkpointedStore{active: true, result: domain.ErrDocumentNotFound}, guardResult: domain.ErrDeletionActivated}
	require.Equal(t, http.StatusNotFound, serveDeletion(t, store, http.Header{}, WithLegacyReservations()).Code)
}

func TestQualifiedDeleteNeverTakesTheLegacyGuard(t *testing.T) {
	header := http.Header{"X-Document-Incarnation": {uuid.NewString()}}
	store := &guardedStore{checkpointedStore: &checkpointedStore{active: true}}
	require.Equal(t, http.StatusOK, serveDeletion(t, store, header, WithLegacyReservations()).Code)
	require.Zero(t, store.guarded)
	require.Len(t, store.deletes, 1)

	// Before activation a qualifier fails closed instead of becoming an ID-only request.
	store = &guardedStore{checkpointedStore: &checkpointedStore{}}
	require.Equal(t, http.StatusPreconditionFailed, serveDeletion(t, store, header, WithLegacyReservations()).Code)
	require.Zero(t, store.guarded)
	require.Zero(t, store.legacy)
	require.Empty(t, store.deletes)
}

func TestMalformedQualifierNeverBecomesAnIDOnlyRequest(t *testing.T) {
	for name, header := range map[string]http.Header{
		"empty":    {"X-Document-Incarnation": {""}},
		"invalid":  {"X-Document-Incarnation": {"latest"}},
		"repeated": {"X-Document-Incarnation": {uuid.NewString(), uuid.NewString()}},
	} {
		for _, active := range []bool{false, true} {
			t.Run(name, func(t *testing.T) {
				store := &guardedStore{checkpointedStore: &checkpointedStore{active: active}}
				rec := serveDeletion(t, store, header, WithLegacyReservations())
				want := http.StatusPreconditionFailed
				if active {
					want = http.StatusBadRequest
				}
				require.Equal(t, want, rec.Code, rec.Body.String())
				require.Zero(t, store.guarded)
				require.Zero(t, store.legacy)
				require.Empty(t, store.deletes)
			})
		}
	}
}
