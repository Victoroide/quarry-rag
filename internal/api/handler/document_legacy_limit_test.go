package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// limitedStore adds the replica's cleanup bound and a writer session to the
// guarded double and records the order of every acquisition.
type limitedStore struct {
	*guardedStore
	mu         sync.Mutex
	events     []string
	reserveErr error
	capacity   int
	inUse      int
}

func (s *limitedStore) note(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *limitedStore) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

func (s *limitedStore) AcquireWriter(context.Context) (func(), error) {
	s.note("writer")
	return func() { s.note("writer-release") }, nil
}

func (s *limitedStore) ReserveLegacyDeletion(ctx context.Context) (context.Context, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reserveErr != nil || (s.capacity > 0 && s.inUse >= s.capacity) {
		s.events = append(s.events, "reserve-refused")
		if s.reserveErr != nil {
			return ctx, nil, s.reserveErr
		}
		return ctx, nil, domain.ErrTooManyLegacyDeletions
	}
	s.inUse++
	s.events = append(s.events, "reserve")
	var once sync.Once
	return ctx, func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.inUse--
			s.events = append(s.events, "slot-release")
		})
	}, nil
}

func (s *limitedStore) WithLegacyDeletion(ctx context.Context, id domain.DocumentID, fn func(domain.LegacyDocumentStore) error) error {
	s.note("legacy")
	return s.guardedStore.WithLegacyDeletion(ctx, id, fn)
}

func newLimitedStore(inner checkpointedStore) *limitedStore {
	return &limitedStore{guardedStore: &guardedStore{checkpointedStore: &inner}}
}

func TestGuardedIDOnlyDeleteRejectsBurstWith503BusyBeforeLocks(t *testing.T) {
	store := newLimitedStore(checkpointedStore{})
	store.reserveErr = domain.ErrTooManyLegacyDeletions
	rec := serveDeletion(t, store, http.Header{}, WithLegacyReservations())
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Equal(t, "5", rec.Header().Get("Retry-After"))
	var body struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "busy", body.Error)
	require.Contains(t, body.Message, "too many concurrent deletions")
	require.Equal(t, []string{"reserve-refused"}, store.recorded(), "no writer session, guard, lock or lookup follows the refusal")
	require.Zero(t, store.guarded)
	require.Zero(t, store.legacy)
	require.Zero(t, store.modeReads)
	require.Empty(t, store.deletes)
}

func TestGuardedIDOnlyDeleteHoldsItsSlotOnlyAroundTheGuard(t *testing.T) {
	store := newLimitedStore(checkpointedStore{})
	rec := serveDeletion(t, store, http.Header{}, WithLegacyReservations())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, []string{"reserve", "writer", "legacy", "slot-release", "writer-release"}, store.recorded())
	require.Zero(t, store.inUse)
}

func TestGuardedIDOnlyDeleteReleasesItsSlotOnEveryOutcome(t *testing.T) {
	for name, tc := range map[string]struct {
		inner  checkpointedStore
		guard  error
		status int
	}{
		"completed":      {status: http.StatusOK},
		"cleanup failed": {guard: domain.ErrVectorStorageUnavailable, status: http.StatusServiceUnavailable},
		"lost the activation race": {
			inner:  checkpointedStore{active: true, result: domain.ErrDeletionPreconditionRequired},
			guard:  domain.ErrDeletionActivated,
			status: http.StatusPreconditionRequired,
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newLimitedStore(tc.inner)
			store.guardResult = tc.guard
			rec := serveDeletion(t, store, http.Header{}, WithLegacyReservations())
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Zero(t, store.inUse)
			events := store.recorded()
			require.Equal(t, 1, count(events, "reserve"))
			require.Equal(t, 1, count(events, "slot-release"))
			if errors.Is(tc.guard, domain.ErrDeletionActivated) {
				require.Less(t, indexOf(events, "slot-release"), indexOf(events, "writer-release"), "the activated contract does not hold a cleanup slot")
				require.Len(t, store.deletes, 1)
			}
		})
	}
}

func TestQualifiedDeleteBypassesLegacyConcurrencyBound(t *testing.T) {
	store := newLimitedStore(checkpointedStore{active: true})
	store.reserveErr = domain.ErrTooManyLegacyDeletions
	header := http.Header{"X-Document-Incarnation": {uuid.NewString()}}
	rec := serveDeletion(t, store, header, WithLegacyReservations())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Zero(t, count(store.recorded(), "reserve-refused"))
	require.Len(t, store.deletes, 1)
}

func TestIDOnlyDeleteWithoutReservationsNeverReservesASlot(t *testing.T) {
	store := newLimitedStore(checkpointedStore{})
	store.reserveErr = domain.ErrTooManyLegacyDeletions
	rec := serveDeletion(t, store, http.Header{})
	require.Equal(t, http.StatusNotFound, rec.Code, "the default path is unchanged")
	require.Zero(t, count(store.recorded(), "reserve-refused"))
}

func TestGuardedDeletionsOverTheBoundAreRefusedWhileOthersRun(t *testing.T) {
	store := newLimitedStore(checkpointedStore{})
	store.capacity = 1
	gate := make(chan struct{})
	entered := make(chan struct{})
	running := &blockingGuard{limitedStore: store, entered: entered, gate: gate}
	done := make(chan int, 1)
	go func() { done <- serveDeletion(t, running, http.Header{}, WithLegacyReservations()).Code }()
	<-entered

	rec := serveDeletion(t, running, http.Header{}, WithLegacyReservations())
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Equal(t, "5", rec.Header().Get("Retry-After"))
	require.Equal(t, 1, count(store.recorded(), "writer"), "the refused request opened no writer session")

	close(gate)
	require.Equal(t, http.StatusOK, <-done)
	require.Equal(t, http.StatusOK, serveDeletion(t, running, http.Header{}, WithLegacyReservations()).Code, "the slot is free again")
}

// blockingGuard holds the first guarded call until gate closes.
type blockingGuard struct {
	*limitedStore
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (b *blockingGuard) WithLegacyDeletion(ctx context.Context, id domain.DocumentID, fn func(domain.LegacyDocumentStore) error) error {
	blocked := false
	b.once.Do(func() { blocked = true })
	if blocked {
		close(b.entered)
		<-b.gate
	}
	return b.limitedStore.WithLegacyDeletion(ctx, id, fn)
}

func count(events []string, want string) int {
	n := 0
	for _, event := range events {
		if event == want {
			n++
		}
	}
	return n
}

func indexOf(events []string, want string) int {
	for i, event := range events {
		if event == want {
			return i
		}
	}
	return -1
}
