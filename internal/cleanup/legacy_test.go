package cleanup

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// journal records the order of every store and provider call of one cleanup.
type journal struct {
	mu    sync.Mutex
	calls []string
}

func (j *journal) add(call string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.calls = append(j.calls, call)
}

func (j *journal) all() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.calls)
}

type legacyStore struct {
	log       *journal
	target    domain.LegacyDeletionTarget
	spec      domain.LegacyDeletionSpec
	prepare   error
	retire    error
	complete  error
	uncertain bool
	completed bool
}

func (s *legacyStore) PrepareLegacyDeletion(_ context.Context, _ domain.DocumentID, spec domain.LegacyDeletionSpec) (domain.LegacyDeletionTarget, error) {
	s.log.add("prepare")
	s.spec = spec
	return s.target, s.prepare
}

func (s *legacyStore) NoteLegacyUncertain(context.Context, domain.DocumentID, domain.LegacyDeletionTarget) error {
	s.log.add("note")
	return nil
}

func (s *legacyStore) RetireLegacyDocument(context.Context, domain.DocumentID, domain.LegacyDeletionTarget) error {
	s.log.add("retire")
	return s.retire
}

func (s *legacyStore) CompleteLegacyDeletion(_ context.Context, _ domain.DocumentID, _ domain.LegacyDeletionTarget, uncertain bool) error {
	s.log.add("complete")
	s.uncertain, s.completed = uncertain, s.complete == nil
	return s.complete
}

func (s *legacyStore) AbandonLegacyDeletion(context.Context, domain.DocumentID, domain.LegacyDeletionTarget) {
	s.log.add("abandon")
}

type legacyGuard struct {
	store     *legacyStore
	activated bool
	lockedID  domain.DocumentID
}

func (g *legacyGuard) WithLegacyDeletion(_ context.Context, id domain.DocumentID, remove func(domain.LegacyDocumentStore) error) error {
	g.lockedID = id
	if g.activated {
		return domain.ErrDeletionActivated
	}
	return remove(g.store)
}

type legacyVectorsDouble struct {
	log       *journal
	namespace func(context.Context) error
	byDoc     func(context.Context) error
	revision  func(context.Context) error
}

func (v *legacyVectorsDouble) DeleteAllVectorsInNamespace(ctx context.Context, _ string) error {
	v.log.add("namespace")
	if v.namespace != nil {
		return v.namespace(ctx)
	}
	return nil
}

func (v *legacyVectorsDouble) DeleteByDocID(ctx context.Context, _ string) error {
	v.log.add("vectors")
	if v.byDoc != nil {
		return v.byDoc(ctx)
	}
	return nil
}

func (v *legacyVectorsDouble) DeleteByRevision(ctx context.Context, _, _ string) error {
	v.log.add("revision")
	if v.revision != nil {
		return v.revision(ctx)
	}
	return nil
}

type journalObjects struct{ log *journal }

func (o journalObjects) Delete(_ context.Context, key string) error {
	o.log.add("source:" + key)
	return nil
}

func newLegacy(target domain.LegacyDeletionTarget) (*journal, *legacyStore, *legacyGuard) {
	log := new(journal)
	store := &legacyStore{log: log, target: target}
	return log, store, &legacyGuard{store: store}
}

func TestLegacyCleanupReservesBeforeTheFirstCallAndCompletesAfterTheLast(t *testing.T) {
	key := "documents/original.pdf"
	log, store, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString(), SourceKey: &key, Claim: uuid.NewString()})
	vectors := &legacyVectorsDouble{log: log}
	targets := Targets{SourceBucket: "originals", VectorIndex: "index"}

	require.NoError(t, RemoveLegacy(t.Context(), guard, vectors, journalObjects{log}, targets, "00000000-0000-4000-8000-0000000000AB"))

	require.Equal(t, []string{"prepare", "namespace", "vectors", "source:" + key, "retire", "vectors", "complete"}, log.all())
	require.Equal(t, domain.DocumentID("00000000-0000-4000-8000-0000000000ab"), guard.lockedID, "stores key the document by its canonical form")
	require.True(t, store.spec.Hosted)
	require.False(t, store.spec.Memory)
	require.Equal(t, "index", store.spec.VectorIndex)
	require.Equal(t, "originals", store.spec.SourceBucket)
	require.True(t, store.spec.SourceAvailable)
	require.True(t, store.completed)
	require.False(t, store.uncertain, "a clean pass records no uncertainty")
}

func TestLegacyCleanupRejectsNonUUIDWithoutTouchingAnything(t *testing.T) {
	log, _, guard := newLegacy(domain.LegacyDeletionTarget{})
	err := RemoveLegacy(t.Context(), guard, &legacyVectorsDouble{log: log}, nil, Targets{}, "not-a-uuid")
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)
	require.Empty(t, log.all())
}

func TestLegacyCleanupReturnsActivationWithoutActing(t *testing.T) {
	log, _, guard := newLegacy(domain.LegacyDeletionTarget{})
	guard.activated = true
	err := RemoveLegacy(t.Context(), guard, &legacyVectorsDouble{log: log}, nil, Targets{}, domain.DocumentID(uuid.NewString()))
	require.ErrorIs(t, err, domain.ErrDeletionActivated)
	require.Empty(t, log.all())
}

func TestLegacyCleanupStopsBeforeAnyCallWhenPreparationFails(t *testing.T) {
	for _, failure := range []error{domain.ErrDocumentNotFound, domain.ErrDeletionBusy, domain.ErrLiveVectorMutation, domain.ErrFileStorageUnavailable} {
		t.Run(failure.Error(), func(t *testing.T) {
			log, store, guard := newLegacy(domain.LegacyDeletionTarget{})
			store.prepare = failure
			err := RemoveLegacy(t.Context(), guard, &legacyVectorsDouble{log: log}, journalObjects{log}, Targets{}, domain.DocumentID(uuid.NewString()))
			require.ErrorIs(t, err, failure)
			require.Equal(t, []string{"prepare"}, log.all(), "no provider or object call precedes the reservation")
		})
	}
}

func TestLegacyCleanupKeepsTheReservationForARetryAfterAFailedCall(t *testing.T) {
	log, store, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString(), Claim: uuid.NewString()})
	boom := errors.New("provider rejected the request")
	vectors := &legacyVectorsDouble{log: log, byDoc: func(context.Context) error { return domain.RejectedVectorMutation(boom) }}

	err := RemoveLegacy(t.Context(), guard, vectors, nil, Targets{}, domain.DocumentID(uuid.NewString()))

	require.ErrorIs(t, err, boom)
	require.Equal(t, []string{"prepare", "namespace", "vectors", "abandon"}, log.all(), "the row stays and the claim is released for a retry")
	require.False(t, store.completed)
}

func TestLegacyCleanupRecordsAnUncertainFailure(t *testing.T) {
	log, store, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString(), Claim: uuid.NewString()})
	vectors := &legacyVectorsDouble{log: log, byDoc: func(context.Context) error { return status.Error(codes.DeadlineExceeded, "timeout") }}

	err := RemoveLegacy(t.Context(), guard, vectors, nil, Targets{}, domain.DocumentID(uuid.NewString()))

	require.Error(t, err)
	require.Equal(t, []string{"prepare", "namespace", "vectors", "note", "abandon"}, log.all())
	require.False(t, store.completed)
}

func TestLegacyCleanupHoldsAfterATimeoutThatARetryHid(t *testing.T) {
	log, store, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString(), Claim: uuid.NewString()})
	vectors := &legacyVectorsDouble{log: log, byDoc: func(ctx context.Context) error {
		// The adapter retried internally: the first attempt timed out, the second succeeded.
		domain.NoteVectorAttempt(ctx, status.Error(codes.DeadlineExceeded, "first attempt"))
		return nil
	}}

	require.NoError(t, RemoveLegacy(t.Context(), guard, vectors, nil, Targets{}, domain.DocumentID(uuid.NewString())))

	require.True(t, store.completed)
	require.True(t, store.uncertain, "an acknowledgment after a hidden timeout must not clear the hold")
	require.Contains(t, log.all(), "note", "the uncertainty is durable before the next step")
}

func TestLegacyCleanupOfScopedVectorsIgnoresAnAttemptTheProviderRejected(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.Unimplemented, codes.FailedPrecondition, codes.Unauthenticated, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			log, store, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString(), ScopedVectors: true, Claim: uuid.NewString()})
			rejected := domain.RejectedVectorMutation(status.Error(code, "answered before any effect"))
			vectors := &legacyVectorsDouble{log: log, revision: func(ctx context.Context) error {
				// The adapter classified the first attempt and a later one succeeded.
				domain.NoteVectorAttempt(ctx, rejected)
				return nil
			}}

			require.NoError(t, RemoveLegacy(t.Context(), guard, vectors, nil, Targets{}, domain.DocumentID(uuid.NewString())))

			require.True(t, store.completed)
			require.False(t, store.uncertain, "a clean scoped cleanup must release normally")
			require.NotContains(t, log.all(), "note")
		})
	}
}

func TestLegacyCleanupDoesNotNoteAFinalRejection(t *testing.T) {
	log, _, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString(), ScopedVectors: true, Claim: uuid.NewString()})
	vectors := &legacyVectorsDouble{log: log, revision: func(context.Context) error {
		return domain.RejectedVectorMutation(status.Error(codes.PermissionDenied, "key rotated"))
	}}

	err := RemoveLegacy(t.Context(), guard, vectors, nil, Targets{}, domain.DocumentID(uuid.NewString()))

	require.Error(t, err)
	require.Equal(t, []string{"prepare", "revision", "abandon"}, log.all(), "an answer that proves nothing ran records no uncertainty")
}

func TestLegacyCleanupDoesNotCompleteWhenTheRowCannotBeRetired(t *testing.T) {
	log, store, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString(), Claim: uuid.NewString()})
	store.retire = domain.ErrLiveVectorMutation
	err := RemoveLegacy(t.Context(), guard, &legacyVectorsDouble{log: log}, nil, Targets{}, domain.DocumentID(uuid.NewString()))
	require.ErrorIs(t, err, domain.ErrLiveVectorMutation)
	require.Equal(t, []string{"prepare", "namespace", "vectors", "retire", "abandon"}, log.all())
}

func TestLegacyCleanupReportsAFailureToComplete(t *testing.T) {
	log, store, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString(), Claim: uuid.NewString()})
	store.complete = errors.New("receipt update failed")
	err := RemoveLegacy(t.Context(), guard, &legacyVectorsDouble{log: log}, nil, Targets{}, domain.DocumentID(uuid.NewString()))
	require.ErrorIs(t, err, store.complete)
	require.Equal(t, "abandon", log.all()[len(log.all())-1])
}

func TestLegacyCleanupOfScopedVectorsUsesTheOriginalRevisionOnly(t *testing.T) {
	incarnation := uuid.NewString()
	log, _, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: incarnation, ScopedVectors: true, Claim: uuid.NewString()})
	require.NoError(t, RemoveLegacy(t.Context(), guard, &legacyVectorsDouble{log: log}, nil, Targets{}, domain.DocumentID(uuid.NewString())))
	require.Equal(t, []string{"prepare", "revision", "retire", "revision", "complete"}, log.all(), "tagged vectors never take the document-wide path")
}

type transactionalVectors struct{ *legacyVectorsDouble }

func (transactionalVectors) TransactionalVectors() {}

func TestLegacyCleanupOfTransactionalVectorsHasNoExternalCall(t *testing.T) {
	log, store, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString()})
	require.NoError(t, RemoveLegacy(t.Context(), guard, transactionalVectors{&legacyVectorsDouble{log: log}}, nil, Targets{}, domain.DocumentID(uuid.NewString())))
	require.Equal(t, []string{"prepare", "retire", "complete"}, log.all())
	require.False(t, store.spec.Hosted)
	require.False(t, store.spec.Memory)
}

func TestLegacyCleanupWithoutAVectorStoreOnlyRetires(t *testing.T) {
	log, _, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString()})
	require.NoError(t, RemoveLegacy(t.Context(), guard, nil, nil, Targets{}, domain.DocumentID(uuid.NewString())))
	require.Equal(t, []string{"prepare", "retire", "complete"}, log.all())
}

func TestLegacyCleanupOfMemoryVectorsRecordsTheOwnerAndSerializes(t *testing.T) {
	index := memory.NewFakeVectorStore()
	log, store, guard := newLegacy(domain.LegacyDeletionTarget{Incarnation: uuid.NewString(), Claim: uuid.NewString()})
	id := domain.DocumentID(uuid.NewString())
	targets := Targets{InMemory: true, Memory: index}
	var duplicate error
	vectors := &legacyVectorsDouble{log: log, byDoc: func(ctx context.Context) error {
		// A second cleanup of the same document must neither prepare nor start a mutation.
		other := &legacyGuard{store: &legacyStore{log: log}}
		duplicate = RemoveLegacy(ctx, other, &legacyVectorsDouble{log: log}, nil, targets, id)
		return nil
	}}

	require.NoError(t, RemoveLegacy(t.Context(), guard, vectors, nil, targets, id))

	require.ErrorIs(t, duplicate, domain.ErrDeletionBusy)
	require.True(t, store.spec.Memory)
	require.False(t, store.spec.Hosted)
	require.Equal(t, index.MemoryOwner(), store.spec.MemoryOwner)
	require.Equal(t, 1, countOf(log.all(), "prepare"), "the duplicate never reached the reservation")
	_, free := index.LockDocument(string(id))
	require.True(t, free, "the slot is released when the cleanup returns")
}

func TestLegacyCleanupOfMemoryVectorsNeedsTheOwningIndex(t *testing.T) {
	log, _, guard := newLegacy(domain.LegacyDeletionTarget{})
	err := RemoveLegacy(t.Context(), guard, &legacyVectorsDouble{log: log}, nil, Targets{InMemory: true}, domain.DocumentID(uuid.NewString()))
	require.ErrorIs(t, err, domain.ErrVectorStorageUnavailable)
	require.Empty(t, log.all())
}

func TestLegacyCleanupReleasesTheMemorySlotAfterAFailure(t *testing.T) {
	index := memory.NewFakeVectorStore()
	log, store, guard := newLegacy(domain.LegacyDeletionTarget{})
	store.prepare = domain.ErrDocumentNotFound
	id := domain.DocumentID(uuid.NewString())
	err := RemoveLegacy(t.Context(), guard, &legacyVectorsDouble{log: log}, nil, Targets{InMemory: true, Memory: index}, id)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)
	unlock, ok := index.LockDocument(string(id))
	require.True(t, ok)
	unlock()
}

func countOf(calls []string, call string) int {
	count := 0
	for _, c := range calls {
		if c == call {
			count++
		}
	}
	return count
}

func TestOperationsCarryTheMemoryOwner(t *testing.T) {
	index := memory.NewFakeVectorStore()
	ops := operations(index, nil, Targets{InMemory: true, Memory: index})
	require.Equal(t, index.MemoryOwner(), ops.MemoryOwner)
	require.Empty(t, operations(nil, nil, Targets{}).MemoryOwner)
}
