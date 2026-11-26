package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

var errTestStop = errors.New("stop after the fence")

var hostedSpec = domain.LegacyDeletionSpec{Hosted: true, RevisionVectors: true, SourceAvailable: true, SourceBucket: "originals", VectorIndex: "index"}

func legacyContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// legacySession runs fn inside the activation guard of one document.
func legacySession(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID, fn func(store *legacyDeletion)) error {
	t.Helper()
	return c.WithLegacyDeletion(ctx, id, func(store domain.LegacyDocumentStore) error {
		fn(store.(*legacyDeletion))
		return nil
	})
}

func legacyStatus(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID) string {
	t.Helper()
	var status string
	if err := c.Pool.QueryRow(ctx, `SELECT status FROM documents WHERE id=$1`, string(id)).Scan(&status); err != nil {
		return ""
	}
	return status
}

// admit tries to admit the identifier again with new content.
func admit(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID, content string) domain.AcceptOutcome {
	t.Helper()
	req := jobsRequest(content)
	req.Document.ID, req.CallerSupplied = id, true
	result, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	return result.Outcome
}

type legacyReceipt struct {
	incarnation string
	kind, mode  string
	index       *string
	bucket      *string
	source      *string
	owner       *string
	held        bool
	finalized   bool
	claim       *string
	claimLive   bool
}

func readLegacyReceipt(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID) legacyReceipt {
	t.Helper()
	var r legacyReceipt
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT incarnation::text,kind,vector_mode,vector_index,source_bucket,source_key,memory_owner::text,
		held,finalized,claim::text,COALESCE(claim_until>clock_timestamp(),FALSE) FROM document_deletions WHERE document_id=$1`, string(id)).
		Scan(&r.incarnation, &r.kind, &r.mode, &r.index, &r.bucket, &r.source, &r.owner, &r.held, &r.finalized, &r.claim, &r.claimLive))
	return r
}

func receiptCount(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID) int {
	t.Helper()
	var n int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM document_deletions WHERE document_id=$1`, string(id)).Scan(&n))
	return n
}

func TestLegacyReservationIsCommittedBeforeAnyExternalCall(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "legacy hosted document", false, true)

	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err)
		require.Equal(t, doc.incarnation, target.Incarnation)
		require.Equal(t, doc.key, *target.SourceKey)
		require.NotEmpty(t, target.Claim)

		// Another session sees the reservation although nothing external ran yet.
		receipt := readLegacyReceipt(ctx, t, c, doc.id)
		require.Equal(t, "legacy_reservation", receipt.kind)
		require.Equal(t, "legacy", receipt.mode)
		require.Equal(t, doc.incarnation, receipt.incarnation)
		require.Equal(t, "index", *receipt.index)
		require.Equal(t, "originals", *receipt.bucket)
		require.Equal(t, doc.key, *receipt.source)
		require.Nil(t, receipt.owner)
		require.True(t, receipt.held, "document-wide hosted effects of older writers stay held")
		require.False(t, receipt.finalized)
		require.True(t, receipt.claimLive)
		require.Equal(t, "processing", legacyStatus(ctx, t, c, doc.id), "a live reservation does not hide the document")
	}))
}

func TestReservationSurvivesRetirementUntilTheLastCallReturns(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "memory document", false, false)
	owner := uuid.NewString()
	spec := domain.LegacyDeletionSpec{Memory: true, MemoryOwner: owner}

	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, spec)
		require.NoError(t, err)
		require.NoError(t, store.RetireLegacyDocument(ctx, doc.id, target))
		require.Empty(t, legacyStatus(ctx, t, c, doc.id), "the row is gone")

		receipt := readLegacyReceipt(ctx, t, c, doc.id)
		require.False(t, receipt.finalized, "the final local effect has not returned")
		require.Equal(t, owner, *receipt.owner)
		require.False(t, receipt.held, "a clean memory cleanup needs no permanent hold")
		require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, doc.id, "replacement"), "admission honors the reservation")

		require.NoError(t, store.CompleteLegacyDeletion(ctx, doc.id, target, false))
	}))
	receipt := readLegacyReceipt(ctx, t, c, doc.id)
	require.True(t, receipt.finalized)
	require.False(t, receipt.held)
	require.Nil(t, receipt.claim)
	require.Nil(t, receipt.source, "a clean receipt drops the source key")
	require.Equal(t, domain.AcceptAccepted, admit(ctx, t, c, doc.id, "replacement"), "reuse succeeds immediately after a normal completion")
}

func TestUncertainAttemptKeepsTheIdentityHeldAfterCompletion(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "hidden timeout", false, false)
	spec := domain.LegacyDeletionSpec{Memory: true, MemoryOwner: uuid.NewString()}

	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, spec)
		require.NoError(t, err)
		require.NoError(t, store.NoteLegacyUncertain(ctx, doc.id, target))
		require.True(t, readLegacyReceipt(ctx, t, c, doc.id).held, "the note is durable at once")
		require.NoError(t, store.RetireLegacyDocument(ctx, doc.id, target))
		require.NoError(t, store.CompleteLegacyDeletion(ctx, doc.id, target, false))
	}))
	receipt := readLegacyReceipt(ctx, t, c, doc.id)
	require.True(t, receipt.finalized)
	require.True(t, receipt.held)
	require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, doc.id, "replacement"))

	other := acceptDocument(t, c, "uncertain at completion", false, false)
	require.NoError(t, legacySession(ctx, t, c, other.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, other.id, spec)
		require.NoError(t, err)
		require.NoError(t, store.RetireLegacyDocument(ctx, other.id, target))
		require.NoError(t, store.CompleteLegacyDeletion(ctx, other.id, target, true))
	}))
	require.True(t, readLegacyReceipt(ctx, t, c, other.id).held, "an acknowledgment after a hidden timeout does not clear the hold")
}

func TestDuplicateCleanupCannotResetALiveClaim(t *testing.T) {
	c := jobsDatabase(t)
	second := otherConnection(t, c)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "duplicate", false, false)
	spec := domain.LegacyDeletionSpec{Memory: true, MemoryOwner: uuid.NewString()}

	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		first, err := store.PrepareLegacyDeletion(ctx, doc.id, spec)
		require.NoError(t, err)
		before := readLegacyReceipt(ctx, t, c, doc.id)

		require.NoError(t, legacySession(ctx, t, second, doc.id, func(duplicate *legacyDeletion) {
			_, err = duplicate.PrepareLegacyDeletion(ctx, doc.id, spec)
			require.ErrorIs(t, err, domain.ErrDeletionBusy)
		}))
		require.Equal(t, before, readLegacyReceipt(ctx, t, c, doc.id), "the live owner's claim is untouched")

		// A duplicate cannot complete another cleanup's reservation either.
		require.NoError(t, legacySession(ctx, t, second, doc.id, func(duplicate *legacyDeletion) {
			foreign := first
			foreign.Claim = uuid.NewString()
			require.ErrorIs(t, duplicate.CompleteLegacyDeletion(ctx, doc.id, foreign, false), domain.ErrDeletionBusy)
			require.ErrorIs(t, duplicate.RetireLegacyDocument(ctx, doc.id, foreign), domain.ErrLiveVectorMutation)
		}))
		require.Equal(t, "processing", legacyStatus(ctx, t, c, doc.id), "a foreign claim cannot retire the row")
		require.False(t, readLegacyReceipt(ctx, t, c, doc.id).finalized)
	}))
}

func TestAbandonedReservationIsResumedByTheSameOwnerOnly(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "retry", false, true)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err)
		require.NoError(t, store.NoteLegacyUncertain(ctx, doc.id, target))
		store.AbandonLegacyDeletion(ctx, doc.id, target)
	}))
	require.Nil(t, readLegacyReceipt(ctx, t, c, doc.id).claim, "the released claim lets a retry resume")

	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err)
		require.True(t, readLegacyReceipt(ctx, t, c, doc.id).held, "the retry inherits the earlier uncertainty")
		store.AbandonLegacyDeletion(ctx, doc.id, target)
	}))

	owner := uuid.NewString()
	memory := domain.LegacyDeletionSpec{Memory: true, MemoryOwner: owner}
	mem := acceptDocument(t, c, "memory retry", false, false)
	require.NoError(t, legacySession(ctx, t, c, mem.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, mem.id, memory)
		require.NoError(t, err)
		store.AbandonLegacyDeletion(ctx, mem.id, target)
	}))
	require.NoError(t, legacySession(ctx, t, c, mem.id, func(store *legacyDeletion) {
		_, err := store.PrepareLegacyDeletion(ctx, mem.id, domain.LegacyDeletionSpec{Memory: true, MemoryOwner: uuid.NewString()})
		require.ErrorIs(t, err, domain.ErrDeletionBusy, "another index cannot resume a reservation it does not own")
		_, err = store.PrepareLegacyDeletion(ctx, mem.id, memory)
		require.NoError(t, err)
	}))
}

func TestLegacyResumeKeepsTheRecordedTarget(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "moved stores", false, true)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err)
		store.AbandonLegacyDeletion(ctx, doc.id, target)
	}))
	for name, spec := range map[string]domain.LegacyDeletionSpec{
		"another index":        {Hosted: true, RevisionVectors: true, SourceAvailable: true, SourceBucket: "originals", VectorIndex: "other-index"},
		"another bucket":       {Hosted: true, RevisionVectors: true, SourceAvailable: true, SourceBucket: "other-bucket", VectorIndex: "index"},
		"no bucket":            {Hosted: true, RevisionVectors: true, SourceAvailable: true, VectorIndex: "index"},
		"transactional vector": {SourceAvailable: true, SourceBucket: "originals"},
		"in-memory vectors":    {Memory: true, MemoryOwner: uuid.NewString(), SourceAvailable: true, SourceBucket: "originals"},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
				_, err := store.PrepareLegacyDeletion(ctx, doc.id, spec)
				require.ErrorIs(t, err, domain.ErrDeletionTargetMismatch)
			}))
			receipt := readLegacyReceipt(ctx, t, c, doc.id)
			require.Equal(t, "legacy", receipt.mode)
			require.Equal(t, "index", *receipt.index)
			require.Equal(t, "originals", *receipt.bucket)
			require.Equal(t, doc.key, *receipt.source)
			require.Nil(t, receipt.owner)
			require.Nil(t, receipt.claim, "a refused resume takes no claim")
			require.Equal(t, "processing", legacyStatus(ctx, t, c, doc.id))
		})
	}
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err, "the recorded target still resumes")
		require.NotEmpty(t, target.Claim)
		store.AbandonLegacyDeletion(ctx, doc.id, target)
	}))
}

func TestLegacyResumeOfAMemoryReservationKeepsItsBackend(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "memory with a file", false, true)
	owner := uuid.NewString()
	memory := domain.LegacyDeletionSpec{Memory: true, MemoryOwner: owner, SourceAvailable: true, SourceBucket: "originals"}
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, memory)
		require.NoError(t, err)
		store.AbandonLegacyDeletion(ctx, doc.id, target)
	}))
	for name, spec := range map[string]domain.LegacyDeletionSpec{
		"hosted index":   {Hosted: true, RevisionVectors: true, SourceAvailable: true, SourceBucket: "originals", VectorIndex: "index"},
		"transactional":  {SourceAvailable: true, SourceBucket: "originals"},
		"another bucket": {Memory: true, MemoryOwner: owner, SourceAvailable: true, SourceBucket: "other-bucket"},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
				_, err := store.PrepareLegacyDeletion(ctx, doc.id, spec)
				require.ErrorIs(t, err, domain.ErrDeletionTargetMismatch, "the request never rebinds the receipt")
			}))
			receipt := readLegacyReceipt(ctx, t, c, doc.id)
			require.Equal(t, "memory", receipt.mode)
			require.Equal(t, owner, *receipt.owner)
			require.Equal(t, "originals", *receipt.bucket)
			require.Nil(t, receipt.index)
			require.Equal(t, doc.key, *receipt.source)
		})
	}
}

func TestMemoryReservationOfScopedVectorsNeedsRevisionDeletion(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "tagged memory vectors", false, false)
	markTagged(ctx, t, c, doc.id)
	spec := domain.LegacyDeletionSpec{Memory: true, MemoryOwner: uuid.NewString()}
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		_, err := store.PrepareLegacyDeletion(ctx, doc.id, spec)
		require.ErrorIs(t, err, domain.ErrVectorStorageUnavailable)
		require.Zero(t, receiptCount(ctx, t, c, doc.id), "nothing is reserved for a cleanup that cannot run")

		spec.RevisionVectors = true
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, spec)
		require.NoError(t, err)
		require.True(t, target.ScopedVectors)
		require.Equal(t, "memory_revision", readLegacyReceipt(ctx, t, c, doc.id).mode)
	}))
}

func TestExpiredClaimLetsALaterCleanupTakeOver(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "expired claim", false, false)
	spec := domain.LegacyDeletionSpec{Memory: true, MemoryOwner: uuid.NewString()}
	var first domain.LegacyDeletionTarget
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		var err error
		first, err = store.PrepareLegacyDeletion(ctx, doc.id, spec)
		require.NoError(t, err)
	}))
	_, err := c.Pool.Exec(ctx, `UPDATE document_deletions SET claim_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(doc.id))
	require.NoError(t, err)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		second, prepareErr := store.PrepareLegacyDeletion(ctx, doc.id, spec)
		require.NoError(t, prepareErr)
		require.NotEqual(t, first.Claim, second.Claim)
		require.ErrorIs(t, store.CompleteLegacyDeletion(ctx, doc.id, first, false), domain.ErrDeletionBusy, "the earlier claim is fenced")
	}))
}

func TestPreparationSynchronizesWithWorkerMutations(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "worker is writing", false, false)
	job, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	entered, release := make(chan struct{}), make(chan struct{})
	written := make(chan error, 1)
	go func() {
		written <- c.mutateJobVectors(ctx, *job, false, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		_, prepareErr := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.ErrorIs(t, prepareErr, domain.ErrDeletionBusy, "a vector write that is still in flight must finish first")
	}))
	require.Zero(t, receiptCount(ctx, t, c, doc.id), "nothing is reserved while a worker may be writing")
	close(release)
	require.NoError(t, <-written)

	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		_, prepareErr := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, prepareErr)
	}))
}

func TestReservationStopsNewWorkerVectorWork(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "leased before reservation", false, false)
	job, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	queued := acceptDocument(t, c, "queued before reservation", false, false)

	for _, id := range []domain.DocumentID{doc.id, queued.id} {
		require.NoError(t, legacySession(ctx, t, c, id, func(store *legacyDeletion) {
			_, prepareErr := store.PrepareLegacyDeletion(ctx, id, hostedSpec)
			require.NoError(t, prepareErr)
		}))
	}
	var ran atomic.Bool
	err = c.mutateJobVectors(ctx, *job, false, func(context.Context) error { ran.Store(true); return nil })
	require.ErrorIs(t, err, domain.ErrJobNotOwned, "the live lease cannot start new broad work under a reservation")
	require.False(t, ran.Load())
	next, err := c.ClaimJob(ctx, "worker-2", time.Minute, 5)
	require.NoError(t, err)
	require.Nil(t, next, "a reserved document is not leased")
}

func TestPreparationTransfersWorkerUncertainty(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "uncertain worker effect", false, false)
	_, err := c.Pool.Exec(ctx, `UPDATE documents SET vector_mutation_uncertain=TRUE WHERE id=$1`, string(doc.id))
	require.NoError(t, err)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		_, prepareErr := store.PrepareLegacyDeletion(ctx, doc.id, domain.LegacyDeletionSpec{Memory: true, MemoryOwner: uuid.NewString()})
		require.NoError(t, prepareErr)
		require.True(t, readLegacyReceipt(ctx, t, c, doc.id).held, "the receipt carries what the live row knew")
	}))
}

func TestPreparationSettlesAnExpiredMutationOfAFailedDocumentOnly(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	spec := domain.LegacyDeletionSpec{Memory: true, MemoryOwner: uuid.NewString()}
	for status, want := range map[string]error{"failed": nil, "processing": domain.ErrLiveVectorMutation} {
		doc := acceptDocument(t, c, "expired mutation "+status, false, false)
		_, err := c.Pool.Exec(ctx, `UPDATE documents SET status=$2,vector_mutation_pending=TRUE,vector_mutation_until=clock_timestamp()-interval '1 minute' WHERE id=$1`, string(doc.id), status)
		require.NoError(t, err)
		require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
			_, prepareErr := store.PrepareLegacyDeletion(ctx, doc.id, spec)
			if want != nil {
				require.ErrorIs(t, prepareErr, want)
				require.Zero(t, receiptCount(ctx, t, c, doc.id))
				return
			}
			require.NoError(t, prepareErr)
			require.True(t, readLegacyReceipt(ctx, t, c, doc.id).held, "an abandoned write is uncertain")
		}))
	}
}

func TestPreparationRefusesWithoutFileStorageAndCreatesNoReservation(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "has a source", false, true)
	spec := hostedSpec
	spec.SourceAvailable = false
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		_, err := store.PrepareLegacyDeletion(ctx, doc.id, spec)
		require.ErrorIs(t, err, domain.ErrFileStorageUnavailable)
	}))
	require.Zero(t, receiptCount(ctx, t, c, doc.id))
}

func TestPreparationOfScopedVectorsNeedsRevisionDeletion(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "tagged vectors", false, false)
	markTagged(ctx, t, c, doc.id)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		spec := hostedSpec
		spec.RevisionVectors = false
		_, prepareErr := store.PrepareLegacyDeletion(ctx, doc.id, spec)
		require.ErrorIs(t, prepareErr, domain.ErrVectorStorageUnavailable)
		require.Zero(t, receiptCount(ctx, t, c, doc.id))

		target, prepareErr := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, prepareErr)
		require.True(t, target.ScopedVectors)
		receipt := readLegacyReceipt(ctx, t, c, doc.id)
		require.Equal(t, "revision", receipt.mode)
		require.False(t, receipt.held, "a clean scoped target permits reuse")
	}))
}

func TestTransactionalVectorsNeedNoReservation(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "pgvector document", false, false)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, domain.LegacyDeletionSpec{})
		require.NoError(t, err)
		require.Empty(t, target.Claim)
		require.NoError(t, store.RetireLegacyDocument(ctx, doc.id, target))
		require.NoError(t, store.CompleteLegacyDeletion(ctx, doc.id, target, false))
		require.NoError(t, store.NoteLegacyUncertain(ctx, doc.id, target))
		store.AbandonLegacyDeletion(ctx, doc.id, target)
	}))
	require.Zero(t, receiptCount(ctx, t, c, doc.id))
	require.Equal(t, domain.AcceptAccepted, admit(ctx, t, c, doc.id, "replacement"))
}

func TestAbsentDocumentReservesNothing(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, legacySession(ctx, t, c, id, func(store *legacyDeletion) {
		_, err := store.PrepareLegacyDeletion(ctx, id, hostedSpec)
		require.ErrorIs(t, err, domain.ErrDocumentNotFound)
	}))
	require.Zero(t, receiptCount(ctx, t, c, id))
}

func TestGuardReturnsActivationWithoutCallingRemove(t *testing.T) {
	c := activatedDatabase(t)
	ctx := legacyContext(t)
	called := false
	err := c.WithLegacyDeletion(ctx, domain.DocumentID(uuid.NewString()), func(domain.LegacyDocumentStore) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, domain.ErrDeletionActivated)
	require.False(t, called)
	require.True(t, lockIsFree(ctx, t, c), "the lock is released when the guard declines")
}

func lockIsFree(ctx context.Context, t *testing.T, c *Client) bool {
	t.Helper()
	tx, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var free bool
	require.NoError(t, tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, DeletionActivationLockID).Scan(&free))
	return free
}

func TestActivationWaitsForAHealthyCleanup(t *testing.T) {
	c := jobsDatabase(t)
	activator := otherConnection(t, c)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "drain before activation", false, false)
	activated := make(chan error, 1)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err)
		// The row is retired and the last external call has not returned. Admission
		// of a live row answers by its status, so only a retired row shows the hold.
		require.NoError(t, store.RetireLegacyDocument(ctx, doc.id, target))
		go func() {
			tx, beginErr := activator.Pool.Begin(ctx)
			if beginErr != nil {
				activated <- beginErr
				return
			}
			defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
			if _, lockErr := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, DeletionActivationLockID); lockErr != nil {
				activated <- lockErr
				return
			}
			if _, updateErr := tx.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`); updateErr != nil {
				activated <- updateErr
				return
			}
			activated <- tx.Commit(ctx)
		}()
		select {
		case err = <-activated:
			t.Fatalf("activation committed under a healthy cleanup: %v", err)
		case <-time.After(750 * time.Millisecond):
		}
	}))
	require.NoError(t, <-activated, "activation proceeds once the cleanup has returned")
	active, err := c.DeletionActive(ctx)
	require.NoError(t, err)
	require.True(t, active)
	require.False(t, readLegacyReceipt(ctx, t, c, doc.id).finalized)
	require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, doc.id, "replacement"), "the reservation outlives the lock")
}

func TestLostSessionReleasesTheLockButNotTheReservation(t *testing.T) {
	c := jobsDatabase(t)
	killer := otherConnection(t, c)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "session loss", false, false)
	owner := uuid.NewString()

	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, domain.LegacyDeletionSpec{Memory: true, MemoryOwner: owner})
		require.NoError(t, err)
		require.NoError(t, store.RetireLegacyDocument(ctx, doc.id, target))

		pid := store.conn.PgConn().PID()
		var terminated bool
		require.NoError(t, killer.Pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, int32(pid)).Scan(&terminated)) // #nosec G115 -- PostgreSQL process IDs are small.
		require.True(t, terminated)
		require.Eventually(t, func() bool { return lockIsFree(ctx, t, killer) }, 10*time.Second, 25*time.Millisecond, "the session lock ended with the session")

		// Activation can now commit although the last external call has not returned.
		_, err = killer.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, killer, doc.id, "replacement"), "the durable reservation still blocks the identity")
		require.False(t, readLegacyReceipt(ctx, t, killer, doc.id).finalized)

		// The owner records the end of its last effect on a fresh connection.
		require.NoError(t, store.CompleteLegacyDeletion(ctx, doc.id, target, false))
	}))
	receipt := readLegacyReceipt(ctx, t, c, doc.id)
	require.True(t, receipt.finalized)
	require.False(t, receipt.held)
	require.Equal(t, domain.AcceptAccepted, admit(ctx, t, c, doc.id, "replacement"))
	require.True(t, lockIsFree(ctx, t, c))
}

// holdReceiptLock blocks every update of one receipt until release is called,
// the way a failover or a lock queue stalls a statement without resetting it.
func holdReceiptLock(ctx context.Context, t *testing.T, other *Client, id domain.DocumentID) (release func()) {
	t.Helper()
	tx, err := other.Pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `SELECT 1 FROM document_deletions WHERE document_id=$1 FOR UPDATE`, string(id))
	require.NoError(t, err)
	released := false
	t.Cleanup(func() {
		if !released {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	})
	return func() {
		released = true
		require.NoError(t, tx.Commit(ctx))
	}
}

// TestBookkeepingFallbackGetsItsOwnBudgetAfterAHungSession proves that an
// update stalled past the guarded session's budget still completes on the
// fresh connection, instead of inheriting the spent deadline.
func TestBookkeepingFallbackGetsItsOwnBudgetAfterAHungSession(t *testing.T) {
	for name, record := range map[string]struct {
		call  func(context.Context, *legacyDeletion, domain.DocumentID, domain.LegacyDeletionTarget) error
		check func(*testing.T, legacyReceipt)
	}{
		"completion": {
			call: func(ctx context.Context, store *legacyDeletion, id domain.DocumentID, target domain.LegacyDeletionTarget) error {
				return store.CompleteLegacyDeletion(ctx, id, target, false)
			},
			check: func(t *testing.T, r legacyReceipt) {
				require.True(t, r.finalized)
				require.False(t, r.held, "a clean memory cleanup releases normally")
			},
		},
		"uncertainty": {
			call: func(ctx context.Context, store *legacyDeletion, id domain.DocumentID, target domain.LegacyDeletionTarget) error {
				return store.NoteLegacyUncertain(ctx, id, target)
			},
			check: func(t *testing.T, r legacyReceipt) {
				require.False(t, r.finalized)
				require.True(t, r.held, "the note is durable once the lock frees")
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := jobsDatabase(t)
			other := otherConnection(t, c)
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			doc := acceptDocument(t, c, "hung session "+name, false, false)

			require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
				target, err := store.PrepareLegacyDeletion(ctx, doc.id, domain.LegacyDeletionSpec{Memory: true, MemoryOwner: uuid.NewString()})
				require.NoError(t, err)
				require.NoError(t, store.RetireLegacyDocument(ctx, doc.id, target))

				release := holdReceiptLock(ctx, t, other, doc.id)
				done := make(chan error, 1)
				started := time.Now()
				go func() { done <- record.call(ctx, store, doc.id, target) }()
				time.Sleep(legacyBookkeepingLimit + time.Second)
				release()

				require.NoError(t, <-done, "the fresh connection waits for the lock with a budget of its own")
				require.Greater(t, time.Since(started), legacyBookkeepingLimit, "the guarded session spent its whole budget first")
			}))
			record.check(t, readLegacyReceipt(ctx, t, c, doc.id))
		})
	}
}

func TestQualifiedRetryConvertsTheReservationAndCommitsTheFence(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "first hosted call failed", false, true)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err)
		store.AbandonLegacyDeletion(ctx, doc.id, target)
	}))
	// The row is live and visible while only the reservation exists.
	require.Equal(t, "processing", legacyStatus(ctx, t, c, doc.id))
	signed := false
	found, err := c.WithDocumentFile(ctx, doc.id, func(_ context.Context, key string) error {
		signed = key == doc.key
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, signed, "a live legacy reservation can still sign its original file")

	_, err = c.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	recorder := newDeletionRecorder()
	ops := recorder.ops(doc.incarnation)
	ops.SourceBucket, ops.VectorIndex = "originals", "index"

	wrong := ops
	wrong.ExpectedIncarnation = uuid.NewString()
	require.ErrorIs(t, c.DeleteWithCleanup(ctx, doc.id, wrong), domain.ErrDeletionPreconditionFailed)
	require.Equal(t, "processing", legacyStatus(ctx, t, c, doc.id), "a wrong token changes nothing")
	require.Equal(t, "legacy_reservation", readLegacyReceipt(ctx, t, c, doc.id).kind)

	require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, ops))
	receipt := readLegacyReceipt(ctx, t, c, doc.id)
	require.Equal(t, "qualified_intent", receipt.kind)
	require.True(t, receipt.finalized)
	require.True(t, receipt.held, "the converted receipt keeps the hold")
	require.Equal(t, 1, recorder.count("namespace"))
	require.Equal(t, 1, recorder.count("source"))
}

func TestConversionCommitsTheVisibilityFenceBeforeAnyStep(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "fence on conversion", false, true)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err)
		store.AbandonLegacyDeletion(ctx, doc.id, target)
	}))
	_, err := c.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	recorder := newDeletionRecorder()
	recorder.hook("namespace", func(context.Context) error { return errTestStop })
	ops := recorder.ops(doc.incarnation)
	ops.SourceBucket, ops.VectorIndex = "originals", "index"

	require.ErrorIs(t, c.DeleteWithCleanup(ctx, doc.id, ops), errTestStop)
	require.Equal(t, "deleting", legacyStatus(ctx, t, c, doc.id), "the intent fenced the document at commit")
	require.Equal(t, "qualified_intent", readLegacyReceipt(ctx, t, c, doc.id).kind)
	found, err := c.WithDocumentFile(ctx, doc.id, func(context.Context, string) error { return nil })
	require.NoError(t, err)
	require.False(t, found, "the signer is silent after the fence")
	require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, doc.id, "conflicting admission"))
}

func TestConversionNeverResetsALiveCleanupClaim(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "live legacy cleanup", false, false)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		_, err := store.PrepareLegacyDeletion(ctx, doc.id, hostedSpec)
		require.NoError(t, err)
		before := readLegacyReceipt(ctx, t, c, doc.id)
		_, err = c.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
		require.NoError(t, err)
		recorder := newDeletionRecorder()
		ops := recorder.ops(doc.incarnation)
		ops.VectorIndex = "index"
		require.ErrorIs(t, c.DeleteWithCleanup(ctx, doc.id, ops), domain.ErrDeletionBusy)
		require.Equal(t, before, readLegacyReceipt(ctx, t, c, doc.id))
		require.Equal(t, "processing", legacyStatus(ctx, t, c, doc.id))
		require.Empty(t, recorder.all())
	}))
}

func TestConversionOfAMemoryReservationBelongsToItsOwner(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "memory conversion", false, false)
	owner := uuid.NewString()
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, domain.LegacyDeletionSpec{Memory: true, MemoryOwner: owner})
		require.NoError(t, err)
		store.AbandonLegacyDeletion(ctx, doc.id, target)
	}))
	_, err := c.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)

	recorder := newDeletionRecorder()
	foreign := recorder.ops(doc.incarnation)
	foreign.InMemory, foreign.MemoryOwner = true, uuid.NewString()
	require.ErrorIs(t, c.DeleteWithCleanup(ctx, doc.id, foreign), domain.ErrDeletionBusy, "an empty index elsewhere cannot complete another owner's deletion")
	require.Empty(t, recorder.all())
	require.Equal(t, "processing", legacyStatus(ctx, t, c, doc.id))

	own := recorder.ops(doc.incarnation)
	own.InMemory, own.MemoryOwner = true, owner
	require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, own))
	require.True(t, readLegacyReceipt(ctx, t, c, doc.id).finalized)
}

func TestRetireRequiresTheObservedIncarnation(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "stale target", false, false)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		stale := domain.LegacyDeletionTarget{Incarnation: uuid.NewString()}
		require.ErrorIs(t, store.RetireLegacyDocument(ctx, doc.id, stale), domain.ErrLiveVectorMutation)
		require.Equal(t, "processing", legacyStatus(ctx, t, c, doc.id))
	}))
}

func TestFinalizeRefusesWhileTheRowIsLive(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	doc := acceptDocument(t, c, "not retired", false, false)
	require.NoError(t, legacySession(ctx, t, c, doc.id, func(store *legacyDeletion) {
		target, err := store.PrepareLegacyDeletion(ctx, doc.id, domain.LegacyDeletionSpec{Memory: true, MemoryOwner: uuid.NewString()})
		require.NoError(t, err)
		require.ErrorIs(t, store.CompleteLegacyDeletion(ctx, doc.id, target, false), domain.ErrDeletionBusy)
		require.False(t, readLegacyReceipt(ctx, t, c, doc.id).finalized)
	}))
}

func TestDeletionActivationLockIsReleasedAfterEveryOutcome(t *testing.T) {
	c := jobsDatabase(t)
	ctx := legacyContext(t)
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, legacySession(ctx, t, c, id, func(*legacyDeletion) {}))
	require.True(t, lockIsFree(ctx, t, c))
	_ = c.WithLegacyDeletion(ctx, id, func(domain.LegacyDocumentStore) error { return domain.ErrDeletionBusy })
	require.True(t, lockIsFree(ctx, t, c))
	// The guarded session is not pooled, so a one-connection pool serves every call.
	single, err := NewClient(ctx, c.Pool.Config().ConnString(), func(cfg *pgxpool.Config) { cfg.MaxConns = 1 })
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, single.Close(context.Background())) })
	require.NoError(t, legacySession(ctx, t, single, id, func(*legacyDeletion) {}))
	require.NoError(t, legacySession(ctx, t, single, id, func(*legacyDeletion) {}))
}
