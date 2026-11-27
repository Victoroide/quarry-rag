package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestHeldPhaseClassification(t *testing.T) {
	require.Equal(t, "released", heldPhase("qualified_intent", true, false, stepDone))
	require.Equal(t, "quarantined", heldPhase("quarantine", true, true, stepDone))
	require.Equal(t, "held", heldPhase("legacy_reservation", true, true, stepDone))
	require.Equal(t, "reserved", heldPhase("legacy_reservation", false, true, stepNamespace))
	require.Equal(t, "namespace", heldPhase("qualified_intent", false, false, stepNamespace))
	require.Equal(t, "vectors", heldPhase("legacy_reservation", false, false, stepVectors))
	require.Equal(t, "source", heldPhase("legacy_reservation", false, false, stepSource))
	require.Equal(t, "finalize", heldPhase("legacy_reservation", false, true, stepDone))
}

func TestLegacyHostedReleaseEligibility(t *testing.T) {
	complete := releaseReceipt{kind: "quarantine", mode: legacyVectors, step: stepDone, held: true, finalized: true}
	require.NoError(t, complete.checkEligibility(true))
	complete.step = stepSource
	require.ErrorIs(t, complete.checkEligibility(true), domain.ErrDeletionPreconditionFailed)
	reserved := releaseReceipt{kind: "legacy_reservation", mode: legacyVectors, step: stepNamespace}
	require.NoError(t, reserved.checkEligibility(true))
	reserved.claimValid = true
	require.ErrorIs(t, reserved.checkEligibility(true), domain.ErrDeletionBusy)
	complete.step = stepDone
	complete.mode = revisionVectors
	require.NoError(t, complete.checkEligibility(false))
	require.ErrorIs(t, complete.checkEligibility(true), domain.ErrDeletionPreconditionFailed)
}

type refusedReleaseBegin struct{ heldReleaseDatabase }

func (refusedReleaseBegin) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errors.New("finalization attempted to begin a transaction")
}

func TestHeldReleaseFinalizeSkipsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	release := heldRelease{db: refusedReleaseBegin{}}
	require.ErrorIs(t, release.finalizeRelease(ctx, domain.DocumentID(uuid.NewString()), uuid.NewString(), uuid.NewString()), context.Canceled)
}

func TestCheckReleaseTargetRejectsMismatchedStores(t *testing.T) {
	bucket := "originals"
	otherBucket := "other-bucket"
	index := "prod-index"
	key := "source/doc/file.pdf"
	owner := uuid.NewString()
	noop := func(context.Context, string) error { return nil }
	noopRev := func(context.Context, string, string) error { return nil }

	t.Run("offline endpoint rejects non-memory mode", func(t *testing.T) {
		err := checkReleaseTarget(releaseReceipt{mode: legacyVectors}, domain.DeleteOperations{InMemory: true, MemoryOwner: owner})
		require.ErrorIs(t, err, domain.ErrDeletionPreconditionFailed)
	})
	t.Run("offline endpoint rejects wrong or missing owner", func(t *testing.T) {
		err := checkReleaseTarget(releaseReceipt{mode: memoryVectors, owner: &owner}, domain.DeleteOperations{
			InMemory: true, MemoryOwner: uuid.NewString(), LegacyNamespace: noop, LegacyVectors: noop,
		})
		require.ErrorIs(t, err, domain.ErrDeletionPreconditionFailed)
		err = checkReleaseTarget(releaseReceipt{mode: memoryVectors}, domain.DeleteOperations{
			InMemory: true, MemoryOwner: owner, LegacyNamespace: noop, LegacyVectors: noop,
		})
		require.ErrorIs(t, err, domain.ErrDeletionPreconditionFailed)
	})
	t.Run("administrative path rejects unconfirmed memory hold", func(t *testing.T) {
		err := checkReleaseTarget(releaseReceipt{mode: memoryVectors, owner: &owner}, domain.DeleteOperations{})
		require.ErrorIs(t, err, domain.ErrDeletionTargetMismatch)
		err = checkReleaseTarget(releaseReceipt{mode: memoryVectors, owner: &owner}, domain.DeleteOperations{
			ConfirmedOfflineOwnerTerminated: true, MemoryOwner: uuid.NewString(),
		})
		require.ErrorIs(t, err, domain.ErrDeletionTargetMismatch)
	})
	t.Run("administrative path accepts matching terminated memory owner", func(t *testing.T) {
		err := checkReleaseTarget(releaseReceipt{mode: memoryVectors, owner: &owner}, domain.DeleteOperations{
			ConfirmedOfflineOwnerTerminated: true, MemoryOwner: owner,
		})
		require.NoError(t, err)
	})
	t.Run("hosted index mismatch", func(t *testing.T) {
		err := checkReleaseTarget(releaseReceipt{mode: legacyVectors, index: &index}, domain.DeleteOperations{VectorIndex: "wrong"})
		require.ErrorIs(t, err, domain.ErrDeletionTargetMismatch)
	})
	t.Run("missing or mismatched source storage", func(t *testing.T) {
		err := checkReleaseTarget(releaseReceipt{mode: legacyVectors, source: &key, bucket: &bucket}, domain.DeleteOperations{})
		require.ErrorIs(t, err, domain.ErrFileStorageUnavailable)
		err = checkReleaseTarget(releaseReceipt{mode: legacyVectors, source: &key, bucket: &otherBucket}, domain.DeleteOperations{
			SourceBucket: bucket, Source: noop,
		})
		require.ErrorIs(t, err, domain.ErrDeletionTargetMismatch)
	})
	t.Run("revision mode needs revision deleter", func(t *testing.T) {
		err := checkReleaseTarget(releaseReceipt{mode: revisionVectors}, domain.DeleteOperations{})
		require.ErrorIs(t, err, domain.ErrVectorStorageUnavailable)
		err = checkReleaseTarget(releaseReceipt{mode: revisionVectors, index: &index}, domain.DeleteOperations{
			VectorIndex: index, RevisionVectors: noopRev,
		})
		require.NoError(t, err)
	})
}

func TestUncertainVectorMutationHoldsUUIDUntilReconciled(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	req := jobsRequest("uncertain write")
	req.CallerSupplied = true
	req.SupportsScopedVectors = true
	result, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	job, err := c.ClaimJob(ctx, "legacy-worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.ErrorIs(t, c.MutateJobVectors(ctx, *job, func(ctx context.Context) error {
		return domain.DispatchVectorMutation(ctx, false, func(context.Context) error {
			return context.DeadlineExceeded
		})
	}), context.DeadlineExceeded)

	var fail bool
	calls := 0
	ops := domain.DeleteOperations{
		ExpectedIncarnation: job.Incarnation,
		RevisionVectors: func(context.Context, string, string) error {
			calls++
			if fail {
				return errors.New("provider unavailable")
			}
			return nil
		},
	}
	require.NoError(t, c.DeleteWithCleanup(ctx, result.DocumentID, ops))
	conflict, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptDeletionConflict, conflict.Outcome)
	fail = true
	require.ErrorContains(t, c.ReleaseHeldDeletion(ctx, result.DocumentID, job.Incarnation, ops), "provider unavailable")
	conflict, err = c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptDeletionConflict, conflict.Outcome)
	fail = false
	require.NoError(t, c.ReleaseHeldDeletion(ctx, result.DocumentID, job.Incarnation, ops))
	require.Equal(t, 3, calls)
	reused, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, reused.Outcome)
}

func TestVerifiedLegacyHoldReleasesWithoutAnotherHostedDelete(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, c, "legacy release", false, false)
	var legacyCalls int
	require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, domain.DeleteOperations{
		ExpectedIncarnation: doc.incarnation,
		LegacyNamespace:     func(context.Context, string) error { legacyCalls++; return nil },
		LegacyVectors:       func(context.Context, string) error { legacyCalls++; return nil },
	}))
	require.Equal(t, 2, legacyCalls)
	conflict, err := c.AcceptDocument(ctx, func() domain.AcceptRequest {
		req := jobsRequest("replacement blocked")
		req.Document.ID, req.CallerSupplied = doc.id, true
		return req
	}())
	require.NoError(t, err)
	require.Equal(t, domain.AcceptDeletionConflict, conflict.Outcome)

	// Releasing a legacy hold issues zero new document-wide deletes.
	require.NoError(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
		LegacyNamespace: func(context.Context, string) error {
			t.Fatal("legacy release must not call LegacyNamespace")
			return nil
		},
		LegacyVectors: func(context.Context, string) error {
			t.Fatal("legacy release must not call LegacyVectors")
			return nil
		},
	}))
	reused, err := c.AcceptDocument(ctx, func() domain.AcceptRequest {
		req := jobsRequest("replacement allowed")
		req.Document.ID, req.CallerSupplied = doc.id, true
		return req
	}())
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, reused.Outcome)
}

func TestQuarantinePreservesExistingReceiptAndSerializesWithAdmission(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()

	t.Run("fresh absent UUID", func(t *testing.T) {
		id := domain.DocumentID(uuid.NewString())
		first, err := c.QuarantineDeletion(ctx, id, domain.QuarantineTarget{VectorMode: legacyVectors, VectorIndex: "idx-a"})
		require.NoError(t, err)
		require.Equal(t, id, first.DocumentID)
		require.NotEmpty(t, first.Incarnation)
		require.Equal(t, "quarantine", first.Kind)
		require.Equal(t, legacyVectors, first.VectorMode)
		require.Equal(t, "idx-a", first.VectorIndex)
		require.True(t, first.Held)
		require.True(t, first.Finalized)
		require.Equal(t, "quarantined", first.Phase)

		second, err := c.QuarantineDeletion(ctx, id, domain.QuarantineTarget{VectorMode: revisionVectors, VectorIndex: "idx-b"})
		require.NoError(t, err)
		require.Equal(t, first, second, "repeating quarantine preserves the existing pair and target")

		listed, err := c.ListHeldDeletions(ctx)
		require.NoError(t, err)
		require.Contains(t, listed, first)
		require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, id, "blocked replacement"))
	})

	t.Run("unfinished legacy reservation with absent row", func(t *testing.T) {
		doc := acceptDocument(t, c, "unfinished reservation", false, true)
		inc := doc.incarnation
		_, err := c.Pool.Exec(t.Context(), `INSERT INTO document_deletions(document_id,incarnation,kind,source_key,source_bucket,vector_mode,vector_index,step,held,finalized)
			VALUES($1,$2,'legacy_reservation',$3,'originals','legacy','idx-1',0,TRUE,FALSE)`,
			string(doc.id), inc, doc.key)
		require.NoError(t, err)
		_, err = c.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, string(doc.id))
		require.NoError(t, err)

		q, err := c.QuarantineDeletion(t.Context(), doc.id, domain.QuarantineTarget{VectorMode: legacyVectors, VectorIndex: "idx-other"})
		require.NoError(t, err)
		require.Equal(t, inc, q.Incarnation)
		require.Equal(t, "legacy_reservation", q.Kind)
		require.Equal(t, doc.key, q.SourceKey)
		require.Equal(t, "originals", q.SourceBucket)
		require.Equal(t, "idx-1", q.VectorIndex)
		require.Equal(t, "reserved", q.Phase)
		require.False(t, q.Finalized)
		require.True(t, q.Held)
	})

	t.Run("live document rejected", func(t *testing.T) {
		doc := acceptDocument(t, c, "live row", false, false)
		_, err := c.QuarantineDeletion(t.Context(), doc.id, domain.QuarantineTarget{VectorMode: legacyVectors})
		require.ErrorIs(t, err, domain.ErrDocumentExists)
	})

	t.Run("previously released receipt replaced by fresh quarantine", func(t *testing.T) {
		id := domain.DocumentID(uuid.NewString())
		oldInc := uuid.NewString()
		_, err := c.Pool.Exec(t.Context(), `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,step,held,finalized)
			VALUES($1,$2,'qualified_intent','revision',3,FALSE,TRUE)`, string(id), oldInc)
		require.NoError(t, err)

		q, err := c.QuarantineDeletion(t.Context(), id, domain.QuarantineTarget{VectorMode: legacyVectors, VectorIndex: "idx-c"})
		require.NoError(t, err)
		require.NotEqual(t, oldInc, q.Incarnation)
		require.Equal(t, "quarantine", q.Kind)
		require.True(t, q.Held)
		require.Equal(t, domain.AcceptDeletionConflict, admit(t.Context(), t, c, id, "blocked after re-quarantine"))
	})

	t.Run("concurrent quarantine and admission serialize on absent ID", func(t *testing.T) {
		id := domain.DocumentID(uuid.NewString())
		other := otherConnection(t, c)
		var wg sync.WaitGroup
		var qRes domain.HeldDeletion
		var qErr, aErr error
		var aRes domain.AcceptResult
		wg.Go(func() {
			qRes, qErr = c.QuarantineDeletion(t.Context(), id, domain.QuarantineTarget{VectorMode: legacyVectors})
		})
		wg.Go(func() {
			req := jobsRequest("concurrent admit")
			req.Document.ID, req.CallerSupplied = id, true
			aRes, aErr = other.AcceptDocument(t.Context(), req)
		})
		wg.Wait()
		// Either quarantine won (admission saw conflict) or admission won (quarantine saw live row).
		if aRes.Outcome == domain.AcceptAccepted {
			require.NoError(t, aErr)
			require.ErrorIs(t, qErr, domain.ErrDocumentExists)
		} else {
			require.NoError(t, qErr)
			require.NotEmpty(t, qRes.Incarnation)
			require.NoError(t, aErr)
			require.Equal(t, domain.AcceptDeletionConflict, aRes.Outcome)
		}
	})
}

func TestReleaseUnfinishedLegacyReservationCleansPendingSourceAndGuardsState(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, c, "pending source legacy", false, true)
	_, err := c.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,source_key,source_bucket,vector_mode,vector_index,step,held,finalized)
		VALUES($1,$2,'legacy_reservation',$3,'originals','legacy','idx-1',0,TRUE,FALSE)`,
		string(doc.id), doc.incarnation, doc.key)
	require.NoError(t, err)
	// Simulate pre-upgrade deletion removing the live row while the reservation stays unfinished.
	_, err = c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, string(doc.id))
	require.NoError(t, err)

	// Wrong token, missing S3, wrong bucket, wrong index, active claim all preserve the hold.
	require.ErrorIs(t, c.ReleaseHeldDeletion(ctx, doc.id, uuid.NewString(), domain.DeleteOperations{
		SourceBucket: "originals", VectorIndex: "idx-1", Source: func(context.Context, string) error { return nil },
	}), domain.ErrDeletionPreconditionFailed)
	require.ErrorIs(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
		SourceBucket: "originals", VectorIndex: "idx-1",
	}), domain.ErrFileStorageUnavailable)
	require.ErrorIs(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
		SourceBucket: "wrong", VectorIndex: "idx-1", Source: func(context.Context, string) error { return nil },
	}), domain.ErrDeletionTargetMismatch)
	require.ErrorIs(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
		SourceBucket: "originals", VectorIndex: "wrong", Source: func(context.Context, string) error { return nil },
	}), domain.ErrDeletionTargetMismatch)

	// Active claim returns busy without touching S3.
	_, err = c.Pool.Exec(ctx, `UPDATE document_deletions SET claim=$2, claim_until=clock_timestamp()+interval '1 minute' WHERE document_id=$1`,
		string(doc.id), uuid.NewString())
	require.NoError(t, err)
	require.ErrorIs(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
		SourceBucket: "originals", VectorIndex: "idx-1", Source: func(context.Context, string) error {
			t.Fatal("busy claim must not run source cleanup")
			return nil
		},
	}), domain.ErrDeletionBusy)
	_, err = c.Pool.Exec(ctx, `UPDATE document_deletions SET claim=NULL, claim_until=NULL WHERE document_id=$1`, string(doc.id))
	require.NoError(t, err)

	// Source delete failure preserves the unfinished held receipt.
	var sourceCalls int
	sourceErr := errors.New("s3 unavailable")
	err = c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
		SourceBucket: "originals",
		VectorIndex:  "idx-1",
		Source: func(context.Context, string) error {
			sourceCalls++
			return sourceErr
		},
	})
	require.ErrorIs(t, err, sourceErr)
	require.Equal(t, 1, sourceCalls)
	r := readReceipt(t, c, doc.id)
	require.False(t, r.finalized)
	require.True(t, r.held)
	require.Equal(t, stepNamespace, r.step)
	require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, doc.id, "still held"))

	// Retry cleans source_key once and atomically finalizes & releases without calling LegacyVectors.
	var deletedKeys []string
	require.NoError(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
		SourceBucket: "originals",
		VectorIndex:  "idx-1",
		LegacyNamespace: func(context.Context, string) error {
			t.Fatal("legacy release must not call LegacyNamespace")
			return nil
		},
		LegacyVectors: func(context.Context, string) error {
			t.Fatal("legacy release must not call LegacyVectors")
			return nil
		},
		Source: func(_ context.Context, key string) error {
			deletedKeys = append(deletedKeys, key)
			return nil
		},
	}))
	require.Equal(t, []string{doc.key}, deletedKeys)
	r = readReceipt(t, c, doc.id)
	require.True(t, r.finalized)
	require.False(t, r.held)
	require.Nil(t, r.source)
	require.Nil(t, r.claim)
	require.Equal(t, stepDone, r.step)
	require.Equal(t, domain.AcceptAccepted, admit(ctx, t, c, doc.id, "reuse after release"))
}

func TestReleaseRefusesLiveDocumentAndSupportsConfirmedDeadMemoryOwner(t *testing.T) {
	c := jobsDatabase(t)

	t.Run("live document refuses release", func(t *testing.T) {
		ctx := t.Context()
		doc := acceptDocument(t, c, "live document", false, false)
		_, err := c.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,step,held,finalized)
			VALUES($1,$2,'legacy_reservation','legacy',0,TRUE,FALSE)`, string(doc.id), doc.incarnation)
		require.NoError(t, err)
		require.ErrorIs(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{}), domain.ErrDeletionPreconditionFailed)
		require.Equal(t, "processing", documentStatus(t, c, doc.id))
	})

	t.Run("dead offline owner recovery", func(t *testing.T) {
		ctx := t.Context()
		doc := acceptDocument(t, c, "dead offline owner", false, true)
		owner := uuid.NewString()
		_, err := c.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,source_key,source_bucket,vector_mode,memory_owner,step,held,finalized)
			VALUES($1,$2,'legacy_reservation',$3,'originals','memory',$4,0,FALSE,FALSE)`,
			string(doc.id), doc.incarnation, doc.key, owner)
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, string(doc.id))
		require.NoError(t, err)

		// Without confirmation or with wrong owner, release fails and keeps UUID reserved.
		require.ErrorIs(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
			SourceBucket: "originals", Source: func(context.Context, string) error { return nil },
		}), domain.ErrDeletionTargetMismatch)
		require.ErrorIs(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
			ConfirmedOfflineOwnerTerminated: true, MemoryOwner: uuid.NewString(),
			SourceBucket: "originals", Source: func(context.Context, string) error { return nil },
		}), domain.ErrDeletionTargetMismatch)
		require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, doc.id, "still reserved"))

		var sourceDeleted string
		require.NoError(t, c.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, domain.DeleteOperations{
			ConfirmedOfflineOwnerTerminated: true,
			MemoryOwner:                     owner,
			SourceBucket:                    "originals",
			Source: func(_ context.Context, key string) error {
				sourceDeleted = key
				return nil
			},
		}))
		require.Equal(t, doc.key, sourceDeleted)
		require.Equal(t, domain.AcceptAccepted, admit(ctx, t, c, doc.id, "reusable after dead owner recovery"))
	})
}

func receiptOwner(t *testing.T, c *Client, id domain.DocumentID) *string {
	t.Helper()
	var owner *string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT memory_owner::text FROM document_deletions WHERE document_id=$1`, string(id)).Scan(&owner))
	return owner
}

func memoryOps(incarnation, owner string) domain.DeleteOperations {
	ops := newDeletionRecorder().ops(incarnation)
	ops.InMemory, ops.MemoryOwner = true, owner
	return ops
}

func TestQualifiedDeletionRecordsTheMemoryOwner(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scoped bool
		mode   string
	}{
		{name: "document-wide vectors", mode: memoryVectors},
		{name: "revision vectors", scoped: true, mode: memoryRevision},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := activatedDatabase(t)
			doc := acceptDocument(t, c, tc.name, tc.scoped, true)
			owner := uuid.NewString()
			_, _, err := c.beginDeletion(t.Context(), doc.id, memoryOps(doc.incarnation, owner))
			require.NoError(t, err)
			require.Equal(t, tc.mode, readReceipt(t, c, doc.id).mode)
			require.Equal(t, owner, deref(receiptOwner(t, c, doc.id)))
		})
	}
}

func TestScopedPrincipalDeletionRecordsTheMemoryOwner(t *testing.T) {
	f := newOwnedFixture(t)
	_, err := f.c.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	docID := f.acceptOwned(t, f.owner, "", "documents/doc/memory-owner/original.pdf")
	owner := uuid.NewString()

	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), domain.DocumentID(docID), memoryOps(storedIncarnation(t, f.c, domain.DocumentID(docID)), owner))
	require.NoError(t, err)
	require.Equal(t, memoryRevision, readReceipt(t, f.c, domain.DocumentID(docID)).mode)
	require.Equal(t, owner, deref(receiptOwner(t, f.c, domain.DocumentID(docID))))
}

func TestMemoryReceiptWithoutAnOwnerIsRefused(t *testing.T) {
	c := activatedDatabase(t)
	for _, scoped := range []bool{false, true} {
		doc := acceptDocument(t, c, "ownerless", scoped, false)
		_, _, err := c.beginDeletion(t.Context(), doc.id, memoryOps(doc.incarnation, ""))
		require.ErrorIs(t, err, domain.ErrVectorStorageUnavailable)
		require.Equal(t, 0, receiptCount(t.Context(), t, c, doc.id), "no ownerless receipt may be created")
		require.NotEqual(t, "deleting", documentStatus(t, c, doc.id))
	}

	f := newOwnedFixture(t)
	_, err := f.c.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	docID := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/doc/ownerless/original.pdf"))
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), docID, memoryOps(storedIncarnation(t, f.c, docID), ""))
	require.ErrorIs(t, err, domain.ErrVectorStorageUnavailable)
	require.Equal(t, 0, receiptCount(t.Context(), t, f.c, docID))
	require.NotEqual(t, "deleting", documentStatus(t, f.c, docID))
}

func TestHostedAndLocalReceiptsRecordNoMemoryOwner(t *testing.T) {
	c := activatedDatabase(t)
	hosted := acceptDocument(t, c, "hosted", true, false)
	ops := newDeletionRecorder().ops(hosted.incarnation)
	ops.MemoryOwner = uuid.NewString()
	_, _, err := c.beginDeletion(t.Context(), hosted.id, ops)
	require.NoError(t, err)
	require.Equal(t, revisionVectors, readReceipt(t, c, hosted.id).mode)
	require.Nil(t, receiptOwner(t, c, hosted.id))

	local := acceptDocument(t, c, "local", false, false)
	_, _, err = c.beginDeletion(t.Context(), local.id, domain.DeleteOperations{ExpectedIncarnation: local.incarnation, MemoryOwner: uuid.NewString()})
	require.NoError(t, err)
	require.Equal(t, localVectors, readReceipt(t, c, local.id).mode)
	require.Nil(t, receiptOwner(t, c, local.id))
}

func TestReplacingAReleasedReceiptRecordsTheNewOwner(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, c, "replaced receipt", false, false)
	previous := uuid.NewString()
	_, err := c.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,memory_owner,step,held,finalized)
		VALUES($1,$2,'qualified_intent','memory',$3,3,FALSE,TRUE)`, string(doc.id), uuid.NewString(), previous)
	require.NoError(t, err)

	owner := uuid.NewString()
	_, _, err = c.beginDeletion(ctx, doc.id, memoryOps(doc.incarnation, owner))
	require.NoError(t, err)
	require.Equal(t, owner, deref(receiptOwner(t, c, doc.id)), "the replaced receipt's owner is not inherited")
	require.Equal(t, doc.incarnation, readLegacyReceipt(ctx, t, c, doc.id).incarnation)

	hosted := acceptDocument(t, c, "replaced by hosted", true, false)
	_, err = c.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,memory_owner,step,held,finalized)
		VALUES($1,$2,'qualified_intent','memory',$3,3,FALSE,TRUE)`, string(hosted.id), uuid.NewString(), previous)
	require.NoError(t, err)
	_, _, err = c.beginDeletion(ctx, hosted.id, newDeletionRecorder().ops(hosted.incarnation))
	require.NoError(t, err)
	require.Nil(t, receiptOwner(t, c, hosted.id), "a hosted receipt never keeps a process owner")
}

func TestQualifiedTakeoverOfAReservationKeepsOrFillsTheOwner(t *testing.T) {
	c := activatedDatabase(t)
	reserve := func(t *testing.T, doc liveDocument, owner *string) {
		t.Helper()
		ctx := t.Context()
		_, err := c.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,memory_owner,step,held,finalized)
			VALUES($1,$2,'legacy_reservation','memory',$3::uuid,0,FALSE,FALSE)`, string(doc.id), doc.incarnation, owner)
		require.NoError(t, err)
	}
	t.Run("recorded owner is kept", func(t *testing.T) {
		ctx := t.Context()
		doc := acceptDocument(t, c, "recorded owner", false, false)
		owner := uuid.NewString()
		reserve(t, doc, &owner)
		_, _, err := c.beginDeletion(ctx, doc.id, memoryOps(doc.incarnation, owner))
		require.NoError(t, err)
		require.Equal(t, "qualified_intent", readReceipt(t, c, doc.id).kind)
		require.Equal(t, owner, deref(receiptOwner(t, c, doc.id)))
	})
	t.Run("an earlier binary's ownerless reservation gains the owner", func(t *testing.T) {
		ctx := t.Context()
		doc := acceptDocument(t, c, "ownerless reservation", false, false)
		reserve(t, doc, nil)
		owner := uuid.NewString()
		_, _, err := c.beginDeletion(ctx, doc.id, memoryOps(doc.incarnation, owner))
		require.NoError(t, err)
		require.Equal(t, "qualified_intent", readReceipt(t, c, doc.id).kind)
		require.Equal(t, owner, deref(receiptOwner(t, c, doc.id)))
	})
	t.Run("another owner's reservation is not converted", func(t *testing.T) {
		ctx := t.Context()
		doc := acceptDocument(t, c, "foreign reservation", false, false)
		recorded := uuid.NewString()
		reserve(t, doc, &recorded)
		_, _, err := c.beginDeletion(ctx, doc.id, memoryOps(doc.incarnation, uuid.NewString()))
		require.ErrorIs(t, err, domain.ErrDeletionBusy)
		require.Equal(t, "legacy_reservation", readReceipt(t, c, doc.id).kind)
		require.Equal(t, recorded, deref(receiptOwner(t, c, doc.id)))
	})
}

func TestHeldQualifiedMemoryDeletionReleasesOnItsOwner(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	req := jobsRequest("uncertain offline write")
	req.CallerSupplied, req.SupportsScopedVectors = true, true
	result, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	job, err := c.ClaimJob(ctx, "legacy-worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.ErrorIs(t, c.MutateJobVectors(ctx, *job, func(ctx context.Context) error {
		return domain.DispatchVectorMutation(ctx, false, func(context.Context) error { return context.DeadlineExceeded })
	}), context.DeadlineExceeded)

	owner := uuid.NewString()
	ops := memoryOps(job.Incarnation, owner)
	require.NoError(t, c.DeleteWithCleanup(ctx, result.DocumentID, ops))
	receipt := readReceipt(t, c, result.DocumentID)
	require.True(t, receipt.held, "the uncertain write keeps the identity held")
	require.Equal(t, memoryRevision, receipt.mode)
	require.Equal(t, owner, deref(receiptOwner(t, c, result.DocumentID)))

	require.ErrorIs(t, c.ReleaseHeldDeletion(ctx, result.DocumentID, job.Incarnation, memoryOps(job.Incarnation, uuid.NewString())),
		domain.ErrDeletionPreconditionFailed, "another process cannot release it")
	require.NoError(t, c.ReleaseHeldDeletion(ctx, result.DocumentID, job.Incarnation, ops), "the owning process releases it")
	require.False(t, readReceipt(t, c, result.DocumentID).held)
}

func TestLegacyHostedReleaseRequiresExactReceipt(t *testing.T) {
	c := jobsDatabase(t)
	const index = "retired-index"
	for _, tt := range []struct {
		name, kind, mode, storedIndex, configuredIndex string
		step                                           int
		held, finalized, live, busy                    bool
		token                                          string
		want                                           error
	}{
		{name: "wrong index", storedIndex: index, configuredIndex: "other", want: domain.ErrDeletionTargetMismatch},
		{name: "missing configured index", storedIndex: index, want: domain.ErrDeletionTargetMismatch},
		{name: "missing recorded index", configuredIndex: index, want: domain.ErrDeletionTargetMismatch},
		{name: "wrong token", storedIndex: index, configuredIndex: index, token: "wrong", want: domain.ErrDeletionPreconditionFailed},
		{name: "missing token", storedIndex: index, configuredIndex: index, token: "missing", want: domain.ErrDeletionPreconditionRequired},
		{name: "live row", storedIndex: index, configuredIndex: index, live: true, want: domain.ErrDeletionPreconditionFailed},
		{name: "released", kind: "quarantine", step: stepDone, finalized: true, want: domain.ErrDeletionPreconditionFailed},
		{name: "ineligible kind", kind: "qualified_intent", held: true, want: domain.ErrDeletionPreconditionFailed},
		{name: "unexpected finalized phase", kind: "legacy_reservation", finalized: true, held: true, want: domain.ErrDeletionPreconditionFailed},
		{name: "busy", storedIndex: index, configuredIndex: index, busy: true, want: domain.ErrDeletionBusy},
		{name: "revision path", mode: revisionVectors, storedIndex: index, configuredIndex: index, want: domain.ErrDeletionPreconditionFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			id, inc := domain.DocumentID(uuid.NewString()), uuid.NewString()
			if tt.live {
				doc := acceptDocument(t, c, tt.name, false, false)
				id, inc = doc.id, doc.incarnation
			}
			if tt.kind == "" {
				tt.kind, tt.finalized, tt.held, tt.step = "quarantine", true, true, stepDone
			}
			if tt.mode == "" {
				tt.mode = legacyVectors
			}
			_, err := c.Pool.Exec(ctx, `INSERT INTO document_deletions
				(document_id, incarnation, kind, vector_mode, vector_index, step, held, finalized, claim, claim_until)
				VALUES ($1,$2,$3,$4,NULLIF($5,''),$6,$7,$8,
				CASE WHEN $9 THEN gen_random_uuid() END, CASE WHEN $9 THEN clock_timestamp()+interval '1 minute' END)`,
				string(id), inc, tt.kind, tt.mode, tt.storedIndex, tt.step, tt.held, tt.finalized, tt.busy)
			require.NoError(t, err)
			before := readReceipt(t, c, id)
			token := inc
			switch tt.token {
			case "wrong":
				token = uuid.NewString()
			case "missing":
				token = ""
			}
			noSource := func(context.Context, string) error {
				t.Error("a refused release attempted cleanup")
				return nil
			}
			err = c.ReleaseLegacyHostedHold(ctx, id, token, domain.DeleteOperations{
				VectorIndex: tt.configuredIndex, Source: noSource, LegacyNamespace: noSource, LegacyVectors: noSource,
			})
			require.ErrorIs(t, err, tt.want)
			require.Equal(t, before, readReceipt(t, c, id))
			if errors.Is(tt.want, domain.ErrDeletionTargetMismatch) {
				mode, recordedIndex, lookupErr := c.HeldDeletionReleaseMode(ctx, id, token)
				require.NoError(t, lookupErr)
				require.Equal(t, legacyVectors, mode)
				require.Equal(t, tt.storedIndex, recordedIndex)
			} else {
				_, _, lookupErr := c.HeldDeletionReleaseMode(ctx, id, token)
				if tt.mode == revisionVectors {
					require.NoError(t, lookupErr, "revision selection must reach the provider path")
				} else {
					require.ErrorIs(t, lookupErr, tt.want)
				}
			}
		})
	}
	ctx := t.Context()
	id := domain.DocumentID(uuid.NewString())
	_, _, err := c.HeldDeletionReleaseMode(ctx, id, uuid.NewString())
	require.ErrorIs(t, err, domain.ErrDeletionPreconditionFailed)
	first, err := c.QuarantineDeletion(ctx, id, domain.QuarantineTarget{VectorMode: legacyVectors, VectorIndex: index})
	require.NoError(t, err)
	mode, recordedIndex, err := c.HeldDeletionReleaseMode(ctx, id, first.Incarnation)
	require.NoError(t, err)
	require.Equal(t, legacyVectors, mode)
	require.Equal(t, index, recordedIndex)
	ops := domain.DeleteOperations{VectorIndex: index}
	_, err = c.Pool.Exec(ctx, `DELETE FROM vector_state`)
	require.NoError(t, err)
	require.ErrorIs(t, c.ReleaseLegacyHostedHold(ctx, id, first.Incarnation, ops), pgx.ErrNoRows)
	_, err = c.Pool.Exec(ctx, `INSERT INTO vector_state (active_profile_hash) VALUES ('current-profile')`)
	require.NoError(t, err)
	require.NoError(t, c.ReleaseLegacyHostedHold(ctx, id, first.Incarnation, ops))
	_, err = c.Pool.Exec(ctx, `UPDATE vector_state SET active_profile_hash=''`)
	require.NoError(t, err)
	second, err := c.QuarantineDeletion(ctx, id, domain.QuarantineTarget{VectorMode: legacyVectors, VectorIndex: index})
	require.NoError(t, err)
	require.NotEqual(t, first.Incarnation, second.Incarnation)
	require.ErrorIs(t, c.ReleaseLegacyHostedHold(ctx, id, first.Incarnation, ops), domain.ErrDeletionPreconditionFailed)
	require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, id, "re-quarantined lifetime"))
}

func TestLegacyHostedReleaseSerializesWithFreezeAndAdmission(t *testing.T) {
	c, observer := sessionTestClientWithPool(t, 1)
	other := otherConnection(t, c)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	doc := acceptDocument(t, c, "legacy source with active freeze", false, true)
	_, err := c.Pool.Exec(ctx, `INSERT INTO document_deletions
		(document_id,incarnation,kind,vector_mode,vector_index,source_bucket,source_key,held)
		VALUES($1,$2,'legacy_reservation','legacy','retired-index','originals',$3,TRUE)`, string(doc.id), doc.incarnation, doc.key)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, string(doc.id))
	require.NoError(t, err)
	started, complete := make(chan struct{}), make(chan struct{})
	ops := domain.DeleteOperations{
		VectorIndex: "retired-index", SourceBucket: "originals",
		Source: func(sourceCtx context.Context, _ string) error {
			close(started)
			select {
			case <-sourceCtx.Done():
				return sourceCtx.Err()
			case <-complete:
				return nil
			}
		},
	}

	c.SetVectorGeneration("different")
	require.ErrorIs(t, c.ReleaseLegacyHostedHold(ctx, doc.id, doc.incarnation, ops), domain.ErrGenerationMismatch)
	c.SetVectorGeneration(LegacyGeneration)
	require.NoError(t, c.FreezeWriters(ctx, "next"))
	require.ErrorIs(t, c.ReleaseLegacyHostedHold(ctx, doc.id, doc.incarnation, ops), domain.ErrWritersFrozen)
	require.NoError(t, c.ThawWriters(ctx))

	released, frozen, admitted := make(chan error, 1), make(chan error, 1), make(chan domain.AcceptResult, 1)
	go func() { released <- c.ReleaseLegacyHostedHold(ctx, doc.id, doc.incarnation, ops) }()
	select {
	case <-started:
	case releaseErr := <-released:
		t.Fatalf("release ended before source cleanup: %v", releaseErr)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	_, _, err = other.HeldDeletionReleaseMode(ctx, doc.id, doc.incarnation)
	require.ErrorIs(t, err, domain.ErrDeletionBusy)
	require.ErrorIs(t, other.ReleaseLegacyHostedHold(ctx, doc.id, doc.incarnation, ops), domain.ErrDeletionBusy)
	require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, other, doc.id, "concurrent replacement"))
	waiter, cancelWaiter := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelWaiter()
	require.ErrorIs(t, c.ReleaseLegacyHostedHold(waiter, doc.id, doc.incarnation, ops), context.DeadlineExceeded)

	go func() { frozen <- c.FreezeWriters(ctx, "next") }()
	require.Eventually(t, func() bool {
		var blocked bool
		err := observer.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
			WHERE datname=current_database() AND application_name=current_setting('application_name')
			AND wait_event_type='Lock'
			AND query LIKE 'UPDATE vector_state SET frozen=TRUE%')`).Scan(&blocked)
		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
	admitErr := make(chan error, 1)
	go func() {
		req := jobsRequest("replacement waiting for freeze")
		req.Document.ID, req.CallerSupplied = doc.id, true
		res, err := other.AcceptDocument(ctx, req)
		admitErr <- err
		admitted <- res
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		err := observer.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
			WHERE datname=current_database() AND application_name=current_setting('application_name')
			AND wait_event='advisory' AND query='SELECT pg_advisory_xact_lock($1)')`).Scan(&blocked)
		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
	close(complete)
	require.NoError(t, <-released)
	require.NoError(t, <-frozen)
	require.NoError(t, <-admitErr)
	require.Equal(t, domain.AcceptFrozen, (<-admitted).Outcome)
	r := readReceipt(t, c, doc.id)
	require.True(t, r.finalized)
	require.False(t, r.held)
	require.Nil(t, r.claim)
	requireDedicatedSessions(t, c, observer, int(other.Pool.Stat().TotalConns()))
	require.NoError(t, c.ThawWriters(ctx))
	require.Equal(t, domain.AcceptAccepted, admit(ctx, t, c, doc.id, "replacement after thaw"))
}

type interruptHeldReleaseKey struct{}

type interruptHeldReleaseTracer struct{}

func (interruptHeldReleaseTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if interrupt, ok := ctx.Value(interruptHeldReleaseKey{}).(func(string)); ok {
		interrupt(data.SQL)
	}
	return ctx
}

func (interruptHeldReleaseTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {
}

func TestLegacyHostedReleaseRetriesOnlyPendingSource(t *testing.T) {
	c := jobsDatabase(t, WithQueryTracer(interruptHeldReleaseTracer{}))
	for _, test := range []struct {
		name, finalizationSQLPrefix string
	}{
		{name: "source completion"},
		{name: "finalization begin", finalizationSQLPrefix: "begin "},
		{name: "finalization commit", finalizationSQLPrefix: "commit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			testLegacyHostedReleaseSourceRetry(t, c, test.finalizationSQLPrefix)
		})
	}
}

func testLegacyHostedReleaseSourceRetry(t *testing.T, c *Client, finalizationSQLPrefix string) {
	t.Helper()
	ctx := t.Context()
	doc := acceptDocument(t, c, "legacy source checkpoint "+t.Name(), false, true)
	_, err := c.Pool.Exec(ctx, `INSERT INTO document_deletions
		(document_id,incarnation,kind,vector_mode,vector_index,source_bucket,source_key,held)
		VALUES($1,$2,'legacy_reservation','legacy','retired-index','originals',$3,TRUE)`, string(doc.id), doc.incarnation, doc.key)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, string(doc.id))
	require.NoError(t, err)

	interrupt, cancel := context.WithCancel(ctx)
	defer cancel()
	var keys []string
	interrupt = context.WithValue(interrupt, interruptHeldReleaseKey{}, func(sql string) {
		if len(keys) != 0 && finalizationSQLPrefix != "" && strings.HasPrefix(sql, finalizationSQLPrefix) {
			cancel()
		}
	})
	ops := domain.DeleteOperations{
		VectorIndex: "retired-index", SourceBucket: "originals",
		Source: func(_ context.Context, key string) error {
			keys = append(keys, key)
			if finalizationSQLPrefix == "" {
				cancel()
			}
			return nil
		},
	}
	require.ErrorIs(t, c.ReleaseLegacyHostedHold(interrupt, doc.id, doc.incarnation, ops), context.Canceled)
	require.Equal(t, []string{doc.key}, keys)
	r := readReceipt(t, c, doc.id)
	require.Equal(t, stepDone, r.step)
	require.True(t, r.held)
	require.False(t, r.finalized)
	require.Nil(t, r.claim)
	require.Equal(t, domain.AcceptDeletionConflict, admit(ctx, t, c, doc.id, "replacement before retry "+t.Name()))
	require.NoError(t, c.ReleaseLegacyHostedHold(ctx, doc.id, doc.incarnation, ops))
	require.Equal(t, []string{doc.key}, keys)

	// A finalized held receipt may still retain its original source key.
	_, err = c.Pool.Exec(ctx, `UPDATE document_deletions SET held=TRUE,source_key=$2 WHERE document_id=$1`, string(doc.id), doc.key)
	require.NoError(t, err)
	require.NoError(t, c.ReleaseLegacyHostedHold(ctx, doc.id, doc.incarnation, ops))
	require.Equal(t, []string{doc.key}, keys)
}

type heldClaimTestDB struct {
	heldReleaseDatabase
	begin func(context.Context, pgx.TxOptions) (pgx.Tx, error)
	exec  func(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (db heldClaimTestDB) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	if db.begin != nil {
		return db.begin(ctx, opts)
	}
	return db.heldReleaseDatabase.BeginTx(ctx, opts)
}

func (db heldClaimTestDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if db.exec != nil {
		return db.exec(ctx, sql, args...)
	}
	return db.heldReleaseDatabase.Exec(ctx, sql, args...)
}

type heldClaimTestTx struct {
	pgx.Tx
	commit func(context.Context, pgx.Tx) error
	exec   func(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (tx heldClaimTestTx) Commit(ctx context.Context) error { return tx.commit(ctx, tx.Tx) }

func (tx heldClaimTestTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if tx.exec != nil {
		return tx.exec(ctx, sql, args...)
	}
	return tx.Tx.Exec(ctx, sql, args...)
}

func interceptFirstHeldReleaseCommit(h heldRelease, commit func(context.Context, pgx.Tx) error) heldRelease {
	db := h.db
	first := true
	h.db = heldClaimTestDB{heldReleaseDatabase: db, begin: func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		tx, err := db.BeginTx(ctx, opts)
		if err != nil || !first {
			return tx, err
		}
		first = false
		return heldClaimTestTx{Tx: tx, commit: commit}, nil
	}}
	return h
}

type heldClaimContextKey struct{}

func TestHeldClaimCleanupUsesBoundedDetachedContexts(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		dedicated, fail, wantConnect bool
	}{
		{name: "pooled failure", fail: true},
		{name: "dedicated failure", dedicated: true, fail: true, wantConnect: true},
		{name: "dedicated zero rows", dedicated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := context.WithValue(t.Context(), heldClaimContextKey{}, "held claim")
			ctx, cancel := context.WithDeadline(parent, time.Now().Add(-time.Second))
			defer cancel()
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)

			checkContext := func(ctx context.Context) time.Time {
				t.Helper()
				require.NoError(t, ctx.Err())
				require.Equal(t, "held claim", ctx.Value(heldClaimContextKey{}))
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.Positive(t, time.Until(deadline))
				require.LessOrEqual(t, time.Until(deadline), deletionClaimReleaseTimeout)
				return deadline
			}

			id, incarnation, claim := domain.DocumentID(uuid.NewString()), uuid.NewString(), uuid.NewString()
			var firstCtx context.Context
			var firstDeadline time.Time
			clears, connects := 0, 0
			failed := errors.New("claim cleanup unavailable")
			h := heldRelease{db: heldClaimTestDB{exec: func(clearCtx context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
				clears++
				firstCtx = clearCtx
				firstDeadline = checkContext(clearCtx)
				require.Equal(t, []any{string(id), incarnation, claim}, args)
				if tc.fail {
					return pgconn.CommandTag{}, failed
				}
				return pgconn.NewCommandTag("UPDATE 0"), nil
			}}}
			if tc.dedicated {
				config, err := pgx.ParseConfig("host=127.0.0.1 sslmode=disable")
				require.NoError(t, err)
				config.ConnectTimeout = 0
				config.DialFunc = func(connectCtx context.Context, _, _ string) (net.Conn, error) {
					connects++
					require.ErrorIs(t, firstCtx.Err(), context.Canceled)
					require.True(t, checkContext(connectCtx).After(firstDeadline))
					return nil, failed
				}
				h.claimConnConfig = config
			}

			h.releaseClaim(ctx, id, incarnation, claim)
			require.Equal(t, 1, clears)
			require.Equal(t, tc.wantConnect, connects == 1)
			require.LessOrEqual(t, connects, 1)
			require.ErrorIs(t, firstCtx.Err(), context.Canceled)
		})
	}
}

func pendingHeldSource(t *testing.T, owner *Client) (id domain.DocumentID, incarnation, key string) {
	t.Helper()
	id, incarnation, key = domain.DocumentID(uuid.NewString()), uuid.NewString(), "source/"+uuid.NewString()
	_, err := owner.Pool.Exec(t.Context(), `INSERT INTO document_deletions
		(document_id, incarnation, kind, vector_mode, vector_index, source_bucket, source_key, step, held)
		VALUES ($1,$2,'legacy_reservation','legacy','retired-index','originals',$3,$4,TRUE)`,
		string(id), incarnation, key, stepSource)
	require.NoError(t, err)
	return id, incarnation, key
}

type heldClaimRow struct {
	full, payload string
	claim         *string
	until         *time.Time
}

func snapshotHeldClaim(ctx context.Context, t *testing.T, owner *Client, id domain.DocumentID) heldClaimRow {
	t.Helper()
	var row heldClaimRow
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT to_jsonb(r)::text,
		(to_jsonb(r)-'claim'-'claim_until')::text, claim::text, claim_until
		FROM document_deletions r WHERE document_id=$1`, string(id)).Scan(&row.full, &row.payload, &row.claim, &row.until))
	return row
}

func dedicatedHeldClaim(t *testing.T, c *Client) (heldRelease, *pgx.Conn, func()) {
	t.Helper()
	guard, closeGuard, err := c.AcquireHeldRecoveryWriter(t.Context())
	require.NoError(t, err)
	closeGuard = sync.OnceFunc(closeGuard)
	t.Cleanup(closeGuard)
	config := c.Pool.Config().ConnConfig
	conn, err := pgx.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	return heldRelease{db: conn, outcome: guard, legacyOnly: true, claimConnConfig: config}, conn, closeGuard
}

func heldSourceOps(t *testing.T, keys *[]string) domain.DeleteOperations {
	t.Helper()
	vector := func(context.Context, string) error {
		t.Error("held source release called a vector provider")
		return nil
	}
	return domain.DeleteOperations{
		VectorIndex: "retired-index", SourceBucket: "originals",
		LegacyNamespace: vector, LegacyVectors: vector,
		RevisionVectors: func(ctx context.Context, id, _ string) error { return vector(ctx, id) },
		Source: func(_ context.Context, key string) error {
			*keys = append(*keys, key)
			return nil
		},
	}
}

func TestHeldReleaseAbandonsUncertainClaimCommit(t *testing.T) {
	owner, observer := sessionTestClientWithPool(t, 1)
	serving, _ := publishedServingClient(t, owner)
	widePool, err := NewClient(t.Context(), owner.Pool.Config().ConnString(), func(cfg *pgxpool.Config) { cfg.MaxConns = 2 })
	require.NoError(t, err)
	t.Cleanup(widePool.Pool.Close)
	require.EqualValues(t, 1, serving.Pool.Config().MaxConns)

	for _, tc := range []struct {
		name                         string
		client                       *Client
		dedicated, cancel, closeConn bool
		rollback, leaveOpen          bool
	}{
		{name: "pool commit", client: widePool},
		{name: "pool rollback", client: widePool, rollback: true},
		{name: "single pooled connection commit", client: serving},
		{name: "single pooled connection rollback", client: serving, rollback: true},
		{name: "single pooled connection unwind before clear", client: serving, leaveOpen: true, cancel: true},
		{name: "dedicated commit", client: owner, dedicated: true},
		{name: "dedicated rollback", client: owner, dedicated: true, rollback: true},
		{name: "canceled with lost dedicated connection", client: owner, dedicated: true, cancel: true, closeConn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, incarnation, key := pendingHeldSource(t, owner)
			before := snapshotHeldClaim(t.Context(), t, owner, id)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var keys []string
			ops := heldSourceOps(t, &keys)
			retry := tc.client.ReleaseHeldDeletion
			h := heldRelease{db: tc.client.Pool, outcome: tc.client.Pool}
			var conn *pgx.Conn
			done := func() {}
			if tc.dedicated {
				retry = tc.client.ReleaseLegacyHostedHold
				h, conn, done = dedicatedHeldClaim(t, tc.client)
			}
			lostReply := errors.New("claim commit reply lost")
			commits := 0
			h = interceptFirstHeldReleaseCommit(h, func(commitCtx context.Context, tx pgx.Tx) error {
				commits++
				switch {
				case tc.rollback:
					require.NoError(t, tx.Rollback(commitCtx))
				case !tc.leaveOpen:
					require.NoError(t, tx.Commit(commitCtx))
					var active bool
					require.NoError(t, observer.QueryRow(commitCtx, `SELECT claim IS NOT NULL AND claim_until>clock_timestamp()
						FROM document_deletions WHERE document_id=$1`, string(id)).Scan(&active))
					require.True(t, active)
				}
				if tc.cancel {
					cancel()
				}
				if tc.closeConn {
					closeCtx, cancelClose := context.WithTimeout(context.WithoutCancel(commitCtx), deletionClaimReleaseTimeout)
					defer cancelClose()
					require.NoError(t, conn.Close(closeCtx))
				}
				return lostReply
			})
			clears := 0
			db := h.db
			h.db = heldClaimTestDB{heldReleaseDatabase: db, exec: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				clears++
				return db.Exec(ctx, sql, args...)
			}}

			require.Same(t, lostReply, h.release(ctx, id, incarnation, ops))
			done()
			require.Equal(t, 1, commits)
			require.Equal(t, 1, clears)
			require.Empty(t, keys)
			require.Equal(t, before, snapshotHeldClaim(t.Context(), t, owner, id))
			require.NoError(t, retry(t.Context(), id, incarnation, ops))
			require.Equal(t, []string{key}, keys)
			released := readReceipt(t, owner, id)
			require.True(t, released.finalized)
			require.False(t, released.held)
			require.Equal(t, stepDone, released.step)
			require.Nil(t, released.source)
			require.Nil(t, released.claim)
		})
	}
}

func TestHeldReleaseSkipsClaimCleanupBeforeUncertainCommit(t *testing.T) {
	c := jobsDatabase(t)
	for _, tc := range []struct {
		name, fault string
		want        error
	}{
		{name: "begin", fault: "begin"},
		{name: "receipt selection", fault: "selection", want: domain.ErrDeletionPreconditionFailed},
		{name: "ineligible active claim", fault: "eligibility", want: domain.ErrDeletionBusy},
		{name: "target mismatch", fault: "target", want: domain.ErrDeletionTargetMismatch},
		{name: "update", fault: "update"},
		{name: "zero updated rows", fault: "row count", want: domain.ErrDeletionBusy},
		{name: "definite rollback", fault: "commit", want: pgx.ErrTxCommitRollback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, incarnation, _ := pendingHeldSource(t, c)
			if tc.fault == "eligibility" {
				_, err := c.Pool.Exec(t.Context(), `UPDATE document_deletions SET claim=$2,claim_until=clock_timestamp()+interval '1 minute'
					WHERE document_id=$1`, string(id), uuid.NewString())
				require.NoError(t, err)
			}
			before := snapshotHeldClaim(t.Context(), t, c, id)
			failure := errors.New("claim statement failed")
			var keys []string
			ops := heldSourceOps(t, &keys)
			if tc.fault == "selection" {
				incarnation = uuid.NewString()
			}
			if tc.fault == "target" {
				ops.VectorIndex = "different-index"
			}
			commits := 0
			var commitErr error
			db := heldClaimTestDB{
				begin: func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
					if tc.fault == "begin" {
						return nil, failure
					}
					tx, err := c.Pool.BeginTx(ctx, opts)
					require.NoError(t, err)
					return heldClaimTestTx{Tx: tx, commit: func(ctx context.Context, tx pgx.Tx) error {
						commits++
						_, err := tx.Exec(ctx, "SELECT 1/0")
						requireSQLState(t, err, "22012")
						err = tx.Commit(ctx)
						require.ErrorIs(t, err, pgx.ErrTxCommitRollback)
						commitErr = fmt.Errorf("commit rejected: %w", err)
						return commitErr
					}, exec: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
						if strings.HasPrefix(sql, "UPDATE document_deletions SET claim=") {
							switch tc.fault {
							case "update":
								return pgconn.CommandTag{}, failure
							case "row count":
								return pgconn.NewCommandTag("UPDATE 0"), nil
							}
						}
						return tx.Exec(ctx, sql, args...)
					}}, nil
				},
				exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					t.Error("non-ambiguous claim failure attempted cleanup")
					return pgconn.CommandTag{}, failure
				},
			}
			err := (heldRelease{db: db}).release(t.Context(), id, incarnation, ops)
			want := tc.want
			if want == nil {
				want = failure
			}
			require.ErrorIs(t, err, want)
			if tc.fault == "commit" {
				require.Equal(t, 1, commits)
				require.Same(t, commitErr, err)
			} else {
				require.Zero(t, commits)
			}
			require.Empty(t, keys)
			require.Equal(t, before, snapshotHeldClaim(t.Context(), t, c, id))
		})
	}
}

func TestHeldClaimCleanupPreservesOtherOwnership(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	reindex, role := provisionReindexRole(t, owner)
	grantExec(t, owner, replaceReindexRole(readPublishedReindexGrantBlocks(t).base, role, ""))

	for _, tc := range []string{"other token", "other incarnation", "other document"} {
		t.Run(tc, func(t *testing.T) {
			id, incarnation, _ := pendingHeldSource(t, owner)
			onOtherDocument, _, _ := pendingHeldSource(t, owner)
			before := snapshotHeldClaim(t.Context(), t, owner, id)
			lostReply := errors.New("claim commit reply lost")
			var abandoned string
			var preserved heldClaimRow
			var preservedID domain.DocumentID
			h := interceptFirstHeldReleaseCommit(heldRelease{db: serving.Pool}, func(ctx context.Context, tx pgx.Tx) error {
				require.NoError(t, tx.Commit(ctx))
				claimed := snapshotHeldClaim(ctx, t, owner, id)
				require.NotNil(t, claimed.claim)
				abandoned = *claimed.claim
				require.Equal(t, before.payload, claimed.payload)
				preservedID = id

				// Apply a concurrent ownership change after COMMIT and before the
				// intercepted outcome returns to the releasing caller.
				var err error
				switch tc {
				case "other token":
					_, err = owner.Pool.Exec(ctx, `UPDATE document_deletions SET claim=$2,claim_until=clock_timestamp()+interval '1 minute'
						WHERE document_id=$1`, string(id), uuid.NewString())
				case "other incarnation":
					_, err = owner.Pool.Exec(ctx, `UPDATE document_deletions SET incarnation=$2,source_key=$3,vector_index=$4
						WHERE document_id=$1`, string(id), uuid.NewString(), "source/reused", "other-index")
				case "other document":
					preservedID = onOtherDocument
					_, err = owner.Pool.Exec(ctx, `UPDATE document_deletions SET claim=$2,claim_until=clock_timestamp()+interval '1 minute',
						source_key=$3,vector_index=$4 WHERE document_id=$1`, string(preservedID), abandoned, "source/other", "other-index")
				}
				require.NoError(t, err)
				preserved = snapshotHeldClaim(ctx, t, owner, preservedID)
				return lostReply
			})
			var keys []string
			require.Same(t, lostReply, h.release(t.Context(), id, incarnation, heldSourceOps(t, &keys)))
			require.Empty(t, keys)
			require.Equal(t, preserved, snapshotHeldClaim(t.Context(), t, owner, preservedID))
			if preservedID != id {
				require.Equal(t, before, snapshotHeldClaim(t.Context(), t, owner, id))
			}

			// The reindex role can inspect receipts but cannot abandon claims.
			var preservedIncarnation, preservedToken string
			require.NoError(t, reindex.Pool.QueryRow(t.Context(), `SELECT incarnation::text,claim::text
				FROM document_deletions WHERE document_id=$1`, string(preservedID)).Scan(&preservedIncarnation, &preservedToken))
			requireSQLState(t, clearHeldDeletionClaim(t.Context(), reindex.Pool, preservedID, preservedIncarnation, preservedToken), "42501")
			require.NoError(t, clearHeldDeletionClaim(t.Context(), serving.Pool, id, incarnation, abandoned))
			require.Equal(t, preserved, snapshotHeldClaim(t.Context(), t, owner, preservedID))
			if preservedID != id {
				require.Equal(t, before, snapshotHeldClaim(t.Context(), t, owner, id))
			}
		})
	}

	t.Run("both attempts fail then claim expires", func(t *testing.T) {
		id, incarnation, key := pendingHeldSource(t, owner)
		before := snapshotHeldClaim(t.Context(), t, owner, id)
		h, _, closeGuard := dedicatedHeldClaim(t, owner)
		lostReply := errors.New("claim commit reply lost")
		var persisted heldClaimRow
		h = interceptFirstHeldReleaseCommit(h, func(ctx context.Context, tx pgx.Tx) error {
			require.NoError(t, tx.Commit(ctx))
			persisted = snapshotHeldClaim(ctx, t, owner, id)
			require.NotNil(t, persisted.claim)
			_, err := uuid.Parse(*persisted.claim)
			require.NoError(t, err)
			return lostReply
		})
		unavailable := errors.New("claim cleanup unavailable")
		clears, connects := 0, 0
		h.db = heldClaimTestDB{heldReleaseDatabase: h.db, exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			clears++
			return pgconn.CommandTag{}, unavailable
		}}
		h.claimConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
			connects++
			return nil, unavailable
		}
		var keys []string
		ops := heldSourceOps(t, &keys)
		require.Same(t, lostReply, h.release(t.Context(), id, incarnation, ops))
		closeGuard()
		require.Equal(t, 1, clears)
		require.Positive(t, connects)
		require.Equal(t, before.payload, persisted.payload)
		require.NotNil(t, persisted.until)
		require.Equal(t, persisted, snapshotHeldClaim(t.Context(), t, owner, id))
		require.ErrorIs(t, owner.ReleaseLegacyHostedHold(t.Context(), id, incarnation, ops), domain.ErrDeletionBusy)
		require.Empty(t, keys)

		// Advance the persisted lease past its boundary without a wall-clock wait.
		_, err := owner.Pool.Exec(t.Context(), `UPDATE document_deletions SET claim_until=clock_timestamp()-interval '1 second'
			WHERE document_id=$1 AND incarnation=$2 AND claim=$3`, string(id), incarnation, *persisted.claim)
		require.NoError(t, err)
		require.NoError(t, owner.ReleaseLegacyHostedHold(t.Context(), id, incarnation, ops))
		require.Equal(t, []string{key}, keys)
		require.Nil(t, snapshotHeldClaim(t.Context(), t, owner, id).claim)
	})
}

func TestLegacyHostedReleaseChecksLostFinalReply(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	id := domain.DocumentID(uuid.NewString())
	hold, err := c.QuarantineDeletion(ctx, id, domain.QuarantineTarget{VectorMode: legacyVectors, VectorIndex: "retired-index"})
	require.NoError(t, err)
	guard, closeGuard, err := c.AcquireHeldRecoveryWriter(ctx)
	require.NoError(t, err)
	defer closeGuard()
	claim := uuid.NewString()
	ops := domain.DeleteOperations{VectorIndex: hold.VectorIndex}
	h := heldRelease{db: c.Pool, outcome: guard, legacyOnly: true}
	_, uncertainCommit, err := h.claimHeldDeletion(ctx, id, hold.Incarnation, claim, time.Minute, ops)
	require.NoError(t, err)
	require.False(t, uncertainCommit)
	h = interceptFirstHeldReleaseCommit(h, func(ctx context.Context, tx pgx.Tx) error {
		require.NoError(t, tx.Commit(ctx))
		return errors.New("final release reply lost")
	})
	require.NoError(t, h.finalizeRelease(ctx, id, hold.Incarnation, claim))
	r := readReceipt(t, c, id)
	require.True(t, r.finalized)
	require.False(t, r.held)
	require.Nil(t, r.claim)
}
