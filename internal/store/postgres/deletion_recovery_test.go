package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
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
