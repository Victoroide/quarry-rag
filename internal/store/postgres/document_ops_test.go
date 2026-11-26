package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func (f *ownedFixture) grant(t *testing.T, recipient domain.Principal, docID, incarnation string, read, download bool) {
	t.Helper()
	f.importBatch(t, func(b map[string]any) {
		b["grants"] = []grantEntry{{
			"tenant_id":    recipient.TenantID,
			"principal_id": recipient.ID,
			"document_id":  docID,
			"incarnation":  incarnation,
			"can_read":     read,
			"can_download": download,
		}}
	})
}

func (f *ownedFixture) acceptOwned(t *testing.T, owner domain.Principal, collection, key string) string {
	t.Helper()
	reservedID, incarnation := f.reserve(t, owner, collection)
	req := suppliedOwnedRequest(owner, collection, "content "+reservedID, reservedID, incarnation)
	req.SupportsScopedVectors = true
	if key != "" {
		req.Document.S3Key = &key
	}
	res := f.accept(t, req)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)
	return reservedID
}

func TestScopedListingAndStatusEnforceTenantGrantsAndState(t *testing.T) {
	f := newOwnedFixture(t)

	// Create owned documents in failed, completed and processing states, with S3 keys.
	failedID := f.acceptOwned(t, f.owner, "team", "documents/doc/f/fail.pdf")
	completedID := f.acceptOwned(t, f.owner, "team", "documents/doc/c/done.pdf")
	peerID := f.acceptOwned(t, f.peer, "", "documents/doc/peer/secret.pdf")
	f.setCreate(t, f.outsider, "team", true)
	outsiderID := f.acceptOwned(t, f.outsider, "team", "documents/doc/out/secret.pdf")

	// Move failedID to failed with an internal error, and complete completedID + peerID + outsiderID.
	for range 4 {
		job := f.claim(t)
		switch string(job.Document.ID) {
		case failedID:
			require.NoError(t, f.c.FailJob(t.Context(), *job, "s3: secret/bucket/key: timeout"))
		case completedID, peerID, outsiderID:
			ok, err := f.c.CompleteJob(t.Context(), *job, []domain.Chunk{chunkFor(job.Document.ID, 0)}, domain.IndexObservation{})
			require.NoError(t, err)
			require.True(t, ok)
		default:
			t.Fatalf("unexpected claimed job %s", job.Document.ID)
		}
	}
	processingID := f.acceptOwned(t, f.owner, "", "documents/doc/p/orig.pdf")

	// Add an unassigned legacy document and a quarantined (unverified) imported document for owner.
	unassignedID := uuid.NewString()
	grantExec(t, f.c, `INSERT INTO documents(id,content,status,s3_key) VALUES($1,'legacy','completed','secret/legacy.pdf')`, unassignedID)
	quarantinedID, _ := f.legacyDocument(t, 1)

	// Grant peer read-only access on completedID.
	completedInc := storedIncarnation(t, f.c, domain.DocumentID(completedID))
	f.grant(t, f.peer, completedID, completedInc, true, false)

	// Owner sees processingID, failedID, completedID (NOT peerID, outsiderID, unassignedID, quarantinedID).
	page, err := f.c.ListScopedDocumentsPage(t.Context(), f.owner, "", 10, "", true)
	require.NoError(t, err)
	require.True(t, page.Active)
	gotIDs := map[string]domain.DocumentSummary{}
	for _, d := range page.Documents {
		require.Nil(t, d.S3Key, "scoped listing never returns raw S3 keys")
		gotIDs[d.ID] = d
	}
	require.Len(t, gotIDs, 3)
	require.Contains(t, gotIDs, processingID)
	require.Contains(t, gotIDs, failedID)
	require.Contains(t, gotIDs, completedID)
	require.Equal(t, completedInc, gotIDs[completedID].Incarnation)

	// Collection filter: "" searches across tenant, "team" narrows to failedID + completedID.
	teamPage, err := f.c.ListScopedDocumentsPage(t.Context(), f.owner, "", 10, "team", false)
	require.NoError(t, err)
	require.Len(t, teamPage.Documents, 2)
	for _, d := range teamPage.Documents {
		require.Empty(t, d.Incarnation)
	}

	// Peer sees peerID (owner) and completedID (read grant), even when filtering collection="".
	peerPage, err := f.c.ListScopedDocumentsPage(t.Context(), f.peer, "", 10, "", false)
	require.NoError(t, err)
	require.Len(t, peerPage.Documents, 2)

	// Arbitrary cursor set to hidden outsiderID acts purely as exclusive comparison.
	afterOutsider, err := f.c.ListScopedDocumentsPage(t.Context(), f.owner, outsiderID, 10, "", false)
	require.NoError(t, err)
	for _, d := range afterOutsider.Documents {
		require.Greater(t, d.ID, outsiderID)
	}

	// Status lookup: owner sees all three, peer sees completedID, neither sees unassigned/quarantined/outsider.
	for _, id := range []string{processingID, failedID, completedID} {
		st, stErr := f.c.GetScopedDocumentSummary(t.Context(), f.owner, domain.DocumentID(id), true)
		require.NoError(t, stErr)
		require.True(t, st.Active)
		require.NotNil(t, st.Document)
		require.Nil(t, st.Document.S3Key)
	}
	for _, hidden := range []string{peerID, outsiderID, unassignedID, quarantinedID, uuid.NewString()} {
		st, stErr := f.c.GetScopedDocumentSummary(t.Context(), f.owner, domain.DocumentID(hidden), true)
		require.NoError(t, stErr)
		require.True(t, st.Active)
		require.Nil(t, st.Document)
	}

	// Disabled principal returns Active=false and zero rows.
	disPage, err := f.c.ListScopedDocumentsPage(t.Context(), f.disabled, "", 10, "", false)
	require.NoError(t, err)
	require.False(t, disPage.Active)
	require.Empty(t, disPage.Documents)
	disStatus, err := f.c.GetScopedDocumentSummary(t.Context(), f.disabled, domain.DocumentID(completedID), false)
	require.NoError(t, err)
	require.False(t, disStatus.Active)
	require.Nil(t, disStatus.Document)
}

func TestScopedFileAuthorizationAndDeletionLocking(t *testing.T) {
	f := newOwnedFixture(t)
	_, err := f.c.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)

	docID := f.acceptOwned(t, f.owner, "", "documents/doc/owned/original.pdf")
	incarnation := storedIncarnation(t, f.c, domain.DocumentID(docID))
	noKeyID := f.acceptOwned(t, f.owner, "", "")

	// Read-only grant is denied signing; adding download grant enables signing.
	f.grant(t, f.peer, docID, incarnation, true, false)
	var signed []string
	res, err := f.c.WithScopedDocumentFile(t.Context(), f.peer, domain.DocumentID(docID), func(_ context.Context, key string) error {
		signed = append(signed, key)
		return nil
	})
	require.NoError(t, err)
	require.True(t, res.Active)
	require.False(t, res.Found)
	require.Empty(t, signed)

	f.grant(t, f.peer, docID, incarnation, true, true)
	res, err = f.c.WithScopedDocumentFile(t.Context(), f.peer, domain.DocumentID(docID), func(_ context.Context, key string) error {
		signed = append(signed, key)
		return nil
	})
	require.NoError(t, err)
	require.True(t, res.Active)
	require.True(t, res.Found)
	require.Equal(t, []string{"documents/doc/owned/original.pdf"}, signed)

	// Missing key returns Active=true, Found=false without calling sign.
	res, err = f.c.WithScopedDocumentFile(t.Context(), f.owner, domain.DocumentID(noKeyID), func(context.Context, string) error {
		t.Fatal("sign called with nil s3_key")
		return nil
	})
	require.NoError(t, err)
	require.True(t, res.Active)
	require.False(t, res.Found)

	// Outsider and disabled principal are refused without calling sign.
	res, err = f.c.WithScopedDocumentFile(t.Context(), f.outsider, domain.DocumentID(docID), func(context.Context, string) error {
		t.Fatal("sign called for outsider")
		return nil
	})
	require.NoError(t, err)
	require.True(t, res.Active)
	require.False(t, res.Found)

	res, err = f.c.WithScopedDocumentFile(t.Context(), f.disabled, domain.DocumentID(docID), func(context.Context, string) error {
		t.Fatal("sign called for disabled principal")
		return nil
	})
	require.NoError(t, err)
	require.False(t, res.Active)
	require.False(t, res.Found)

	// Non-owner grantee (even with read+download) cannot delete.
	rec := newDeletionRecorder()
	ops := rec.ops("")
	ops.InMemory, ops.MemoryOwner = true, uuid.NewString()
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.peer, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)
	require.Empty(t, rec.all())

	// Outsider cannot delete.
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.outsider, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)

	// Owner deletion succeeds across partial failure and retry, and rejects signing once deleting.
	rec.hook("source", failFirst("s3 timeout"))
	resolvedInc, complete, err := f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
	require.NoError(t, err)
	require.False(t, complete)
	require.Equal(t, incarnation, resolvedInc)

	// Once intent commits, file signing sees status='deleting' and refuses.
	res, err = f.c.WithScopedDocumentFile(t.Context(), f.owner, domain.DocumentID(docID), func(context.Context, string) error {
		t.Fatal("sign called after deletion intent committed")
		return nil
	})
	require.NoError(t, err)
	require.False(t, res.Found)

	err = f.c.ResumeScopedDeletion(t.Context(), domain.DocumentID(docID), resolvedInc, ops)
	require.ErrorContains(t, err, "s3 timeout")

	// Non-owner still gets 404 while status='deleting'.
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.peer, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)

	// Owner retries and finishes cleanup without repeating completed vector step.
	rec.hook("source", nil)
	resolvedInc, complete, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
	require.NoError(t, err)
	require.False(t, complete)
	require.NoError(t, f.c.ResumeScopedDeletion(t.Context(), domain.DocumentID(docID), resolvedInc, ops))
	require.Equal(t, 1, rec.count("revision"), "completed vector step is not repeated on retry")
	require.Equal(t, 2, rec.count("source"))

	// Qualified retry after finalization acknowledges only the original owner.
	opsWithInc := ops
	opsWithInc.ExpectedIncarnation = incarnation
	_, complete, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), domain.DocumentID(docID), opsWithInc)
	require.NoError(t, err)
	require.True(t, complete)

	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.peer, time.Now().Add(time.Minute), domain.DocumentID(docID), opsWithInc)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)

	// Unqualified retry after finalization returns 404.
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)

	// Replacement document reusing the same UUID gets a new incarnation and is safe from the old intent.
	repReq := jobsRequest("replacement")
	repReq.Document.ID, repReq.CallerSupplied, repReq.SupportsScopedVectors = domain.DocumentID(docID), true, true
	require.Equal(t, domain.AcceptAccepted, f.accept(t, repReq).Outcome)
	newInc := storedIncarnation(t, f.c, domain.DocumentID(docID))
	require.NotEqual(t, incarnation, newInc)
	f.importBatch(t, func(b map[string]any) {
		b["assignments"] = []grantEntry{grantAssignment(t, f.c, f.otherTenant, f.outsider.ID, docID)}
	})
	grantExec(t, f.c, `UPDATE grant_catalog_document_ownership SET ready=true WHERE document_id=$1`, docID)

	// Stale resume of finalized old incarnation acknowledges without touching stores or replacement, and unstarted new incarnation returns 404.
	beforeCalls := len(rec.all())
	require.NoError(t, f.c.ResumeScopedDeletion(t.Context(), domain.DocumentID(docID), incarnation, ops))
	require.Len(t, rec.all(), beforeCalls)
	require.ErrorIs(t, f.c.ResumeScopedDeletion(t.Context(), domain.DocumentID(docID), newInc, ops), domain.ErrDocumentNotFound)

	repStatus, repErr := f.c.GetScopedDocumentSummary(t.Context(), f.outsider, domain.DocumentID(docID), true)
	require.NoError(t, repErr)
	require.NotNil(t, repStatus.Document)
	require.Equal(t, newInc, repStatus.Document.Incarnation)

	// Old owner's qualified retry with old incarnation gets 404; new owner gets 412.
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), domain.DocumentID(docID), opsWithInc)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.outsider, time.Now().Add(time.Minute), domain.DocumentID(docID), opsWithInc)
	require.ErrorIs(t, err, domain.ErrDeletionPreconditionFailed)
}

func TestScopedDeletionCancellationBeforeCommitLeavesDocumentIntact(t *testing.T) {
	f := newOwnedFixture(t)
	docID := f.acceptOwned(t, f.owner, "", "documents/doc/cancel/orig.pdf")
	rec := newDeletionRecorder()
	ops := rec.ops("")
	ops.InMemory, ops.MemoryOwner = true, uuid.NewString()

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := f.c.BeginScopedDeletion(canceled, f.owner, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, rec.all())

	st, err := f.c.GetScopedDocumentSummary(t.Context(), f.owner, domain.DocumentID(docID), false)
	require.NoError(t, err)
	require.NotNil(t, st.Document)
	require.NotEqual(t, "deleting", st.Document.Status)

	// Expired credential also aborts before commit.
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(-time.Second), domain.DocumentID(docID), ops)
	require.True(t, errors.Is(err, domain.ErrUnauthorized))
	require.Empty(t, rec.all())
}

func TestScopedDeletionIntentWaitsForServiceSigning(t *testing.T) {
	f := newOwnedFixture(t)
	_, err := f.c.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	other := otherConnection(t, f.c)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	const expectedKey = "documents/doc/owned/original.pdf"
	docID := f.acceptOwned(t, f.owner, "", expectedKey)

	inside, finish := make(chan struct{}), make(chan struct{})
	signing := make(chan error, 1)
	signed := make(chan string, 1)
	go func() {
		found, signErr := f.c.WithDocumentFile(ctx, domain.DocumentID(docID), func(_ context.Context, key string) error {
			signed <- key
			close(inside)
			<-finish
			return nil
		})
		if signErr == nil && !found {
			signErr = errors.New("expected signable document")
		}
		signing <- signErr
	}()
	select {
	case <-inside:
	case signErr := <-signing:
		t.Fatalf("signing ended before its callback ran: %v", signErr)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	rec := newDeletionRecorder()
	ops := rec.ops("")
	ops.InMemory, ops.MemoryOwner = true, uuid.NewString()
	type beginResult struct {
		complete bool
		err      error
	}
	begun := make(chan beginResult, 1)
	go func() {
		_, complete, beginErr := other.BeginScopedDeletion(ctx, f.owner, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
		begun <- beginResult{complete: complete, err: beginErr}
	}()
	waitForAdvisoryWaiter(t, other, "ExclusiveLock")
	require.NotEqual(t, "deleting", documentStatus(t, f.c, domain.DocumentID(docID)))

	close(finish)
	require.NoError(t, <-signing)
	require.Equal(t, expectedKey, <-signed)
	res := <-begun
	require.NoError(t, res.err)
	require.False(t, res.complete)
	require.Equal(t, "deleting", documentStatus(t, f.c, domain.DocumentID(docID)))

	found, err := f.c.WithDocumentFile(ctx, domain.DocumentID(docID), func(context.Context, string) error {
		t.Error("signed after the scoped deletion intent committed")
		return nil
	})
	require.NoError(t, err)
	require.False(t, found)
}
