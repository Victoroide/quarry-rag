package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func noopFileSign(context.Context, string) error { return nil }

func unexpectedScopedFileSignature(context.Context, string) error {
	return errors.New("unauthorized source reached signer")
}

func fileImportBody(t *testing.T, f *ownedFixture, changes map[string]any) string {
	t.Helper()
	var revision int64
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT revision FROM grant_catalog_policy_revision`).Scan(&revision))
	batch := testGrantBatch(revision)
	for group, entries := range changes {
		batch[group] = entries
	}
	raw, err := json.Marshal(batch)
	require.NoError(t, err)
	return string(raw)
}

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

func (f *ownedFixture) acceptOwned(t testing.TB, owner domain.Principal, collection, key string) string {
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

func TestScopedFileAdvisoryAuthorization(t *testing.T) {
	f := newOwnedFixture(t)
	const source = "documents/original.pdf"
	doc := f.acceptOwned(t, f.owner, "", source)
	noSource := f.acceptOwned(t, f.owner, "", "")
	deleting := f.acceptOwned(t, f.owner, "", source)
	stale := f.acceptOwned(t, f.owner, "", source)
	pending := jobsRequest("historical file awaiting verification")
	pending.Document.S3Key = new(source)
	require.Equal(t, domain.AcceptAccepted, f.accept(t, pending).Outcome)
	f.importBatch(t, func(b map[string]any) {
		b["assignments"] = []grantEntry{grantAssignment(t, f.c, f.tenant, f.owner.ID, string(pending.Document.ID))}
	})
	grantExec(t, f.c, `UPDATE documents SET status='deleting' WHERE id=$1`, deleting)
	// Model stale ownership from a previous incarnation without changing the
	// live document's immutable identity.
	older := uuid.NewString()
	grantExec(t, f.c, `INSERT INTO grant_catalog_assigned_incarnations VALUES ($1,$2)`, stale, older)
	grantExec(t, f.c, `UPDATE grant_catalog_document_ownership SET incarnation=$2 WHERE document_id=$1`, stale, older)

	inc := storedIncarnation(t, f.c, domain.DocumentID(doc))
	f.grant(t, f.peer, doc, inc, true, true)
	readOnly := f.acceptOwned(t, f.owner, "", source)
	f.grant(t, f.peer, readOnly, storedIncarnation(t, f.c, domain.DocumentID(readOnly)), true, false)
	for _, mode := range []string{"legacy", "checkpointed"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "checkpointed" {
				grantExec(t, f.c, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
			}
			for _, tc := range []struct {
				name, id      string
				principal     domain.Principal
				active, found bool
			}{
				{"owner", doc, f.owner, true, true},
				{"reader with download", doc, f.peer, true, true},
				{"canonical UUIDs", strings.ToUpper(doc), domain.Principal{TenantID: strings.ToUpper(f.tenant), ID: strings.ToUpper(f.peer.ID)}, true, true},
				{"read alone", readOnly, f.peer, true, false},
				{"missing principal", doc, domain.Principal{TenantID: f.tenant, ID: uuid.NewString()}, false, false},
				{"disabled", doc, f.disabled, false, false},
				{"foreign tenant", doc, f.outsider, true, false},
				{"wrong tenant for principal", doc, domain.Principal{TenantID: f.otherTenant, ID: f.owner.ID}, false, false},
				{"missing document", uuid.NewString(), f.owner, true, false},
				{"missing source", noSource, f.owner, true, false},
				{"pending assignment", string(pending.Document.ID), f.owner, true, false},
				{"mismatched incarnation", stale, f.owner, true, false},
				{"deleting", deleting, f.owner, true, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var keys []string
					res, err := f.c.WithScopedDocumentFile(t.Context(), tc.principal, domain.DocumentID(tc.id), func(_ context.Context, key string) error {
						keys = append(keys, key)
						return nil
					})
					require.NoError(t, err)
					require.Equal(t, domain.ScopedFileResult{Active: tc.active, Found: tc.found}, res)
					if tc.found {
						require.Equal(t, []string{source}, keys)
					} else {
						require.Empty(t, keys)
					}
				})
			}
		})
	}
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

func TestScopedDeleteRequiresObservedIncarnation(t *testing.T) {
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
	ops := rec.ops(incarnation)
	ops.InMemory, ops.MemoryOwner = true, uuid.NewString()
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.peer, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)
	require.Empty(t, rec.all())

	// Outsider cannot delete.
	_, _, err = f.c.BeginScopedDeletion(t.Context(), f.outsider, time.Now().Add(time.Minute), domain.DocumentID(docID), ops)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)

	// Missing and wrong qualifiers preserve the live document and create no intent.
	id := domain.DocumentID(docID)
	for _, tc := range []struct {
		qualifier string
		want      error
	}{{"", domain.ErrDeletionPreconditionRequired}, {uuid.NewString(), domain.ErrDeletionPreconditionFailed}} {
		refused := ops
		refused.ExpectedIncarnation = tc.qualifier
		_, _, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), id, refused)
		require.ErrorIs(t, err, tc.want)
		require.NotEqual(t, "deleting", documentStatus(t, f.c, id))
		require.Zero(t, receiptCount(t.Context(), t, f.c, id))
	}
	require.Empty(t, rec.all())

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
	beforeRetry := rec.all()
	open := readReceipt(t, f.c, id)
	for _, tc := range []struct {
		qualifier string
		want      error
	}{{"", domain.ErrDeletionPreconditionRequired}, {uuid.NewString(), domain.ErrDeletionPreconditionFailed}} {
		refused := ops
		refused.ExpectedIncarnation = tc.qualifier
		_, _, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), id, refused)
		require.ErrorIs(t, err, tc.want)
	}
	require.Equal(t, open, readReceipt(t, f.c, id))
	require.Equal(t, beforeRetry, rec.all())

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

	// Only the active original owner can acknowledge a matching finalized receipt.
	beforeCalls := rec.all()
	for _, tc := range []struct {
		principal domain.Principal
		qualifier string
		want      error
	}{
		{f.owner, incarnation, nil},
		{f.owner, "", domain.ErrDocumentNotFound},
		{f.owner, uuid.NewString(), domain.ErrDocumentNotFound},
		{f.peer, incarnation, domain.ErrDocumentNotFound},
		{f.outsider, incarnation, domain.ErrDocumentNotFound},
		{f.disabled, incarnation, domain.ErrForbidden},
	} {
		attempt := ops
		attempt.ExpectedIncarnation = tc.qualifier
		_, complete, err = f.c.BeginScopedDeletion(t.Context(), tc.principal, time.Now().Add(time.Minute), id, attempt)
		require.ErrorIs(t, err, tc.want)
		require.Equal(t, tc.want == nil, complete)
	}
	f.setEnabled(t, f.owner, "owner", false)
	_, complete, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), id, ops)
	require.ErrorIs(t, err, domain.ErrForbidden)
	require.False(t, complete)
	f.setEnabled(t, f.owner, "owner", true)
	require.Equal(t, beforeCalls, rec.all())

	// Test reuse by the same owner and a different tenant's owner in turn.
	for _, replacementOwner := range []domain.Principal{f.owner, f.outsider} {
		newInc := replaceScopedDocument(t, f, id, replacementOwner, new("documents/doc/replacement/original.pdf"))
		require.NotEqual(t, incarnation, newInc)
		unqualified := ops
		unqualified.ExpectedIncarnation = ""
		for _, attempt := range []domain.DeleteOperations{ops, unqualified} {
			_, _, err = f.c.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), id, attempt)
			switch {
			case replacementOwner != f.owner:
				require.ErrorIs(t, err, domain.ErrDocumentNotFound)
			case attempt.ExpectedIncarnation == "":
				require.ErrorIs(t, err, domain.ErrDeletionPreconditionRequired)
			default:
				require.ErrorIs(t, err, domain.ErrDeletionPreconditionFailed)
			}
		}
		_, _, err = f.c.BeginScopedDeletion(t.Context(), replacementOwner, time.Now().Add(time.Minute), id, ops)
		require.ErrorIs(t, err, domain.ErrDeletionPreconditionFailed)
		require.NoError(t, f.c.ResumeScopedDeletion(t.Context(), id, incarnation, ops))
		require.ErrorIs(t, f.c.ResumeScopedDeletion(t.Context(), id, newInc, ops), domain.ErrDocumentNotFound)
		require.Equal(t, beforeCalls, rec.all())
		repStatus, repErr := f.c.GetScopedDocumentSummary(t.Context(), replacementOwner, id, true)
		require.NoError(t, repErr)
		require.NotNil(t, repStatus.Document)
		require.Equal(t, newInc, repStatus.Document.Incarnation)
		ownerFile, err := f.c.WithScopedDocumentFile(t.Context(), replacementOwner, id, noopFileSign)
		require.NoError(t, err)
		require.Equal(t, domain.ScopedFileResult{Active: true, Found: true}, ownerFile)
		peerFile, err := f.c.WithScopedDocumentFile(t.Context(), f.peer, id, unexpectedScopedFileSignature)
		require.NoError(t, err)
		require.Equal(t, domain.ScopedFileResult{Active: true}, peerFile, "a prior grant cannot follow a reused UUID")
		require.Equal(t, 1, receiptCount(t.Context(), t, f.c, id))
		if replacementOwner == f.owner {
			require.NoError(t, f.c.DeleteDocument(t.Context(), id))
		}
	}
}

func replaceScopedDocument(t *testing.T, f *ownedFixture, id domain.DocumentID, owner domain.Principal, source *string) string {
	t.Helper()
	req := jobsRequest("replacement " + uuid.NewString())
	req.Document.S3Key = source
	req.Document.ID, req.CallerSupplied, req.SupportsScopedVectors = id, true, true
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	f.importBatch(t, func(b map[string]any) {
		b["assignments"] = []grantEntry{grantAssignment(t, f.c, owner.TenantID, owner.ID, string(id))}
	})
	grantExec(t, f.c, `UPDATE grant_catalog_document_ownership SET ready=true WHERE document_id=$1`, string(id))
	return storedIncarnation(t, f.c, id)
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
	ops := rec.ops(storedIncarnation(t, f.c, domain.DocumentID(docID)))
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

func TestScopedDeleteQualifierSurvivesRetryAndReuse(t *testing.T) {
	f := provisionOwnedFixture(t, activatedDatabase(t))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	id := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/doc/retry/original.pdf"))
	inc := storedIncarnation(t, f.c, id)
	rec := newDeletionRecorder()
	ops := rec.ops(inc)
	begin := func(ctx context.Context, validUntil time.Time) (bool, error) {
		_, complete, err := f.c.BeginScopedDeletion(ctx, f.owner, validUntil, id, ops)
		return complete, err
	}
	attemptAsync := func(ctx context.Context, validUntil time.Time) <-chan error {
		result := make(chan error, 1)
		go func() {
			_, err := begin(ctx, validUntil)
			result <- err
		}()
		return result
	}

	t.Run("cancel during marker lookup", func(t *testing.T) {
		subCtx, cancelSubtest := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancelSubtest()
		tx, err := f.c.Pool.Begin(subCtx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.WithoutCancel(subCtx)) }()
		_, err = tx.Exec(subCtx, `LOCK TABLE document_deletion_activation IN ACCESS EXCLUSIVE MODE`)
		require.NoError(t, err)
		request, cancelRequest := context.WithCancel(subCtx)
		defer cancelRequest()
		result := attemptAsync(request, time.Now().Add(time.Minute))
		waitForLockWaiter(t, f.c, "SELECT COALESCE(document_deletion_active()")
		cancelRequest()
		require.ErrorIs(t, <-result, context.Canceled)
	})
	t.Run("policy revoked before owner lookup", func(t *testing.T) {
		subCtx, cancelSubtest := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancelSubtest()
		tx, err := f.c.Pool.Begin(subCtx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.WithoutCancel(subCtx)) }()
		require.NoError(t, lockDocumentIntent(subCtx, tx, id))
		result := attemptAsync(subCtx, time.Now().Add(time.Minute))
		waitForAdvisoryWaiter(t, f.c, "ExclusiveLock")
		f.setEnabled(t, f.owner, "owner", false)
		require.NoError(t, tx.Commit(subCtx))
		require.ErrorIs(t, <-result, domain.ErrForbidden)
		f.setEnabled(t, f.owner, "owner", true)
	})
	t.Run("credentials expire before intent", func(t *testing.T) {
		subCtx, cancelSubtest := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancelSubtest()
		tx, err := f.c.Pool.Begin(subCtx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.WithoutCancel(subCtx)) }()
		require.NoError(t, lockDocumentIntent(subCtx, tx, id))
		expires := time.Now().Add(5 * time.Second)
		result := attemptAsync(subCtx, expires)
		waitForAdvisoryWaiter(t, f.c, "ExclusiveLock")
		select {
		case <-time.After(time.Until(expires)):
		case <-subCtx.Done():
			t.Fatal(subCtx.Err())
		}
		require.NoError(t, tx.Commit(subCtx))
		require.ErrorIs(t, <-result, domain.ErrUnauthorized)
	})
	require.Zero(t, receiptCount(ctx, t, f.c, id))
	require.NotEqual(t, "deleting", documentStatus(t, f.c, id))
	require.Empty(t, rec.all())

	complete, err := begin(ctx, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.False(t, complete)
	stale := uuid.NewString()
	grantExec(t, f.c, `UPDATE document_deletions SET claim=$2,claim_until=clock_timestamp()+interval '1 hour' WHERE document_id=$1`, string(id), stale)
	require.ErrorIs(t, f.c.ResumeScopedDeletion(ctx, id, inc, ops), domain.ErrDeletionBusy)
	require.Empty(t, rec.all())
	grantExec(t, f.c, `UPDATE document_deletions SET claim_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(id))
	rec.hook("source", failFirst("source unavailable"))
	require.ErrorContains(t, f.c.ResumeScopedDeletion(ctx, id, inc, ops), "source unavailable")
	require.ErrorIs(t, f.c.checkpointDeletion(ctx, id, inc, stale, stepNamespace), domain.ErrDeletionBusy)
	complete, err = begin(ctx, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.False(t, complete)
	require.NoError(t, f.c.ResumeScopedDeletion(ctx, id, inc, ops))

	// Reuse the UUID while the qualified retry is waiting to read it.
	tx, err := f.c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	require.NoError(t, lockDocumentIntent(ctx, tx, id))
	result := attemptAsync(ctx, time.Now().Add(time.Minute))
	waitForAdvisoryWaiter(t, f.c, "ExclusiveLock")
	newInc := replaceScopedDocument(t, f, id, f.owner, nil)
	require.NoError(t, tx.Commit(ctx))
	require.ErrorIs(t, <-result, domain.ErrDeletionPreconditionFailed)
	require.Equal(t, newInc, storedIncarnation(t, f.c, id))
	require.NotEqual(t, "deleting", documentStatus(t, f.c, id))
	require.Equal(t, []string{
		"revision:" + string(id) + "/" + inc,
		"source:documents/doc/retry/original.pdf",
		"source:documents/doc/retry/original.pdf",
	}, rec.all())
	require.Equal(t, 1, receiptCount(ctx, t, f.c, id))
	require.True(t, readReceipt(t, f.c, id).finalized)
}
