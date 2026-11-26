package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

// A document that owns manifested revisions is deleted by exact revision on a
// node that enabled manifests, whatever its vector format. Its retired
// revisions then wait for the sweeper, which enforces the drain window.
func TestCoordinatorDeletesAManifestedDocumentByRevision(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "unscoped"
		if scoped {
			name = "scoped"
		}
		t.Run(name, func(t *testing.T) {
			c := activatedDatabase(t)
			c.EnableHostedVectorManifests(true)
			ctx := t.Context()
			doc := acceptDocument(t, c, "manifested "+name, scoped, true)
			attempt := startManifestAttempt(t, c, doc.id, "worker")

			recorder := newDeletionRecorder()
			require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
			require.Equal(t, []string{
				"manifest:" + string(doc.id) + "/" + attempt.revision + "/" + attempt.vectorID,
				"source:" + doc.key,
			}, recorder.all(), "no document-wide or namespace delete runs for a manifested document")
			receipt := readReceipt(t, c, doc.id)
			require.Equal(t, manifestVectors, receipt.mode)
			require.False(t, receipt.held, "exact deletes leave nothing unproven")
			require.True(t, receipt.finalized)
			require.Empty(t, documentStatus(t, c, doc.id))

			require.Equal(t, "deleted", cleanupReason(t, c, attempt.revision), "the deletion trigger retires the revision for the sweeper")
			held, err := c.ClaimVectorCleanup(ctx, "sweeper", time.Minute, time.Hour)
			require.NoError(t, err)
			require.Nil(t, held)
		})
	}
}

func TestCoordinatorResumesAManifestedDeletionAfterAProviderFailure(t *testing.T) {
	c := activatedDatabase(t)
	c.EnableHostedVectorManifests(true)
	ctx := t.Context()
	doc := acceptDocument(t, c, "manifested retry", false, false)
	attempt := startManifestAttempt(t, c, doc.id, "worker")

	recorder := newDeletionRecorder()
	recorder.hook("manifest", failFirst("provider outage"))
	require.ErrorContains(t, c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), "provider outage")
	require.Equal(t, "deleting", documentStatus(t, c, doc.id))
	require.False(t, readReceipt(t, c, doc.id).finalized)

	require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
	require.Equal(t, 2, recorder.count("manifest"))
	require.Empty(t, documentStatus(t, c, doc.id))
	require.Equal(t, "deleted", cleanupReason(t, c, attempt.revision))
}

func TestTransactionalDeletionRefusesUnretiredHostedManifests(t *testing.T) {
	owner, serving, roles := restrictedDatabase(t)
	ctx := t.Context()
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "SELECT (document_id, incarnation, state)"))
	_, err := owner.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	owner.EnableHostedVectorManifests(true)
	f := provisionOwnedFixture(t, owner)

	acceptTagged := func(ctx context.Context, t *testing.T, text string) domain.AcceptRequest {
		t.Helper()
		req := ownedRequest(f.owner, "", text)
		req.SupportsScopedVectors = true
		result, acceptErr := owner.AcceptDocument(ctx, req)
		require.NoError(t, acceptErr)
		require.Equal(t, domain.AcceptAccepted, result.Outcome)
		var scoped bool
		require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT scoped_vectors FROM documents WHERE id=$1`, req.Document.ID).Scan(&scoped))
		require.True(t, scoped)
		return req
	}
	active := acceptTagged(ctx, t, "published hosted revision")
	published := startManifestAttempt(t, owner, active.Document.ID, "worker")
	ok, err := owner.CompleteJob(ctx, published.job, published.chunks(active.Document.ID, active.Document.Content), domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)

	writing := acceptTagged(ctx, t, "abandoned hosted revision")
	abandoned := startManifestAttempt(t, owner, writing.Document.ID, "worker")
	expireJobLease(t, owner, writing.Document.ID)

	for _, mode := range []string{"service", "scoped"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			remove := func(req domain.AcceptRequest) error {
				id := req.Document.ID
				ops := domain.DeleteOperations{ExpectedIncarnation: req.Owner.Incarnation, TransactionalVectors: true}
				if mode == "service" {
					return serving.DeleteWithCleanup(ctx, id, ops)
				}
				incarnation, complete, err := serving.BeginScopedDeletion(ctx, f.owner, time.Now().Add(time.Minute), id, ops)
				if err != nil || complete {
					return err
				}
				return serving.ResumeScopedDeletion(ctx, id, incarnation, ops)
			}
			for _, tc := range []struct {
				req                     domain.AcceptRequest
				revision, status, state string
			}{
				{active, published.revision, "completed", "active"},
				{writing, abandoned.revision, "processing", "writing"},
			} {
				id := tc.req.Document.ID
				require.ErrorIs(t, remove(tc.req), domain.ErrVectorStorageUnavailable, tc.state)
				require.Equal(t, tc.status, documentStatus(t, owner, id))
				require.Equal(t, tc.state, manifestState(t, owner, tc.revision))
				require.Empty(t, cleanupReason(t, owner, tc.revision))
				var receipts int
				require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT count(*) FROM document_deletions WHERE document_id=$1`, id).Scan(&receipts))
				require.Zero(t, receipts)
			}
			plain := acceptTagged(ctx, t, "tagged without a hosted revision in "+mode)
			require.NoError(t, remove(plain))
			require.Empty(t, documentStatus(t, owner, plain.Document.ID))
			receipt := readReceipt(t, owner, plain.Document.ID)
			require.Equal(t, localVectors, receipt.mode)
			require.True(t, receipt.finalized)
		})
	}
}

func TestCoordinatorResumesAManifestedDeletionOnFlagOffRestrictedLogin(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	_, err := owner.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)

	flagged, flaggedRoles := publishedServingClient(t, owner)
	grantExec(t, owner, publishedServingSQL(t, flaggedRoles, "grant-catalog.md", "public.hosted_vector_manifests"))
	flagged.EnableHostedVectorManifests(true)

	doc := acceptDocument(t, flagged, "manifested retry on flag-off replica", false, true)
	attempt := startManifestAttempt(t, flagged, doc.id, "worker")
	ok, err := flagged.CompleteJob(ctx, attempt.job, attempt.chunks(doc.id, "manifested retry on flag-off replica"), domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)

	recorder := newDeletionRecorder()
	recorder.hook("manifest", failFirst("outage"))
	require.ErrorContains(t, flagged.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), "outage")
	require.Equal(t, "deleting", documentStatus(t, owner, doc.id))
	receipt := readReceipt(t, owner, doc.id)
	require.Equal(t, manifestVectors, receipt.mode)
	require.Equal(t, stepVectors, receipt.step)
	require.False(t, receipt.finalized)

	transactional := recorder.ops(doc.incarnation)
	transactional.TransactionalVectors = true
	transactional.ManifestVectors = nil
	require.ErrorIs(t, serving.DeleteWithCleanup(ctx, doc.id, transactional), domain.ErrVectorStorageUnavailable)
	require.Equal(t, receipt, readReceipt(t, owner, doc.id))

	require.NoError(t, serving.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
	require.Equal(t, 1, recorder.count("manifest"))
	require.Equal(t, 1, recorder.count("source"))
	require.Empty(t, documentStatus(t, owner, doc.id))
	finalReceipt := readReceipt(t, owner, doc.id)
	require.True(t, finalReceipt.finalized)
	require.False(t, finalReceipt.held)
	require.Equal(t, "deleted", cleanupReason(t, owner, attempt.revision))

	held, err := flagged.ClaimVectorCleanup(ctx, "sweeper", time.Minute, time.Hour)
	require.NoError(t, err)
	require.Nil(t, held)
}

// A node that did not enable manifests never reads their tables; the trigger
// still hands every revision to the sweeping nodes.
func TestCoordinatorOnANodeWithoutManifestsLeavesRevisionsToTheSweeper(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	_, err := owner.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	owner.EnableHostedVectorManifests(true)
	doc := acceptDocument(t, owner, "manifested elsewhere", true, false)
	attempt := startManifestAttempt(t, owner, doc.id, "worker")

	recorder := newDeletionRecorder()
	require.NoError(t, serving.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
	require.Zero(t, recorder.count("manifest"))
	require.Equal(t, revisionVectors, readReceipt(t, owner, doc.id).mode)
	require.Equal(t, "deleted", cleanupReason(t, owner, attempt.revision))
}

func TestBeginScopedDeletionUsesManifestModeForManifestedDocuments(t *testing.T) {
	c := manifestDatabase(t)
	ctx := t.Context()
	_, err := c.ActivateDeletion(ctx)
	require.NoError(t, err)
	f := provisionOwnedFixture(t, c)
	req := ownedRequest(f.owner, "", "scoped manifested doc")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	attempt := startManifestAttempt(t, c, req.Document.ID, "worker")
	ok, err := c.CompleteJob(ctx, attempt.job, attempt.chunks(req.Document.ID, "scoped manifested doc"), domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)

	recorder := newDeletionRecorder()
	ops := recorder.ops(req.Owner.Incarnation)
	inc, complete, err := c.BeginScopedDeletion(ctx, f.owner, time.Now().Add(time.Minute), req.Document.ID, ops)
	require.NoError(t, err)
	require.False(t, complete)
	require.Equal(t, req.Owner.Incarnation, inc)

	receipt := readReceipt(t, c, req.Document.ID)
	require.Equal(t, manifestVectors, receipt.mode)

	require.NoError(t, c.ResumeScopedDeletion(ctx, req.Document.ID, inc, ops))
	require.Equal(t, []string{
		"manifest:" + string(req.Document.ID) + "/" + attempt.revision + "/" + attempt.vectorID,
	}, recorder.all())
	require.Empty(t, documentStatus(t, c, req.Document.ID))
	require.Equal(t, "deleted", cleanupReason(t, c, attempt.revision))
}
