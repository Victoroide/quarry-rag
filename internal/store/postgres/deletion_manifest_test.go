package postgres

import (
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
