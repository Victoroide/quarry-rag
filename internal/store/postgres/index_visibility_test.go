package postgres

import (
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIndexObservationBelongsToClaim(t *testing.T) {
	c := jobsDatabase(t)
	for _, transition := range []string{"released", "failed and accepted again", "deleted and reused"} {
		t.Run(transition, func(t *testing.T) {
			ctx := t.Context()
			req := jobsRequest(transition)
			req.CallerSupplied = true
			accepted, err := c.AcceptDocument(ctx, req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
			stale, err := c.ClaimJob(ctx, "same-worker", time.Minute, 3)
			require.NoError(t, err)
			require.NotNil(t, stale)
			switch transition {
			case "released":
				require.NoError(t, c.ReleaseJob(ctx, *stale))
			case "failed and accepted again":
				require.NoError(t, c.FailJob(ctx, *stale, "synthetic failure"))
				result, acceptErr := c.AcceptDocument(ctx, req)
				require.NoError(t, acceptErr)
				require.Equal(t, domain.AcceptRequeued, result.Outcome)
			case "deleted and reused":
				require.NoError(t, c.DeleteDocument(ctx, accepted.DocumentID))
				result, acceptErr := c.AcceptDocument(ctx, req)
				require.NoError(t, acceptErr)
				require.Equal(t, domain.AcceptAccepted, result.Outcome)
			}
			current, err := c.ClaimJob(ctx, "same-worker", time.Minute, 3)
			require.NoError(t, err)
			require.NotNil(t, current)
			require.Equal(t, stale.Attempts, current.Attempts)
			require.Equal(t, stale.LeaseOwner, current.LeaseOwner)
			require.Equal(t, stale.Document.ID, current.Document.ID)
			timestamp := time.Now().UTC().Truncate(time.Microsecond)
			staleObservation := domain.IndexObservation{Visibility: domain.VisibilityWritesIndexed, ObservedAt: &timestamp}
			ok, err := c.CompleteJob(ctx, *stale, nil, staleObservation)
			require.NoError(t, err)
			require.False(t, ok)
			summary, err := c.GetDocumentSummary(ctx, accepted.DocumentID)
			require.NoError(t, err)
			require.Equal(t, "processing", summary.Status)
			require.Equal(t, domain.VisibilityUnknown, summary.IndexVisibility)
			require.Nil(t, summary.IndexVisibilityObservedAt)

			currentObservation := domain.IndexObservation{Visibility: domain.VisibilitySampleSeen, ObservedAt: &timestamp}
			ok, err = c.CompleteJob(ctx, *current, nil, currentObservation)
			require.NoError(t, err)
			require.True(t, ok)
			summary, err = c.GetDocumentSummary(ctx, accepted.DocumentID)
			require.NoError(t, err)
			require.Equal(t, "completed", summary.Status)
			require.Equal(t, domain.VisibilitySampleSeen, summary.IndexVisibility)
			require.Equal(t, timestamp, summary.IndexVisibilityObservedAt.UTC())
		})
	}
}

func TestIndexObservationRollsBackOnCompletionFailure(t *testing.T) {
	c := jobsDatabase(t)
	id := jobsAccept(t, c, "synthetic rollback")
	job, err := c.ClaimJob(t.Context(), "worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	now := time.Now()
	observation := domain.IndexObservation{Visibility: domain.VisibilityWritesIndexed, ObservedAt: &now}
	ok, err := c.CompleteJob(t.Context(), *job, []domain.Chunk{{ID: "invalid uuid"}}, observation)
	require.Error(t, err)
	require.False(t, ok)
	doc, err := c.GetDocumentSummary(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, "processing", doc.Status)
	require.Equal(t, domain.VisibilityUnknown, doc.IndexVisibility)
	require.Nil(t, doc.IndexVisibilityObservedAt)
}

func TestIndexObservationRollsBackWhenLeaseExpiresDuringCompletion(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	id := jobsAccept(t, c, "synthetic lease expiry")
	job, err := c.ClaimJob(ctx, "worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	_, err = c.Pool.Exec(ctx, `CREATE FUNCTION expire_lease_on_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE ingest_jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE document_id=NEW.document_id;
    RETURN NEW;
END $$;
CREATE TRIGGER expire_lease_on_insert BEFORE INSERT ON document_fingerprints FOR EACH ROW EXECUTE FUNCTION expire_lease_on_insert()`)
	require.NoError(t, err)
	now := time.Now()
	observation := domain.IndexObservation{Visibility: domain.VisibilityWritesIndexed, ObservedAt: &now}
	ok, err := c.CompleteJob(ctx, *job, []domain.Chunk{{ID: domain.ChunkID(uuid.NewString())}}, observation)
	require.NoError(t, err)
	require.False(t, ok)
	doc, err := c.GetDocumentSummary(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "processing", doc.Status)
	require.Equal(t, domain.VisibilityUnknown, doc.IndexVisibility)
	require.Nil(t, doc.IndexVisibilityObservedAt)
	var storedChunks int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM chunks WHERE document_id=$1`, string(id)).Scan(&storedChunks))
	require.Zero(t, storedChunks)
}
