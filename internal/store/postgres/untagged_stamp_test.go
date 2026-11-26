package postgres

import (
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func untaggedStamp(t *testing.T, c *Client, id domain.DocumentID) *string {
	t.Helper()
	var stamp *string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT untagged_since::text FROM documents WHERE id=$1`, id).Scan(&stamp))
	return stamp
}

func TestUntaggedStampTracksTheHistoricalState(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	id := domain.DocumentID(uuid.NewString())
	scopedID := domain.DocumentID(uuid.NewString())
	var inserted *string
	var xid string
	// The insert omits the new column, as it would on an older replica.
	require.NoError(t, c.Pool.QueryRow(ctx, `
INSERT INTO documents(id, content) VALUES ($1, 'legacy writer')
RETURNING untagged_since::text, pg_current_xact_id()::text`, id).Scan(&inserted, &xid))
	require.Equal(t, &xid, inserted)

	_, err := c.Pool.Exec(ctx, `INSERT INTO documents(id, content, scoped_vectors) VALUES ($1, 'scoped writer', true)`, scopedID)
	require.NoError(t, err)
	require.Nil(t, untaggedStamp(t, c, scopedID))

	_, err = c.Pool.Exec(ctx, `UPDATE documents SET status='completed', content='edited body', fingerprint='edited fingerprint' WHERE id=$1`, id)
	require.NoError(t, err)
	require.Equal(t, inserted, untaggedStamp(t, c, id))

	_, err = c.Pool.Exec(ctx, `UPDATE documents SET untagged_since=NULL WHERE id=$1`, id)
	require.NoError(t, err)
	require.Equal(t, inserted, untaggedStamp(t, c, id))

	_, err = c.Pool.Exec(ctx, `UPDATE documents SET vector_incarnation_required=true WHERE id=$1`, id)
	require.NoError(t, err)
	require.Nil(t, untaggedStamp(t, c, id))

	var restored *string
	require.NoError(t, c.Pool.QueryRow(ctx, `
UPDATE documents SET vector_incarnation_required=false WHERE id=$1
RETURNING untagged_since::text, pg_current_xact_id()::text`, id).Scan(&restored, &xid))
	require.Equal(t, &xid, restored)
	require.NotEqual(t, inserted, restored)
}

func TestRestrictedServingLoginStampsUntaggedDocuments(t *testing.T) {
	ctx := t.Context()
	owner := migrationTestClient(t, startTestDatabase(t))
	applyMigrationPrefix(t, owner, 26)
	_, err := owner.Pool.Exec(ctx, `ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`)
	require.NoError(t, err)
	require.NoError(t, owner.RunMigrations(ctx))
	serving, _ := publishedServingClient(t, owner)

	var canExecute bool
	require.NoError(t, serving.Pool.QueryRow(ctx, `SELECT has_function_privilege('documents_stamp_untagged()', 'EXECUTE')`).Scan(&canExecute))
	require.False(t, canExecute)
	id := jobsAccept(t, serving, "ordinary untagged content")
	inserted := untaggedStamp(t, owner, id)
	require.NotNil(t, inserted)

	job, err := serving.ClaimJob(ctx, "worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, id, job.Document.ID)
	done, err := serving.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, inserted, untaggedStamp(t, owner, id))
}
