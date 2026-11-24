package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEvaluationWriterNeedsPublishedReceiptGrants(t *testing.T) {
	owner, serving := restrictedDatabase(t)
	ctx := t.Context()
	id, receipt := fixtureShapedID(), uuid.NewString()
	_, err := serving.AcceptDocument(ctx, fixtureRequest(id, receipt, "postgres"))
	requireSQLState(t, err, "42501")
	_, err = serving.ReadEvaluationFixture(ctx, id, receipt)
	requireSQLState(t, err, "42501")

	_, err = owner.Pool.Exec(ctx, publishedSQL(t, "development.md", "evaluation_fixtures"))
	require.NoError(t, err)
	accepted, err := serving.AcceptDocument(ctx, fixtureRequest(id, receipt, "postgres"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	status, err := serving.ReadEvaluationFixture(ctx, id, receipt)
	require.NoError(t, err)
	require.Equal(t, domain.FixtureOwned, status.State)
	job, err := serving.ClaimJobForFixtureStore(ctx, "worker", time.Minute, 3, "postgres")
	require.NoError(t, err)
	require.NotNil(t, job)
	done, err := serving.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)
	state, err := serving.DeleteEvaluationFixture(ctx, id, receipt, accepted.Incarnation, "postgres", func(context.Context) error { return nil })
	require.NoError(t, err)
	require.Equal(t, domain.FixtureDeleted, state)

	absent := fixtureShapedID()
	retired, err := serving.ReconcileEvaluationFixture(ctx, absent, receipt)
	require.NoError(t, err)
	require.Equal(t, domain.FixtureRetired, retired.State)
	late := fixtureRequest(absent, receipt, "postgres")
	conflict, err := serving.AcceptDocument(ctx, late)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptConflict, conflict.Outcome)
}
