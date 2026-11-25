package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func pineconeGeneration(name string) Generation {
	return Generation{Name: name, Store: "pinecone", Target: "quarry-" + name, Host: "host-" + name, Profile: generationProfile(), Origin: OriginRecorded}
}

type admittedFixture struct {
	id          domain.DocumentID
	receipt     string
	incarnation string
}

// admitFixture reserves a fixture bound to the given store identity and leaves
// its ingestion job pending.
func admitFixture(t *testing.T, c *Client, identity string) admittedFixture {
	t.Helper()
	f := admittedFixture{id: fixtureShapedID(), receipt: uuid.NewString()}
	accepted, err := c.AcceptDocument(t.Context(), fixtureRequest(f.id, f.receipt, identity))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	f.incarnation = accepted.Incarnation
	return f
}

func completeFixture(t *testing.T, c *Client, f admittedFixture, identity string) {
	t.Helper()
	job, err := c.ClaimJobForFixtureStore(t.Context(), "worker", time.Minute, 3, identity)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, f.id, job.Document.ID)
	done, err := c.CompleteJob(t.Context(), *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)
}

func deleteFixture(t *testing.T, c *Client, f admittedFixture, identity string) string {
	t.Helper()
	state, err := c.DeleteEvaluationFixture(t.Context(), f.id, f.receipt, f.incarnation, identity, func(context.Context) error { return nil })
	require.NoError(t, err)
	return state
}

func TestStoreSwitchRefusesFixturesBoundElsewhere(t *testing.T) {
	target := pineconeGeneration("g2")
	identity := domain.FixtureVectorIdentity(target.Store, target.Target, target.Host)
	require.NotEqual(t, "postgres", identity)

	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *Client) admittedFixture
	}{
		{"pending job", func(t *testing.T, c *Client) admittedFixture { return admitFixture(t, c, "postgres") }},
		{"completed fixture", func(t *testing.T, c *Client) admittedFixture {
			f := admitFixture(t, c, "postgres")
			completeFixture(t, c, f, "postgres")
			return f
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := jobsDatabase(t)
			_, err := c.RegisterGeneration(t.Context(), target)
			require.NoError(t, err)
			f := tc.setup(t, c)

			err = c.FreezeWriters(t.Context(), "g2")
			require.ErrorIs(t, err, ErrFixtureReceipts)
			state, err := c.VectorState(t.Context())
			require.NoError(t, err)
			require.False(t, state.Frozen, "the refusal precedes the freeze, so writers keep running")
			require.NoError(t, c.WritersOpen(t.Context()))

			require.Equal(t, domain.FixtureDeleted, deleteFixture(t, c, f, "postgres"), "the original receipt still cleans the fixture")
			require.NoError(t, c.FreezeWriters(t.Context(), "g2"))
			require.NoError(t, c.RecordVerification(t.Context(), "g2", "digest", json.RawMessage(`{"passed":true}`)))
			require.NoError(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0))
			state, err = c.VectorState(t.Context())
			require.NoError(t, err)
			require.Equal(t, "g2", state.Active)
		})
	}

	t.Run("unknown legacy binding", func(t *testing.T) {
		c := jobsDatabase(t)
		_, err := c.RegisterGeneration(t.Context(), target)
		require.NoError(t, err)
		f := admitFixture(t, c, "postgres")
		_, err = c.Pool.Exec(t.Context(), `UPDATE evaluation_fixtures SET vector_identity=NULL WHERE document_id=$1`, string(f.id))
		require.NoError(t, err)
		require.ErrorIs(t, c.FreezeWriters(t.Context(), "g2"), ErrFixtureReceipts)
		require.Equal(t, domain.FixtureWrongStore, deleteFixture(t, c, f, "postgres"), "an unknown binding cannot be deleted by receipt")
		require.ErrorIs(t, c.FreezeWriters(t.Context(), "g2"), ErrFixtureReceipts)
	})

	t.Run("retired receipt", func(t *testing.T) {
		c := jobsDatabase(t)
		_, err := c.RegisterGeneration(t.Context(), target)
		require.NoError(t, err)
		status, err := c.ReconcileEvaluationFixture(t.Context(), fixtureShapedID(), uuid.NewString())
		require.NoError(t, err)
		require.Equal(t, domain.FixtureRetired, status.State)
		require.NoError(t, c.FreezeWriters(t.Context(), "g2"))
	})

	t.Run("same store", func(t *testing.T) {
		c := jobsDatabase(t)
		_, err := c.RegisterGeneration(t.Context(), stagingGeneration("g3"))
		require.NoError(t, err)
		admitFixture(t, c, "postgres")
		require.NoError(t, c.FreezeWriters(t.Context(), "g3"), "a PostgreSQL fixture stays serviceable on another PostgreSQL generation")
	})

	t.Run("matching Pinecone binding", func(t *testing.T) {
		c := jobsDatabase(t)
		_, err := c.RegisterGeneration(t.Context(), target)
		require.NoError(t, err)
		admitFixture(t, c, identity)
		require.NoError(t, c.FreezeWriters(t.Context(), "g2"))
	})
}

func TestActivationRepeatsTheFixtureCheck(t *testing.T) {
	c := jobsDatabase(t)
	target := pineconeGeneration("g2")
	_, err := c.RegisterGeneration(t.Context(), target)
	require.NoError(t, err)
	f := admitFixture(t, c, domain.FixtureVectorIdentity(target.Store, target.Target, target.Host))
	require.NoError(t, c.FreezeWriters(t.Context(), "g2"))
	require.NoError(t, c.RecordVerification(t.Context(), "g2", "digest", json.RawMessage(`{"passed":true}`)))

	_, err = c.Pool.Exec(t.Context(), `UPDATE evaluation_fixtures SET vector_identity='postgres' WHERE document_id=$1`, string(f.id))
	require.NoError(t, err)
	require.ErrorIs(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0), ErrFixtureReceipts)
	state, err := c.VectorState(t.Context())
	require.NoError(t, err)
	require.Equal(t, LegacyGeneration, state.Active, "a refused activation leaves the live generation unchanged")

	_, err = c.Pool.Exec(t.Context(), `UPDATE evaluation_fixtures SET active_document_id=NULL WHERE document_id=$1`, string(f.id))
	require.NoError(t, err)
	require.NoError(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0))
}
