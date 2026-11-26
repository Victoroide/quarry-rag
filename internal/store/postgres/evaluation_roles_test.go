package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

var servingRoles = []string{"quarry_service", "quarry_operator", "quarry_auditor"}

// publishedSQL returns the first SQL block of a guide that contains marker.
func publishedSQL(t *testing.T, guide, marker string) string {
	t.Helper()
	return pgtest.PublishedSQL(t, "../../../docs", guide, marker)
}

// restrictedDatabase migrates as the owner and returns a client limited to the
// published grants for the serving login.
func restrictedDatabase(t *testing.T) (owner, serving *Client, roles grantRoles) {
	t.Helper()
	owner = jobsDatabase(t)
	serving, roles = publishedServingClient(t, owner)
	return owner, serving, roles
}

func publishedServingClient(t *testing.T, owner *Client) (*Client, grantRoles) {
	t.Helper()
	roles := newGrantRoles(t, owner.Pool.Config().ConnString(), servingRoles...)
	for _, purpose := range servingRoles {
		grantExec(t, owner, "CREATE ROLE "+pgx.Identifier{roles.name(purpose)}.Sanitize())
	}
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
	return roleClient(t, owner, roles.name("quarry_service")), roles
}

func publishedServingSQL(t *testing.T, roles grantRoles, guide, marker string) string {
	t.Helper()
	return pgtest.ReplaceRoles(publishedSQL(t, guide, marker), roles)
}

func roleClient(t *testing.T, owner *Client, role string) *Client {
	t.Helper()
	config := owner.Pool.Config()
	config.ConnConfig.RuntimeParams["role"] = role
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &Client{Pool: pool}
}

func requireSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, code, pgErr.Code, pgErr.Message)
}

func fixtureShapedID() domain.DocumentID {
	return domain.DocumentID(uuid.NewSHA1(uuid.New(), []byte("fixture-shaped")).String())
}

func shapedRequest(id domain.DocumentID, content string) domain.AcceptRequest {
	req := jobsRequest(content)
	req.Document.ID, req.CallerSupplied = id, true
	return req
}

func fixtureRequest(id domain.DocumentID, receipt, store string) domain.AcceptRequest {
	req := shapedRequest(id, "fixture body")
	req.Collection = "evaluation-collection"
	req.Fingerprint = chunker.ScopedFingerprint(req.Collection, req.Document.Content)
	req.EvaluationReceipt, req.EvaluationVectorIdentity = receipt, store
	return req
}

func TestPublishedServingRoleProcessesOrdinaryDocuments(t *testing.T) {
	ctx := t.Context()
	owner := migrationTestClient(t, startTestDatabase(t))
	applyMigrationPrefix(t, owner, 18)
	_, err := owner.Pool.Exec(ctx, `ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`)
	require.NoError(t, err)
	require.NoError(t, owner.RunMigrations(ctx))
	serving, _ := publishedServingClient(t, owner)

	t.Run("UUIDv4 lease", func(t *testing.T) {
		ctx := t.Context()
		ordinary, err := serving.AcceptDocument(ctx, jobsRequest("ordinary v4 body"))
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, ordinary.Outcome)
		job, err := serving.ClaimJobForFixtureStore(ctx, "worker", time.Minute, 3, "postgres")
		require.NoError(t, err)
		require.NotNil(t, job)
		require.Equal(t, ordinary.DocumentID, job.Document.ID)
		done, err := serving.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, done)
	})
	t.Run("UUIDv5 admission and removal", func(t *testing.T) {
		ctx := t.Context()
		id := fixtureShapedID()
		ordinary, err := serving.AcceptDocument(ctx, shapedRequest(id, "ordinary v5 body"))
		require.NoError(t, err, "a version 5 identifier is ordinary unless a reservation exists")
		require.Equal(t, domain.AcceptAccepted, ordinary.Outcome)
		job, err := serving.ClaimJob(ctx, "unbound-worker", time.Minute, 3)
		require.NoError(t, err)
		require.NotNil(t, job)
		require.Equal(t, id, job.Document.ID)
		done, err := serving.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, done)
		require.NoError(t, serving.WithOrdinaryRemoval(ctx, id, func(removeCtx context.Context) error {
			return serving.DeleteDocument(removeCtx, id)
		}))
	})
}

func TestPublishedServingRoleKeepsReservationGuards(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	id, receipt := fixtureShapedID(), uuid.NewString()
	accepted, err := owner.AcceptDocument(ctx, fixtureRequest(id, receipt, "pinecone:bound"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)

	unbound, err := serving.ClaimJob(ctx, "ordinary", time.Minute, 3)
	require.NoError(t, err)
	require.Nil(t, unbound, "an ordinary claim never leases fixture work")
	for _, identity := range []string{"postgres", "pinecone:replacement"} {
		wrong, claimErr := serving.ClaimJobForFixtureStore(ctx, "wrong-store", time.Minute, 3, identity)
		require.NoError(t, claimErr)
		require.Nil(t, wrong, identity)
		wrong, claimErr = serving.ClaimPDFJobForFixtureStore(ctx, "wrong-store-with-ocr", time.Minute, 3, identity)
		require.NoError(t, claimErr)
		require.Nil(t, wrong, identity)
	}
	err = serving.WithOrdinaryRemoval(ctx, id, func(context.Context) error {
		require.FailNow(t, "ordinary removal must stop at the reservation")
		return nil
	})
	require.ErrorIs(t, err, domain.ErrFixtureDeletionRequired)
	job, err := serving.ClaimPDFJobForFixtureStore(ctx, "bound-store-with-ocr", time.Minute, 3, "pinecone:bound")
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, id, job.Document.ID)
	done, err := serving.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)

	state, err := owner.DeleteEvaluationFixture(ctx, id, receipt, accepted.Incarnation, "pinecone:bound", func(context.Context) error { return nil })
	require.NoError(t, err)
	require.Equal(t, domain.FixtureDeleted, state)
	_, err = serving.AcceptDocument(ctx, shapedRequest(id, "replacement body"))
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "the retired identifier stays reserved: %v", err)
	require.Equal(t, "23505", pgErr.Code)
}

func TestPublishedServingRolesAreCleanedUpBetweenFixtures(t *testing.T) {
	if _, configured := os.LookupEnv("QUARRY_TEST_POSTGRES_ADMIN_DSN"); !configured {
		t.Skip("requires a shared PostgreSQL test service")
	}
	observer := migrationTestClient(t, startTestDatabase(t))
	seen := make(map[string]bool)
	for range 2 {
		var roles grantRoles
		t.Run("fixture", func(t *testing.T) {
			var serving *Client
			_, serving, roles = restrictedDatabase(t)
			var login string
			require.NoError(t, serving.Pool.QueryRow(t.Context(), `SELECT current_user`).Scan(&login))
			require.Equal(t, roles.name("quarry_service"), login)
			for _, purpose := range servingRoles {
				name := roles.name(purpose)
				require.False(t, seen[name], "role %s was reused by a later fixture", name)
				seen[name] = true
				var exists bool
				require.NoError(t, observer.Pool.QueryRow(t.Context(),
					`SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)`, name).Scan(&exists))
				require.True(t, exists, "fixture role %s was not created", name)
			}
		})
		for _, name := range roles {
			var exists bool
			require.NoError(t, observer.Pool.QueryRow(t.Context(),
				`SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)`, name).Scan(&exists))
			require.False(t, exists, "fixture role %s survived cleanup", name)
		}
	}
}

func TestClaimBeforeFixtureMigration(t *testing.T) {
	c := jobsDatabase(t)
	ctx := t.Context()
	// A binary that starts before the fixture migrations finds neither the
	// reservation table nor its predicates.
	_, err := c.Pool.Exec(ctx, `DROP FUNCTION evaluation_fixture_claimable(uuid, text); DROP FUNCTION evaluation_fixture_reserved(uuid); DROP TABLE evaluation_fixtures`)
	require.NoError(t, err)
	first := jobsAccept(t, c, "claimed without a reservation table")
	job, err := c.ClaimJobForFixtureStore(ctx, "worker", time.Minute, 3, "postgres")
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, first, job.Document.ID)
	second := jobsAccept(t, c, "claimed by an unbound worker")
	job, err = c.ClaimJob(ctx, "legacy-worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, second, job.Document.ID)
}
