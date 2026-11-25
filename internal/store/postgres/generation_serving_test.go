package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestRestrictedServingPinAndClaim(t *testing.T) {
	owner, _ := vectorDatabase(t)
	ctx := t.Context()
	legacy, err := OpenGenerationVectorStore(ctx, owner, 3, LegacyGeneration, WithExactScopeMax(10))
	require.NoError(t, err)
	require.NoError(t, legacy.EnsureExactScopeIndexes(ctx))
	roles := newGrantRoles(t, owner.Pool.Config().ConnString(), servingRoles...)
	for _, purpose := range servingRoles {
		grantExec(t, owner, "CREATE ROLE "+pgx.Identifier{roles.name(purpose)}.Sanitize())
	}
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "ON public.chunk_vectors TO quarry_service"))
	service := pgx.Identifier{roles.name("quarry_service")}.Sanitize()
	start := func(generation string) (*Client, *VectorStore) {
		t.Helper()
		srv := grantRoleClient(t, owner.Pool.Config().ConnString(), "SET SESSION AUTHORIZATION "+service)
		startupCtx := t.Context()
		require.NoError(t, srv.RunMigrations(startupCtx))
		var vs *VectorStore
		var openErr error
		if generation == LegacyGeneration {
			vs, openErr = NewVectorStore(startupCtx, srv, 3, WithExactScopeMax(10))
		} else {
			vs, openErr = OpenGenerationVectorStore(startupCtx, srv, 3, generation, WithExactScopeMax(10))
		}
		require.NoError(t, openErr)
		require.NoError(t, vs.EnsureExactScopeIndexes(startupCtx))
		vs.RouteCompletion()
		require.NoError(t, srv.PinVectorGeneration(startupCtx, generation, "postgres", VectorTable(generation), "", generationProfile()))
		require.NoError(t, srv.WritersOpen(startupCtx))
		return srv, vs
	}
	first, _ := start(LegacyGeneration)
	restart, _ := start(LegacyGeneration)
	id := jobsAccept(t, first, "accepted by the restricted serving role")
	job, err := restart.ClaimJob(ctx, "restarted replica", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, id, job.Document.ID)
	done, err := restart.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)
	_, err = first.Pool.Exec(ctx, "UPDATE vector_state SET frozen=TRUE")
	require.ErrorContains(t, err, "permission denied")
	_, err = first.Pool.Exec(ctx, "UPDATE vector_generations SET state='verified'")
	require.ErrorContains(t, err, "permission denied")

	staged, err := CreateGenerationVectorStore(ctx, owner, 3, "g2", WithExactScopeMax(10))
	require.NoError(t, err)
	require.NoError(t, staged.EnsureExactScopeIndexes(ctx))
	grantExec(t, owner, `GRANT SELECT, INSERT, UPDATE, DELETE ON chunk_vectors_g2 TO `+service)
	verifiedGeneration(t, owner, "g2", "digest")
	require.NoError(t, owner.ActivateGeneration(ctx, "g2", "digest", 0))
	require.NoError(t, owner.ThawWriters(ctx))

	named, vectors := start("g2")
	require.NoError(t, vectors.Ping(ctx))
	id = jobsAccept(t, named, "served by the promoted generation")
	job, err = named.ClaimJob(ctx, "promoted replica", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, id, job.Document.ID)
}

func TestAttestationFencesRunningReplicaWithDifferentProfile(t *testing.T) {
	owner := jobsDatabase(t)
	ctx := t.Context()
	old, err := NewClient(ctx, owner.Pool.Config().ConnString())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, old.Close(context.Background())) })

	profile := generationProfile()
	different := profile
	different.ModelRevision += "-other-model"
	require.NoError(t, old.PinVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", different))
	require.NoError(t, owner.PinVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", profile))
	jobsAccept(t, old, "accepted before attestation")
	job, err := old.ClaimJob(ctx, "old replica", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)

	legacy := Generation{Name: LegacyGeneration, Store: "postgres", Target: "chunk_vectors", Origin: OriginAttested, Profile: profile}
	_, err = owner.RegisterGeneration(ctx, legacy)
	require.ErrorContains(t, err, "in-flight jobs")
	row, err := owner.Generation(ctx, LegacyGeneration)
	require.NoError(t, err)
	require.Nil(t, row)

	require.NoError(t, old.ReleaseJob(ctx, *job))
	_, err = owner.RegisterGeneration(ctx, legacy)
	require.NoError(t, err)
	require.ErrorIs(t, old.WritersOpen(ctx), domain.ErrGenerationMismatch)
	_, err = old.AcceptDocument(ctx, jobsRequest("rejected after attestation"))
	require.ErrorIs(t, err, domain.ErrGenerationMismatch)
	_, err = old.ClaimJob(ctx, "old replica", time.Minute, 3)
	require.ErrorIs(t, err, domain.ErrGenerationMismatch)

	require.NoError(t, owner.WritersOpen(ctx))
	jobsAccept(t, owner, "accepted by the matching replica")
	claimed, err := owner.ClaimJob(ctx, "matching replica", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, claimed)
}
