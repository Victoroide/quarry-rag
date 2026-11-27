package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const bindLegacyPostgres = "SELECT vector_bind_legacy('postgres','chunk_vectors','')"
const registerLegacyPostgres = `INSERT INTO vector_generations
	(name, store, target, profile, profile_hash, profile_origin, state, ever_active)
	VALUES ('legacy','postgres','chunk_vectors',$1,$2,'recorded','active',TRUE)
	ON CONFLICT (name) DO NOTHING`
const insertLegacyPostgres = `INSERT INTO vector_generations
	(name, store, target, profile, profile_hash, profile_origin)
	VALUES ('legacy', 'postgres', 'chunk_vectors', '{}', 'test', 'recorded')`
const registerLegacyPinecone = `INSERT INTO vector_generations
	(name, store, target, host, profile, profile_hash, profile_origin)
	VALUES ('legacy', 'pinecone', 'source', 'source-host', '{}', 'test', 'recorded')`
const updateLegacyPostgres = `UPDATE vector_generations
	SET store='postgres', target='chunk_vectors', host='' WHERE name='legacy'`
const upsertLegacyPostgres = insertLegacyPostgres + ` ON CONFLICT (name) DO UPDATE
	SET store=EXCLUDED.store, target=EXCLUDED.target, host=EXCLUDED.host`

func requireLegacyBindRefusal(t *testing.T, err error) {
	t.Helper()
	requireCutoverRefusal(t, err)
	require.ErrorContains(t, err, "drain outstanding hosted manifest work on the original source before binding legacy to PostgreSQL")
}

func vectorBindingSnapshot(ctx context.Context, t *testing.T, db queryRower) string {
	t.Helper()
	var snapshot string
	require.NoError(t, db.QueryRow(ctx, `SELECT jsonb_build_array(
		(SELECT to_jsonb(s) FROM vector_state s),
		(SELECT jsonb_agg(g ORDER BY g.name) FROM vector_generations g),
		(SELECT jsonb_agg(m ORDER BY m.revision_id) FROM hosted_vector_manifests m),
		(SELECT jsonb_agg(c ORDER BY c.revision_id) FROM hosted_vector_cleanups c)
	)::text`).Scan(&snapshot))
	return snapshot
}

func seedLegacyBindingManifest(ctx context.Context, t *testing.T, c *Client, collection, state, cleanup string) {
	t.Helper()
	revision, doc, incarnation := uuid.NewString(), uuid.NewString(), uuid.NewString()
	grantExecContext(ctx, t, c, insertCutoverManifest, revision, doc, incarnation, collection, state)
	if cleanup != "" {
		grantExecContext(ctx, t, c, `INSERT INTO hosted_vector_cleanups
			(revision_id, document_id, incarnation, collection, id_prefix, vector_ids, state)
			VALUES ($1, $2, $3, $4, 'cutover', ARRAY['vector'], $5)`, revision, doc, incarnation, collection, cleanup)
	}
}

func TestLegacyPostgresBindingRequiresHostedDrain(t *testing.T) {
	for _, tc := range []struct{ name, state, cleanup, collection string }{
		{name: "writing", state: "writing"},
		{name: "active", state: "active", collection: "north"},
		{name: "retired", state: "retired", collection: "south"},
		{name: "pending cleanup", state: "retired", cleanup: "pending", collection: "north"},
		{name: "verifying cleanup", state: "retired", cleanup: "verifying", collection: "south"},
		{name: "unresolved cleanup", state: "retired", cleanup: "unresolved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := migrationTestClient(t, pgtest.StartVector(t))
			applyMigrationPrefix(t, c, 46)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			_, err := NewVectorStore(ctx, c, 3)
			require.NoError(t, err)
			// None of the historical intents has a live document.
			seedLegacyBindingManifest(ctx, t, c, tc.collection, tc.state, tc.cleanup)
			before := vectorBindingSnapshot(ctx, t, c.Pool)
			require.NoError(t, c.RunMigrations(ctx))
			require.Equal(t, before, vectorBindingSnapshot(ctx, t, c.Pool))

			profile := generationProfile()
			requireLegacyBindRefusal(t, c.PinVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", profile))
			require.Nil(t, c.vectorPin.Load())
			require.Equal(t, before, vectorBindingSnapshot(ctx, t, c.Pool))

			requireLegacyBindRefusal(t, c.PinCleanupVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", profile))
			require.Nil(t, c.vectorPin.Load())
			require.Equal(t, before, vectorBindingSnapshot(ctx, t, c.Pool))

			_, err = c.RegisterGeneration(ctx, stagingGeneration(LegacyGeneration))
			requireLegacyBindRefusal(t, err)
			require.Equal(t, before, vectorBindingSnapshot(ctx, t, c.Pool))
		})
	}
}

type bindingExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type bindingStatement func(context.Context, bindingExecutor) error

func bindLegacyStatement(ctx context.Context, db bindingExecutor) error {
	_, err := db.Exec(ctx, bindLegacyPostgres)
	return err
}

func registerLegacyStatement(ctx context.Context, db bindingExecutor) error {
	profile := generationProfile()
	payload, err := json.Marshal(profile)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, registerLegacyPostgres, payload, profile.Hash())
	return err
}

func insertBindingManifest(ctx context.Context, db bindingExecutor) error {
	doc := uuid.NewString()
	_, err := db.Exec(ctx, insertCutoverManifest, uuid.NewString(), doc, doc, "", "writing")
	return err
}

func waitForLegacyBindingLock(ctx context.Context, t *testing.T, observer *Client, holder, waiter uint32) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var blocked bool
		require.NoError(collect, observer.Pool.QueryRow(ctx, `SELECT $1=ANY(pg_blocking_pids($2))`,
			holder, waiter).Scan(&blocked))
		require.True(collect, blocked)
	}, 5*time.Second, 10*time.Millisecond)
}

func requireBindingLocksReleased(ctx context.Context, t *testing.T, observer *Client, holder, waiter uint32) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var locked bool
		require.NoError(collect, observer.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks
			WHERE pid IN ($1,$2) AND (locktype IN ('transactionid','tuple') OR NOT granted))`,
			holder, waiter).Scan(&locked))
		require.False(collect, locked)
	}, 5*time.Second, 10*time.Millisecond)
}

type bindingRace struct {
	name                string
	bindFirst, rollback bool
	isolation           pgx.TxIsoLevel
	registration        string
}

func TestLegacyPostgresBindingSerializesWithHostedInsert(t *testing.T) {
	for _, route := range []struct {
		name string
		bind bindingStatement
	}{
		{name: "binder", bind: bindLegacyStatement},
		{name: "registration", bind: registerLegacyStatement},
	} {
		t.Run(route.name, func(t *testing.T) {
			for _, tc := range []bindingRace{
				{name: "insert commits before binding"},
				{name: "insert rolls back before binding", rollback: true},
				{name: "binding commits before insert", bindFirst: true},
				{name: "binding rolls back before insert", bindFirst: true, rollback: true},
				{name: "binding requires read committed", rollback: true, isolation: pgx.RepeatableRead},
			} {
				t.Run(tc.name, func(t *testing.T) {
					testLegacyBindingRace(t, route.bind, tc)
				})
			}
		})
	}
	t.Run("stale insert after singleton update", func(t *testing.T) {
		testLegacyBindingRace(t, bindLegacyStatement, bindingRace{bindFirst: true, isolation: pgx.RepeatableRead})
	})
	t.Run("stale insert after registration publishes profile", func(t *testing.T) {
		publish := func(ctx context.Context, db bindingExecutor) error {
			if err := registerLegacyStatement(ctx, db); err != nil {
				return err
			}
			// RegisterGeneration publishes the profile on the singleton before commit.
			_, err := db.Exec(ctx, `UPDATE vector_state SET active_profile_hash=$1`, generationProfile().Hash())
			return err
		}
		testLegacyBindingRace(t, publish, bindingRace{bindFirst: true, isolation: pgx.RepeatableRead})
	})
	for _, tc := range []bindingRace{
		{name: "repeatable read after direct registration commits", bindFirst: true, isolation: pgx.RepeatableRead},
		{name: "serializable after direct registration commits", bindFirst: true, isolation: pgx.Serializable},
		{name: "repeatable read after direct registration rolls back", bindFirst: true, isolation: pgx.RepeatableRead, rollback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testLegacyBindingRace(t, registerLegacyStatement, tc)
		})
	}
	for _, route := range []struct{ name, setup, statement string }{
		{name: "default-state registration", statement: insertLegacyPostgres},
		{name: "registered store update", setup: registerLegacyPinecone, statement: updateLegacyPostgres},
		{name: "registry upsert", setup: registerLegacyPinecone, statement: upsertLegacyPostgres},
	} {
		t.Run(route.name, func(t *testing.T) {
			for _, tc := range []bindingRace{
				{name: "manifest wins", registration: route.setup},
				{name: "binding wins", bindFirst: true, registration: route.setup},
				{name: "stale manifest loses", bindFirst: true, isolation: pgx.RepeatableRead, registration: route.setup},
			} {
				t.Run(tc.name, func(t *testing.T) {
					testLegacyBindingRace(t, func(ctx context.Context, db bindingExecutor) error {
						_, err := db.Exec(ctx, route.statement)
						return err
					}, tc)
				})
			}
		})
	}
	t.Run("owned manifest waits for binding", testOwnedManifestWaitsForLegacyBinding)
}

func testLegacyBindingRace(t *testing.T, bind bindingStatement, tc bindingRace) {
	t.Helper()
	c := jobsDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if tc.registration != "" {
		grantExecContext(ctx, t, c, tc.registration)
	}
	holder, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback(context.WithoutCancel(ctx)) }()
	waiter, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: tc.isolation})
	require.NoError(t, err)
	defer func() { _ = waiter.Rollback(context.WithoutCancel(ctx)) }()

	holderPID, waiterPID := holder.Conn().PgConn().PID(), waiter.Conn().PgConn().PID()
	var prior *string
	require.NoError(t, waiter.QueryRow(ctx, `SELECT legacy_store FROM vector_state`).Scan(&prior))
	require.Nil(t, prior)
	first, second := insertBindingManifest, bind
	if tc.bindFirst {
		first, second = bind, insertBindingManifest
	}
	require.NoError(t, first(ctx, holder))
	waitCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- second(waitCtx, waiter)
	}()
	defer func() {
		stop()
		for range done {
		}
	}()
	waitForLegacyBindingLock(ctx, t, c, holderPID, waiterPID)
	wantBound, wantWriting := false, int64(0)
	if tc.rollback {
		require.NoError(t, holder.Rollback(ctx))
	} else {
		require.NoError(t, holder.Commit(ctx))
		wantBound, wantWriting = tc.bindFirst, 1
		if wantBound {
			wantWriting = 0
		}
	}
	select {
	case err = <-done:
	case <-ctx.Done():
		t.Fatal("waiting statement did not return")
	}
	switch {
	case tc.bindFirst && !tc.rollback && tc.isolation != "":
		requireLegacyBindingSQLState(t, err, "40001")
	case !tc.bindFirst && tc.isolation != "":
		requireCutoverRefusal(t, err)
		require.ErrorContains(t, err, "first PostgreSQL legacy binding requires READ COMMITTED")
	case tc.rollback:
		require.NoError(t, err)
		require.NoError(t, waiter.Commit(ctx))
		wantBound = !tc.bindFirst
		if tc.bindFirst {
			wantWriting = 1
		}
	case tc.bindFirst:
		require.ErrorContains(t, err, "PostgreSQL generation is active")
	default:
		requireLegacyBindRefusal(t, err)
	}
	if err != nil {
		require.NoError(t, waiter.Rollback(ctx))
	}
	requireBindingLocksReleased(ctx, t, c, holderPID, waiterPID)
	if wantBound && tc.isolation != "" {
		retryErr := insertBindingManifest(ctx, c.Pool)
		requireLegacyBindingSQLState(t, retryErr, "55000")
		require.ErrorContains(t, retryErr, "PostgreSQL generation is active")
	}
	var store string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT store FROM vector_writer_state()`).Scan(&store))
	require.Equal(t, wantBound, store == "postgres")
	counts, err := c.HostedManifestCutoverCounts(ctx)
	require.NoError(t, err)
	require.Equal(t, HostedManifestCutoverCounts{Writing: wantWriting}, counts)
}

func testOwnedManifestWaitsForLegacyBinding(t *testing.T) {
	c := manifestDatabase(t)
	doc := acceptManifested(t, c, "hosted job with an owned lease")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	job, err := c.ClaimJob(ctx, "hosted-writer", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, doc, job.Document.ID)

	writer, err := NewClient(ctx, c.Pool.Config().ConnString(), func(cfg *pgxpool.Config) { cfg.MaxConns = 1 })
	require.NoError(t, err)
	t.Cleanup(writer.Pool.Close)
	conn, err := writer.Pool.Acquire(ctx)
	require.NoError(t, err)
	pid := conn.Conn().PgConn().PID()
	conn.Release()

	var before, after string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT to_jsonb(j)::text FROM ingest_jobs j WHERE document_id=$1`, doc).Scan(&before))
	holder, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback(context.WithoutCancel(ctx)) }()
	require.NoError(t, bindLegacyStatement(ctx, holder))
	holderPID := holder.Conn().PgConn().PID()
	revision := uuid.NewString()
	waitCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- writer.PersistVectorManifest(waitCtx, *job, domain.VectorManifest{
			RevisionID: revision, DocumentID: doc, Incarnation: job.Incarnation, Attempt: job.Attempts, LeaseToken: job.LeaseToken,
			IDPrefix: domain.RevisionVectorPrefix(doc, revision), VectorIDs: []string{domain.FormatRevisionVectorID(doc, revision, 0)},
			ChunkIDs: []string{uuid.NewString()},
		})
	}()
	defer func() {
		stop()
		for range done {
		}
	}()
	waitForLegacyBindingLock(ctx, t, c, holderPID, pid)
	require.NoError(t, holder.Commit(ctx))
	select {
	case err = <-done:
		require.ErrorContains(t, err, "PostgreSQL generation is active")
	case <-ctx.Done():
		t.Fatal("manifest insert did not return")
	}
	requireBindingLocksReleased(ctx, t, c, holderPID, pid)
	got, err := c.GetVectorManifest(ctx, revision)
	require.NoError(t, err)
	require.Nil(t, got)
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT to_jsonb(j)::text FROM ingest_jobs j WHERE document_id=$1`, doc).Scan(&after))
	require.Equal(t, before, after)
}

func TestEffectiveLegacyRegistrationRequiresHostedDrain(t *testing.T) {
	for _, tc := range []struct{ name, setup, statement string }{
		{name: "default staging", statement: insertLegacyPostgres},
		{name: "explicit verified", statement: `INSERT INTO vector_generations
			(name, store, target, profile, profile_hash, profile_origin, state)
			VALUES ('legacy','postgres','chunk_vectors','{}','test','recorded','verified')`},
		{name: "explicit retired", statement: `INSERT INTO vector_generations
			(name, store, target, profile, profile_hash, profile_origin, state)
			VALUES ('legacy','postgres','chunk_vectors','{}','test','recorded','retired')`},
		{name: "store update", setup: registerLegacyPinecone, statement: updateLegacyPostgres},
		{name: "rename", setup: `INSERT INTO vector_generations
			(name, store, target, profile, profile_hash, profile_origin)
			VALUES ('g2','postgres','chunk_vectors','{}','test','recorded')`,
			statement: `UPDATE vector_generations SET name='legacy' WHERE name='g2'`},
		{name: "upsert store", setup: registerLegacyPinecone, statement: upsertLegacyPostgres},
		{name: "upsert rename", setup: `INSERT INTO vector_generations
			(name, store, target, profile, profile_hash, profile_origin)
			VALUES ('g2','postgres','chunk_vectors','{}','test','recorded')`,
			statement: `INSERT INTO vector_generations
			(name, store, target, profile, profile_hash, profile_origin)
			VALUES ('g2','postgres','chunk_vectors','{}','test','recorded')
			ON CONFLICT (name) DO UPDATE SET name='legacy'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := startTestDatabase(t)
			c := migrationTestClient(t, dsn)
			require.NoError(t, c.RunMigrations(t.Context()))
			roles := newGrantRoles(t, dsn, "reindex")
			grantExec(t, c, roles.sql(`CREATE ROLE {reindex};
				GRANT SELECT, INSERT, UPDATE ON vector_generations TO {reindex};
				GRANT SELECT, UPDATE ON vector_state TO {reindex}`))
			reindex := grantRoleClient(t, dsn, roles.sql(`SET ROLE {reindex}`))
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			if tc.setup != "" {
				grantExecContext(ctx, t, c, tc.setup)
			}
			seedLegacyBindingManifest(ctx, t, c, "north", "writing", "")
			before := vectorBindingSnapshot(ctx, t, c.Pool)
			var beforeStore string
			require.NoError(t, c.Pool.QueryRow(ctx, `SELECT store FROM vector_writer_state()`).Scan(&beforeStore))
			_, err := reindex.Pool.Exec(ctx, tc.statement)
			requireLegacyBindRefusal(t, err)
			require.Equal(t, before, vectorBindingSnapshot(ctx, t, c.Pool))
			var store string
			require.NoError(t, c.Pool.QueryRow(ctx, `SELECT store FROM vector_writer_state()`).Scan(&store))
			require.Equal(t, beforeStore, store)
			counts, err := c.HostedManifestCutoverCounts(ctx)
			require.NoError(t, err)
			require.Equal(t, HostedManifestCutoverCounts{Writing: 1}, counts)

			grantExecContext(ctx, t, c, `DELETE FROM hosted_vector_manifests`)
			tag, err := reindex.Pool.Exec(ctx, tc.statement)
			require.NoError(t, err)
			require.EqualValues(t, 1, tag.RowsAffected())
			require.NoError(t, c.Pool.QueryRow(ctx, `SELECT store FROM vector_writer_state()`).Scan(&store))
			require.Equal(t, "postgres", store)
			err = insertBindingManifest(ctx, c.Pool)
			requireLegacyBindingSQLState(t, err, "55000")
		})
	}
}

func TestLegacyRegistrationNoOpsPreserveHostedJournal(t *testing.T) {
	for _, tc := range []struct{ name, setup, statement string }{
		{name: "conflict on name", setup: registerLegacyPinecone,
			statement: insertLegacyPostgres + ` ON CONFLICT (name) DO NOTHING`},
		{name: "conflict on target", setup: `INSERT INTO vector_generations
			(name, store, target, profile, profile_hash, profile_origin)
			VALUES ('g2','postgres','chunk_vectors','{}','test','recorded')`,
			statement: insertLegacyPostgres + ` ON CONFLICT (store, target) DO NOTHING`},
		{name: "existing postgres route", setup: insertLegacyPostgres,
			statement: `UPDATE vector_generations SET name=name, store=store WHERE name='legacy'`},
		{name: "unchanged postgres upsert", setup: insertLegacyPostgres, statement: upsertLegacyPostgres},
		{name: "existing pinecone route", setup: registerLegacyPinecone,
			statement: `UPDATE vector_generations SET store=store WHERE name='legacy'`},
		{name: "inactive postgres name", setup: registerLegacyPinecone,
			statement: `INSERT INTO vector_generations
			(name, store, target, profile, profile_hash, profile_origin)
			VALUES ('g2','postgres','chunk_vectors_g2','{}','test','recorded')`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := migrationTestClient(t, startTestDatabase(t))
			applyMigrationPrefix(t, c, 46)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			seedLegacyBindingManifest(ctx, t, c, "north", "retired", "unresolved")
			grantExecContext(ctx, t, c, tc.setup)
			require.NoError(t, c.RunMigrations(ctx))
			const routeQuery = `SELECT w.store, w.target, s.xmin::text FROM vector_writer_state() w CROSS JOIN vector_state s`
			var before, after [3]string
			require.NoError(t, c.Pool.QueryRow(ctx, routeQuery).Scan(&before[0], &before[1], &before[2]))
			journal := legacyBindingJournal(ctx, t, c)
			grantExecContext(ctx, t, c, tc.statement)
			require.NoError(t, c.Pool.QueryRow(ctx, routeQuery).Scan(&after[0], &after[1], &after[2]))
			require.Equal(t, before, after)
			require.Equal(t, journal, legacyBindingJournal(ctx, t, c))
		})
	}
}

func TestLegacyBindingCannotProduceHostedManifest(t *testing.T) {
	const manifestInsert = `INSERT INTO hosted_vector_manifests
		(revision_id, document_id, incarnation, attempt, id_prefix, vector_ids, chunk_ids)
		SELECT gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 1, `
	const manifestIDs = `, ARRAY['vector'], '{}'::uuid[]`
	for _, tc := range []struct {
		name, statement string
		bind            bindingStatement
	}{
		{name: "writable registry CTE", statement: `WITH binding AS (
			INSERT INTO vector_generations
				(name, store, target, profile, profile_hash, profile_origin, state)
			VALUES ('legacy','postgres','chunk_vectors','{}','test','recorded','active')
			RETURNING name
		) ` + manifestInsert + `name` + manifestIDs + ` FROM binding`, bind: registerLegacyStatement},
		{name: "materialized binder CTE", statement: `WITH binding AS MATERIALIZED (` + bindLegacyPostgres + `) ` +
			manifestInsert + `'physical'` + manifestIDs + ` FROM binding`, bind: bindLegacyStatement},
		{name: "binder in the manifest input", statement: manifestInsert +
			`COALESCE((` + bindLegacyPostgres + `)::text, '') || 'physical'` + manifestIDs,
			bind: bindLegacyStatement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := jobsDatabase(t)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			before := vectorBindingSnapshot(ctx, t, c.Pool)
			_, err := c.Pool.Exec(ctx, tc.statement)
			requireLegacyBindingSQLState(t, err, "55000")
			require.Equal(t, before, vectorBindingSnapshot(ctx, t, c.Pool))

			require.NoError(t, insertBindingManifest(ctx, c.Pool))
			counts, err := c.HostedManifestCutoverCounts(ctx)
			require.NoError(t, err)
			require.Equal(t, HostedManifestCutoverCounts{Writing: 1}, counts)
			grantExecContext(ctx, t, c, `DELETE FROM hosted_vector_manifests`)
			require.NoError(t, tc.bind(ctx, c.Pool))
			var store string
			require.NoError(t, c.Pool.QueryRow(ctx, `SELECT store FROM vector_writer_state()`).Scan(&store))
			require.Equal(t, "postgres", store)
			err = insertBindingManifest(ctx, c.Pool)
			requireLegacyBindingSQLState(t, err, "55000")
		})
	}
}

func TestLegacyRegistrationCannotMaskFirstPhysicalBinding(t *testing.T) {
	for _, tc := range []struct{ name, setup, registration string }{
		{name: "insert", registration: insertLegacyPostgres},
		{name: "update", setup: registerLegacyPinecone, registration: updateLegacyPostgres},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := jobsDatabase(t)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			if tc.setup != "" {
				grantExecContext(ctx, t, c, tc.setup)
			}
			seedLegacyBindingManifest(ctx, t, c, "north", "retired", "unresolved")
			before := vectorBindingSnapshot(ctx, t, c.Pool)
			statement := `WITH registration AS (` + tc.registration + ` RETURNING name)
				SELECT vector_bind_legacy('postgres','chunk_vectors','') FROM registration`
			_, err := c.Pool.Exec(ctx, statement)
			requireLegacyBindRefusal(t, err)
			require.Equal(t, before, vectorBindingSnapshot(ctx, t, c.Pool))

			grantExecContext(ctx, t, c, `DELETE FROM hosted_vector_cleanups; DELETE FROM hosted_vector_manifests`)
			grantExecContext(ctx, t, c, statement)
			var store, physicalStore string
			require.NoError(t, c.Pool.QueryRow(ctx, `SELECT w.store, s.legacy_store
				FROM vector_writer_state() w CROSS JOIN vector_state s`).Scan(&store, &physicalStore))
			require.Equal(t, "postgres", store)
			require.Equal(t, "postgres", physicalStore)
		})
	}
}

func legacyBindingJournal(ctx context.Context, t *testing.T, c *Client) string {
	t.Helper()
	var journal string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT jsonb_build_array(
		(SELECT jsonb_agg(m ORDER BY m.revision_id) FROM hosted_vector_manifests m),
		(SELECT jsonb_agg(c ORDER BY c.revision_id) FROM hosted_vector_cleanups c)
	)::text`).Scan(&journal))
	return journal
}

func TestLegacyBindingGuardPreservesExistingRoutes(t *testing.T) {
	t.Run("serving", func(t *testing.T) {
		c := jobsDatabase(t)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		profile := generationProfile()
		require.ErrorIs(t, c.PinCleanupVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", profile), ErrGenerationDrift)
		require.ErrorIs(t, c.PinVectorGeneration(ctx, "g2", "postgres", "chunk_vectors_g2", "", profile), domain.ErrGenerationMismatch)

		require.NoError(t, c.PinVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", profile))
		require.NoError(t, c.PinVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", profile))
		require.ErrorIs(t, c.PinVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors_g2", "", profile), ErrGenerationDrift)
	})

	t.Run("cleanup", func(t *testing.T) {
		c := migrationTestClient(t, pgtest.StartVector(t))
		require.NoError(t, c.RunMigrations(t.Context()))
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		_, err := NewVectorStore(ctx, c, 3)
		require.NoError(t, err)
		for range 2 {
			require.NoError(t, c.PinCleanupVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", generationProfile()))
		}
	})

	t.Run("registration", func(t *testing.T) {
		c := jobsDatabase(t)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		legacy := stagingGeneration(LegacyGeneration)
		invalid := legacy
		invalid.Target = ""
		_, err := c.RegisterGeneration(ctx, invalid)
		require.ErrorContains(t, err, "generation target is required")

		got, err := c.RegisterGeneration(ctx, legacy)
		require.NoError(t, err)
		require.Equal(t, StateActive, got.State)
		require.Equal(t, got.Profile.Hash(), legacy.Profile.Hash())
		again, err := c.RegisterGeneration(ctx, legacy)
		require.NoError(t, err)
		require.Equal(t, got, again)

		drifted := legacy
		drifted.Profile.ModelRevision = "different"
		_, err = c.RegisterGeneration(ctx, drifted)
		require.ErrorIs(t, err, ErrGenerationDrift)
	})

	t.Run("pinecone and inactive targets with hosted journal", func(t *testing.T) {
		c := jobsDatabase(t)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		seedLegacyBindingManifest(ctx, t, c, "north", "writing", "")
		profile := generationProfile()
		require.NoError(t, c.PinVectorGeneration(ctx, LegacyGeneration, "pinecone", "source", "source-host", profile))
		grantExecContext(ctx, t, c, `UPDATE vector_state SET active_generation = active_generation, legacy_store = legacy_store`)
		got, err := c.RegisterGeneration(ctx, stagingGeneration("g2"))
		require.NoError(t, err)
		require.Equal(t, StateStaging, got.State)

		require.NoError(t, c.FreezeWriters(ctx, "g2"))
		_, err = c.Pool.Exec(ctx, updateCutoverGeneration, "g2")
		requireCutoverRefusal(t, err)
		require.ErrorContains(t, err, "before activating PostgreSQL")
	})

	for _, registered := range []bool{false, true} {
		name := "physical postgres binding with historical work"
		if registered {
			name = "registered postgres binding with historical work"
		}
		t.Run(name, func(t *testing.T) {
			c := migrationTestClient(t, startTestDatabase(t))
			applyMigrationPrefix(t, c, 46)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			seedLegacyBindingManifest(ctx, t, c, "", "retired", "unresolved")
			legacy := stagingGeneration(LegacyGeneration)
			if registered {
				_, err := c.RegisterGeneration(ctx, legacy)
				require.NoError(t, err)
			} else {
				require.NoError(t, c.PinVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", legacy.Profile))
			}
			snapshot := vectorBindingSnapshot(ctx, t, c.Pool)
			require.NoError(t, c.RunMigrations(ctx))
			if registered {
				_, err := c.RegisterGeneration(ctx, legacy)
				require.NoError(t, err)
			} else {
				require.NoError(t, c.PinVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", legacy.Profile))
			}
			require.NoError(t, c.PinExistingVectorGeneration(ctx, LegacyGeneration, "postgres", "chunk_vectors", "", legacy.Profile))
			require.Equal(t, snapshot, vectorBindingSnapshot(ctx, t, c.Pool))
		})
	}

	t.Run("cleanup requires a manifest", func(t *testing.T) {
		c := jobsDatabase(t)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		_, err := c.Pool.Exec(ctx, `INSERT INTO hosted_vector_cleanups
			(revision_id, document_id, incarnation, id_prefix, vector_ids)
			VALUES ($1,$2,$3,'cutover',ARRAY['vector'])`, uuid.NewString(), uuid.NewString(), uuid.NewString())
		requireLegacyBindingSQLState(t, err, "23503")
	})
}

func TestRegisteredLegacyPostgresAllowsStartupWithHistoricalJournal(t *testing.T) {
	for _, tc := range []struct {
		name string
		pin  func(*Client, context.Context, string, string, string, string, domain.EmbeddingProfile) error
	}{
		{name: "serving", pin: (*Client).PinVectorGeneration},
		{name: "cleanup", pin: (*Client).PinCleanupVectorGeneration},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := pgtest.StartVector(t)
			owner := migrationTestClient(t, dsn)
			applyMigrationPrefix(t, owner, 46)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			_, err := NewVectorStore(ctx, owner, 3)
			require.NoError(t, err)
			seedLegacyBindingManifest(ctx, t, owner, "north", "retired", "unresolved")
			legacy := stagingGeneration(LegacyGeneration)
			_, err = owner.RegisterGeneration(ctx, legacy)
			require.NoError(t, err)
			journal := legacyBindingJournal(ctx, t, owner)
			const routeQuery = `SELECT w.store, w.target, w.profile_hash, s.legacy_store, s.legacy_target
				FROM vector_writer_state() w CROSS JOIN vector_state s`
			var store, target, profile string
			var physicalStore, physicalTarget *string
			require.NoError(t, owner.Pool.QueryRow(ctx, routeQuery).Scan(&store, &target, &profile, &physicalStore, &physicalTarget))
			require.Equal(t, legacy.Store, store)
			require.Equal(t, legacy.Target, target)
			require.Equal(t, legacy.Profile.Hash(), profile)
			require.Nil(t, physicalStore)
			require.Nil(t, physicalTarget)
			before := vectorBindingSnapshot(ctx, t, owner.Pool)
			require.NoError(t, owner.RunMigrations(ctx))
			require.Equal(t, before, vectorBindingSnapshot(ctx, t, owner.Pool))

			startup := migrationTestClient(t, dsn)
			require.NoError(t, tc.pin(startup, ctx, legacy.Name, legacy.Store, legacy.Target, "", legacy.Profile))
			require.NoError(t, owner.Pool.QueryRow(ctx, routeQuery).Scan(&store, &target, &profile, &physicalStore, &physicalTarget))
			require.Equal(t, legacy.Store, store)
			require.Equal(t, legacy.Target, target)
			require.Equal(t, legacy.Profile.Hash(), profile)
			require.NotNil(t, physicalStore)
			require.NotNil(t, physicalTarget)
			require.Equal(t, legacy.Store, *physicalStore)
			require.Equal(t, legacy.Target, *physicalTarget)
			require.Equal(t, journal, legacyBindingJournal(ctx, t, owner))

			bound := vectorBindingSnapshot(ctx, t, owner.Pool)
			retry := migrationTestClient(t, dsn)
			require.NoError(t, tc.pin(retry, ctx, legacy.Name, legacy.Store, legacy.Target, "", legacy.Profile))
			drift := migrationTestClient(t, dsn)
			other := legacy.Profile
			other.ModelRevision = "different"
			require.ErrorIs(t, tc.pin(drift, ctx, legacy.Name, legacy.Store, legacy.Target, "", other), ErrGenerationDrift)
			require.ErrorIs(t, tc.pin(drift, ctx, legacy.Name, legacy.Store, "chunk_vectors_g2", "", legacy.Profile), ErrGenerationDrift)
			require.Nil(t, drift.vectorPin.Load())
			require.Equal(t, bound, vectorBindingSnapshot(ctx, t, owner.Pool))
		})
	}
}

func requireLegacyBindingSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, code, pgErr.Code)
}

func TestLegacyBindingGuardUsesMetadataSchemaAndExistingGrants(t *testing.T) {
	dsn := startTestDatabase(t)
	roles := newGrantRoles(t, dsn, "binder", "reindex", "denied")
	admin := migrationTestClient(t, dsn)
	grantExec(t, admin, roles.sql(`CREATE ROLE {binder}; CREATE ROLE {reindex}; CREATE ROLE {denied}`))
	grantExec(t, admin, `CREATE SCHEMA "Legacy Work";
		CREATE SCHEMA "Clean Work";
		CREATE SCHEMA shadow;
		CREATE TABLE shadow.vector_state (singleton BOOLEAN, active_generation TEXT, legacy_store TEXT);
		INSERT INTO shadow.vector_state VALUES (TRUE, 'legacy', 'pinecone');
		CREATE TABLE shadow.vector_generations (name TEXT PRIMARY KEY);
		INSERT INTO shadow.vector_generations VALUES ('legacy');
		CREATE FUNCTION shadow.hosted_manifest_cutover_counts()
			RETURNS TABLE(writing BIGINT, active BIGINT, retired BIGINT, cleanups BIGINT)
			LANGUAGE SQL AS 'SELECT 0::BIGINT, 0::BIGINT, 0::BIGINT, 0::BIGINT';
		CREATE TABLE shadow.hosted_vector_manifests (state TEXT);
		INSERT INTO shadow.hosted_vector_manifests VALUES ('writing')`)
	schemaClient := func(schema string) *Client {
		c, err := NewClient(t.Context(), dsn, func(cfg *pgxpool.Config) {
			cfg.ConnConfig.RuntimeParams["search_path"] = schema
		})
		require.NoError(t, err)
		t.Cleanup(c.Pool.Close)
		return c
	}
	dirty, clean := schemaClient(`"Legacy Work"`), schemaClient(`"Clean Work"`)
	applyMigrationPrefix(t, dirty, 47)
	grantExec(t, admin, roles.sql(`GRANT USAGE ON SCHEMA "Legacy Work", "Clean Work", shadow TO {binder}, {reindex}, {denied};
		GRANT EXECUTE ON FUNCTION "Legacy Work".vector_bind_legacy(text,text,text) TO {binder};
		GRANT SELECT, INSERT, UPDATE ON "Legacy Work".vector_generations TO {reindex};
		GRANT SELECT, UPDATE ON "Legacy Work".vector_state TO {reindex}`))
	require.NoError(t, clean.RunMigrations(t.Context()))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	seedLegacyBindingManifest(ctx, t, dirty, "north", "retired", "unresolved")
	before := vectorBindingACLs(ctx, t, dirty)
	require.NoError(t, dirty.RunMigrations(ctx))
	require.NoError(t, dirty.RunMigrations(ctx))
	requireRecordedMigrations(t, dirty)
	require.Equal(t, before, vectorBindingACLs(ctx, t, dirty))

	binder := grantRoleClient(t, dsn, roles.sql(`SET ROLE {binder}`))
	reindex := grantRoleClient(t, dsn, roles.sql(`SET ROLE {reindex}`))
	denied := grantRoleClient(t, dsn, roles.sql(`SET ROLE {denied}`))
	const qualifiedBind = `SELECT "Legacy Work".vector_bind_legacy('postgres','chunk_vectors','')`
	qualifiedRegister := `INSERT INTO "Legacy Work".vector_generations
		(name, store, target, profile, profile_hash, profile_origin, state)
		VALUES ('legacy','postgres','chunk_vectors','{}','test','recorded','active')`

	for _, tc := range []struct {
		name   string
		client *Client
		query  string
	}{
		{name: "restricted binder", client: binder, query: qualifiedBind},
		{name: "restricted registration", client: reindex, query: qualifiedRegister},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := tc.client.Pool.Begin(ctx)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
			_, err = tx.Exec(ctx, `CREATE TEMP TABLE vector_state (singleton BOOLEAN, active_generation TEXT, legacy_store TEXT);
				INSERT INTO pg_temp.vector_state VALUES (TRUE, 'legacy', 'pinecone');
				SET LOCAL search_path = pg_temp, shadow, "Legacy Work", pg_catalog`)
			require.NoError(t, err)
			_, err = tx.Exec(ctx, tc.query)
			requireLegacyBindRefusal(t, err)
			require.NoError(t, tx.Rollback(ctx))
		})
	}

	for _, tc := range []struct {
		name   string
		client *Client
		query  string
	}{
		{name: "ungranted binder", client: denied, query: qualifiedBind},
		{name: "ungranted registration", client: denied, query: qualifiedRegister},
		{name: "ungranted counts", client: binder, query: `SELECT * FROM "Legacy Work".hosted_manifest_cutover_counts()`},
		// Use scalar calls because a trigger result cannot be expanded by FROM.
		{name: "ungranted cutover trigger", client: denied, query: `SELECT "Legacy Work".guard_hosted_manifest_cutover()`},
		{name: "ungranted insert trigger", client: denied, query: `SELECT "Legacy Work".guard_hosted_manifest_insert()`},
		{name: "reindex cannot call cutover trigger", client: reindex, query: `SELECT "Legacy Work".guard_hosted_manifest_cutover()`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.client.Pool.Exec(ctx, tc.query)
			requireLegacyBindingSQLState(t, err, "42501")
		})
	}

	// Neither the shadow schema nor the other migrated corpus can veto a
	// first binding when this corpus's own journal is empty.
	grantExecContext(ctx, t, admin, `CREATE OR REPLACE FUNCTION shadow.hosted_manifest_cutover_counts()
		RETURNS TABLE(writing BIGINT, active BIGINT, retired BIGINT, cleanups BIGINT)
		LANGUAGE SQL AS 'SELECT 1::BIGINT, 0::BIGINT, 0::BIGINT, 0::BIGINT'`)
	cleanTx, err := clean.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = cleanTx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = cleanTx.Exec(ctx, `SET LOCAL search_path = shadow, "Legacy Work", "Clean Work", pg_catalog`)
	require.NoError(t, err)
	_, err = cleanTx.Exec(ctx, `SELECT "Clean Work".vector_bind_legacy('postgres','chunk_vectors','')`)
	require.NoError(t, err)
	require.NoError(t, cleanTx.Commit(ctx))
	counts, err := dirty.HostedManifestCutoverCounts(ctx)
	require.NoError(t, err)
	require.Equal(t, HostedManifestCutoverCounts{Retired: 1, Cleanups: 1}, counts)

	grantExecContext(ctx, t, dirty, `DELETE FROM hosted_vector_cleanups; DELETE FROM hosted_vector_manifests`)
	t.Run("restricted registration with an empty journal", func(t *testing.T) {
		tx, err := reindex.Pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		_, err = tx.Exec(ctx, `SET LOCAL search_path = shadow, "Clean Work", "Legacy Work", pg_catalog`)
		require.NoError(t, err)
		const stateQuery = `SELECT to_jsonb(s)::text, xmin::text FROM "Legacy Work".vector_state s`
		var before, after, oldVersion, newVersion string
		require.NoError(t, tx.QueryRow(ctx, stateQuery).Scan(&before, &oldVersion))
		_, err = tx.Exec(ctx, qualifiedRegister)
		require.NoError(t, err)
		require.NoError(t, tx.QueryRow(ctx, stateQuery).Scan(&after, &newVersion))
		require.Equal(t, before, after)
		require.NotEqual(t, oldVersion, newVersion)
		require.NoError(t, tx.Rollback(ctx))
	})
	testCanceledLegacyBinding(ctx, t, dirty, binder, qualifiedBind)
}

func vectorBindingACLs(ctx context.Context, t *testing.T, c *Client) string {
	t.Helper()
	var acl string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT jsonb_agg(jsonb_build_array(p.oid, p.proname, p.proacl)
		ORDER BY p.proname)::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
		WHERE n.nspname=current_schema() AND p.proname IN ('vector_bind_legacy', 'vector_writer_state',
			'guard_hosted_manifest_cutover', 'guard_hosted_manifest_insert', 'hosted_manifest_cutover_counts')`).Scan(&acl))
	return acl
}

func testCanceledLegacyBinding(ctx context.Context, t *testing.T, owner, binder *Client, statement string) {
	t.Helper()
	holder, err := owner.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback(context.WithoutCancel(ctx)) }()
	require.NoError(t, insertBindingManifest(ctx, holder))
	waiter, err := binder.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = waiter.Rollback(context.WithoutCancel(ctx)) }()
	holderPID, waiterPID := holder.Conn().PgConn().PID(), waiter.Conn().PgConn().PID()
	waitCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		_, waitErr := waiter.Exec(waitCtx, statement)
		done <- waitErr
	}()
	defer func() {
		stop()
		for range done {
		}
	}()
	waitForLegacyBindingLock(ctx, t, owner, holderPID, waiterPID)
	stop()
	select {
	case err = <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("canceled binding did not return")
	}
	// A canceled query can leave an aborted transaction or a closed connection.
	_ = waiter.Rollback(ctx)
	require.NoError(t, holder.Rollback(ctx))
	requireBindingLocksReleased(ctx, t, owner, holderPID, waiterPID)
	grantExecContext(ctx, t, binder, statement)
	state, err := owner.VectorState(ctx)
	require.NoError(t, err)
	require.Equal(t, "postgres", state.LegacyStore)
	counts, err := owner.HostedManifestCutoverCounts(ctx)
	require.NoError(t, err)
	require.Empty(t, counts)
}
