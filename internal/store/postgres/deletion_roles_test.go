package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestRestrictedServingLoginReadsDeletionStateThroughOwnerPredicates(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	_, err := serving.Pool.Exec(ctx, `SELECT 1 FROM document_deletion_activation`)
	requireSQLState(t, err, "42501")

	// Ordinary traffic on an inactive database needs no privilege on the marker.
	active, err := serving.DeletionActive(ctx)
	require.NoError(t, err)
	require.False(t, active)
	admitted, err := serving.AcceptDocument(ctx, jobsRequest("restricted admission"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, admitted.Outcome)

	_, err = owner.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	active, err = serving.DeletionActive(ctx)
	require.NoError(t, err)
	require.True(t, active)
}

func TestRestrictedServingLoginRunsTheCheckpointedCoordinator(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	_, err := owner.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	doc := acceptDocument(t, owner, "restricted coordinator", true, true)
	job, err := serving.ClaimJob(ctx, "worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)

	var signedKey string
	found, err := serving.WithDocumentFile(ctx, doc.id, func(_ context.Context, key string) error {
		signedKey = key
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, doc.key, signedKey)

	recorder := newDeletionRecorder()
	require.NoError(t, serving.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
	require.Equal(t, []string{"revision:" + string(doc.id) + "/" + doc.incarnation, "source:" + doc.key}, recorder.all())
	require.True(t, readReceipt(t, owner, doc.id).finalized)
	require.Empty(t, documentStatus(t, owner, doc.id))
	require.NoError(t, serving.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), "the receipt acknowledges the retry")

	reused, err := serving.AcceptDocument(ctx, func() domain.AcceptRequest {
		req := jobsRequest("replacement under the restricted login")
		req.Document.ID, req.CallerSupplied = doc.id, true
		return req
	}())
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, reused.Outcome, "the owner-defined predicate sees the released receipt")
}

func TestPublishedGrantsCoverTheDeletionPredicates(t *testing.T) {
	owner, _, roles := restrictedDatabase(t)
	var executable bool
	require.NoError(t, owner.Pool.QueryRow(t.Context(), `SELECT
		has_function_privilege($1,'document_deletion_active()','EXECUTE')
		AND has_function_privilege($1,'document_deletion_active_shared()','EXECUTE')
		AND has_function_privilege($1,'document_deletion_reserved(uuid)','EXECUTE')
		AND has_table_privilege($1,'document_deletions','SELECT,INSERT,UPDATE')
		AND NOT has_table_privilege($1,'document_deletion_activation','SELECT,INSERT,UPDATE,DELETE')`,
		roles.name("quarry_service")).Scan(&executable))
	require.True(t, executable)
}

func TestFileSigningNeedsOnlySelect(t *testing.T) {
	owner := activatedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, owner, "select only signing", true, true)
	roles := newGrantRoles(t, owner.Pool.Config().ConnString(), "file_reader")
	role := pgx.Identifier{roles.name("file_reader")}.Sanitize()
	grantExec(t, owner, "CREATE ROLE "+role)
	grantExec(t, owner, "GRANT USAGE ON SCHEMA public TO "+role)
	grantExec(t, owner, "GRANT SELECT ON public.documents TO "+role)
	reader := roleClient(t, owner, roles.name("file_reader"))

	var signedKey string
	found, err := reader.WithDocumentFile(ctx, doc.id, func(_ context.Context, key string) error {
		signedKey = key
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, doc.key, signedKey)

	_, err = reader.Pool.Exec(ctx, `UPDATE documents SET updated_at=NOW() WHERE id=$1`, string(doc.id))
	requireSQLState(t, err, "42501")
}

func waitForLockWaiter(t *testing.T, c *Client, queryPrefix string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting bool
		err := c.Pool.QueryRow(t.Context(), `SELECT EXISTS(
			SELECT FROM pg_stat_activity
			WHERE pid<>pg_backend_pid()
			  AND datname=current_database()
			  AND wait_event_type='Lock'
			  AND starts_with(query, $1))`, queryPrefix).Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 10*time.Millisecond, "query %q must wait for a row lock", queryPrefix)
}

func TestRestrictedServingLoginSelectsTheVectorFormat(t *testing.T) {
	t.Run("inactive", func(t *testing.T) {
		owner, serving, _ := restrictedDatabase(t)
		ctx := t.Context()
		req := jobsRequest("restricted tagged inactive")
		req.SupportsScopedVectors = true
		admitted, err := serving.AcceptDocument(ctx, req)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, admitted.Outcome)

		var scoped bool
		require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT scoped_vectors FROM documents WHERE id=$1`, string(admitted.DocumentID)).Scan(&scoped))
		require.False(t, scoped)
	})

	t.Run("active", func(t *testing.T) {
		owner, serving, _ := restrictedDatabase(t)
		ctx := t.Context()
		_, err := owner.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
		require.NoError(t, err)

		req := jobsRequest("restricted tagged active")
		req.SupportsScopedVectors = true
		admitted, err := serving.AcceptDocument(ctx, req)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, admitted.Outcome)

		var scoped bool
		require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT scoped_vectors FROM documents WHERE id=$1`, string(admitted.DocumentID)).Scan(&scoped))
		require.True(t, scoped)
	})

	t.Run("no_table_privilege", func(t *testing.T) {
		owner, serving, roles := restrictedDatabase(t)
		ctx := t.Context()
		role := roles.name("quarry_service")
		assertNoPrivilege := func() {
			t.Helper()
			var privileged bool
			require.NoError(t, owner.Pool.QueryRow(ctx,
				`SELECT has_table_privilege($1,'document_deletion_activation','SELECT,INSERT,UPDATE,DELETE')`, role).Scan(&privileged))
			require.False(t, privileged)
			_, err := serving.Pool.Exec(ctx, `SELECT 1 FROM document_deletion_activation`)
			requireSQLState(t, err, "42501")
		}

		assertNoPrivilege()
		_, err := owner.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
		require.NoError(t, err)
		assertNoPrivilege()
	})
}

func TestDeletionModeLockIsNotInherited(t *testing.T) {
	databaseURL := startTestDatabase(t)
	admin := migrationTestClient(t, databaseURL)
	applyMigrationPrefix(t, admin, 29)
	roles := newGrantRoles(t, databaseURL, servingRoles...)
	grantExec(t, admin, roles.sql(`CREATE ROLE {quarry_service}; CREATE ROLE {quarry_operator}; CREATE ROLE {quarry_auditor};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {quarry_service}, {quarry_operator};
		ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {quarry_service}, {quarry_operator}`))
	require.NoError(t, admin.RunMigrations(t.Context()))

	var inherited bool
	require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT
		has_function_privilege('{quarry_service}','document_deletion_active_shared()','execute')
		OR has_function_privilege('{quarry_operator}','document_deletion_active_shared()','execute')
		OR has_function_privilege('public','document_deletion_active_shared()','execute')`)).Scan(&inherited))
	require.False(t, inherited, "document_deletion_active_shared must not be reachable through default or PUBLIC privileges")
}

func TestDeletionModeLockDefinition(t *testing.T) {
	owner, _, _ := restrictedDatabase(t)
	var secdef bool
	var volatile string
	var config []string
	var fnOwner, currentUser string
	require.NoError(t, owner.Pool.QueryRow(t.Context(), `SELECT p.prosecdef, p.provolatile::text, p.proconfig, pg_get_userbyid(p.proowner), current_user
		FROM pg_proc p
		WHERE p.oid = 'document_deletion_active_shared()'::regprocedure`).Scan(&secdef, &volatile, &config, &fnOwner, &currentUser))
	require.True(t, secdef)
	require.Equal(t, "v", volatile)
	require.Contains(t, config, "search_path=pg_catalog, pg_temp")
	require.Equal(t, currentUser, fnOwner)
}

func TestActivationWaitsForTaggedAdmission(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()

	tx, err := serving.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var active bool
	require.NoError(t, tx.QueryRow(ctx, `SELECT document_deletion_active_shared()`).Scan(&active))
	require.False(t, active)

	const activationUpdate = `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`
	done := make(chan error, 1)
	go func() {
		_, updateErr := owner.Pool.Exec(ctx, activationUpdate)
		done <- updateErr
	}()
	waitForLockWaiter(t, owner, activationUpdate)
	select {
	case updateErr := <-done:
		require.FailNowf(t, "activation completed before admission committed", "err=%v", updateErr)
	default:
	}

	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, <-done)
}

func TestTaggedAdmissionWaitsForActivation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		commit bool
	}{
		{name: "committed", commit: true},
		{name: "rolled_back", commit: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, serving, _ := restrictedDatabase(t)
			ctx := t.Context()

			tx, err := owner.Pool.Begin(ctx)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
			_, err = tx.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
			require.NoError(t, err)

			type acceptOutcome struct {
				result domain.AcceptResult
				err    error
			}
			done := make(chan acceptOutcome, 1)
			req := jobsRequest("waiting tagged admission " + tc.name)
			req.SupportsScopedVectors = true
			go func() {
				res, acceptErr := serving.AcceptDocument(ctx, req)
				done <- acceptOutcome{result: res, err: acceptErr}
			}()

			waitForLockWaiter(t, owner, `SELECT document_deletion_active_shared()`)
			select {
			case out := <-done:
				require.FailNowf(t, "admission completed before activation finished", "outcome=%+v err=%v", out.result, out.err)
			default:
			}

			if tc.commit {
				require.NoError(t, tx.Commit(ctx))
			} else {
				require.NoError(t, tx.Rollback(ctx))
			}
			out := <-done
			require.NoError(t, out.err)
			require.Equal(t, domain.AcceptAccepted, out.result.Outcome)

			var scoped bool
			require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT scoped_vectors FROM documents WHERE id=$1`, string(out.result.DocumentID)).Scan(&scoped))
			require.Equal(t, tc.commit, scoped)
		})
	}
}

func TestOldAndNewModeLocksBothHoldActivation(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()

	oldTx, err := owner.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = oldTx.Rollback(context.WithoutCancel(ctx)) }()
	var oldActive bool
	require.NoError(t, oldTx.QueryRow(ctx, `SELECT activated_at IS NOT NULL FROM document_deletion_activation WHERE singleton FOR SHARE`).Scan(&oldActive))
	require.False(t, oldActive)

	newTx, err := serving.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = newTx.Rollback(context.WithoutCancel(ctx)) }()
	var newActive bool
	require.NoError(t, newTx.QueryRow(ctx, `SELECT document_deletion_active_shared()`).Scan(&newActive))
	require.False(t, newActive)

	const activationUpdate = `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`
	done := make(chan error, 1)
	go func() {
		_, updateErr := owner.Pool.Exec(ctx, activationUpdate)
		done <- updateErr
	}()
	waitForLockWaiter(t, owner, activationUpdate)

	require.NoError(t, oldTx.Commit(ctx))
	select {
	case updateErr := <-done:
		require.FailNowf(t, "activation completed while the function share lock was still held", "err=%v", updateErr)
	case <-time.After(100 * time.Millisecond):
	}
	waitForLockWaiter(t, owner, activationUpdate)

	require.NoError(t, newTx.Commit(ctx))
	require.NoError(t, <-done)
}

func TestTaggedAdmissionFailsWithoutMarker(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()

	_, err := owner.Pool.Exec(ctx, `DELETE FROM document_deletion_activation`)
	require.NoError(t, err)

	req := jobsRequest("missing marker tagged")
	req.SupportsScopedVectors = true
	_, err = serving.AcceptDocument(ctx, req)
	require.Error(t, err)

	var docs, jobs int
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM documents), (SELECT count(*) FROM ingest_jobs)`).Scan(&docs, &jobs))
	require.Zero(t, docs)
	require.Zero(t, jobs)
}

func TestUntaggedAdmissionNeedsNoModeLock(t *testing.T) {
	owner, serving, roles := restrictedDatabase(t)
	ctx := t.Context()

	grantExec(t, owner, `REVOKE EXECUTE ON FUNCTION document_deletion_active_shared() FROM `+pgx.Identifier{roles.name("quarry_service")}.Sanitize())

	admitted, err := serving.AcceptDocument(ctx, jobsRequest("untagged without mode lock"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, admitted.Outcome)
}

func TestTransactionalStoreAdmissionNeedsNoModeLock(t *testing.T) {
	owner, _ := vectorDatabase(t)
	serving, roles := publishedServingClient(t, owner)
	ctx := t.Context()

	grantExec(t, owner, `REVOKE EXECUTE ON FUNCTION document_deletion_active_shared() FROM `+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
	vs, err := OpenGenerationVectorStore(ctx, serving, 3, LegacyGeneration)
	require.NoError(t, err)
	vs.RouteCompletion()

	req := jobsRequest("pgvector tagged admission")
	req.SupportsScopedVectors = true
	admitted, err := serving.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, admitted.Outcome)

	var scoped bool
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT scoped_vectors FROM documents WHERE id=$1`, string(admitted.DocumentID)).Scan(&scoped))
	require.False(t, scoped)
}

func TestTaggedAdmissionWithoutGrantFailsClosed(t *testing.T) {
	owner, serving, roles := restrictedDatabase(t)
	ctx := t.Context()

	grantExec(t, owner, `REVOKE EXECUTE ON FUNCTION document_deletion_active_shared() FROM `+pgx.Identifier{roles.name("quarry_service")}.Sanitize())

	req := jobsRequest("tagged without grant")
	req.SupportsScopedVectors = true
	_, err := serving.AcceptDocument(ctx, req)
	requireSQLState(t, err, "42501")

	var docs, jobs int
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM documents), (SELECT count(*) FROM ingest_jobs)`).Scan(&docs, &jobs))
	require.Zero(t, docs)
	require.Zero(t, jobs)
}

// UpgradedServingRoleDatabase gives an existing serving role only its original
// service-mode privileges and the SQL from the published upgrade section.
func UpgradedServingRoleDatabase(t *testing.T) (owner, serving *Client) {
	t.Helper()
	owner = jobsDatabase(t)
	roles := newGrantRoles(t, owner.Pool.Config().ConnString(), "quarry_service")
	grantExec(t, owner, roles.sql(`
		CREATE ROLE {quarry_service} LOGIN PASSWORD 'fixture-password';
		GRANT USAGE ON SCHEMA public TO {quarry_service};
		GRANT SELECT ON public.schema_migrations TO {quarry_service};
		GRANT SELECT, INSERT, UPDATE, DELETE ON public.documents, public.chunks,
			public.ingest_jobs, public.document_fingerprints TO {quarry_service};
		GRANT SELECT, INSERT, UPDATE ON public.document_pdf_extractions TO {quarry_service};
		GRANT EXECUTE ON FUNCTION public.vector_writer_state(),
			public.vector_bind_legacy(text, text, text) TO {quarry_service};
	`))
	for _, marker := range []string{
		"-- Serving role upgrade for migrations 019, 023 and 025",
		"-- Serving role upgrade for migration 029",
		"-- Serving role upgrade for migration 030",
		"-- Serving role upgrade for migration 036",
	} {
		grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", marker))
	}
	serving = writerLogin(t, owner.Pool.Config().ConnString(), roles.name("quarry_service"))
	serving.SetAuthzMode("service")
	return owner, serving
}

func TestUpgradedServingRoleAdmitsAcrossActivation(t *testing.T) {
	owner, serving := UpgradedServingRoleDatabase(t)
	_, err := serving.Pool.Exec(t.Context(), `SELECT activated_at FROM document_deletion_activation`)
	requireSQLState(t, err, "42501")

	admit := func(t *testing.T, content string, tagged bool) {
		t.Helper()
		req := jobsRequest(content)
		req.SupportsScopedVectors = true
		result, acceptErr := serving.AcceptDocument(t.Context(), req)
		require.NoError(t, acceptErr)
		require.Equal(t, domain.AcceptAccepted, result.Outcome)
		var scoped bool
		require.NoError(t, owner.Pool.QueryRow(t.Context(), `SELECT scoped_vectors FROM documents WHERE id=$1`, result.DocumentID).Scan(&scoped))
		require.Equal(t, tagged, scoped)
	}

	admit(t, "admitted before activation", false)
	job, err := serving.ClaimJob(t.Context(), "upgraded-serving-worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	_, err = owner.ActivateDeletion(t.Context())
	require.NoError(t, err)
	admit(t, "admitted after activation", true)
}

func TestRestrictedServingLoginReleasesOfflineHeldReceipt(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, owner, "restricted offline release", false, false)
	memoryOwner := uuid.NewString()
	_, err := owner.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,memory_owner,step,held,finalized)
		VALUES($1,$2,'legacy_reservation','memory',$3,0,FALSE,FALSE)`, string(doc.id), doc.incarnation, memoryOwner)
	require.NoError(t, err)
	_, err = owner.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, string(doc.id))
	require.NoError(t, err)

	var namespaceCalls, vectorCalls int
	ops := domain.DeleteOperations{
		InMemory:    true,
		MemoryOwner: memoryOwner,
		LegacyNamespace: func(context.Context, string) error {
			namespaceCalls++
			return nil
		},
		LegacyVectors: func(context.Context, string) error {
			vectorCalls++
			return nil
		},
	}
	require.NoError(t, serving.ReleaseHeldDeletion(ctx, doc.id, doc.incarnation, ops))
	require.Equal(t, 1, namespaceCalls)
	require.Equal(t, 1, vectorCalls)
	receipt := readReceipt(t, owner, doc.id)
	require.True(t, receipt.finalized)
	require.False(t, receipt.held)
}
