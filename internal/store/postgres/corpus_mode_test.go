package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

const runtimeAuditFunction = "grant_catalog_record_security_audit(text,uuid,uuid,bigint,text,text,text,uuid,uuid)"
const runtimeAuditValidator = "grant_catalog_validate_security_audit(text,text)"
const runtimeAuditCall = `SELECT grant_catalog_record_security_audit($1, $2, $3, $4, $5, $6, $7, $8, $9)`
const runtimeAuditShadowRelations = `CREATE TEMP TABLE grant_catalog_policy_revision (singleton boolean, revision bigint);
	INSERT INTO grant_catalog_policy_revision VALUES (true, 19);
	CREATE TEMP TABLE grant_catalog_security_audit (request_id text, tenant_id uuid, principal_id uuid,
		policy_revision bigint, action text, outcome text, reason text, document_id uuid, incarnation uuid)`

type runtimeAuditFunctionIdentity struct {
	oid, owner int64
	acl        string
}

func readRuntimeAuditFunctionIdentity(t *testing.T, owner *Client) runtimeAuditFunctionIdentity {
	t.Helper()
	var identity runtimeAuditFunctionIdentity
	require.NoError(t, owner.Pool.QueryRow(t.Context(), `SELECT oid::bigint, proowner::bigint, proacl::text
		FROM pg_proc WHERE oid = $1::text::regprocedure`, runtimeAuditFunction).Scan(&identity.oid, &identity.owner, &identity.acl))
	return identity
}

func requireRuntimeAuditRejection(t *testing.T, err error, field string) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "22023", pgErr.Code)
	require.Equal(t, "invalid runtime audit "+field, pgErr.Message)
	require.Empty(t, pgErr.Detail)
	require.Empty(t, pgErr.Hint)
	require.Empty(t, pgErr.InternalQuery)
}

// The published 029 upgrade grants work on the historical schema.
func grantLegacyRuntimeAuditAccess(t *testing.T, owner *Client, roles grantRoles) {
	t.Helper()
	grantExec(t, owner, roles.sql(`GRANT USAGE ON SCHEMA public TO {quarry_service}`))
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "Serving role upgrade for migration 029"))
}

func TestRuntimeSecurityAuditWriterRestrictsActions(t *testing.T) {
	ctx := t.Context()
	databaseURL := startTestDatabase(t)
	owner := migrationTestClient(t, databaseURL)
	applyMigrationPrefix(t, owner, 34)
	grantExec(t, owner, `UPDATE grant_catalog_policy_revision SET revision = 17 WHERE singleton`)
	roles := newGrantRoles(t, databaseURL, "quarry_service")
	grantExec(t, owner, roles.sql(`CREATE ROLE {quarry_service}`))
	grantLegacyRuntimeAuditAccess(t, owner, roles)
	serving := roleClient(t, owner, roles.name("quarry_service"))
	before := readRuntimeAuditFunctionIdentity(t, owner)
	conn, err := serving.Pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, runtimeAuditShadowRelations)
	require.NoError(t, err)
	const prepared = "record_runtime_audit"
	_, err = conn.Conn().Prepare(ctx, prepared, runtimeAuditCall)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, prepared, "before upgrade", nil, nil, nil, "query", "allow", "synthetic", nil, nil)
	require.NoError(t, err)

	require.NoError(t, owner.RunMigrations(ctx))
	require.Equal(t, before, readRuntimeAuditFunctionIdentity(t, owner))
	fresh := roleClient(t, owner, roles.name("quarry_service"))
	for _, session := range []struct {
		name, statement string
		db              auditExecer
	}{
		{"prepared connection", prepared, conn},
		{"reconnected", runtimeAuditCall, fresh.Pool},
	} {
		t.Run(session.name, func(t *testing.T) {
			ctx := t.Context()
			var beforeCount int
			require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT count(*) FROM grant_catalog_security_audit`).Scan(&beforeCount))
			actions := []string{"ingest", "query", "document_list", "document_status", "file_sign", "document_delete", "subject_auth", "trust_key_load"}
			outcomes := []string{"allow", "deny", "error"}
			id := uuid.NewString()
			for _, action := range actions {
				for _, outcome := range outcomes {
					_, err := session.db.Exec(ctx, session.statement, nil, nil, nil, nil, action, outcome, session.name, nil, nil)
					require.NoError(t, err, "%s/%s", action, outcome)
					var count int
					require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT count(*) FROM grant_catalog_security_audit
						WHERE action=$1 AND outcome=$2 AND reason=$3 AND request_id='' AND policy_revision=17
						  AND operator IS NULL AND tenant_id IS NULL AND principal_id IS NULL
						  AND document_id IS NULL AND incarnation IS NULL`, action, outcome, session.name).Scan(&count))
					require.Equal(t, 1, count, "%s/%s", action, outcome)
				}
			}
			_, err := session.db.Exec(ctx, session.statement, session.name, id, id, int64(0), "query", "allow", "override", id, id)
			require.NoError(t, err)
			var overrideCount int
			require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT count(*) FROM grant_catalog_security_audit
				WHERE request_id=$1 AND tenant_id=$2 AND principal_id=$2 AND policy_revision=0
				  AND action='query' AND outcome='allow' AND reason='override' AND document_id=$2
				  AND incarnation=$2 AND operator IS NULL`, session.name, id).Scan(&overrideCount))
			require.Equal(t, 1, overrideCount)

			for _, action := range []any{"corpus_cutover", "grant_import", "QUERY", " query", "query ", "", nil, "query\n\t", strings.Repeat("x", 4096)} {
				_, err = session.db.Exec(ctx, session.statement, nil, nil, nil, nil, action, "allow", "synthetic", nil, nil)
				requireRuntimeAuditRejection(t, err, "action")
			}
			for _, outcome := range []any{"unknown", "ALLOW", " allow", "allow ", "", nil, "allow\n\t", strings.Repeat("x", 4096)} {
				_, err = session.db.Exec(ctx, session.statement, nil, nil, nil, nil, "query", outcome, "synthetic", nil, nil)
				requireRuntimeAuditRejection(t, err, "outcome")
			}
			limit, tooLong := strings.Repeat("é", 64), strings.Repeat("é", 64)+"x"
			_, err = session.db.Exec(ctx, session.statement, limit, nil, nil, nil, "query", "allow", limit, nil, nil)
			require.NoError(t, err)
			_, err = session.db.Exec(ctx, session.statement, tooLong, nil, nil, nil, "query", "allow", limit, nil, nil)
			requireSQLState(t, err, "23514")
			_, err = session.db.Exec(ctx, session.statement, limit, nil, nil, nil, "query", "allow", tooLong, nil, nil)
			requireSQLState(t, err, "23514")
			assertGrantCount(t, owner, "security_audit", beforeCount+len(actions)*len(outcomes)+2)
		})
	}
	assertRuntimeAuditShadowEmpty(t, conn.Conn())
}

func TestRuntimeSecurityAuditWriterPreservesRolesAndOperatorEvents(t *testing.T) {
	ctx := t.Context()
	databaseURL := startTestDatabase(t)
	owner := migrationTestClient(t, databaseURL)
	roles := newGrantRoles(t, databaseURL, servingRoles...)
	grantExec(t, owner, roles.sql(`CREATE ROLE {quarry_service}; CREATE ROLE {quarry_operator}; CREATE ROLE {quarry_auditor};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {quarry_service}, {quarry_operator}, {quarry_auditor}`))
	applyMigrationPrefix(t, owner, 34)
	for _, role := range []string{"public", roles.name("quarry_service"), roles.name("quarry_operator"), roles.name("quarry_auditor")} {
		assertRuntimeAuditExecute(t, owner, role, false)
	}
	grantLegacyRuntimeAuditAccess(t, owner, roles)
	grantExec(t, owner, roles.sql(`GRANT USAGE ON SCHEMA public TO {quarry_auditor};
		GRANT SELECT ON public.grant_catalog_security_audit TO {quarry_auditor}`))
	before := readRuntimeAuditFunctionIdentity(t, owner)
	service := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {quarry_service}`))
	operator := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {quarry_operator}`))
	auditor := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {quarry_auditor}`))
	const legacyRows = `SELECT to_jsonb(audit)::text FROM grant_catalog_security_audit AS audit
		WHERE request_id='legacy' AND operator IS NULL ORDER BY audit_id`
	for _, action := range []string{"corpus_cutover", "grant_import"} {
		grantExec(t, service, runtimeAuditCall, "legacy", nil, nil, nil, action, "allow", "synthetic", nil, nil)
	}
	rows, err := auditor.Pool.Query(ctx, legacyRows)
	require.NoError(t, err)
	legacy, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Len(t, legacy, 2)

	require.NoError(t, owner.RunMigrations(ctx))
	require.Equal(t, before, readRuntimeAuditFunctionIdentity(t, owner))
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
	require.Equal(t, before, readRuntimeAuditFunctionIdentity(t, owner))
	assertRuntimeAuditExecute(t, owner, roles.name("quarry_service"), true)
	for _, role := range []string{"public", roles.name("quarry_operator"), roles.name("quarry_auditor")} {
		assertRuntimeAuditExecute(t, owner, role, false)
	}
	for _, role := range []string{"public", roles.name("quarry_service"), roles.name("quarry_operator"), roles.name("quarry_auditor")} {
		var executable bool
		require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT has_function_privilege($1::name, $2::text, 'EXECUTE')`,
			role, runtimeAuditValidator).Scan(&executable))
		require.False(t, executable, role)
	}
	for _, query := range []string{
		`INSERT INTO grant_catalog_security_audit (action, outcome, reason) VALUES ('query', 'allow', 'synthetic')`,
		`DELETE FROM grant_catalog_security_audit`,
		`SELECT * FROM grant_catalog_security_audit`,
		`SELECT * FROM grant_catalog_activate_grants(false)`,
		`SELECT grant_catalog_validate_security_audit('query', 'allow')`,
	} {
		_, err = service.Pool.Exec(ctx, query)
		requireSQLState(t, err, "42501")
	}
	_, err = auditor.Pool.Exec(ctx, runtimeAuditCall, nil, nil, nil, nil, "query", "allow", "synthetic", nil, nil)
	requireSQLState(t, err, "42501")

	_, err = operator.ActivateGrants(ctx, false)
	require.EqualError(t, err, "grants activation: checkpointed document deletion must be activated first")
	assertGrantCount(t, owner, "security_audit", len(legacy))
	_, err = owner.ActivateDeletion(ctx)
	require.NoError(t, err)
	activation, err := operator.ActivateGrants(ctx, false)
	require.NoError(t, err)
	require.False(t, activation.AlreadyActive)
	batch := testGrantBatch(activation.Revision)
	batch["tenants"] = []grantEntry{{"tenant_id": uuid.NewString()}}
	imported, err := importTestBatch(ctx, operator, batch, false)
	require.NoError(t, err)
	for _, event := range []struct {
		action, reason string
		revision       int64
	}{
		{"corpus_cutover", "grants_activated", activation.Revision},
		{"grant_import", "batch_committed", imported.Revision},
	} {
		var count int
		require.NoError(t, auditor.Pool.QueryRow(ctx, `SELECT count(*) FROM grant_catalog_security_audit
			WHERE action=$1 AND reason=$2 AND policy_revision=$3 AND operator=$4 AND outcome='allow'`,
			event.action, event.reason, event.revision, roles.name("quarry_operator")).Scan(&count))
		require.Equal(t, 1, count, event.action)
	}
	rows, err = auditor.Pool.Query(ctx, legacyRows)
	require.NoError(t, err)
	after, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Equal(t, legacy, after)
	assertGrantCount(t, owner, "security_audit", 4)
}

func assertRuntimeAuditExecute(t *testing.T, owner *Client, role string, want bool) {
	t.Helper()
	var executable bool
	require.NoError(t, owner.Pool.QueryRow(t.Context(), `SELECT has_function_privilege($1::name, $2::text, 'EXECUTE')`,
		role, runtimeAuditFunction).Scan(&executable))
	require.Equal(t, want, executable, role)
}

func TestRuntimeSecurityAuditWriterBindsMetadataSchema(t *testing.T) {
	ctx := t.Context()
	databaseURL := startTestDatabase(t)
	admin := migrationTestClient(t, databaseURL)
	require.NoError(t, admin.RunMigrations(ctx))
	grantExec(t, admin, `CREATE SCHEMA "AuditCorpus"; UPDATE public.grant_catalog_policy_revision SET revision=11`)
	owner := grantRoleClient(t, databaseURL, `SET search_path TO "AuditCorpus", public`)
	require.NoError(t, owner.RunMigrations(ctx))
	grantExec(t, owner, `UPDATE grant_catalog_policy_revision SET revision=13 WHERE singleton`)
	roles := newGrantRoles(t, databaseURL, servingRoles...)
	grantExec(t, owner, roles.sql(`CREATE ROLE {quarry_service}; CREATE ROLE {quarry_operator}; CREATE ROLE {quarry_auditor}`))
	grants := publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public")
	grantExec(t, owner, strings.NewReplacer("ON SCHEMA public", `ON SCHEMA "AuditCorpus"`, "public.", `"AuditCorpus".`).Replace(grants))
	assertRuntimeAuditExecute(t, owner, roles.name("quarry_service"), true)
	assertRuntimeAuditExecute(t, owner, roles.name("quarry_auditor"), false)
	assertRuntimeAuditExecute(t, owner, "public", false)
	service := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {quarry_service}; SET search_path TO pg_temp, "AuditCorpus", public`))
	conn, err := service.Pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, runtimeAuditShadowRelations)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, runtimeAuditCall, "scoped", nil, nil, nil, "query", "allow", "synthetic", nil, nil)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, runtimeAuditCall, nil, nil, nil, nil, "corpus_cutover", "allow", "synthetic", nil, nil)
	requireRuntimeAuditRejection(t, err, "action")
	var revision int64
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT policy_revision FROM "AuditCorpus".grant_catalog_security_audit`).Scan(&revision))
	require.EqualValues(t, 13, revision)
	assertGrantCount(t, owner, "security_audit", 1)
	assertRuntimeAuditShadowEmpty(t, conn.Conn())
	assertGrantCount(t, admin, "security_audit", 0)

	t.Run("case-insensitive collation", func(t *testing.T) {
		ctx := t.Context()
		var icu bool
		require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_collation WHERE collprovider='i')`).Scan(&icu))
		if !icu {
			t.Skip("requires ICU collation support")
		}
		grantExec(t, owner, `CREATE COLLATION "AuditCorpus".case_insensitive (provider=icu, locale='und-u-ks-level2', deterministic=false)`)
		var same bool
		require.NoError(t, conn.QueryRow(ctx, `SELECT 'QUERY' COLLATE "AuditCorpus".case_insensitive = 'query'`).Scan(&same))
		require.True(t, same)
		const collatedCall = `SELECT grant_catalog_record_security_audit($1, $2, $3, $4,
			$5::text COLLATE "AuditCorpus".case_insensitive, $6, $7, $8, $9)`
		_, err := conn.Exec(ctx, collatedCall, nil, nil, nil, nil, "QUERY", "allow", "synthetic", nil, nil)
		requireRuntimeAuditRejection(t, err, "action")
		_, err = conn.Exec(ctx, collatedCall, nil, nil, nil, nil, "query", "ALLOW", "synthetic", nil, nil)
		requireRuntimeAuditRejection(t, err, "outcome")
		assertGrantCount(t, owner, "security_audit", 1)
	})
}

func TestRuntimeSecurityAuditWriterRestoresDefinitions(t *testing.T) {
	ctx := t.Context()
	databaseURL := startTestDatabase(t)
	source := migrationTestClient(t, databaseURL)
	require.NoError(t, source.RunMigrations(ctx))
	grantExec(t, source, `ALTER SCHEMA public RENAME TO "Restored Corpus"`)
	var validatorDDL, writerDDL string
	require.NoError(t, source.Pool.QueryRow(ctx, `SELECT pg_get_functiondef(validator.oid), pg_get_functiondef(writer.oid)
		FROM pg_proc AS validator JOIN pg_depend AS dependency
		  ON dependency.refclassid = 'pg_proc'::regclass AND dependency.refobjid = validator.oid
		JOIN pg_proc AS writer ON dependency.classid = 'pg_proc'::regclass AND dependency.objid = writer.oid
		WHERE dependency.deptype = 'n' AND validator.oid = $1::text::regprocedure AND writer.oid = $2::text::regprocedure`,
		`"Restored Corpus".`+runtimeAuditValidator, `"Restored Corpus".`+runtimeAuditFunction).Scan(&validatorDDL, &writerDDL))

	targetURL := pgtest.Sibling(t, databaseURL)
	target := migrationTestClient(t, targetURL)
	applyMigrationPrefix(t, target, 34)
	grantExec(t, target, `ALTER SCHEMA public RENAME TO "Restored Corpus"`)
	grantExec(t, target, validatorDDL)
	grantExec(t, target, writerDDL)
	restored := grantRoleClient(t, targetURL, `SET search_path TO "Restored Corpus"`)
	require.NoError(t, restored.RecordSecurityAudit(ctx, domain.SecurityAuditEvent{Action: "query", Outcome: "allow"}))
	_, err := restored.Pool.Exec(ctx, runtimeAuditCall, nil, nil, nil, nil, "query", "invalid", "synthetic", nil, nil)
	requireRuntimeAuditRejection(t, err, "outcome")
	assertGrantCount(t, restored, "security_audit", 1)
}

func assertRuntimeAuditShadowEmpty(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	var tempCount int
	require.NoError(t, conn.QueryRow(t.Context(), `SELECT count(*) FROM pg_temp.grant_catalog_security_audit`).Scan(&tempCount))
	require.Zero(t, tempCount)
}

func TestRuntimeSecurityAuditWriterPreservesTransactions(t *testing.T) {
	ctx := t.Context()
	owner, serving, _ := restrictedDatabase(t)
	before := readRuntimeAuditFunctionIdentity(t, owner)
	const functionRowVersion = `SELECT xmin::text FROM pg_proc WHERE oid = $1::text::regprocedure`
	var originalVersion, repeatedVersion string
	require.NoError(t, owner.Pool.QueryRow(ctx, functionRowVersion, runtimeAuditFunction).Scan(&originalVersion))
	require.NoError(t, owner.RunMigrations(ctx))
	requireRecordedMigrations(t, owner)
	require.Equal(t, before, readRuntimeAuditFunctionIdentity(t, owner))
	require.NoError(t, owner.Pool.QueryRow(ctx, functionRowVersion, runtimeAuditFunction).Scan(&repeatedVersion))
	require.Equal(t, originalVersion, repeatedVersion, "repeated migrations must not replace the writer again")

	for _, scenario := range []struct {
		name   string
		reject bool
	}{
		{"caller rollback", false},
		{"rejected call", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			txCtx := t.Context()
			tx, err := serving.Pool.Begin(txCtx)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback(context.WithoutCancel(txCtx)) }()
			_, err = tx.Exec(txCtx, runtimeAuditCall, "rolled back", nil, nil, nil, "query", "allow", "synthetic", nil, nil)
			require.NoError(t, err)
			if scenario.reject {
				_, err = tx.Exec(txCtx, runtimeAuditCall, nil, nil, nil, nil, "grant_import", "allow", "synthetic", nil, nil)
				requireRuntimeAuditRejection(t, err, "action")
				require.ErrorIs(t, tx.Commit(txCtx), pgx.ErrTxCommitRollback)
			} else {
				require.NoError(t, tx.Rollback(txCtx))
			}
			assertGrantCount(t, owner, "security_audit", 0)
		})
	}

	tx, err := serving.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, runtimeAuditCall, "committed", nil, nil, nil, "query", "allow", "synthetic", nil, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	assertGrantCount(t, owner, "security_audit", 1)
}

func TestPublishedServingUpgradeRestoresServiceMode(t *testing.T) {
	owner, serving, roles := restrictedDatabase(t)
	ctx := t.Context()
	role := pgx.Identifier{roles.name("quarry_service")}.Sanitize()
	grantExec(t, owner, `REVOKE EXECUTE ON FUNCTION grant_catalog_corpus_authz_mode() FROM `+role)
	serving.SetAuthzMode("service")
	require.Error(t, serving.WritersOpen(ctx), "a pre-029 restricted login must fail closed")

	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "Serving role upgrade for migration 029"))
	require.NoError(t, serving.WritersOpen(ctx))
	mode, err := serving.CorpusAuthzMode(ctx)
	require.NoError(t, err)
	require.Equal(t, "service", mode)
	result, err := serving.AcceptDocument(ctx, jobsRequest("restricted service upgrade"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
}

func TestGrantsActivationRequiresCheckpointedDeletion(t *testing.T) {
	databaseURL := startTestDatabase(t)
	c := migrationTestClient(t, databaseURL)
	ctx := t.Context()
	require.NoError(t, c.RunMigrations(ctx))

	mode, err := c.CorpusAuthzMode(ctx)
	require.NoError(t, err)
	require.Equal(t, "service", mode)

	// New service-only installation works with service mode configured.
	c.SetAuthzMode("service")
	require.NoError(t, c.WritersOpen(ctx))
	serviceReq := jobsRequest("service mode document")
	res, err := c.AcceptDocument(ctx, serviceReq)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)

	assertServiceUnchanged := func() {
		t.Helper()
		var currentMode string
		var revision, auditCount int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT authz_mode, revision,
			(SELECT count(*) FROM grant_catalog_security_audit WHERE action='corpus_cutover')
			FROM grant_catalog_corpus_mode CROSS JOIN grant_catalog_policy_revision`).Scan(&currentMode, &revision, &auditCount))
		require.Equal(t, "service", currentMode)
		require.Zero(t, revision)
		require.Zero(t, auditCount)
	}
	assertActivationRefused := func() {
		t.Helper()
		for _, preview := range []bool{true, false} {
			_, activationErr := c.ActivateGrants(ctx, preview)
			require.EqualError(t, activationErr, "grants activation: checkpointed document deletion must be activated first")
			assertServiceUnchanged()
		}
	}
	assertActivationRefused()
	grantExec(t, c, `DELETE FROM document_deletion_activation WHERE singleton`)
	assertActivationRefused()
	grantExec(t, c, `INSERT INTO document_deletion_activation DEFAULT VALUES`)

	// Preview and activation see only a committed deletion marker.
	tx, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp() WHERE singleton`)
	require.NoError(t, err)
	assertActivationRefused()
	require.NoError(t, tx.Rollback(ctx))
	assertActivationRefused()
	// The real cutover waits for the corpus row. It must read the deletion
	// marker after acquiring that lock, then roll back if the audit fails.
	grantExec(t, c, `CREATE FUNCTION refuse_security_audit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'synthetic security audit interruption'; END; $$;
		CREATE TRIGGER refuse_security_audit BEFORE INSERT ON grant_catalog_security_audit
		FOR EACH ROW EXECUTE FUNCTION refuse_security_audit()`)
	cutoverLock, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = cutoverLock.Rollback(context.WithoutCancel(ctx)) }()
	_, err = cutoverLock.Exec(ctx, `SELECT 1 FROM grant_catalog_corpus_mode WHERE singleton FOR UPDATE`)
	require.NoError(t, err)
	cutoverErr := make(chan error, 1)
	go func() {
		_, activationErr := c.ActivateGrants(ctx, false)
		cutoverErr <- activationErr
	}()
	waitForLockWaiter(t, c, "SELECT resulting_revision, already_active")
	_, err = c.ActivateDeletion(ctx)
	require.NoError(t, err)
	require.NoError(t, cutoverLock.Commit(ctx))
	require.EqualError(t, <-cutoverErr, "grants activation failed (SQLSTATE P0001)")
	assertServiceUnchanged()
	grantExec(t, c, `DROP TRIGGER refuse_security_audit ON grant_catalog_security_audit; DROP FUNCTION refuse_security_audit()`)

	// Dry-run cutover previews without changing corpus mode or policy revision.
	dry, err := c.ActivateGrants(ctx, true)
	require.NoError(t, err)
	require.True(t, dry.DryRun)
	require.False(t, dry.AlreadyActive)
	require.EqualValues(t, 0, dry.Revision)
	assertServiceUnchanged()

	// Cutover succeeds atomically and advances revision + writes audit.
	activated, err := c.ActivateGrants(ctx, false)
	require.NoError(t, err)
	require.False(t, activated.DryRun)
	require.False(t, activated.AlreadyActive)
	require.EqualValues(t, 1, activated.Revision)
	mode, err = c.CorpusAuthzMode(ctx)
	require.NoError(t, err)
	require.Equal(t, "grants", mode)

	preview, err := c.ActivateGrants(ctx, true)
	require.NoError(t, err)
	require.True(t, preview.AlreadyActive)
	require.EqualValues(t, 1, preview.Revision)

	// Retrying cutover after an uncertain response is idempotent.
	replayed, err := c.ActivateGrants(ctx, false)
	require.NoError(t, err)
	require.True(t, replayed.AlreadyActive)
	require.EqualValues(t, 1, replayed.Revision)

	// Service-mode replica is now fenced at the gate, admission, and job claim.
	require.ErrorIs(t, c.WritersOpen(ctx), domain.ErrAuthzModeMismatch)
	_, err = c.AcceptDocument(ctx, jobsRequest("blocked service upload"))
	require.ErrorIs(t, err, domain.ErrAuthzModeMismatch)
	_, err = c.ClaimJob(ctx, "old-service-worker", time.Minute, 5)
	require.ErrorIs(t, err, domain.ErrAuthzModeMismatch)

	var auditAction, auditOutcome, auditReason string
	var auditCount int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM grant_catalog_security_audit WHERE action='corpus_cutover'`).Scan(&auditCount))
	require.Equal(t, 1, auditCount)
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT action, outcome, reason FROM grant_catalog_security_audit WHERE action='corpus_cutover'`).
		Scan(&auditAction, &auditOutcome, &auditReason))
	require.Equal(t, "corpus_cutover", auditAction)
	require.Equal(t, "allow", auditOutcome)
	require.Equal(t, "grants_activated", auditReason)
}

func TestUpgradeWithUnownedDocumentsAndExistingJobsQuarantinesLegacy(t *testing.T) {
	databaseURL := startTestDatabase(t)
	c := migrationTestClient(t, databaseURL)
	ctx := t.Context()
	applyMigrationPrefix(t, c, 26)

	// Seed an unowned completed document and an existing unowned pending job before migration 029.
	completedReq := jobsRequest("unowned completed legacy")
	require.Equal(t, domain.AcceptAccepted, func() domain.AcceptOutcome {
		res, err := c.AcceptDocument(ctx, completedReq)
		require.NoError(t, err)
		return res.Outcome
	}())
	job, err := c.ClaimJob(ctx, "pre-cutover-worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	ok, err := c.CompleteJob(ctx, *job, []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), Text: "unowned completed legacy", TokenCount: 3}}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)

	pendingReq := jobsRequest("unowned pending legacy")
	res, err := c.AcceptDocument(ctx, pendingReq)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)
	// Clear the eager fingerprint row to mirror a pre-dedup pending job that CompleteJob
	// would backfill in service mode, ensuring grants-mode completion skips fingerprinting.
	grantExec(t, c, `DELETE FROM document_fingerprints; UPDATE documents SET fingerprint=NULL WHERE status='processing'`)

	// Apply migration 029; corpus defaults to service mode until operator cutover.
	require.NoError(t, c.RunMigrations(ctx))
	c.SetAuthzMode("service")
	require.NoError(t, c.WritersOpen(ctx))

	// Provision tenant/principal and activate grants mode.
	f := provisionOwnedFixture(t, c)
	_, err = c.ActivateDeletion(ctx)
	require.NoError(t, err)
	_, err = c.ActivateGrants(ctx, false)
	require.NoError(t, err)
	c.SetAuthzMode("grants")
	require.NoError(t, c.WritersOpen(ctx))

	// A grants-mode worker drains the existing unowned job without registering
	// any shared fingerprint or exposing the unowned document.
	drainedJob, err := c.ClaimJob(ctx, "grants-worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, drainedJob)
	require.Equal(t, pendingReq.Document.ID, drainedJob.Document.ID)
	ok, err = c.CompleteJob(ctx, *drainedJob, []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), Text: "unowned pending legacy", TokenCount: 3}}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)

	var fpCount int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM document_fingerprints WHERE document_id=$1`, string(pendingReq.Document.ID)).Scan(&fpCount))
	require.Zero(t, fpCount, "grants mode must not register shared fingerprints for pre-cutover jobs")

	// Unowned documents remain invisible on scoped listing, status, file, delete, and retrieval.
	page, err := c.ListScopedDocumentsPage(ctx, f.owner, "", 100, "", false)
	require.NoError(t, err)
	require.Empty(t, page.Documents)
	for _, unownedID := range []domain.DocumentID{completedReq.Document.ID, pendingReq.Document.ID} {
		sum, getErr := c.GetScopedDocumentSummary(ctx, f.owner, unownedID, false)
		require.NoError(t, getErr)
		require.Nil(t, sum.Document)
		fileRes, fileErr := c.WithScopedDocumentFile(ctx, f.owner, unownedID, func(context.Context, string) error { return nil })
		require.NoError(t, fileErr)
		require.False(t, fileRes.Found)
		_, _, delErr := c.BeginScopedDeletion(ctx, f.owner, time.Now().Add(time.Minute), unownedID, domain.DeleteOperations{})
		require.ErrorIs(t, delErr, domain.ErrDocumentNotFound)
	}
	reader := GrantReader{Pool: c.Pool}
	scope, err := reader.DeriveReadScope(ctx, f.owner, domain.ReadScopeRequest{Membership: 100})
	require.NoError(t, err)
	require.True(t, scope.Empty)

	// Grants mode rejects unowned AcceptDocument calls.
	_, err = c.AcceptDocument(ctx, jobsRequest("unowned in grants"))
	require.Error(t, err)
}

func TestCorpusModeAndSecurityAuditPrivilegesAndBlocking(t *testing.T) {
	databaseURL := startTestDatabase(t)
	admin := migrationTestClient(t, databaseURL)
	applyMigrationPrefix(t, admin, 10)
	roles := newGrantRoles(t, databaseURL, servingRoles...)
	grantExec(t, admin, roles.sql(`CREATE ROLE {quarry_service}; CREATE ROLE {quarry_operator}; CREATE ROLE {quarry_auditor};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {quarry_service}, {quarry_operator};
		ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {quarry_service}, {quarry_operator}`))
	require.NoError(t, admin.RunMigrations(t.Context()))

	for _, fn := range []string{
		"grant_catalog_corpus_authz_mode()",
		"grant_catalog_record_security_audit(text,uuid,uuid,bigint,text,text,text,uuid,uuid)",
		"grant_catalog_activate_grants(boolean)",
		"grant_catalog_activate_grants_impl(boolean)",
	} {
		var inherited bool
		require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT has_function_privilege('{quarry_service}',$1,'execute')
			OR has_function_privilege('{quarry_operator}',$1,'execute') OR has_function_privilege('public',$1,'execute')`), fn).Scan(&inherited))
		require.False(t, inherited, fn+" must not be reachable through default privileges")
	}
	for _, tbl := range []string{"grant_catalog_corpus_mode", "grant_catalog_security_audit"} {
		var inherited bool
		require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT has_table_privilege('{quarry_service}',$1,'select')
			OR has_table_privilege('{quarry_operator}',$1,'select') OR has_table_privilege('public',$1,'select')`), tbl).Scan(&inherited))
		require.False(t, inherited, tbl+" must not be readable through default privileges")
	}

	grantExec(t, admin, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
	f := provisionOwnedFixture(t, admin)
	service := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
	operator := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_operator")}.Sanitize())
	auditor := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_auditor")}.Sanitize())

	// Serving role cannot activate grants or read/delete audit rows.
	_, err := service.ActivateGrants(t.Context(), false)
	require.Error(t, err)
	_, err = service.Pool.Exec(t.Context(), `SELECT * FROM grant_catalog_security_audit`)
	require.Error(t, err)
	_, err = service.Pool.Exec(t.Context(), `DELETE FROM grant_catalog_security_audit`)
	require.Error(t, err)
	_, err = operator.Pool.Exec(t.Context(), `SELECT * FROM grant_catalog_security_audit`)
	require.Error(t, err)
	_, err = auditor.ActivateGrants(t.Context(), false)
	require.Error(t, err)

	// Only the schema owner can activate deletion, even before grants cutover.
	_, err = operator.ActivateDeletion(t.Context())
	require.Error(t, err)
	_, err = admin.ActivateDeletion(t.Context())
	require.NoError(t, err)

	// Operator activates grants, service ingests, signs, and deletes with transactional audit.
	_, err = operator.ActivateGrants(t.Context(), false)
	require.NoError(t, err)
	service.SetAuthzMode("grants")

	req := ownedRequest(f.owner, "", "audited owned document")
	req.SupportsScopedVectors = true
	key := "documents/document/" + string(req.Document.ID) + "/orig.pdf"
	req.Document.S3Key = &key
	reqCtx := logctx.WithRequestID(t.Context(), "req-audit-1")
	res, err := service.AcceptDocument(reqCtx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)
	var scopedStored bool
	require.NoError(t, admin.Pool.QueryRow(t.Context(), `SELECT scoped_vectors FROM documents WHERE id=$1`, string(req.Document.ID)).Scan(&scopedStored))
	require.True(t, scopedStored, "grants mode must mark hosted owned uploads with scoped_vectors even when quarry_service runs")

	// Blocking security audit insert prevents file signing and deletion intent commit.
	grantExec(t, admin, `CREATE FUNCTION refuse_runtime_audit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'audit sink blocked'; END; $$;
		CREATE TRIGGER refuse_runtime_audit BEFORE INSERT ON grant_catalog_security_audit
		FOR EACH ROW EXECUTE FUNCTION refuse_runtime_audit()`)
	signed := false
	_, err = service.WithScopedDocumentFile(reqCtx, f.owner, req.Document.ID, func(context.Context, string) error {
		signed = true
		return nil
	})
	require.Error(t, err)
	require.True(t, signed) // Returned error before commit, so handler refuses to deliver signed URL.
	ops := newDeletionRecorder().ops(req.Owner.Incarnation)
	ops.SourceBucket, ops.VectorIndex = "originals", "vectors"
	_, _, err = service.BeginScopedDeletion(reqCtx, f.owner, time.Now().Add(time.Minute), req.Document.ID, ops)
	requireSQLState(t, err, "P0001")
	var deletingCount int
	require.NoError(t, admin.Pool.QueryRow(t.Context(), `SELECT count(*) FROM document_deletions WHERE document_id=$1`, string(req.Document.ID)).Scan(&deletingCount))
	require.Zero(t, deletingCount, "failed audit must roll back deletion intent")

	grantExec(t, admin, `DROP TRIGGER refuse_runtime_audit ON grant_catalog_security_audit; DROP FUNCTION refuse_runtime_audit()`)

	canceled, cancel := context.WithCancel(reqCtx)
	cancel()
	require.ErrorIs(t, service.RecordSecurityAudit(canceled, domain.SecurityAuditEvent{
		RequestID: "canceled-audit", Action: "query", Outcome: "allow", Reason: "synthetic",
	}), context.Canceled)
	var canceledRows int
	require.NoError(t, auditor.Pool.QueryRow(t.Context(), `SELECT count(*) FROM grant_catalog_security_audit
		WHERE request_id='canceled-audit'`).Scan(&canceledRows))
	require.Zero(t, canceledRows)

	// Auditor reads the recorded cutover, import, and ingest audit events.
	var total int
	require.NoError(t, auditor.Pool.QueryRow(t.Context(), `SELECT count(*) FROM grant_catalog_security_audit`).Scan(&total))
	require.GreaterOrEqual(t, total, 3)
}
