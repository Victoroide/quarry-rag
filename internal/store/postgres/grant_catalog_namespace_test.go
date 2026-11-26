package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestGrantSourceViewReplacementKeepsRoleAndNamespaceIsolation(t *testing.T) {
	ctx := t.Context()
	dbURL := startTestDatabase(t)
	admin := migrationTestClient(t, dbURL)
	r := newGrantRoles(t, dbURL, "source_owner", "source_sibling", "source_operator", "source_serving", "source_inspector")
	grantExec(t, admin, r.sql(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
		CREATE ROLE {source_owner}; CREATE ROLE {source_sibling}; CREATE ROLE {source_operator};
		CREATE ROLE {source_serving}; CREATE ROLE {source_inspector};
		CREATE SCHEMA grant_ledger AUTHORIZATION {source_owner};
		CREATE SCHEMA sibling_ledger AUTHORIZATION {source_sibling};
		GRANT USAGE ON SCHEMA grant_ledger TO {source_operator}, {source_serving}, {source_inspector};
		GRANT USAGE ON SCHEMA sibling_ledger TO {source_operator}`))
	owner := grantRoleClient(t, dbURL, r.sql(`SET SESSION AUTHORIZATION {source_owner}; SET search_path TO grant_ledger`))
	applyMigrationsExcept(t, owner, grantSourceVersionMigration)
	grantExec(t, owner, grantGoldenInsert, grantGoldenID, nil)
	grantExec(t, owner, r.sql(`GRANT SELECT ON grant_catalog_source_documents TO {source_inspector};
		GRANT SELECT ON grant_catalog_document_access TO {source_serving};
		GRANT EXECUTE ON FUNCTION grant_catalog_source_snapshot(uuid), grant_catalog_import_batch(text,boolean) TO {source_operator}`))
	oldView := grantSourceViewState(t, owner)
	oldVersion := grantSource(t, owner, grantGoldenID)

	sibling := grantRoleClient(t, dbURL, r.sql(`SET SESSION AUTHORIZATION {source_sibling}; SET search_path TO sibling_ledger`))
	require.NoError(t, sibling.RunMigrations(ctx))
	grantExec(t, sibling, `INSERT INTO documents (id,content) VALUES ($1,'sibling corpus')`, grantGoldenID)
	grantExec(t, sibling, `CREATE FUNCTION sha256(bytea) RETURNS bytea LANGUAGE sql
		RETURN decode(repeat('00', 32), 'hex')`)
	siblingVersion := grantSource(t, sibling, grantGoldenID)
	require.NotEqual(t, oldVersion, siblingVersion)

	cancelGrantSourceViewReplacement(t, owner)
	require.Equal(t, oldView, grantSourceViewState(t, owner))
	require.Equal(t, oldVersion, grantSource(t, owner, grantGoldenID))
	var migrationCount int
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version=$1`,
		grantSourceVersionMigration).Scan(&migrationCount))
	require.Zero(t, migrationCount)

	grantExec(t, admin, `ALTER SCHEMA grant_ledger RENAME TO "Grant Ledger";
		CREATE SCHEMA grant_ledger;
		CREATE TABLE grant_ledger.documents (id uuid);
		CREATE VIEW grant_ledger.grant_catalog_source_documents AS SELECT 'decoy'::text AS source_version`)
	renamed := grantRoleClient(t, dbURL, r.sql(`SET SESSION AUTHORIZATION {source_owner}; SET search_path TO "Grant Ledger", sibling_ledger, grant_ledger`))
	operator := grantRoleClient(t, dbURL, r.sql(`SET SESSION AUTHORIZATION {source_operator};
		SET search_path TO sibling_ledger, pg_catalog, "Grant Ledger"`))
	serving := grantRoleClient(t, dbURL, r.sql(`SET SESSION AUTHORIZATION {source_serving}; SET search_path TO "Grant Ledger"`))
	batch := testGrantBatch(0)
	batch["tenants"] = []grantEntry{{"tenant_id": uuid.NewString()}}
	payload, err := json.Marshal(batch)
	require.NoError(t, err)
	assertGrantSourceRoleBoundary(t, operator, serving, oldVersion, string(payload))

	require.NoError(t, renamed.RunMigrations(ctx))
	requireRecordedMigrations(t, renamed)
	assertGrantSourceRoleBoundary(t, operator, serving, grantGoldenVersion, string(payload))
	require.Equal(t, oldView, grantSourceViewState(t, renamed))
	require.NoError(t, renamed.RunMigrations(ctx))
	requireRecordedMigrations(t, renamed)
	require.Equal(t, oldView, grantSourceViewState(t, renamed))
	require.Equal(t, grantGoldenVersion, grantSource(t, renamed, grantGoldenID))
	require.Equal(t, siblingVersion, grantSource(t, sibling, grantGoldenID))
	assertGrantRevision(t, sibling, 0)
}

type grantSourceViewIdentity struct {
	oid        uint32
	owner, acl string
}

func grantSourceViewState(t *testing.T, owner *Client) grantSourceViewIdentity {
	t.Helper()
	var view grantSourceViewIdentity
	var publicAccess bool
	require.NoError(t, owner.Pool.QueryRow(t.Context(), `SELECT oid, relowner::regrole::text, relacl::text,
		has_table_privilege('public', oid, 'SELECT') FROM pg_class
		WHERE oid='grant_catalog_source_documents'::regclass`).Scan(&view.oid, &view.owner, &view.acl, &publicAccess))
	require.False(t, publicAccess)
	return view
}

func assertGrantSourceRoleBoundary(t *testing.T, operator, serving *Client, expected, payload string) {
	t.Helper()
	ctx := t.Context()
	for _, client := range []*Client{serving, operator} {
		_, err := client.Pool.Exec(ctx, `SELECT * FROM "Grant Ledger".grant_catalog_source_documents`)
		var denied *pgconn.PgError
		require.ErrorAs(t, err, &denied)
		require.Equal(t, "42501", denied.Code)
	}
	_, err := serving.Pool.Exec(ctx, `SELECT * FROM "Grant Ledger".grant_catalog_source_snapshot($1)`, grantGoldenID)
	var denied *pgconn.PgError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, "42501", denied.Code)
	var version string
	require.NoError(t, operator.Pool.QueryRow(ctx, `SELECT source_version FROM "Grant Ledger".grant_catalog_source_snapshot($1)`,
		grantGoldenID).Scan(&version))
	require.Equal(t, expected, version)
	var result GrantImportResult
	require.NoError(t, operator.Pool.QueryRow(ctx, `SELECT * FROM "Grant Ledger".grant_catalog_import_batch($1,true)`,
		payload).Scan(&result.Revision, &result.Replayed))
	require.Zero(t, result.Revision)
	require.False(t, result.Replayed)
}

func cancelGrantSourceViewReplacement(t *testing.T, owner *Client) {
	t.Helper()
	ctx := t.Context()
	writer, err := owner.Pool.Acquire(ctx)
	require.NoError(t, err)
	defer writer.Release()
	lock, err := writer.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(context.WithoutCancel(ctx)) }()
	_, err = lock.Exec(ctx, `LOCK TABLE grant_catalog_source_documents IN ACCESS SHARE MODE`)
	require.NoError(t, err)
	migrationCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- owner.RunMigrations(migrationCtx) }()
	waitForGrantSourceBlocker(t, owner, writer.Conn().PgConn().PID())
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	require.NoError(t, lock.Rollback(ctx))
}

func testGrantCatalogQuotedACLs(t *testing.T, databaseURL string) {
	roles := newGrantRoles(t, databaseURL, "quoted_catalog_reader", "quoted_catalog_inherited", "sales_corpus_owner", "regional_corpus_owner")
	admin := migrationTestClient(t, databaseURL)
	grantExec(t, admin, roles.sql(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
		CREATE ROLE {quoted_catalog_reader};
		CREATE ROLE {quoted_catalog_inherited};
		GRANT {quoted_catalog_inherited} TO {quoted_catalog_reader};
		CREATE ROLE {sales_corpus_owner};
		CREATE ROLE {regional_corpus_owner};
		CREATE SCHEMA "SalesLedger" AUTHORIZATION {sales_corpus_owner};
		CREATE SCHEMA "RegionalLedger" AUTHORIZATION {regional_corpus_owner};
		CREATE SCHEMA regionalledger;
		CREATE FUNCTION regionalledger.guard_active_cleanup_state() RETURNS boolean LANGUAGE sql RETURN true;
		GRANT USAGE ON SCHEMA regionalledger TO {quoted_catalog_reader};
		GRANT EXECUTE ON FUNCTION regionalledger.guard_active_cleanup_state() TO {quoted_catalog_reader}`))
	for _, fixture := range []struct{ schema, owner string }{
		{"SalesLedger", roles.name("sales_corpus_owner")},
		{"RegionalLedger", roles.name("regional_corpus_owner")},
	} {
		t.Run(fixture.schema, func(t *testing.T) {
			ctx := t.Context()
			schema := pgx.Identifier{fixture.schema}.Sanitize()
			role := pgx.Identifier{fixture.owner}.Sanitize()
			corpus := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+role+"; SET search_path TO "+schema)
			applyMigrationPrefix(t, corpus, 10)
			id := uuid.NewString()
			insertGrantSource(t, corpus, id)
			grantExec(t, corpus, roles.sql(`CREATE TABLE grant_catalog_metrics (value INTEGER);
				CREATE FUNCTION grant_catalog_parse_import(text) RETURNS text LANGUAGE sql RETURN $1;
				GRANT SELECT ON grant_catalog_metrics TO {quoted_catalog_reader};
				GRANT EXECUTE ON FUNCTION grant_catalog_parse_import(text) TO {quoted_catalog_reader};
				ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {quoted_catalog_reader};
				ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {quoted_catalog_reader}`))
			grantExec(t, admin, roles.sql("GRANT USAGE ON SCHEMA "+schema+" TO {quoted_catalog_reader}"))
			require.NoError(t, corpus.RunMigrations(ctx))
			requireRecordedMigrations(t, corpus)
			require.Len(t, grantSource(t, corpus, id), 64)
			var importAccess, parserAccess, auditAccess, metricsAccess, overloadAccess bool
			var cleanupStateAccess, cleanupBindingAccess bool
			require.NoError(t, admin.Pool.QueryRow(ctx, roles.sql(`SELECT
				has_function_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_import_batch(text,boolean)',$1::text),'EXECUTE'),
				has_function_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_parse_import(jsonb)',$1::text),'EXECUTE'),
				has_table_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_policy_audit',$1::text),'SELECT'),
				has_table_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_metrics',$1::text),'SELECT'),
				has_function_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_parse_import(text)',$1::text),'EXECUTE'),
				has_function_privilege('{quoted_catalog_reader}', format('%I.guard_active_cleanup_state()',$1::text),'EXECUTE'),
				has_function_privilege('{quoted_catalog_reader}', format('%I.guard_active_cleanup_binding()',$1::text),'EXECUTE')`), fixture.schema).
				Scan(&importAccess, &parserAccess, &auditAccess, &metricsAccess, &overloadAccess, &cleanupStateAccess, &cleanupBindingAccess))
			require.False(t, importAccess || parserAccess || auditAccess)
			require.True(t, metricsAccess && overloadAccess)
			require.False(t, cleanupStateAccess || cleanupBindingAccess)

			grantExec(t, corpus, roles.sql(`GRANT EXECUTE ON FUNCTION grant_catalog_import_batch(text,boolean) TO {quoted_catalog_reader}`))
			reader := grantRoleClient(t, databaseURL, roles.sql("SET SESSION AUTHORIZATION {quoted_catalog_reader}; SET search_path TO "+schema))
			batch := testGrantBatch(0)
			batch["tenants"] = []grantEntry{{"tenant_id": uuid.NewString()}}
			for _, grantee := range []string{"PUBLIC", roles.name("quoted_catalog_reader"), roles.name("quoted_catalog_inherited")} {
				grantExec(t, admin, "GRANT CREATE ON SCHEMA "+schema+" TO "+grantee)
				var canCreate bool
				require.NoError(t, admin.Pool.QueryRow(ctx, roles.sql(`SELECT has_schema_privilege('{quoted_catalog_reader}',oid,'CREATE')
					FROM pg_namespace WHERE nspname=$1::text`), fixture.schema).Scan(&canCreate))
				require.True(t, canCreate)
				_, err := importTestBatch(ctx, reader, batch, true)
				require.ErrorContains(t, err, "catalog schema must restrict CREATE to its owner")
				grantExec(t, admin, "REVOKE CREATE ON SCHEMA "+schema+" FROM "+grantee)
			}
			var createAccess bool
			require.NoError(t, admin.Pool.QueryRow(ctx, roles.sql(`SELECT has_schema_privilege('{quoted_catalog_reader}',oid,'CREATE')
				FROM pg_namespace WHERE nspname=$1::text`), fixture.schema).Scan(&createAccess))
			require.False(t, createAccess)
			_, err := importTestBatch(ctx, reader, batch, true)
			require.NoError(t, err)
		})
	}
	var unrelatedAccess bool
	require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT has_function_privilege(
		'{quoted_catalog_reader}', 'regionalledger.guard_active_cleanup_state()', 'EXECUTE')`)).Scan(&unrelatedAccess))
	require.True(t, unrelatedAccess)
}

func testGrantCatalogRename(t *testing.T, databaseURL string) {
	ctx := t.Context()
	roles := newGrantRoles(t, databaseURL, "rename_catalog_owner", "replacement_catalog_owner", "quarry_service")
	admin := migrationTestClient(t, databaseURL)
	grantExec(t, admin, roles.sql(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
		CREATE ROLE {rename_catalog_owner}; CREATE ROLE {replacement_catalog_owner}; CREATE ROLE {quarry_service};
		CREATE SCHEMA rename_corpus AUTHORIZATION {rename_catalog_owner}`))
	before := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {rename_catalog_owner}; SET search_path TO rename_corpus`))
	applyMigrationPrefix(t, before, 34)
	adminMigrator := grantRoleClient(t, databaseURL, `SET search_path TO rename_corpus`)
	require.NoError(t, adminMigrator.RunMigrations(ctx))
	f := newGrantFixture()
	insertGrantSource(t, before, f.document)
	insertGrantSource(t, before, f.otherDocument)
	initial := f.initialBatch(t, before, false)
	_, err := importTestBatch(ctx, before, initial, false)
	require.NoError(t, err)
	beforeSnapshot := grantSource(t, before, f.document)
	grantExec(t, before, roles.sql(`GRANT USAGE ON SCHEMA rename_corpus TO {quarry_service};
		GRANT EXECUTE ON FUNCTION `+runtimeAuditFunction+` TO {quarry_service}`))

	grantExec(t, admin, `ALTER SCHEMA rename_corpus RENAME TO "Renamed Ledger"`)
	serving := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {quarry_service}; SET search_path TO "Renamed Ledger", rename_corpus`))
	require.NoError(t, serving.RecordSecurityAudit(ctx, domain.SecurityAuditEvent{
		RequestID: "after rename", Action: "query", Outcome: "allow",
	}))

	grantExec(t, admin, roles.sql(`CREATE SCHEMA rename_corpus AUTHORIZATION {replacement_catalog_owner}`))
	renamed := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {rename_catalog_owner}; SET search_path TO "Renamed Ledger", rename_corpus`))
	replaced := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {replacement_catalog_owner}; SET search_path TO rename_corpus`))
	require.NoError(t, renamed.RunMigrations(ctx))
	require.NoError(t, replaced.RunMigrations(ctx))
	grantExec(t, replaced, roles.sql(`GRANT USAGE ON SCHEMA rename_corpus TO {quarry_service};
		GRANT EXECUTE ON FUNCTION `+runtimeAuditFunction+` TO {quarry_service}`))
	reconnected := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {quarry_service}; SET search_path TO "Renamed Ledger", rename_corpus`))
	require.NoError(t, reconnected.RecordSecurityAudit(ctx, domain.SecurityAuditEvent{
		RequestID: "after reuse", Action: "query", Outcome: "allow",
	}))
	_, err = reconnected.Pool.Exec(ctx, runtimeAuditCall, nil, nil, nil, nil, "grant_import", "allow", "synthetic", nil, nil)
	requireRuntimeAuditRejection(t, err, "action")
	rows, err := renamed.Pool.Query(ctx, `SELECT request_id FROM grant_catalog_security_audit
		WHERE operator IS NULL AND action='query' AND outcome='allow' AND policy_revision=1 ORDER BY audit_id`)
	require.NoError(t, err)
	runtimeRequests, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Equal(t, []string{"after rename", "after reuse"}, runtimeRequests)
	assertGrantCount(t, renamed, "security_audit", 3)
	assertGrantCount(t, replaced, "security_audit", 0)

	grantExec(t, replaced, `INSERT INTO documents (id,content) VALUES ($1,'independent corpus')`, f.document)
	require.NotEqual(t, beforeSnapshot, grantSource(t, replaced, f.document))

	var snapshot string
	require.NoError(t, admin.Pool.QueryRow(ctx, `SELECT source_version FROM "Renamed Ledger".grant_catalog_source_snapshot($1)`, f.document).Scan(&snapshot))
	require.Equal(t, beforeSnapshot, snapshot)
	replay, err := importTestBatch(ctx, renamed, initial, true)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	batch := testGrantBatch(1)
	batch["tenants"] = []grantEntry{{"tenant_id": uuid.NewString()}}
	_, err = importTestBatch(ctx, renamed, batch, false)
	require.NoError(t, err)
	require.NoError(t, renamed.DeleteDocument(ctx, domain.DocumentID(f.document)))
	assertGrantRevision(t, renamed, 3)
	assertGrantCount(t, renamed, "document_grants", 0)
	assertGrantRevision(t, replaced, 0)
	assertGrantCount(t, replaced, "document_ownership", 0)
	require.NotEmpty(t, grantSource(t, replaced, f.document))
}

func testGrantCatalogTruncate(t *testing.T, databaseURL string) {
	c := migrationTestClient(t, databaseURL)
	ctx := t.Context()
	require.NoError(t, c.RunMigrations(ctx))
	grantExec(t, c, `TRUNCATE documents CASCADE`)
	f := newGrantFixture()
	insertGrantSource(t, c, f.document)
	insertGrantSource(t, c, f.otherDocument)
	batch := f.initialBatch(t, c, false)
	_, err := importTestBatch(ctx, c, batch, false)
	require.NoError(t, err)
	oldIncarnation := grantIncarnation(t, c, f.document)
	_, err = c.Pool.Exec(ctx, `TRUNCATE documents CASCADE`)
	require.ErrorContains(t, err, "delete assigned documents before truncating")
	assertGrantRevision(t, c, 1)
	assertGrantCount(t, c, "document_grants", 2)
	require.Equal(t, oldIncarnation, grantIncarnation(t, c, f.document))
	for _, id := range []string{f.document, f.otherDocument} {
		require.NoError(t, c.DeleteDocument(ctx, domain.DocumentID(id)))
	}
	grantExec(t, c, `TRUNCATE documents CASCADE`)
	assertGrantCount(t, c, "assigned_incarnations", 2)
	assertGrantCount(t, c, "policy_audit", 3)
	insertGrantSource(t, c, f.document)
	require.NotEqual(t, oldIncarnation, grantIncarnation(t, c, f.document))
	replacement := testGrantBatch(3)
	replacement["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.creator, f.document)}
	_, err = importTestBatch(ctx, c, replacement, false)
	require.NoError(t, err)
}

func testGrantCatalogPreviewSnapshot(t *testing.T, databaseURL string) {
	c := migrationTestClient(t, withFixtureOptions(t, databaseURL, "application_name=quarry-grant-preview-test"))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	require.NoError(t, c.RunMigrations(ctx))
	writer, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = writer.Rollback(context.WithoutCancel(ctx)) }()
	_, err = writer.Exec(ctx, `LOCK TABLE grant_catalog_tenants IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	var writerPID int
	require.NoError(t, writer.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&writerPID))
	tenant := uuid.NewString()
	batch := testGrantBatch(0)
	batch["collections"] = []grantEntry{{"tenant_id": tenant, "collection": "snapshot test"}}
	payload, err := json.Marshal(batch)
	require.NoError(t, err)
	direct := make(chan error, 1)
	store := make(chan error, 1)
	go func() {
		var revision int64
		var replayed bool
		direct <- c.Pool.QueryRow(ctx, `SELECT * FROM grant_catalog_import_batch($1,true)`, string(payload)).Scan(&revision, &replayed)
	}()
	go func() {
		_, importErr := c.ImportGrantBatch(ctx, bytes.NewReader(payload), true)
		store <- importErr
	}()
	require.Eventually(t, func() bool {
		var waiting int
		readErr := c.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
			AND application_name=current_setting('application_name') AND $1 = ANY(pg_blocking_pids(pid))`, writerPID).Scan(&waiting)
		return readErr == nil && waiting == 2
	}, 10*time.Second, 20*time.Millisecond)
	mutation := testGrantBatch(0)
	mutation["tenants"] = []grantEntry{{"tenant_id": tenant}}
	mutationPayload, err := json.Marshal(mutation)
	require.NoError(t, err)
	_, err = writer.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1,false)`, string(mutationPayload))
	require.NoError(t, err)
	require.NoError(t, writer.Commit(ctx))
	for i, result := range []chan error{direct, store} {
		select {
		case previewErr := <-result:
			require.ErrorContains(t, previewErr, "invalid or conflicting collections entry", fmt.Sprint(i))
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	assertGrantRevision(t, c, 1)
	assertGrantCount(t, c, "collections", 0)
	batch["expected_revision"] = 1
	freshPayload, err := json.Marshal(batch)
	require.NoError(t, err)
	var directResult GrantImportResult
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT * FROM grant_catalog_import_batch($1,true)`, string(freshPayload)).
		Scan(&directResult.Revision, &directResult.Replayed))
	directResult.DryRun = true
	storeResult, err := c.ImportGrantBatch(ctx, bytes.NewReader(freshPayload), true)
	require.NoError(t, err)
	require.Equal(t, directResult, storeResult)
	require.EqualValues(t, 1, storeResult.Revision)
}
