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
	"github.com/stretchr/testify/require"
)

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
		CREATE SCHEMA regionalledger`))
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
			require.NoError(t, admin.Pool.QueryRow(ctx, roles.sql(`SELECT
				has_function_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_import_batch(text,boolean)',$1::text),'EXECUTE'),
				has_function_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_parse_import(jsonb)',$1::text),'EXECUTE'),
				has_table_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_policy_audit',$1::text),'SELECT'),
				has_table_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_metrics',$1::text),'SELECT'),
				has_function_privilege('{quoted_catalog_reader}', format('%I.grant_catalog_parse_import(text)',$1::text),'EXECUTE')`), fixture.schema).
				Scan(&importAccess, &parserAccess, &auditAccess, &metricsAccess, &overloadAccess))
			require.False(t, importAccess || parserAccess || auditAccess)
			require.True(t, metricsAccess && overloadAccess)

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
}

func testGrantCatalogRename(t *testing.T, databaseURL string) {
	ctx := t.Context()
	roles := newGrantRoles(t, databaseURL, "rename_catalog_owner", "replacement_catalog_owner")
	admin := migrationTestClient(t, databaseURL)
	grantExec(t, admin, roles.sql(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
		CREATE ROLE {rename_catalog_owner}; CREATE ROLE {replacement_catalog_owner};
		CREATE SCHEMA rename_corpus AUTHORIZATION {rename_catalog_owner}`))
	before := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {rename_catalog_owner}; SET search_path TO rename_corpus`))
	require.NoError(t, before.RunMigrations(ctx))
	f := newGrantFixture()
	insertGrantSource(t, before, f.document)
	insertGrantSource(t, before, f.otherDocument)
	initial := f.initialBatch(t, before, false)
	_, err := importTestBatch(ctx, before, initial, false)
	require.NoError(t, err)
	beforeSnapshot := grantSource(t, before, f.document)

	grantExec(t, admin, roles.sql(`ALTER SCHEMA rename_corpus RENAME TO "Renamed Ledger";
		CREATE SCHEMA rename_corpus AUTHORIZATION {replacement_catalog_owner}`))
	renamed := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {rename_catalog_owner}; SET search_path TO "Renamed Ledger", rename_corpus`))
	replaced := grantRoleClient(t, databaseURL, roles.sql(`SET SESSION AUTHORIZATION {replacement_catalog_owner}; SET search_path TO rename_corpus`))
	require.NoError(t, renamed.RunMigrations(ctx))
	require.NoError(t, replaced.RunMigrations(ctx))
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
	c := migrationTestClient(t, databaseURL)
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
		readErr := c.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))`, writerPID).Scan(&waiting)
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
