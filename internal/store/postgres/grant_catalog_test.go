package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type grantFixture struct {
	tenant, otherTenant, owner, reader, creator, disabled, stranger string
	document, otherDocument, legacy, reserved                       string
}

func newGrantFixture() grantFixture {
	return grantFixture{
		tenant: uuid.NewString(), otherTenant: uuid.NewString(), owner: uuid.NewString(),
		reader: uuid.NewString(), creator: uuid.NewString(), disabled: uuid.NewString(), stranger: uuid.NewString(),
		document: uuid.NewString(), otherDocument: uuid.NewString(), legacy: uuid.NewString(), reserved: uuid.NewString(),
	}
}

func TestGrantCatalog(t *testing.T) {
	clusterURL := startTestDatabase(t)
	newDatabase := func(t *testing.T) string {
		t.Helper()
		return pgtest.Sibling(t, clusterURL)
	}
	t.Run("fresh_and_populated", func(t *testing.T) {
		testGrantCatalogFreshAndUpgrade(t, newDatabase)
	})
	t.Run("conflicts_and_replay", func(t *testing.T) {
		testGrantCatalogImportConflictsAndReplay(t, newDatabase(t))
	})
	t.Run("uuid_reuse", func(t *testing.T) {
		testGrantCatalogUUIDReuse(t, newDatabase(t))
	})
	t.Run("role_boundary", func(t *testing.T) {
		testGrantCatalogRoleBoundary(t, newDatabase(t))
	})
	t.Run("schema_isolation", func(t *testing.T) {
		testGrantCatalogSchemaIsolation(t, newDatabase(t))
	})
	t.Run("quoted_catalog_acls", func(t *testing.T) {
		testGrantCatalogQuotedACLs(t, newDatabase(t))
	})
	t.Run("rename_and_reuse", func(t *testing.T) {
		testGrantCatalogRename(t, newDatabase(t))
	})
	t.Run("truncate_guard", func(t *testing.T) {
		testGrantCatalogTruncate(t, newDatabase(t))
	})
	t.Run("preview_snapshot", func(t *testing.T) {
		testGrantCatalogPreviewSnapshot(t, newDatabase(t))
	})
	t.Run("raw_input_and_labels", func(t *testing.T) {
		testGrantCatalogRawInputAndLabels(t, newDatabase(t))
	})
	t.Run("pending_refresh_and_locked_replay", func(t *testing.T) {
		testGrantCatalogPendingRefreshAndLockedReplay(t, newDatabase(t))
	})
}

func testGrantCatalogFreshAndUpgrade(t *testing.T, newDatabase func(*testing.T) string) {
	for _, upgrade := range []bool{false, true} {
		name := "fresh"
		if upgrade {
			name = "populated"
		}
		t.Run(name, func(t *testing.T) {
			c := migrationTestClient(t, newDatabase(t))
			ctx := t.Context()
			f := newGrantFixture()
			if upgrade {
				applyMigrationPrefix(t, c, 9)
				insertGrantSource(t, c, f.legacy)
			}
			require.NoError(t, c.RunMigrations(ctx))
			grantExec(t, c, `INSERT INTO documents(id,content,collection) VALUES ($1,'same text',''),($2,'same text','')`, f.document, f.otherDocument)
			assertGrantCount(t, c, "document_ownership", 0)
			assertGrantCount(t, c, "document_access", 0)
			if upgrade {
				var incarnation *string
				require.NoError(t, c.Pool.QueryRow(ctx, `SELECT incarnation::text FROM documents WHERE id=$1`, f.legacy).Scan(&incarnation))
				require.Nil(t, incarnation)
				doc, err := c.GetDocument(ctx, domain.DocumentID(f.legacy))
				require.NoError(t, err)
				require.Equal(t, "same text", doc.Content)
			}

			batch := f.initialBatch(t, c, upgrade)
			preview, err := importTestBatch(ctx, c, batch, true)
			require.NoError(t, err)
			require.EqualValues(t, 0, preview.Revision)
			require.True(t, preview.DryRun)
			for _, name := range []string{"tenants", "principals", "document_ownership", "policy_audit"} {
				assertGrantCount(t, c, name, 0)
			}
			applied, err := importTestBatch(ctx, c, batch, false)
			require.NoError(t, err)
			require.EqualValues(t, 1, applied.Revision)
			require.False(t, applied.Replayed)
			assertGrantCount(t, c, "active_principals", 5)
			assertGrantCount(t, c, "collection_creators", 2)
			assertGrantCount(t, c, "document_access", 0)
			require.NoError(t, c.UpdateDocumentStatus(ctx, domain.DocumentID(f.document), "failed"))
			refreshed := testGrantBatch(1)
			refreshed["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.owner, f.document)}
			_, err = importTestBatch(ctx, c, refreshed, false)
			require.NoError(t, err)
			var storedVersion string
			require.NoError(t, c.Pool.QueryRow(ctx, `SELECT source_version FROM grant_catalog_document_ownership WHERE document_id=$1`, f.document).Scan(&storedVersion))
			require.Equal(t, grantSource(t, c, f.document), storedVersion)
			assertGrantCount(t, c, "document_access", 0)
			// Simulate the later readiness step with the catalog owner's credentials.
			grantExec(t, c, `UPDATE grant_catalog_document_ownership SET ready=true`)
			assertDocumentAccess(t, c, f.document, f.owner, true, true, true)
			assertDocumentAccess(t, c, f.document, f.reader, true, false, false)
			assertDocumentAccess(t, c, f.document, f.creator, false, false, false)
			assertDocumentAccess(t, c, f.document, f.disabled, false, false, false)
			assertDocumentAccess(t, c, f.document, f.stranger, false, false, false)
			assertDocumentAccess(t, c, f.otherDocument, f.stranger, true, true, true)
			assertDocumentAccess(t, c, f.otherDocument, f.owner, false, false, false)
			_, err = c.Pool.Exec(ctx, `INSERT INTO grant_catalog_document_grants
				(tenant_id, document_id, incarnation, principal_id, can_read, can_download)
				VALUES ($1,$2,$3,$4,true,true)`, f.tenant, f.document, grantIncarnation(t, c, f.document), f.stranger)
			require.Error(t, err, "the foreign key must reject a cross-tenant principal")
			if upgrade {
				assertDocumentAccess(t, c, f.legacy, f.owner, true, true, true)
			}

			req := jobsRequest("live service write")
			res, err := c.AcceptDocument(ctx, req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, res.Outcome)
			job, err := c.ClaimJob(ctx, "legacy-worker", time.Minute, 3)
			require.NoError(t, err)
			require.NotNil(t, job)
			completed, err := c.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
			require.NoError(t, err)
			require.True(t, completed)
			assertDocumentAccess(t, c, string(res.DocumentID), f.owner, false, false, false)
			items, err := c.ListDocumentsPage(ctx, "", 100)
			require.NoError(t, err)
			wantCount := 3
			if upgrade {
				wantCount++
			}
			require.Len(t, items, wantCount)
		})
	}
}

func testGrantCatalogImportConflictsAndReplay(t *testing.T, databaseURL string) {
	c := migrationTestClient(t, databaseURL)
	ctx := t.Context()
	require.NoError(t, c.RunMigrations(ctx))
	f := newGrantFixture()
	insertGrantSource(t, c, f.document)
	insertGrantSource(t, c, f.otherDocument)
	first := f.initialBatch(t, c, false)
	_, err := importTestBatch(ctx, c, first, false)
	require.NoError(t, err)

	grantExec(t, c, `CREATE FUNCTION refuse_catalog_audit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'synthetic audit interruption'; END; $$;
		CREATE TRIGGER refuse_catalog_audit BEFORE INSERT ON grant_catalog_policy_audit
		FOR EACH ROW EXECUTE FUNCTION refuse_catalog_audit()`)
	interrupted := testGrantBatch(1)
	interrupted["tenants"] = []grantEntry{{"tenant_id": uuid.NewString()}}
	_, err = importTestBatch(ctx, c, interrupted, false)
	require.ErrorContains(t, err, "SQLSTATE P0001")
	assertGrantCount(t, c, "tenants", 2)
	assertGrantCount(t, c, "policy_audit", 1)
	assertGrantRevision(t, c, 1)
	grantExec(t, c, `DROP TRIGGER refuse_catalog_audit ON grant_catalog_policy_audit; DROP FUNCTION refuse_catalog_audit()`)

	for _, tc := range []struct {
		name, group string
		entry       grantEntry
	}{
		{"foreign create", "create_permissions", grantEntry{"tenant_id": f.tenant, "principal_id": f.stranger, "collection": "", "can_create": true}},
		{"foreign owner", "assignments", grantEntry{"tenant_id": f.tenant, "principal_id": f.stranger, "collection": "", "document_id": f.document, "source_version": grantSource(t, c, f.document)}},
		{"conflicting owner", "assignments", grantEntry{"tenant_id": f.tenant, "principal_id": f.reader, "collection": "", "document_id": f.document, "source_version": grantSource(t, c, f.document)}},
		{"wrong collection", "assignments", grantEntry{"tenant_id": f.tenant, "principal_id": f.owner, "collection": "wrong", "document_id": f.document, "source_version": grantSource(t, c, f.document)}},
		{"foreign grant", "grants", grantEntry{"tenant_id": f.otherTenant, "principal_id": f.stranger, "document_id": f.document, "incarnation": grantIncarnation(t, c, f.document), "can_read": true, "can_download": false}},
		{"download without read", "grants", grantEntry{"tenant_id": f.tenant, "principal_id": f.reader, "document_id": f.document, "incarnation": grantIncarnation(t, c, f.document), "can_read": false, "can_download": true}},
		{"foreign reservation", "reservations", grantEntry{"tenant_id": f.tenant, "principal_id": f.stranger, "collection": "", "document_id": uuid.NewString()}},
		{"occupied reservation", "reservations", grantEntry{"tenant_id": f.tenant, "principal_id": f.owner, "collection": "", "document_id": f.document}},
		{"conflicting reservation", "reservations", grantEntry{"tenant_id": f.tenant, "principal_id": f.creator, "collection": "", "document_id": f.reserved}},
		{"moved principal", "principals", grantPrincipal(f.otherTenant, f.owner, "Owner", true)},
		{"reassigned subject", "principals", grantPrincipal(f.tenant, uuid.NewString(), "Owner", true)},
		{"stale source", "assignments", grantEntry{"tenant_id": f.tenant, "principal_id": f.owner, "collection": "", "document_id": f.document, "source_version": strings.Repeat("0", 64)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newTenant := uuid.NewString()
			batch := testGrantBatch(1)
			batch["tenants"] = []grantEntry{{"tenant_id": newTenant}}
			batch[tc.group] = []grantEntry{tc.entry}
			for _, dryRun := range []bool{true, false} {
				_, importErr := importTestBatch(t.Context(), c, batch, dryRun)
				require.Error(t, importErr)
				assertGrantCount(t, c, "tenants", 2)
				assertGrantCount(t, c, "policy_audit", 1)
				assertGrantRevision(t, c, 1)
			}
		})
	}

	previousSource := grantSource(t, c, f.document)
	require.NoError(t, c.UpdateDocumentStatus(ctx, domain.DocumentID(f.document), "failed"))
	require.NotEqual(t, previousSource, grantSource(t, c, f.document))
	staleSource := testGrantBatch(1)
	staleSource["assignments"] = []grantEntry{{
		"tenant_id": f.tenant, "principal_id": f.owner, "collection": "", "document_id": f.document, "source_version": previousSource,
	}}
	_, err = importTestBatch(ctx, c, staleSource, false)
	require.ErrorContains(t, err, "invalid or conflicting assignments entry")
	assertGrantRevision(t, c, 1)

	two := testGrantBatch(1)
	two["principals"] = []grantEntry{grantPrincipal(f.tenant, f.owner, "Owner", false)}
	three := testGrantBatch(1)
	three["principals"] = []grantEntry{grantPrincipal(f.tenant, f.reader, "owner", false)}
	type attempt struct {
		batch map[string]any
		err   error
	}
	start, done := make(chan struct{}), make(chan attempt, 2)
	for _, b := range []map[string]any{two, three} {
		go func() {
			<-start
			_, importErr := importTestBatch(ctx, c, b, false)
			done <- attempt{batch: b, err: importErr}
		}()
	}
	close(start)
	a, b := <-done, <-done
	if a.err != nil {
		a, b = b, a
	}
	require.NoError(t, a.err)
	require.ErrorContains(t, b.err, "stale revision")
	replayed, err := importTestBatch(ctx, c, a.batch, false)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.EqualValues(t, 2, replayed.Revision)
	a.batch["expected_revision"] = 2
	_, err = importTestBatch(ctx, c, a.batch, false)
	require.ErrorContains(t, err, "batch ID conflict")
	assertGrantRevision(t, c, 2)
	assertGrantCount(t, c, "policy_audit", 2)

	revocation := testGrantBatch(2)
	incarnation := grantIncarnation(t, c, f.document)
	revocation["grants"] = []grantEntry{
		{"tenant_id": f.tenant, "principal_id": f.reader, "document_id": f.document, "incarnation": incarnation, "can_read": false, "can_download": false},
		{"tenant_id": f.tenant, "principal_id": f.creator, "document_id": f.document, "incarnation": incarnation, "can_read": true, "can_download": true},
	}
	_, err = importTestBatch(ctx, c, revocation, false)
	require.NoError(t, err)
	grantExec(t, c, `UPDATE grant_catalog_document_ownership SET ready=true`)
	assertDocumentAccess(t, c, f.document, f.reader, false, false, false)
	assertDocumentAccess(t, c, f.document, f.creator, true, true, false)
	assertGrantRevision(t, c, 3)
	grantExec(t, c, `UPDATE grant_catalog_policy_revision SET revision=9223372036854775807`)
	next := testGrantBatch(9223372036854775807)
	next["tenants"] = []grantEntry{{"tenant_id": uuid.NewString()}}
	_, err = importTestBatch(ctx, c, next, false)
	require.ErrorContains(t, err, "revision exhausted")
	assertGrantCount(t, c, "tenants", 2)
}

func testGrantCatalogUUIDReuse(t *testing.T, databaseURL string) {
	c := migrationTestClient(t, databaseURL)
	ctx := t.Context()
	require.NoError(t, c.RunMigrations(ctx))
	f := newGrantFixture()
	insertGrantSource(t, c, f.document)
	insertGrantSource(t, c, f.otherDocument)
	b := f.initialBatch(t, c, false)
	_, err := importTestBatch(ctx, c, b, false)
	require.NoError(t, err)
	before := grantSource(t, c, f.document)
	grantExec(t, c, `UPDATE grant_catalog_document_ownership SET ready=true WHERE document_id=$1`, f.document)
	assertDocumentAccess(t, c, f.document, f.reader, true, false, false)
	var incarnation, reservedIncarnation string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT incarnation::text FROM documents WHERE id=$1`, f.document).Scan(&incarnation))
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT incarnation::text FROM grant_catalog_document_reservations WHERE document_id=$1`, f.reserved).Scan(&reservedIncarnation))
	_, err = c.Pool.Exec(ctx, `UPDATE documents SET incarnation=gen_random_uuid() WHERE id=$1`, f.document)
	require.Error(t, err)
	require.NoError(t, c.DeleteDocument(ctx, domain.DocumentID(f.document)))
	assertGrantRevision(t, c, 2)
	insertGrantSource(t, c, f.document)
	insertGrantSource(t, c, f.reserved)
	require.NoError(t, c.DeleteDocument(ctx, domain.DocumentID(f.reserved)))
	insertGrantSource(t, c, f.reserved)
	require.NotEqual(t, before, grantSource(t, c, f.document))
	for _, id := range []string{f.document, f.reserved} {
		assertDocumentAccess(t, c, id, f.owner, false, false, false)
		assertDocumentAccess(t, c, id, f.reader, false, false, false)
	}
	var matches bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM documents WHERE id=$1 AND incarnation=$2)`, f.reserved, reservedIncarnation).Scan(&matches))
	require.False(t, matches)

	require.NoError(t, c.DeleteDocument(ctx, domain.DocumentID(f.document)))
	grantExec(t, c, `INSERT INTO documents(id,incarnation,content) VALUES($1,$2,'same text')`, f.document, incarnation)
	b = testGrantBatch(2)
	b["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.owner, f.document)}
	_, err = importTestBatch(ctx, c, b, false)
	require.Error(t, err, "a previously assigned incarnation cannot be imported again")
	assertGrantRevision(t, c, 2)

	// An import targets the captured incarnation even if the UUID has now been assigned again.
	require.NoError(t, c.DeleteDocument(ctx, domain.DocumentID(f.document)))
	insertGrantSource(t, c, f.document)
	reassign := testGrantBatch(2)
	reassign["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.creator, f.document)}
	_, err = importTestBatch(ctx, c, reassign, false)
	require.NoError(t, err)
	obsoleteGrant := testGrantBatch(3)
	obsoleteGrant["grants"] = []grantEntry{{
		"tenant_id": f.tenant, "principal_id": f.reader, "document_id": f.document,
		"incarnation": incarnation, "can_read": true, "can_download": true,
	}}
	_, err = importTestBatch(ctx, c, obsoleteGrant, false)
	require.ErrorContains(t, err, "invalid or conflicting grants entry")
	assertGrantRevision(t, c, 3)
	grantExec(t, c, `UPDATE grant_catalog_document_ownership SET ready=true WHERE document_id=$1`, f.document)
	assertDocumentAccess(t, c, f.document, f.creator, true, true, true)
	assertDocumentAccess(t, c, f.document, f.reader, false, false, false)

	// The unrelated incarnation can be assigned to a different owner.
	replacement := testGrantBatch(3)
	replacement["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.creator, f.reserved)}
	_, err = importTestBatch(ctx, c, replacement, false)
	require.NoError(t, err)
	assertReservationState(t, c, f.reserved, reservedIncarnation, "superseded")

	// A document created at the exact intended incarnation keeps its subject binding.
	exactID := uuid.NewString()
	reserve := testGrantBatch(4)
	reserve["reservations"] = []grantEntry{{"tenant_id": f.tenant, "principal_id": f.owner, "collection": "", "document_id": exactID}}
	_, err = importTestBatch(ctx, c, reserve, false)
	require.NoError(t, err)
	var exactIncarnation string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT incarnation::text FROM grant_catalog_document_reservations WHERE document_id=$1`, exactID).Scan(&exactIncarnation))
	grantExec(t, c, `INSERT INTO documents(id,incarnation,content) VALUES($1,$2,'reserved text')`, exactID, exactIncarnation)
	conflict := testGrantBatch(5)
	conflict["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.creator, exactID)}
	_, err = importTestBatch(ctx, c, conflict, false)
	require.ErrorContains(t, err, "invalid or conflicting assignments entry")
	assertGrantRevision(t, c, 5)
	assertReservationState(t, c, exactID, exactIncarnation, "pending")
	spend := testGrantBatch(5)
	spend["assignments"] = []grantEntry{grantAssignment(t, c, f.tenant, f.owner, exactID)}
	_, err = importTestBatch(ctx, c, spend, false)
	require.NoError(t, err)
	assertReservationState(t, c, exactID, exactIncarnation, "spent")
	require.NoError(t, c.DeleteDocument(ctx, domain.DocumentID(exactID)))
	insertGrantSource(t, c, exactID)
	assertReservationState(t, c, exactID, exactIncarnation, "spent")
	assertDocumentAccess(t, c, exactID, f.owner, false, false, false)
}

func assertReservationState(t *testing.T, c *Client, id, incarnation, expected string) {
	t.Helper()
	var state string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT state FROM grant_catalog_document_reservations WHERE document_id=$1 AND incarnation=$2`, id, incarnation).Scan(&state))
	require.Equal(t, expected, state)
}

func testGrantCatalogRoleBoundary(t *testing.T, databaseURL string) {
	roles := newGrantRoles(t, databaseURL, "catalog_operator", "catalog_service", "catalog_auditor")
	admin := migrationTestClient(t, databaseURL)
	applyMigrationPrefix(t, admin, 10)
	grantExec(t, admin, roles.sql(`CREATE ROLE {catalog_operator}; CREATE ROLE {catalog_service}; CREATE ROLE {catalog_auditor};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {catalog_auditor}, {catalog_service};
		ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {catalog_operator}, {catalog_service}`))
	require.NoError(t, admin.RunMigrations(t.Context()))
	grantExec(t, admin, roles.sql(`GRANT EXECUTE ON FUNCTION grant_catalog_import_batch(text,boolean) TO {catalog_operator};
		GRANT SELECT ON grant_catalog_policy_audit TO {catalog_auditor};
		GRANT SELECT ON schema_migrations TO {catalog_service};
		GRANT SELECT, INSERT, UPDATE, DELETE ON documents, ingest_jobs, document_fingerprints TO {catalog_service}`))
	operator := grantRoleClient(t, databaseURL, roles.sql("SET SESSION AUTHORIZATION {catalog_operator}"))
	service := grantRoleClient(t, databaseURL, roles.sql("SET SESSION AUTHORIZATION {catalog_service}; SET plan_cache_mode = force_generic_plan"))
	auditor := grantRoleClient(t, databaseURL, roles.sql("SET SESSION AUTHORIZATION {catalog_auditor}"))
	require.NoError(t, service.RunMigrations(t.Context()))
	var operatorImport, auditorImport, auditorAudit, operatorAudit, servingAccess, servingImport, operatorHelper bool
	require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT
		has_function_privilege('{catalog_operator}','grant_catalog_import_batch(text,boolean)','execute'),
		has_function_privilege('{catalog_auditor}','grant_catalog_import_batch(text,boolean)','execute'),
		has_table_privilege('{catalog_auditor}','grant_catalog_policy_audit','select'),
		has_table_privilege('{catalog_operator}','grant_catalog_policy_audit','select'),
		has_table_privilege('{catalog_service}','grant_catalog_document_access','select'),
		has_function_privilege('{catalog_service}','grant_catalog_import_batch(text,boolean)','execute'),
		has_function_privilege('{catalog_operator}','grant_catalog_parse_import(jsonb)','execute')`)).
		Scan(&operatorImport, &auditorImport, &auditorAudit, &operatorAudit, &servingAccess, &servingImport, &operatorHelper))
	require.True(t, operatorImport && auditorAudit)
	require.False(t, auditorImport || operatorAudit || servingAccess || servingImport || operatorHelper)

	b := testGrantBatch(0)
	b["tenants"] = []grantEntry{{"tenant_id": uuid.NewString()}}
	_, err := importTestBatch(t.Context(), service, b, false)
	require.Error(t, err)
	_, err = importTestBatch(t.Context(), auditor, b, false)
	require.Error(t, err)
	_, err = importTestBatch(t.Context(), operator, b, false)
	require.NoError(t, err)
	var login string
	require.NoError(t, admin.Pool.QueryRow(t.Context(), `SELECT operator FROM grant_catalog_policy_audit`).Scan(&login))
	require.Equal(t, roles.name("catalog_operator"), login)
	for _, login := range []*Client{operator, service} {
		for _, query := range []string{
			`SELECT * FROM grant_catalog_policy_audit`,
			`DELETE FROM grant_catalog_tenants`,
			`UPDATE grant_catalog_policy_revision SET revision=0`,
		} {
			_, err = login.Pool.Exec(t.Context(), query)
			require.Error(t, err)
		}
	}
	replay, err := importTestBatch(t.Context(), admin, b, false)
	require.ErrorContains(t, err, "batch ID conflict")
	require.False(t, replay.Replayed)

	grantExec(t, admin, roles.sql(`GRANT SELECT ON grant_catalog_document_access TO {catalog_service}`))
	assertGrantCount(t, service, "document_access", 0)
	_, err = importTestBatch(t.Context(), service, b, false)
	require.Error(t, err, "a scoped reader cannot execute policy mutations")
	doc, err := service.AcceptDocument(t.Context(), jobsRequest("service login document"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, doc.Outcome)
	lock, err := admin.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(context.WithoutCancel(t.Context())) }()
	_, err = lock.Exec(t.Context(), `LOCK TABLE grant_catalog_document_ownership IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	deleteCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, service.DeleteDocument(deleteCtx, doc.DocumentID))
	require.NoError(t, lock.Rollback(t.Context()))
	t.Run("assigned_cleanup", func(t *testing.T) {
		f := newGrantFixture()
		insertGrantSource(t, admin, f.document)
		insertGrantSource(t, admin, f.otherDocument)
		assignment := f.initialBatch(t, admin, false)
		assignment["expected_revision"] = 1
		_, applyErr := importTestBatch(t.Context(), operator, assignment, false)
		require.NoError(t, applyErr)
		_, markerErr := service.Pool.Exec(t.Context(), `UPDATE documents SET grant_catalog_assigned=false WHERE id=$1`, f.document)
		require.Error(t, markerErr)
		beforeInc := grantIncarnation(t, admin, f.document)
		grantExec(t, admin, `CREATE FUNCTION refuse_catalog_deletion() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'synthetic deletion audit interruption'; END; $$;
			CREATE TRIGGER refuse_catalog_deletion BEFORE INSERT ON grant_catalog_policy_audit
			FOR EACH ROW EXECUTE FUNCTION refuse_catalog_deletion()`)
		require.Error(t, service.DeleteDocument(t.Context(), domain.DocumentID(f.document)))
		assertGrantRevision(t, admin, 2)
		assertGrantCount(t, admin, "document_grants", 2)
		require.Equal(t, beforeInc, grantIncarnation(t, admin, f.document))
		grantExec(t, admin, `DROP TRIGGER refuse_catalog_deletion ON grant_catalog_policy_audit; DROP FUNCTION refuse_catalog_deletion()`)
		require.NoError(t, service.DeleteDocument(t.Context(), domain.DocumentID(f.document)))
		assertGrantRevision(t, admin, 3)
		assertGrantCount(t, admin, "document_ownership", 1)
		assertGrantCount(t, admin, "document_grants", 0)
		var deletedInc, deletedBy string
		require.NoError(t, admin.Pool.QueryRow(t.Context(), `SELECT incarnation::text,operator FROM grant_catalog_policy_audit WHERE revision=3`).Scan(&deletedInc, &deletedBy))
		require.Equal(t, beforeInc, deletedInc)
		require.Equal(t, roles.name("catalog_service"), deletedBy)
	})
}

type grantEntry map[string]any

func (f grantFixture) initialBatch(t *testing.T, c *Client, upgrade bool) map[string]any {
	t.Helper()
	b := testGrantBatch(0)
	b["tenants"] = []grantEntry{{"tenant_id": f.tenant}, {"tenant_id": f.otherTenant}}
	b["collections"] = []grantEntry{{"tenant_id": f.tenant, "collection": ""}, {"tenant_id": f.otherTenant, "collection": ""}}
	b["principals"] = []grantEntry{
		grantPrincipal(f.tenant, f.owner, "Owner", true), grantPrincipal(f.tenant, f.reader, "owner", true),
		grantPrincipal(f.tenant, f.creator, "creator", true), grantPrincipal(f.tenant, f.disabled, "disabled", false),
		grantPrincipal(f.otherTenant, f.stranger, "other", true),
		{"tenant_id": f.otherTenant, "principal_id": uuid.NewString(), "issuer": "Synthetic Issuer", "subject": "Owner", "enabled": true},
	}
	b["create_permissions"] = []grantEntry{
		{"tenant_id": f.tenant, "principal_id": f.owner, "collection": "", "can_create": true},
		{"tenant_id": f.tenant, "principal_id": f.creator, "collection": "", "can_create": true},
	}
	assignments := []grantEntry{grantAssignment(t, c, f.tenant, f.owner, f.document), grantAssignment(t, c, f.otherTenant, f.stranger, f.otherDocument)}
	if upgrade {
		assignments = append(assignments, grantAssignment(t, c, f.tenant, f.owner, f.legacy))
	}
	b["assignments"] = assignments
	b["grants"] = []grantEntry{
		{"tenant_id": f.tenant, "principal_id": f.reader, "document_id": f.document, "incarnation": grantIncarnation(t, c, f.document), "can_read": true, "can_download": false},
		{"tenant_id": f.tenant, "principal_id": f.disabled, "document_id": f.document, "incarnation": grantIncarnation(t, c, f.document), "can_read": true, "can_download": true},
	}
	b["reservations"] = []grantEntry{{"tenant_id": f.tenant, "principal_id": f.owner, "collection": "", "document_id": f.reserved}}
	return b
}

func grantPrincipal(tenant, id, subject string, enabled bool) grantEntry {
	return grantEntry{"tenant_id": tenant, "principal_id": id, "issuer": "synthetic issuer", "subject": subject, "enabled": enabled}
}

func grantAssignment(t *testing.T, c *Client, tenant, owner, id string) grantEntry {
	t.Helper()
	return grantEntry{"tenant_id": tenant, "principal_id": owner, "collection": "", "document_id": id, "source_version": grantSource(t, c, id)}
}

func insertGrantSource(t *testing.T, c *Client, id string) {
	t.Helper()
	grantExec(t, c, `INSERT INTO documents(id,content,status,metadata) VALUES($1,'same text','completed','{"uploader":"owner","tenant":"synthetic"}')`, id)
}

func grantIncarnation(t *testing.T, c *Client, id string) string {
	t.Helper()
	var incarnation string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT incarnation::text FROM grant_catalog_source_snapshot($1)`, id).Scan(&incarnation))
	return incarnation
}

func grantSource(t *testing.T, c *Client, id string) string {
	t.Helper()
	var version string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT source_version FROM grant_catalog_source_snapshot($1)`, id).Scan(&version))
	return version
}

func testGrantBatch(revision int64) map[string]any {
	return map[string]any{"batch_id": uuid.NewString(), "expected_revision": revision}
}

func importTestBatch(ctx context.Context, c *Client, batch map[string]any, dryRun bool) (GrantImportResult, error) {
	body, err := json.Marshal(batch)
	if err != nil {
		return GrantImportResult{}, err
	}
	return c.ImportGrantBatch(ctx, bytes.NewReader(body), dryRun)
}

// grantRoles names the cluster-wide roles one test uses. Every test shares
// the service, so each role gets a name no other test has and is dropped
// with the test.
type grantRoles map[string]string

func newGrantRoles(t *testing.T, databaseURL string, purposes ...string) grantRoles {
	t.Helper()
	roles := make(grantRoles, len(purposes))
	for _, purpose := range purposes {
		roles[purpose] = pgtest.Role(t, databaseURL, purpose)
	}
	return roles
}

// sql replaces each {purpose} in statement with the role's name, which is
// valid both as an identifier and inside a string literal.
func (r grantRoles) sql(statement string) string {
	pairs := make([]string, 0, 2*len(r))
	for purpose, name := range r {
		pairs = append(pairs, "{"+purpose+"}", name)
	}
	return strings.NewReplacer(pairs...).Replace(statement)
}

func (r grantRoles) name(purpose string) string {
	return r[purpose]
}

func grantExec(t *testing.T, c *Client, query string, args ...any) {
	t.Helper()
	_, err := c.Pool.Exec(t.Context(), query, args...)
	require.NoError(t, err)
}

func assertGrantCount(t *testing.T, c *Client, relation string, want int) {
	t.Helper()
	var count int
	query := "SELECT count(*) FROM " + pgx.Identifier{"grant_catalog_" + relation}.Sanitize()
	require.NoError(t, c.Pool.QueryRow(t.Context(), query).Scan(&count))
	require.Equal(t, want, count, relation)
}

func assertGrantRevision(t *testing.T, c *Client, want int64) {
	t.Helper()
	var revision int64
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT revision FROM grant_catalog_policy_revision`).Scan(&revision))
	require.Equal(t, want, revision)
}

func assertDocumentAccess(t *testing.T, c *Client, id, principal string, read, download, owner bool) {
	t.Helper()
	var actualRead, actualDownload, actualOwner bool
	err := c.Pool.QueryRow(t.Context(), `SELECT can_read, can_download, is_owner
		FROM grant_catalog_document_access WHERE document_id=$1 AND principal_id=$2`, id, principal).
		Scan(&actualRead, &actualDownload, &actualOwner)
	if !read {
		require.ErrorIs(t, err, pgx.ErrNoRows)
		return
	}
	require.NoError(t, err)
	require.True(t, actualRead)
	require.Equal(t, download, actualDownload)
	require.Equal(t, owner, actualOwner)
}

func grantRoleClient(t *testing.T, databaseURL, setRole string) *Client {
	t.Helper()
	c, err := NewClient(t.Context(), databaseURL, func(cfg *pgxpool.Config) {
		cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, setRole)
			return err
		}
	})
	require.NoError(t, err)
	t.Cleanup(c.Pool.Close)
	return c
}
