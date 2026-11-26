package postgres

import (
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestOwnedAcceptanceNeedsOnlyTheDocumentedFunctionPrivileges(t *testing.T) {
	databaseURL := startTestDatabase(t)
	admin := migrationTestClient(t, databaseURL)
	applyMigrationPrefix(t, admin, 10)
	roles := newGrantRoles(t, databaseURL, servingRoles...)
	grantExec(t, admin, roles.sql(`CREATE ROLE {quarry_service}; CREATE ROLE {quarry_operator}; CREATE ROLE {quarry_auditor};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {quarry_service}, {quarry_operator};
		ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {quarry_service}, {quarry_operator}`))
	require.NoError(t, admin.RunMigrations(t.Context()))
	functions := []string{
		"grant_catalog_authorize_ingest(uuid,uuid,text,uuid)", "grant_catalog_lock_ingest_permission(uuid,uuid,text)",
		"grant_catalog_bind_accepted_document(uuid,uuid,text,uuid,uuid,boolean)", "grant_catalog_publish_accepted_document(uuid,uuid)",
		"grant_catalog_document_tenant(uuid,uuid)", "grant_catalog_may_bind(uuid,uuid,text,uuid,uuid,boolean)",
		"grant_catalog_backfill_candidates(uuid,integer)", "grant_catalog_publish_verified_document(uuid,uuid,text,text)",
	}
	for _, function := range functions {
		var inherited bool
		require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT has_function_privilege('{quarry_service}',$1,'execute')
			OR has_function_privilege('{quarry_operator}',$1,'execute') OR has_function_privilege('public',$1,'execute')`), function).Scan(&inherited))
		require.False(t, inherited, function+" must not be reachable through default privileges")
	}
	var viewInherited bool
	require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT has_table_privilege('{quarry_service}','grant_catalog_active_principals','select')
		OR has_table_privilege('{quarry_operator}','grant_catalog_active_principals','select')
		OR has_table_privilege('public','grant_catalog_active_principals','select')`)).Scan(&viewInherited))
	require.False(t, viewInherited, "grant_catalog_active_principals must not be reachable through default privileges")
	grantExec(t, admin, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
	f := provisionOwnedFixture(t, admin)
	service := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
	operator := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_operator")}.Sanitize())
	require.NoError(t, service.RunMigrations(t.Context()))

	var tableAccess bool
	require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT has_table_privilege('{quarry_service}','grant_catalog_document_ownership','select')
		OR has_table_privilege('{quarry_service}','grant_catalog_create_permissions','select')
		OR has_table_privilege('{quarry_service}','grant_catalog_document_reservations','select')
		OR has_table_privilege('{quarry_service}','grant_catalog_principals','select')`)).Scan(&tableAccess))
	require.False(t, tableAccess, "the serving login reads no catalog table")

	resolved, found, err := service.ResolvePrincipal(t.Context(), "synthetic issuer", "owner")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, f.owner, resolved)
	_, found, err = service.ResolvePrincipal(t.Context(), "synthetic issuer", "disabled")
	require.NoError(t, err)
	require.False(t, found)

	req := ownedRequest(f.owner, "", "service login text")
	req.SupportsScopedVectors = true
	target, err := service.AuthorizeOwnedIngest(t.Context(), f.owner, "", "")
	require.NoError(t, err)
	require.Equal(t, domain.OwnedNew, target.Decision)
	result, err := service.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	job, err := service.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, f.tenant, job.Tenant)
	ok, err := service.CompleteJob(t.Context(), *job, []domain.Chunk{chunkFor(req.Document.ID, 0)}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	row, found := f.ownership(t, string(req.Document.ID), req.Owner.Incarnation)
	require.True(t, found)
	require.True(t, row.ready)
	_, err = service.Pool.Exec(t.Context(), `UPDATE grant_catalog_document_ownership SET principal_id=principal_id`)
	require.Error(t, err)
	_, err = service.BackfillCandidates(t.Context(), "", 1)
	require.Error(t, err, "serving logins cannot list backfill candidates")

	legacy, _ := f.legacyDocument(t, 1)
	candidates, err := operator.BackfillCandidates(t.Context(), "", 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, legacy, candidates[0].DocumentID)
	published, err := operator.PublishVerifiedDocument(t.Context(), candidates[0])
	require.NoError(t, err)
	require.True(t, published)
	_, err = operator.AuthorizeOwnedIngest(t.Context(), f.owner, "", "")
	require.Error(t, err, "operator logins cannot authorize uploads")

	// Exceeding maxAttempts on an assigned document must still fail the job even
	// if the caller lacks EXECUTE on grant_catalog_document_tenant.
	exhausted := ownedRequest(f.owner, "", "exhausted job")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, exhausted).Outcome)
	grantExec(t, admin, `UPDATE ingest_jobs SET attempts=5 WHERE document_id=$1`, string(exhausted.Document.ID))
	grantExec(t, admin, `REVOKE EXECUTE ON FUNCTION public.grant_catalog_document_tenant(uuid, uuid) FROM `+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
	claimed, err := service.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	require.Nil(t, claimed)
	var status string
	require.NoError(t, admin.Pool.QueryRow(t.Context(), `SELECT status FROM documents WHERE id=$1`, string(exhausted.Document.ID)).Scan(&status))
	require.Equal(t, "failed", status)
}
