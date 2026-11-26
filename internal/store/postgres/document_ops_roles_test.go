package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestScopedDocumentOperationsNeedOnlyDocumentedFunctionPrivileges(t *testing.T) {
	databaseURL := startTestDatabase(t)
	admin := migrationTestClient(t, databaseURL)
	applyMigrationPrefix(t, admin, 10)
	roles := newGrantRoles(t, databaseURL, servingRoles...)
	grantExec(t, admin, roles.sql(`CREATE ROLE {quarry_service}; CREATE ROLE {quarry_operator}; CREATE ROLE {quarry_auditor};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {quarry_service}, {quarry_operator};
		ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {quarry_service}, {quarry_operator}`))
	require.NoError(t, admin.RunMigrations(t.Context()))

	functions := []string{
		"grant_catalog_policy_state(uuid,uuid)",
		"grant_catalog_list_documents(uuid,uuid,uuid,integer,text,boolean)",
		"grant_catalog_get_document(uuid,uuid,uuid,boolean)",
		"grant_catalog_authorize_file(uuid,uuid,uuid)",
		"grant_catalog_authorize_delete(uuid,uuid,uuid,uuid)",
		"grant_catalog_record_deletion_owner(uuid,uuid,uuid,uuid)",
	}
	for _, fn := range functions {
		var inherited bool
		require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT has_function_privilege('{quarry_service}',$1,'execute')
			OR has_function_privilege('{quarry_operator}',$1,'execute') OR has_function_privilege('public',$1,'execute')`), fn).Scan(&inherited))
		require.False(t, inherited, fn+" must not be reachable through default privileges")
	}
	var receiptTableAccess bool
	require.NoError(t, admin.Pool.QueryRow(t.Context(), roles.sql(`SELECT has_table_privilege('{quarry_service}','grant_catalog_deletion_receipts','select')
		OR has_table_privilege('{quarry_operator}','grant_catalog_deletion_receipts','select')
		OR has_table_privilege('public','grant_catalog_deletion_receipts','select')`)).Scan(&receiptTableAccess))
	require.False(t, receiptTableAccess, "grant_catalog_deletion_receipts must not be readable through default privileges")

	grantExec(t, admin, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
	f := provisionOwnedFixture(t, admin)
	service := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
	operator := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_operator")}.Sanitize())
	require.NoError(t, service.RunMigrations(t.Context()))

	req := ownedRequest(f.owner, "team", "restricted role document")
	req.SupportsScopedVectors = true
	key := "documents/document/" + string(req.Document.ID) + "/orig.pdf"
	req.Document.S3Key = &key
	res, err := service.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)

	state, err := service.ScopedPolicyState(t.Context(), f.owner)
	require.NoError(t, err)
	require.True(t, state.Active)

	page, err := service.ListScopedDocumentsPage(t.Context(), f.owner, "", 10, "", true)
	require.NoError(t, err)
	require.Len(t, page.Documents, 1)
	require.Nil(t, page.Documents[0].S3Key)

	summary, err := service.GetScopedDocumentSummary(t.Context(), f.owner, req.Document.ID, true)
	require.NoError(t, err)
	require.NotNil(t, summary.Document)
	require.Nil(t, summary.Document.S3Key)

	fileRes, err := service.WithScopedDocumentFile(t.Context(), f.owner, req.Document.ID, func(context.Context, string) error { return nil })
	require.NoError(t, err)
	require.True(t, fileRes.Active && fileRes.Found)

	rec := newDeletionRecorder()
	ops := rec.ops("")
	ops.InMemory = true
	inc, complete, err := service.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), req.Document.ID, ops)
	require.NoError(t, err)
	require.False(t, complete)
	require.NoError(t, service.ResumeScopedDeletion(t.Context(), req.Document.ID, inc, ops))

	// Operator login cannot invoke serving document-operation functions.
	_, err = operator.ScopedPolicyState(t.Context(), f.owner)
	require.Error(t, err, "operator logins cannot query serving policy state")
}
