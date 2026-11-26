package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
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
		"grant_catalog_authorize_file_advisory(uuid,uuid,uuid)",
		"grant_catalog_lock_file_principal(uuid,uuid)",
		"grant_catalog_file_principal_key(uuid,uuid)",
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

	_, err := admin.ActivateDeletion(t.Context())
	require.NoError(t, err)
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

	grantExec(t, admin, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	fileRes, err = service.WithScopedDocumentFile(t.Context(), f.owner, req.Document.ID, func(ctx context.Context, _ string) error {
		var held bool
		lockErr := admin.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_locks
			WHERE locktype='advisory' AND granted AND mode='ShareLock' AND objsubid=2
			AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
			AND classid::bigint=$1 AND objid::bigint=(grant_catalog_file_principal_key($2,$3)::bigint & 4294967295))`,
			scopedFilePrincipalLockClass, f.owner.TenantID, f.owner.ID).Scan(&held)
		require.NoError(t, lockErr)
		require.True(t, held)
		return nil
	})
	require.NoError(t, err)
	require.True(t, fileRes.Active && fileRes.Found)
	// An old serving replica can still call the row-locked function after migration and activation.
	var active bool
	var oldKey string
	require.NoError(t, service.Pool.QueryRow(t.Context(), `SELECT active,s3_key FROM grant_catalog_authorize_file($1,$2,$3)`,
		f.owner.TenantID, f.owner.ID, string(req.Document.ID)).Scan(&active, &oldKey))
	require.True(t, active)
	require.Equal(t, key, oldKey)
	_, err = service.Pool.Exec(t.Context(), `SELECT grant_catalog_file_principal_key($1,$2)`, f.owner.TenantID, f.owner.ID)
	require.Error(t, err)
	_, err = operator.Pool.Exec(t.Context(), `SELECT grant_catalog_lock_file_principal($1,$2)`, f.owner.TenantID, f.owner.ID)
	require.Error(t, err)
	_, err = operator.Pool.Exec(t.Context(), `SELECT * FROM grant_catalog_authorize_file_advisory($1,$2,$3)`, f.owner.TenantID, f.owner.ID, string(req.Document.ID))
	require.Error(t, err)
	for _, table := range []string{"grant_catalog_principals", "grant_catalog_document_ownership", "grant_catalog_document_grants"} {
		var permitted bool
		require.NoError(t, service.Pool.QueryRow(t.Context(), `SELECT has_table_privilege($1,'SELECT,INSERT,UPDATE,DELETE')`, table).Scan(&permitted))
		require.False(t, permitted, table)
	}

	rec := newDeletionRecorder()
	ops := rec.ops(summary.Document.Incarnation)
	ops.InMemory, ops.MemoryOwner = true, uuid.NewString()
	inc, complete, err := service.BeginScopedDeletion(t.Context(), f.owner, time.Now().Add(time.Minute), req.Document.ID, ops)
	require.NoError(t, err)
	require.False(t, complete)
	require.NoError(t, service.ResumeScopedDeletion(t.Context(), req.Document.ID, inc, ops))

	// Operator login cannot invoke serving document-operation functions.
	_, err = operator.ScopedPolicyState(t.Context(), f.owner)
	require.Error(t, err, "operator logins cannot query serving policy state")

	t.Run("alternate corpus schema", func(t *testing.T) {
		grantExec(t, admin, `CREATE SCHEMA alternate_file_corpus`)
		const path = `SET search_path TO alternate_file_corpus, public`
		alternate := grantRoleClient(t, databaseURL, path)
		require.NoError(t, alternate.RunMigrations(t.Context()))
		grants := publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public")
		grantExec(t, alternate, strings.ReplaceAll(strings.ReplaceAll(grants, "public.", "alternate_file_corpus."),
			"SCHEMA public", "SCHEMA alternate_file_corpus"))
		f := provisionOwnedFixture(t, alternate)
		id := domain.DocumentID(f.acceptOwned(t, f.owner, "", key))
		grantExec(t, alternate, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)

		alternateService := grantRoleClient(t, databaseURL, roles.sql("SET SESSION AUTHORIZATION {quarry_service}; "+path))
		alternateOperator := grantRoleClient(t, databaseURL, roles.sql("SET SESSION AUTHORIZATION {quarry_operator}; "+path))
		res, err := alternateService.WithScopedDocumentFile(t.Context(), f.owner, id, noopFileSign)
		require.NoError(t, err)
		require.True(t, res.Found)

		changes := map[string]any{"principals": []grantEntry{grantPrincipal(f.tenant, f.owner.ID, "owner", false)}}
		_, err = alternateOperator.Pool.Exec(t.Context(), `SELECT * FROM grant_catalog_import_batch($1,false)`, fileImportBody(t, f, changes))
		require.NoError(t, err)
		res, err = alternateService.WithScopedDocumentFile(t.Context(), f.owner, id, unexpectedScopedFileSignature)
		require.NoError(t, err)
		require.False(t, res.Active)
	})
}

func TestScopedDeletionUpgradeWithUnsetMarker(t *testing.T) {
	ctx := t.Context()
	databaseURL := startTestDatabase(t)
	admin := migrationTestClient(t, databaseURL)
	applyMigrationsExcept(t, admin, 38)
	roles := newGrantRoles(t, databaseURL, servingRoles...)
	grantExec(t, admin, roles.sql(`CREATE ROLE {quarry_service}; CREATE ROLE {quarry_operator}; CREATE ROLE {quarry_auditor}`))
	grantExec(t, admin, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
	f := provisionOwnedFixture(t, admin)
	operator := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_operator")}.Sanitize())
	_, err := operator.ActivateGrants(ctx, false)
	require.NoError(t, err)

	service := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
	service.SetAuthzMode("grants")
	oldID := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/doc/history/original.pdf"))
	liveID := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/doc/live/original.pdf"))
	oldInc, liveInc := storedIncarnation(t, admin, oldID), storedIncarnation(t, admin, liveID)
	rec := newDeletionRecorder()
	oldOps := rec.ops(oldInc)
	oldOps.InMemory, oldOps.MemoryOwner = true, uuid.NewString()
	// Seed the receipt that the previous binary could finalize before activation.
	// The migration and upgraded binary must preserve this read-only result.
	grantExec(t, admin, `INSERT INTO document_deletions (document_id, incarnation, kind, finalized, vector_mode, step)
		VALUES ($1,$2,'qualified_intent',true,'local',$3)`, string(oldID), oldInc, stepDone)
	grantExec(t, admin, `INSERT INTO grant_catalog_deletion_receipts (document_id, incarnation, tenant_id, principal_id)
		VALUES ($1,$2,$3,$4)`, string(oldID), oldInc, f.owner.TenantID, f.owner.ID)
	require.NoError(t, admin.DeleteDocument(ctx, oldID))
	job := f.claim(t)
	require.Equal(t, liveID, job.Document.ID)
	completed, err := admin.CompleteJob(ctx, *job, []domain.Chunk{chunkFor(liveID, 0)}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, completed)
	var oldRevision int64
	require.NoError(t, admin.Pool.QueryRow(ctx, `SELECT revision FROM grant_catalog_policy_revision`).Scan(&oldRevision))
	require.NoError(t, admin.RunMigrations(ctx))
	require.NoError(t, service.WritersOpen(ctx))
	page, err := service.ListScopedDocumentsPage(ctx, f.owner, "", 10, "", true)
	require.NoError(t, err)
	require.Len(t, page.Documents, 1)
	require.Equal(t, liveInc, page.Documents[0].Incarnation)
	reader := GrantReader{Pool: service.Pool}
	scope, err := reader.DeriveReadScope(ctx, f.owner, domain.ReadScopeRequest{Membership: 1})
	require.NoError(t, err)
	require.Equal(t, []domain.ReadableDocument{{ID: string(liveID), Incarnation: liveInc}}, scope.Documents)

	for _, preview := range []bool{true, false} {
		_, err = operator.ActivateGrants(ctx, preview)
		require.EqualError(t, err, "grants activation: checkpointed document deletion must be activated first")
	}
	ops := rec.ops(liveInc)
	ops.InMemory, ops.MemoryOwner = true, oldOps.MemoryOwner
	for _, qualifier := range []string{"", liveInc, oldInc} {
		refused := ops
		refused.ExpectedIncarnation = qualifier
		_, _, err = service.BeginScopedDeletion(ctx, f.owner, time.Now().Add(time.Minute), liveID, refused)
		require.ErrorIs(t, err, domain.ErrDeletionInactive)
		_, _, err = service.BeginScopedDeletion(ctx, f.peer, time.Now().Add(time.Minute), liveID, refused)
		require.ErrorIs(t, err, domain.ErrDocumentNotFound)
	}
	_, complete, err := service.BeginScopedDeletion(ctx, f.owner, time.Now().Add(time.Minute), oldID, oldOps)
	require.NoError(t, err)
	require.True(t, complete)
	_, _, err = service.BeginScopedDeletion(ctx, f.peer, time.Now().Add(time.Minute), oldID, oldOps)
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)
	require.Empty(t, rec.all())
	require.Zero(t, receiptCount(ctx, t, admin, liveID))
	require.Equal(t, "completed", documentStatus(t, admin, liveID))
	mode, err := service.CorpusAuthzMode(ctx)
	require.NoError(t, err)
	require.Equal(t, "grants", mode)
	var revision int64
	var auditCount int
	require.NoError(t, admin.Pool.QueryRow(ctx, `SELECT revision,
		(SELECT count(*) FROM grant_catalog_security_audit WHERE action='corpus_cutover')
		FROM grant_catalog_policy_revision`).Scan(&revision, &auditCount))
	require.Equal(t, oldRevision, revision)
	require.Equal(t, 1, auditCount)

	activation, err := admin.ActivateDeletion(ctx)
	require.NoError(t, err)
	require.False(t, activation.AlreadyActive)
	cutover, err := operator.ActivateGrants(ctx, false)
	require.NoError(t, err)
	require.True(t, cutover.AlreadyActive)
	require.Equal(t, revision, cutover.Revision)
	_, complete, err = service.BeginScopedDeletion(ctx, f.owner, time.Now().Add(time.Minute), liveID, ops)
	require.NoError(t, err)
	require.False(t, complete)
	require.NoError(t, service.ResumeScopedDeletion(ctx, liveID, liveInc, ops))
	require.Equal(t, 1, rec.count("source"))
}
