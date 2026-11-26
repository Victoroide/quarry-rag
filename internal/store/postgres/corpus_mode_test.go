package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

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

func TestCorpusModeDefaultsToServiceAndActivatesIdempotently(t *testing.T) {
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

	// Dry-run cutover previews without changing corpus mode or policy revision.
	dry, err := c.ActivateGrants(ctx, true)
	require.NoError(t, err)
	require.True(t, dry.DryRun)
	require.False(t, dry.AlreadyActive)
	require.EqualValues(t, 0, dry.Revision)
	mode, err = c.CorpusAuthzMode(ctx)
	require.NoError(t, err)
	require.Equal(t, "service", mode)

	// Interrupted cutover (audit insert trigger fails) rolls back atomically.
	grantExec(t, c, `CREATE FUNCTION refuse_security_audit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'synthetic security audit interruption'; END; $$;
		CREATE TRIGGER refuse_security_audit BEFORE INSERT ON grant_catalog_security_audit
		FOR EACH ROW EXECUTE FUNCTION refuse_security_audit()`)
	_, err = c.ActivateGrants(ctx, false)
	require.Error(t, err)
	mode, err = c.CorpusAuthzMode(ctx)
	require.NoError(t, err)
	require.Equal(t, "service", mode)
	grantExec(t, c, `DROP TRIGGER refuse_security_audit ON grant_catalog_security_audit; DROP FUNCTION refuse_security_audit()`)

	// Cutover succeeds atomically and advances revision + writes audit.
	activated, err := c.ActivateGrants(ctx, false)
	require.NoError(t, err)
	require.False(t, activated.DryRun)
	require.False(t, activated.AlreadyActive)
	require.EqualValues(t, 1, activated.Revision)
	mode, err = c.CorpusAuthzMode(ctx)
	require.NoError(t, err)
	require.Equal(t, "grants", mode)

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
	_, _, err = service.BeginScopedDeletion(reqCtx, f.owner, time.Now().Add(time.Minute), req.Document.ID, domain.DeleteOperations{})
	require.Error(t, err)
	var deletingCount int
	require.NoError(t, admin.Pool.QueryRow(t.Context(), `SELECT count(*) FROM document_deletions WHERE document_id=$1`, string(req.Document.ID)).Scan(&deletingCount))
	require.Zero(t, deletingCount, "failed audit must roll back deletion intent")

	grantExec(t, admin, `DROP TRIGGER refuse_runtime_audit ON grant_catalog_security_audit; DROP FUNCTION refuse_runtime_audit()`)

	// Auditor reads the recorded cutover, import, and ingest audit events.
	var total int
	require.NoError(t, auditor.Pool.QueryRow(t.Context(), `SELECT count(*) FROM grant_catalog_security_audit`).Scan(&total))
	require.GreaterOrEqual(t, total, 3)
}
