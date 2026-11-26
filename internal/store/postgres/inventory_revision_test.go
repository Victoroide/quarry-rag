package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func inventoryFixture(t *testing.T, c *Client) grantFixture {
	t.Helper()
	f := newGrantFixture()
	insertGrantSource(t, c, f.document)
	insertGrantSource(t, c, f.otherDocument)
	_, err := importTestBatch(t.Context(), c, f.initialBatch(t, c, false), false)
	require.NoError(t, err)
	return f
}

func revisionFor(t *testing.T, c *Client, tenant, principal string) int64 {
	t.Helper()
	state, err := c.ScopedPolicyState(t.Context(), domain.Principal{TenantID: tenant, ID: principal})
	require.NoError(t, err)
	return state.Revision
}

func TestInventoryRevisionTracksEffectivePolicy(t *testing.T) {
	c := migrationTestClient(t, startTestDatabase(t))
	require.NoError(t, c.RunMigrations(t.Context()))
	f := inventoryFixture(t, c)
	owner := domain.Principal{TenantID: f.tenant, ID: f.owner}
	inc := grantIncarnation(t, c, f.document)
	people := []string{f.owner, f.reader, f.creator, f.disabled, f.stranger}
	last := make([]int64, len(people))
	check := func(t *testing.T, delta ...int) {
		t.Helper()
		for i, person := range people {
			tenant := f.tenant
			if person == f.stranger {
				tenant = f.otherTenant
			}
			last[i] += int64(delta[i])
			require.Equal(t, last[i], revisionFor(t, c, tenant, person), "principal %d", i)
		}
		assertGrantCount(t, c, "inventory_revision_pending", 0)
	}
	check(t, 1, 1, 1, 1, 1)
	catalog := int64(1)
	apply := func(t *testing.T, group string, entry grantEntry) {
		t.Helper()
		b := testGrantBatch(catalog)
		b[group] = []grantEntry{entry}
		res, err := importTestBatch(t.Context(), c, b, false)
		require.NoError(t, err)
		catalog++
		require.Equal(t, catalog, res.Revision)
	}
	grant := func(principal string, read, download bool) grantEntry {
		return grantEntry{"tenant_id": f.tenant, "principal_id": principal, "document_id": f.document,
			"incarnation": inc, "can_read": read, "can_download": download}
	}
	for _, tc := range []struct {
		name, group string
		entry       grantEntry
		delta       []int
	}{
		{"no_op", "grants", grant(f.reader, true, false), []int{0, 0, 0, 0, 0}},
		{"reservation", "reservations", grantEntry{"tenant_id": f.tenant, "principal_id": f.owner, "collection": "", "document_id": uuid.NewString()}, []int{0, 0, 0, 0, 0}},
		{"download", "grants", grant(f.reader, true, true), []int{0, 1, 0, 0, 0}},
		{"revoke", "grants", grant(f.reader, false, false), []int{0, 1, 0, 0, 0}},
		{"grant", "grants", grant(f.reader, true, false), []int{0, 1, 0, 0, 0}},
		{"disabled_download", "grants", grant(f.disabled, true, false), []int{0, 0, 0, 1, 0}},
		{"disable", "principals", grantPrincipal(f.tenant, f.reader, "owner", false), []int{0, 1, 0, 0, 0}},
		{"reenable", "principals", grantPrincipal(f.tenant, f.reader, "owner", true), []int{0, 1, 0, 0, 0}},
		{"create_revocation", "create_permissions", grantEntry{"tenant_id": f.tenant, "principal_id": f.creator, "collection": "", "can_create": false}, []int{0, 0, 1, 0, 0}},
		{"collection_only", "collections", grantEntry{"tenant_id": f.tenant, "collection": "new"}, []int{0, 0, 0, 0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apply(t, tc.group, tc.entry)
			check(t, tc.delta...)
		})
	}

	candidates, err := c.BackfillCandidates(t.Context(), "", 10)
	require.NoError(t, err)
	for _, candidate := range candidates {
		if candidate.DocumentID == f.document {
			published, publishErr := c.PublishVerifiedDocument(t.Context(), candidate)
			require.NoError(t, publishErr)
			require.True(t, published)
		}
	}
	check(t, 1, 1, 0, 1, 0)

	const fileKey = "documents/inventory.pdf"
	grantExec(t, c, `UPDATE documents SET s3_key=$2 WHERE id=$1`, f.document, fileKey)
	checkOwnerAccess := func(t *testing.T) {
		t.Helper()
		page, listErr := c.ListScopedDocumentsPage(t.Context(), owner, "", 10, "", true)
		require.NoError(t, listErr)
		require.True(t, page.Active)
		require.Equal(t, catalog, page.CatalogRevision)
		require.Len(t, page.Documents, 1)
		require.Equal(t, f.document, page.Documents[0].ID)
		require.Equal(t, inc, page.Documents[0].Incarnation)

		status, statusErr := c.GetScopedDocumentSummary(t.Context(), owner, domain.DocumentID(f.document), true)
		require.NoError(t, statusErr)
		require.True(t, status.Active)
		require.Equal(t, catalog, status.CatalogRevision)
		require.NotNil(t, status.Document)
		require.Equal(t, page.Documents[0], *status.Document)

		var active bool
		var key string
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT active, s3_key FROM grant_catalog_authorize_file($1,$2,$3)`,
			f.tenant, f.owner, f.document).Scan(&active, &key))
		require.True(t, active)
		require.Equal(t, fileKey, key)
	}
	checkOwnerAccess(t)
	for _, tc := range []struct {
		name           string
		read, download bool
	}{
		{"owner_grant", true, false},
		{"owner_download", true, true},
		{"owner_revoke", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apply(t, "grants", grant(f.owner, tc.read, tc.download))
			checkOwnerAccess(t)
			check(t, 0, 0, 0, 0, 0)
		})
	}

	grantExec(t, c, `UPDATE grant_catalog_document_ownership SET source_version=repeat('a',64) WHERE document_id=$1`, f.document)
	grantExec(t, c, `UPDATE documents SET status='processing' WHERE id=$1`, f.document)
	check(t, 0, 0, 0, 0, 0)
	grantExec(t, c, `UPDATE documents SET collection='new' WHERE id=$1`, f.document)
	check(t, 1, 1, 0, 1, 0)
	grantExec(t, c, `UPDATE documents SET collection='' WHERE id=$1`, f.document)
	check(t, 1, 1, 0, 1, 0)
	grantExec(t, c, `UPDATE documents SET status='deleting' WHERE id=$1`, f.document)
	check(t, 1, 1, 0, 1, 0)
	grantExec(t, c, `DELETE FROM documents WHERE id=$1`, f.document)
	check(t, 1, 1, 0, 1, 0)

	req := ownedRequest(owner, "", "inventory accepted document")
	req.SupportsScopedVectors = true
	res, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)
	check(t, 1, 0, 0, 0, 0)
	accepted := string(req.Document.ID)
	grantExec(t, c, `SELECT grant_catalog_publish_accepted_document($1,$2)`, accepted, grantIncarnation(t, c, accepted))
	check(t, 0, 0, 0, 0, 0)
	_, err = c.Pool.Exec(t.Context(), `UPDATE documents SET grant_catalog_assigned=false WHERE id=$1`, accepted)
	require.Error(t, err, "the catalog forbids clearing an assigned marker")
	check(t, 0, 0, 0, 0, 0)
	grantExec(t, c, `UPDATE grant_catalog_document_ownership SET principal_id=$2 WHERE document_id=$1`, accepted, f.creator)
	check(t, 1, 0, 1, 0, 0)
	grantExec(t, c, `INSERT INTO grant_catalog_document_grants VALUES($1,$2,$3,$4,true,false)`,
		f.tenant, accepted, grantIncarnation(t, c, accepted), f.reader)
	check(t, 0, 1, 0, 0, 0)
	grantExec(t, c, `UPDATE grant_catalog_document_grants SET principal_id=$2 WHERE document_id=$1`, accepted, f.disabled)
	check(t, 0, 1, 0, 1, 0)
	grantExec(t, c, `UPDATE grant_catalog_document_grants SET principal_id=$2 WHERE document_id=$1`, accepted, f.creator)
	check(t, 0, 0, 0, 1, 0)
	grantExec(t, c, `UPDATE grant_catalog_document_grants SET principal_id=$2 WHERE document_id=$1`, accepted, f.reader)
	check(t, 0, 1, 0, 0, 0)

	otherReq := ownedRequest(owner, "", "inventory second accepted document")
	otherReq.SupportsScopedVectors = true
	res, err = c.AcceptDocument(t.Context(), otherReq)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)
	check(t, 1, 0, 0, 0, 0)
	other := string(otherReq.Document.ID)
	grantExec(t, c, `INSERT INTO grant_catalog_document_grants VALUES($1,$2,$3,$4,true,false)`,
		f.tenant, accepted, grantIncarnation(t, c, accepted), f.creator)
	check(t, 0, 0, 0, 0, 0)
	moveGrant := `UPDATE grant_catalog_document_grants SET document_id=$2, incarnation=$3
		WHERE document_id=$1 AND principal_id=$4`
	grantExec(t, c, moveGrant, accepted, other, grantIncarnation(t, c, other), f.creator)
	check(t, 0, 0, 1, 0, 0)
	grantExec(t, c, moveGrant, other, accepted, grantIncarnation(t, c, accepted), f.creator)
	check(t, 0, 0, 1, 0, 0)
}

func TestInventoryRevisionCommitOrder(t *testing.T) {
	c := migrationTestClient(t, startTestDatabase(t))
	require.NoError(t, c.RunMigrations(t.Context()))
	f := inventoryFixture(t, c)
	owner := domain.Principal{TenantID: f.tenant, ID: f.owner}
	other := domain.Principal{TenantID: f.otherTenant, ID: f.stranger}
	var order [2][2]string
	for i := range order {
		for j, principal := range []domain.Principal{owner, other} {
			req := ownedRequest(principal, "", fmt.Sprintf("concurrent inventory %d %d", i, j))
			req.SupportsScopedVectors = true
			// The initial fixture seeds create permission only in the first tenant.
			if j == 1 && i == 0 {
				grantExec(t, c, `INSERT INTO grant_catalog_create_permissions VALUES ($1,$2,'',true)`, other.TenantID, other.ID)
			}
			res, err := c.AcceptDocument(t.Context(), req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, res.Outcome)
			order[i][j] = string(req.Document.ID)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	begin := func(t *testing.T, opts pgx.TxOptions) pgx.Tx {
		t.Helper()
		tx, err := c.Pool.BeginTx(ctx, opts)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(ctx)) })
		return tx
	}
	hide := func(tx pgx.Tx, id string) {
		t.Helper()
		_, err := tx.Exec(ctx, `UPDATE documents SET status='deleting' WHERE id=$1`, id)
		require.NoError(t, err)
	}
	read := begin(t, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	old, err := readScopedPolicyState(ctx, read, owner)
	require.NoError(t, err)
	otherOld := revisionFor(t, c, other.TenantID, other.ID)
	aborted := begin(t, pgx.TxOptions{})
	hide(aborted, order[0][0])
	require.NoError(t, aborted.Rollback(ctx))
	require.Equal(t, old.Revision, revisionFor(t, c, owner.TenantID, owner.ID))

	first, second := begin(t, pgx.TxOptions{}), begin(t, pgx.TxOptions{})
	hide(first, order[0][0])
	hide(first, order[0][1])
	hide(second, order[1][1])
	hide(second, order[1][0])
	require.Equal(t, old.Revision, revisionFor(t, c, owner.TenantID, owner.ID))
	results := make(chan error, 2)
	go func() { results <- first.Commit(ctx) }()
	go func() { results <- second.Commit(ctx) }()
	firstErr, secondErr := <-results, <-results
	require.NoError(t, firstErr)
	require.NoError(t, secondErr)
	stillOld, err := readScopedPolicyState(ctx, read, owner)
	require.NoError(t, err)
	require.Equal(t, old, stillOld)
	var visible int
	require.NoError(t, read.QueryRow(ctx, `SELECT count(*) FROM grant_catalog_list_documents($1,$2,NULL,10,'',false)`, owner.TenantID, owner.ID).Scan(&visible))
	require.Equal(t, 2, visible)
	require.NoError(t, read.Commit(ctx))
	page, err := c.ListScopedDocumentsPage(ctx, owner, "", 1000, "", false)
	require.NoError(t, err)
	require.Empty(t, page.Documents)
	require.Equal(t, old.Revision+2, page.Revision)
	require.Equal(t, otherOld+2, revisionFor(t, c, other.TenantID, other.ID))
	assertGrantCount(t, c, "inventory_revision_pending", 0)

	t.Run("publish_before_hide", func(t *testing.T) {
		candidates, candidateErr := c.BackfillCandidates(ctx, "", 10)
		require.NoError(t, candidateErr)
		candidateIndex := slices.IndexFunc(candidates, func(candidate BackfillDocument) bool {
			return candidate.DocumentID == f.document
		})
		require.NotEqual(t, -1, candidateIndex, "missing quarantined inventory document")
		candidate := candidates[candidateIndex]
		writer, hider := begin(t, pgx.TxOptions{}), begin(t, pgx.TxOptions{})
		var published bool
		require.NoError(t, writer.QueryRow(ctx, `SELECT grant_catalog_publish_verified_document($1,$2,$3,$4)`,
			candidate.DocumentID, candidate.Incarnation, candidate.SourceVersion, candidate.ChunkDigest).Scan(&published))
		require.True(t, published)
		var pid int32
		require.NoError(t, hider.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
		updated := make(chan error, 1)
		finished := make(chan struct{})
		defer func() {
			_ = writer.Rollback(context.WithoutCancel(ctx))
			<-finished
		}()
		go func() {
			defer close(finished)
			_, hideErr := hider.Exec(ctx, `UPDATE documents SET status='deleting' WHERE id=$1`, candidate.DocumentID)
			updated <- hideErr
		}()
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			var blocked bool
			require.NoError(ct, c.Pool.QueryRow(ctx, `SELECT cardinality(pg_blocking_pids($1)) > 0`, pid).Scan(&blocked))
			require.True(ct, blocked)
		}, 3*time.Second, 10*time.Millisecond)
		require.NoError(t, writer.Commit(ctx))
		require.NoError(t, <-updated)
		visible, readErr := c.GetScopedDocumentSummary(ctx, owner, domain.DocumentID(candidate.DocumentID), false)
		require.NoError(t, readErr)
		require.NotNil(t, visible.Document)
		require.NoError(t, hider.Commit(ctx))
		hidden, readErr := c.GetScopedDocumentSummary(ctx, owner, domain.DocumentID(candidate.DocumentID), false)
		require.NoError(t, readErr)
		require.Nil(t, hidden.Document)
		require.Equal(t, visible.Revision+1, hidden.Revision)
	})

	t.Run("largest_import_and_point_lookup", func(t *testing.T) {
		batch := testGrantBatch(1)
		people := make([]grantEntry, maxGrantBatchEntries)
		for i := range people {
			people[i] = grantPrincipal(f.tenant, uuid.NewString(), fmt.Sprintf("inventory-bulk-%d", i), true)
		}
		batch["principals"] = people
		res, importErr := importTestBatch(t.Context(), c, batch, false)
		require.NoError(t, importErr)
		require.EqualValues(t, 2, res.Revision)
		assertGrantCount(t, c, "inventory_revision_pending", 0)
		require.EqualValues(t, 1, revisionFor(t, c, f.tenant, people[len(people)-1]["principal_id"].(string)))
		grantExec(t, c, `INSERT INTO grant_catalog_inventory_revisions SELECT $1,gen_random_uuid(),1 FROM generate_series(1,20000)`, f.tenant)
		grantExec(t, c, `ANALYZE grant_catalog_inventory_revisions`)
		var definition string
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT pg_get_functiondef('grant_catalog_inventory_policy_state(uuid,uuid)'::regprocedure)`).Scan(&definition))
		_, body, ok := strings.Cut(definition, "BEGIN ATOMIC")
		require.True(t, ok)
		statement := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(body), "END"))
		statement = strings.NewReplacer(
			"grant_catalog_inventory_policy_state.p_tenant", "$1", "p_tenant", "$1",
			"grant_catalog_inventory_policy_state.p_principal", "$2", "p_principal", "$2",
		).Replace(statement)
		tx, txErr := c.Pool.Begin(t.Context())
		require.NoError(t, txErr)
		defer func() { _ = tx.Rollback(context.Background()) }()
		_, planErr := tx.Exec(t.Context(), "PREPARE inventory_lookup(uuid,uuid) AS "+statement)
		require.NoError(t, planErr)
		rows, planErr := tx.Query(t.Context(), `EXPLAIN (COSTS OFF) EXECUTE inventory_lookup($1,$2)`,
			pgx.QueryExecModeSimpleProtocol, f.tenant, f.owner)
		require.NoError(t, planErr)
		lines, planErr := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, planErr)
		plan := strings.Join(lines, "\n")
		require.Contains(t, plan, "Index Scan using grant_catalog_inventory_revisions_pkey")
		require.NotContains(t, plan, "Seq Scan on grant_catalog_inventory_revisions")
		require.NotContains(t, plan, "document_ownership")
		_, planErr = tx.Exec(t.Context(), "DEALLOCATE inventory_lookup")
		require.NoError(t, planErr)
	})

	t.Run("overflow_rolls_back_policy_and_audit", func(t *testing.T) {
		const maximum = int64(1<<63 - 1)
		grantExec(t, c, `UPDATE grant_catalog_inventory_revisions SET revision=$3 WHERE tenant_id=$1 AND principal_id=$2`,
			owner.TenantID, owner.ID, maximum)
		batch := testGrantBatch(2)
		batch["principals"] = []grantEntry{grantPrincipal(f.tenant, f.owner, "Owner", false)}
		_, importErr := importTestBatch(t.Context(), c, batch, false)
		require.ErrorContains(t, importErr, "SQLSTATE 22003")
		state, readErr := c.ScopedPolicyState(t.Context(), owner)
		require.NoError(t, readErr)
		require.True(t, state.Active)
		require.Equal(t, maximum, state.Revision)
		require.EqualValues(t, 2, state.CatalogRevision)
		assertGrantCount(t, c, "inventory_revision_pending", 0)
		assertGrantCount(t, c, "policy_audit", 2)
	})
}

func TestInventoryRevisionUpgradeAndGrants(t *testing.T) {
	databaseURL := startTestDatabase(t)
	admin := migrationTestClient(t, databaseURL)
	roles := newGrantRoles(t, databaseURL, "inventory_owner", "inventory_service", "inventory_operator", "inventory_inherited")
	grantExec(t, admin, roles.sql(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
		CREATE ROLE {inventory_owner}; CREATE ROLE {inventory_service}; CREATE ROLE {inventory_operator};
		CREATE ROLE {inventory_inherited}; GRANT {inventory_inherited} TO {inventory_service}, {inventory_operator};
		CREATE SCHEMA inventory_corpus AUTHORIZATION {inventory_owner};
		GRANT USAGE ON SCHEMA inventory_corpus TO {inventory_service}, {inventory_operator}`))
	owner := grantRoleClient(t, databaseURL, roles.sql("SET SESSION AUTHORIZATION {inventory_owner}; SET search_path TO inventory_corpus"))
	applyMigrationPrefix(t, owner, 36)
	f := inventoryFixture(t, owner)
	grantExec(t, owner, roles.sql(`GRANT EXECUTE ON FUNCTION grant_catalog_policy_state(uuid,uuid) TO {inventory_service};
		ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO {inventory_inherited};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {inventory_inherited}`))
	require.NoError(t, owner.RunMigrations(t.Context()))
	grantExec(t, admin, `ALTER SCHEMA inventory_corpus RENAME TO "Inventory Ledger"`)
	connect := func(role string) *Client {
		t.Helper()
		return grantRoleClient(t, databaseURL, roles.sql("SET SESSION AUTHORIZATION {"+role+"}; SET search_path TO \"Inventory Ledger\""))
	}
	renamed, service, operator := connect("inventory_owner"), connect("inventory_service"), connect("inventory_operator")
	ctx := t.Context()
	principal := domain.Principal{TenantID: f.tenant, ID: f.owner}
	var active bool
	var legacy int64
	legacyRead := func(expected int64) {
		t.Helper()
		require.NoError(t, service.Pool.QueryRow(ctx, `SELECT * FROM grant_catalog_policy_state($1,$2)`,
			principal.TenantID, principal.ID).Scan(&active, &legacy))
		require.True(t, active)
		require.Equal(t, expected, legacy)
	}
	legacyRead(1)
	_, err := service.ScopedPolicyState(ctx, principal)
	require.Error(t, err)
	functions := []string{
		"grant_catalog_enqueue_inventory_revision(uuid,uuid)",
		"grant_catalog_capture_inventory_principal()", "grant_catalog_capture_inventory_create()",
		"grant_catalog_capture_inventory_grant()", "grant_catalog_capture_inventory_ownership()",
		"grant_catalog_capture_inventory_document()", "grant_catalog_flush_inventory_revisions()",
		"grant_catalog_inventory_policy_state(uuid,uuid)",
	}
	for _, role := range []string{"public", roles.name("inventory_service"), roles.name("inventory_operator")} {
		for _, fn := range functions {
			var allowed bool
			require.NoError(t, renamed.Pool.QueryRow(ctx, `SELECT has_function_privilege($1,$2,'execute')`, role, fn).Scan(&allowed))
			require.False(t, allowed, "%s via %s", fn, role)
		}
		for _, table := range []string{"grant_catalog_inventory_revisions", "grant_catalog_inventory_revision_pending"} {
			for _, privilege := range []string{"select", "insert", "update", "delete", "truncate", "references", "trigger"} {
				var allowed bool
				require.NoError(t, renamed.Pool.QueryRow(ctx, `SELECT has_table_privilege($1,$2,$3)`, role, table, privilege).Scan(&allowed))
				require.False(t, allowed, "%s on %s via %s", privilege, table, role)
			}
		}
	}
	grantExec(t, renamed, roles.sql(`GRANT EXECUTE ON FUNCTION grant_catalog_inventory_policy_state(uuid,uuid) TO {inventory_service};
		GRANT EXECUTE ON FUNCTION grant_catalog_import_batch(text,boolean) TO {inventory_operator}`))
	check := func(p domain.Principal, active bool, scoped, catalog int64) {
		t.Helper()
		state, readErr := service.ScopedPolicyState(ctx, p)
		require.NoError(t, readErr)
		require.Equal(t, domain.ScopedPolicyState{Active: active, Revision: scoped, CatalogRevision: catalog}, state)
	}
	check(principal, true, 0, 1)
	check(domain.Principal{TenantID: f.otherTenant, ID: principal.ID}, false, 0, 1)
	check(domain.Principal{TenantID: f.tenant, ID: uuid.NewString()}, false, 0, 1)
	check(domain.Principal{TenantID: f.tenant, ID: f.disabled}, false, 0, 1)
	_, err = operator.ScopedPolicyState(ctx, principal)
	require.Error(t, err)
	unrelated := testGrantBatch(1)
	unrelated["principals"] = []grantEntry{grantPrincipal(f.otherTenant, f.stranger, "other", false)}
	_, err = importTestBatch(ctx, operator, unrelated, false)
	require.NoError(t, err)
	check(principal, true, 0, 2)
	legacyRead(2)
	reuse := domain.Principal{TenantID: f.tenant, ID: uuid.NewString()}
	grantExec(t, renamed, `INSERT INTO grant_catalog_principals VALUES($1,$2,'issuer','reuse',true)`, reuse.ID, reuse.TenantID)
	grantExec(t, renamed, `DELETE FROM grant_catalog_principals WHERE principal_id=$1`, reuse.ID)
	check(reuse, false, 2, 2)
	grantExec(t, renamed, `INSERT INTO grant_catalog_principals VALUES($1,$2,'issuer','reuse',true)`, reuse.ID, reuse.TenantID)
	check(reuse, true, 3, 2)
	assertGrantCount(t, renamed, "inventory_revision_pending", 0)
}
