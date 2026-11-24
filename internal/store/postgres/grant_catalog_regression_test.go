package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/textutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/unicode/norm"
)

func testGrantCatalogSchemaIsolation(t *testing.T, databaseURL string) {
	ctx := t.Context()
	admin := migrationTestClient(t, databaseURL)
	require.NoError(t, admin.RunMigrations(ctx))
	grantExec(t, admin, `CREATE ROLE corpus_first_owner; CREATE ROLE corpus_second_owner;
		CREATE ROLE corpus_fresh_owner; CREATE ROLE corpus_serving;
		CREATE SCHEMA corpus_first AUTHORIZATION corpus_first_owner;
		CREATE SCHEMA corpus_second AUTHORIZATION corpus_second_owner;
		CREATE SCHEMA corpus_fresh AUTHORIZATION corpus_fresh_owner;
		GRANT SELECT ON public.schema_migrations, public.documents TO corpus_fresh_owner`)
	first := grantRoleClient(t, databaseURL, `SET SESSION AUTHORIZATION corpus_first_owner; SET search_path TO corpus_first, public`)
	second := grantRoleClient(t, databaseURL, `SET SESSION AUTHORIZATION corpus_second_owner; SET search_path TO corpus_second, public`)
	fresh := grantRoleClient(t, databaseURL, `SET SESSION AUTHORIZATION corpus_fresh_owner; SET search_path TO corpus_fresh, public`)
	require.NoError(t, fresh.RunMigrations(ctx))
	var ownHistory, ownDocuments bool
	require.NoError(t, admin.Pool.QueryRow(ctx, `SELECT to_regclass('corpus_fresh.schema_migrations') IS NOT NULL,
		to_regclass('corpus_fresh.documents') IS NOT NULL`).Scan(&ownHistory, &ownDocuments))
	require.True(t, ownHistory && ownDocuments)
	requireRecordedMigrations(t, fresh)

	id := uuid.NewString()
	for _, fixture := range []struct {
		client *Client
		text   string
	}{
		{first, "first corpus"},
		{second, "second corpus"},
	} {
		applyMigrationPrefix(t, fixture.client, 10)
		grantExec(t, fixture.client, `INSERT INTO documents (id,content) VALUES ($1,$2)`, id, fixture.text)
		require.NoError(t, fixture.client.RunMigrations(ctx))
		requireRecordedMigrations(t, fixture.client)
	}
	require.NotEqual(t, grantSource(t, first, id), grantSource(t, second, id))
	for _, fixture := range []struct {
		client *Client
		other  *Client
	}{
		{first, second},
		{second, first},
	} {
		tenant, principal := uuid.NewString(), uuid.NewString()
		b := testGrantBatch(0)
		b["tenants"] = []grantEntry{{"tenant_id": tenant}}
		b["principals"] = []grantEntry{grantPrincipal(tenant, principal, "corpus owner", true)}
		b["collections"] = []grantEntry{{"tenant_id": tenant, "collection": ""}}
		b["assignments"] = []grantEntry{grantAssignment(t, fixture.other, tenant, principal, id)}
		_, err := importTestBatch(ctx, fixture.client, b, false)
		require.ErrorContains(t, err, "invalid or conflicting assignments entry")
		b["assignments"] = []grantEntry{grantAssignment(t, fixture.client, tenant, principal, id)}
		_, err = importTestBatch(ctx, fixture.client, b, false)
		require.NoError(t, err)
	}
	grantExec(t, admin, `GRANT USAGE ON SCHEMA corpus_second TO corpus_serving;
		GRANT SELECT ON corpus_second.schema_migrations, corpus_second.documents TO corpus_serving`)
	serving := grantRoleClient(t, databaseURL, `SET SESSION AUTHORIZATION corpus_serving; SET search_path TO corpus_second, public`)
	require.NoError(t, serving.RunMigrations(ctx))
	var content string
	require.NoError(t, serving.Pool.QueryRow(ctx, `SELECT content FROM documents WHERE id=$1`, id).Scan(&content))
	require.Equal(t, "second corpus", content)
}

func testGrantCatalogRawInputAndLabels(t *testing.T, databaseURL string) {
	c := migrationTestClient(t, databaseURL)
	ctx := t.Context()
	require.NoError(t, c.RunMigrations(ctx))
	b := testGrantBatch(0)
	tenant, principal := uuid.NewString(), uuid.NewString()
	b["tenants"] = []grantEntry{{"tenant_id": tenant}}
	b["principals"] = []grantEntry{grantPrincipal(tenant, principal, "label owner", true)}
	b["collections"] = []grantEntry{{"tenant_id": tenant, "collection": ""}}
	b["create_permissions"] = []grantEntry{{"tenant_id": tenant, "principal_id": principal, "collection": "", "can_create": true}}
	payload, err := json.Marshal(b)
	require.NoError(t, err)
	var unicodeVersion string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT grant_catalog_unicode_data()->>'version'`).Scan(&unicodeVersion))
	require.Equal(t, norm.Version, unicodeVersion)
	applyRaw := func(raw string) error {
		_, importErr := c.Pool.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1::text,true)`, raw)
		return importErr
	}
	require.NoError(t, applyRaw(string(payload)))
	oversized := strings.Repeat(" ", maxGrantBatchBytes) + string(payload)
	for _, tc := range []struct{ body, message string }{
		{strings.Replace(string(payload), `"expected_revision":0`, `"expected_revision":1,"expected_revision":0`, 1), "duplicate batch field"},
		{strings.Replace(string(payload), `"expected_revision":0`, `"expected_revision":0e0`, 1), "invalid batch header"},
		{strings.Replace(string(payload), `"tenant_id":`, `"tenant\u005fid":null,"tenant_id":`, 1), "duplicate entry field"},
		{oversized, "batch exceeds 262144 bytes"},
	} {
		require.ErrorContains(t, applyRaw(tc.body), tc.message)
		_, cliErr := c.ImportGrantBatch(ctx, strings.NewReader(tc.body), true)
		require.Error(t, cliErr)
	}
	grantExec(t, c, `CREATE TABLE stored_grant_payloads (payload TEXT)`)
	grantExec(t, c, `INSERT INTO stored_grant_payloads VALUES ($1)`, oversized)
	var storedBytes int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT pg_column_size(payload) FROM stored_grant_payloads`).Scan(&storedBytes))
	require.Less(t, storedBytes, maxGrantBatchBytes)
	_, err = c.Pool.Exec(ctx, `SELECT * FROM stored_grant_payloads p CROSS JOIN LATERAL grant_catalog_import_batch(p.payload,true)`)
	require.ErrorContains(t, err, "batch exceeds 262144 bytes")
	grantExec(t, c, `UPDATE stored_grant_payloads SET payload=$1`, string(payload))
	grantExec(t, c, `SELECT * FROM stored_grant_payloads p CROSS JOIN LATERAL grant_catalog_import_batch(p.payload,true)`)

	newUnicodeComposition := "\U000105D2\u0307"
	longNonstarterRun := "x" + strings.Repeat("\u0300", 31)
	labels := []string{"", "Café", " padded", "Cafe\u0301", "inside\nlabel", "format\u200dlabel", "space\u00a0",
		"plain", newUnicodeComposition, longNonstarterRun,
		"a\u0315\u0300", "\u1100\u1161\u11A8", "\uFB1D", "x" + strings.Repeat("\u0300", 30),
		"\u00E0" + strings.Repeat("\u0300", 30), "x" + strings.Repeat("\uFF9E", 31)}
	for _, original := range []string{newUnicodeComposition, longNonstarterRun} {
		canonical, normalizeErr := textutil.NormalizeLabel(original)
		require.NoError(t, normalizeErr)
		require.NotEqual(t, original, canonical)
		labels = append(labels, canonical)
	}
	for _, label := range labels {
		t.Run(label, func(t *testing.T) {
			b["collections"] = []grantEntry{{"tenant_id": tenant, "collection": label}}
			// The collection entry itself can describe an exact historical key.
			delete(b, "create_permissions")
			delete(b, "reservations")
			body, marshalErr := json.Marshal(b)
			require.NoError(t, marshalErr)
			require.NoError(t, applyRaw(string(body)))
			for _, group := range []string{"create_permissions", "reservations"} {
				entry := grantEntry{"tenant_id": tenant, "principal_id": principal, "collection": label}
				if group == "create_permissions" {
					entry["can_create"] = true
				} else {
					entry["document_id"] = uuid.NewString()
				}
				b[group] = []grantEntry{entry}
				body, marshalErr = json.Marshal(b)
				require.NoError(t, marshalErr)
				_, cliErr := c.ImportGrantBatch(ctx, bytes.NewReader(body), true)
				canonical, normalizeErr := textutil.NormalizeLabel(label)
				if normalizeErr == nil && canonical == label {
					require.NoError(t, cliErr)
					require.NoError(t, applyRaw(string(body)))
				} else {
					require.ErrorContains(t, cliErr, "collection")
					require.ErrorContains(t, applyRaw(string(body)), "collection")
				}
				delete(b, group)
			}
		})
	}
	assertGrantRevision(t, c, 0)
	assertGrantCount(t, c, "policy_audit", 0)
	var revision int64
	var replayed bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT resulting_revision,replayed FROM stored_grant_payloads p
		CROSS JOIN LATERAL grant_catalog_import_batch(p.payload,false)`).Scan(&revision, &replayed))
	require.EqualValues(t, 1, revision)
	require.False(t, replayed)
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT resulting_revision,replayed FROM grant_catalog_import_batch($1,false)`,
		string(payload)).Scan(&revision, &replayed))
	require.EqualValues(t, 1, revision)
	require.True(t, replayed)
}

func testGrantCatalogPendingRefreshAndLockedReplay(t *testing.T, databaseURL string) {
	c := migrationTestClient(t, databaseURL)
	ctx := t.Context()
	require.NoError(t, c.RunMigrations(ctx))
	f := newGrantFixture()
	insertGrantSource(t, c, f.document)
	insertGrantSource(t, c, f.otherDocument)
	initial := f.initialBatch(t, c, false)
	initial["collections"] = append(initial["collections"].([]grantEntry), grantEntry{"tenant_id": f.tenant, "collection": "group"})
	_, err := importTestBatch(ctx, c, initial, false)
	require.NoError(t, err)
	grantExec(t, c, `UPDATE documents SET collection='group' WHERE id=$1`, f.document)
	fresh := grantAssignment(t, c, f.tenant, f.owner, f.document)
	fresh["collection"] = "group"
	refresh := testGrantBatch(1)
	refresh["assignments"] = []grantEntry{fresh}
	_, err = importTestBatch(ctx, c, refresh, false)
	require.NoError(t, err)
	var collection, source string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT collection,source_version FROM grant_catalog_document_ownership WHERE document_id=$1`, f.document).Scan(&collection, &source))
	require.Equal(t, "group", collection)
	require.Equal(t, fresh["source_version"], source)
	grantExec(t, c, `UPDATE grant_catalog_document_ownership SET ready=true WHERE document_id=$1`, f.document)
	assertDocumentAccess(t, c, f.document, f.owner, true, true, true)

	lock, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(context.WithoutCancel(ctx)) }()
	_, err = lock.Exec(ctx, `UPDATE documents SET content='later update' WHERE id=$1`, f.document)
	require.NoError(t, err)
	for _, dryRun := range []bool{false, true} {
		replayCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		result, replayErr := importTestBatch(replayCtx, c, initial, dryRun)
		cancel()
		require.NoError(t, replayErr)
		require.True(t, result.Replayed)
		require.EqualValues(t, 1, result.Revision)
	}
	assertGrantRevision(t, c, 2)
	require.NoError(t, lock.Rollback(ctx))

	// An exact stored label remains assignable even when new uploads normalize it.
	legacyID, legacyLabel := uuid.NewString(), " Cafe\u0301 "
	grantExec(t, c, `INSERT INTO documents(id,content,collection) VALUES($1,'historical text',$2)`, legacyID, legacyLabel)
	legacy := testGrantBatch(2)
	legacy["collections"] = []grantEntry{{"tenant_id": f.tenant, "collection": legacyLabel}}
	assignment := grantAssignment(t, c, f.tenant, f.owner, legacyID)
	assignment["collection"] = legacyLabel
	legacy["assignments"] = []grantEntry{assignment}
	_, err = importTestBatch(ctx, c, legacy, false)
	require.NoError(t, err)
	grantExec(t, c, `UPDATE grant_catalog_document_ownership SET ready=true WHERE document_id=$1`, legacyID)
	assertDocumentAccess(t, c, legacyID, f.owner, true, true, true)
}

func TestGrantCatalogRestrictedVectorStartup(t *testing.T) {
	owner, _ := vectorDatabase(t)
	ctx := t.Context()
	grantExec(t, owner, `CREATE ROLE catalog_vector_service; GRANT SELECT ON schema_migrations TO catalog_vector_service;
		GRANT SELECT, INSERT, UPDATE, DELETE ON documents, ingest_jobs, document_fingerprints, chunks, chunk_vectors TO catalog_vector_service`)
	service := grantRoleClient(t, owner.Pool.Config().ConnString(), "SET SESSION AUTHORIZATION catalog_vector_service")
	require.NoError(t, service.RunMigrations(ctx))
	store, err := NewVectorStore(ctx, service, 3)
	require.NoError(t, err)
	require.NoError(t, store.Ping(ctx))
	_, err = NewVectorStore(ctx, service, 4)
	require.ErrorContains(t, err, "embedding dimension mismatch")
	grantExec(t, owner, `DROP INDEX idx_chunk_vectors_collection`)
	_, err = NewVectorStore(ctx, service, 3)
	require.Error(t, err)
	_, err = NewVectorStore(ctx, owner, 3)
	require.NoError(t, err)
	_, err = NewVectorStore(ctx, service, 3)
	require.NoError(t, err)
}
