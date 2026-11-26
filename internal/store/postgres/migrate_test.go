package postgres

import (
	"context"
	"crypto/sha256"
	"embed"
	"fmt"
	neturl "net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedMigrationsIncludeIndexVisibilityAndMixedPDF(t *testing.T) {
	migrations, err := loadMigrations(migrationFiles)
	require.NoError(t, err)
	names := make(map[int]string, len(migrations))
	for _, migration := range migrations {
		names[migration.version] = migration.name
	}
	require.Equal(t, "012_index_visibility.sql", names[12])
	require.Equal(t, "015_mixed_pdf_ocr.sql", names[15])
}

func TestVectorGenerationMigrationSharesPublishedInventory(t *testing.T) {
	migrations, err := loadMigrations(migrationFiles)
	require.NoError(t, err)
	names := make(map[int]string, len(migrations))
	for _, m := range migrations {
		names[m.version] = m.name
	}
	require.Equal(t, "012_index_visibility.sql", names[12])
	require.Equal(t, "016_vector_generations.sql", names[16])
}

func TestVectorGenerationMigrationUpgradesPublishedVisibilitySchema(t *testing.T) {
	c := migrationTestClient(t, startTestDatabase(t))
	applyMigrationPrefix(t, c, 12)
	ctx := t.Context()
	id := uuid.NewString()
	leaseToken := uuid.NewString()
	observedAt := time.Now().UTC().Truncate(time.Second)
	_, err := c.Pool.Exec(ctx, `INSERT INTO documents(id,content,index_visibility,index_visibility_observed_at)
		VALUES($1,'preserved source','sample_seen',$2)`, id, observedAt)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `INSERT INTO ingest_jobs(document_id,lease_token) VALUES($1,$2)`, id, leaseToken)
	require.NoError(t, err)

	var checksum string
	var appliedAt time.Time
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT checksum,applied_at FROM schema_migrations WHERE version=12`).Scan(&checksum, &appliedAt))
	require.NoError(t, c.RunMigrations(ctx))
	requireRecordedMigrations(t, c)
	var preserved bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT checksum=$1 AND applied_at=$2 FROM schema_migrations WHERE version=12`, checksum, appliedAt).Scan(&preserved))
	require.True(t, preserved)
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT d.index_visibility='sample_seen' AND d.index_visibility_observed_at=$2 AND j.lease_token=$3
		FROM documents d JOIN ingest_jobs j ON j.document_id=d.id WHERE d.id=$1`, id, observedAt, leaseToken).Scan(&preserved))
	require.True(t, preserved)
	state, err := c.VectorState(ctx)
	require.NoError(t, err)
	require.Equal(t, LegacyGeneration, state.Active)
	require.Empty(t, state.LegacyStore)
	require.False(t, state.Frozen)
}

func TestDeletionMigrationPreservesLiveStorageAndLease(t *testing.T) {
	url := startTestDatabase(t)
	c := migrationTestClient(t, url)
	applyMigrationPrefix(t, c, 9)
	ctx := t.Context()
	req := jobsRequest("preserved source content")
	key := "objects/preserved.txt"
	req.Document.S3Key = &key
	id := req.Document.ID
	// Seed the SQL shape used by an executable before the additive migrations.
	_, err := c.Pool.Exec(ctx, `INSERT INTO documents(id,content,s3_key,fingerprint) VALUES($1,$2,$3,$4)`,
		id, req.Document.Content, key, req.Fingerprint)
	require.NoError(t, err)
	require.NoError(t, c.StoreFingerprint(ctx, req.Fingerprint, string(id)))
	_, err = c.Pool.Exec(ctx, `INSERT INTO ingest_jobs(document_id,state,attempts,lease_owner,lease_until)
		VALUES($1,'running',1,'schema-upgrade',clock_timestamp()+interval '1 minute')`, id)
	require.NoError(t, err)
	chunkID := uuid.NewString()
	_, err = c.Pool.Exec(ctx, `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,'preserved chunk',2)`, chunkID, id)
	require.NoError(t, err)

	type snapshot struct {
		filenode uint32
		rows     string
	}
	read := func() snapshot {
		var s snapshot
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT pg_relation_filenode('documents'), jsonb_build_array(
			to_jsonb(d)-ARRAY['incarnation','grant_catalog_assigned','index_visibility','index_visibility_observed_at','scoped_vectors','vector_attempt','vector_incarnation_required',
				'vector_mutation_deletes','vector_delete_uncertain','vector_mutation_pending','vector_mutation_uncertain',
				'vector_mutation_claim','vector_mutation_until'], to_jsonb(ch), to_jsonb(f), to_jsonb(j)-ARRAY['lease_token','pdf_required','pdf_claim_attempt'])::text
			FROM documents d JOIN chunks ch ON ch.document_id=d.id
			JOIN document_fingerprints f ON f.document_id=d.id JOIN ingest_jobs j ON j.document_id=d.id
			WHERE d.id=$1 AND j.state='running' AND j.lease_until > clock_timestamp()`, id).Scan(&s.filenode, &s.rows))
		return s
	}
	before := read()
	require.NoError(t, c.RunMigrations(ctx))
	require.Equal(t, before, read())
	requireRecordedMigrations(t, c)
	requireInactiveDeletionSchema(t, c)

	restarted := migrationTestClient(t, url)
	require.NoError(t, restarted.RunMigrations(ctx))
	var stored *string
	require.NoError(t, restarted.Pool.QueryRow(ctx, `SELECT incarnation::text FROM documents WHERE id=$1`, id).Scan(&stored))
	require.Nil(t, stored)
	requireHistoricalIncarnation(t, restarted.WithDocumentIncarnations(), string(id))
	summary, err := restarted.GetDocumentSummary(ctx, id)
	require.NoError(t, err)
	require.Equal(t, domain.VisibilityUnknown, summary.IndexVisibility)
	require.Nil(t, summary.IndexVisibilityObservedAt)

	// The old worker's heartbeat still works with the vector mutation migration in place.
	tag, err := restarted.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=now()+interval '1 minute'
		WHERE document_id=$1 AND lease_owner='schema-upgrade' AND attempts=1 AND state='running' AND lease_until>now()`, id)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
	// The upgraded worker can reclaim the original job after its old lease expires.
	_, err = restarted.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp() WHERE document_id=$1`, id)
	require.NoError(t, err)
	job, err := restarted.ClaimJob(ctx, "schema-upgrade", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, string(id), job.Incarnation)
	require.False(t, job.ScopedVectors)
	require.Equal(t, 2, job.Attempts)

	completed, err := restarted.CompleteJob(ctx, *job, []domain.Chunk{{ID: domain.ChunkID(chunkID), Text: "preserved chunk", TokenCount: 2}}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, completed)
	effective := string(id)
	for range 2 {
		require.NoError(t, restarted.DeleteDocument(ctx, id))
		result, err := restarted.AcceptDocument(ctx, req)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, result.Outcome)
		var next string
		require.NoError(t, restarted.Pool.QueryRow(ctx, `SELECT incarnation::text FROM documents WHERE id=$1`, id).Scan(&next))
		_, err = uuid.Parse(next)
		require.NoError(t, err)
		require.NotEqual(t, effective, next)
		require.NotEqual(t, string(id), next)
		effective = next
	}
	requireInactiveDeletionSchema(t, restarted)
}

func requireHistoricalIncarnation(t *testing.T, lister domain.DocumentLister, id string) {
	t.Helper()
	t.Run("status", func(t *testing.T) {
		doc, err := lister.GetDocumentSummary(t.Context(), domain.DocumentID(id))
		require.NoError(t, err)
		require.NotNil(t, doc)
		require.Equal(t, id, doc.Incarnation)
	})
	t.Run("stream", func(t *testing.T) {
		var incarnations []string
		require.NoError(t, lister.StreamDocuments(t.Context(), func(doc domain.DocumentSummary) error {
			incarnations = append(incarnations, doc.Incarnation)
			return nil
		}))
		require.Equal(t, []string{id}, incarnations)
	})
	t.Run("page", func(t *testing.T) {
		docs, err := lister.ListDocumentsPage(t.Context(), "", 1)
		require.NoError(t, err)
		require.Len(t, docs, 1)
		require.Equal(t, id, docs[0].Incarnation)
	})
}

func TestDeletionMigrationRetriesAfterLockTimeout(t *testing.T) {
	c := migrationTestClient(t, startTestDatabase(t))
	applyMigrationPrefix(t, c, 9)
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	tx, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(ctx, `LOCK TABLE documents IN ACCESS SHARE MODE`)
	require.NoError(t, err)
	start := time.Now()
	err = c.RunMigrations(ctx)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "55P03", pgErr.Code)
	require.Less(t, time.Since(start), 20*time.Second)
	var columns, tables, history int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM information_schema.columns WHERE table_name='documents' AND column_name='incarnation'),
		(SELECT count(*) FROM information_schema.tables WHERE table_name IN ('document_deletions','document_deletion_activation')),
		(SELECT count(*) FROM schema_migrations)`).Scan(&columns, &tables, &history))
	require.Zero(t, columns)
	require.Zero(t, tables)
	require.Equal(t, 9, history)
	require.NoError(t, tx.Rollback(ctx))
	require.NoError(t, c.RunMigrations(ctx))
	requireRecordedMigrations(t, c)
	requireInactiveDeletionSchema(t, c)
}

func TestDeletionReceiptStorageIsIndependentOfDocument(t *testing.T) {
	c := migrationTestClient(t, startTestDatabase(t))
	ctx := t.Context()
	require.NoError(t, c.RunMigrations(ctx))
	id, incarnation, owner := uuid.NewString(), uuid.NewString(), uuid.NewString()
	_, err := c.Pool.Exec(ctx, `INSERT INTO documents(id,content) VALUES($1,'source content')`, id)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,source_key,source_bucket,vector_mode,memory_owner,step,held,claim,claim_until)
		VALUES($1,$2,'legacy_reservation','original.txt','stored-files','memory',$3,1,TRUE,$3,now()+interval '1 minute')`, id, incarnation, owner)
	require.NoError(t, err)
	require.NoError(t, c.DeleteDocument(ctx, domain.DocumentID(id)))
	var retained bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT incarnation=$2 AND kind='legacy_reservation' AND source_key='original.txt'
		AND source_bucket='stored-files' AND vector_mode='memory' AND memory_owner=$3 AND step=1
		AND held AND NOT finalized AND claim=$3 AND claim_until>now() FROM document_deletions WHERE document_id=$1`, id, incarnation, owner).Scan(&retained))
	require.True(t, retained)
	for _, kind := range []string{"qualified_intent", "quarantine"} {
		_, err = c.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,vector_index,vector_namespace)
			VALUES($1,$2,$3,'revision','original-index','original-namespace')`, uuid.NewString(), incarnation, kind)
		require.NoError(t, err)
	}
	_, err = c.Pool.Exec(ctx, `INSERT INTO document_deletion_activation(singleton) VALUES (FALSE)`)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23514", pgErr.Code)
}

func requireInactiveDeletionSchema(t *testing.T, c *Client) {
	t.Helper()
	var receipts, markers, active int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM document_deletions), count(*), count(activated_at) FROM document_deletion_activation`).Scan(&receipts, &markers, &active))
	require.Zero(t, receipts)
	require.Equal(t, 1, markers)
	require.Zero(t, active)
}

func TestDocumentCollectionsUpgradePreservesIdentity(t *testing.T) {
	client := migrationTestClient(t, startTestDatabase(t))
	applyMigrationPrefix(t, client, 6)
	const content = "Existing default collection content"
	const id = "00000000-0000-4000-8000-000000000005"
	fingerprint := chunker.Fingerprint(content)
	_, err := client.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status,fingerprint) VALUES($1,$2,'completed',$3)`, id, content, fingerprint)
	require.NoError(t, err)
	_, err = client.Pool.Exec(t.Context(), `INSERT INTO document_fingerprints(fingerprint,document_id) VALUES($1,$2)`, fingerprint, id)
	require.NoError(t, err)
	const legacyID = "00000000-0000-4000-8000-000000000006"
	_, err = client.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status) VALUES($1,'Legacy content without stored fingerprint','completed')`, legacyID)
	require.NoError(t, err)
	require.NoError(t, client.RunMigrations(t.Context()))
	requireRecordedMigrations(t, client)
	var collection, storedContent, storedFingerprint string
	require.NoError(t, client.Pool.QueryRow(t.Context(), `SELECT collection,content,fingerprint FROM documents WHERE id=$1`, id).Scan(&collection, &storedContent, &storedFingerprint))
	require.Empty(t, collection)
	require.Equal(t, content, storedContent)
	require.Equal(t, fingerprint, storedFingerprint)
	result, err := client.AcceptDocument(t.Context(), jobsRequest(content))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptDuplicate, result.Outcome)
	require.Equal(t, domain.DocumentID(id), result.DocumentID)
	req := jobsRequest("Legacy content without stored fingerprint")
	req.Document.ID = legacyID
	req.CallerSupplied = true
	result, err = client.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptDuplicate, result.Outcome)
	req.Collection = "archive"
	req.Fingerprint = chunker.ScopedFingerprint(req.Collection, req.Document.Content)
	result, err = client.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptConflict, result.Outcome)
	var index string
	require.NoError(t, client.Pool.QueryRow(t.Context(), `SELECT indexdef FROM pg_indexes WHERE tablename='documents' AND indexname='idx_documents_collection_id'`).Scan(&index))
	require.Contains(t, index, "(collection, id)")
	require.NoError(t, client.RunMigrations(t.Context()))
}

func applyMigrationPrefix(t *testing.T, client *Client, through int) {
	t.Helper()
	applyMigrationsWhere(t, client, func(version int) bool { return version <= through })
}

// applyMigrationsExcept applies and records every embedded migration except
// the listed versions, which reproduces a database migrated by a build that
// predates them even when their numbers sit below later migrations.
func applyMigrationsExcept(t *testing.T, client *Client, versions ...int) {
	t.Helper()
	applyMigrationsWhere(t, client, func(version int) bool { return !slices.Contains(versions, version) })
}

func applyMigrationsWhere(t *testing.T, client *Client, include func(version int) bool) {
	t.Helper()
	migrations, err := loadMigrations(testMigrationFiles)
	require.NoError(t, err)
	ctx := t.Context()
	_, err = client.Pool.Exec(ctx, `CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`)
	require.NoError(t, err)
	for _, m := range migrations {
		if !include(m.version) {
			continue
		}
		_, err = client.Pool.Exec(ctx, m.sql)
		require.NoError(t, err)
		_, err = client.Pool.Exec(ctx, `INSERT INTO schema_migrations(version,name,checksum) VALUES($1,$2,$3)`, m.version, m.name, m.checksum)
		require.NoError(t, err)
	}
}

//go:embed migrations/*.sql
var testMigrationFiles embed.FS

type migrationCounter struct {
	queries atomic.Int32
}

func (c *migrationCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(data.SQL, "CREATE EXTENSION") || strings.HasPrefix(data.SQL, "ALTER TABLE documents") || strings.HasPrefix(data.SQL, "DO $$") {
		c.queries.Add(1)
	}
	return ctx
}

func (*migrationCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func migrationTestClient(t *testing.T, url string) *Client {
	t.Helper()
	client, err := NewClient(t.Context(), url)
	require.NoError(t, err)
	t.Cleanup(client.Pool.Close)
	return client
}

func requireRecordedMigrations(t *testing.T, client *Client) {
	t.Helper()
	files, err := testMigrationFiles.ReadDir("migrations")
	require.NoError(t, err)
	var count int
	require.NoError(t, client.Pool.QueryRow(t.Context(), `SELECT count(*) FROM schema_migrations`).Scan(&count))
	require.Len(t, files, count)
	for _, file := range files {
		body, readErr := testMigrationFiles.ReadFile("migrations/" + file.Name())
		require.NoError(t, readErr)
		var name, checksum string
		var appliedAt time.Time
		require.NoError(t, client.Pool.QueryRow(t.Context(), `SELECT name, checksum, applied_at FROM schema_migrations WHERE version=$1`, file.Name()[:3]).Scan(&name, &checksum, &appliedAt))
		require.Equal(t, file.Name(), name)
		require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(body)), checksum)
		require.False(t, appliedAt.IsZero())
	}
}

func TestMigrationsAreAppliedOnce(t *testing.T) {
	url := startTestDatabase(t)
	counter := &migrationCounter{}
	config, err := pgxpool.ParseConfig(url)
	require.NoError(t, err)
	config.ConnConfig.Tracer = counter
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	client := &Client{Pool: pool}
	require.NoError(t, client.RunMigrations(t.Context()))
	first := counter.queries.Load()
	require.Positive(t, first)
	require.NoError(t, client.RunMigrations(t.Context()))
	require.Equal(t, first, counter.queries.Load(), "restart re-executed migration SQL")
	requireRecordedMigrations(t, client)
}

func TestConcurrentMigrationsSucceed(t *testing.T) {
	url := startTestDatabase(t)
	clients := make([]*Client, 6)
	for i := range clients {
		clients[i] = migrationTestClient(t, url)
	}
	start := make(chan struct{})
	errors := make(chan error, len(clients))
	var group sync.WaitGroup
	ctx := t.Context()
	for _, client := range clients {
		group.Go(func() {
			<-start
			errors <- client.RunMigrations(ctx)
		})
	}
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	requireRecordedMigrations(t, clients[0])
}

func TestLegacySchemaIsAdopted(t *testing.T) {
	client := migrationTestClient(t, startTestDatabase(t))
	files, err := testMigrationFiles.ReadDir("migrations")
	require.NoError(t, err)
	for _, file := range files {
		if file.Name()[:3] > "005" {
			continue
		}
		body, readErr := testMigrationFiles.ReadFile("migrations/" + file.Name())
		require.NoError(t, readErr)
		_, err = client.Pool.Exec(t.Context(), string(body))
		require.NoError(t, err)
	}
	var id string
	require.NoError(t, client.Pool.QueryRow(t.Context(), `INSERT INTO documents(content, metadata) VALUES ('preserved text', '{"doc_type":"reference"}') RETURNING id::text`).Scan(&id))
	require.NoError(t, client.RunMigrations(t.Context()))
	var content, docType string
	require.NoError(t, client.Pool.QueryRow(t.Context(), `SELECT content, doc_type FROM documents WHERE id=$1`, id).Scan(&content, &docType))
	require.Equal(t, "preserved text", content)
	require.Equal(t, "document", docType)
	requireRecordedMigrations(t, client)
}

func TestSteadyStateStartupTakesNoTableLock(t *testing.T) {
	client := migrationTestClient(t, startTestDatabase(t))
	require.NoError(t, client.RunMigrations(t.Context()))
	tx, err := client.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback(context.Background())) }()
	_, err = tx.Exec(t.Context(), `LOCK TABLE documents IN ACCESS SHARE MODE`)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, client.RunMigrations(ctx))
}

func TestModifiedMigrationIsRejected(t *testing.T) {
	client := migrationTestClient(t, startTestDatabase(t))
	require.NoError(t, client.RunMigrations(t.Context()))
	_, err := client.Pool.Exec(t.Context(), `UPDATE schema_migrations SET checksum='changed' WHERE version=1`)
	require.NoError(t, err)
	err = client.RunMigrations(t.Context())
	require.ErrorContains(t, err, "migration 001 was modified after it was applied")
}

const migrationWaiterOptions = "statement_timeout=20&lock_timeout=20"

func TestMigrationsWaitForAdvisoryLock(t *testing.T) {
	client := migrationTestClient(t, startTestDatabase(t))
	tx, err := client.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(t.Context(), `SELECT pg_advisory_xact_lock($1)`, int64(0x7175617272790001))
	require.NoError(t, err)
	waiter := migrationTestClient(t, withFixtureOptions(t, client.Pool.Config().ConnString(), migrationWaiterOptions))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- waiter.RunMigrations(ctx) }()
	select {
	case err = <-done:
		t.Fatalf("migration returned while advisory lock was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, tx.Rollback(t.Context()))
	require.NoError(t, <-done)
	conn, err := waiter.Pool.Acquire(t.Context())
	require.NoError(t, err)
	defer conn.Release()
	var statementTimeout, lockTimeout string
	require.NoError(t, conn.QueryRow(t.Context(), `SHOW statement_timeout`).Scan(&statementTimeout))
	require.NoError(t, conn.QueryRow(t.Context(), `SHOW lock_timeout`).Scan(&lockTimeout))
	require.Equal(t, "20ms", statementTimeout)
	require.Equal(t, "20ms", lockTimeout)
}

func withFixtureOptions(t testing.TB, address, options string) string {
	t.Helper()
	dsn, err := neturl.Parse(address)
	require.NoError(t, err)
	// pgx applies the last value for a key and reads a literal + as data.
	// Append the encoded options to preserve the original query bytes.
	if dsn.RawQuery != "" && !strings.HasSuffix(dsn.RawQuery, "&") {
		dsn.RawQuery += "&"
	}
	dsn.RawQuery += options
	return dsn.String()
}

func TestWithFixtureOptionsKeepsConnectionOptions(t *testing.T) {
	const address = "postgres://cluster_admin@127.0.0.1/owned?password=fixture%20secret&options=-c%20search_path%3Dpublic&statement_timeout=100&lock_timeout=100&client_encoding=LATIN1"
	before, err := pgx.ParseConfig(address)
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		options string
		want    map[string]string
	}{
		{"migration waiter", migrationWaiterOptions, map[string]string{"statement_timeout": "20", "lock_timeout": "20"}},
		{"LATIN1 client", utf8ClientOptions, map[string]string{"client_encoding": "UTF8"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			after, err := pgx.ParseConfig(withFixtureOptions(t, address, tc.options))
			require.NoError(t, err)
			require.Equal(t, before.Password, after.Password)
			require.Equal(t, before.RuntimeParams["options"], after.RuntimeParams["options"])
			for key, want := range tc.want {
				require.Equal(t, want, after.RuntimeParams[key], key)
			}
		})
	}
	t.Run("empty query", func(t *testing.T) {
		after, err := pgx.ParseConfig(withFixtureOptions(t, "postgres://cluster_admin@127.0.0.1/owned", utf8ClientOptions))
		require.NoError(t, err)
		require.Equal(t, "UTF8", after.RuntimeParams["client_encoding"])
	})
}

func TestFailedMigrationRollsBack(t *testing.T) {
	client := migrationTestClient(t, startTestDatabase(t))
	_, err := client.Pool.Exec(t.Context(), `CREATE TABLE documents (id UUID PRIMARY KEY)`)
	require.NoError(t, err)
	require.Error(t, client.RunMigrations(t.Context()))
	var tables int
	require.NoError(t, client.Pool.QueryRow(t.Context(), `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('chunks', 'schema_migrations', 'document_fingerprints')`).Scan(&tables))
	require.Zero(t, tables, "failed migration left partially applied schema")
}

func TestPoolDefaultsApplyWhenAbsent(t *testing.T) {
	for _, url := range []string{
		"postgres://localhost/rag",
		"host=localhost dbname=rag",
		"postgres://user:pool_max_conns@localhost/rag",
	} {
		t.Run(url, func(t *testing.T) {
			config, err := poolConfig(url)
			require.NoError(t, err)
			require.EqualValues(t, 16, config.MaxConns)
			require.Equal(t, "30000", config.ConnConfig.RuntimeParams["statement_timeout"])
		})
	}
}

func TestURLSettingsOverrideDefaults(t *testing.T) {
	for _, url := range []string{
		"postgres://localhost/rag?pool_max_conns=7&statement_timeout=1200",
		"host=localhost dbname=rag pool_max_conns=7 statement_timeout=1200",
	} {
		t.Run(url, func(t *testing.T) {
			config, err := poolConfig(url)
			require.NoError(t, err)
			require.EqualValues(t, 7, config.MaxConns)
			require.Equal(t, "1200", config.ConnConfig.RuntimeParams["statement_timeout"])
		})
	}
	config, err := poolConfig("postgres://localhost/rag?statement_timeout=0")
	require.NoError(t, err)
	require.Equal(t, "0", config.ConnConfig.RuntimeParams["statement_timeout"])
	_, err = poolConfig("postgres://localhost/rag?pool_max_conns=invalid")
	require.Error(t, err)
}

func TestMigrationFilesAreDiscoveredInOrder(t *testing.T) {
	files := fstest.MapFS{
		"migrations/012_later.sql": {Data: []byte("SELECT 12;")},
		"migrations/001_first.sql": {Data: []byte("SELECT 1;")},
		"migrations/006_next.sql":  {Data: []byte("SELECT 6;")},
	}
	migrations, err := loadMigrations(files)
	require.NoError(t, err)
	require.Len(t, migrations, 3)
	require.Equal(t, []int{1, 6, 12}, []int{migrations[0].version, migrations[1].version, migrations[2].version})
	require.Equal(t, "006_next.sql", migrations[1].name)
	require.Equal(t, "SELECT 6;", migrations[1].sql)
	require.Equal(t, "17db4fd369edb9244b9f91d9aeed145c3d04ad8ba6e95d06247f07a63527d11a", migrations[0].checksum)
}

func TestEvaluationMigrationInventory(t *testing.T) {
	migrations, err := loadMigrations(migrationFiles)
	require.NoError(t, err)

	names := make([]string, 0, len(migrations))
	for _, migration := range migrations {
		names = append(names, migration.name)
	}
	require.Subset(t, names, []string{
		"012_index_visibility.sql",
		"017_evaluation_fixtures.sql",
		"018_evaluation_vector_identity.sql",
		"019_evaluation_fixture_guards.sql",
		"025_hosted_vector_manifest.sql",
	})
}

func TestEmbeddedMigrationsIncludeIndexVisibilityAndSourceUploads(t *testing.T) {
	migrations, err := loadMigrations(migrationFiles)
	require.NoError(t, err)
	names := make(map[int]string, len(migrations))
	for _, migration := range migrations {
		names[migration.version] = migration.name
	}
	require.Equal(t, "012_index_visibility.sql", names[12])
	require.Equal(t, "014_source_uploads.sql", names[14])
}

func TestInvalidMigrationFilesAreRejected(t *testing.T) {
	for _, names := range [][]string{
		{"001_first.sql", "001_duplicate.sql"},
		{"invalid.sql"},
		{"01_short.sql"},
		{"001_Upper.sql"},
	} {
		t.Run(strings.Join(names, ","), func(t *testing.T) {
			files := fstest.MapFS{}
			for _, name := range names {
				files["migrations/"+name] = &fstest.MapFile{Data: []byte("SELECT 1;")}
			}
			_, err := loadMigrations(files)
			require.Error(t, err)
		})
	}
}
