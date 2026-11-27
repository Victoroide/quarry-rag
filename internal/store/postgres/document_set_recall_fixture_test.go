package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
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

func TestDocumentSetRecallFixture(t *testing.T) {
	dsn := withFixtureOptions(t, pgtest.StartVector(t), "pool_max_conns=1")
	f := newRecallFixture(t.Context(), t, dsn, 3)
	p := &retrievalFixture{ownedFixture: f.ownership, vectors: f.store}
	p.own, _ = p.complete(t, p.owner, "included document", []float32{.8, .6, 0})
	p.shared, _ = p.complete(t, p.peer, "shared and revoked", []float32{1, 0, 0})
	p.peerOnly, _ = p.complete(t, p.peer, "included second owner", []float32{.6, .8, 0})
	p.other, _ = p.complete(t, p.outsider, "foreign tenant", retrievalQuery)
	p.grantRead(t, p.peer, p.shared, p.owner, true)
	p.grantRead(t, p.peer, p.peerOnly, p.owner, true)
	p.grantRead(t, p.owner, p.own, p.owner, true) // Overlapping ownership and grant.
	grantExec(t, f.owner, "UPDATE documents SET collection='team' WHERE id=$1", p.peerOnly)
	grantExec(t, f.owner, "UPDATE chunk_vectors SET collection='team' WHERE document_id=$1", p.peerOnly)
	grantExec(t, f.owner, "UPDATE grant_catalog_document_ownership SET collection='team' WHERE document_id=$1", p.peerOnly)
	grantExec(t, f.owner, "UPDATE chunk_vectors SET doc_type='report' WHERE document_id=$1", p.own)
	replaced, _ := p.complete(t, p.owner, "replaced member", retrievalQuery)
	excluded, _ := p.complete(t, p.owner, "readable, not selected", retrievalQuery)
	hidden, _ := p.complete(t, p.peer, "ungranted, same tenant", retrievalQuery)
	set := p.createSet(t, p.owner, "", p.own, p.shared, p.peerOnly, replaced)
	empty := p.createSet(t, p.owner, "")
	expired := p.createSet(t, p.owner, "", p.own)
	p.expire(t, expired)
	p.grantRead(t, p.peer, p.shared, p.owner, false)
	grantExec(t, f.owner, "DELETE FROM documents WHERE id=$1", replaced)
	grantExec(t, f.owner, `INSERT INTO documents(id,incarnation,content,status,chunk_count,doc_type)
VALUES($1,$2,'replacement','completed',1,'guide')`, replaced, uuid.New())
	grantExec(t, f.owner, `INSERT INTO chunks (id,document_id,chunk_index,text,token_count)
VALUES($1,$2,0,'replacement chunk',1)`, uuid.New(), replaced)
	grantExec(t, f.owner, `INSERT INTO chunk_vectors (chunk_id,document_id,chunk_index,doc_type,text,token_count,embedding)
SELECT id::text,document_id,chunk_index,'guide',text,token_count,'[1,0,0]'::vector FROM chunks WHERE document_id=$1`, replaced)
	grantExec(t, f.owner, recallAnalyze)

	var indexed int
	require.NoError(t, f.owner.Pool.QueryRow(t.Context(), "SELECT count(*) FROM chunk_vectors").Scan(&indexed))
	filter := recallFilter(p.owner, p.setRef(set))
	want := []string{p.own, p.peerOnly}
	for _, test := range []struct {
		name   string
		filter domain.VectorFilter
		want   []string
	}{
		{"authorized", filter, want},
		{"expired", recallFilter(p.owner, p.setRef(expired)), nil},
		{"empty set", recallFilter(p.owner, p.setRef(empty)), nil},
		{"foreign owner", recallFilter(p.outsider, p.setRef(set)), nil},
		{"explicit allowlist", domain.VectorFilter{Authorized: filter.Authorized,
			DocumentIDs: []string{p.peerOnly, p.shared, p.other, excluded, hidden, replaced}, DocType: "guide"}, []string{p.peerOnly}},
		{"empty allowlist", domain.VectorFilter{Authorized: filter.Authorized, DocumentIDs: []string{}}, nil},
		{"collection and type", domain.VectorFilter{Authorized: filter.Authorized, Collection: "team", DocType: "guide"}, []string{p.peerOnly}},
		{"different collection", domain.VectorFilter{Authorized: filter.Authorized, Collection: "other"}, nil},
		{"stale vector type", domain.VectorFilter{Authorized: filter.Authorized, DocType: "report"}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, inRecallSnapshot(t.Context(), f.serving, func(tx pgx.Tx) error {
				ground, err := buildRecallGround(t.Context(), tx, f.store, retrievalQuery, indexed, test.filter)
				require.NoError(t, err)
				require.Equal(t, len(test.want), ground.eligible)
				require.Equal(t, test.want, documentIDs(ground.chunks))
				return nil
			}))
		})
	}

	for _, forced := range []bool{false, true} {
		require.NoError(t, inRecallSnapshot(t.Context(), f.serving, func(tx pgx.Tx) error {
			ground, err := buildRecallGround(t.Context(), tx, f.store, retrievalQuery, indexed, filter)
			require.NoError(t, err)
			settings := recallANN{Method: "hnsw", Search: 40, Iteration: "off", MaxScan: 20_000, MemoryMultiplier: 1, Forced: forced}
			row, err := compareRecallANN(t.Context(), tx, f.store, ground, f.hnswName(), settings, 5)
			require.NoError(t, err)
			require.Contains(t, []string{"hnsw", recallNonANN, recallUnavailable}, row.Path)
			if row.Path == recallUnavailable {
				require.True(t, forced)
				require.Nil(t, row.Score)
				return nil
			}
			require.NotNil(t, row.Score)
			require.Zero(t, row.Score.Unauthorized)
			require.Less(t, row.Score.Returned, 5, "sparse ANN result")
			return nil
		}))
	}
	servingStore := *f.store
	servingStore.client, servingStore.exactScopeMax = f.serving, 0
	got, report, err := servingStore.QueryFilteredDetailed(t.Context(), retrievalQuery, 5, filter)
	require.NoError(t, err)
	require.Equal(t, SearchStatusPartial, report.Status)
	require.Equal(t, SearchStrategyHNSW, report.Strategy)
	require.False(t, report.ProbeAttempted)
	require.False(t, report.Probed)
	require.Subset(t, want, documentIDs(got))
	servingStore.exactScopeMax = 2
	got, report, err = servingStore.QueryFilteredDetailed(t.Context(), retrievalQuery, 5, filter)
	require.NoError(t, err)
	require.Equal(t, SearchStrategyExact, report.Strategy)
	require.True(t, report.Probed)
	require.Equal(t, 2, report.Candidates)
	require.Equal(t, want, documentIDs(got))
	_, err = f.denied.Pool.Exec(t.Context(), "SELECT 1 FROM grant_catalog_document_set_readable")
	requireSQLState(t, err, "42501")
	_, err = f.serving.Pool.Exec(t.Context(), "SELECT 1 FROM grant_catalog_document_set_members")
	requireSQLState(t, err, "42501")

	t.Run("configured evaluation with one connection in the DSN", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		require.EqualValues(t, 1, f.owner.Pool.Config().MaxConns)
		seed := recallSeed{f: f, indexed: indexed}
		index := recallIndexReport{Name: f.hnswName(), Method: "hnsw"}
		cells := []recallCell{{Set: recallSelection{Eligible: len(want), filter: filter}, K: 5, Threshold: new(2)}}
		vectors := make([][]float32, recallMeasurements)
		for i := range vectors {
			vectors[i] = retrievalQuery
		}
		require.NoError(t, evaluateRecallCells(ctx, seed, index, vectors, cells))
		require.False(t, cells[0].WarmupTimedOut)
		require.Len(t, cells[0].Observations, recallMeasurements)
		require.Equal(t, map[string]int{string(SearchStrategyExact): recallMeasurements}, cells[0].Paths)
		require.Equal(t, 1.0, *cells[0].meanStrict(string(SearchStrategyExact)))
	})
	t.Run("measurement timeout recovery", func(t *testing.T) {
		testRecallTimeoutRecovery(t, f.serving)
	})

	// Revoke a grant, then shorten expiry, each after a probe. The old row
	// versions remain visible during the comparison, until a new snapshot.
	ctx := t.Context()
	require.NoError(t, inRecallSnapshot(ctx, f.serving, func(tx pgx.Tx) error {
		eligible, err := probeRecall(ctx, tx, f.store.table, indexed, filter)
		require.NoError(t, err)
		require.Equal(t, 2, eligible)
		p.grantReadContext(ctx, t, p.peer, p.peerOnly, p.owner, false)
		ground, err := buildRecallGround(ctx, tx, f.store, retrievalQuery, indexed, filter)
		require.NoError(t, err)
		require.Equal(t, want, documentIDs(ground.chunks))
		return nil
	}))
	require.NoError(t, inRecallSnapshot(ctx, f.serving, func(tx pgx.Tx) error {
		eligible, err := probeRecall(ctx, tx, f.store.table, indexed, filter)
		require.NoError(t, err)
		require.Equal(t, 1, eligible)
		p.expireContext(ctx, t, set)
		ground, err := buildRecallGround(ctx, tx, f.store, retrievalQuery, indexed, filter)
		require.NoError(t, err)
		require.Equal(t, []string{p.own}, documentIDs(ground.chunks))
		return nil
	}))
	require.NoError(t, inRecallSnapshot(ctx, f.serving, func(tx pgx.Tx) error {
		eligible, err := probeRecall(ctx, tx, f.store.table, indexed, filter)
		require.NoError(t, err)
		require.Zero(t, eligible)
		return nil
	}))
}

func testRecallTimeoutRecovery(t *testing.T, serving *Client) {
	t.Helper()
	for _, test := range []struct {
		name           string
		failure        string
		failAt         int
		wantOpens      int
		oracleTimeouts int
	}{
		{"server timeout at first savepoint", "begin", 0, 2, 0},
		{"server timeout after one result", "begin", 1, 2, 0},
		{"server timeout inside a savepoint", "statement", 1, 1, 0},
		{"wire deadline after one result", "wire", 1, 2, 0},
		{"unexpected PostgreSQL error", "unexpected", 1, 1, 0},
		{"persistent oracle timeout after snapshot loss", "begin", 1, 3, 2},
		{"transient oracle timeout after snapshot loss", "begin", 1, 3, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cells := []recallCell{{K: 5}, {K: 10}, {K: 50}}
			cells = append(cells, recallCell{K: 5, elapsed: recallCellBudget})
			cells = append(cells, recallCell{K: 50})
			starts, opens := 0, 0
			open := func(ctx context.Context) (pgx.Tx, error) {
				tx, err := beginRecallSnapshot(ctx, serving)
				if err != nil {
					return nil, err
				}
				opens++
				if opens > 1 && opens <= 1+test.oracleTimeouts {
					timeoutErr := waitForRecallStatementTimeout(ctx, tx)
					requireSQLState(t, timeoutErr, "57014")
					return tx, timeoutErr
				}
				return recallSavepointTestTx{Tx: tx, beforeBegin: func(ctx context.Context) error {
					attempt := starts
					starts++
					if test.failure != "begin" || attempt != test.failAt {
						return nil
					}
					timeoutErr := waitForRecallStatementTimeout(ctx, tx)
					requireSQLState(t, timeoutErr, "57014")
					return timeoutErr
				}}, nil
			}
			compare := func(ctx context.Context, tx pgx.Tx, cell *recallCell) (recallObservation, error) {
				if starts-1 == test.failAt {
					switch test.failure {
					case "wire":
						short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
						defer cancel()
						_, err := tx.Exec(short, "SELECT pg_sleep(5)")
						require.ErrorIs(t, err, context.DeadlineExceeded)
						require.True(t, tx.Conn().IsClosed(), "a timed-out wire read must exercise connection replacement")
						return recallObservation{}, err
					case "statement":
						err := waitForRecallStatementTimeout(ctx, tx)
						requireSQLState(t, err, "57014")
						return recallObservation{}, err
					case "unexpected":
						_, err := tx.Exec(ctx, "SELECT 1 FROM nonexistent_recall_relation")
						return recallObservation{}, err
					}
				}
				var value int
				err := tx.QueryRow(ctx, "SELECT $1::int", cell.K).Scan(&value)
				require.NoError(t, err)
				require.Equal(t, cell.K, value)
				return recallObservation{Status: SearchStatusOK}, nil
			}
			report := recallReport{Version: 1}
			dir := t.TempDir()
			path := filepath.Join(dir, "report.json")
			err := completeRecallReport(ctx, path, &report, func(*recallReport) error {
				return runRecallQuery(ctx, cells, 0, recallOracleBudget, open, compare)
			})
			if test.failure == "unexpected" {
				requireSQLState(t, err, "42P01")
				require.NoFileExists(t, path)
				require.Equal(t, []recallObservation{{Status: SearchStatusOK}}, cells[0].Observations)
				for _, cell := range cells[1:] {
					require.Empty(t, cell.Observations)
				}
				return
			}
			require.NoError(t, err)
			require.FileExists(t, path)
			wantStarts := len(cells) - 1
			if test.oracleTimeouts == 2 {
				wantStarts = test.failAt + 1
			}
			require.Equal(t, test.wantOpens, opens)
			require.Equal(t, wantStarts, starts)
			for i, cell := range cells {
				want := recallObservation{Status: SearchStatusOK}
				if i == test.failAt || cell.elapsed >= recallCellBudget || test.oracleTimeouts == 2 && i > test.failAt {
					want = recallTimeoutObservation()
				}
				require.Equal(t, []recallObservation{want}, cell.Observations)
			}
			if test.oracleTimeouts == 2 {
				// A later held-out query has a fresh preparation budget and
				// appends one observation without altering the earlier samples.
				require.NoError(t, runRecallQuery(ctx, cells, 1, recallOracleBudget, open, compare))
				require.Equal(t, test.wantOpens+1, opens)
				for _, cell := range cells {
					want := recallObservation{Status: SearchStatusOK}
					if cell.elapsed >= recallCellBudget {
						want = recallTimeoutObservation()
					}
					require.Len(t, cell.Observations, 2)
					require.Equal(t, want, cell.Observations[1])
				}
			}
		})
	}
}

type recallSavepointTestTx struct {
	pgx.Tx
	beforeBegin func(context.Context) error
}

func (tx recallSavepointTestTx) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := tx.beforeBegin(ctx); err != nil {
		return nil, err
	}
	return tx.Tx.Begin(ctx)
}

func waitForRecallStatementTimeout(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '10ms'"); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "SELECT pg_sleep(5)")
	return err
}

func TestDocumentSetRecallIsolation(t *testing.T) {
	t.Run("unsupported vector version", func(t *testing.T) {
		dsn := pgtest.StartVector(t)
		owner := newRecallOwner(t.Context(), t, dsn)
		grantExec(t, owner, "CREATE EXTENSION vector")
		grantExec(t, owner, "UPDATE pg_extension SET extversion='0.7.4' WHERE extname='vector'")
		_, err := NewVectorStore(t.Context(), owner, recallDimensions)
		require.Error(t, err)
	})
	t.Run("missing extension", func(t *testing.T) {
		dsn := pgtest.Start(t)
		owner := newRecallOwner(t.Context(), t, dsn)
		roles := newGrantRoles(t, dsn, "recall_no_extension")
		grantExec(t, owner, roles.sql("CREATE ROLE {recall_no_extension}"))
		denied := roleClient(t, owner, roles.name("recall_no_extension"))
		_, err := NewVectorStore(t.Context(), denied, recallDimensions)
		require.Error(t, err)
	})
	t.Run("unsupported migration", func(t *testing.T) {
		dsn := pgtest.StartVector(t)
		owner := migrationTestClient(t, dsn)
		applyMigrationPrefix(t, owner, 30)
		_, err := owner.Pool.Exec(t.Context(), "SELECT 1 FROM grant_catalog_document_set_readable")
		requireSQLState(t, err, "42P01")
	})
	for _, label := range []string{"fresh seed", "rollback and retry"} {
		t.Run(label, func(t *testing.T) {
			t.Parallel()
			dsn := pgtest.StartVector(t)
			ctx := t.Context()
			f := newRecallFixture(ctx, t, dsn, recallDimensions)
			sibling := pgtest.Sibling(t, dsn)
			retry := newRecallFixture(ctx, t, sibling, recallDimensions)
			require.NotEqual(t, f.owner.Pool.Config().ConnConfig.Database, retry.owner.Pool.Config().ConnConfig.Database)
			testRecallSeed(ctx, t, retry, label == "rollback and retry")
			grantExec(t, f.owner, "CREATE TABLE recall_fixture_marker (id int)")
			_, err := retry.owner.Pool.Exec(ctx, "SELECT 1 FROM recall_fixture_marker")
			requireSQLState(t, err, "42P01")
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			require.ErrorIs(t, inRecallSnapshot(canceled, f.serving, func(pgx.Tx) error {
				return fmt.Errorf("pre-canceled snapshot executed work")
			}), context.Canceled)
			err = inRecallSnapshot(ctx, f.serving, func(tx pgx.Tx) error {
				_, setupErr := tx.Exec(ctx, "SET LOCAL statement_timeout = '10ms'")
				require.NoError(t, setupErr)
				_, queryErr := tx.Exec(ctx, "SELECT pg_sleep(1)")
				return queryErr
			})
			require.True(t, recallTimedOut(err))
			_, err = f.denied.Pool.Exec(ctx, "SELECT 1 FROM grant_catalog_document_set_readable")
			requireSQLState(t, err, "42501")
		})
	}
}

const recallAnalyze = `ANALYZE chunk_vectors, documents, grant_catalog_document_sets,
  grant_catalog_document_set_members, grant_catalog_document_ownership,
  grant_catalog_document_grants, grant_catalog_principals`

type recallFixture struct {
	owner, serving, denied *Client
	ownership              *ownedFixture
	store                  *VectorStore
}

func newRecallOwner(ctx context.Context, t *testing.T, dsn string) *Client {
	t.Helper()
	owner, err := NewClient(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(owner.Pool.Close)
	require.NoError(t, owner.RunMigrations(ctx))
	return owner
}

func newRecallFixture(ctx context.Context, t *testing.T, dsn string, dimensions int) *recallFixture {
	t.Helper()
	owner := newRecallOwner(ctx, t, dsn)
	store, err := NewVectorStore(ctx, owner, dimensions, WithExactScopeMax(maxExactScopeLimit))
	require.NoError(t, err)
	baseServing, roles := publishedServingClientContext(ctx, t, owner)
	cfg := baseServing.Pool.Config()
	// Use bound parameters without cached generic plans so the timed statements
	// and EXPLAIN are planned against the same arguments.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	// A configured search uses a production transaction while the comparison
	// holds a separate read-only snapshot on the same restricted pool.
	cfg.MaxConns = max(cfg.MaxConns, 2)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	serving := &Client{Pool: pool}
	grantExecContext(ctx, t, owner, roles.sql("GRANT SELECT ON chunk_vectors TO {quarry_service}"))
	return &recallFixture{owner, serving, roleClientContext(ctx, t, owner, roles.name("quarry_auditor")),
		provisionOwnedFixtureContext(ctx, t, owner), store}
}

func (f *recallFixture) hnswName() string { return "idx_" + f.store.table + "_embedding" }

func recallFilter(principal domain.Principal, ref *domain.DocumentSetRef) domain.VectorFilter {
	return domain.VectorFilter{Authorized: &domain.AuthorizedScope{
		TenantID: principal.TenantID, PrincipalID: principal.ID, DocumentSet: ref,
	}}
}

type recallSelection struct {
	Label    string `json:"sample"`
	Members  int    `json:"members"`
	Eligible int    `json:"eligible_chunks"`
	filter   domain.VectorFilter
}

type recallSeed struct {
	f              *recallFixture
	load           recallInput
	indexed, count int
}

const recallSeedBatchSize = 512

func seedRecallCorpus(ctx context.Context, f *recallFixture, path string, expected int) (recallSeed, error) {
	seed := recallSeed{f: f, count: expected}
	err := pgx.BeginFunc(ctx, f.owner.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
SET LOCAL statement_timeout = '30min';
CREATE TABLE recall_input (
  ordinal INTEGER PRIMARY KEY, key_hash BYTEA NOT NULL, doc UUID NOT NULL UNIQUE,
  incarnation UUID NOT NULL, chunk UUID NOT NULL, extra UUID NOT NULL, embedding vector(768) NOT NULL
);
CREATE TEMP TABLE recall_input_batch (
  ordinal INTEGER, key_hash BYTEA, doc UUID, incarnation UUID, chunk UUID, extra UUID, embedding TEXT
) ON COMMIT DROP`); err != nil {
			return err
		}
		batch := make([][]any, 0, recallSeedBatchSize)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			_, err := tx.CopyFrom(ctx, pgx.Identifier{"recall_input_batch"},
				[]string{"ordinal", "key_hash", "doc", "incarnation", "chunk", "extra", "embedding"},
				pgx.CopyFromRows(batch))
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `
INSERT INTO recall_input
SELECT ordinal,key_hash,doc,incarnation,chunk,extra,embedding::vector FROM recall_input_batch;
TRUNCATE recall_input_batch`)
			clear(batch)
			batch = batch[:0]
			return err
		}
		loaded, loadErr := readRecallFile(ctx, path, expected, func(ordinal int, hash [32]byte, embedding []float32) error {
			literal, err := vectorLiteral(embedding, recallDimensions)
			if err != nil {
				return err
			}
			batch = append(batch, []any{ordinal, hash[:], recallID("document", hash), recallID("incarnation", hash),
				recallID("chunk", hash), recallID("extra-chunk", hash), literal})
			if len(batch) == recallSeedBatchSize {
				return flush()
			}
			return nil
		})
		if loadErr != nil {
			return loadErr
		}
		seed.load = loaded
		if flushErr := flush(); flushErr != nil {
			return flushErr
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO documents (id,incarnation,content,status,chunk_count,doc_type,collection)
SELECT doc,incarnation,'fixture document','completed',1,'guide','' FROM recall_input`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO grant_catalog_assigned_incarnations (document_id,incarnation)
SELECT doc,incarnation FROM recall_input;
`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO grant_catalog_document_ownership
(document_id,incarnation,tenant_id,principal_id,collection,source_version,ready)
SELECT i.doc,i.incarnation, CASE WHEN i.ordinal%4=3 THEN $1::uuid ELSE $2::uuid END,
 CASE WHEN i.ordinal%4=3 THEN $3::uuid WHEN i.ordinal%4=2 THEN $4::uuid ELSE $5::uuid END,
 '', s.source_version, true
FROM recall_input i JOIN grant_catalog_source_documents s ON s.document_id=i.doc AND s.incarnation=i.incarnation`,
			f.ownership.otherTenant, f.ownership.tenant, f.ownership.outsider.ID, f.ownership.peer.ID, f.ownership.owner.ID); err != nil {
			return err
		}
		_, insertErr := tx.Exec(ctx, `
UPDATE documents SET grant_catalog_assigned=true;
INSERT INTO chunks (id,document_id,chunk_index,text,token_count)
SELECT chunk,doc,0,'fixture chunk',1 FROM recall_input;
INSERT INTO chunk_vectors (chunk_id,document_id,chunk_index,doc_type,collection,text,token_count,embedding)
SELECT chunk::text,doc,0,'guide','','fixture chunk',1,embedding FROM recall_input ORDER BY ordinal;
ANALYZE recall_input;
`+recallAnalyze)
		seed.indexed = expected
		return insertErr
	})
	return seed, err
}

func recallID(kind string, hash [32]byte) uuid.UUID {
	namespace := uuid.NewSHA1(uuid.NameSpaceOID, []byte("quarry-document-set-recall/"+kind))
	return uuid.NewSHA1(namespace, hash[:])
}

func (s *recallSeed) addMultiplicity(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.f.owner.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
SET LOCAL statement_timeout = '30min';
INSERT INTO chunks (id,document_id,chunk_index,text,token_count)
SELECT a.extra,a.doc,1,'fixture chunk',1
FROM recall_input a JOIN recall_input b ON b.ordinal=a.ordinal+1 WHERE a.ordinal%10=0;
UPDATE documents d SET chunk_count=2 FROM chunks c WHERE c.document_id=d.id AND c.chunk_index=1
`); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
INSERT INTO chunk_vectors (chunk_id,document_id,chunk_index,doc_type,collection,text,token_count,embedding)
SELECT a.extra::text,a.doc,1,'guide','','fixture chunk',1,b.embedding
FROM recall_input a JOIN recall_input b ON b.ordinal=a.ordinal+1 WHERE a.ordinal%10=0 ORDER BY a.ordinal
`)
		if err != nil {
			return err
		}
		s.indexed = s.count + int(tag.RowsAffected())
		return nil
	})
}

func (s recallSeed) makeSelections(ctx context.Context, t *testing.T, selectionQuery []float32) []recallSelection {
	t.Helper()
	literal, err := vectorLiteral(selectionQuery, recallDimensions)
	require.NoError(t, err)
	const ownerWhere = " WHERE ordinal%4 IN (0,1) "
	selection := func(label, order string, args ...any) []string {
		t.Helper()
		rows, err := s.f.owner.Pool.Query(ctx, "SELECT doc::text FROM recall_input"+ownerWhere+order+" LIMIT 100000", args...)
		require.NoError(t, err)
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, err)
		require.Len(t, ids, recallCorpusRows/2, label)
		return ids
	}
	samples := []struct {
		label string
		ids   []string
	}{
		{"hash", selection("hash", "ORDER BY key_hash,ordinal")},
		{"correlated", selection("correlated", "ORDER BY embedding <=> $1::text::vector,ordinal", literal)},
	}
	var selections []recallSelection
	add := func(label string, ids []string) {
		t.Helper()
		p := s.f.ownership.owner
		set, err := s.f.owner.CreateDocumentSet(ctx, DocumentSetRequest{
			TenantID: p.TenantID, OwnerID: p.ID, TTL: MaxDocumentSetTTL, MaxMembers: MaxDocumentSetMembers, DocumentIDs: ids,
		})
		require.NoError(t, err)
		require.Equal(t, len(ids), set.Members)
		reader := GrantReader{Pool: s.f.serving.Pool}
		scope, err := reader.DeriveReadScope(ctx, p, domain.ReadScopeRequest{DocumentSetID: set.ID, Membership: max(1, len(ids))})
		require.NoError(t, err)
		require.NotNil(t, scope.DocumentSet)
		require.False(t, scope.Exceeded)
		require.Len(t, scope.Documents, len(ids))
		seen := make(map[string]struct{}, len(ids))
		for _, doc := range scope.Documents {
			seen[doc.ID] = struct{}{}
		}
		for _, id := range ids {
			if _, ok := seen[id]; !ok {
				t.Fatal("restricted reader returned different document set members")
			}
		}
		selections = append(selections, recallSelection{Label: label, Members: len(ids), filter: recallFilter(p, scope.DocumentSet)})
	}
	add("empty", nil)
	for _, sample := range samples {
		for _, size := range []int{1, 100, 200, 2_000, 10_000, 20_000, 100_000} {
			add(sample.label, sample.ids[:size])
		}
	}
	return selections
}

func withRecallSetupDeadline(parent context.Context, execute func(context.Context)) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Minute)
	defer cancel()
	execute(ctx)
}

func testRecallSeed(ctx context.Context, t *testing.T, f *recallFixture, retry bool) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "corpus.jsonl")
	principals := []domain.Principal{f.ownership.owner, f.ownership.owner, f.ownership.peer, f.ownership.outsider}
	input := recallTestHeader()
	want := make(map[domain.Principal][]domain.ReadableDocument)
	for ordinal, p := range principals {
		key := fmt.Sprintf("document-%d", ordinal)
		input += "\n" + recallTestRecord(key)
		hash := sha256.Sum256([]byte(key))
		want[p] = append(want[p], domain.ReadableDocument{
			ID: recallID("document", hash).String(), Incarnation: recallID("incarnation", hash).String(),
		})
	}
	require.NoError(t, os.WriteFile(path, []byte(input), 0o600))
	if retry {
		bad := filepath.Join(dir, "bad.jsonl")
		var invalid strings.Builder
		invalid.WriteString(recallTestHeader())
		for ordinal := range recallSeedBatchSize {
			invalid.WriteByte('\n')
			invalid.WriteString(recallTestRecord(fmt.Sprintf("batch-%d", ordinal)))
		}
		invalid.WriteString("\n{")
		require.NoError(t, os.WriteFile(bad, []byte(invalid.String()), 0o600))
		_, err := seedRecallCorpus(ctx, f, bad, recallSeedBatchSize+1)
		require.ErrorContains(t, err, fmt.Sprintf("recall input row %d", recallSeedBatchSize+1))
		var stagingRemoved bool
		require.NoError(t, f.owner.Pool.QueryRow(ctx, "SELECT to_regclass('recall_input') IS NULL").Scan(&stagingRemoved))
		require.True(t, stagingRemoved, "rollback removed the staging relation after COPY")
	}
	seed, err := seedRecallCorpus(ctx, f, path, len(principals))
	require.NoError(t, err)
	require.Equal(t, len(principals), seed.indexed)
	reader := GrantReader{Pool: f.serving.Pool}
	for principal, documents := range want {
		scope, scopeErr := reader.DeriveReadScope(ctx, principal, domain.ReadScopeRequest{Membership: len(principals)})
		require.NoError(t, scopeErr)
		require.False(t, scope.Exceeded)
		require.ElementsMatch(t, documents, scope.Documents)
	}
	require.NoError(t, seed.addMultiplicity(ctx))
	require.Equal(t, len(principals)+1, seed.indexed)
	var indexed int
	require.NoError(t, f.owner.Pool.QueryRow(ctx, "SELECT count(*) FROM chunk_vectors").Scan(&indexed))
	require.Equal(t, seed.indexed, indexed)
}
