package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	recallOptIn        = "QUARRY_DOCUMENT_SET_RECALL"
	recallCorpusPath   = "QUARRY_DOCUMENT_SET_RECALL_CORPUS"
	recallQueryPath    = "QUARRY_DOCUMENT_SET_RECALL_QUERIES"
	recallOutputPath   = "QUARRY_DOCUMENT_SET_RECALL_REPORT"
	recallCellBudget   = 5 * time.Minute
	recallOracleBudget = 5 * time.Minute
	recallTotalBudget  = 2 * time.Hour
	recallMeasurements = recallQueryRows - 1
)

var recallKs = []int{5, 10, 50}
var recallThresholds = []int{0, 100, 1_000, 10_000, 100_000}

type recallPaths struct{ corpus, queries, report string }

func getRecallPaths(lookup func(string) string) (paths recallPaths, enabled bool, err error) {
	if lookup(recallOptIn) != "1" {
		return paths, false, nil
	}
	paths = recallPaths{lookup(recallCorpusPath), lookup(recallQueryPath), lookup(recallOutputPath)}
	if paths.corpus == "" || paths.queries == "" {
		return paths, true, errors.New("both recall input paths must be specified")
	}
	return paths, true, nil
}

func TestDocumentSetRecallReport(t *testing.T) {
	paths, enabled, err := getRecallPaths(os.Getenv)
	if !enabled {
		t.Skip("set QUARRY_DOCUMENT_SET_RECALL=1 to evaluate document-set recall")
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), recallTotalBudget)
	defer cancel()

	embeddings := make([][]float32, 0, recallQueryRows)
	queries, err := readRecallFile(ctx, paths.queries, recallQueryRows, func(_ int, _ [32]byte, embedding []float32) error {
		embeddings = append(embeddings, embedding)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, absentRecallDestination(paths.report))

	var container recallContainerInfo
	dsn := pgtest.StartVector(t, &container)
	var f *recallFixture
	var seed recallSeed
	withRecallSetupDeadline(ctx, func(setup context.Context) {
		f = newRecallFixture(setup, t, dsn, recallDimensions)
		_, dropErr := f.owner.Pool.Exec(setup, "DROP INDEX "+pgx.Identifier{f.hnswName()}.Sanitize())
		require.NoError(t, dropErr)
		loaded, seedErr := seedRecallCorpus(setup, f, paths.corpus, recallCorpusRows)
		require.NoError(t, seedErr)
		seed = loaded
	})
	info, err := recallEnvironment(ctx, f, container)
	require.NoError(t, err)
	require.NoError(t, matchRecallInputs(seed.load, queries))
	var selections []recallSelection
	withRecallSetupDeadline(ctx, func(setup context.Context) {
		selections = seed.makeSelections(setup, t, embeddings[0])
	})
	report := recallReport{Version: 1, Corpus: seed.load, Queries: queries, Environment: info,
		K: slices.Clone(recallKs), Thresholds: slices.Clone(recallThresholds)}
	err = completeRecallReport(ctx, paths.report, &report, func(report *recallReport) error {
		return evaluateRecallReport(ctx, t, seed, selections, embeddings[1:], report)
	})
	require.NoError(t, err)
	t.Logf("recommended exact-scope threshold: %d eligible chunks", report.Recommendation)
}

func evaluateRecallReport(ctx context.Context, t *testing.T, seed recallSeed, selections []recallSelection, vectors [][]float32, report *recallReport) error {
	t.Helper()
	for _, label := range []string{"one_chunk", "synthetic_extra_chunk"} {
		if label == "synthetic_extra_chunk" {
			setup, cancel := context.WithTimeout(ctx, 30*time.Minute)
			err := seed.addMultiplicity(setup)
			cancel()
			if err != nil {
				return err
			}
		}
		layout, err := evaluateRecallLayout(ctx, seed, selections, vectors, label)
		if err != nil {
			return fmt.Errorf("layout %s: %w", label, err)
		}
		report.Layouts = append(report.Layouts, layout)
		for _, index := range layout.Indexes {
			t.Logf("%s %s: %d indexed chunks, %d cells, %d verified ANN observations, %s build, %d bytes",
				label, index.Method, layout.Indexed, len(index.Cells), index.verifiedANN(), index.BuildTime, index.Bytes)
			logRecallSample(t, label, index)
		}
	}
	return nil
}

func completeRecallReport(ctx context.Context, path string, report *recallReport, evaluate func(*recallReport) error) error {
	if err := evaluate(report); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	report.Recommendation = recommendRecallThreshold(report.thresholdEvidence())
	return publishRecallReport(ctx, path, *report)
}

func logRecallSample(t *testing.T, layout string, index recallIndexReport) {
	t.Helper()
	for _, cell := range index.Cells {
		// Show both sample shapes at the 0.1% and 50% member ratios.
		if cell.K != 50 || (cell.Set.Members != 200 && cell.Set.Members != 100_000) {
			continue
		}
		if cell.ANN == nil || cell.ANN.Iteration == "off" ||
			(index.Method == "hnsw" && cell.ANN.Search != 40) ||
			(index.Method == "ivfflat" && cell.ANN.Search != 1) {
			continue
		}
		for _, path := range []string{index.Method, recallNonANN, recallUnavailable, "unknown"} {
			count := cell.Paths[path]
			if count == 0 {
				continue
			}
			strict, p95 := "unavailable", "unavailable"
			if mean := cell.meanStrict(path); mean != nil {
				strict = fmt.Sprintf("%.3f", *mean)
			}
			if latency := cell.ByPath[path].P95; latency != nil {
				p95 = latency.Round(time.Microsecond).String()
			}
			t.Logf("%s %s %s forced=%t path=%s members=%d eligible=%d samples=%d strict_recall@50=%s p95=%s",
				layout, index.Method, cell.Set.Label, cell.ANN.Forced, path, cell.Set.Members, cell.Set.Eligible, count, strict, p95)
		}
	}
}

type recallReport struct {
	Version        int            `json:"version"`
	Corpus         recallInput    `json:"corpus"`
	Queries        recallInput    `json:"queries"`
	Environment    recallRunInfo  `json:"environment"`
	K              []int          `json:"k"`
	Thresholds     []int          `json:"configured_thresholds"`
	Layouts        []recallLayout `json:"layouts"`
	Recommendation int            `json:"recommended_exact_scope_max"`
}

type recallLayout struct {
	Label   string              `json:"layout"`
	Indexed int                 `json:"indexed_chunks"`
	Indexes []recallIndexReport `json:"indexes"`
}

type recallIndexReport struct {
	Method    string         `json:"method"`
	Name      string         `json:"index_name"`
	BuildTime time.Duration  `json:"build_time_ns"`
	Bytes     int64          `json:"index_bytes"`
	Options   map[string]int `json:"index_options"`
	Cells     []recallCell   `json:"cells"`
}

func (r recallIndexReport) verifiedANN() int {
	verified := 0
	for _, cell := range r.Cells {
		for _, sample := range cell.Observations {
			if sample.Path == r.Method && sample.Score != nil {
				verified++
			}
		}
	}
	return verified
}

type recallCell struct {
	Set            recallSelection              `json:"set"`
	K              int                          `json:"k"`
	MemberRatio    float64                      `json:"member_ratio"`
	ChunkRatio     float64                      `json:"eligible_chunk_ratio"`
	ANN            *recallANN                   `json:"ann,omitempty"`
	Threshold      *int                         `json:"configured_threshold,omitempty"`
	Observations   []recallObservation          `json:"samples"`
	Paths          map[string]int               `json:"observed_path_counts"`
	Statuses       map[SearchStatus]int         `json:"status_counts"`
	P50            *time.Duration               `json:"p50_ns"`
	P95            *time.Duration               `json:"p95_ns"`
	ByPath         map[string]recallPathLatency `json:"latency_by_path"`
	WarmupTimedOut bool                         `json:"warmup_timed_out,omitempty"`
	elapsed        time.Duration
}

type recallPathLatency struct {
	Samples int            `json:"samples"`
	P50     *time.Duration `json:"p50_ns"`
	P95     *time.Duration `json:"p95_ns"`
}

func (c *recallCell) summarize() {
	var latencies []time.Duration
	grouped := make(map[string][]time.Duration)
	c.Paths = make(map[string]int)
	c.Statuses = make(map[SearchStatus]int)
	for _, row := range c.Observations {
		c.Paths[row.Path]++
		c.Statuses[row.Status]++
		if row.Latency != nil {
			latencies = append(latencies, *row.Latency)
			grouped[row.Path] = append(grouped[row.Path], *row.Latency)
		}
	}
	c.ByPath = make(map[string]recallPathLatency, len(grouped))
	for path, values := range grouped {
		c.ByPath[path] = recallPathLatency{len(values), recallQuantile(values, 50), recallQuantile(values, 95)}
	}
	c.P50 = recallQuantile(latencies, 50)
	c.P95 = recallQuantile(latencies, 95)
}

func (c recallCell) timeouts() int {
	count := c.Statuses[SearchStatusTimeout]
	if c.WarmupTimedOut {
		count++
	}
	return count
}

func (c recallCell) meanStrict(path string) *float64 {
	var sum float64
	count := 0
	for _, row := range c.Observations {
		if row.Path == path && row.Score != nil && row.Score.Strict != nil {
			sum += *row.Score.Strict
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return new(sum / float64(count))
}

func (r recallReport) thresholdEvidence() []recallThresholdEvidence {
	var evidence []recallThresholdEvidence
	for _, layout := range r.Layouts {
		type setKey struct {
			sample string
			size   int
		}
		baseline := make(map[setKey]*float64)
		for _, index := range layout.Indexes {
			for _, cell := range index.Cells {
				if cell.K == 50 && cell.Threshold != nil && *cell.Threshold == 0 {
					baseline[setKey{cell.Set.Label, cell.Set.Members}] = cell.meanStrict("hnsw")
				}
			}
			for _, cell := range index.Cells {
				if cell.K != 50 || cell.Threshold == nil || *cell.Threshold == 0 {
					continue
				}
				evidence = append(evidence, recallThresholdEvidence{
					Threshold: *cell.Threshold, K: cell.K, Eligible: cell.Set.Eligible,
					Samples: len(cell.Observations), Exact: cell.Paths[string(SearchStrategyExact)], TimedOut: cell.timeouts(),
					P95: cell.P95, MeanExact: cell.meanStrict(string(SearchStrategyExact)),
					DefaultStrict: baseline[setKey{cell.Set.Label, cell.Set.Members}],
				})
			}
		}
	}
	return evidence
}

func evaluateRecallLayout(ctx context.Context, seed recallSeed, sets []recallSelection, queries [][]float32, label string) (recallLayout, error) {
	layout := recallLayout{Label: label, Indexed: seed.indexed}
	var count int
	err := seed.f.owner.Pool.QueryRow(ctx, `
SELECT count(*) FROM chunk_vectors v JOIN documents d ON d.id=v.document_id AND d.status='completed'`).Scan(&count)
	if err != nil {
		return layout, err
	}
	if count != seed.indexed {
		return layout, errors.New("indexed live chunk count differs from the fixture")
	}
	for _, method := range []string{"hnsw", "ivfflat"} {
		index, variants, err := installRecallIndex(ctx, seed, method)
		if err != nil {
			return layout, err
		}
		for _, set := range sets {
			selection, err := recallEligibility(ctx, seed, set, label)
			if err != nil {
				return layout, err
			}
			cells := recallCells(selection, seed, variants, method)
			if err := evaluateRecallCells(ctx, seed, index, queries, cells); err != nil {
				return layout, fmt.Errorf("%s %s set with %d members: %w", method, set.Label, set.Members, err)
			}
			index.Cells = append(index.Cells, cells...)
		}
		if index.verifiedANN() == 0 {
			return layout, fmt.Errorf("no %s access path verified by EXPLAIN", method)
		}
		if _, err := seed.f.owner.Pool.Exec(ctx, "DROP INDEX "+pgx.Identifier{index.Name}.Sanitize()); err != nil {
			return layout, err
		}
		layout.Indexes = append(layout.Indexes, index)
	}
	return layout, nil
}

func recallEligibility(ctx context.Context, seed recallSeed, set recallSelection, layout string) (recallSelection, error) {
	// Derive the expected count from staging and set membership independently of
	// the vector predicates, then verify it through the restricted probe.
	multiplicity := 0
	if layout == "synthetic_extra_chunk" {
		err := seed.f.owner.Pool.QueryRow(ctx, `
SELECT count(*) FROM recall_input i
JOIN grant_catalog_document_set_members m ON m.document_id=i.doc AND m.incarnation=i.incarnation
WHERE m.set_id=$1::uuid AND i.ordinal%10=0
  AND EXISTS (SELECT 1 FROM recall_input b WHERE b.ordinal=i.ordinal+1)`, set.filter.Authorized.DocumentSet.ID).Scan(&multiplicity)
		if err != nil {
			return set, err
		}
	}
	err := inRecallSnapshot(ctx, seed.f.serving, func(tx pgx.Tx) error {
		var err error
		set.Eligible, err = probeRecall(ctx, tx, seed.f.store.table, seed.indexed, set.filter)
		return err
	})
	if err == nil && set.Eligible != set.Members+multiplicity {
		err = errors.New("restricted probe disagrees with fixture membership and multiplicity")
	}
	return set, err
}

func installRecallIndex(parent context.Context, seed recallSeed, method string) (recallIndexReport, []recallANN, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Minute)
	defer cancel()
	index, variants, options := recallIndexConfig(seed, method)
	start := time.Now()
	err := pgx.BeginFunc(ctx, seed.f.owner.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
SET LOCAL statement_timeout = '30min';
SET LOCAL maintenance_work_mem = '1GB';
SET LOCAL max_parallel_maintenance_workers = 0`); err != nil {
			return err
		}
		ddl := fmt.Sprintf("CREATE INDEX %s ON %s USING %s (embedding vector_cosine_ops) WITH (%s)",
			pgx.Identifier{index.Name}.Sanitize(), pgx.Identifier{seed.f.store.table}.Sanitize(),
			pgx.Identifier{method}.Sanitize(), options)
		_, err := tx.Exec(ctx, ddl)
		return err
	})
	index.BuildTime = time.Since(start)
	if err != nil {
		return index, nil, err
	}
	if _, analyzeErr := seed.f.owner.Pool.Exec(ctx, recallAnalyze); analyzeErr != nil {
		return index, nil, analyzeErr
	}
	const catalog = `
SELECT am.amname, pg_relation_size(i.indexrelid)
FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
JOIN pg_am am ON am.oid=c.relam
JOIN pg_opclass op ON op.oid=i.indclass[0]
WHERE i.indrelid=to_regclass($1) AND c.relname=$2 AND i.indisvalid AND i.indisready
  AND op.opcname='vector_cosine_ops'`
	var actual string
	err = seed.f.owner.Pool.QueryRow(ctx, catalog, seed.f.store.table, index.Name).Scan(&actual, &index.Bytes)
	if err == nil && actual != method {
		err = errors.New("fixture index has the wrong access method")
	}
	return index, variants, err
}

func recallIndexConfig(seed recallSeed, method string) (recallIndexReport, []recallANN, string) {
	index := recallIndexReport{Method: method, Name: seed.f.hnswName(), Options: map[string]int{"m": 16, "ef_construction": 64}}
	options := "m=16, ef_construction=64"
	search := []int{40, 200}
	iteration := "strict_order"
	maxScan, memory := 20_000, 1
	if method == "ivfflat" {
		lists := (seed.indexed + 999) / 1000
		root := int(math.Ceil(math.Sqrt(float64(lists))))
		index.Name = "idx_" + seed.f.store.table + "_recall_ivfflat"
		index.Options = map[string]int{"lists": lists}
		options = "lists=" + strconv.Itoa(lists)
		search = []int{1, root}
		iteration = "relaxed_order"
		maxScan, memory = min(lists-1, 4*root), 0
	}
	var variants []recallANN
	for _, candidateCount := range search {
		for _, scan := range []string{"off", iteration} {
			for _, forced := range []bool{false, true} {
				variants = append(variants, recallANN{method, candidateCount, scan, maxScan, memory, forced})
			}
		}
	}
	return index, variants, options
}

func recallCells(set recallSelection, seed recallSeed, variants []recallANN, method string) []recallCell {
	var cells []recallCell
	for _, k := range recallKs {
		base := recallCell{Set: set, K: k, MemberRatio: float64(set.Members) / float64(seed.count),
			ChunkRatio: float64(set.Eligible) / float64(seed.indexed)}
		for _, variant := range variants {
			cell := base
			cell.ANN = new(variant)
			cells = append(cells, cell)
		}
		if method == "hnsw" {
			for _, threshold := range recallThresholds {
				cell := base
				cell.Threshold = new(threshold)
				cells = append(cells, cell)
			}
		}
	}
	return cells
}

func evaluateRecallCells(ctx context.Context, seed recallSeed, index recallIndexReport, vectors [][]float32, cells []recallCell) error {
	if len(vectors) != recallMeasurements {
		return errors.New("recall cell requires all held-out measurement queries")
	}
	for i := range cells {
		cell := &cells[i]
		if cell.Threshold == nil {
			continue
		}
		store := *seed.f.store
		store.client, store.exactScopeMax = seed.f.serving, *cell.Threshold
		start := time.Now()
		warm, cancel := context.WithTimeout(ctx, recallCellBudget)
		_, _, err := store.QueryFilteredDetailed(warm, vectors[0], cell.K, cell.Set.filter)
		cell.WarmupTimedOut = err != nil && recallTimedOut(err)
		cancel()
		cell.elapsed = time.Since(start)
		if err != nil && !cell.WarmupTimedOut {
			return err
		}
	}
	for queryIndex, vector := range vectors {
		var ground recallGround
		err := runRecallQuery(ctx, cells, queryIndex, recallOracleBudget, func(ctx context.Context) (pgx.Tx, error) {
			tx, beginErr := beginRecallSnapshot(ctx, seed.f.serving)
			if beginErr != nil {
				return nil, beginErr
			}
			var groundErr error
			ground, groundErr = prepareRecallGround(ctx, tx, seed, index, vector, cells[0].Set)
			return tx, groundErr
		}, func(ctx context.Context, tx pgx.Tx, cell *recallCell) (recallObservation, error) {
			if cell.ANN != nil {
				return compareRecallANN(ctx, tx, seed.f.store, ground, index.Name, *cell.ANN, cell.K)
			}
			return compareRecallConfigured(ctx, tx, seed, index, ground, vector, cell.K, *cell.Threshold)
		})
		if err != nil {
			return err
		}
	}
	for i := range cells {
		if len(cells[i].Observations) != len(vectors) {
			return errors.New("incomplete recall cell")
		}
		cells[i].summarize()
	}
	return nil
}

func recallTimeoutObservation() recallObservation {
	return recallObservation{Status: SearchStatusTimeout, Path: "unknown"}
}

func prepareRecallGround(ctx context.Context, tx pgx.Tx, seed recallSeed, index recallIndexReport, vector []float32, set recallSelection) (recallGround, error) {
	ground, err := buildRecallGround(ctx, tx, seed.f.store, vector, seed.indexed, set.filter)
	if err != nil {
		return ground, err
	}
	if ground.eligible != set.Eligible {
		return ground, errors.New("eligible row count changed during the measurement")
	}
	query, args, _ := exactSQL(seed.f.store.table, ground.literal, 50, seed.indexed+1, set.filter)
	uses, err := explainRecall(ctx, tx, seed.f.store.table, index.Name, query, args)
	if err == nil && uses {
		err = errors.New("ground-truth ranking used the active ANN index")
	}
	return ground, err
}

type recallComparison func(context.Context, pgx.Tx, *recallCell) (recallObservation, error)

const recallTimeoutHeadroom = time.Second

func openRecallOracle(parent context.Context, remaining *time.Duration, open func(context.Context) (pgx.Tx, error)) (pgx.Tx, error) {
	start := time.Now()
	defer func() { *remaining -= time.Since(start) }()
	ctx, cancel := context.WithTimeout(parent, *remaining)
	defer cancel()

	// Allow one retry per preparation, with the same deadline for both attempts.
	for range 2 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tx, err := open(ctx)
		if err == nil {
			err = ctx.Err()
		}
		if err == nil {
			return tx, nil
		}
		if tx != nil {
			// Rollback releases the pool slot even if the deadline has expired.
			_ = tx.Rollback(ctx)
		}
		if !recallTimedOut(err) {
			return nil, err
		}
	}
	return nil, context.DeadlineExceeded
}

func runRecallQuery(ctx context.Context, cells []recallCell, queryIndex int, oracleRemaining time.Duration,
	open func(context.Context) (pgx.Tx, error), compare recallComparison) error {
	var snapshot pgx.Tx
	var oracleUnavailable bool
	closeSnapshot := func() {
		if snapshot != nil {
			_ = rollbackRecall(ctx, snapshot)
			snapshot = nil
		}
	}
	defer closeSnapshot()

	for position := range cells {
		if err := ctx.Err(); err != nil {
			return err
		}
		i := position
		if queryIndex%2 == 1 {
			i = len(cells) - 1 - position
		}
		cell := &cells[i]
		row := recallTimeoutObservation()
		remaining := recallCellBudget - cell.elapsed
		if remaining > recallTimeoutHeadroom && !oracleUnavailable {
			if snapshot == nil {
				var openErr error
				snapshot, openErr = openRecallOracle(ctx, &oracleRemaining, open)
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				if openErr != nil {
					if !recallTimedOut(openErr) {
						return openErr
					}
					oracleUnavailable = true
				}
			}
			if snapshot != nil {
				observation, rollbackFailed, err := measureRecallCell(ctx, snapshot, remaining, cell, compare)
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				switch {
				case err == nil:
					row = observation
				case !recallTimedOut(err):
					return err
				case rollbackFailed || snapshot.Conn().IsClosed() || snapshot.Conn().PgConn().TxStatus() != 'T':
					// A timeout broke the snapshot. The immutable fixture lets us
					// reopen it for the remaining cells within the query's budget.
					closeSnapshot()
				}
			}
		}
		cell.Observations = append(cell.Observations, row)
	}
	return ctx.Err()
}

func measureRecallCell(parent context.Context, snapshot pgx.Tx, remaining time.Duration, cell *recallCell,
	compare recallComparison) (row recallObservation, rollbackFailed bool, err error) {
	start := time.Now()
	defer func() { cell.elapsed += time.Since(start) }()
	ctx, cancel := context.WithTimeout(parent, remaining)
	defer cancel()

	tx, err := snapshot.Begin(ctx)
	if err != nil {
		return row, false, err
	}
	defer func() {
		rollbackErr := rollbackRecall(parent, tx)
		rollbackFailed = rollbackErr != nil
		if rollbackFailed && (err == nil || recallTimedOut(err) && !snapshot.Conn().IsClosed()) {
			err = rollbackErr
		}
	}()
	// The server should report a timeout before the client wire deadline.
	// Keep a shorter statement timeout configured by the fixture DSN.
	const limit = `
SELECT set_config('statement_timeout', LEAST($1::bigint,
	COALESCE(NULLIF(setting::bigint, 0), $1::bigint))::text, true)
FROM pg_settings WHERE name='statement_timeout'`
	deadline, _ := ctx.Deadline()
	serverMillis := (time.Until(deadline) - recallTimeoutHeadroom).Milliseconds()
	if serverMillis <= 0 {
		return row, false, context.DeadlineExceeded
	}
	if _, limitErr := tx.Exec(ctx, limit, serverMillis); limitErr != nil {
		return row, false, limitErr
	}
	row, err = compare(ctx, tx, cell)
	return row, false, err
}

func compareRecallConfigured(ctx context.Context, tx pgx.Tx, seed recallSeed, index recallIndexReport, ground recallGround, vector []float32, k, threshold int) (recallObservation, error) {
	store := *seed.f.store
	store.client, store.exactScopeMax = seed.f.serving, threshold
	start := time.Now()
	chunks, detail, err := store.QueryFilteredDetailed(ctx, vector, k, ground.filter)
	elapsed := time.Since(start)
	row := recallObservation{Status: detail.Status, Path: string(detail.Strategy)}
	if err != nil {
		return row, err
	}
	wantProbe := threshold > 0
	wantExact := wantProbe && ground.eligible <= threshold
	if detail.Probed != wantProbe || wantProbe && detail.Candidates != min(ground.eligible, threshold+1) ||
		(detail.Strategy == SearchStrategyExact) != wantExact {
		return row, errors.New("configured strategy disagrees with the bounded exact-scope probe")
	}
	row.Latency = &elapsed
	if detail.Strategy == SearchStrategyHNSW {
		if _, settingsErr := tx.Exec(ctx, "SET LOCAL hnsw.iterative_scan = strict_order"); settingsErr != nil {
			return row, settingsErr
		}
		query, args, _ := searchSQL(store.table, ground.literal, k, ground.filter, wantProbe)
		uses, explainErr := explainRecall(ctx, tx, store.table, index.Name, query, args)
		if explainErr != nil {
			return row, explainErr
		}
		if !uses {
			row.Path = recallNonANN
		}
	}
	row.Score, err = compareRecallHits(ctx, tx, store.table, ground, chunks, k)
	if err == nil && wantExact && row.Score.Strict != nil && *row.Score.Strict != 1 {
		err = errors.New("configured exact ranking disagrees with the exhaustive oracle")
	}
	return row, err
}

func absentRecallDestination(path string) error {
	if path == "" {
		return nil
	}
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return errors.New("recall report destination must not exist")
}

func publishRecallReport(ctx context.Context, path string, report recallReport) error {
	if path == "" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".quarry-recall-*")
	if err != nil {
		return fmt.Errorf("stage recall report: %w", err)
	}
	defer func() { _ = os.Remove(file.Name()) }()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(report)
	closeErr := file.Close()
	if err := errors.Join(encodeErr, closeErr, ctx.Err()); err != nil {
		return err
	}
	if err := publishRecallFile(file.Name(), path); err != nil {
		return fmt.Errorf("publish recall report: %w", err)
	}
	return nil
}
