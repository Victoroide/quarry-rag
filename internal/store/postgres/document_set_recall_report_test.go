package postgres

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestRecallPersistentOracleTimeout(t *testing.T) {
	seed := recallSeed{f: &recallFixture{store: &VectorStore{table: "chunk_vectors"}}, count: recallCorpusRows, indexed: recallCorpusRows}
	index, variants, _ := recallIndexConfig(seed, "hnsw")
	cells := recallCells(recallSelection{Members: 100, Eligible: 100}, seed, variants, index.Method)
	require.Len(t, cells, 39)
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	opens := 0
	err := runRecallQuery(ctx, cells, 0, recallOracleBudget, func(ctx context.Context) (pgx.Tx, error) {
		opens++
		timer := time.NewTimer(60 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, &pgconn.PgError{Code: "57014"}
		}
	}, func(context.Context, pgx.Tx, *recallCell) (recallObservation, error) {
		t.Fatal("comparison ran without a prepared oracle")
		return recallObservation{}, nil
	})
	require.NoError(t, err, "oracle opened %d times", opens)
	require.Equal(t, 2, opens)
	for _, cell := range cells {
		require.Equal(t, []recallObservation{recallTimeoutObservation()}, cell.Observations)
	}
}

func TestRecallOraclePreparationBudget(t *testing.T) {
	remaining := 200 * time.Millisecond
	tx, err := openRecallOracle(t.Context(), &remaining, func(ctx context.Context) (pgx.Tx, error) {
		timer := time.NewTimer(40 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return &recallOracleTestTx{}, nil
		}
	})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(t.Context()))
	require.Positive(t, remaining)
	require.LessOrEqual(t, remaining, 160*time.Millisecond)

	var recoveryDeadline time.Time
	failed := &recallOracleTestTx{}
	opens := 0
	tx, err = openRecallOracle(t.Context(), &remaining, func(ctx context.Context) (pgx.Tx, error) {
		opens++
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		if opens == 1 {
			recoveryDeadline = deadline
			require.LessOrEqual(t, time.Until(deadline), remaining)
			return failed, &pgconn.PgError{Code: "57014"}
		}
		require.Equal(t, recoveryDeadline, deadline)
		require.True(t, failed.rolledBack)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, tx)
	require.Equal(t, 2, opens)
	require.Negative(t, remaining)
}

func TestRecallOracleFailurePreventsPublication(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"unexpected PostgreSQL error", &pgconn.PgError{Code: "42P01"}},
		{"parent canceled", context.Canceled},
		{"parent deadline", context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if errors.Is(test.err, context.DeadlineExceeded) {
				var end context.CancelFunc
				ctx, end = context.WithTimeout(ctx, 200*time.Millisecond)
				defer end()
			}
			previous := recallObservation{Status: SearchStatusOK}
			cells := []recallCell{{Observations: []recallObservation{previous}}, {}}
			path := filepath.Join(t.TempDir(), "report.json")
			failed := &recallOracleTestTx{}
			opens := 0
			err := completeRecallReport(ctx, path, &recallReport{}, func(*recallReport) error {
				return runRecallQuery(ctx, cells, 0, recallOracleBudget, func(ctx context.Context) (pgx.Tx, error) {
					opens++
					if opens == 1 {
						return failed, &pgconn.PgError{Code: "57014"}
					}
					require.True(t, failed.rolledBack)
					switch {
					case errors.Is(test.err, context.Canceled):
						cancel()
					case errors.Is(test.err, context.DeadlineExceeded):
						<-ctx.Done()
					}
					return nil, test.err
				}, func(context.Context, pgx.Tx, *recallCell) (recallObservation, error) {
					t.Fatal("comparison ran without a prepared oracle")
					return recallObservation{}, nil
				})
			})
			require.ErrorIs(t, err, test.err)
			require.Equal(t, 2, opens)
			require.Equal(t, []recallObservation{previous}, cells[0].Observations)
			require.Empty(t, cells[1].Observations)
			require.NoFileExists(t, path)
		})
	}
}

type recallOracleTestTx struct {
	pgx.Tx
	rolledBack bool
}

func (tx *recallOracleTestTx) Rollback(context.Context) error {
	tx.rolledBack = true
	return nil
}

func TestRecallOptInAndPublication(t *testing.T) {
	for _, value := range []string{"", "0", "true", "2"} {
		paths, enabled, err := getRecallPaths(func(key string) string {
			require.Equal(t, recallOptIn, key, "disabled evaluation must not access other inputs")
			return value
		})
		require.NoError(t, err)
		require.False(t, enabled)
		require.Empty(t, paths)
	}
	_, enabled, err := getRecallPaths(func(key string) string {
		if key == recallOptIn {
			return "1"
		}
		return ""
	})
	require.True(t, enabled)
	require.Error(t, err)

	ctx := t.Context()
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	const name = "complete.json"
	path := filepath.Join(dir, name)
	want := recallReport{Version: 1}
	require.NoError(t, absentRecallDestination(path))
	require.NoError(t, publishRecallReport(ctx, path, want))
	before, err := root.ReadFile(name)
	require.NoError(t, err)
	var actual recallReport
	require.NoError(t, json.Unmarshal(before, &actual))
	require.Equal(t, want, actual)

	require.Error(t, absentRecallDestination(path))
	require.Error(t, publishRecallReport(ctx, path, recallReport{Version: 2}))
	after, err := root.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Error(t, publishRecallReport(ctx, filepath.Join(dir, "missing", "report.json"), want))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, publishRecallReport(canceled, filepath.Join(dir, "canceled.json"), want), context.Canceled)
	files, err := fs.ReadDir(root.FS(), ".")
	require.NoError(t, err)
	require.Len(t, files, 1)

	concurrent := filepath.Join(dir, "race.json")
	finished := make(chan error, 2)
	for range 2 {
		go func() { finished <- publishRecallReport(ctx, concurrent, want) }()
	}
	winners := 0
	for range 2 {
		if err := <-finished; err == nil {
			winners++
		} else {
			require.True(t, errors.Is(err, os.ErrExist))
		}
	}
	require.Equal(t, 1, winners)
}

func TestRecallMatrixAndThresholdGrouping(t *testing.T) {
	seed := recallSeed{f: &recallFixture{store: &VectorStore{table: "chunk_vectors"}}, count: recallCorpusRows, indexed: recallCorpusRows}
	set := recallSelection{Label: "hash", Members: 100, Eligible: 100}
	hnsw, variants, _ := recallIndexConfig(seed, "hnsw")
	cells := recallCells(set, seed, variants, hnsw.Method)
	require.Len(t, cells, 39)
	require.Equal(t, cells, recallCells(set, seed, variants, hnsw.Method))
	require.Equal(t, .0005, cells[0].MemberRatio)
	ivf, variants, _ := recallIndexConfig(seed, "ivfflat")
	require.Equal(t, 200, ivf.Options["lists"])
	require.Len(t, recallCells(set, seed, variants, ivf.Method), 24)
	require.Equal(t, 15, variants[len(variants)-1].Search)
	require.Equal(t, 60, variants[len(variants)-1].MaxScan)

	baseline := recallCell{Set: set, K: 50, Threshold: new(0)}
	baseline.Observations = []recallObservation{
		{Path: "hnsw", Score: &recallScore{Strict: new(.8)}},
		{Path: recallNonANN, Score: &recallScore{Strict: new(1.0)}},
	}
	exact := recallCell{Set: set, K: 50, Threshold: new(100)}
	for range recallMeasurements {
		exact.Observations = append(exact.Observations, recallObservation{
			Path: string(SearchStrategyExact), Status: SearchStatusOK,
			Latency: new(recallCellBudget / 30_000), Score: &recallScore{Strict: new(1.0)},
		})
	}
	exact.summarize()
	report := recallReport{Layouts: []recallLayout{{Indexes: []recallIndexReport{{Cells: []recallCell{baseline, exact}}}}}}
	require.Equal(t, 100, recommendRecallThreshold(report.thresholdEvidence()))
	exact.Observations = append(exact.Observations[:recallMeasurements-1], recallTimeoutObservation())
	exact.summarize()
	exact.Set.Label = "correlated"
	report.Layouts = append(report.Layouts, recallLayout{Indexes: []recallIndexReport{{Cells: []recallCell{exact}}}})
	require.Zero(t, recommendRecallThreshold(report.thresholdEvidence()), "a timeout in another layout disqualifies the threshold")
}

type recallContainerInfo struct {
	Image    string `json:"image,omitempty"`
	Memory   int64  `json:"requested_memory_bytes,omitempty"`
	NanoCPUs int64  `json:"requested_cpu_nanocores,omitempty"`
}

func (c *recallContainerInfo) Customize(req *testcontainers.GenericContainerRequest) error {
	c.Image = req.Image
	configure := req.HostConfigModifier
	req.HostConfigModifier = func(config *container.HostConfig) {
		if configure != nil {
			configure(config)
		}
		c.Memory, c.NanoCPUs = config.Memory, config.NanoCPUs
	}
	return nil
}

type recallRunInfo struct {
	FixtureMode     string              `json:"fixture_mode"`
	Container       recallContainerInfo `json:"container"`
	Repository      string              `json:"repository_revision"`
	GoVersion       string              `json:"go_version"`
	PostgresVersion string              `json:"postgres_version"`
	PostgresBuild   string              `json:"postgres_build"`
	VectorVersion   string              `json:"pgvector_version"`
	ClientOS        string              `json:"client_os"`
	ClientArch      string              `json:"client_arch"`
	ClientCPUs      int                 `json:"client_logical_cpus"`
	ClientRAM       *uint64             `json:"client_memory_bytes"`
	ServingSettings map[string]string   `json:"effective_serving_settings"`
	IndexSettings   map[string]string   `json:"index_build_settings"`
	QueryExecMode   string              `json:"query_exec_mode"`
}

func recallEnvironment(ctx context.Context, f *recallFixture, containerInfo recallContainerInfo) (recallRunInfo, error) {
	mode := "configured test service"
	if containerInfo.Image != "" {
		mode = "disposable pgvector container"
	}
	info := recallRunInfo{
		FixtureMode: mode, Container: containerInfo, GoVersion: runtime.Version(),
		ClientOS: runtime.GOOS, ClientArch: runtime.GOARCH, ClientCPUs: runtime.NumCPU(), ClientRAM: recallRAM(ctx),
		ServingSettings: make(map[string]string),
		IndexSettings:   map[string]string{"maintenance_work_mem": "1GB", "max_parallel_maintenance_workers": "0"},
		QueryExecMode:   f.serving.Pool.Config().ConnConfig.DefaultQueryExecMode.String(),
	}
	revision, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		return info, err
	}
	info.Repository = strings.TrimSpace(string(revision))
	err = f.owner.Pool.QueryRow(ctx, `
SELECT current_setting('server_version'), version(), extversion FROM pg_extension WHERE extname='vector'`).
		Scan(&info.PostgresVersion, &info.PostgresBuild, &info.VectorVersion)
	if err != nil {
		return info, err
	}
	err = inRecallSnapshot(ctx, f.serving, func(tx pgx.Tx) error {
		// Load pgvector in this backend before reading its GUCs.
		if _, vectorErr := tx.Exec(ctx, "SELECT '[1]'::vector"); vectorErr != nil {
			return vectorErr
		}
		settings := []string{"work_mem", "statement_timeout", "enable_seqscan", "enable_indexscan", "enable_bitmapscan",
			"random_page_cost", "effective_cache_size", "max_parallel_workers_per_gather",
			"hnsw.ef_search", "hnsw.max_scan_tuples", "hnsw.scan_mem_multiplier",
			"ivfflat.probes", "ivfflat.max_probes"}
		rows, queryErr := tx.Query(ctx, "SELECT name,current_setting(name) FROM unnest($1::text[]) AS s(name)", settings)
		if queryErr != nil {
			return queryErr
		}
		defer rows.Close()
		for rows.Next() {
			var name, value string
			if scanErr := rows.Scan(&name, &value); scanErr != nil {
				return scanErr
			}
			info.ServingSettings[name] = value
		}
		info.ServingSettings["hnsw.iterative_scan"] = "strict_order"
		return rows.Err()
	})
	return info, err
}

func recallRAM(ctx context.Context) *uint64 {
	switch runtime.GOOS {
	case "darwin":
		output, err := exec.CommandContext(ctx, "sysctl", "-n", "hw.memsize").Output()
		value, parseErr := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64)
		if err == nil && parseErr == nil {
			return &value
		}
	case "linux":
		file, err := os.Open("/proc/meminfo")
		if err != nil {
			return nil
		}
		defer func() { _ = file.Close() }()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) != 3 || fields[0] != "MemTotal:" || fields[2] != "kB" {
				continue
			}
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				return new(value * 1024)
			}
		}
	}
	return nil
}
