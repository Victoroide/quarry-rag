package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

const (
	recallNonANN      = "exact/non-ann"
	recallUnavailable = "unavailable"
)

func TestRecallExplainClassification(t *testing.T) {
	doc := []byte(`[{"Plan":{"Node Type":"Sort","Plans":[{"Node Type":"Index Scan","Relation Name":"chunk_vectors","Index Name":"idx_chunk_vectors_embedding"}]}}]`)
	uses, err := recallPlanUsesIndex(doc, "chunk_vectors", "idx_chunk_vectors_embedding")
	require.NoError(t, err)
	require.True(t, uses)
	uses, err = recallPlanUsesIndex(doc, "chunk_vectors", "another_index")
	require.NoError(t, err)
	require.False(t, uses)
	uses, err = recallPlanUsesIndex(doc, "another_table", "idx_chunk_vectors_embedding")
	require.NoError(t, err)
	require.False(t, uses)
	_, err = recallPlanUsesIndex([]byte(`[{}]`), "chunk_vectors", "idx_chunk_vectors_embedding")
	require.Error(t, err)
}

type recallPlan struct {
	Node     string       `json:"Node Type"`
	Relation string       `json:"Relation Name"`
	Index    string       `json:"Index Name"`
	Plans    []recallPlan `json:"Plans"`
}

func recallPlanUsesIndex(doc []byte, table, index string) (bool, error) {
	var plans []struct{ Plan recallPlan }
	if err := json.Unmarshal(doc, &plans); err != nil || len(plans) != 1 || plans[0].Plan.Node == "" {
		return false, errors.New("invalid PostgreSQL EXPLAIN JSON")
	}
	return plans[0].Plan.usesIndex(table, index), nil
}

func (p recallPlan) usesIndex(table, index string) bool {
	if p.Relation == table && p.Index == index && p.Node == "Index Scan" {
		return true
	}
	return slices.ContainsFunc(p.Plans, func(child recallPlan) bool { return child.usesIndex(table, index) })
}

func explainRecall(ctx context.Context, tx pgx.Tx, table, index, query string, args []any) (bool, error) {
	var plan []byte
	if err := tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+query, args...).Scan(&plan); err != nil {
		return false, err
	}
	return recallPlanUsesIndex(plan, table, index)
}

func inRecallSnapshot(ctx context.Context, client *Client, work func(pgx.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := beginRecallSnapshot(ctx, client)
	if err != nil {
		return err
	}
	defer func() { _ = rollbackRecall(ctx, tx) }()
	return work(tx)
}

func beginRecallSnapshot(ctx context.Context, client *Client) (pgx.Tx, error) {
	return client.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
}

func rollbackRecall(parent context.Context, tx pgx.Tx) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	return tx.Rollback(ctx)
}

func recallTimedOut(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "57014"
	}
	return errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err)
}

func probeRecall(ctx context.Context, tx pgx.Tx, table string, indexed int, filter domain.VectorFilter) (int, error) {
	query, args, matches := probeSQL(table, indexed+1, filter)
	if !matches {
		return 0, nil
	}
	var eligible int
	if err := tx.QueryRow(ctx, query, args...).Scan(&eligible); err != nil {
		return 0, err
	}
	if eligible > indexed {
		return 0, errors.New("eligible relation exceeds the verified number of indexed rows")
	}
	return eligible, nil
}

type recallGround struct {
	filter   domain.VectorFilter
	literal  string
	eligible int
	chunks   []domain.ScoredChunk
	hits     []recallHit
}

func buildRecallGround(ctx context.Context, tx pgx.Tx, store *VectorStore, embedding []float32, indexed int, filter domain.VectorFilter) (recallGround, error) {
	ground := recallGround{filter: filter}
	literal, err := vectorLiteral(embedding, store.dimensions)
	if err != nil {
		return ground, err
	}
	ground.literal = literal
	ground.eligible, err = probeRecall(ctx, tx, store.table, indexed, filter)
	if err != nil {
		return ground, err
	}
	query, args, matches := exactSQL(store.table, literal, 50, indexed+1, filter)
	if !matches {
		return ground, nil
	}
	ground.chunks, err = scanScoredChunks(ctx, tx, query, args...)
	if err != nil {
		return ground, err
	}
	ground.hits, err = recallCandidateDistances(ctx, tx, store.table, ground, ground.chunks)
	if err != nil {
		return ground, err
	}
	if len(ground.hits) != min(50, ground.eligible) || !validRecallGround(ground.hits) {
		return ground, errors.New("exhaustive ranking disagrees with the eligible relation or tie order")
	}
	return ground, nil
}

func validRecallGround(hits []recallHit) bool {
	for i, hit := range hits {
		if !hit.Eligible {
			return false
		}
		if i > 0 {
			prev := hits[i-1]
			if prev.Distance > hit.Distance || (prev.Distance == hit.Distance && prev.ID >= hit.ID) {
				return false
			}
		}
	}
	return true
}

// Test-only lookup of raw distances and eligibility for exactly the returned
// keys. Reuse the same completion, label and grant/set predicates as exactSQL.
// A missing key has no scope credit, even when its document was eligible.
func recallCandidateDistances(ctx context.Context, tx pgx.Tx, table string, ground recallGround, chunks []domain.ScoredChunk) ([]recallHit, error) {
	if len(chunks) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		ids = append(ids, string(chunk.Chunk.ID))
	}
	filter, args, ok := appendFilterPredicates([]any{ground.literal, ids}, ground.filter, true)
	join, args, authorized := authorizedJoin(args, ground.filter.Authorized)
	if !ok || !authorized {
		return nil, errors.New("an impossible filter returned scored chunks")
	}
	query := fmt.Sprintf(`
SELECT v.chunk_id, v.embedding <=> $1::text::vector
FROM %s v JOIN documents d ON d.id=v.document_id AND d.status='completed'%s
WHERE v.chunk_id=ANY($2::text[])%s`, pgx.Identifier{table}.Sanitize(), join, filter)
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	distances, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (recallHit, error) {
		hit := recallHit{Eligible: true}
		scanErr := row.Scan(&hit.ID, &hit.Distance)
		if scanErr == nil && (math.IsNaN(hit.Distance) || math.IsInf(hit.Distance, 0)) {
			scanErr = errors.New("nonfinite pgvector cosine distance")
		}
		return hit, scanErr
	})
	if err != nil {
		return nil, err
	}
	index := make(map[string]recallHit, len(distances))
	for _, hit := range distances {
		if _, duplicate := index[hit.ID]; duplicate {
			return nil, errors.New("eligible scope contains a repeated chunk")
		}
		index[hit.ID] = hit
	}
	hits := make([]recallHit, 0, len(ids))
	for _, id := range ids {
		if hit, ok := index[id]; ok {
			hits = append(hits, hit)
		} else {
			hits = append(hits, recallHit{ID: id})
		}
	}
	return hits, nil
}

type recallANN struct {
	Method           string `json:"method"`
	Search           int    `json:"ef_search_or_probes"`
	Iteration        string `json:"iterative_scan"`
	MaxScan          int    `json:"max_scan_tuples_or_probes"`
	MemoryMultiplier int    `json:"scan_memory_multiplier,omitempty"`
	Forced           bool   `json:"forced_seqscan_off"`
}

func (r recallANN) apply(ctx context.Context, tx pgx.Tx) error {
	settings := map[string]string{
		r.Method + ".iterative_scan": r.Iteration,
	}
	switch r.Method {
	case "hnsw":
		settings["hnsw.ef_search"] = strconv.Itoa(r.Search)
		settings["hnsw.max_scan_tuples"] = strconv.Itoa(r.MaxScan)
		settings["hnsw.scan_mem_multiplier"] = strconv.Itoa(r.MemoryMultiplier)
	case "ivfflat":
		settings["ivfflat.probes"] = strconv.Itoa(r.Search)
		settings["ivfflat.max_probes"] = strconv.Itoa(r.MaxScan)
	default:
		return errors.New("unknown pgvector recall method")
	}
	if r.Forced {
		settings["enable_seqscan"] = "off"
	}
	names := make([]string, 0, len(settings))
	values := make([]string, 0, len(settings))
	for setting, value := range settings {
		names = append(names, setting)
		values = append(values, value)
	}
	_, err := tx.Exec(ctx, "SELECT set_config(name,value,true) FROM unnest($1::text[],$2::text[]) AS s(name,value)", names, values)
	return err
}

type recallObservation struct {
	Path    string         `json:"observed_path"`
	Status  SearchStatus   `json:"status"`
	Latency *time.Duration `json:"latency_ns"`
	Score   *recallScore   `json:"score"`
}

func compareRecallANN(ctx context.Context, tx pgx.Tx, store *VectorStore, ground recallGround, index string, settings recallANN, k int) (recallObservation, error) {
	row := recallObservation{Status: SearchStatusOK, Path: recallNonANN}
	if settingsErr := settings.apply(ctx, tx); settingsErr != nil {
		return row, settingsErr
	}
	query, args, matches := searchSQL(store.table, ground.literal, k, ground.filter, true)
	if !matches {
		return row, errors.New("ANN comparison requires a resolved filter")
	}
	uses, err := explainRecall(ctx, tx, store.table, index, query, args)
	if err != nil {
		return row, err
	}
	switch {
	case uses:
		row.Path = settings.Method
	case settings.Forced:
		row.Path = recallUnavailable
		return row, nil
	}
	start := time.Now()
	chunks, err := scanScoredChunks(ctx, tx, query, args...)
	elapsed := time.Since(start)
	if err != nil {
		return row, err
	}
	if len(chunks) < k {
		row.Status = SearchStatusPartial
	}
	row.Latency = &elapsed
	row.Score, err = compareRecallHits(ctx, tx, store.table, ground, chunks, k)
	return row, err
}

func compareRecallHits(ctx context.Context, tx pgx.Tx, table string, ground recallGround, chunks []domain.ScoredChunk, k int) (*recallScore, error) {
	candidates, err := recallCandidateDistances(ctx, tx, table, ground, chunks)
	if err != nil {
		return nil, err
	}
	score := scoreRecall(ground.hits[:min(k, len(ground.hits))], candidates, k, ground.eligible)
	switch {
	case score.Unauthorized != 0:
		return nil, errors.New("ranking returned chunks outside the exact eligible scope")
	case score.Returned != score.EligibleHits || score.Returned > k:
		return nil, errors.New("ranking returned repeated or too many chunks")
	}
	return &score, nil
}
