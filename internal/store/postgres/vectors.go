package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const maxExactScopeLimit = 100000

// SearchStrategy identifies the execution strategy selected by pgvector search.
type SearchStrategy string

// Supported pgvector search strategies.
const (
	SearchStrategyExact SearchStrategy = "exact"
	SearchStrategyHNSW  SearchStrategy = "hnsw"
	SearchStrategyProbe SearchStrategy = "probe"
)

// SearchStatus classifies the completion state of a pgvector search.
type SearchStatus string

// Supported pgvector search outcome statuses.
const (
	SearchStatusOK       SearchStatus = "ok"
	SearchStatusPartial  SearchStatus = "partial"
	SearchStatusTimeout  SearchStatus = "timeout"
	SearchStatusCanceled SearchStatus = "canceled"
	SearchStatusError    SearchStatus = "error"
)

// SearchReport summarizes the strategy and completion status of a pgvector query.
type SearchReport struct {
	Strategy       SearchStrategy
	Status         SearchStatus
	ProbeAttempted bool // The count statement was attempted.
	Probed         bool // The count statement succeeded.
	Candidates     int  // Valid only if Probed is true.
}

// VectorOption customizes pgvector VectorStore behavior.
type VectorOption func(*VectorStore)

// WithExactScopeMax enables exact cosine search for filtered queries whose
// matching completed chunk count does not exceed limit. Zero disables it.
func WithExactScopeMax(limit int) VectorOption {
	return func(s *VectorStore) { s.exactScopeMax = limit }
}

// VectorStore searches pgvector embeddings committed with document completion.
type VectorStore struct {
	client        *Client
	dimensions    int
	exactScopeMax int
}

var (
	_ domain.VectorStore                      = (*VectorStore)(nil)
	_ domain.FilteredVectorStoreWithExactness = (*VectorStore)(nil)
	_ domain.VectorDeleter                    = (*VectorStore)(nil)
	_ domain.TransactionalVectorStore         = (*VectorStore)(nil)
)

// NewVectorStore validates or initializes the optional schema under the migration advisory lock.
// The client must have applied its base migrations before calling this function.
func NewVectorStore(ctx context.Context, client *Client, dimensions int, opts ...VectorOption) (*VectorStore, error) {
	validDimensions, err := ValidateVectorDimensions(dimensions)
	if err != nil {
		return nil, err
	}
	vs := &VectorStore{client: client, dimensions: dimensions}
	for _, opt := range opts {
		if opt != nil {
			opt(vs)
		}
	}
	if vs.exactScopeMax < 0 || vs.exactScopeMax > maxExactScopeLimit {
		return nil, fmt.Errorf("postgres: exact scope limit must be between 0 and %d", maxExactScopeLimit)
	}
	if err := client.initVectors(ctx, dimensions); err != nil {
		return nil, fmt.Errorf("postgres: initialize vectors: %w", err)
	}
	if vs.exactScopeMax > 0 {
		if err := client.ensureExactScopeIndexes(ctx); err != nil {
			return nil, fmt.Errorf("postgres: ensure exact-scope indexes: %w", err)
		}
	}
	client.vectorDimensions.Store(validDimensions)
	return vs, nil
}

// ValidateVectorDimensions returns the bounded dimension supported by the pgvector index.
func ValidateVectorDimensions(dimensions int) (int32, error) {
	if dimensions < 1 || dimensions > 2000 {
		return 0, fmt.Errorf("postgres: vector dimensions must be between 1 and 2000")
	}
	return int32(dimensions), nil
}

func (c *Client) initVectors(ctx context.Context, dimensions int) error {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout = 0; SET LOCAL lock_timeout = 0`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout = '15s'`); err != nil {
		return err
	}
	if extensionErr := ensureVectorExtension(ctx, tx); extensionErr != nil {
		return extensionErr
	}
	var exists bool
	if relationErr := tx.QueryRow(ctx, `SELECT to_regclass(format('%I.chunk_vectors', current_schema())) IS NOT NULL`).Scan(&exists); relationErr != nil {
		return relationErr
	}
	if !exists {
		if createErr := createVectorTable(ctx, tx, dimensions); createErr != nil {
			return createErr
		}
	}
	var storedDimensions int
	var vectorType bool
	if dimensionErr := tx.QueryRow(ctx, `SELECT atttypmod, atttypid=to_regtype('vector')
		FROM pg_attribute WHERE attrelid=to_regclass(format('%I.chunk_vectors',current_schema()))
		AND attname='embedding' AND NOT attisdropped`).Scan(&storedDimensions, &vectorType); dimensionErr != nil {
		return dimensionErr
	}
	if !vectorType || storedDimensions != dimensions {
		return fmt.Errorf("embedding dimension mismatch: existing %d, configured %d", storedDimensions, dimensions)
	}
	if indexErr := ensureVectorIndexes(ctx, tx); indexErr != nil {
		return indexErr
	}
	return tx.Commit(ctx)
}

func ensureVectorExtension(ctx context.Context, tx pgx.Tx) error {
	const versionQuery = `SELECT extversion FROM pg_extension WHERE extname='vector'`
	var version string
	err := tx.QueryRow(ctx, versionQuery).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, err = tx.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`); err != nil {
			return err
		}
		err = tx.QueryRow(ctx, versionQuery).Scan(&version)
	}
	if err != nil {
		return err
	}
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return fmt.Errorf("unrecognized pgvector version")
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	if majorErr != nil || minorErr != nil || major < 0 || (major == 0 && minor < 8) {
		return fmt.Errorf("pgvector 0.8 or later is required")
	}
	return nil
}

func createVectorTable(ctx context.Context, tx pgx.Tx, dimensions int) error {
	schema := fmt.Sprintf(`CREATE TABLE chunk_vectors (
  chunk_id TEXT PRIMARY KEY,
  document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL,
  doc_type TEXT NOT NULL,
  collection TEXT NOT NULL DEFAULT '',
  text TEXT NOT NULL,
  kind TEXT,
  quality TEXT,
  token_count INTEGER NOT NULL,
  embedding vector(%d) NOT NULL
 )`, dimensions)
	_, err := tx.Exec(ctx, schema)
	return err
}

func ensureVectorIndexes(ctx context.Context, tx pgx.Tx) error {
	const check = `WITH expected(index_name, access_method, column_name, operator_class) AS (
		VALUES ('idx_chunk_vectors_embedding','hnsw','embedding','vector_cosine_ops'),
		       ('idx_chunk_vectors_document_id','btree','document_id','uuid_ops'),
		       ('idx_chunk_vectors_collection','btree','collection','text_ops')
	)
	SELECT count(*) = (SELECT count(*) FROM expected)
	FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid
	JOIN pg_am am ON am.oid=idx.relam
	JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
	JOIN pg_opclass op ON op.oid=i.indclass[0]
	JOIN expected e ON (idx.relname,am.amname,a.attname,op.opcname) =
		(e.index_name,e.access_method,e.column_name,e.operator_class)
	WHERE i.indrelid=to_regclass(format('%I.chunk_vectors',current_schema()))
	  AND i.indisvalid AND i.indisready AND i.indislive AND i.indnatts=1
	  AND i.indpred IS NULL AND i.indexprs IS NULL`
	var prepared bool
	if err := tx.QueryRow(ctx, check).Scan(&prepared); err != nil || prepared {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_chunk_vectors_embedding ON chunk_vectors USING hnsw (embedding vector_cosine_ops);
 CREATE INDEX IF NOT EXISTS idx_chunk_vectors_document_id ON chunk_vectors(document_id);
 CREATE INDEX IF NOT EXISTS idx_chunk_vectors_collection ON chunk_vectors(collection)`); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, check).Scan(&prepared); err != nil {
		return err
	}
	if !prepared {
		return fmt.Errorf("vector indexes do not match the required schema")
	}
	return nil
}

// TransactionalVectors marks vectors persisted by CompleteJob instead of PersistStage.
func (*VectorStore) TransactionalVectors() {}

func vectorLiteral(embedding []float32, dimensions int) (string, error) {
	if len(embedding) != dimensions {
		return "", fmt.Errorf("embedding dimension %d does not match configured dimension %d", len(embedding), dimensions)
	}
	var b strings.Builder
	b.WriteByte('[')
	nonzero := false
	for i, value := range embedding {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return "", fmt.Errorf("embedding contains a non-finite value")
		}
		nonzero = nonzero || value != 0
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(value), 'g', -1, 32))
	}
	if !nonzero {
		return "", fmt.Errorf("cosine embedding must be nonzero")
	}
	b.WriteByte(']')
	return b.String(), nil
}

func insertVectors(ctx context.Context, tx pgx.Tx, chunks []domain.Chunk, dimensions int) error {
	literals := make([]string, len(chunks))
	for i, ch := range chunks {
		literal, err := vectorLiteral(ch.Embedding, dimensions)
		if err != nil {
			return err
		}
		literals[i] = literal
	}
	batch := &pgx.Batch{}
	for i, ch := range chunks {
		docType, _ := ch.Metadata["doc_type"].(string)
		if docType == "" {
			docType = domain.DefaultDocType
		}
		collection, _ := ch.Metadata["collection"].(string)
		kind, _ := ch.Metadata["kind"].(string)
		quality, _ := ch.Metadata["quality"].(string)
		batch.Queue(`INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,collection,text,kind,quality,token_count,embedding)
   VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),$9,$10::text::vector)
   ON CONFLICT(chunk_id) DO UPDATE SET document_id=EXCLUDED.document_id,chunk_index=EXCLUDED.chunk_index,doc_type=EXCLUDED.doc_type,collection=EXCLUDED.collection,text=EXCLUDED.text,kind=EXCLUDED.kind,quality=EXCLUDED.quality,token_count=EXCLUDED.token_count,embedding=EXCLUDED.embedding`,
			string(ch.ID), string(ch.DocumentID), ch.Index, docType, collection, ch.Text, kind, quality, ch.TokenCount, literals[i])
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("insert vectors: %w", err)
	}
	return nil
}

// Upsert atomically inserts or replaces vectors in the default namespace.
func (s *VectorStore) Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error {
	if namespace != "" {
		return fmt.Errorf("postgres: vector writes require the default namespace")
	}
	tx, err := s.client.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = insertVectors(ctx, tx, chunks, s.dimensions); err != nil {
		return fmt.Errorf("postgres: upsert vectors: %w", err)
	}
	return tx.Commit(ctx)
}

// Query searches the default namespace or scopes a legacy namespace to its document.
func (s *VectorStore) Query(ctx context.Context, namespace string, embedding []float32, topK int, docType string) ([]domain.ScoredChunk, error) {
	filter := domain.VectorFilter{DocType: docType}
	if namespace != "" {
		filter.DocumentIDs = []string{namespace}
	}
	return s.QueryFiltered(ctx, embedding, topK, filter)
}

// QueryAllNamespaces searches the canonical default namespace.
func (s *VectorStore) QueryAllNamespaces(ctx context.Context, embedding []float32, topK int, docType string) ([]domain.ScoredChunk, error) {
	return s.QueryFiltered(ctx, embedding, topK, domain.VectorFilter{DocType: docType})
}

// QueryFiltered applies collection, type and ID constraints before ranking completed documents.
func (s *VectorStore) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	chunks, _, err := s.QueryFilteredDetailed(ctx, embedding, topK, filter)
	return chunks, err
}

// QueryFilteredWithExactness also reports whether exact ranking selected the results.
func (s *VectorStore) QueryFilteredWithExactness(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	chunks, report, err := s.QueryFilteredDetailed(ctx, embedding, topK, filter)
	return chunks, err == nil && report.Strategy == SearchStrategyExact, err
}

// QueryFilteredDetailed executes a scoped pgvector query and reports the selected
// search strategy and completion status. Calls rejected during input validation
// return SearchStatusError with an empty Strategy without touching PostgreSQL;
// impossible filters short-circuit with SearchStatusOK, an empty Strategy, and
// Probed=false. Single-statement paths (disabled or unfiltered) inherit the
// pool isolation level, whereas probed filtered paths use RepeatableRead for a
// consistent candidate count and ranking snapshot.
func (s *VectorStore) QueryFilteredDetailed(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, SearchReport, error) {
	if topK > 10000 {
		return nil, SearchReport{Status: SearchStatusError}, fmt.Errorf("postgres: topK must not exceed 10000")
	}
	if filter.MatchesNothing() {
		return []domain.ScoredChunk{}, SearchReport{Status: SearchStatusOK}, nil
	}
	literal, err := vectorLiteral(embedding, s.dimensions)
	if err != nil {
		return nil, SearchReport{Status: SearchStatusError}, err
	}
	// When probing, all ranking paths must use the probe's label eligibility.
	useProbe := s.exactScopeMax > 0 && hasScopeFilter(filter)
	searchQuery, searchArgs, ok := searchSQL(literal, topK, filter, useProbe)
	if !ok {
		return []domain.ScoredChunk{}, SearchReport{Status: SearchStatusOK}, nil
	}

	txOpts := pgx.TxOptions{AccessMode: pgx.ReadOnly}
	report := SearchReport{Strategy: SearchStrategyHNSW, Status: SearchStatusOK}
	if useProbe {
		txOpts.IsoLevel = pgx.RepeatableRead
		report.Strategy = SearchStrategyProbe
	}

	tx, err := s.client.Pool.BeginTx(ctx, txOpts)
	if err != nil {
		return finishVectorSearch(ctx, report, nil, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if useProbe {
		report.ProbeAttempted = true
		probeQuery, probeArgs, _ := probeSQL(s.exactScopeMax+1, filter)
		var candidates int
		if err = tx.QueryRow(ctx, probeQuery, probeArgs...).Scan(&candidates); err != nil {
			return finishVectorSearch(ctx, report, nil, err)
		}
		report.Probed = true
		report.Candidates = candidates
		if candidates <= s.exactScopeMax {
			report.Strategy = SearchStrategyExact
			if candidates == 0 {
				return finishVectorSearch(ctx, report, []domain.ScoredChunk{}, nil)
			}
			exactQuery, exactArgs, _ := exactSQL(literal, topK, s.exactScopeMax+1, filter)
			out, exactErr := scanScoredChunks(ctx, tx, exactQuery, exactArgs...)
			return finishVectorSearch(ctx, report, out, exactErr)
		}
	}

	report.Strategy = SearchStrategyHNSW
	if _, err = tx.Exec(ctx, `SET LOCAL hnsw.iterative_scan = strict_order`); err != nil {
		return finishVectorSearch(ctx, report, nil, err)
	}
	out, err := scanScoredChunks(ctx, tx, searchQuery, searchArgs...)
	if err == nil && topK > 0 && len(out) < topK {
		report.Status = SearchStatusPartial
	}
	return finishVectorSearch(ctx, report, out, err)
}

func scanScoredChunks(ctx context.Context, tx pgx.Tx, query string, args ...any) ([]domain.ScoredChunk, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.ScoredChunk, 0)
	for rows.Next() {
		var sc domain.ScoredChunk
		var docType, collection string
		var kind, quality *string
		if scanErr := rows.Scan(&sc.Chunk.ID, &sc.Chunk.DocumentID, &sc.Chunk.Index, &docType, &collection, &sc.Chunk.Text, &kind, &quality, &sc.Chunk.TokenCount, &sc.Score); scanErr != nil {
			return nil, scanErr
		}
		sc.Chunk.Metadata = map[string]any{"doc_type": docType}
		if collection != "" {
			sc.Chunk.Metadata["collection"] = collection
		}
		if kind != nil {
			sc.Chunk.Metadata["kind"] = *kind
		}
		if quality != nil {
			sc.Chunk.Metadata["quality"] = *quality
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func hasScopeFilter(filter domain.VectorFilter) bool {
	return filter.DocType != "" || filter.Collection != "" || filter.DocumentIDs != nil
}

func appendFilterPredicates(args []any, filter domain.VectorFilter, matchDocumentLabels bool) (predicates string, nextArgs []any, matches bool) {
	var b strings.Builder
	for _, f := range []struct{ vectorCol, docCol, value string }{
		{"v.doc_type", "d.doc_type", filter.DocType},
		{"v.collection", "d.collection", filter.Collection},
	} {
		if f.value != "" {
			args = append(args, f.value)
			n := len(args)
			fmt.Fprintf(&b, " AND %s=$%d", f.vectorCol, n)
			if matchDocumentLabels {
				fmt.Fprintf(&b, " AND %s=$%d", f.docCol, n)
			}
		}
	}
	if filter.DocumentIDs != nil {
		ids := make([]string, 0, len(filter.DocumentIDs))
		for _, id := range filter.DocumentIDs {
			if parsed, parseErr := uuid.Parse(id); parseErr == nil && parsed.String() == id {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return "", nil, false
		}
		args = append(args, ids)
		fmt.Fprintf(&b, " AND v.document_id=ANY($%d::uuid[])", len(args))
	}
	return b.String(), args, true
}

func probeSQL(limit int, filter domain.VectorFilter) (query string, args []any, matches bool) {
	predicates, args, ok := appendFilterPredicates(nil, filter, true)
	if !ok {
		return "", nil, false
	}
	args = append(args, limit)
	query = fmt.Sprintf(`SELECT count(*) FROM (
 SELECT 1
 FROM chunk_vectors v JOIN documents d ON d.id=v.document_id AND d.status='completed'
 WHERE TRUE%s
 LIMIT $%d
) s`, predicates, len(args))
	return query, args, true
}

func exactSQL(literal string, topK, scopeLimit int, filter domain.VectorFilter) (query string, args []any, matches bool) {
	predicates, args, ok := appendFilterPredicates([]any{literal}, filter, true)
	if !ok {
		return "", nil, false
	}
	args = append(args, scopeLimit)
	scopeArg := len(args)
	limitClause := ""
	if topK > 0 {
		args = append(args, topK)
		limitClause = fmt.Sprintf(" LIMIT $%d", len(args))
	}
	query = fmt.Sprintf(`WITH candidates AS MATERIALIZED (
 SELECT v.chunk_id,v.embedding <=> $1::text::vector AS distance
 FROM chunk_vectors v JOIN documents d ON d.id=v.document_id AND d.status='completed'
 WHERE TRUE%s
 LIMIT $%d
),
ranked AS (
 SELECT chunk_id,distance
 FROM candidates
 ORDER BY distance,chunk_id%s
)
SELECT v.chunk_id,v.document_id::text,v.chunk_index,v.doc_type,v.collection,v.text,v.kind,v.quality,v.token_count,1-r.distance
FROM ranked r JOIN chunk_vectors v ON v.chunk_id=r.chunk_id
ORDER BY r.distance,v.chunk_id`, predicates, scopeArg, limitClause)
	return query, args, true
}

func searchSQL(literal string, topK int, filter domain.VectorFilter, matchDocumentLabels bool) (query string, args []any, matches bool) {
	predicates, args, ok := appendFilterPredicates([]any{literal}, filter, matchDocumentLabels)
	if !ok {
		return "", nil, false
	}
	query = `SELECT v.chunk_id,v.document_id::text,v.chunk_index,v.doc_type,v.collection,v.text,v.kind,v.quality,v.token_count,v.embedding <=> $1::text::vector AS distance
 FROM chunk_vectors v JOIN documents d ON d.id=v.document_id AND d.status='completed' WHERE TRUE` + predicates
	if topK > 0 {
		args = append(args, topK)
		query += fmt.Sprintf(" ORDER BY distance LIMIT $%d", len(args))
	}
	query = `SELECT chunk_id,document_id,chunk_index,doc_type,collection,text,kind,quality,token_count,1-distance
 FROM (` + query + `) s ORDER BY distance,chunk_id`
	return query, args, true
}

func finishVectorSearch(ctx context.Context, report SearchReport, chunks []domain.ScoredChunk, err error) ([]domain.ScoredChunk, SearchReport, error) {
	if err != nil {
		report.Status, err = classifyAndWrapSearchError(ctx, err)
	}
	recordSearchTelemetry(ctx, report, len(chunks), err)
	return chunks, report, err
}

func classifyAndWrapSearchError(ctx context.Context, err error) (SearchStatus, error) {
	// Explicit caller cancellation or deadline takes precedence over server
	// SQLSTATE 57014, which covers statement timeouts and administrative cancellation.
	cause := ctx.Err()
	if cause == nil {
		var pgErr *pgconn.PgError
		switch {
		case errors.Is(err, context.Canceled):
			cause = context.Canceled
		case errors.Is(err, context.DeadlineExceeded), errors.As(err, &pgErr) && pgErr.Code == "57014":
			cause = context.DeadlineExceeded
		}
	}
	status := SearchStatusError
	switch cause {
	case context.Canceled:
		status = SearchStatusCanceled
	case context.DeadlineExceeded:
		status = SearchStatusTimeout
	}
	if cause != nil && !errors.Is(err, cause) {
		err = fmt.Errorf("%w: %w", cause, err)
	}
	return status, fmt.Errorf("postgres: query vectors: %w", err)
}

func recordSearchTelemetry(ctx context.Context, report SearchReport, results int, err error) {
	metrics.FromContext(ctx).PGVectorSearch.WithLabelValues(string(report.Strategy), string(report.Status)).Inc()
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		attrs := []attribute.KeyValue{
			attribute.String("quarry.vector.strategy", string(report.Strategy)),
			attribute.String("quarry.vector.status", string(report.Status)),
		}
		if report.Probed {
			attrs = append(attrs, attribute.Int("quarry.vector.scope_candidates", report.Candidates))
		}
		span.SetAttributes(attrs...)
	}
	if err != nil {
		slog.DebugContext(ctx, "pgvector query failed", "strategy", report.Strategy, "status", report.Status, "error", err)
		return
	}
	slog.DebugContext(ctx, "pgvector query completed", "strategy", report.Strategy, "status", report.Status, "results", results)
}

// DeleteByDocID removes all vectors belonging to a document.
func (s *VectorStore) DeleteByDocID(ctx context.Context, docID string) error {
	parsed, err := uuid.Parse(docID)
	if err != nil {
		return fmt.Errorf("postgres: invalid document ID: %w", err)
	}
	_, err = s.client.Pool.Exec(ctx, `DELETE FROM chunk_vectors WHERE document_id=$1`, parsed.String())
	return err
}

// DeleteAllVectorsInNamespace removes a document scope or the entire default namespace.
func (s *VectorStore) DeleteAllVectorsInNamespace(ctx context.Context, namespace string) error {
	if namespace != "" {
		return s.DeleteByDocID(ctx, namespace)
	}
	_, err := s.client.Pool.Exec(ctx, `DELETE FROM chunk_vectors`)
	return err
}

// Ping verifies that the vector table is available for search.
func (s *VectorStore) Ping(ctx context.Context) error {
	_, err := s.client.Pool.Exec(ctx, `SELECT 1 FROM chunk_vectors LIMIT 0`)
	return err
}
