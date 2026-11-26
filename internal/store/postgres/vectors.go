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
	table         string
}

var (
	_ domain.VectorStore                      = (*VectorStore)(nil)
	_ domain.FilteredVectorStoreWithExactness = (*VectorStore)(nil)
	_ domain.VectorDeleter                    = (*VectorStore)(nil)
	_ domain.TransactionalVectorStore         = (*VectorStore)(nil)
)

// NewVectorStore validates or initializes the optional schema under the migration advisory lock.
// The client must have applied its base migrations before calling this function.
// Job completion writes to the fixed chunk_vectors table.
func NewVectorStore(ctx context.Context, client *Client, dimensions int, opts ...VectorOption) (*VectorStore, error) {
	store, err := CreateGenerationVectorStore(ctx, client, dimensions, LegacyGeneration, opts...)
	if err != nil {
		return nil, err
	}
	store.RouteCompletion()
	return store, nil
}

// CreateGenerationVectorStore creates the vector table of a generation and
// returns a store over it. It never changes where this process completes jobs.
func CreateGenerationVectorStore(ctx context.Context, client *Client, dimensions int, generation string, opts ...VectorOption) (*VectorStore, error) {
	store, err := vectorStore(client, dimensions, generation, opts)
	if err != nil {
		return nil, err
	}
	if err = client.initVectors(ctx, dimensions, store.table); err != nil {
		return nil, fmt.Errorf("postgres: initialize vectors: %w", err)
	}
	if indexErr := store.EnsureExactScopeIndexes(ctx); indexErr != nil {
		return nil, indexErr
	}
	return store, nil
}

func vectorStore(client *Client, dimensions int, generation string, opts []VectorOption) (*VectorStore, error) {
	if !ValidGenerationName(generation) {
		return nil, fmt.Errorf("postgres: invalid vector generation %q", generation)
	}
	if _, err := ValidateVectorDimensions(dimensions); err != nil {
		return nil, err
	}
	store := &VectorStore{client: client, dimensions: dimensions, table: VectorTable(generation)}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}
	if store.exactScopeMax < 0 || store.exactScopeMax > maxExactScopeLimit {
		return nil, fmt.Errorf("postgres: exact scope limit must be between 0 and %d", maxExactScopeLimit)
	}
	return store, nil
}

// EnsureExactScopeIndexes prepares the indexes required by enabled exact search.
func (s *VectorStore) EnsureExactScopeIndexes(ctx context.Context) error {
	if s.exactScopeMax == 0 {
		return nil
	}
	if err := s.client.ensureExactScopeIndexes(ctx, s.table); err != nil {
		return fmt.Errorf("postgres: ensure exact-scope indexes: %w", err)
	}
	return nil
}

// ValidateVectorDimensions returns the bounded dimension supported by the pgvector index.
func ValidateVectorDimensions(dimensions int) (int32, error) {
	if dimensions < 1 || dimensions > 2000 {
		return 0, fmt.Errorf("postgres: vector dimensions must be between 1 and 2000")
	}
	return int32(dimensions), nil
}

// OpenGenerationVectorStore returns a store over an existing generation table
// without creating or altering any schema.
func OpenGenerationVectorStore(ctx context.Context, client *Client, dimensions int, generation string, opts ...VectorOption) (*VectorStore, error) {
	store, err := vectorStore(client, dimensions, generation, opts)
	if err != nil {
		return nil, err
	}
	table := store.table
	var stored int
	var vectorType bool
	err = client.Pool.QueryRow(ctx, `SELECT atttypmod, atttypid = to_regtype('vector') FROM pg_attribute WHERE attrelid = to_regclass(format('%I.%I', current_schema(), $1::text)) AND attname = 'embedding' AND NOT attisdropped`, table).Scan(&stored, &vectorType)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("postgres: vector table %q does not exist", table)
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: inspect vector table %q: %w", table, err)
	}
	if !vectorType || stored != dimensions {
		return nil, fmt.Errorf("%w: postgres embedding dimension mismatch: existing %d, configured %d", ErrGenerationDrift, stored, dimensions)
	}
	return store, nil
}

// RouteCompletion makes job completion write embeddings to this store's table.
func (s *VectorStore) RouteCompletion() {
	table := s.table
	s.client.vectorTable.Store(&table)
	s.client.vectorDimensions.Store(int64(s.dimensions))
}

func (c *Client) vectorTargetTable() string {
	if table := c.vectorTable.Load(); table != nil {
		return *table
	}
	return VectorTable(LegacyGeneration)
}

func (c *Client) initVectors(ctx context.Context, dimensions int, table string) error {
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
	const relation = `to_regclass(format('%I.%I', current_schema(), $1::text))`
	var exists bool
	if relationErr := tx.QueryRow(ctx, `SELECT `+relation+` IS NOT NULL`, table).Scan(&exists); relationErr != nil {
		return relationErr
	}
	if !exists {
		if createErr := createVectorTable(ctx, tx, dimensions, table); createErr != nil {
			return createErr
		}
	}
	var storedDimensions int
	var vectorType bool
	if dimensionErr := tx.QueryRow(ctx, `SELECT atttypmod, atttypid=to_regtype('vector')
		FROM pg_attribute WHERE attrelid=`+relation+`
		AND attname='embedding' AND NOT attisdropped`, table).Scan(&storedDimensions, &vectorType); dimensionErr != nil {
		return dimensionErr
	}
	if !vectorType || storedDimensions != dimensions {
		return fmt.Errorf("embedding dimension mismatch: existing %d, configured %d", storedDimensions, dimensions)
	}
	if indexErr := ensureVectorIndexes(ctx, tx, table); indexErr != nil {
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

func createVectorTable(ctx context.Context, tx pgx.Tx, dimensions int, table string) error {
	schema := fmt.Sprintf(`CREATE TABLE %s (
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
 )`, pgx.Identifier{table}.Sanitize(), dimensions)
	_, err := tx.Exec(ctx, schema)
	return err
}

func ensureVectorIndexes(ctx context.Context, tx pgx.Tx, table string) error {
	embeddingIndex, documentIndex, collectionIndex := "idx_"+table+"_embedding", "idx_"+table+"_document_id", "idx_"+table+"_collection"
	const check = `WITH expected(index_name, access_method, column_name, operator_class) AS (
		VALUES ($2::text,'hnsw','embedding','vector_cosine_ops'),
		       ($3::text,'btree','document_id','uuid_ops'),
		       ($4::text,'btree','collection','text_ops')
	)
	SELECT count(*) = (SELECT count(*) FROM expected)
	FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid
	JOIN pg_am am ON am.oid=idx.relam
	JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
	JOIN pg_opclass op ON op.oid=i.indclass[0]
	JOIN expected e ON (idx.relname,am.amname,a.attname,op.opcname) =
		(e.index_name,e.access_method,e.column_name,e.operator_class)
	WHERE i.indrelid=to_regclass(format('%I.%I',current_schema(),$1::text))
	  AND i.indisvalid AND i.indisready AND i.indislive AND i.indnatts=1
	  AND i.indpred IS NULL AND i.indexprs IS NULL`
	var prepared bool
	if err := tx.QueryRow(ctx, check, table, embeddingIndex, documentIndex, collectionIndex).Scan(&prepared); err != nil || prepared {
		return err
	}
	quoted := pgx.Identifier{table}.Sanitize()
	indexes := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s USING hnsw (embedding vector_cosine_ops);
 CREATE INDEX IF NOT EXISTS %s ON %s(document_id);
 CREATE INDEX IF NOT EXISTS %s ON %s(collection)`,
		pgx.Identifier{embeddingIndex}.Sanitize(), quoted,
		pgx.Identifier{documentIndex}.Sanitize(), quoted,
		pgx.Identifier{collectionIndex}.Sanitize(), quoted)
	if _, err := tx.Exec(ctx, indexes); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, check, table, embeddingIndex, documentIndex, collectionIndex).Scan(&prepared); err != nil {
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

func insertVectors(ctx context.Context, tx pgx.Tx, table string, chunks []domain.Chunk, dimensions int) error {
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
		batch.Queue(`INSERT INTO `+pgx.Identifier{table}.Sanitize()+`(chunk_id,document_id,chunk_index,doc_type,collection,text,kind,quality,token_count,embedding)
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
	if err = insertVectors(ctx, tx, s.table, chunks, s.dimensions); err != nil {
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

// ObservesVectorIncarnation reports that search reads incarnation with score.
func (*VectorStore) ObservesVectorIncarnation() bool { return true }

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
	searchQuery, searchArgs, ok := searchSQL(s.table, literal, topK, filter, useProbe)
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
		probeQuery, probeArgs, _ := probeSQL(s.table, s.exactScopeMax+1, filter)
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
			exactQuery, exactArgs, _ := exactSQL(s.table, literal, topK, s.exactScopeMax+1, filter)
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
		var kind, quality, revision, attempt *string
		var incarnationRequired bool
		if scanErr := rows.Scan(&sc.Chunk.ID, &sc.Chunk.DocumentID, &sc.Chunk.Index, &docType, &collection, &sc.Chunk.Text, &kind, &quality, &sc.Chunk.TokenCount, &sc.Score, &sc.ObservedIncarnation, &revision, &attempt, &incarnationRequired); scanErr != nil {
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
		published := revision != nil && attempt != nil
		if published {
			sc.Chunk.Metadata[domain.VectorRevisionKey] = *revision
			sc.Chunk.Metadata[domain.VectorAttemptKey] = *attempt
		}
		if published || incarnationRequired {
			sc.Chunk.VectorIncarnation = sc.ObservedIncarnation
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func hasScopeFilter(filter domain.VectorFilter) bool {
	return filter.DocType != "" || filter.Collection != "" || filter.DocumentIDs != nil || filter.Authorized != nil
}

// GrantScope reports that search joins the readable-document relation itself.
func (*VectorStore) GrantScope() domain.GrantScopeKind { return domain.GrantScopeRelation }

// DocumentSetScope reports that search also joins a set's membership, so a
// query that carries a set is confined to it inside the ranking statement.
func (*VectorStore) DocumentSetScope() {}

var _ domain.DocumentSetScopedVectorStore = (*VectorStore)(nil)

// authorizedJoin restricts the completed documents joined as d to those the
// filter's subject may read at their current incarnation. The relation sits in
// the FROM clause, so ranking and LIMIT only ever see readable rows. A scope
// that does not name both a tenant and a principal matches nothing. A scope
// bound to a document set joins the set's readable members instead, which are
// the same rows further limited to the set's immutable membership.
func authorizedJoin(args []any, scope *domain.AuthorizedScope) (join string, next []any, matches bool) {
	if scope == nil {
		return "", args, true
	}
	tenant, tenantErr := uuid.Parse(scope.TenantID)
	principal, principalErr := uuid.Parse(scope.PrincipalID)
	if tenantErr != nil || principalErr != nil {
		return "", nil, false
	}
	if scope.DocumentSet == nil {
		args = append(args, tenant.String(), principal.String())
		return fmt.Sprintf(` JOIN grant_catalog_readable_documents g ON g.tenant_id=$%d::uuid AND g.principal_id=$%d::uuid
  AND g.document_id=d.id AND g.incarnation=COALESCE(d.incarnation,d.id)`, len(args)-1, len(args)), args, true
	}
	set, setErr := uuid.Parse(scope.DocumentSet.ID)
	if setErr != nil {
		return "", nil, false
	}
	args = append(args, tenant.String(), principal.String(), set.String())
	return fmt.Sprintf(` JOIN grant_catalog_document_set_readable g ON g.tenant_id=$%d::uuid AND g.principal_id=$%d::uuid
  AND g.set_id=$%d::uuid AND g.document_id=d.id AND g.incarnation=COALESCE(d.incarnation,d.id)`, len(args)-2, len(args)-1, len(args)), args, true
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

func probeSQL(table string, limit int, filter domain.VectorFilter) (query string, args []any, matches bool) {
	predicates, args, ok := appendFilterPredicates(nil, filter, true)
	if !ok {
		return "", nil, false
	}
	join, args, ok := authorizedJoin(args, filter.Authorized)
	if !ok {
		return "", nil, false
	}
	args = append(args, limit)
	query = fmt.Sprintf(`SELECT count(*) FROM (
 SELECT 1
 FROM %s v JOIN documents d ON d.id=v.document_id AND d.status='completed'%s
 WHERE TRUE%s
 LIMIT $%d
) s`, pgx.Identifier{table}.Sanitize(), join, predicates, len(args))
	return query, args, true
}

// Capture publication identity from the same document snapshot used to rank the row.
const rankedIdentitySQL = `COALESCE(d.incarnation,d.id)::text AS observed_incarnation,
 CASE WHEN d.scoped_vectors AND d.vector_attempt IS NOT NULL AND NOT d.vector_mutation_pending
      THEN COALESCE(d.incarnation,d.id)::text END AS observed_revision,
 CASE WHEN d.scoped_vectors AND d.vector_attempt IS NOT NULL AND NOT d.vector_mutation_pending
      THEN d.vector_attempt::text END AS observed_attempt,
 d.vector_incarnation_required`

func exactSQL(table, literal string, topK, scopeLimit int, filter domain.VectorFilter) (query string, args []any, matches bool) {
	predicates, args, ok := appendFilterPredicates([]any{literal}, filter, true)
	if !ok {
		return "", nil, false
	}
	join, args, ok := authorizedJoin(args, filter.Authorized)
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
 SELECT v.chunk_id,v.embedding <=> $1::text::vector AS distance,
        `+rankedIdentitySQL+`
 FROM %[1]s v JOIN documents d ON d.id=v.document_id AND d.status='completed'%[5]s
 WHERE TRUE%[2]s
 LIMIT $%[3]d
),
ranked AS (
 SELECT chunk_id,distance,observed_incarnation,observed_revision,observed_attempt,vector_incarnation_required
 FROM candidates
 ORDER BY distance,chunk_id%[4]s
)
SELECT v.chunk_id,v.document_id::text,v.chunk_index,v.doc_type,v.collection,v.text,v.kind,v.quality,v.token_count,1-r.distance,
       r.observed_incarnation,r.observed_revision,r.observed_attempt,r.vector_incarnation_required
FROM ranked r JOIN %[1]s v ON v.chunk_id=r.chunk_id
ORDER BY r.distance,v.chunk_id`, pgx.Identifier{table}.Sanitize(), predicates, scopeArg, limitClause, join)
	return query, args, true
}

func searchSQL(table, literal string, topK int, filter domain.VectorFilter, matchDocumentLabels bool) (query string, args []any, matches bool) {
	predicates, args, ok := appendFilterPredicates([]any{literal}, filter, matchDocumentLabels)
	if !ok {
		return "", nil, false
	}
	join, args, ok := authorizedJoin(args, filter.Authorized)
	if !ok {
		return "", nil, false
	}
	query = `SELECT v.chunk_id,v.document_id::text,v.chunk_index,v.doc_type,v.collection,v.text,v.kind,v.quality,v.token_count,v.embedding <=> $1::text::vector AS distance,
 ` + rankedIdentitySQL + `
 FROM ` + pgx.Identifier{table}.Sanitize() + ` v JOIN documents d ON d.id=v.document_id AND d.status='completed'` + join + ` WHERE TRUE` + predicates
	if topK > 0 {
		args = append(args, topK)
		query += fmt.Sprintf(" ORDER BY distance LIMIT $%d", len(args))
	}
	query = `SELECT chunk_id,document_id,chunk_index,doc_type,collection,text,kind,quality,token_count,1-distance,
        observed_incarnation,observed_revision,observed_attempt,vector_incarnation_required
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
	_, err = s.client.execRemoval(ctx, `DELETE FROM `+pgx.Identifier{s.table}.Sanitize()+` WHERE document_id=$1`, parsed.String())
	return err
}

// DeleteAllVectorsInNamespace removes a document scope or the entire default namespace.
func (s *VectorStore) DeleteAllVectorsInNamespace(ctx context.Context, namespace string) error {
	if namespace != "" {
		return s.DeleteByDocID(ctx, namespace)
	}
	_, err := s.client.Pool.Exec(ctx, `DELETE FROM `+pgx.Identifier{s.table}.Sanitize())
	return err
}

// Ping verifies that the vector table is available for search.
func (s *VectorStore) Ping(ctx context.Context) error {
	_, err := s.client.Pool.Exec(ctx, `SELECT 1 FROM `+pgx.Identifier{s.table}.Sanitize()+` LIMIT 0`)
	return err
}

// MaxFetchIDs bounds the keys of one Fetch or DeleteIDs call.
const MaxFetchIDs = 1000

func parseVectorText(text string) ([]float32, error) {
	text = strings.TrimSpace(text)
	if len(text) < 2 || text[0] != '[' || text[len(text)-1] != ']' {
		return nil, fmt.Errorf("unrecognized vector text")
	}
	parts := strings.Split(text[1:len(text)-1], ",")
	out := make([]float32, len(parts))
	for i, part := range parts {
		value, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
		if err != nil {
			return nil, fmt.Errorf("unrecognized vector value: %w", err)
		}
		out[i] = float32(value)
	}
	return out, nil
}

// Fetch returns the stored vectors for the supplied keys. Missing keys are absent from the result.
func (s *VectorStore) Fetch(ctx context.Context, ids []string) (map[string]domain.Chunk, error) {
	out := make(map[string]domain.Chunk, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	if len(ids) > MaxFetchIDs {
		return nil, fmt.Errorf("postgres: fetch accepts at most %d keys", MaxFetchIDs)
	}
	rows, err := s.client.Pool.Query(ctx, `SELECT chunk_id,document_id::text,chunk_index,doc_type,collection,text,kind,quality,token_count,embedding::text
 FROM `+pgx.Identifier{s.table}.Sanitize()+` WHERE chunk_id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("postgres: fetch vectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var chunk domain.Chunk
		var id, docType, collection, embedding string
		var kind, quality *string
		if err = rows.Scan(&id, &chunk.DocumentID, &chunk.Index, &docType, &collection, &chunk.Text, &kind, &quality, &chunk.TokenCount, &embedding); err != nil {
			return nil, fmt.Errorf("postgres: read vector: %w", err)
		}
		chunk.ID = domain.ChunkID(id)
		if chunk.Embedding, err = parseVectorText(embedding); err != nil {
			return nil, fmt.Errorf("postgres: decode vector %s: %w", id, err)
		}
		chunk.Metadata = map[string]any{"doc_type": docType, "collection": collection}
		if kind != nil {
			chunk.Metadata["kind"] = *kind
		}
		if quality != nil {
			chunk.Metadata["quality"] = *quality
		}
		out[id] = chunk
	}
	return out, rows.Err()
}

// ListIDs returns up to limit keys after the cursor in ascending order and the
// cursor for the next page, which is empty at the end.
func (s *VectorStore) ListIDs(ctx context.Context, after string, limit int) (ids []string, next string, err error) {
	if limit < 1 || limit > 10000 {
		return nil, "", errors.New("postgres: page size must be between 1 and 10000")
	}
	rows, err := s.client.Pool.Query(ctx, `SELECT chunk_id FROM `+pgx.Identifier{s.table}.Sanitize()+` WHERE chunk_id > $1 ORDER BY chunk_id LIMIT $2`, after, limit+1)
	if err != nil {
		return nil, "", fmt.Errorf("postgres: list vector keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, "", fmt.Errorf("postgres: read vector key: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(ids) > limit {
		ids = ids[:limit]
		next = ids[limit-1]
	}
	return ids, next, nil
}

// DeleteIDs removes vectors by key. Missing keys are ignored.
func (s *VectorStore) DeleteIDs(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if len(ids) > MaxFetchIDs {
		return fmt.Errorf("postgres: delete accepts at most %d keys", MaxFetchIDs)
	}
	if _, err := s.client.Pool.Exec(ctx, `DELETE FROM `+pgx.Identifier{s.table}.Sanitize()+` WHERE chunk_id = ANY($1)`, ids); err != nil {
		return fmt.Errorf("postgres: delete vectors: %w", err)
	}
	return nil
}
