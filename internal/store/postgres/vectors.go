package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// VectorStore searches pgvector embeddings committed with document completion.
type VectorStore struct {
	client     *Client
	dimensions int
}

var (
	_ domain.VectorStore              = (*VectorStore)(nil)
	_ domain.FilteredVectorStore      = (*VectorStore)(nil)
	_ domain.VectorDeleter            = (*VectorStore)(nil)
	_ domain.TransactionalVectorStore = (*VectorStore)(nil)
)

// NewVectorStore validates or initializes the optional schema under the migration advisory lock.
// The client must have applied its base migrations before calling this function.
func NewVectorStore(ctx context.Context, client *Client, dimensions int) (*VectorStore, error) {
	validDimensions, err := ValidateVectorDimensions(dimensions)
	if err != nil {
		return nil, err
	}
	if err := client.initVectors(ctx, dimensions); err != nil {
		return nil, fmt.Errorf("postgres: initialize vectors: %w", err)
	}
	client.vectorDimensions.Store(validDimensions)
	return &VectorStore{client: client, dimensions: dimensions}, nil
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
	if topK > 10000 {
		return nil, fmt.Errorf("postgres: topK must not exceed 10000")
	}
	if filter.MatchesNothing() {
		return []domain.ScoredChunk{}, nil
	}
	literal, err := vectorLiteral(embedding, s.dimensions)
	if err != nil {
		return nil, err
	}
	query, args, ok := searchSQL(literal, topK, filter)
	if !ok {
		return []domain.ScoredChunk{}, nil
	}
	tx, err := s.client.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("postgres: query vectors: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SET LOCAL hnsw.iterative_scan = strict_order`); err != nil {
		return nil, fmt.Errorf("postgres: query vectors: %w", err)
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: query vectors: %w", err)
	}
	defer rows.Close()
	out := make([]domain.ScoredChunk, 0)
	for rows.Next() {
		var sc domain.ScoredChunk
		var docType, collection string
		var kind, quality *string
		if err := rows.Scan(&sc.Chunk.ID, &sc.Chunk.DocumentID, &sc.Chunk.Index, &docType, &collection, &sc.Chunk.Text, &kind, &quality, &sc.Chunk.TokenCount, &sc.Score); err != nil {
			return nil, err
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
	return out, rows.Err()
}

func searchSQL(literal string, topK int, filter domain.VectorFilter) (query string, args []any, matches bool) {
	args = []any{literal}
	query = `SELECT v.chunk_id,v.document_id::text,v.chunk_index,v.doc_type,v.collection,v.text,v.kind,v.quality,v.token_count,v.embedding <=> $1::text::vector AS distance
 FROM chunk_vectors v JOIN documents d ON d.id=v.document_id AND d.status='completed' WHERE TRUE`
	for _, f := range []struct{ column, value string }{{"v.doc_type", filter.DocType}, {"v.collection", filter.Collection}} {
		if f.value != "" {
			args = append(args, f.value)
			query += fmt.Sprintf(" AND %s=$%d", f.column, len(args))
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
		query += fmt.Sprintf(" AND v.document_id=ANY($%d::uuid[])", len(args))
	}
	if topK > 0 {
		args = append(args, topK)
		query += fmt.Sprintf(" ORDER BY distance LIMIT $%d", len(args))
	}
	query = `SELECT chunk_id,document_id,chunk_index,doc_type,collection,text,kind,quality,token_count,1-distance
 FROM (` + query + `) s ORDER BY distance,chunk_id`
	return query, args, true
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
