package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const testDims = 4

func testProfile(model string) domain.EmbeddingProfile {
	return domain.EmbeddingProfile{Provider: domain.EmbeddingProviderOpenAI, Model: model, ModelRevision: "rev-1", Dimensions: testDims, Metric: domain.MetricCosine, ExtractionChunkVersion: (&config.Config{ExtractionChunkVersion: "chunk-1"}).TextProfile()}
}

// embedWith returns a deterministic vector that depends on the model, so two
// models with equal dimensions produce different representations of one text.
func embedWith(model, text string) []float32 {
	sum := sha256.Sum256([]byte(model + "\x00" + text))
	out := make([]float32, testDims)
	for i := range out {
		out[i] = 0.1 + float32(binary.BigEndian.Uint32(sum[i*4:]))/float32(1<<32)
	}
	return out
}

type countingEmbedder struct {
	model string
	calls atomic.Int32
	fail  bool
}

func (e *countingEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	e.calls.Add(1)
	if e.fail {
		return nil, fmt.Errorf("provider unavailable")
	}
	return embedWith(e.model, text), nil
}

type docSpec struct {
	ID          string
	Incarnation string
	Collection  string
	DocType     string
	Status      string
	Texts       []string
	S3Key       string
	Tagged      bool
	Attempt     string
}

func hexChunkID(docID string, index int) string {
	sum := sha256.Sum256([]byte(docID + strconv.Itoa(index)))
	return hex.EncodeToString(sum[:16])
}

func newDoc(collection, docType string, texts ...string) docSpec {
	return docSpec{ID: uuid.NewString(), Collection: collection, DocType: docType, Status: "completed", Texts: texts}
}

func vectorDB(t *testing.T) *pgstore.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	pg, err := pgstore.NewClient(t.Context(), pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(context.Background())) })
	require.NoError(t, pg.RunMigrations(t.Context()))
	return pg
}

func newTaggedDoc(collection, docType string, texts ...string) docSpec {
	return docSpec{
		ID:          uuid.NewString(),
		Incarnation: uuid.NewString(),
		Collection:  collection,
		DocType:     docType,
		Status:      "completed",
		Texts:       texts,
		Tagged:      true,
		Attempt:     uuid.NewString(),
	}
}

func insertDoc(t *testing.T, pg *pgstore.Client, d docSpec) []domain.Chunk {
	t.Helper()
	var s3 *string
	if d.S3Key != "" {
		s3 = &d.S3Key
	}
	var inc *string
	if d.Incarnation != "" {
		inc = &d.Incarnation
	}
	var att *string
	if d.Attempt != "" {
		att = &d.Attempt
	}
	_, err := pg.Pool.Exec(t.Context(), `INSERT INTO documents(id,incarnation,content,status,collection,doc_type,chunk_count,fingerprint,s3_key,scoped_vectors,vector_attempt)
 VALUES($1,COALESCE($2::uuid, gen_random_uuid()),$3,$4,$5,$6,$7,$8,$9,$10,$11::uuid)`,
		d.ID, inc, "content of "+d.ID, d.Status, d.Collection, d.DocType, len(d.Texts), "fp-"+d.ID, s3, d.Tagged, att)
	require.NoError(t, err)
	chunks := make([]domain.Chunk, len(d.Texts))
	for i, text := range d.Texts {
		id := hexChunkID(d.ID, i)
		_, err = pg.Pool.Exec(t.Context(), `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,$3,$4,$5)`, id, d.ID, i, text, chunker.EstimateTokens(text))
		require.NoError(t, err)
		chunks[i] = domain.Chunk{ID: domain.ChunkID(id), DocumentID: domain.DocumentID(d.ID), Index: i, Text: text, TokenCount: chunker.EstimateTokens(text)}
	}
	return chunks
}

// sourceChunks adds vectors from model as the live store would hold them.
func sourceChunks(d docSpec, chunks []domain.Chunk, model string) []domain.Chunk {
	out := make([]domain.Chunk, len(chunks))
	for i, c := range chunks {
		kind, quality := chunker.Classify(c.Text)
		c.Embedding = embedWith(model, c.Text)
		c.Metadata = map[string]any{"doc_type": d.DocType, "kind": string(kind), "quality": string(quality)}
		if d.Collection != "" {
			c.Metadata["collection"] = d.Collection
		}
		if d.Tagged {
			revision := d.Incarnation
			if revision == "" {
				revision = d.ID
			}
			c.Metadata[domain.VectorRevisionKey] = revision
			if d.Attempt != "" {
				c.Metadata[domain.VectorAttemptKey] = d.Attempt
			}
		}
		out[i] = c
	}
	return out
}

func testConfig() *config.Config {
	return &config.Config{PineconeUpsertBatchSize: 100, PineconeTimeout: time.Second, PineconeMaxAttempts: 1, EmbeddingDimensions: testDims}
}

func memoryPinecone() (*pcstore.Client, *pcstore.MemoryIndex) {
	index := pcstore.NewMemoryIndex(testDims)
	return pcstore.NewClientForIndex(index, testConfig(), ""), index
}

func defaultOptions() options {
	return options{MaxDocuments: 1000, MaxChunks: 100000, MaxBytes: 128 << 20, Batch: 100, Page: 100, Timeout: time.Minute}
}

// relationalState captures every document-identity table so a test can prove they are untouched.
func relationalState(t *testing.T, pg *pgstore.Client) string {
	t.Helper()
	var out []string
	for _, query := range []string{
		`SELECT id::text||'|'||status||'|'||collection||'|'||doc_type||'|'||COALESCE(fingerprint,'')||'|'||COALESCE(incarnation::text,'')||'|'||chunk_count||'|'||content||'|'||COALESCE(s3_key,'') FROM documents ORDER BY id`,
		`SELECT id::text||'|'||document_id::text||'|'||chunk_index||'|'||text||'|'||token_count FROM chunks ORDER BY id`,
		`SELECT fingerprint||'|'||document_id::text FROM document_fingerprints ORDER BY fingerprint`,
	} {
		rows, err := pg.Pool.Query(t.Context(), query)
		require.NoError(t, err)
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			out = append(out, line)
		}
		require.NoError(t, rows.Err())
		rows.Close()
		out = append(out, "--")
	}
	encoded, err := json.Marshal(out)
	require.NoError(t, err)
	return string(encoded)
}

// writeTableCounts reports row counts of the tables a read-only run must not change.
func writeTableCounts(t *testing.T, pg *pgstore.Client) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"documents", "chunks", "document_fingerprints", "ingest_jobs", "vector_generations", "vector_generation_documents", "vector_state"} {
		var n int
		require.NoError(t, pg.Pool.QueryRow(t.Context(), `SELECT count(*) FROM `+table).Scan(&n))
		counts[table] = n
	}
	var state string
	require.NoError(t, pg.Pool.QueryRow(t.Context(), `SELECT active_generation||'|'||frozen::text FROM vector_state`).Scan(&state))
	counts["state:"+state] = 1
	rows, err := pg.Pool.Query(t.Context(), `SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'chunk_vectors%' ORDER BY tablename`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		counts["table:"+name] = 1
	}
	return counts
}

// expectedKeys lists every chunk key of the eligible documents in PostgreSQL.
func expectedKeys(t *testing.T, pg *pgstore.Client) []string {
	t.Helper()
	rows, err := pg.Pool.Query(t.Context(), `SELECT replace(c.id::text,'-','') FROM chunks c JOIN documents d ON d.id=c.document_id WHERE d.status='completed' ORDER BY 1`)
	require.NoError(t, err)
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		require.NoError(t, rows.Scan(&key))
		keys = append(keys, key)
	}
	require.NoError(t, rows.Err())
	return keys
}

func allKeys(t *testing.T, r vectorReader) []string {
	t.Helper()
	var keys []string
	token := ""
	for {
		page, next, err := r.ListIDs(t.Context(), token, 1000)
		require.NoError(t, err)
		keys = append(keys, page...)
		if next == "" {
			break
		}
		token = next
	}
	sort.Strings(keys)
	return keys
}

// requireTargetMatches proves that the target holds exactly the corpus's keys with
// PostgreSQL's identity and scope and the vectors from model.
func requireTargetMatches(t *testing.T, pg *pgstore.Client, target vectorReader, model string) {
	t.Helper()
	want := expectedKeys(t, pg)
	require.Equal(t, want, allKeys(t, target), "the target holds exactly the expected keys")
	fetched := map[string]domain.Chunk{}
	for start := 0; start < len(want); start += 100 {
		part, err := target.Fetch(t.Context(), want[start:min(start+100, len(want))])
		require.NoError(t, err)
		for id, c := range part {
			fetched[id] = c
		}
	}
	require.Len(t, fetched, len(want))
	rows, err := pg.Pool.Query(t.Context(), `SELECT replace(c.id::text,'-',''), d.id::text, c.chunk_index, c.text, c.token_count, d.collection, d.doc_type FROM chunks c JOIN documents d ON d.id=c.document_id WHERE d.status='completed'`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var key, doc, text, collection, docType string
		var index, tokens int
		require.NoError(t, rows.Scan(&key, &doc, &index, &text, &tokens, &collection, &docType))
		got := fetched[key]
		require.Equal(t, domain.DocumentID(doc), got.DocumentID, key)
		require.Equal(t, index, got.Index, key)
		require.Equal(t, text, got.Text, key)
		require.Equal(t, tokens, got.TokenCount, key)
		require.Equal(t, docType, got.Metadata["doc_type"], key)
		gotCollection, _ := got.Metadata["collection"].(string)
		require.Equal(t, collection, gotCollection, key)
		require.Equal(t, embedWith(model, text), got.Embedding, key)
	}
	require.NoError(t, rows.Err())
}
