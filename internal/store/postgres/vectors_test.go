package postgres

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func vectorDatabase(t *testing.T) (*Client, *VectorStore) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	t.Helper()
	c, err := NewClient(t.Context(), pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(t.Context()))
	v, err := NewVectorStore(t.Context(), c, 3)
	require.NoError(t, err)
	return c, v
}

func TestVectorSchemaDimensions(t *testing.T) {
	c, v := vectorDatabase(t)
	require.NoError(t, v.Ping(t.Context()))
	_, err := NewVectorStore(t.Context(), c, 3)
	require.NoError(t, err)
	_, err = NewVectorStore(t.Context(), c, 4)
	require.ErrorContains(t, err, "dimension")
	for _, dimensions := range []int{0, -1, 2001} {
		_, err = NewVectorStore(t.Context(), c, dimensions)
		require.Error(t, err)
	}
	var indexes int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_indexes WHERE tablename='chunk_vectors' AND (indexdef LIKE '%USING hnsw%' OR indexdef LIKE '%(document_id)%' OR indexdef LIKE '%(collection)%')`).Scan(&indexes))
	require.Equal(t, 3, indexes)
}

func TestVectorSchemaRejectsOlderExtension(t *testing.T) {
	c, _ := vectorDatabase(t)
	_, err := c.Pool.Exec(t.Context(), `UPDATE pg_extension SET extversion='0.7.4' WHERE extname='vector'`)
	require.NoError(t, err)
	_, err = NewVectorStore(t.Context(), c, 3)
	require.ErrorContains(t, err, "pgvector 0.8 or later is required")
}

func seedSearchVectors(t *testing.T, c *Client, near, far int) {
	t.Helper()
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, c.InsertDocument(t.Context(), domain.Document{ID: id}))
	require.NoError(t, c.UpdateDocumentStatus(t.Context(), id, "completed"))
	_, err := c.Pool.Exec(t.Context(), `INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,text,token_count,embedding)
 SELECT 'near-'||i,$1::uuid,i,'report','synthetic',1,('['||(1+i*0.001)||',0.01,0]')::vector FROM generate_series(1,$2::int) i
 UNION ALL SELECT 'far-'||i,$1::uuid,i,'note','synthetic',1,('[-1,'||(i*0.001)||',0]')::vector FROM generate_series(1,$3::int) i`, string(id), near, far)
	require.NoError(t, err)
	_, err = c.Pool.Exec(t.Context(), `ANALYZE chunk_vectors`)
	require.NoError(t, err)
}

func TestVectorSearchCanUseHNSW(t *testing.T) {
	c, _ := vectorDatabase(t)
	seedSearchVectors(t, c, 2000, 0)
	query, args, ok := searchSQL("[1,0,0]", 10, domain.VectorFilter{})
	require.True(t, ok)
	tx, err := c.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	_, err = tx.Exec(t.Context(), `SET LOCAL enable_seqscan = off; SET LOCAL enable_sort = off`)
	require.NoError(t, err)
	rows, err := tx.Query(t.Context(), "EXPLAIN "+query, args...)
	require.NoError(t, err)
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan = append(plan, line)
	}
	require.NoError(t, rows.Err())
	t.Log(strings.Join(plan, "\n"))
	require.Contains(t, strings.Join(plan, "\n"), "idx_chunk_vectors_embedding")
}

func TestVectorFilteredSearchFillsLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	c, err := NewClient(t.Context(), pgtest.StartVector(t)+"&enable_seqscan=off&enable_sort=off")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(t.Context()))
	v, err := NewVectorStore(t.Context(), c, 3)
	require.NoError(t, err)
	seedSearchVectors(t, c, 200, 10)
	t.Run("filtered", func(t *testing.T) {
		got, queryErr := v.QueryFiltered(t.Context(), []float32{1, 0, 0}, 10, domain.VectorFilter{DocType: "note"})
		require.NoError(t, queryErr)
		require.Len(t, got, 10)
	})
	t.Run("unfiltered", func(t *testing.T) {
		got, queryErr := v.QueryFiltered(t.Context(), []float32{1, 0, 0}, 100, domain.VectorFilter{})
		require.NoError(t, queryErr)
		require.Len(t, got, 100)
	})
}

func TestVectorFiltersAndCompletedVisibility(t *testing.T) {
	c, v := vectorDatabase(t)
	ids := []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	chunks := []domain.Chunk{
		{ID: "outside", DocumentID: domain.DocumentID(ids[0]), Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "b"}},
		{ID: "wrong-type", DocumentID: domain.DocumentID(ids[1]), Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "note", "collection": "a"}},
		{ID: "wanted", DocumentID: domain.DocumentID(ids[2]), Index: 2, Text: "Synthetic reference.", TokenCount: 3, Embedding: []float32{0, 1, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "a", "kind": "paragraph", "quality": "high"}},
		{ID: "processing", DocumentID: domain.DocumentID(ids[3]), Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "a"}},
	}
	for i, id := range ids {
		require.NoError(t, c.InsertDocument(t.Context(), domain.Document{ID: domain.DocumentID(id)}))
		if i != 3 {
			require.NoError(t, c.UpdateDocumentStatus(t.Context(), domain.DocumentID(id), "completed"))
		}
	}
	require.NoError(t, v.Upsert(t.Context(), "", chunks))
	for _, tc := range []struct {
		name   string
		filter domain.VectorFilter
		want   []domain.ChunkID
	}{
		{"collection", domain.VectorFilter{Collection: "a", DocType: "report"}, []domain.ChunkID{"wanted"}},
		{"all filters", domain.VectorFilter{Collection: "a", DocType: "report", DocumentIDs: []string{ids[0], ids[2]}}, []domain.ChunkID{"wanted"}},
		{"wrong ID", domain.VectorFilter{Collection: "a", DocumentIDs: []string{ids[0]}}, []domain.ChunkID{}},
		{"wrong type", domain.VectorFilter{Collection: "a", DocType: "manual"}, []domain.ChunkID{}},
		{"wrong collection", domain.VectorFilter{Collection: "missing"}, []domain.ChunkID{}},
		{"empty IDs", domain.VectorFilter{DocumentIDs: []string{}}, []domain.ChunkID{}},
		{"invalid ID", domain.VectorFilter{DocumentIDs: []string{"invalid"}}, []domain.ChunkID{}},
		{"type and ID", domain.VectorFilter{DocType: "report", DocumentIDs: []string{ids[2]}}, []domain.ChunkID{"wanted"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := v.QueryFiltered(t.Context(), []float32{1, 0, 0}, 1, tc.filter)
			require.NoError(t, err)
			found := make([]domain.ChunkID, 0, len(got))
			for _, sc := range got {
				found = append(found, sc.Chunk.ID)
			}
			require.Equal(t, tc.want, found)
		})
	}
	got, err := v.QueryAllNamespaces(t.Context(), []float32{1, 0, 0}, 10, "")
	require.NoError(t, err)
	require.Len(t, got, 3)
	got, err = v.Query(t.Context(), ids[2], []float32{0, 1, 0}, 1, "report")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, chunks[2].Text, got[0].Chunk.Text)
	require.Equal(t, 2, got[0].Chunk.Index)
	require.Equal(t, 3, got[0].Chunk.TokenCount)
	require.Equal(t, chunks[2].Metadata, got[0].Chunk.Metadata)
	require.InDelta(t, 1, got[0].Score, 1e-6)
	require.NoError(t, c.DeleteDocument(t.Context(), domain.DocumentID(ids[2])))
	var count int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors WHERE document_id=$1`, ids[2]).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, v.DeleteAllVectorsInNamespace(t.Context(), ids[0]))
	require.NoError(t, v.DeleteByDocID(t.Context(), ids[1]))
	got, err = v.QueryFiltered(t.Context(), []float32{1, 0, 0}, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestCompleteJobVectorsAtomic(t *testing.T) {
	c, v := vectorDatabase(t)
	req := jobsRequest("Synthetic atomic completion.")
	req.DocType = "report"
	req.Collection = "archive"
	_, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	job, err := c.ClaimJob(t.Context(), "atomic", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	chunks := []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), DocumentID: job.Document.ID, Text: "Synthetic atomic completion.", TokenCount: 4, Embedding: []float32{1, 0, 0}}}
	holder, err := c.Pool.Acquire(t.Context())
	require.NoError(t, err)
	defer holder.Release()
	_, err = holder.Exec(t.Context(), `SELECT pg_advisory_lock(8191)`)
	require.NoError(t, err)
	defer func() {
		_, unlockErr := holder.Exec(context.WithoutCancel(t.Context()), `SELECT pg_advisory_unlock(8191)`)
		require.NoError(t, unlockErr)
	}()
	_, err = c.Pool.Exec(t.Context(), `CREATE FUNCTION pause_vectors() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(8191); RETURN NEW; END $$; CREATE TRIGGER pause_vectors AFTER INSERT ON chunk_vectors FOR EACH ROW EXECUTE FUNCTION pause_vectors()`)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		ok, completeErr := c.CompleteJob(t.Context(), *job, chunks)
		if completeErr == nil && !ok {
			completeErr = domain.ErrJobNotOwned
		}
		done <- completeErr
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := c.Pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event='advisory' AND pid<>pg_backend_pid())`).Scan(&waiting)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	var count int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors`).Scan(&count))
	require.Zero(t, count)
	got, err := v.QueryFiltered(t.Context(), []float32{1, 0, 0}, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Empty(t, got)
	_, err = holder.Exec(t.Context(), `SELECT pg_advisory_unlock(8191)`)
	require.NoError(t, err)
	require.NoError(t, <-done)
	got, err = v.QueryFiltered(t.Context(), []float32{1, 0, 0}, 10, domain.VectorFilter{Collection: "archive", DocType: "report"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, chunks[0].ID, got[0].Chunk.ID)
}

func TestCompleteJobVectorsRollback(t *testing.T) {
	c, v := vectorDatabase(t)
	jobsAccept(t, c, "Synthetic rollback completion.")
	job, err := c.ClaimJob(t.Context(), "rollback", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	chunks := []domain.Chunk{
		{ID: domain.ChunkID(uuid.NewString()), DocumentID: job.Document.ID, Embedding: []float32{1, 0, 0}},
		{ID: domain.ChunkID(uuid.NewString()), DocumentID: job.Document.ID, Index: 1, Embedding: []float32{1, 0}},
	}
	ok, err := c.CompleteJob(t.Context(), *job, chunks)
	require.Error(t, err)
	require.False(t, ok)
	var vectors, storedChunks, jobs int
	var status string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors`).Scan(&vectors))
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunks`).Scan(&storedChunks))
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM ingest_jobs`).Scan(&jobs))
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT status FROM documents WHERE id=$1`, string(job.Document.ID)).Scan(&status))
	require.Zero(t, vectors)
	require.Zero(t, storedChunks)
	require.Equal(t, 1, jobs)
	require.Equal(t, "processing", status)
	chunks = chunks[:1]
	ok, err = c.CompleteJob(t.Context(), *job, chunks)
	require.NoError(t, err)
	require.True(t, ok)
	got, err := v.QueryFiltered(t.Context(), []float32{1, 0, 0}, 1, domain.VectorFilter{})
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestCompleteJobVectorsRollBackOnLaterFailure(t *testing.T) {
	c, _ := vectorDatabase(t)
	jobsAccept(t, c, "Synthetic late failure.")
	job, err := c.ClaimJob(t.Context(), "late", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	_, err = c.Pool.Exec(t.Context(), `CREATE FUNCTION fail_fingerprint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic failure'; END $$; CREATE TRIGGER fail_fingerprint BEFORE INSERT ON document_fingerprints FOR EACH ROW EXECUTE FUNCTION fail_fingerprint()`)
	require.NoError(t, err)
	ok, err := c.CompleteJob(t.Context(), *job, []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), DocumentID: job.Document.ID, Embedding: []float32{1, 0, 0}}})
	require.ErrorContains(t, err, "synthetic failure")
	require.False(t, ok)
	var vectors, storedChunks, jobs int
	var status string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors`).Scan(&vectors))
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunks`).Scan(&storedChunks))
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM ingest_jobs`).Scan(&jobs))
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT status FROM documents WHERE id=$1`, string(job.Document.ID)).Scan(&status))
	require.Zero(t, vectors)
	require.Zero(t, storedChunks)
	require.Equal(t, 1, jobs)
	require.Equal(t, "processing", status)
}

func TestVectorRejectsInvalidEmbeddings(t *testing.T) {
	c, v := vectorDatabase(t)
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, c.InsertDocument(t.Context(), domain.Document{ID: id}))
	require.Error(t, v.DeleteByDocID(t.Context(), "invalid"))
	for _, embedding := range [][]float32{{1, 0}, {1, float32(math.NaN()), 0}, {1, float32(math.Inf(1)), 0}} {
		require.Error(t, v.Upsert(t.Context(), "", []domain.Chunk{{ID: "bad", DocumentID: id, Embedding: embedding}}))
		_, err := v.QueryFiltered(t.Context(), embedding, 1, domain.VectorFilter{})
		require.Error(t, err)
	}
	_, err := v.QueryFiltered(t.Context(), []float32{1, 0, 0}, 10001, domain.VectorFilter{})
	require.Error(t, err)
}

func TestVectorInsertValidatesBatchBeforeWriting(t *testing.T) {
	c, _ := vectorDatabase(t)
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, c.InsertDocument(t.Context(), domain.Document{ID: id}))
	tx, err := c.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	err = insertVectors(t.Context(), tx, []domain.Chunk{
		{ID: "valid", DocumentID: id, Embedding: []float32{1, 0, 0}},
		{ID: "invalid", DocumentID: id, Index: 1, Embedding: []float32{1, 0}},
	}, 3)
	require.ErrorContains(t, err, "dimension")
	var inserted int
	require.NoError(t, tx.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors`).Scan(&inserted))
	require.Zero(t, inserted)
}

func TestVectorTieOrdering(t *testing.T) {
	c, v := vectorDatabase(t)
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, c.InsertDocument(t.Context(), domain.Document{ID: id}))
	require.NoError(t, c.UpdateDocumentStatus(t.Context(), id, "completed"))
	require.NoError(t, v.Upsert(t.Context(), "", []domain.Chunk{
		{ID: "z-last", DocumentID: id, Embedding: []float32{1, 0, 0}},
		{ID: "a-first", DocumentID: id, Index: 1, Embedding: []float32{1, 0, 0}},
	}))
	got, err := v.QueryFiltered(t.Context(), []float32{1, 0, 0}, 2, domain.VectorFilter{})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, domain.ChunkID("a-first"), got[0].Chunk.ID)
	require.Equal(t, domain.ChunkID("z-last"), got[1].Chunk.ID)
}
