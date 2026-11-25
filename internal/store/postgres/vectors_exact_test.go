package postgres

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func isolatedVectorDatabase(t *testing.T, baseURL string, clientOpts ...Option) (client *Client, dbURL string) {
	t.Helper()
	dbURL = pgtest.Sibling(t, baseURL)
	client, err := NewClient(t.Context(), dbURL, clientOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.Background())) })
	require.NoError(t, client.RunMigrations(t.Context()))
	return client, dbURL
}

func exhaustiveQuerySnapshot(t *testing.T, tx pgx.Tx, embedding []float32, dims, topK int, filter domain.VectorFilter) []domain.ScoredChunk {
	t.Helper()
	literal, err := vectorLiteral(embedding, dims)
	require.NoError(t, err)
	args := []any{literal}
	var where strings.Builder
	where.WriteString("d.status='completed'")
	if filter.DocType != "" {
		args = append(args, filter.DocType)
		fmt.Fprintf(&where, " AND v.doc_type=$%d AND d.doc_type=$%d", len(args), len(args))
	}
	if filter.Collection != "" {
		args = append(args, filter.Collection)
		fmt.Fprintf(&where, " AND v.collection=$%d AND d.collection=$%d", len(args), len(args))
	}
	if filter.DocumentIDs != nil {
		var ids []string
		seen := make(map[string]struct{}, len(filter.DocumentIDs))
		for _, id := range filter.DocumentIDs {
			if parsed, parseErr := uuid.Parse(id); parseErr == nil && parsed.String() == id {
				if _, dup := seen[id]; !dup {
					seen[id] = struct{}{}
					ids = append(ids, id)
				}
			}
		}
		if len(ids) == 0 {
			return []domain.ScoredChunk{}
		}
		args = append(args, ids)
		fmt.Fprintf(&where, " AND d.id=ANY($%d::uuid[])", len(args))
	}
	limitClause := ""
	if topK > 0 {
		args = append(args, topK)
		limitClause = fmt.Sprintf(" LIMIT $%d", len(args))
	}
	sql := fmt.Sprintf(`SELECT v.chunk_id,v.document_id::text,v.chunk_index,v.doc_type,v.collection,v.text,v.kind,v.quality,v.token_count,1-(v.embedding <=> $1::text::vector) AS score,
 COALESCE(d.incarnation,d.id)::text
 FROM chunk_vectors v JOIN documents d ON d.id=v.document_id
 WHERE %s
 ORDER BY (v.embedding <=> $1::text::vector) ASC, v.chunk_id ASC%s`, where.String(), limitClause)
	_, err = tx.Exec(t.Context(), `SET LOCAL enable_indexscan = off`)
	require.NoError(t, err)
	out, err := scanScoredChunks(t.Context(), tx, sql, args...)
	require.NoError(t, err)
	return out
}

func requireExactScoredParity(t *testing.T, want, got []domain.ScoredChunk) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].Chunk, got[i].Chunk, "chunk mismatch at position %d", i)
		require.InDelta(t, want[i].Score, got[i].Score, 1e-12, "score mismatch at position %d", i)
		require.Equal(t, want[i].ObservedIncarnation, got[i].ObservedIncarnation, "incarnation mismatch at position %d", i)
	}
}

func recallAtK(want, got []domain.ScoredChunk, k int) float64 {
	if k <= 0 || len(want) == 0 {
		return 1
	}
	wantN := min(k, len(want))
	wantSet := make(map[domain.ChunkID]struct{}, wantN)
	for _, sc := range want[:wantN] {
		wantSet[sc.Chunk.ID] = struct{}{}
	}
	hits := 0
	for _, sc := range got {
		if _, ok := wantSet[sc.Chunk.ID]; ok {
			hits++
		}
	}
	return float64(hits) / float64(wantN)
}

func seedCompletedDocument(t *testing.T, c *Client, id, docType, collection string) {
	t.Helper()
	_, err := c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,content_preview,status,doc_type,collection)
		VALUES($1,'Synthetic body','Synthetic preview','completed',$2,$3)`, id, docType, collection)
	require.NoError(t, err)
}

func explainVectorPlan(ctx context.Context, c *Client, settings, statement string, args ...any) (string, error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if settings != "" {
		if _, setupErr := tx.Exec(ctx, settings); setupErr != nil {
			return "", setupErr
		}
	}
	rows, err := tx.Query(ctx, statement, args...)
	if err != nil {
		return "", err
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	return strings.Join(lines, "\n"), err
}

func TestExactScope(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	baseURL := pgtest.StartVector(t, testcontainers.WithAfterReadyCommand(testcontainers.NewRawCommand(
		[]string{"localedef", "--no-archive", "-i", "es_ES", "-f", "UTF-8", "es_ES.utf8"},
		tcexec.WithUser("root"),
	)))

	t.Run("OptionsValidation", func(t *testing.T) {
		c, _ := isolatedVectorDatabase(t, baseURL)
		for _, invalid := range []int{-1, 100001} {
			_, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(invalid))
			require.Error(t, storeErr)
		}
		for _, valid := range []int{0, 1, 100000} {
			vs, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(valid))
			require.NoError(t, storeErr)
			require.Equal(t, valid, vs.exactScopeMax)
		}
	})

	t.Run("ThresholdBoundariesAndParity", func(t *testing.T) {
		c, _ := isolatedVectorDatabase(t, baseURL)
		const threshold = 10
		const topK = 5
		v, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(threshold))
		require.NoError(t, storeErr)

		distractorDoc := uuid.NewString()
		seedCompletedDocument(t, c, distractorDoc, "report", "distractor")
		_, execErr := c.Pool.Exec(t.Context(), `INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,collection,text,token_count,embedding)
			SELECT 'dist-'||i,$1::uuid,i,'report','distractor','distractor',1,('[1,'||(i*0.0001)||',0]')::vector
			FROM generate_series(1,5000) i`, distractorDoc)
		require.NoError(t, execErr)

		rareDoc := uuid.NewString()
		seedCompletedDocument(t, c, rareDoc, "report", "rare")
		initial := []domain.Chunk{
			{ID: "01-closest", DocumentID: domain.DocumentID(rareDoc), Index: 0, Text: "closest", TokenCount: 1, Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
			{ID: "02-second", DocumentID: domain.DocumentID(rareDoc), Index: 1, Text: "second", TokenCount: 1, Embedding: []float32{0.9, 0.1, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
			{ID: "03-third", DocumentID: domain.DocumentID(rareDoc), Index: 2, Text: "third", TokenCount: 1, Embedding: []float32{0.8, 0.2, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
			{ID: "tie-d", DocumentID: domain.DocumentID(rareDoc), Index: 3, Text: "tie-d", TokenCount: 1, Embedding: []float32{0.6, 0.4, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
			{ID: "tie-b", DocumentID: domain.DocumentID(rareDoc), Index: 4, Text: "tie-b", TokenCount: 1, Embedding: []float32{0.6, 0.4, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
			{ID: "tie-c", DocumentID: domain.DocumentID(rareDoc), Index: 5, Text: "tie-c", TokenCount: 1, Embedding: []float32{0.6, 0.4, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
			{ID: "tie-e", DocumentID: domain.DocumentID(rareDoc), Index: 6, Text: "tie-e", TokenCount: 1, Embedding: []float32{0.6, 0.4, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
			{ID: "08-farther", DocumentID: domain.DocumentID(rareDoc), Index: 7, Text: "farther", TokenCount: 1, Embedding: []float32{0.4, 0.8, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
			{ID: "09-farthest", DocumentID: domain.DocumentID(rareDoc), Index: 8, Text: "farthest", TokenCount: 1, Embedding: []float32{0.1, 0.9, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
		}
		require.NoError(t, v.Upsert(t.Context(), "", initial))
		_, execErr = c.Pool.Exec(t.Context(), `ANALYZE chunk_vectors`)
		require.NoError(t, execErr)

		queryVec := []float32{1, 0, 0}
		filter := domain.VectorFilter{Collection: "rare", DocType: "report"}

		checkExactStep := func(subT *testing.T, wantCandidates int) {
			subT.Helper()
			m := metrics.NewService(nil)
			ctx := metrics.WithService(subT.Context(), m)
			got, report, qErr := v.QueryFilteredDetailed(ctx, queryVec, topK, filter)
			require.NoError(subT, qErr)
			require.Equal(subT, SearchStrategyExact, report.Strategy)
			require.Equal(subT, SearchStatusOK, report.Status)
			require.True(subT, report.Probed)
			require.Equal(subT, wantCandidates, report.Candidates)

			tx, txErr := c.Pool.BeginTx(subT.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
			require.NoError(subT, txErr)
			defer func() { _ = tx.Rollback(context.Background()) }()
			want := exhaustiveQuerySnapshot(subT, tx, queryVec, 3, topK, filter)
			requireExactScoredParity(subT, want, got)

			var b strings.Builder
			require.NoError(subT, m.Registry.WriteText(&b))
			require.Contains(subT, b.String(), `quarry_pgvector_search_total{strategy="exact",status="ok"} 1`)
		}

		t.Run("threshold-1", func(subT *testing.T) {
			checkExactStep(subT, threshold-1)
		})

		require.NoError(t, v.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "tie-a", DocumentID: domain.DocumentID(rareDoc), Index: 9, Text: "tie-a", TokenCount: 1, Embedding: []float32{0.6, 0.4, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
		}))
		t.Run("threshold", func(subT *testing.T) {
			checkExactStep(subT, threshold)
			got, qErr := v.QueryFiltered(subT.Context(), queryVec, topK, filter)
			require.NoError(subT, qErr)
			require.Len(subT, got, topK)
			require.Equal(subT, domain.ChunkID("tie-a"), got[3].Chunk.ID)
			require.Equal(subT, domain.ChunkID("tie-b"), got[4].Chunk.ID)
		})

		require.NoError(t, v.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "11-overflow", DocumentID: domain.DocumentID(rareDoc), Index: 10, Text: "overflow", TokenCount: 1, Embedding: []float32{0.2, 0.9, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}},
		}))
		t.Run("threshold+1", func(t *testing.T) {
			m := metrics.NewService(nil)
			ctx := metrics.WithService(t.Context(), m)
			got, report, qErr := v.QueryFilteredDetailed(ctx, queryVec, topK, filter)
			require.NoError(t, qErr)
			require.Equal(t, SearchStrategyHNSW, report.Strategy)
			require.Equal(t, SearchStatusOK, report.Status)
			require.True(t, report.Probed)
			require.Equal(t, threshold+1, report.Candidates)
			require.Len(t, got, topK)

			var b strings.Builder
			require.NoError(t, m.Registry.WriteText(&b))
			require.Contains(t, b.String(), `quarry_pgvector_search_total{strategy="hnsw",status="ok"} 1`)
		})
	})

	t.Run("OneDocumentWithManyChunksAndFilterCombinations", func(t *testing.T) {
		c, _ := isolatedVectorDatabase(t, baseURL)
		v, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(10))
		require.NoError(t, storeErr)

		largeDoc := uuid.NewString()
		smallDocA := uuid.NewString()
		smallDocB := uuid.NewString()
		processingDoc := uuid.NewString()
		failedDoc := uuid.NewString()
		deletedDoc := uuid.NewString()

		seedCompletedDocument(t, c, largeDoc, "manual", "big")
		seedCompletedDocument(t, c, smallDocA, "report", "small")
		seedCompletedDocument(t, c, smallDocB, "note", "small")
		seedCompletedDocument(t, c, processingDoc, "report", "small")
		require.NoError(t, c.UpdateDocumentStatus(t.Context(), domain.DocumentID(processingDoc), "processing"))
		seedCompletedDocument(t, c, failedDoc, "report", "small")
		require.NoError(t, c.UpdateDocumentStatus(t.Context(), domain.DocumentID(failedDoc), "failed"))
		seedCompletedDocument(t, c, deletedDoc, "report", "small")

		_, execErr := c.Pool.Exec(t.Context(), `INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,collection,text,token_count,embedding)
			SELECT 'large-'||i,$1::uuid,i,'manual','big','large',1,('[1,'||(i*0.01)||',0]')::vector
			FROM generate_series(1,2000) i`, largeDoc)
		require.NoError(t, execErr)

		for i := range 25 {
			require.NoError(t, v.Upsert(t.Context(), "", []domain.Chunk{
				{ID: domain.ChunkID(fmt.Sprintf("proc-%d", i)), DocumentID: domain.DocumentID(processingDoc), Index: i, Text: "processing", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "small"}},
				{ID: domain.ChunkID(fmt.Sprintf("fail-%d", i)), DocumentID: domain.DocumentID(failedDoc), Index: i, Text: "failed", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "small"}},
			}))
		}
		require.NoError(t, v.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "small-a-1", DocumentID: domain.DocumentID(smallDocA), Index: 0, Text: "a1", Embedding: []float32{0.9, 0.1, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "small"}},
			{ID: "small-a-2", DocumentID: domain.DocumentID(smallDocA), Index: 1, Text: "a2", Embedding: []float32{0.7, 0.7, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "small"}},
			{ID: "small-b-1", DocumentID: domain.DocumentID(smallDocB), Index: 0, Text: "b1", Embedding: []float32{0.8, 0.2, 0}, Metadata: map[string]any{"doc_type": "note", "collection": "small"}},
			{ID: "deleted-1", DocumentID: domain.DocumentID(deletedDoc), Index: 0, Text: "del", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "small"}},
		}))
		require.NoError(t, c.DeleteDocument(t.Context(), domain.DocumentID(deletedDoc)))

		got, report, qErr := v.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 5, domain.VectorFilter{DocumentIDs: []string{largeDoc}})
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyHNSW, report.Strategy)
		require.Equal(t, 11, report.Candidates)
		require.Len(t, got, 5)

		got, report, qErr = v.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 5, domain.VectorFilter{})
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyHNSW, report.Strategy)
		require.False(t, report.Probed)
		require.Len(t, got, 5)

		manyIDs := make([]string, 0, 15)
		for range 12 {
			manyIDs = append(manyIDs, uuid.NewString())
		}
		manyIDs = append(manyIDs, processingDoc, failedDoc, smallDocA)
		got, report, qErr = v.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "small", DocType: "report", DocumentIDs: manyIDs})
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyExact, report.Strategy)
		require.Equal(t, SearchStatusOK, report.Status)
		require.Equal(t, 2, report.Candidates)
		require.Equal(t, []domain.ChunkID{"small-a-1", "small-a-2"}, []domain.ChunkID{got[0].Chunk.ID, got[1].Chunk.ID})

		revokedIDs := []string{smallDocB, deletedDoc}
		tx, txErr := c.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		require.NoError(t, txErr)
		defer func() { _ = tx.Rollback(context.Background()) }()
		for _, tc := range []struct {
			name         string
			filter       domain.VectorFilter
			wantStrategy SearchStrategy
		}{
			{"collection", domain.VectorFilter{Collection: "small"}, SearchStrategyExact},
			{"doc_type", domain.VectorFilter{DocType: "note"}, SearchStrategyExact},
			{"doc_ids", domain.VectorFilter{DocumentIDs: []string{smallDocA, smallDocB, deletedDoc, processingDoc}}, SearchStrategyExact},
			{"collection+doc_type", domain.VectorFilter{Collection: "small", DocType: "report"}, SearchStrategyExact},
			{"collection+doc_ids", domain.VectorFilter{Collection: "small", DocumentIDs: revokedIDs}, SearchStrategyExact},
			{"doc_type+doc_ids", domain.VectorFilter{DocType: "note", DocumentIDs: revokedIDs}, SearchStrategyExact},
			{"all_three", domain.VectorFilter{Collection: "small", DocType: "report", DocumentIDs: []string{smallDocA, smallDocB}}, SearchStrategyExact},
			{"empty_collection", domain.VectorFilter{Collection: "missing"}, SearchStrategyExact},
			{"empty_revoked", domain.VectorFilter{Collection: "small", DocType: "report", DocumentIDs: revokedIDs}, SearchStrategyExact},
		} {
			t.Run(tc.name, func(t *testing.T) {
				actual, rpt, subErr := v.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 5, tc.filter)
				require.NoError(t, subErr)
				require.Equal(t, tc.wantStrategy, rpt.Strategy)
				require.Equal(t, SearchStatusOK, rpt.Status)
				expected := exhaustiveQuerySnapshot(t, tx, []float32{1, 0, 0}, 3, 5, tc.filter)
				requireExactScoredParity(t, expected, actual)
				for _, sc := range actual {
					require.NotEqual(t, domain.DocumentID(processingDoc), sc.Chunk.DocumentID)
					require.NotEqual(t, domain.DocumentID(failedDoc), sc.Chunk.DocumentID)
					require.NotEqual(t, domain.DocumentID(deletedDoc), sc.Chunk.DocumentID)
				}
			})
		}
	})

	t.Run("StaleDocumentLabelsAndCollectionMove", func(t *testing.T) {
		c, _ := isolatedVectorDatabase(t, baseURL)
		movedDoc := uuid.NewString()
		eligibleDoc := uuid.NewString()
		seedCompletedDocument(t, c, movedDoc, "report", "archive")
		seedCompletedDocument(t, c, eligibleDoc, "report", "archive")

		movedChunk := domain.Chunk{
			ID:         domain.ChunkID(uuid.NewString()),
			DocumentID: domain.DocumentID(movedDoc),
			Index:      0,
			Text:       "Moved document top score.",
			TokenCount: 4,
			Embedding:  []float32{1, 0, 0},
			Metadata:   map[string]any{"doc_type": "report", "collection": "archive"},
		}
		eligibleChunk := domain.Chunk{
			ID:         domain.ChunkID(uuid.NewString()),
			DocumentID: domain.DocumentID(eligibleDoc),
			Index:      0,
			Text:       "Eligible archive evidence.",
			TokenCount: 3,
			Embedding:  []float32{0.8, 0.4, 0},
			Metadata:   map[string]any{"doc_type": "report", "collection": "archive"},
		}
		require.NoError(t, c.InsertChunks(t.Context(), []domain.Chunk{movedChunk, eligibleChunk}))

		disabled, storeErr := NewVectorStore(t.Context(), c, 3)
		require.NoError(t, storeErr)
		require.NoError(t, disabled.Upsert(t.Context(), "", []domain.Chunk{movedChunk, eligibleChunk}))

		// Move movedDoc to another collection without rewriting chunk_vectors.
		_, execErr := c.Pool.Exec(t.Context(), `UPDATE documents SET collection='moved' WHERE id=$1`, movedDoc)
		require.NoError(t, execErr)

		filter := domain.VectorFilter{Collection: "archive", DocType: "report"}
		t.Run("DisabledPreservesRankingAfterLabelMove", func(t *testing.T) {
			got, report, queryErr := disabled.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 2, filter)
			require.NoError(t, queryErr)
			require.Equal(t, SearchStrategyHNSW, report.Strategy)
			require.False(t, report.ProbeAttempted)
			require.Len(t, got, 2)
			require.Equal(t, movedChunk.ID, got[0].Chunk.ID)
			require.Equal(t, eligibleChunk.ID, got[1].Chunk.ID)
			require.Greater(t, got[0].Score, got[1].Score)
		})

		storeOne, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(1))
		require.NoError(t, storeErr)
		storeTwo, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(2))
		require.NoError(t, storeErr)

		tx, txErr := c.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		require.NoError(t, txErr)
		defer func() { _ = tx.Rollback(context.Background()) }()

		wantMoved := exhaustiveQuerySnapshot(t, tx, []float32{1, 0, 0}, 3, 1, filter)
		require.Len(t, wantMoved, 1)
		require.Equal(t, eligibleChunk.ID, wantMoved[0].Chunk.ID)

		for _, tc := range []struct {
			name  string
			store *VectorStore
		}{
			{"threshold=1", storeOne},
			{"threshold=2", storeTwo},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, rpt, qErr := tc.store.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 1, filter)
				require.NoError(t, qErr)
				require.Equal(t, SearchStrategyExact, rpt.Strategy)
				require.Equal(t, SearchStatusOK, rpt.Status)
				require.True(t, rpt.Probed)
				require.Equal(t, 1, rpt.Candidates)
				requireExactScoredParity(t, wantMoved, got)

				// End-to-end pipeline retrieval must not let the higher-scoring moved doc displace eligibleDoc at K=1.
				ret := retriever.NewRetriever(tc.store, staticEmbedder{vec: []float32{1, 0, 0}}, 0.7, 1)
				qc := &pipeline.QueryContext{
					OriginalQuery: "What is in the archive?",
					RequestedMode: "local",
					MaxChunks:     1,
					FilterDocType: "report",
					Collection:    "archive",
				}
				vis := Visibility{Pool: c.Pool, Collection: "archive", DocType: "report"}
				prevs := Previews{Pool: c.Pool, Collection: "archive", DocType: "report"}
				_, pipeErr := pipeline.RunRetrievalPipeline(t.Context(), qc, []pipeline.QueryStage{
					pipeline.NewDecomposeStage(),
					pipeline.NewRoutingStageWithScope(ret, vis, prevs),
					pipeline.NewRetrievalStageWithVisibility(ret, vis),
				})
				require.NoError(t, pipeErr)
				require.Len(t, qc.RetrievedChunks, 1)
				require.Equal(t, eligibleChunk.ID, qc.RetrievedChunks[0].Chunk.ID)
			})
		}

		// With 3 eligible docs (> threshold 2) and 1 stale moved doc, HNSW also excludes movedDoc before K.
		extraDocA, extraDocB := uuid.NewString(), uuid.NewString()
		seedCompletedDocument(t, c, extraDocA, "report", "archive")
		seedCompletedDocument(t, c, extraDocB, "report", "archive")
		extraChunks := []domain.Chunk{
			{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(extraDocA), Text: "extra a", TokenCount: 2, Embedding: []float32{0.7, 0.5, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "archive"}},
			{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(extraDocB), Text: "extra b", TokenCount: 2, Embedding: []float32{0.6, 0.6, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "archive"}},
		}
		require.NoError(t, c.InsertChunks(t.Context(), extraChunks))
		require.NoError(t, storeTwo.Upsert(t.Context(), "", extraChunks))

		txHNSW, txHErr := c.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		require.NoError(t, txHErr)
		defer func() { _ = txHNSW.Rollback(context.Background()) }()
		wantHNSW := exhaustiveQuerySnapshot(t, txHNSW, []float32{1, 0, 0}, 3, 3, filter)
		require.Len(t, wantHNSW, 3)

		gotHNSW, rptHNSW, qErr := storeTwo.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 3, filter)
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyHNSW, rptHNSW.Strategy)
		require.Equal(t, 3, rptHNSW.Candidates)
		requireExactScoredParity(t, wantHNSW, gotHNSW)

		// Also verify stale doc_type exclusion against independent baseline.
		_, execErr = c.Pool.Exec(t.Context(), `UPDATE documents SET doc_type='memo' WHERE id=$1`, eligibleDoc)
		require.NoError(t, execErr)
		txType, txTErr := c.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		require.NoError(t, txTErr)
		defer func() { _ = txType.Rollback(context.Background()) }()
		wantType := exhaustiveQuerySnapshot(t, txType, []float32{1, 0, 0}, 3, 2, filter)
		require.Len(t, wantType, 2)
		gotType, rptType, qErr := storeTwo.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 2, filter)
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyExact, rptType.Strategy)
		require.Equal(t, 2, rptType.Candidates)
		requireExactScoredParity(t, wantType, gotType)
		approxType, approxReport, qErr := storeOne.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 3, filter)
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyHNSW, approxReport.Strategy)
		require.Equal(t, 2, approxReport.Candidates)
		requireExactScoredParity(t, wantType, approxType)

		t.Run("DestinationLabels", func(t *testing.T) {
			_, updateErr := c.Pool.Exec(t.Context(), `UPDATE documents SET doc_type='memo' WHERE id=$1`, movedDoc)
			require.NoError(t, updateErr)
			filters := []domain.VectorFilter{
				{Collection: "moved"},
				{DocType: "memo"},
				{Collection: "moved", DocType: "memo"},
			}
			emptySnapshot, emptySnapErr := c.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
			require.NoError(t, emptySnapErr)
			defer func() { _ = emptySnapshot.Rollback(context.Background()) }()
			for _, filter := range filters {
				want := exhaustiveQuerySnapshot(t, emptySnapshot, []float32{1, 0, 0}, 3, 3, filter)
				require.Empty(t, want, "%+v", filter)
				got, report, queryErr := storeTwo.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 3, filter)
				require.NoError(t, queryErr)
				require.Equal(t, SearchStrategyExact, report.Strategy)
				require.True(t, report.Probed)
				require.Zero(t, report.Candidates)
				require.Empty(t, got)
			}
			require.NoError(t, emptySnapshot.Rollback(t.Context()))

			destination := uuid.NewString()
			seedCompletedDocument(t, c, destination, "memo", "moved")
			require.NoError(t, storeTwo.Upsert(t.Context(), "", []domain.Chunk{
				{ID: "destination-first", DocumentID: domain.DocumentID(destination), Embedding: []float32{0.7, 0.5, 0}, Metadata: map[string]any{"doc_type": "memo", "collection": "moved"}},
				{ID: "destination-second", DocumentID: domain.DocumentID(destination), Index: 1, Embedding: []float32{0.6, 0.6, 0}, Metadata: map[string]any{"doc_type": "memo", "collection": "moved"}},
			}))
			snapshot, snapErr := c.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
			require.NoError(t, snapErr)
			defer func() { _ = snapshot.Rollback(context.Background()) }()
			for _, filter := range filters {
				want := exhaustiveQuerySnapshot(t, snapshot, []float32{1, 0, 0}, 3, 3, filter)
				require.Len(t, want, 2)
				for _, tc := range []struct {
					store    *VectorStore
					strategy SearchStrategy
				}{
					{storeTwo, SearchStrategyExact},
					{storeOne, SearchStrategyHNSW},
				} {
					got, report, queryErr := tc.store.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 3, filter)
					require.NoError(t, queryErr)
					require.Equal(t, tc.strategy, report.Strategy)
					require.True(t, report.Probed)
					require.Equal(t, 2, report.Candidates)
					requireExactScoredParity(t, want, got)
					require.Equal(t, domain.ChunkID("destination-first"), got[0].Chunk.ID)
					require.Equal(t, domain.ChunkID("destination-second"), got[1].Chunk.ID)
					for _, chunk := range got {
						require.Equal(t, domain.DocumentID(destination), chunk.Chunk.DocumentID)
						require.Equal(t, map[string]any{"doc_type": "memo", "collection": "moved"}, chunk.Chunk.Metadata)
					}
				}
			}
		})
	})

	t.Run("LocalTopUpUsesDatabaseCollation", func(t *testing.T) {
		c, err := NewClient(t.Context(), pgtest.SiblingNumericCollation(t, baseURL))
		require.NoError(t, err)
		t.Cleanup(c.Pool.Close)
		require.NoError(t, c.RunMigrations(t.Context()))
		vectors, err := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(8))
		require.NoError(t, err)

		const target = "00000000-0000-4000-8000-000000000003"
		const other = "00000000-0000-4000-8000-000000000004"
		query := []float32{1, 0, 0}
		splitter := chunker.NewChunker(32, 4)
		for _, docID := range []string{target, other} {
			seedCompletedDocument(t, c, docID, "report", "archive")
			chunks := splitter.Chunk(domain.DocumentID(docID), "Identical search evidence.")
			require.Len(t, chunks, 1)
			chunks[0].Embedding = query
			chunks[0].Metadata["doc_type"] = "report"
			chunks[0].Metadata["collection"] = "archive"
			require.NoError(t, vectors.Upsert(t.Context(), "", chunks))
		}

		filter := domain.VectorFilter{Collection: "archive"}
		ret := retriever.NewRetriever(tracing.WrapVectorStore(vectors, "postgresql"), nil, 0.7, 2)
		for _, k := range []int{1, 2} {
			t.Run(fmt.Sprintf("K=%d", k), func(t *testing.T) {
				tx, err := c.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
				require.NoError(t, err)
				defer func() { _ = tx.Rollback(context.Background()) }()
				want := exhaustiveQuerySnapshot(t, tx, query, 3, k, filter)
				require.Len(t, want, k)
				require.Equal(t, domain.DocumentID(other), want[0].Chunk.DocumentID)
				for _, chunk := range want {
					require.Equal(t, 1.0, chunk.Score)
				}
				direct, report, err := vectors.QueryFilteredDetailed(t.Context(), query, k, filter)
				require.NoError(t, err)
				require.Equal(t, SearchStrategyExact, report.Strategy)
				requireExactScoredParity(t, want, direct)

				local, err := ret.RetrieveWithEmbedding(t.Context(), query, retriever.RetrievalModeLocal, target, k, filter)
				require.NoError(t, err)
				require.Equal(t, retriever.RetrievalModeLocal, local.Mode)
				requireExactScoredParity(t, want, local.Chunks)
			})
		}
	})

	t.Run("IndexRolloutDisabledAndConcurrentPrebuild", func(t *testing.T) {
		c, dbURL := isolatedVectorDatabase(t, baseURL)
		// Initialize base vector schema with feature disabled (exactScopeMax=0).
		disabledStore, storeErr := NewVectorStore(t.Context(), c, 3)
		require.NoError(t, storeErr)

		docID := uuid.NewString()
		seedCompletedDocument(t, c, docID, "report", "rollout")
		require.NoError(t, disabledStore.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "r1", DocumentID: domain.DocumentID(docID), Index: 0, Text: "rollout-1", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rollout"}},
			{ID: "r2", DocumentID: domain.DocumentID(docID), Index: 1, Text: "rollout-2", Embedding: []float32{0.9, 0.1, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rollout"}},
		}))

		// Simulate an upgrade on a populated table at exactScopeMax=0 while a writer holds RowExclusiveLock:
		// startup must skip composite index creation and must not block on ShareLock.
		writerTx, wErr := c.Pool.Begin(t.Context())
		require.NoError(t, wErr)
		defer func() { _ = writerTx.Rollback(context.Background()) }()
		_, updErr := writerTx.Exec(t.Context(), `UPDATE chunk_vectors SET text='updating' WHERE chunk_id='r1'`)
		require.NoError(t, updErr)

		upgradeCtx, cancelUpgrade := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancelUpgrade()
		_, storeErr = NewVectorStore(upgradeCtx, c, 3, WithExactScopeMax(0))
		require.NoError(t, storeErr)
		require.NoError(t, writerTx.Commit(t.Context()))

		var compositeCount int
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_indexes WHERE tablename='chunk_vectors' AND indexname IN ('idx_chunk_vectors_collection_doc_type','idx_chunk_vectors_doc_type_document_id')`).Scan(&compositeCount))
		require.Zero(t, compositeCount)

		// Hold a writer open so PostgreSQL waits during the concurrent build.
		writeClient, wcErr := NewClient(t.Context(), dbURL)
		require.NoError(t, wcErr)
		t.Cleanup(func() { require.NoError(t, writeClient.Close(context.Background())) })
		prebuildWriter, wErr := writeClient.Pool.Begin(t.Context())
		require.NoError(t, wErr)
		defer func() { _ = prebuildWriter.Rollback(context.Background()) }()
		_, updErr = prebuildWriter.Exec(t.Context(), `UPDATE chunk_vectors SET token_count=2 WHERE chunk_id='r1'`)
		require.NoError(t, updErr)

		buildCtx, cancelBuild := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancelBuild()
		buildErr := make(chan error, 1)
		go func() {
			_, err := NewVectorStore(buildCtx, c, 3, WithExactScopeMax(10))
			buildErr <- err
		}()
		require.Eventually(t, func() bool {
			var waiting bool
			err := c.Pool.QueryRow(buildCtx, `SELECT EXISTS (
				SELECT FROM pg_stat_activity
				WHERE datname=current_database() AND wait_event_type='Lock'
				  AND query LIKE 'CREATE INDEX CONCURRENTLY%chunk_vectors%'
			)`).Scan(&waiting)
			return err == nil && waiting
		}, 5*time.Second, 20*time.Millisecond)
		writeCtx, cancelWrite := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancelWrite()
		_, updErr = c.Pool.Exec(writeCtx, `UPDATE chunk_vectors SET token_count=3 WHERE chunk_id='r2'`)
		require.NoError(t, updErr)
		admitCtx, cancelAdmission := context.WithTimeout(t.Context(), time.Second)
		defer cancelAdmission()
		accepted, admitErr := writeClient.AcceptDocument(admitCtx, jobsRequest("document accepted during optional index build"))
		require.NoError(t, admitErr)
		require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
		require.NoError(t, prebuildWriter.Commit(t.Context()))
		require.NoError(t, <-buildErr)

		var validCount int
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid WHERE i.indrelid='chunk_vectors'::regclass AND idx.relname IN ('idx_chunk_vectors_collection_doc_type','idx_chunk_vectors_doc_type_document_id') AND i.indisvalid`).Scan(&validCount))
		require.Equal(t, 2, validCount)

		// Cancel a concurrent build of the intended definition while a writer keeps it waiting.
		_, dropErr := c.Pool.Exec(t.Context(), `DROP INDEX CONCURRENTLY idx_chunk_vectors_collection_doc_type`)
		require.NoError(t, dropErr)
		interruptedWriter, beginErr := writeClient.Pool.Begin(t.Context())
		require.NoError(t, beginErr)
		defer func() { _ = interruptedWriter.Rollback(context.Background()) }()
		_, updateErr := interruptedWriter.Exec(t.Context(), `UPDATE chunk_vectors SET token_count=4 WHERE chunk_id='r1'`)
		require.NoError(t, updateErr)
		interruptedCtx, cancelInterrupted := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancelInterrupted()
		interrupted := make(chan error, 1)
		go func() {
			_, interruptedErr := NewVectorStore(interruptedCtx, c, 3, WithExactScopeMax(10))
			interrupted <- interruptedErr
		}()
		var builderPID int32
		require.Eventually(t, func() bool {
			err := c.Pool.QueryRow(interruptedCtx, `SELECT pid FROM pg_stat_activity
				WHERE datname=current_database() AND wait_event_type='Lock'
				  AND query LIKE 'CREATE INDEX CONCURRENTLY%chunk_vectors%'
				  AND EXISTS (SELECT FROM pg_index
				              WHERE indexrelid=to_regclass('idx_chunk_vectors_collection_doc_type') AND NOT indisvalid)`).Scan(&builderPID)
			return err == nil
		}, 5*time.Second, 20*time.Millisecond)
		var canceled bool
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT pg_cancel_backend($1)`, builderPID).Scan(&canceled))
		require.True(t, canceled)
		interruptedErr := <-interrupted
		var pgErr *pgconn.PgError
		require.ErrorAs(t, interruptedErr, &pgErr)
		require.Equal(t, "57014", pgErr.Code)
		require.ErrorContains(t, interruptedErr, "inspect")
		require.NotContains(t, interruptedErr.Error(), "DROP INDEX")
		require.NoError(t, interruptedWriter.Rollback(t.Context()))

		_, failErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(10))
		require.Error(t, failErr)
		require.Contains(t, failErr.Error(), "idx_chunk_vectors_collection_doc_type")
		require.Contains(t, failErr.Error(), `DROP INDEX CONCURRENTLY "public"."idx_chunk_vectors_collection_doc_type"`)
		require.Contains(t, failErr.Error(), "CREATE INDEX CONCURRENTLY")
		require.Contains(t, failErr.Error(), `ON "public"."chunk_vectors"`)

		_, dropInvalidErr := c.Pool.Exec(t.Context(), `DROP INDEX CONCURRENTLY IF EXISTS idx_chunk_vectors_collection_doc_type`)
		require.NoError(t, dropInvalidErr)
		rebuiltStore, rebuildErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(10))
		require.NoError(t, rebuildErr)
		got, rpt, qErr := rebuiltStore.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "rollout"})
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyExact, rpt.Strategy)
		require.Len(t, got, 2)
	})

	t.Run("IndexRolloutServingRoleDuringOwnerPrebuild", func(t *testing.T) {
		c, dbURL := isolatedVectorDatabase(t, baseURL)
		_, err := NewVectorStore(t.Context(), c, 3)
		require.NoError(t, err)
		role := pgx.Identifier{pgtest.Role(t, dbURL, "exact_scope_service")}.Sanitize()
		_, err = c.Pool.Exec(t.Context(), `CREATE ROLE `+role+`;
			GRANT USAGE ON SCHEMA public TO `+role+`;
			GRANT SELECT, INSERT, UPDATE, DELETE ON chunk_vectors TO `+role)
		require.NoError(t, err)
		service := grantRoleClient(t, dbURL, "SET SESSION AUTHORIZATION "+role)

		writer, err := c.Pool.Begin(t.Context())
		require.NoError(t, err)
		defer func() { _ = writer.Rollback(context.Background()) }()
		_, err = writer.Exec(t.Context(), `LOCK TABLE chunk_vectors IN ROW EXCLUSIVE MODE`)
		require.NoError(t, err)

		buildCtx, cancelBuild := context.WithTimeout(t.Context(), 20*time.Second)
		buildDone := make(chan struct{})
		var buildErr error
		go func() {
			defer close(buildDone)
			_, buildErr = c.Pool.Exec(buildCtx, `CREATE INDEX CONCURRENTLY idx_chunk_vectors_collection_doc_type
				ON public.chunk_vectors USING btree (collection, doc_type, document_id)`)
		}()
		defer func() {
			cancelBuild()
			<-buildDone
		}()

		const progress = `SELECT EXISTS (SELECT FROM pg_stat_progress_create_index
			WHERE datname = current_database() AND index_relid = to_regclass('public.idx_chunk_vectors_collection_doc_type'))`
		require.Eventually(t, func() bool {
			var building bool
			queryErr := c.Pool.QueryRow(buildCtx, progress).Scan(&building)
			return queryErr == nil && building
		}, 7*time.Second, 20*time.Millisecond)
		var serviceSeesBuild bool
		require.NoError(t, service.Pool.QueryRow(buildCtx, progress).Scan(&serviceSeesBuild))
		require.False(t, serviceSeesBuild)

		_, startupErr := NewVectorStore(buildCtx, service, 3, WithExactScopeMax(8))
		require.Error(t, startupErr)
		require.NotContains(t, startupErr.Error(), "DROP INDEX")
		require.ErrorContains(t, startupErr, "pg_read_all_stats")

		require.NoError(t, writer.Rollback(t.Context()))
		<-buildDone
		require.NoError(t, buildErr)
		_, err = c.Pool.Exec(t.Context(), `CREATE INDEX CONCURRENTLY idx_chunk_vectors_doc_type_document_id
			ON public.chunk_vectors USING btree (doc_type, document_id)`)
		require.NoError(t, err)
		_, err = NewVectorStore(t.Context(), service, 3, WithExactScopeMax(8))
		require.NoError(t, err)
	})

	t.Run("IndexRolloutRejectsConflictingObjects", func(t *testing.T) {
		for _, tc := range []struct {
			name, ddl, diagnostic string
		}{
			{"foreign_unique_index", `CREATE TABLE application_records(seq int); CREATE UNIQUE INDEX idx_chunk_vectors_collection_doc_type ON application_records(seq)`, "object-name conflict"},
			{"table_with_index_name", `CREATE TABLE idx_chunk_vectors_collection_doc_type(seq int)`, "object-name conflict"},
			{"wrong_keys", `CREATE INDEX idx_chunk_vectors_collection_doc_type ON chunk_vectors(token_count)`, "incompatible definition"},
			{"partial_index", `CREATE INDEX idx_chunk_vectors_doc_type_document_id ON chunk_vectors(doc_type, document_id) WHERE false`, "incompatible definition"},
			{"extra_key", `CREATE INDEX idx_chunk_vectors_doc_type_document_id ON chunk_vectors(doc_type, document_id, token_count DESC)`, "incompatible definition"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c, _ := isolatedVectorDatabase(t, baseURL)
				_, initErr := NewVectorStore(t.Context(), c, 3)
				require.NoError(t, initErr)
				_, ddlErr := c.Pool.Exec(t.Context(), tc.ddl)
				require.NoError(t, ddlErr)
				_, startupErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(10))
				require.ErrorContains(t, startupErr, tc.diagnostic)
				require.NotContains(t, startupErr.Error(), "DROP INDEX")
			})
		}
	})

	t.Run("IndexRolloutResolvesVectorTableSchema", func(t *testing.T) {
		c, dbURL := isolatedVectorDatabase(t, baseURL)
		_, initErr := NewVectorStore(t.Context(), c, 3)
		require.NoError(t, initErr)
		_, ddlErr := c.Pool.Exec(t.Context(), `CREATE SCHEMA "vector scope";
			ALTER TABLE chunk_vectors SET SCHEMA "vector scope";
			CREATE TABLE application_records(seq int);
			CREATE UNIQUE INDEX idx_chunk_vectors_collection_doc_type ON application_records(seq)`)
		require.NoError(t, ddlErr)
		scoped, connectErr := NewClient(t.Context(), dbURL, func(config *pgxpool.Config) {
			config.ConnConfig.RuntimeParams["search_path"] = `"vector scope", public`
		})
		require.NoError(t, connectErr)
		t.Cleanup(func() { require.NoError(t, scoped.Close(context.Background())) })

		var schema string
		require.NoError(t, scoped.Pool.QueryRow(t.Context(), `SELECT current_schema()`).Scan(&schema))
		require.Equal(t, "vector scope", schema)
		_, startupErr := NewVectorStore(t.Context(), scoped, 3, WithExactScopeMax(10))
		require.NoError(t, startupErr)
		var count int
		require.NoError(t, scoped.Pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_indexes
			WHERE schemaname='vector scope' AND tablename='chunk_vectors'
			  AND indexname IN ('idx_chunk_vectors_collection_doc_type','idx_chunk_vectors_doc_type_document_id')`).Scan(&count))
		require.Equal(t, 2, count)
	})

	t.Run("DenseDistractorsRareCollectionAndApproximateRecall", func(t *testing.T) {
		c, dbURL := isolatedVectorDatabase(t, baseURL)
		hnswStore, storeErr := NewVectorStore(t.Context(), c, 3)
		require.NoError(t, storeErr)
		exactStore, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(20))
		require.NoError(t, storeErr)

		distractorDoc := uuid.NewString()
		rareDoc := uuid.NewString()
		mediumDoc := uuid.NewString()
		seedCompletedDocument(t, c, distractorDoc, "report", "shared")
		seedCompletedDocument(t, c, rareDoc, "report", "shared")
		seedCompletedDocument(t, c, mediumDoc, "report", "medium")

		seedTx, seedErr := c.Pool.Begin(t.Context())
		require.NoError(t, seedErr)
		defer func() { _ = seedTx.Rollback(context.Background()) }()
		_, execErr := seedTx.Exec(t.Context(), `SET LOCAL statement_timeout = '2min'`)
		require.NoError(t, execErr)
		_, execErr = seedTx.Exec(t.Context(), `INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,collection,text,token_count,embedding)
			SELECT 'dist-'||i,$1::uuid,i,'report','shared','distractor',1,('[1,'||((i%1000)*0.0001)||','||((i/1000)*0.0001)||']')::vector
			FROM generate_series(1,25000) i`, distractorDoc)
		require.NoError(t, execErr)
		require.NoError(t, seedTx.Commit(t.Context()))
		// Place six eligible chunks farther from the query than the dense distractors.
		_, execErr = c.Pool.Exec(t.Context(), `INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,collection,text,token_count,embedding)
			SELECT 'rare-'||i,$1::uuid,i,'report','shared','rare',1,('['||(-0.8+i*0.05)||',0.3,0]')::vector
			FROM generate_series(1,6) i`, rareDoc)
		require.NoError(t, execErr)
		_, execErr = c.Pool.Exec(t.Context(), `INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,collection,text,token_count,embedding)
			SELECT 'med-'||i,$1::uuid,i,'report','medium','medium',1,('['||(1.0-i*0.004)||','||(0.01+i*0.004)||',0]')::vector
			FROM generate_series(1,150) i`, mediumDoc)
		require.NoError(t, execErr)
		_, execErr = c.Pool.Exec(t.Context(), `ANALYZE chunk_vectors`)
		require.NoError(t, execErr)

		queryVec := []float32{1, 0, 0}
		rareIDs := make([]string, 0, 1000)
		for range 999 {
			rareIDs = append(rareIDs, uuid.NewString())
		}
		rareIDs = append(rareIDs, rareDoc)
		rareFilter := domain.VectorFilter{Collection: "shared", DocType: "report", DocumentIDs: rareIDs}
		mediumFilter := domain.VectorFilter{Collection: "medium", DocType: "report"}

		tx, txErr := c.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		require.NoError(t, txErr)
		defer func() { _ = tx.Rollback(context.Background()) }()
		wantRare := exhaustiveQuerySnapshot(t, tx, queryVec, 3, 10, rareFilter)
		require.Len(t, wantRare, 6)
		wantMedium := exhaustiveQuerySnapshot(t, tx, queryVec, 3, 10, mediumFilter)
		require.Len(t, wantMedium, 10)

		explainFilter := func(store *VectorStore, filter domain.VectorFilter) string {
			t.Helper()
			sqlQ, sqlArgs, ok := searchSQL("[1,0,0]", 10, filter, store.exactScopeMax > 0)
			require.True(t, ok)
			plan, expErr := explainVectorPlan(t.Context(), store.client, `SET LOCAL hnsw.iterative_scan = strict_order`, "EXPLAIN "+sqlQ, sqlArgs...)
			require.NoError(t, expErr)
			return plan
		}

		checkHNSWResults := func(t *testing.T, got []domain.ScoredChunk, rpt SearchReport, wantDoc, wantColl string, wantK int, want []domain.ScoredChunk, recallK int, label, plan string) {
			t.Helper()
			require.Equal(t, SearchStrategyHNSW, rpt.Strategy)
			underfill := max(0, wantK-len(got))
			if underfill > 0 {
				require.Equal(t, SearchStatusPartial, rpt.Status)
			} else {
				require.Equal(t, SearchStatusOK, rpt.Status)
			}
			for i, sc := range got {
				require.Equal(t, domain.DocumentID(wantDoc), sc.Chunk.DocumentID)
				require.Equal(t, "report", sc.Chunk.Metadata["doc_type"])
				require.Equal(t, wantColl, sc.Chunk.Metadata["collection"])
				if i > 0 {
					require.GreaterOrEqual(t, got[i-1].Score, sc.Score)
					if got[i-1].Score == sc.Score {
						require.Less(t, got[i-1].Chunk.ID, sc.Chunk.ID)
					}
				}
			}
			recall := recallAtK(want, got, recallK)
			require.GreaterOrEqual(t, recall, 0.0)
			require.LessOrEqual(t, recall, 1.0)
			t.Logf("%s: returned=%d underfill=%d status=%s recall@%d=%.3f plan_has_hnsw=%v",
				label, len(got), underfill, rpt.Status, recallK, recall, strings.Contains(plan, "idx_chunk_vectors_embedding"))
			t.Logf("%s EXPLAIN:\n%s", label, plan)
		}

		// Compare approximate HNSW when the planner chooses idx_chunk_vectors_embedding
		// (enable_seqscan=off&enable_sort=off) and under default planner selection.
		hnswForcedURL := withFixtureOptions(t, dbURL, "enable_seqscan=off&enable_sort=off")
		hnswForcedClient, fErr := NewClient(t.Context(), hnswForcedURL)
		require.NoError(t, fErr)
		t.Cleanup(func() { require.NoError(t, hnswForcedClient.Close(context.Background())) })
		hnswForcedStore, storeErr := NewVectorStore(t.Context(), hnswForcedClient, 3)
		require.NoError(t, storeErr)

		forcedRarePlan := explainFilter(hnswForcedStore, rareFilter)
		require.Contains(t, forcedRarePlan, "idx_chunk_vectors_embedding")
		gotForcedRare, rptForcedRare, qErr := hnswForcedStore.QueryFilteredDetailed(t.Context(), queryVec, 10, rareFilter)
		require.NoError(t, qErr)
		checkHNSWResults(t, gotForcedRare, rptForcedRare, rareDoc, "shared", 10, wantRare, 6, "hnsw forced-index rare", forcedRarePlan)

		defaultRarePlan := explainFilter(hnswStore, rareFilter)
		gotApproxRare, rptApproxRare, qErr := hnswStore.QueryFilteredDetailed(t.Context(), queryVec, 10, rareFilter)
		require.NoError(t, qErr)
		require.False(t, rptApproxRare.Probed)
		checkHNSWResults(t, gotApproxRare, rptApproxRare, rareDoc, "shared", 10, wantRare, 6, "hnsw default-planner rare", defaultRarePlan)

		gotExactRare, rptExactRare, qErr := exactStore.QueryFilteredDetailed(t.Context(), queryVec, 10, rareFilter)
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyExact, rptExactRare.Strategy)
		require.Equal(t, SearchStatusOK, rptExactRare.Status)
		require.True(t, rptExactRare.Probed)
		require.Equal(t, 6, rptExactRare.Candidates)
		requireExactScoredParity(t, wantRare, gotExactRare)
		require.Equal(t, 1.0, recallAtK(wantRare, gotExactRare, 6))

		defaultMediumPlan := explainFilter(exactStore, mediumFilter)
		gotMedium, rptMedium, qErr := exactStore.QueryFilteredDetailed(t.Context(), queryVec, 10, mediumFilter)
		require.NoError(t, qErr)
		require.True(t, rptMedium.Probed)
		require.Equal(t, 21, rptMedium.Candidates)
		checkHNSWResults(t, gotMedium, rptMedium, mediumDoc, "medium", 10, wantMedium, 10, "hnsw default-planner medium (150 rows > max 20)", defaultMediumPlan)

		// Verify exact mode still achieves 100% recall under enable_seqscan=off&enable_sort=off,
		// and record forced-HNSW recall@10 on the larger 150-row slice.
		exactForcedStore, storeErr := NewVectorStore(t.Context(), hnswForcedClient, 3, WithExactScopeMax(20))
		require.NoError(t, storeErr)
		gotExactForced, rptExactForced, qErr := exactForcedStore.QueryFilteredDetailed(t.Context(), queryVec, 10, rareFilter)
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyExact, rptExactForced.Strategy)
		require.Equal(t, SearchStatusOK, rptExactForced.Status)
		requireExactScoredParity(t, wantRare, gotExactForced)

		forcedMediumPlan := explainFilter(exactForcedStore, mediumFilter)
		require.Contains(t, forcedMediumPlan, "idx_chunk_vectors_embedding")
		gotMedForced, rptMedForced, qErr := exactForcedStore.QueryFilteredDetailed(t.Context(), queryVec, 10, mediumFilter)
		require.NoError(t, qErr)
		checkHNSWResults(t, gotMedForced, rptMedForced, mediumDoc, "medium", 10, wantMedium, 10, "hnsw forced-index medium (150 rows > max 20)", forcedMediumPlan)
	})

	t.Run("ShortHNSWResultWithoutExhaustion", func(t *testing.T) {
		c, _ := isolatedVectorDatabase(t, baseURL)
		docID := uuid.NewString()
		seedCompletedDocument(t, c, docID, "report", "small")
		v, storeErr := NewVectorStore(t.Context(), c, 3)
		require.NoError(t, storeErr)
		require.NoError(t, v.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "only-1", DocumentID: domain.DocumentID(docID), Index: 0, Text: "one", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "small"}},
			{ID: "only-2", DocumentID: domain.DocumentID(docID), Index: 1, Text: "two", Embedding: []float32{0.8, 0.2, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "small"}},
		}))
		got, rpt, qErr := v.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 10, domain.VectorFilter{Collection: "small"})
		require.NoError(t, qErr)
		require.Equal(t, SearchStrategyHNSW, rpt.Strategy)
		require.Equal(t, SearchStatusPartial, rpt.Status)
		require.False(t, rpt.ProbeAttempted)
		require.False(t, rpt.Probed)
		require.Len(t, got, 2)
	})

	t.Run("ProbeReportingAndShortCircuits", func(t *testing.T) {
		c, dbURL := isolatedVectorDatabase(t, baseURL)
		docID := uuid.NewString()
		seedCompletedDocument(t, c, docID, "report", "populated")
		vs, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(10))
		require.NoError(t, storeErr)
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "p1", DocumentID: domain.DocumentID(docID), Index: 0, Text: "populated", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "populated"}},
		}))

		recorder := tracetest.NewSpanRecorder()
		prev := otel.GetTracerProvider()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		otel.SetTracerProvider(tp)
		t.Cleanup(func() {
			otel.SetTracerProvider(prev)
			require.NoError(t, tp.Shutdown(context.Background()))
		})

		// Zero matches on an enabled filtered probe: Probed=true and Candidates=0 is measured.
		gotZero, rptZero, qErr := vs.QueryFilteredDetailed(metrics.WithService(t.Context(), metrics.NewService(nil)), []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "missing"})
		require.NoError(t, qErr)
		require.Empty(t, gotZero)
		require.Equal(t, SearchStrategyExact, rptZero.Strategy)
		require.Equal(t, SearchStatusOK, rptZero.Status)
		require.True(t, rptZero.ProbeAttempted)
		require.True(t, rptZero.Probed)
		require.Zero(t, rptZero.Candidates)

		ctxZero, spanZero := otel.Tracer("test").Start(metrics.WithService(t.Context(), metrics.NewService(nil)), "zero-match-span")
		_, rptSpanZero, zErr := vs.QueryFilteredDetailed(ctxZero, []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "missing"})
		require.NoError(t, zErr)
		require.True(t, rptSpanZero.Probed)
		spanZero.End()
		ended := recorder.Ended()
		lastSpan := ended[len(ended)-1]
		var foundCandidates bool
		var recordedCandidates int64
		for _, attr := range lastSpan.Attributes() {
			if attr.Key == "quarry.vector.scope_candidates" {
				foundCandidates = true
				recordedCandidates = attr.Value.AsInt64()
			}
		}
		require.True(t, foundCandidates)
		require.Zero(t, recordedCandidates)

		// Short-circuits before SQL: empty allowlist & invalid-only UUID list.
		for _, f := range []domain.VectorFilter{
			{DocumentIDs: []string{}},
			{DocumentIDs: []string{"not-a-uuid"}},
		} {
			gotShort, rptShort, sErr := vs.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 5, f)
			require.NoError(t, sErr)
			require.Empty(t, gotShort)
			require.Empty(t, rptShort.Strategy)
			require.Equal(t, SearchStatusOK, rptShort.Status)
			require.False(t, rptShort.ProbeAttempted)
			require.False(t, rptShort.Probed)
			require.Zero(t, rptShort.Candidates)
		}

		// Invalid topK short-circuit before SQL returns SearchStatusError.
		_, rptTopK, topKErr := vs.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 10001, domain.VectorFilter{Collection: "populated"})
		require.Error(t, topKErr)
		require.Empty(t, rptTopK.Strategy)
		require.Equal(t, SearchStatusError, rptTopK.Status)
		require.False(t, rptTopK.ProbeAttempted)
		require.False(t, rptTopK.Probed)

		// Failed count query against populated table must NOT publish scope_candidates=0.
		closedCountClient, ccErr := NewClient(t.Context(), dbURL)
		require.NoError(t, ccErr)
		t.Cleanup(func() { require.NoError(t, closedCountClient.Close(context.Background())) })
		brokenVS, bvErr := NewVectorStore(t.Context(), closedCountClient, 3, WithExactScopeMax(10))
		require.NoError(t, bvErr)

		failSchemaClient, fsErr := NewClient(t.Context(), dbURL)
		require.NoError(t, fsErr)
		t.Cleanup(func() { require.NoError(t, failSchemaClient.Close(context.Background())) })
		_, renameErr := failSchemaClient.Pool.Exec(t.Context(), `ALTER TABLE documents RENAME TO hidden_documents`)
		require.NoError(t, renameErr)

		ctxFailCount, spanFailCount := otel.Tracer("test").Start(metrics.WithService(t.Context(), metrics.NewService(nil)), "failed-probe-count")
		_, rptFailCount, fcErr := brokenVS.QueryFilteredDetailed(ctxFailCount, []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "populated"})
		spanFailCount.End()
		require.Error(t, fcErr)
		require.Equal(t, SearchStrategyProbe, rptFailCount.Strategy)
		require.Equal(t, SearchStatusError, rptFailCount.Status)
		require.True(t, rptFailCount.ProbeAttempted)
		require.False(t, rptFailCount.Probed)
		require.Zero(t, rptFailCount.Candidates)
		for _, sp := range recorder.Ended() {
			if sp.Name() != "failed-probe-count" {
				continue
			}
			for _, attr := range sp.Attributes() {
				require.NotEqual(t, "quarry.vector.scope_candidates", string(attr.Key))
			}
		}

		// A closed pool prevents the probe from being attempted.
		_, restoreErr := failSchemaClient.Pool.Exec(t.Context(), `ALTER TABLE hidden_documents RENAME TO documents`)
		require.NoError(t, restoreErr)
		require.NoError(t, closedCountClient.Close(t.Context()))
		ctxAcqFail, spanAcqFail := otel.Tracer("test").Start(metrics.WithService(t.Context(), metrics.NewService(nil)), "failed-acquire")
		_, rptAcqFail, acqErr := brokenVS.QueryFilteredDetailed(ctxAcqFail, []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "populated"})
		spanAcqFail.End()
		require.Error(t, acqErr)
		require.Equal(t, SearchStrategyProbe, rptAcqFail.Strategy)
		require.Equal(t, SearchStatusError, rptAcqFail.Status)
		require.False(t, rptAcqFail.ProbeAttempted)
		require.False(t, rptAcqFail.Probed)
		for _, sp := range recorder.Ended() {
			if sp.Name() != "failed-acquire" {
				continue
			}
			for _, attr := range sp.Attributes() {
				require.NotEqual(t, "quarry.vector.scope_candidates", string(attr.Key))
			}
		}
	})

	t.Run("PreservesPoolIsolationWhenSkippingProbe", func(t *testing.T) {
		tracer := &transactionSettingsTracer{}
		isoURL := withFixtureOptions(t, pgtest.Sibling(t, baseURL), "default_transaction_isolation=serializable")
		setupClient, scErr := NewClient(t.Context(), isoURL)
		require.NoError(t, scErr)
		t.Cleanup(func() { require.NoError(t, setupClient.Close(context.Background())) })
		require.NoError(t, setupClient.RunMigrations(t.Context()))
		initVS, ivErr := NewVectorStore(t.Context(), setupClient, 3, WithExactScopeMax(10))
		require.NoError(t, ivErr)
		docID := uuid.NewString()
		seedCompletedDocument(t, setupClient, docID, "report", "iso")
		require.NoError(t, initVS.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "iso-1", DocumentID: domain.DocumentID(docID), Index: 0, Text: "serializable", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "iso"}},
		}))

		c, cErr := NewClient(t.Context(), isoURL, WithQueryTracer(tracer))
		require.NoError(t, cErr)
		t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })

		disabledVS, dErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(0))
		require.NoError(t, dErr)
		enabledVS, eErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(10))
		require.NoError(t, eErr)

		checkRuntimeIsolation := func(wantIsolation string, filter domain.VectorFilter, vs *VectorStore, wantProbed bool) {
			t.Helper()
			tracer.settings = nil
			tracer.err = nil
			got, rpt, qErr := vs.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 5, filter)
			require.NoError(t, qErr)
			require.Len(t, got, 1)
			require.Equal(t, wantProbed, rpt.Probed)
			require.Equal(t, wantProbed, rpt.ProbeAttempted)
			require.NoError(t, tracer.err)
			require.Equal(t, []string{wantIsolation, "on"}, tracer.settings)

			conn, acqErr := c.Pool.Acquire(t.Context())
			require.NoError(t, acqErr)
			defer conn.Release()
			var iso string
			require.NoError(t, conn.QueryRow(t.Context(), `SHOW transaction_isolation`).Scan(&iso))
			require.Equal(t, "serializable", iso)
		}

		checkRuntimeIsolation("serializable", domain.VectorFilter{Collection: "iso"}, disabledVS, false)
		checkRuntimeIsolation("serializable", domain.VectorFilter{}, enabledVS, false)
		checkRuntimeIsolation("repeatable read", domain.VectorFilter{Collection: "iso"}, enabledVS, true)
	})

	t.Run("RejectsInvalidVectorsInBothStrategies", func(t *testing.T) {
		c, _ := isolatedVectorDatabase(t, baseURL)
		docID := uuid.NewString()
		seedCompletedDocument(t, c, docID, "report", "small")
		base, storeErr := NewVectorStore(t.Context(), c, 3)
		require.NoError(t, storeErr)
		require.NoError(t, base.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "c1", DocumentID: domain.DocumentID(docID), Index: 0, Text: "one", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "small"}},
		}))

		exactStore, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(10))
		require.NoError(t, storeErr)
		invalidEmbeddings := [][]float32{
			nil,
			{},
			{1, 0},
			{1, 0, 0, 0},
			{0, 0, 0},
			{1, float32(math.NaN()), 0},
			{1, float32(math.Inf(1)), 0},
			{1, float32(math.Inf(-1)), 0},
		}
		filters := []domain.VectorFilter{
			{},
			{Collection: "small"},
			{Collection: "missing"},
			{DocumentIDs: []string{docID}},
			{DocumentIDs: []string{"invalid"}},
		}
		for _, vs := range []*VectorStore{base, exactStore} {
			for _, filter := range filters {
				for _, emb := range invalidEmbeddings {
					_, rpt, queryErr := vs.QueryFilteredDetailed(t.Context(), emb, 5, filter)
					require.Error(t, queryErr)
					require.Equal(t, SearchStatusError, rpt.Status)
					require.Empty(t, rpt.Strategy)
					require.False(t, rpt.ProbeAttempted)
					require.False(t, rpt.Probed)
				}
			}
		}
	})

	t.Run("ConcurrentDeletionConsistentSnapshot", func(t *testing.T) {
		tracer := &probePauseTracer{}
		c, _ := isolatedVectorDatabase(t, baseURL, WithQueryTracer(tracer))

		for _, tc := range []struct {
			name      string
			threshold int
			docs      int
			wantStrat SearchStrategy
		}{
			{"exact branch snapshot", 5, 3, SearchStrategyExact},
			{"hnsw fallback snapshot", 2, 4, SearchStrategyHNSW},
		} {
			t.Run(tc.name, func(t *testing.T) {
				vs, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(tc.threshold))
				require.NoError(t, storeErr)
				collection := "snap-" + uuid.NewString()
				docIDs := make([]string, tc.docs)
				for i := range tc.docs {
					docIDs[i] = uuid.NewString()
					seedCompletedDocument(t, c, docIDs[i], "report", collection)
					require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
						{ID: domain.ChunkID(fmt.Sprintf("%s-%d", collection, i)), DocumentID: domain.DocumentID(docIDs[i]), Index: 0, Text: "keep", Embedding: []float32{1, float32(i) * 0.01, 0}, Metadata: map[string]any{"doc_type": "report", "collection": collection}},
					}))
				}

				tracer.mu.Lock()
				tracer.onProbe = func() {
					require.NoError(t, c.DeleteDocument(context.Background(), domain.DocumentID(docIDs[0])))
				}
				tracer.mu.Unlock()

				got, rpt, qErr := vs.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 10, domain.VectorFilter{Collection: collection})
				require.NoError(t, qErr)
				require.Equal(t, tc.wantStrat, rpt.Strategy)
				require.Len(t, got, tc.docs)

				after, _, qErr := vs.QueryFilteredDetailed(t.Context(), []float32{1, 0, 0}, 10, domain.VectorFilter{Collection: collection})
				require.NoError(t, qErr)
				require.Len(t, after, tc.docs-1)
			})
		}
	})

	t.Run("ExplainShapesAndIndexUsage", func(t *testing.T) {
		c, _ := isolatedVectorDatabase(t, baseURL)
		vs, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(20))
		require.NoError(t, storeErr)
		seedSearchVectors(t, c, 2000, 100)
		rareDoc := uuid.NewString()
		seedCompletedDocument(t, c, rareDoc, "brief", "rare")
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "rare-1", DocumentID: domain.DocumentID(rareDoc), Index: 0, Text: "rare", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "brief", "collection": "rare"}},
		}))
		_, execErr := c.Pool.Exec(t.Context(), `ANALYZE chunk_vectors; ANALYZE documents`)
		require.NoError(t, execErr)

		explain := func(settings, sql string, args ...any) string {
			t.Helper()
			plan, planErr := explainVectorPlan(t.Context(), c, settings, "EXPLAIN (ANALYZE, BUFFERS) "+sql, args...)
			require.NoError(t, planErr)
			require.Zero(t, c.Pool.Stat().AcquiredConns())
			return plan
		}

		_, planErr := explainVectorPlan(t.Context(), c, "", "EXPLAIN SELECT * FROM missing_vector_plan_fixture")
		var pgErr *pgconn.PgError
		require.ErrorAs(t, planErr, &pgErr)
		require.Equal(t, "42P01", pgErr.Code)
		require.Zero(t, c.Pool.Stat().AcquiredConns())

		var compositeCount int
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_indexes WHERE tablename='chunk_vectors' AND indexname IN ('idx_chunk_vectors_collection_doc_type','idx_chunk_vectors_doc_type_document_id')`).Scan(&compositeCount))
		require.Equal(t, 2, compositeCount)

		for _, filter := range []domain.VectorFilter{
			{Collection: "rare"},
			{DocType: "brief"},
			{Collection: "rare", DocType: "brief"},
			{DocumentIDs: []string{rareDoc}},
			{Collection: "rare", DocumentIDs: []string{rareDoc}},
			{DocType: "brief", DocumentIDs: []string{rareDoc}},
			{Collection: "rare", DocType: "brief", DocumentIDs: []string{rareDoc}},
		} {
			probeQ, probeArgs, ok := probeSQL(21, filter)
			require.True(t, ok)
			exactQ, exactArgs, ok := exactSQL("[1,0,0]", 5, 21, filter)
			require.True(t, ok)
			searchQ, searchArgs, ok := searchSQL("[1,0,0]", 5, filter, true)
			require.True(t, ok)

			// Default planner settings must avoid HNSW on probe/exact and keep Limit.
			defProbe := explain("", probeQ, probeArgs...)
			defExact := explain("", exactQ, exactArgs...)
			require.Contains(t, defProbe, "Limit")
			require.NotContains(t, defProbe, "idx_chunk_vectors_embedding")
			require.Contains(t, defExact, "CTE candidates")
			require.NotContains(t, defExact, "idx_chunk_vectors_embedding")

			// Even with seqscan/sort penalized, exactSQL must never touch HNSW, while searchSQL uses HNSW.
			const penalizedSetup = `SET LOCAL enable_seqscan = off; SET LOCAL enable_sort = off`
			probePlan := explain(penalizedSetup, probeQ, probeArgs...)
			t.Logf("PROBE EXPLAIN %+v:\n%s", filter, probePlan)
			require.Contains(t, probePlan, "Limit")
			require.NotContains(t, probePlan, "idx_chunk_vectors_embedding")
			require.Contains(t, probePlan, "idx_chunk_vectors_")

			exactPlan := explain(penalizedSetup, exactQ, exactArgs...)
			t.Logf("EXACT EXPLAIN %+v:\n%s", filter, exactPlan)
			require.NotContains(t, exactPlan, "idx_chunk_vectors_embedding")
			require.Contains(t, exactPlan, "CTE candidates")
			require.Contains(t, exactPlan, "Sort")

			searchPlan := explain(penalizedSetup, searchQ, searchArgs...)
			require.Contains(t, searchPlan, "idx_chunk_vectors_embedding")
		}
	})

	t.Run("CancellationTimeoutsAndPoolDefaults", func(t *testing.T) {
		setupClient, dbURL := isolatedVectorDatabase(t, baseURL)
		baseSetup, storeErr := NewVectorStore(t.Context(), setupClient, 3)
		require.NoError(t, storeErr)

		docID := uuid.NewString()
		seedCompletedDocument(t, setupClient, docID, "report", "timeout-scope")
		chunks := make([]domain.Chunk, 0, 10)
		for i := range 10 {
			chunks = append(chunks, domain.Chunk{
				ID:         domain.ChunkID(fmt.Sprintf("t-%d", i)),
				DocumentID: domain.DocumentID(docID),
				Index:      i,
				Text:       "timeout",
				TokenCount: 1,
				Embedding:  []float32{1, float32(i) * 0.01, 0},
				Metadata:   map[string]any{"doc_type": "report", "collection": "timeout-scope"},
			})
		}
		require.NoError(t, baseSetup.Upsert(t.Context(), "", chunks))

		checkPoolDefaults := func(t *testing.T, poolClient *Client, wantTimeout, wantScan, wantScanTuples, wantLocale string) {
			t.Helper()
			conn, acquireErr := poolClient.Pool.Acquire(t.Context())
			require.NoError(t, acquireErr)
			defer conn.Release()
			// An unused or replaced connection may not have loaded pgvector yet.
			_, loadErr := conn.Exec(t.Context(), `LOAD 'vector'`)
			require.NoError(t, loadErr)
			var st, hnswScan, hnswScanTuples, lc string
			require.NoError(t, conn.QueryRow(t.Context(), `SHOW statement_timeout`).Scan(&st))
			require.NoError(t, conn.QueryRow(t.Context(), `SHOW hnsw.iterative_scan`).Scan(&hnswScan))
			require.NoError(t, conn.QueryRow(t.Context(), `SHOW hnsw.max_scan_tuples`).Scan(&hnswScanTuples))
			require.NoError(t, conn.QueryRow(t.Context(), `SHOW lc_messages`).Scan(&lc))
			require.Equal(t, wantTimeout, st)
			require.Equal(t, wantScan, hnswScan)
			require.Equal(t, wantScanTuples, hnswScanTuples)
			if wantLocale != "" {
				require.Equal(t, wantLocale, lc)
			}
		}

		t.Run("canceled context", func(t *testing.T) {
			vs, vErr := NewVectorStore(t.Context(), setupClient, 3, WithExactScopeMax(20))
			require.NoError(t, vErr)
			m := metrics.NewService(nil)
			ctx, cancel := context.WithCancel(metrics.WithService(t.Context(), m))
			cancel()
			_, rpt, qErr := vs.QueryFilteredDetailed(ctx, []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "timeout-scope"})
			require.ErrorIs(t, qErr, context.Canceled)
			require.NotErrorIs(t, qErr, context.DeadlineExceeded)
			require.Equal(t, SearchStatusCanceled, rpt.Status)
			checkPoolDefaults(t, setupClient, "30s", "off", "20000", "")
		})

		t.Run("expired deadline", func(t *testing.T) {
			vs, vErr := NewVectorStore(t.Context(), setupClient, 3, WithExactScopeMax(20))
			require.NoError(t, vErr)
			m := metrics.NewService(nil)
			ctx, cancel := context.WithDeadline(metrics.WithService(t.Context(), m), time.Now().Add(-time.Millisecond))
			defer cancel()
			_, rpt, qErr := vs.QueryFilteredDetailed(ctx, []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "timeout-scope"})
			require.ErrorIs(t, qErr, context.DeadlineExceeded)
			require.Equal(t, SearchStatusTimeout, rpt.Status)
			checkPoolDefaults(t, setupClient, "30s", "off", "20000", "")
		})

		t.Run("in-flight caller cancel under conflicting lock", func(t *testing.T) {
			waitURL := withFixtureOptions(t, dbURL, "statement_timeout=5000&application_name=cancel-wait-worker")
			waitClient, wcErr := NewClient(t.Context(), waitURL)
			require.NoError(t, wcErr)
			t.Cleanup(func() { require.NoError(t, waitClient.Close(context.Background())) })
			vs, vErr := NewVectorStore(t.Context(), waitClient, 3, WithExactScopeMax(20))
			require.NoError(t, vErr)

			lockerTx, lockErr := setupClient.Pool.Begin(t.Context())
			require.NoError(t, lockErr)
			defer func() { _ = lockerTx.Rollback(context.Background()) }()
			_, execErr := lockerTx.Exec(t.Context(), `LOCK TABLE chunk_vectors IN ACCESS EXCLUSIVE MODE`)
			require.NoError(t, execErr)

			ctx, cancel := context.WithCancel(metrics.WithService(t.Context(), metrics.NewService(nil)))
			defer cancel()
			type res struct {
				rpt SearchReport
				err error
			}
			done := make(chan res, 1)
			go func() {
				_, rpt, qErr := vs.QueryFilteredDetailed(ctx, []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "timeout-scope"})
				done <- res{rpt: rpt, err: qErr}
			}()

			require.Eventually(t, func() bool {
				var waiting bool
				err := setupClient.Pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
					WHERE datname=current_database() AND application_name='cancel-wait-worker' AND wait_event_type='Lock')`).Scan(&waiting)
				return err == nil && waiting
			}, 5*time.Second, 10*time.Millisecond)
			cancel()

			out := <-done
			require.ErrorIs(t, out.err, context.Canceled)
			require.NotErrorIs(t, out.err, context.DeadlineExceeded)
			require.Equal(t, SearchStatusCanceled, out.rpt.Status)
			require.True(t, out.rpt.ProbeAttempted)
			require.False(t, out.rpt.Probed)
			require.NoError(t, lockerTx.Rollback(t.Context()))
			checkPoolDefaults(t, waitClient, "5s", "off", "20000", "")
		})

		t.Run("deterministic lock timeout across locales and modes", func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			prevTP := otel.GetTracerProvider()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			otel.SetTracerProvider(tp)
			t.Cleanup(func() {
				otel.SetTracerProvider(prevTP)
				require.NoError(t, tp.Shutdown(context.Background()))
			})

			for _, tc := range []struct {
				name     string
				locale   string
				scopeMax int
				wantStat SearchStrategy
			}{
				{"C disabled", "C", 0, SearchStrategyHNSW},
				{"C exact-probed", "C", 20, SearchStrategyProbe},
				{"es_ES disabled", "es_ES.utf8", 0, SearchStrategyHNSW},
				{"es_ES exact-probed", "es_ES.utf8", 20, SearchStrategyProbe},
			} {
				t.Run(tc.name, func(t *testing.T) {
					options := "statement_timeout=250&hnsw.iterative_scan=relaxed_order&hnsw.max_scan_tuples=7777&lc_messages=" + tc.locale
					timeoutClient, cErr := NewClient(t.Context(), withFixtureOptions(t, dbURL, options))
					require.NoError(t, cErr)
					t.Cleanup(func() { require.NoError(t, timeoutClient.Close(context.Background())) })
					vs, vErr := NewVectorStore(t.Context(), timeoutClient, 3, WithExactScopeMax(tc.scopeMax))
					require.NoError(t, vErr)

					// Verify server actually honors the requested locale on this PostgreSQL image.
					var activeLocale string
					require.NoError(t, timeoutClient.Pool.QueryRow(t.Context(), `SHOW lc_messages`).Scan(&activeLocale))
					require.Equal(t, tc.locale, activeLocale)

					lockerTx, lockErr := setupClient.Pool.Begin(t.Context())
					require.NoError(t, lockErr)
					defer func() { _ = lockerTx.Rollback(context.Background()) }()
					_, execErr := lockerTx.Exec(t.Context(), `LOCK TABLE chunk_vectors IN ACCESS EXCLUSIVE MODE`)
					require.NoError(t, execErr)

					type queryOutcome struct {
						rpt SearchReport
						err error
					}
					const workers = 8
					outcomes := make([]queryOutcome, workers)
					m := metrics.NewService(nil)
					var wg sync.WaitGroup
					for i := range workers {
						wg.Go(func() {
							qCtx, span := otel.Tracer("test").Start(metrics.WithService(t.Context(), m), "locked-timeout")
							_, rpt, qErr := vs.QueryFilteredDetailed(qCtx, []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "timeout-scope"})
							span.End()
							outcomes[i] = queryOutcome{rpt: rpt, err: qErr}
						})
					}
					wg.Wait()

					for i, outcome := range outcomes {
						require.Error(t, outcome.err, "worker %d", i)
						require.ErrorIs(t, outcome.err, context.DeadlineExceeded, "worker %d", i)
						var pgErr *pgconn.PgError
						require.ErrorAs(t, outcome.err, &pgErr, "worker %d", i)
						require.Equal(t, "57014", pgErr.Code, "worker %d", i)
						if tc.locale == "C" {
							require.Contains(t, pgErr.Message, "statement timeout")
						} else {
							require.NotContains(t, pgErr.Message, "statement timeout")
						}
						require.Equal(t, SearchStatusTimeout, outcome.rpt.Status, "worker %d", i)
						require.Equal(t, tc.wantStat, outcome.rpt.Strategy, "worker %d", i)
						require.Equal(t, tc.scopeMax > 0, outcome.rpt.ProbeAttempted, "worker %d", i)
						require.False(t, outcome.rpt.Probed, "worker %d", i)
					}

					var out strings.Builder
					require.NoError(t, m.Registry.WriteText(&out))
					require.Contains(t, out.String(), fmt.Sprintf(`quarry_pgvector_search_total{strategy=%q,status="timeout"} %d`, tc.wantStat, workers))

					for _, sp := range recorder.Ended() {
						if sp.Name() != "locked-timeout" {
							continue
						}
						for _, attr := range sp.Attributes() {
							require.NotEqual(t, "quarry.vector.scope_candidates", string(attr.Key))
						}
					}

					// Release conflicting lock and verify pooled connection reuse succeeds and settings stay intact.
					require.NoError(t, lockerTx.Rollback(t.Context()))
					reuseGot, reuseRpt, reuseErr := vs.QueryFilteredDetailed(metrics.WithService(t.Context(), m), []float32{1, 0, 0}, 5, domain.VectorFilter{Collection: "timeout-scope"})
					require.NoError(t, reuseErr)
					require.Equal(t, SearchStatusOK, reuseRpt.Status)
					require.Len(t, reuseGot, 5)
					checkPoolDefaults(t, timeoutClient, "250ms", "relaxed_order", "7777", tc.locale)
				})
			}
		})
	})

	t.Run("IntegrationWithRetrieverAndTelemetry", func(t *testing.T) {
		c, _ := isolatedVectorDatabase(t, baseURL)
		vs, storeErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(20))
		require.NoError(t, storeErr)

		distractor := uuid.NewString()
		seedCompletedDocument(t, c, distractor, "report", "distractor")
		_, execErr := c.Pool.Exec(t.Context(), `INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,collection,text,token_count,embedding)
			SELECT 'd-'||i,$1::uuid,i,'report','distractor','distractor',1,'[1,0.01,0]'::vector FROM generate_series(1,3000) i`, distractor)
		require.NoError(t, execErr)

		rareID := uuid.NewString()
		seedCompletedDocument(t, c, rareID, "report", "rare")
		rareChunk := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(rareID), Index: 0, Text: "Rare archive record.", TokenCount: 3, Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "rare"}}
		require.NoError(t, c.InsertChunks(t.Context(), []domain.Chunk{rareChunk}))
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{rareChunk}))

		recorder := tracetest.NewSpanRecorder()
		prev := otel.GetTracerProvider()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		otel.SetTracerProvider(tp)
		t.Cleanup(func() {
			otel.SetTracerProvider(prev)
			require.NoError(t, tp.Shutdown(context.Background()))
		})

		tracedStore := tracing.WrapVectorStore(vs, "postgresql")
		ret := retriever.NewRetriever(tracedStore, staticEmbedder{vec: []float32{1, 0, 0}}, 0.7, 5)
		m := metrics.NewService(nil)
		ctx := metrics.WithService(t.Context(), m)

		qc := &pipeline.QueryContext{
			OriginalQuery: "What is in the rare archive?",
			MaxChunks:     5,
			FilterDocType: "report",
			Collection:    "rare",
		}
		vis := Visibility{Pool: c.Pool, Collection: "rare", DocType: "report"}
		prevs := Previews{Pool: c.Pool, Collection: "rare", DocType: "report"}
		_, pipeErr := pipeline.RunRetrievalPipeline(ctx, qc, []pipeline.QueryStage{
			pipeline.NewDecomposeStage(),
			pipeline.NewRoutingStageWithScope(ret, vis, prevs),
			pipeline.NewRetrievalStageWithVisibility(ret, vis),
		})
		require.NoError(t, pipeErr)
		require.Len(t, qc.RetrievedChunks, 1)
		require.Equal(t, rareChunk.ID, qc.RetrievedChunks[0].Chunk.ID)

		var b strings.Builder
		require.NoError(t, m.Registry.WriteText(&b))
		require.Contains(t, b.String(), `quarry_pgvector_search_total{strategy="exact",status="ok"}`)

		var vectorSpans int
		for _, sp := range recorder.Ended() {
			if sp.Name() != "vector.query" {
				continue
			}
			vectorSpans++
			attrs := map[string]string{}
			for _, a := range sp.Attributes() {
				attrs[string(a.Key)] = a.Value.AsString()
			}
			require.Equal(t, "exact", attrs["quarry.vector.strategy"])
			require.Equal(t, "ok", attrs["quarry.vector.status"])
		}
		require.GreaterOrEqual(t, vectorSpans, 2)

		t.Run("LocalTopUpTieBreakParity", func(t *testing.T) {
			tieDocOther := uuid.NewString()
			tieDocLocal := uuid.NewString()
			_, insErr := c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,content_preview,status,doc_type,collection) VALUES
				($1,'Overview text','Overview text','completed','report','ties'),
				($2,'Cedar Facility operations report','Cedar Facility operations report','completed','report','ties')`, tieDocOther, tieDocLocal)
			require.NoError(t, insErr)

			chunk001 := domain.Chunk{ID: domain.ChunkID("00000000-0000-4000-8000-000000000001"), DocumentID: domain.DocumentID(tieDocOther), Index: 0, Text: "First tied record.", TokenCount: 3, Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "ties"}}
			chunk002 := domain.Chunk{ID: domain.ChunkID("00000000-0000-4000-8000-000000000002"), DocumentID: domain.DocumentID(tieDocOther), Index: 1, Text: "Second tied record.", TokenCount: 3, Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "ties"}}
			chunk003 := domain.Chunk{ID: domain.ChunkID("00000000-0000-4000-8000-000000000003"), DocumentID: domain.DocumentID(tieDocLocal), Index: 0, Text: "Cedar Facility tied record.", TokenCount: 4, Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"doc_type": "report", "collection": "ties"}}

			require.NoError(t, c.InsertChunks(t.Context(), []domain.Chunk{chunk001, chunk002, chunk003}))
			require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{chunk001, chunk002, chunk003}))

			disabled, disabledErr := NewVectorStore(t.Context(), c, 3)
			require.NoError(t, disabledErr)
			fallback, fallbackErr := NewVectorStore(t.Context(), c, 3, WithExactScopeMax(1))
			require.NoError(t, fallbackErr)
			mem := memory.NewFakeVectorStore()
			require.NoError(t, mem.Upsert(t.Context(), "", []domain.Chunk{chunk001, chunk002, chunk003}))

			for _, tc := range []struct {
				name, system string
				store        domain.VectorStore
				want         []domain.ChunkID
			}{
				{"exact", "postgresql", vs, []domain.ChunkID{chunk001.ID, chunk002.ID}},
				{"approximate_top_up", "postgresql", fallback, []domain.ChunkID{chunk003.ID, chunk001.ID}},
				{"postgres_disabled", "postgresql", disabled, []domain.ChunkID{chunk003.ID, chunk001.ID}},
				{"memory", "memory", mem, []domain.ChunkID{chunk003.ID, chunk001.ID}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tieMetrics := metrics.NewService(nil)
					tieCtx := metrics.WithService(t.Context(), tieMetrics)
					tieRet := retriever.NewRetriever(tracing.WrapVectorStore(tc.store, tc.system), staticEmbedder{vec: []float32{1, 0, 0}}, 0.7, 2)
					tieQC := &pipeline.QueryContext{
						OriginalQuery: "What does Cedar Facility require?",
						AnswerMode:    domain.AnswerModeEvidence,
						MaxChunks:     2,
						FilterDocType: "report",
						Collection:    "ties",
					}
					tieVis := Visibility{Pool: c.Pool, Collection: "ties", DocType: "report"}
					tiePrevs := Previews{Pool: c.Pool, Collection: "ties", DocType: "report"}
					_, tiePipeErr := pipeline.RunRetrievalPipeline(tieCtx, tieQC, []pipeline.QueryStage{
						pipeline.NewDecomposeStage(),
						pipeline.NewRoutingStageWithScope(tieRet, tieVis, tiePrevs),
						pipeline.NewRetrievalStageWithVisibility(tieRet, tieVis),
					})
					require.NoError(t, tiePipeErr)
					require.Equal(t, "LOCAL", tieQC.RetrievalMode)
					require.Equal(t, tieDocLocal, tieQC.LocalDocumentID)
					require.Len(t, tieQC.RetrievedChunks, 2)
					require.Equal(t, tc.want, []domain.ChunkID{tieQC.RetrievedChunks[0].Chunk.ID, tieQC.RetrievedChunks[1].Chunk.ID})

					if tc.name == "exact" {
						var mb strings.Builder
						require.NoError(t, tieMetrics.Registry.WriteText(&mb))
						require.Contains(t, mb.String(), `quarry_pgvector_search_total{strategy="exact",status="ok"} 3`)

						txSnap, snapErr := c.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
						require.NoError(t, snapErr)
						defer func() { _ = txSnap.Rollback(context.Background()) }()
						wantTie := exhaustiveQuerySnapshot(t, txSnap, []float32{1, 0, 0}, 3, 2, domain.VectorFilter{Collection: "ties", DocType: "report"})
						requireExactScoredParity(t, wantTie, tieQC.RetrievedChunks)
					}
				})
			}
		})
	})
}

type transactionSettingsTracer struct {
	settings []string
	err      error
}

type traceTransactionSettings struct{}

func (*transactionSettingsTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, traceTransactionSettings{}, strings.HasPrefix(data.SQL, "begin"))
}

func (b *transactionSettingsTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(traceTransactionSettings{}) != true || data.Err != nil {
		return
	}
	// Use the physical connection to inspect the open transaction without
	// recursively invoking the tracer.
	rows, err := conn.PgConn().Exec(ctx, `SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')`).ReadAll()
	b.err = err
	if err == nil {
		b.settings = []string{string(rows[0].Rows[0][0]), string(rows[0].Rows[0][1])}
	}
}

type probePauseTracer struct {
	mu      sync.Mutex
	onProbe func()
}

type probeSpanKey struct{}

func (p *probePauseTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "SELECT count(*) FROM") {
		return context.WithValue(ctx, probeSpanKey{}, true)
	}
	return ctx
}

func (p *probePauseTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if active, ok := ctx.Value(probeSpanKey{}).(bool); ok && active {
		p.mu.Lock()
		fn := p.onProbe
		p.onProbe = nil
		p.mu.Unlock()
		if fn != nil {
			fn()
		}
	}
}

type staticEmbedder struct{ vec []float32 }

func (s staticEmbedder) Embed(context.Context, string) ([]float32, error) { return s.vec, nil }
