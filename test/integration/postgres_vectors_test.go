package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/stretchr/testify/require"
)

type countedPostgresVectors struct {
	*pgstore.VectorStore
	searches int
}

func (s *countedPostgresVectors) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.searches++
	return s.VectorStore.QueryFiltered(ctx, embedding, topK, filter)
}

func (s *countedPostgresVectors) QueryFilteredWithExactness(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	s.searches++
	return s.VectorStore.QueryFilteredWithExactness(ctx, embedding, topK, filter)
}

func TestPostgresVectorsOfflineParity(t *testing.T) {
	c, err := pgstore.NewClient(t.Context(), pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(t.Context()))
	vectors, err := pgstore.NewVectorStore(t.Context(), c, 256)
	require.NoError(t, err)
	traced := tracing.WrapVectorStore(vectors, "postgresql")
	e := embedder.NewFakeEmbedder(256)
	chk := chunker.NewChunker(32, 4)
	w := worker.New(c, []pipeline.Stage{pipeline.NewChunkStage(chk), pipeline.NewEmbedStage(e, 2), pipeline.NewPersistStage(traced, pipeline.WithVisibilityWait(time.Second))}, traced.(domain.VectorDeleter), worker.Options{Workers: 1, MaxAttempts: 3, Lease: time.Second, JobTimeout: time.Minute, PollInterval: 10 * time.Millisecond, Owner: "postgres-parity"})
	w.Start(t.Context())
	defer w.Shutdown()
	mem := memory.NewFakeVectorStore()
	var ids []string
	for _, fixture := range []struct{ text, collection, docType string }{
		{"The archive retains reports for seven years.", "archive", "report"},
		{"The schedule retains notes for two years.", "archive", "note"},
		{"The private archive retains reports for one year.", "private", "report"},
		{"The default archive retains references.", "", "report"},
	} {
		body, marshalErr := json.Marshal(map[string]string{"content": fixture.text, "collection": fixture.collection, "doc_type": fixture.docType})
		require.NoError(t, marshalErr)
		response := durableSubmit(t, w, c, string(body))
		require.Equal(t, 202, response.Code, response.Body.String())
		var accepted handler.IngestResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &accepted))
		durableStatus(t, c, accepted.DocumentID, "completed")
		ids = append(ids, accepted.DocumentID)
		chunks := chk.Chunk(domain.DocumentID(accepted.DocumentID), fixture.text)
		for i := range chunks {
			chunks[i].Embedding, err = e.Embed(t.Context(), chunks[i].Text)
			require.NoError(t, err)
			chunks[i].Metadata["doc_type"] = fixture.docType
			if fixture.collection != "" {
				chunks[i].Metadata["collection"] = fixture.collection
			}
		}
		require.NoError(t, mem.Upsert(t.Context(), "", chunks))
	}
	query, err := e.Embed(t.Context(), "The archive retains reports for seven years.")
	require.NoError(t, err)
	for _, filter := range []domain.VectorFilter{
		{}, {Collection: "archive"}, {Collection: "archive", DocType: "report"},
		{Collection: "archive", DocType: "report", DocumentIDs: []string{ids[0], ids[2]}},
		{Collection: "archive", DocumentIDs: []string{ids[2]}},
		{DocumentIDs: []string{}}, {DocumentIDs: []string{ids[3]}},
	} {
		want, wantErr := mem.QueryFiltered(t.Context(), query, 10, filter)
		require.NoError(t, wantErr)
		got, gotErr := traced.(domain.FilteredVectorStore).QueryFiltered(t.Context(), query, 10, filter)
		require.NoError(t, gotErr)
		require.Len(t, got, len(want))
		for i := range want {
			require.Equal(t, want[i].Chunk, got[i].Chunk)
			require.InDelta(t, want[i].Score, got[i].Score, 1e-5)
		}
	}
	require.NoError(t, c.DeleteDocument(t.Context(), domain.DocumentID(ids[0])))
	got, err := vectors.QueryFiltered(t.Context(), query, 10, domain.VectorFilter{DocumentIDs: []string{ids[0]}})
	require.NoError(t, err)
	require.Empty(t, got)

	t.Run("hosted refill is inactive for pgvector", func(t *testing.T) {
		store := &countedPostgresVectors{VectorStore: vectors}
		ret := retriever.NewRetriever(tracing.WrapVectorStore(store, "postgresql"), e, .7, 10)
		require.False(t, ret.SupportsVisibilityRefill())
		cfg := &config.Config{MaxChunksPerQuery: 10, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1}
		metricsService := metrics.NewService(nil)
		queryHandler := handler.QueryHandler(ret, nil, cfg, c.Pool)
		body := `{"query":"archive","mode":"global","max_chunks":2,"allowed_document_ids":["` + ids[0] + `"]}`
		response := scopeQuery(t, func(w http.ResponseWriter, r *http.Request) {
			queryHandler(w, r.WithContext(metrics.WithService(r.Context(), metricsService)))
		}, body)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		require.Equal(t, 1, store.searches)
		var result handler.QueryResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		require.Equal(t, "no_evidence", result.FinishReason)
		require.Empty(t, result.ScoredChunks)
		var exposition strings.Builder
		require.NoError(t, metricsService.Registry.WriteText(&exposition))
		require.NotContains(t, exposition.String(), "quarry_hosted_visibility_refill_total{")
	})
}
