package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pg "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStandaloneCleanupUpgradesPreviousSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("requires pgvector")
	}
	ctx := t.Context()
	url := pgtest.StartVector(t)
	c, err := pg.NewClient(ctx, url)
	require.NoError(t, err)
	t.Cleanup(c.Pool.Close)
	_, err = c.Pool.Exec(ctx, "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
	require.NoError(t, err)
	migrations := "../../internal/store/postgres/migrations"
	entries, err := os.ReadDir(migrations)
	require.NoError(t, err)
	const vectorMutationVersion = 12
	for _, entry := range entries {
		version, versionErr := strconv.Atoi(entry.Name()[:3])
		require.NoError(t, versionErr)
		if version >= vectorMutationVersion {
			break
		}
		sql, readErr := os.ReadFile(filepath.Join(migrations, entry.Name()))
		require.NoError(t, readErr)
		_, err = c.Pool.Exec(ctx, string(sql))
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, "INSERT INTO schema_migrations(version,name,checksum) VALUES($1,$2,$3)", version, entry.Name(), fmt.Sprintf("%x", sha256.Sum256(sql)))
		require.NoError(t, err)
	}
	index, err := pg.NewVectorStore(ctx, c, 2)
	require.NoError(t, err)
	id := domain.DocumentID(uuid.NewString())
	require.NoError(t, c.InsertDocument(ctx, domain.Document{ID: id, Content: "offline source"}))
	require.NoError(t, index.Upsert(ctx, "", []domain.Chunk{{ID: "stored", DocumentID: id, Embedding: []float32{1, 0}}}))
	tool, err := cleanup.New(ctx, &config.Config{VectorStore: "postgres", DatabaseURL: url})
	require.NoError(t, err)
	t.Cleanup(func() { _ = tool.Close(context.Background()) })
	require.NoError(t, tool.Delete(ctx, string(id)))
	exists, err := c.DocumentExists(ctx, id)
	require.NoError(t, err)
	require.False(t, exists)
	var installed, inactive bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1),
		activated_at IS NULL FROM document_deletion_activation`, vectorMutationVersion).Scan(&installed, &inactive))
	require.True(t, installed)
	require.True(t, inactive)
	hits, err := index.Query(ctx, "", []float32{1, 0}, 1, "")
	require.NoError(t, err)
	require.Empty(t, hits)
}

func TestUntaggedGeneratedReleaseRejectsSameScopeReplacement(t *testing.T) {
	c := durableDatabase(t)
	for _, mode := range []string{"json", "sse"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			id := domain.DocumentID(uuid.NewString())
			chunkID := domain.ChunkID(uuid.NewString())
			index := memory.NewFakeVectorStore()
			seed := func(embedding []float32) string {
				body := "The archive opens on Monday. " + mode
				res, err := c.AcceptDocument(ctx, domain.AcceptRequest{
					Document: domain.Document{ID: id, Content: body}, CallerSupplied: true, Collection: "public",
					Fingerprint: chunker.Fingerprint(body), MaxPending: 2,
				})
				require.NoError(t, err)
				require.Equal(t, domain.AcceptAccepted, res.Outcome)
				job, err := c.ClaimJob(ctx, "same", time.Minute, 3)
				require.NoError(t, err)
				require.False(t, job.ScopedVectors)
				chunk := domain.Chunk{ID: chunkID, DocumentID: id, Text: body, Embedding: embedding, Metadata: map[string]any{"collection": "public"}}
				require.NoError(t, index.Upsert(ctx, "", []domain.Chunk{chunk}))
				ok, err := c.CompleteJob(ctx, *job, []domain.Chunk{chunk}, domain.IndexObservation{})
				require.NoError(t, err)
				require.True(t, ok)
				return job.Incarnation
			}
			old := seed([]float32{1, 0})
			changed := false
			cfg := &config.Config{BedrockFallbackModel: "offline", MaxChunksPerQuery: 10}
			amb := llm.NewAmbassador(cfg)
			amb.SetGenerator(retiringGenerator{func() {
				require.NoError(t, c.DeleteDocument(ctx, id))
				require.NotEqual(t, old, seed([]float32{0, 1}))
				changed = true
			}})
			query := handler.QueryHandler(retriever.NewRetriever(index, revisionEmbedder{1, 0}, .7, 10), amb, cfg, c.Pool)
			req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/query", strings.NewReader("{\"query\":\"What day?\",\"collection\":\"public\",\"mode\":\"global\",\"include_citations\":true}"))
			req.Header.Set("Content-Type", "application/json")
			if mode == "sse" {
				req.Header.Set("Accept", "text/event-stream")
			}
			output := httptest.NewRecorder()
			query(output, req)
			require.True(t, changed)
			fresh, err := index.Query(ctx, "", []float32{1, 0}, 1, "")
			require.NoError(t, err)
			require.EqualValues(t, 0, fresh[0].Score)
			if mode == "json" {
				require.Equal(t, http.StatusInternalServerError, output.Code, output.Body.String())
				var response handler.QueryResponse
				require.NoError(t, json.Unmarshal(output.Body.Bytes(), &response))
				require.Empty(t, response.Citations)
				require.Empty(t, response.ScoredChunks)
			} else {
				body := output.Body.String()
				require.Contains(t, body, "event: context")
				require.Contains(t, body, "event: error")
				require.NotContains(t, body, "event: delta")
				require.NotContains(t, body, "event: done")
				require.NotContains(t, body, "citations")
			}
		})
	}
}

type retryEmbedding struct{ calls atomic.Int32 }

func (e *retryEmbedding) Embed(context.Context, string) ([]float32, error) {
	if e.calls.Add(1) == 1 {
		return []float32{0, 1}, nil
	}
	return []float32{1, 0}, nil
}

type unacknowledgedUpsert struct {
	*memory.FakeVectorStore
	late func(context.Context) error
}

func (v *unacknowledgedUpsert) Upsert(ctx context.Context, ns string, chunks []domain.Chunk) error {
	if v.late == nil {
		v.late = func(ctx context.Context) error { return v.FakeVectorStore.Upsert(ctx, ns, chunks) }
		return context.DeadlineExceeded
	}
	return v.FakeVectorStore.Upsert(ctx, ns, chunks)
}

type completionOutcome struct {
	ok  bool
	err error
}

type observedCompletionStore struct {
	observedRevisionJobs
	completed chan completionOutcome
}

func (s observedCompletionStore) CompleteJob(ctx context.Context, job domain.Job, chunks []domain.Chunk, observation domain.IndexObservation) (bool, error) {
	ok, err := s.Client.CompleteJob(ctx, job, chunks, observation)
	select {
	case s.completed <- completionOutcome{ok, err}:
	case <-ctx.Done():
	}
	return ok, err
}

func TestUnsettledSharedUpsertPreventsUntaggedPublication(t *testing.T) {
	c := durableDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	v := &unacknowledgedUpsert{FakeVectorStore: memory.NewFakeVectorStore()}
	claims := make(chan domain.Job, 4)
	emb := new(retryEmbedding)
	stages := []pipeline.Stage{pipeline.NewChunkStage(chunker.NewChunker(128, 16)), pipeline.NewEmbedStage(emb, 1), pipeline.NewPersistStage(v)}
	completed := make(chan completionOutcome, 1)
	w := worker.New(observedCompletionStore{observeClaims(c, claims), completed}, stages, v, worker.Options{Workers: 1, Owner: "same-worker", Lease: 5 * time.Second, JobTimeout: 20 * time.Second, PollInterval: 10 * time.Millisecond})
	rec := durableSubmit(t, w, c, "{\"content\":\"The archive opens on Monday.\"}")
	require.Equal(t, 202, rec.Code)
	w.Start(ctx)
	defer w.Shutdown()
	first := awaitRevisionJob(ctx, t, claims)
	next := awaitRevisionJob(ctx, t, claims)
	require.Equal(t, first.Incarnation, next.Incarnation)
	require.NotEqual(t, first.LeaseToken, next.LeaseToken)
	require.False(t, next.ScopedVectors)
	var outcome completionOutcome
	select {
	case outcome = <-completed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	w.Shutdown()
	var state string
	var uncertain, deletes bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT status,vector_mutation_uncertain,vector_delete_uncertain
		FROM documents WHERE id=$1`, next.Document.ID).Scan(&state, &uncertain, &deletes))
	require.True(t, uncertain)
	require.False(t, deletes)
	before, err := v.Query(ctx, "", []float32{1, 0}, 1, "")
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.EqualValues(t, 1, before[0].Score)
	visibility := pg.Visibility{Pool: c.Pool}
	visibleBefore, err := visibility.VisibleEvidence(ctx, before)
	require.NoError(t, err)
	require.NoError(t, v.late(ctx))
	after, err := v.Query(ctx, "", []float32{1, 0}, 1, "")
	require.NoError(t, err)
	require.Len(t, after, 1)
	require.Equal(t, before[0].Chunk.ID, after[0].Chunk.ID)
	require.Zero(t, after[0].Score)
	visibleAfter, err := visibility.VisibleEvidence(ctx, after)
	require.NoError(t, err)
	t.Logf("completion=%v err=%v state=%s SQL-visible hits before=%+v after=%+v", outcome.ok, outcome.err, state, visibleBefore, visibleAfter)
	require.False(t, outcome.ok)
	require.ErrorContains(t, outcome.err, "unresolved")
	require.Equal(t, "processing", state)
	require.Empty(t, visibleBefore)
	require.Empty(t, visibleAfter)
}

func TestAtomicMemoryRejectionsAddNoUncertainty(t *testing.T) {
	c := durableDatabase(t)
	ctx := t.Context()
	id := domain.DocumentID(uuid.NewString())
	_, err := c.AcceptDocument(ctx, domain.AcceptRequest{Document: domain.Document{ID: id, Content: "offline"}, Fingerprint: chunker.Fingerprint("offline"), MaxPending: 2})
	require.NoError(t, err)
	job, err := c.ClaimJob(ctx, "w", time.Minute, 3)
	require.NoError(t, err)
	index := memory.NewFakeVectorStore()
	ic := pipeline.NewIngestContext(id, "offline", nil)
	valid := domain.Chunk{ID: "valid", DocumentID: id, Text: "valid", Embedding: []float32{1}}
	for _, tc := range []struct {
		name string
		bad  domain.Chunk
	}{
		{"metadata size", domain.Chunk{ID: "chunk", DocumentID: id, Text: strings.Repeat("x", 42000), Embedding: []float32{1}}},
		{"metadata type", domain.Chunk{ID: "chunk", DocumentID: id, Metadata: map[string]any{"bad": map[string]any{}}, Embedding: []float32{1}}},
		{"attempt without revision", domain.Chunk{ID: "chunk", DocumentID: id, Metadata: map[string]any{domain.VectorAttemptKey: "owner"}, Embedding: []float32{1}}},
		{"id size", domain.Chunk{ID: domain.ChunkID(strings.Repeat("x", 513)), DocumentID: id, Embedding: []float32{1}}},
		{"dimension", domain.Chunk{ID: "chunk", DocumentID: id, Embedding: []float32{1, 0}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			ic.Chunks = []domain.Chunk{valid, tc.bad}
			ic.MutateVectors = func(ctx context.Context, call func(context.Context) error) error {
				return c.MutateJobVectors(ctx, *job, call)
			}
			require.Error(t, pipeline.NewPersistStage(index).Run(ctx, ic))
			hits, queryErr := index.Query(ctx, "", []float32{1}, 2, "")
			require.NoError(t, queryErr)
			require.Empty(t, hits)
			var unknown bool
			require.NoError(t, c.Pool.QueryRow(ctx, "SELECT vector_mutation_uncertain FROM documents WHERE id=$1", id).Scan(&unknown))
			require.False(t, unknown)
		})
	}
}
