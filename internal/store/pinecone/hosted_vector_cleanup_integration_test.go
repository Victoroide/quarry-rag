package pinecone_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type delayedHTTPVectorRecord struct {
	ID        string
	Namespace string
	Values    []float32
	Metadata  map[string]any
}

type delayedHTTPIndex struct {
	mu                 sync.Mutex
	vectors            map[string]delayedHTTPVectorRecord
	upsertFailOnBatch  int
	upsertCalls        int
	deleteOutage       bool
	disableList        bool
	ignoreNextDeleteID map[string]int
}

func newDelayedHTTPIndex() *delayedHTTPIndex {
	return &delayedHTTPIndex{
		vectors:            make(map[string]delayedHTTPVectorRecord),
		ignoreNextDeleteID: make(map[string]int),
	}
}

func (idx *delayedHTTPIndex) key(ns, id string) string {
	return ns + "\x00" + id
}

func (idx *delayedHTTPIndex) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/vectors/upsert":
			body, _ := io.ReadAll(r.Body)
			var req struct {
				Namespace string `json:"namespace"`
				Vectors   []struct {
					ID       string         `json:"id"`
					Values   []float32      `json:"values"`
					Metadata map[string]any `json:"metadata"`
				} `json:"vectors"`
			}
			if err := json.Unmarshal(body, &req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			idx.mu.Lock()
			idx.upsertCalls++
			if idx.upsertFailOnBatch > 0 && idx.upsertCalls == idx.upsertFailOnBatch {
				idx.mu.Unlock()
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, "injected batch failure")
				return
			}
			for _, v := range req.Vectors {
				idx.vectors[idx.key(req.Namespace, v.ID)] = delayedHTTPVectorRecord{
					ID: v.ID, Namespace: req.Namespace, Values: append([]float32(nil), v.Values...), Metadata: v.Metadata,
				}
			}
			idx.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"upsertedCount":%d}`, len(req.Vectors))

		case "/vectors/delete":
			body, _ := io.ReadAll(r.Body)
			var req struct {
				Namespace string         `json:"namespace"`
				IDs       []string       `json:"ids"`
				Filter    map[string]any `json:"filter"`
				DeleteAll bool           `json:"deleteAll"`
			}
			if err := json.Unmarshal(body, &req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			idx.mu.Lock()
			if idx.deleteOutage {
				idx.mu.Unlock()
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "injected transient delete outage")
				return
			}
			switch {
			case req.DeleteAll:
				for k, v := range idx.vectors {
					if v.Namespace == req.Namespace {
						delete(idx.vectors, k)
					}
				}
			case len(req.IDs) > 0:
				for _, id := range req.IDs {
					if rem := idx.ignoreNextDeleteID[id]; rem > 0 {
						idx.ignoreNextDeleteID[id] = rem - 1
						continue
					}
					delete(idx.vectors, idx.key(req.Namespace, id))
				}
			case len(req.Filter) > 0:
				docFilter, _ := req.Filter["doc_id"].(string)
				revFilter, _ := req.Filter["revision_id"].(string)
				for k, v := range idx.vectors {
					if v.Namespace != req.Namespace {
						continue
					}
					docID, _ := v.Metadata["doc_id"].(string)
					revID, _ := v.Metadata["revision_id"].(string)
					if (docFilter == "" || docID == docFilter) && (revFilter == "" || revID == revFilter) {
						delete(idx.vectors, k)
					}
				}
			}
			idx.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{}`)

		case "/vectors/fetch":
			ns := r.URL.Query().Get("namespace")
			ids := r.URL.Query()["ids"]
			idx.mu.Lock()
			found := make(map[string]map[string]any)
			for _, id := range ids {
				if rec, ok := idx.vectors[idx.key(ns, id)]; ok {
					found[id] = map[string]any{"id": id, "metadata": rec.Metadata}
				}
			}
			idx.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"vectors": found})

		case "/vectors/list":
			idx.mu.Lock()
			disable := idx.disableList
			idx.mu.Unlock()
			if disable {
				w.WriteHeader(http.StatusNotImplemented)
				_, _ = io.WriteString(w, "list unsupported on pod index")
				return
			}
			ns := r.URL.Query().Get("namespace")
			prefix := r.URL.Query().Get("prefix")
			idx.mu.Lock()
			var matched []map[string]string
			for _, rec := range idx.vectors {
				if rec.Namespace == ns && strings.HasPrefix(rec.ID, prefix) {
					matched = append(matched, map[string]string{"id": rec.ID})
				}
			}
			idx.mu.Unlock()
			sort.Slice(matched, func(i, j int) bool { return matched[i]["id"] < matched[j]["id"] })
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"vectors": matched})

		case "/query":
			body, _ := io.ReadAll(r.Body)
			var req struct {
				Namespace       string         `json:"namespace"`
				TopK            int            `json:"topK"`
				IncludeValues   bool           `json:"includeValues"`
				IncludeMetadata bool           `json:"includeMetadata"`
				Filter          map[string]any `json:"filter"`
			}
			_ = json.Unmarshal(body, &req)
			docFilter, _ := req.Filter["doc_id"].(string)
			revFilter, _ := req.Filter["revision_id"].(string)
			idx.mu.Lock()
			var matches []map[string]any
			for _, rec := range idx.vectors {
				if rec.Namespace != req.Namespace {
					continue
				}
				docID, _ := rec.Metadata["doc_id"].(string)
				revID, _ := rec.Metadata["revision_id"].(string)
				if docFilter != "" && docID != docFilter {
					continue
				}
				if revFilter != "" && revID != revFilter {
					continue
				}
				m := map[string]any{"id": rec.ID, "score": 0.99}
				if req.IncludeMetadata {
					m["metadata"] = rec.Metadata
				}
				if req.IncludeValues {
					m["values"] = rec.Values
				}
				matches = append(matches, m)
			}
			idx.mu.Unlock()
			sort.Slice(matches, func(i, j int) bool { return matches[i]["id"].(string) < matches[j]["id"].(string) })
			if req.TopK > 0 && len(matches) > req.TopK {
				matches = matches[:req.TopK]
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"matches": matches})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func (idx *delayedHTTPIndex) flushLateWrite(ns, id string, vec []float32, meta map[string]any) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.vectors[idx.key(ns, id)] = delayedHTTPVectorRecord{ID: id, Namespace: ns, Values: vec, Metadata: meta}
}

func (idx *delayedHTTPIndex) setDeleteOutage(on bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.deleteOutage = on
}

func (idx *delayedHTTPIndex) idsInNamespace(ns string) []string {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	var ids []string
	for _, rec := range idx.vectors {
		if rec.Namespace == ns {
			ids = append(ids, rec.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

type queryEmbedderFunc func(context.Context, string) ([]float32, error)

func (f queryEmbedderFunc) Embed(ctx context.Context, text string) ([]float32, error) {
	return f(ctx, text)
}

func (queryEmbedderFunc) EmbedBatch(context.Context, []string) ([][]float32, error) {
	return nil, nil
}

type captureConverser struct {
	*llm.FakeConverser
	prompts []string
}

func (c *captureConverser) record(messages []brtypes.Message) {
	var b strings.Builder
	for _, m := range messages {
		for _, block := range m.Content {
			if text, ok := llm.BlockText(block); ok {
				b.WriteString(text)
				b.WriteByte('\n')
			}
		}
	}
	c.prompts = append(c.prompts, b.String())
}

func (c *captureConverser) Converse(ctx context.Context, messages []brtypes.Message) (*bedrockruntime.ConverseOutput, error) {
	c.record(messages)
	return c.FakeConverser.Converse(ctx, messages)
}

func (c *captureConverser) ConverseWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temp float32, maxTokens int) (*bedrockruntime.ConverseOutput, error) {
	c.record(messages)
	return c.FakeConverser.ConverseWithModelAndSystem(ctx, model, system, messages, temp, maxTokens)
}

func setupPostgresClient(t *testing.T) *pgstore.Client {
	t.Helper()
	client, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.Background())) })
	require.NoError(t, client.RunMigrations(t.Context()))
	return client
}

// makeCleanupsDue moves every tombstone past its drain window and retry time
// in SQL, so a test controls each transition instead of waiting on a clock.
func makeCleanupsDue(t *testing.T, client *pgstore.Client) {
	t.Helper()
	_, err := client.Pool.Exec(t.Context(), `UPDATE hosted_vector_cleanups
		SET retired_at = retired_at - interval '2 hours', next_attempt_at = now() - interval '1 second'`)
	require.NoError(t, err)
}

func cleanupState(t *testing.T, client *pgstore.Client, revision string) string {
	t.Helper()
	var state string
	require.NoError(t, client.Pool.QueryRow(t.Context(), `SELECT state FROM hosted_vector_cleanups WHERE revision_id = $1`, revision).Scan(&state))
	return state
}

func manifestCount(t *testing.T, client *pgstore.Client, revision string) int {
	t.Helper()
	var count int
	require.NoError(t, client.Pool.QueryRow(t.Context(), `SELECT count(*) FROM hosted_vector_manifests WHERE revision_id = $1`, revision).Scan(&count))
	return count
}

func TestHostedVectorManifestLifecycleAndRecovery(t *testing.T) {
	client := setupPostgresClient(t)
	client.EnableHostedVectorManifests(true)

	idx := newDelayedHTTPIndex()
	srv := idx.server()
	t.Cleanup(srv.Close)

	drv, err := pinecone.NewHTTPTestDriver(srv.URL, srv.Client(), 4, false)
	require.NoError(t, err)

	docUUID := uuid.NewString()
	docID := domain.DocumentID(docUUID)
	incarnation1 := uuid.NewString()
	rev1 := uuid.NewString()
	rev2 := uuid.NewString()
	lease1 := uuid.NewString()

	_, err = client.Pool.Exec(t.Context(), `
		INSERT INTO documents (id, content, status, collection, incarnation)
		VALUES ($1, 'v1', 'processing', 'archive', $2)
	`, docUUID, incarnation1)
	require.NoError(t, err)
	_, err = client.Pool.Exec(t.Context(), `
		INSERT INTO ingest_jobs (document_id, state, attempts, lease_owner, lease_token, lease_until)
		VALUES ($1, 'running', 1, 'w1', $2, now() + interval '1 minute')
	`, docUUID, lease1)
	require.NoError(t, err)

	job1 := domain.Job{
		Document:    domain.Document{ID: docID, Content: "v1"},
		Collection:  "archive",
		Incarnation: incarnation1,
		Attempts:    1,
		LeaseOwner:  "w1",
		LeaseToken:  lease1,
		RevisionID:  rev1,
	}

	chunk0ID := domain.ChunkID(uuid.NewString())
	chunk1ID := domain.ChunkID(uuid.NewString())
	chunks1 := []domain.Chunk{
		{ID: chunk0ID, DocumentID: docID, RevisionID: rev1, VectorID: domain.FormatRevisionVectorID(docID, rev1, 0), Index: 0, Text: "old chunk 0", Embedding: []float32{1, 0, 0, 0}, Metadata: map[string]any{"collection": "archive"}},
		{ID: chunk1ID, DocumentID: docID, RevisionID: rev1, VectorID: domain.FormatRevisionVectorID(docID, rev1, 1), Index: 1, Text: "old chunk 1", Embedding: []float32{0, 1, 0, 0}, Metadata: map[string]any{"collection": "archive"}},
	}
	intent1 := domain.VectorManifest{
		RevisionID:  rev1,
		DocumentID:  docID,
		Incarnation: incarnation1,
		Attempt:     1,
		LeaseToken:  lease1,
		Collection:  "archive",
		IDPrefix:    domain.RevisionVectorPrefix(docID, rev1),
		VectorIDs:   []string{chunks1[0].VectorID, chunks1[1].VectorID},
		ChunkIDs:    []string{string(chunk0ID), string(chunk1ID)},
	}
	require.NoError(t, client.PersistVectorManifest(t.Context(), job1, intent1))

	// Partial batch uploaded before Attempt 1 crashes / loses its lease.
	require.NoError(t, drv.Upsert(t.Context(), "", chunks1[:1]))
	require.Equal(t, []string{chunks1[0].VectorID}, idx.idsInNamespace(""))

	lease2 := uuid.NewString()
	_, err = client.Pool.Exec(t.Context(), `UPDATE ingest_jobs SET attempts = 2, lease_owner = 'w2', lease_token = $2 WHERE document_id = $1`, docUUID, lease2)
	require.NoError(t, err)

	// Stale Attempt 1 tries to complete after losing lease: fenced out cleanly without activating rev1.
	completed, err := client.CompleteJob(t.Context(), job1, chunks1, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, completed)

	// Attempt 2 runs PersistStage with rev2 and deletes only stale revision rev1.
	require.NoError(t, drv.DeleteByManifestRevision(t.Context(), docUUID, rev1, nil))
	chunks2 := []domain.Chunk{
		{ID: chunk0ID, DocumentID: docID, Index: 0, Text: "new chunk 0", Embedding: []float32{1, 0, 0, 0}, Metadata: map[string]any{"collection": "archive"}},
	}
	job2 := domain.Job{
		Document:    domain.Document{ID: docID, Content: "v2"},
		Collection:  "archive",
		Incarnation: incarnation1,
		Attempts:    2,
		LeaseOwner:  "w2",
		LeaseToken:  lease2,
		RevisionID:  rev2,
	}
	stage := pipeline.NewPersistStage(drv)
	pctx := &pipeline.IngestContext{
		DocumentID: docID,
		RawText:    "v2",
		Collection: "archive",
		RevisionID: rev2,
		PersistManifest: func(ctx context.Context, prepared []domain.Chunk) error {
			vectorIDs := make([]string, len(prepared))
			chunkIDs := make([]string, len(prepared))
			for i, c := range prepared {
				vectorIDs[i] = c.VectorID
				chunkIDs[i] = string(c.ID)
			}
			return client.PersistVectorManifest(ctx, job2, domain.VectorManifest{
				RevisionID:  rev2,
				DocumentID:  docID,
				Incarnation: incarnation1,
				Attempt:     2,
				LeaseToken:  lease2,
				Collection:  "archive",
				IDPrefix:    domain.RevisionVectorPrefix(docID, rev2),
				VectorIDs:   vectorIDs,
				ChunkIDs:    chunkIDs,
			})
		},
		Chunks: chunks2,
	}
	require.NoError(t, stage.Run(t.Context(), pctx))
	completed, err = client.CompleteJob(t.Context(), job2, chunks2, pctx.IndexObservation)
	require.NoError(t, err)
	require.True(t, completed)

	var activeRev string
	err = client.Pool.QueryRow(t.Context(), `SELECT active_revision_id::text FROM hosted_vector_revisions WHERE document_id = $1`, docUUID).Scan(&activeRev)
	require.NoError(t, err)
	require.Equal(t, rev2, activeRev)

	// Every delay is far beyond the test, so each pass performs exactly one transition.
	sweeper := worker.NewSweeper(client, drv, worker.SweepOptions{
		Owner:            "test-sweeper",
		PageSize:         1,
		Budget:           10,
		DrainDelay:       time.Hour,
		ConvergenceDelay: time.Hour,
		RetryBaseDelay:   time.Hour,
		RetryMaxDelay:    time.Hour,
	})
	rep, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, rep.TasksClaimed, "the superseded revision waits out its drain window")

	// A transient provider failure defers the task without losing the tombstone.
	idx.flushLateWrite("", chunks1[0].VectorID, []float32{1, 0, 0, 0}, map[string]any{"doc_id": docUUID, "revision_id": rev1, "chunk_id": string(chunk0ID)})
	// The adapter retries a failed delete on its own, so the outage lasts until
	// the test ends it rather than for a fixed number of requests.
	idx.setDeleteOutage(true)
	makeCleanupsDue(t, client)
	rep, err = sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksClaimed)
	require.Equal(t, 1, rep.TasksDeferred)
	require.Zero(t, rep.TasksCompleted)
	require.Equal(t, "pending", cleanupState(t, client, rev1))
	require.Contains(t, idx.idsInNamespace(""), chunks1[0].VectorID, "the outage left the vector in place")
	idx.setDeleteOutage(false)

	// The retry deletes the revision and moves to verification.
	makeCleanupsDue(t, client)
	rep, err = sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksDeferred)
	require.Zero(t, rep.TasksCompleted)
	require.Equal(t, "verifying", cleanupState(t, client, rev1))
	require.Equal(t, []string{domain.FormatRevisionVectorID(docID, rev2, 0)}, idx.idsInNamespace(""))

	// Verification finds nothing, so the tombstone and its retired manifest go.
	makeCleanupsDue(t, client)
	rep, err = sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksCompleted)
	require.Zero(t, manifestCount(t, client, rev1), "a completed cleanup leaves no retired manifest")
	require.Equal(t, 1, manifestCount(t, client, rev2), "the active manifest is untouched")

	// Delete document while delayed write is pending, then reuse the same doc UUID immediately.
	require.NoError(t, client.DeleteDocument(t.Context(), docID))

	incarnation2 := uuid.NewString()
	rev3 := uuid.NewString()
	lease3 := uuid.NewString()
	_, err = client.Pool.Exec(t.Context(), `
		INSERT INTO documents (id, content, status, collection, incarnation)
		VALUES ($1, 'reused', 'processing', 'archive', $2)
	`, docUUID, incarnation2)
	require.NoError(t, err)
	_, err = client.Pool.Exec(t.Context(), `
		INSERT INTO ingest_jobs (document_id, state, attempts, lease_owner, lease_token, lease_until)
		VALUES ($1, 'running', 1, 'w3', $2, now() + interval '1 minute')
	`, docUUID, lease3)
	require.NoError(t, err)
	reusedChunks := []domain.Chunk{
		{ID: domain.ChunkID(uuid.NewString()), DocumentID: docID, Index: 0, Text: "reused chunk", Embedding: []float32{1, 0, 0, 0}, Metadata: map[string]any{"collection": "archive"}},
	}
	reusedJob := domain.Job{
		Document:    domain.Document{ID: docID, Content: "reused"},
		Collection:  "archive",
		Incarnation: incarnation2,
		Attempts:    1,
		LeaseOwner:  "w3",
		LeaseToken:  lease3,
		RevisionID:  rev3,
	}
	reusedCtx := &pipeline.IngestContext{
		DocumentID: docID,
		RawText:    "reused",
		Collection: "archive",
		RevisionID: rev3,
		PersistManifest: func(ctx context.Context, prepared []domain.Chunk) error {
			return client.PersistVectorManifest(ctx, reusedJob, domain.VectorManifest{
				RevisionID:  rev3,
				DocumentID:  docID,
				Incarnation: incarnation2,
				Attempt:     1,
				LeaseToken:  lease3,
				Collection:  "archive",
				IDPrefix:    domain.RevisionVectorPrefix(docID, rev3),
				VectorIDs:   []string{prepared[0].VectorID},
				ChunkIDs:    []string{string(prepared[0].ID)},
			})
		},
		Chunks: reusedChunks,
	}
	require.NoError(t, stage.Run(t.Context(), reusedCtx))
	completed, err = client.CompleteJob(t.Context(), reusedJob, reusedChunks, reusedCtx.IndexObservation)
	require.NoError(t, err)
	require.True(t, completed)

	rev2VecID := domain.FormatRevisionVectorID(docID, rev2, 0)
	idx.flushLateWrite("", rev2VecID, []float32{1, 0, 0, 0}, map[string]any{"doc_id": docUUID, "revision_id": rev2, "chunk_id": string(chunk0ID)})

	// The deleted revision outlived the document, its UUID reuse and a late write.
	makeCleanupsDue(t, client)
	rep, err = sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksDeferred)
	makeCleanupsDue(t, client)
	rep, err = sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksCompleted)
	require.Equal(t, []string{domain.FormatRevisionVectorID(docID, rev3, 0)}, idx.idsInNamespace(""))
	require.Zero(t, manifestCount(t, client, rev2))
}

func TestHostedVectorSweeperPodResidueConvergenceAndReaderVisibilityGate(t *testing.T) {
	client := setupPostgresClient(t)
	idx := newDelayedHTTPIndex()
	idx.disableList = true
	srv := idx.server()
	t.Cleanup(srv.Close)

	drv, err := pinecone.NewHTTPTestDriver(srv.URL, srv.Client(), 4, false)
	require.NoError(t, err)

	docOldUUID := uuid.NewString()
	docOld := domain.DocumentID(docOldUUID)
	incOld := uuid.NewString()
	revOld := uuid.NewString()
	vecOld0 := domain.FormatRevisionVectorID(docOld, revOld, 0)
	_, err = client.Pool.Exec(t.Context(), `INSERT INTO hosted_vector_manifests (revision_id, document_id, incarnation, attempt, lease_token, collection, vector_namespace, id_prefix, vector_ids, chunk_ids, state) VALUES ($1,$2,$3,1,$4,'archive','', $5, ARRAY[$6], ARRAY[$7::uuid], 'retired')`,
		revOld, docOldUUID, incOld, uuid.NewString(), domain.RevisionVectorPrefix(docOld, revOld), vecOld0, uuid.NewString())
	require.NoError(t, err)
	_, err = client.Pool.Exec(t.Context(), `INSERT INTO hosted_vector_cleanups (revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids, next_attempt_at, retired_at, reason) VALUES ($1,$2,$3,'archive','', $4, ARRAY[$5], now() - interval '1 second', now() - interval '2 hours', 'superseded')`,
		revOld, docOldUUID, incOld, domain.RevisionVectorPrefix(docOld, revOld), vecOld0)
	require.NoError(t, err)

	idx.flushLateWrite("", vecOld0, []float32{1, 0, 0, 0}, map[string]any{"doc_id": docOldUUID, "revision_id": revOld, "chunk_id": uuid.NewString()})
	idx.mu.Lock()
	idx.ignoreNextDeleteID[vecOld0] = 1
	idx.mu.Unlock()

	// A pod index has no write fence: a vector may reappear after its delete, so
	// the task verifies twice and each pass performs exactly one transition.
	sweeper := worker.NewSweeper(client, drv, worker.SweepOptions{
		Owner:            "pod-sweeper",
		PageSize:         10,
		Budget:           10,
		DrainDelay:       time.Hour,
		ConvergenceDelay: time.Hour,
		RetryBaseDelay:   time.Hour,
		RetryMaxDelay:    time.Hour,
	})
	rep, err := sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksDeferred, "the delete reports success while the vector is still indexed")
	require.Equal(t, "verifying", cleanupState(t, client, revOld))
	require.Equal(t, []string{vecOld0}, idx.idsInNamespace(""))

	makeCleanupsDue(t, client)
	rep, err = sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksUnresolved)
	require.Equal(t, 1, rep.UnresolvedVectors)
	require.Equal(t, "unresolved", cleanupState(t, client, revOld))

	makeCleanupsDue(t, client)
	rep, err = sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksDeferred)
	require.Equal(t, "verifying", cleanupState(t, client, revOld))

	makeCleanupsDue(t, client)
	rep, err = sweeper.SweepOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, rep.TasksCompleted)
	require.Empty(t, idx.idsInNamespace(""))
	require.Zero(t, manifestCount(t, client, revOld))

	docManUUID, docLegUUID := uuid.NewString(), uuid.NewString()
	docMan := domain.DocumentID(docManUUID)
	revA, revStale := uuid.NewString(), uuid.NewString()
	for _, d := range []struct{ id, content, preview string }{
		{docManUUID, "Active revision alpine vault rules.", "Active revision preview"},
		{docLegUUID, "Legacy unrevisioned handbook.", "Legacy preview"},
	} {
		_, insertErr := client.Pool.Exec(t.Context(), `INSERT INTO documents (id, content, content_preview, status, collection) VALUES ($1, $2, $3, 'completed', 'archive')`, d.id, d.content, d.preview)
		require.NoError(t, insertErr)
	}
	_, err = client.Pool.Exec(t.Context(), `INSERT INTO hosted_vector_manifests (revision_id, document_id, incarnation, attempt, lease_token, collection, vector_namespace, id_prefix, vector_ids, chunk_ids, state) VALUES ($1,$2,$3,1,$4,'archive','', $5, ARRAY[$6], ARRAY[$7::uuid], 'active')`,
		revA, docManUUID, uuid.NewString(), uuid.NewString(), domain.RevisionVectorPrefix(docMan, revA), domain.FormatRevisionVectorID(docMan, revA, 0), uuid.NewString())
	require.NoError(t, err)
	_, err = client.Pool.Exec(t.Context(), `INSERT INTO hosted_vector_revisions (document_id, incarnation, active_revision_id) VALUES ($1, $2, $3)`,
		docManUUID, uuid.NewString(), revA)
	require.NoError(t, err)

	activeChunkID := uuid.NewString()
	staleChunkID := uuid.NewString()
	legacyChunkID := uuid.NewString()
	// Evidence is admitted only when the vector hit matches a stored chunk of a
	// completed document, so every vector has its chunk row. The stale vector's
	// chunk exists too: only the active-revision gate may exclude it.
	for _, c := range []struct{ id, doc, text string }{
		{activeChunkID, docManUUID, "ACTIVE REVISION SECRET PHRASE"},
		{staleChunkID, docManUUID, "STALE SUPPRESSED PHRASE"},
		{legacyChunkID, docLegUUID, "LEGACY ACTIVE PHRASE"},
	} {
		_, insertErr := client.Pool.Exec(t.Context(), `INSERT INTO chunks (id, document_id, chunk_index, text, token_count) VALUES ($1, $2, 0, $3, 3)`, c.id, c.doc, c.text)
		require.NoError(t, insertErr)
	}
	idx.flushLateWrite("", domain.FormatRevisionVectorID(docMan, revA, 0), []float32{1, 0, 0, 0}, map[string]any{
		"doc_id": docManUUID, "revision_id": revA, "chunk_id": activeChunkID, "chunk_index": 0, "collection": "archive", "text": "ACTIVE REVISION SECRET PHRASE",
	})
	idx.flushLateWrite("", domain.FormatRevisionVectorID(docMan, revStale, 0), []float32{1, 0, 0, 0}, map[string]any{
		"doc_id": docManUUID, "revision_id": revStale, "chunk_id": staleChunkID, "chunk_index": 0, "collection": "archive", "text": "STALE SUPPRESSED PHRASE",
	})
	idx.flushLateWrite("", legacyChunkID, []float32{1, 0, 0, 0}, map[string]any{
		"doc_id": docLegUUID, "chunk_id": legacyChunkID, "chunk_index": 0, "collection": "archive", "text": "LEGACY ACTIVE PHRASE",
	})

	emb := queryEmbedderFunc(func(context.Context, string) ([]float32, error) {
		return []float32{1, 0, 0, 0}, nil
	})
	capturer := &captureConverser{FakeConverser: llm.NewFakeConverser()}
	cfg := &config.Config{
		BedrockFallbackModel: "fallback",
		MaxChunksPerQuery:    10,
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(capturer)

	qHandler := handler.QueryHandler(retriever.NewRetriever(drv, emb, 0.5, 10), amb, cfg, client.Pool)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(`{"query":"rules"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	qHandler(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp handler.QueryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, 2, resp.ChunksUsed)
	for _, sc := range resp.ScoredChunks {
		require.NotContains(t, sc.Text, "STALE SUPPRESSED")
	}
	require.NotContains(t, resp.Answer, "STALE SUPPRESSED")
	require.NotEmpty(t, capturer.prompts)
	require.NotContains(t, capturer.prompts[0], "STALE SUPPRESSED")
	require.Contains(t, capturer.prompts[0], "ACTIVE REVISION SECRET PHRASE")
	require.Contains(t, capturer.prompts[0], "LEGACY ACTIVE PHRASE")
}
