package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// delayedLegacyBatch models provider batches whose caller can return before
// the first physical mutation takes effect. The test releases that effect
// explicitly after the worker records its retry.
type delayedLegacyBatch struct {
	*memory.FakeVectorStore
	loseDelete  bool
	failure     error
	applyLate   func(context.Context) error
	upserts     []domain.Chunk
	bareDeletes int
}

func (v *delayedLegacyBatch) Upsert(ctx context.Context, ns string, chunks []domain.Chunk) error {
	for _, ch := range chunks {
		v.upserts = append(v.upserts, ch)
		batch := []domain.Chunk{ch}
		if v.failure != nil && !v.loseDelete && v.applyLate == nil {
			v.applyLate = func(ctx context.Context) error { return v.FakeVectorStore.Upsert(ctx, ns, batch) }
			return v.failure
		}
		if err := v.FakeVectorStore.Upsert(ctx, ns, batch); err != nil {
			return err
		}
	}
	return nil
}

func (v *delayedLegacyBatch) DeleteByDocID(ctx context.Context, id string) error {
	v.bareDeletes++
	if v.failure != nil && v.loseDelete && v.applyLate == nil {
		v.applyLate = func(ctx context.Context) error { return v.FakeVectorStore.DeleteByDocID(ctx, id) }
		return v.failure
	}
	return v.FakeVectorStore.DeleteByDocID(ctx, id)
}

type recordedMutationRetry struct {
	job     domain.Job
	delay   time.Duration
	message string
	err     error
}

type mutationRetryStore struct {
	*pgstore.Client
	retries chan recordedMutationRetry
}

func (s mutationRetryStore) RetryJob(ctx context.Context, job domain.Job, delay time.Duration, message string) error {
	err := s.Client.RetryJob(ctx, job, delay, message)
	select {
	case s.retries <- recordedMutationRetry{job, delay, message, err}:
	case <-ctx.Done():
	}
	return err
}

func mutationRetryWorker(s mutationRetryStore, vectors *delayedLegacyBatch, manifested bool) *worker.Worker {
	stages := []pipeline.Stage{
		pipeline.NewChunkStage(chunker.NewChunker(16, 0)),
		pipeline.NewEmbedStage(revisionEmbedder{1, 0}, 1),
		pipeline.NewPersistStage(vectors),
	}
	return worker.New(s, stages, vectors, worker.Options{
		Workers: 1, Owner: "mutation-retry", MaxAttempts: 3, Lease: 30 * time.Second,
		JobTimeout: 20 * time.Second, PollInterval: 10 * time.Millisecond, HostedVectorManifest: manifested,
	})
}

func awaitMutationRetry(ctx context.Context, t *testing.T, s mutationRetryStore) recordedMutationRetry {
	t.Helper()
	select {
	case retry := <-s.retries:
		require.NoError(t, retry.err)
		require.Greater(t, retry.delay, time.Duration(0))
		return retry
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return recordedMutationRetry{}
	}
}

func TestManifestedWorkerRecoversAfterLostLegacyUpsertAck(t *testing.T) {
	c := durableDatabase(t)
	for _, tc := range []struct {
		name       string
		before     bool
		manifested bool
		loseDelete bool
		failure    error
	}{
		{name: "late batch before manifested retry", before: true, manifested: true, failure: context.DeadlineExceeded},
		{name: "late batch after manifested publication", manifested: true, failure: context.DeadlineExceeded},
		{name: "legacy retry after acknowledged cleanup", before: true, failure: context.DeadlineExceeded},
		{name: "manifested retry after uncertain delete", manifested: true, loseDelete: true, failure: errors.New("provider disconnected during cleanup")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			id := domain.DocumentID(uuid.NewString())
			body := tc.name + ". " + strings.Repeat("The community archive opens early every Monday. ", 12)
			res, err := c.AcceptDocument(ctx, domain.AcceptRequest{
				Document: domain.Document{ID: id, Content: body}, CallerSupplied: true,
				Fingerprint: chunker.Fingerprint(body), MaxPending: 1,
			})
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, res.Outcome)
			t.Cleanup(func() {
				_, cleanupErr := c.Pool.Exec(context.WithoutCancel(ctx), `DELETE FROM documents WHERE id=$1`, id)
				require.NoError(t, cleanupErr)
			})
			index := memory.NewFakeVectorStore()
			old := &delayedLegacyBatch{FakeVectorStore: index, loseDelete: tc.loseDelete, failure: tc.failure}
			store := mutationRetryStore{Client: c, retries: make(chan recordedMutationRetry, 1)}
			legacy := mutationRetryWorker(store, old, false)
			legacy.Start(ctx)
			defer legacy.Shutdown()
			first := awaitMutationRetry(ctx, t, store)
			legacy.Shutdown()
			require.Equal(t, 1, first.job.Attempts)
			require.Equal(t, id, first.job.Document.ID)
			require.False(t, first.job.ScopedVectors)
			require.Contains(t, first.message, tc.failure.Error())
			require.NotNil(t, old.applyLate)
			require.Equal(t, 1, old.bareDeletes)
			physical, err := index.Query(ctx, "", []float32{1, 0}, 100, "")
			require.NoError(t, err)
			require.Empty(t, physical)
			if tc.before {
				require.NoError(t, old.applyLate(ctx))
			}
			tag, err := c.Pool.Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at=clock_timestamp() WHERE document_id=$1 AND state='pending'`, id)
			require.NoError(t, err)
			require.EqualValues(t, 1, tag.RowsAffected())

			next := &delayedLegacyBatch{FakeVectorStore: index}
			retry := mutationRetryWorker(store, next, tc.manifested)
			c.EnableHostedVectorManifests(tc.manifested)
			retry.Start(ctx)
			defer retry.Shutdown()
			if !tc.manifested || tc.loseDelete {
				rejected := awaitMutationRetry(ctx, t, store)
				retry.Shutdown()
				require.Equal(t, 2, rejected.job.Attempts)
				require.Equal(t, first.job.Incarnation, rejected.job.Incarnation)
				require.Contains(t, rejected.message, "vector mutation is unresolved for the publication target")
				if tc.manifested {
					require.Zero(t, next.bareDeletes)
				} else {
					require.Equal(t, 1, next.bareDeletes)
				}
				requireRejectedManifestRetry(t, c, id, tc.loseDelete)
				return
			}
			durableStatus(t, c, string(id), "completed")
			retry.Shutdown()
			if !tc.before {
				require.NoError(t, old.applyLate(ctx))
			}
			require.Zero(t, next.bareDeletes)
			require.Len(t, old.upserts, 1)
			require.Empty(t, old.upserts[0].RevisionID)
			require.Empty(t, old.upserts[0].VectorID)
			requirePublishedManifestVectors(ctx, t, c, index, old.upserts[0], next.upserts)
		})
	}
}

func requireRejectedManifestRetry(t *testing.T, c *pgstore.Client, id domain.DocumentID, deleteUncertain bool) {
	t.Helper()
	var pending, uncertain, erase bool
	var status string
	var chunks, active, pointers int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT status, vector_mutation_pending,
		vector_mutation_uncertain, vector_delete_uncertain,
		(SELECT count(*) FROM chunks WHERE document_id=$1),
		(SELECT count(*) FROM hosted_vector_manifests WHERE document_id=$1 AND state='active'),
		(SELECT count(*) FROM hosted_vector_revisions WHERE document_id=$1)
		FROM documents WHERE id=$1`, id).Scan(&status, &pending, &uncertain, &erase, &chunks, &active, &pointers))
	require.Equal(t, "processing", status)
	require.False(t, pending)
	require.True(t, uncertain)
	require.Equal(t, deleteUncertain, erase)
	require.Zero(t, chunks)
	require.Zero(t, active)
	require.Zero(t, pointers)
}

func requirePublishedManifestVectors(ctx context.Context, t *testing.T, c *pgstore.Client, index *memory.FakeVectorStore, bare domain.Chunk, batches []domain.Chunk) {
	t.Helper()
	id := bare.DocumentID
	var revision string
	var pending, uncertain, erase bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT r.active_revision_id::text, d.vector_mutation_pending,
		d.vector_mutation_uncertain, d.vector_delete_uncertain FROM hosted_vector_revisions r
		JOIN documents d ON d.id=r.document_id WHERE d.id=$1`, id).Scan(&revision, &pending, &uncertain, &erase))
	require.False(t, pending)
	require.True(t, uncertain)
	require.False(t, erase)
	require.Greater(t, len(batches), 1)
	var keys []string
	for _, ch := range batches {
		require.Equal(t, revision, ch.RevisionID)
		require.Equal(t, domain.FormatRevisionVectorID(id, revision, ch.Index), ch.VectorID)
		require.NotEqual(t, string(bare.ID), ch.VectorID)
		keys = append(keys, ch.VectorID)
	}
	manifest, err := c.GetVectorManifest(ctx, revision)
	require.NoError(t, err)
	require.NotNil(t, manifest)
	require.Equal(t, "active", manifest.State)
	require.Equal(t, 2, manifest.Attempt)
	require.Equal(t, keys, manifest.VectorIDs)
	// PostgreSQL returns canonical UUIDs for logical chunk IDs.
	bareChunkID, err := uuid.Parse(string(bare.ID))
	require.NoError(t, err)
	require.Contains(t, manifest.ChunkIDs, bareChunkID.String())
	keys = append(keys, string(bare.ID))
	present, err := index.FetchPresentVectorIDs(ctx, "", keys)
	require.NoError(t, err)
	require.ElementsMatch(t, keys, present)
	physical, err := index.Query(ctx, "", []float32{1, 0}, len(keys), "")
	require.NoError(t, err)
	require.Len(t, physical, len(keys))
	visible, previews, err := (pgstore.Visibility{Pool: c.Pool}).VisibleEvidenceWithPreviews(ctx, physical, []string{string(id)})
	require.NoError(t, err)
	require.Len(t, visible, len(batches))
	require.NotEmpty(t, previews[string(id)])
	for _, hit := range visible {
		require.Equal(t, revision, hit.Chunk.RevisionID)
	}
}
