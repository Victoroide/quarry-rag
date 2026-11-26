package integration

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type revisionEmbedder []float32

func (e revisionEmbedder) Embed(context.Context, string) ([]float32, error) { return e, nil }

type revisionSources map[string]string

func (s revisionSources) Delete(_ context.Context, key string) error {
	delete(s, key)
	return nil
}

type terminalRevisionStage struct{}

func (terminalRevisionStage) ID() pipeline.StageID { return pipeline.StagePersist }
func (terminalRevisionStage) Run(context.Context, *pipeline.IngestContext) error {
	return domain.Permanent(errors.New("terminal test failure"))
}

// claimSource is the part of the job store that the worker calls to lease work.
type claimSource interface {
	ClaimJob(ctx context.Context, owner string, lease time.Duration, maxAttempts int) (*domain.Job, error)
	ClaimJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*domain.Job, error)
}

// observedRevisionJobs reports every lease that the worker obtains. The worker
// prefers the fixture-aware claim, so both claim methods must be observed or a
// promoted method of the embedded client would lease work unnoticed.
type observedRevisionJobs struct {
	*pgstore.Client
	claimed     chan domain.Job
	source      claimSource
	claimedOnce *bool // Optional guard for a fixture with one claim loop.
}

func observeClaims(c *pgstore.Client, claimed chan domain.Job) observedRevisionJobs {
	return observedRevisionJobs{Client: c, claimed: claimed, source: c}
}

func (s observedRevisionJobs) observe(ctx context.Context, job *domain.Job, err error) (*domain.Job, error) {
	if job == nil || err != nil {
		return job, err
	}
	if s.claimedOnce != nil {
		*s.claimedOnce = true
	}
	select {
	case s.claimed <- *job:
	case <-ctx.Done():
	}
	return job, err
}

func (s observedRevisionJobs) ClaimJob(ctx context.Context, owner string, lease time.Duration, maxAttempts int) (*domain.Job, error) {
	if s.claimedOnce != nil && *s.claimedOnce {
		return nil, nil
	}
	job, err := s.source.ClaimJob(ctx, owner, lease, maxAttempts)
	return s.observe(ctx, job, err)
}

func (s observedRevisionJobs) ClaimJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*domain.Job, error) {
	if s.claimedOnce != nil && *s.claimedOnce {
		return nil, nil
	}
	job, err := s.source.ClaimJobForFixtureStore(ctx, owner, lease, maxAttempts, vectorIdentity)
	return s.observe(ctx, job, err)
}

func revisionWorker(c *pgstore.Client, v interface {
	domain.VectorStore
	domain.VectorDeleter
}, e revisionEmbedder, extra ...pipeline.Stage) (processor *worker.Worker, jobs <-chan domain.Job) {
	claimed := make(chan domain.Job, 1)
	return newRevisionWorker(observeClaims(c, claimed), v, e, extra...), claimed
}

func newRevisionWorker(store observedRevisionJobs, v interface {
	domain.VectorStore
	domain.VectorDeleter
}, e revisionEmbedder, extra ...pipeline.Stage) *worker.Worker {
	stages := []pipeline.Stage{
		pipeline.NewChunkStage(chunker.NewChunker(128, 16)), pipeline.NewEmbedStage(e, 1), pipeline.NewPersistStage(v),
	}
	return worker.New(store, append(stages, extra...), v, worker.Options{
		Workers: 1, Owner: "same-worker", Lease: 2 * time.Second,
		JobTimeout: 20 * time.Second, PollInterval: 10 * time.Millisecond,
	})
}

func TestDelayedWorkerEffectsCannotReachReplacement(t *testing.T) {
	c := durableDatabase(t)
	ctx := t.Context()
	_, err := c.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	cases := []struct {
		operation delayedVectorOperation
		mode      delayedVectorMode
		rawHits   int
	}{
		{operation: startupCleanup, mode: lateEffect, rawHits: 1},
		{operation: upsert, mode: lateEffect, rawHits: 2},
		{operation: terminalCleanup, mode: lateEffect, rawHits: 1},
		{operation: terminalCleanup, mode: canceledEffect, rawHits: 2},
	}
	for _, tc := range cases {
		t.Run(string(tc.operation)+"/"+string(tc.mode), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			id := domain.DocumentID(uuid.NewString())
			const body = "The archive opens on Monday."
			firstKey, secondKey := "first-source", "replacement-source"
			sources := revisionSources{firstKey: body}
			accept := func(key *string) {
				t.Helper()
				result, err := c.AcceptDocument(ctx, domain.AcceptRequest{
					Document: domain.Document{ID: id, Content: body, S3Key: key}, CallerSupplied: true,
					Fingerprint: chunker.Fingerprint(body), MaxPending: 5, SupportsScopedVectors: true,
				})
				require.NoError(t, err)
				require.Equal(t, domain.AcceptAccepted, result.Outcome)
			}
			accept(&firstKey)
			index := memory.NewFakeVectorStore()
			delayed := &delayedRevisionVectors{
				FakeVectorStore: index,
				operation:       tc.operation,
				mode:            tc.mode,
				entered:         make(chan delayedVectorCall, 1),
				proceed:         make(chan struct{}),
				finished:        make(chan delayedVectorResult, 1),
			}
			unblock := sync.OnceFunc(func() { close(delayed.proceed) })
			var extra []pipeline.Stage
			if tc.operation == terminalCleanup {
				extra = append(extra, terminalRevisionStage{})
			}
			oldJobs := make(chan domain.Job, 1)
			oldStore := observeClaims(c, oldJobs)
			// A heartbeat can cancel the call as soon as the SQL row is retired.
			// Only the replacement worker should be able to claim the reused ID.
			oldStore.claimedOnce = new(bool)
			old := newRevisionWorker(oldStore, delayed, revisionEmbedder{0, 1}, extra...)
			oldCtx, cancelOld := context.WithCancel(ctx)
			old.Start(oldCtx)
			defer old.Shutdown()
			defer cancelOld()
			defer unblock()
			oldJob := awaitRevisionJob(ctx, t, oldJobs)
			require.Equal(t, id, oldJob.Document.ID)
			oldIdentity := revisionVectorIdentity{documentID: id, revision: oldJob.Incarnation, attempt: oldJob.LeaseToken}
			expectedCall := delayedVectorCall{operation: tc.operation, identity: oldIdentity}
			require.Equal(t, expectedCall, awaitRevisionEvent(ctx, t, delayed.entered, "vector dispatch"))

			// Retire the source and SQL row while the original vector effect is still in flight.
			doc, err := c.GetDocument(ctx, id)
			require.NoError(t, err)
			require.NoError(t, cleanup.Remove(ctx, c, nil, sources, doc))
			require.NotContains(t, sources, firstKey)
			sources[secondKey] = body
			accept(&secondKey)
			next, nextJobs := revisionWorker(c, index, revisionEmbedder{1, 0})
			next.Start(ctx)
			defer next.Shutdown()
			newJob := awaitRevisionJob(ctx, t, nextJobs)
			require.Equal(t, id, newJob.Document.ID)
			require.Equal(t, oldJob.LeaseOwner, newJob.LeaseOwner)
			require.Equal(t, oldJob.Attempts, newJob.Attempts)
			require.NotEmpty(t, oldJob.LeaseToken)
			require.NotEmpty(t, newJob.LeaseToken)
			require.NotEqual(t, oldJob.LeaseToken, newJob.LeaseToken)
			require.NotEmpty(t, oldJob.Incarnation)
			require.NotEmpty(t, newJob.Incarnation)
			require.NotEqual(t, oldJob.Incarnation, newJob.Incarnation)
			durableStatus(t, c, string(id), "completed")
			if tc.mode == canceledEffect {
				cancelOld()
			} else {
				unblock()
			}
			result := awaitRevisionEvent(ctx, t, delayed.finished, "vector outcome")
			require.Equal(t, expectedCall, result.call)
			if tc.mode == canceledEffect {
				require.False(t, result.invoked)
				require.ErrorIs(t, result.err, context.Canceled, "dispatch cancellation cause: %v", result.cause)
				require.Error(t, result.cause)
			} else {
				require.True(t, result.invoked)
				require.NoError(t, result.err)
			}
			cancelOld()
			old.Shutdown()
			require.Zero(t, delayed.unqualifiedDeletes)
			require.False(t, delayed.repeatedDispatch)

			key, err := c.GetDocumentS3Key(ctx, id)
			require.NoError(t, err)
			require.Equal(t, &secondKey, key)
			require.Equal(t, body, sources[*key])
			hits, err := index.Query(ctx, "", []float32{1, 0}, 10, "")
			require.NoError(t, err)
			require.Len(t, hits, tc.rawHits)
			newIdentity := revisionVectorIdentity{documentID: id, revision: newJob.Incarnation, attempt: newJob.LeaseToken}
			current := requireRevisionHit(t, hits, newIdentity, body, 1)
			require.Equal(t, newIdentity, chunkVectorIdentity(hits[0].Chunk))
			if tc.rawHits == 2 {
				retired := requireRevisionHit(t, hits, oldIdentity, body, 0)
				require.Equal(t, current.Chunk.ID, retired.Chunk.ID)
			}
			visible, err := (pgstore.Visibility{Pool: c.Pool}).VisibleEvidence(ctx, hits)
			require.NoError(t, err)
			require.Len(t, visible, 1)
			validated := requireRevisionHit(t, visible, newIdentity, body, 1)
			require.Equal(t, current.Chunk.ID, validated.Chunk.ID)
		})
	}
}

func awaitRevisionJob(ctx context.Context, t *testing.T, jobs <-chan domain.Job) domain.Job {
	t.Helper()
	return awaitRevisionEvent(ctx, t, jobs, "worker claim")
}

func awaitRevisionEvent[T any](ctx context.Context, t *testing.T, events <-chan T, event string) T {
	t.Helper()
	select {
	case value := <-events:
		return value
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", event, ctx.Err())
		var zero T
		return zero
	}
}

func requireRevisionHit(t *testing.T, hits []domain.ScoredChunk, identity revisionVectorIdentity, body string, score float64) domain.ScoredChunk {
	t.Helper()
	index := slices.IndexFunc(hits, func(hit domain.ScoredChunk) bool {
		return chunkVectorIdentity(hit.Chunk) == identity
	})
	require.NotEqual(t, -1, index, "missing hit for %+v", identity)
	hit := hits[index]
	require.Equal(t, body, hit.Chunk.Text)
	require.Equal(t, score, hit.Score)
	return hit
}
