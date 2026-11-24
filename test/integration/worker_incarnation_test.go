package integration

import (
	"context"
	"errors"
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

type delayedRevisionVectors struct {
	*memory.FakeVectorStore
	operation string
	entered   chan string
	proceed   chan struct{}
	deletes   int
}

func (v *delayedRevisionVectors) wait(ctx context.Context, revision string) error {
	select {
	case v.entered <- revision:
	case <-ctx.Done():
		return ctx.Err()
	}
	// The caller can lose its lease while this effect is in flight.
	select {
	case <-v.proceed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (v *delayedRevisionVectors) Upsert(ctx context.Context, ns string, chunks []domain.Chunk) error {
	if v.operation == "upsert" {
		revision, _ := chunks[0].Metadata[domain.VectorRevisionKey].(string)
		if err := v.wait(ctx, revision); err != nil {
			return err
		}
	}
	return v.FakeVectorStore.Upsert(ctx, ns, chunks)
}

func (v *delayedRevisionVectors) DeleteByAttempt(ctx context.Context, id, revision, attempt string) error {
	v.deletes++
	if (v.operation == "startup cleanup" && v.deletes == 1) || (v.operation == "terminal cleanup" && v.deletes == 2) {
		if err := v.wait(ctx, revision); err != nil {
			return err
		}
	}
	return v.FakeVectorStore.DeleteByAttempt(ctx, id, revision, attempt)
}

func (v *delayedRevisionVectors) DeleteByDocID(context.Context, string) error {
	return errors.New("tagged worker attempted an unqualified cleanup")
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
	claimed chan domain.Job
	source  claimSource
}

func observeClaims(c *pgstore.Client, claimed chan domain.Job) observedRevisionJobs {
	return observedRevisionJobs{Client: c, claimed: claimed, source: c}
}

func (s observedRevisionJobs) observe(ctx context.Context, job *domain.Job, err error) (*domain.Job, error) {
	if job == nil || err != nil {
		return job, err
	}
	select {
	case s.claimed <- *job:
	case <-ctx.Done():
	}
	return job, err
}

func (s observedRevisionJobs) ClaimJob(ctx context.Context, owner string, lease time.Duration, maxAttempts int) (*domain.Job, error) {
	job, err := s.source.ClaimJob(ctx, owner, lease, maxAttempts)
	return s.observe(ctx, job, err)
}

func (s observedRevisionJobs) ClaimJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*domain.Job, error) {
	job, err := s.source.ClaimJobForFixtureStore(ctx, owner, lease, maxAttempts, vectorIdentity)
	return s.observe(ctx, job, err)
}

func revisionWorker(c *pgstore.Client, v interface {
	domain.VectorStore
	domain.VectorDeleter
}, e revisionEmbedder, extra ...pipeline.Stage) (processor *worker.Worker, jobs <-chan domain.Job) {
	claimed := make(chan domain.Job, 1)
	stages := []pipeline.Stage{
		pipeline.NewChunkStage(chunker.NewChunker(128, 16)), pipeline.NewEmbedStage(e, 1), pipeline.NewPersistStage(v),
	}
	w := worker.New(observeClaims(c, claimed), append(stages, extra...), v, worker.Options{
		Workers: 1, Owner: "same-worker", Lease: 2 * time.Second,
		JobTimeout: 20 * time.Second, PollInterval: 10 * time.Millisecond,
	})
	return w, claimed
}

func TestDelayedWorkerEffectsCannotReachReplacement(t *testing.T) {
	c := durableDatabase(t)
	ctx := t.Context()
	_, err := c.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	for _, operation := range []string{"startup cleanup", "upsert", "terminal cleanup"} {
		t.Run(operation, func(t *testing.T) {
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
			delayed := &delayedRevisionVectors{index, operation, make(chan string, 1), make(chan struct{}), 0}
			unblock := sync.OnceFunc(func() { close(delayed.proceed) })
			var extra []pipeline.Stage
			if operation == "terminal cleanup" {
				extra = append(extra, terminalRevisionStage{})
			}
			old, oldJobs := revisionWorker(c, delayed, revisionEmbedder{0, 1}, extra...)
			old.Start(ctx)
			defer old.Shutdown()
			defer unblock()
			oldJob := awaitRevisionJob(ctx, t, oldJobs)
			select {
			case revision := <-delayed.entered:
				require.Equal(t, oldJob.Incarnation, revision)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

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
			require.Equal(t, oldJob.LeaseOwner, newJob.LeaseOwner)
			require.Equal(t, oldJob.Attempts, newJob.Attempts)
			require.NotEmpty(t, oldJob.LeaseToken)
			require.NotEmpty(t, newJob.LeaseToken)
			require.NotEqual(t, oldJob.LeaseToken, newJob.LeaseToken)
			require.NotEqual(t, oldJob.Incarnation, newJob.Incarnation)
			durableStatus(t, c, string(id), "completed")
			unblock()
			old.Shutdown()

			key, err := c.GetDocumentS3Key(ctx, id)
			require.NoError(t, err)
			require.Equal(t, &secondKey, key)
			require.Equal(t, body, sources[*key])
			hits, err := index.Query(ctx, "", []float32{1, 0}, 10, "")
			require.NoError(t, err)
			require.NotEmpty(t, hits)
			require.Equal(t, newJob.Incarnation, hits[0].Chunk.Metadata[domain.VectorRevisionKey])
			require.Equal(t, body, hits[0].Chunk.Text)
			require.EqualValues(t, 1, hits[0].Score)
			if operation == "upsert" {
				require.Len(t, hits, 2)
				require.Equal(t, hits[0].Chunk.ID, hits[1].Chunk.ID)
				require.Equal(t, hits[0].Chunk.Text, hits[1].Chunk.Text)
				require.Equal(t, oldJob.Incarnation, hits[1].Chunk.Metadata[domain.VectorRevisionKey])
			} else {
				require.Len(t, hits, 1)
			}
		})
	}
}

func awaitRevisionJob(ctx context.Context, t *testing.T, jobs <-chan domain.Job) domain.Job {
	t.Helper()
	select {
	case job := <-jobs:
		return job
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return domain.Job{}
	}
}
