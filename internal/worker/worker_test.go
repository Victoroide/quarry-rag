package worker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type leaseTestStore struct {
	mu        sync.Mutex
	jobs      []domain.Job
	events    chan string
	heartbeat bool
	delay     time.Duration
}

func leaseStore(n int) *leaseTestStore {
	s := &leaseTestStore{events: make(chan string, 100), heartbeat: true}
	for range n {
		s.jobs = append(s.jobs, domain.Job{Document: domain.Document{ID: "doc", Content: "text"}, Attempts: 1})
	}
	return s
}
func (s *leaseTestStore) ClaimJob(_ context.Context, owner string, _ time.Duration, _ int) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.jobs) == 0 {
		return nil, nil
	}
	j := s.jobs[0]
	j.LeaseOwner = owner
	s.jobs = s.jobs[1:]
	return &j, nil
}
func (s *leaseTestStore) HeartbeatJob(context.Context, domain.Job, time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartbeat, nil
}
func (s *leaseTestStore) CompleteJob(context.Context, domain.Job, []domain.Chunk, domain.IndexObservation) (bool, error) {
	s.events <- "completed"
	return true, nil
}
func (s *leaseTestStore) RetryJob(ctx context.Context, _ domain.Job, d time.Duration, _ string) error {
	s.mu.Lock()
	s.delay = d
	s.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.events <- "retry"
	return nil
}
func (s *leaseTestStore) FailJob(context.Context, domain.Job, string) error {
	s.events <- "failed"
	return nil
}
func (s *leaseTestStore) ReleaseJob(ctx context.Context, _ domain.Job) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.events <- "released"
	return nil
}
func (*leaseTestStore) DocumentExists(context.Context, domain.DocumentID) (bool, error) {
	return true, nil
}

type leaseTestStage struct{ run func(context.Context) error }

func (leaseTestStage) ID() pipeline.StageID                                       { return pipeline.StageEmbed }
func (s leaseTestStage) Run(ctx context.Context, _ *pipeline.IngestContext) error { return s.run(ctx) }
func leaseWorker(s *leaseTestStore, st pipeline.Stage, workers int) *Worker {
	return New(s, []pipeline.Stage{st}, nil, Options{Workers: workers, MaxAttempts: 5, Lease: 30 * time.Millisecond, JobTimeout: time.Second, PollInterval: time.Millisecond, Owner: "test"})
}
func leaseEvent(t *testing.T, s *leaseTestStore, want string) {
	t.Helper()
	select {
	case got := <-s.events:
		require.Equal(t, want, got)
	case <-time.After(3 * time.Second):
		t.Fatal("no job state transition")
	}
}
func TestRetryableErrorIsRescheduled(t *testing.T) {
	s := leaseStore(1)
	w := leaseWorker(s, leaseTestStage{func(context.Context) error { return errors.New("temporary") }}, 1)
	w.Start(t.Context())
	defer w.Shutdown()
	leaseEvent(t, s, "retry")
	s.mu.Lock()
	defer s.mu.Unlock()
	require.GreaterOrEqual(t, s.delay, 5*time.Second)
	require.LessOrEqual(t, s.delay, 6*time.Second)
}
func TestPermanentErrorMarksDocumentFailed(t *testing.T) {
	s := leaseStore(1)
	w := leaseWorker(s, leaseTestStage{func(context.Context) error { return domain.Permanent(errors.New("invalid")) }}, 1)
	w.Start(t.Context())
	defer w.Shutdown()
	leaseEvent(t, s, "failed")
}
func TestHeartbeatLossCancelsJob(t *testing.T) {
	s := leaseStore(1)
	s.heartbeat = false
	cause := make(chan error, 1)
	w := leaseWorker(s, leaseTestStage{func(ctx context.Context) error { <-ctx.Done(); cause <- context.Cause(ctx); return ctx.Err() }}, 1)
	w.Start(t.Context())
	defer w.Shutdown()
	select {
	case err := <-cause:
		require.ErrorIs(t, err, errLeaseLost)
	case <-time.After(time.Second):
		t.Fatal("lease loss did not cancel processing")
	}
	w.Shutdown()
	select {
	case event := <-s.events:
		t.Fatalf("lost owner changed job: %s", event)
	default:
	}
}
func TestShutdownReleasesLease(t *testing.T) {
	s := leaseStore(1)
	started := make(chan struct{})
	w := leaseWorker(s, leaseTestStage{func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() }}, 1)
	w.Start(t.Context())
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, w.ShutdownContext(ctx), context.Canceled)
	leaseEvent(t, s, "released")
	w.Shutdown()
}
func TestWorkersProcessJobsInParallel(t *testing.T) {
	s := leaseStore(2)
	entered := make(chan struct{}, 2)
	finish := make(chan struct{})
	w := leaseWorker(s, leaseTestStage{func(ctx context.Context) error {
		entered <- struct{}{}
		select {
		case <-finish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}, 2)
	w.Start(t.Context())
	defer w.Shutdown()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("documents did not process in parallel")
		}
	}
	close(finish)
	leaseEvent(t, s, "completed")
	leaseEvent(t, s, "completed")
}

func TestJobTimeoutReschedulesWithDetachedContext(t *testing.T) {
	s := leaseStore(1)
	w := leaseWorker(s, leaseTestStage{func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}, 1)
	w.opts.JobTimeout = 5 * time.Millisecond
	w.opts.Lease = time.Second
	w.Start(t.Context())
	defer w.Shutdown()
	leaseEvent(t, s, "retry")
}
func TestExhaustedAttemptMarksDocumentFailed(t *testing.T) {
	s := leaseStore(1)
	s.jobs[0].Attempts = 5
	w := leaseWorker(s, leaseTestStage{func(context.Context) error { return errors.New("temporary") }}, 1)
	w.Start(t.Context())
	defer w.Shutdown()
	leaseEvent(t, s, "failed")
}

type leaseVectorRecorder struct{ events chan string }

func (leaseVectorRecorder) DeleteAllVectorsInNamespace(context.Context, string) error { return nil }
func (v leaseVectorRecorder) DeleteByDocID(context.Context, string) error {
	v.events <- "delete"
	return nil
}
func TestEveryAttemptDeletesVectorsBeforeStages(t *testing.T) {
	for _, attempt := range []int{1, 2} {
		t.Run(strconv.Itoa(attempt), func(t *testing.T) {
			s := leaseStore(1)
			s.jobs[0].Attempts = attempt
			w := leaseWorker(s, leaseTestStage{func(context.Context) error { s.events <- "stage"; return nil }}, 1)
			w.vectors = leaseVectorRecorder{s.events}
			w.Start(t.Context())
			defer w.Shutdown()
			leaseEvent(t, s, "delete")
			leaseEvent(t, s, "stage")
			leaseEvent(t, s, "completed")
		})
	}
}
func TestTerminalCleanupPrecedesFailure(t *testing.T) {
	s := leaseStore(1)
	s.jobs[0].Attempts = 2
	w := leaseWorker(s, leaseTestStage{func(context.Context) error { s.events <- "stage"; return domain.Permanent(errors.New("invalid")) }}, 1)
	w.vectors = leaseVectorRecorder{s.events}
	w.Start(t.Context())
	defer w.Shutdown()
	leaseEvent(t, s, "delete")
	leaseEvent(t, s, "stage")
	leaseEvent(t, s, "delete")
	leaseEvent(t, s, "failed")
}
func TestTerminalFailureDoesNotDeleteAfterLeaseLoss(t *testing.T) {
	s := leaseStore(1)
	s.jobs[0].Attempts = 2
	w := leaseWorker(s, leaseTestStage{func(context.Context) error {
		s.mu.Lock()
		s.heartbeat = false
		s.mu.Unlock()
		return domain.Permanent(errors.New("invalid"))
	}}, 1)
	w.vectors = leaseVectorRecorder{s.events}
	w.process(t.Context(), s.jobs[0])
	leaseEvent(t, s, "delete")
	require.Empty(t, s.events)
}

type leaseRenewalStore struct {
	*leaseTestStore
	renewals atomic.Int32
	renewed  chan struct{}
}

func (s *leaseRenewalStore) HeartbeatJob(context.Context, domain.Job, time.Duration) (bool, error) {
	if s.renewals.Add(1) == 3 {
		close(s.renewed)
	}
	return true, nil
}

type leaseBlockingVectors struct {
	calls   int
	renewed <-chan struct{}
}

func (*leaseBlockingVectors) DeleteAllVectorsInNamespace(context.Context, string) error { return nil }
func (v *leaseBlockingVectors) DeleteByDocID(ctx context.Context, _ string) error {
	v.calls++
	if v.calls == 1 {
		return nil
	}
	select {
	case <-v.renewed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Second):
		return errors.New("lease was not renewed during cleanup")
	}
}
func TestTerminalCleanupKeepsHeartbeatActive(t *testing.T) {
	s := &leaseRenewalStore{leaseTestStore: leaseStore(1), renewed: make(chan struct{})}
	vectors := &leaseBlockingVectors{renewed: s.renewed}
	w := New(s, []pipeline.Stage{leaseTestStage{func(context.Context) error { return domain.Permanent(errors.New("invalid")) }}}, vectors, Options{Workers: 1, MaxAttempts: 5, Lease: 30 * time.Millisecond, JobTimeout: 5 * time.Millisecond, PollInterval: time.Millisecond, Owner: "renewal"})
	w.Start(t.Context())
	defer w.Shutdown()
	leaseEvent(t, s.leaseTestStore, "failed")
	w.Shutdown()
	require.GreaterOrEqual(t, s.renewals.Load(), int32(3))
	require.Equal(t, 2, vectors.calls)
}

type visibilityEmbedStage struct{}

func (visibilityEmbedStage) ID() pipeline.StageID { return pipeline.StageEmbed }
func (visibilityEmbedStage) Run(_ context.Context, ic *pipeline.IngestContext) error {
	ic.Chunks = []domain.Chunk{{ID: "chunk", DocumentID: ic.DocumentID, Embedding: []float32{1, 0}}}
	return nil
}

type laggingVectorStore struct {
	domain.VectorStore
	probed chan struct{}
}

func (*laggingVectorStore) Upsert(context.Context, string, []domain.Chunk) error { return nil }
func (s *laggingVectorStore) QueryFiltered(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
	select {
	case s.probed <- struct{}{}:
	default:
	}
	return nil, nil
}

func TestShutdownCompletesJobWaitingForVisibility(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := leaseStore(1)
		vs := &laggingVectorStore{probed: make(chan struct{}, 1)}
		stages := []pipeline.Stage{visibilityEmbedStage{}, pipeline.NewPersistStage(vs, pipeline.WithVisibilityWait(30*time.Second))}
		w := New(s, stages, nil, Options{Workers: 1, MaxAttempts: 5, Lease: 2 * time.Minute, JobTimeout: 15 * time.Minute, PollInterval: time.Second, Owner: "test"})
		w.Start(t.Context())
		<-vs.probed
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		start := time.Now()
		require.NoError(t, w.ShutdownContext(ctx))
		require.Zero(t, time.Since(start))
		require.Equal(t, "completed", <-s.events)
	})
}

type collectionStage struct{ got chan string }

func (collectionStage) ID() pipeline.StageID { return pipeline.StageChunk }
func (s collectionStage) Run(_ context.Context, ic *pipeline.IngestContext) error {
	s.got <- ic.Collection
	return nil
}
func TestProcessorPassesCollection(t *testing.T) {
	s := leaseStore(1)
	s.jobs[0].Collection = "archive"
	got := make(chan string, 1)
	w := leaseWorker(s, collectionStage{got: got}, 1)
	w.Start(t.Context())
	defer w.Shutdown()
	leaseEvent(t, s, "completed")
	require.Equal(t, "archive", <-got)
}

func TestManifestedWorkerTargetsOnlyFencedRevision(t *testing.T) {
	s := leaseStore(2)
	s.jobs[0].Incarnation = uuid.NewString()
	s.jobs[0].Attempts = 1
	s.jobs[0].LeaseToken = "lease-1"
	s.jobs[1].Incarnation = s.jobs[0].Incarnation
	s.jobs[1].Attempts = 2
	s.jobs[1].LeaseToken = "lease-2"

	vecs := &revisionVectorRecorder{activeChunks: map[string]domain.Chunk{}}
	rev2 := revisionIDForAttempt(s.jobs[1], "test")
	vecs.activeChunks[domain.FormatRevisionVectorID("doc", rev2, 0)] = domain.Chunk{
		ID: "pub-0", DocumentID: "doc", Index: 0, RevisionID: rev2,
	}

	// Delayed attempt 1 fails permanently (exercising both pre-upsert at run() and terminal cleanup in process()).
	w := New(s, []pipeline.Stage{leaseTestStage{func(_ context.Context) error {
		return domain.Permanent(errors.New("delayed attempt 1 failed"))
	}}}, vecs, Options{
		HostedVectorManifest: true, Workers: 1, MaxAttempts: 5,
		Lease: 50 * time.Millisecond, JobTimeout: time.Second, PollInterval: time.Millisecond, Owner: "test",
	})
	w.process(t.Context(), s.jobs[0])
	leaseEvent(t, s, "failed")

	vecs.mu.Lock()
	defer vecs.mu.Unlock()
	require.Zero(t, vecs.docDeletes, "manifested mode must never call document-wide DeleteByDocID")
	want := revisionVectorDelete{
		documentID: string(s.jobs[0].Document.ID),
		revisionID: revisionIDForAttempt(s.jobs[0], "test"),
	}
	require.Equal(t, []revisionVectorDelete{want, want}, vecs.revDeletes, "both pre-upsert and terminal cleanup must target attempt 1's fenced revision")
	require.Contains(t, vecs.activeChunks, domain.FormatRevisionVectorID("doc", rev2, 0), "attempt 2's active vectors must remain intact")
}

type manifestRejectingStore struct {
	*leaseTestStore
	persistCalls int
	persistErr   error
}

func (s *manifestRejectingStore) PersistVectorManifest(context.Context, domain.Job, domain.VectorManifest) error {
	s.persistCalls++
	return s.persistErr
}
func (*manifestRejectingStore) GetVectorManifest(context.Context, string) (*domain.VectorManifest, error) {
	return nil, nil
}
func (*manifestRejectingStore) ListDocumentVectorManifests(context.Context, domain.DocumentID, string) ([]domain.VectorManifest, error) {
	return nil, nil
}
func (*manifestRejectingStore) ClaimVectorCleanup(context.Context, string, time.Duration, time.Duration) (*domain.VectorCleanupTask, error) {
	return nil, nil
}
func (*manifestRejectingStore) RenewVectorCleanup(context.Context, domain.VectorCleanupTask, time.Duration) (bool, error) {
	return true, nil
}
func (*manifestRejectingStore) AdvanceVectorCleanup(context.Context, domain.VectorCleanupTask, int) (bool, error) {
	return true, nil
}
func (*manifestRejectingStore) DeferVectorCleanup(context.Context, domain.VectorCleanupTask, string, time.Duration, int, string) (bool, error) {
	return true, nil
}
func (*manifestRejectingStore) CompleteVectorCleanup(context.Context, domain.VectorCleanupTask) (bool, error) {
	return true, nil
}
func (*manifestRejectingStore) ReleaseVectorCleanup(context.Context, domain.VectorCleanupTask) error {
	return nil
}

type upsertSpyStore struct {
	domain.VectorStore
	upserts int
	write   func([]domain.Chunk) error
}

func (u *upsertSpyStore) Upsert(_ context.Context, _ string, chunks []domain.Chunk) error {
	u.upserts++
	if u.write != nil {
		return u.write(chunks)
	}
	return nil
}

func TestHostedManifestUsesClaimedIncarnation(t *testing.T) {
	base := leaseStore(1)
	s := &manifestRejectingStore{leaseTestStore: base}
	vecs := &revisionVectorRecorder{}
	job := base.jobs[0]
	job.Incarnation, job.Tenant = uuid.NewString(), uuid.NewString()
	job.ScopedVectors = true
	job.LeaseToken = uuid.NewString()
	owner := "test"
	var wantDeletes []revisionVectorDelete
	spy := &upsertSpyStore{write: func(chunks []domain.Chunk) error {
		require.Equal(t, len(wantDeletes)+1, s.persistCalls, "intent precedes each upsert")
		require.Len(t, chunks, 1)
		chunk := chunks[0]
		revision := revisionIDForAttempt(job, owner)
		require.Equal(t, job.Incarnation, chunk.VectorIncarnation)
		require.Equal(t, revision, chunk.RevisionID)
		require.Equal(t, domain.FormatRevisionVectorID(job.Document.ID, revision, 0), chunk.VectorID)
		require.Equal(t, job.Tenant, chunk.Metadata[domain.VectorTenantKey])
		require.NotContains(t, chunk.Metadata, domain.VectorRevisionKey)
		require.NotContains(t, chunk.Metadata, domain.VectorAttemptKey)
		wantDeletes = append(wantDeletes, revisionVectorDelete{documentID: string(job.Document.ID), revisionID: revision})
		if len(wantDeletes) == 1 {
			return errors.New("provider unavailable")
		}
		return nil
	}}
	w := New(s, []pipeline.Stage{visibilityEmbedStage{}, pipeline.NewPersistStage(spy)}, vecs, Options{
		HostedVectorManifest: true, MaxAttempts: 5, Lease: time.Second, JobTimeout: time.Second, Owner: owner,
	})
	ctx := domain.WithVectorIncarnation(t.Context(), uuid.NewString())
	w.process(ctx, job)
	leaseEvent(t, base, "retry")
	job.Attempts++
	job.LeaseToken = uuid.NewString()
	w.process(ctx, job)
	leaseEvent(t, base, "completed")
	require.Len(t, wantDeletes, 2)
	require.NotEqual(t, wantDeletes[0].revisionID, wantDeletes[1].revisionID)
	require.Equal(t, wantDeletes, vecs.revDeletes)
	require.Zero(t, vecs.docDeletes)
}

func TestHostedManifestInvalidClaimAndCancellationDoNotDispatch(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(strconv.FormatBool(canceled), func(t *testing.T) {
			base := leaseStore(1)
			store := &manifestRejectingStore{leaseTestStore: base}
			vectors := &revisionVectorRecorder{}
			spy := &upsertSpyStore{}
			job := base.jobs[0]
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := "failed"
			if canceled {
				job.Incarnation = uuid.NewString()
				cancel()
				want = "released"
			}
			stages := []pipeline.Stage{visibilityEmbedStage{}, pipeline.NewPersistStage(spy)}
			w := New(store, stages, vectors, Options{
				HostedVectorManifest: true, MaxAttempts: 1, Lease: time.Second, JobTimeout: time.Second, Owner: "test",
			})
			w.process(ctx, job)
			leaseEvent(t, base, want)
			require.Zero(t, store.persistCalls)
			require.Zero(t, spy.upserts)
			require.Empty(t, vectors.revDeletes)
			require.Zero(t, vectors.docDeletes)
		})
	}
}

func TestManifestedWorkerAbortsUpsertWhenManifestLeaseCheckFails(t *testing.T) {
	t.Run("transient manifest error retries without upsert", func(t *testing.T) {
		base := leaseStore(1)
		base.jobs[0].Incarnation = uuid.NewString()
		s := &manifestRejectingStore{leaseTestStore: base, persistErr: errors.New("db unavailable")}
		vecs := &revisionVectorRecorder{activeChunks: map[string]domain.Chunk{}}
		spy := &upsertSpyStore{}
		stages := []pipeline.Stage{visibilityEmbedStage{}, pipeline.NewPersistStage(spy)}
		w := New(s, stages, vecs, Options{
			HostedVectorManifest: true, Workers: 1, MaxAttempts: 5,
			Lease: 50 * time.Millisecond, JobTimeout: time.Second, PollInterval: time.Millisecond, Owner: "test",
		})
		w.process(t.Context(), s.jobs[0])
		leaseEvent(t, base, "retry")
		require.Equal(t, 1, s.persistCalls)
		require.Zero(t, spy.upserts, "no upsert may run after manifest failure")
	})

	t.Run("lost lease aborts cleanly without upsert or retry", func(t *testing.T) {
		base := leaseStore(1)
		base.jobs[0].Incarnation = uuid.NewString()
		s := &manifestRejectingStore{leaseTestStore: base, persistErr: domain.ErrJobNotOwned}
		vecs := &revisionVectorRecorder{activeChunks: map[string]domain.Chunk{}}
		spy := &upsertSpyStore{}
		stages := []pipeline.Stage{visibilityEmbedStage{}, pipeline.NewPersistStage(spy)}
		w := New(s, stages, vecs, Options{
			HostedVectorManifest: true, Workers: 1, MaxAttempts: 5,
			Lease: 50 * time.Millisecond, JobTimeout: time.Second, PollInterval: time.Millisecond, Owner: "test",
		})
		w.process(t.Context(), s.jobs[0])
		require.Equal(t, 1, s.persistCalls)
		require.Zero(t, spy.upserts, "no upsert may run after manifest lease rejection")
		select {
		case ev := <-base.events:
			t.Fatalf("unexpected job state transition %q on ErrJobNotOwned", ev)
		default:
		}
	})
}

type modeFencedLeaseStore struct {
	*profileCapableStore
	completed  domain.Job
	released   domain.Job
	exists     bool
	releaseErr error
}

func (s *modeFencedLeaseStore) CompleteJob(_ context.Context, job domain.Job, _ []domain.Chunk, _ domain.IndexObservation) (bool, error) {
	s.completed = job
	return false, fmt.Errorf("publish: %w", domain.ErrAuthzModeMismatch)
}

func (s *modeFencedLeaseStore) ReleaseJob(ctx context.Context, job domain.Job) error {
	s.released = job
	if s.releaseErr != nil {
		return s.releaseErr
	}
	return s.leaseTestStore.ReleaseJob(ctx, job)
}

func (s *modeFencedLeaseStore) DocumentExists(context.Context, domain.DocumentID) (bool, error) {
	return s.exists, nil
}

func TestCorpusModeMismatchReleasesOwnedLease(t *testing.T) {
	for _, tc := range []struct {
		name            string
		hosted, deleted bool
		attempt         int
		releaseErr      error
	}{
		{name: "retry budget remains", attempt: 1},
		{name: "retry budget exhausted", attempt: 5},
		{name: "lease moved", attempt: 5, releaseErr: domain.ErrJobNotOwned},
		{name: "hosted document remains", hosted: true, attempt: 5},
		{name: "hosted document deleted", hosted: true, deleted: true, attempt: 5, releaseErr: domain.ErrJobNotOwned},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := leaseStore(1)
			store := &modeFencedLeaseStore{profileCapableStore: newProfileCapableStore(base), exists: !tc.deleted, releaseErr: tc.releaseErr}
			queued := &base.jobs[0]
			queued.Document.ID = domain.DocumentID(uuid.NewString())
			queued.Incarnation, queued.LeaseToken, queued.Attempts = uuid.NewString(), uuid.NewString(), tc.attempt

			var vectors domain.VectorDeleter
			var recorder *revisionVectorRecorder
			opts := Options{HostedVectorManifest: tc.hosted, Owner: "test", MaxAttempts: 5, Lease: time.Minute, JobTimeout: time.Second}
			if tc.hosted {
				queued.PDF = &domain.PDFExtraction{Profile: domain.LayoutPDFProfile, Coverage: domain.PDFComplete}
				opts.OCR = analyzerOnlyOCR{}
				recorder = &revisionVectorRecorder{activeChunks: make(map[string]domain.Chunk)}
				vectors = recorder
			}
			w := New(store, []pipeline.Stage{leaseTestStage{func(context.Context) error { return nil }}}, vectors, opts)
			claimed, err := w.claim(t.Context())
			require.NoError(t, err)
			require.NotNil(t, claimed)
			require.Equal(t, []profileClaimCall{{method: "profiles", layout: tc.hosted}}, store.calls)
			job := *claimed
			w.process(t.Context(), job)
			require.Equal(t, job.LeaseToken, store.completed.LeaseToken)
			require.Equal(t, store.completed, store.released, "release must retain the exact attempt identity")
			if tc.releaseErr == nil {
				leaseEvent(t, base, "released")
			}
			require.Empty(t, base.events, "fenced publication must neither retry nor fail the job")
			if tc.hosted {
				deletion := revisionVectorDelete{documentID: string(job.Document.ID), revisionID: revisionIDForAttempt(job, "test")}
				want := []revisionVectorDelete{deletion}
				if tc.deleted {
					want = append(want, deletion)
				}
				require.Equal(t, want, recorder.revDeletes)
				require.Zero(t, recorder.docDeletes)
			}
		})
	}
}
