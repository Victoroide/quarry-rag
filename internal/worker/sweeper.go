package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/tracing"
)

// SweepOptions bounds leased sweeping of retired hosted vector revisions.
type SweepOptions struct {
	Owner            string
	Lease            time.Duration
	PollInterval     time.Duration
	PageSize         int
	Budget           int
	ConvergenceDelay time.Duration
	// DrainDelay is the minimum time a retired revision waits before cleanup, so
	// a write that was in flight when it retired has finished.
	DrainDelay     time.Duration
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	HasWriteFence  bool
	Metrics        *metrics.Service
}

// SweepReport summarizes one bounded sweep pass over retired revision tombstones.
type SweepReport struct {
	TasksClaimed      int    `json:"tasks_claimed"`
	TasksCompleted    int    `json:"tasks_completed"`
	TasksDeferred     int    `json:"tasks_deferred"`
	TasksUnresolved   int    `json:"tasks_unresolved"`
	VectorsDeleted    int    `json:"vectors_deleted"`
	UnresolvedVectors int    `json:"unresolved_vectors"`
	BudgetRemaining   int    `json:"budget_remaining"`
	LastError         string `json:"last_error,omitempty"`
}

// Sweeper claims retired hosted vector tombstones under leases, deletes exact
// external IDs in bounded pages, verifies convergence, and backs off on outages.
type Sweeper struct {
	store   domain.VectorManifestStore
	vectors domain.VectorPresenceChecker
	opts    SweepOptions
	stop    chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
	cancel  context.CancelCauseFunc
	started bool
	stopped bool
}

// NewSweeper constructs a leased retired-vector sweeper.
func NewSweeper(store domain.VectorManifestStore, vectors domain.VectorPresenceChecker, opts SweepOptions) *Sweeper {
	if opts.Metrics == nil {
		opts.Metrics = metrics.Default
	}
	if opts.Lease <= 0 {
		opts.Lease = 2 * time.Minute
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 10 * time.Second
	}
	if opts.PageSize < 1 {
		opts.PageSize = 100
	}
	if opts.PageSize > 1000 {
		opts.PageSize = 1000
	}
	if opts.Budget < 1 {
		opts.Budget = 1000
	}
	if opts.RetryBaseDelay <= 0 {
		opts.RetryBaseDelay = 5 * time.Second
	}
	if opts.RetryMaxDelay <= 0 {
		opts.RetryMaxDelay = 5 * time.Minute
	}
	return &Sweeper{
		store:   store,
		vectors: vectors,
		opts:    opts,
		stop:    make(chan struct{}),
	}
}

// Start launches the periodic sweep loop once.
func (s *Sweeper) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.stopped {
		return
	}
	s.started = true
	loopCtx, cancel := context.WithCancelCause(ctx)
	s.cancel = cancel
	s.wg.Go(func() { s.loop(loopCtx) })
}

// ShutdownContext stops future sweeps and waits for any active pass until ctx expires.
func (s *Sweeper) ShutdownContext(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		close(s.stop)
	}
	cancel := s.cancel
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		if cancel != nil {
			cancel(errShutdown)
		}
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel(errShutdown)
		}
		<-done
		return ctx.Err()
	}
}

func (s *Sweeper) loop(ctx context.Context) {
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()
	for {
		if _, err := s.SweepOnce(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "sweep retired hosted vectors", "error", tracing.ErrorForTelemetry(ctx, err))
		}
		select {
		case <-s.stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// SweepOnce processes drained, ready tombstones within the configured page budget.
func (s *Sweeper) SweepOnce(ctx context.Context) (SweepReport, error) {
	remaining := s.opts.Budget
	report := SweepReport{BudgetRemaining: remaining}
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			report.BudgetRemaining = remaining
			return report, err
		}
		task, err := s.store.ClaimVectorCleanup(ctx, s.opts.Owner, s.opts.Lease, s.opts.DrainDelay)
		if err != nil {
			report.BudgetRemaining = remaining
			return report, err
		}
		if task == nil {
			break
		}
		report.TasksClaimed++
		keepGoing, taskErr := s.runTask(ctx, *task, &remaining, &report)
		if taskErr != nil {
			report.BudgetRemaining = remaining
			return report, taskErr
		}
		if !keepGoing {
			break
		}
	}
	report.BudgetRemaining = remaining
	return report, nil
}

func (s *Sweeper) runTask(parent context.Context, task domain.VectorCleanupTask, remaining *int, report *SweepReport) (bool, error) {
	taskCtx, cancelTask := context.WithCancelCause(parent)
	defer cancelTask(nil)
	leaseCtx, cancelLease := context.WithCancelCause(parent)
	hbDone, hbStop := make(chan struct{}), make(chan struct{})
	defer func() { close(hbStop); cancelLease(nil); <-hbDone }()
	go func() {
		defer close(hbDone)
		interval := max(s.opts.Lease/3, time.Millisecond)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-hbStop:
				return
			case <-ticker.C:
				ok, err := s.store.RenewVectorCleanup(leaseCtx, task, s.opts.Lease)
				if err != nil {
					logCleanupError(leaseCtx, "renew vector cleanup lease", task, err)
				}
				if err != nil || !ok {
					cancelLease(errLeaseLost)
					cancelTask(errLeaseLost)
					return
				}
			}
		}
	}()

	for task.NextOffset < len(task.VectorIDs) && *remaining > 0 {
		if err := taskCtx.Err(); err != nil {
			return s.handleTaskInterruption(parent, taskCtx, task)
		}
		end := min(len(task.VectorIDs), task.NextOffset+min(s.opts.PageSize, *remaining))
		page := task.VectorIDs[task.NextOffset:end]
		if err := s.vectors.DeleteVectorIDs(taskCtx, task.VectorNamespace, page); err != nil {
			if errors.Is(context.Cause(taskCtx), errLeaseLost) {
				s.recordCleanupMetric("lease_lost")
				return false, nil
			}
			if parent.Err() != nil {
				s.releaseTask(parent, task)
				return false, parent.Err()
			}
			s.deferOnError(parent, task, task.State, err.Error(), report)
			return false, nil
		}
		ok, err := s.store.AdvanceVectorCleanup(taskCtx, task, end)
		if err != nil {
			if errors.Is(context.Cause(taskCtx), errLeaseLost) {
				s.recordCleanupMetric("lease_lost")
				return false, nil
			}
			s.releaseTask(parent, task)
			return false, err
		}
		if !ok {
			s.recordCleanupMetric("lease_lost")
			return false, nil
		}
		task.NextOffset = end
		deleted := len(page)
		*remaining -= deleted
		report.VectorsDeleted += deleted
	}

	if task.NextOffset < len(task.VectorIDs) {
		s.releaseTask(parent, task)
		return false, nil
	}

	if task.State == "pending" && s.opts.ConvergenceDelay > 0 {
		ok, err := s.store.DeferVectorCleanup(taskCtx, task, "verifying", s.opts.ConvergenceDelay, 0, "")
		if err != nil {
			if errors.Is(context.Cause(taskCtx), errLeaseLost) {
				s.recordCleanupMetric("lease_lost")
				return false, nil
			}
			s.releaseTask(parent, task)
			return false, err
		}
		if !ok {
			s.recordCleanupMetric("lease_lost")
			return false, nil
		}
		report.TasksDeferred++
		s.recordCleanupMetric("deferred")
		return true, nil
	}

	present, checkedAll, checkErr := s.findResidualVectors(taskCtx, task)
	if checkErr != nil {
		if errors.Is(context.Cause(taskCtx), errLeaseLost) {
			s.recordCleanupMetric("lease_lost")
			return false, nil
		}
		if parent.Err() != nil {
			s.releaseTask(parent, task)
			return false, parent.Err()
		}
		s.deferOnError(parent, task, task.State, checkErr.Error(), report)
		return false, nil
	}

	if len(present) > 0 {
		for start := 0; start < len(present) && *remaining > 0; {
			end := min(len(present), start+min(s.opts.PageSize, *remaining))
			batch := present[start:end]
			if delErr := s.vectors.DeleteVectorIDs(taskCtx, task.VectorNamespace, batch); delErr != nil {
				break
			}
			*remaining -= len(batch)
			report.VectorsDeleted += len(batch)
			start = end
		}
		msg := fmt.Sprintf("unresolved residue: %d vector(s) still present for revision %s", len(present), task.RevisionID)
		delay := sweepBackoff(task.Attempts, s.opts.RetryBaseDelay, s.opts.RetryMaxDelay)
		ok, err := s.store.DeferVectorCleanup(taskCtx, task, "unresolved", delay, 0, msg)
		if err != nil {
			if errors.Is(context.Cause(taskCtx), errLeaseLost) {
				s.recordCleanupMetric("lease_lost")
				return false, nil
			}
			return false, err
		}
		if !ok {
			s.recordCleanupMetric("lease_lost")
			return false, nil
		}
		attrs := []any{"residue_vectors", len(present)}
		if !tracing.RedactIdentifiers(parent) {
			attrs = append(attrs, "document_id", task.DocumentID, "revision_id", task.RevisionID)
		}
		slog.WarnContext(parent, "hosted vector cleanup residue detected", attrs...)
		report.TasksUnresolved++
		report.UnresolvedVectors += len(present)
		report.LastError = msg
		s.recordCleanupMetric("unresolved")
		return true, nil
	}

	if !checkedAll {
		s.releaseTask(parent, task)
		return false, nil
	}

	if !s.opts.HasWriteFence && task.State == "unresolved" && task.ConvergenceChecks < 1 && s.opts.ConvergenceDelay > 0 {
		ok, err := s.store.DeferVectorCleanup(taskCtx, task, "verifying", s.opts.ConvergenceDelay, task.ConvergenceChecks+1, "")
		if err != nil {
			if errors.Is(context.Cause(taskCtx), errLeaseLost) {
				s.recordCleanupMetric("lease_lost")
				return false, nil
			}
			return false, err
		}
		if !ok {
			s.recordCleanupMetric("lease_lost")
			return false, nil
		}
		report.TasksDeferred++
		s.recordCleanupMetric("deferred")
		return true, nil
	}

	ok, err := s.store.CompleteVectorCleanup(taskCtx, task)
	if err != nil {
		if errors.Is(context.Cause(taskCtx), errLeaseLost) {
			s.recordCleanupMetric("lease_lost")
			return false, nil
		}
		return false, err
	}
	if !ok {
		s.recordCleanupMetric("lease_lost")
		return false, nil
	}
	report.TasksCompleted++
	s.recordCleanupMetric("completed")
	return true, nil
}

func (s *Sweeper) findResidualVectors(ctx context.Context, task domain.VectorCleanupTask) (present []string, checkedAll bool, err error) {
	seen := make(map[string]struct{})
	for start := 0; start < len(task.VectorIDs); start += s.opts.PageSize {
		end := min(len(task.VectorIDs), start+s.opts.PageSize)
		found, fetchErr := s.vectors.FetchPresentVectorIDs(ctx, task.VectorNamespace, task.VectorIDs[start:end])
		if fetchErr != nil {
			return present, false, fetchErr
		}
		for _, id := range found {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				present = append(present, id)
			}
		}
	}
	if task.IDPrefix != "" {
		token := ""
		for range 100 {
			listed, nextToken, supported, listErr := s.vectors.ListPrefixVectorIDs(ctx, task.VectorNamespace, task.IDPrefix, s.opts.PageSize, token)
			if listErr != nil {
				return present, false, listErr
			}
			if !supported {
				break
			}
			for _, id := range listed {
				if _, ok := seen[id]; !ok {
					seen[id] = struct{}{}
					present = append(present, id)
				}
			}
			if nextToken == "" {
				break
			}
			token = nextToken
		}
	}
	return present, true, nil
}

func (s *Sweeper) handleTaskInterruption(parent, taskCtx context.Context, task domain.VectorCleanupTask) (bool, error) {
	if errors.Is(context.Cause(taskCtx), errLeaseLost) {
		s.recordCleanupMetric("lease_lost")
		return false, nil
	}
	s.releaseTask(parent, task)
	return false, parent.Err()
}

func (s *Sweeper) releaseTask(parent context.Context, task domain.VectorCleanupTask) {
	relCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	if err := s.store.ReleaseVectorCleanup(relCtx, task); err != nil {
		logCleanupError(relCtx, "release vector cleanup lease", task, err)
	}
}

func (s *Sweeper) deferOnError(parent context.Context, task domain.VectorCleanupTask, state, message string, report *SweepReport) {
	delay := sweepBackoff(task.Attempts, s.opts.RetryBaseDelay, s.opts.RetryMaxDelay)
	deferCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	ok, err := s.store.DeferVectorCleanup(deferCtx, task, state, delay, 0, message)
	if err != nil {
		logCleanupError(deferCtx, "defer vector cleanup", task, err)
		return
	}
	if !ok {
		s.recordCleanupMetric("lease_lost")
		return
	}
	report.TasksDeferred++
	report.LastError = message
	s.recordCleanupMetric("failed")
}

func (s *Sweeper) recordCleanupMetric(result string) {
	if s.opts.Metrics != nil && s.opts.Metrics.HostedVectorCleanups != nil {
		s.opts.Metrics.HostedVectorCleanups.WithLabelValues(result).Inc()
	}
}

func sweepBackoff(attempts int, base, maxDelay time.Duration) time.Duration {
	delay := base
	for i := 0; i < attempts && delay < maxDelay; i++ {
		delay *= 2
	}
	delay = min(delay, maxDelay)
	return delay + time.Duration(rand.Int64N(int64(time.Second)+1)) // #nosec G404 -- Retry jitter does not require cryptographic randomness.
}

func logCleanupError(ctx context.Context, message string, task domain.VectorCleanupTask, err error) {
	if tracing.RedactIdentifiers(ctx) {
		slog.ErrorContext(ctx, message, "error", tracing.ErrorForTelemetry(ctx, err))
		return
	}
	slog.ErrorContext(ctx, message, "revision_id", task.RevisionID, "error", err)
}
