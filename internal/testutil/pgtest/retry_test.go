package pgtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go/wait"
)

// stoppedReaper is a wait target whose container is in a fixed, non-running
// state, which is what the library finds when it reuses a reaper that Docker
// is shutting down.
type stoppedReaper struct {
	wait.StrategyTarget
	state container.State
}

func (r stoppedReaper) State(context.Context) (*container.State, error) {
	return &r.state, nil
}

func (r stoppedReaper) Logs(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

// reuseFailure reproduces the error the library returns from a start that
// reuses a reaper in the given state. The inner message comes from the real
// wait strategy the library uses, so a change in the library's wording fails
// this test instead of silently disabling the retry.
func reuseFailure(t *testing.T, state container.State) error {
	t.Helper()
	strategy := wait.ForAll(
		wait.ForLog("Started"),
		wait.ForExposedPort().WithPollInterval(time.Millisecond).SkipInternalCheck(),
	)
	waitErr := strategy.WaitUntilReady(t.Context(), stoppedReaper{state: state})
	if waitErr == nil {
		t.Fatal("waiting on a stopped reaper succeeded")
	}
	return fmt.Errorf("create container: reaper: from container %q: wait for reaper %s: %w",
		"0123abcd", "0123abcd", waitErr)
}

func TestReaperShuttingDownRecognizesReuseOfAStoppingReaper(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "removing as reported",
			err: errors.New(`create container: reaper: from container "0123abcd": ` +
				`wait for reaper 0123abcd: unexpected container status "removing"`),
			want: true,
		},
		{
			name: "removing from the wait strategy",
			err:  reuseFailure(t, container.State{Status: container.StateRemoving}),
			want: true,
		},
		{
			name: "exited from the wait strategy",
			err:  reuseFailure(t, container.State{Status: container.StateExited}),
			want: true,
		},
		{
			name: "wrapped again by the caller",
			err:  fmt.Errorf("start postgres container: %w", reuseFailure(t, container.State{Status: container.StateRemoving})),
			want: true,
		},
		{
			name: "database container removed while waiting",
			err:  errors.New(`wait until ready: unexpected container status "removing"`),
		},
		{
			name: "reaper that cannot be created",
			err:  errors.New(`create container: reaper: new reaper: run container: pull access denied`),
		},
		{
			name: "reaper unreachable",
			err:  errors.New(`create container: reaper: connect: dial reaper 127.0.0.1:1: connection refused`),
		},
		{
			name: "database container exited",
			err:  errors.New("wait until ready: container exited with code 1"),
		},
		{
			name: "no error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reaperShuttingDown(tt.err); got != tt.want {
				t.Fatalf("reaperShuttingDown(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

var quickRetry = retryPolicy{window: time.Second, first: time.Millisecond, limit: 4 * time.Millisecond}

func TestRetryRunsOnceOnSuccess(t *testing.T) {
	var calls int
	err := quickRetry.run(t.Context(), func() error {
		calls++
		return nil
	})
	if err != nil || calls != 1 {
		t.Fatalf("run() = %v after %d calls, want nil after 1", err, calls)
	}
}

func TestRetryWaitsOutAShuttingDownReaper(t *testing.T) {
	shutdown := errors.New(`reaper: from container "0123abcd": wait for reaper 0123abcd: unexpected container status "removing"`)
	var calls int
	err := quickRetry.run(t.Context(), func() error {
		calls++
		if calls < 4 {
			return shutdown
		}
		return nil
	})
	if err != nil || calls != 4 {
		t.Fatalf("run() = %v after %d calls, want nil after 4", err, calls)
	}
}

func TestRetryReturnsOtherFailuresWithoutRepeating(t *testing.T) {
	failure := errors.New("pull access denied for postgres")
	var calls int
	err := quickRetry.run(t.Context(), func() error {
		calls++
		return failure
	})
	if !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("run() = %v after %d calls, want the original error after 1", err, calls)
	}
}

func TestRetryGivesUpWhenTheReaperStaysRemoved(t *testing.T) {
	shutdown := errors.New(`reaper: from container "0123abcd": wait for reaper 0123abcd: unexpected container status "removing"`)
	policy := retryPolicy{window: 30 * time.Millisecond, first: time.Millisecond, limit: 5 * time.Millisecond}
	var calls atomic.Int32
	begin := time.Now()
	err := policy.run(t.Context(), func() error {
		calls.Add(1)
		return shutdown
	})
	if !errors.Is(err, shutdown) {
		t.Fatalf("run() = %v, want the reaper error", err)
	}
	if calls.Load() < 2 {
		t.Fatalf("run() attempted %d times, want repeated attempts", calls.Load())
	}
	if elapsed := time.Since(begin); elapsed > 20*policy.window {
		t.Fatalf("run() took %s, want to stop near the %s window", elapsed, policy.window)
	}
}

func TestRetryStopsWhenTheContextEnds(t *testing.T) {
	shutdown := errors.New(`reaper: wait for reaper 0123abcd: unexpected container status "removing"`)
	ctx, cancel := context.WithCancel(t.Context())
	policy := retryPolicy{window: 3 * time.Hour, first: time.Hour, limit: time.Hour}
	var calls int
	err := policy.run(ctx, func() error {
		calls++
		cancel()
		return shutdown
	})
	if !errors.Is(err, shutdown) || !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("run() = %v after %d calls, want both causes after 1", err, calls)
	}
}
