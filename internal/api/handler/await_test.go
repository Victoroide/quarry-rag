package handler

import (
	"context"
	"testing"
	"time"
)

// testDeadlineReserve is the time kept free before the test binary's deadline,
// so that a wait that never completes fails with a message and still lets
// cleanups run instead of ending in the harness timeout panic.
const testDeadlineReserve = time.Second

// testBudget returns a context that ends shortly before the test binary's
// deadline. Without a deadline it never ends before the test does.
func testBudget(t *testing.T) context.Context {
	t.Helper()
	ctx := t.Context()
	if deadline, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-testDeadlineReserve))
		t.Cleanup(cancel)
	}
	return ctx
}

// awaitSignal returns the next value from ch, or fails the test when the
// test deadline is reached first. Waiting on the event itself, not on a fixed
// duration, keeps tests independent of scheduler load.
func awaitSignal[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-testBudget(t).Done():
		t.Fatalf("test deadline reached while waiting for %s", what)
		var zero T
		return zero
	}
}
