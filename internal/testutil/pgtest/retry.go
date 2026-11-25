package pgtest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Wording of the library's wait strategy for a container that is not
// running, and of the step that reuses an existing reaper.
const (
	reaperReuseStep = "wait for reaper"
	reaperRemoving  = `unexpected container status "removing"`
	reaperExited    = "container exited with code"
)

// retryPolicy bounds the repetition of an operation: it stops after window,
// and the delay between attempts starts at first and doubles up to limit.
type retryPolicy struct {
	window time.Duration
	first  time.Duration
	limit  time.Duration
}

// run calls attempt until it succeeds, fails for another reason than a
// shutting-down reaper, the window ends or ctx is done. It returns the last
// error.
func (p retryPolicy) run(ctx context.Context, attempt func() error) error {
	deadline := time.Now().Add(p.window)
	delay := p.first
	for {
		err := attempt()
		if err == nil || !reaperShuttingDown(err) {
			return err
		}
		if !time.Now().Add(delay).Before(deadline) {
			return fmt.Errorf("reaper still shutting down after %s: %w", p.window, err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		delay = min(delay*2, p.limit)
	}
}

// reaperShuttingDown reports whether err is a start that reused a reaper
// which was exiting or being removed.
//
// The reaper exits once the process has held no container for the
// reconnection timeout, and Docker then removes it automatically. A container
// started while that removal is under way finds the reaper by its labels, and
// the library fails the start with a permanent error instead of waiting for
// the removal to end, although creating a new reaper afterwards succeeds.
func reaperShuttingDown(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, reaperReuseStep) &&
		(strings.Contains(msg, reaperRemoving) || strings.Contains(msg, reaperExited))
}
