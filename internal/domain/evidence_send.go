package domain

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrEvidenceCheck marks a provider send blocked by the authoritative check.
var ErrEvidenceCheck = errors.New("provider evidence check failed")

type evidenceCheckKey struct{}

type evidenceCheck struct {
	check func(context.Context) error
	mu    sync.Mutex
	err   error
}

// WithEvidenceCheck carries a request-scoped check to content-bearing sends.
func WithEvidenceCheck(ctx context.Context, check func(context.Context) error) context.Context {
	return context.WithValue(ctx, evidenceCheckKey{}, &evidenceCheck{check: check})
}

// HasEvidenceCheck reports whether content sends require a visibility check.
func HasEvidenceCheck(ctx context.Context) bool {
	_, ok := ctx.Value(evidenceCheckKey{}).(*evidenceCheck)
	return ok
}

// CheckEvidenceBeforeSend rechecks evidence and keeps subsequent sends blocked
// after a rejection or a failed visibility read.
func CheckEvidenceBeforeSend(ctx context.Context) error {
	guard, ok := ctx.Value(evidenceCheckKey{}).(*evidenceCheck)
	if !ok {
		return nil
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.err == nil {
		if err := guard.check(ctx); err != nil {
			guard.err = fmt.Errorf("%w: %w", ErrEvidenceCheck, err)
		}
	}
	return guard.err
}

// RecordedEvidenceCheckError returns a prior rejection without checking again.
func RecordedEvidenceCheckError(ctx context.Context) error {
	guard, ok := ctx.Value(evidenceCheckKey{}).(*evidenceCheck)
	if !ok {
		return nil
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	return guard.err
}
