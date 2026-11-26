package postgres

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

var _ domain.LegacyDeletionLimiter = (*Client)(nil)

// legacyBound returns how many guarded ID-only cleanups a replica runs at once.
// Each one holds a dedicated session for as long as its provider calls take, on
// top of the pool and of the writer sessions, so the default is a quarter of the
// pool and never less than one. A configured value is capped at the pool size,
// because a replica admits no more deletions than it has pooled connections.
func legacyBound(poolMax, configured int) int {
	poolMax = max(poolMax, 1)
	if configured <= 0 {
		return max(1, poolMax/4)
	}
	return min(configured, poolMax)
}

// legacySlots counts the guarded cleanups that are running. Acquiring never waits.
type legacySlots struct {
	mu     sync.Mutex
	active int
	limit  int
}

func (s *legacySlots) acquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active >= s.limit {
		return false
	}
	s.active++
	return true
}

func (s *legacySlots) release() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
}

func (s *legacySlots) setLimit(limit int) {
	s.mu.Lock()
	s.limit = limit
	s.mu.Unlock()
}

// SetLegacyDeletionConcurrency sets the number of guarded ID-only cleanups this
// replica runs at once. Zero or less selects the default, a quarter of the pool.
// Call it before serving: cleanups that already hold a slot keep it.
func (c *Client) SetLegacyDeletionConcurrency(n int) {
	c.legacy.setLimit(legacyBound(int(c.Pool.Config().MaxConns), n))
}

// heldLegacySlot marks a context whose caller already claimed a cleanup slot.
type heldLegacySlot struct {
	client   *Client
	released atomic.Bool
}

type heldLegacySlotKey struct{}

// ReserveLegacyDeletion implements domain.LegacyDeletionLimiter.
func (c *Client) ReserveLegacyDeletion(ctx context.Context) (context.Context, func(), error) {
	if !c.legacy.acquire() {
		return ctx, nil, domain.ErrTooManyLegacyDeletions
	}
	held := &heldLegacySlot{client: c}
	release := func() {
		if held.released.CompareAndSwap(false, true) {
			c.legacy.release()
		}
	}
	return context.WithValue(ctx, heldLegacySlotKey{}, held), release, nil
}

// claimLegacySlot takes a slot for one WithLegacyDeletion call unless the
// context already carries a live one. It returns the function that gives the
// slot back, or ErrTooManyLegacyDeletions without waiting.
func (c *Client) claimLegacySlot(ctx context.Context) (func(), error) {
	if held, ok := ctx.Value(heldLegacySlotKey{}).(*heldLegacySlot); ok && held.client == c && !held.released.Load() {
		return func() {}, nil
	}
	if !c.legacy.acquire() {
		return nil, domain.ErrTooManyLegacyDeletions
	}
	return c.legacy.release, nil
}
