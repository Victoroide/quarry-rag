package domain

import (
	"context"
	"errors"
	"time"
)

// EvaluationFixtureStatus is an authoritative result for a fixture receipt.
type EvaluationFixtureStatus struct {
	State        string
	Incarnation  string
	DatabaseTime time.Time
}

// States returned for evaluation fixture receipts.
const (
	FixtureMissing    = "missing"
	FixtureBorrowed   = "borrowed"
	FixtureOwned      = "owned"
	FixtureDeleted    = "deleted"
	FixtureBusy       = "busy"
	FixtureRetired    = "retired"
	FixtureWrongStore = "store_mismatch"
)

// ErrFixtureDeletionRequired directs an ordinary caller to receipt-qualified cleanup.
var ErrFixtureDeletionRequired = errors.New("evaluation fixture requires its receipt and incarnation")

// DocumentRemovalGuard protects the full ordinary deletion, including external side effects.
type DocumentRemovalGuard interface {
	WithOrdinaryRemoval(context.Context, DocumentID, func(context.Context) error) error
}

// FixtureJobStore restricts ingestion leases to workers attached to the reserved store.
type FixtureJobStore interface {
	ClaimJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*Job, error)
}

// EvaluationFixtureStore verifies and deletes only the reserved fixture revision.
type EvaluationFixtureStore interface {
	ReadEvaluationFixture(ctx context.Context, id DocumentID, receipt string) (EvaluationFixtureStatus, error)
	ReconcileEvaluationFixture(ctx context.Context, id DocumentID, receipt string) (EvaluationFixtureStatus, error)
	DeleteEvaluationFixture(ctx context.Context, id DocumentID, receipt, incarnation, vectorIdentity string, sweep func(context.Context) error) (string, error)
}
