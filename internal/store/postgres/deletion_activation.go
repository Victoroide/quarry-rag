package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ domain.DeletionActivator = (*Client)(nil)

// errActivationMarkerMissing reports a database that has not applied the
// migration that creates the activation marker.
var errActivationMarkerMissing = errors.New("postgres: the deletion activation marker is missing; apply the schema migrations first")

// ActivateDeletion is the only writer of the activation marker. It takes
// DeletionActivationLockID exclusively, so it waits for every healthy ID-only
// cleanup to return its last external call, and it makes later cleanups wait for
// the outcome. Under the lock it sets the marker from the database clock at
// most once. A repeated call, from this or another process, reports the recorded
// value and changes nothing. A cleanup whose database session was lost holds no
// lock, and its durable reservation keeps its identity protected after
// activation. Ordinary startup, request handling and test cleanup never call it.
func (c *Client) ActivateDeletion(ctx context.Context) (domain.DeletionActivation, error) {
	var result domain.DeletionActivation
	conn, err := c.Pool.Acquire(ctx)
	if err != nil {
		return result, fmt.Errorf("postgres: activate deletion: %w", err)
	}
	defer releaseSessionLocks(ctx, conn)
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, DeletionActivationLockID); err != nil {
		return result, activationError(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return result, activationError(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = bindMetadataSchema(ctx, tx); err != nil {
		return result, activationError(err)
	}
	var activatedAt time.Time
	err = tx.QueryRow(ctx, `UPDATE document_deletion_activation
		SET activated_at = clock_timestamp()
		WHERE singleton AND activated_at IS NULL
		RETURNING activated_at`).Scan(&activatedAt)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		var recorded *time.Time
		err = tx.QueryRow(ctx, `SELECT activated_at FROM document_deletion_activation WHERE singleton`).Scan(&recorded)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && recorded == nil) {
			return result, errActivationMarkerMissing
		}
		if err != nil {
			return result, activationError(err)
		}
		activatedAt, result.AlreadyActive = *recorded, true
	default:
		return result, activationError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		// The outcome of a lost commit reply is unknown. A repeated activation
		// reports the recorded value either way.
		return domain.DeletionActivation{}, activationError(err)
	}
	result.ActivatedAt = activatedAt.UTC()
	return result, nil
}

func activationError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42501" {
		return fmt.Errorf("postgres: activate deletion: the login cannot update document_deletion_activation; run it with the schema owner: %w", err)
	}
	return fmt.Errorf("postgres: activate deletion: %w", err)
}

// releaseSessionLocks returns the activation connection to the pool. A
// connection that cannot prove it released its session lock is closed, which
// releases the lock in the server. The caller's context may already be spent,
// so the release has its own budget.
func releaseSessionLocks(parent context.Context, conn *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock_all()`); err != nil {
		_ = conn.Hijack().Close(ctx)
		return
	}
	conn.Release()
}
