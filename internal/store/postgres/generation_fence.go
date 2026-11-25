package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const reindexLockNamespace = 0x71726569

// GenerationFence owns the exclusive operation lock for the physical target
// registered to a generation. A physical target can have only one generation.
type GenerationFence struct {
	conn *pgx.Conn
	name string
}

// Ledger uses the session that owns the target lock for every checkpoint.
func (f *GenerationFence) Ledger() GenerationLedger {
	return GenerationLedger{db: f.conn}
}

// FenceGeneration excludes other staging, verification and cutover operations.
func (c *Client) FenceGeneration(ctx context.Context, name string) (*GenerationFence, error) {
	if !ValidGenerationName(name) {
		return nil, fmt.Errorf("postgres: invalid generation name %q", name)
	}
	conn, err := pgx.ConnectConfig(ctx, c.Pool.Config().ConnConfig)
	if err != nil {
		return nil, err
	}
	var acquired bool
	err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, hashtext($2))`, reindexLockNamespace, name).Scan(&acquired)
	if err != nil {
		closeFenceConnection(ctx, conn)
		return nil, fmt.Errorf("postgres: lock generation %q: %w", name, err)
	}
	if !acquired {
		closeFenceConnection(ctx, conn)
		return nil, fmt.Errorf("postgres: another operation owns generation %q", name)
	}
	return &GenerationFence{conn: conn, name: name}, nil
}

// Close releases the lock and the dedicated session.
func (f *GenerationFence) Close(ctx context.Context) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, _ = f.conn.Exec(cleanupCtx, `SELECT pg_advisory_unlock($1, hashtext($2))`, reindexLockNamespace, f.name)
	closeFenceConnection(ctx, f.conn)
}

func closeFenceConnection(ctx context.Context, conn *pgx.Conn) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = conn.Close(cleanupCtx)
}

// RequireAcknowledged checks that no preceding operation lost its response.
// The operator may clear an uncertain write once the target has drained it.
func (f *GenerationFence) RequireAcknowledged(ctx context.Context, confirmedDrained bool) error {
	row, err := readGeneration(ctx, f.conn, f.name, "")
	if err != nil || row == nil || !row.PendingWrite {
		return err
	}
	if !confirmedDrained {
		return fmt.Errorf("generation %q has an unconfirmed target write; resolve the outstanding request before resuming staging", f.name)
	}
	return f.acknowledge(ctx)
}

// Write persists a barrier before dispatching a mutation and clears it only
// when the target acknowledges it. Losing either connection leaves the barrier
// visible to the next invocation, even after the advisory lock is released.
func (f *GenerationFence) Write(ctx context.Context, write func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tag, err := f.conn.Exec(ctx, `UPDATE vector_generations
 SET pending_write=TRUE, state='staging', corpus_digest=NULL, verification=NULL, verified_at=NULL, updated_at=NOW()
 WHERE name=$1 AND NOT pending_write AND state IN ('staging','verified','retired')
 AND name <> (SELECT active_generation FROM vector_state)`, f.name)
	if err != nil {
		return fmt.Errorf("postgres: record target write: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("generation %q is missing, active or has an unconfirmed target write", f.name)
	}
	if writeErr := write(); writeErr != nil {
		return writeErr
	}
	return f.acknowledge(ctx)
}

func (f *GenerationFence) acknowledge(ctx context.Context) error {
	tag, err := f.conn.Exec(ctx, `UPDATE vector_generations SET pending_write=FALSE
 WHERE name=$1 AND pending_write AND name <> (SELECT active_generation FROM vector_state)`, f.name)
	if err != nil {
		return fmt.Errorf("postgres: acknowledge target write: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("postgres: target write lost its generation")
	}
	return nil
}
