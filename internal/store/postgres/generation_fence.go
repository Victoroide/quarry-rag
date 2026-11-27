package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
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

const activeCleanupEpoch = `singleton AND active_generation=$1 AND frozen
 AND freeze_generation=$1 AND frozen_at=$2`

// ActiveCleanupBatch identifies a single reservation of exact provider IDs.
type ActiveCleanupBatch struct {
	Reservation uuid.UUID
	Sender      uuid.UUID
	IDs         []string
}

// ActiveCleanupBatch reads the unresolved batch for the held freeze, if any.
func (f *GenerationFence) ActiveCleanupBatch(ctx context.Context, epoch time.Time) (*ActiveCleanupBatch, error) {
	var batch ActiveCleanupBatch
	var reservation, sender uuid.NullUUID
	if err := f.conn.QueryRow(ctx, `SELECT active_cleanup_ids, active_cleanup_reservation, active_cleanup_sender FROM vector_state WHERE `+activeCleanupEpoch,
		f.name, epoch).Scan(&batch.IDs, &reservation, &sender); err != nil {
		return nil, fmt.Errorf("postgres: read active cleanup journal under the pinned freeze: %w", err)
	}
	if reservation.Valid != sender.Valid || reservation.Valid != (batch.IDs != nil) ||
		(reservation.Valid && (reservation.UUID == uuid.Nil || sender.UUID == uuid.Nil || len(batch.IDs) == 0)) {
		return nil, errors.New("postgres: inconsistent active cleanup journal identity")
	}
	if !reservation.Valid {
		return nil, nil
	}
	batch.Reservation = reservation.UUID
	batch.Sender = sender.UUID
	return &batch, nil
}

// ReserveActiveCleanup commits an exact batch before the caller contacts Pinecone.
func (f *GenerationFence) ReserveActiveCleanup(ctx context.Context, epoch time.Time, target, host string, profile domain.EmbeddingProfile, ids []string, sender uuid.UUID) (uuid.UUID, error) {
	if err := ctx.Err(); err != nil {
		return uuid.Nil, err
	}
	if len(ids) < 1 || len(ids) > 1000 {
		return uuid.Nil, errors.New("postgres: reserve between 1 and 1000 active cleanup IDs")
	}
	if sender == uuid.Nil {
		return uuid.Nil, errors.New("postgres: active cleanup requires a nonzero sender UUID")
	}
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("postgres: start active cleanup reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var reservation uuid.UUID
	err = tx.QueryRow(ctx, `UPDATE vector_state SET active_cleanup_ids=$3, active_cleanup_reservation=gen_random_uuid(), active_cleanup_sender=$4
 WHERE `+activeCleanupEpoch+` AND active_cleanup_reservation IS NULL RETURNING active_cleanup_reservation`,
		f.name, epoch, ids, sender).Scan(&reservation)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, errors.New("postgres: active cleanup freeze changed or a batch is already pending")
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("postgres: reserve active cleanup: %w", err)
	}
	// The UPDATE holds the singleton lock before this binding check.
	if err := checkActiveVectorBinding(ctx, tx, f.name, "pinecone", target, host, profile.Hash()); err != nil {
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("postgres: commit active cleanup reservation: %w", err)
	}
	return reservation, nil
}

// ClearActiveCleanup clears only the acknowledged or externally settled batch.
func (f *GenerationFence) ClearActiveCleanup(ctx context.Context, epoch time.Time, reservation uuid.UUID, ids []string, sender uuid.UUID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reservation == uuid.Nil || sender == uuid.Nil {
		return errors.New("postgres: active cleanup requires nonzero reservation and sender UUIDs")
	}
	tag, err := f.conn.Exec(ctx, `UPDATE vector_state SET active_cleanup_ids=NULL, active_cleanup_reservation=NULL, active_cleanup_sender=NULL
 WHERE `+activeCleanupEpoch+` AND active_cleanup_reservation=$3 AND active_cleanup_ids=$4 AND active_cleanup_sender=$5`, f.name, epoch, reservation, ids, sender)
	if err != nil {
		return fmt.Errorf("postgres: clear active cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("postgres: active cleanup freeze or recorded batch changed")
	}
	return nil
}

// RequireAcknowledged blocks staging until an operator confirms settlement of
// the write currently recorded for this generation.
func (f *GenerationFence) RequireAcknowledged(ctx context.Context, confirmation uuid.UUID) error {
	row, err := readGeneration(ctx, f.conn, f.name, "")
	if err != nil {
		return err
	}
	if row == nil || !row.PendingWrite {
		if confirmation != uuid.Nil {
			return fmt.Errorf("generation %q: stale confirmation for target write %s, as no write is pending", f.name, confirmation)
		}
		return nil
	}
	pending := row.PendingWriteID.UUID
	if confirmation == uuid.Nil {
		return fmt.Errorf("generation %q has an unconfirmed target write %s. Resolve it before resuming staging with -confirm-target-drained=%s", f.name, pending, pending)
	}
	if confirmation != pending {
		return fmt.Errorf("generation %q: stale confirmation for target write %s. The pending write is %s (recovery: -confirm-target-drained=%s)", f.name, confirmation, pending, pending)
	}
	return f.acknowledge(ctx, pending)
}

// Write persists a barrier before dispatching a mutation and clears it only
// when the target acknowledges that write. If either response is lost, the
// durable journal determines whether the next invocation can proceed.
func (f *GenerationFence) Write(ctx context.Context, write func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var writeID uuid.UUID
	err := f.conn.QueryRow(ctx, `UPDATE vector_generations
 SET pending_write=TRUE, pending_write_id=gen_random_uuid(),
 state='staging', corpus_digest=NULL, verification=NULL, verified_at=NULL, updated_at=NOW()
 WHERE name=$1 AND NOT pending_write AND state IN ('staging','verified','retired')
 AND name <> (SELECT active_generation FROM vector_state)
 RETURNING pending_write_id`, f.name).Scan(&writeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("generation %q is missing, active or has an unconfirmed target write", f.name)
	}
	if err != nil {
		return fmt.Errorf("postgres: record target write for generation %q: %w", f.name, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("generation %q: dispatch target write %s: %w", f.name, writeID, err)
	}
	if err := write(); err != nil {
		return fmt.Errorf("generation %q: target write %s: %w", f.name, writeID, err)
	}
	return f.acknowledge(ctx, writeID)
}

func (f *GenerationFence) acknowledge(ctx context.Context, writeID uuid.UUID) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("postgres: acknowledge target write %s for generation %q: %w", writeID, f.name, err)
	}
	tag, err := f.conn.Exec(ctx, `UPDATE vector_generations SET pending_write=FALSE, pending_write_id=NULL
 WHERE name=$1 AND pending_write AND pending_write_id=$2 AND state IN ('staging','verified','retired')
 AND name <> (SELECT active_generation FROM vector_state)`, f.name, writeID)
	if err != nil {
		return fmt.Errorf("postgres: acknowledge target write %s for generation %q: %w", writeID, f.name, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("postgres: target write %s is no longer pending for an eligible generation %q", writeID, f.name)
	}
	return nil
}
