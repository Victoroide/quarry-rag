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

var _ domain.HeldDeletionStore = (*Client)(nil)

const heldDeletionColumns = `r.document_id::text, r.incarnation::text, r.kind, r.vector_mode,
	COALESCE(r.vector_index, ''), COALESCE(r.vector_namespace, ''),
	COALESCE(r.memory_owner::text, ''), COALESCE(r.source_key, ''),
	COALESCE(r.source_bucket, ''), r.step, r.held, r.finalized,
	EXISTS(SELECT 1 FROM documents d WHERE d.id = r.document_id),
	COALESCE(r.claim_until > clock_timestamp(), FALSE),
	COALESCE(r.claim::text, ''), r.claim_until`

func scanHeldDeletion(row pgx.Row) (domain.HeldDeletion, error) {
	var h domain.HeldDeletion
	var id string
	err := row.Scan(
		&id, &h.Incarnation, &h.Kind, &h.VectorMode,
		&h.VectorIndex, &h.VectorNamespace, &h.MemoryOwner,
		&h.SourceKey, &h.SourceBucket, &h.Step, &h.Held, &h.Finalized,
		&h.DocumentPresent, &h.Claimed, &h.Claim, &h.ClaimUntil,
	)
	if err != nil {
		return domain.HeldDeletion{}, err
	}
	h.DocumentID = domain.DocumentID(id)
	h.Phase = heldPhase(h.Kind, h.Finalized, h.Held, h.Step)
	return h, nil
}

func heldPhase(kind string, finalized, held bool, step int) string {
	if finalized {
		if !held {
			return "released"
		}
		if kind == "quarantine" {
			return "quarantined"
		}
		return "held"
	}
	switch step {
	case stepNamespace:
		if kind == "legacy_reservation" {
			return "reserved"
		}
		return "namespace"
	case stepVectors:
		return "vectors"
	case stepSource:
		return "source"
	case stepDone:
		return "finalize"
	default:
		return "pending"
	}
}

// ListHeldDeletions returns all held or unfinished deletion receipts ordered by
// document UUID so operators can inspect their incarnations, backends, phases
// and claim state.
func (c *Client) ListHeldDeletions(ctx context.Context) ([]domain.HeldDeletion, error) {
	rows, err := c.Pool.Query(ctx, `SELECT `+heldDeletionColumns+`
		FROM document_deletions r
		WHERE r.held OR NOT r.finalized
		ORDER BY r.document_id`)
	if err != nil {
		return nil, fmt.Errorf("list held deletions: %w", err)
	}
	defer rows.Close()
	out := make([]domain.HeldDeletion, 0)
	for rows.Next() {
		item, scanErr := scanHeldDeletion(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan held deletion: %w", scanErr)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list held deletions: %w", err)
	}
	return out, nil
}

// QuarantineDeletion reserves an absent document UUID against reuse while
// preserving any existing held or unfinished receipt verbatim.
func (c *Client) QuarantineDeletion(ctx context.Context, id domain.DocumentID, target domain.QuarantineTarget) (domain.HeldDeletion, error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return domain.HeldDeletion{}, fmt.Errorf("invalid document UUID: %w", err)
	}
	canonical := parsed.String()
	mode := target.VectorMode
	if mode == "" {
		mode = legacyVectors
	}
	switch mode {
	case localVectors, legacyVectors, revisionVectors, memoryVectors, memoryRevision:
	default:
		return domain.HeldDeletion{}, fmt.Errorf("invalid quarantine vector mode %q", mode)
	}
	var vectorIndex *string
	if hostedVectors(mode) {
		vectorIndex = nullable(target.VectorIndex)
	}

	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.HeldDeletion{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
		return domain.HeldDeletion{}, err
	}
	var liveID string
	err = tx.QueryRow(ctx, `SELECT id::text FROM documents WHERE id=$1 FOR UPDATE`, canonical).Scan(&liveID)
	if err == nil {
		return domain.HeldDeletion{}, fmt.Errorf("%w: document %s exists; pause writes and delete it before quarantine", domain.ErrDocumentExists, canonical)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.HeldDeletion{}, fmt.Errorf("check live document: %w", err)
	}

	existing, err := scanHeldDeletion(tx.QueryRow(ctx, `SELECT `+heldDeletionColumns+`
		FROM document_deletions r WHERE r.document_id=$1 FOR UPDATE`, canonical))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		incarnation := uuid.NewString()
		created, insertErr := scanHeldDeletion(tx.QueryRow(ctx, `INSERT INTO document_deletions AS r
			(document_id, incarnation, kind, vector_mode, vector_index, step, held, finalized)
			VALUES ($1, $2, 'quarantine', $3, $4, $5, TRUE, TRUE)
			RETURNING `+heldDeletionColumns, canonical, incarnation, mode, vectorIndex, stepDone))
		if insertErr != nil {
			return domain.HeldDeletion{}, fmt.Errorf("insert quarantine receipt: %w", insertErr)
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return domain.HeldDeletion{}, commitErr
		}
		return created, nil
	case err != nil:
		return domain.HeldDeletion{}, fmt.Errorf("lock deletion receipt: %w", err)
	case existing.Finalized && !existing.Held:
		if existing.Claimed {
			return domain.HeldDeletion{}, domain.ErrDeletionBusy
		}
		incarnation := uuid.NewString()
		updated, updateErr := scanHeldDeletion(tx.QueryRow(ctx, `UPDATE document_deletions AS r
			SET incarnation=$2, kind='quarantine', source_key=NULL, source_bucket=NULL,
				vector_mode=$3, vector_index=$4, vector_namespace=NULL, memory_owner=NULL,
				step=$5, held=TRUE, finalized=TRUE, claim=NULL, claim_until=NULL
			WHERE r.document_id=$1
			RETURNING `+heldDeletionColumns, canonical, incarnation, mode, vectorIndex, stepDone))
		if updateErr != nil {
			return domain.HeldDeletion{}, fmt.Errorf("replace released receipt with quarantine: %w", updateErr)
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return domain.HeldDeletion{}, commitErr
		}
		return updated, nil
	default:
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return domain.HeldDeletion{}, commitErr
		}
		return existing, nil
	}
}

type releaseReceipt struct {
	kind, mode                  string
	source, bucket, index       *string
	owner                       *string
	step                        int
	held, finalized, claimValid bool
}

// ReleaseHeldDeletion claims a held finalized receipt or an unfinished legacy
// reservation whose document row is absent, completes any permitted remaining
// cleanup under the claim, and atomically releases the UUID.
func (c *Client) ReleaseHeldDeletion(ctx context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return domain.ErrDocumentNotFound
	}
	id = domain.DocumentID(parsed.String())
	expected, err := canonicalIncarnation(incarnation)
	if err != nil {
		return err
	}
	if expected == "" {
		return domain.ErrDeletionPreconditionRequired
	}
	ctx, cancel, deadline := deletionDeadline(ctx)
	defer cancel()
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return context.DeadlineExceeded
	}
	claim := uuid.NewString()
	receipt, err := c.claimHeldDeletion(ctx, id, expected, claim, remaining, ops)
	if err != nil {
		return err
	}
	defer c.releaseDeletionClaim(ctx, id, claim)

	if cleanupErr := c.executeReleaseCleanup(ctx, id, expected, claim, receipt, ops); cleanupErr != nil {
		return cleanupErr
	}
	return c.finalizeRelease(ctx, id, expected, claim)
}

func (c *Client) claimHeldDeletion(ctx context.Context, id domain.DocumentID, incarnation, claim string, remaining time.Duration, ops domain.DeleteOperations) (releaseReceipt, error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return releaseReceipt{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
		return releaseReceipt{}, err
	}
	var liveID string
	err = tx.QueryRow(ctx, `SELECT id::text FROM documents WHERE id=$1 FOR UPDATE`, string(id)).Scan(&liveID)
	if err == nil {
		return releaseReceipt{}, domain.ErrDeletionPreconditionFailed
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return releaseReceipt{}, err
	}

	var r releaseReceipt
	var recordedIncarnation string
	err = tx.QueryRow(ctx, `SELECT incarnation::text, kind, vector_mode, source_key, source_bucket, vector_index, memory_owner::text,
		step, held, finalized, COALESCE(claim_until > clock_timestamp(), FALSE)
		FROM document_deletions WHERE document_id=$1 FOR UPDATE`, string(id)).
		Scan(&recordedIncarnation, &r.kind, &r.mode, &r.source, &r.bucket, &r.index, &r.owner, &r.step, &r.held, &r.finalized, &r.claimValid)
	if errors.Is(err, pgx.ErrNoRows) {
		return releaseReceipt{}, domain.ErrDeletionPreconditionFailed
	}
	if err != nil {
		return releaseReceipt{}, err
	}
	if recordedIncarnation != incarnation {
		return releaseReceipt{}, domain.ErrDeletionPreconditionFailed
	}
	if r.claimValid {
		return releaseReceipt{}, domain.ErrDeletionBusy
	}
	eligible := (r.finalized && r.held) || (!r.finalized && r.kind == "legacy_reservation")
	if !eligible {
		return releaseReceipt{}, domain.ErrDeletionPreconditionFailed
	}
	if targetErr := checkReleaseTarget(r, ops); targetErr != nil {
		return releaseReceipt{}, targetErr
	}
	tag, err := tx.Exec(ctx, `UPDATE document_deletions SET claim=$3, claim_until=clock_timestamp()+make_interval(secs=>$4)
		WHERE document_id=$1 AND incarnation=$2`, string(id), incarnation, claim, remaining.Seconds())
	if err != nil {
		return releaseReceipt{}, fmt.Errorf("claim held deletion: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return releaseReceipt{}, domain.ErrDeletionBusy
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return releaseReceipt{}, commitErr
	}
	return r, nil
}

func checkReleaseTarget(r releaseReceipt, ops domain.DeleteOperations) error {
	if ops.InMemory {
		if r.mode != memoryVectors && r.mode != memoryRevision {
			return domain.ErrDeletionPreconditionFailed
		}
		if ops.MemoryOwner == "" || r.owner == nil || *r.owner != ops.MemoryOwner {
			return domain.ErrDeletionPreconditionFailed
		}
		switch r.mode {
		case memoryVectors:
			if ops.LegacyNamespace == nil || ops.LegacyVectors == nil {
				return domain.ErrVectorStorageUnavailable
			}
		case memoryRevision:
			if ops.RevisionVectors == nil {
				return domain.ErrVectorStorageUnavailable
			}
		}
	} else {
		switch r.mode {
		case memoryVectors, memoryRevision:
			if !ops.ConfirmedOfflineOwnerTerminated || deref(r.owner) != ops.MemoryOwner {
				return domain.ErrDeletionTargetMismatch
			}
		case revisionVectors:
			if ops.RevisionVectors == nil {
				return domain.ErrVectorStorageUnavailable
			}
		}
		if hostedVectors(r.mode) && deref(r.index) != ops.VectorIndex {
			return domain.ErrDeletionTargetMismatch
		}
	}
	if r.source != nil {
		if ops.Source == nil {
			return domain.ErrFileStorageUnavailable
		}
		if deref(r.bucket) != ops.SourceBucket {
			return domain.ErrDeletionTargetMismatch
		}
	}
	return nil
}

func (c *Client) executeReleaseCleanup(ctx context.Context, id domain.DocumentID, incarnation, claim string, r releaseReceipt, ops domain.DeleteOperations) error {
	if r.finalized {
		// Finalized legacy holds issue zero new document-wide deletes; only
		// revision-scoped receipts or the live memory owner can sweep vectors.
		switch {
		case r.mode == revisionVectors || (ops.InMemory && r.mode == memoryRevision):
			if err := ctx.Err(); err != nil {
				return err
			}
			return ops.RevisionVectors(domain.TrackVectorMutation(ctx), string(id), incarnation)
		case ops.InMemory && r.mode == memoryVectors:
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := ops.LegacyNamespace(domain.TrackVectorMutation(ctx), string(id)); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return ops.LegacyVectors(domain.TrackVectorMutation(ctx), string(id))
		default:
			return nil
		}
	}

	step := r.step
	if ops.InMemory {
		switch r.mode {
		case memoryVectors:
			if step <= stepNamespace {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := ops.LegacyNamespace(domain.TrackVectorMutation(ctx), string(id)); err != nil {
					return err
				}
				if err := c.checkpointReleaseStep(ctx, id, incarnation, claim, stepVectors); err != nil {
					return err
				}
				step = stepVectors
			}
			if step <= stepVectors {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := ops.LegacyVectors(domain.TrackVectorMutation(ctx), string(id)); err != nil {
					return err
				}
				if err := c.checkpointReleaseStep(ctx, id, incarnation, claim, stepSource); err != nil {
					return err
				}
				step = stepSource
			}
		case memoryRevision:
			if step <= stepVectors {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := ops.RevisionVectors(domain.TrackVectorMutation(ctx), string(id), incarnation); err != nil {
					return err
				}
				if err := c.checkpointReleaseStep(ctx, id, incarnation, claim, stepSource); err != nil {
					return err
				}
				step = stepSource
			}
		}
	} else if r.mode == revisionVectors && step <= stepVectors {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ops.RevisionVectors(domain.TrackVectorMutation(ctx), string(id), incarnation); err != nil {
			return err
		}
		if err := c.checkpointReleaseStep(ctx, id, incarnation, claim, stepSource); err != nil {
			return err
		}
		step = stepSource
	}

	if step <= stepSource && r.source != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ops.Source(ctx, *r.source); err != nil {
			return err
		}
		if err := c.checkpointReleaseStep(ctx, id, incarnation, claim, stepDone); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) checkpointReleaseStep(ctx context.Context, id domain.DocumentID, incarnation, claim string, nextStep int) error {
	checkpointCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	tag, err := c.Pool.Exec(checkpointCtx, `UPDATE document_deletions SET step=$4
		WHERE document_id=$1 AND incarnation=$2 AND claim=$3 AND step<$4 AND claim_until>clock_timestamp()`,
		string(id), incarnation, claim, nextStep)
	if err != nil {
		return fmt.Errorf("checkpoint release cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrDeletionBusy
	}
	return nil
}

func (c *Client) finalizeRelease(ctx context.Context, id domain.DocumentID, incarnation, claim string) error {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
		return err
	}
	var liveID string
	err = tx.QueryRow(ctx, `SELECT id::text FROM documents WHERE id=$1 FOR UPDATE`, string(id)).Scan(&liveID)
	if err == nil {
		return domain.ErrDeletionPreconditionFailed
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE document_deletions
		SET finalized=TRUE, held=FALSE, step=$4, source_key=NULL, claim=NULL, claim_until=NULL
		WHERE document_id=$1 AND incarnation=$2 AND claim=$3 AND claim_until>clock_timestamp()`,
		string(id), incarnation, claim, stepDone)
	if err != nil {
		return fmt.Errorf("finalize hold release: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrDeletionBusy
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		released, checkErr := c.deletionReleased(checkCtx, id, incarnation)
		if checkErr == nil && released {
			return nil
		}
		return commitErr
	}
	return nil
}

func (c *Client) deletionReleased(ctx context.Context, id domain.DocumentID, incarnation string) (bool, error) {
	var released bool
	err := c.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM document_deletions r
		WHERE r.document_id=$1 AND r.incarnation=$2 AND r.finalized AND NOT r.held
		AND NOT EXISTS(SELECT 1 FROM documents d WHERE d.id=$1))`, string(id), incarnation).Scan(&released)
	return released, err
}
