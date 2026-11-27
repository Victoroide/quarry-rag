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

const releaseReceiptColumns = `kind, vector_mode, source_key, source_bucket, vector_index, memory_owner::text,
	step, held, finalized, COALESCE(claim_until > clock_timestamp(), FALSE)`

func scanReleaseReceipt(row pgx.Row) (releaseReceipt, error) {
	var r releaseReceipt
	err := row.Scan(&r.kind, &r.mode, &r.source, &r.bucket, &r.index, &r.owner, &r.step, &r.held, &r.finalized, &r.claimValid)
	if errors.Is(err, pgx.ErrNoRows) {
		err = domain.ErrDeletionPreconditionFailed
	}
	return r, err
}

func (r releaseReceipt) checkEligibility(legacyOnly bool) error {
	if r.claimValid {
		return domain.ErrDeletionBusy
	}
	if (r.finalized && !r.held) || (!r.finalized && r.kind != "legacy_reservation") {
		return domain.ErrDeletionPreconditionFailed
	}
	if legacyOnly && (r.mode != legacyVectors || (r.finalized && r.step != stepDone)) {
		return domain.ErrDeletionPreconditionFailed
	}
	return nil
}

func canonicalReleasePair(id domain.DocumentID, incarnation string) (domain.DocumentID, string, error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return "", "", domain.ErrDocumentNotFound
	}
	expected, err := canonicalIncarnation(incarnation)
	if err != nil {
		return "", "", err
	}
	if expected == "" {
		return "", "", domain.ErrDeletionPreconditionRequired
	}
	return domain.DocumentID(parsed.String()), expected, nil
}

// HeldDeletionReleaseMode identifies the eligible receipt for an exact absent
// document lifetime before the caller initializes a provider. Claiming the
// receipt still requires an authoritative check in the release transaction.
func (c *Client) HeldDeletionReleaseMode(ctx context.Context, id domain.DocumentID, incarnation string) (mode, index string, err error) {
	id, incarnation, err = canonicalReleasePair(id, incarnation)
	if err != nil {
		return "", "", err
	}
	r, err := scanReleaseReceipt(c.Pool.QueryRow(ctx, `SELECT `+releaseReceiptColumns+`
		FROM document_deletions WHERE document_id=$1 AND incarnation=$2
		AND NOT EXISTS(SELECT 1 FROM documents WHERE id=$1)`, string(id), incarnation))
	if err != nil {
		return "", "", err
	}
	if eligibilityErr := r.checkEligibility(r.mode == legacyVectors); eligibilityErr != nil {
		return "", "", eligibilityErr
	}
	return r.mode, deref(r.index), nil
}

type heldReleaseDatabase interface {
	deletionExecer
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type heldRelease struct {
	db              heldReleaseDatabase
	outcome         queryRower
	legacyOnly      bool
	claimConnConfig *pgx.ConnConfig
}

// ReleaseHeldDeletion claims a held finalized receipt or an unfinished legacy
// reservation whose document row is absent, completes any permitted remaining
// cleanup under the claim, and atomically releases the UUID.
func (c *Client) ReleaseHeldDeletion(ctx context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	id, incarnation, err := canonicalReleasePair(id, incarnation)
	if err != nil {
		return err
	}
	return (heldRelease{db: c.Pool, outcome: c.Pool}).release(ctx, id, incarnation, ops)
}

// ReleaseLegacyHostedHold releases a receipt for the exact retired hosted
// index under the writer barrier. Only pending source cleanup can run.
func (c *Client) ReleaseLegacyHostedHold(ctx context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	id, incarnation, err := canonicalReleasePair(id, incarnation)
	if err != nil {
		return err
	}
	ctx, cancel, _ := deletionDeadline(ctx)
	defer cancel()
	entered := false
	err = c.runWriterOperation(ctx, domain.WriterOperationLegacyHostedRelease, string(id)+":"+incarnation,
		c.gateHeldRecoveryWriterInSchema, func(ctx context.Context, guard pgx.Tx) error {
			entered = true
			return c.releaseLegacyHostedHold(ctx, guard, id, incarnation, ops)
		})
	if err != nil && !entered {
		return fmt.Errorf("check document writers: %w", err)
	}
	return err
}

func (c *Client) releaseLegacyHostedHold(ctx context.Context, guard pgx.Tx, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	// Use an independent mutation connection while the legacy guard is live.
	// With a reservation, a lost commit is read back through the pool.
	config := c.Pool.Config().ConnConfig
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("connect legacy hold release: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	var outcome queryRower = c.Pool
	if guard != nil {
		outcome = guard
	}
	sourceOnly := domain.DeleteOperations{VectorIndex: ops.VectorIndex, SourceBucket: ops.SourceBucket, Source: ops.Source}
	return (heldRelease{db: conn, outcome: outcome, legacyOnly: true, claimConnConfig: config}).release(ctx, id, incarnation, sourceOnly)
}

func (h heldRelease) release(ctx context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	ctx, cancel, deadline := deletionDeadline(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return context.DeadlineExceeded
	}
	claim := uuid.NewString()
	receipt, uncertainCommit, err := h.claimHeldDeletion(ctx, id, incarnation, claim, remaining, ops)
	if err != nil {
		if uncertainCommit {
			// The claim transaction has unwound, so cleanup can use the pool again.
			h.releaseClaim(ctx, id, incarnation, claim)
		}
		return err
	}
	defer h.releaseClaim(ctx, id, incarnation, claim)

	if cleanupErr := h.executeReleaseCleanup(ctx, id, incarnation, claim, receipt, ops); cleanupErr != nil {
		return cleanupErr
	}
	return h.finalizeRelease(ctx, id, incarnation, claim)
}

func (h heldRelease) releaseClaim(ctx context.Context, id domain.DocumentID, incarnation, claim string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deletionClaimReleaseTimeout)
	err := clearHeldDeletionClaim(releaseCtx, h.db, id, incarnation, claim)
	cancel()
	if err == nil || h.claimConnConfig == nil || !retryWriterSQL(releaseCtx, err) {
		return
	}

	// A canceled transaction can close the mutation session. Retry only the
	// claim update outside the pool while the writer barrier is still held.
	retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deletionClaimReleaseTimeout)
	defer cancel()
	conn, err := pgx.ConnectConfig(retryCtx, h.claimConnConfig)
	if err != nil {
		return
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deletionClaimReleaseTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	_ = clearHeldDeletionClaim(retryCtx, conn, id, incarnation, claim)
}

func clearHeldDeletionClaim(ctx context.Context, db deletionExecer, id domain.DocumentID, incarnation, claim string) error {
	_, err := execWriterSQL(ctx, db, `UPDATE document_deletions SET claim=NULL,claim_until=NULL
		WHERE document_id=$1 AND incarnation=$2 AND claim=$3`, string(id), incarnation, claim)
	return err
}

func rollbackHeldReleaseTx(ctx context.Context, tx pgx.Tx) {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deletionClaimReleaseTimeout)
	defer cancel()
	_ = tx.Rollback(rollbackCtx)
}

// claimHeldDeletion reports whether COMMIT returned an error that leaves
// ownership of the claim uncertain.
func (h heldRelease) claimHeldDeletion(ctx context.Context, id domain.DocumentID, incarnation, claim string, remaining time.Duration, ops domain.DeleteOperations) (releaseReceipt, bool, error) {
	tx, err := h.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return releaseReceipt{}, false, err
	}
	defer rollbackHeldReleaseTx(ctx, tx)
	// An unfinished or held exact legacy receipt already blocks admission.
	// Taking the admission lock here would invert the order used by freeze.
	if !h.legacyOnly {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
			return releaseReceipt{}, false, err
		}
	}
	if absentErr := requireAbsentReleaseDocument(ctx, tx, id); absentErr != nil {
		return releaseReceipt{}, false, absentErr
	}

	r, err := scanReleaseReceipt(tx.QueryRow(ctx, `SELECT `+releaseReceiptColumns+`
		FROM document_deletions WHERE document_id=$1 AND incarnation=$2 FOR UPDATE`, string(id), incarnation))
	if err != nil {
		return releaseReceipt{}, false, err
	}
	if eligibilityErr := r.checkEligibility(h.legacyOnly); eligibilityErr != nil {
		return releaseReceipt{}, false, eligibilityErr
	}
	if h.legacyOnly && ops.VectorIndex == "" {
		return releaseReceipt{}, false, domain.ErrDeletionTargetMismatch
	}
	if targetErr := checkReleaseTarget(r, ops); targetErr != nil {
		return releaseReceipt{}, false, targetErr
	}
	tag, err := tx.Exec(ctx, `UPDATE document_deletions SET claim=$3, claim_until=clock_timestamp()+make_interval(secs=>$4)
		WHERE document_id=$1 AND incarnation=$2`, string(id), incarnation, claim, remaining.Seconds())
	if err != nil {
		return releaseReceipt{}, false, fmt.Errorf("claim held deletion: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return releaseReceipt{}, false, domain.ErrDeletionBusy
	}
	if commitErr := commitWriterSQL(ctx, tx); commitErr != nil {
		return releaseReceipt{}, sqlMutationUnconfirmed(commitErr), commitErr
	}
	return r, false, nil
}

func requireAbsentReleaseDocument(ctx context.Context, tx pgx.Tx, id domain.DocumentID) error {
	var liveID string
	err := tx.QueryRow(ctx, `SELECT id::text FROM documents WHERE id=$1 FOR UPDATE`, string(id)).Scan(&liveID)
	if err == nil {
		return domain.ErrDeletionPreconditionFailed
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return err
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

func (h heldRelease) executeReleaseCleanup(ctx context.Context, id domain.DocumentID, incarnation, claim string, r releaseReceipt, ops domain.DeleteOperations) error {
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
				if err := h.checkpointReleaseStep(ctx, id, incarnation, claim, stepVectors); err != nil {
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
				if err := h.checkpointReleaseStep(ctx, id, incarnation, claim, stepSource); err != nil {
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
				if err := h.checkpointReleaseStep(ctx, id, incarnation, claim, stepSource); err != nil {
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
		if err := h.checkpointReleaseStep(ctx, id, incarnation, claim, stepSource); err != nil {
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
		if err := h.checkpointReleaseStep(ctx, id, incarnation, claim, stepDone); err != nil {
			return err
		}
	}
	return nil
}

func (h heldRelease) checkpointReleaseStep(ctx context.Context, id domain.DocumentID, incarnation, claim string, nextStep int) error {
	checkpointCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	tag, err := execWriterSQL(checkpointCtx, h.db, `UPDATE document_deletions SET step=$4
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

func (h heldRelease) finalizeRelease(ctx context.Context, id domain.DocumentID, incarnation, claim string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := h.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollbackHeldReleaseTx(ctx, tx)
	if !h.legacyOnly {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
			return err
		}
	}
	if absentErr := requireAbsentReleaseDocument(ctx, tx, id); absentErr != nil {
		return absentErr
	}
	// Admission may reuse the UUID only after this exact release commits.
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
		released, checkErr := h.deletionReleased(checkCtx, id, incarnation)
		if checkErr == nil && released {
			return nil
		}
		return observeWriterSQL(ctx, commitErr)
	}
	return nil
}

func (h heldRelease) deletionReleased(ctx context.Context, id domain.DocumentID, incarnation string) (bool, error) {
	var released bool
	err := h.outcome.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM document_deletions r
		WHERE r.document_id=$1 AND r.incarnation=$2 AND r.finalized AND NOT r.held
		AND NOT EXISTS(SELECT 1 FROM documents d WHERE d.id=$1))`, string(id), incarnation).Scan(&released)
	return released, err
}
