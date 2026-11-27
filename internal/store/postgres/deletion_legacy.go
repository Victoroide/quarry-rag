package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// legacyBookkeepingLimit bounds a receipt update that must outlive the request.
const legacyBookkeepingLimit = 5 * time.Second

var _ domain.LegacyDeletionGuard = (*Client)(nil)

// WithLegacyDeletion keeps the activation lock shared while remove runs, so an
// activation commits only after the cleanup, including its last external call,
// has returned. It returns ErrDeletionActivated without calling remove once the
// database selected checkpointed deletion. A lost session releases the lock, so
// the reservation that remove commits is what protects the identity afterwards.
//
// The lock lives on a session of its own, outside the pool, so that a cleanup
// waiting on a provider never holds a pooled connection that queries, admissions
// and worker leases need. Every cleanup holds one of a bounded number of slots,
// and a call with none free returns ErrTooManyLegacyDeletions before it opens a
// connection or takes a lock.
func (c *Client) WithLegacyDeletion(ctx context.Context, id domain.DocumentID, remove func(domain.LegacyDocumentStore) error) error {
	releaseSlot, err := c.claimLegacySlot(ctx)
	if err != nil {
		return err
	}
	defer releaseSlot()
	conn, err := pgx.ConnectConfig(ctx, c.Pool.Config().ConnConfig)
	if err != nil {
		return fmt.Errorf("open legacy document deletion session: %w", err)
	}
	store := &legacyDeletion{client: c, conn: conn}
	defer store.close(ctx)
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock_shared($1)`, DeletionActivationLockID); err != nil {
		return fmt.Errorf("lock legacy document deletion: %w", err)
	}
	var active bool
	if err = conn.QueryRow(ctx, `SELECT document_deletion_active()`).Scan(&active); err != nil {
		return fmt.Errorf("postgres: read deletion activation: %w", err)
	}
	if active {
		return domain.ErrDeletionActivated
	}
	if parsed, parseErr := uuid.Parse(string(id)); parseErr == nil && parsed.Version() == 5 {
		// Hold the evaluation fixture fence over the whole cleanup, as ordinary removal does.
		if _, err = conn.Exec(ctx, "SELECT pg_advisory_lock("+fixtureDocumentLockKey+")", parsed.String()); err != nil {
			return err
		}
		var reserved bool
		if reservedErr := conn.QueryRow(ctx, `SELECT evaluation_fixture_reserved($1)`, parsed.String()).Scan(&reserved); reservedErr != nil {
			return reservedErr
		}
		if reserved {
			return domain.ErrFixtureDeletionRequired
		}
	}
	return remove(store)
}

// legacyDeletion adapts one guarded session to the cleanup's store interface.
type legacyDeletion struct {
	client *Client
	// conn is a dedicated connection outside the pool, and nil once the guarded
	// session was found to be lost.
	conn *pgx.Conn
}

var _ domain.LegacyDocumentStore = (*legacyDeletion)(nil)

// close releases every session lock the cleanup took and ends the session. The
// explicit release comes first so that the lock is free when close returns,
// because the server frees a closed session's locks only once its backend exits.
func (l *legacyDeletion) close(parent context.Context) {
	if l.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
	defer cancel()
	conn := l.conn
	l.conn = nil
	_, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock_all()`)
	_ = conn.Close(ctx)
}

// sessionLost reports whether err means the guarded session ended, as opposed
// to a statement that failed on a healthy one.
func sessionLost(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Severity == "FATAL" || pgErr.Severity == "PANIC" || strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "57P")
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// discardSession closes the guarded connection, which ends its locks, before the
// pool connection that completes the receipt takes over. The close has its own
// budget, because the caller's context may already be spent.
func (l *legacyDeletion) discardSession(parent context.Context) {
	if l.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), legacyBookkeepingLimit)
	defer cancel()
	conn := l.conn
	l.conn = nil
	_ = conn.Close(ctx)
}

// exec runs a receipt update that must succeed even after the guarded session
// or the request deadline is gone. Every such update names its claim, so
// repeating it on another connection cannot affect a different cleanup. Each
// connection has its own budget: a session that hangs, as opposed to resetting,
// spends all of its own and leaves the fresh connection a full one.
func (l *legacyDeletion) exec(parent context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if l.conn != nil {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), legacyBookkeepingLimit)
		tag, err := execWriterSQL(ctx, l.conn, query, args...)
		cancel()
		if err == nil || !retryWriterSQL(ctx, err) || (!sessionLost(err) && !l.conn.IsClosed()) {
			return tag, err
		}
		l.discardSession(parent)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), legacyBookkeepingLimit)
	defer cancel()
	return execWriterSQL(ctx, l.client.Pool, query, args...)
}

func legacyVectorModeFor(scoped bool, spec domain.LegacyDeletionSpec) (string, error) {
	switch {
	case spec.Hosted && scoped:
		if !spec.RevisionVectors {
			return "", domain.ErrVectorStorageUnavailable
		}
		return revisionVectors, nil
	case spec.Hosted:
		return legacyVectors, nil
	case spec.Memory && scoped:
		if !spec.RevisionVectors {
			return "", domain.ErrVectorStorageUnavailable
		}
		return memoryRevision, nil
	case spec.Memory:
		return memoryVectors, nil
	default:
		return localVectors, nil
	}
}

// PrepareLegacyDeletion locks the live row and synchronizes with worker
// mutation claims, then commits the reservation for the observed incarnation,
// source, backend and owner. Pending or uncertain worker effects move to the
// reservation before the row can be retired, and the reservation stops new
// worker leases and mutations for the identity.
func (l *legacyDeletion) PrepareLegacyDeletion(ctx context.Context, id domain.DocumentID, spec domain.LegacyDeletionSpec) (domain.LegacyDeletionTarget, error) {
	var target domain.LegacyDeletionTarget
	if l.conn == nil {
		return target, errors.New("postgres: legacy deletion session was lost")
	}
	tx, err := l.conn.Begin(ctx)
	if err != nil {
		return target, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var state string
	var pending, liveClaim, uncertain bool
	err = tx.QueryRow(ctx, `SELECT COALESCE(incarnation,id)::text,status,s3_key,scoped_vectors,vector_mutation_pending,
		COALESCE(vector_mutation_until>clock_timestamp(),FALSE),vector_mutation_uncertain OR vector_delete_uncertain
		FROM documents WHERE id=$1 AND status<>'deleting' FOR UPDATE`, string(id)).
		Scan(&target.Incarnation, &state, &target.SourceKey, &target.ScopedVectors, &pending, &liveClaim, &uncertain)
	if errors.Is(err, pgx.ErrNoRows) {
		return target, domain.ErrDocumentNotFound
	}
	if err != nil {
		return target, err
	}
	if target.SourceKey != nil && !spec.SourceAvailable {
		return target, domain.ErrFileStorageUnavailable
	}
	mode, err := legacyVectorModeFor(target.ScopedVectors, spec)
	if err != nil {
		return target, err
	}
	if liveClaim {
		// A worker mutation that is still running may be writing vectors.
		return target, domain.ErrDeletionBusy
	}
	if pending {
		// An untagged worker mutation that expired outside a failed job stays unsettled.
		if !target.ScopedVectors && state != "failed" {
			return target, domain.ErrLiveVectorMutation
		}
		if _, err = settleExpiredVectorMutation(ctx, tx, id); err != nil {
			return target, err
		}
		uncertain = true
	}
	// The intended target is what this request would record. A resumed
	// reservation keeps the one it recorded, so any difference fails closed.
	var index, bucket, owner *string
	if mode == legacyVectors || mode == revisionVectors {
		index = nullable(spec.VectorIndex)
	}
	if target.SourceKey != nil {
		bucket = nullable(spec.SourceBucket)
	}
	if mode == memoryVectors || mode == memoryRevision {
		owner = nullable(spec.MemoryOwner)
		if owner == nil {
			return target, domain.ErrVectorStorageUnavailable
		}
	}
	// Document-wide hosted effects of older writers cannot be proved settled, so
	// that identity stays held after an acknowledged request.
	held := mode == legacyVectors || uncertain
	var recorded recordedReservation
	err = tx.QueryRow(ctx, `SELECT kind,incarnation::text,finalized,vector_mode,vector_index,source_bucket,source_key,memory_owner::text,
		COALESCE(claim_until>clock_timestamp(),FALSE) FROM document_deletions WHERE document_id=$1 FOR UPDATE`, string(id)).
		Scan(&recorded.kind, &recorded.incarnation, &recorded.finalized, &recorded.mode,
			&recorded.index, &recorded.bucket, &recorded.sourceKey, &recorded.owner, &recorded.claimed)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return target, fmt.Errorf("read legacy cleanup reservation: %w", err)
	case recorded.resumable(target.Incarnation):
		switch {
		case recorded.claimed:
			// Another cleanup is running.
			return target, domain.ErrDeletionBusy
		case recorded.mode != mode:
			return target, domain.ErrDeletionTargetMismatch
		case !sameValue(recorded.owner, owner):
			// Only the process that owns the memory index resumes its reservation.
			return target, domain.ErrDeletionBusy
		case !sameValue(recorded.index, index) || !sameValue(recorded.bucket, bucket) || !sameValue(recorded.sourceKey, target.SourceKey):
			return target, domain.ErrDeletionTargetMismatch
		}
	}
	if mode == localVectors {
		// Settling an expired mutation above can change the document in this transaction.
		return target, commitWriterSQL(ctx, tx)
	}
	target.Claim = uuid.NewString()
	remaining := time.Until(legacyDeadline(ctx))
	if remaining <= 0 {
		return target, context.DeadlineExceeded
	}
	var tag pgconn.CommandTag
	if recorded.resumable(target.Incarnation) {
		// Only the claim and the hold change. Where the original effects went is never rewritten.
		tag, err = tx.Exec(ctx, `UPDATE document_deletions SET held=held OR $3,claim=$2,claim_until=clock_timestamp()+make_interval(secs=>$4)
			WHERE document_id=$1`, string(id), target.Claim, held, remaining.Seconds())
	} else {
		tag, err = tx.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,source_key,source_bucket,vector_mode,vector_index,memory_owner,step,held,finalized,claim,claim_until)
			VALUES($1,$2,'legacy_reservation',$3,$4,$5,$6,$7,0,$8,FALSE,$9,clock_timestamp()+make_interval(secs=>$10))
			ON CONFLICT(document_id) DO UPDATE SET incarnation=EXCLUDED.incarnation,kind=EXCLUDED.kind,source_key=EXCLUDED.source_key,
				source_bucket=EXCLUDED.source_bucket,vector_mode=EXCLUDED.vector_mode,vector_index=EXCLUDED.vector_index,
				memory_owner=EXCLUDED.memory_owner,step=0,held=EXCLUDED.held,
				finalized=FALSE,claim=EXCLUDED.claim,claim_until=EXCLUDED.claim_until
			WHERE document_deletions.finalized AND NOT document_deletions.held`,
			string(id), target.Incarnation, target.SourceKey, bucket, mode, index, owner, held, target.Claim, remaining.Seconds())
	}
	if err != nil {
		return target, fmt.Errorf("reserve legacy cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		// Another cleanup owns the reservation, or an operator holds this identity.
		return target, domain.ErrDeletionBusy
	}
	return target, commitWriterSQL(ctx, tx)
}

// recordedReservation is the part of an existing deletion receipt that a resumed
// cleanup must agree with.
type recordedReservation struct {
	kind, incarnation, mode         string
	finalized, claimed              bool
	index, bucket, sourceKey, owner *string
}

// resumable reports whether the receipt is an unfinished reservation for the
// observed incarnation, which a retry continues instead of replacing.
func (r recordedReservation) resumable(incarnation string) bool {
	return r.kind == "legacy_reservation" && !r.finalized && r.incarnation == incarnation
}

// sameValue compares two nullable values the way IS NOT DISTINCT FROM does.
func sameValue(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// legacyDeadline bounds the claim to the caller's deadline, or to the default limit.
func legacyDeadline(ctx context.Context) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}
	return time.Now().Add(deletionClaimLimit)
}

// NoteLegacyUncertain records that some attempt may have reached a provider.
func (l *legacyDeletion) NoteLegacyUncertain(ctx context.Context, id domain.DocumentID, target domain.LegacyDeletionTarget) error {
	if target.Claim == "" {
		return nil
	}
	tag, err := l.exec(ctx, `UPDATE document_deletions SET held=TRUE
		WHERE document_id=$1 AND incarnation=$2 AND claim=$3 AND NOT finalized`, string(id), target.Incarnation, target.Claim)
	if err == nil && tag.RowsAffected() != 1 {
		return domain.ErrDeletionBusy
	}
	return err
}

// RetireLegacyDocument removes the observed row while its reservation, if any,
// still belongs to this cleanup. The reservation stays unfinished until the
// last external effect has returned.
func (l *legacyDeletion) RetireLegacyDocument(ctx context.Context, id domain.DocumentID, target domain.LegacyDeletionTarget) error {
	if l.conn == nil {
		return errors.New("postgres: legacy deletion session was lost")
	}
	var retired bool
	err := l.conn.QueryRow(ctx, `WITH removed AS (
			DELETE FROM documents WHERE id=$1 AND COALESCE(incarnation,id)=$2 AND status<>'deleting'
			AND (scoped_vectors OR NOT vector_mutation_pending)
			AND ($3::text='' OR EXISTS (SELECT 1 FROM document_deletions r WHERE r.document_id=$1 AND r.incarnation=$2
				AND r.claim=NULLIF($3,'')::uuid AND NOT r.finalized))
			RETURNING id
		) SELECT EXISTS(SELECT 1 FROM removed)`, string(id), target.Incarnation, target.Claim).Scan(&retired)
	if err != nil {
		return observeWriterSQL(ctx, err)
	}
	if !retired {
		return domain.ErrLiveVectorMutation
	}
	return nil
}

// CompleteLegacyDeletion finishes the reservation after the last external
// effect returned. It runs on a fresh connection when the guarded session is
// gone, because only the cleanup that made the effect can record its end.
func (l *legacyDeletion) CompleteLegacyDeletion(ctx context.Context, id domain.DocumentID, target domain.LegacyDeletionTarget, uncertain bool) error {
	if target.Claim == "" {
		return nil
	}
	tag, err := l.exec(ctx, `UPDATE document_deletions SET finalized=TRUE,step=$5,held=held OR $4,
		source_key=CASE WHEN held OR $4 THEN source_key ELSE NULL END,claim=NULL,claim_until=NULL
		WHERE document_id=$1 AND incarnation=$2 AND claim=$3 AND kind='legacy_reservation' AND NOT finalized
		AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.id=document_deletions.document_id)`,
		string(id), target.Incarnation, target.Claim, uncertain, stepDone)
	if err != nil {
		return fmt.Errorf("complete legacy cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrDeletionBusy
	}
	return nil
}

// AbandonLegacyDeletion releases the claim so that a retry can resume.
func (l *legacyDeletion) AbandonLegacyDeletion(ctx context.Context, id domain.DocumentID, target domain.LegacyDeletionTarget) {
	if target.Claim == "" {
		return
	}
	_, _ = l.exec(ctx, `UPDATE document_deletions SET claim=NULL,claim_until=NULL
		WHERE document_id=$1 AND claim=$2 AND NOT finalized`, string(id), target.Claim)
}
