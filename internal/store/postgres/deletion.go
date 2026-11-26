package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Vector targets recorded with a cleanup receipt.
const (
	localVectors    = "local"
	legacyVectors   = "legacy"
	revisionVectors = "revision"
	manifestVectors = "manifest"
	memoryVectors   = "memory"
	memoryRevision  = "memory_revision"
)

// Cleanup steps in the order they are checkpointed.
const (
	stepNamespace = iota
	stepVectors
	stepSource
	stepDone
)

// deletionClaimLimit bounds one claim when the caller's context has no deadline.
const deletionClaimLimit = time.Minute

var (
	_ domain.DocumentDeleter          = (*Client)(nil)
	_ domain.DocumentFileReader       = (*Client)(nil)
	_ domain.ScopedDocumentFileReader = (*Client)(nil)
	_ domain.ScopedDocumentDeleter    = (*Client)(nil)
)

// DeletionActive reports whether an operator selected checkpointed deletion.
// A failed lookup is an error, never an answer.
func (c *Client) DeletionActive(ctx context.Context) (bool, error) {
	var active bool
	// The owner-defined predicate lets a restricted serving login ask without
	// any privilege on the marker table.
	err := c.Pool.QueryRow(ctx, `SELECT document_deletion_active()`).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("postgres: read deletion activation: %w", err)
	}
	return active, nil
}

// DeleteWithCleanup records the cleanup target before it touches any store and
// removes the document only after every required step has been acknowledged.
// It never changes the activation marker.
func (c *Client) DeleteWithCleanup(ctx context.Context, id domain.DocumentID, ops domain.DeleteOperations) error {
	// External stores key their records by the canonical UUID.
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return domain.ErrDocumentNotFound
	}
	id = domain.DocumentID(parsed.String())
	incarnation, complete, err := c.beginDeletion(ctx, id, ops)
	if err != nil || complete {
		return err
	}
	return c.resumeObservedDeletion(ctx, id, incarnation, ops)
}

// resumeObservedDeletion lets a matching completed receipt acknowledge a final
// commit whose reply was lost.
func (c *Client) resumeObservedDeletion(ctx context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	err := c.resumeDeletion(ctx, id, incarnation, ops)
	var finalErr *finalDeletionError
	if !errors.As(err, &finalErr) {
		return err
	}
	checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	complete, checkErr := c.deletionFinalized(checkCtx, id, incarnation)
	if checkErr == nil && complete {
		return nil
	}
	return err
}

func (c *Client) deletionFinalized(ctx context.Context, id domain.DocumentID, incarnation string) (bool, error) {
	var complete bool
	err := c.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM document_deletions r
		WHERE r.document_id=$1 AND r.incarnation=$2 AND r.finalized
		AND NOT EXISTS(SELECT 1 FROM documents d WHERE d.id=r.document_id AND COALESCE(d.incarnation,d.id)=r.incarnation))`,
		string(id), incarnation).Scan(&complete)
	return complete, err
}

// finalDeletionError marks a failure of the last commit, whose outcome may be
// hidden by a lost reply.
type finalDeletionError struct{ error }

func (e *finalDeletionError) Unwrap() error { return e.error }

func canonicalIncarnation(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", domain.ErrDeletionPreconditionFailed
	}
	return parsed.String(), nil
}

func vectorMode(scoped, manifested bool, ops domain.DeleteOperations) (string, error) {
	switch {
	case manifested:
		if ops.ManifestVectors == nil {
			return "", domain.ErrVectorStorageUnavailable
		}
		return manifestVectors, nil
	case scoped:
		if ops.RevisionVectors == nil {
			return "", domain.ErrVectorStorageUnavailable
		}
		if ops.InMemory {
			return memoryRevision, nil
		}
		return revisionVectors, nil
	case ops.LegacyNamespace != nil || ops.LegacyVectors != nil:
		if ops.LegacyNamespace == nil || ops.LegacyVectors == nil {
			return "", domain.ErrVectorStorageUnavailable
		}
		if ops.InMemory {
			return memoryVectors, nil
		}
		return legacyVectors, nil
	default:
		return localVectors, nil
	}
}

func hostedVectors(mode string) bool {
	return mode == legacyVectors || mode == revisionVectors || mode == manifestVectors
}

// memoryBacked reports whether the receipt's vectors live in a serving process,
// whose owner alone can replay and release them.
func memoryBacked(mode string) bool {
	return mode == memoryVectors || mode == memoryRevision
}

func nullable(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// beginDeletion commits the intent: it locks the live row, validates the
// caller's incarnation and the required storage, records the exact source and
// vector target, hides the document and cancels its job. complete reports that
// a matching receipt already acknowledged the deletion.
func (c *Client) beginDeletion(ctx context.Context, id domain.DocumentID, ops domain.DeleteOperations) (incarnation string, complete bool, err error) {
	expected, err := canonicalIncarnation(ops.ExpectedIncarnation)
	if err != nil {
		return "", false, err
	}
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if lockErr := lockDocumentIntent(ctx, tx, id); lockErr != nil {
		return "", false, lockErr
	}
	var state string
	var source *string
	var scoped bool
	err = tx.QueryRow(ctx, `SELECT COALESCE(incarnation,id)::text,status,s3_key,scoped_vectors FROM documents WHERE id=$1 FOR UPDATE`, string(id)).
		Scan(&incarnation, &state, &source, &scoped)
	if errors.Is(err, pgx.ErrNoRows) {
		// A qualified retry acknowledges the deletion its receipt completed.
		if expected == "" {
			return "", false, domain.ErrDocumentNotFound
		}
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM document_deletions
			WHERE document_id=$1 AND incarnation=$2 AND kind='qualified_intent' AND finalized)`, string(id), expected).Scan(&complete)
		if err != nil {
			return "", false, err
		}
		if !complete {
			return "", false, domain.ErrDocumentNotFound
		}
		return expected, true, nil
	}
	if err != nil {
		return "", false, err
	}
	// Activation is monotonic, so an active reading never needs a lock to stay true.
	var active bool
	if err = tx.QueryRow(ctx, `SELECT document_deletion_active()`).Scan(&active); err != nil {
		return "", false, fmt.Errorf("postgres: read deletion activation: %w", err)
	}
	if !active {
		return "", false, domain.ErrDeletionInactive
	}
	if parsed, parseErr := uuid.Parse(string(id)); parseErr == nil && parsed.Version() == 5 {
		var reserved bool
		if reserveErr := tx.QueryRow(ctx, `SELECT evaluation_fixture_reserved($1)`, parsed.String()).Scan(&reserved); reserveErr != nil {
			return "", false, reserveErr
		}
		if reserved {
			return "", false, domain.ErrFixtureDeletionRequired
		}
	}
	if expected == "" {
		return "", false, domain.ErrDeletionPreconditionRequired
	}
	if expected != incarnation {
		return "", false, domain.ErrDeletionPreconditionFailed
	}
	if source != nil && ops.Source == nil {
		return "", false, domain.ErrFileStorageUnavailable
	}
	manifested, err := c.ownsManifests(ctx, tx, id, ops)
	if err != nil {
		return "", false, err
	}
	mode, err := vectorMode(scoped, manifested, ops)
	if err != nil {
		return "", false, err
	}
	if state == "deleting" {
		// The intent is already durable; the caller resumes it.
		return incarnation, false, nil
	}
	if intentErr := insertDeletionIntentTx(ctx, tx, id, incarnation, source, mode, ops); intentErr != nil {
		return "", false, intentErr
	}
	return incarnation, false, tx.Commit(ctx)
}

// lockDocumentIntent takes the per-document intent key exclusively. Every
// transaction that commits a deletion intent takes it before the row lock,
// so the intent waits for signatures in flight.
func lockDocumentIntent(ctx context.Context, tx pgx.Tx, id domain.DocumentID) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(`+documentIntentLockKey+`)`, string(id))
	return err
}

// insertDeletionIntentTx records the deletion receipt, hides the document as
// deleting, and cancels its ingest job. Callers hold the intent key and the row lock.
func insertDeletionIntentTx(ctx context.Context, tx pgx.Tx, id domain.DocumentID, incarnation string, source *string, mode string, ops domain.DeleteOperations) error {
	var bucket, index *string
	if source != nil {
		bucket = nullable(ops.SourceBucket)
	}
	if hostedVectors(mode) {
		index = nullable(ops.VectorIndex)
	}
	// A receipt for process-local vectors names the process that owns them, so
	// only that process can later release the hold. Without an owner the receipt
	// could never be released by its service, so none is created.
	owner := nullable(ops.MemoryOwner)
	if memoryBacked(mode) && owner == nil {
		return domain.ErrVectorStorageUnavailable
	}
	// An ID-only cleanup that began before activation committed a reservation for
	// this lifetime. The qualified retry takes it over, keeping its hold and its
	// recorded backend, unless a live cleanup still owns its claim.
	tag, err := tx.Exec(ctx, `UPDATE document_deletions SET kind='qualified_intent',step=0,claim=NULL,claim_until=NULL,
		memory_owner=CASE WHEN vector_mode IN ('memory','memory_revision') THEN COALESCE(memory_owner,$4::uuid) END
		WHERE document_id=$1 AND incarnation=$2 AND kind='legacy_reservation' AND NOT finalized AND vector_mode=$3
		AND (claim_until IS NULL OR claim_until<=clock_timestamp())
		AND (memory_owner IS NULL OR memory_owner=$4::uuid)`, string(id), incarnation, mode, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		// Historical document-wide deletes cannot prove that no older write is
		// outstanding, so their identity stays held until it is reconciled.
		tag, err = tx.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,source_key,source_bucket,vector_mode,vector_index,held,memory_owner)
			VALUES($1,$2,'qualified_intent',$3,$4,$5,$6,$7,CASE WHEN $5::text IN ('memory','memory_revision') THEN $8::uuid END)
			ON CONFLICT(document_id) DO UPDATE SET incarnation=EXCLUDED.incarnation,kind=EXCLUDED.kind,source_key=EXCLUDED.source_key,
				source_bucket=EXCLUDED.source_bucket,vector_mode=EXCLUDED.vector_mode,vector_index=EXCLUDED.vector_index,
				memory_owner=EXCLUDED.memory_owner,step=0,held=EXCLUDED.held,finalized=FALSE,claim=NULL,claim_until=NULL
			WHERE document_deletions.finalized AND NOT document_deletions.held
				AND (document_deletions.claim_until IS NULL OR document_deletions.claim_until<=clock_timestamp())`,
			string(id), incarnation, source, bucket, mode, index, mode == legacyVectors, owner)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			// Another cleanup record owns this identity and is not yet released.
			return domain.ErrDeletionBusy
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE documents SET status='deleting',updated_at=NOW() WHERE id=$1`, string(id)); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM ingest_jobs WHERE document_id=$1`, string(id)); err != nil {
		return err
	}
	// Shared content must not match a document that is going away.
	if _, err = tx.Exec(ctx, `DELETE FROM document_fingerprints WHERE document_id=$1`, string(id)); err != nil {
		return err
	}
	return nil
}

// ownsManifests reports whether the document owns unretired manifested
// revisions. Only a node that enabled manifests and can delete by revision asks,
// so no other login reads the manifest tables.
func (c *Client) ownsManifests(ctx context.Context, tx pgx.Tx, id domain.DocumentID, ops domain.DeleteOperations) (bool, error) {
	if !c.hostedManifests.Load() || ops.ManifestVectors == nil {
		return false, nil
	}
	var owns bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM hosted_vector_manifests WHERE document_id=$1 AND state<>'retired')`, string(id)).Scan(&owns)
	if err != nil {
		return false, fmt.Errorf("postgres: read document manifests: %w", err)
	}
	return owns, nil
}

// deleteManifestVectors removes every unretired revision of the document by its
// recorded IDs. The document is already hidden and cannot gain a revision, so
// the list is stable across resumptions. Revisions that retire later reach the
// sweeper through the deletion trigger.
func (c *Client) deleteManifestVectors(ctx context.Context, id domain.DocumentID, ops domain.DeleteOperations) error {
	manifests, err := c.listManifests(ctx, id, "", true)
	if err != nil {
		return err
	}
	for _, m := range manifests {
		if err = ops.ManifestVectors(ctx, string(id), m.RevisionID, m.VectorIDs); err != nil {
			return fmt.Errorf("delete revision %s vectors: %w", m.RevisionID, err)
		}
	}
	return nil
}

// deletionDeadline returns the context bound and the time its claim may last.
func deletionDeadline(ctx context.Context) (context.Context, context.CancelFunc, time.Time) {
	if deadline, ok := ctx.Deadline(); ok {
		return ctx, func() {}, deadline
	}
	bounded, cancel := context.WithTimeout(ctx, deletionClaimLimit)
	deadline, _ := bounded.Deadline()
	return bounded, cancel, deadline
}

// resumeDeletion claims the receipt, runs its pending steps in order and
// finalizes the removal. Every step advances only under the current claim.
func (c *Client) resumeDeletion(ctx context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	ctx, cancel, deadline := deletionDeadline(ctx)
	defer cancel()
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return context.DeadlineExceeded
	}
	claim := uuid.NewString()
	var mode string
	var source, bucket, index *string
	var step int
	// A worker mutation claim that is still live may be writing vectors for this
	// document, so cleanup waits for it to finish or expire.
	err := c.Pool.QueryRow(ctx, `UPDATE document_deletions r SET claim=$3,claim_until=clock_timestamp()+make_interval(secs=>$4)
		WHERE r.document_id=$1 AND r.incarnation=$2 AND r.kind='qualified_intent' AND NOT r.finalized
		AND (r.claim_until IS NULL OR r.claim_until<=clock_timestamp())
		AND NOT EXISTS(SELECT 1 FROM documents d WHERE d.id=r.document_id AND d.vector_mutation_until>clock_timestamp())
		RETURNING r.vector_mode,r.source_key,r.source_bucket,r.vector_index,r.step`,
		string(id), incarnation, claim, remaining.Seconds()).Scan(&mode, &source, &bucket, &index, &step)
	if errors.Is(err, pgx.ErrNoRows) {
		return c.unclaimedDeletion(ctx, id, incarnation)
	}
	if err != nil {
		return err
	}
	defer c.releaseDeletionClaim(ctx, id, claim)
	if targetErr := checkDeletionTarget(mode, source, bucket, index, ops); targetErr != nil {
		return targetErr
	}
	for ; step < stepDone; step++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if stepErr := c.deleteArtifact(domain.TrackVectorMutation(ctx), string(id), incarnation, mode, source, step, ops); stepErr != nil {
			return stepErr
		}
		if checkpointErr := c.checkpointDeletion(ctx, id, incarnation, claim, step); checkpointErr != nil {
			return checkpointErr
		}
	}
	if err = c.finalizeDeletion(ctx, id, incarnation, claim); err != nil {
		return &finalDeletionError{err}
	}
	return nil
}

// unclaimedDeletion explains why no claim could be taken.
func (c *Client) unclaimedDeletion(ctx context.Context, id domain.DocumentID, incarnation string) error {
	var finalized bool
	err := c.Pool.QueryRow(ctx, `SELECT finalized FROM document_deletions WHERE document_id=$1 AND incarnation=$2`, string(id), incarnation).Scan(&finalized)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.ErrDocumentNotFound
	case err != nil:
		return err
	case finalized:
		return nil
	default:
		return domain.ErrDeletionBusy
	}
}

// checkDeletionTarget refuses stores that differ from the recorded target.
func checkDeletionTarget(mode string, source, bucket, index *string, ops domain.DeleteOperations) error {
	memory := mode == memoryVectors || mode == memoryRevision
	if mode != localVectors && memory != ops.InMemory {
		return domain.ErrDeletionTargetMismatch
	}
	switch mode {
	case legacyVectors, memoryVectors:
		if ops.LegacyNamespace == nil || ops.LegacyVectors == nil {
			return domain.ErrVectorStorageUnavailable
		}
	case revisionVectors, memoryRevision:
		if ops.RevisionVectors == nil {
			return domain.ErrVectorStorageUnavailable
		}
	case manifestVectors:
		if ops.ManifestVectors == nil {
			return domain.ErrVectorStorageUnavailable
		}
	}
	if source != nil {
		if ops.Source == nil {
			return domain.ErrFileStorageUnavailable
		}
		if deref(bucket) != ops.SourceBucket {
			return domain.ErrDeletionTargetMismatch
		}
	}
	if hostedVectors(mode) && deref(index) != ops.VectorIndex {
		return domain.ErrDeletionTargetMismatch
	}
	return nil
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (c *Client) deleteArtifact(ctx context.Context, id, incarnation, mode string, source *string, step int, ops domain.DeleteOperations) error {
	switch step {
	case stepNamespace:
		if mode == legacyVectors || mode == memoryVectors {
			return ops.LegacyNamespace(ctx, id)
		}
	case stepVectors:
		switch mode {
		case legacyVectors, memoryVectors:
			return ops.LegacyVectors(ctx, id)
		case revisionVectors, memoryRevision:
			return ops.RevisionVectors(ctx, id, incarnation)
		case manifestVectors:
			if !c.hostedManifests.Load() {
				// The metadata step's DELETE FROM documents fires
				// documents_retire_hosted_vectors, which hands every unretired
				// revision to the drained cleanup queue. A flag-enabled
				// replica, or scripts/reconcile -apply, then removes them.
				return nil
			}
			return c.deleteManifestVectors(ctx, domain.DocumentID(id), ops)
		}
	case stepSource:
		if source != nil {
			return ops.Source(ctx, *source)
		}
	}
	return nil
}

// checkpointDeletion records an acknowledged step even when the request
// deadline expired while the provider replied.
func (c *Client) checkpointDeletion(ctx context.Context, id domain.DocumentID, incarnation, claim string, step int) error {
	checkpointCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	tag, err := c.Pool.Exec(checkpointCtx, `UPDATE document_deletions SET step=$5 WHERE document_id=$1 AND incarnation=$2
		AND claim=$3 AND step=$4 AND claim_until>clock_timestamp()`, string(id), incarnation, claim, step, step+1)
	if err != nil {
		return fmt.Errorf("record cleanup checkpoint: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrDeletionBusy
	}
	return nil
}

func (c *Client) releaseDeletionClaim(ctx context.Context, id domain.DocumentID, claim string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	_, _ = c.Pool.Exec(releaseCtx, `UPDATE document_deletions SET claim=NULL,claim_until=NULL
		WHERE document_id=$1 AND claim=$2`, string(id), claim)
}

// finalizeDeletion removes the document and completes its receipt in one
// transaction. A worker write that was never confirmed keeps the identity held.
func (c *Client) finalizeDeletion(ctx context.Context, id domain.DocumentID, incarnation, claim string) error {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var uncertain bool
	err = tx.QueryRow(ctx, `DELETE FROM documents d WHERE d.id=$1 AND COALESCE(d.incarnation,d.id)=$2 AND d.status='deleting'
		AND EXISTS (SELECT 1 FROM document_deletions r WHERE r.document_id=d.id AND r.incarnation=$2 AND r.step=$4
			AND NOT r.finalized AND r.claim=$3 AND r.claim_until>clock_timestamp())
		RETURNING d.vector_mutation_pending OR d.vector_mutation_uncertain`, string(id), incarnation, claim, stepDone).Scan(&uncertain)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrDeletionBusy
	}
	if err != nil {
		return fmt.Errorf("finalize document deletion: %w", err)
	}
	tag, err := tx.Exec(ctx, `UPDATE document_deletions SET held=held OR $4,finalized=TRUE,
		source_key=CASE WHEN held OR $4 THEN source_key ELSE NULL END,claim=NULL,claim_until=NULL
		WHERE document_id=$1 AND incarnation=$2 AND claim=$3`, string(id), incarnation, claim, uncertain)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrDeletionBusy
	}
	return tx.Commit(ctx)
}

// WithDocumentFile signs the stored source while a deletion intent for the
// document cannot commit. The shared advisory key serializes signing with the
// intent without writing to the row, and the read-only transaction keeps it so.
func (c *Client) WithDocumentFile(ctx context.Context, id domain.DocumentID, sign func(context.Context, string) error) (bool, error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	key, found, err := readSignableSource(ctx, tx, string(id))
	if err != nil || !found {
		return false, err
	}
	if signErr := sign(ctx, key); signErr != nil {
		return false, signErr
	}
	return true, tx.Commit(ctx)
}

// readSignableSource takes the shared intent key and then reads the source in
// a separate statement, so the read observes an intent that committed while
// the key was awaited. One statement would keep its pre-wait snapshot.
func readSignableSource(ctx context.Context, tx pgx.Tx, id string) (key string, found bool, err error) {
	batch := &pgx.Batch{}
	batch.Queue(`SELECT pg_advisory_xact_lock_shared(`+documentIntentLockKey+`)`, id)
	batch.Queue(`SELECT s3_key FROM documents WHERE id=$1 AND status<>'deleting' AND s3_key IS NOT NULL`, id)
	results := tx.SendBatch(ctx, batch)
	_, lockErr := results.Exec()
	readErr := lockErr
	if lockErr == nil {
		readErr = results.QueryRow().Scan(&key)
	}
	closeErr := results.Close()
	switch {
	case lockErr != nil:
		return "", false, lockErr
	case errors.Is(readErr, pgx.ErrNoRows):
		return "", false, closeErr
	case readErr != nil:
		return "", false, readErr
	default:
		return key, true, closeErr
	}
}

// WithScopedDocumentFile verifies active principal status and owner or
// read-plus-download access under shared row locks while the caller signs.
func (c *Client) WithScopedDocumentFile(ctx context.Context, principal domain.Principal, id domain.DocumentID, sign func(context.Context, string) error) (domain.ScopedFileResult, error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.ScopedFileResult{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var active bool
	var key *string
	err = tx.QueryRow(ctx, `SELECT active, s3_key FROM grant_catalog_authorize_file($1::uuid,$2::uuid,$3::uuid)`,
		principal.TenantID, principal.ID, string(id)).Scan(&active, &key)
	if err != nil {
		return domain.ScopedFileResult{}, err
	}
	if !active {
		return domain.ScopedFileResult{Active: false}, nil
	}
	if key == nil {
		return domain.ScopedFileResult{Active: true, Found: false}, nil
	}
	if signErr := sign(ctx, *key); signErr != nil {
		return domain.ScopedFileResult{Active: true}, signErr
	}
	if auditErr := recordSecurityAuditTx(ctx, tx, domain.SecurityAuditEvent{
		RequestID:   logctx.RequestID(ctx),
		TenantID:    principal.TenantID,
		PrincipalID: principal.ID,
		Action:      "file_sign",
		Outcome:     "allow",
		Reason:      "ok",
		DocumentID:  string(id),
	}); auditErr != nil {
		return domain.ScopedFileResult{}, auditErr
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ScopedFileResult{}, err
	}
	return domain.ScopedFileResult{Active: true, Found: true}, nil
}

// BeginScopedDeletion atomically verifies owner-only deletion against the live
// incarnation and persists the checkpointed cleanup intent.
func (c *Client) BeginScopedDeletion(ctx context.Context, principal domain.Principal, validUntil time.Time, id domain.DocumentID, ops domain.DeleteOperations) (incarnation string, complete bool, err error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return "", false, domain.ErrDocumentNotFound
	}
	id = domain.DocumentID(parsed.String())
	expected, err := canonicalIncarnation(ops.ExpectedIncarnation)
	if err != nil {
		return "", false, err
	}
	if !time.Now().Before(validUntil) {
		return "", false, domain.ErrUnauthorized
	}
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if lockErr := lockDocumentIntent(ctx, tx, id); lockErr != nil {
		return "", false, lockErr
	}

	var state string
	var source *string
	var scoped bool
	docErr := tx.QueryRow(ctx, `SELECT COALESCE(incarnation,id)::text,status,s3_key,scoped_vectors FROM documents WHERE id=$1 FOR UPDATE`, string(id)).
		Scan(&incarnation, &state, &source, &scoped)
	if docErr != nil && !errors.Is(docErr, pgx.ErrNoRows) {
		return "", false, docErr
	}

	var active, authorized, finalizedMatch bool
	if authErr := tx.QueryRow(ctx, `SELECT active, authorized, finalized_match FROM grant_catalog_authorize_delete($1::uuid,$2::uuid,$3::uuid,$4::uuid)`,
		principal.TenantID, principal.ID, string(id), nullable(expected)).Scan(&active, &authorized, &finalizedMatch); authErr != nil {
		return "", false, authErr
	}
	if !active {
		return "", false, domain.ErrForbidden
	}
	if errors.Is(docErr, pgx.ErrNoRows) {
		if !finalizedMatch {
			return "", false, domain.ErrDocumentNotFound
		}
		if !time.Now().Before(validUntil) {
			return "", false, domain.ErrUnauthorized
		}
		return expected, true, nil
	}
	if !authorized {
		return "", false, domain.ErrDocumentNotFound
	}
	if !time.Now().Before(validUntil) {
		return "", false, domain.ErrUnauthorized
	}
	if parsed.Version() == 5 {
		var reserved bool
		if reserveErr := tx.QueryRow(ctx, `SELECT evaluation_fixture_reserved($1)`, parsed.String()).Scan(&reserved); reserveErr != nil {
			return "", false, reserveErr
		}
		if reserved {
			return "", false, domain.ErrFixtureDeletionRequired
		}
	}
	if expected != "" && expected != incarnation {
		return "", false, domain.ErrDeletionPreconditionFailed
	}
	if source != nil && ops.Source == nil {
		return "", false, domain.ErrFileStorageUnavailable
	}
	manifested, err := c.ownsManifests(ctx, tx, id, ops)
	if err != nil {
		return "", false, err
	}
	mode, err := vectorMode(scoped, manifested, ops)
	if err != nil {
		return "", false, err
	}
	if state == "deleting" {
		if _, recErr := tx.Exec(ctx, `SELECT grant_catalog_record_deletion_owner($1::uuid,$2::uuid,$3::uuid,$4::uuid)`,
			principal.TenantID, principal.ID, string(id), incarnation); recErr != nil {
			return "", false, recErr
		}
		if auditErr := recordSecurityAuditTx(ctx, tx, domain.SecurityAuditEvent{
			RequestID:   logctx.RequestID(ctx),
			TenantID:    principal.TenantID,
			PrincipalID: principal.ID,
			Action:      "document_delete",
			Outcome:     "allow",
			Reason:      "intent_recorded",
			DocumentID:  string(id),
			Incarnation: incarnation,
		}); auditErr != nil {
			return "", false, auditErr
		}
		return incarnation, false, tx.Commit(ctx)
	}
	if intentErr := insertDeletionIntentTx(ctx, tx, id, incarnation, source, mode, ops); intentErr != nil {
		return "", false, intentErr
	}
	if _, err = tx.Exec(ctx, `SELECT grant_catalog_record_deletion_owner($1::uuid,$2::uuid,$3::uuid,$4::uuid)`,
		principal.TenantID, principal.ID, string(id), incarnation); err != nil {
		return "", false, err
	}
	if auditErr := recordSecurityAuditTx(ctx, tx, domain.SecurityAuditEvent{
		RequestID:   logctx.RequestID(ctx),
		TenantID:    principal.TenantID,
		PrincipalID: principal.ID,
		Action:      "document_delete",
		Outcome:     "allow",
		Reason:      "intent_recorded",
		DocumentID:  string(id),
		Incarnation: incarnation,
	}); auditErr != nil {
		return "", false, auditErr
	}
	return incarnation, false, tx.Commit(ctx)
}

// ResumeScopedDeletion finishes detached cross-store cleanup for an already
// authorized deletion intent.
func (c *Client) ResumeScopedDeletion(ctx context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return domain.ErrDocumentNotFound
	}
	return c.resumeObservedDeletion(ctx, domain.DocumentID(parsed.String()), incarnation, ops)
}
