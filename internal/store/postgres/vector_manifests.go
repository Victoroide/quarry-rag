package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
)

var _ domain.VectorManifestStore = (*Client)(nil)

// PersistVectorManifest records a write-intent manifest for the owned job lease
// before any external vector batch is dispatched.
func (c *Client) PersistVectorManifest(ctx context.Context, j domain.Job, m domain.VectorManifest) error {
	if m.RevisionID == "" {
		return errors.New("postgres: vector manifest revision_id is required")
	}
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: persist vector manifest: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var locked int
	err = tx.QueryRow(ctx, `SELECT 1 FROM documents WHERE id=$1 AND COALESCE(incarnation,id)=$2 AND status='processing' FOR UPDATE`,
		string(j.Document.ID), j.Incarnation).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrJobNotOwned
	}
	if err != nil {
		return fmt.Errorf("postgres: lock document for manifest: %w", err)
	}
	err = tx.QueryRow(ctx, `SELECT 1 FROM ingest_jobs WHERE `+jobLive,
		string(j.Document.ID), j.LeaseOwner, j.Attempts, j.LeaseToken).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrJobNotOwned
	}
	if err != nil {
		return fmt.Errorf("postgres: verify lease for manifest: %w", err)
	}
	chunkIDs := m.ChunkIDs
	if chunkIDs == nil {
		chunkIDs = []string{}
	}
	vectorIDs := m.VectorIDs
	if vectorIDs == nil {
		vectorIDs = []string{}
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO hosted_vector_manifests (
			revision_id, document_id, incarnation, attempt, lease_token,
			collection, vector_namespace, id_prefix, vector_ids, chunk_ids, state
		) VALUES ($1, $2, $3, $4, NULLIF($5, '')::uuid, $6, $7, $8, $9, $10::uuid[], 'writing')
		ON CONFLICT (revision_id) DO NOTHING`,
		m.RevisionID, string(j.Document.ID), j.Incarnation, m.Attempt, m.LeaseToken,
		m.Collection, m.VectorNamespace, m.IDPrefix, vectorIDs, chunkIDs)
	if err != nil {
		return fmt.Errorf("postgres: insert vector manifest: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit vector manifest: %w", err)
	}
	return nil
}

// GetVectorManifest returns the persisted manifest for revisionID, or nil if absent.
func (c *Client) GetVectorManifest(ctx context.Context, revisionID string) (*domain.VectorManifest, error) {
	var m domain.VectorManifest
	var docID string
	err := c.Pool.QueryRow(ctx, `
		SELECT revision_id::text, document_id::text, incarnation::text, attempt, COALESCE(lease_token::text, ''),
		       collection, vector_namespace, id_prefix, vector_ids,
		       ARRAY(SELECT x::text FROM unnest(chunk_ids) AS x), state
		FROM hosted_vector_manifests
		WHERE revision_id = $1`, revisionID).
		Scan(&m.RevisionID, &docID, &m.Incarnation, &m.Attempt, &m.LeaseToken,
			&m.Collection, &m.VectorNamespace, &m.IDPrefix, &m.VectorIDs, &m.ChunkIDs, &m.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get vector manifest: %w", err)
	}
	m.DocumentID = domain.DocumentID(docID)
	return &m, nil
}

// ListDocumentVectorManifests returns manifests recorded for a document,
// optionally filtered by incarnation. A node that did not enable manifests
// reports none without reading the table; deletion on such a node removes
// vectors by document and leaves retired manifests to the sweeping nodes.
func (c *Client) ListDocumentVectorManifests(ctx context.Context, docID domain.DocumentID, incarnation string) ([]domain.VectorManifest, error) {
	if !c.hostedManifests.Load() {
		return nil, nil
	}
	return c.listManifests(ctx, docID, incarnation, false)
}

func (c *Client) listManifests(ctx context.Context, docID domain.DocumentID, incarnation string, unretiredOnly bool) ([]domain.VectorManifest, error) {
	rows, err := c.Pool.Query(ctx, `
		SELECT revision_id::text, document_id::text, incarnation::text, attempt, COALESCE(lease_token::text, ''),
		       collection, vector_namespace, id_prefix, vector_ids,
		       ARRAY(SELECT x::text FROM unnest(chunk_ids) AS x), state
		FROM hosted_vector_manifests
		WHERE document_id = $1 AND ($2 = '' OR incarnation = NULLIF($2, '')::uuid)
		  AND (NOT $3 OR state <> 'retired')
		ORDER BY created_at, attempt`, string(docID), incarnation, unretiredOnly)
	if err != nil {
		return nil, fmt.Errorf("postgres: list document vector manifests: %w", err)
	}
	defer rows.Close()
	var manifests []domain.VectorManifest
	for rows.Next() {
		var m domain.VectorManifest
		var id string
		if err := rows.Scan(&m.RevisionID, &id, &m.Incarnation, &m.Attempt, &m.LeaseToken,
			&m.Collection, &m.VectorNamespace, &m.IDPrefix, &m.VectorIDs, &m.ChunkIDs, &m.State); err != nil {
			return nil, fmt.Errorf("postgres: scan vector manifest: %w", err)
		}
		m.DocumentID = domain.DocumentID(id)
		manifests = append(manifests, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list document vector manifests: %w", err)
	}
	return manifests, nil
}

// ClaimVectorCleanup leases one ready tombstone for bounded sweeping. A tombstone
// becomes claimable only after drain has passed since its revision was retired,
// so every retirement path honors the same window, whichever node retired it.
func (c *Client) ClaimVectorCleanup(ctx context.Context, owner string, lease, drain time.Duration) (*domain.VectorCleanupTask, error) {
	var task domain.VectorCleanupTask
	var docID string
	var lastErr *string
	err := c.Pool.QueryRow(ctx, `
		WITH ready AS (
			SELECT c.revision_id
			FROM hosted_vector_cleanups c
			WHERE c.retired_at + make_interval(secs => $3) <= clock_timestamp()
			  AND c.next_attempt_at <= clock_timestamp()
			  AND c.state IN ('pending', 'verifying', 'unresolved')
			  AND (c.lease_until IS NULL OR c.lease_until < clock_timestamp())
			  AND NOT EXISTS (
				SELECT 1 FROM hosted_vector_revisions r WHERE r.active_revision_id = c.revision_id
			  )
			  AND NOT EXISTS (
				SELECT 1
				FROM ingest_jobs j
				JOIN documents d ON d.id = j.document_id
				JOIN hosted_vector_manifests m
				  ON m.document_id = d.id
				 AND m.incarnation = COALESCE(d.incarnation, d.id)
				 AND m.attempt = j.attempts
				 AND m.lease_token IS NOT DISTINCT FROM j.lease_token
				WHERE m.revision_id = c.revision_id
				  AND j.state = 'running'
				  AND j.lease_until > clock_timestamp()
			  )
			ORDER BY c.next_attempt_at, c.retired_at, c.revision_id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE hosted_vector_cleanups c
		   SET lease_owner = $1,
		       lease_token = gen_random_uuid(),
		       lease_until = clock_timestamp() + make_interval(secs => $2)
		  FROM ready
		 WHERE c.revision_id = ready.revision_id
		RETURNING c.revision_id::text, c.document_id::text, c.incarnation::text,
		          c.collection, c.vector_namespace, c.id_prefix, c.vector_ids,
		          c.next_offset, c.state, c.attempts, c.convergence_checks,
		          c.last_error, c.lease_owner, c.lease_token::text,
		          c.next_attempt_at, c.retired_at, c.reason`,
		owner, lease.Seconds(), max(drain, 0).Seconds()).
		Scan(&task.RevisionID, &docID, &task.Incarnation,
			&task.Collection, &task.VectorNamespace, &task.IDPrefix, &task.VectorIDs,
			&task.NextOffset, &task.State, &task.Attempts, &task.ConvergenceChecks,
			&lastErr, &task.LeaseOwner, &task.LeaseToken,
			&task.NextAttemptAt, &task.RetiredAt, &task.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: claim vector cleanup: %w", err)
	}
	task.DocumentID = domain.DocumentID(docID)
	if lastErr != nil {
		task.LastError = *lastErr
	}
	return &task, nil
}

const cleanupLeaseMatch = `revision_id=$1 AND lease_owner=$2 AND lease_token IS NOT DISTINCT FROM $3 AND lease_until>clock_timestamp()`

// RenewVectorCleanup extends an active cleanup lease without reviving an expired one.
func (c *Client) RenewVectorCleanup(ctx context.Context, task domain.VectorCleanupTask, lease time.Duration) (bool, error) {
	tag, err := c.Pool.Exec(ctx, `UPDATE hosted_vector_cleanups SET lease_until=clock_timestamp()+make_interval(secs=>$4) WHERE `+cleanupLeaseMatch,
		task.RevisionID, task.LeaseOwner, task.LeaseToken, lease.Seconds())
	if err != nil {
		return false, fmt.Errorf("postgres: renew vector cleanup: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// AdvanceVectorCleanup persists page cursor progress under a live cleanup lease.
func (c *Client) AdvanceVectorCleanup(ctx context.Context, task domain.VectorCleanupTask, nextOffset int) (bool, error) {
	tag, err := c.Pool.Exec(ctx, `UPDATE hosted_vector_cleanups SET next_offset=$4 WHERE `+cleanupLeaseMatch,
		task.RevisionID, task.LeaseOwner, task.LeaseToken, nextOffset)
	if err != nil {
		return false, fmt.Errorf("postgres: advance vector cleanup: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// DeferVectorCleanup reschedules a tombstone for convergence verification or retry backoff.
func (c *Client) DeferVectorCleanup(ctx context.Context, task domain.VectorCleanupTask, state string, delay time.Duration, convergenceChecks int, lastError string) (bool, error) {
	msg := ""
	if lastError != "" {
		msg = jobError(lastError)
	}
	tag, err := c.Pool.Exec(ctx, `
		UPDATE hosted_vector_cleanups
		   SET state = $4,
		       next_attempt_at = clock_timestamp() + make_interval(secs => $5),
		       convergence_checks = $6,
		       attempts = attempts + 1,
		       last_error = NULLIF($7, ''),
		       lease_owner = NULL,
		       lease_token = NULL,
		       lease_until = NULL
		 WHERE `+cleanupLeaseMatch,
		task.RevisionID, task.LeaseOwner, task.LeaseToken, state, delay.Seconds(), convergenceChecks, msg)
	if err != nil {
		return false, fmt.Errorf("postgres: defer vector cleanup: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// CompleteVectorCleanup removes a converged tombstone and its retired manifest
// under the live cleanup lease. Nothing refers to a retired manifest once its
// vectors are verified gone, so keeping it would only grow the table.
func (c *Client) CompleteVectorCleanup(ctx context.Context, task domain.VectorCleanupTask) (bool, error) {
	var completed bool
	err := c.Pool.QueryRow(ctx, `
		WITH done AS (
			DELETE FROM hosted_vector_cleanups WHERE `+cleanupLeaseMatch+`
			RETURNING revision_id
		), purged AS (
			DELETE FROM hosted_vector_manifests m USING done
			 WHERE m.revision_id = done.revision_id AND m.state = 'retired'
		)
		SELECT EXISTS (SELECT 1 FROM done)`,
		task.RevisionID, task.LeaseOwner, task.LeaseToken).Scan(&completed)
	if err != nil {
		return false, fmt.Errorf("postgres: complete vector cleanup: %w", err)
	}
	return completed, nil
}

// ReleaseVectorCleanup drops the cleanup lease without advancing attempts on shutdown.
func (c *Client) ReleaseVectorCleanup(ctx context.Context, task domain.VectorCleanupTask) error {
	_, err := c.Pool.Exec(ctx, `
		UPDATE hosted_vector_cleanups
		   SET lease_owner = NULL,
		       lease_token = NULL,
		       lease_until = NULL
		 WHERE revision_id=$1 AND lease_owner=$2 AND lease_token IS NOT DISTINCT FROM $3`,
		task.RevisionID, task.LeaseOwner, task.LeaseToken)
	if err != nil {
		return fmt.Errorf("postgres: release vector cleanup: %w", err)
	}
	return nil
}

// ListPendingVectorCleanups returns outstanding cleanup tombstones for dry-run inspection.
func (c *Client) ListPendingVectorCleanups(ctx context.Context, limit int) ([]domain.VectorCleanupTask, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := c.Pool.Query(ctx, `
		SELECT revision_id::text, document_id::text, incarnation::text,
		       collection, vector_namespace, id_prefix, vector_ids,
		       next_offset, state, attempts, convergence_checks,
		       last_error, next_attempt_at, retired_at, reason
		FROM hosted_vector_cleanups
		ORDER BY retired_at, revision_id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list vector cleanups: %w", err)
	}
	defer rows.Close()
	var tasks []domain.VectorCleanupTask
	for rows.Next() {
		var task domain.VectorCleanupTask
		var docID string
		var lastErr *string
		if err := rows.Scan(&task.RevisionID, &docID, &task.Incarnation,
			&task.Collection, &task.VectorNamespace, &task.IDPrefix, &task.VectorIDs,
			&task.NextOffset, &task.State, &task.Attempts, &task.ConvergenceChecks,
			&lastErr, &task.NextAttemptAt, &task.RetiredAt, &task.Reason); err != nil {
			return nil, fmt.Errorf("postgres: scan vector cleanup: %w", err)
		}
		task.DocumentID = domain.DocumentID(docID)
		if lastErr != nil {
			task.LastError = *lastErr
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list vector cleanups: %w", err)
	}
	return tasks, nil
}
