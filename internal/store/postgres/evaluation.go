package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
)

var _ domain.EvaluationFixtureStore = (*Client)(nil)

// ReadEvaluationFixture identifies the reservation without exposing another receipt.
func (c *Client) ReadEvaluationFixture(ctx context.Context, id domain.DocumentID, receipt string) (domain.EvaluationFixtureStatus, error) {
	return readEvaluationFixture(ctx, c.Pool, id, receipt)
}

type fixtureStatusQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readEvaluationFixture(ctx context.Context, source fixtureStatusQuery, id domain.DocumentID, receipt string) (domain.EvaluationFixtureStatus, error) {
	var s domain.EvaluationFixtureStatus
	err := source.QueryRow(ctx, `
		SELECT CASE
			WHEN f.receipt = $2 AND f.incarnation IS NULL THEN 'retired'
			WHEN f.receipt = $2 AND f.active_document_id IS NULL AND f.vector_identity IS NULL THEN 'store_mismatch'
			WHEN f.receipt = $2 AND f.active_document_id IS NULL THEN 'deleted'
			WHEN f.receipt = $2 AND f.active_document_id=d.id AND d.incarnation = f.incarnation THEN 'owned'
			WHEN f.document_id IS NOT NULL OR d.id IS NOT NULL THEN 'borrowed'
			ELSE 'missing'
		END, CASE WHEN f.receipt = $2 THEN COALESCE(f.incarnation::text, '') ELSE '' END,
		clock_timestamp()
		FROM (SELECT $1::uuid AS id) requested
		LEFT JOIN evaluation_fixtures f ON f.document_id = requested.id
		LEFT JOIN documents d ON d.id = requested.id`, string(id), receipt).Scan(&s.State, &s.Incarnation, &s.DatabaseTime)
	if err != nil {
		return s, fmt.Errorf("postgres: read evaluation fixture: %w", err)
	}
	return s, nil
}

// ReconcileEvaluationFixture retires absent intent under the same fence as admission.
func (c *Client) ReconcileEvaluationFixture(ctx context.Context, id domain.DocumentID, receipt string) (domain.EvaluationFixtureStatus, error) {
	var s domain.EvaluationFixtureStatus
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return s, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
		return s, err
	}
	if lockErr := lockFixtureDocument(ctx, tx, string(id)); lockErr != nil {
		return s, lockErr
	}
	s, err = readEvaluationFixture(ctx, tx, id, receipt)
	if err != nil || s.State != domain.FixtureMissing && s.State != domain.FixtureBorrowed {
		return s, err
	}
	// An ordinary document can be removed before a delayed admission arrives.
	// Fence the fixture intent even if that document currently occupies the ID.
	tag, err := tx.Exec(ctx, `INSERT INTO evaluation_fixtures(document_id,receipt)
		VALUES($1,$2) ON CONFLICT(document_id) DO NOTHING`, string(id), receipt)
	if err != nil {
		return domain.EvaluationFixtureStatus{}, err
	}
	if tag.RowsAffected() == 0 {
		// A durable fixture reservation already blocks any late admission.
		return s, nil
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return domain.EvaluationFixtureStatus{}, commitErr
	}
	return domain.EvaluationFixtureStatus{State: domain.FixtureRetired}, nil
}

// DeleteEvaluationFixture locks the receipt and any pending job through cleanup.
// The UUID remains reserved even after completion or a lost database connection.
func (c *Client) DeleteEvaluationFixture(ctx context.Context, id domain.DocumentID, receipt, incarnation, vectorIdentity string, sweep func(context.Context) error) (string, error) {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	args := []any{string(id), receipt, incarnation}
	var done bool
	var boundStore *string
	err = tx.QueryRow(ctx, `
		SELECT active_document_id IS NULL, vector_identity FROM evaluation_fixtures
		WHERE document_id=$1 AND receipt=$2 AND incarnation=$3 FOR UPDATE`, args...).Scan(&done, &boundStore)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.FixtureBorrowed, nil
	}
	if err != nil {
		return "", err
	}
	if done && boundStore != nil {
		return domain.FixtureDeleted, nil
	}
	if vectorIdentity == "" || boundStore == nil || *boundStore != vectorIdentity {
		return domain.FixtureWrongStore, nil
	}

	// Inspect the job before locking the document. A running job may be
	// completing against the document row, so defer deleting it. Locking a
	// pending job prevents it from being claimed during the sweep.
	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM ingest_jobs WHERE document_id=$1 FOR UPDATE`, string(id)).Scan(&state)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if state == "running" {
		return domain.FixtureBusy, nil
	}

	var current string
	var source *string
	err = tx.QueryRow(ctx, `SELECT incarnation::text, s3_key FROM documents WHERE id=$1 FOR UPDATE`, string(id)).Scan(&current, &source)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && (current != incarnation || source != nil) {
		return domain.FixtureBorrowed, nil
	}
	if err != nil {
		return "", err
	}
	if sweepErr := sweep(ctx); sweepErr != nil {
		return "", fmt.Errorf("sweep evaluation vectors: %w", sweepErr)
	}
	if _, updateErr := tx.Exec(ctx, `UPDATE evaluation_fixtures SET active_document_id=NULL
		WHERE document_id=$1 AND receipt=$2 AND incarnation=$3`, args...); updateErr != nil {
		return "", updateErr
	}
	if _, err = tx.Exec(ctx, `DELETE FROM documents WHERE id=$1 AND incarnation=$2`, string(id), incarnation); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return domain.FixtureDeleted, nil
}
