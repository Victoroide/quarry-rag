package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// MutateJobVectors binds the job to each dispatched vector request.
func (c *Client) MutateJobVectors(ctx context.Context, job domain.Job, mutate func(context.Context) error) error {
	if !job.ScopedVectors {
		ctx = domain.WithVectorIncarnation(ctx, job.Incarnation)
	}
	dispatch := func(ctx context.Context, deletes bool, call func(context.Context) error) error {
		return c.mutateJobVectors(ctx, job, deletes, call)
	}
	return mutate(domain.WithVectorDispatch(domain.TrackVectorMutation(ctx), dispatch))
}

func (c *Client) mutateJobVectors(ctx context.Context, job domain.Job, deletes bool, mutate func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	deadline, _ := ctx.Deadline()
	claim := uuid.NewString()
	if err := c.claimJobVectorMutation(ctx, job, claim, deadline, deletes); err != nil {
		return err
	}

	callErr := ctx.Err()
	unknown := false
	if callErr == nil {
		callErr = mutate(domain.TrackVectorMutation(ctx))
		unknown = domain.VectorMutationUncertain(callErr)
	}
	checkpointCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer stop()
	var expired bool
	err := c.Pool.QueryRow(checkpointCtx, `WITH owned AS MATERIALIZED (
		SELECT id,vector_mutation_until FROM documents
		WHERE id=$1 AND COALESCE(incarnation,id)=$2 AND vector_mutation_claim=$3 FOR UPDATE
	), outcome AS MATERIALIZED (
		SELECT id,vector_mutation_until<=clock_timestamp() AS expired FROM owned
	) UPDATE documents d SET vector_mutation_pending=FALSE,vector_mutation_claim=NULL,vector_mutation_until=NULL,
		vector_mutation_uncertain=d.vector_mutation_uncertain OR $4 OR outcome.expired,
		vector_delete_uncertain=d.vector_delete_uncertain OR (d.vector_mutation_deletes AND ($4 OR outcome.expired)),
		vector_mutation_deletes=FALSE
		FROM outcome WHERE d.id=outcome.id RETURNING outcome.expired`, job.Document.ID, job.Incarnation, claim, unknown).Scan(&expired)
	if errors.Is(err, pgx.ErrNoRows) || expired {
		err = domain.ErrJobNotOwned
	}
	return errors.Join(callErr, err)
}

func (c *Client) claimJobVectorMutation(ctx context.Context, job domain.Job, claim string, deadline time.Time, deletes bool) error {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if lockErr := lockLiveJob(ctx, tx, job); lockErr != nil {
		return lockErr
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return context.DeadlineExceeded
	}
	tag, err := tx.Exec(ctx, `UPDATE documents d SET
		vector_mutation_uncertain=d.vector_mutation_uncertain OR d.vector_mutation_pending,
		vector_delete_uncertain=d.vector_delete_uncertain OR (d.vector_mutation_pending AND d.vector_mutation_deletes),
		vector_mutation_pending=TRUE,vector_mutation_deletes=$4,vector_mutation_claim=$2,
		vector_mutation_until=clock_timestamp()+make_interval(secs=>$3)
		WHERE d.id=$1 AND (d.vector_mutation_until IS NULL OR d.vector_mutation_until<=clock_timestamp())
		AND EXISTS (SELECT 1 FROM ingest_jobs j WHERE j.document_id=d.id AND j.lease_until>clock_timestamp())`,
		job.Document.ID, claim, remaining.Seconds(), deletes)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrJobNotOwned
	}
	return tx.Commit(ctx)
}

// settleExpiredVectorMutation retires an abandoned claim while the caller holds the document lock.
func settleExpiredVectorMutation(ctx context.Context, tx pgx.Tx, id domain.DocumentID) (bool, error) {
	tag, err := tx.Exec(ctx, `UPDATE documents SET vector_mutation_pending=FALSE,
		vector_mutation_claim=NULL,vector_mutation_until=NULL,vector_mutation_deletes=FALSE,
		vector_mutation_uncertain=TRUE,vector_delete_uncertain=vector_delete_uncertain OR vector_mutation_deletes
		WHERE id=$1 AND vector_mutation_pending AND vector_mutation_until<=clock_timestamp()`, id)
	return tag.RowsAffected() == 1, err
}
