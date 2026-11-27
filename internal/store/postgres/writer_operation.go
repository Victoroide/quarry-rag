package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var writerOperationSender = sync.OnceValue(uuid.New)

const writerOperationCompletionTimeout = 5 * time.Second

// ErrWriterOperationsPending identifies the bounded set of reservations
// preventing a freeze. The count includes reservations beyond the sample.
type ErrWriterOperationsPending struct {
	Count        int64
	OperationIDs []uuid.UUID
}

func (e *ErrWriterOperationsPending) Error() string {
	return fmt.Sprintf("postgres: %d writer operations pending (sample %v); retry after settlement", e.Count, e.OperationIDs)
}

// WriterOperationFinishError reports an unconfirmed reservation finish.
// Operators can look up the exact operation in the journal.
type WriterOperationFinishError struct {
	OperationID uuid.UUID
	err         error
}

func (e *WriterOperationFinishError) Error() string {
	return "postgres: writer operation finish unconfirmed"
}

func (e *WriterOperationFinishError) Unwrap() error { return e.err }

func writerOperationSupported(ctx context.Context, tx pgx.Tx) (bool, error) {
	var function, table, column bool
	err := tx.QueryRow(ctx, `SELECT to_regprocedure(format('%I.vector_writer_operations_enabled()', current_schema())) IS NOT NULL,
		to_regclass(format('%I.vector_writer_operations', current_schema())) IS NOT NULL,
		EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(format('%I.vector_state', current_schema()))
			AND attname='writer_operations_enabled' AND NOT attisdropped)`).Scan(&function, &table, &column)
	if err != nil {
		return false, err
	}
	if function != table || function != column {
		return false, errors.New("postgres: incomplete writer operation schema")
	}
	return function, nil
}

func checkWriterOperationsDrained(ctx context.Context, tx pgx.Tx) error {
	var pending ErrWriterOperationsPending
	if err := tx.QueryRow(ctx, `SELECT pending_count, operation_ids FROM vector_writer_operation_drain_status()`).
		Scan(&pending.Count, &pending.OperationIDs); err != nil {
		return err
	}
	if pending.Count != 0 {
		return &pending
	}
	return nil
}

// RunWriterOperation admits work under the legacy session barrier or, when
// enabled, commits a reservation that survives the loss of that session.
func (c *Client) RunWriterOperation(ctx context.Context, kind domain.WriterOperationKind, target string, work func(context.Context) error) error {
	return c.runWriterOperation(ctx, kind, target, c.gateWriter, func(ctx context.Context, _ pgx.Tx) error {
		return work(ctx)
	})
}

func (c *Client) runWriterOperation(ctx context.Context, kind domain.WriterOperationKind, target string, check writerTxCheck, work func(context.Context, pgx.Tx) error) error {
	guard, closeGuard, returnSlot, err := c.openWriterSession(ctx, check)
	if err != nil {
		return err
	}
	defer returnSlot()
	defer closeGuard()

	present, err := writerOperationSupported(ctx, guard)
	if err != nil {
		return fmt.Errorf("postgres: check writer operation support: %w", err)
	}
	var enabled bool
	if present {
		if err = guard.QueryRow(ctx, `SELECT vector_writer_operations_enabled()`).Scan(&enabled); err != nil {
			return fmt.Errorf("postgres: check writer operation mode: %w", err)
		}
	}
	if !enabled {
		return work(ctx, guard)
	}
	op, sender := uuid.New(), writerOperationSender()
	var enrolled bool
	if err = guard.QueryRow(ctx, `SELECT vector_writer_operation_begin($1,$2,$3,$4,$5)`,
		op, sender, c.servingPin().generation, string(kind), target).Scan(&enrolled); err != nil {
		return fmt.Errorf("postgres: register writer operation: %w", err)
	}
	if !enrolled {
		return errors.New("postgres: writer operation registration refused")
	}
	if err = guard.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: confirm writer operation registration: %w", err)
	}
	closeGuard()

	state := externalWriterState{mark: func(ctx context.Context) error {
		return c.confirmWriterOperationChange(ctx, `SELECT vector_writer_operation_mark($1,$2)`, op, sender)
	}}
	defer state.close()
	workErr := ctx.Err()
	if workErr == nil {
		workErr = work(domain.WithExternalWriterDispatch(ctx, state.dispatch), nil)
	}
	if !state.close() {
		return workErr
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writerOperationCompletionTimeout)
	defer cancel()
	if err = c.confirmWriterOperationChange(finishCtx, `SELECT vector_writer_operation_finish($1,$2)`, op, sender); err != nil {
		finishErr := &WriterOperationFinishError{OperationID: op, err: err}
		if workErr != nil {
			return errors.Join(workErr, finishErr)
		}
		return finishErr
	}
	return workErr
}

var _ domain.WriterOperationRunner = (*Client)(nil)

func (c *Client) confirmWriterOperationChange(ctx context.Context, query string, op, sender uuid.UUID) error {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollbackFencedWriterTx(ctx, tx)
	var changed bool
	if err = tx.QueryRow(ctx, query, op, sender).Scan(&changed); err != nil {
		return err
	}
	if !changed {
		return errors.New("postgres: writer operation reservation missing")
	}
	return tx.Commit(ctx)
}

type externalWriterState struct {
	mu      sync.Mutex
	mark    func(context.Context) error
	markErr error
	active  int
	unknown bool
	closed  bool
}

func (s *externalWriterState) start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("postgres: writer operation ended before dispatch")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.mark != nil {
		s.markErr = s.mark(ctx)
		s.mark = nil
	}
	if s.markErr != nil {
		return fmt.Errorf("postgres: confirm writer operation dispatch: %w", s.markErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.active++
	return nil
}

func (s *externalWriterState) dispatch(ctx context.Context, send domain.ExternalWriterAttempt) error {
	if err := s.start(ctx); err != nil {
		return err
	}
	settled := false
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.active--
		s.unknown = s.unknown || !settled
	}()
	var err error
	settled, err = send(ctx)
	return err
}

func (s *externalWriterState) close() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return !s.unknown && s.active == 0 && s.markErr == nil
}
