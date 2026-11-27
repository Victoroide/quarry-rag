package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var writerOperationSender = sync.OnceValue(uuid.New)
var writerOperationHost = sync.OnceValue(func() string {
	host, _ := os.Hostname()
	return host
})

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

// writerOperationSchema checks that the effective gate and storage belong to
// the same metadata corpus as its independently resolved migration history.
// A schema the caller can populate cannot displace an independent corpus
// owned by a role the caller does not control.
func writerOperationSchema(ctx context.Context, tx pgx.Tx) (string, error) {
	var schema *string
	var valid bool
	err := tx.QueryRow(ctx, `WITH resolved AS (
		SELECT to_regprocedure('vector_writer_state()') AS gate,
			to_regclass('vector_state') AS state, to_regclass('documents') AS documents,
			to_regclass('schema_migrations') AS history,
			to_regprocedure('grant_catalog_corpus_authz_mode()') AS authz
	)
	SELECT n.nspname,
		(history.relkind = 'r' AND history.relpersistence = 'p'
			AND metadata.relkind = 'r' AND metadata.relnamespace = history.relnamespace
			AND NOT (has_schema_privilege(history.relnamespace, 'CREATE') AND EXISTS (
				SELECT 1 FROM pg_class alternate JOIN pg_namespace ns ON ns.oid=alternate.relnamespace
				WHERE alternate.relname='schema_migrations' AND alternate.relkind='r'
				AND alternate.relpersistence='p' AND ns.nspname=ANY(current_schemas(false))
				AND alternate.relowner<>history.relowner AND NOT pg_has_role(alternate.relowner, 'USAGE')
				AND EXISTS (SELECT 1 FROM pg_proc mode
					WHERE mode.pronamespace=ns.oid AND mode.proname='grant_catalog_corpus_authz_mode'
					AND mode.pronargs=0 AND mode.proowner=alternate.relowner AND mode.prosecdef)
			))
			AND (r.authz IS NULL OR (authz.pronamespace = history.relnamespace
				AND authz.proowner = history.relowner AND authz.prosecdef))
			AND CASE WHEN r.gate IS NULL AND r.state IS NULL THEN
				to_regprocedure('vector_writer_operations_enabled()') IS NULL
				AND to_regclass('vector_writer_operations') IS NULL
			ELSE p.pronamespace = history.relnamespace AND c.relnamespace = history.relnamespace
				AND c.relkind = 'r' AND p.proowner = history.relowner AND c.relowner = history.relowner
				AND p.prosecdef AND EXISTS (
					SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid
					AND d.refclassid='pg_class'::regclass AND d.refobjid=c.oid
				)
			END) IS TRUE
	FROM resolved r LEFT JOIN pg_proc p ON p.oid=r.gate
	LEFT JOIN pg_class metadata ON metadata.oid=r.documents
	LEFT JOIN pg_class history ON history.oid=r.history
	LEFT JOIN pg_proc authz ON authz.oid=r.authz
	LEFT JOIN pg_class c ON c.oid=r.state LEFT JOIN pg_namespace n ON n.oid=c.relnamespace`).Scan(&schema, &valid)
	if err != nil {
		return "", err
	}
	if !valid {
		return "", errors.New("postgres: ambiguous vector writer schema")
	}
	if schema == nil {
		return "", nil
	}
	return pgx.Identifier{*schema}.Sanitize() + ".", nil
}

func writerOperationSupported(ctx context.Context, tx pgx.Tx, schema string) (bool, error) {
	if schema == "" {
		return false, nil
	}
	var getter, table, column, begin, mark, finish, drain bool
	err := tx.QueryRow(ctx, `SELECT to_regprocedure($1||'vector_writer_operations_enabled()') IS NOT NULL,
		to_regclass($1||'vector_writer_operations') IS NOT NULL,
		EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass($1||'vector_state')
			AND attname='writer_operations_enabled' AND NOT attisdropped),
		to_regprocedure($1||'vector_writer_operation_begin(uuid,uuid,text,text,text)') IS NOT NULL,
		to_regprocedure($1||'vector_writer_operation_mark(uuid,uuid)') IS NOT NULL,
		to_regprocedure($1||'vector_writer_operation_finish(uuid,uuid)') IS NOT NULL,
		to_regprocedure($1||'vector_writer_operation_drain_status()') IS NOT NULL`, schema).
		Scan(&getter, &table, &column, &begin, &mark, &finish, &drain)
	if err != nil {
		return false, err
	}
	if getter != table || getter != column || getter != begin || getter != mark || getter != finish || getter != drain {
		return false, errors.New("postgres: incomplete writer operation schema")
	}
	return getter, nil
}

func requireWriterOperationPrivileges(ctx context.Context, tx pgx.Tx, schema string) error {
	var allowed bool
	if err := tx.QueryRow(ctx, `SELECT has_function_privilege($1||'vector_writer_operation_begin(uuid,uuid,text,text,text)', 'EXECUTE')
		AND has_function_privilege($1||'vector_writer_operation_mark(uuid,uuid)', 'EXECUTE')
		AND has_function_privilege($1||'vector_writer_operation_finish(uuid,uuid)', 'EXECUTE')`, schema).
		Scan(&allowed); err != nil {
		return err
	}
	if !allowed {
		return errors.New("postgres: writer operation privileges are required")
	}
	return nil
}

func checkWriterOperationsDrained(ctx context.Context, tx pgx.Tx, schema string) error {
	var pending ErrWriterOperationsPending
	if err := tx.QueryRow(ctx, `SELECT pending_count, operation_ids FROM `+schema+`vector_writer_operation_drain_status()`).
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
	return c.runWriterOperation(ctx, kind, target, c.gateWriterInSchema, func(ctx context.Context, _ pgx.Tx) error {
		return work(ctx)
	})
}

func (c *Client) runWriterOperation(ctx context.Context, kind domain.WriterOperationKind, target string, check func(context.Context, pgx.Tx, string) error, work func(context.Context, pgx.Tx) error) error {
	var schema string
	guard, closeGuard, returnSlot, err := c.openWriterSession(ctx, func(ctx context.Context, tx pgx.Tx) error {
		resolved, err := writerOperationSchema(ctx, tx)
		if err != nil {
			return fmt.Errorf("postgres: resolve vector writer schema: %w", err)
		}
		schema = resolved
		return check(ctx, tx, schema)
	})
	if err != nil {
		return err
	}
	defer returnSlot()
	defer closeGuard()

	present, err := writerOperationSupported(ctx, guard, schema)
	if err != nil {
		return fmt.Errorf("postgres: check writer operation support: %w", err)
	}
	var enabled bool
	if present {
		if err = guard.QueryRow(ctx, `SELECT `+schema+`vector_writer_operations_enabled()`).Scan(&enabled); err != nil {
			return fmt.Errorf("postgres: check writer operation mode: %w", err)
		}
	}
	if !enabled {
		return work(ctx, guard)
	}
	if privilegeErr := requireWriterOperationPrivileges(ctx, guard, schema); privilegeErr != nil {
		return privilegeErr
	}
	op, sender := uuid.New(), writerOperationSender()
	generation := c.servingPin().generation
	var pinnedTarget string
	if targetErr := guard.QueryRow(ctx, `SELECT target FROM `+schema+`vector_writer_state()`).Scan(&pinnedTarget); targetErr != nil {
		return fmt.Errorf("postgres: read enrolled writer target: %w", targetErr)
	}
	correlation := []slog.Attr{
		slog.Any("writer_operation", op), slog.Any("writer_sender", sender),
		slog.String("process_host", boundedWriterEvidence(writerOperationHost())), slog.Int("process_pid", os.Getpid()),
		slog.String("generation", generation), slog.String("kind", string(kind)),
		slog.String("target_reference", boundedWriterEvidence(target)),
		slog.String("pinned_vector_target", boundedWriterEvidence(pinnedTarget)),
	}
	sink := c.writerEvidence
	if sink == nil {
		sink = writerRecoveryEvidence
	}
	sink = sink.withAttrs(correlation)
	var enrolled bool
	if err = guard.QueryRow(ctx, `SELECT `+schema+`vector_writer_operation_begin($1,$2,$3,$4,$5)`,
		op, sender, generation, string(kind), target).Scan(&enrolled); err != nil {
		return fmt.Errorf("postgres: register writer operation: %w", err)
	}
	if !enrolled {
		return errors.New("postgres: writer operation registration refused")
	}
	// Enrollment now holds the corpus lock even for held recovery. Recheck
	// the actual locked marker before committing to a configured mode.
	if modeErr := c.checkAuthzMode(ctx, guard, `SELECT `+schema+`grant_catalog_corpus_authz_mode()`); modeErr != nil {
		return modeErr
	}
	if evidenceErr := writeWriterEvidence(ctx, sink, "writer operation enrollment started"); evidenceErr != nil {
		return evidenceErr
	}
	if err = guard.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: confirm writer operation registration: %w", err)
	}
	closeGuard()

	state := externalWriterState{evidence: sink, mark: func(ctx context.Context) error {
		return c.confirmWriterOperationChange(ctx, `SELECT `+schema+`vector_writer_operation_mark($1,$2)`, op, sender)
	}}
	defer state.close()
	workErr := ctx.Err()
	if workErr == nil {
		workCtx := context.WithValue(ctx, writerSQLSettlementKey{}, &state)
		workErr = work(domain.WithExternalWriterDispatch(workCtx, state.dispatch), nil)
	}
	if !state.close() {
		return workErr
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writerOperationCompletionTimeout)
	defer cancel()
	if err = c.confirmWriterOperationChange(finishCtx, `SELECT `+schema+`vector_writer_operation_finish($1,$2)`, op, sender); err != nil {
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
	begin := c.Pool.BeginTx
	if conn := c.removalSession(ctx); conn != nil {
		// A fixture-shaped ordinary deletion can hold the only pooled session
		// while its provider attempt records the committed dispatch mark.
		begin = conn.BeginTx
	}
	tx, err := begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollbackFencedWriterTx(ctx, tx)
	var changed bool
	if scanErr := tx.QueryRow(ctx, query, op, sender).Scan(&changed); scanErr != nil {
		return scanErr
	}
	if !changed {
		return errors.New("postgres: writer operation reservation missing")
	}
	return tx.Commit(ctx)
}

type externalWriterState struct {
	mu       sync.Mutex
	evidence *writerEvidenceStream
	attempts uint64
	mark     func(context.Context) error
	markErr  error
	active   int
	unknown  bool
	closed   bool
}

func (s *externalWriterState) start(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("postgres: writer operation ended before dispatch")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.mark != nil {
		s.markErr = s.mark(ctx)
		s.mark = nil
	}
	if s.markErr != nil {
		return 0, fmt.Errorf("postgres: confirm writer operation dispatch: %w", s.markErr)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.active++
	s.attempts++
	return s.attempts, nil
}

func (s *externalWriterState) dispatch(ctx context.Context, send domain.ExternalWriterAttempt) error {
	attempt, err := s.start(ctx)
	if err != nil {
		return err
	}
	if s.evidence != nil {
		ctx = domain.WithExternalWriterEvidence(ctx, func(ctx context.Context, evidence domain.ExternalWriterEvidence) error {
			return writeWriterEvidence(ctx, s.evidence, "writer provider attempt", "attempt", attempt,
				"provider", evidence.Provider, "namespace", boundedWriterEvidence(evidence.Namespace),
				"outcome", evidence.Outcome, "provider_request_id", boundedWriterEvidence(evidence.RequestID),
				"provider_request_sequence", boundedWriterEvidence(evidence.RequestSequence))
		})
	}
	settled := false
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.active--
		s.unknown = s.unknown || !settled
	}()
	settled, err = send(ctx)
	return err
}

func (s *externalWriterState) close() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return !s.unknown && s.active == 0 && s.markErr == nil
}

// An unavailable or oversized field cannot be used as settlement evidence.
// Keep its absence distinct from the provider's default (empty) namespace.
func boundedWriterEvidence(value string) string {
	if len(value) > 512 || strings.ContainsFunc(value, unicode.IsControl) {
		return "[unavailable]"
	}
	return value
}
