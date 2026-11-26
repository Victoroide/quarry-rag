package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// GrantsActivationResult reports the corpus policy revision and idempotent replay
// state of a corpus cutover command.
type GrantsActivationResult struct {
	AuthzMode     string `json:"authz_mode"`
	Revision      int64  `json:"revision"`
	DryRun        bool   `json:"dry_run"`
	AlreadyActive bool   `json:"already_active"`
}

var (
	_ domain.CorpusModeReader      = (*Client)(nil)
	_ domain.SecurityAuditRecorder = (*Client)(nil)
)

// SetAuthzMode configures the expected corpus authorization mode enforced on
// database gates and admissions.
func (c *Client) SetAuthzMode(mode string) {
	c.authzMode.Store(&mode)
}

func (c *Client) configuredAuthzMode() string {
	if p := c.authzMode.Load(); p != nil {
		return *p
	}
	return ""
}

// CorpusAuthzMode returns the active authorization mode recorded on the corpus.
// A missing or unrecognized marker fails closed.
func (c *Client) CorpusAuthzMode(ctx context.Context) (string, error) {
	return readCorpusAuthzMode(ctx, c.Pool)
}

func readCorpusAuthzMode(ctx context.Context, q queryRower) (string, error) {
	var mode *string
	if err := q.QueryRow(ctx, `SELECT grant_catalog_corpus_authz_mode()`).Scan(&mode); err != nil {
		return "", fmt.Errorf("postgres: read corpus authz mode: %w", err)
	}
	if mode == nil || (*mode != "service" && *mode != "grants") {
		return "", errors.New("postgres: corpus authz mode marker is missing or invalid")
	}
	return *mode, nil
}

func (c *Client) checkAuthzMode(ctx context.Context, q queryRower) error {
	expected := c.configuredAuthzMode()
	if expected == "" {
		return nil
	}
	actual, err := readCorpusAuthzMode(ctx, q)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%w: replica configured for %q, corpus is %q", domain.ErrAuthzModeMismatch, expected, actual)
	}
	return nil
}

// ActivateGrants transitions the corpus authorization mode marker to grants
// inside an operator-only transaction, advancing the policy revision and
// recording the cutover audit event.
func (c *Client) ActivateGrants(ctx context.Context, dryRun bool) (GrantsActivationResult, error) {
	ctx, cancel := context.WithTimeout(ctx, GrantImportTimeout)
	defer cancel()
	options := pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
	if dryRun {
		options = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
	}
	result := GrantsActivationResult{AuthzMode: "grants", DryRun: dryRun}
	err := pgx.BeginTxFunc(ctx, c.Pool, options, func(tx pgx.Tx) error {
		if _, setupErr := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '25s'`); setupErr != nil {
			return setupErr
		}
		if schemaErr := bindMetadataSchema(ctx, tx); schemaErr != nil {
			return schemaErr
		}
		return tx.QueryRow(ctx, `SELECT resulting_revision, already_active FROM grant_catalog_activate_grants($1)`, dryRun).
			Scan(&result.Revision, &result.AlreadyActive)
	})
	if err != nil {
		return GrantsActivationResult{}, grantActivationError(err)
	}
	return result, nil
}

func grantActivationError(err error) error {
	var postgresErr *pgconn.PgError
	if errors.As(err, &postgresErr) {
		switch postgresErr.Code {
		case "QG006":
			return errors.New("grants activation: checkpointed document deletion must be activated first")
		case "QG001", "QG004", "QG005":
			return fmt.Errorf("grants activation: %s", postgresErr.Message)
		default:
			return fmt.Errorf("grants activation failed (SQLSTATE %s)", postgresErr.Code)
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("grants activation interrupted: %w", err)
	}
	return errors.New("grants activation interrupted before confirmation")
}

// RecordSecurityAudit persists a bounded security audit event using the
// owner-defined audit entry point.
func (c *Client) RecordSecurityAudit(ctx context.Context, event domain.SecurityAuditEvent) error {
	return recordSecurityAuditTx(ctx, c.Pool, event)
}

type auditExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func recordSecurityAuditTx(ctx context.Context, db auditExecer, event domain.SecurityAuditEvent) error {
	requestID := event.RequestID
	if requestID == "" {
		requestID = logctx.RequestID(ctx)
	}
	if len(requestID) > 128 {
		requestID = requestID[:128]
	}
	reason := event.Reason
	if len(reason) > 128 {
		reason = reason[:128]
	}
	if reason == "" {
		reason = event.Outcome
	}
	_, err := db.Exec(ctx,
		`SELECT grant_catalog_record_security_audit($1,$2::uuid,$3::uuid,$4,$5,$6,$7,$8::uuid,$9::uuid)`,
		requestID,
		nullableAuditUUID(event.TenantID),
		nullableAuditUUID(event.PrincipalID),
		event.PolicyRevision,
		event.Action,
		event.Outcome,
		reason,
		nullableAuditUUID(event.DocumentID),
		nullableAuditUUID(event.Incarnation),
	)
	if err != nil {
		return fmt.Errorf("postgres: record security audit: %w", err)
	}
	return nil
}

func nullableAuditUUID(raw string) *string {
	if raw == "" {
		return nil
	}
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed == uuid.Nil {
		return nil
	}
	canonical := parsed.String()
	return &canonical
}
