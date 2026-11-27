package postgres

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type writerSQLSettlementKey struct{}

// sqlMutationUnconfirmed distinguishes replies proving no effect from a
// connection or cancellation error that can precede server-side settlement.
func sqlMutationUnconfirmed(err error) bool {
	if err == nil || errors.Is(err, pgx.ErrNoRows) || errors.Is(err, pgx.ErrTxCommitRollback) || pgconn.SafeToRetry(err) {
		return false
	}
	var reply *pgconn.PgError
	return !errors.As(err, &reply) || reply.SeverityUnlocalized != "ERROR" ||
		strings.HasPrefix(reply.Code, "08") || reply.Code == "40003"
}

// observeWriterSQL applies to autocommit statements and COMMIT. Statements
// inside an explicit transaction cannot take effect without its COMMIT.
func observeWriterSQL(ctx context.Context, err error) error {
	if state, ok := ctx.Value(writerSQLSettlementKey{}).(*externalWriterState); ok && sqlMutationUnconfirmed(err) {
		state.mu.Lock()
		state.unknown = true
		state.mu.Unlock()
	}
	return err
}

func execWriterSQL(ctx context.Context, db deletionExecer, query string, args ...any) (pgconn.CommandTag, error) {
	tag, err := db.Exec(ctx, query, args...)
	return tag, observeWriterSQL(ctx, err)
}

func commitWriterSQL(ctx context.Context, tx pgx.Tx) error {
	return observeWriterSQL(ctx, tx.Commit(ctx))
}

// retryWriterSQL allows the established reconnect path only if it cannot
// repeat an unconfirmed strict-mode mutation.
func retryWriterSQL(ctx context.Context, err error) bool {
	return ctx.Value(writerSQLSettlementKey{}) == nil || !sqlMutationUnconfirmed(err)
}
