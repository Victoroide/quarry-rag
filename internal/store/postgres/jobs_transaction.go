package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

const ingestTxRollbackTimeout = 3 * time.Second

func rollbackIngestTx(ctx context.Context, tx pgx.Tx) {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ingestTxRollbackTimeout)
	defer cancel()
	_ = tx.Rollback(rollbackCtx)
}
