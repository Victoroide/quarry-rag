package postgres

import (
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ingestPoolReleaseTracer struct {
	ingestTxTracer
	closedPID atomic.Uint32
}

func (tracer *ingestPoolReleaseTracer) TraceRelease(_ *pgxpool.Pool, data pgxpool.TraceReleaseData) {
	if data.Conn.IsClosed() {
		tracer.closedPID.Store(data.Conn.PgConn().PID())
	}
}
