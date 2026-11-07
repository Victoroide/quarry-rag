package postgres

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type queryCounter struct{ queries atomic.Int32 }

func (c *queryCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.queries.Add(1)
	return ctx
}
func (*queryCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestNewClientInstallsQueryTracer(t *testing.T) {
	url := startTestDatabase(t)
	tracer := &queryCounter{}
	client, err := NewClient(t.Context(), url, WithQueryTracer(tracer))
	require.NoError(t, err)
	t.Cleanup(client.Pool.Close)
	before := tracer.queries.Load()
	var number int
	require.NoError(t, client.Pool.QueryRow(t.Context(), "SELECT 42").Scan(&number))
	require.Equal(t, 42, number)
	require.Greater(t, tracer.queries.Load(), before)
}
