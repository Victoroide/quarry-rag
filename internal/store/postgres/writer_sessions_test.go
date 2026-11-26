package postgres

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func sessionTestClient(t *testing.T) (*Client, *pgx.Conn) {
	t.Helper()
	return sessionTestClientWithPool(t, 2)
}

// sessionTestClientWithPool returns a migrated client whose pool holds at most
// maxConns connections, with an independent observer of the server's sessions.
func sessionTestClientWithPool(t *testing.T, maxConns int) (*Client, *pgx.Conn) {
	t.Helper()
	u, err := url.Parse(pgtest.Start(t))
	require.NoError(t, err)
	q := u.Query()
	q.Set("pool_max_conns", strconv.Itoa(maxConns))
	u.RawQuery = q.Encode()
	c, err := NewClient(t.Context(), u.String())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(t.Context()))
	q.Del("pool_max_conns")
	u.RawQuery = q.Encode()
	observer, err := pgx.Connect(t.Context(), u.String())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close(context.Background())) })
	return c, observer
}

func requireDedicatedSessions(t *testing.T, c *Client, observer *pgx.Conn, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var count int
		err := observer.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND backend_type = 'client backend'`).Scan(&count)
		return err == nil && count-1-int(c.Pool.Stat().TotalConns()) == want
	}, 5*time.Second, 10*time.Millisecond)
}

func TestWriterSessionsAreCappedAtPoolMaximum(t *testing.T) {
	c, observer := sessionTestClient(t)
	first, err := c.AcquireWriter(t.Context())
	require.NoError(t, err)
	second, err := c.AcquireWriter(t.Context())
	require.NoError(t, err)
	requireDedicatedSessions(t, c, observer, 2)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err = c.AcquireWriter(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	first()
	third, err := c.AcquireWriter(t.Context())
	require.NoError(t, err)
	requireDedicatedSessions(t, c, observer, 2)
	second()
	third()
	requireDedicatedSessions(t, c, observer, 0)
}

func TestGenerationFenceHoldsOneSession(t *testing.T) {
	c, observer := sessionTestClient(t)
	fence, err := c.FenceGeneration(t.Context(), "g2")
	require.NoError(t, err)
	requireDedicatedSessions(t, c, observer, 1)
	_, err = c.FenceGeneration(t.Context(), "g2")
	require.ErrorContains(t, err, "another operation owns generation")
	requireDedicatedSessions(t, c, observer, 1)
	fence.Close(t.Context())
	requireDedicatedSessions(t, c, observer, 0)
}
