package pgtest

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
)

func queryOne(t *testing.T, url, query string) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(t.Context()) })
	var got int
	if err := conn.QueryRow(t.Context(), query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if got != 1 {
		t.Fatalf("%s = %d, want 1", query, got)
	}
}

// configuredSession is the session the caller chose before any test ran.
var configuredSession = os.Getenv(sessionEnv)

func TestStartServesQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	queryOne(t, Start(t), "SELECT 1")
	if configuredSession != "" {
		return
	}
	if got, prefix := testcontainers.SessionID(), fmt.Sprintf("quarry-%d-", os.Getpid()); !strings.HasPrefix(got, prefix) {
		t.Fatalf("session %q does not belong to this process, want prefix %q", got, prefix)
	}
}

func TestStartVectorCreatesTheExtension(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	url := StartVector(t)
	conn, err := pgx.Connect(t.Context(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(t.Context()) })
	if _, err := conn.Exec(t.Context(), "CREATE EXTENSION vector"); err != nil {
		t.Fatalf("create extension: %v", err)
	}
	queryOne(t, url, "SELECT count(*)::int FROM pg_extension WHERE extname = 'vector'")
}
