package pgtest

import (
	"testing"

	"github.com/jackc/pgx/v5"
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

func TestStartServesQueries(t *testing.T) {
	queryOne(t, Start(t), "SELECT 1")
}

func TestStartVectorCreatesTheExtension(t *testing.T) {
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
