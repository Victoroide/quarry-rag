// Package pgtest starts disposable PostgreSQL containers for tests.
package pgtest

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const image = "postgres:16.13-alpine3.23"

const vectorImage = "pgvector/pgvector:0.8.7-pg16@sha256:0dd3149a06e4579a0d70d8fef9945636cd514005e7237deb27f9b67886f8dc4f"

// Start runs a PostgreSQL container and returns its connection URL once the
// server accepts connections. The container is removed when the test ends.
func Start(t testing.TB) string {
	return start(t, image)
}

// StartVector runs PostgreSQL with pgvector available for extension creation.
func StartVector(t testing.TB) string {
	return start(t, vectorImage)
}

func start(t testing.TB, containerImage string) string {
	t.Helper()
	ctx := context.Background()
	// The image opens its port for a temporary server during initialization,
	// so readiness also waits for the second startup log line.
	container, err := postgres.Run(ctx, containerImage,
		postgres.WithDatabase("rag"),
		postgres.WithUsername("rag"),
		postgres.WithPassword("rag"),
		testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if termErr := container.Terminate(context.Background()); termErr != nil {
			t.Errorf("terminate postgres container: %v", termErr)
		}
	})
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return url
}
