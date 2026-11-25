package pgtest

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// reaperShutdownRetry repeats a start that failed because Docker was removing
// the reaper that exited after an idle period, until the library can create a
// new one. See reaperShuttingDown.
var reaperShutdownRetry = retryPolicy{window: time.Minute, first: 250 * time.Millisecond, limit: 2 * time.Second}

// startContainer runs a disposable container for s and returns the address of
// its database once the server accepts connections. The container is removed
// when the test ends.
func startContainer(t testing.TB, s server, opts ...testcontainers.ContainerCustomizer) string {
	t.Helper()
	begin := time.Now()
	if err := isolateReaper(); err != nil {
		t.Fatalf("isolate reaper session: %v", err)
	}
	ctx := context.Background()
	// The image opens its port for a temporary server during initialization,
	// so readiness also waits for the second startup log line.
	options := append([]testcontainers.ContainerCustomizer{
		postgres.WithDatabase("rag"),
		postgres.WithUsername("rag"),
		postgres.WithPassword("rag"),
		testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	}, opts...)
	var container *postgres.PostgresContainer
	err := reaperShutdownRetry.run(ctx, func() error {
		var runErr error
		container, runErr = postgres.Run(ctx, s.image, options...)
		if runErr != nil {
			// A start that fails after creating the container returns it,
			// and a retry would otherwise leave it behind.
			runErr = errors.Join(runErr, testcontainers.TerminateContainer(container))
		}
		return runErr
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		stopping := time.Now()
		if termErr := container.Terminate(context.Background()); termErr != nil {
			t.Errorf("terminate postgres container: %v", termErr)
		}
		t.Logf("pgtest: cleanup %s", time.Since(stopping).Round(time.Millisecond))
	})
	address, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	u, err := url.Parse(address)
	if err != nil {
		t.Fatalf("postgres connection string is not a URL: %v", err)
	}
	f := &fixture{admin: u, disposable: true}
	f.publish(u)
	t.Cleanup(func() { _ = f.release(context.Background()) })
	t.Logf("pgtest: setup %s", time.Since(begin).Round(time.Millisecond))
	return address
}
