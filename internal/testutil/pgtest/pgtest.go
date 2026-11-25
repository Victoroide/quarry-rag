// Package pgtest provides empty PostgreSQL databases for tests.
//
// Without configuration every fixture is a disposable container that is
// removed when the test ends. When QUARRY_TEST_POSTGRES_ADMIN_DSN and
// QUARRY_TEST_PGVECTOR_ADMIN_DSN both name a running service, a fixture is
// instead a new empty database, created from the empty template, on the
// matching service and dropped when the test ends. Setting only one of them,
// a malformed address or an unreachable service fails the test rather than
// falling back to containers.
package pgtest

import (
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

const image = "postgres:16.13-alpine3.23"

const vectorImage = "pgvector/pgvector:0.8.7-pg16@sha256:0dd3149a06e4579a0d70d8fef9945636cd514005e7237deb27f9b67886f8dc4f"

// server describes one kind of database server a fixture can live on.
type server struct {
	// name identifies the server in messages.
	name string
	// image is the container started when no service is configured.
	image string
	// adminEnv names the variable that holds the service administrator
	// address.
	adminEnv string
	// extension is the extension the server must offer, if any.
	extension string
}

var (
	plainServer  = server{name: "PostgreSQL", image: image, adminEnv: "QUARRY_TEST_POSTGRES_ADMIN_DSN"}
	vectorServer = server{name: "pgvector", image: vectorImage, adminEnv: "QUARRY_TEST_PGVECTOR_ADMIN_DSN", extension: "vector"}
)

// Start returns the connection URL of a new empty database on an ordinary
// PostgreSQL server. It is removed when the test ends, after the cleanup
// functions registered later, so clients close their pools first.
func Start(t testing.TB) string {
	t.Helper()
	return start(t, plainServer)
}

// StartVector returns the connection URL of a new empty database on a server
// where the vector extension can be created. Container customizers apply when
// starting a disposable container. A shared service must be configured before use.
func StartVector(t testing.TB, opts ...testcontainers.ContainerCustomizer) string {
	t.Helper()
	return start(t, vectorServer, opts...)
}

func start(t testing.TB, s server, opts ...testcontainers.ContainerCustomizer) string {
	t.Helper()
	if testing.Short() {
		t.Skip("requires a Docker daemon or the test services")
	}
	return startBackend(t, s, opts...)
}

func startBackend(t testing.TB, s server, opts ...testcontainers.ContainerCustomizer) string {
	t.Helper()
	admins, err := loadServices(os.LookupEnv)
	if err != nil {
		t.Fatalf("test database services: %v", err)
	}
	if admins == nil {
		return startContainer(t, s, opts...)
	}
	return startService(t, s, admins[s.adminEnv])
}
