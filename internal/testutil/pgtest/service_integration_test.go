package pgtest

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
)

// configuredSession is the session the caller chose before any test ran.
var configuredSession = os.Getenv(sessionEnv)

func servicesConfigured(t *testing.T) bool {
	t.Helper()
	admins, err := loadServices(os.LookupEnv)
	if err != nil {
		t.Fatalf("test database services: %v", err)
	}
	return admins != nil
}

// adminFor returns an administrator address for s: the configured test
// service when there is one, otherwise a disposable container.
func adminFor(t *testing.T, s server) *url.URL {
	t.Helper()
	if testing.Short() {
		t.Skip("requires a Docker daemon or the test services")
	}
	admins, err := loadServices(os.LookupEnv)
	if err != nil {
		t.Fatalf("test database services: %v", err)
	}
	if admins != nil {
		return admins[s.adminEnv]
	}
	u, err := url.Parse(startContainer(t, s))
	if err != nil {
		t.Fatalf("parse container address: %v", err)
	}
	return u
}

func connectTo(t *testing.T, address string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), address)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func scalar[T any](t *testing.T, conn *pgx.Conn, query string, args ...any) T {
	t.Helper()
	var got T
	if err := conn.QueryRow(t.Context(), query, args...).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return got
}

func databaseName(t *testing.T, address string) string {
	t.Helper()
	u, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(u.Path, "/")
}

func exists(t *testing.T, admin *pgx.Conn, catalog, column, name string) bool {
	t.Helper()
	return scalar[bool](t, admin, fmt.Sprintf("SELECT EXISTS (SELECT FROM %s WHERE %s = $1)", catalog, column), name)
}

func checkLeakedConnectionClosed(t testing.TB, leaked *pgx.Conn) {
	t.Helper()
	if leaked == nil {
		return
	}
	if err := leaked.Ping(context.Background()); err == nil {
		t.Error("a connection that outlived the test still works")
	}
}

func withCredentials(u *url.URL, user, password string) *url.URL {
	c := *u
	c.User = url.UserPassword(user, password)
	c.RawQuery = queryWithout(u.RawQuery, "user", "password")
	return &c
}

func requireConcurrentFixtures(t testing.TB, provisioned []*fixture, errs []error) {
	t.Helper()
	for _, f := range provisioned {
		if f != nil {
			t.Cleanup(f.cleanup(t))
		}
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("provision %d: %v", i, err)
		}
	}
}

func requireMissingExtension(t testing.TB, f *fixture, err error) {
	t.Helper()
	if f != nil {
		t.Cleanup(f.cleanup(t))
	}
	if err == nil || f != nil || !strings.Contains(err.Error(), "does not offer the vector extension") {
		t.Fatalf("provision() = %v, %v, want a missing extension error without a fixture", f, err)
	}
}

func TestContainerFallbackStartsEachServer(t *testing.T) {
	for _, s := range []server{plainServer, vectorServer} {
		t.Run(s.name, func(t *testing.T) {
			if testing.Short() {
				t.Skip("requires a Docker daemon")
			}
			if servicesConfigured(t) {
				t.Skip("the test services are configured; the container fallback runs without them")
			}
			address := startContainer(t, s)
			conn := connectTo(t, address)
			available := scalar[bool](t, conn, `SELECT EXISTS (SELECT FROM pg_available_extensions WHERE name = 'vector')`)
			if available != (s.extension == "vector") {
				t.Fatalf("%s container offers the vector extension = %t", s.name, available)
			}
			if configuredSession == "" {
				prefix := fmt.Sprintf("quarry-%d-", os.Getpid())
				if got := testcontainers.SessionID(); !strings.HasPrefix(got, prefix) {
					t.Fatalf("session %q does not belong to this process, want prefix %q", got, prefix)
				}
			}
		})
	}
}

func TestServiceFixturesIgnoreAdministratorDatabaseSelectors(t *testing.T) {
	plain := adminFor(t, plainServer)
	vector := adminFor(t, vectorServer)
	t.Setenv(vectorServer.adminEnv, vector.String())
	adminDatabase := scalar[string](t, connectTo(t, plain.String()), "SELECT current_database()")
	encodedDatabase := strings.ReplaceAll(url.QueryEscape(adminDatabase), "+", "%20")

	for _, test := range []struct {
		name     string
		selector string
	}{
		{name: "dbname", selector: "dbname"},
		{name: "database", selector: "database"},
		{name: "encoded dbname", selector: "db%6Eame"},
		{name: "encoded database", selector: "datab%61se"},
		{name: "space around key", selector: " dbname "},
	} {
		t.Run(test.name, func(t *testing.T) {
			admin := *plain
			const appName = "application_name=quarry%20fixture"
			admin.RawQuery = strings.TrimSuffix(admin.RawQuery, "&")
			if admin.RawQuery != "" {
				admin.RawQuery += "&"
			}
			admin.RawQuery += test.selector + "=" + encodedDatabase + "&" + appName
			t.Setenv(plainServer.adminEnv, admin.String())

			first, second := Start(t), Start(t)
			seen := map[string]bool{adminDatabase: true}
			for _, address := range []string{first, second, Sibling(t, first)} {
				conn := connectTo(t, address)
				name := scalar[string](t, conn, "SELECT current_database()")
				if want := databaseName(t, address); name != want || seen[name] {
					t.Errorf("fixture connected to %q, want its own database %q", name, want)
				}
				seen[name] = true
				if got := scalar[string](t, conn, "SELECT current_setting('application_name')"); got != "quarry fixture" {
					t.Errorf("application name = %q, want quarry fixture", got)
				}
				if !strings.Contains(address, appName) {
					t.Error("application name lost its original URL encoding")
				}
			}
		})
	}
}

func TestServiceFixtures(t *testing.T) {
	plain := adminFor(t, plainServer)
	vector := adminFor(t, vectorServer)

	t.Run("concurrent fixtures are isolated", func(t *testing.T) {
		ctx := t.Context()
		var wg sync.WaitGroup
		provisioned := make([]*fixture, 2)
		errs := make([]error, 2)
		for i := range provisioned {
			wg.Add(1)
			go func() {
				defer wg.Done()
				provisioned[i], errs[i] = provision(ctx, plainServer, plain, nil)
			}()
		}
		wg.Wait()
		requireConcurrentFixtures(t, provisioned, errs)
		first := withDatabase(plain, provisioned[0].databases[0]).String()
		second := withDatabase(plain, provisioned[1].databases[0]).String()
		if first == second {
			t.Fatal("two fixtures share a database")
		}
		if _, err := connectTo(t, first).Exec(ctx, `CREATE TABLE marker (id int)`); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if scalar[bool](t, connectTo(t, second), `SELECT to_regclass('marker') IS NULL`) != true {
			t.Fatal("a table created in one fixture is visible in the other")
		}
		if _, err := connectTo(t, second).Exec(ctx, `CREATE TABLE marker (id int)`); err != nil {
			t.Fatalf("the second fixture is not empty: %v", err)
		}
	})

	t.Run("cleanup removes what the test created", func(t *testing.T) {
		var database, sibling, latin, role string
		var leaked *pgx.Conn
		t.Run("fixture", func(t *testing.T) {
			address := startService(t, plainServer, plain)
			database = databaseName(t, address)
			sibling = databaseName(t, Sibling(t, address))
			latinAddress := SiblingLatin1(t, address)
			latin = databaseName(t, latinAddress)
			if got := scalar[string](t, connectTo(t, latinAddress), `SELECT current_setting('server_encoding')`); got != "LATIN1" {
				t.Fatalf("server encoding = %q, want LATIN1", got)
			}
			role = Role(t, address, "cleanup_owner")
			conn := connectTo(t, address)
			if _, err := conn.Exec(t.Context(), `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+`; CREATE SCHEMA owned AUTHORIZATION `+pgx.Identifier{role}.Sanitize()); err != nil {
				t.Fatalf("create role: %v", err)
			}
			// A pool that the test forgot to close must not block the cleanup.
			var err error
			leaked, err = pgx.Connect(context.Background(), address)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
		})
		admin := connectTo(t, plain.String())
		for _, name := range []string{database, sibling, latin} {
			if name != "" && exists(t, admin, "pg_database", "datname", name) {
				t.Errorf("database %s was not dropped", name)
			}
		}
		if role != "" && exists(t, admin, "pg_roles", "rolname", role) {
			t.Errorf("role %s was not dropped", role)
		}
		checkLeakedConnectionClosed(t, leaked)
	})

	t.Run("a failed setup is still cleaned up", func(t *testing.T) {
		var name string
		if !t.Run("fixture", func(t *testing.T) {
			failure := errors.New("setup failed")
			f, err := provision(t.Context(), plainServer, plain, func(context.Context, *fixture) error { return failure })
			if f != nil {
				t.Cleanup(f.cleanup(t))
			}
			if !errors.Is(err, failure) || f == nil {
				t.Fatalf("provision() = %v, %v, want the setup error and a fixture", f, err)
			}
			admin := connectTo(t, plain.String())
			name = f.databases[0]
			if !exists(t, admin, "pg_database", "datname", name) {
				t.Fatalf("database %s was not created before the failure", name)
			}
		}) {
			return
		}
		admin := connectTo(t, plain.String())
		if exists(t, admin, "pg_database", "datname", name) {
			t.Fatalf("database %s survived the cleanup", name)
		}
	})

	t.Run("an unreachable service is an error", func(t *testing.T) {
		unreachable, err := url.Parse("postgres://rag:secret@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=2")
		if err != nil {
			t.Fatal(err)
		}
		f, err := provision(t.Context(), plainServer, unreachable, nil)
		if f != nil {
			t.Cleanup(f.cleanup(t))
		}
		if err == nil || f != nil {
			t.Fatalf("provision() = %v, %v, want an error without a fixture", f, err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("error %q repeats the password", err)
		}
	})

	t.Run("a server without the extension is an error", func(t *testing.T) {
		f, err := provision(t.Context(), vectorServer, plain, nil)
		requireMissingExtension(t, f, err)
	})

	t.Run("a limited administrator is an error", func(t *testing.T) {
		address := startService(t, plainServer, plain)
		role := Role(t, address, "limited_admin")
		if _, err := connectTo(t, address).Exec(t.Context(), `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` LOGIN PASSWORD 'limited'`); err != nil {
			t.Fatalf("create role: %v", err)
		}
		admin := withDatabase(plain, databaseName(t, address))
		config, err := pgx.ParseConfig(admin.String())
		if err != nil {
			t.Fatal(err)
		}
		credentials := url.Values{"user": {config.User}, "password": {config.Password}}
		rawCredentials := strings.ReplaceAll(credentials.Encode(), "+", "%20")
		if admin.RawQuery != "" {
			rawCredentials += "&" + admin.RawQuery
		}
		admin.RawQuery = rawCredentials
		admin.User = nil

		limited := withCredentials(admin, role, "limited")
		f, err := provision(t.Context(), plainServer, limited, nil)
		if f != nil {
			t.Cleanup(f.cleanup(t))
		}
		if err == nil || f != nil || !strings.Contains(err.Error(), "must be a superuser") {
			t.Fatalf("provision() = %v, %v, want a superuser error without a fixture", f, err)
		}
		if got := scalar[string](t, connectTo(t, limited.String()), "SELECT current_user"); got != role {
			t.Fatalf("current_user = %q, want %q", got, role)
		}
	})

	t.Run("Start and StartVector use the configured services", func(t *testing.T) {
		t.Setenv(plainServer.adminEnv, plain.String())
		t.Setenv(vectorServer.adminEnv, vector.String())
		plainAddress := Start(t)
		vectorAddress := StartVector(t)
		for address, service := range map[string]*url.URL{plainAddress: plain, vectorAddress: vector} {
			u, err := url.Parse(address)
			if err != nil {
				t.Fatal(err)
			}
			if u.Host != service.Host || !strings.HasPrefix(u.Path, "/quarry_test_") {
				t.Fatalf("fixture %s is not a new database on %s", u.Redacted(), service.Host)
			}
		}
		if _, err := connectTo(t, vectorAddress).Exec(t.Context(), `CREATE EXTENSION vector`); err != nil {
			t.Fatalf("create extension on the pgvector service: %v", err)
		}
		if got := scalar[bool](t, connectTo(t, plainAddress), `SELECT NOT EXISTS (SELECT FROM pg_extension WHERE extname = 'vector')`); !got {
			t.Fatal("a fresh database on the plain service has the vector extension")
		}
	})
}
