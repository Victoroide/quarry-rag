package pgtest

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// serviceAddress builds the address of a test service.
func serviceAddress(user, password, hostPort string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     hostPort,
		Path:     "/postgres",
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

var (
	plainAddress  = serviceAddress("rag", "rag", "127.0.0.1:5432")
	vectorAddress = serviceAddress("rag", "rag", "127.0.0.1:5433")
)

// unset makes name absent for the test and restores it afterwards.
func unset(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "")
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
}

func environment(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func TestLoadServicesWithoutConfigurationSelectsContainers(t *testing.T) {
	admins, err := loadServices(environment(nil))
	if err != nil || admins != nil {
		t.Fatalf("loadServices() = %v, %v, want nil, nil", admins, err)
	}
}

func TestLoadServicesReadsBothAddresses(t *testing.T) {
	admins, err := loadServices(environment(map[string]string{
		plainServer.adminEnv:  plainAddress,
		vectorServer.adminEnv: vectorAddress,
	}))
	if err != nil {
		t.Fatalf("loadServices() error = %v", err)
	}
	if got := admins[plainServer.adminEnv].Host; got != "127.0.0.1:5432" {
		t.Fatalf("plain service host = %q", got)
	}
	if got := admins[vectorServer.adminEnv].Host; got != "127.0.0.1:5433" {
		t.Fatalf("pgvector service host = %q", got)
	}
}

func TestFixtureAddressIndependentOfPGPort(t *testing.T) {
	t.Setenv("PGPORT", "6543")
	for _, tc := range []struct {
		name      string
		authority string
		query     string
		host      string
		port      uint16
	}{
		{name: "default port", authority: "127.0.0.1", host: "127.0.0.1", port: 5432},
		{name: "explicit port", authority: "127.0.0.1:5434", host: "127.0.0.1", port: 5434},
		{name: "query port", authority: "127.0.0.1", query: "port=5435&application_name=quarry%20test", host: "127.0.0.1", port: 5435},
		{name: "IPv6 default port", authority: "[::1]", host: "::1", port: 5432},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address := url.URL{Scheme: "postgres", User: url.User("fixture"), Host: tc.authority, Path: "/postgres", RawQuery: tc.query}
			admins, err := loadServices(environment(map[string]string{
				plainServer.adminEnv:  address.String(),
				vectorServer.adminEnv: vectorAddress,
			}))
			if err != nil {
				t.Fatal(err)
			}
			admin := admins[plainServer.adminEnv]
			for _, dsn := range []*url.URL{admin, withDatabase(admin, "quarry_test_control")} {
				cfg, err := pgx.ParseConfig(dsn.String())
				if err != nil {
					t.Fatal(err)
				}
				if cfg.Host != tc.host || cfg.Port != tc.port {
					t.Errorf("%s endpoint = %s port %d, want %s port %d", cfg.Database, cfg.Host, cfg.Port, tc.host, tc.port)
				}
				if dsn.RawQuery != tc.query {
					t.Errorf("%s query = %q, want %q", cfg.Database, dsn.RawQuery, tc.query)
				}
			}
		})
	}
}

func TestFixtureAdminDatabaseIndependentOfPGDatabase(t *testing.T) {
	for _, tc := range []struct {
		name     string
		path     string
		selector string
		want     string
	}{
		{name: "omitted database", want: "postgres"},
		{name: "empty path", path: "/", want: "postgres"},
		{name: "path database", path: "/maintenance db", want: "maintenance db"},
		{name: "query dbname", selector: "&dbname=maintenance%20db", want: "maintenance db"},
		{name: "encoded query database", path: "/ignored", selector: "&datab%61se=maintenance%20db", want: "maintenance db"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const preserved = "sslmode=disable&application_name=quarry%20fixture"
			address := url.URL{
				Scheme: "postgres", User: url.User("fixture"), Host: "127.0.0.1:5432",
				Path: tc.path, RawQuery: preserved + tc.selector,
			}
			for _, ambient := range []string{"ambient_one", "ambient_two"} {
				t.Setenv("PGDATABASE", ambient)
				admin, err := parseAdmin(address.String())
				if err != nil {
					t.Fatal(err)
				}
				if admin.RawQuery != address.RawQuery {
					t.Errorf("administrator query = %q, want %q", admin.RawQuery, address.RawQuery)
				}
				config, err := pgx.ParseConfig(admin.String())
				if err != nil {
					t.Fatal(err)
				}
				if config.Database != tc.want {
					t.Errorf("PGDATABASE=%s: administrator database = %q, want %q", ambient, config.Database, tc.want)
				}
				fixtureURL := withDatabase(admin, "quarry_test_owned")
				config, err = pgx.ParseConfig(fixtureURL.String())
				if err != nil {
					t.Fatal(err)
				}
				if config.Database != "quarry_test_owned" || fixtureURL.RawQuery != preserved {
					t.Errorf("fixture database = %q, query = %q, want the owned database and unchanged options", config.Database, fixtureURL.RawQuery)
				}
			}
		})
	}
}

func TestFixtureAdminLoginIndependentOfPGUser(t *testing.T) {
	for _, tc := range []struct {
		name  string
		user  *url.Userinfo
		query string
		want  string
	}{
		{name: "omitted login"},
		{name: "empty authority login", user: url.UserPassword("", "s3cret-pass")},
		{name: "empty query login", user: url.User("authority"), query: "&user="},
		{name: "authority login", user: url.User("authority"), want: "authority"},
		{name: "query login", query: "&user=query%20login", want: "query login"},
		{name: "encoded query overrides authority", user: url.User("authority"), query: "&us%65r=query", want: "query"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address := url.URL{
				Scheme: "postgres", User: tc.user, Host: "127.0.0.1:5432",
				Path: "/postgres", RawQuery: "sslmode=disable" + tc.query,
			}
			for _, ambient := range []string{"ambient_one", "ambient_two"} {
				t.Setenv("PGUSER", ambient)
				admin, err := parseAdmin(address.String())
				if tc.want == "" {
					if err == nil || err.Error() != "must name an administrator" {
						t.Errorf("PGUSER=%s: administrator validation error = %v, want a missing administrator error", ambient, err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				config, err := pgx.ParseConfig(admin.String())
				if err != nil {
					t.Fatal(err)
				}
				if config.User != tc.want || admin.RawQuery != address.RawQuery {
					t.Errorf("PGUSER=%s: administrator login = %q, query = %q, want %q and %q",
						ambient, config.User, admin.RawQuery, tc.want, address.RawQuery)
				}
			}
		})
	}
}

func TestLoadServicesRejectsBadConfiguration(t *testing.T) {
	const password = "s3cret-pass"
	tests := []struct {
		name   string
		values map[string]string
		want   string
	}{
		{"only the plain address", map[string]string{plainServer.adminEnv: plainAddress}, vectorServer.adminEnv},
		{"only the pgvector address", map[string]string{vectorServer.adminEnv: vectorAddress}, plainServer.adminEnv},
		{"empty address", map[string]string{plainServer.adminEnv: "", vectorServer.adminEnv: vectorAddress}, plainServer.adminEnv},
		{"another scheme", map[string]string{plainServer.adminEnv: "mysql://rag:" + password + "@127.0.0.1/db", vectorServer.adminEnv: vectorAddress}, plainServer.adminEnv},
		{"missing host", map[string]string{plainServer.adminEnv: plainAddress, vectorServer.adminEnv: "postgres://rag:" + password + "@/db"}, vectorServer.adminEnv},
		{"keyword form", map[string]string{plainServer.adminEnv: "host=127.0.0.1 user=rag password=" + password, vectorServer.adminEnv: vectorAddress}, plainServer.adminEnv},
		{"bad port", map[string]string{plainServer.adminEnv: "postgres://rag:" + password + "@127.0.0.1:port/db", vectorServer.adminEnv: vectorAddress}, plainServer.adminEnv},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			admins, err := loadServices(environment(tt.values))
			if err == nil {
				t.Fatalf("loadServices() = %v, want an error", admins)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not name %s", err, tt.want)
			}
			if strings.Contains(err.Error(), password) {
				t.Fatalf("error %q repeats the password", err)
			}
		})
	}
}

func TestLoadServicesReadsOnlyTheTestVariables(t *testing.T) {
	var asked []string
	values := map[string]string{
		plainServer.adminEnv:  plainAddress,
		vectorServer.adminEnv: vectorAddress,
		"DATABASE_URL":        serviceAddress("production", "unrelated", "db.internal:5432"),
	}
	_, err := loadServices(func(name string) (string, bool) {
		asked = append(asked, name)
		value, ok := values[name]
		return value, ok
	})
	if err != nil {
		t.Fatalf("loadServices() error = %v", err)
	}
	slices.Sort(asked)
	want := []string{plainServer.adminEnv, vectorServer.adminEnv}
	slices.Sort(want)
	if !slices.Equal(asked, want) {
		t.Fatalf("loadServices() read %v, want only %v", asked, want)
	}
}

func TestWithDatabaseKeepsCredentialsAndParameters(t *testing.T) {
	admin, err := url.Parse("postgres://rag:p%40ss%2Fw%3Ard%3F@127.0.0.1:5433/postgres?sslmode=disable&application_name=quarry%20test&connect_timeout=7")
	if err != nil {
		t.Fatal(err)
	}
	derived := withDatabase(admin, "quarry_test_0123456789abcdef")
	if admin.Path != "/postgres" {
		t.Fatalf("withDatabase changed its argument: %q", admin.Path)
	}
	config, err := pgx.ParseConfig(derived.String())
	if err != nil {
		t.Fatalf("ParseConfig(%q) error = %v", derived.Redacted(), err)
	}
	if config.Database != "quarry_test_0123456789abcdef" || config.User != "rag" || config.Password != "p@ss/w:rd?" ||
		config.Host != "127.0.0.1" || config.Port != 5433 {
		t.Fatalf("derived address = %+v", config)
	}
	if got := config.RuntimeParams["application_name"]; got != "quarry test" {
		t.Fatalf("application_name = %q", got)
	}
	if derived.Query().Get("sslmode") != "disable" || derived.Query().Get("connect_timeout") != "7" {
		t.Fatalf("query = %q", derived.RawQuery)
	}
}

func TestWithCredentialsOverridesAdministratorQueryCredentials(t *testing.T) {
	for _, tc := range []struct {
		name    string
		address string
	}{
		{"userinfo", "postgres://cluster_admin:fixture_only@127.0.0.1/postgres?sslmode=disable&application_name=quarry%20test"},
		{"query", "postgres://127.0.0.1/postgres?user=cluster_admin&password=fixture_only&sslmode=disable&application_name=quarry%20test"},
		{"encoded query keys", "postgres://127.0.0.1/postgres?%75ser=cluster_admin&pass%77ord=fixture_only&sslmode=disable&application_name=quarry%20test"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admin, err := parseAdmin(tc.address)
			if err != nil {
				t.Fatal(err)
			}
			limited := withCredentials(withDatabase(admin, "owned_database"), "limited_admin", "limited")
			config, err := pgx.ParseConfig(limited.String())
			if err != nil {
				t.Fatal(err)
			}
			if config.User != "limited_admin" || config.Password != "limited" {
				t.Errorf("limited-role connection user = %q, limited password = %t", config.User, config.Password == "limited")
			}
			if got := limited.RawQuery; !strings.Contains(got, "sslmode=disable&application_name=quarry%20test") {
				t.Error("unrelated encoded options changed")
			}
		})
	}
}

func TestNewNameIsUniqueAndWithinTheIdentifierLimit(t *testing.T) {
	seen := map[string]bool{}
	for range 2000 {
		name, err := newName("quarry_test")
		if err != nil {
			t.Fatal(err)
		}
		if len(name) > maxIdentifier || seen[name] {
			t.Fatalf("name %q is too long or repeated", name)
		}
		seen[name] = true
	}
}

func TestDropStatementsRemoveDatabasesBeforeRoles(t *testing.T) {
	f := &fixture{
		databases: []string{"first", "second", `quoted"third`},
		roles:     []string{"reader", "owner"},
	}
	want := []string{
		`DROP DATABASE IF EXISTS "quoted""third" WITH (FORCE)`,
		`DROP DATABASE IF EXISTS "second" WITH (FORCE)`,
		`DROP DATABASE IF EXISTS "first" WITH (FORCE)`,
		`DROP ROLE IF EXISTS "owner"`,
		`DROP ROLE IF EXISTS "reader"`,
	}
	if got := f.dropStatements(); !slices.Equal(got, want) {
		t.Fatalf("dropStatements() = %q, want %q", got, want)
	}
}

// recorder captures a fatal report from code under test.
type recorder struct {
	testing.TB
	message string
}

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.message = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// fatalMessage runs fn and returns what it reported with Fatalf. An empty
// string means fn finished without a fatal report.
func fatalMessage(t *testing.T, fn func(testing.TB)) string {
	t.Helper()
	r := &recorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(r)
	}()
	<-done
	return r.message
}

func registeredFixture(t *testing.T) (f *fixture, address string) {
	t.Helper()
	admin, err := url.Parse(plainAddress)
	if err != nil {
		t.Fatal(err)
	}
	f = &fixture{admin: admin, disposable: true}
	database := withDatabase(admin, "quarry_test_registered")
	f.publish(database)
	t.Cleanup(func() { _ = f.release(t.Context()) })
	return f, database.String()
}

func TestRoleNamesAreUniqueAndRecorded(t *testing.T) {
	f, address := registeredFixture(t)
	first := Role(t, address, "catalog_reader")
	second := Role(t, address, "catalog_reader")
	if first == second || !strings.HasPrefix(first, "catalog_reader_") || len(first) > maxIdentifier {
		t.Fatalf("Role() returned %q and %q", first, second)
	}
	if !slices.Equal(f.roles, []string{first, second}) {
		t.Fatalf("fixture roles = %v", f.roles)
	}
}

func TestRoleAndSiblingRejectInvalidRequests(t *testing.T) {
	_, address := registeredFixture(t)
	tests := []struct {
		name string
		fn   func(testing.TB)
		want string
	}{
		{"role for an unknown address", func(tb testing.TB) { Role(tb, plainAddress, "reader") }, "was not returned by Start or StartVector"},
		{"role with an upper case base", func(tb testing.TB) { Role(tb, address, "Reader") }, "lower case"},
		{"role that is too long", func(tb testing.TB) { Role(tb, address, strings.Repeat("r", 60)) }, "longer than"},
		{"sibling for an unknown address", func(tb testing.TB) { Sibling(tb, plainAddress) }, "was not returned by Start or StartVector"},
		{"address that is not a URL", func(tb testing.TB) { Role(tb, "postgres://[::1", "reader") }, "not a URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fatalMessage(t, tt.fn); !strings.Contains(got, tt.want) {
				t.Fatalf("fatal report %q does not contain %q", got, tt.want)
			}
		})
	}
}

func TestReleaseForgetsTheFixture(t *testing.T) {
	f, address := registeredFixture(t)
	if err := f.release(t.Context()); err != nil {
		t.Fatalf("release() error = %v", err)
	}
	got := fatalMessage(t, func(tb testing.TB) { Role(tb, address, "reader") })
	if !strings.Contains(got, "was not returned by Start or StartVector") {
		t.Fatalf("a released fixture is still reachable: %q", got)
	}
}

func TestFixtureCleanupWithoutLeakedConnection(t *testing.T) {
	// Fixture setup can fail before it reaches the connection it leaves open.
	checkLeakedConnectionClosed(t, nil)
}

func TestConcurrentProvisionFailureCleansUpSuccessfulSecondFixture(t *testing.T) {
	f, _ := registeredFixture(t)
	key := f.addresses[0]
	t.Run("first provision fails", func(t *testing.T) {
		got := fatalMessage(t, func(tb testing.TB) {
			requireConcurrentFixtures(tb, []*fixture{nil, f}, []error{errors.New("injected setup failure"), nil})
		})
		if got != "provision 0: injected setup failure" {
			t.Fatalf("fatal report = %q, want the first provision error", got)
		}
	})
	if _, ok := fixtures.Load(key); ok {
		t.Error("fixture from successful second provision still registered after subtest cleanup")
	}
}

func TestMissingExtensionCheckCleansUpUnexpectedFixture(t *testing.T) {
	f, _ := registeredFixture(t)
	key := f.addresses[0]
	t.Run("vector unexpectedly available", func(t *testing.T) {
		got := fatalMessage(t, func(tb testing.TB) { requireMissingExtension(tb, f, nil) })
		if !strings.Contains(got, "want a missing extension error without a fixture") {
			t.Fatalf("fatal report = %q, want the missing extension assertion", got)
		}
	})
	if _, ok := fixtures.Load(key); ok {
		t.Error("unexpectedly provisioned fixture still registered after subtest cleanup")
	}
}

func TestShortSkipsConfiguredDatabaseFixtures(t *testing.T) {
	if !testing.Short() {
		t.Skip("requires short mode")
	}
	t.Setenv(plainServer.adminEnv, plainAddress)
	t.Setenv(vectorServer.adminEnv, vectorAddress)

	t.Run("plain", func(t *testing.T) {
		Start(t)
		t.Error("Start returned in short mode")
	})
	t.Run("vector", func(t *testing.T) {
		StartVector(t)
		t.Error("StartVector returned in short mode")
	})
}

func TestStartFailsForPartialConfiguration(t *testing.T) {
	t.Setenv(plainServer.adminEnv, plainAddress)
	// The other variable must be absent, not empty.
	unset(t, vectorServer.adminEnv)
	got := fatalMessage(t, func(tb testing.TB) { startBackend(tb, plainServer) })
	if !strings.Contains(got, vectorServer.adminEnv) {
		t.Fatalf("fatal report %q does not name %s", got, vectorServer.adminEnv)
	}
}

func TestStartFailsForUnreachableService(t *testing.T) {
	unreachable := serviceAddress("rag", "rag", "127.0.0.1:1") + "&connect_timeout=2"
	t.Setenv(plainServer.adminEnv, unreachable)
	t.Setenv(vectorServer.adminEnv, unreachable)
	for _, s := range []server{plainServer, vectorServer} {
		got := fatalMessage(t, func(tb testing.TB) { startBackend(tb, s) })
		if !strings.Contains(got, "127.0.0.1:1") || strings.Contains(got, "rag:rag") {
			t.Fatalf("fatal report %q should name the host and not the credentials", got)
		}
	}
}
