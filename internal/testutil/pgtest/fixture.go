package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// setupTimeout bounds creating a database on a service.
	setupTimeout = 30 * time.Second
	// cleanupTimeout bounds dropping what a fixture created.
	cleanupTimeout = 30 * time.Second
	// maxIdentifier is the longest name PostgreSQL keeps for an identifier.
	maxIdentifier = 63
)

// fixtures maps the host and database of every address handed out to the
// fixture that owns it, so that helpers can find a fixture from its URL.
var fixtures sync.Map

// fixture is the set of databases and roles one test created on a server.
type fixture struct {
	mu sync.Mutex
	// admin is an address with administrator rights on the server.
	admin *url.URL
	// disposable reports that the server is removed with the test, so nothing
	// needs dropping.
	disposable bool
	// databases and roles list what the test owns, in creation order.
	databases []string
	roles     []string
	// addresses are the keys the fixture holds in fixtures.
	addresses []string
}

func addressKey(u *url.URL) string {
	return u.Host + u.Path
}

// withDatabase returns a copy of u that names database and preserves unrelated
// connection parameters in their original form.
func withDatabase(u *url.URL, database string) *url.URL {
	c := *u
	c.Path = "/" + database
	c.RawPath = ""
	// pgx applies query parameters after the path, so remove both spellings
	// of the database selector before handing the URL to a client.
	c.RawQuery = queryWithout(u.RawQuery, "dbname", "database")
	return &c
}

func queryWithout(rawQuery string, keys ...string) string {
	parameters := slices.DeleteFunc(strings.Split(rawQuery, "&"), func(parameter string) bool {
		key, _, _ := strings.Cut(parameter, "=")
		decoded, _ := url.PathUnescape(strings.Trim(key, " "))
		return slices.Contains(keys, decoded)
	})
	return strings.Join(parameters, "&")
}

// newName returns a name that is unique in a cluster. It stays within the
// identifier limit when prefix does.
func newName(prefix string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate name: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(b[:]), nil
}

func (f *fixture) publish(u *url.URL) {
	key := addressKey(u)
	f.addresses = append(f.addresses, key)
	fixtures.Store(key, f)
}

// addDatabase creates an empty database next to the fixture database and
// returns its address. The database is recorded before it is created, so
// that a failed creation is still dropped.
func (f *fixture) addDatabase(ctx context.Context, options string) (*url.URL, error) {
	name, err := newName("quarry_test")
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.databases = append(f.databases, name)
	conn, err := connect(ctx, f.admin)
	if err != nil {
		return nil, err
	}
	defer closeConn(ctx, conn)
	if err := createDatabase(ctx, conn, name, options); err != nil {
		return nil, err
	}
	u := withDatabase(f.admin, name)
	f.publish(u)
	return u, nil
}

func createDatabase(ctx context.Context, conn *pgx.Conn, name, options string) error {
	query := "CREATE DATABASE " + pgx.Identifier{name}.Sanitize() + " TEMPLATE template0"
	if options != "" {
		query += " " + options
	}
	if _, err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// dropStatements lists what removes the fixture: databases first, newest
// first, then the roles that may own or have been granted access to them.
func (f *fixture) dropStatements() []string {
	var statements []string
	for i := len(f.databases) - 1; i >= 0; i-- {
		statements = append(statements, "DROP DATABASE IF EXISTS "+pgx.Identifier{f.databases[i]}.Sanitize()+" WITH (FORCE)")
	}
	for i := len(f.roles) - 1; i >= 0; i-- {
		statements = append(statements, "DROP ROLE IF EXISTS "+pgx.Identifier{f.roles[i]}.Sanitize())
	}
	return statements
}

// release forgets the fixture's addresses and drops what it created. Every
// statement is attempted, and the errors are joined.
func (f *fixture) release(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range f.addresses {
		fixtures.Delete(key)
	}
	f.addresses = nil
	if f.disposable {
		return nil
	}
	statements := f.dropStatements()
	if len(statements) == 0 {
		return nil
	}
	conn, err := connect(ctx, f.admin)
	if err != nil {
		return err
	}
	defer closeConn(ctx, conn)
	var errs []error
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", statement, err))
		}
	}
	return errors.Join(errs...)
}

// cleanup returns the function a test registers to remove its fixture, which
// records how long it took.
func (f *fixture) cleanup(t testing.TB) func() {
	return func() {
		begin := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := f.release(ctx); err != nil {
			t.Errorf("pgtest: remove fixture: %v", err)
		}
		t.Logf("pgtest: cleanup %s", time.Since(begin).Round(time.Millisecond))
	}
}

func connect(ctx context.Context, admin *url.URL) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, admin.String())
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", admin.Host, err)
	}
	return conn, nil
}

// closeConn closes conn within the cleanup bound even when ctx is done.
func closeConn(ctx context.Context, conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	_ = conn.Close(ctx)
}

func lookup(t testing.TB, address string) *fixture {
	t.Helper()
	u, err := url.Parse(address)
	if err != nil {
		t.Fatalf("pgtest: the address is not a URL")
	}
	f, ok := fixtures.Load(addressKey(u))
	if !ok {
		t.Fatalf("pgtest: %s was not returned by Start or StartVector", addressKey(u))
	}
	return f.(*fixture)
}

// Sibling returns the address of a new empty database on the same server as
// the fixture database at fixtureURL. It is dropped when the test ends,
// before the fixture database.
func Sibling(t testing.TB, fixtureURL string) string {
	t.Helper()
	return sibling(t, fixtureURL, "")
}

// SiblingLatin1 is like Sibling for a database whose server encoding is
// LATIN1 and whose collation is C.
func SiblingLatin1(t testing.TB, fixtureURL string) string {
	t.Helper()
	return sibling(t, fixtureURL, "ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C'")
}

// SiblingNumericCollation is like Sibling for a database where digit sequences
// sort by numeric value under ICU collation.
func SiblingNumericCollation(t testing.TB, fixtureURL string) string {
	t.Helper()
	return sibling(t, fixtureURL, "LOCALE_PROVIDER icu ICU_LOCALE 'en-u-kn-true'")
}

func sibling(t testing.TB, fixtureURL, options string) string {
	t.Helper()
	f := lookup(t, fixtureURL)
	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()
	u, err := f.addDatabase(ctx, options)
	if err != nil {
		t.Fatalf("pgtest: add database: %v", err)
	}
	return u.String()
}

var roleBase = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Role returns a cluster-wide role name that no other test uses, made from
// base. The test creates the role itself. It is dropped when the test ends,
// after every database of the fixture.
func Role(t testing.TB, fixtureURL, base string) string {
	t.Helper()
	f := lookup(t, fixtureURL)
	if !roleBase.MatchString(base) {
		t.Fatalf("pgtest: role base %q must be lower case letters, digits and underscores", base)
	}
	name, err := newName(base)
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	if len(name) > maxIdentifier {
		t.Fatalf("pgtest: role %q is longer than %d bytes", name, maxIdentifier)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.roles = append(f.roles, name)
	return name
}
