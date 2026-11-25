package pgtest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// loadServices reads the administrator addresses of the shared test services
// through lookup. It returns nil without an error when neither variable is
// set, which selects disposable containers. An address that is set but empty,
// only one of the two variables, or an address that is not a PostgreSQL URL is
// an error, so a misconfigured check cannot fall back to containers unnoticed.
// Only the two test variables are read: the service's own connection settings
// never supply an administrator.
func loadServices(lookup func(string) (string, bool)) (map[string]*url.URL, error) {
	servers := []server{plainServer, vectorServer}
	raw := make(map[string]string, len(servers))
	var missing []string
	for _, s := range servers {
		if value, ok := lookup(s.adminEnv); ok {
			raw[s.adminEnv] = value
		} else {
			missing = append(missing, s.adminEnv)
		}
	}
	switch {
	case len(raw) == 0:
		return nil, nil
	case len(missing) > 0:
		return nil, fmt.Errorf("%s is set but %s is not: set both or neither", setNames(servers, raw), missing[0])
	}
	admins := make(map[string]*url.URL, len(servers))
	for _, s := range servers {
		u, err := parseAdmin(raw[s.adminEnv])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", s.adminEnv, err)
		}
		admins[s.adminEnv] = u
	}
	return admins, nil
}

func setNames(servers []server, raw map[string]string) string {
	for _, s := range servers {
		if _, ok := raw[s.adminEnv]; ok {
			return s.adminEnv
		}
	}
	return ""
}

// parseAdmin accepts a PostgreSQL URL. The error never repeats the value,
// which carries a password.
func parseAdmin(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("is not a valid URL")
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, errors.New("must be a postgres:// or postgresql:// URL")
	}
	host := u.Hostname()
	if host == "" {
		return nil, errors.New("must name a host")
	}
	if u.Port() == "" && !strings.Contains(u.Host, ",") {
		// Pin the default before pgx can supply an ambient PGPORT.
		u.Host = net.JoinHostPort(host, "5432")
	}
	if u.Path == "" || u.Path == "/" {
		// A query database selector still takes precedence over the path.
		u.Path = "/postgres"
	}
	if u.User.Username() == "" && queryWithout(u.RawQuery, "user") == u.RawQuery {
		return nil, errors.New("must name an administrator")
	}
	config, err := pgx.ParseConfig(u.String())
	if err != nil {
		return nil, errors.New("is not a connection URL PostgreSQL clients accept")
	}
	if config.User == "" {
		return nil, errors.New("must name an administrator")
	}
	return u, nil
}

// requireServer checks that the administrator can create databases and roles
// and that the server offers what s needs.
func requireServer(ctx context.Context, conn *pgx.Conn, s server) error {
	var superuser bool
	if err := conn.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&superuser); err != nil {
		return fmt.Errorf("read administrator rights: %w", err)
	}
	if !superuser {
		return fmt.Errorf("the %s test service administrator must be a superuser", s.name)
	}
	if s.extension == "" {
		return nil
	}
	var offered bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_available_extensions WHERE name = $1)`, s.extension).Scan(&offered); err != nil {
		return fmt.Errorf("read available extensions: %w", err)
	}
	if !offered {
		return fmt.Errorf("the %s test service does not offer the %s extension", s.name, s.extension)
	}
	return nil
}

// provision creates the empty fixture database on the service at admin. It
// returns the fixture, and the address of its database, as soon as the
// database may exist, even when a later step fails, so that the caller can
// remove it. check, when not nil, runs after the database is created.
func provision(ctx context.Context, s server, admin *url.URL, check func(context.Context, *fixture) error) (*fixture, error) {
	conn, err := connect(ctx, admin)
	if err != nil {
		return nil, fmt.Errorf("%s test service: %w", s.name, err)
	}
	defer closeConn(ctx, conn)
	err = requireServer(ctx, conn, s)
	if err != nil {
		return nil, err
	}
	name, err := newName("quarry_test")
	if err != nil {
		return nil, err
	}
	f := &fixture{admin: admin, databases: []string{name}}
	if err := createDatabase(ctx, conn, name, ""); err != nil {
		return f, err
	}
	f.publish(withDatabase(admin, name))
	if check != nil {
		if err := check(ctx, f); err != nil {
			return f, err
		}
	}
	return f, nil
}

func startService(t testing.TB, s server, admin *url.URL) string {
	t.Helper()
	begin := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()
	f, err := provision(ctx, s, admin, nil)
	if f != nil {
		t.Cleanup(f.cleanup(t))
	}
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	t.Logf("pgtest: setup %s", time.Since(begin).Round(time.Millisecond))
	return withDatabase(admin, f.databases[0]).String()
}
