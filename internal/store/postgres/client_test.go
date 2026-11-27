package postgres

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	healthCheckParameter    = "pool_health_check_period"
	healthCheckConfigError  = "postgres: configure pool: pool_health_check_period must be greater than zero"
	healthCheckTestHost     = "127.0.0.1"
	healthCheckTestPassword = "fixture-password"
	healthCheckTestDatabase = "fixture_database"
	healthCheckChildCase    = "QUARRY_TEST_POOL_HEALTH_CHECK_CHILD"
)

func TestNewClientRejectsNonpositiveHealthCheckPeriod(t *testing.T) {
	runHealthCheckSubprocess(t, "nonpositive")
}

func checkNonpositiveHealthCheckPeriods(t *testing.T) {
	check := func(t *testing.T, dsn string, opts ...Option) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		var connects atomic.Int32
		opts = append(opts, func(config *pgxpool.Config) {
			config.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
				connects.Add(1)
				return errors.New("unexpected connect attempt")
			}
		})
		client, err := NewClient(ctx, dsn, opts...)
		require.Nil(t, client)
		require.Zero(t, connects.Load())
		require.EqualError(t, err, healthCheckConfigError)
		for _, field := range []string{healthCheckTestPassword, healthCheckTestHost, healthCheckTestDatabase} {
			require.NotContains(t, err.Error(), field)
		}
	}
	for _, period := range []string{"0s", "0", "-1s", "-1ns", "-0s"} {
		t.Run("keyword/"+period, func(t *testing.T) { check(t, keywordHealthCheckDSN(period)) })
		t.Run("uri/"+period, func(t *testing.T) { check(t, uriHealthCheckDSN(period)) })
	}
	for _, period := range []time.Duration{0, -time.Second} {
		t.Run("option/"+period.String(), func(t *testing.T) {
			check(t, keywordHealthCheckDSN("2m"), setHealthCheckPeriod(period))
		})
	}
}

func TestPoolHealthCheckPeriodParsesSupportedValues(t *testing.T) {
	t.Setenv("PGSERVICE", "")
	formats := []struct {
		name string
		dsn  func(string) string
	}{
		{"keyword", keywordHealthCheckDSN},
		{"uri", uriHealthCheckDSN},
	}
	for _, format := range formats {
		t.Run(format.name, func(t *testing.T) {
			for _, tc := range []struct {
				name, setting string
				want          time.Duration
			}{
				{"omitted", "", time.Minute},
				{"minimum", "1ns", time.Nanosecond},
				{"milliseconds", "100ms", 100 * time.Millisecond},
				{"minutes", "2m", 2 * time.Minute},
				{"maximum", "9223372036854775807ns", time.Duration(1<<63 - 1)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					config, err := poolConfig(format.dsn(tc.setting))
					require.NoError(t, err)
					require.Equal(t, tc.want, config.HealthCheckPeriod)
					require.NotContains(t, config.ConnConfig.RuntimeParams, healthCheckParameter)
				})
			}
			for _, setting := range []string{"4fortnights", "9223372036854775808ns"} {
				t.Run(setting, func(t *testing.T) {
					client, err := NewClient(t.Context(), format.dsn(setting), setHealthCheckPeriod(time.Minute))
					require.Nil(t, client)
					require.ErrorContains(t, err, "postgres: configure pool:")
					require.ErrorContains(t, err, "cannot parse "+healthCheckParameter)
					var parseErr *pgconn.ParseConfigError
					require.ErrorAs(t, err, &parseErr)
				})
			}
		})
	}
}

func TestNewClientUsesEffectivePositiveHealthCheckPeriod(t *testing.T) {
	dsn := startTestDatabase(t)
	base, err := url.Parse(dsn)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, setting string
		opts          []Option
		want          time.Duration
	}{
		{name: "omitted", want: time.Minute},
		{name: "explicit", setting: "2m", want: 2 * time.Minute},
		{name: "option", opts: []Option{setHealthCheckPeriod(100 * time.Millisecond)}, want: 100 * time.Millisecond},
		{
			name: "last option overrides nonpositive period", setting: "0s",
			opts: []Option{setHealthCheckPeriod(-time.Second), setHealthCheckPeriod(100 * time.Millisecond)},
			want: 100 * time.Millisecond,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connection := *base
			query := connection.Query()
			if tc.setting == "" {
				query.Del(healthCheckParameter)
			} else {
				query.Set(healthCheckParameter, tc.setting)
			}
			connection.RawQuery = query.Encode()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			client, err := NewClient(ctx, connection.String(), tc.opts...)
			require.NoError(t, err)
			defer func() { require.NoError(t, client.Close(ctx)) }()
			require.Equal(t, tc.want, client.Pool.Config().HealthCheckPeriod)
			require.NoError(t, client.Ping(ctx))
		})
	}
}

func TestInvalidHealthCheckPeriodWithCanceledStartup(t *testing.T) {
	runHealthCheckSubprocess(t, "canceled")
}

func checkCanceledHealthCheckStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var connects atomic.Int32
	client, err := NewClient(ctx, keywordHealthCheckDSN("0s"), func(config *pgxpool.Config) {
		config.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
			connects.Add(1)
			return errors.New("unexpected connect attempt")
		}
	})
	require.Nil(t, client)
	require.Zero(t, connects.Load())
	require.EqualError(t, err, healthCheckConfigError)

	client, err = NewClient(ctx, keywordHealthCheckDSN("2m"))
	require.Nil(t, client)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "postgres: ping:")
}

func TestPoolHealthCheckSubprocess(t *testing.T) {
	switch os.Getenv(healthCheckChildCase) {
	case "":
		t.Skip("subprocess entry point")
	case "nonpositive":
		checkNonpositiveHealthCheckPeriods(t)
	case "canceled":
		checkCanceledHealthCheckStartup(t)
	default:
		t.Fatal("unknown pool health check scenario")
	}
}

func runHealthCheckSubprocess(t *testing.T, scenario string) {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	watchdog, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var output bytes.Buffer
	cmd := &exec.Cmd{
		Path:      executable,
		Args:      []string{executable, "-test.run=^TestPoolHealthCheckSubprocess$"},
		Stdout:    &output,
		Stderr:    &output,
		WaitDelay: time.Second,
	}
	cmd.Env = slices.DeleteFunc(cmd.Environ(), func(entry string) bool { return strings.HasPrefix(entry, "PG") })
	cmd.Env = append(cmd.Env, healthCheckChildCase+"="+scenario)
	require.NoError(t, cmd.Start())
	stopKill := context.AfterFunc(watchdog, func() { _ = cmd.Process.Kill() })
	defer stopKill()
	err = cmd.Wait()
	require.NoError(t, watchdog.Err(), "pool startup exceeded its watchdog: %s", &output)
	require.NoError(t, err, "%s", &output)
}

func keywordHealthCheckDSN(period string) string {
	dsn := "host=" + healthCheckTestHost + " port=1 user=fixture password=" + healthCheckTestPassword +
		" dbname=" + healthCheckTestDatabase + " sslmode=disable"
	if period != "" {
		dsn += " " + healthCheckParameter + "=" + period
	}
	return dsn
}

func uriHealthCheckDSN(period string) string {
	query := url.Values{"sslmode": {"disable"}}
	if period != "" {
		query.Set(healthCheckParameter, period)
	}
	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("fixture", healthCheckTestPassword),
		Host:     healthCheckTestHost + ":1",
		Path:     "/" + healthCheckTestDatabase,
		RawQuery: query.Encode(),
	}
	return dsn.String()
}

func setHealthCheckPeriod(period time.Duration) Option {
	return func(config *pgxpool.Config) { config.HealthCheckPeriod = period }
}
