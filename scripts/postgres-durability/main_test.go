package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestDurabilityPreflightModesAndExitStatus(t *testing.T) {
	for _, args := range [][]string{
		nil, {"-mode"}, {"-mode", ""}, {"-mode", "LOCAL"}, {"-mode", strings.Repeat("x", 8192)},
		{"-mode", "local", "-mode", "local"}, {"--mode", "local"}, {"-mode=local"},
		{"-mode", "local", "-x", "SECRET"}, {"-mode", "local", "SECRET"},
		{"-mode", "local", "-standby", "node"}, {"-mode", "ha-session", "-standby", "node"},
		{"-mode", "ha-topology"}, {"-mode", "ha-topology", "-standby", ""},
		{"-mode", "ha-topology", "-standby", "first"},
		{"-mode", "ha-topology", "-standby", "one,two"},
		{"-mode", "ha-topology", "-standby", strings.Repeat("x", 64)},
		{"-mode", "ha-topology", "-standby", "node", "-standby", "node"},
	} {
		var out, stderr bytes.Buffer
		if got := run(t.Context(), args, testEnv("SECRET"), &out, &stderr); got != 2 || out.Len() != 0 || stderr.String() != usage+"\n" {
			t.Fatalf("invalid invocation: code=%d stdout=%q stderr=%q", got, out.String(), stderr.String())
		}
	}
	for _, lookup := range []func(string) (string, bool){testEnv(""), func(string) (string, bool) { return "", false }} {
		var out, stderr bytes.Buffer
		if got := run(t.Context(), []string{"-mode", "local"}, lookup, &out, &stderr); got != 2 || stderr.String() != usage+"\n" || out.Len() != 0 {
			t.Fatalf("missing DSN: code=%d stdout=%q stderr=%q", got, out.String(), stderr.String())
		}
	}

	reports := []struct {
		mode           preflightMode
		reason, status string
		code           int
	}{
		{modeLocal, "primary_settings_valid", "pass", 0},
		{modeHASession, "ha_session_settings_valid", "pass", 0},
		{modeHATopology, "standby_snapshot_valid", "pass", 0},
		{modeLocal, "not_primary", "fail", 1},
		{modeLocal, "unsupported_server", "fail", 1},
		{modeLocal, "unsafe_wal_settings", "fail", 1},
		{modeLocal, "unsafe_commit_mode", "fail", 1},
		{modeHASession, "nonzero_statement_timeout", "fail", 1},
		{modeHATopology, "unsupported_standby_configuration", "fail", 1},
		{modeHATopology, "standby_not_ready", "fail", 1},
		{modeLocal, "connection_unavailable", "unavailable", 1},
		{modeHATopology, "inspection_unavailable", "unavailable", 1},
		{modeLocal, "deadline_or_cancellation", "unavailable", 1},
	}
	for _, tc := range reports {
		t.Run(tc.reason, func(t *testing.T) {
			args := []string{"-mode", string(tc.mode)}
			if tc.mode == modeHATopology {
				args = append([]string{"-standby", "Named_Node"}, args...)
			}
			const dsn = "password=SECRET"
			var out, stderr bytes.Buffer
			got := runWithInspection(t.Context(), args, testEnv(dsn), &out, &stderr, func(_ context.Context, gotDSN string, mode preflightMode, standby string) reason {
				if gotDSN != dsn || mode != tc.mode || (mode == modeHATopology && standby != "Named_Node") {
					t.Error("inspection did not receive the exact arguments")
				}
				return reason(tc.reason)
			})
			assertOutput(t, got, tc.code, &out, &stderr, report{tc.mode, tc.status, reason(tc.reason)})
		})
	}

	for _, dsn := range []string{"postgres://user:SECRET@localhost:invalid/private", "password='SECRET"} {
		var out, stderr bytes.Buffer
		got := run(t.Context(), []string{"-mode", "local"}, testEnv(dsn), &out, &stderr)
		assertOutput(t, got, 1, &out, &stderr, report{modeLocal, "unavailable", "connection_unavailable"})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var out, stderr bytes.Buffer
	got := run(ctx, []string{"-mode", "local"}, testEnv("password=SECRET"), &out, &stderr)
	assertOutput(t, got, 1, &out, &stderr, report{modeLocal, "unavailable", "deadline_or_cancellation"})
}

func TestDurabilityPreflightDeadlineStopsStartupAndPoolCleanup(t *testing.T) {
	for _, poolOptions := range []string{
		"pool_min_idle_conns=0",
		"pool_min_idle_conns=5",
		"pool_min_conns=5",
	} {
		t.Run(poolOptions, func(t *testing.T) {
			listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() { _ = listener.Close() }()
			host, port, err := net.SplitHostPort(listener.Addr().String())
			require.NoError(t, err)
			dsn := fmt.Sprintf("host=%s port=%s user=journal dbname=quarry password=SECRET sslmode=disable connect_timeout=10 %s", host, port, poolOptions)

			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			completed := make(chan reason, 1)
			go func() { completed <- inspect(ctx, dsn, modeLocal, "") }()
			tcp := listener.(*net.TCPListener)
			require.NoError(t, tcp.SetDeadline(time.Now().Add(5*time.Second)))
			conn, err := tcp.AcceptTCP()
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
			n, err := io.Copy(io.Discard, conn)
			require.NoError(t, err, "the probe must close the stalled startup socket")
			require.Positive(t, n, "the socket must receive a startup message")
			select {
			case got := <-completed:
				require.Equal(t, deadlineOrCancellation, got)
			case <-time.After(5 * time.Second):
				t.Fatal("inspection did not finish pool cleanup")
			}
			require.NoError(t, tcp.SetDeadline(time.Now().Add(100*time.Millisecond)))
			extra, err := tcp.AcceptTCP()
			if err == nil {
				_ = extra.Close()
				t.Fatal("inspection opened more than one socket")
			}
			require.ErrorIs(t, err, os.ErrDeadlineExceeded)
		})
	}
}

func TestDurabilityPreflightPoolSettingsOnConnectionFailure(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	for _, interval := range []string{"0s", "-1s"} {
		t.Run(interval, func(t *testing.T) {
			dsn := "postgres://journal:SECRET@" + address + "/quarry?sslmode=disable&pool_max_conns=1024&pool_health_check_period=" + interval
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			child := startDurabilityProcess(ctx, t, dsn)
			<-child.done
			require.NoError(t, ctx.Err())
			var exit *exec.ExitError
			require.ErrorAs(t, child.err, &exit)
			assertOutput(t, exit.ExitCode(), 1, &child.stdout, &child.stderr, report{modeLocal, "unavailable", "connection_unavailable"})
		})
	}
}

func TestDurabilityPreflightPoolConfigPreservesWriterSettings(t *testing.T) {
	passfile := filepath.Join(t.TempDir(), "pgpass")
	dsn := fmt.Sprintf("host=db,standby port=5432 dbname=quarry user=journal sslmode=require sslrootcert='' passfile='%s' "+
		"target_session_attrs=read-write synchronous_commit=remote_apply statement_timeout=0 "+
		"pool_max_conns=2147483647 pool_min_conns=2147483647 pool_min_idle_conns=2147483647 "+
		"pool_max_conn_lifetime=0s pool_max_conn_idle_time=-1s pool_max_conn_lifetime_jitter=100h pool_ping_timeout=1ns pool_health_check_period=1ns", passfile)
	t.Setenv("PGPASSWORD", "")
	for _, password := range []string{"original", "rotated"} {
		require.NoError(t, os.WriteFile(passfile, []byte("db:5432:quarry:journal:"+password+"\n"), 0o600))
		config, err := pgxpool.ParseConfig(dsn)
		require.NoError(t, err)
		writer := config.ConnConfig.Copy()
		tls := config.ConnConfig.TLSConfig
		configureInspectionPool(t.Context())(config)

		require.EqualValues(t, 1, config.MaxConns)
		require.Zero(t, config.MinConns)
		require.Zero(t, config.MinIdleConns)
		require.Equal(t, inspectionTimeout, config.HealthCheckPeriod)
		require.Equal(t, inspectionTimeout, config.MaxConnLifetime)
		require.Equal(t, inspectionTimeout, config.MaxConnIdleTime)
		require.Zero(t, config.MaxConnLifetimeJitter)
		require.Zero(t, config.PingTimeout)

		connection := config.ConnConfig
		require.Equal(t, password, connection.Password)
		require.Equal(t, writer.Host, connection.Host)
		require.Equal(t, writer.Port, connection.Port)
		require.Equal(t, writer.User, connection.User)
		require.Equal(t, writer.Database, connection.Database)
		require.True(t, maps.Equal(writer.RuntimeParams, connection.RuntimeParams))
		require.Same(t, tls, connection.TLSConfig)
		require.Len(t, connection.Fallbacks, 1)
		require.Equal(t, "standby", connection.Fallbacks[0].Host)
		require.NotNil(t, connection.Fallbacks[0].TLSConfig)
		require.NotNil(t, connection.ValidateConnect)
	}
}

func TestDurabilityPreflightDiscardsLatePreparation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	preparing := make(chan struct{})
	resume := make(chan struct{})
	prepared := make(chan error, 1)
	dialed := make(chan struct{}, 1)
	t.Cleanup(func() { close(resume) })
	const dsn = "host=127.0.0.1 port=54321 user=journal dbname=quarry password=SECRET sslmode=disable"
	var out, stderr bytes.Buffer
	completed := make(chan int, 1)
	go func() {
		completed <- runWithInspection(ctx, []string{"-mode", "local"}, testEnv(dsn), &out, &stderr, func(ctx context.Context, dsn string, _ preflightMode, _ string) reason {
			close(preparing)
			<-resume
			client, err := postgres.NewClient(ctx, dsn, func(config *pgxpool.Config) {
				config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
					dialed <- struct{}{}
					return nil, io.EOF
				}
				configureInspectionPool(ctx)(config)
			})
			if client != nil {
				_ = client.Close(ctx)
			}
			prepared <- err
			return primarySettingsValid
		})
	}()
	<-preparing
	cancel()
	select {
	case code := <-completed:
		assertOutput(t, code, 1, &out, &stderr, report{modeLocal, "unavailable", deadlineOrCancellation})
	case <-time.After(5 * time.Second):
		t.Fatal("canceled inspection waited for configuration")
	}
	// Release the late preparation only after the result has been reported.
	resume <- struct{}{}
	require.ErrorIs(t, <-prepared, context.Canceled)
	require.Empty(t, dialed)
}

const durabilityChild = "QUARRY_DURABILITY_TEST_CHILD"

func TestDurabilityPreflightSubprocess(_ *testing.T) {
	if os.Getenv(durabilityChild) == "1" {
		os.Args = []string{os.Args[0], "-mode", "local"}
		main()
	}
}

type durabilityProcess struct {
	*os.Process
	done           chan struct{}
	stdout, stderr bytes.Buffer
	err            error
}

func startDurabilityProcess(ctx context.Context, t *testing.T, dsn string, settings ...string) *durabilityProcess {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	child := &durabilityProcess{done: make(chan struct{})}
	cmd := &exec.Cmd{
		Path: binary, Args: []string{binary, "-test.run=^TestDurabilityPreflightSubprocess$"},
		Stdout: &child.stdout, Stderr: &child.stderr, WaitDelay: time.Second,
	}
	cmd.Env = slices.DeleteFunc(cmd.Environ(), func(entry string) bool { return strings.HasPrefix(entry, "PG") })
	cmd.Env = append(cmd.Env, durabilityChild+"=1", "DATABASE_URL="+dsn)
	cmd.Env = append(cmd.Env, settings...)
	require.NoError(t, ctx.Err())
	require.NoError(t, cmd.Start())
	child.Process = cmd.Process
	stop := context.AfterFunc(ctx, func() { _ = child.Kill() })
	go func() {
		defer close(child.done)
		defer stop()
		child.err = cmd.Wait()
	}()
	t.Cleanup(func() {
		_ = child.Kill()
		<-child.done
	})
	return child
}

func assertOutput(t *testing.T, code, expected int, out, stderr *bytes.Buffer, want report) {
	t.Helper()
	var got report
	canonical, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || code != expected || stderr.Len() != 0 || got != want || out.String() != string(canonical)+"\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q, want %d %s", code, out.String(), stderr.String(), expected, canonical)
	}
}

func testEnv(dsn string) func(string) (string, bool) {
	return func(key string) (string, bool) { return dsn, key == "DATABASE_URL" }
}
