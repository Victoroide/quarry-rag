package pgtest

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	docker "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Keep the replication fixture on the same pinned ordinary image as hosted CI.
const replicationImage = image + "@sha256:4e6e670bb069649261c9c18031f0aded7bb249a5b6664ddec29c013a89310d50"

// ReplicationStandbyName is the application name used by the physical standby.
const ReplicationStandbyName = "quarry_standby"

const standbyData = "/var/lib/postgresql/quarry-standby"

// Replication is an isolated primary and its named physical standby. Both
// DSNs use the fixture's administrator and default to asynchronous commit.
type Replication struct {
	PrimaryDSN, StandbyDSN string
	primary, standby       testcontainers.Container
}

// StartReplication always creates two Docker containers. It calls prepare on
// the primary before taking the physical base backup.
func StartReplication(t *testing.T, prepare func(context.Context, string)) *Replication {
	t.Helper()
	if testing.Short() {
		t.Skip("requires two physical PostgreSQL nodes in Docker")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	if err := isolateReaper(); err != nil {
		t.Fatal("isolate the Docker reaper for replication")
	}
	network, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatal("create isolated Docker network for replication")
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer stop()
		if err := network.Remove(cleanup); err != nil {
			t.Error("remove isolated replication network")
		}
	})
	admin, password := replicationName(t, "admin"), replicationName(t, "secret")
	peer, peerPassword := replicationName(t, "peer"), replicationName(t, "secret")
	slot := replicationName(t, "slot")
	const primaryHost = "quarry-primary"
	pair := &Replication{}
	pair.primary = replicationContainer(ctx, t, "primary",
		tcnetwork.WithNetwork([]string{primaryHost}, network),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_DB": "quarry", "POSTGRES_USER": admin, "POSTGRES_PASSWORD": password,
		}),
		testcontainers.WithCmd("postgres", "-c", "fsync=on", "-c", "full_page_writes=on",
			"-c", "synchronous_commit=off", "-c", "wal_level=replica", "-c", "max_wal_senders=4",
			"-c", "max_replication_slots=2"),
		testcontainers.WithWaitStrategyAndDeadline(time.Minute,
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp")))
	pair.PrimaryDSN = replicationDSN(ctx, t, pair.primary, admin, password)
	prepare(ctx, pair.PrimaryDSN)

	primary := replicationConnect(ctx, t, pair.PrimaryDSN)
	defer closeConn(ctx, primary)
	if _, err := primary.Exec(ctx, "CREATE ROLE "+pgx.Identifier{peer}.Sanitize()+" LOGIN REPLICATION PASSWORD '"+peerPassword+"'"); err != nil {
		t.Fatal("create physical replication login")
	}
	if _, err := primary.Exec(ctx, "SELECT pg_catalog.pg_create_physical_replication_slot($1)", slot); err != nil {
		t.Fatal("create permanent physical replication slot")
	}
	replicationExec(ctx, t, pair.primary, "add the physical replication host rule",
		"sh", "-ec", `printf '\nhost replication %s all scram-sha-256\n' "$1" >> "$PGDATA/pg_hba.conf"`, "replication", peer)
	reloadPrimary(ctx, t, primary, "")

	pair.standby = replicationContainer(ctx, t, "standby",
		tcnetwork.WithNetwork(nil, network),
		testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("3600"))
	pair.StandbyDSN = replicationDSN(ctx, t, pair.standby, admin, password)
	replicationExec(ctx, t, pair.standby, "prepare empty standby data directory",
		"install", "-d", "-m", "700", "-o", "postgres", "-g", "postgres", standbyData)
	conninfo := fmt.Sprintf("host=%s user=%s password=%s application_name=%s sslmode=disable", primaryHost, peer, peerPassword, ReplicationStandbyName)
	replicationExec(ctx, t, pair.standby, "take streaming base backup into standby",
		"gosu", "postgres", "pg_basebackup", "-d", conninfo, "-D", standbyData, "-R", "-X", "stream", "-S", slot, "-c", "fast", "-w")
	pair.startStandby(ctx, t)
	pair.waitForStreaming(ctx, t, primary, "async")

	if _, err := primary.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names = '"+ReplicationStandbyName+"'"); err != nil {
		t.Fatal("configure named synchronous candidate")
	}
	reloadPrimary(ctx, t, primary, ReplicationStandbyName)
	pair.waitForStreaming(ctx, t, primary, "sync")
	return pair
}

func replicationName(t *testing.T, prefix string) string {
	t.Helper()
	name, err := newName(prefix)
	if err != nil {
		t.Fatal("generate replication fixture identity")
	}
	return name
}

func replicationContainer(ctx context.Context, t *testing.T, name string, opts ...testcontainers.ContainerCustomizer) testcontainers.Container {
	t.Helper()
	var node *testcontainers.DockerContainer
	err := reaperShutdownRetry.run(ctx, func() error {
		var err error
		node, err = testcontainers.Run(ctx, replicationImage, append(opts, testcontainers.WithExposedPorts("5432/tcp"))...)
		if err != nil && node != nil {
			_ = testcontainers.TerminateContainer(node)
		}
		return err
	})
	if err != nil {
		t.Fatalf("start PostgreSQL %s container (%T)", name, err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer stop()
		if err := node.Terminate(cleanup); err != nil {
			t.Errorf("remove PostgreSQL %s container (%T)", name, err)
		}
	})
	return node
}

func replicationDSN(ctx context.Context, t *testing.T, node testcontainers.Container, user, password string) string {
	t.Helper()
	host, err := node.Host(ctx)
	if err != nil {
		t.Fatal("resolve PostgreSQL container host")
	}
	port, err := node.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal("resolve PostgreSQL container port")
	}
	u := url.URL{
		Scheme: "postgres", Host: net.JoinHostPort(host, port.Port()), Path: "/quarry",
		User: url.UserPassword(user, password), RawQuery: "sslmode=disable&connect_timeout=2",
	}
	return u.String()
}

func replicationExec(ctx context.Context, t *testing.T, node testcontainers.Container, action string, cmd ...string) {
	t.Helper()
	code, _, err := node.Exec(ctx, cmd)
	if err != nil || code != 0 {
		t.Fatalf("%s (exit %d, error %T)", action, code, err)
	}
}

func replicationConnect(ctx context.Context, t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to replication fixture (%T)", err)
	}
	return conn
}

func reloadPrimary(ctx context.Context, t *testing.T, primary *pgx.Conn, candidate string) {
	t.Helper()
	var previous time.Time
	if err := primary.QueryRow(ctx, "SELECT pg_catalog.pg_conf_load_time()").Scan(&previous); err != nil {
		t.Fatal("read primary configuration load time")
	}
	var signaled bool
	if err := primary.QueryRow(ctx, "SELECT pg_catalog.pg_reload_conf()").Scan(&signaled); err != nil || !signaled {
		t.Fatal("reload primary configuration")
	}
	waitForSQL(ctx, t, primary, "primary configuration reload", `SELECT
		pg_catalog.pg_conf_load_time()>$2 AND current_setting('synchronous_standby_names')=$1 AND NOT EXISTS
		(SELECT 1 FROM pg_catalog.pg_hba_file_rules WHERE error IS NOT NULL)`, candidate, previous)
}

func (r *Replication) startStandby(ctx context.Context, t *testing.T) {
	t.Helper()
	replicationExec(ctx, t, r.standby, "start physical standby", "gosu", "postgres", "pg_ctl",
		"-D", standbyData, "-l", "/tmp/quarry-postgres.log", "-w", "-t", "45",
		"-o", "-c listen_addresses='*' -c fsync=on -c full_page_writes=on -c synchronous_commit=off -c wal_receiver_status_interval=1s", "start")
}

func (r *Replication) waitForStreaming(ctx context.Context, t *testing.T, primary *pgx.Conn, state string) {
	t.Helper()
	waitForSQL(ctx, t, primary, "physical standby streaming ("+state+")", `SELECT EXISTS (
		SELECT 1 FROM pg_catalog.pg_stat_replication r
		JOIN pg_catalog.pg_replication_slots s ON s.active_pid = r.pid
		WHERE r.application_name=$1 AND r.state='streaming' AND r.sync_state=$2
		AND r.flush_lsn > '0/0' AND s.slot_type='physical' AND NOT s.temporary)`, ReplicationStandbyName, state)
}

func waitForSQL(ctx context.Context, t *testing.T, conn *pgx.Conn, action, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		var ready bool
		if err := conn.QueryRow(ctx, query, args...).Scan(&ready); err != nil {
			t.Fatalf("%s: probe failed (%T)", action, err)
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: condition did not become true", action)
		case <-tick.C:
		}
	}
}

// StopStandby disconnects the only WAL receiver and waits for the primary to
// observe the disconnect. Its data remains available to restart or promote.
func (r *Replication) StopStandby(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	replicationExec(ctx, t, r.standby, "stop physical standby", "gosu", "postgres", "pg_ctl",
		"-D", standbyData, "-m", "fast", "-w", "-t", "30", "stop")
	primary := replicationConnect(ctx, t, r.PrimaryDSN)
	defer closeConn(ctx, primary)
	waitForSQL(ctx, t, primary, "standby disconnect", "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_replication)")
}

// StartStandby starts the existing data directory without taking another
// backup. It can also start the old standby after the primary was fenced.
func (r *Replication) StartStandby(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	r.startStandby(ctx, t)
}

// WaitForReplay waits for the primary's current flush watermark to be replayed
// on the reconnected synchronous standby.
func (r *Replication) WaitForReplay(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	primary := replicationConnect(ctx, t, r.PrimaryDSN)
	defer closeConn(ctx, primary)
	r.waitForStreaming(ctx, t, primary, "sync")
	var lsn string
	if err := primary.QueryRow(ctx, "SELECT pg_catalog.pg_current_wal_flush_lsn()::text").Scan(&lsn); err != nil {
		t.Fatal("capture primary WAL flush position")
	}
	standby := replicationConnect(ctx, t, r.StandbyDSN)
	defer closeConn(ctx, standby)
	waitForSQL(ctx, t, standby, "standby replay through primary flush position",
		"SELECT COALESCE(pg_catalog.pg_last_wal_replay_lsn() >= $1::text::pg_lsn, FALSE)", lsn)
}

// CrashPrimary kills the postmaster without a shutdown checkpoint and leaves
// the old primary unreachable for the remainder of the exercise.
func (r *Replication) CrashPrimary(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	api, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal("connect to Docker to fence old primary")
	}
	defer func() { _ = api.Close() }()
	if _, killErr := api.ContainerKill(ctx, r.primary.GetContainerID(), docker.ContainerKillOptions{Signal: "SIGKILL"}); killErr != nil {
		t.Fatal("kill the old primary")
	}
	state, err := r.primary.State(ctx)
	if err != nil || state.Running || state.ExitCode != 137 {
		t.Fatal("verify abrupt primary exit")
	}
	if conn, err := pgx.Connect(ctx, r.PrimaryDSN); err == nil {
		closeConn(ctx, conn)
		t.Fatal("old primary still accepts connections")
	}
}

// Promote operates on the running physical standby and waits for recovery to
// complete before returning.
func (r *Replication) Promote(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	standby := replicationConnect(ctx, t, r.StandbyDSN)
	defer closeConn(ctx, standby)
	var recovery, promoted bool
	if err := standby.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery); err != nil || !recovery {
		t.Fatal("verify candidate is a physical standby before promotion")
	}
	if err := standby.QueryRow(ctx, "SELECT pg_catalog.pg_promote(true, 30)").Scan(&promoted); err != nil || !promoted {
		t.Fatal("promote the physical standby")
	}
	waitForSQL(ctx, t, standby, "standby promotion", "SELECT NOT pg_catalog.pg_is_in_recovery()")
}
