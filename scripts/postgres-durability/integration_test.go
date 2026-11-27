package main

import (
	"bytes"
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestDurabilityPreflightSamplesEffectiveSession(t *testing.T) {
	writerPassword, monitorPassword := uuid.NewString(), uuid.NewString()
	cluster := pgtest.StartReplication(t, func(ctx context.Context, dsn string) {
		client, err := postgres.NewClient(ctx, dsn)
		require.NoError(t, err)
		defer client.Pool.Close()
		require.NoError(t, client.RunMigrations(ctx))
		for _, stmt := range []string{
			"ALTER DATABASE quarry SET synchronous_commit=off",
			"CREATE ROLE journal_writer LOGIN PASSWORD '" + writerPassword + "' NOSUPERUSER",
			"ALTER ROLE journal_writer IN DATABASE quarry SET synchronous_commit=off",
			"CREATE ROLE journal_monitor LOGIN PASSWORD '" + monitorPassword + "' NOSUPERUSER",
			"GRANT pg_read_all_stats TO journal_monitor",
		} {
			_, err := client.Pool.Exec(ctx, stmt)
			require.NoError(t, err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	writer := readerDSN(t, cluster.PrimaryDSN, "journal_writer", writerPassword)
	monitor := readerDSN(t, cluster.PrimaryDSN, "journal_monitor", monitorPassword)
	standby := readerDSN(t, cluster.StandbyDSN, "journal_monitor", monitorPassword)

	assertCheck := func(dsn string, mode preflightMode, want reason) {
		t.Helper()
		var out, stderr bytes.Buffer
		args := []string{"-mode", string(mode)}
		if mode == modeHATopology {
			args = append(args, "-standby", pgtest.ReplicationStandbyName)
		}
		code := run(ctx, args, testEnv(dsn), &out, &stderr)
		status, expected := want.status()
		assertOutput(t, code, expected, &out, &stderr, report{mode, status, want})
	}
	assertCheck(writer, modeLocal, unsafeCommitMode)
	// The monitoring login's own asynchronous default does not attest writer sessions.
	assertCheck(monitor, modeHATopology, standbySnapshotValid)
	assertCheck(writer, modeHATopology, inspectionUnavailable)
	assertCheck(standby, modeHATopology, notPrimary)

	u, err := url.Parse(writer)
	require.NoError(t, err)
	q := u.Query()
	q.Set("synchronous_commit", "on")
	u.RawQuery = q.Encode()
	assertCheck(u.String(), modeLocal, primarySettingsValid)
	assertCheck(u.String(), modeHASession, nonzeroStatementTimeout)
	q.Set("statement_timeout", "0")
	u.RawQuery = q.Encode()
	assertCheck(u.String(), modeHASession, haSessionSettingsValid)
	keywordSettings := " dbname=quarry user=journal_writer password=" + writerPassword +
		" sslmode=disable synchronous_commit=on statement_timeout=0 default_transaction_read_only=on"
	assertCheck("host="+u.Hostname()+" port="+u.Port()+keywordSettings, modeHASession, haSessionSettingsValid)

	client, err := postgres.NewClient(ctx, u.String())
	require.NoError(t, err)
	defer client.Pool.Close()
	var lastPID uint32
	for range 2 {
		conn, err := client.Pool.Acquire(ctx)
		require.NoError(t, err)
		func() {
			defer conn.Release()
			before := sessionFingerprint(ctx, t, conn.Conn())
			require.NotEqual(t, lastPID, conn.Conn().PgConn().PID())
			lastPID = conn.Conn().PgConn().PID()
			sampled, err := sampleConnection(ctx, conn.Conn(), modeHASession, "")
			require.NoError(t, err)
			require.Equal(t, haSessionSettingsValid, sampled)
			require.Equal(t, before, sessionFingerprint(ctx, t, conn.Conn()))
		}()
		client.Pool.Reset()
	}

	cluster.StopStandby(t)
	assertCheck(monitor, modeHATopology, standbyNotReady)
}

func sessionFingerprint(ctx context.Context, t *testing.T, conn *pgx.Conn) [4]string {
	t.Helper()
	var values [4]string
	err := conn.QueryRow(ctx, `SELECT current_user, current_setting('synchronous_commit'),
		current_setting('statement_timeout'), current_setting('transaction_read_only')`).
		Scan(&values[0], &values[1], &values[2], &values[3])
	require.NoError(t, err)
	require.Equal(t, [4]string{"journal_writer", "on", "0", "on"}, values)
	return values
}

func readerDSN(t *testing.T, dsn, user, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.User = url.UserPassword(user, password)
	query := u.Query()
	query.Set("default_transaction_read_only", "on")
	u.RawQuery = query.Encode()
	return u.String()
}
