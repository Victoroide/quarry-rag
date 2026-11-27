package postgres

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func activationTLSDatabase(t *testing.T) (string, *tls.Config) {
	t.Helper()
	if testing.Short() {
		t.Skip("requires an isolated PostgreSQL server with TLS")
	}
	const serverName = "postgres-activation.test"
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		DNSNames:     []string{serverName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, cert, cert, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(certPEM))

	// The image sources this script as the PostgreSQL user. The final server
	// loads the fixture credentials after the initialization server stops.
	const installTLS = `umask 077
cat /tmp/activation.crt > "$PGDATA/server.crt"
cat /tmp/activation.key > "$PGDATA/server.key"
docker_process_sql -c 'ALTER SYSTEM SET ssl = on'
`
	dir := t.TempDir()
	certPath := filepath.Join(dir, "activation.crt")
	keyPath := filepath.Join(dir, "activation.key")
	scriptPath := filepath.Join(dir, "activation-tls.sh")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))
	require.NoError(t, os.WriteFile(scriptPath, []byte(installTLS), 0o600))
	dsn := pgtest.StartIsolated(t, testcontainers.WithFiles(
		testcontainers.ContainerFile{HostFilePath: certPath, ContainerFilePath: "/tmp/activation.crt", FileMode: 0o644},
		testcontainers.ContainerFile{HostFilePath: keyPath, ContainerFilePath: "/tmp/activation.key", FileMode: 0o644},
		testcontainers.ContainerFile{HostFilePath: scriptPath, ContainerFilePath: "/docker-entrypoint-initdb.d/activation-tls.sh", FileMode: 0o644},
	))
	return dsn, &tls.Config{RootCAs: roots, ServerName: serverName, MinVersion: tls.VersionTLS12}
}

func TestGenerationActivationRollbackDiscardsTLSTransport(t *testing.T) {
	dsn, secure := activationTLSDatabase(t)
	owner := activationDatabase(t, dsn)
	wrongActivationFreeze(t, owner)
	socket, trace := activationFaultFixture(t, "rollback", activationBlockRead)
	client := activationFaultClient(t, dsn, trace, secure)
	ctx := context.WithValue(t.Context(), activationSocketKey{}, socket)
	fence := openActivationFence(ctx, t, client, activationTarget)
	transport, ok := fence.conn.PgConn().Conn().(*tls.Conn)
	require.True(t, ok)
	require.Same(t, socket, transport.NetConn())
	require.NotEmpty(t, transport.ConnectionState().VerifiedChains)
	pid := fence.conn.PgConn().PID()

	result := startFaultedActivation(ctx, t, fence, socket)
	started := awaitActivationSocket(t, socket)
	observerCtx, cancel := context.WithDeadline(t.Context(), started.Add(2*time.Second))
	defer cancel()
	require.Eventually(t, func() bool {
		var idle bool
		err := owner.Pool.QueryRow(observerCtx, `SELECT state='idle' AND xact_start IS NULL
 FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&idle)
		return err == nil && idle
	}, time.Until(started.Add(2*time.Second)), 20*time.Millisecond)
	require.Equal(t, 1, activationAdvisoryLocks(observerCtx, t, owner, pid))
	require.EqualError(t, activationResult(t, result, started.Add(5*time.Second)), wrongFreezeError)
	assertActivationRollbackContext(ctx, t, trace)
	require.Error(t, trace.query(t, "rollback").err)
	assertActivationSocketClosed(t, socket, fence)
	require.Zero(t, socket.laterWrites.Load(), "TLS disposal wrote to the underlying transport")
	fence.Close(ctx)
	fence.Close(ctx)
	require.Equal(t, []string{"begin", "rollback"}, trace.controls())
	awaitActivationBackendExit(t, owner, pid, started.Add(6*time.Second))
}
