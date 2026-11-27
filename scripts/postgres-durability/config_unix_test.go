//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestDurabilityPreflightStalledConfiguration(t *testing.T) {
	for _, scenario := range []struct {
		name, keyword, queryKey, envKey string
		waitForDeadline                 bool
	}{
		{name: "keyword passfile deadline", keyword: "sslmode=disable passfile='%s'", waitForDeadline: true},
		{name: "URI passfile interrupt", queryKey: "passfile"},
		{name: "service file interrupt", keyword: "sslmode=disable service=journal servicefile='%s'"},
		{name: "inherited passfile interrupt", envKey: "PGPASSFILE"},
		{name: "TLS trust file interrupt", queryKey: "sslrootcert"},
		{name: "TLS client key interrupt", keyword: "sslmode=require sslrootcert='' sslcert=client.crt sslkey='%s'"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			fifo := filepath.Join(t.TempDir(), "configuration")
			require.NoError(t, unix.Mkfifo(fifo, 0o600))
			listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })
			host, port, err := net.SplitHostPort(listener.Addr().String())
			require.NoError(t, err)
			dsn := fmt.Sprintf("host=%s port=%s user=journal dbname=quarry", host, port)
			var env []string
			switch {
			case scenario.keyword != "":
				dsn += " " + fmt.Sprintf(scenario.keyword, fifo)
			case scenario.envKey != "":
				dsn += " sslmode=disable"
				env = append(env, scenario.envKey+"="+fifo)
			default:
				mode := "disable"
				if scenario.queryKey == "sslrootcert" {
					mode = "verify-full"
				}
				uri := url.URL{Scheme: "postgres", User: url.User("journal"), Host: listener.Addr().String(), Path: "/quarry"}
				uri.RawQuery = url.Values{"sslmode": {mode}, scenario.queryKey: {fifo}}.Encode()
				dsn = uri.String()
			}

			watchdog, cancel := context.WithTimeout(t.Context(), 14*time.Second)
			defer cancel()
			child := startDurabilityProcess(watchdog, t, dsn, env...)
			if !scenario.waitForDeadline {
				// Rendezvous with pgx at the FIFO, then keep it open with
				// no data so its read remains blocked at the interrupt.
				writer := awaitConfigurationReader(t, fifo, child.done)
				defer func() { _ = unix.Close(writer) }()
				require.NoError(t, child.Signal(os.Interrupt))
				select {
				case <-child.done:
				case <-time.After(3 * time.Second):
					t.Fatal("SIGINT did not interrupt the command during DSN loading")
				}
			}
			<-child.done
			require.NoError(t, watchdog.Err(), "the command outlived its deadline")
			var exit *exec.ExitError
			require.ErrorAs(t, child.err, &exit)
			assertOutput(t, exit.ExitCode(), 1, &child.stdout, &child.stderr, report{modeLocal, "unavailable", deadlineOrCancellation})

			tcp := listener.(*net.TCPListener)
			require.NoError(t, tcp.SetDeadline(time.Now().Add(50*time.Millisecond)))
			conn, err := tcp.AcceptTCP()
			if err == nil {
				_ = conn.Close()
				t.Fatal("the command opened a socket while configuration was blocked")
			}
			require.ErrorIs(t, err, os.ErrDeadlineExceeded)
		})
	}
}

func awaitConfigurationReader(t *testing.T, fifo string, finished <-chan struct{}) int {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		fd, err := unix.Open(fifo, unix.O_WRONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err == nil {
			return fd
		}
		if !errors.Is(err, unix.ENXIO) {
			t.Fatalf("could not rendezvous with the configuration reader: %v", err)
		}
		select {
		case <-finished:
			t.Fatal("the command exited before reading the configuration file")
		case <-timer.C:
			t.Fatal("the command did not read the configuration file")
		case <-ticker.C:
		}
	}
}
