package main

import (
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/stretchr/testify/require"
)

func TestRunBoundsListenFailures(t *testing.T) {
	configureListenFailureTest(t)

	t.Run("api listener", func(t *testing.T) {
		listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", ":0")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, listener.Close()) })
		t.Setenv("SERVER_PORT", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))

		require.EqualError(t, run(), "server: listen: operation failed")
	})

	t.Run("metrics listener", func(t *testing.T) {
		cfg := &net.ListenConfig{}
		metrics, err := cfg.Listen(t.Context(), "tcp", ":0")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, metrics.Close()) })
		metricsPort := strconv.Itoa(metrics.Addr().(*net.TCPAddr).Port)
		t.Setenv("METRICS_ADDR", ":"+metricsPort)

		api, err := cfg.Listen(t.Context(), "tcp", ":0")
		require.NoError(t, err)
		apiPort := strconv.Itoa(api.Addr().(*net.TCPAddr).Port)
		t.Setenv("SERVER_PORT", apiPort)
		require.NoError(t, api.Close())

		require.EqualError(t, run(), "metrics: listen: operation failed")
		rebound, err := cfg.Listen(t.Context(), "tcp", ":"+apiPort)
		require.NoError(t, err, "the failed metrics bind must close the API listener")
		require.NoError(t, rebound.Close())
	})
}

func configureListenFailureTest(t *testing.T) {
	t.Helper()
	dsn := pgtest.StartVector(t)
	configureFatalStartupTest(t)
	t.Setenv("DATABASE_URL", dsn)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected provider request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(provider.Close)
	t.Setenv("OPENAI_BASE_URL", provider.URL)

	logger, writer, flags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(logger)
		log.SetOutput(writer)
		log.SetFlags(flags)
	})
}
