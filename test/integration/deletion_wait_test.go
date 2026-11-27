package integration

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSmallPoolReplyRejectsUnauthorizedTraffic(t *testing.T) {
	server := deletionServer(t, nil, "token", deletionRoutes{})
	for _, route := range []struct {
		method     string
		path       string
		wantStatus int
	}{
		{http.MethodPost, "/ingest", http.StatusAccepted},
		{http.MethodGet, "/documents?limit=100", http.StatusOK},
		{http.MethodGet, "/documents/" + uuid.NewString(), http.StatusNotFound},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			reply, err := readHTTPReply(ctx, server, route.method, route.path, "wrong-token", nil, "")
			require.NoError(t, err)
			require.Equal(t, http.StatusUnauthorized, reply.code, reply.body)
			require.ErrorContains(t, checkSmallPoolReply(reply, route.wantStatus), "HTTP 401")
		})
	}
}

func testAbandonedQualifiedDeletionBatch(t *testing.T) {
	t.Helper()
	checkAbandonedDeletionBatch(t, duplicateDeletionRequests)
}

func TestQualifiedDeletionShutdownWaits(t *testing.T) {
	for _, name := range []string{"disconnect_after_entry", "cancel_before_release"} {
		t.Run(name, func(t *testing.T) {
			watchdog, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			fixture, cancelFixture := context.WithCancelCause(watchdog)
			defer cancelFixture(nil)
			stores := newDeletionStores()
			entered, release := blockFirstCalls(fixture, stores, "source", 1)
			defer release()
			requests := make(chan context.Context, 1)
			providerResults := make(chan error, 1)
			server, address, served := serveDeletionFixture(watchdog, t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Context()
				provider, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
				defer cancel()
				providerResults <- stores.Delete(provider, "original-source")
				w.WriteHeader(http.StatusOK)
			}))
			clientCtx, disconnect := context.WithCancel(watchdog)
			defer disconnect()
			deleted := startDisconnectingDelete(clientCtx, address, "token", "doc", "incarnation")
			require.NoError(t, waitDeletionArrivals(watchdog, entered, deleted, 1, "before disconnecting at the source gate"))
			disconnect()
			_, err := waitDeletionReply(watchdog, deleted, "after disconnecting the DELETE client")
			require.ErrorIs(t, err, context.Canceled)
			requestCtx, err := waitDeletionResult(watchdog, requests, "receiving the handler request context")
			require.NoError(t, err)
			select {
			case <-requestCtx.Done():
			case <-watchdog.Done():
				t.Fatal("the server did not observe the client disconnect")
			}

			shutdown := startDeletionDrain(watchdog, t, server, served)
			cause := errors.New("fixture stopped before release")
			if name == "cancel_before_release" {
				cancelFixture(cause)
			} else {
				release()
			}
			require.NoError(t, waitDeletionServer(watchdog, shutdown, "waiting for Shutdown after the source call"))
			providerErr, err := waitDeletionResult(watchdog, providerResults, "waiting for the detached provider")
			require.NoError(t, err)
			if name == "cancel_before_release" {
				require.ErrorIs(t, providerErr, cause)
			} else {
				require.NoError(t, providerErr)
			}
			require.Equal(t, 1, stores.count("source"))
		})
	}
}

func checkSmallPoolReply(reply httpReply, wantStatus int) error {
	if reply.code != wantStatus {
		return fmt.Errorf("got HTTP %d, want HTTP %d: %s", reply.code, wantStatus, reply.body)
	}
	return nil
}
