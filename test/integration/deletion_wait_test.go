package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSmallPoolReplyRejectsUnauthorizedTraffic(t *testing.T) {
	server := deletionServer(t, nil, "token", deletionRoutes{})
	id := uuid.NewString()
	for _, route := range []struct {
		name       string
		method     string
		path       string
		payload    string
		wantStatus int
		list       bool
	}{
		{"ingest", http.MethodPost, "/ingest", `{"content":"small pool authorization control"}`, http.StatusAccepted, false},
		{"list", http.MethodGet, "/documents?limit=100", "", http.StatusOK, true},
		{"status", http.MethodGet, "/documents/" + id, "", http.StatusOK, false},
	} {
		t.Run(route.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			reply, err := readHTTPReply(ctx, server, route.method, route.path, "wrong-token", nil, route.payload)
			require.NoError(t, err)
			require.Equal(t, http.StatusUnauthorized, reply.code, reply.body)
			require.ErrorContains(t, checkSmallPoolReply(reply, route.wantStatus, id, route.list), "HTTP 401")
		})
	}
}

func TestSmallPoolReplyRejectsUnsuccessfulRoutes(t *testing.T) {
	id, otherID := uuid.NewString(), uuid.NewString()
	processing := `{"document_id":"` + id + `","status":"processing"}`
	anchor := `{"document_id":"` + id + `"}`
	other := `{"document_id":"` + otherID + `"}`
	list := `{"documents":[` + anchor + `],"count":1}`
	invalidDocument := map[string]string{
		"wrong ID":      `{"document_id":"` + otherID + `","status":"processing"}`,
		"missing ID":    `{"status":"processing"}`,
		"wrong state":   `{"document_id":"` + id + `","status":"done"}`,
		"missing state": anchor,
	}
	for _, route := range []struct {
		name        string
		wantStatus  int
		otherStatus int
		list        bool
		goodBody    string
		invalid     map[string]string
	}{
		{"ingest", http.StatusAccepted, http.StatusOK, false, processing, invalidDocument},
		{"status", http.StatusOK, http.StatusAccepted, false, processing, invalidDocument},
		{"list", http.StatusOK, http.StatusAccepted, true, list, map[string]string{
			"missing anchor":    `{"documents":[` + other + `],"count":1}`,
			"empty array":       `{"documents":[],"count":0}`,
			"missing documents": `{"count":1}`,
			"null documents":    `{"documents":null,"count":0}`,
			"invalid array":     `{"documents":` + anchor + `,"count":1}`,
			"invalid row":       `{"documents":[` + anchor + `,null],"count":2}`,
			"missing count":     `{"documents":[` + anchor + `]}`,
			"null count":        `{"documents":[` + anchor + `],"count":null}`,
			"wrong count":       `{"documents":[` + anchor + `],"count":2}`,
		}},
	} {
		t.Run(route.name, func(t *testing.T) {
			check := func(code int, body string) error {
				return checkSmallPoolReply(httpReply{code: code, body: body}, route.wantStatus, id, route.list)
			}
			require.NoError(t, check(route.wantStatus, route.goodBody))
			for _, code := range []int{
				route.otherStatus,
				http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
				http.StatusConflict, http.StatusInternalServerError, http.StatusServiceUnavailable,
			} {
				t.Run(fmt.Sprint(code), func(t *testing.T) {
					require.ErrorContains(t, check(code, route.goodBody), fmt.Sprintf("got HTTP %d", code))
				})
			}
			for _, body := range []string{"", `{"`, `null`, route.goodBody + `{}`} {
				require.Error(t, check(route.wantStatus, body), "body %q", body)
			}
			for name, body := range route.invalid {
				t.Run(name, func(t *testing.T) {
					require.Error(t, check(route.wantStatus, body))
				})
			}
		})
	}
	t.Run("list with additional rows in either order", func(t *testing.T) {
		for _, rows := range []string{anchor + `,` + other, other + `,` + anchor} {
			body := `{"documents":[` + rows + `],"count":2}`
			require.NoError(t, checkSmallPoolReply(httpReply{code: http.StatusOK, body: body}, http.StatusOK, id, true))
		}
	})
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

func checkSmallPoolReply(reply httpReply, wantStatus int, expectedID string, list bool) error {
	if reply.code != wantStatus {
		return fmt.Errorf("got HTTP %d, want HTTP %d: %s", reply.code, wantStatus, reply.body)
	}
	if !list {
		var doc handler.IngestResponse
		if err := json.Unmarshal([]byte(reply.body), &doc); err != nil {
			return fmt.Errorf("decode document reply: %w", err)
		}
		if doc.DocumentID != expectedID || doc.Status != "processing" {
			return fmt.Errorf("got document %q with status %q, want %q with status processing", doc.DocumentID, doc.Status, expectedID)
		}
		return nil
	}

	var page struct {
		Documents []struct {
			ID string `json:"document_id"`
		} `json:"documents"`
		Count *int `json:"count"`
	}
	if err := json.Unmarshal([]byte(reply.body), &page); err != nil {
		return fmt.Errorf("decode document list: %w", err)
	}
	if page.Documents == nil || page.Count == nil {
		return errors.New("document list must contain a documents array and count")
	}
	if *page.Count != len(page.Documents) {
		return fmt.Errorf("list count %d does not match %d documents", *page.Count, len(page.Documents))
	}
	found := false
	for _, doc := range page.Documents {
		if doc.ID == "" {
			return errors.New("document list entry is missing its document ID")
		}
		found = found || doc.ID == expectedID
	}
	if !found {
		return fmt.Errorf("document list is missing %q", expectedID)
	}
	return nil
}
