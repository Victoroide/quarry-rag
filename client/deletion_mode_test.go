package client_test

import (
	"net/http"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDeletionModeReadsOnce(t *testing.T) {
	for mode, active := range map[string]bool{"inactive": false, "active": true, "future": false, "": false} {
		var requests int
		c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			requests++
			assert.Equal(t, http.MethodGet+" /api/deletion-mode", r.Method+" "+r.URL.Path)
			assert.Empty(t, r.URL.RawQuery)
			assert.Equal(t, "synthetic-token", r.Header.Get("X-Service-Token"))
			assert.Empty(t, r.Header.Values("X-Document-Incarnation"))
			writeJSON(t, w, client.DeletionModeResponse{DeletionMode: mode})
		})
		response, err := c.GetDeletionMode(t.Context())
		require.NoError(t, err)
		require.Equal(t, mode, response.DeletionMode)
		require.Equal(t, active, response.Active(), "only the exact value active is active")
		require.Equal(t, 1, requests)
	}
}

func TestGetDeletionModeReportsServiceErrors(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusServiceUnavailable} {
		c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"unavailable","message":"document deletion mode is unavailable"}`))
		})
		response, err := c.GetDeletionMode(t.Context())
		require.Nil(t, response)
		var apiErr *client.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, status, apiErr.StatusCode)
	}
}
