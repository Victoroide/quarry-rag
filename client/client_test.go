package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newClient(t *testing.T, h http.HandlerFunc) *client.Client {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	c, err := client.New(s.URL+"/api/", client.WithServiceToken("synthetic-token"))
	require.NoError(t, err)
	return c
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	assert.NoError(t, json.NewEncoder(w).Encode(value))
}

func TestNewValidatesURLAndOptions(t *testing.T) {
	for _, raw := range []string{"", "/relative", "localhost:8081", (&url.URL{Scheme: "ftp", Host: "localhost"}).String(), (&url.URL{Scheme: "http"}).String(), (&url.URL{Scheme: "http", Host: "localhost", User: url.UserPassword("user", "pass")}).String()} {
		_, err := client.New(raw)
		require.Error(t, err)
	}
	base := (&url.URL{Scheme: "http", Host: "localhost:8081", Path: "/prefix"}).String()
	_, err := client.New(base, client.WithHTTPClient(nil))
	require.Error(t, err)
	_, err = client.New(base, client.WithTimeout(-time.Second))
	require.Error(t, err)
	_, err = client.New(base)
	require.NoError(t, err)
}

func TestAPIError(t *testing.T) {
	for _, status := range []int{409, 413, 415, 422, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var calls atomic.Int32
			c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				assert.Equal(t, "synthetic-token", r.Header.Get("X-Service-Token"))
				assert.Empty(t, r.Header.Get("Authorization"))
				assert.NotContains(t, r.URL.String(), "synthetic-token")
				w.Header().Set("X-Request-ID", "request-123")
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(status)
				writeJSON(t, w, map[string]string{"error": "extraction", "message": "synthetic failure", "code": "no_text"})
			})
			_, err := c.Query(t.Context(), client.QueryRequest{Query: "synthetic question"})
			var apiErr *client.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, status, apiErr.StatusCode)
			assert.Equal(t, "extraction", apiErr.Code)
			assert.Equal(t, "synthetic failure", apiErr.Message)
			assert.Equal(t, "no_text", apiErr.ExtractionCode)
			assert.Equal(t, "request-123", apiErr.RequestID)
			assert.Equal(t, 5*time.Second, apiErr.RetryAfter)
			assert.EqualValues(t, 1, calls.Load())
			assert.Contains(t, apiErr.Error(), "synthetic failure")
		})
	}
}

func TestRetryAfterHTTPDateAndNonJSONError(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", time.Now().Add(time.Minute).UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("private upstream failure"))
	})
	_, err := c.GetDocument(t.Context(), "00000000-0000-4000-8000-000000000001")
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.InDelta(t, time.Minute.Seconds(), apiErr.RetryAfter.Seconds(), 2)
	assert.NotContains(t, err.Error(), "private upstream failure")
}

func TestTimeoutAndCancellation(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer s.Close()
	httpClient := s.Client()
	c, err := client.New(s.URL, client.WithHTTPClient(httpClient), client.WithTimeout(30*time.Millisecond))
	require.NoError(t, err)
	_, err = c.GetDocument(t.Context(), "00000000-0000-4000-8000-000000000001")
	require.Error(t, err)
	assert.Zero(t, httpClient.Timeout, "options must not mutate a shared HTTP client")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.GetDocument(ctx, "00000000-0000-4000-8000-000000000001")
	require.True(t, errors.Is(err, context.Canceled))
}

func TestRedirectDoesNotForwardToken(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeJSON(t, w, client.DocumentSummary{})
	}))
	defer target.Close()
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	})
	_, err := c.GetDocument(t.Context(), "00000000-0000-4000-8000-000000000001")
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusTemporaryRedirect, apiErr.StatusCode)
	assert.Zero(t, calls.Load())
}

func TestMalformedSuccessIsAnError(t *testing.T) {
	for _, body := range []string{"", "{", `{} {}`} {
		c := newClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) })
		_, err := c.Query(t.Context(), client.QueryRequest{Query: "synthetic question"})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), strings.Repeat("synthetic question", 2))
	}
}
