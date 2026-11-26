package integration

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type deleteRoundTripFunc func(*http.Request) (*http.Response, error)

func (f deleteRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type deleteReplyBody struct {
	io.Reader
	closeErr error
	closed   bool
}

func (b *deleteReplyBody) Close() error {
	b.closed = true
	return b.closeErr
}

type deleteReadError struct{ err error }

func (r deleteReadError) Read([]byte) (int, error) { return 0, r.err }

func TestHostedDeleteRequestReturnsTransportError(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	t.Run("request construction", func(t *testing.T) {
		_, err := readHTTPReply(t.Context(), server, "invalid method", "/documents/doc", "", nil, "")
		require.ErrorContains(t, err, "construct HTTP request")
	})

	t.Run("transport", func(t *testing.T) {
		transportErr := errors.New("synthetic transport failure")
		server.Client().Transport = deleteRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, transportErr
		})
		_, err := readHTTPReply(t.Context(), server, http.MethodDelete, "/documents/doc", "", nil, "")
		require.ErrorIs(t, err, transportErr)
		require.ErrorContains(t, err, "send HTTP request")
	})

	readErr := errors.New("synthetic read failure")
	closeErr := errors.New("synthetic close failure")
	for _, tc := range []struct {
		name     string
		readErr  error
		closeErr error
	}{
		{name: "read", readErr: readErr},
		{name: "close", closeErr: closeErr},
		{name: "read and close", readErr: readErr, closeErr: closeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &deleteReplyBody{Reader: strings.NewReader("response"), closeErr: tc.closeErr}
			if tc.readErr != nil {
				body.Reader = deleteReadError{err: tc.readErr}
			}
			server.Client().Transport = deleteRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
			})
			_, err := readHTTPReply(t.Context(), server, http.MethodDelete, "/documents/doc", "", nil, "")
			require.True(t, body.closed)
			if tc.readErr != nil {
				require.ErrorIs(t, err, tc.readErr)
				require.ErrorContains(t, err, "read HTTP response body")
			}
			if tc.closeErr != nil {
				require.ErrorIs(t, err, tc.closeErr)
				require.ErrorContains(t, err, "close HTTP response body")
			}
		})
	}

	t.Run("success", func(t *testing.T) {
		const requestBody = `{"content":"document"}`
		server.Client().Transport = deleteRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			require.Equal(t, http.MethodPost, req.Method)
			require.Equal(t, "/ingest", req.URL.Path)
			require.Equal(t, "token", req.Header.Get("X-Service-Token"))
			require.Equal(t, "application/json", req.Header.Get("Content-Type"))
			require.Equal(t, "custom", req.Header.Get("X-Test-Header"))
			data, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.NoError(t, req.Body.Close())
			require.Equal(t, requestBody, string(data))
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     http.Header{"Retry-After": {"5"}},
				Body:       io.NopCloser(strings.NewReader("retry")),
			}, nil
		})
		reply, err := readHTTPReply(t.Context(), server, http.MethodPost, "/ingest", "token",
			map[string]string{"X-Test-Header": "custom"}, requestBody)
		require.NoError(t, err)
		require.Equal(t, httpReply{code: http.StatusServiceUnavailable, body: "retry", retryAfter: "5"}, reply)
	})
}
