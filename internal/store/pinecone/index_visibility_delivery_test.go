package pinecone

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// failFirstTransport fails the first request before it reaches the network.
type failFirstTransport struct {
	next  http.RoundTripper
	calls atomic.Int32
}

func (f *failFirstTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if f.calls.Add(1) == 1 {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	return f.next.RoundTrip(req)
}

func TestTrackedHTTPWriteDelivery(t *testing.T) {
	for _, tt := range []struct {
		name        string
		firstStatus int
		code        codes.Code
		uncertain   bool
	}{
		{name: "refused before connecting", code: codes.Unavailable},
		{name: "throttled", firstStatus: http.StatusTooManyRequests, code: codes.ResourceExhausted},
		{name: "gateway timeout", firstStatus: http.StatusGatewayTimeout, code: codes.Unavailable, uncertain: true},
		{name: "unauthorized", firstStatus: http.StatusUnauthorized, code: codes.Unauthenticated},
		{name: "partial acknowledgment", firstStatus: http.StatusOK, code: codes.Unknown, uncertain: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, received := visibilityClientWithFirstWriteFailure(t, tt.firstStatus)
			ctx := domain.TrackVectorMutation(t.Context())
			chunks := testChunks(1, 2, 10)

			probe, firstErr := client.UpsertWithVisibility(ctx, "", chunks)
			require.Error(t, firstErr)
			require.Nil(t, probe)
			firstRequests := int32(1)
			if tt.firstStatus == 0 {
				firstRequests = 0
			}
			require.Equal(t, firstRequests, received.Load())

			probe, err := client.UpsertWithVisibility(ctx, "", chunks)
			require.NoError(t, err)
			require.NotNil(t, probe)
			require.Equal(t, firstRequests+1, received.Load())
			require.Equal(t, tt.code, status.Code(firstErr))
			require.Equal(t, tt.uncertain, domain.VectorMutationUncertain(firstErr), "first write: %v", firstErr)
		})
	}
}

func visibilityClientWithFirstWriteFailure(t *testing.T, firstStatus int) (*Client, *atomic.Int32) {
	t.Helper()
	var received atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/vectors/upsert" {
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if received.Add(1) == 1 && firstStatus != 0 {
			if firstStatus == http.StatusOK {
				_, _ = io.WriteString(w, `{"upsertedCount":0}`)
			} else {
				w.WriteHeader(firstStatus)
			}
			return
		}
		w.Header().Set("x-pinecone-request-lsn", "12")
		_, _ = io.WriteString(w, `{"upsertedCount":1}`)
	}))
	t.Cleanup(server.Close)
	client := visibilityClient(t, server)
	if firstStatus == 0 {
		client.visibility.http.Transport = &failFirstTransport{next: client.visibility.http.Transport}
	}
	return client, &received
}

// A write that may already have reached the index can still be applied after
// its retry, so the retry's receipt does not bound it.
func TestWriteVisibilityRetryAfterPossibleDeliveryCannotAttest(t *testing.T) {
	for _, tt := range []struct {
		name              string
		timeout           time.Duration
		transport         func(http.RoundTripper) http.RoundTripper
		first             func(http.ResponseWriter, *http.Request)
		forwardedOriginal bool
		attests           bool
	}{
		{name: "gateway timeout", forwardedOriginal: true, first: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusGatewayTimeout)
		}},
		{name: "bad gateway", first: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }},
		{name: "service unavailable", first: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }},
		{name: "internal error", first: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }},
		{name: "lost response", first: func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}},
		{name: "attempt deadline", timeout: 100 * time.Millisecond, first: func(_ http.ResponseWriter, r *http.Request) {
			// The server notices the closed connection only after the body is read.
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-time.After(10 * time.Second):
			}
		}},
		{name: "throttled before processing", attests: true, first: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTooManyRequests) }},
		{
			name: "refused before the request was sent", attests: true,
			transport: func(next http.RoundTripper) http.RoundTripper { return &failFirstTransport{next: next} },
			first:     func(http.ResponseWriter, *http.Request) {},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var writes, originalLSN atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/vectors/upsert":
					// The first attempt that reaches the server fails; the retry succeeds with 15.
					if writes.Add(1) == 1 && tt.transport == nil {
						tt.first(w, r)
						return
					}
					w.Header().Set("x-pinecone-request-lsn", "15")
					if tt.forwardedOriginal {
						// The origin applies the forwarded write after the retry.
						// The gateway has already returned 504 to the client.
						originalLSN.Store(16)
					}
					_, _ = io.WriteString(w, `{"upsertedCount":1}`)
				case "/query":
					w.Header().Set("x-pinecone-max-indexed-lsn", "15")
					_, _ = io.WriteString(w, `{"namespace":"archive","matches":[]}`)
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			client := visibilityClient(t, server)
			client.maxAttempts = 2
			if tt.timeout > 0 {
				client.timeout = tt.timeout
			}
			if tt.transport != nil {
				client.visibility.http.Transport = tt.transport(client.visibility.http.Transport)
			}
			probe, err := client.UpsertWithVisibility(t.Context(), "archive", testChunks(1, 2, 10))
			require.NoError(t, err, "the ingestion retry still completes the write")
			if tt.forwardedOriginal {
				require.EqualValues(t, 2, writes.Load())
				require.EqualValues(t, 16, originalLSN.Load(), "watermark 15 omits the forwarded write")
			}
			if !tt.attests {
				require.Nil(t, probe, "the delivered original has no receipt")
				return
			}
			require.NotNil(t, probe)
			got, probeErr := probe(t.Context(), []float32{1, 0})
			require.NoError(t, probeErr)
			require.Equal(t, domain.VisibilityWritesIndexed, got)
		})
	}
}

// One uncertain batch removes the evidence for the attempt, even when a later
// batch is clean and the watermark passes every receipt.
func TestWriteVisibilityUncertainBatchInvalidatesAttempt(t *testing.T) {
	var writes atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch writes.Add(1) {
		case 1:
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		case 2:
			w.Header().Set("x-pinecone-request-lsn", "12")
		default:
			w.Header().Set("x-pinecone-request-lsn", "15")
		}
		_, _ = io.WriteString(w, `{"upsertedCount":1}`)
	}))
	t.Cleanup(server.Close)
	client := visibilityClient(t, server)
	client.maxAttempts = 2
	probe, err := client.UpsertWithVisibility(t.Context(), "archive", testChunks(2, 2, 10))
	require.NoError(t, err)
	require.EqualValues(t, 3, writes.Load())
	require.Nil(t, probe)
}
