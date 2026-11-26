package pinecone

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pc "github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// roundTripFunc serves requests in memory.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	// Like http.Transport, refuse a request whose context has already ended.
	if err := r.Context().Err(); err != nil {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return nil, err
	}
	return f(r)
}

func jsonResponse(req *http.Request, header http.Header, body string) *http.Response {
	if header == nil {
		header = make(http.Header)
	} else {
		header = header.Clone()
	}
	header.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

// failFirstTransport fails the first request before it reaches the network.
type failFirstTransport struct {
	next  http.RoundTripper
	calls atomic.Int32
}

func (f *failFirstTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, err
	}
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

// A write that reached a connection and outlived its attempt deadline may
// still be applied, so the retry's receipt cannot bound it. A deadline that
// expired before any connection was a definite rejection, so it can.
func TestWriteVisibilityAttemptDeadline(t *testing.T) {
	for _, tt := range []struct {
		name      string
		connected bool
		attests   bool
	}{
		{name: "after connecting", connected: true},
		{name: "before connecting", attests: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var attempts []time.Time
				var budgets []time.Duration
				var bodies [][]byte
				var firstCause error
				plane, err := newVisibilityPlane(&pc.Index{Host: "index.test", Spec: &pc.IndexSpec{Serverless: &pc.ServerlessSpec{}}}, "test-key")
				require.NoError(t, err)
				plane.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
					body, readErr := io.ReadAll(req.Body)
					_ = req.Body.Close()
					if readErr != nil {
						return nil, readErr
					}
					switch req.URL.Path {
					case "/vectors/upsert":
						attempts = append(attempts, time.Now())
						deadline, ok := req.Context().Deadline()
						if !ok {
							return nil, fmt.Errorf("attempt %d has no deadline", len(attempts))
						}
						budgets = append(budgets, time.Until(deadline))
						bodies = append(bodies, body)
						if len(attempts) == 1 {
							if trace := httptrace.ContextClientTrace(req.Context()); tt.connected && trace != nil && trace.GotConn != nil {
								trace.GotConn(httptrace.GotConnInfo{})
							}
							<-req.Context().Done()
							firstCause = context.Cause(req.Context())
							return nil, req.Context().Err()
						}
						return jsonResponse(req, http.Header{"X-Pinecone-Request-Lsn": {"15"}}, `{"upsertedCount":1}`), nil
					case "/query":
						return jsonResponse(req, http.Header{"X-Pinecone-Max-Indexed-Lsn": {"15"}}, `{"namespace":"archive","matches":[]}`), nil
					}
					return nil, fmt.Errorf("unexpected path %s", req.URL.Path)
				})
				const attemptDeadline = 100 * time.Millisecond
				client := &Client{visibility: plane, batchSize: 1, timeout: attemptDeadline, maxAttempts: 2}
				probe, err := client.UpsertWithVisibility(t.Context(), "archive", testChunks(1, 2, 10))
				require.NoError(t, err, "the ingestion retry still completes the write")
				require.Len(t, attempts, 2)
				require.Equal(t, []time.Duration{attemptDeadline, attemptDeadline}, budgets, "every attempt gets the full deadline")
				require.ErrorIs(t, firstCause, context.DeadlineExceeded)
				require.GreaterOrEqual(t, attempts[1].Sub(attempts[0]), attemptDeadline, "the retry follows the first attempt's deadline")
				require.Equal(t, bodies[0], bodies[1], "the retry resends the same batch")
				if !tt.attests {
					require.Nil(t, probe, "the delivered original has no receipt")
					return
				}
				require.NotNil(t, probe)
				got, probeErr := probe(t.Context(), []float32{1, 0})
				require.NoError(t, probeErr)
				require.Equal(t, domain.VisibilityWritesIndexed, got)
			})
		})
	}
}

// Like http.Transport, the in-memory transports must refuse requests whose
// context has already ended instead of answering them.
func TestInMemoryTransportRefusesEndedContext(t *testing.T) {
	for _, tc := range []struct {
		name      string
		makeCtx   func(context.Context) (context.Context, context.CancelFunc)
		wantCause error
	}{
		{
			name: "canceled",
			makeCtx: func(parent context.Context) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(parent)
				cancel()
				return ctx, cancel
			},
			wantCause: context.Canceled,
		},
		{
			name: "deadline exceeded",
			makeCtx: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithDeadline(parent, time.Now().Add(-time.Millisecond))
			},
			wantCause: context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := 0
				rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
					called++
					return jsonResponse(req, http.Header{"X-Pinecone-Request-Lsn": {"15"}}, `{"upsertedCount":1}`), nil
				})
				plane, err := newVisibilityPlane(&pc.Index{Host: "index.test", Spec: &pc.IndexSpec{Serverless: &pc.ServerlessSpec{}}}, "test-key")
				require.NoError(t, err)
				plane.http.Transport = rt

				endedCtx, cancel := tc.makeCtx(t.Context())
				defer cancel()

				for _, path := range []string{"/vectors/upsert", "/query"} {
					_, postErr := plane.post(endedCtx, path, []byte(`{}`), &struct{}{})
					require.ErrorIs(t, postErr, tc.wantCause)
				}
				require.Zero(t, called, "roundTripFunc must not answer an ended request context")

				ff := &failFirstTransport{next: rt}
				req, reqErr := http.NewRequestWithContext(endedCtx, http.MethodPost, "https://index.test/vectors/upsert", http.NoBody)
				require.NoError(t, reqErr)
				resp, ffErr := ff.RoundTrip(req)
				if resp != nil {
					_ = resp.Body.Close()
				}
				require.ErrorIs(t, ffErr, tc.wantCause)
				require.Zero(t, ff.calls.Load(), "failFirstTransport must not dial an ended request context")
			})
		})
	}
}
