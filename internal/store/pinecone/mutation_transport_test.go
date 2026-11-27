package pinecone

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestTrackedGRPCWriteDelivery(t *testing.T) {
	ctx := domain.TrackVectorMutation(t.Context())
	fixture := newGRPCWriteFixture(t)
	upsert := func() error { return fixture.driver.Upsert(ctx, "", testChunks(1, 2, 10)) }

	first := upsert()
	require.Equal(t, codes.Unavailable, status.Code(first))
	require.Positive(t, fixture.dialRefusals.Load())
	require.Zero(t, fixture.received.Load())

	fixture.allowWrites(t)
	require.NoError(t, upsert())
	require.EqualValues(t, 1, fixture.received.Load())

	fixture.failAfterReceipt.Store(true)
	late := upsert()
	require.Equal(t, codes.Unavailable, status.Code(late))
	require.EqualValues(t, 2, fixture.received.Load())
	require.True(t, domain.VectorMutationUncertain(late))
	require.False(t, domain.VectorMutationUncertain(first))
}

type grpcWriteFixture struct {
	driver           *Driver
	allowDial        atomic.Bool
	failAfterReceipt atomic.Bool
	received         atomic.Int32
	dialRefusals     atomic.Int32
}

func (f *grpcWriteFixture) allowWrites(t *testing.T) {
	t.Helper()
	f.failAfterReceipt.Store(false)
	f.allowDial.Store(true)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := f.driver.client.defaultConn.QueryByVectorValues(ctx, &pinecone.QueryByVectorValuesRequest{
			Vector: []float32{1, 0}, TopK: 1,
		})
		return err == nil
	}, 5*time.Second, 15*time.Millisecond)
}

func newGRPCWriteFixture(t *testing.T, options ...grpc.DialOption) *grpcWriteFixture {
	t.Helper()
	f := new(grpcWriteFixture)
	var listen net.ListenConfig
	listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server := grpc.NewServer(grpc.UnknownServiceHandler(f.serve))
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if serveErr := <-served; !errors.Is(serveErr, grpc.ErrServerStopped) {
			require.NoError(t, serveErr)
		}
	})
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		encodeErr := json.NewEncoder(w).Encode(map[string]any{
			"name": "fixture", "dimension": 2, "metric": "cosine", "host": "http://" + listener.Addr().String(),
			"status": map[string]any{"ready": true, "state": "Ready"},
			"spec":   map[string]any{"serverless": map[string]string{"cloud": "aws", "region": "us-east-1"}},
		})
		if encodeErr != nil {
			t.Error(encodeErr)
		}
	}))
	t.Cleanup(control.Close)
	pc, err := pinecone.NewClient(pinecone.NewClientParams{ApiKey: "fixture", Host: control.URL, RestClient: control.Client()})
	require.NoError(t, err)
	options = append(options, grpc.WithConnectParams(grpc.ConnectParams{
		Backoff:           backoff.Config{BaseDelay: 5 * time.Millisecond, Multiplier: 1, MaxDelay: 10 * time.Millisecond},
		MinConnectTimeout: time.Second,
	}), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		if !f.allowDial.Load() {
			f.dialRefusals.Add(1)
			return nil, errors.New("connection refused before any stream")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}))
	client, err := newPineconeClient(t.Context(), &config.Config{
		PineconeIndexName: "fixture", IndexVisibilityMode: "sample", PineconeTimeout: 5 * time.Second,
		PineconeMaxAttempts: 2, PineconeUpsertBatchSize: 10, EmbeddingDimensions: 2,
	}, pc, options...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	f.driver = NewDriver(client)
	return f
}

func (f *grpcWriteFixture) serve(_ any, stream grpc.ServerStream) error {
	// The empty message accepts the request's serialized fields as unknown.
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		return err
	}
	method, _ := grpc.MethodFromServerStream(stream)
	switch method {
	case "/VectorService/Query":
		return stream.SendMsg(&emptypb.Empty{})
	case "/VectorService/Delete":
		f.received.Add(1)
		if f.failAfterReceipt.Load() {
			return status.Error(codes.Unavailable, "temporary error after receiving the delete")
		}
		// Return evidence in the trailers to cover both response locations.
		stream.SetTrailer(metadata.Pairs("x-pinecone-request-id", "fixture-request", "x-pinecone-request-lsn", "42"))
		return stream.SendMsg(&emptypb.Empty{})
	case "/VectorService/Upsert":
		f.received.Add(1)
		if f.failAfterReceipt.Load() {
			return status.Error(codes.Unavailable, "temporary error after receiving the write")
		}
		// UpsertResponse carries upserted_count as uint32 field 1.
		return stream.SendMsg(wrapperspb.UInt32(1))
	default:
		return status.Error(codes.Unimplemented, method)
	}
}

func TestStrictWriterTransportRefusesGRPCResend(t *testing.T) {
	const retry = `{"methodConfig":[{"name":[{"service":"VectorService","method":"Delete"}],
		"retryPolicy":{"maxAttempts":3,"initialBackoff":"0.001s","maxBackoff":"0.005s",
		"backoffMultiplier":1,"retryableStatusCodes":["UNAVAILABLE"]}}]}`
	fixture := newGRPCWriteFixture(t, grpc.WithDefaultServiceConfig(retry))
	fixture.allowWrites(t)
	fixture.failAfterReceipt.Store(true)
	client := fixture.driver.client
	require.Equal(t, codes.Unavailable, status.Code(client.defaultConn.DeleteVectorsByID(t.Context(), []string{"fixture-vector"})))
	require.EqualValues(t, 3, fixture.received.Load(), "the service config attempts resends on the same transport")
	fixture.received.Store(0)

	var evidence []domain.ExternalWriterEvidence
	ctx := domain.WithExternalWriterDispatch(t.Context(), func(ctx context.Context, send domain.ExternalWriterAttempt) error {
		reportCtx := domain.WithExternalWriterEvidence(ctx, func(_ context.Context, event domain.ExternalWriterEvidence) error {
			evidence = append(evidence, event)
			return nil
		})
		settled, err := send(reportCtx)
		require.False(t, settled)
		return err
	})
	send := func(ctx context.Context) error {
		return client.defaultConn.DeleteVectorsByID(ctx, []string{"fixture-vector"})
	}
	require.Equal(t, codes.Unavailable, status.Code(DispatchWriterMutation(ctx, "", send)))
	require.EqualValues(t, 1, fixture.received.Load(), "gRPC cannot resend a strict physical attempt")
	require.NoError(t, DispatchWriterMutation(ctx, "other-namespace", func(ctx context.Context) error {
		fixture.failAfterReceipt.Store(false)
		index, release, err := client.conn("other-namespace")
		require.NoError(t, err)
		defer release()
		return index.DeleteVectorsByID(ctx, []string{"fixture-vector"})
	}))
	require.EqualValues(t, 2, fixture.received.Load())
	require.Equal(t, []domain.ExternalWriterEvidence{
		{Provider: "pinecone", Outcome: "started"},
		{Provider: "pinecone", Outcome: "unconfirmed"},
		{Provider: "pinecone", Namespace: "other-namespace", Outcome: "started"},
		{Provider: "pinecone", Namespace: "other-namespace", Outcome: "acknowledged_pending_indexing",
			RequestID: "fixture-request", RequestSequence: "42"},
	}, evidence)
}
