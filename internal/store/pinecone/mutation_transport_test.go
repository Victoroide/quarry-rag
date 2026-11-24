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

func newGRPCWriteFixture(t *testing.T) *grpcWriteFixture {
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
	client, err := newPineconeClient(t.Context(), &config.Config{
		PineconeIndexName: "fixture", IndexVisibilityMode: "sample", PineconeTimeout: 5 * time.Second,
		PineconeMaxAttempts: 2, PineconeUpsertBatchSize: 10, EmbeddingDimensions: 2,
	}, pc, grpc.WithConnectParams(grpc.ConnectParams{
		Backoff:           backoff.Config{BaseDelay: 5 * time.Millisecond, Multiplier: 1, MaxDelay: 10 * time.Millisecond},
		MinConnectTimeout: time.Second,
	}), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		if !f.allowDial.Load() {
			f.dialRefusals.Add(1)
			return nil, errors.New("connection refused before any stream")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}))
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
