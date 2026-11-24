package pinecone

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// ErrMetadataTooLarge marks metadata that exceeds the per-record limit.
var ErrMetadataTooLarge = errors.New("pinecone: record metadata exceeds 40960 bytes")

type indexConn interface {
	UpsertVectors(context.Context, []*pinecone.Vector) (uint32, error)
	QueryByVectorValues(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error)
	DeleteVectorsByFilter(context.Context, *structpb.Struct) error
	DeleteVectorsById(context.Context, []string) error
	DeleteAllVectorsInNamespace(context.Context) error
	Close() error
}

var _ indexConn = (*pinecone.IndexConnection)(nil)

type queryAttemptKey struct{}

func singleTransportAttempt(ctx context.Context) context.Context {
	return context.WithValue(ctx, queryAttemptKey{}, new(atomic.Bool))
}

// gRPC can transparently retry a stream even with configured retries disabled.
// Its transport asks for per-RPC metadata before sending each stream. Refuse
// a second stream for the same budgeted attempt and let the caller count retries.
type queryAttemptCredentials struct{}

func (queryAttemptCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	if sent, ok := ctx.Value(queryAttemptKey{}).(*atomic.Bool); ok && sent.Swap(true) {
		return nil, status.Error(codes.Unavailable, "pinecone: retry requires another query attempt")
	}
	return nil, nil
}

func (queryAttemptCredentials) RequireTransportSecurity() bool { return true }

type mutationDeliveryKey struct{}

type mutationDeliveryObserver struct{}

func (mutationDeliveryObserver) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (mutationDeliveryObserver) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (mutationDeliveryObserver) HandleConn(context.Context, stats.ConnStats) {}

func (mutationDeliveryObserver) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if _, ok := event.(*stats.OutHeader); ok {
		if opened, ok := ctx.Value(mutationDeliveryKey{}).(*atomic.Bool); ok {
			opened.Store(true)
		}
	}
}

func classifyMutationDelivery(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoke(ctx, method, req, reply, conn, opts...)
	// All transparent attempts share the observation. After any outbound
	// headers, a failed RPC may have reached the provider.
	if opened, ok := ctx.Value(mutationDeliveryKey{}).(*atomic.Bool); ok && status.Code(err) == codes.Unavailable && !opened.Load() {
		return domain.RejectedVectorMutation(err)
	}
	return err
}

func (p *Client) mutate(ctx context.Context, deletes bool, op func(context.Context) error) error {
	if !domain.IsTrackedVectorMutation(ctx) {
		return p.do(ctx, p.maxAttempts, op)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return domain.DispatchVectorMutation(rpcCtx, deletes, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return domain.RejectedVectorMutation(err)
		}
		err := op(context.WithValue(ctx, mutationDeliveryKey{}, new(atomic.Bool)))
		switch status.Code(err) {
		case codes.InvalidArgument, codes.Unimplemented, codes.FailedPrecondition, codes.Unauthenticated, codes.PermissionDenied:
			return domain.RejectedVectorMutation(err)
		default:
			return err
		}
	})
}

func (p *Client) do(ctx context.Context, attempts int, op func(context.Context) error) error {
	if attempts < 1 {
		return errors.New("pinecone: attempts must be positive")
	}
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, p.timeout)
		err := op(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable(err) || attempt == attempts-1 {
			return err
		}
		// Retry timing does not require cryptographic randomness.
		timer := time.NewTimer(time.Duration(rand.Int64N(int64(backoff) + 1))) // #nosec G404
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(5*time.Second, 2*backoff)
	}
	return nil
}

func retryable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func validateIndex(idx *pinecone.Index, cfg *config.Config) error {
	if idx == nil {
		return errors.New("pinecone: missing index description")
	}
	if idx.Dimension == nil {
		return errors.New("pinecone: missing index dimension")
	}
	if int64(*idx.Dimension) != int64(cfg.EmbeddingDimensions) {
		return fmt.Errorf("pinecone: index dimension %d does not match EMBEDDING_DIMENSIONS %d", *idx.Dimension, cfg.EmbeddingDimensions)
	}
	if idx.Metric != pinecone.Cosine {
		return fmt.Errorf("pinecone: index metric must be cosine, got %q", idx.Metric)
	}
	if idx.Status == nil || !idx.Status.Ready {
		return errors.New("pinecone: index is not ready")
	}
	return nil
}
