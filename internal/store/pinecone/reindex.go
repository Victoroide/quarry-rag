package pinecone

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// Read limits for migration tooling.
const (
	// FetchBatchSize bounds the keys of one fetch request.
	FetchBatchSize = 100
	// MaxListPage bounds the keys of one listing request.
	MaxListPage = 1000
)

// ErrListingUnsupported marks an index that cannot enumerate its record keys.
var ErrListingUnsupported = errors.New("pinecone: the index does not support listing record keys")

const invalidReindexChunkIndex = -1

func reindexChunkIndex(md *structpb.Struct) int {
	position, ok := md.GetFields()["chunk_index"].GetKind().(*structpb.Value_NumberValue)
	if !ok || position == nil {
		return invalidReindexChunkIndex
	}
	n := position.NumberValue
	// PostgreSQL stores the canonical position as INTEGER on every platform.
	if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > math.MaxInt32 || math.Trunc(n) != n {
		return invalidReindexChunkIndex
	}
	return int(n)
}

// ParseTaggedVectorID splits a revision- and attempt-qualified vector ID. It
// accepts only the form TaggedVectorID produces with a non-empty attempt.
func ParseTaggedVectorID(id string) (document, revision, attempt, chunk string, ok bool) {
	parts := strings.Split(id, "/")
	if len(parts) != 4 {
		return "", "", "", "", false
	}
	document, revision, attempt, chunk = parts[0], parts[1], parts[2], parts[3]
	if document == "" || revision == "" || attempt == "" || chunk == "" {
		return "", "", "", "", false
	}
	formatted, err := TaggedVectorID(document, revision, attempt, chunk)
	if err != nil || formatted != id {
		return "", "", "", "", false
	}
	return document, revision, attempt, chunk, true
}

// IndexConn is the index connection used by Client.
type IndexConn = indexConn

// NewClientForIndex builds a client over an already opened index connection.
// The connection is taken to filter on every ownership field, as the in-memory
// index does, so document cleanup can use metadata filters.
func NewClientForIndex(index IndexConn, cfg *config.Config, host string) *Client {
	return &Client{
		cfg: cfg, host: host, defaultConn: index, ownershipFilters: ownershipFilters{document: true, revision: true, attempt: true},
		batchSize: cfg.PineconeUpsertBatchSize,
		timeout:   cfg.PineconeTimeout, maxAttempts: cfg.PineconeMaxAttempts, dims: cfg.EmbeddingDimensions,
		dial: func(string) (indexConn, error) { return index, nil },
	}
}

// ListingUnsupported reports whether err means that the index cannot list record keys.
func ListingUnsupported(err error) bool {
	if status.Code(err) == codes.Unimplemented {
		return true
	}
	if status.Code(err) != codes.InvalidArgument && status.Code(err) != codes.FailedPrecondition {
		return false
	}
	message := strings.ToLower(status.Convert(err).Message())
	return strings.Contains(message, "list") && (strings.Contains(message, "not supported") ||
		(strings.Contains(message, "only supported") && strings.Contains(message, "serverless")))
}

// Fetch returns the stored vectors for the supplied keys from the default
// namespace. A key without a record, or whose record carries inconsistent
// ownership tags, is absent from the result. An invalid position is returned
// as a negative index so callers can distinguish it from a missing record.
func (p *Client) Fetch(ctx context.Context, ids []string) (map[string]domain.Chunk, error) {
	out := make(map[string]domain.Chunk, len(ids))
	for start := 0; start < len(ids); start += FetchBatchSize {
		batch := ids[start:min(start+FetchBatchSize, len(ids))]
		var res *pinecone.FetchVectorsResponse
		if err := p.do(ctx, p.maxAttempts, func(attemptCtx context.Context) error {
			var fetchErr error
			res, fetchErr = p.defaultConn.FetchVectors(attemptCtx, batch)
			return fetchErr
		}); err != nil {
			return nil, fmt.Errorf("pinecone: fetch %d vectors: %w", len(batch), err)
		}
		for id, vector := range res.Vectors {
			if vector == nil {
				continue
			}
			if vector.Values == nil {
				return nil, fmt.Errorf("pinecone: fetched record %s has no values", id)
			}
			chunk, consistent := chunkFromMetadataAtIndex(id, vector.Metadata, reindexChunkIndex(vector.Metadata))
			if !consistent {
				continue
			}
			chunk.Embedding = *vector.Values
			out[id] = chunk
		}
	}
	return out, nil
}

// ListIDs returns up to limit record keys from the default namespace and the
// token for the next page, which is empty at the end. It returns
// ErrListingUnsupported for an index that cannot list keys.
func (p *Client) ListIDs(ctx context.Context, token string, limit int) (ids []string, next string, err error) {
	if limit < 1 || uint64(limit) > math.MaxUint32 {
		return nil, "", fmt.Errorf("pinecone: invalid listing page size %d", limit)
	}
	if limit > MaxListPage {
		return nil, "", fmt.Errorf("pinecone: listing page size must be between 1 and %d", MaxListPage)
	}
	request := &pinecone.ListVectorsRequest{}
	page := uint32(limit)
	request.Limit = &page
	if token != "" {
		request.PaginationToken = &token
	}
	var res *pinecone.ListVectorsResponse
	if err = p.do(ctx, min(3, p.maxAttempts), func(attemptCtx context.Context) error {
		var listErr error
		res, listErr = p.defaultConn.ListVectors(attemptCtx, request)
		return listErr
	}); err != nil {
		if ListingUnsupported(err) {
			return nil, "", fmt.Errorf("%w: %w", ErrListingUnsupported, err)
		}
		return nil, "", fmt.Errorf("pinecone: list record keys: %w", err)
	}
	for _, id := range res.VectorIds {
		if id != nil && *id != "" {
			ids = append(ids, *id)
		}
	}
	if res.NextPaginationToken != nil {
		next = *res.NextPaginationToken
	}
	return ids, next, nil
}
