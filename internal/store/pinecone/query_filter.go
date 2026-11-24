package pinecone

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	maxInValues = 10000
	// Pinecone caps query results at 4 MB and 10,000 matches. Allow 40 KiB
	// of metadata, up to 512 four-byte ID characters, and 1 KiB of framing
	// per match. Refill requests omit vector values.
	maxRefillTopK = min(10_000, 4_000_000/(maxMetadataBytes+512*4+1024))
)

func buildFilter(docType string, ids []string, collection string) *structpb.Struct {
	fields := make(map[string]*structpb.Value)
	if collection != "" {
		fields["collection"] = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$eq": structpb.NewStringValue(collection)}})
	}
	if docType != "" {
		fields["doc_type"] = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$eq": structpb.NewStringValue(docType)}})
	}
	if ids != nil {
		values := make([]*structpb.Value, len(ids))
		for i, id := range ids {
			values[i] = structpb.NewStringValue(id)
		}
		fields["doc_id"] = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$in": structpb.NewListValue(&structpb.ListValue{Values: values})}})
	}
	if len(fields) == 0 {
		return nil
	}
	return &structpb.Struct{Fields: fields}
}

// QueryFiltered searches the default namespace with document scope predicates.
func (p *Client) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	if filter.MatchesNothing() {
		return []domain.ScoredChunk{}, nil
	}
	merged := make(map[domain.ChunkID]domain.ScoredChunk)
	if err := p.queryPartitions(ctx, embedding, topK, filter, nil, func(chunks []domain.ScoredChunk) {
		for _, sc := range chunks {
			if prev, ok := merged[sc.Chunk.ID]; !ok || sc.Score > prev.Score {
				merged[sc.Chunk.ID] = sc
			}
		}
	}); err != nil {
		return nil, err
	}
	chunks := make([]domain.ScoredChunk, 0, len(merged))
	for _, sc := range merged {
		chunks = append(chunks, sc)
	}
	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].Score == chunks[j].Score {
			return chunks[i].Chunk.ID < chunks[j].Chunk.ID
		}
		return chunks[i].Score > chunks[j].Score
	})
	return chunks[:min(topK, len(chunks))], nil
}

func (p *Client) queryCandidates(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, budget *int) ([]domain.ScoredChunk, error) {
	var candidates []domain.ScoredChunk
	if err := p.queryPartitions(ctx, embedding, topK, filter, budget, func(chunks []domain.ScoredChunk) {
		candidates = append(candidates, chunks...)
	}); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (p *Client) queryPartitions(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, budget *int, accept func([]domain.ScoredChunk)) error {
	if filter.MatchesNothing() {
		return nil
	}
	if topK < 1 || uint64(topK) > math.MaxUint32 {
		return fmt.Errorf("pinecone: topK must be between 1 and %d", uint64(math.MaxUint32))
	}
	partitions := 1 + max(0, len(filter.DocumentIDs)-1)/maxInValues
	if budget != nil && *budget < partitions {
		return domain.ErrVectorQueryBudget
	}
	for part := range partitions {
		start := part * maxInValues
		ids := filter.DocumentIDs[start:min(start+maxInValues, len(filter.DocumentIDs))]
		req := &pinecone.QueryByVectorValuesRequest{Vector: embedding, TopK: uint32(topK), IncludeMetadata: true, MetadataFilter: buildFilter(filter.DocType, ids, filter.Collection)}
		var partitionBudget *int
		limited := false
		remainingPartitions := partitions - part - 1
		if budget != nil {
			// Keep one attempt for each later partition before allowing retries.
			available := *budget - remainingPartitions
			limited = available < p.queryAttempts()
			partitionBudget = &available
		}
		chunks, err := p.queryNamespace(ctx, "", req, partitionBudget)
		if partitionBudget != nil {
			*budget = *partitionBudget + remainingPartitions
			if limited && *partitionBudget == 0 && err != nil && retryable(err) {
				err = errors.Join(err, domain.ErrVectorQueryBudget)
			}
		}
		if err != nil {
			return err
		}
		accept(chunks)
	}
	return nil
}
