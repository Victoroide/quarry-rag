package pinecone

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	sdk "github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestBudgetedQueryReportsUpstreamAtOrdinaryRetryCeiling(t *testing.T) {
	client := testClient(&fakeIndexConn{query: func(context.Context, *sdk.QueryByVectorValuesRequest) (*sdk.QueryVectorsResponse, error) {
		return nil, status.Error(codes.Unavailable, "upstream unavailable")
	}})
	client.maxAttempts = 1
	var exhausted []bool
	for _, budget := range []int{1, 2} {
		_, used, err := NewDriver(client).QueryFilteredBudgeted(t.Context(), []float32{1}, 4, domain.VectorFilter{}, budget)
		require.Error(t, err)
		require.Equal(t, 1, used)
		exhausted = append(exhausted, errors.Is(err, domain.ErrVectorQueryBudget))
	}
	t.Logf("budget_exhausted for budgets 1/2 with provider max_attempts=1: %v", exhausted)
	require.Equal(t, []bool{false, false}, exhausted)
}

func TestHostedRefillKeepsLiveMatchesFromEveryPartition(t *testing.T) {
	ids := make([]string, maxInValues+1)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	var calls []string
	availableResponses := 0
	client := testClient(&fakeIndexConn{query: func(_ context.Context, req *sdk.QueryByVectorValuesRequest) (*sdk.QueryVectorsResponse, error) {
		group := req.MetadataFilter.Fields["doc_id"].GetStructValue().Fields["$in"].GetListValue().Values
		doc := group[0].GetStringValue()
		calls = append(calls, fmt.Sprintf("%s/K%d", doc, req.TopK))
		md, err := structpb.NewStruct(map[string]any{"doc_id": doc})
		require.NoError(t, err)
		var matches []*sdk.ScoredVector
		if doc == ids[len(ids)-1] {
			availableResponses++
			matches = append(matches, &sdk.ScoredVector{Score: .5, Vector: &sdk.Vector{Id: "available", Metadata: md}})
		} else {
			for i := 0; i < int(req.TopK); i++ {
				matches = append(matches, &sdk.ScoredVector{Score: 1 - float32(i)*.01, Vector: &sdk.Vector{Id: fmt.Sprintf("pending-%d", i), Metadata: md}})
			}
		}
		return &sdk.QueryVectorsResponse{Matches: matches}, nil
	}})
	var checks [][]domain.ChunkID
	verify := func(_ context.Context, hits []domain.ScoredChunk, _ domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
		var out []domain.ScoredChunk
		var checked []domain.ChunkID
		for _, h := range hits {
			checked = append(checked, h.Chunk.ID)
			if h.Chunk.ID == "available" {
				out = append(out, h)
			}
		}
		checks = append(checks, checked)
		return out, len(out) < len(hits), nil
	}
	r := retriever.NewRetriever(NewDriver(client), nil, .7, 2)
	got, err := r.RetrieveVisibleWithEmbedding(t.Context(), []float32{1}, retriever.RetrievalModeGlobal, "", 2, domain.VectorFilter{DocumentIDs: ids}, 2, verify)
	require.NoError(t, err)
	t.Logf("calls=%v; responses containing available chunk=%d; IDs submitted for visibility=%v", calls, availableResponses, checks)
	require.Equal(t, []string{"0/K2", "10000/K2", "0/K4", "10000/K4"}, calls)
	require.Equal(t, 2, availableResponses)
	require.Contains(t, checks[0], domain.ChunkID("available"))
	require.Len(t, got.Chunks, 1)
	require.Equal(t, domain.ChunkID("available"), got.Chunks[0].Chunk.ID)
}

func TestHostedRefillVerifiesBothVersionsThroughDriver(t *testing.T) {
	match := func(id, body string, score float32) *sdk.ScoredVector {
		md, err := structpb.NewStruct(map[string]any{"doc_id": "same-doc", "text": body})
		require.NoError(t, err)
		return &sdk.ScoredVector{Score: score, Vector: &sdk.Vector{Id: id, Metadata: md}}
	}
	previous := match("same-id", "Previous.", .99)
	other := match("other-id", "Unavailable.", .98)
	current := match("same-id", "Current.", .90)
	client := testClient(&fakeIndexConn{query: func(_ context.Context, req *sdk.QueryByVectorValuesRequest) (*sdk.QueryVectorsResponse, error) {
		raw := []*sdk.ScoredVector{previous, other}
		if req.TopK > 2 {
			raw = append(raw, current)
		}
		return &sdk.QueryVectorsResponse{Matches: raw}, nil
	}})
	var seen [][]string
	verify := func(_ context.Context, hits []domain.ScoredChunk, _ domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
		var checked []string
		var out []domain.ScoredChunk
		for _, h := range hits {
			checked = append(checked, h.Chunk.Text)
			if h.Chunk.Text == "Current." {
				out = append(out, h)
			}
		}
		seen = append(seen, checked)
		return out, len(out) < len(hits), nil
	}
	r := retriever.NewRetriever(NewDriver(client), nil, .7, 2)
	got, err := r.RetrieveVisibleWithEmbedding(t.Context(), []float32{1}, retriever.RetrievalModeGlobal, "", 2, domain.VectorFilter{}, 1, verify)
	require.NoError(t, err)
	t.Logf("bodies reaching visibility checks=%v", seen)
	require.Len(t, got.Chunks, 1)
	require.Equal(t, "Current.", got.Chunks[0].Chunk.Text)
	require.Contains(t, seen[1], "Previous.")
	require.Contains(t, seen[1], "Current.")
}
