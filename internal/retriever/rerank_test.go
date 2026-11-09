package retriever

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentruntime/types"
	"github.com/stretchr/testify/require"
)

type rerankClientFunc func(context.Context, *bedrockagentruntime.RerankInput) (*bedrockagentruntime.RerankOutput, error)

func (f rerankClientFunc) Rerank(ctx context.Context, in *bedrockagentruntime.RerankInput, _ ...func(*bedrockagentruntime.Options)) (*bedrockagentruntime.RerankOutput, error) {
	return f(ctx, in)
}

func rerankResult(index int32, score float32) types.RerankResult {
	return types.RerankResult{Index: aws.Int32(index), RelevanceScore: aws.Float32(score)}
}

func TestBedrockRerankRequestAndOrdering(t *testing.T) {
	chunks := []domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: "a", Text: "General archive information"}, Score: .95},
		{Chunk: domain.Chunk{ID: "b", Text: "The archive opens on Monday"}, Score: .8},
		{Chunk: domain.Chunk{ID: "c", Text: "Archive hours"}, Score: .7},
	}
	r := &BedrockReranker{modelARN: "arn:aws:bedrock:us-east-1::foundation-model/test", client: rerankClientFunc(func(_ context.Context, in *bedrockagentruntime.RerankInput) (*bedrockagentruntime.RerankOutput, error) {
		require.Len(t, in.Queries, 1)
		require.Equal(t, types.RerankQueryContentTypeText, in.Queries[0].Type)
		require.Equal(t, "When does the archive open?", aws.ToString(in.Queries[0].TextQuery.Text))
		require.Equal(t, types.RerankingConfigurationTypeBedrockRerankingModel, in.RerankingConfiguration.Type)
		cfg := in.RerankingConfiguration.BedrockRerankingConfiguration
		require.Equal(t, "arn:aws:bedrock:us-east-1::foundation-model/test", aws.ToString(cfg.ModelConfiguration.ModelArn))
		require.EqualValues(t, 3, aws.ToInt32(cfg.NumberOfResults))
		require.Len(t, in.Sources, 3)
		for i, source := range in.Sources {
			require.Equal(t, types.RerankSourceTypeInline, source.Type)
			require.Equal(t, types.RerankDocumentTypeText, source.InlineDocumentSource.Type)
			require.Equal(t, chunks[i].Chunk.Text, aws.ToString(source.InlineDocumentSource.TextDocument.Text))
		}
		return &bedrockagentruntime.RerankOutput{Results: []types.RerankResult{rerankResult(0, .25), rerankResult(2, .75), rerankResult(1, .75)}}, nil
	})}
	got, err := r.Rerank(t.Context(), "When does the archive open?", chunks)
	require.NoError(t, err)
	require.Equal(t, []domain.ChunkID{"b", "c", "a"}, []domain.ChunkID{got[0].Chunk.ID, got[1].Chunk.ID, got[2].Chunk.ID})
	require.Equal(t, .8, got[0].Score)
	require.InDelta(t, .75, *got[0].RerankScore, .00001)
	require.Nil(t, chunks[0].RerankScore)
	require.Equal(t, domain.ChunkID("a"), chunks[0].Chunk.ID)
}

func TestBedrockRerankBoundsAndPagination(t *testing.T) {
	chunks := make([]domain.ScoredChunk, 101)
	for i := range chunks {
		chunks[i] = domain.ScoredChunk{Chunk: domain.Chunk{ID: domain.ChunkID(fmt.Sprint(i)), Text: "Synthetic archive evidence"}, Score: .9}
	}
	calls := 0
	r := &BedrockReranker{modelARN: "model", client: rerankClientFunc(func(_ context.Context, in *bedrockagentruntime.RerankInput) (*bedrockagentruntime.RerankOutput, error) {
		calls++
		require.Len(t, in.Sources, 100)
		require.EqualValues(t, 100, aws.ToInt32(in.RerankingConfiguration.BedrockRerankingConfiguration.NumberOfResults))
		if calls == 1 {
			require.Nil(t, in.NextToken)
			return &bedrockagentruntime.RerankOutput{Results: []types.RerankResult{rerankResult(99, 1)}, NextToken: aws.String("next")}, nil
		}
		require.Equal(t, "next", aws.ToString(in.NextToken))
		out := &bedrockagentruntime.RerankOutput{}
		for i := range int32(99) {
			out.Results = append(out.Results, rerankResult(i, 0))
		}
		return out, nil
	})}
	got, err := r.Rerank(t.Context(), "archive", chunks)
	require.NoError(t, err)
	require.Len(t, got, 101)
	require.Equal(t, domain.ChunkID("99"), got[0].Chunk.ID)
	require.Equal(t, domain.ChunkID("100"), got[100].Chunk.ID)
	require.Nil(t, got[100].RerankScore)
	require.Equal(t, 2, calls)
}

func TestBedrockRerankRejectsIncompleteOrInvalidResults(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  *bedrockagentruntime.RerankOutput
		err  error
	}{
		{"upstream error", nil, errors.New("unavailable")},
		{"nil response", nil, nil},
		{"missing result", &bedrockagentruntime.RerankOutput{}, nil},
		{"duplicate", &bedrockagentruntime.RerankOutput{Results: []types.RerankResult{rerankResult(0, .9), rerankResult(0, .8)}}, nil},
		{"negative index", &bedrockagentruntime.RerankOutput{Results: []types.RerankResult{rerankResult(-1, .9)}}, nil},
		{"large index", &bedrockagentruntime.RerankOutput{Results: []types.RerankResult{rerankResult(2, .9)}}, nil},
		{"missing index", &bedrockagentruntime.RerankOutput{Results: []types.RerankResult{{RelevanceScore: aws.Float32(.9)}}}, nil},
		{"missing score", &bedrockagentruntime.RerankOutput{Results: []types.RerankResult{{Index: aws.Int32(0)}}}, nil},
		{"nan score", &bedrockagentruntime.RerankOutput{Results: []types.RerankResult{rerankResult(0, float32(math.NaN()))}}, nil},
		{"infinite score", &bedrockagentruntime.RerankOutput{Results: []types.RerankResult{rerankResult(0, float32(math.Inf(1)))}}, nil},
		{"repeated token", &bedrockagentruntime.RerankOutput{NextToken: aws.String("repeat")}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &BedrockReranker{client: rerankClientFunc(func(context.Context, *bedrockagentruntime.RerankInput) (*bedrockagentruntime.RerankOutput, error) {
				return tc.out, tc.err
			})}
			in := []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "a"}, Score: .9}, {Chunk: domain.Chunk{ID: "b"}, Score: .8}}
			_, err := r.Rerank(t.Context(), "archive", in)
			require.Error(t, err)
			require.Nil(t, in[0].RerankScore)
			require.Nil(t, in[1].RerankScore)
		})
	}
}

func TestBedrockRerankEmptySkipsCall(t *testing.T) {
	r := &BedrockReranker{client: rerankClientFunc(func(context.Context, *bedrockagentruntime.RerankInput) (*bedrockagentruntime.RerankOutput, error) {
		t.Fatal("empty evidence must skip reranking")
		return nil, nil
	})}
	got, err := r.Rerank(t.Context(), "archive", nil)
	require.NoError(t, err)
	require.Empty(t, got)
}
