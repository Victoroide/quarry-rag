package retriever

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/Victoroide/quarry-rag/internal/awsconf"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentruntime/types"
)

// Reranker reorders evidence without replacing its vector similarity scores.
type Reranker interface {
	Rerank(context.Context, string, []domain.ScoredChunk) ([]domain.ScoredChunk, error)
}

// BedrockReranker scores at most 100 candidates against the original question.
type BedrockReranker struct {
	client   bedrockagentruntime.RerankAPIClient
	modelARN string
}

// NewBedrockReranker uses the shared AWS credential and retry policy.
func NewBedrockReranker(ctx context.Context, cfg *config.Config) (*BedrockReranker, error) {
	awsCfg, err := awsconf.Load(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("rerank: load aws config: %w", err)
	}
	return &BedrockReranker{client: bedrockagentruntime.NewFromConfig(awsCfg), modelARN: cfg.RerankModelARN}, nil
}

// Rerank returns candidates in descending relevance order, preserving vector scores.
// Equal relevance scores retain vector order; candidates beyond 100 follow unchanged.
func (r *BedrockReranker) Rerank(ctx context.Context, question string, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	n := min(len(chunks), 100)
	if n == 0 {
		return chunks, nil
	}
	sources := make([]types.RerankSource, n)
	for i, chunk := range chunks[:n] {
		sources[i] = types.RerankSource{Type: types.RerankSourceTypeInline, InlineDocumentSource: &types.RerankDocument{
			Type: types.RerankDocumentTypeText, TextDocument: &types.RerankTextDocument{Text: aws.String(chunk.Chunk.Text)},
		}}
	}
	in := &bedrockagentruntime.RerankInput{
		Queries: []types.RerankQuery{{Type: types.RerankQueryContentTypeText, TextQuery: &types.RerankTextDocument{Text: aws.String(question)}}},
		Sources: sources,
		RerankingConfiguration: &types.RerankingConfiguration{
			Type: types.RerankingConfigurationTypeBedrockRerankingModel,
			BedrockRerankingConfiguration: &types.BedrockRerankingConfiguration{
				ModelConfiguration: &types.BedrockRerankingModelConfiguration{ModelArn: aws.String(r.modelARN)},
				NumberOfResults:    aws.Int32(int32(n)),
			},
		},
	}
	out := slices.Clone(chunks)
	seen := make([]bool, n)
	tokens := make(map[string]bool)
	count := 0
	for {
		page, err := r.client.Rerank(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("rerank: request: %w", err)
		}
		if page == nil {
			return nil, fmt.Errorf("rerank: empty response")
		}
		for _, result := range page.Results {
			if result.Index == nil || result.RelevanceScore == nil {
				return nil, fmt.Errorf("rerank: missing result index or score")
			}
			i, score := int(*result.Index), float64(*result.RelevanceScore)
			if i < 0 || i >= n || seen[i] || math.IsNaN(score) || math.IsInf(score, 0) {
				return nil, fmt.Errorf("rerank: invalid result index or score")
			}
			seen[i], count = true, count+1
			out[i].RerankScore = &score
		}
		token := aws.ToString(page.NextToken)
		if token == "" {
			break
		}
		if tokens[token] {
			return nil, fmt.Errorf("rerank: repeated pagination token")
		}
		tokens[token] = true
		in.NextToken = aws.String(token)
	}
	if count != n {
		return nil, fmt.Errorf("rerank: incomplete results")
	}
	slices.SortStableFunc(out[:n], func(a, b domain.ScoredChunk) int {
		switch {
		case *a.RerankScore > *b.RerankScore:
			return -1
		case *a.RerankScore < *b.RerankScore:
			return 1
		default:
			return 0
		}
	})
	return out, nil
}
