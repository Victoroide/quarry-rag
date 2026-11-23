package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/stretchr/testify/require"
)

type unexpectedEvidenceGenerator struct{ t *testing.T }

func (g unexpectedEvidenceGenerator) Generate(context.Context, llm.Request) (*llm.Response, error) {
	g.t.Fatal("evidence query called the generator")
	return nil, nil
}

func (g unexpectedEvidenceGenerator) Stream(context.Context, llm.Request, func(string) error) (*llm.Response, error) {
	g.t.Fatal("evidence query called the stream generator")
	return nil, nil
}

func TestEvidenceSelectionSkipsGenerationAndReranking(t *testing.T) {
	for _, provider := range []string{"bedrock", "openai"} {
		t.Run(provider, func(t *testing.T) {
			cfg := &config.Config{LLMProvider: provider, SynthesisContextTokens: 160, SynthesisRelativeScoreFloor: .55}
			amb := llm.NewAmbassador(cfg)
			amb.SetGenerator(unexpectedEvidenceGenerator{t})
			qc := &QueryContext{
				AnswerMode: domain.AnswerModeEvidence,
				RetrievedChunks: []domain.ScoredChunk{
					{Chunk: domain.Chunk{DocumentID: "allowed", Index: 3, Text: "Fold the paper.\u202e " + strings.Repeat("x", 750)}, Score: .9},
					{Chunk: domain.Chunk{DocumentID: "allowed", Index: 4, Text: "Other text."}, Score: .8},
				},
			}
			started := 0
			qc.Stream = &QueryStream{
				Context: func() error { started++; return nil },
				Delta:   func(string) error { t.Fatal("evidence query sent a text delta"); return nil },
			}
			stages := []QueryStage{NewRerankStage(unexpectedEvidenceReranker{t}, 0), NewSynthesisStage(amb, cfg)}
			_, err := RunRetrievalPipeline(t.Context(), qc, stages)
			require.NoError(t, err)
			require.Equal(t, 1, started)
			require.Equal(t, "evidence_only", qc.FinishReason)
			require.Zero(t, qc.TokenUsage)
			require.Empty(t, qc.FinalAnswer)
			require.Nil(t, qc.Citations)
			require.Len(t, qc.Excerpts, 1)
			require.Equal(t, []bool{true}, qc.Truncated)
			require.Equal(t, 3, qc.Excerpts[0].Chunk.Index)
			require.Equal(t, "Fold the paper. "+strings.Repeat("x", 627), qc.Excerpts[0].Chunk.Text)
			require.Equal(t, 160, qc.ContextTokens)
		})
	}
}

type unexpectedEvidenceReranker struct{ t *testing.T }

func (r unexpectedEvidenceReranker) Rerank(context.Context, string, []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	r.t.Fatal("evidence query called the reranker")
	return nil, nil
}
