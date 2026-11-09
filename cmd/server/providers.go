package main

import (
	"context"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/tracing"
)

func newEmbeddingProvider(ctx context.Context, cfg *config.Config) (domain.EmbeddingProvider, error) {
	if cfg.EmbeddingProvider == "openai" {
		return embedder.NewOpenAIEmbedder(cfg)
	}
	return embedder.NewBedrockEmbedder(ctx, cfg)
}

func newSynthesisProvider(ctx context.Context, cfg *config.Config) (*llm.Ambassador, error) {
	amb := llm.NewAmbassador(cfg)
	if cfg.LLMProvider == "openai" {
		client, err := llm.NewOpenAIClient(cfg)
		if err != nil {
			return nil, err
		}
		amb.SetGenerator(tracing.WrapGenerator(client, "openai"))
	} else {
		client, err := llm.NewClient(ctx, cfg)
		if err != nil {
			return nil, err
		}
		amb.SetClient(tracing.WrapConverser(client, "aws.bedrock", cfg.BedrockFallbackModel))
	}
	return amb, nil
}
