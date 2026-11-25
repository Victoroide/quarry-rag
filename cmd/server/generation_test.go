package main

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestConfiguredVectorTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cfg    config.Config
		store  string
		target string
	}{
		{name: "legacy postgres", cfg: config.Config{VectorStore: "postgres"}, store: "postgres", target: "chunk_vectors"},
		{name: "generation postgres", cfg: config.Config{VectorStore: "postgres", VectorGeneration: "g2"}, store: "postgres", target: "chunk_vectors_g2"},
		{name: "explicit legacy postgres", cfg: config.Config{VectorStore: "postgres", VectorGeneration: "legacy"}, store: "postgres", target: "chunk_vectors"},
		{name: "pinecone index", cfg: config.Config{VectorStore: "pinecone", PineconeIndexName: "quarry-g2", VectorGeneration: "g2"}, store: "pinecone", target: "quarry-g2"},
		{name: "default pinecone", cfg: config.Config{PineconeIndexName: "quarry"}, store: "pinecone", target: "quarry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, target := configuredVectorTarget(&tc.cfg)
			require.Equal(t, tc.store, store)
			require.Equal(t, tc.target, target)
		})
	}
}

func TestConfiguredEmbedding(t *testing.T) {
	bedrock := configuredEmbedding(&config.Config{EmbeddingProvider: "bedrock", BedrockEmbedModel: "amazon.titan-embed-text-v2:0", EmbeddingDimensions: 1024})
	require.Equal(t, domain.EmbeddingProfile{Provider: "bedrock", Model: "amazon.titan-embed-text-v2:0", Dimensions: 1024, Metric: domain.MetricCosine}, bedrock)
	compatible := configuredEmbedding(&config.Config{EmbeddingProvider: "openai", OpenAIEmbeddingModel: "embed-small", BedrockEmbedModel: "ignored", EmbeddingDimensions: 384})
	require.Equal(t, domain.EmbeddingProfile{Provider: "openai", Model: "embed-small", Dimensions: 384, Metric: domain.MetricCosine}, compatible)
}
