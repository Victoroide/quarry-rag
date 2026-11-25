package main

import (
	"context"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

// configuredVectorTarget names the store and target the configuration selects.
func configuredVectorTarget(cfg *config.Config) (store, target string) {
	if cfg.VectorStore == "postgres" {
		return "postgres", pgstore.VectorTable(cfg.ActiveVectorGeneration())
	}
	return "pinecone", cfg.PineconeIndexName
}

// configuredEmbedding returns the embedding settings the process uses. The
// model revision and text version are recorded with a generation, not configured.
func configuredEmbedding(cfg *config.Config) domain.EmbeddingProfile {
	model := cfg.BedrockEmbedModel
	if cfg.EmbeddingProvider == domain.EmbeddingProviderOpenAI {
		model = cfg.OpenAIEmbeddingModel
	}
	return domain.EmbeddingProfile{Provider: cfg.EmbeddingProvider, Model: model, Dimensions: cfg.EmbeddingDimensions, Metric: domain.MetricCosine}
}

// pinVectorGeneration refuses to serve unless the configured generation is the
// active one and matches its recorded target and embedding settings.
func pinVectorGeneration(ctx context.Context, cfg *config.Config, pg *pgstore.Client) error {
	generation := cfg.ActiveVectorGeneration()
	if cfg.FakeMode {
		pg.SetVectorGeneration(generation)
		return nil
	}
	store, target := configuredVectorTarget(cfg)
	if err := pg.PinVectorGeneration(ctx, generation, store, target, configuredEmbedding(cfg)); err != nil {
		return fmt.Errorf("vector generation %q: %w", generation, err)
	}
	pg.SetVectorGeneration(generation)
	return nil
}

// newPostgresVectors opens the vector table of the configured generation.
func newPostgresVectors(ctx context.Context, cfg *config.Config, pg *pgstore.Client) (*pgstore.VectorStore, error) {
	generation := cfg.ActiveVectorGeneration()
	exactScope := pgstore.WithExactScopeMax(cfg.PGVectorExactScopeMax)
	if generation == domain.LegacyGeneration {
		return pgstore.NewVectorStore(ctx, pg, cfg.EmbeddingDimensions, exactScope)
	}
	store, err := pgstore.OpenGenerationVectorStore(ctx, pg, cfg.EmbeddingDimensions, generation, exactScope)
	if err != nil {
		return nil, err
	}
	if err = store.EnsureExactScopeIndexes(ctx); err != nil {
		return nil, err
	}
	store.RouteCompletion()
	return store, nil
}
