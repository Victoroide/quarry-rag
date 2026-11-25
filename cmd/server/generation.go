package main

import (
	"context"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

// pinVectorGeneration refuses to serve unless the configured generation is the
// active one and matches its recorded target and embedding settings.
func pinVectorGeneration(ctx context.Context, cfg *config.Config, pg *pgstore.Client) error {
	generation := cfg.ActiveVectorGeneration()
	if cfg.FakeMode {
		pg.SetVectorGeneration(generation)
		return nil
	}
	store, target := cfg.VectorTarget()
	if err := pg.PinVectorGeneration(ctx, generation, store, target, cfg.EmbeddingSettings()); err != nil {
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
	if indexErr := store.EnsureExactScopeIndexes(ctx); indexErr != nil {
		return nil, indexErr
	}
	store.RouteCompletion()
	return store, nil
}
