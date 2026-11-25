package main

import (
	"context"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

type servingVectorPinner interface {
	PinVectorGeneration(context.Context, string, string, string, string, domain.EmbeddingProfile) error
}

// pinVectorGeneration checks the opened target before admitting any work.
func pinVectorGeneration(ctx context.Context, cfg *config.Config, pg servingVectorPinner, host string) error {
	generation := cfg.ActiveVectorGeneration()
	store, target := cfg.VectorTarget()
	if err := pg.PinVectorGeneration(ctx, generation, store, target, host, cfg.EmbeddingSettings()); err != nil {
		return fmt.Errorf("vector generation %q: %w", generation, err)
	}
	return nil
}

func openPineconeVectors(ctx context.Context, cfg *config.Config, pg servingVectorPinner, open func(context.Context, *config.Config) (*pcstore.Client, error)) (*pcstore.Client, error) {
	client, err := open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = pinVectorGeneration(ctx, cfg, pg, client.Host()); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
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
