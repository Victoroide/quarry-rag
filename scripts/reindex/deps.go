package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

// targetSpec names one vector store the tool reads or writes.
type targetSpec struct {
	Store      string
	Target     string
	Generation string
	Dimensions int
	// Create permits creating the schema of a PostgreSQL generation table.
	Create bool
}

// deps holds the process-wide collaborators so tests can substitute stores and models.
type deps struct {
	cfg *config.Config
	pg  *pgstore.Client
	// open returns a vector store and its closer.
	open        func(ctx context.Context, spec targetSpec) (vectorTarget, func(), error)
	newEmbedder func(ctx context.Context, profile domain.EmbeddingProfile) (domain.EmbeddingProvider, error)
	readProfile func(path string) (domain.EmbeddingProfile, error)
}

// pineconeStore adapts a Pinecone client to the target interface.
type pineconeStore struct{ *pcstore.Client }

func (p pineconeStore) DeleteIDs(ctx context.Context, ids []string) error {
	return p.Delete(ctx, "", ids)
}

func readProfileFile(path string) (domain.EmbeddingProfile, error) {
	file, err := os.Open(path) // #nosec G304 -- The operator names the profile file.
	if err != nil {
		return domain.EmbeddingProfile{}, fmt.Errorf("read profile: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, domain.MaxProfileBytes+1))
	if err = errors.Join(readErr, file.Close()); err != nil {
		return domain.EmbeddingProfile{}, fmt.Errorf("read profile: %w", err)
	}
	return domain.ParseEmbeddingProfile(data)
}

func newDeps(cfg *config.Config, pg *pgstore.Client) deps {
	return deps{
		cfg: cfg, pg: pg, readProfile: readProfileFile,
		open: func(ctx context.Context, spec targetSpec) (vectorTarget, func(), error) {
			switch spec.Store {
			case "postgres":
				var store *pgstore.VectorStore
				var err error
				if spec.Create {
					store, err = pgstore.CreateGenerationVectorStore(ctx, pg, spec.Dimensions, spec.Generation)
				} else {
					store, err = pgstore.OpenGenerationVectorStore(ctx, pg, spec.Dimensions, spec.Generation)
				}
				return store, func() {}, err
			case "pinecone":
				indexConfig := *cfg
				indexConfig.PineconeIndexName = spec.Target
				indexConfig.EmbeddingDimensions = spec.Dimensions
				client, err := pcstore.NewPineconeClient(ctx, &indexConfig)
				if err != nil {
					return nil, nil, err
				}
				return pineconeStore{client}, func() { _ = client.Close() }, nil
			}
			return nil, nil, fmt.Errorf("unsupported store %q", spec.Store)
		},
		newEmbedder: func(ctx context.Context, profile domain.EmbeddingProfile) (domain.EmbeddingProvider, error) {
			settings := *cfg
			settings.EmbeddingProvider = profile.Provider
			settings.EmbeddingDimensions = profile.Dimensions
			if profile.Provider == domain.EmbeddingProviderOpenAI {
				settings.OpenAIEmbeddingModel = profile.Model
				return embedder.NewOpenAIEmbedder(&settings)
			}
			settings.BedrockEmbedModel = profile.Model
			return embedder.NewBedrockEmbedder(ctx, &settings)
		},
	}
}

// targetPlan is the resolved identity of the generation an invocation addresses.
type targetPlan struct {
	generation string
	store      string
	target     string
	live       liveTarget
}

type liveTarget struct {
	store      string
	target     string
	generation string
}

func (d deps) resolve(opts options) targetPlan {
	liveStore, liveName := d.cfg.VectorTarget()
	p := targetPlan{generation: opts.Generation, live: liveTarget{liveStore, liveName, d.cfg.ActiveVectorGeneration()}}
	p.store = opts.TargetStore
	if p.store == "" {
		p.store = d.cfg.VectorStore
	}
	if p.store == "postgres" {
		p.target = domain.VectorTable(opts.Generation)
	} else {
		p.store = "pinecone"
		p.target = opts.TargetIndex
	}
	return p
}

// protectedReason explains why the target must not be written, or returns "".
func (d deps) protectedReason(ctx context.Context, p targetPlan) (string, error) {
	state, err := d.pg.VectorState(ctx)
	if err != nil {
		return "", err
	}
	switch {
	case state.Active == p.generation:
		return fmt.Sprintf("generation %q is the active generation", p.generation), nil
	case p.store == p.live.store && p.target == p.live.target:
		return fmt.Sprintf("%s %q is the live target of this configuration", p.store, p.target), nil
	}
	return "", nil
}
