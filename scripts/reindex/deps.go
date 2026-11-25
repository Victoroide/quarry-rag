package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
	open        func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error)
	newEmbedder func(ctx context.Context, profile domain.EmbeddingProfile) (domain.EmbeddingProvider, error)
	readProfile func(path string) (domain.EmbeddingProfile, error)
}

// pineconeStore adapts a Pinecone client to the target interface.
type pineconeStore struct{ *pcstore.Client }

func (p pineconeStore) DeleteIDs(ctx context.Context, ids []string) error {
	return p.Delete(ctx, "", ids)
}

func readProfileFile(path string) (domain.EmbeddingProfile, error) {
	cleanPath := filepath.Clean(path)
	info, err := os.Stat(cleanPath)
	if err != nil {
		return domain.EmbeddingProfile{}, fmt.Errorf("stat profile: %w", err)
	}
	if !info.Mode().IsRegular() {
		return domain.EmbeddingProfile{}, errors.New("the profile must be a regular file")
	}
	file, err := os.Open(cleanPath)
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
		open: func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
			switch spec.Store {
			case "postgres":
				var store *pgstore.VectorStore
				var err error
				exactScope := pgstore.WithExactScopeMax(cfg.PGVectorExactScopeMax)
				if spec.Create {
					store, err = pgstore.CreateGenerationVectorStore(ctx, pg, spec.Dimensions, spec.Generation, exactScope)
					if err == nil {
						err = store.EnsureExactScopeIndexes(ctx)
					}
				} else {
					store, err = pgstore.OpenGenerationVectorStore(ctx, pg, spec.Dimensions, spec.Generation, exactScope)
				}
				return store, func() {}, "", err
			case "pinecone":
				indexConfig := *cfg
				indexConfig.PineconeIndexName = spec.Target
				indexConfig.EmbeddingDimensions = spec.Dimensions
				if spec.Create {
					// A lost mutation response has to reach the reindex fence.
					indexConfig.PineconeMaxAttempts = 1
				}
				client, err := pcstore.NewPineconeClient(ctx, &indexConfig)
				if err != nil {
					if errors.Is(err, pcstore.ErrIndexShape) {
						err = errors.Join(pgstore.ErrGenerationDrift, err)
					}
					return nil, nil, "", err
				}
				return pineconeStore{client}, func() { _ = client.Close() }, client.Host(), nil
			}
			return nil, nil, "", fmt.Errorf("unsupported store %q", spec.Store)
		},
		newEmbedder: func(ctx context.Context, profile domain.EmbeddingProfile) (domain.EmbeddingProvider, error) {
			configured := cfg.EmbeddingSettings()
			if err := configured.Validate(); err != nil {
				return nil, fmt.Errorf("configured embedding identity: %w", err)
			}
			if configured.Hash() != profile.Hash() {
				return nil, fmt.Errorf("%w: configured embedding identity does not match the target profile", pgstore.ErrGenerationDrift)
			}
			if profile.Provider == domain.EmbeddingProviderOpenAI {
				return embedder.NewOpenAIEmbedder(cfg)
			}
			// Each staging call is charged once, including a failed transport attempt.
			staging := *cfg
			staging.AWSMaxAttempts = 1
			return embedder.NewBedrockEmbedder(ctx, &staging)
		},
	}
}

// openBound rejects a changed physical index before the caller can use it.
// An empty expected host is allowed only for a new Pinecone target.
func (d deps) openBound(ctx context.Context, spec targetSpec, expectedHost string) (target vectorTarget, closeTarget func(), resolvedHost string, err error) {
	store, closeStore, host, err := d.open(ctx, spec)
	if err != nil {
		return nil, nil, "", err
	}
	if spec.Store == "pinecone" && (host == "" || expectedHost != "" && expectedHost != host) {
		closeStore()
		return nil, nil, "", fmt.Errorf("%w: index %q resolved to %q, recorded host %q", pgstore.ErrGenerationDrift, spec.Target, host, expectedHost)
	}
	return store, closeStore, host, nil
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
	case state.Active == domain.LegacyGeneration && p.store == state.LegacyStore && p.target == state.LegacyTarget:
		return fmt.Sprintf("%s %q is the recorded live target", p.store, p.target), nil
	}
	row, err := d.pg.Generation(ctx, state.Active)
	if err != nil {
		return "", err
	}
	if row != nil && p.store == row.Store && p.target == row.Target {
		return fmt.Sprintf("%s %q is the recorded live target", p.store, p.target), nil
	}
	return "", nil
}
