package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

// stage copies or recomputes vectors into the named staging target.
func (d deps) stage(ctx context.Context, opts options, p targetPlan, profile domain.EmbeddingProfile, out io.Writer) error {
	blockers, err := d.blockers(ctx, p, profile)
	if err != nil {
		return err
	}
	if len(blockers) > 0 {
		return fmt.Errorf("staging is blocked: %s", strings.Join(blockers, "; "))
	}
	target, closeTarget, err := d.open(ctx, targetSpec{Store: p.store, Target: p.target, Generation: p.generation, Dimensions: profile.Dimensions, Create: true})
	if err != nil {
		return fmt.Errorf("open the target: %w", err)
	}
	defer closeTarget()
	if p.generation != domain.LegacyGeneration {
		if _, err = d.pg.RegisterGeneration(ctx, pgstore.Generation{Name: p.generation, Store: p.store, Target: p.target, Profile: profile, Origin: pgstore.OriginRecorded}); err != nil {
			return err
		}
	}
	source, _, err := d.sourceProfile(ctx, p.live)
	if err != nil {
		return err
	}
	reuse, _, err := decideCopy(source, profile)
	if err != nil {
		return err
	}
	s := &stager{corpus: d.pg, ledger: d.pg, target: target, generation: p.generation, profile: profile, opts: opts}
	if reuse {
		live, closeLive, openErr := d.open(ctx, targetSpec{Store: p.live.store, Target: p.live.target, Generation: p.live.generation, Dimensions: source.Dimensions})
		if openErr != nil {
			return fmt.Errorf("the source profile is proven, but its vectors cannot be read: %w", openErr)
		}
		defer closeLive()
		s.copyFrom = live
	}
	if opts.MaxEmbedCalls > 0 {
		if s.embed, err = d.newEmbedder(ctx, profile); err != nil {
			return fmt.Errorf("embedding provider: %w", err)
		}
	}
	rep, runErr := s.run(ctx)
	if encodeErr := json.NewEncoder(out).Encode(rep); encodeErr != nil {
		return encodeErr
	}
	if runErr != nil {
		return runErr
	}
	if len(rep.Inconsistent) > 0 {
		return fmt.Errorf("%d documents declare a chunk count that differs from their stored chunks", len(rep.Inconsistent))
	}
	return nil
}

type attestReport struct {
	Action  string                  `json:"action"`
	Applied bool                    `json:"applied"`
	Store   string                  `json:"store"`
	Target  string                  `json:"target"`
	Profile domain.EmbeddingProfile `json:"profile"`
}

// attest records the operator's statement of what the legacy target stores.
func (d deps) attest(ctx context.Context, opts options, out io.Writer) error {
	profile, err := d.loadTargetProfile(opts)
	if err != nil {
		return err
	}
	state, err := d.pg.VectorState(ctx)
	if err != nil {
		return err
	}
	if state.Active != domain.LegacyGeneration || d.cfg.ActiveVectorGeneration() != domain.LegacyGeneration {
		return errors.New("only the legacy target can be attested; a reindexed generation already records its profile")
	}
	settings := d.cfg.EmbeddingSettings()
	if profile.Provider != settings.Provider || profile.Model != settings.Model || profile.Dimensions != settings.Dimensions || profile.Metric != settings.Metric {
		return fmt.Errorf("the attested provider, model and dimensions contradict this configuration (%s %q at %d dimensions)", settings.Provider, settings.Model, settings.Dimensions)
	}
	store, target := d.cfg.VectorTarget()
	rep := attestReport{Action: string(actionAttest), Store: store, Target: target, Profile: profile}
	if opts.Apply {
		if _, err = d.pg.RegisterGeneration(ctx, pgstore.Generation{Name: domain.LegacyGeneration, Store: store, Target: target, Profile: profile, Origin: pgstore.OriginAttested}); err != nil {
			return err
		}
		rep.Applied = true
	}
	return json.NewEncoder(out).Encode(rep)
}
