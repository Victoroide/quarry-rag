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
	fence, err := d.pg.FenceGeneration(ctx, p.generation)
	if err != nil {
		return err
	}
	defer fence.Close(ctx)
	blockers, err := d.blockers(ctx, p, profile)
	if err != nil {
		return err
	}
	if len(blockers) > 0 {
		return fmt.Errorf("staging is blocked: %s", strings.Join(blockers, "; "))
	}
	source, _, sourceHost, err := d.sourceProfile(ctx, p.live)
	if err != nil {
		return err
	}
	reuse, _, err := decideCopy(source, profile)
	if err != nil {
		return err
	}
	s := &stager{corpus: d.pg, ledger: fence.Ledger(), generation: p.generation, profile: profile, opts: opts, fence: fence}
	if reuse {
		live, closeLive, _, openErr := d.openBound(ctx, targetSpec{Store: p.live.store, Target: p.live.target, Generation: p.live.generation, Dimensions: source.Dimensions}, sourceHost)
		if hardSourceError(ctx, openErr) {
			return fmt.Errorf("the source profile is proven, but its vectors cannot be read: %w", openErr)
		}
		if openErr == nil {
			defer closeLive()
			s.copyFrom = live
		}
	}
	if opts.MaxEmbedCalls > 0 {
		if s.embed, err = d.newEmbedder(ctx, profile); err != nil {
			return fmt.Errorf("embedding provider: %w", err)
		}
	}
	spec := targetSpec{Store: p.store, Target: p.target, Generation: p.generation, Dimensions: profile.Dimensions, Create: true}
	var host string
	if p.store == "pinecone" {
		row, readErr := d.pg.Generation(ctx, p.generation)
		if readErr != nil {
			return readErr
		}
		if row != nil {
			host = row.Host
		}
		target, closeTarget, resolvedHost, openErr := d.openBound(ctx, spec, host)
		if openErr != nil {
			return fmt.Errorf("open the target: %w", openErr)
		}
		defer closeTarget()
		s.target, host = target, resolvedHost
	}
	if p.generation != domain.LegacyGeneration {
		if _, err = d.pg.RegisterGeneration(ctx, pgstore.Generation{Name: p.generation, Store: p.store, Target: p.target, Host: host, Profile: profile, Origin: pgstore.OriginRecorded}); err != nil {
			return err
		}
	}
	if pendingErr := fence.RequireAcknowledged(ctx, opts.ConfirmTargetDrained); pendingErr != nil {
		return pendingErr
	}
	row, err := d.pg.Generation(ctx, p.generation)
	if err != nil {
		return err
	}
	if row.EverActive {
		state, stateErr := d.pg.VectorState(ctx)
		if stateErr != nil {
			return stateErr
		}
		if !state.Frozen || state.FreezeGeneration != row.Name {
			return errors.New("freeze writers for the retired generation before rollback catch-up")
		}
		quiet, quietErr := d.pg.WriterQuiesce(ctx, opts.Quiesce)
		if quietErr != nil {
			return quietErr
		}
		if !quiet.Quiet() {
			return errors.New("wait for writers to settle before rollback catch-up")
		}
		s.retired = d.pg
	}
	if p.store == "postgres" {
		target, closeTarget, _, openErr := d.openBound(ctx, spec, "")
		if openErr != nil {
			return fmt.Errorf("open the target: %w", openErr)
		}
		defer closeTarget()
		s.target = target
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
	if len(rep.Unsupported) > 0 {
		return fmt.Errorf("%d documents store tagged vectors, which a reindex does not stage", len(rep.Unsupported))
	}
	return nil
}

type attestReport struct {
	Action  string                  `json:"action"`
	Applied bool                    `json:"applied"`
	Store   string                  `json:"store"`
	Target  string                  `json:"target"`
	Host    string                  `json:"host,omitempty"`
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
	if profile.Hash() != settings.Hash() {
		return fmt.Errorf("%w: the attested profile contradicts the configured embedding identity", pgstore.ErrGenerationDrift)
	}
	store, target := d.cfg.VectorTarget()
	_, _, host, err := d.sourceProfile(ctx, liveTarget{store, target, state.Active})
	if err != nil {
		return err
	}
	rep := attestReport{Action: string(actionAttest), Store: store, Target: target, Host: host, Profile: profile}
	if opts.Apply {
		if store == "pinecone" {
			_, closeSource, _, openErr := d.openBound(ctx, targetSpec{Store: store, Target: target, Generation: state.Active, Dimensions: profile.Dimensions}, host)
			if openErr != nil {
				return fmt.Errorf("open the source for attestation: %w", openErr)
			}
			defer closeSource()
		}
		if _, err = d.pg.RegisterGeneration(ctx, pgstore.Generation{Name: domain.LegacyGeneration, Store: store, Target: target, Host: host, Profile: profile, Origin: pgstore.OriginAttested}); err != nil {
			return err
		}
		rep.Applied = true
	}
	return json.NewEncoder(out).Encode(rep)
}
