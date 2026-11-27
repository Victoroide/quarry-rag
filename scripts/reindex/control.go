package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgconn"
)

type controlReport struct {
	Action           string           `json:"action"`
	Generation       string           `json:"generation,omitempty"`
	Applied          bool             `json:"applied"`
	Active           string           `json:"active_generation"`
	Frozen           bool             `json:"frozen"`
	FreezeGeneration string           `json:"freeze_generation,omitempty"`
	Quiesce          *pgstore.Quiesce `json:"quiesce,omitempty"`
	Ready            *bool            `json:"ready,omitempty"`
	Reasons          []string         `json:"reasons,omitempty"`

	HostedManifestCutover *pgstore.HostedManifestCutoverCounts `json:"hosted_manifest_cutover,omitempty"`
}

var errActivationNotReady = errors.New("the generation is not ready for activation")

func (d deps) reportHostedManifestCutover(ctx context.Context, rep *controlReport) (bool, error) {
	counts, err := d.pg.HostedManifestCutoverCounts(ctx)
	if err != nil {
		rep.HostedManifestCutover = nil
		return false, err
	}
	rep.HostedManifestCutover = &counts
	if counts == (pgstore.HostedManifestCutoverCounts{}) {
		return false, nil
	}
	rep.Reasons = append(rep.Reasons, fmt.Sprintf(
		"drain hosted work on the original source before activation (writing: %d, active: %d, retired: %d, cleanups: %d)",
		counts.Writing, counts.Active, counts.Retired, counts.Cleanups))
	return true, nil
}

func (d deps) state(ctx context.Context, rep *controlReport) error {
	state, err := d.pg.VectorState(ctx)
	if err != nil {
		return err
	}
	rep.Active, rep.Frozen, rep.FreezeGeneration = state.Active, state.Frozen, state.FreezeGeneration
	return nil
}

// freeze pauses every writer and, with -apply, waits until in-flight writes have settled.
func (d deps) freeze(ctx context.Context, opts options, out io.Writer, poll time.Duration) error {
	rep := controlReport{Action: string(actionFreeze), Generation: opts.Generation}
	if opts.Apply {
		if err := d.pg.FreezeWriters(ctx, opts.Generation); err != nil {
			return err
		}
		rep.Applied = true
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			quiesce, err := d.pg.WriterQuiesce(ctx, opts.Quiesce)
			if err != nil && ctx.Err() == nil {
				return err
			}
			if err == nil {
				rep.Quiesce = &quiesce
				if quiesce.Quiet() {
					break
				}
			}
			select {
			case <-ctx.Done():
				_ = d.state(context.WithoutCancel(ctx), &rep)
				_ = json.NewEncoder(out).Encode(rep)
				var last pgstore.Quiesce
				if rep.Quiesce != nil {
					last = *rep.Quiesce
				}
				return fmt.Errorf("writers did not quiesce within the timeout: %d jobs running, %d deletions open%s; the freeze stays in force",
					last.RunningJobs, last.OpenDeletions, pgstore.FormatOpenDeletions(last.OpenDeletions, last.OpenDeletionIDs))
			case <-timer.C:
				timer.Reset(poll)
			}
		}
	}
	if err := d.state(ctx, &rep); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(rep)
}

func (d deps) thaw(ctx context.Context, opts options, out io.Writer) error {
	rep := controlReport{Action: string(actionThaw)}
	if opts.Apply {
		if err := d.pg.ThawWriters(ctx); err != nil {
			return err
		}
		rep.Applied = true
	}
	if err := d.state(ctx, &rep); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(rep)
}

// corpusDigest identifies the exact set and revisions of the eligible documents.
func corpusDigest(ctx context.Context, c corpus, opts options) (string, error) {
	h := sha256.New()
	after := ""
	for remaining := opts.MaxDocuments; remaining > 0; {
		limit := min(opts.Page, remaining)
		revisions, err := c.DocumentRevisions(ctx, after, limit)
		if err != nil {
			return "", err
		}
		for _, rev := range revisions {
			after = rev.ID
			_, _ = fmt.Fprintf(h, "%s|%s\n", rev.ID, rev.Revision)
		}
		if len(revisions) < limit {
			return hex.EncodeToString(h.Sum(nil)), nil
		}
		remaining -= len(revisions)
	}
	more, err := c.HasDocumentsAfter(ctx, after)
	if err != nil {
		return "", err
	}
	if more {
		return "", errors.New("activation exceeds -max-documents, so the corpus digest cannot be proven")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// activate pins every replica to a verified generation under the freeze. Traffic
// moves only when the operator deploys replicas that name the generation.
func (d deps) activate(ctx context.Context, opts options, out io.Writer) error {
	rep := controlReport{Action: string(actionActivate), Generation: opts.Generation, Reasons: []string{}}
	var fence *pgstore.GenerationFence
	if opts.Apply {
		var err error
		fence, err = d.pg.FenceGeneration(ctx, opts.Generation)
		if err != nil {
			return err
		}
		defer fence.Close(ctx)
	}
	row, err := d.pg.Generation(ctx, opts.Generation)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("generation %q is not registered", opts.Generation)
	}
	if row.Store == "postgres" {
		blocked, countErr := d.reportHostedManifestCutover(ctx, &rep)
		if countErr != nil {
			return countErr
		}
		if blocked {
			if stateErr := d.state(ctx, &rep); stateErr != nil {
				return stateErr
			}
			ready := false
			rep.Ready = &ready
			if encodeErr := json.NewEncoder(out).Encode(rep); encodeErr != nil {
				return encodeErr
			}
			return errActivationNotReady
		}
	}
	digest, err := corpusDigest(ctx, d.pg, opts)
	if err != nil {
		return err
	}
	if stateErr := d.state(ctx, &rep); stateErr != nil {
		return stateErr
	}
	quiesce, qErr := d.pg.WriterQuiesce(ctx, opts.Quiesce)
	if qErr != nil {
		return qErr
	}
	rep.Quiesce = &quiesce
	if row.State != pgstore.StateVerified {
		rep.Reasons = append(rep.Reasons, fmt.Sprintf("the generation is %s, not verified", row.State))
	}
	if row.CorpusDigest != digest {
		rep.Reasons = append(rep.Reasons, "the corpus changed since verification")
	}
	if !rep.Frozen || rep.FreezeGeneration != opts.Generation {
		rep.Reasons = append(rep.Reasons, "writers are not frozen for this generation")
	}
	if quiesce.OpenDeletions > 0 {
		rep.Reasons = append(rep.Reasons, fmt.Sprintf("%d open deletions%s must finish before activation",
			quiesce.OpenDeletions, pgstore.FormatOpenDeletions(quiesce.OpenDeletions, quiesce.OpenDeletionIDs)))
	}
	ready := len(rep.Reasons) == 0
	rep.Ready = &ready
	if opts.Apply {
		if ready && row.Store == "pinecone" {
			_, closeTarget, _, openErr := d.openBound(ctx, targetSpec{Store: row.Store, Target: row.Target, Generation: row.Name, Dimensions: row.Profile.Dimensions}, row.Host)
			if openErr != nil {
				return fmt.Errorf("open the target for activation: %w", openErr)
			}
			defer closeTarget()
		}
		if err = fence.Activate(ctx, digest, opts.Quiesce); err != nil {
			var pgErr *pgconn.PgError
			if row.Store == "postgres" && errors.As(err, &pgErr) && pgErr.Code == "55000" &&
				pgErr.ConstraintName == "hosted_manifest_cutover_guard" {
				ready = false
				if blocked, _ := d.reportHostedManifestCutover(ctx, &rep); !blocked {
					rep.Reasons = append(rep.Reasons, "hosted manifest work blocked activation; check the original source before retrying")
				}
			}
			_ = json.NewEncoder(out).Encode(rep)
			return err
		}
		rep.Applied = true
		rep.Active = opts.Generation
		rep.Reasons = []string{}
	}
	if encodeErr := json.NewEncoder(out).Encode(rep); encodeErr != nil {
		return encodeErr
	}
	if rep.Applied || ready {
		return nil
	}
	return errActivationNotReady
}
