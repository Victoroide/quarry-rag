package main

import (
	"context"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

type inventoryReport struct {
	Action          string                  `json:"action"`
	Generation      string                  `json:"generation"`
	Profile         domain.EmbeddingProfile `json:"profile"`
	ProfileHash     string                  `json:"profile_hash"`
	Target          targetReport            `json:"target"`
	Source          sourceReport            `json:"source"`
	Copy            copyReport              `json:"copy"`
	Documents       documentCounts          `json:"documents"`
	Chunks          int                     `json:"chunks"`
	TextBytes       int64                   `json:"text_bytes"`
	Ledger          ledgerCounts            `json:"ledger"`
	Plan            planReport              `json:"plan"`
	Truncated       bool                    `json:"truncated"`
	OpenDeletions   int                     `json:"open_deletions"`
	OpenDeletionIDs []string                `json:"open_deletion_ids,omitempty"`
	Blockers        []string                `json:"blockers"`
	Writes          int                     `json:"writes"`
}

type targetReport struct {
	Store  string `json:"store"`
	Target string `json:"target"`
	State  string `json:"state"`
	Host   string `json:"host,omitempty"`
}

type sourceReport struct {
	Store         string `json:"store"`
	Target        string `json:"target"`
	Host          string `json:"host,omitempty"`
	Generation    string `json:"generation"`
	ProfileKnown  bool   `json:"profile_known"`
	ProfileOrigin string `json:"profile_origin,omitempty"`
}

type copyReport struct {
	Eligible bool   `json:"eligible"`
	Reason   string `json:"reason,omitempty"`
}

type documentCounts struct {
	Completed       int `json:"completed"`
	Processing      int `json:"processing"`
	Failed          int `json:"failed"`
	Deleting        int `json:"deleting_or_held"`
	Inconsistent    int `json:"inconsistent"`
	TaggedVectors   int `json:"tagged_vectors"`
	WithoutOriginal int `json:"completed_without_original"`
}

type ledgerCounts struct {
	Current  int `json:"current"`
	Stale    int `json:"stale"`
	Missing  int `json:"missing"`
	Orphaned int `json:"orphaned"`
}

type planReport struct {
	DocumentsToStage         int `json:"documents_to_stage"`
	ChunksToStage            int `json:"chunks_to_stage"`
	EmbeddingCallsUpperBound int `json:"embedding_calls_upper_bound"`
}

// loadTargetProfile reads the profile file named by the invocation.
func (d deps) loadTargetProfile(opts options) (domain.EmbeddingProfile, error) {
	profile, err := d.readProfile(opts.Profile)
	if err != nil {
		return profile, err
	}
	return profile, profile.Validate()
}

// sourceProfile returns the recorded or attested profile of the live generation, or nil.
func (d deps) sourceProfile(ctx context.Context, live liveTarget) (profile *domain.EmbeddingProfile, origin, resolvedHost string, err error) {
	state, err := d.pg.VectorState(ctx)
	if err != nil {
		return nil, "", "", err
	}
	if state.Active != live.generation {
		return nil, "", "", fmt.Errorf("%w: configured source generation %q, active generation %q", pgstore.ErrGenerationDrift, live.generation, state.Active)
	}
	row, err := d.pg.Generation(ctx, live.generation)
	if err != nil {
		return nil, "", "", err
	}
	store, target, host := state.LegacyStore, state.LegacyTarget, state.LegacyHost
	if row != nil {
		store, target, host = row.Store, row.Target, row.Host
	} else if live.generation != domain.LegacyGeneration {
		return nil, "", "", fmt.Errorf("%w: active generation %q is not registered", pgstore.ErrGenerationDrift, live.generation)
	}
	if store == "" || target == "" {
		return nil, "", "", fmt.Errorf("%w: start a serving replica to bind the physical legacy target", pgstore.ErrGenerationDrift)
	}
	if live.store != store || live.target != target {
		return nil, "", "", fmt.Errorf("%w: configured source %s %q differs from the active %s %q", pgstore.ErrGenerationDrift, live.store, live.target, store, target)
	}
	if row == nil {
		return nil, "", host, nil
	}
	return &row.Profile, row.Origin, host, nil
}

// blockers lists reasons the target cannot be staged, before anything is written.
func (d deps) blockers(ctx context.Context, p targetPlan, profile domain.EmbeddingProfile) ([]string, error) {
	var out []string
	if err := config.ValidateEmbeddingDimensions(profile.Provider, profile.Dimensions); err != nil {
		out = append(out, err.Error())
	}
	configuredText := d.cfg.TextProfile()
	if configuredText == "" {
		out = append(out, "EXTRACTION_CHUNK_VERSION is required to identify the configured text settings")
	} else if configuredText != profile.ExtractionChunkVersion {
		out = append(out, fmt.Sprintf("the profile's extraction_chunk_version must match the configured text identity %q", configuredText))
	}
	if p.store == "postgres" && profile.Dimensions > 2000 {
		out = append(out, fmt.Sprintf("pgvector indexes support at most 2000 dimensions; the profile declares %d", profile.Dimensions))
	}
	if p.store == "pinecone" && p.target == "" {
		out = append(out, "a Pinecone target needs an index name")
	}
	if reason, err := d.protectedReason(ctx, p); err != nil {
		return nil, err
	} else if reason != "" {
		out = append(out, "the target is protected from writes: "+reason)
	}
	var tagged int
	if err := d.pg.Pool.QueryRow(ctx, `SELECT count(*) FROM documents WHERE status='completed' AND scoped_vectors`).Scan(&tagged); err != nil {
		return nil, fmt.Errorf("count tagged vector documents: %w", err)
	}
	if tagged > 0 {
		out = append(out, fmt.Sprintf("%d completed documents store tagged vectors, whose revision-qualified keys a copy by chunk key cannot reproduce", tagged))
	}
	row, err := d.pg.Generation(ctx, p.generation)
	if err != nil {
		return nil, err
	}
	if row != nil && (row.Store != p.store || row.Target != p.target || row.Profile.Hash() != profile.Hash()) {
		out = append(out, fmt.Sprintf("generation %q is already registered with a different store, target or profile", p.generation))
	}
	if p.generation == domain.LegacyGeneration && row == nil {
		out = append(out, "the legacy generation can be a target only after its profile is attested")
	}
	source, _, _, err := d.sourceProfile(ctx, p.live)
	if err != nil {
		return nil, err
	}
	if _, _, decideErr := decideCopy(source, profile); decideErr != nil {
		out = append(out, decideErr.Error())
	}
	return out, nil
}

// inventory reads PostgreSQL and the generation records and writes nothing. It
// never constructs an embedding provider or opens a vector store.
func (d deps) inventory(ctx context.Context, opts options, p targetPlan, profile domain.EmbeddingProfile) (inventoryReport, error) {
	rep := inventoryReport{Action: string(actionInventory), Generation: p.generation, Profile: profile, ProfileHash: profile.Hash(), Blockers: []string{}}
	rep.Target = targetReport{Store: p.store, Target: p.target, State: "unregistered"}
	row, err := d.pg.Generation(ctx, p.generation)
	if err != nil {
		return rep, err
	}
	if row != nil {
		rep.Target.State = row.State
		rep.Target.Host = row.Host
	}
	source, origin, host, err := d.sourceProfile(ctx, p.live)
	if err != nil {
		return rep, err
	}
	rep.Source = sourceReport{Store: p.live.store, Target: p.live.target, Host: host, Generation: p.live.generation, ProfileKnown: source != nil, ProfileOrigin: origin}
	reuse, reason, decideErr := decideCopy(source, profile)
	rep.Copy = copyReport{Eligible: reuse && decideErr == nil, Reason: reason}
	if rep.Blockers, err = d.blockers(ctx, p, profile); err != nil {
		return rep, err
	}
	if countErr := d.countDocuments(ctx, &rep); countErr != nil {
		return rep, countErr
	}
	quiesce, err := d.pg.WriterQuiesce(ctx, 0)
	if err != nil {
		return rep, err
	}
	rep.OpenDeletions = quiesce.OpenDeletions
	rep.OpenDeletionIDs = quiesce.OpenDeletionIDs
	if scanErr := d.scanPlan(ctx, opts, p, row != nil, &rep); scanErr != nil {
		return rep, scanErr
	}
	return rep, nil
}

func (d deps) countDocuments(ctx context.Context, rep *inventoryReport) error {
	rows, err := d.pg.Pool.Query(ctx, `SELECT status, count(*) FROM documents GROUP BY status`)
	if err != nil {
		return fmt.Errorf("count documents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if err = rows.Scan(&status, &n); err != nil {
			return fmt.Errorf("count documents: %w", err)
		}
		switch status {
		case "completed":
			rep.Documents.Completed = n
		case "processing":
			rep.Documents.Processing = n
		case "failed":
			rep.Documents.Failed = n
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return rowsErr
	}
	return d.pg.Pool.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM documents d WHERE d.status='completed' AND EXISTS (SELECT 1 FROM document_deletions x WHERE x.document_id=d.id AND (x.held OR NOT x.finalized))),
 (SELECT count(*) FROM documents WHERE status='completed' AND s3_key IS NULL),
 (SELECT COALESCE(sum(octet_length(c.text)), 0) FROM chunks c JOIN documents d ON d.id=c.document_id WHERE d.status='completed')`).
		Scan(&rep.Documents.Deleting, &rep.Documents.WithoutOriginal, &rep.TextBytes)
}

// scanPlan compares the eligible documents with the generation's ledger.
func (d deps) scanPlan(ctx context.Context, opts options, p targetPlan, registered bool, rep *inventoryReport) error {
	after := opts.After
	scanned := 0
	for {
		page, err := d.pg.DocumentRevisions(ctx, after, min(opts.Page, opts.MaxDocuments-scanned))
		if err != nil {
			return err
		}
		ids := make([]string, len(page))
		for i, rev := range page {
			ids[i] = rev.ID
		}
		var prior map[string]pgstore.Checkpoint
		if registered {
			if prior, err = d.pg.GenerationCheckpoints(ctx, p.generation, ids); err != nil {
				return err
			}
		}
		for _, rev := range page {
			after = rev.ID
			scanned++
			rep.Chunks += rev.Chunks
			cp, seen := prior[rev.ID]
			switch {
			case rev.DeclaredChunks != rev.Chunks:
				rep.Documents.Inconsistent++
			case rev.ScopedVectors:
				rep.Documents.TaggedVectors++
			case seen && cp.Complete && cp.SourceRevision == rev.Revision && cp.Incarnation == rev.Incarnation:
				rep.Ledger.Current++
			default:
				if seen {
					rep.Ledger.Stale++
				} else {
					rep.Ledger.Missing++
				}
				rep.Plan.DocumentsToStage++
				rep.Plan.ChunksToStage += rev.Chunks
			}
		}
		if len(page) == 0 || scanned >= opts.MaxDocuments {
			rep.Truncated = len(page) > 0 && scanned >= opts.MaxDocuments
			break
		}
	}
	// Reusable vectors are checked per chunk during staging, so the bound is an upper bound.
	rep.Plan.EmbeddingCallsUpperBound = rep.Plan.ChunksToStage
	if registered {
		orphans, err := d.pg.OrphanedGenerationDocuments(ctx, p.generation, opts.MaxDocuments)
		if err != nil {
			return err
		}
		rep.Ledger.Orphaned = len(orphans)
	}
	return nil
}
