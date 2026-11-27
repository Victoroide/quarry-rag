package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

const activationTarget = "g2"

func activationHarness(t *testing.T) (*harness, options) {
	t.Helper()
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	h.stagePinecone(activationTarget, "quarry-g2", sourceModel, 0)
	require.NoError(t, f.pg.FreezeWriters(t.Context(), activationTarget))
	verify := verifyOptions(activationTarget)
	verify.Apply = true
	_, err := h.run(verify)
	require.NoError(t, err)
	setActivationFreeze(t.Context(), t, f.pg, activationTarget, false)
	opts, err := parse(t, "-activate", "-generation", activationTarget, "-quiesce", domain.WriteRequestTimeout.String())
	require.NoError(t, err)
	return h, opts
}

// Set the database clock inputs directly while keeping the recorded verification.
func setActivationFreeze(ctx context.Context, t *testing.T, pg *pgstore.Client, generation string, settling bool) {
	t.Helper()
	_, err := pg.Pool.Exec(ctx, `UPDATE vector_state SET frozen = ($1 <> ''),
		freeze_generation = NULLIF($1, ''),
		frozen_at = CASE WHEN $1 = '' THEN NULL WHEN $2 THEN NOW() ELSE NOW() - INTERVAL '2 hours' END`, generation, settling)
	require.NoError(t, err)
}

func insertActivationJob(t *testing.T, pg *pgstore.Client, state string, live bool) string {
	t.Helper()
	doc := newDoc("jobs", "note")
	doc.Status = "processing"
	insertDoc(t, pg, doc)
	_, err := pg.Pool.Exec(t.Context(), `INSERT INTO ingest_jobs(document_id, state, lease_owner, lease_until, lease_token)
		VALUES ($1, $2, CASE WHEN $2 = 'running' THEN 'worker' END,
			CASE WHEN $3 THEN NOW() + INTERVAL '1 day' ELSE NOW() - INTERVAL '1 hour' END,
			CASE WHEN $2 = 'running' THEN gen_random_uuid() END)`, doc.ID, state, live)
	require.NoError(t, err)
	return doc.ID
}

func insertActivationDeletion(ctx context.Context, t *testing.T, pg *pgstore.Client, held bool) string {
	t.Helper()
	id := uuid.NewString()
	_, err := pg.Pool.Exec(ctx, `INSERT INTO document_deletions
		(document_id,incarnation,kind,vector_mode,finalized,held)
		VALUES($1,$2,'quarantine','legacy',$3,$3)`, id, uuid.NewString(), held)
	require.NoError(t, err)
	return id
}

func activationReport(t *testing.T, output []byte, ready, applied bool, active, freeze string) controlReport {
	t.Helper()
	var rep controlReport
	require.NoError(t, json.Unmarshal(output, &rep), string(output))
	require.Equal(t, string(actionActivate), rep.Action)
	require.Equal(t, activationTarget, rep.Generation)
	require.NotNil(t, rep.Ready)
	require.NotNil(t, rep.Quiesce)
	require.Contains(t, string(output), `"applied":`)
	require.Equal(t, ready, *rep.Ready)
	require.Equal(t, applied, rep.Applied)
	require.Equal(t, active, rep.Active)
	require.Equal(t, freeze != "", rep.Frozen)
	require.Equal(t, freeze, rep.FreezeGeneration)
	return rep
}

func requireActivationState(ctx context.Context, t *testing.T, pg *pgstore.Client, active, freeze, targetState string) {
	t.Helper()
	state, err := pg.VectorState(ctx)
	require.NoError(t, err)
	require.Equal(t, active, state.Active)
	require.Equal(t, freeze != "", state.Frozen)
	require.Equal(t, freeze, state.FreezeGeneration)
	target, err := pg.Generation(ctx, activationTarget)
	require.NoError(t, err)
	require.NotNil(t, target)
	require.Equal(t, targetState, target.State)
	previous, err := pg.Generation(ctx, pgstore.LegacyGeneration)
	require.NoError(t, err)
	require.NotNil(t, previous)
	if active == pgstore.LegacyGeneration {
		require.Equal(t, pgstore.StateActive, previous.State)
	} else {
		require.Equal(t, pgstore.StateRetired, previous.State)
	}
}

func TestAppendActivationQuiesceReasons(t *testing.T) {
	q := pgstore.Quiesce{Frozen: true, RunningJobs: 2, OpenDeletions: 1, OpenDeletionIDs: []string{"receipt-id"}}
	blockers := []string{"2 in-flight jobs must finish before activation", "1 open deletions (receipt-id) must finish before activation"}
	rep := controlReport{Generation: activationTarget, Frozen: true, FreezeGeneration: activationTarget}
	appendActivationQuiesceReasons(&rep, q, time.Minute)
	require.Equal(t, append([]string{"the freeze is still settling; wait 1m0s after freezing"}, blockers...), rep.Reasons)

	rep.Reasons, rep.FreezeGeneration = nil, "g3"
	appendActivationQuiesceReasons(&rep, q, time.Minute)
	require.Equal(t, blockers, rep.Reasons)

	rep.Reasons, rep.FreezeGeneration = nil, activationTarget
	q.Frozen = false
	appendActivationQuiesceReasons(&rep, q, time.Minute)
	require.Equal(t, blockers, rep.Reasons)
}

func TestActivationWriterBlockersSkipCorpusOverLimit(t *testing.T) {
	h, opts := activationHarness(t)
	pg := h.f.pg
	opts.MaxDocuments = 1
	job := insertActivationJob(t, pg, "running", false)
	id := insertActivationDeletion(t.Context(), t, pg, true)
	opens, embeds := h.stores.opened, h.embedCalls()
	settling := fmt.Sprintf("the freeze is still settling; wait %s after freezing", opts.Quiesce)
	const running = "1 in-flight jobs must finish before activation"
	deleting := fmt.Sprintf("1 open deletions (%s) must finish before activation", id)
	const freezeRequired = "writers are not frozen for this generation"
	for _, tt := range []struct {
		name, freeze, targetState     string
		settling, live, open, pending bool
		missingFreezeTime             bool
		reasons                       []string
	}{
		{name: "settling", freeze: activationTarget, settling: true, reasons: []string{settling}},
		{name: "live lease", freeze: activationTarget, live: true, reasons: []string{running}},
		{name: "settling and live lease", freeze: activationTarget, settling: true, live: true, reasons: []string{settling, running}},
		{name: "open deletion", freeze: activationTarget, open: true, reasons: []string{deleting}},
		{name: "all blockers", freeze: activationTarget, settling: true, live: true, open: true, reasons: []string{settling, running, deleting}},
		{name: "no freeze", reasons: []string{freezeRequired}},
		{name: "wrong freeze", freeze: "g3", settling: true, reasons: []string{freezeRequired}},
		{name: "missing freeze timestamp", freeze: activationTarget, missingFreezeTime: true, reasons: []string{settling}},
		{name: "staging", freeze: activationTarget, targetState: pgstore.StateStaging, reasons: []string{"the generation is staging; it must be verified"}},
		{name: "pending write", freeze: activationTarget, pending: true, reasons: []string{"the generation has an unconfirmed target write"}},
		{name: "already active", freeze: activationTarget, targetState: pgstore.StateActive, settling: true, live: true, open: true, reasons: []string{`generation "g2" is already active`}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			setActivationFreeze(ctx, t, pg, tt.freeze, tt.settling)
			_, err := pg.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until =
				CASE WHEN $2 THEN NOW() + INTERVAL '1 day' ELSE NOW() - INTERVAL '1 hour' END WHERE document_id=$1`, job, tt.live)
			require.NoError(t, err)
			_, err = pg.Pool.Exec(ctx, "UPDATE document_deletions SET finalized=NOT $2, held=NOT $2 WHERE document_id=$1", id, tt.open)
			require.NoError(t, err)
			if tt.missingFreezeTime {
				_, err = pg.Pool.Exec(ctx, `UPDATE vector_state SET frozen_at=NULL`)
				require.NoError(t, err)
			}
			targetState := tt.targetState
			if targetState == "" {
				targetState = pgstore.StateVerified
			}
			_, err = pg.Pool.Exec(ctx, `UPDATE vector_generations SET state=$2, pending_write=$3,
				pending_write_id=CASE WHEN $3 THEN gen_random_uuid() END WHERE name=$1`, activationTarget, targetState, tt.pending)
			require.NoError(t, err)
			active := pgstore.LegacyGeneration
			if targetState == pgstore.StateActive {
				active = activationTarget
				_, err = pg.Pool.Exec(ctx, `UPDATE vector_state SET active_generation=$1`, active)
				require.NoError(t, err)
			}
			beforeState, err := pg.VectorState(ctx)
			require.NoError(t, err)
			beforeTarget, err := pg.Generation(ctx, activationTarget)
			require.NoError(t, err)
			tracer := &activationTracer{}
			d := traceActivation(t, h.deps, tracer)
			for _, apply := range []bool{false, true} {
				opts.Apply = apply
				var out bytes.Buffer
				err = execute(ctx, d, opts, &out, time.Millisecond)
				require.ErrorIs(t, err, errActivationNotReady)
				for _, reason := range tt.reasons {
					require.ErrorContains(t, err, reason)
				}
				rep := activationReport(t, out.Bytes(), false, false, active, tt.freeze)
				require.Equal(t, tt.reasons, rep.Reasons)
				require.EqualValues(t, boolCount(tt.live), rep.Quiesce.RunningJobs)
				require.EqualValues(t, boolCount(tt.open), rep.Quiesce.OpenDeletions)
				if tt.open {
					require.Equal(t, []string{id}, rep.Quiesce.OpenDeletionIDs)
				}
				require.Zero(t, tracer.pages)
				require.Zero(t, tracer.probes)
				require.Equal(t, opens, h.stores.opened)
				require.Equal(t, embeds, h.embedCalls())
				afterState, stateErr := pg.VectorState(ctx)
				require.NoError(t, stateErr)
				require.Equal(t, beforeState, afterState)
				afterTarget, targetErr := pg.Generation(ctx, activationTarget)
				require.NoError(t, targetErr)
				require.Equal(t, beforeTarget, afterTarget)
			}
		})
	}
}

func boolCount(b bool) int {
	if b {
		return 1
	}
	return 0
}

type activationTracer struct {
	reads, pages, probes int
	before               func(context.Context, *pgx.Conn, string, int)
	queryErr             error
}

func (tr *activationTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	var kind string
	var count int
	switch {
	case strings.Contains(data.SQL, "FROM vector_state CROSS JOIN vector_generations AS target"):
		tr.reads++
		kind, count = "readiness", tr.reads
	case strings.HasPrefix(data.SQL, "WITH eligible AS MATERIALIZED"):
		tr.pages++
		kind, count = "page", tr.pages
	case strings.HasPrefix(data.SQL, "SELECT EXISTS(SELECT 1 FROM documents d WHERE"):
		tr.probes++
		kind, count = "probe", tr.probes
	}
	if kind != "" && tr.before != nil {
		tr.before(ctx, conn, kind, count)
	}
	return ctx
}

func (tr *activationTracer) TraceQueryEnd(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if tr.queryErr == nil {
		tr.queryErr = data.Err
	}
}

func traceActivation(t *testing.T, d deps, tracer *activationTracer) deps {
	t.Helper()
	traced, err := pgstore.NewClient(t.Context(), d.pg.Pool.Config().ConnString(), pgstore.WithQueryTracer(tracer))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, traced.Close(context.Background())) })
	d.pg = traced
	return d
}

func TestActivationPreviewObservesRefreezeAndVerificationTogether(t *testing.T) {
	h, opts := activationHarness(t)
	opts.Quiesce = time.Hour
	opts.Page = 1
	pg := h.f.pg
	target, err := pg.Generation(t.Context(), activationTarget)
	require.NoError(t, err)
	require.NotNil(t, target)
	revisions, err := pg.DocumentRevisions(t.Context(), "", opts.MaxDocuments)
	require.NoError(t, err)
	require.Greater(t, len(revisions), 1)
	registerGeneration(t, pg, "g3", "pinecone", "quarry-g3", sourceModel)
	opens, embeds := h.stores.opened, h.embedCalls()
	const unverified = "the generation is staging; it must be verified"
	const freezeRequired = "writers are not frozen for this generation"
	const epochChanged = "the writer freeze changed during the corpus scan. Retry activation"
	settling := fmt.Sprintf("the freeze is still settling; wait %s after freezing", opts.Quiesce)
	for _, tt := range []struct {
		name, freeze                              string
		settled, reverify, midScan, sourceChanged bool
		reasons                                   []string
	}{
		{name: "early thaw", reasons: []string{unverified, freezeRequired}},
		{name: "early different freeze", freeze: "g3", reasons: []string{unverified, freezeRequired}},
		{name: "early settling refreeze", freeze: activationTarget, reasons: []string{unverified, settling}},
		{name: "early settled refreeze without verification", freeze: activationTarget, settled: true, reasons: []string{unverified}},
		{name: "mid-scan thaw", midScan: true, reasons: []string{unverified, freezeRequired}},
		{name: "mid-scan different freeze", midScan: true, freeze: "g3", reasons: []string{unverified, freezeRequired}},
		{name: "mid-scan settling refreeze", midScan: true, freeze: activationTarget, reasons: []string{unverified, settling}},
		{name: "mid-scan settled and reverified refreeze", midScan: true, freeze: activationTarget, settled: true, reverify: true, reasons: []string{epochChanged}},
		{name: "mid-scan active generation changes", midScan: true, freeze: activationTarget, settled: true, sourceChanged: true, reasons: []string{epochChanged}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			require.NoError(t, pg.ThawWriters(ctx))
			require.NoError(t, pg.FreezeWriters(ctx, activationTarget))
			require.NoError(t, pg.RecordVerification(ctx, activationTarget, target.CorpusDigest, target.Verification))
			setActivationFreeze(ctx, t, pg, activationTarget, false)

			transition := func() error {
				if tt.sourceChanged {
					// Change only the active-source identity to isolate it from the freeze epoch check.
					_, err := pg.Pool.Exec(ctx, `UPDATE vector_state SET active_generation='g3'`)
					return err
				}
				if err := pg.ThawWriters(ctx); err != nil || tt.freeze == "" {
					return err
				}
				if err := pg.FreezeWriters(ctx, tt.freeze); err != nil {
					return err
				}
				if tt.settled {
					if _, err := pg.Pool.Exec(ctx, `UPDATE vector_state SET frozen_at=NOW() - INTERVAL '3 hours'`); err != nil {
						return err
					}
				}
				if tt.reverify {
					return pg.RecordVerification(ctx, activationTarget, target.CorpusDigest, target.Verification)
				}
				return nil
			}
			var transitionErr error
			tracer := &activationTracer{before: func(_ context.Context, _ *pgx.Conn, kind string, count int) {
				if (!tt.midScan && kind == "readiness" && count == 1) || (tt.midScan && kind == "page" && count == 2) {
					// Commit through the other pool while activation is paused between statements.
					done := make(chan error, 1)
					go func() { done <- transition() }()
					transitionErr = <-done
				}
			}}
			d := traceActivation(t, h.deps, tracer)
			var out bytes.Buffer
			err := execute(ctx, d, opts, &out, time.Millisecond)
			require.NoError(t, transitionErr)
			if tt.midScan {
				require.Equal(t, 2, tracer.reads)
				require.Equal(t, len(revisions)+1, tracer.pages)
			} else {
				require.Equal(t, 1, tracer.reads)
				require.Zero(t, tracer.pages)
			}
			require.Zero(t, tracer.probes)
			require.ErrorIs(t, err, errActivationNotReady)
			active := pgstore.LegacyGeneration
			if tt.sourceChanged {
				active = "g3"
			}
			rep := activationReport(t, out.Bytes(), false, false, active, tt.freeze)
			require.Equal(t, rep.Frozen, rep.Quiesce.Frozen)
			require.Equal(t, tt.settled, rep.Quiesce.Settled)
			require.Equal(t, tt.reasons, rep.Reasons)
			require.Equal(t, opens, h.stores.opened)
			require.Equal(t, embeds, h.embedCalls())
			if tt.sourceChanged {
				return
			}
			targetState := pgstore.StateStaging
			if tt.reverify {
				targetState = pgstore.StateVerified
			}
			requireActivationState(ctx, t, pg, pgstore.LegacyGeneration, tt.freeze, targetState)
		})
	}
}

func TestActivationEligibleCorpusBudget(t *testing.T) {
	for _, page := range []int{1, 1000} {
		t.Run(fmt.Sprintf("page %d", page), func(t *testing.T) {
			ctx := t.Context()
			f := newTaggedFixture(t)
			h := newHarness(t, f, "postgres")
			h.attestSource()
			opens, embeds := h.stores.opened, h.embedCalls()
			registerGeneration(t, f.pg, activationTarget, "pinecone", "quarry-g2", sourceModel)
			require.NoError(t, f.pg.FreezeWriters(ctx, activationTarget))
			setActivationFreeze(ctx, t, f.pg, activationTarget, false)
			opts := defaultOptions()
			opts.Activate, opts.Generation, opts.Page, opts.MaxDocuments = true, activationTarget, page, 2
			receipt := json.RawMessage(`{"passed":true}`)
			digest, err := corpusDigest(ctx, f.pg, opts)
			require.NoError(t, err)
			require.NoError(t, f.pg.RecordVerification(ctx, activationTarget, digest, receipt))
			tracer := &activationTracer{}
			d := traceActivation(t, h.deps, tracer)
			var out bytes.Buffer
			require.NoError(t, execute(ctx, d, opts, &out, time.Millisecond))
			activationReport(t, out.Bytes(), true, false, pgstore.LegacyGeneration, activationTarget)
			require.Equal(t, 1, tracer.pages)
			require.Zero(t, tracer.probes)
			require.Equal(t, 2, tracer.reads)

			bare := newDoc("", "note", "Bare document")
			tagged := newTaggedDoc("alpha", "report", "Grant-owned tagged document")
			f.add(t, bare)
			f.add(t, tagged)
			tenant := uuid.NewString()
			assignDocTenant(t, f.pg, tagged, tenant, uuid.NewString())
			insertActivationJob(t, f.pg, "pending", false)
			revisions, err := f.pg.DocumentRevisions(ctx, "", 3)
			require.NoError(t, err)
			require.Len(t, revisions, 2)
			require.ElementsMatch(t, []string{bare.ID, tagged.ID}, []string{revisions[0].ID, revisions[1].ID})
			require.ElementsMatch(t, []string{"", tenant}, []string{revisions[0].TenantID, revisions[1].TenantID})
			require.NoError(t, f.pg.RecordVerification(ctx, activationTarget, "old digest", receipt))
			complete, err := corpusDigest(ctx, f.pg, opts)
			require.NoError(t, err)

			// Reverification on the same freeze can update the digest during the scan.
			var verificationErr error
			*tracer = activationTracer{before: func(ctx context.Context, _ *pgx.Conn, kind string, _ int) {
				if kind == "probe" {
					verificationErr = f.pg.RecordVerification(ctx, activationTarget, complete, receipt)
				}
			}}
			out.Reset()
			err = execute(ctx, d, opts, &out, time.Millisecond)
			require.NoError(t, verificationErr)
			require.NoError(t, err)
			activationReport(t, out.Bytes(), true, false, pgstore.LegacyGeneration, activationTarget)
			require.Equal(t, 2/min(page, 2), tracer.pages)
			require.Equal(t, 1, tracer.probes)
			require.Equal(t, 2, tracer.reads)

			for _, apply := range []bool{false, true} {
				opts.Apply, opts.MaxDocuments = apply, 1
				*tracer = activationTracer{}
				out.Reset()
				err = execute(ctx, d, opts, &out, time.Millisecond)
				require.ErrorContains(t, err, "activation exceeds -max-documents")
				require.ErrorIs(t, err, errActivationCorpusLimit)
				require.Empty(t, out.String())
				require.Equal(t, 1, tracer.pages)
				require.Equal(t, 1, tracer.probes)
				require.Equal(t, 1, tracer.reads)
			}

			opts.MaxDocuments = 2
			_, err = f.pg.Pool.Exec(ctx, `UPDATE documents SET doc_type='changed' WHERE id=$1`, bare.ID)
			require.NoError(t, err)
			for _, apply := range []bool{false, true} {
				opts.Apply = apply
				out.Reset()
				require.ErrorIs(t, execute(ctx, d, opts, &out, time.Millisecond), errActivationNotReady)
				rep := activationReport(t, out.Bytes(), false, false, pgstore.LegacyGeneration, activationTarget)
				require.Equal(t, []string{"the corpus changed since verification"}, rep.Reasons)
			}
			require.Equal(t, opens, h.stores.opened)
			require.Equal(t, embeds, h.embedCalls())
			requireActivationState(ctx, t, f.pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)
			target, err := f.pg.Generation(ctx, activationTarget)
			require.NoError(t, err)
			require.Equal(t, complete, target.CorpusDigest)

			_, err = f.pg.Pool.Exec(ctx, `UPDATE documents SET doc_type=$2 WHERE id=$1`, bare.ID, bare.DocType)
			require.NoError(t, err)
			out.Reset()
			require.NoError(t, execute(ctx, d, opts, &out, time.Millisecond))
			activationReport(t, out.Bytes(), true, true, activationTarget, activationTarget)
			require.Equal(t, opens+1, h.stores.opened)
			require.Equal(t, embeds, h.embedCalls())
			requireActivationState(ctx, t, f.pg, activationTarget, activationTarget, pgstore.StateActive)
		})
	}
}

func TestActivationReadyScanErrors(t *testing.T) {
	h, opts := activationHarness(t)
	opts.MaxDocuments = 4
	pg := h.f.pg
	for _, boundary := range []struct {
		kind  string
		count int
	}{
		{kind: "readiness", count: 1},
		{kind: "page", count: 1},
		{kind: "probe", count: 1},
		{kind: "readiness", count: 2},
	} {
		for _, failure := range []string{"42501", "42P01", "cancel"} {
			name := fmt.Sprintf("%s %d %s", boundary.kind, boundary.count, failure)
			t.Run(name, func(t *testing.T) {
				for _, apply := range []bool{false, true} {
					opts.Apply = apply
					ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
					opens, embeds := h.stores.opened, h.embedCalls()
					var injectionErr error
					tracer := &activationTracer{before: func(ctx context.Context, conn *pgx.Conn, kind string, count int) {
						if kind != boundary.kind || count != boundary.count {
							return
						}
						switch failure {
						case "cancel":
							cancel()
						case "42501":
							_, injectionErr = conn.PgConn().Exec(ctx, `SET ROLE pg_read_all_stats`).ReadAll()
						case "42P01":
							_, injectionErr = conn.PgConn().Exec(ctx, `SET search_path TO pg_catalog`).ReadAll()
						}
					}}
					d := traceActivation(t, h.deps, tracer)
					var out bytes.Buffer
					err := execute(ctx, d, opts, &out, time.Millisecond)
					cancel()
					require.NoError(t, injectionErr)
					if failure == "cancel" {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						var pgErr *pgconn.PgError
						require.ErrorAs(t, err, &pgErr)
						require.Equal(t, failure, pgErr.Code)
						require.ErrorIs(t, err, tracer.queryErr)
					}
					require.Empty(t, out.String())
					require.Equal(t, opens, h.stores.opened)
					require.Equal(t, embeds, h.embedCalls())
					fence, lockErr := pg.FenceGeneration(t.Context(), activationTarget)
					require.NoError(t, lockErr)
					fence.Close(t.Context())
					requireActivationState(t.Context(), t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)
				}
			})
		}
	}
}

func TestActivationAlreadyActiveBeforeAndAfterThaw(t *testing.T) {
	for _, store := range []string{"pinecone", "postgres"} {
		t.Run(store, func(t *testing.T) {
			ctx := t.Context()
			f := newFixture(t)
			h := newHarness(t, f, "postgres")
			h.attestSource()
			opts := defaultOptions()
			opts.Activate, opts.Apply, opts.Generation, opts.Quiesce = true, true, activationTarget, 0
			if store == "pinecone" {
				registerGeneration(t, f.pg, activationTarget, store, "quarry-g2", sourceModel)
			} else {
				pgTarget(t, f.pg, activationTarget)
			}
			require.NoError(t, f.pg.FreezeWriters(ctx, activationTarget))
			digest, err := corpusDigest(ctx, f.pg, opts)
			require.NoError(t, err)
			require.NoError(t, f.pg.RecordVerification(ctx, activationTarget, digest, json.RawMessage(`{"passed":true}`)))
			var out bytes.Buffer
			require.NoError(t, execute(ctx, h.deps, opts, &out, time.Millisecond))
			activationReport(t, out.Bytes(), true, true, activationTarget, activationTarget)
			opens, embeds := h.stores.opened, h.embedCalls()
			for _, freeze := range []string{activationTarget, ""} {
				if freeze == "" {
					require.NoError(t, f.pg.ThawWriters(ctx))
				}
				for _, apply := range []bool{true, false} {
					opts.Apply = apply
					out.Reset()
					err = execute(ctx, h.deps, opts, &out, time.Millisecond)
					require.ErrorIs(t, err, errActivationNotReady)
					reason := fmt.Sprintf("generation %q is already active", activationTarget)
					require.ErrorContains(t, err, reason)
					rep := activationReport(t, out.Bytes(), false, false, activationTarget, freeze)
					require.Equal(t, []string{reason}, rep.Reasons)
					require.Equal(t, opens, h.stores.opened)
					require.Equal(t, embeds, h.embedCalls())
					requireActivationState(ctx, t, f.pg, activationTarget, freeze, pgstore.StateActive)
				}
			}
		})
	}
}

func TestActivationPreflightUsesLiveLeaseBoundary(t *testing.T) {
	h, opts := activationHarness(t)
	pg := h.f.pg
	ctx := t.Context()
	insertActivationJob(t, pg, "pending", true)
	expired := insertActivationJob(t, pg, "running", false)
	insertActivationDeletion(ctx, t, pg, true)
	opens := h.stores.opened

	var out bytes.Buffer
	require.NoError(t, execute(ctx, h.deps, opts, &out, time.Millisecond))
	rep := activationReport(t, out.Bytes(), true, false, pgstore.LegacyGeneration, activationTarget)
	require.True(t, rep.Quiesce.Quiet())
	require.Equal(t, opens, h.stores.opened)
	requireActivationState(ctx, t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)

	maxWindow := time.Duration(math.MaxInt64)
	maxOpts, err := parse(t, "-activate", "-generation", activationTarget, "-quiesce", maxWindow.String())
	require.NoError(t, err)
	out.Reset()
	err = execute(ctx, h.deps, maxOpts, &out, time.Millisecond)
	require.ErrorIs(t, err, errActivationNotReady)
	rep = activationReport(t, out.Bytes(), false, false, pgstore.LegacyGeneration, activationTarget)
	require.False(t, rep.Quiesce.Settled)
	require.Equal(t, []string{fmt.Sprintf("the freeze is still settling; wait %s after freezing", maxWindow)}, rep.Reasons)

	_, err = pg.Pool.Exec(ctx, "UPDATE ingest_jobs SET lease_until=NOW() + INTERVAL '1 day' WHERE document_id=$1", expired)
	require.NoError(t, err)
	opts.Apply = true
	out.Reset()
	require.ErrorIs(t, execute(ctx, h.deps, opts, &out, time.Millisecond), errActivationNotReady)
	rep = activationReport(t, out.Bytes(), false, false, pgstore.LegacyGeneration, activationTarget)
	require.Equal(t, 1, rep.Quiesce.RunningJobs)
	require.Equal(t, opens, h.stores.opened)
	requireActivationState(ctx, t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)

	_, err = pg.Pool.Exec(ctx, "UPDATE ingest_jobs SET lease_until=NOW() - INTERVAL '1 hour' WHERE document_id=$1", expired)
	require.NoError(t, err)
	zero := opts
	zero.Apply, zero.Quiesce = false, 0
	out.Reset()
	require.NoError(t, execute(ctx, h.deps, zero, &out, time.Millisecond))
	activationReport(t, out.Bytes(), true, false, pgstore.LegacyGeneration, activationTarget)
	out.Reset()
	require.NoError(t, execute(ctx, h.deps, opts, &out, time.Millisecond))
	activationReport(t, out.Bytes(), true, true, activationTarget, activationTarget)
	require.Equal(t, opens+1, h.stores.opened)
	requireActivationState(ctx, t, pg, activationTarget, activationTarget, pgstore.StateActive)

	out.Reset()
	require.ErrorIs(t, execute(ctx, h.deps, opts, &out, time.Millisecond), errActivationNotReady)
	activationReport(t, out.Bytes(), false, false, activationTarget, activationTarget)
	require.Equal(t, opens+1, h.stores.opened)
	requireActivationState(ctx, t, pg, activationTarget, activationTarget, pgstore.StateActive)
}

func TestActivationRechecksLockedReadiness(t *testing.T) {
	h, opts := activationHarness(t)
	pg := h.f.pg
	opts.Apply = true
	t.Run("freeze resumes settling", func(t *testing.T) {
		tracer := &activationTracer{}
		d := traceActivation(t, h.deps, tracer)
		d.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
			setActivationFreeze(ctx, t, pg, activationTarget, true)
			return h.stores.open(ctx, spec)
		}
		var out bytes.Buffer
		err := execute(t.Context(), d, opts, &out, time.Millisecond)
		var refused *pgstore.ActivationQuiesceError
		require.ErrorAs(t, err, &refused)
		require.Equal(t, 2, tracer.reads)
		require.Equal(t, opts.Quiesce, refused.Window)
		rep := activationReport(t, out.Bytes(), false, false, pgstore.LegacyGeneration, activationTarget)
		require.Equal(t, refused.Quiesce, *rep.Quiesce)
		require.False(t, rep.Quiesce.Settled)
		require.Equal(t, []string{fmt.Sprintf("the freeze is still settling; wait %s after freezing", opts.Quiesce)}, rep.Reasons)
		requireActivationState(t.Context(), t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)
	})
	t.Run("late jobs and deletions", func(t *testing.T) {
		setActivationFreeze(t.Context(), t, pg, activationTarget, false)
		job := insertActivationJob(t, pg, "running", false)
		ids := make([]string, 25)
		tracer := &activationTracer{}
		d := traceActivation(t, h.deps, tracer)
		d.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
			_, err := pg.Pool.Exec(ctx, "UPDATE ingest_jobs SET lease_until=NOW() + INTERVAL '1 day' WHERE document_id=$1", job)
			require.NoError(t, err)
			for i := range ids {
				ids[i] = insertActivationDeletion(ctx, t, pg, false)
			}
			slices.Sort(ids)
			return h.stores.open(ctx, spec)
		}
		var out bytes.Buffer
		err := execute(t.Context(), d, opts, &out, time.Millisecond)
		var refused *pgstore.ActivationQuiesceError
		require.ErrorAs(t, err, &refused)
		require.Equal(t, 2, tracer.reads)
		rep := activationReport(t, out.Bytes(), false, false, pgstore.LegacyGeneration, activationTarget)
		require.Equal(t, refused.Quiesce, *rep.Quiesce)
		require.True(t, rep.Quiesce.Settled)
		require.Equal(t, 1, rep.Quiesce.RunningJobs)
		require.Equal(t, 25, rep.Quiesce.OpenDeletions)
		require.Equal(t, ids[:20], rep.Quiesce.OpenDeletionIDs)
		require.Equal(t, []string{
			"1 in-flight jobs must finish before activation",
			fmt.Sprintf("25 open deletions%s must finish before activation", pgstore.FormatOpenDeletions(25, ids[:20])),
		}, rep.Reasons)
		for _, omitted := range ids[20:] {
			require.NotContains(t, out.String(), omitted)
		}
		requireActivationState(t.Context(), t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)
	})
}

func TestActivationReadinessErrorPaths(t *testing.T) {
	h, opts := activationHarness(t)
	pg := h.f.pg
	opts.Apply = true
	t.Run("advisory refusal and output failure", func(t *testing.T) {
		ctx := t.Context()
		setActivationFreeze(ctx, t, pg, activationTarget, true)
		writeFailure := errors.New("test output unavailable")
		err := execute(ctx, h.deps, opts, activationErrorWriter{writeFailure}, time.Millisecond)
		require.ErrorIs(t, err, errActivationNotReady)
		require.ErrorIs(t, err, writeFailure)
		requireActivationState(ctx, t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)
		setActivationFreeze(ctx, t, pg, activationTarget, false)
	})
	t.Run("canceled before the transaction", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		d := h.deps
		d.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
			cancel()
			return h.stores.open(ctx, spec)
		}
		var out bytes.Buffer
		require.ErrorIs(t, execute(ctx, d, opts, &out, time.Millisecond), context.Canceled)
		activationReport(t, out.Bytes(), false, false, pgstore.LegacyGeneration, activationTarget)
		requireActivationState(t.Context(), t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)
	})
	t.Run("database error and output failure", func(t *testing.T) {
		const detail = "SYNTHETIC_DATABASE_DETAIL"
		const fallback = "activation was not confirmed; check the active generation before retrying"
		ctx := t.Context()
		_, err := pg.Pool.Exec(ctx, `CREATE FUNCTION test_reject_activation() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'test activation rejected' USING DETAIL = 'SYNTHETIC_DATABASE_DETAIL'; END; $$`)
		require.NoError(t, err)
		_, err = pg.Pool.Exec(ctx, `CREATE TRIGGER test_reject_activation BEFORE UPDATE OF active_generation ON vector_state
			FOR EACH ROW WHEN (OLD.active_generation IS DISTINCT FROM NEW.active_generation)
			EXECUTE FUNCTION test_reject_activation()`)
		require.NoError(t, err)
		var out bytes.Buffer
		err = execute(ctx, h.deps, opts, &out, time.Millisecond)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, detail, pgErr.Detail)
		rep := activationReport(t, out.Bytes(), false, false, pgstore.LegacyGeneration, activationTarget)
		require.Equal(t, []string{fallback}, rep.Reasons)
		require.NotContains(t, out.String(), detail)
		require.NotContains(t, out.String(), pgErr.Message)
		requireActivationState(ctx, t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)

		writeFailure := errors.New("test output unavailable")
		err = execute(ctx, h.deps, opts, activationErrorWriter{writeFailure}, time.Millisecond)
		require.ErrorIs(t, err, writeFailure)
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, detail, pgErr.Detail)
		requireActivationState(ctx, t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)
		_, err = pg.Pool.Exec(ctx, `DROP TRIGGER test_reject_activation ON vector_state`)
		require.NoError(t, err)
	})
	t.Run("retry with a healthy provider", func(t *testing.T) {
		ctx := t.Context()
		opens := h.stores.opened
		openFailure := errors.New("test index unavailable")
		d := h.deps
		d.open = func(context.Context, targetSpec) (vectorTarget, func(), string, error) {
			return nil, nil, "", openFailure
		}
		var out bytes.Buffer
		err := execute(ctx, d, opts, &out, time.Millisecond)
		require.ErrorIs(t, err, openFailure)
		require.ErrorContains(t, err, "open the target for activation")
		require.Empty(t, out.String())
		requireActivationState(ctx, t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)

		require.NoError(t, execute(ctx, h.deps, opts, &out, time.Millisecond))
		require.Equal(t, opens+1, h.stores.opened)
		activationReport(t, out.Bytes(), true, true, activationTarget, activationTarget)
		requireActivationState(ctx, t, pg, activationTarget, activationTarget, pgstore.StateActive)
	})
}

type activationErrorWriter struct{ err error }

func (w activationErrorWriter) Write([]byte) (int, error) { return 0, w.err }
