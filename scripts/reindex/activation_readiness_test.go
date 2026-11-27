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

func TestActivationPreflightQuiescence(t *testing.T) {
	h, opts := activationHarness(t)
	pg := h.f.pg
	job := insertActivationJob(t, pg, "running", false)
	id := insertActivationDeletion(t.Context(), t, pg, true)
	opens, embeds := h.stores.opened, h.embedCalls()
	settling := fmt.Sprintf("the freeze is still settling; wait %s after freezing", opts.Quiesce)
	const running = "1 in-flight jobs must finish before activation"
	deleting := fmt.Sprintf("1 open deletions (%s) must finish before activation", id)
	const freezeRequired = "writers are not frozen for this generation"
	for _, tt := range []struct {
		name, freeze         string
		settling, live, open bool
		reasons              []string
	}{
		{name: "settling", freeze: activationTarget, settling: true, reasons: []string{settling}},
		{name: "live lease", freeze: activationTarget, live: true, reasons: []string{running}},
		{name: "settling and live lease", freeze: activationTarget, settling: true, live: true, reasons: []string{settling, running}},
		{name: "open deletion", freeze: activationTarget, open: true, reasons: []string{deleting}},
		{name: "all blockers", freeze: activationTarget, settling: true, live: true, open: true, reasons: []string{settling, running, deleting}},
		{name: "no freeze", reasons: []string{freezeRequired}},
		{name: "wrong freeze", freeze: "g3", settling: true, reasons: []string{freezeRequired}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			setActivationFreeze(ctx, t, pg, tt.freeze, tt.settling)
			_, err := pg.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until =
				CASE WHEN $2 THEN NOW() + INTERVAL '1 day' ELSE NOW() - INTERVAL '1 hour' END WHERE document_id=$1`, job, tt.live)
			require.NoError(t, err)
			_, err = pg.Pool.Exec(ctx, "UPDATE document_deletions SET finalized=NOT $2, held=NOT $2 WHERE document_id=$1", id, tt.open)
			require.NoError(t, err)
			for _, apply := range []bool{false, true} {
				opts.Apply = apply
				var out bytes.Buffer
				err = execute(ctx, h.deps, opts, &out, time.Millisecond)
				require.ErrorIs(t, err, errActivationNotReady)
				for _, reason := range tt.reasons {
					require.ErrorContains(t, err, reason)
				}
				rep := activationReport(t, out.Bytes(), false, false, pgstore.LegacyGeneration, tt.freeze)
				require.Equal(t, tt.reasons, rep.Reasons)
				require.Equal(t, opens, h.stores.opened)
				require.Equal(t, embeds, h.embedCalls())
				requireActivationState(ctx, t, pg, pgstore.LegacyGeneration, tt.freeze, pgstore.StateVerified)
			}
		})
	}
}

type activationQuiesceTracer struct {
	beforeRead func(context.Context)
}

func (tr activationQuiesceTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "FROM ingest_jobs") && strings.Contains(data.SQL, "FROM vector_state") {
		tr.beforeRead(ctx)
	}
	return ctx
}

func (activationQuiesceTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestActivationPreviewObservesRefreezeAndVerificationTogether(t *testing.T) {
	h, opts := activationHarness(t)
	opts.Quiesce = time.Hour
	pg := h.f.pg
	target, err := pg.Generation(t.Context(), activationTarget)
	require.NoError(t, err)
	require.NotNil(t, target)
	registerGeneration(t, pg, "g3", "pinecone", "quarry-g3", sourceModel)
	opens, embeds := h.stores.opened, h.embedCalls()
	const unverified = "the generation is staging; it must be verified"
	const changed = "the corpus changed since verification"
	const freezeRequired = "writers are not frozen for this generation"
	settling := fmt.Sprintf("the freeze is still settling; wait %s after freezing", opts.Quiesce)
	for _, tt := range []struct {
		name, freeze string
		settled      bool
		reasons      []string
	}{
		{name: "thaw", reasons: []string{unverified, changed, freezeRequired}},
		{name: "different freeze", freeze: "g3", reasons: []string{unverified, changed, freezeRequired}},
		{name: "same freeze target starts settling again", freeze: activationTarget, reasons: []string{unverified, changed, settling}},
		{name: "same freeze target settles without verification", freeze: activationTarget, settled: true, reasons: []string{unverified, changed}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			require.NoError(t, pg.ThawWriters(ctx))
			require.NoError(t, pg.FreezeWriters(ctx, activationTarget))
			require.NoError(t, pg.RecordVerification(ctx, activationTarget, target.CorpusDigest, target.Verification))
			setActivationFreeze(ctx, t, pg, activationTarget, false)

			var transitionErr error
			reads := 0
			// Pause immediately before reading quiescence and commit the transition through the other pool.
			tracer := activationQuiesceTracer{beforeRead: func(ctx context.Context) {
				reads++
				transitionErr = pg.ThawWriters(ctx)
				if transitionErr == nil && tt.freeze != "" {
					transitionErr = pg.FreezeWriters(ctx, tt.freeze)
				}
				if transitionErr == nil && tt.settled {
					_, transitionErr = pg.Pool.Exec(ctx, `UPDATE vector_state SET frozen_at=NOW() - INTERVAL '2 hours'`)
				}
			}}
			traced, connectErr := pgstore.NewClient(ctx, pg.Pool.Config().ConnString(), pgstore.WithQueryTracer(tracer))
			require.NoError(t, connectErr)
			t.Cleanup(func() { require.NoError(t, traced.Close(context.Background())) })
			d := h.deps
			d.pg = traced
			var out bytes.Buffer
			err := execute(ctx, d, opts, &out, time.Millisecond)
			require.Equal(t, 1, reads)
			require.NoError(t, transitionErr)
			require.ErrorIs(t, err, errActivationNotReady)
			rep := activationReport(t, out.Bytes(), false, false, pgstore.LegacyGeneration, tt.freeze)
			require.Equal(t, rep.Frozen, rep.Quiesce.Frozen)
			require.Equal(t, tt.settled, rep.Quiesce.Settled)
			require.Equal(t, tt.reasons, rep.Reasons)
			require.Equal(t, opens, h.stores.opened)
			require.Equal(t, embeds, h.embedCalls())
			requireActivationState(ctx, t, pg, pgstore.LegacyGeneration, tt.freeze, pgstore.StateStaging)
		})
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

func TestActivationReportsLockedQuiescenceRefusal(t *testing.T) {
	h, opts := activationHarness(t)
	pg := h.f.pg
	opts.Apply = true
	t.Run("freeze resumes settling", func(t *testing.T) {
		d := h.deps
		d.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
			setActivationFreeze(ctx, t, pg, activationTarget, true)
			return h.stores.open(ctx, spec)
		}
		var out bytes.Buffer
		err := execute(t.Context(), d, opts, &out, time.Millisecond)
		var refused *pgstore.ActivationQuiesceError
		require.ErrorAs(t, err, &refused)
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
		d := h.deps
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
	t.Run("target open failure", func(t *testing.T) {
		openFailure := errors.New("test index unavailable")
		d := h.deps
		d.open = func(context.Context, targetSpec) (vectorTarget, func(), string, error) {
			return nil, nil, "", openFailure
		}
		var out bytes.Buffer
		err := execute(t.Context(), d, opts, &out, time.Millisecond)
		require.ErrorIs(t, err, openFailure)
		require.ErrorContains(t, err, "open the target for activation")
		require.Empty(t, out.String())
		requireActivationState(t.Context(), t, pg, pgstore.LegacyGeneration, activationTarget, pgstore.StateVerified)
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
	})
}

type activationErrorWriter struct{ err error }

func (w activationErrorWriter) Write([]byte) (int, error) { return 0, w.err }
