package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

type hostedReportWriteError struct{ cause error }

func (e *hostedReportWriteError) Error() string { return e.cause.Error() }
func (e *hostedReportWriteError) Unwrap() error { return e.cause }

type hostedReportWriter struct {
	accept   int
	attempts int
	written  []byte
	err      error
}

func (w *hostedReportWriter) Write(p []byte) (int, error) {
	w.attempts++
	n := min(w.accept, len(p))
	w.written = append(w.written, p[:n]...)
	return n, w.err
}

type hostedActivationQueryTracer func(string)

func (tr hostedActivationQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tr(data.SQL)
	return ctx
}

func (hostedActivationQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func hostedActivationState(t *testing.T, pg *pgstore.Client) string {
	t.Helper()
	var snapshot string
	err := pg.Pool.QueryRow(t.Context(), `SELECT jsonb_build_object(
		'state', (SELECT to_jsonb(s) FROM vector_state s),
		'registry', (SELECT jsonb_agg(g ORDER BY name) FROM vector_generations g),
		'manifests', (SELECT jsonb_agg(m ORDER BY revision_id) FROM hosted_vector_manifests m),
		'cleanups', (SELECT jsonb_agg(c ORDER BY revision_id) FROM hosted_vector_cleanups c)
	)::text`).Scan(&snapshot)
	require.NoError(t, err)
	return snapshot
}

func prepareHostedActivationReport(t *testing.T, pg *pgstore.Client) options {
	t.Helper()
	opts := defaultOptions()
	opts.Generation, opts.Activate, opts.Quiesce = activationTarget, true, 0
	registerGeneration(t, pg, opts.Generation, "postgres", pgstore.VectorTable(opts.Generation), sourceModel)
	require.NoError(t, pg.FreezeWriters(t.Context(), opts.Generation))
	digest, err := corpusDigest(t.Context(), pg, opts)
	require.NoError(t, err)
	require.NoError(t, pg.RecordVerification(t.Context(), opts.Generation, digest, json.RawMessage(`{"passed":true}`)))
	_, err = pg.Pool.Exec(t.Context(), insertCutoverManifest, uuid.NewString(), uuid.NewString(), "writing")
	require.NoError(t, err)
	return opts
}

func TestHostedActivationRefusalPreservesReportWriteError(t *testing.T) {
	bare := newDoc("", "note", "A completed bare document.")
	tagged := newTaggedDoc("alpha", "report", "A completed tagged document.")
	f := newTaggedFixture(t, bare, tagged)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	opts := prepareHostedActivationReport(t, f.pg)
	opts.MaxDocuments = 1
	revisions, err := f.pg.DocumentRevisions(t.Context(), "", opts.MaxDocuments)
	require.NoError(t, err)
	require.Len(t, revisions, 1)
	more, err := f.pg.HasDocumentsAfter(t.Context(), revisions[0].ID)
	require.NoError(t, err)
	require.True(t, more)

	pages, probes := 0, 0
	tracer := hostedActivationQueryTracer(func(sql string) {
		switch {
		case strings.HasPrefix(sql, "WITH eligible AS MATERIALIZED (SELECT d.* FROM documents"):
			pages++
		case strings.HasPrefix(sql, "SELECT EXISTS(SELECT 1 FROM documents d WHERE"):
			probes++
		}
	})
	traced, err := pgstore.NewClient(t.Context(), f.pg.Pool.Config().ConnString(), pgstore.WithQueryTracer(tracer))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, traced.Close(context.Background())) })
	d := h.deps
	d.pg = traced
	before := hostedActivationState(t, f.pg)
	providerOpens := h.stores.opened
	failure := &hostedReportWriteError{cause: io.ErrClosedPipe}
	for _, tc := range []struct {
		name   string
		apply  bool
		prefix string
	}{
		{name: "preview with zero bytes"},
		{name: "preview with a partial write", prefix: "{"},
		{name: "apply with zero bytes", apply: true},
		{name: "apply with a partial write", apply: true, prefix: "{"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts.Apply = tc.apply
			out := &hostedReportWriter{accept: len(tc.prefix), err: failure}
			err := execute(t.Context(), d, opts, out, time.Millisecond)
			require.ErrorIs(t, err, errActivationNotReady)
			require.ErrorIs(t, err, failure)
			require.ErrorIs(t, err, io.ErrClosedPipe)
			var typed *hostedReportWriteError
			require.ErrorAs(t, err, &typed)
			require.Same(t, failure, typed)
			require.Equal(t, 1, out.attempts)
			require.Equal(t, tc.prefix, string(out.written))
			require.Zero(t, pages, "activation must exit before reading corpus revisions")
			require.Zero(t, probes, "activation must exit before probing for more documents")
			require.Equal(t, providerOpens, h.stores.opened)
			require.Equal(t, before, hostedActivationState(t, f.pg))

			if tc.apply {
				var retry bytes.Buffer
				require.ErrorIs(t, execute(t.Context(), d, opts, &retry, time.Millisecond), errActivationNotReady,
					"a same-target apply must reacquire the generation fence and reach the hosted check")
				require.Equal(t, before, hostedActivationState(t, f.pg))
			}
		})
	}
}

func TestHostedActivationReportSuccessKeepsRefusal(t *testing.T) {
	pg := vectorDB(t)
	opts := prepareHostedActivationReport(t, pg)
	restricted, role := newHostedCutoverReindexRole(t, pg)
	_, err := pg.Pool.Exec(t.Context(), replaceReindexRoleSQL(
		publishedDeploymentSQL(t, "GRANT USAGE ON SCHEMA public TO quarry_reindex"), role, ""))
	require.NoError(t, err)
	d := deps{pg: restricted}
	for _, apply := range []bool{false, true} {
		opts.Apply = apply
		var out bytes.Buffer
		require.Same(t, errActivationNotReady, execute(t.Context(), d, opts, &out, time.Millisecond), "apply=%t", apply)
		require.JSONEq(t, `{
			"action": "activate",
			"generation": "g2",
			"applied": false,
			"active_generation": "legacy",
			"frozen": true,
			"freeze_generation": "g2",
			"ready": false,
			"reasons": ["drain hosted work on the original source before activation (writing: 1, active: 0, retired: 0, cleanups: 0)"],
			"hosted_manifest_cutover": {"writing": 1, "active": 0, "retired": 0, "cleanups": 0}
		}`, out.String(), "apply=%t", apply)
	}
}

func TestHostedActivationLookupErrorsPrecedeReport(t *testing.T) {
	pg := vectorDB(t)
	opts := prepareHostedActivationReport(t, pg)
	for _, tc := range []struct {
		name, revokeSQL, message string
	}{
		{
			name:      "count denied",
			revokeSQL: "REVOKE EXECUTE ON FUNCTION public.hosted_manifest_cutover_counts() FROM ",
			message:   "count hosted manifest work for cutover",
		},
		{
			name:      "metadata read denied",
			revokeSQL: "REVOKE SELECT ON public.vector_state FROM ",
			message:   "read vector state",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			restricted, role := newHostedCutoverReindexRole(t, pg)
			_, err := pg.Pool.Exec(ctx, replaceReindexRoleSQL(
				publishedDeploymentSQL(t, "GRANT USAGE ON SCHEMA public TO quarry_reindex"), role, ""))
			require.NoError(t, err)
			_, err = pg.Pool.Exec(ctx, tc.revokeSQL+pgx.Identifier{role}.Sanitize())
			require.NoError(t, err)
			for _, apply := range []bool{false, true} {
				opts.Apply = apply
				out := &hostedReportWriter{err: io.ErrClosedPipe}
				err := execute(ctx, deps{pg: restricted}, opts, out, time.Millisecond)
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr, "apply=%t", apply)
				require.Equal(t, "42501", pgErr.Code)
				require.ErrorContains(t, err, tc.message)
				require.NotErrorIs(t, err, errActivationNotReady)
				require.NotErrorIs(t, err, io.ErrClosedPipe)
				require.Zero(t, out.attempts)
			}
		})
	}
	t.Run("count canceled", func(t *testing.T) {
		for _, apply := range []bool{false, true} {
			opts.Apply = apply
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			countQueries := 0
			tracer := hostedActivationQueryTracer(func(sql string) {
				if strings.Contains(sql, "FROM hosted_manifest_cutover_counts()") {
					countQueries++
					cancel()
				}
			})
			traced, err := pgstore.NewClient(ctx, pg.Pool.Config().ConnString(), pgstore.WithQueryTracer(tracer))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, traced.Close(context.Background())) })
			out := &hostedReportWriter{err: io.ErrClosedPipe}
			err = execute(ctx, deps{pg: traced}, opts, out, time.Millisecond)
			require.Equal(t, 1, countQueries, "apply=%t", apply)
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorContains(t, err, "count hosted manifest work for cutover")
			require.NotErrorIs(t, err, errActivationNotReady)
			require.NotErrorIs(t, err, io.ErrClosedPipe)
			require.Zero(t, out.attempts)
		}
	})
}
