package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stage runs a staging invocation into a Pinecone index.
func (h *harness) stagePinecone(generation, index, model string, calls int) map[string]any {
	h.t.Helper()
	report, err := h.run(options{Generation: generation, Profile: h.profileFile(generation+".json", testProfile(model)), TargetStore: "pinecone", TargetIndex: index, Apply: true, MaxEmbedCalls: calls})
	require.NoError(h.t, err)
	return report
}

func (h *harness) attestSource() {
	h.t.Helper()
	_, err := h.run(options{Attest: true, Apply: true, Profile: h.profileFile("source.json", testProfile(sourceModel))})
	require.NoError(h.t, err)
}

func TestVerifyAcceptsAStagedCopyAndRecordsItOnlyUnderTheFreeze(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	h.stagePinecone("g2", "quarry-g2", sourceModel, 0)

	report, err := h.run(verifyOptions("g2"))
	require.NoError(t, err, "%v", report)
	require.Equal(t, true, report["passed"])
	coverage := report["coverage"].(map[string]any)
	require.EqualValues(t, 1, coverage["key_coverage"])
	require.EqualValues(t, 1, coverage["profile_coverage"])
	require.Equal(t, true, report["unexpected"].(map[string]any)["listing_checked"])
	require.EqualValues(t, 0, report["unexpected"].(map[string]any)["out_of_scope_records"])
	sample := report["sample"].(map[string]any)
	require.EqualValues(t, 6, sample["queries"])
	require.EqualValues(t, 0, sample["forbidden_hits"])
	require.EqualValues(t, 1, sample["mean_recall"], "a copied vector ranks like its source")
	require.EqualValues(t, 1, sample["self_hit_rate"])

	// Recording a verdict needs the freeze: only a quiescent corpus has a final result.
	apply := verifyOptions("g2")
	apply.Apply = true
	_, err = h.run(apply)
	require.ErrorContains(t, err, "frozen")
	row, getErr := f.pg.Generation(t.Context(), "g2")
	require.NoError(t, getErr)
	require.Equal(t, pgstore.StateStaging, row.State)

	freeze := options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0}
	_, err = h.run(freeze)
	require.NoError(t, err)
	report, err = h.run(apply)
	require.NoError(t, err)
	require.Equal(t, true, report["recorded"])
	row, err = f.pg.Generation(t.Context(), "g2")
	require.NoError(t, err)
	require.Equal(t, pgstore.StateVerified, row.State)
	require.Equal(t, report["corpus_digest"], row.CorpusDigest)
}

func TestVerifyFailsClosedForEveryIncompleteTarget(t *testing.T) {
	for name, tc := range map[string]struct {
		damage  func(t *testing.T, f *fixture, index *pcstore.MemoryIndex)
		message string
	}{
		"missing vector": {func(t *testing.T, f *fixture, index *pcstore.MemoryIndex) {
			require.NoError(t, index.DeleteVectorsByID(t.Context(), []string{string(f.chunks[f.docs[0].ID][1].ID)}))
		}, "missing from the target"},
		"unexpected record": {func(t *testing.T, f *fixture, index *pcstore.MemoryIndex) {
			client := pcstore.NewClientForIndex(index, testConfig(), "")
			stray := f.chunks[f.docs[0].ID][0]
			stray.ID = "00000000000000000000000000000001"
			stray.Embedding = embedWith(sourceModel, "stray")
			stray.Metadata = map[string]any{"doc_type": "report"}
			require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{stray}))
		}, "outside the expected set"},
		"wrong text": {func(t *testing.T, f *fixture, index *pcstore.MemoryIndex) {
			client := pcstore.NewClientForIndex(index, testConfig(), "")
			chunk := f.chunks[f.docs[0].ID][0]
			chunk.Text = "different text"
			chunk.Embedding = embedWith(sourceModel, chunk.Text)
			chunk.Metadata = map[string]any{"doc_type": "report", "collection": "alpha"}
			require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{chunk}))
		}, "differ from PostgreSQL"},
		"wrong scope": {func(t *testing.T, f *fixture, index *pcstore.MemoryIndex) {
			client := pcstore.NewClientForIndex(index, testConfig(), "")
			chunk := f.chunks[f.docs[0].ID][0]
			chunk.Embedding = embedWith(sourceModel, chunk.Text)
			chunk.Metadata = map[string]any{"doc_type": "report", "collection": "beta"}
			require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{chunk}))
		}, "differ from PostgreSQL"},
		"zero vector": {func(t *testing.T, f *fixture, index *pcstore.MemoryIndex) {
			client := pcstore.NewClientForIndex(index, testConfig(), "")
			chunk := f.chunks[f.docs[0].ID][0]
			chunk.Embedding = make([]float32, testDims)
			chunk.Metadata = map[string]any{"doc_type": "report", "collection": "alpha"}
			require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{chunk}))
		}, "wrong dimension or an invalid value"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			h := newHarness(t, f, "postgres")
			h.attestSource()
			h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
			tc.damage(t, f, h.stores.index("quarry-g2"))
			report, err := h.run(verifyOptions("g2"))
			require.ErrorIs(t, err, errNotVerified)
			require.Equal(t, false, report["passed"])
			require.Contains(t, fmt.Sprint(report["failures"]), tc.message)
			// A failed verification is never recorded.
			_, err = h.run(options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
			require.NoError(t, err)
			apply := verifyOptions("g2")
			apply.Apply = true
			_, err = h.run(apply)
			require.ErrorIs(t, err, errNotVerified)
			row, getErr := f.pg.Generation(t.Context(), "g2")
			require.NoError(t, getErr)
			require.Equal(t, pgstore.StateStaging, row.State)
		})
	}
}

func TestVerifyRejectsATargetWhoseDimensionDiffersFromItsProfile(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	_, err := pgstore.CreateGenerationVectorStore(t.Context(), f.pg, testDims+1, "g2")
	require.NoError(t, err)
	_, err = f.pg.RegisterGeneration(t.Context(), pgstore.Generation{Name: "g2", Store: "postgres", Target: pgstore.VectorTable("g2"), Profile: testProfile(sourceModel), Origin: pgstore.OriginRecorded})
	require.NoError(t, err)
	_, err = h.run(verifyOptions("g2"))
	require.ErrorContains(t, err, "dimension")
}

func TestVerifyRequiresARegisteredInactiveGenerationAndAMatchingProfile(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	_, err := h.run(verifyOptions("g2"))
	require.ErrorContains(t, err, "not registered")
	h.attestSource()
	h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
	other := verifyOptions("g2")
	other.Profile = h.profileFile("other.json", testProfile("model-b"))
	_, err = h.run(other)
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
	_, err = f.pg.Pool.Exec(t.Context(), `UPDATE vector_state SET active_generation='g2'`)
	require.NoError(t, err)
	_, err = h.run(verifyOptions("g2"))
	require.ErrorContains(t, err, "active")
}

// scriptedReader returns fixed query results so thresholds can be exercised directly.
type scriptedReader struct {
	vectorReader
	results func(vector []float32, filter domain.VectorFilter) []domain.ScoredChunk
	records func(ids []string) map[string]domain.Chunk
}

func (s scriptedReader) QueryFiltered(ctx context.Context, v []float32, k int, f domain.VectorFilter) ([]domain.ScoredChunk, error) {
	if s.results == nil {
		return s.vectorReader.QueryFiltered(ctx, v, k, f)
	}
	return s.results(v, f), nil
}

func (s scriptedReader) Fetch(ctx context.Context, ids []string) (map[string]domain.Chunk, error) {
	if s.records != nil {
		return s.records(ids), nil
	}
	return s.vectorReader.Fetch(ctx, ids)
}

type listingUnsupported struct{ vectorReader }

func (listingUnsupported) ListIDs(context.Context, string, int) (ids []string, next string, err error) {
	return nil, "", fmt.Errorf("%w: pod index", pcstore.ErrListingUnsupported)
}

// verifierOver stages a complete copy into PostgreSQL and returns a verifier whose readers can be wrapped.
func verifierOver(t *testing.T, f *fixture) (*verifier, *pgstore.VectorStore) {
	t.Helper()
	target := pgTarget(t, f.pg, "g2")
	s := f.stager(t, "g2", target, sourceModel, f.legacy, nil)
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete)
	opts := verifyOptions("g2")
	base := defaultOptions()
	base.Generation, base.Verify, base.SampleSize, base.TopK, base.ScopeDocuments, base.MinRecall, base.MaxP95 = opts.Generation, true, opts.SampleSize, opts.TopK, opts.ScopeDocuments, opts.MinRecall, opts.MaxP95
	return &verifier{corpus: f.pg, ledger: f.pg, target: target, source: f.legacy, generation: "g2", profile: testProfile(sourceModel), sourceProfile: testProfile(sourceModel), opts: base, now: time.Now}, target
}

func TestForbiddenHitsRecallAndLatencyAreEnforced(t *testing.T) {
	f := newFixture(t)
	v, target := verifierOver(t, f)
	rep, err := v.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Passed, "%v", rep.Failures)

	t.Run("forbidden hit", func(t *testing.T) {
		v := *v
		outsider := domain.ScoredChunk{Chunk: domain.Chunk{ID: "outsider", DocumentID: "another-document", Metadata: map[string]any{"collection": "beta"}}}
		v.target = scriptedReader{vectorReader: target, results: func(vector []float32, filter domain.VectorFilter) []domain.ScoredChunk {
			hits, _ := target.QueryFiltered(t.Context(), vector, 5, filter)
			return append(hits, outsider)
		}}
		rep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.Positive(t, rep.Sample.ForbiddenHits)
		require.Contains(t, fmt.Sprint(rep.Failures), "outside the query scope")
	})
	t.Run("low recall", func(t *testing.T) {
		v := *v
		v.opts.MinRecall = 1
		v.target = scriptedReader{vectorReader: target, results: func([]float32, domain.VectorFilter) []domain.ScoredChunk { return nil }}
		rep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.Zero(t, rep.Sample.MeanRecall)
		require.Contains(t, fmt.Sprint(rep.Failures), "below the declared")
	})
	t.Run("slow", func(t *testing.T) {
		v := *v
		v.opts.MaxP95 = time.Millisecond
		clock := time.Unix(0, 0)
		v.now = func() time.Time { clock = clock.Add(50 * time.Millisecond); return clock }
		rep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.Contains(t, fmt.Sprint(rep.Failures), "p95 latency")
	})
	t.Run("listing unsupported", func(t *testing.T) {
		v := *v
		v.target = listingUnsupported{vectorReader: target}
		rep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.False(t, rep.Unexpected.Checked)
		require.Contains(t, fmt.Sprint(rep.Failures), "cannot list its keys")
	})
	t.Run("short vectors", func(t *testing.T) {
		v := *v
		v.target = scriptedReader{vectorReader: target, records: func(ids []string) map[string]domain.Chunk {
			got, _ := target.Fetch(t.Context(), ids)
			for id, c := range got {
				c.Embedding = c.Embedding[:2]
				got[id] = c
			}
			return got
		}}
		rep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.InDelta(t, 0, rep.Coverage.ProfileCoverage, 1e-9)
		require.Contains(t, fmt.Sprint(rep.Failures), "wrong dimension")
	})
	t.Run("sample too small", func(t *testing.T) {
		v := *v
		v.source = scriptedReader{vectorReader: f.legacy, records: func([]string) map[string]domain.Chunk { return map[string]domain.Chunk{} }}
		rep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.Equal(t, rep.Sample.Requested, rep.Sample.Skipped)
		require.Contains(t, fmt.Sprint(rep.Failures), "sample queries ran")
	})
	t.Run("bounded", func(t *testing.T) {
		v := *v
		v.opts.MaxChunks = 2
		rep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.Contains(t, fmt.Sprint(rep.Failures), "cannot be proven")
	})
}

func TestVerificationQueriesAreAlwaysScoped(t *testing.T) {
	f := newFixture(t)
	v, target := verifierOver(t, f)
	seen := 0
	v.target = scriptedReader{vectorReader: target, results: func(vector []float32, filter domain.VectorFilter) []domain.ScoredChunk {
		seen++
		require.NotNil(t, filter.DocumentIDs, "an unrestricted query is never a validation shortcut")
		require.NotEmpty(t, filter.DocumentIDs)
		require.LessOrEqual(t, len(filter.DocumentIDs), v.opts.ScopeDocuments)
		hits, _ := target.QueryFiltered(t.Context(), vector, 5, filter)
		return hits
	}}
	rep, err := v.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Passed, "%v", rep.Failures)
	require.Equal(t, rep.Sample.Queries, seen)

	chunk := sampleChunk{id: "c", doc: "d", collection: ""}
	scope := scopeFor(chunk, []string{"d", "x", "y", "z"}, 2)
	require.Len(t, scope, 2)
	require.Equal(t, "d", scope[0])
	require.Equal(t, scope, scopeFor(chunk, []string{"d", "x", "y", "z"}, 2), "the scope is deterministic")
	require.Equal(t, []string{"d"}, scopeFor(chunk, []string{"d"}, 5))
}

func TestVerificationSampleIsDeterministicForACorpus(t *testing.T) {
	f := newFixture(t)
	v, _ := verifierOver(t, f)
	first, err := v.run(t.Context())
	require.NoError(t, err)
	second, err := v.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, first.CorpusDigest, second.CorpusDigest)
	require.Equal(t, first.Sample.MeanRecall, second.Sample.MeanRecall)
	_, err = f.pg.Pool.Exec(t.Context(), `UPDATE chunks SET text='edited' WHERE document_id=$1`, f.docs[3].ID)
	require.NoError(t, err)
	third, err := v.run(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, first.CorpusDigest, third.CorpusDigest, "any content change changes the digest")
	require.False(t, third.Passed, "the edited chunk has no current checkpoint")
}

func TestFreezeActivateAndThawLifecycle(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	h.stagePinecone("g2", "quarry-g2", sourceModel, 0)

	// Previews change nothing.
	report, err := h.run(options{Generation: "g2", Freeze: true, Quiesce: time.Minute})
	require.NoError(t, err)
	require.Equal(t, false, report["applied"])
	require.Equal(t, false, report["frozen"])

	report, err = h.run(options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	require.Equal(t, true, report["frozen"])
	require.Equal(t, "g2", report["freeze_generation"])
	require.ErrorIs(t, f.pg.WritersOpen(t.Context()), domain.ErrWritersFrozen)

	// Activation needs a verified generation.
	report, err = h.run(options{Generation: "g2", Activate: true, Quiesce: 0})
	require.ErrorContains(t, err, "not ready")
	require.Contains(t, fmt.Sprint(report["reasons"]), "not verified")

	apply := verifyOptions("g2")
	apply.Apply = true
	_, err = h.run(apply)
	require.NoError(t, err)

	report, err = h.run(options{Generation: "g2", Activate: true, Quiesce: 0})
	require.NoError(t, err)
	require.Equal(t, true, report["ready"])
	require.Equal(t, "legacy", report["active_generation"], "a preview does not activate")

	// A change after verification blocks activation.
	extra := newDoc("alpha", "note", "Completed after the verification ran.")
	f.add(t, extra)
	report, err = h.run(options{Generation: "g2", Activate: true, Quiesce: 0})
	require.Error(t, err)
	require.Contains(t, fmt.Sprint(report["reasons"]), "corpus changed")
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.ErrorContains(t, err, "corpus changed")
	_, err = h.run(apply)
	require.ErrorIs(t, err, errNotVerified, "the new document has no checkpoint yet")

	// Catch up under the freeze (the live vector of the new document is reused), verify again, then activate.
	caught := h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
	require.EqualValues(t, 1, caught["staged_documents"])
	require.EqualValues(t, len(extra.Texts), caught["copied_chunks"])
	_, err = h.run(apply)
	require.NoError(t, err)
	report, err = h.run(options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	require.Equal(t, true, report["applied"])
	require.Equal(t, "g2", report["active_generation"])

	// Replicas are pinned: the legacy configuration can no longer write.
	require.ErrorIs(t, f.pg.WritersOpen(t.Context()), domain.ErrGenerationMismatch)
	report, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)
	require.Equal(t, false, report["frozen"])
	replica, err := pgstore.NewClient(t.Context(), f.pg.Pool.Config().ConnString())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, replica.Close(context.Background())) })
	require.NoError(t, replica.PinVectorGeneration(t.Context(), "g2", "pinecone", "quarry-g2", "quarry-g2-host", testProfile(sourceModel)))
	require.NoError(t, replica.WritersOpen(t.Context()))
}

const insertCutoverManifest = `INSERT INTO hosted_vector_manifests
	(revision_id, document_id, incarnation, attempt, id_prefix, vector_ids, chunk_ids, state)
	VALUES ($1, $2, $2, 1, 'cutover', '{}', '{}', $3)`

func TestReindexActivateReportsHostedManifestWork(t *testing.T) {
	f := &fixture{pg: vectorDB(t)}
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	ctx := t.Context()
	opts := defaultOptions()
	opts.Generation, opts.Activate, opts.Quiesce = "g2", true, 0
	registerGeneration(t, f.pg, opts.Generation, "postgres", pgstore.VectorTable(opts.Generation), sourceModel)
	require.NoError(t, f.pg.FreezeWriters(ctx, opts.Generation))
	digest, err := corpusDigest(ctx, f.pg, opts)
	require.NoError(t, err)
	verify := func() {
		require.NoError(t, f.pg.RecordVerification(ctx, opts.Generation, digest, json.RawMessage(`{"passed":true}`)))
	}
	verify()
	profile := h.profileFile("pg.json", testProfile(sourceModel))
	inventory := options{Generation: opts.Generation, Profile: profile, TargetStore: "postgres"}
	requireCounts := func(rep map[string]any, writing, active, retired, cleanups float64) {
		t.Helper()
		require.Equal(t, map[string]any{"writing": writing, "active": active, "retired": retired, "cleanups": cleanups}, rep["hosted_manifest_cutover"])
	}
	seed := func(state string) string {
		t.Helper()
		rev := uuid.NewString()
		_, insertErr := f.pg.Pool.Exec(ctx, insertCutoverManifest, rev, uuid.NewString(), state)
		require.NoError(t, insertErr)
		return rev
	}
	writing, active, retired := seed("writing"), seed("active"), seed("retired")
	_, err = f.pg.Pool.Exec(ctx, `INSERT INTO hosted_vector_cleanups
		(revision_id, document_id, incarnation, id_prefix, vector_ids)
		SELECT revision_id, document_id, incarnation, id_prefix, vector_ids
		FROM hosted_vector_manifests WHERE revision_id=$1`, retired)
	require.NoError(t, err)
	providerOpens := h.stores.opened
	rep, err := h.run(inventory)
	require.NoError(t, err)
	require.Equal(t, providerOpens, h.stores.opened, "inventory needs no provider")
	requireCounts(rep, 1, 1, 1, 1)
	require.Empty(t, rep["blockers"], "unattached manifest counts are advisory during staging")
	for _, apply := range []bool{false, true} {
		opts.Apply = apply
		providerOpens = h.stores.opened
		rep, err = h.run(opts)
		require.ErrorContains(t, err, "not ready")
		require.Equal(t, providerOpens, h.stores.opened, "blocked activation needs no provider")
		requireCounts(rep, 1, 1, 1, 1)
		require.Equal(t, false, rep["ready"])
		require.Equal(t, false, rep["applied"])
		require.Equal(t, true, rep["frozen"])
		require.Equal(t, "legacy", rep["active_generation"])
		require.Equal(t, []any{"drain hosted work on the original source before activation (writing: 1, active: 1, retired: 1, cleanups: 1)"}, rep["reasons"])
	}
	_, err = f.pg.Pool.Exec(ctx, `DELETE FROM hosted_vector_cleanups WHERE revision_id=$1`, retired)
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(ctx, `DELETE FROM hosted_vector_manifests WHERE revision_id=ANY($1::uuid[])`, []string{writing, active, retired})
	require.NoError(t, err)

	// The journal is invisible to the advisory query while its transaction holds
	// SHARE. Observe the attempted switch waiting before publishing the journal.
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	holder, err := f.pg.Pool.Begin(waitCtx)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback(ctx) }()
	pid := holder.Conn().PgConn().PID()
	late := uuid.NewString()
	_, err = holder.Exec(waitCtx, insertCutoverManifest, late, uuid.NewString(), "writing")
	require.NoError(t, err)
	opts.Apply = true
	providerOpens = h.stores.opened
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- execute(waitCtx, h.deps, opts, &out, time.Millisecond)
	}()
	defer func() {
		cancel()
		for range done {
		}
	}()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var blocked bool
		require.NoError(collect, f.pg.Pool.QueryRow(waitCtx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked))
		require.True(collect, blocked)
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, holder.Commit(waitCtx))
	var pgErr *pgconn.PgError
	require.ErrorAs(t, <-done, &pgErr)
	require.Equal(t, "hosted_manifest_cutover_guard", pgErr.ConstraintName)
	require.Equal(t, providerOpens, h.stores.opened, "raced activation needs no provider")
	rep = nil
	require.NoError(t, json.Unmarshal(out.Bytes(), &rep))
	requireCounts(rep, 1, 0, 0, 0)
	require.Equal(t, false, rep["applied"])
	require.Equal(t, false, rep["ready"])

	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(ctx, `DELETE FROM hosted_vector_manifests WHERE revision_id=$1`, late)
	require.NoError(t, err)
	require.NoError(t, f.pg.FreezeWriters(ctx, opts.Generation))
	providerOpens = h.stores.opened
	_, err = h.run(opts)
	require.ErrorContains(t, err, "must be verified")
	require.Equal(t, providerOpens, h.stores.opened, "unverified activation needs no provider")
	verify()
	providerOpens = h.stores.opened
	rep, err = h.run(opts)
	require.NoError(t, err)
	requireCounts(rep, 0, 0, 0, 0)
	require.Equal(t, true, rep["applied"])
	require.Equal(t, providerOpens, h.stores.opened, "PostgreSQL activation needs neither source nor target provider")
}

func TestReindexActivateReportsHostedManifestWorkBeyondCorpusLimit(t *testing.T) {
	bare := newDoc("", "note", "A completed bare document.")
	tagged := newTaggedDoc("alpha", "report", "A completed tagged document.")
	f := newTaggedFixture(t, bare, tagged)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	ctx := t.Context()
	opts := defaultOptions()
	opts.Generation, opts.Activate, opts.Quiesce = "g2", true, 0
	registerGeneration(t, f.pg, opts.Generation, "postgres", pgstore.VectorTable(opts.Generation), sourceModel)
	require.NoError(t, f.pg.FreezeWriters(ctx, opts.Generation))
	digest, err := corpusDigest(ctx, f.pg, opts)
	require.NoError(t, err)
	require.NoError(t, f.pg.RecordVerification(ctx, opts.Generation, digest, json.RawMessage(`{"passed":true}`)))

	revision := uuid.NewString()
	_, err = f.pg.Pool.Exec(ctx, insertCutoverManifest, revision, uuid.NewString(), "writing")
	require.NoError(t, err)
	opts.MaxDocuments = 1
	for _, apply := range []bool{false, true} {
		opts.Apply = apply
		providerOpens := h.stores.opened
		rep, runErr := h.run(opts)
		require.ErrorContains(t, runErr, "not ready")
		require.Equal(t, providerOpens, h.stores.opened)
		require.Equal(t, map[string]any{"writing": float64(1), "active": float64(0), "retired": float64(0), "cleanups": float64(0)}, rep["hosted_manifest_cutover"])
		require.Equal(t, []any{"drain hosted work on the original source before activation (writing: 1, active: 0, retired: 0, cleanups: 0)"}, rep["reasons"])
		require.Equal(t, false, rep["ready"])
		require.Equal(t, false, rep["applied"])
		require.Equal(t, pgstore.LegacyGeneration, rep["active_generation"])
		require.Equal(t, true, rep["frozen"])
		require.Equal(t, opts.Generation, rep["freeze_generation"])
	}

	_, err = f.pg.Pool.Exec(ctx, `DELETE FROM hosted_vector_manifests WHERE revision_id=$1`, revision)
	require.NoError(t, err)
	for _, apply := range []bool{false, true} {
		opts.Apply = apply
		providerOpens := h.stores.opened
		rep, runErr := h.run(opts)
		require.ErrorContains(t, runErr, "activation exceeds -max-documents")
		require.Nil(t, rep)
		require.Equal(t, providerOpens, h.stores.opened)
	}
	state, err := f.pg.VectorState(ctx)
	require.NoError(t, err)
	require.Equal(t, pgstore.LegacyGeneration, state.Active)
	require.True(t, state.Frozen)
	require.Equal(t, opts.Generation, state.FreezeGeneration)
}

func TestHostedManifestCutoverMissingMigration(t *testing.T) {
	f := &fixture{pg: vectorDB(t)}
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	ctx := t.Context()
	const destination = "g2"
	registerGeneration(t, f.pg, destination, "postgres", pgstore.VectorTable(destination), sourceModel)
	_, err := f.pg.Pool.Exec(ctx, insertCutoverManifest, uuid.NewString(), uuid.NewString(), "writing")
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(ctx, `DROP FUNCTION hosted_manifest_cutover_counts()`)
	require.NoError(t, err)
	profile := h.profileFile("pg.json", testProfile(sourceModel))
	for _, opts := range []options{
		{Generation: destination, Profile: profile, TargetStore: "postgres"},
		{Generation: destination, Activate: true},
		{Generation: destination, Activate: true, Apply: true},
	} {
		providerOpens := h.stores.opened
		rep, runErr := h.run(opts)
		require.ErrorContains(t, runErr, "apply migration 047")
		require.Nil(t, rep)
		require.Equal(t, providerOpens, h.stores.opened, "a missing migration needs no provider")
	}
	providerOpens := h.stores.opened
	rep, err := h.run(options{Generation: "hosted", Profile: profile, TargetStore: "pinecone", TargetIndex: "new-index"})
	require.NoError(t, err)
	require.NotContains(t, rep, "hosted_manifest_cutover")
	require.Equal(t, providerOpens, h.stores.opened, "inventory works with an offline Pinecone source")
	registerGeneration(t, f.pg, "hosted", "pinecone", "new-index", sourceModel)
	require.NoError(t, f.pg.FreezeWriters(ctx, "hosted"))
	digest, err := corpusDigest(ctx, f.pg, defaultOptions())
	require.NoError(t, err)
	require.NoError(t, f.pg.RecordVerification(ctx, "hosted", digest, json.RawMessage(`{"passed":true}`)))
	providerOpens = h.stores.opened
	rep, err = h.run(options{Generation: "hosted", Activate: true, Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, rep["applied"])
	require.NotContains(t, rep, "hosted_manifest_cutover")
	require.Equal(t, providerOpens+1, h.stores.opened, "Pinecone activation checks target connectivity")
}

type cutoverCleanupProvider struct {
	domain.VectorPresenceChecker
	delete func(context.Context, string, []string) error
}

func (p cutoverCleanupProvider) DeleteVectorIDs(ctx context.Context, namespace string, ids []string) error {
	return p.delete(ctx, namespace, ids)
}

func TestCutoverAfterHostedManifestDrain(t *testing.T) {
	bare := newDoc("", "note", "A safe bare document stays in the corpus.")
	tagged := newTaggedDoc("alpha", "report", "A safe tagged document stays in the corpus.")
	f := newTaggedFixture(t, tagged, bare)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	ctx := t.Context()
	pg := f.pg
	pg.EnableHostedVectorManifests(true)
	source := pcstore.NewClientForIndex(h.stores.index("quarry"), testConfig(), "quarry-host")
	exec := func(sql string, args ...any) {
		t.Helper()
		_, err := pg.Pool.Exec(ctx, sql, args...)
		require.NoError(t, err)
	}
	seed := func(namespace string) (domain.Job, string, []domain.Chunk) {
		t.Helper()
		doc := domain.DocumentID(uuid.NewString())
		res, err := pg.AcceptDocument(ctx, domain.AcceptRequest{
			Document: domain.Document{ID: doc, Content: "interrupted hosted ingestion"}, Fingerprint: string(doc), MaxPending: 10,
		})
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, res.Outcome)
		job, err := pg.ClaimJob(ctx, "source-worker", time.Minute, 3)
		require.NoError(t, err)
		require.NotNil(t, job)
		require.Equal(t, doc, job.Document.ID)
		rev := uuid.NewString()
		m := domain.VectorManifest{RevisionID: rev, Incarnation: job.Incarnation, Attempt: job.Attempts,
			LeaseToken: job.LeaseToken, VectorNamespace: namespace, IDPrefix: domain.RevisionVectorPrefix(doc, rev)}
		var chunks []domain.Chunk
		for i, content := range []string{"first interrupted chunk", "second interrupted chunk"} {
			id, key := hexChunkID(string(doc), i), domain.FormatRevisionVectorID(doc, rev, i)
			m.ChunkIDs, m.VectorIDs = append(m.ChunkIDs, id), append(m.VectorIDs, key)
			chunks = append(chunks, domain.Chunk{ID: domain.ChunkID(id), DocumentID: doc, Index: i, Text: content,
				RevisionID: rev, VectorID: key, VectorIncarnation: job.Incarnation, Embedding: embedWith(sourceModel, content)})
		}
		require.NoError(t, pg.PersistVectorManifest(ctx, *job, m))
		require.NoError(t, source.Upsert(ctx, namespace, chunks))
		return *job, rev, chunks
	}
	const namespace = "retired-namespace"
	failedJob, retired, retiredChunks := seed(namespace)
	require.NoError(t, pg.FailJob(ctx, failedJob, "interrupted"))
	abandoned, writing, writingChunks := seed("")
	exec(`UPDATE ingest_jobs SET lease_until=NOW()-interval '1 second' WHERE document_id=$1`, abandoned.Document.ID)

	checkCounts := func(writing, retired int64) {
		t.Helper()
		counts, err := pg.HostedManifestCutoverCounts(ctx)
		require.NoError(t, err)
		require.Equal(t, pgstore.HostedManifestCutoverCounts{Writing: writing, Retired: retired, Cleanups: retired}, counts)
	}
	checkCounts(1, 1)
	stage := options{Generation: "g2", Profile: h.profileFile("pg.json", testProfile(sourceModel)), TargetStore: "postgres", Apply: true}
	_, err := h.run(stage)
	require.NoError(t, err, "writing and retired work do not block stage")
	freeze := options{Generation: "g2", Freeze: true, Apply: true}
	_, err = h.run(freeze)
	require.NoError(t, err)
	verify := verifyOptions("g2")
	verify.SampleSize = f.completedChunks()
	verify.Apply = true
	require.Positive(t, verify.SampleSize)
	verifyCorpus := func() {
		t.Helper()
		report, verifyErr := h.run(verify)
		require.NoError(t, verifyErr, "%v", report["failures"])
		require.Equal(t, true, report["passed"])
		require.Equal(t, true, report["recorded"])
		sample := report["sample"].(map[string]any)
		require.EqualValues(t, verify.SampleSize, sample["queries"])
		require.EqualValues(t, 1, sample["self_hit_rate"])
	}
	verifyCorpus()
	activate := options{Generation: "g2", Activate: true, Apply: true}
	providerOpens := h.stores.opened
	rep, err := h.run(activate)
	require.ErrorContains(t, err, "not ready")
	require.Equal(t, providerOpens, h.stores.opened)
	require.Equal(t, false, rep["applied"])
	require.Equal(t, true, rep["frozen"])
	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)

	exec(`UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	releaseWriter, err := pg.AcquireWriter(ctx)
	require.NoError(t, err)
	defer func() {
		if releaseWriter != nil {
			releaseWriter()
		}
	}()
	liveTarget := targetSpec{Store: "pinecone", Target: "quarry", Generation: "legacy", Dimensions: testDims}
	_, _, _, err = h.deps.openBound(ctx, liveTarget, "different-host")
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
	outage := errors.New("original provider unavailable")
	deleteCalls := 0
	ops := domain.DeleteOperations{
		ExpectedIncarnation: abandoned.Incarnation, VectorIndex: "quarry",
		ManifestVectors: func(context.Context, string, string, []string) error {
			deleteCalls++
			return outage
		},
	}
	require.ErrorIs(t, pg.DeleteWithCleanup(ctx, abandoned.Document.ID, ops), outage)
	wrongIndex := ops
	wrongIndex.VectorIndex = "different-index"
	require.ErrorIs(t, pg.DeleteWithCleanup(ctx, abandoned.Document.ID, wrongIndex), domain.ErrDeletionTargetMismatch)
	require.Equal(t, 1, deleteCalls)
	checkCounts(1, 1)
	ops.ManifestVectors = source.DeleteByManifestRevision
	require.NoError(t, pg.DeleteWithCleanup(ctx, abandoned.Document.ID, ops))
	checkCounts(0, 2)
	exec(`UPDATE hosted_vector_cleanups SET next_attempt_at=NOW()+interval '1 hour' WHERE revision_id=$1`, writing)

	sweep := func(sweepCtx context.Context, budget int, deleteIDs func(context.Context, string, []string) error) (worker.SweepReport, error) {
		t.Helper()
		p := cutoverCleanupProvider{VectorPresenceChecker: source, delete: deleteIDs}
		return worker.NewSweeper(pg, p, worker.SweepOptions{
			Owner: "source-sweeper", PageSize: 1, Budget: budget, ConvergenceDelay: time.Hour, RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour,
		}).SweepOnce(sweepCtx)
	}
	makeReady := func(rev string) {
		t.Helper()
		exec(`UPDATE hosted_vector_cleanups SET next_attempt_at=NOW() WHERE revision_id=$1`, rev)
	}
	leased, err := pg.ClaimVectorCleanup(ctx, "old-sweeper", time.Minute, 0)
	require.NoError(t, err)
	require.NotNil(t, leased)
	require.Equal(t, retired, leased.RevisionID)
	swept, err := sweep(ctx, 10, source.DeleteVectorIDs)
	require.NoError(t, err)
	require.Zero(t, swept.TasksClaimed)
	checkCounts(0, 2)
	exec(`UPDATE hosted_vector_cleanups SET lease_until=NOW()-interval '1 second' WHERE revision_id=$1`, retired)

	for _, providerErr := range []error{outage, errors.New("provider reports a namespace mismatch")} {
		swept, err = sweep(ctx, 10, func(_ context.Context, gotNamespace string, ids []string) error {
			require.Equal(t, namespace, gotNamespace)
			require.Equal(t, []string{retiredChunks[0].VectorID}, ids)
			return providerErr
		})
		require.NoError(t, err)
		require.Equal(t, 1, swept.TasksDeferred)
		require.Equal(t, providerErr.Error(), swept.LastError)
		checkCounts(0, 2)
		noRetry, retryErr := sweep(ctx, 10, source.DeleteVectorIDs)
		require.NoError(t, retryErr)
		require.Zero(t, noRetry.TasksClaimed, "existing backoff remains in force")
		makeReady(retired)
	}
	sweepCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err = sweep(sweepCtx, 10, func(callCtx context.Context, _ string, _ []string) error {
		cancel()
		return callCtx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
	checkCounts(0, 2)
	swept, err = sweep(ctx, 1, source.DeleteVectorIDs)
	require.NoError(t, err)
	require.Equal(t, 1, swept.VectorsDeleted)
	require.Zero(t, swept.TasksCompleted)
	checkCounts(0, 2)
	swept, err = sweep(ctx, 10, func(callCtx context.Context, gotNamespace string, ids []string) error {
		require.Equal(t, namespace, gotNamespace)
		require.Equal(t, []string{retiredChunks[1].VectorID}, ids)
		if deleteErr := source.DeleteVectorIDs(callCtx, gotNamespace, ids); deleteErr != nil {
			return deleteErr
		}
		return source.Upsert(callCtx, namespace, retiredChunks[:1]) // An old write arrives late.
	})
	require.NoError(t, err)
	require.Equal(t, 1, swept.TasksDeferred)
	checkCounts(0, 2)
	makeReady(retired)
	swept, err = sweep(ctx, 10, source.DeleteVectorIDs)
	require.NoError(t, err)
	require.Equal(t, 1, swept.TasksUnresolved)
	checkCounts(0, 2)
	makeReady(retired)
	makeReady(writing)
	swept, err = sweep(ctx, 10, source.DeleteVectorIDs)
	require.NoError(t, err)
	require.Equal(t, 2, swept.TasksDeferred)
	makeReady(retired)
	makeReady(writing)
	swept, err = sweep(ctx, 10, source.DeleteVectorIDs)
	require.NoError(t, err)
	require.Equal(t, 2, swept.TasksCompleted)
	checkCounts(0, 0)
	for ns, chunks := range map[string][]domain.Chunk{"": writingChunks, namespace: retiredChunks} {
		residue, fetchErr := source.FetchPresentVectorIDs(ctx, ns, []string{chunks[0].VectorID, chunks[1].VectorID})
		require.NoError(t, fetchErr)
		require.Empty(t, residue)
	}
	require.Len(t, h.stores.indexes, 1, "all hosted mutations used the original source")
	releaseWriter()
	releaseWriter = nil

	_, err = h.run(freeze)
	require.NoError(t, err)
	_, err = h.run(stage)
	require.NoError(t, err)
	verifyCorpus()
	providerOpens = h.stores.opened
	rep, err = h.run(activate)
	require.NoError(t, err)
	require.Equal(t, true, rep["applied"])
	require.Equal(t, "g2", rep["active_generation"])
	require.Equal(t, providerOpens, h.stores.opened)

	target, err := pgstore.OpenGenerationVectorStore(ctx, pg, testDims, activate.Generation)
	require.NoError(t, err)
	for _, doc := range []docSpec{bare, tagged} {
		hits, queryErr := target.QueryFiltered(ctx, embedWith(sourceModel, doc.Texts[0]), 1,
			domain.VectorFilter{DocumentIDs: []string{doc.ID}, Collection: doc.Collection, DocType: doc.DocType})
		require.NoError(t, queryErr)
		require.Len(t, hits, 1)
		require.Equal(t, f.chunks[doc.ID][0].ID, hits[0].Chunk.ID)
	}
}

func TestFreezeWaitsForRunningJobsAndTimesOut(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	accepted, err := f.pg.AcceptDocument(t.Context(), domain.AcceptRequest{Document: domain.Document{ID: "11111111-1111-4111-8111-111111111111", Content: "queued"}, Fingerprint: "fp-queued", MaxPending: 10})
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	job, err := f.pg.ClaimJob(t.Context(), "worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var out bytes.Buffer
	err = execute(ctx, h.deps, options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0}, &out, 10*time.Millisecond)
	require.ErrorContains(t, err, "did not quiesce")
	require.ErrorContains(t, err, "1 jobs running")
	require.ErrorIs(t, f.pg.WritersOpen(t.Context()), domain.ErrWritersFrozen, "the freeze stays in force after a timeout")

	require.NoError(t, f.pg.FailJob(t.Context(), *job, "stop"))
	_, err = h.run(options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
}

func TestRollbackCatchesUpTheRetiredTargetBeforeSwitchingBack(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
	cutover := func(generation string) {
		t.Helper()
		_, err := h.run(options{Generation: generation, Freeze: true, Apply: true, Quiesce: 0})
		require.NoError(t, err)
		apply := verifyOptions(generation)
		apply.Apply = true
		_, err = h.run(apply)
		require.NoError(t, err)
		_, err = h.run(options{Generation: generation, Activate: true, Apply: true, Quiesce: 0})
		require.NoError(t, err)
		_, err = h.run(options{Thaw: true, Apply: true})
		require.NoError(t, err)
	}
	cutover("g2")

	// After the cutover the process serves g2 (the live store is its Pinecone index).
	h.deps.cfg.VectorStore, h.deps.cfg.PineconeIndexName, h.deps.cfg.VectorGeneration = "pinecone", "quarry-g2", "g2"
	f.pg.SetVectorGeneration("g2")
	// Live traffic lands only in g2, so the retired legacy table misses it.
	live := newDoc("alpha", "note", "Written after the cutover to the new target.")
	chunks := insertDoc(t, f.pg, live)
	f.docs = append(f.docs, live)
	f.chunks[live.ID] = chunks
	require.NoError(t, pcstore.NewClientForIndex(h.stores.index("quarry-g2"), testConfig(), "").Upsert(t.Context(), "", sourceChunks(live, chunks, sourceModel)))

	// The retired legacy target is stale until it is caught up under a new freeze.
	rollback := verifyOptions("legacy")
	_, err := h.run(rollback)
	require.ErrorIs(t, err, errNotVerified, "the new document has no checkpoint in the retired target")
	_, err = h.run(options{Generation: "legacy", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	profile := h.profileFile("legacy.json", testProfile(sourceModel))
	report, err := h.run(options{Generation: "legacy", Profile: profile, TargetStore: "postgres", Apply: true})
	require.NoError(t, err, "%v", report)
	// The retired legacy target has no write ledger, so every eligible document is rewritten idempotently.
	require.EqualValues(t, 5, report["staged_documents"])
	require.EqualValues(t, f.completedChunks(), report["copied_chunks"])
	apply := verifyOptions("legacy")
	apply.Apply = true
	_, err = h.run(apply)
	require.NoError(t, err)
	report, err = h.run(options{Generation: "legacy", Activate: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	require.Equal(t, "legacy", report["active_generation"])
	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)

	// Replicas configured for the legacy target write again; those still on g2 are pinned out.
	require.NoError(t, f.pg.PinVectorGeneration(t.Context(), "legacy", "postgres", "chunk_vectors", "", testProfile(sourceModel)))
	require.NoError(t, f.pg.WritersOpen(t.Context()))
	f.pg.SetVectorGeneration("g2")
	require.ErrorIs(t, f.pg.WritersOpen(t.Context()), domain.ErrGenerationMismatch)
	retired, err := f.pg.Generation(t.Context(), "g2")
	require.NoError(t, err)
	require.Equal(t, pgstore.StateRetired, retired.State)
}

func TestFreezeReportsOpenDeletionReceiptsAndRecoversAfterQualifiedRetry(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	_, err := f.pg.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)

	target := f.docs[0]
	var incarnation string
	require.NoError(t, f.pg.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, target.ID).Scan(&incarnation))

	err = f.pg.DeleteWithCleanup(t.Context(), domain.DocumentID(target.ID), domain.DeleteOperations{
		ExpectedIncarnation: incarnation,
		LegacyNamespace:     func(context.Context, string) error { return nil },
		LegacyVectors:       func(context.Context, string) error { return errors.New("provider outage") },
	})
	require.ErrorContains(t, err, "provider outage")

	freeze := options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0}
	h.requireFreezeBlockedByDeletion(freeze, target.ID)

	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)
	require.NoError(t, f.pg.WritersOpen(t.Context()))

	// A bare UUID retry cannot finish a checkpointed deletion; it must use the incarnation.
	err = f.pg.DeleteWithCleanup(t.Context(), domain.DocumentID(target.ID), domain.DeleteOperations{
		LegacyNamespace: func(context.Context, string) error { return nil },
		LegacyVectors:   func(context.Context, string) error { return nil },
	})
	require.ErrorIs(t, err, domain.ErrDeletionPreconditionRequired)

	err = f.pg.DeleteWithCleanup(t.Context(), domain.DocumentID(target.ID), domain.DeleteOperations{
		ExpectedIncarnation: incarnation,
		LegacyNamespace:     func(context.Context, string) error { return nil },
		LegacyVectors:       func(context.Context, string) error { return nil },
	})
	require.NoError(t, err)

	rep, err := h.run(freeze)
	require.NoError(t, err)
	quiesce := rep["quiesce"].(map[string]any)
	require.EqualValues(t, 0, quiesce["OpenDeletions"])
	_, hasIDs := quiesce["open_deletion_ids"]
	require.False(t, hasIDs)
}

func TestFreezeRecoversAfterReleaseOfRowGoneLegacyReservation(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "pinecone")
	ctx := t.Context()
	target := newDoc("alpha", "report", "Original stored in the source bucket.")
	target.Incarnation = uuid.NewString()
	target.S3Key = "originals/" + target.ID + "/source.txt"
	const sourceBucket = "originals"
	_, vectorIndex := h.deps.cfg.VectorTarget()
	insertDoc(t, f.pg, target)
	_, err := f.pg.Pool.Exec(ctx, `INSERT INTO document_deletions
		(document_id,incarnation,kind,source_key,source_bucket,vector_mode,vector_index,step,held,finalized)
		VALUES($1,$2,'legacy_reservation',$3,$4,'legacy',$5,0,TRUE,FALSE)`,
		target.ID, target.Incarnation, target.S3Key, sourceBucket, vectorIndex)
	require.NoError(t, err)
	// An older executable removed the document while the reservation was still open.
	removed, err := f.pg.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, target.ID)
	require.NoError(t, err)
	require.EqualValues(t, 1, removed.RowsAffected())
	_, err = f.pg.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)

	freeze := options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0}
	h.requireFreezeBlockedByDeletion(freeze, target.ID)
	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)
	require.NoError(t, f.pg.WritersOpen(ctx))

	err = f.pg.DeleteWithCleanup(ctx, domain.DocumentID(target.ID), domain.DeleteOperations{
		ExpectedIncarnation: target.Incarnation,
	})
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)

	held, err := f.pg.ListHeldDeletions(ctx)
	require.NoError(t, err)
	require.Len(t, held, 1)
	receipt := held[0]
	require.Equal(t, domain.DocumentID(target.ID), receipt.DocumentID)
	require.Equal(t, target.Incarnation, receipt.Incarnation)
	require.Equal(t, "legacy_reservation", receipt.Kind)
	require.Equal(t, "legacy", receipt.VectorMode)
	require.Equal(t, vectorIndex, receipt.VectorIndex)
	require.Equal(t, sourceBucket, receipt.SourceBucket)
	require.Equal(t, target.S3Key, receipt.SourceKey)
	require.True(t, receipt.Held)
	require.False(t, receipt.Finalized)
	require.False(t, receipt.DocumentPresent)
	require.False(t, receipt.Claimed)

	var deletedKeys []string
	require.NoError(t, f.pg.ReleaseHeldDeletion(ctx, receipt.DocumentID, receipt.Incarnation, domain.DeleteOperations{
		SourceBucket: sourceBucket,
		VectorIndex:  vectorIndex,
		Source: func(_ context.Context, key string) error {
			deletedKeys = append(deletedKeys, key)
			return nil
		},
	}))
	require.Equal(t, []string{target.S3Key}, deletedKeys)
	var finalized, stillHeld bool
	require.NoError(t, f.pg.Pool.QueryRow(ctx, `SELECT finalized, held FROM document_deletions
		WHERE document_id=$1 AND incarnation=$2`, target.ID, target.Incarnation).Scan(&finalized, &stillHeld))
	require.True(t, finalized)
	require.False(t, stillHeld)
	held, err = f.pg.ListHeldDeletions(ctx)
	require.NoError(t, err)
	require.Empty(t, held)

	report, err := h.run(freeze)
	require.NoError(t, err, "%v", report)
	quiesce := report["quiesce"].(map[string]any)
	require.EqualValues(t, 0, quiesce["OpenDeletions"])
	require.NotContains(t, quiesce, "open_deletion_ids")
	require.ErrorIs(t, f.pg.WritersOpen(ctx), domain.ErrWritersFrozen)
}

func TestActivateRefusesAndNamesOpenDeletionReceipts(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	h.stagePinecone("g2", "quarry-g2", sourceModel, 0)

	_, err := h.run(options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	apply := verifyOptions("g2")
	apply.Apply = true
	_, err = h.run(apply)
	require.NoError(t, err)

	openID := uuid.NewString()
	_, err = f.pg.Pool.Exec(t.Context(), `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode) VALUES($1,$2,'quarantine','local')`, openID, uuid.NewString())
	require.NoError(t, err)

	report, err := h.run(options{Generation: "g2", Activate: true, Quiesce: 0})
	require.ErrorContains(t, err, "not ready")
	require.Equal(t, false, report["ready"])
	require.Contains(t, fmt.Sprint(report["reasons"]), fmt.Sprintf("1 open deletions (%s) must finish before activation", openID))
	quiesce := report["quiesce"].(map[string]any)
	require.Equal(t, []any{openID}, quiesce["open_deletion_ids"])

	report, err = h.run(options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.ErrorContains(t, err, fmt.Sprintf("0 in-flight jobs and 1 open deletions (%s) must finish before activation", openID))
	require.Equal(t, false, report["applied"])
	require.Contains(t, fmt.Sprint(report["reasons"]), openID)
}

func TestFreezeAndActivateErrorsExcludeDocumentContent(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	h.stagePinecone("g2", "quarry-g2", sourceModel, 0)

	docID := uuid.NewString()
	const (
		secretContent    = "SECRET_CONTENT_MARKER_256"
		secretChunk      = "SECRET_CHUNK_MARKER_256"
		secretCollection = "secret-collection-256"
		secretDocType    = "secret-type-256"
		secretFile       = "secret-file-256.txt"
	)
	_, err := f.pg.Pool.Exec(t.Context(),
		`INSERT INTO documents(id,content,status,collection,doc_type,file_name,chunk_count,fingerprint) VALUES($1,$2,'deleting',$3,$4,$5,1,$6)`,
		docID, secretContent, secretCollection, secretDocType, secretFile, "fp-"+docID)
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(t.Context(),
		`INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,$3,3)`,
		hexChunkID(docID, 0), docID, secretChunk)
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(t.Context(),
		`INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode) VALUES($1,$2,'qualified_intent','local')`,
		docID, uuid.NewString())
	require.NoError(t, err)

	assertExcludesSecrets := func(text string) {
		t.Helper()
		require.Contains(t, text, docID)
		for _, secret := range []string{secretContent, secretChunk, secretCollection, secretDocType, secretFile} {
			require.NotContains(t, text, secret)
		}
	}

	var invOut bytes.Buffer
	err = execute(t.Context(), h.deps, options{
		Generation: "g2", Profile: h.profileFile("g2.json", testProfile(sourceModel)),
		TargetStore: "pinecone", TargetIndex: "quarry-g2",
		MaxDocuments: 1000, MaxChunks: 100000, MaxBytes: 128 << 20, Batch: 100, Page: 100, Timeout: time.Minute,
	}, &invOut, 10*time.Millisecond)
	require.NoError(t, err)
	assertExcludesSecrets(invOut.String())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var freezeOut bytes.Buffer
	err = execute(ctx, h.deps, options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0}, &freezeOut, 10*time.Millisecond)
	require.Error(t, err)
	assertExcludesSecrets(err.Error())
	assertExcludesSecrets(freezeOut.String())

	apply := verifyOptions("g2")
	apply.Apply = true
	_, err = h.run(apply)
	require.NoError(t, err)

	previewReport, err := h.run(options{Generation: "g2", Activate: true, Quiesce: 0})
	require.Error(t, err)
	previewJSON, marshalErr := json.Marshal(previewReport)
	require.NoError(t, marshalErr)
	assertExcludesSecrets(string(previewJSON))

	applyReport, err := h.run(options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.Error(t, err)
	assertExcludesSecrets(err.Error())
	applyJSON, marshalErr := json.Marshal(applyReport)
	require.NoError(t, marshalErr)
	assertExcludesSecrets(string(applyJSON))
}
