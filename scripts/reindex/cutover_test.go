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
	"github.com/google/uuid"
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
