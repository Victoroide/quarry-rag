package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/stretchr/testify/require"
)

const sourceModel = "model-a"

type fixture struct {
	pg     *pgstore.Client
	docs   []docSpec
	chunks map[string][]domain.Chunk
	legacy *pgstore.VectorStore
}

// newFixture seeds a corpus whose live vectors sit in the fixed PostgreSQL table.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	pg := vectorDB(t)
	legacy, err := pgstore.NewVectorStore(t.Context(), pg, testDims)
	require.NoError(t, err)
	f := &fixture{pg: pg, legacy: legacy, chunks: map[string][]domain.Chunk{}}
	for _, d := range []docSpec{
		newDoc("alpha", "report", "Quarterly revenue grew in the northern region.", "Operating costs fell after the renewal.", "The forecast assumes stable demand."),
		newDoc("alpha", "note", "Meeting notes from the planning session.", "Action items were assigned to the finance team."),
		newDoc("", "report", "An uncategorized report about archive hours.", "Reading room access requires a reader card."),
		newDoc("beta", "memo", "A short memo about building maintenance."),
		{ID: "00000000-0000-4000-8000-0000000000f1", Collection: "alpha", DocType: "report", Status: "failed", Texts: []string{"failed ingestion left this chunk"}},
		{ID: "00000000-0000-4000-8000-0000000000f2", Collection: "alpha", DocType: "report", Status: "processing"},
	} {
		f.add(t, d)
	}
	return f
}

func (f *fixture) add(t *testing.T, d docSpec) {
	t.Helper()
	chunks := insertDoc(t, f.pg, d)
	f.docs = append(f.docs, d)
	f.chunks[d.ID] = chunks
	if d.Status == "completed" && len(chunks) > 0 {
		require.NoError(t, f.legacy.Upsert(t.Context(), "", sourceChunks(d, chunks, sourceModel)))
	}
}

func (f *fixture) completedChunks() int {
	n := 0
	for _, d := range f.docs {
		if d.Status == "completed" {
			n += len(d.Texts)
		}
	}
	return n
}

func (f *fixture) stager(t *testing.T, generation string, target vectorTarget, model string, copyFrom vectorReader, embed domain.EmbeddingProvider) *stager {
	t.Helper()
	opts := defaultOptions()
	opts.MaxEmbedCalls = 100000
	return &stager{corpus: f.pg, ledger: f.pg, target: target, generation: generation, profile: testProfile(model), opts: opts, copyFrom: copyFrom, embed: embed}
}

func registerGeneration(t *testing.T, pg *pgstore.Client, name, store, target, model string) {
	t.Helper()
	_, err := pg.RegisterGeneration(t.Context(), pgstore.Generation{Name: name, Store: store, Target: target, Profile: testProfile(model), Origin: pgstore.OriginRecorded})
	require.NoError(t, err)
}

func pgTarget(t *testing.T, pg *pgstore.Client, generation string) *pgstore.VectorStore {
	t.Helper()
	store, err := pgstore.CreateGenerationVectorStore(t.Context(), pg, testDims, generation)
	require.NoError(t, err)
	registerGeneration(t, pg, generation, "postgres", pgstore.VectorTable(generation), sourceModel)
	return store
}

func TestCopyDecision(t *testing.T) {
	attested := testProfile(sourceModel)
	other := testProfile("model-b")
	revised := testProfile(sourceModel)
	revised.ModelRevision = "rev-2"
	wider := testProfile(sourceModel)
	wider.Dimensions = 8
	newText := testProfile(sourceModel)
	newText.ExtractionChunkVersion = "chunk-2"

	reuse, _, err := decideCopy(&attested, testProfile(sourceModel))
	require.NoError(t, err)
	require.True(t, reuse)
	for name, target := range map[string]domain.EmbeddingProfile{"model": other, "revision": revised, "dimensions": wider} {
		recompute, reason, decideErr := decideCopy(&attested, target)
		require.NoError(t, decideErr, name)
		require.False(t, recompute, name)
		require.Contains(t, reason, "recomputed", name)
	}
	reuse, reason, err := decideCopy(nil, testProfile(sourceModel))
	require.NoError(t, err)
	require.False(t, reuse)
	require.Contains(t, reason, "unknown")
	_, _, err = decideCopy(&attested, newText)
	require.ErrorContains(t, err, "re-extraction")
}

func TestMigratePostgresToPineconeCopiesVectorsAndKeepsIdentity(t *testing.T) {
	f := newFixture(t)
	before := relationalState(t, f.pg)
	client, index := memoryPinecone()
	registerGeneration(t, f.pg, "g2", "pinecone", "quarry-g2", sourceModel)
	s := f.stager(t, "g2", pineconeStore{client}, sourceModel, f.legacy, nil)

	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete, "%+v", rep)
	require.Equal(t, 4, rep.Staged)
	require.Equal(t, f.completedChunks(), rep.Copied)
	require.Zero(t, rep.Embedded)
	require.Equal(t, 4, rep.Scanned, "only completed documents are scanned")
	requireTargetMatches(t, f.pg, pineconeStore{client}, sourceModel)
	require.Equal(t, f.completedChunks(), index.Len())
	require.Equal(t, before, relationalState(t, f.pg), "UUIDs, chunk indices, scope, fingerprints and status stay untouched")

	again, err := s.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, stageReport{Generation: "g2", Scanned: 4, Skipped: 4, Inconsistent: []string{}, Complete: true}, again, "a completed run is idempotent")
	require.Equal(t, f.completedChunks(), index.Len())
}

func TestMigratePineconeToPostgresCopiesVectorsAndKeepsIdentity(t *testing.T) {
	f := newFixture(t)
	source, _ := memoryPinecone()
	for _, d := range f.docs {
		if d.Status == "completed" {
			require.NoError(t, source.Upsert(t.Context(), "", sourceChunks(d, f.chunks[d.ID], sourceModel)))
		}
	}
	before := relationalState(t, f.pg)
	target := pgTarget(t, f.pg, "g2")
	s := f.stager(t, "g2", target, sourceModel, source, nil)

	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete, "%+v", rep)
	require.Equal(t, f.completedChunks(), rep.Copied)
	require.Zero(t, rep.Embedded)
	requireTargetMatches(t, f.pg, target, sourceModel)
	require.Equal(t, before, relationalState(t, f.pg))
	complete, incomplete, err := f.pg.CountGenerationDocuments(t.Context(), "g2")
	require.NoError(t, err)
	require.Equal(t, [2]int{4, 0}, [2]int{complete, incomplete})
}

func TestReembeddingReplacesVectorsOfAnotherModelAtTheSameDimension(t *testing.T) {
	f := newFixture(t)
	client, _ := memoryPinecone()
	registerGeneration(t, f.pg, "g2", "pinecone", "quarry-g2", "model-b")
	embedder := &countingEmbedder{model: "model-b"}
	// No source reader: the decision found the profiles incompatible.
	s := f.stager(t, "g2", pineconeStore{client}, "model-b", nil, embedder)
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete)
	require.Equal(t, f.completedChunks(), rep.Embedded)
	require.Zero(t, rep.Copied)
	require.Equal(t, f.completedChunks(), int(embedder.calls.Load()))
	requireTargetMatches(t, f.pg, pineconeStore{client}, "model-b")
	fetched, err := client.Fetch(t.Context(), []string{string(f.chunks[f.docs[0].ID][0].ID)})
	require.NoError(t, err)
	for _, c := range fetched {
		require.NotEqual(t, embedWith(sourceModel, c.Text), c.Embedding, "a same-size vector from another model is a different representation")
	}
}

func TestEmbeddingCallsAreBudgetedAndNothingIsWrittenBeyondThem(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		name   string
		budget int
		embed  func() domain.EmbeddingProvider
	}{
		{"forbidden", 0, func() domain.EmbeddingProvider { return &countingEmbedder{model: "model-b"} }},
		{"too small", 2, func() domain.EmbeddingProvider { return &countingEmbedder{model: "model-b"} }},
		{"no provider", 100, func() domain.EmbeddingProvider { return nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, index := memoryPinecone()
			generation := "b" + fmt.Sprint(tc.budget)
			registerGeneration(t, f.pg, generation, "pinecone", "idx-"+generation, "model-b")
			embed := tc.embed()
			s := f.stager(t, generation, pineconeStore{client}, "model-b", nil, embed)
			s.opts.MaxEmbedCalls = tc.budget
			rep, err := s.run(t.Context())
			require.NoError(t, err)
			require.Equal(t, "embedding call budget", rep.Stopped)
			require.False(t, rep.Complete)
			// A document that cannot be fully embedded writes nothing: the target holds
			// exactly the documents that were completed, never a fraction of one.
			require.Equal(t, rep.Chunks, index.Len())
			complete, incomplete, countErr := f.pg.CountGenerationDocuments(t.Context(), generation)
			require.NoError(t, countErr)
			require.Equal(t, [2]int{rep.Staged, 0}, [2]int{complete, incomplete})
			if counting, ok := embed.(*countingEmbedder); ok {
				require.LessOrEqual(t, int(counting.calls.Load()), tc.budget)
			}
			if tc.budget == 0 || tc.name == "no provider" {
				require.Zero(t, rep.Staged)
			}
		})
	}
}

func TestProviderFailureWritesNothing(t *testing.T) {
	f := newFixture(t)
	client, index := memoryPinecone()
	registerGeneration(t, f.pg, "g2", "pinecone", "quarry-g2", "model-b")
	s := f.stager(t, "g2", pineconeStore{client}, "model-b", nil, &countingEmbedder{model: "model-b", fail: true})
	_, err := s.run(t.Context())
	require.ErrorContains(t, err, "provider unavailable")
	require.Zero(t, index.Len())
}

// stubReader serves crafted source vectors.
type stubReader struct{ chunks map[string]domain.Chunk }

func (s stubReader) Fetch(_ context.Context, ids []string) (map[string]domain.Chunk, error) {
	out := map[string]domain.Chunk{}
	for _, id := range ids {
		if c, ok := s.chunks[id]; ok {
			out[id] = c
		}
	}
	return out, nil
}
func (stubReader) ListIDs(context.Context, string, int) (ids []string, next string, err error) {
	return nil, "", nil
}
func (stubReader) QueryFiltered(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return nil, nil
}

func TestOnlyValidSourceVectorsAreReused(t *testing.T) {
	pg := vectorDB(t)
	legacy, err := pgstore.NewVectorStore(t.Context(), pg, testDims)
	require.NoError(t, err)
	f := &fixture{pg: pg, legacy: legacy, chunks: map[string][]domain.Chunk{}}
	texts := []string{"valid vector", "zero vector", "not a number", "infinite", "short vector", "stale text", "other document", "other position", "no vector"}
	d := newDoc("alpha", "report", texts...)
	chunks := insertDoc(t, pg, d)
	f.docs = append(f.docs, d)
	good := sourceChunks(d, chunks, sourceModel)
	stub := stubReader{chunks: map[string]domain.Chunk{}}
	mutate := map[string]func(*domain.Chunk){
		"zero vector":    func(c *domain.Chunk) { c.Embedding = make([]float32, testDims) },
		"not a number":   func(c *domain.Chunk) { c.Embedding = []float32{float32(math.NaN()), 1, 1, 1} },
		"infinite":       func(c *domain.Chunk) { c.Embedding = []float32{float32(math.Inf(1)), 1, 1, 1} },
		"short vector":   func(c *domain.Chunk) { c.Embedding = []float32{1, 1} },
		"stale text":     func(c *domain.Chunk) { c.Text = "earlier chunk text" },
		"other document": func(c *domain.Chunk) { c.DocumentID = "another-document" },
		"other position": func(c *domain.Chunk) { c.Index += 5 },
	}
	for i, c := range good {
		if texts[i] == "no vector" {
			continue
		}
		if m, ok := mutate[texts[i]]; ok {
			m(&c)
		}
		stub.chunks[string(c.ID)] = c
	}
	client, _ := memoryPinecone()
	registerGeneration(t, pg, "g2", "pinecone", "quarry-g2", sourceModel)
	embedder := &countingEmbedder{model: sourceModel}
	s := f.stager(t, "g2", pineconeStore{client}, sourceModel, stub, embedder)
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete)
	require.Equal(t, 1, rep.Copied, "only the valid vector is reused")
	require.Equal(t, len(texts)-1, rep.Embedded)
	require.Equal(t, len(texts)-1, int(embedder.calls.Load()))
	requireTargetMatches(t, pg, pineconeStore{client}, sourceModel)
}

func TestProtectedTargetIsNeverWritten(t *testing.T) {
	f := newFixture(t)
	client, index := memoryPinecone()
	registerGeneration(t, f.pg, "g2", "pinecone", "quarry-g2", sourceModel)
	_, err := f.pg.Pool.Exec(t.Context(), `UPDATE vector_state SET active_generation='g2'`)
	require.NoError(t, err)
	s := f.stager(t, "g2", pineconeStore{client}, sourceModel, f.legacy, nil)
	_, err = s.run(t.Context())
	require.ErrorIs(t, err, pgstore.ErrLiveTarget)
	require.Zero(t, index.Len())
}

func TestRunsStopAtBoundsAndResume(t *testing.T) {
	f := newFixture(t)
	client, _ := memoryPinecone()
	registerGeneration(t, f.pg, "g2", "pinecone", "quarry-g2", sourceModel)
	s := f.stager(t, "g2", pineconeStore{client}, sourceModel, f.legacy, nil)
	s.opts.MaxDocuments = 3
	s.opts.Page = 2
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, "document limit", rep.Stopped)
	require.Equal(t, 3, rep.Staged)
	require.False(t, rep.Complete)
	require.NotEmpty(t, rep.NextAfter)
	rep, err = s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete)
	require.Equal(t, 1, rep.Staged)
	require.Equal(t, 3, rep.Skipped)
	requireTargetMatches(t, f.pg, pineconeStore{client}, sourceModel)

	t.Run("chunks", func(t *testing.T) {
		client, _ := memoryPinecone()
		registerGeneration(t, f.pg, "g3", "pinecone", "quarry-g3", sourceModel)
		s := f.stager(t, "g3", pineconeStore{client}, sourceModel, f.legacy, nil)
		s.opts.MaxChunks = 4
		rep, err := s.run(t.Context())
		require.NoError(t, err)
		require.Equal(t, "chunk or byte limit", rep.Stopped)
		require.LessOrEqual(t, rep.Chunks, 4)

		// Resume at an unstaged multi-chunk document so MaxChunks = 1 fails regardless of UUID order.
		revisions, err := f.pg.DocumentRevisions(t.Context(), "", 4)
		require.NoError(t, err)
		ids := make([]string, len(revisions))
		for i, r := range revisions {
			ids[i] = r.ID
		}
		checkpoints, err := f.pg.GenerationCheckpoints(t.Context(), "g3", ids)
		require.NoError(t, err)
		after := ""
		for _, r := range revisions {
			if !checkpoints[r.ID].Complete && r.Chunks > 1 {
				break
			}
			after = r.ID
		}
		s.opts.After = after
		s.opts.MaxChunks = 1
		_, err = s.run(t.Context())
		require.ErrorContains(t, err, "alone exceeds")
	})
	t.Run("bytes", func(t *testing.T) {
		client, _ := memoryPinecone()
		registerGeneration(t, f.pg, "g4", "pinecone", "quarry-g4", sourceModel)
		s := f.stager(t, "g4", pineconeStore{client}, sourceModel, f.legacy, nil)
		s.opts.MaxBytes = 150
		rep, err := s.run(t.Context())
		require.NoError(t, err)
		require.Equal(t, "chunk or byte limit", rep.Stopped)
		require.LessOrEqual(t, rep.Bytes, int64(150))
	})
}

func TestCatchUpFollowsDeletionsRecreationsAndMetadataChanges(t *testing.T) {
	f := newFixture(t)
	target := pgTarget(t, f.pg, "g2")
	embedder := &countingEmbedder{model: "model-b"}
	s := f.stager(t, "g2", target, "model-b", nil, embedder)
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete)
	requireTargetMatches(t, f.pg, target, "model-b")

	a, b, c, d := f.docs[0], f.docs[1], f.docs[2], f.docs[3]
	// Deleted document: its keys must leave the target.
	_, err = f.pg.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, b.ID)
	require.NoError(t, err)
	// A document deleted and recreated under the same UUID has a new incarnation and new text.
	_, err = f.pg.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, a.ID)
	require.NoError(t, err)
	recreated := docSpec{ID: a.ID, Collection: "alpha", DocType: "report", Status: "completed", Texts: []string{"Rewritten first chunk after the recreation."}}
	insertDoc(t, f.pg, recreated)
	// A document that fails later is no longer eligible.
	_, err = f.pg.Pool.Exec(t.Context(), `UPDATE documents SET status='failed' WHERE id=$1`, c.ID)
	require.NoError(t, err)
	// Metadata changed in PostgreSQL, the authority for scope.
	_, err = f.pg.Pool.Exec(t.Context(), `UPDATE documents SET collection='gamma', doc_type='policy' WHERE id=$1`, d.ID)
	require.NoError(t, err)
	// A newly completed document.
	insertDoc(t, f.pg, newDoc("alpha", "note", "A document completed during the migration."))

	rep, err = s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete, "%+v", rep)
	require.Equal(t, 3, rep.Staged, "the recreated, relabeled and new documents")
	require.Equal(t, 2, rep.Pruned, "the deleted and the failed documents")
	requireTargetMatches(t, f.pg, target, "model-b")
	orphans, err := f.pg.OrphanedGenerationDocuments(t.Context(), "g2", 10)
	require.NoError(t, err)
	require.Empty(t, orphans)
}

func TestInconsistentDocumentsAreReportedNotStaged(t *testing.T) {
	f := newFixture(t)
	_, err := f.pg.Pool.Exec(t.Context(), `UPDATE documents SET chunk_count=chunk_count+1 WHERE id=$1`, f.docs[0].ID)
	require.NoError(t, err)
	client, _ := memoryPinecone()
	registerGeneration(t, f.pg, "g2", "pinecone", "quarry-g2", sourceModel)
	s := f.stager(t, "g2", pineconeStore{client}, sourceModel, f.legacy, nil)
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{f.docs[0].ID}, rep.Inconsistent)
	require.False(t, rep.Complete, "an inconsistent document blocks completion")
	require.Equal(t, 3, rep.Staged)
}

var errInjected = errors.New("injected interruption")

// faults fails the nth boundary crossed, once before and once after each operation.
type faults struct{ failAt, hits int }

func (f *faults) tick() error {
	f.hits++
	if f.hits == f.failAt {
		return errInjected
	}
	return nil
}

func (f *faults) around(op func() error) error {
	if err := f.tick(); err != nil {
		return err
	}
	if err := op(); err != nil {
		return err
	}
	return f.tick()
}

type faultyLedger struct {
	ledger
	f *faults
}

func (l faultyLedger) BeginGenerationDocument(ctx context.Context, g, d string, ids []string) (out []string, err error) {
	err = l.f.around(func() (inner error) { out, inner = l.ledger.BeginGenerationDocument(ctx, g, d, ids); return })
	return out, err
}

func (l faultyLedger) CompleteGenerationDocument(ctx context.Context, g, d, rev, inc string, ids []string, copied, embedded int) error {
	return l.f.around(func() error { return l.ledger.CompleteGenerationDocument(ctx, g, d, rev, inc, ids, copied, embedded) })
}

func (l faultyLedger) ForgetGenerationDocument(ctx context.Context, g, d string) error {
	return l.f.around(func() error { return l.ledger.ForgetGenerationDocument(ctx, g, d) })
}

type faultyTarget struct {
	vectorTarget
	f *faults
}

func (t faultyTarget) Upsert(ctx context.Context, ns string, chunks []domain.Chunk) error {
	return t.f.around(func() error { return t.vectorTarget.Upsert(ctx, ns, chunks) })
}

func (t faultyTarget) DeleteIDs(ctx context.Context, ids []string) error {
	return t.f.around(func() error { return t.vectorTarget.DeleteIDs(ctx, ids) })
}

// restartMatrix interrupts a run at every write and checkpoint boundary, restarts it
// and requires the converged target to equal the corpus exactly.
func restartMatrix(t *testing.T, f *fixture, open func(t *testing.T, generation string) (vectorTarget, func()), copyFrom vectorReader, embedder domain.EmbeddingProvider, model string) {
	t.Helper()
	// Remove one document first so that every run also has a stale key to clean up.
	crossed := 0
	for failAt := 1; ; failAt++ {
		generation := fmt.Sprintf("r%d", failAt)
		target, cleanup := open(t, generation)
		faulty := &faults{failAt: failAt}
		s := f.stager(t, generation, faultyTarget{target, faulty}, model, copyFrom, embedder)
		s.ledger = faultyLedger{f.pg, faulty}
		// Pre-seed a stale key so that the cleanup boundary is also crossed.
		stale := f.chunks[f.docs[0].ID][0]
		stale.Embedding = embedWith("stale", "stale")
		stale.Text = "removed text"
		stale.Metadata = map[string]any{"doc_type": "report"}
		_, beginErr := f.pg.BeginGenerationDocument(t.Context(), generation, f.docs[0].ID, []string{"ffffffffffffffffffffffffffffffff"})
		require.NoError(t, beginErr)
		stale.ID = "ffffffffffffffffffffffffffffffff"
		require.NoError(t, target.Upsert(t.Context(), "", []domain.Chunk{stale}))

		_, err := s.run(t.Context())
		if faulty.hits < failAt {
			require.NoError(t, err)
			requireTargetMatches(t, f.pg, target, model)
			cleanup()
			break
		}
		require.ErrorIs(t, err, errInjected, "boundary %d", failAt)
		crossed++
		recovery := f.stager(t, generation, target, model, copyFrom, embedder)
		rep, err := recovery.run(t.Context())
		require.NoError(t, err, "boundary %d", failAt)
		require.True(t, rep.Complete, "boundary %d: %+v", failAt, rep)
		requireTargetMatches(t, f.pg, target, model)
		again, err := recovery.run(t.Context())
		require.NoError(t, err)
		require.Zero(t, again.Staged, "boundary %d converged", failAt)
		cleanup()
	}
	require.Greater(t, crossed, 20, "the matrix must cross many write and checkpoint boundaries")
}

func TestRestartAtEveryBoundaryPostgresToPinecone(t *testing.T) {
	f := newFixture(t)
	restartMatrix(t, f, func(t *testing.T, generation string) (vectorTarget, func()) {
		client, _ := memoryPinecone()
		registerGeneration(t, f.pg, generation, "pinecone", "idx-"+generation, sourceModel)
		return pineconeStore{client}, func() {}
	}, f.legacy, nil, sourceModel)
}

func TestRestartAtEveryBoundaryPineconeToPostgres(t *testing.T) {
	f := newFixture(t)
	source, _ := memoryPinecone()
	for _, d := range f.docs {
		if d.Status == "completed" {
			require.NoError(t, source.Upsert(t.Context(), "", sourceChunks(d, f.chunks[d.ID], sourceModel)))
		}
	}
	restartMatrix(t, f, func(t *testing.T, generation string) (vectorTarget, func()) {
		return pgTarget(t, f.pg, generation), func() {}
	}, source, nil, sourceModel)
}

func TestRestartAtEveryBoundaryWhenReembedding(t *testing.T) {
	f := newFixture(t)
	embedder := &countingEmbedder{model: "model-b"}
	restartMatrix(t, f, func(t *testing.T, generation string) (vectorTarget, func()) {
		client, _ := memoryPinecone()
		registerGeneration(t, f.pg, generation, "pinecone", "idx-"+generation, "model-b")
		return pineconeStore{client}, func() {}
	}, nil, embedder, "model-b")
}
