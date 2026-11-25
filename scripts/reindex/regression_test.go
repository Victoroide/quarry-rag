package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestConfiguredSourceMustMatchTheActiveTarget(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "pinecone")
	profile := h.profileFile("target.json", testProfile(sourceModel))
	live := h.stores.index("quarry")
	key := string(f.chunks[f.docs[0].ID][0].ID)
	before, err := (pineconeStore{pcstore.NewClientForIndex(live, testConfig(), "")}).Fetch(t.Context(), []string{key})
	require.NoError(t, err)
	h.deps.cfg.PineconeIndexName = "wrong-index"
	_, err = h.run(options{Generation: "g2", TargetStore: "pinecone", TargetIndex: "quarry", Profile: profile, Apply: true, MaxEmbedCalls: 100})
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
	require.Zero(t, h.stores.opened)
	after, err := (pineconeStore{pcstore.NewClientForIndex(live, testConfig(), "")}).Fetch(t.Context(), []string{key})
	require.NoError(t, err)
	require.Equal(t, before, after)

	h.deps.cfg.PineconeIndexName = "quarry"
	h.attestSource()
	opened := h.stores.opened
	wrong := pcstore.NewClientForIndex(h.stores.index("other-index"), testConfig(), "")
	for _, doc := range f.docs {
		if doc.Status == "completed" {
			require.NoError(t, wrong.Upsert(t.Context(), "", sourceChunks(doc, f.chunks[doc.ID], "different-model")))
		}
	}
	h.deps.cfg.PineconeIndexName = "other-index"
	_, err = h.run(options{Generation: "g2", TargetStore: "postgres", Profile: profile, Apply: true})
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
	require.Equal(t, opened, h.stores.opened)
	_, err = h.run(options{Generation: "g2", TargetStore: "postgres", Profile: profile})
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
}

type beforeUpsert struct {
	vectorTarget
	before func(context.Context) error
}

func (p beforeUpsert) Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error {
	if err := p.before(ctx); err != nil {
		return err
	}
	return p.vectorTarget.Upsert(ctx, namespace, chunks)
}

func TestPausedStagerExcludesCatchUpVerificationAndActivation(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	stage := defaultOptions()
	stage.Generation, stage.TargetStore, stage.TargetIndex, stage.Apply = "g2", "pinecone", "quarry-g2", true
	stage.Profile = h.profileFile("g2.json", testProfile(sourceModel))
	original := h.deps.open
	stalling := h.deps
	started, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	stalling.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
		target, closeTarget, host, err := original(ctx, spec)
		if err != nil || spec.Generation != "g2" {
			return target, closeTarget, host, err
		}
		return beforeUpsert{target, func(ctx context.Context) error {
			once.Do(func() {
				close(started)
				select {
				case <-proceed:
				case <-ctx.Done():
				}
			})
			return ctx.Err()
		}}, closeTarget, host, nil
	}
	finished := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { finished <- execute(ctx, stalling, stage, &bytes.Buffer{}, time.Millisecond) }()
	select {
	case <-started:
	case err := <-finished:
		t.Fatalf("staging ran without reaching the paused upsert: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("staging did not reach its first upsert")
	}
	revisions, err := f.pg.DocumentRevisions(t.Context(), "", 1)
	require.NoError(t, err)
	id := revisions[0].ID
	_, err = f.pg.Pool.Exec(t.Context(), "UPDATE documents SET collection='newscope' WHERE id=$1", id)
	require.NoError(t, err)
	// Make the current source searchable under the updated collection as well.
	snap, err := f.pg.LoadDocumentSnapshot(t.Context(), id)
	require.NoError(t, err)
	updated := sourceChunks(docSpec{ID: id, Collection: "newscope", DocType: snap.DocType}, snap.Chunks, sourceModel)
	require.NoError(t, f.legacy.Upsert(t.Context(), "", updated))
	require.NoError(t, f.pg.FreezeWriters(t.Context(), "g2"))
	_, err = h.run(stage)
	require.ErrorContains(t, err, "another operation owns")
	apply := verifyOptions("g2")
	apply.Apply = true
	_, err = h.run(apply)
	require.ErrorContains(t, err, "another operation owns")
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
	require.ErrorContains(t, err, "another operation owns")
	close(proceed)
	require.NoError(t, <-finished)
	_, err = h.run(stage)
	require.NoError(t, err)
	_, err = h.run(apply)
	require.NoError(t, err)
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
	require.NoError(t, err)
	target := pcstore.NewClientForIndex(h.stores.index("quarry-g2"), testConfig(), "")
	got, err := target.Fetch(t.Context(), []string{string(updated[0].ID)})
	require.NoError(t, err)
	require.Equal(t, "newscope", got[string(updated[0].ID)].Metadata["collection"])
}

type lostUpsertResponse struct{ vectorTarget }

func (p lostUpsertResponse) Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error {
	if err := p.vectorTarget.Upsert(ctx, namespace, chunks); err != nil {
		return err
	}
	return errors.New("target acknowledgement lost")
}

func TestUnacknowledgedTargetWriteSurvivesRestart(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	open := h.deps.open
	h.deps.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
		target, closeTarget, host, err := open(ctx, spec)
		if err == nil && spec.Generation == "g2" {
			target = lostUpsertResponse{target}
		}
		return target, closeTarget, host, err
	}
	stage := options{Generation: "g2", TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true,
		Profile: h.profileFile("g2.json", testProfile(sourceModel))}
	_, err := h.run(stage)
	require.ErrorContains(t, err, "acknowledgement lost")
	h.deps.open = open
	_, err = h.run(stage)
	require.ErrorContains(t, err, "unconfirmed target write")
	require.NoError(t, f.pg.FreezeWriters(t.Context(), "g2"))
	apply := verifyOptions("g2")
	apply.Apply = true
	_, err = h.run(apply)
	require.ErrorContains(t, err, "unconfirmed target write")
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
	require.Error(t, err)
	stage.ConfirmTargetDrained = true
	report, err := h.run(stage)
	require.NoError(t, err)
	require.Equal(t, true, report["complete"])
	_, err = h.run(apply)
	require.NoError(t, err)
}

func TestFailedAppliedVerificationRevokesPreviousApproval(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
	require.NoError(t, f.pg.FreezeWriters(t.Context(), "g2"))
	apply := verifyOptions("g2")
	apply.Apply = true
	_, err := h.run(apply)
	require.NoError(t, err)
	require.NoError(t, h.stores.index("quarry-g2").DeleteVectorsByID(t.Context(), []string{string(f.chunks[f.docs[0].ID][0].ID)}))
	_, err = h.run(apply)
	require.ErrorIs(t, err, errNotVerified)
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
	require.ErrorContains(t, err, "must be verified")
	row, err := f.pg.Generation(t.Context(), "g2")
	require.NoError(t, err)
	require.Empty(t, row.CorpusDigest)
	require.Nil(t, row.VerifiedAt)
}

func TestVerificationRejectsEmptyRankingsAndIncompleteDocumentScan(t *testing.T) {
	f := newFixture(t)
	v, target := verifierOver(t, f)
	empty := func([]float32, domain.VectorFilter) []domain.ScoredChunk { return nil }
	v.target = scriptedReader{vectorReader: target, results: empty}
	v.source = scriptedReader{vectorReader: f.legacy, results: empty}
	v.opts.MinRecall = 1
	report, err := v.run(t.Context())
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Zero(t, report.Sample.Queries)
	require.Zero(t, report.Sample.MeanRecall)
	require.Equal(t, report.Sample.Requested, report.Sample.Skipped)
	v.opts.MaxDocuments = 1
	report, err = v.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, report.Expected.Documents)
	require.False(t, report.Passed)
	require.Contains(t, fmt.Sprint(report.Failures), "-max-documents")
}

func TestRollbackPrunesDeletedAndRecreatedDocuments(t *testing.T) {
	for _, source := range []string{"pinecone", "postgres"} {
		t.Run(source, func(t *testing.T) {
			f := newFixture(t)
			h := newHarness(t, f, source)
			h.attestSource()
			profile := h.profileFile("copy.json", testProfile(sourceModel))
			nextStore, nextIndex := "postgres", ""
			if source == "postgres" {
				nextStore, nextIndex = "pinecone", "quarry-g2"
			}
			_, err := h.run(options{Generation: "g2", TargetStore: nextStore, TargetIndex: nextIndex, Profile: profile, Apply: true})
			require.NoError(t, err)
			require.NoError(t, f.pg.FreezeWriters(t.Context(), "g2"))
			apply := verifyOptions("g2")
			apply.Apply = true
			_, err = h.run(apply)
			require.NoError(t, err)
			_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
			require.NoError(t, err)
			require.NoError(t, f.pg.ThawWriters(t.Context()))
			h.deps.cfg.VectorStore, h.deps.cfg.PineconeIndexName, h.deps.cfg.VectorGeneration = nextStore, nextIndex, "g2"
			f.pg.SetVectorGeneration("g2")
			active, closeActive, _, err := h.deps.open(t.Context(), targetSpec{Store: nextStore, Target: nextIndex, Generation: "g2", Dimensions: testDims})
			require.NoError(t, err)
			defer closeActive()

			deleted, recreated := f.docs[0], f.docs[1]
			for _, doc := range []docSpec{deleted, recreated} {
				_, err = f.pg.Pool.Exec(t.Context(), "DELETE FROM documents WHERE id=$1", doc.ID)
				require.NoError(t, err)
				var ids []string
				for _, chunk := range f.chunks[doc.ID] {
					ids = append(ids, string(chunk.ID))
				}
				require.NoError(t, active.DeleteIDs(t.Context(), ids))
			}
			recreated.Texts = []string{"A recreated document has one chunk."}
			recreated.Collection = "newscope"
			newChunks := insertDoc(t, f.pg, recreated)
			require.NoError(t, active.Upsert(t.Context(), "", sourceChunks(recreated, newChunks, sourceModel)))

			require.NoError(t, f.pg.FreezeWriters(t.Context(), "legacy"))
			rollbackIndex := ""
			if source == "pinecone" {
				rollbackIndex = "quarry"
			}
			report, err := h.run(options{Generation: "legacy", TargetStore: source, TargetIndex: rollbackIndex, Profile: profile, Apply: true})
			require.NoError(t, err)
			require.Equal(t, true, report["complete"])
			if source == "pinecone" {
				require.EqualValues(t, 4, report["pruned_keys"])
			}
			apply = verifyOptions("legacy")
			apply.SampleSize = 1 // This fixture removes most of the original corpus.
			apply.Apply = true
			_, err = h.run(apply)
			require.NoError(t, err)
			_, err = h.run(options{Generation: "legacy", Activate: true, Apply: true})
			require.NoError(t, err)
		})
	}
}

type partialSource struct {
	vectorReader
	err error
}

func (s partialSource) Fetch(ctx context.Context, ids []string) (map[string]domain.Chunk, error) {
	if len(ids) < 2 {
		return nil, s.err
	}
	got, err := s.vectorReader.Fetch(ctx, ids[:1])
	return got, errors.Join(err, s.err)
}

func TestUnreadableSourceUsesOnlyTheAuthorizedEmbeddingBudget(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	open := h.deps.open
	unavailable := errors.New("source read unavailable")
	h.deps.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
		if spec.Generation == "legacy" {
			return nil, nil, "", unavailable
		}
		return open(ctx, spec)
	}
	stage := options{Generation: "g2", TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true,
		Profile: h.profileFile("g2.json", testProfile(sourceModel))}
	report, err := h.run(stage)
	require.NoError(t, err)
	require.Equal(t, "embedding call budget", report["stopped"])
	require.Zero(t, h.embedCalls())
	stage.MaxEmbedCalls = f.completedChunks()
	report, err = h.run(stage)
	require.NoError(t, err)
	require.EqualValues(t, f.completedChunks(), report["embedded_chunks"])
	require.Equal(t, true, report["complete"])

	target := pgTarget(t, f.pg, "partial")
	s := f.stager(t, "partial", target, sourceModel, partialSource{f.legacy, unavailable}, &countingEmbedder{model: sourceModel})
	s.opts.MaxEmbedCalls = 0
	part, err := s.run(t.Context())
	require.NoError(t, err)
	require.Zero(t, part.Staged)
	require.Equal(t, "embedding call budget", part.Stopped)
	s.opts.MaxEmbedCalls = f.completedChunks()
	part, err = s.run(t.Context())
	require.NoError(t, err)
	require.True(t, part.Complete)
	require.Positive(t, part.Copied)
	require.Positive(t, part.Embedded)
	require.Equal(t, f.completedChunks(), part.Copied+part.Embedded)

	for _, hard := range []error{context.Canceled, pgstore.ErrGenerationDrift} {
		h.deps.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
			if spec.Generation == "legacy" {
				return nil, nil, "", hard
			}
			return open(ctx, spec)
		}
		_, err = h.run(stage)
		require.ErrorIs(t, err, hard)
	}
}

func TestStageBoundsCurrentRowsOrphanRemovalsAndSnapshotBytes(t *testing.T) {
	f := newFixture(t)
	client, _ := memoryPinecone()
	target := pineconeStore{client}
	registerGeneration(t, f.pg, "g2", "pinecone", "quarry-g2", sourceModel)
	s := f.stager(t, "g2", target, sourceModel, f.legacy, nil)
	_, err := s.run(t.Context())
	require.NoError(t, err)
	s.opts.MaxDocuments = 1
	report, err := s.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, report.Scanned)
	require.Equal(t, 1, report.Skipped)
	require.False(t, report.Complete)
	_, err = f.pg.Pool.Exec(t.Context(), "DELETE FROM documents WHERE status='completed'")
	require.NoError(t, err)
	report, err = s.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, report.Pruned)
	require.Equal(t, "orphan document limit", report.Stopped)
	require.False(t, report.Complete)

	doc := newDoc("alpha", "report", "longer than one byte")
	insertDoc(t, f.pg, doc)
	snapshot, err := f.pg.LoadDocumentSnapshotBounded(t.Context(), doc.ID, 100, 1)
	require.ErrorIs(t, err, pgstore.ErrSnapshotLimit)
	require.Nil(t, snapshot)
	s.opts.MaxBytes = 1
	report, err = s.run(t.Context())
	require.ErrorContains(t, err, "alone exceeds")
	require.Empty(t, report.NextAfter)
}

func TestEmbedderRejectsTheWrongDeploymentRevision(t *testing.T) {
	cfg := liveConfig("postgres")
	cfg.EmbeddingModelRevision = "other-deployment"
	provider, err := newDeps(cfg, nil).newEmbedder(t.Context(), testProfile(sourceModel))
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
	require.Nil(t, provider)
}

func TestSameNamedPineconeIndexCannotSupplyForeignEmbeddings(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "pinecone")
	h.deps.cfg.PineconeAPIKey = "original-project-key"
	h.attestSource()

	foreign := pcstore.NewClientForIndex(pcstore.NewMemoryIndex(testDims), h.deps.cfg, "foreign-quarry-host")
	for _, doc := range f.docs {
		if doc.Status == "completed" {
			require.NoError(t, foreign.Upsert(t.Context(), "", sourceChunks(doc, f.chunks[doc.ID], "different-model")))
		}
	}
	open := h.deps.open
	h.deps.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
		if spec.Target == "quarry" && h.deps.cfg.PineconeAPIKey == "foreign-project-key" {
			return pineconeStore{foreign}, func() {}, foreign.Host(), nil
		}
		return open(ctx, spec)
	}

	stage := options{Generation: "g2", TargetStore: "postgres", Apply: true,
		Profile: h.profileFile("g2.json", testProfile(sourceModel))}
	h.deps.cfg.PineconeAPIKey = "foreign-project-key"
	_, err := h.run(stage)
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
	row, err := f.pg.Generation(t.Context(), "g2")
	require.NoError(t, err)
	require.Nil(t, row)

	h.deps.cfg.PineconeAPIKey = "rotated-original-project-key"
	require.NoError(t, f.pg.PinVectorGeneration(t.Context(), "legacy", "pinecone", "quarry", "quarry-host", testProfile(sourceModel)))
	report, err := h.run(stage)
	require.NoError(t, err)
	require.EqualValues(t, f.completedChunks(), report["copied_chunks"])

	opens := h.stores.opened
	stage.Apply = false
	report, err = h.run(stage)
	require.NoError(t, err)
	require.Equal(t, opens, h.stores.opened, "inventory uses the recorded host")
	require.Equal(t, "quarry-host", report["source"].(map[string]any)["host"])
}

func admitFixture(t *testing.T, f *fixture, identity string) (id domain.DocumentID, receipt, incarnation string) {
	t.Helper()
	id, receipt = domain.DocumentID(uuid.NewSHA1(uuid.New(), []byte("fixture")).String()), uuid.NewString()
	accepted, err := f.pg.AcceptDocument(t.Context(), domain.AcceptRequest{
		Document: domain.Document{ID: id, Content: "fixture body"}, CallerSupplied: true,
		Fingerprint: chunker.ScopedFingerprint("evaluation-collection", "fixture body"), ContentDedup: domain.ContentDedupOff,
		MaxPending: 100, Collection: "evaluation-collection", EvaluationReceipt: receipt, EvaluationVectorIdentity: identity,
	})
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	return id, receipt, accepted.Incarnation
}

func TestCutoverKeepsActiveFixtureReceiptsServiceable(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	id, receipt, incarnation := admitFixture(t, f, "postgres")
	h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
	identity := domain.FixtureVectorIdentity("pinecone", "quarry-g2", "quarry-g2-host")
	noSweep := func(context.Context) error { return nil }

	_, err := h.run(options{Generation: "g2", Freeze: true, Apply: true})
	require.ErrorIs(t, err, pgstore.ErrFixtureReceipts)
	state, err := f.pg.VectorState(t.Context())
	require.NoError(t, err)
	require.False(t, state.Frozen, "the refusal precedes the freeze")
	require.NoError(t, f.pg.WritersOpen(t.Context()))

	deleted, err := f.pg.DeleteEvaluationFixture(t.Context(), id, receipt, incarnation, "postgres", noSweep)
	require.NoError(t, err)
	require.Equal(t, domain.FixtureDeleted, deleted)

	_, err = h.run(options{Generation: "g2", Freeze: true, Apply: true})
	require.NoError(t, err)
	verify := verifyOptions("g2")
	verify.Apply = true
	_, err = h.run(verify)
	require.NoError(t, err)
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
	require.NoError(t, err)
	require.NoError(t, f.pg.ThawWriters(t.Context()))
	require.NoError(t, f.pg.PinVectorGeneration(t.Context(), "g2", "pinecone", "quarry-g2", "quarry-g2-host", testProfile(sourceModel)))

	id, receipt, incarnation = admitFixture(t, f, identity)
	job, err := f.pg.ClaimJobForFixtureStore(t.Context(), "replacement", time.Minute, 3, identity)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, id, job.Document.ID)
	require.NoError(t, f.pg.FailJob(t.Context(), *job, "stop"))
	deleted, err = f.pg.DeleteEvaluationFixture(t.Context(), id, receipt, incarnation, identity, noSweep)
	require.NoError(t, err)
	require.Equal(t, domain.FixtureDeleted, deleted)
}
