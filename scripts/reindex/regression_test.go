package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
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

type lostTargetResponse struct {
	vectorTarget
	upsert func([]domain.Chunk)
	delete func([]string)
}

var errTargetResponseLost = errors.New("target acknowledgement lost")

func (p lostTargetResponse) Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error {
	if err := p.vectorTarget.Upsert(ctx, namespace, chunks); err != nil || p.upsert == nil {
		return err
	}
	p.upsert(chunks)
	return errTargetResponseLost
}

func (p lostTargetResponse) DeleteIDs(ctx context.Context, ids []string) error {
	if err := p.vectorTarget.DeleteIDs(ctx, ids); err != nil || p.delete == nil {
		return err
	}
	p.delete(ids)
	return errTargetResponseLost
}

func trapTargetResponse(d deps, generation string, loss lostTargetResponse) deps {
	open := d.open
	d.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
		target, closeTarget, host, err := open(ctx, spec)
		if err == nil && spec.Generation == generation {
			target = lostTargetResponse{target, loss.upsert, loss.delete}
		}
		return target, closeTarget, host, err
	}
	return d
}

func pendingTargetWrite(t *testing.T, f *fixture, generation string) uuid.UUID {
	t.Helper()
	row, err := f.pg.Generation(t.Context(), generation)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.True(t, row.PendingWrite)
	require.True(t, row.PendingWriteID.Valid)
	require.NotEqual(t, uuid.Nil, row.PendingWriteID.UUID)
	return row.PendingWriteID.UUID
}

func TestUnacknowledgedTargetWriteSurvivesRestart(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	normal := h.deps
	h.deps = trapTargetResponse(normal, "g2", lostTargetResponse{upsert: func([]domain.Chunk) {}})
	stage := options{Generation: "g2", TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true,
		Profile: h.profileFile("g2.json", testProfile(sourceModel))}
	_, err := h.run(stage)
	require.ErrorIs(t, err, errTargetResponseLost)
	id := pendingTargetWrite(t, f, stage.Generation)
	require.ErrorContains(t, err, id.String())
	h.deps = normal
	opened := h.stores.opened
	_, err = h.run(stage)
	require.ErrorContains(t, err, "unconfirmed target write")
	require.ErrorContains(t, err, "-confirm-target-drained="+id.String())
	require.Equal(t, opened, h.stores.opened)
	require.NoError(t, f.pg.FreezeWriters(t.Context(), "g2"))
	apply := verifyOptions("g2")
	apply.Apply = true
	_, err = h.run(apply)
	require.ErrorContains(t, err, "unconfirmed target write")
	require.ErrorContains(t, err, id.String())
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
	require.Error(t, err)

	// Settlement is durable even if an unrelated provider preflight fails.
	stage.ConfirmTargetDrained = id
	preflightErr := errors.New("target preflight failed")
	h.deps.open = func(ctx context.Context, spec targetSpec) (vectorTarget, func(), string, error) {
		if spec.Generation == "g2" {
			return nil, nil, "", preflightErr
		}
		return normal.open(ctx, spec)
	}
	_, err = h.run(stage)
	require.ErrorIs(t, err, preflightErr)
	h.deps = normal
	opened = h.stores.opened
	_, err = h.run(stage)
	require.ErrorContains(t, err, "stale")
	require.Equal(t, opened, h.stores.opened)
	stage.ConfirmTargetDrained = uuid.Nil
	report, err := h.run(stage)
	require.NoError(t, err)
	require.Equal(t, true, report["complete"])
	_, err = h.run(apply)
	require.NoError(t, err)
}

func TestStaleTargetWriteConfirmationCannotAdvance(t *testing.T) {
	for _, store := range []string{"pinecone", "postgres"} {
		for _, mutation := range []string{"identical upsert", "different exact delete"} {
			t.Run(store+"/"+mutation, func(t *testing.T) {
				testStaleTargetWriteConfirmation(t, store, mutation == "different exact delete")
			})
		}
	}
}

func testStaleTargetWriteConfirmation(t *testing.T, store string, deleteSecond bool) {
	t.Helper()
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()
	normal := h.deps
	stage := options{Generation: "g2", TargetStore: store, Apply: true,
		Profile: h.profileFile("g2.json", testProfile(sourceModel))}
	if store == "pinecone" {
		stage.TargetIndex = "quarry-g2"
	}
	var orphan string
	var second lostTargetResponse
	if deleteSecond {
		spec := normal.resolve(stage)
		registerGeneration(t, f.pg, spec.generation, store, spec.target, sourceModel)
		orphan = uuid.NewString()
		key := strings.ReplaceAll(orphan, "-", "")
		_, err := f.pg.BeginGenerationDocument(t.Context(), stage.Generation, orphan, []string{key})
		require.NoError(t, err)
		second.delete = func(ids []string) { require.Equal(t, []string{key}, ids) }
	}
	var first []domain.Chunk
	h.deps = trapTargetResponse(normal, stage.Generation, lostTargetResponse{
		upsert: func(chunks []domain.Chunk) { first = chunks },
	})
	report, err := h.run(stage)
	require.ErrorIs(t, err, errTargetResponseLost)
	require.NotContains(t, report, "next_after")
	require.NotEmpty(t, first)
	a := pendingTargetWrite(t, f, stage.Generation)

	// A different generation must reject the token before creating a target.
	h.deps = normal
	other := stage
	other.Generation = "g3"
	if store == "pinecone" {
		other.TargetIndex = "quarry-g3"
	}
	other.ConfirmTargetDrained = a
	opened := h.stores.opened
	_, err = h.run(other)
	require.ErrorContains(t, err, "stale")
	require.Equal(t, opened, h.stores.opened)
	row, err := f.pg.Generation(t.Context(), other.Generation)
	require.NoError(t, err)
	require.Nil(t, row)
	require.Equal(t, a, pendingTargetWrite(t, f, stage.Generation))

	if !deleteSecond {
		second.upsert = func(chunks []domain.Chunk) { require.Equal(t, first, chunks) }
	}
	stage.ConfirmTargetDrained = a
	h.deps = trapTargetResponse(normal, stage.Generation, second)
	report, err = h.run(stage)
	require.ErrorIs(t, err, errTargetResponseLost)
	b := pendingTargetWrite(t, f, stage.Generation)
	require.NotEqual(t, a, b)
	require.ErrorContains(t, err, b.String())
	if after, ok := report["next_after"].(string); ok {
		stage.After = after
	}

	// Another invocation can replay A after losing the clear response. It
	// must leave B and the stage checkpoint untouched.
	h.deps = normal
	documentIDs := []string{string(first[0].DocumentID)}
	if deleteSecond {
		documentIDs = append(documentIDs, orphan)
	}
	before, err := f.pg.GenerationCheckpoints(t.Context(), stage.Generation, documentIDs)
	require.NoError(t, err)
	opened = h.stores.opened
	stage.MaxEmbedCalls = 1
	rejected, err := h.run(stage)
	require.ErrorContains(t, err, "stale")
	require.ErrorContains(t, err, b.String())
	require.Nil(t, rejected)
	require.Equal(t, opened, h.stores.opened)
	require.Nil(t, h.embed)
	require.Equal(t, b, pendingTargetWrite(t, f, stage.Generation))
	after, err := f.pg.GenerationCheckpoints(t.Context(), stage.Generation, documentIDs)
	require.NoError(t, err)
	require.Equal(t, before, after)

	require.NoError(t, f.pg.FreezeWriters(t.Context(), stage.Generation))
	verify := verifyOptions(stage.Generation)
	verify.Apply = true
	_, err = h.run(verify)
	require.ErrorContains(t, err, b.String())
	cutover := options{Generation: stage.Generation, Activate: true, Apply: true}
	_, err = h.run(cutover)
	require.ErrorContains(t, err, "must be verified")

	stage.ConfirmTargetDrained = b
	report, err = h.run(stage)
	require.NoError(t, err)
	require.Equal(t, true, report["complete"])
	_, err = h.run(verify)
	require.NoError(t, err)
	opened = h.stores.opened
	_, err = h.run(stage)
	require.ErrorContains(t, err, "stale")
	require.Equal(t, opened, h.stores.opened)
	_, err = h.run(cutover)
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

func TestAppliedVerificationRevokesApprovalForBadSourceSeed(t *testing.T) {
	ctx := t.Context()
	f := newFixture(t)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
	require.NoError(t, f.pg.FreezeWriters(ctx, "g2"))
	apply := verifyOptions("g2")
	apply.Apply, apply.SampleSize = true, f.completedChunks()
	report, err := h.run(apply)
	require.NoError(t, err, "%v", report["failures"])
	require.Equal(t, true, report["recorded"])
	approved, err := f.pg.Generation(ctx, "g2")
	require.NoError(t, err)
	require.NotNil(t, approved.VerifiedAt)
	require.NotEmpty(t, approved.CorpusDigest)
	require.NotEmpty(t, approved.Verification)

	doc := f.docs[0]
	chunk := f.chunks[doc.ID][0]
	key := string(chunk.ID)
	source := h.stores.index("quarry")
	updateHostedRecord(t, source, key, func(record *pinecone.Vector) {
		record.Metadata.Fields["chunk_id"] = structpb.NewStringValue(string(f.chunks[doc.ID][1].ID))
	})
	dry := apply
	dry.Apply = false
	for _, opts := range []options{dry, apply} {
		bad, verifyErr := h.run(opts)
		require.ErrorIs(t, verifyErr, errNotVerified)
		require.Equal(t, false, bad["recorded"])
		require.Equal(t, false, bad["passed"])
		sample := bad["sample"].(map[string]any)
		require.EqualValues(t, 1, sample["skipped"])
		require.EqualValues(t, apply.SampleSize-1, sample["queries"])
		cov := bad["coverage"].(map[string]any)
		require.EqualValues(t, 1, cov["key_coverage"])
		require.EqualValues(t, 1, cov["profile_coverage"])
		row, generationErr := f.pg.Generation(ctx, "g2")
		require.NoError(t, generationErr)
		if opts.Apply {
			require.Nil(t, row.VerifiedAt)
			require.Empty(t, row.CorpusDigest)
			require.JSONEq(t, "null", string(row.Verification))
		} else {
			require.Equal(t, approved, row)
		}
	}
	activate := options{Generation: "g2", Activate: true, Apply: true}
	_, err = h.run(activate)
	require.ErrorContains(t, err, "must be verified")
	require.ErrorIs(t, f.pg.WritersOpen(ctx), domain.ErrWritersFrozen)

	sourceClient := pcstore.NewClientForIndex(source, testConfig(), "")
	require.NoError(t, sourceClient.Upsert(ctx, "", sourceChunks(doc, []domain.Chunk{chunk}, sourceModel)))
	report, err = h.run(apply)
	require.NoError(t, err, "%v", report["failures"])
	require.Equal(t, true, report["recorded"])
	sample := report["sample"].(map[string]any)
	require.EqualValues(t, apply.SampleSize, sample["queries"])
	require.EqualValues(t, 0, sample["skipped"])
	switched, err := h.run(activate)
	require.NoError(t, err, "%v", switched)
	require.Equal(t, "g2", switched["active_generation"])
	require.Zero(t, h.embedCalls())
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
