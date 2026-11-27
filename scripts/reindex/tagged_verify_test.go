package main

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestVerifyHostedChunkPositionBeforeCoverage(t *testing.T) {
	for _, tc := range []struct {
		name, form string
		index      int
		position   *structpb.Value
	}{
		{"missing", "tagged", 0, nil},
		{"string zero", "manifested", 0, structpb.NewStringValue("0")},
		{"NaN", "bare", 0, structpb.NewNumberValue(math.NaN())},
		{"infinite", "manifested", 1, structpb.NewNumberValue(math.Inf(-1))},
		{"fraction", "bare", 1, structpb.NewNumberValue(1.5)},
		{"negative", "tagged", 0, structpb.NewNumberValue(-1)},
		{"out of range", "manifested", 1, structpb.NewNumberValue(math.MaxInt32 + 1)},
		{"integer mismatch", "tagged", 1, structpb.NewNumberValue(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, live := hostedPositionSnapshot(tc.form)
			for i := range live {
				live[i].VectorIncarnation = snap.Incarnation
			}
			src, sourceIndex := memoryPinecone()
			dst, targetIndex := memoryPinecone()
			for _, client := range []*pcstore.Client{src, dst} {
				require.NoError(t, client.Upsert(t.Context(), "", live))
			}
			key, err := vectorKey(true, snap.DocumentRevision, snap.Chunks[tc.index])
			require.NoError(t, err)
			v := hostedPositionVerifier(snap, src, dst)
			setHostedPosition(t, targetIndex, key, tc.position)
			rep, err := v.run(t.Context())
			require.NoError(t, err)
			require.False(t, rep.Passed)
			require.Equal(t, 1.0, rep.Coverage.KeyCoverage)
			require.Equal(t, 0.5, rep.Coverage.ProfileCoverage)
			require.Equal(t, 1, rep.Coverage.Mismatched)
			require.Zero(t, rep.Coverage.Missing)
			require.Zero(t, rep.Coverage.BadVectors)
			require.Equal(t, 2, rep.Sample.Queries)

			setHostedPosition(t, targetIndex, key, structpb.NewNumberValue(float64(tc.index)))
			setHostedPosition(t, sourceIndex, key, tc.position)
			rep, err = v.run(t.Context())
			require.NoError(t, err)
			require.False(t, rep.Passed)
			require.Equal(t, 1.0, rep.Coverage.ProfileCoverage)
			require.Equal(t, 1, rep.Sample.Queries)
			require.Equal(t, 1, rep.Sample.Skipped)

			setHostedPosition(t, sourceIndex, key, structpb.NewNumberValue(float64(tc.index)))
			rep, err = v.run(t.Context())
			require.NoError(t, err)
			require.True(t, rep.Passed, "%v", rep.Failures)
			require.Equal(t, 1.0, rep.Coverage.ProfileCoverage)
			require.Equal(t, 2, rep.Sample.Queries)
		})
	}

	t.Run("corrupt snapshot position", func(t *testing.T) {
		snap, live := hostedPositionSnapshot("bare")
		snap.Chunks[0].Index = -1
		for i := range live {
			live[i].VectorIncarnation = snap.Incarnation
		}
		client, index := memoryPinecone()
		require.NoError(t, client.Upsert(t.Context(), "", live))
		setHostedPosition(t, index, string(live[0].ID), structpb.NewNumberValue(-1))
		s := stager{profile: testProfile(sourceModel), copyFrom: client, opts: defaultOptions()}
		_, _, _, err := s.vectors(t.Context(), &snap)
		require.ErrorContains(t, err, "embedding call budget")
		v := hostedPositionVerifier(snap, client, client)
		rep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.Equal(t, 1, rep.Coverage.Mismatched)
		require.Equal(t, 1, rep.Sample.Skipped)
	})

	t.Run("layout fallback and bad vector precedence", func(t *testing.T) {
		snap, live := hostedPositionSnapshot("bare")
		for i := range live {
			live[i].VectorIncarnation = snap.Incarnation
		}
		live[0].Metadata[domain.LayoutPagesKey] = []int{1}
		src, sourceIndex := memoryPinecone()
		dst, targetIndex := memoryPinecone()
		for _, client := range []*pcstore.Client{src, dst} {
			require.NoError(t, client.Upsert(t.Context(), "", live))
		}
		key := string(live[0].ID)
		setHostedPosition(t, sourceIndex, key, nil)
		v := hostedPositionVerifier(snap, src, dst)
		rep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.Equal(t, 1, rep.Coverage.Mismatched)
		require.Equal(t, 1, rep.Sample.Skipped)

		setHostedPosition(t, sourceIndex, key, structpb.NewNumberValue(0))
		rep, err = v.run(t.Context())
		require.NoError(t, err)
		require.True(t, rep.Passed, "%v", rep.Failures)

		setHostedPosition(t, targetIndex, key, nil)
		updateHostedRecord(t, targetIndex, key, func(record *pinecone.Vector) { clear(*record.Values) })
		rep, err = v.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Passed)
		require.Equal(t, 1, rep.Coverage.BadVectors)
		require.Zero(t, rep.Coverage.Mismatched)
	})
}

func hostedPositionVerifier(snap pgstore.DocumentSnapshot, source, target *pcstore.Client) verifier {
	opts := defaultOptions()
	opts.SampleSize, opts.ScopeDocuments, opts.MinRecall, opts.MaxP95, opts.TopK = 2, 1, 1, time.Second, 2
	return verifier{
		corpus: snapshotCorpus{snap}, ledger: currentCheckpointLedger{doc: snap.ID, revision: snap.Revision, incarnation: snap.Incarnation},
		target: target, source: source, generation: "g2", taggedTarget: true, sourceTagged: true,
		profile: testProfile(sourceModel), sourceProfile: testProfile(sourceModel), opts: opts, now: time.Now,
	}
}

func TestVerifyTaggedSnapshotUnit(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Tagged verification chunk one.", "Tagged verification chunk two.")
	chunks := []domain.Chunk{
		{ID: domain.ChunkID(hexChunkID(doc.ID, 0)), DocumentID: domain.DocumentID(doc.ID), Index: 0, Text: doc.Texts[0], TokenCount: 4},
		{ID: domain.ChunkID(hexChunkID(doc.ID, 1)), DocumentID: domain.DocumentID(doc.ID), Index: 1, Text: doc.Texts[1], TokenCount: 4},
	}
	src, _ := memoryPinecone()
	require.NoError(t, src.Upsert(t.Context(), "", sourceChunks(doc, chunks, sourceModel)))

	targetChunks := sourceChunks(doc, chunks, sourceModel)
	for i := range targetChunks {
		targetChunks[i].VectorIncarnation = doc.Incarnation
	}
	dst, dstIdx := memoryPinecone()
	require.NoError(t, dst.Upsert(t.Context(), "", targetChunks))

	snapshot := pgstore.DocumentSnapshot{
		DocumentRevision: pgstore.DocumentRevision{
			ID: doc.ID, Incarnation: doc.Incarnation, DocType: doc.DocType, Collection: doc.Collection,
			DeclaredChunks: len(chunks), Chunks: len(chunks), TextBytes: int64(len(doc.Texts[0]) + len(doc.Texts[1])),
			Revision: "tagged-rev-1", ScopedVectors: true, VectorAttempt: doc.Attempt,
		},
		Chunks: chunks,
	}
	opts := defaultOptions()
	opts.SampleSize, opts.ScopeDocuments, opts.MinRecall, opts.MaxP95, opts.TopK = 2, 1, 1, time.Second, 2
	v := verifier{
		corpus: snapshotCorpus{snapshot}, ledger: currentCheckpointLedger{doc: doc.ID, revision: "tagged-rev-1", incarnation: doc.Incarnation},
		target: dst, source: src, generation: "g2", sourceTagged: true,
		profile: testProfile(sourceModel), sourceProfile: testProfile(sourceModel), taggedTarget: true, opts: opts, now: time.Now,
	}
	rep, err := v.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Passed, "%v", rep.Failures)
	require.Equal(t, 1.0, rep.Coverage.KeyCoverage)
	require.Equal(t, 1.0, rep.Coverage.ProfileCoverage)
	require.Equal(t, 2, rep.Sample.Queries)

	// A relational projection has the same chunks under their bare keys.
	relational, _ := memoryPinecone()
	plain := doc
	plain.Tagged = false
	require.NoError(t, relational.Upsert(t.Context(), "", sourceChunks(plain, chunks, sourceModel)))
	for _, pair := range []struct {
		source, target             vectorReader
		sourceTagged, targetTagged bool
	}{
		{src, relational, true, false},
		{relational, dst, false, true},
	} {
		cross := v
		cross.source, cross.target = pair.source, pair.target
		cross.sourceTagged, cross.taggedTarget = pair.sourceTagged, pair.targetTagged
		rep, err = cross.run(t.Context())
		require.NoError(t, err)
		require.True(t, rep.Passed, "%v", rep.Failures)
		require.Equal(t, 1.0, rep.Sample.MeanRecall)
		require.Equal(t, 1.0, rep.Sample.HitRate)
		require.Zero(t, rep.Sample.ForbiddenHits)
	}

	// A wrong attempt tag at the right physical key counts as Mismatched.
	badAttempt := v
	badAttempt.target = scriptedReader{vectorReader: dst, records: func(ids []string) map[string]domain.Chunk {
		got, fetchErr := dst.Fetch(t.Context(), ids)
		require.NoError(t, fetchErr)
		for k, c := range got {
			c.Metadata[domain.VectorAttemptKey] = uuid.NewString()
			got[k] = c
			break
		}
		return got
	}}
	rep, err = badAttempt.run(t.Context())
	require.NoError(t, err)
	require.False(t, rep.Passed)
	require.Equal(t, 1, rep.Coverage.Mismatched)

	// An extra old-attempt key counts as out of scope.
	oldSpec := doc
	oldSpec.Attempt = uuid.NewString()
	require.NoError(t, dst.Upsert(t.Context(), "", sourceChunks(oldSpec, chunks[:1], sourceModel)))
	require.Equal(t, 3, dstIdx.Len())
	rep, err = v.run(t.Context())
	require.NoError(t, err)
	require.False(t, rep.Passed)
	require.Equal(t, 1, rep.Unexpected.Records)
}

func TestRepublishedTaggedDocumentIsRestaged(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Initial tagged body.")
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	verify := verifyOptions("g2")
	verify.SampleSize = 1
	vRep, err := h.run(verify)
	require.NoError(t, err, "%v", vRep["failures"])
	require.Equal(t, true, vRep["passed"])

	// Requeue, claim and complete with a new lease token.
	_, err = f.pg.Pool.Exec(t.Context(), `UPDATE documents SET status='processing' WHERE id=$1`, doc.ID)
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(t.Context(), `INSERT INTO ingest_jobs(document_id, state) VALUES($1, 'pending')`, doc.ID)
	require.NoError(t, err)
	job, err := f.pg.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.True(t, job.ScopedVectors)
	require.NotEqual(t, doc.Attempt, job.LeaseToken)

	republished := doc
	republished.Attempt = job.LeaseToken
	newLiveChunks := sourceChunks(republished, f.chunks[doc.ID], sourceModel)
	require.NoError(t, pcstore.NewClientForIndex(h.stores.index("quarry"), testConfig(), "").Upsert(t.Context(), "", newLiveChunks))
	done, err := f.pg.CompleteJob(t.Context(), *job, f.chunks[doc.ID], domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)

	staleRep, err := h.run(verify)
	require.ErrorIs(t, err, errNotVerified)
	require.EqualValues(t, 1, staleRep["coverage"].(map[string]any)["documents_with_stale_checkpoint"])

	restaged, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, restaged["complete"])
	require.EqualValues(t, 1, restaged["staged_documents"])
	require.EqualValues(t, 1, restaged["pruned_keys"])

	newKey, err := pcstore.TaggedVectorID(doc.ID, doc.Incarnation, republished.Attempt, string(f.chunks[doc.ID][0].ID))
	require.NoError(t, err)
	require.Equal(t, []string{newKey}, h.stores.index("quarry-g2").IDs())

	vRep, err = h.run(verify)
	require.NoError(t, err, "%v", vRep["failures"])
	require.Equal(t, true, vRep["passed"])
}

func TestActivationRejectsRepublishedTaggedDocument(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Verified tagged body.")
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	_, err = h.run(options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	verify := verifyOptions("g2")
	verify.SampleSize = 1
	verify.Apply = true
	vr, err := h.run(verify)
	require.NoError(t, err, "%v", vr["failures"])

	// Re-publish with a new attempt after verification and before activation.
	_, err = f.pg.Pool.Exec(t.Context(), `UPDATE documents SET vector_attempt=$2 WHERE id=$1`, doc.ID, uuid.NewString())
	require.NoError(t, err)

	actRep, err := h.run(options{Generation: "g2", Activate: true, Quiesce: 0})
	require.Error(t, err)
	require.Contains(t, fmt.Sprint(actRep["reasons"]), "corpus changed")
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.ErrorContains(t, err, "corpus changed")
}

func TestVerifyChecksTaggedKeysAndTags(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Tagged verify check.")
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)

	verify := verifyOptions("g2")
	verify.SampleSize = 1
	rep, err := h.run(verify)
	require.NoError(t, err, "%v", rep["failures"])
	require.Equal(t, true, rep["passed"])
	cov := rep["coverage"].(map[string]any)
	require.EqualValues(t, 1, cov["key_coverage"])
	require.EqualValues(t, 1, cov["profile_coverage"])

	// A wrong attempt tag at the right physical key counts Mismatched.
	targetIdx := h.stores.index("quarry-g2")
	targetClient := pcstore.NewClientForIndex(targetIdx, testConfig(), "")
	sourceClient := pcstore.NewClientForIndex(h.stores.index("quarry"), testConfig(), "")
	vOpts := defaultOptions()
	vOpts.SampleSize, vOpts.ScopeDocuments, vOpts.MinRecall, vOpts.MaxP95, vOpts.TopK = 1, 1, 1, time.Second, 2
	wrongTagVerifier := verifier{
		corpus: f.pg, ledger: f.pg,
		target: scriptedReader{vectorReader: targetClient, records: func(ids []string) map[string]domain.Chunk {
			got, fetchErr := targetClient.Fetch(t.Context(), ids)
			require.NoError(t, fetchErr)
			for k, c := range got {
				c.Metadata[domain.VectorAttemptKey] = uuid.NewString()
				got[k] = c
			}
			return got
		}},
		source: sourceClient, generation: "g2", sourceTagged: true,
		profile: testProfile(sourceModel), sourceProfile: testProfile(sourceModel), taggedTarget: true, opts: vOpts, now: time.Now,
	}
	vRep, err := wrongTagVerifier.run(t.Context())
	require.NoError(t, err)
	require.False(t, vRep.Passed)
	require.Equal(t, 1, vRep.Coverage.Mismatched)

	// An extra old-attempt key counts as out of scope.
	oldSpec := doc
	oldSpec.Attempt = uuid.NewString()
	require.NoError(t, targetClient.Upsert(t.Context(), "", sourceChunks(oldSpec, f.chunks[doc.ID], sourceModel)))
	extraRep, err := h.run(verify)
	require.ErrorIs(t, err, errNotVerified)
	require.EqualValues(t, 1, extraRep["unexpected"].(map[string]any)["out_of_scope_records"])
}

func TestVerifySamplesTaggedChunks(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Sample chunk one.", "Sample chunk two.", "Sample chunk three.")
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)

	verify := verifyOptions("g2")
	verify.SampleSize = 3
	rep, err := h.run(verify)
	require.NoError(t, err, "%v", rep["failures"])
	require.Equal(t, true, rep["passed"])
	sample := rep["sample"].(map[string]any)
	require.EqualValues(t, 3, sample["queries"])
	require.EqualValues(t, 1, sample["mean_recall"])
	require.EqualValues(t, 1, sample["self_hit_rate"])
}

func TestTaggedDocumentIsVisibleAfterCutover(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Visible tagged chunk after cutover.")
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	_, err = h.run(options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	verify := verifyOptions("g2")
	verify.SampleSize = 1
	verify.Apply = true
	vr, err := h.run(verify)
	require.NoError(t, err, "%v", vr["failures"])
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)

	targetClient := pcstore.NewClientForIndex(h.stores.index("quarry-g2"), testConfig(), "")
	hits, err := targetClient.QueryFiltered(t.Context(), embedWith(sourceModel, doc.Texts[0]), 5, domain.VectorFilter{DocumentIDs: []string{doc.ID}, Collection: doc.Collection})
	require.NoError(t, err)
	require.Len(t, hits, 1)

	vis := pgstore.Visibility{Pool: f.pg.Pool, Collection: doc.Collection, DocType: doc.DocType}
	visible, err := vis.VisibleEvidence(t.Context(), hits)
	require.NoError(t, err)
	require.Len(t, visible, 1)
	require.Equal(t, f.chunks[doc.ID][0].ID, visible[0].Chunk.ID)
	require.Equal(t, doc.Incarnation, visible[0].ObservedIncarnation)
}

func TestTaggedDocumentDeletionAfterCutover(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Deleted after cutover.")
	kept := newTaggedDoc("alpha", "report", "Kept after cutover.")
	f := newTaggedFixture(t, doc, kept)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	_, err = h.run(options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	verify := verifyOptions("g2")
	verify.SampleSize = 2
	verify.Apply = true
	vr, err := h.run(verify)
	require.NoError(t, err, "%v", vr["failures"])
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)

	require.NoError(t, f.pg.PinVectorGeneration(t.Context(), "g2", "pinecone", "quarry-g2", "quarry-g2-host", testProfile(sourceModel)))
	targetClient := pcstore.NewClientForIndex(h.stores.index("quarry-g2"), testConfig(), "")
	_, err = f.pg.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	err = f.pg.DeleteWithCleanup(t.Context(), domain.DocumentID(doc.ID), domain.DeleteOperations{
		ExpectedIncarnation: doc.Incarnation,
		VectorIndex:         "quarry-g2",
		RevisionVectors: func(ctx context.Context, id, inc string) error {
			return targetClient.DeleteByRevision(ctx, id, inc)
		},
	})
	require.NoError(t, err)

	keptKey, err := pcstore.TaggedVectorID(kept.ID, kept.Incarnation, kept.Attempt, string(f.chunks[kept.ID][0].ID))
	require.NoError(t, err)
	require.Equal(t, []string{keptKey}, h.stores.index("quarry-g2").IDs())

	// A later staging run on the new generation prunes nothing for the deleted document.
	h.deps.cfg.VectorStore, h.deps.cfg.PineconeIndexName, h.deps.cfg.VectorGeneration = "pinecone", "quarry-g2", "g2"
	rep, err := h.run(options{Generation: "g3", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g3", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, rep["complete"])
	require.EqualValues(t, 0, rep["pruned_documents"])
	require.EqualValues(t, 0, rep["pruned_keys"])
	require.Equal(t, []string{keptKey}, h.stores.index("quarry-g3").IDs())
}

// snapshotsCorpus serves several documents in ascending ID order.
type snapshotsCorpus struct{ snapshots []pgstore.DocumentSnapshot }

func newSnapshotsCorpus(snapshots ...pgstore.DocumentSnapshot) snapshotsCorpus {
	sorted := append([]pgstore.DocumentSnapshot(nil), snapshots...)
	slices.SortFunc(sorted, func(a, b pgstore.DocumentSnapshot) int { return strings.Compare(a.ID, b.ID) })
	return snapshotsCorpus{snapshots: sorted}
}

func (c snapshotsCorpus) DocumentRevisions(_ context.Context, after string, limit int) ([]pgstore.DocumentRevision, error) {
	var out []pgstore.DocumentRevision
	for _, snap := range c.snapshots {
		if snap.ID > after && len(out) < limit {
			out = append(out, snap.DocumentRevision)
		}
	}
	return out, nil
}

func (c snapshotsCorpus) HasDocumentsAfter(_ context.Context, after string) (bool, error) {
	return len(c.snapshots) > 0 && c.snapshots[len(c.snapshots)-1].ID > after, nil
}

func (c snapshotsCorpus) LoadDocumentSnapshotBounded(_ context.Context, id string, _ int, _ int64) (*pgstore.DocumentSnapshot, error) {
	for _, snap := range c.snapshots {
		if snap.ID == id {
			return &snap, nil
		}
	}
	return nil, nil
}

func chunksOf(doc docSpec) []domain.Chunk {
	chunks := make([]domain.Chunk, len(doc.Texts))
	for i, text := range doc.Texts {
		chunks[i] = domain.Chunk{ID: domain.ChunkID(hexChunkID(doc.ID, i)), DocumentID: domain.DocumentID(doc.ID), Index: i, Text: text, TokenCount: 4}
	}
	return chunks
}

func snapshotOf(doc docSpec, chunks []domain.Chunk, revision string) pgstore.DocumentSnapshot {
	var textBytes int64
	for _, c := range chunks {
		textBytes += int64(len(c.Text))
	}
	return pgstore.DocumentSnapshot{
		DocumentRevision: pgstore.DocumentRevision{
			ID: doc.ID, Incarnation: doc.Incarnation, DocType: doc.DocType, Collection: doc.Collection,
			DeclaredChunks: len(chunks), Chunks: len(chunks), TextBytes: textBytes, Revision: revision,
		},
		Chunks: chunks,
	}
}

// A corpus mixing untagged, tagged, manifested and tagged-and-manifested
// documents stages under each document's own key form, verifies, and is refused
// per document by a PostgreSQL target.
func TestStageAndVerifyMixedKeyForms(t *testing.T) {
	plain := newDoc("alpha", "report", "Plain neighbor chunk.")
	plain.Incarnation = plain.ID
	tagged := newTaggedDoc("alpha", "report", "Tagged neighbor chunk one.", "Tagged neighbor chunk two.")
	manifestRev, bothRev := uuid.NewString(), uuid.NewString()
	manifestedDoc, manifestedSnap, manifestedLive := manifestedSnapshot(manifestRev, sourceModel)
	bothDoc, bothSnap, bothLive := manifestedSnapshot(bothRev, sourceModel)
	bothSnap.ScopedVectors, bothSnap.VectorAttempt = true, uuid.NewString()

	plainChunks, taggedChunks := chunksOf(plain), chunksOf(tagged)
	snapshots := []pgstore.DocumentSnapshot{
		snapshotOf(plain, plainChunks, "plain-rev"),
		snapshotOf(tagged, taggedChunks, "tagged-rev"),
		manifestedSnap,
		bothSnap,
	}
	snapshots[1].ScopedVectors, snapshots[1].VectorAttempt = true, tagged.Attempt
	corpus := newSnapshotsCorpus(snapshots...)

	src, _ := memoryPinecone()
	for _, records := range [][]domain.Chunk{sourceChunks(plain, plainChunks, sourceModel), sourceChunks(tagged, taggedChunks, sourceModel), manifestedLive, bothLive} {
		require.NoError(t, src.Upsert(t.Context(), "", records))
	}

	var want []string
	for _, c := range plainChunks {
		want = append(want, string(c.ID))
	}
	for _, c := range taggedChunks {
		want = append(want, tagged.ID+"/"+tagged.Incarnation+"/"+tagged.Attempt+"/"+string(c.ID))
	}
	for i := range manifestedLive {
		want = append(want, domain.FormatRevisionVectorID(domain.DocumentID(manifestedDoc.ID), manifestRev, i))
		want = append(want, domain.FormatRevisionVectorID(domain.DocumentID(bothDoc.ID), bothRev, i))
	}
	slices.Sort(want)

	dst, _ := memoryPinecone()
	led := newMemoryManifestLedger()
	opts := defaultOptions()
	s := &stager{corpus: corpus, ledger: led, target: pineconeStore{dst}, generation: "g2", taggedTarget: true, copyFromTagged: true,
		profile: testProfile(sourceModel), opts: opts, copyFrom: src}
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete)
	require.Equal(t, 4, rep.Staged)
	require.Empty(t, rep.Unsupported)
	require.Equal(t, len(want), rep.Copied, "every record is copied from its own key form")
	require.Zero(t, rep.Embedded)
	require.Equal(t, want, allKeys(t, pineconeStore{dst}))

	vopts := defaultOptions()
	vopts.Verify, vopts.SampleSize, vopts.TopK, vopts.ScopeDocuments, vopts.MinRecall, vopts.MaxP95 = true, len(want), 2, 2, 1, time.Second
	v := &verifier{corpus: corpus, ledger: led, target: dst, source: src, generation: "g2", profile: testProfile(sourceModel),
		sourceProfile: testProfile(sourceModel), taggedTarget: true, sourceTagged: true, opts: vopts, now: time.Now}
	vrep, err := v.run(t.Context())
	require.NoError(t, err)
	require.True(t, vrep.Passed, "%v", vrep.Failures)
	require.Equal(t, 1.0, vrep.Coverage.KeyCoverage)
	require.Zero(t, vrep.Unexpected.Records)
	require.Zero(t, vrep.Sample.ForbiddenHits)

	// A bare record of the tagged document is out of scope on a hosted target.
	require.NoError(t, dst.Upsert(t.Context(), "", sourceChunks(plain, []domain.Chunk{{ID: taggedChunks[0].ID, DocumentID: domain.DocumentID(tagged.ID), Index: 0, Text: tagged.Texts[0], TokenCount: 4}}, sourceModel)))
	vrep, err = v.run(t.Context())
	require.NoError(t, err)
	require.False(t, vrep.Passed)
	require.Equal(t, 1, vrep.Unexpected.Records)

	// A PostgreSQL target stages the plain and tagged documents under bare keys.
	pgDst, _ := memoryPinecone()
	pgStage := &stager{corpus: corpus, ledger: newMemoryManifestLedger(), target: pineconeStore{pgDst}, generation: "g3", taggedTarget: false,
		profile: testProfile(sourceModel), opts: opts, copyFrom: src, copyFromTagged: true}
	pgRep, err := pgStage.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, pgRep.Staged)
	require.ElementsMatch(t, []string{manifestedDoc.ID, bothDoc.ID}, pgRep.Unsupported)
}

func TestMixedKeyFormsSurviveCutover(t *testing.T) {
	tagged := newTaggedDoc("alpha", "report", "Tagged document beside manifested ones.")
	plain := newDoc("alpha", "report", "Untagged document beside manifested ones.")
	f := newTaggedFixture(t, tagged, plain)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	ctx := t.Context()
	live := h.stores.index("quarry")

	manifestedID, manifestedRev, manifestedChunks := seedManifestedPineconeDoc(t, f.pg, live, "Manifested document beside a tagged one.")
	bothID, bothRev, _ := seedManifestedPineconeDoc(t, f.pg, live, "Manifested document that is also marked tagged.")
	_, err := f.pg.Pool.Exec(ctx, `UPDATE documents SET scoped_vectors=TRUE, vector_attempt=$2 WHERE id=$1`, bothID, uuid.NewString())
	require.NoError(t, err)

	rep, err := h.run(options{Generation: "g2", Profile: h.profileFile("same.json", testProfile(sourceModel)), TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, rep["complete"])
	require.EqualValues(t, 4, rep["staged_documents"])
	require.Empty(t, rep["unsupported_documents"])

	taggedKey, err := pcstore.TaggedVectorID(tagged.ID, tagged.Incarnation, tagged.Attempt, string(f.chunks[tagged.ID][0].ID))
	require.NoError(t, err)
	wantKeys := []string{
		taggedKey,
		string(f.chunks[plain.ID][0].ID),
		domain.FormatRevisionVectorID(domain.DocumentID(manifestedID), manifestedRev, 0),
		domain.FormatRevisionVectorID(domain.DocumentID(bothID), bothRev, 0),
	}
	slices.Sort(wantKeys)

	_, err = h.run(options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	// Verification fails unless every requested sample query runs, so the sample
	// is as large as the corpus: one query per chunk.
	verify := verifyOptions("g2")
	verify.SampleSize = len(wantKeys)
	verify.Apply = true
	vrep, err := h.run(verify)
	require.NoError(t, err, "%v", vrep["failures"])
	require.Equal(t, true, vrep["passed"], "%v", vrep["failures"])
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)

	require.Equal(t, wantKeys, h.stores.index("quarry-g2").IDs())

	target := pcstore.NewClientForIndex(h.stores.index("quarry-g2"), testConfig(), "")
	vis := pgstore.Visibility{Pool: f.pg.Pool}
	for _, probe := range []struct {
		id         string
		text       string
		collection string
	}{
		{tagged.ID, tagged.Texts[0], "alpha"},
		{plain.ID, plain.Texts[0], "alpha"},
		{manifestedID, manifestedChunks[0].Text, "catalog"},
	} {
		hits, queryErr := target.QueryFiltered(ctx, embedWith(sourceModel, probe.text), 5, domain.VectorFilter{DocumentIDs: []string{probe.id}, Collection: probe.collection})
		require.NoError(t, queryErr)
		require.Len(t, hits, 1, probe.id)
		visible, visErr := vis.VisibleEvidence(ctx, hits)
		require.NoError(t, visErr)
		require.Len(t, visible, 1, "document %s stays visible after cutover", probe.id)
	}

	// A later staging run keeps every form: nothing is stale and nothing is pruned.
	h.deps.cfg.VectorStore, h.deps.cfg.PineconeIndexName, h.deps.cfg.VectorGeneration = "pinecone", "quarry-g2", "g2"
	again, err := h.run(options{Generation: "g3", Profile: h.profileFile("same3.json", testProfile(sourceModel)), TargetStore: "pinecone", TargetIndex: "quarry-g3", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, again["complete"])
	require.Empty(t, again["unsupported_documents"])
	require.Equal(t, wantKeys, h.stores.index("quarry-g3").IDs())
}

// A live record that carries only a newer attempt than the snapshot names a
// different physical key, so it is never copied and the vector is computed.
func TestStageIgnoresNewerAttemptRecordsUnit(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Published attempt chunk.")
	chunks := chunksOf(doc)
	newer := doc
	newer.Attempt = uuid.NewString()
	src, _ := memoryPinecone()
	require.NoError(t, src.Upsert(t.Context(), "", sourceChunks(newer, chunks, sourceModel)))

	snapshot := snapshotOf(doc, chunks, "tagged-rev-1")
	snapshot.ScopedVectors, snapshot.VectorAttempt = true, doc.Attempt
	target, targetIdx := memoryPinecone()
	opts := defaultOptions()
	opts.MaxEmbedCalls = 10
	embedder := &countingEmbedder{model: sourceModel}
	s := &stager{corpus: snapshotCorpus{snapshot}, ledger: &recordingLedger{}, target: pineconeStore{target}, generation: "g2", taggedTarget: true, copyFromTagged: true,
		profile: testProfile(sourceModel), opts: opts, copyFrom: src, embed: embedder}
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete)
	require.Zero(t, rep.Copied)
	require.Equal(t, 1, rep.Embedded)
	require.Equal(t, []string{doc.ID + "/" + doc.Incarnation + "/" + doc.Attempt + "/" + string(chunks[0].ID)}, targetIdx.IDs())
}

func TestTaggedDocumentRoundTripsThroughPostgres(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "An owned tagged chunk in both stores.")
	distractor := newDoc("beta", "report", "Another collection is outside the authorized query.")
	f := newTaggedFixture(t, doc, distractor)
	ctx := t.Context()
	tenant, principal := uuid.NewString(), uuid.NewString()
	assignDocTenant(t, f.pg, doc, tenant, principal)
	h := newHarness(t, f, "pinecone")
	original := pcstore.NewClientForIndex(h.stores.index("quarry"), testConfig(), "")
	source := sourceChunks(doc, f.chunks[doc.ID], sourceModel)
	source[0].VectorIncarnation = ""
	source[0].Metadata[domain.VectorTenantKey] = tenant
	require.NoError(t, original.Upsert(ctx, "", source))
	candidates, err := f.pg.BackfillCandidates(ctx, "", 2)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, doc.ID, candidates[0].DocumentID)
	require.Equal(t, doc.Attempt, candidates[0].Attempt)
	published, err := f.pg.PublishVerifiedDocument(ctx, candidates[0])
	require.NoError(t, err)
	require.True(t, published)
	originalRevision, err := f.pg.LoadDocumentSnapshotBounded(ctx, doc.ID, 2, 4096)
	require.NoError(t, err)
	require.False(t, originalRevision.VectorIncarnationRequired)

	serving := useRestrictedTaggedRoundTripRoles(t, h, "g2")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))
	toPG := options{Generation: "g2", Profile: profile, TargetStore: "postgres", Apply: true}
	report, err := h.run(toPG)
	require.NoError(t, err, "%v", report)
	require.EqualValues(t, 2, report["copied_chunks"])
	require.Zero(t, h.embedCalls())

	verifyTaggedCutover(t, h, "g2", 2)
	target, err := pgstore.OpenGenerationVectorStore(ctx, serving, testDims, "g2", pgstore.WithExactScopeMax(10))
	require.NoError(t, err)
	key := string(f.chunks[doc.ID][0].ID)
	cp, err := f.pg.GenerationCheckpoints(ctx, "g2", []string{doc.ID})
	require.NoError(t, err)
	require.Equal(t, []string{key}, cp[doc.ID].ChunkIDs)
	records, err := target.Fetch(ctx, []string{key})
	require.NoError(t, err)
	require.Contains(t, records, key)
	require.NotContains(t, records[key].Metadata, domain.VectorRevisionKey)
	require.NotContains(t, records[key].Metadata, domain.VectorAttemptKey)

	subject := domain.Principal{ID: principal, TenantID: tenant}
	hits, err := target.QueryFiltered(ctx, source[0].Embedding, 2, domain.VectorFilter{
		Collection: doc.Collection, Authorized: &domain.AuthorizedScope{TenantID: tenant, PrincipalID: principal},
	})
	require.NoError(t, err)
	vis := pgstore.Visibility{Pool: serving.Pool, Subject: &subject}
	visible, err := vis.VisibleEvidence(ctx, hits)
	require.NoError(t, err)
	require.Len(t, visible, 1)
	require.Equal(t, doc.Attempt, visible[0].Chunk.Metadata[domain.VectorAttemptKey])
	require.Equal(t, doc.Incarnation, visible[0].Chunk.VectorIncarnation)

	h.deps.cfg.VectorStore, h.deps.cfg.VectorGeneration = "postgres", "g2"
	require.EqualValues(t, 2, h.stagePinecone("g3", "quarry-g3", sourceModel, 0)["copied_chunks"])
	require.Zero(t, h.embedCalls())
	verifyTaggedCutover(t, h, "g3", 2)
	hostedKey, err := pcstore.TaggedVectorID(doc.ID, doc.Incarnation, doc.Attempt, key)
	require.NoError(t, err)
	hosted := pcstore.NewClientForIndex(h.stores.index("quarry-g3"), testConfig(), "")
	hostedRecords, err := hosted.Fetch(ctx, []string{hostedKey})
	require.NoError(t, err)
	got := hostedRecords[hostedKey]
	require.Equal(t, doc.Incarnation, got.VectorIncarnation)
	require.Equal(t, doc.Incarnation, got.Metadata[domain.VectorRevisionKey])
	require.Equal(t, doc.Attempt, got.Metadata[domain.VectorAttemptKey])
	require.Equal(t, tenant, got.Metadata[domain.VectorTenantKey])
	after, err := f.pg.LoadDocumentSnapshotBounded(ctx, doc.ID, 2, 4096)
	require.NoError(t, err)
	require.Equal(t, originalRevision, after)
}

func verifyTaggedCutover(t *testing.T, h *harness, generation string, chunks int) {
	t.Helper()
	_, err := h.run(options{Generation: generation, Freeze: true, Apply: true})
	require.NoError(t, err)
	verify := verifyOptions(generation)
	verify.SampleSize, verify.Apply, verify.MinRecall = chunks, true, 1
	report, err := h.run(verify)
	require.NoError(t, err, "%v", report["failures"])
	coverage := report["coverage"].(map[string]any)
	require.EqualValues(t, 1, coverage["key_coverage"])
	require.EqualValues(t, 1, coverage["profile_coverage"])
	require.EqualValues(t, 0, report["sample"].(map[string]any)["forbidden_hits"])
	switched, err := h.run(options{Generation: generation, Activate: true, Apply: true})
	require.NoError(t, err, "%v", switched)
	require.Equal(t, generation, switched["active_generation"])
	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)
}

func TestRollbackTaggedDocumentAfterPostgresRepublication(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Same document body and key after publication.")
	plain := newDoc("beta", "note", "The untagged source revision stays the same.")
	f := newTaggedFixture(t, doc, plain)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	ctx := t.Context()
	profile := h.profileFile("same.json", testProfile(sourceModel))
	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "postgres", Apply: true})
	require.NoError(t, err)
	verifyTaggedCutover(t, h, "g2", 2)
	h.deps.cfg.VectorStore, h.deps.cfg.VectorGeneration = "postgres", "g2"
	require.NoError(t, f.pg.PinVectorGeneration(ctx, "g2", "postgres", pgstore.VectorTable("g2"), "", testProfile(sourceModel)))
	target, err := pgstore.OpenGenerationVectorStore(ctx, f.pg, testDims, "g2")
	require.NoError(t, err)
	target.RouteCompletion()
	before, err := f.pg.LoadDocumentSnapshotBounded(ctx, plain.ID, 2, 4096)
	require.NoError(t, err)

	_, err = f.pg.Pool.Exec(ctx, `UPDATE documents SET status='processing' WHERE id=$1`, doc.ID)
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(ctx, `INSERT INTO ingest_jobs(document_id,state) VALUES($1,'pending')`, doc.ID)
	require.NoError(t, err)
	job, err := f.pg.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.True(t, job.ScopedVectors)
	require.NotEqual(t, doc.Attempt, job.LeaseToken)
	done, err := f.pg.CompleteJob(ctx, *job, sourceChunks(doc, f.chunks[doc.ID], sourceModel), domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)

	_, err = h.run(options{Generation: "legacy", Freeze: true, Apply: true})
	require.NoError(t, err)
	old := verifyOptions("legacy")
	old.SampleSize, old.Apply = 2, true
	_, err = h.run(old)
	require.ErrorIs(t, err, errNotVerified)
	report, err := h.run(options{Generation: "legacy", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry", Apply: true})
	require.NoError(t, err, "%v", report)
	require.EqualValues(t, 2, report["copied_chunks"])
	require.EqualValues(t, 1, report["pruned_keys"])
	report, err = h.run(old)
	require.NoError(t, err, "%v", report["failures"])
	_, err = h.run(options{Generation: "legacy", Activate: true, Apply: true})
	require.NoError(t, err)
	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)
	key, err := pcstore.TaggedVectorID(doc.ID, doc.Incarnation, job.LeaseToken, string(f.chunks[doc.ID][0].ID))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{key, string(f.chunks[plain.ID][0].ID)}, h.stores.index("quarry").IDs())
	after, err := f.pg.LoadDocumentSnapshotBounded(ctx, plain.ID, 2, 4096)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRetiredPostgresPrunesNonRelationalKeys(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "A tagged document under a bare relational key.")
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	ctx := t.Context()
	profile := h.profileFile("same.json", testProfile(sourceModel))
	stage := options{Generation: "g2", Profile: profile, TargetStore: "postgres", Apply: true}
	_, err := h.run(stage)
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(ctx, `UPDATE vector_generations SET state='retired', ever_active=TRUE WHERE name='g2'`)
	require.NoError(t, err)
	target, err := pgstore.OpenGenerationVectorStore(ctx, f.pg, testDims, "g2")
	require.NoError(t, err)
	current := f.chunks[doc.ID][0]
	tagged, err := vectorKey(true, pgstore.DocumentRevision{ID: doc.ID, Incarnation: doc.Incarnation, VectorAttempt: doc.Attempt, ScopedVectors: true}, current)
	require.NoError(t, err)
	for _, wrong := range []string{tagged, doc.ID + "#" + uuid.NewString() + "#0", "malformed"} {
		chunk := sourceChunks(doc, []domain.Chunk{current}, sourceModel)[0]
		chunk.ID = domain.ChunkID(wrong)
		require.NoError(t, target.Upsert(ctx, "", []domain.Chunk{chunk}))
	}
	require.NoError(t, f.pg.FreezeWriters(ctx, "g2"))
	report, err := h.run(stage)
	require.NoError(t, err, "%v", report)
	require.EqualValues(t, 3, report["pruned_keys"])
	keys, next, err := target.ListIDs(ctx, "", 10)
	require.NoError(t, err)
	require.Empty(t, next)
	require.Equal(t, []string{string(current.ID)}, keys)
}

type betweenTaggedBatches struct {
	vectorTarget
	afterFirstWrite func()
	failNext        error
	firstWritten    bool
}

func (t *betweenTaggedBatches) Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error {
	if t.firstWritten && t.failNext != nil {
		return t.failNext
	}
	if err := t.vectorTarget.Upsert(ctx, namespace, chunks); err != nil {
		return err
	}
	if !t.firstWritten {
		t.firstWritten = true
		if t.afterFirstWrite != nil {
			t.afterFirstWrite()
		}
	}
	return nil
}

func TestPostgresTaggedStageResumesWithoutLosingCheckpoint(t *testing.T) {
	for _, reason := range []error{errInjected, context.Canceled} {
		t.Run(reason.Error(), func(t *testing.T) {
			doc := newTaggedDoc("alpha", "report", "First checkpointed record.", "Second checkpointed record.")
			f := newTaggedFixture(t, doc)
			target := pgTarget(t, f.pg, "g2")
			s := f.stager(t, "g2", &betweenTaggedBatches{vectorTarget: target, failNext: reason}, sourceModel, f.legacy, nil)
			s.opts.Batch = 1
			_, err := s.run(t.Context())
			require.ErrorIs(t, err, reason)
			cps, err := f.pg.GenerationCheckpoints(t.Context(), "g2", []string{doc.ID})
			require.NoError(t, err)
			require.Contains(t, cps, doc.ID)
			require.False(t, cps[doc.ID].Complete)
			var want []string
			for _, chunk := range f.chunks[doc.ID] {
				want = append(want, string(chunk.ID))
			}
			require.ElementsMatch(t, want, cps[doc.ID].ChunkIDs)
			partialKeys, _, err := target.ListIDs(t.Context(), "", 10)
			require.NoError(t, err)
			require.Equal(t, want[:1], partialKeys)

			s.target = target
			s.opts.MaxChunks = 1
			_, err = s.run(t.Context())
			require.ErrorContains(t, err, "alone exceeds")
			s.opts.MaxChunks = 2
			report, err := s.run(t.Context())
			require.NoError(t, err)
			require.True(t, report.Complete)
			require.Equal(t, 2, report.Copied)
			cps, err = f.pg.GenerationCheckpoints(t.Context(), "g2", []string{doc.ID})
			require.NoError(t, err)
			require.True(t, cps[doc.ID].Complete)
			require.ElementsMatch(t, want, cps[doc.ID].ChunkIDs)
		})
	}
}

func TestPostgresStageRejectsStaleCheckpointAfterTaggedPublication(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "same incarnation new attempt"
		if replace {
			name = "deleted and recreated UUID"
		}
		t.Run(name, func(t *testing.T) {
			doc := newTaggedDoc("alpha", "report", "Same body and physical key.", "Same second body and key.")
			f := newTaggedFixture(t, doc)
			h := newHarness(t, f, "postgres")
			h.attestSource()
			ctx := t.Context()
			target := pgTarget(t, f.pg, "g2")
			before, err := f.pg.LoadDocumentSnapshotBounded(ctx, doc.ID, 2, 4096)
			require.NoError(t, err)
			change := func() {
				writeCtx := t.Context()
				if replace {
					_, deleteErr := f.pg.Pool.Exec(writeCtx, `DELETE FROM documents WHERE id=$1`, doc.ID)
					require.NoError(t, deleteErr)
					replacement := doc
					replacement.Incarnation, replacement.Attempt = uuid.NewString(), uuid.NewString()
					f.add(t, replacement)
					return
				}
				f.legacy.RouteCompletion()
				_, queueErr := f.pg.Pool.Exec(writeCtx, `UPDATE documents SET status='processing' WHERE id=$1`, doc.ID)
				require.NoError(t, queueErr)
				_, queueErr = f.pg.Pool.Exec(writeCtx, `INSERT INTO ingest_jobs(document_id,state) VALUES($1,'pending')`, doc.ID)
				require.NoError(t, queueErr)
				job, claimErr := f.pg.ClaimJob(writeCtx, "worker", time.Minute, 5)
				require.NoError(t, claimErr)
				require.NotNil(t, job)
				done, completeErr := f.pg.CompleteJob(writeCtx, *job, sourceChunks(doc, f.chunks[doc.ID], sourceModel), domain.IndexObservation{})
				require.NoError(t, completeErr)
				require.True(t, done)
			}
			s := f.stager(t, "g2", &betweenTaggedBatches{vectorTarget: target, afterFirstWrite: change}, sourceModel, f.legacy, nil)
			s.opts.Batch = 1
			report, err := s.run(ctx)
			require.NoError(t, err)
			require.True(t, report.Complete)
			current, err := f.pg.LoadDocumentSnapshotBounded(ctx, doc.ID, 2, 4096)
			require.NoError(t, err)
			require.NotEqual(t, before.Revision, current.Revision)
			require.Equal(t, before.Chunks, current.Chunks)
			_, err = h.run(options{Generation: "g2", Freeze: true, Apply: true})
			require.NoError(t, err)
			verify := verifyOptions("g2")
			verify.SampleSize, verify.Apply = 2, true
			bad, err := h.run(verify)
			require.ErrorIs(t, err, errNotVerified)
			require.EqualValues(t, 1, bad["coverage"].(map[string]any)["documents_with_stale_checkpoint"])
			_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
			require.Error(t, err)
			stage := options{Generation: "g2", Profile: h.profileFile("same.json", testProfile(sourceModel)), TargetStore: "postgres", Apply: true}
			restaged, err := h.run(stage)
			require.NoError(t, err)
			require.EqualValues(t, 2, restaged["copied_chunks"])
			good, err := h.run(verify)
			require.NoError(t, err, "%v", good["failures"])
			cps, err := f.pg.GenerationCheckpoints(ctx, "g2", []string{doc.ID})
			require.NoError(t, err)
			require.Equal(t, current.Revision, cps[doc.ID].SourceRevision)
			require.Equal(t, current.Incarnation, cps[doc.ID].Incarnation)
			_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
			require.NoError(t, err)
		})
	}
}
