package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

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

	// A PostgreSQL target refuses the tagged and the manifested documents, never the plain one.
	pgDst, _ := memoryPinecone()
	pgStage := &stager{corpus: corpus, ledger: newMemoryManifestLedger(), target: pineconeStore{pgDst}, generation: "g3", taggedTarget: false,
		profile: testProfile(sourceModel), opts: opts, copyFrom: src}
	pgRep, err := pgStage.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, pgRep.Staged)
	require.ElementsMatch(t, []string{tagged.ID, manifestedDoc.ID, bothDoc.ID}, pgRep.Unsupported)
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
