package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type memoryManifestLedger struct {
	mu          sync.Mutex
	recorded    map[string][]string
	checkpoints map[string]pgstore.Checkpoint
}

func newMemoryManifestLedger() *memoryManifestLedger {
	return &memoryManifestLedger{
		recorded:    map[string][]string{},
		checkpoints: map[string]pgstore.Checkpoint{},
	}
}

func (l *memoryManifestLedger) GenerationCheckpoints(_ context.Context, _ string, documentIDs []string) (map[string]pgstore.Checkpoint, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]pgstore.Checkpoint, len(documentIDs))
	for _, id := range documentIDs {
		if cp, ok := l.checkpoints[id]; ok {
			out[id] = cp
		}
	}
	return out, nil
}

func (l *memoryManifestLedger) BeginGenerationDocument(_ context.Context, _, documentID string, chunkIDs []string) ([]string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	prev := append([]string(nil), l.recorded[documentID]...)
	seen := make(map[string]bool, len(prev))
	for _, id := range prev {
		seen[id] = true
	}
	merged := append([]string(nil), prev...)
	for _, id := range chunkIDs {
		if !seen[id] {
			seen[id] = true
			merged = append(merged, id)
		}
	}
	l.recorded[documentID] = merged
	return prev, nil
}

func (l *memoryManifestLedger) CompleteGenerationDocument(_ context.Context, _, documentID, revision, incarnation string, chunkIDs []string, copied, embedded int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recorded[documentID] = append([]string(nil), chunkIDs...)
	l.checkpoints[documentID] = pgstore.Checkpoint{
		DocumentID:     documentID,
		SourceRevision: revision,
		Incarnation:    incarnation,
		ChunkIDs:       append([]string(nil), chunkIDs...),
		Complete:       true,
		Copied:         copied,
		Embedded:       embedded,
	}
	return nil
}

func (*memoryManifestLedger) OrphanedGenerationDocuments(context.Context, string, int) ([]pgstore.Checkpoint, error) {
	return nil, nil
}

func (*memoryManifestLedger) HasOrphanedGenerationDocuments(context.Context, string) (bool, error) {
	return false, nil
}

func (*memoryManifestLedger) ForgetGenerationDocument(context.Context, string, string) error {
	return nil
}

func manifestedSnapshot(revID, model string) (docSpec, pgstore.DocumentSnapshot, []domain.Chunk) {
	doc := newDoc("catalog", "report", "Manifested chunk zero describes retention.", "Manifested chunk one describes cutover.")
	chunks := make([]domain.Chunk, len(doc.Texts))
	var textBytes int64
	for i, text := range doc.Texts {
		chunks[i] = domain.Chunk{
			ID:         domain.ChunkID(hexChunkID(doc.ID, i)),
			DocumentID: domain.DocumentID(doc.ID),
			Index:      i,
			Text:       text,
			TokenCount: 7,
		}
		textBytes += int64(len(text))
	}
	live := sourceChunks(doc, chunks, model)
	for i := range live {
		live[i].RevisionID = revID
		live[i].VectorIncarnation = doc.ID
		live[i].VectorID = domain.FormatRevisionVectorID(domain.DocumentID(doc.ID), revID, i)
	}
	snap := pgstore.DocumentSnapshot{
		DocumentRevision: pgstore.DocumentRevision{
			ID:               doc.ID,
			Incarnation:      doc.ID,
			DocType:          doc.DocType,
			Collection:       doc.Collection,
			DeclaredChunks:   len(chunks),
			Chunks:           len(chunks),
			TextBytes:        textBytes,
			Revision:         "rev-" + revID,
			ActiveRevisionID: revID,
		},
		Chunks: chunks,
	}
	return doc, snap, live
}

func TestHitKeyPrefersVectorID(t *testing.T) {
	require.Equal(t, "doc#rev#0", hitKey(domain.Chunk{ID: "chunk-0", VectorID: "doc#rev#0"}))
	require.Equal(t, "chunk-0", hitKey(domain.Chunk{ID: "chunk-0"}))
}

func TestManifestedSnapshotStagesAndVerifiesOnPinecone(t *testing.T) {
	t.Run("copy same profile", func(t *testing.T) {
		ctx := t.Context()
		revID := uuid.NewString()
		doc, snap, live := manifestedSnapshot(revID, sourceModel)
		snap.TenantID = "tenant-1"
		for i := range live {
			live[i].Metadata[domain.VectorTenantKey] = snap.TenantID
			live[i].Metadata["pages"] = []int{2, 5}
			live[i].Metadata["table_ids"] = []string{"p2-t1"}
			live[i].Metadata["row_ids"] = []string{"p2-t1-r3"}
		}
		src, _ := memoryPinecone()
		dst, _ := memoryPinecone()
		require.NoError(t, src.Upsert(t.Context(), "", live))

		ledger := newMemoryManifestLedger()
		opts := defaultOptions()
		opts.MaxEmbedCalls = 100
		s := &stager{
			corpus:       snapshotCorpus{snap},
			ledger:       ledger,
			target:       pineconeStore{dst},
			generation:   "g2",
			profile:      testProfile(sourceModel),
			taggedTarget: true,
			opts:         opts,
			copyFrom:     src,
		}
		rep, err := s.run(t.Context())
		require.NoError(t, err)
		require.True(t, rep.Complete)
		require.Equal(t, 1, rep.Staged)
		require.Equal(t, 2, rep.Copied)
		require.Zero(t, rep.Embedded)
		for _, index := range []*pcstore.Client{src, dst} {
			fetched, fetchErr := index.Fetch(ctx, []string{live[0].VectorID})
			require.NoError(t, fetchErr)
			queried, queryErr := index.QueryFiltered(ctx, live[0].Embedding, 2, domain.VectorFilter{DocumentIDs: []string{doc.ID}})
			require.NoError(t, queryErr)
			require.Len(t, queried, 2)
			for _, chunk := range []domain.Chunk{fetched[live[0].VectorID], queried[0].Chunk, queried[1].Chunk} {
				require.Equal(t, revID, chunk.RevisionID)
				require.Equal(t, snap.Chunks[chunk.Index].ID, chunk.ID)
				require.Equal(t, "tenant-1", chunk.Metadata[domain.VectorTenantKey])
				require.Equal(t, []int{2, 5}, chunk.Metadata["pages"])
				require.Equal(t, []string{"p2-t1"}, chunk.Metadata["table_ids"])
				require.Equal(t, []string{"p2-t1-r3"}, chunk.Metadata["row_ids"])
			}
		}
		require.ElementsMatch(t, []string{
			domain.FormatRevisionVectorID(domain.DocumentID(doc.ID), revID, 0),
			domain.FormatRevisionVectorID(domain.DocumentID(doc.ID), revID, 1),
		}, allKeys(t, pineconeStore{dst}))

		vopts := defaultOptions()
		vopts.Verify, vopts.SampleSize, vopts.TopK, vopts.ScopeDocuments, vopts.MinRecall, vopts.MaxP95 = true, 2, 1, 1, 1, time.Second
		v := &verifier{
			corpus:        snapshotCorpus{snap},
			ledger:        ledger,
			target:        dst,
			source:        src,
			generation:    "g2",
			profile:       testProfile(sourceModel),
			sourceProfile: testProfile(sourceModel),
			taggedTarget:  true,
			opts:          vopts,
			now:           time.Now,
		}
		vrep, err := v.run(ctx)
		require.NoError(t, err)
		require.True(t, vrep.Passed, "%v", vrep.Failures)
		require.Equal(t, 2, vrep.Sample.Queries)
		require.Equal(t, 1.0, vrep.Sample.MeanRecall)
		require.Zero(t, vrep.Sample.ForbiddenHits)

		// Advancing the active revision invalidates the checkpoint, stages the new keys,
		// and prunes the superseded revision keys.
		newRev := uuid.NewString()
		_, snap2, _ := manifestedSnapshot(newRev, sourceModel)
		snap2.ID = doc.ID
		snap2.Incarnation = doc.ID
		for i := range snap2.Chunks {
			snap2.Chunks[i] = snap.Chunks[i]
		}
		s2 := &stager{
			corpus:       snapshotCorpus{snap2},
			ledger:       ledger,
			target:       pineconeStore{dst},
			generation:   "g2",
			profile:      testProfile(sourceModel),
			taggedTarget: true,
			opts:         opts,
			embed:        &countingEmbedder{model: sourceModel},
		}
		rep2, err := s2.run(ctx)
		require.NoError(t, err)
		require.True(t, rep2.Complete)
		require.Equal(t, 1, rep2.Staged)
		require.Equal(t, 2, rep2.PrunedKeys)
		require.ElementsMatch(t, []string{
			domain.FormatRevisionVectorID(domain.DocumentID(doc.ID), newRev, 0),
			domain.FormatRevisionVectorID(domain.DocumentID(doc.ID), newRev, 1),
		}, allKeys(t, pineconeStore{dst}))
	})

	t.Run("reembed different profile with fixtures", func(t *testing.T) {
		ctx := t.Context()
		revID := uuid.NewString()
		doc, snap, live := manifestedSnapshot(revID, sourceModel)
		src, _ := memoryPinecone()
		dst, _ := memoryPinecone()
		require.NoError(t, src.Upsert(t.Context(), "", live))

		ledger := newMemoryManifestLedger()
		opts := defaultOptions()
		opts.MaxEmbedCalls = 100
		embed := &countingEmbedder{model: "model-b"}
		s := &stager{
			corpus:       snapshotCorpus{snap},
			ledger:       ledger,
			target:       pineconeStore{dst},
			generation:   "g2",
			profile:      testProfile("model-b"),
			taggedTarget: true,
			opts:         opts,
			embed:        embed,
		}
		rep, err := s.run(ctx)
		require.NoError(t, err)
		require.True(t, rep.Complete)
		require.Equal(t, 2, rep.Embedded)
		require.Zero(t, rep.Copied)

		fixtures := queryFixtures{
			SourceProfile: testProfile(sourceModel).Hash(),
			TargetProfile: testProfile("model-b").Hash(),
			Queries:       map[string]queryVectors{},
		}
		for _, c := range snap.Chunks {
			vecID := domain.FormatRevisionVectorID(domain.DocumentID(doc.ID), revID, c.Index)
			sum := sha256.Sum256([]byte(c.Text))
			fixtures.Queries[vecID] = queryVectors{
				TextHash: fmt.Sprintf("%x", sum),
				Source:   embedWith(sourceModel, c.Text),
				Target:   embedWith("model-b", c.Text),
			}
		}
		fixturePath := filepath.Join(t.TempDir(), "fixtures.json")
		raw, err := json.Marshal(fixtures)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(fixturePath, raw, 0o600))

		vopts := defaultOptions()
		vopts.Verify, vopts.SampleSize, vopts.TopK, vopts.ScopeDocuments, vopts.MinRecall, vopts.MaxP95 = true, 2, 1, 1, 1, time.Second
		vopts.QueryFixtures = fixturePath
		v := &verifier{
			corpus:        snapshotCorpus{snap},
			ledger:        ledger,
			target:        dst,
			source:        src,
			generation:    "g2",
			profile:       testProfile("model-b"),
			sourceProfile: testProfile(sourceModel),
			taggedTarget:  true,
			opts:          vopts,
			now:           time.Now,
		}
		vrep, err := v.run(ctx)
		require.NoError(t, err)
		require.True(t, vrep.Passed, "%v", vrep.Failures)
		require.Equal(t, 2, vrep.Sample.Queries)
	})

	t.Run("missing or wrong revision_id or chunk_id fails verification", func(t *testing.T) {
		for _, badCase := range []string{"missing revision", "wrong revision", "wrong chunk_id"} {
			t.Run(badCase, func(t *testing.T) {
				revID := uuid.NewString()
				_, snap, live := manifestedSnapshot(revID, sourceModel)
				src, _ := memoryPinecone()
				dst, _ := memoryPinecone()
				require.NoError(t, src.Upsert(t.Context(), "", live))

				bad := append([]domain.Chunk(nil), live...)
				switch badCase {
				case "missing revision":
					bad[0].RevisionID = ""
				case "wrong revision":
					bad[0].RevisionID = uuid.NewString()
				case "wrong chunk_id":
					bad[0].ID = domain.ChunkID(uuid.NewString())
				}
				require.NoError(t, dst.Upsert(t.Context(), "", bad))

				vopts := defaultOptions()
				vopts.Verify, vopts.SampleSize, vopts.TopK, vopts.ScopeDocuments, vopts.MinRecall, vopts.MaxP95 = true, 2, 1, 1, 1, time.Second
				v := &verifier{
					corpus:        snapshotCorpus{snap},
					ledger:        currentCheckpointLedger{doc: snap.ID, revision: snap.Revision, incarnation: snap.Incarnation},
					target:        dst,
					source:        src,
					generation:    "g2",
					profile:       testProfile(sourceModel),
					sourceProfile: testProfile(sourceModel),
					taggedTarget:  true,
					opts:          vopts,
					now:           time.Now,
				}
				vrep, err := v.run(t.Context())
				require.NoError(t, err)
				require.False(t, vrep.Passed)
				require.Equal(t, 1, vrep.Coverage.Mismatched)
				require.Contains(t, fmt.Sprint(vrep.Failures), "differ from PostgreSQL")
			})
		}
	})

	t.Run("non-pinecone target blocks manifested snapshot", func(t *testing.T) {
		revID := uuid.NewString()
		doc, snap, live := manifestedSnapshot(revID, sourceModel)
		dst, _ := memoryPinecone()
		opts := defaultOptions()
		opts.MaxEmbedCalls = 100
		s := &stager{
			corpus:       snapshotCorpus{snap},
			ledger:       newMemoryManifestLedger(),
			target:       pineconeStore{dst},
			generation:   "g2",
			profile:      testProfile(sourceModel),
			taggedTarget: false,
			opts:         opts,
			embed:        &countingEmbedder{model: sourceModel},
		}
		rep, err := s.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Complete)
		require.Equal(t, []string{doc.ID}, rep.Unsupported)

		vopts := defaultOptions()
		vopts.Verify = true
		v := &verifier{
			corpus:        snapshotCorpus{snap},
			ledger:        newMemoryManifestLedger(),
			target:        dst,
			source:        dst,
			generation:    "g2",
			profile:       testProfile(sourceModel),
			sourceProfile: testProfile(sourceModel),
			taggedTarget:  false,
			opts:          vopts,
			now:           time.Now,
		}
		_ = live
		vrep, err := v.run(t.Context())
		require.NoError(t, err)
		require.False(t, vrep.Passed)
		require.Equal(t, 1, vrep.Coverage.ManifestedVectors)
		require.Contains(t, fmt.Sprint(vrep.Failures), "own hosted vector revisions")
	})
}

func seedManifestedPineconeDoc(t *testing.T, pg *pgstore.Client, index *pcstore.MemoryIndex, texts ...string) (docID, revID string, chunks []domain.Chunk) {
	t.Helper()
	ctx := t.Context()
	pg.EnableHostedVectorManifests(true)
	doc := newDoc("catalog", "report", texts...)
	docID = doc.ID
	req := domain.AcceptRequest{
		Document:    domain.Document{ID: domain.DocumentID(docID), Content: "manifested body"},
		Collection:  doc.Collection,
		DocType:     doc.DocType,
		Fingerprint: "fp-" + docID,
		MaxPending:  100,
	}
	res, err := pg.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)

	job, err := pg.ClaimJob(ctx, "worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, domain.DocumentID(docID), job.Document.ID)

	revID = uuid.NewString()
	job.RevisionID = revID
	chunks = make([]domain.Chunk, len(texts))
	vecIDs := make([]string, len(texts))
	chunkIDs := make([]string, len(texts))
	for i, text := range texts {
		cid := hexChunkID(docID, i)
		vid := domain.FormatRevisionVectorID(domain.DocumentID(docID), revID, i)
		chunks[i] = domain.Chunk{
			ID:                domain.ChunkID(cid),
			DocumentID:        domain.DocumentID(docID),
			Index:             i,
			Text:              text,
			TokenCount:        7,
			RevisionID:        revID,
			VectorID:          vid,
			VectorIncarnation: job.Incarnation,
			Embedding:         embedWith(sourceModel, text),
			Metadata:          map[string]any{"doc_type": doc.DocType, "collection": doc.Collection, "kind": "prose", "quality": "high"},
		}
		vecIDs[i] = vid
		chunkIDs[i] = cid
	}
	require.NoError(t, pg.PersistVectorManifest(ctx, *job, domain.VectorManifest{
		RevisionID:  revID,
		DocumentID:  domain.DocumentID(docID),
		Incarnation: job.Incarnation,
		Attempt:     job.Attempts,
		LeaseToken:  job.LeaseToken,
		IDPrefix:    domain.RevisionVectorPrefix(domain.DocumentID(docID), revID),
		VectorIDs:   vecIDs,
		ChunkIDs:    chunkIDs,
	}))
	client := pcstore.NewClientForIndex(index, testConfig(), "")
	require.NoError(t, client.Upsert(ctx, "", chunks))
	ok, err := pg.CompleteJob(ctx, *job, chunks, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	return docID, revID, chunks
}

func TestManifestedDocumentStaysVisibleAfterReindexCutover(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "pinecone")
	ctx := t.Context()

	docID, revID, chunks := seedManifestedPineconeDoc(t, f.pg, h.stores.index("quarry"),
		"Manifested visibility survives reindex cutover.",
		"Second manifested chunk keeps active revision metadata.")

	h.attestSource()
	stage := h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
	require.EqualValues(t, 5, stage["staged_documents"])
	require.Equal(t, true, stage["complete"])
	require.EqualValues(t, f.completedChunks()+len(chunks), stage["copied_chunks"])

	_, err := h.run(options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	apply := verifyOptions("g2")
	apply.Apply = true
	verifyRep, err := h.run(apply)
	require.NoError(t, err)
	require.Equal(t, true, verifyRep["passed"])
	require.Equal(t, true, verifyRep["recorded"])

	actRep, err := h.run(options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	require.Equal(t, "g2", actRep["active_generation"])
	_, err = h.run(options{Thaw: true, Apply: true})
	require.NoError(t, err)

	g2Client := pcstore.NewClientForIndex(h.stores.index("quarry-g2"), testConfig(), "")
	hits, err := g2Client.QueryFiltered(ctx, embedWith(sourceModel, chunks[0].Text), 5, domain.VectorFilter{DocumentIDs: []string{docID}, Collection: "catalog"})
	require.NoError(t, err)
	require.Len(t, hits, 2)
	for _, hit := range hits {
		require.Equal(t, revID, hit.Chunk.RevisionID)
		require.Equal(t, domain.FormatRevisionVectorID(domain.DocumentID(docID), revID, hit.Chunk.Index), hit.Chunk.VectorID)
	}

	vis := pgstore.Visibility{Pool: f.pg.Pool}
	stale := domain.ScoredChunk{
		Chunk: domain.Chunk{ID: chunks[0].ID, DocumentID: domain.DocumentID(docID), Index: 0, Text: chunks[0].Text, RevisionID: uuid.NewString()},
		Score: 0.99,
	}
	visible, previews, err := vis.VisibleEvidenceWithPreviews(ctx, append([]domain.ScoredChunk{stale}, hits...), []string{docID})
	require.NoError(t, err)
	require.Len(t, visible, 2, "only the manifested document's active revision stays visible after cutover")
	require.Contains(t, previews, docID)
	for _, sc := range visible {
		require.Equal(t, revID, sc.Chunk.RevisionID)
	}

	revHits, err := vis.VisibleRevisionChunks(ctx, append([]domain.ScoredChunk{stale}, hits...))
	require.NoError(t, err)
	require.Len(t, revHits, 2)
	for _, sc := range revHits {
		require.Equal(t, revID, sc.Chunk.RevisionID)
	}
}

func TestManifestedCheckpointInvalidationAndUnexpectedChunkKeys(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "pinecone")
	ctx := t.Context()

	docID, oldRev, chunks := seedManifestedPineconeDoc(t, f.pg, h.stores.index("quarry"), "Manifested revision churn.")
	h.attestSource()
	initial := h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
	require.Equal(t, true, initial["complete"])
	require.EqualValues(t, f.completedChunks()+len(chunks), initial["copied_chunks"])

	newRev := uuid.NewString()
	_, err := f.pg.Pool.Exec(ctx, `UPDATE hosted_vector_manifests SET state='retired', retired_at=NOW() WHERE revision_id=$1`, oldRev)
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(ctx, `INSERT INTO hosted_vector_manifests(revision_id, document_id, incarnation, attempt, id_prefix, vector_ids, chunk_ids, state, activated_at)
		VALUES($1, $2, $6, 2, $3, ARRAY[$4], ARRAY[$5::uuid], 'active', NOW())`,
		newRev, docID, domain.RevisionVectorPrefix(domain.DocumentID(docID), newRev),
		domain.FormatRevisionVectorID(domain.DocumentID(docID), newRev, 0), string(chunks[0].ID), chunks[0].VectorIncarnation)
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(ctx, `UPDATE hosted_vector_revisions SET active_revision_id=$2 WHERE document_id=$1`, docID, newRev)
	require.NoError(t, err)

	inv, err := h.run(options{Generation: "g2", Profile: h.profileFile("g2.json", testProfile(sourceModel)), TargetStore: "pinecone", TargetIndex: "quarry-g2"})
	require.NoError(t, err)
	ledger := inv["ledger"].(map[string]any)
	require.EqualValues(t, 1, ledger["stale"], "advancing active_revision_id invalidates the prior staging checkpoint")

	oldKey := domain.FormatRevisionVectorID(domain.DocumentID(docID), oldRev, 0)
	newKey := domain.FormatRevisionVectorID(domain.DocumentID(docID), newRev, 0)
	bareKey := string(chunks[0].ID)
	unexpected, err := f.pg.UnexpectedChunkKeys(ctx, []string{newKey, oldKey, bareKey}, true)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{oldKey, bareKey}, unexpected, "only the active revision key is valid for a manifested document")

	// Update live index with new active revision so re-staging copies and prunes the old key.
	liveClient := pcstore.NewClientForIndex(h.stores.index("quarry"), testConfig(), "")
	updated := append([]domain.Chunk(nil), chunks...)
	updated[0].RevisionID = newRev
	updated[0].VectorID = newKey
	require.NoError(t, liveClient.Upsert(ctx, "", updated))

	restage := h.stagePinecone("g2", "quarry-g2", sourceModel, 0)
	require.EqualValues(t, 1, restage["staged_documents"])
	require.Equal(t, true, restage["complete"])
	require.EqualValues(t, 1, restage["copied_chunks"])
	require.EqualValues(t, 1, restage["pruned_keys"])

	verifyRep, err := h.run(verifyOptions("g2"))
	require.NoError(t, err, "%v", verifyRep)
	require.Equal(t, true, verifyRep["passed"])
}

func TestPostgresTargetBlocksManifestedDocuments(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "pinecone")
	seedManifestedPineconeDoc(t, f.pg, h.stores.index("quarry"), "Blocked on postgres target.")
	h.attestSource()
	profile := h.profileFile("pg-g2.json", testProfile(sourceModel))

	inv, err := h.run(options{Generation: "g2", TargetStore: "postgres", Profile: profile})
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(inv["blockers"]), "own hosted vector revisions")
	docs := inv["documents"].(map[string]any)
	require.EqualValues(t, 1, docs["manifested_vectors"])

	_, err = h.run(options{Generation: "g2", TargetStore: "postgres", Profile: profile, Apply: true})
	require.ErrorContains(t, err, "staging is blocked:")
}
