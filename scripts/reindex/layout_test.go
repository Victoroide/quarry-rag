package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStageLayoutWithPostgresTargets(t *testing.T) {
	t.Run("postgres_source_to_postgres_target_copy_and_reembed_with_frozen_verify", func(t *testing.T) {
		pg := vectorDB(t)
		ctx := t.Context()
		live, err := pgstore.NewVectorStore(ctx, pg, testDims, pgstore.WithExactScopeMax(20))
		require.NoError(t, err)
		require.NoError(t, pg.PinVectorGeneration(ctx, pgstore.LegacyGeneration, "postgres", "chunk_vectors", "", testProfile(sourceModel)))

		// Document 1: persisted layout extraction in document_pdf_extractions.
		ch := chunker.NewChunker(8, 2)
		sections := []domain.LayoutSection{
			{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "North region revenue is 120."},
			{Page: 2, TableID: "p1-t1", RowID: "p1-t1-r2", Text: "South region revenue is 180."},
		}
		layoutDocID := uuid.NewString()
		produced, err := ch.ChunkLayout(ctx, domain.DocumentID(layoutDocID), sections, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(produced), 2)

		var fullContent string
		for i, c := range produced {
			if i > 0 {
				fullContent += " "
			}
			fullContent += c.Text
		}
		_, err = pg.Pool.Exec(ctx, `INSERT INTO documents(id,content,status,chunk_count,doc_type,collection,fingerprint)
			VALUES($1,$2,'completed',$3,'report','alpha',$4)`,
			layoutDocID, fullContent, len(produced), chunker.ScopedFingerprint("alpha", fullContent))
		require.NoError(t, err)
		for i := range produced {
			cid := hexChunkID(layoutDocID, i)
			produced[i].ID = domain.ChunkID(cid)
			produced[i].DocumentID = domain.DocumentID(layoutDocID)
			produced[i].Embedding = embedWith(sourceModel, produced[i].Text)
			produced[i].Metadata["doc_type"] = "report"
			produced[i].Metadata["collection"] = "alpha"
			_, err = pg.Pool.Exec(ctx, `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,$3,$4,$5)`,
				cid, layoutDocID, produced[i].Index, produced[i].Text, produced[i].TokenCount)
			require.NoError(t, err)
		}
		pageResults, err := json.Marshal([]domain.PDFPageResult{
			{Number: 1, State: "text", Sections: sections[:1]},
			{Number: 2, State: "text", Sections: sections[1:]},
		})
		require.NoError(t, err)
		_, err = pg.Pool.Exec(ctx, `INSERT INTO document_pdf_extractions(document_id,profile,manifest,coverage,page_results) VALUES($1,$2,'{}','complete',$3)`,
			layoutDocID, domain.LayoutPDFProfile, pageResults)
		require.NoError(t, err)
		require.NoError(t, live.Upsert(ctx, "", produced))

		// Document 2: plain document without layout provenance.
		plainDoc := newDoc("alpha", "report", "Plain text document without PDF layout sections.")
		plainChunks := insertDoc(t, pg, plainDoc)
		require.NoError(t, live.Upsert(ctx, "", sourceChunks(plainDoc, plainChunks, sourceModel)))

		// Stage copy-only into g2.
		g2, err := pgstore.CreateGenerationVectorStore(ctx, pg, testDims, "g2", pgstore.WithExactScopeMax(20))
		require.NoError(t, err)
		registerGeneration(t, pg, "g2", "postgres", pgstore.VectorTable("g2"), sourceModel)

		opts := defaultOptions()
		opts.MaxEmbedCalls = 0
		embedder := &countingEmbedder{model: sourceModel}
		copyStager := &stager{corpus: pg, ledger: pg, target: g2, generation: "g2", profile: testProfile(sourceModel), opts: opts, copyFrom: live, embed: embedder}
		rep, err := copyStager.run(ctx)
		require.NoError(t, err)
		require.True(t, rep.Complete)
		require.Zero(t, rep.Embedded)
		require.Equal(t, len(produced)+len(plainChunks), rep.Copied)

		// Frozen verification passes on g2.
		vOpts := defaultOptions()
		vOpts.Verify, vOpts.Apply, vOpts.SampleSize, vOpts.TopK, vOpts.ScopeDocuments, vOpts.MinRecall, vOpts.MaxP95 = true, true, 2, 2, 2, 1, time.Second
		require.NoError(t, pg.FreezeWriters(ctx, "g2"))
		func() {
			fence, fenceErr := pg.FenceGeneration(ctx, "g2")
			require.NoError(t, fenceErr)
			defer fence.Close(ctx)
			frozenAt, beginErr := fence.BeginVerification(ctx)
			require.NoError(t, beginErr)
			v := &verifier{corpus: pg, ledger: pg, target: g2, source: live, generation: "g2", profile: testProfile(sourceModel), sourceProfile: testProfile(sourceModel), opts: vOpts, now: time.Now}
			vRep, verifyErr := v.run(ctx)
			require.NoError(t, verifyErr)
			require.True(t, vRep.Passed, "%v", vRep.Failures)
			encoded, encodeErr := json.Marshal(vRep)
			require.NoError(t, encodeErr)
			require.NoError(t, fence.RecordVerification(ctx, frozenAt, vRep.CorpusDigest, encoded))
		}()
		require.NoError(t, pg.ThawWriters(ctx))

		// Re-embed into g3 (different model, copyFrom=nil) preserves canonical labels and provenance.
		g3, err := pgstore.CreateGenerationVectorStore(ctx, pg, testDims, "g3", pgstore.WithExactScopeMax(20))
		require.NoError(t, err)
		registerGeneration(t, pg, "g3", "postgres", pgstore.VectorTable("g3"), "model-b")
		opts.MaxEmbedCalls = 100
		reembedder := &countingEmbedder{model: "model-b"}
		reembedStager := &stager{corpus: pg, ledger: pg, target: g3, generation: "g3", profile: testProfile("model-b"), opts: opts, copyFrom: nil, embed: reembedder}
		rep3, err := reembedStager.run(ctx)
		require.NoError(t, err)
		require.True(t, rep3.Complete)
		require.Zero(t, rep3.Copied)
		require.Equal(t, len(produced)+len(plainChunks), rep3.Embedded)

		// Compare coverage in the new space without making a cross-profile ranking claim.
		v3 := &verifier{target: g3, profile: testProfile("model-b")}
		checkCoverage := func(wantMismatched int) {
			t.Helper()
			snap, snapErr := pg.LoadDocumentSnapshot(ctx, layoutDocID)
			require.NoError(t, snapErr)
			require.NotNil(t, snap)
			var report verifyReport
			var sample []sampleChunk
			present, valid, compareErr := v3.compareDocument(ctx, snap, &report, map[string]scopeDoc{}, &sample)
			require.NoError(t, compareErr)
			require.Equal(t, len(produced), present)
			require.Equal(t, len(produced)-wantMismatched, valid)
			require.Equal(t, wantMismatched, report.Coverage.Mismatched)
		}
		checkCoverage(0)

		// Missing and incorrect canonical provenance both fail the coverage comparison.
		_, err = pg.Pool.Exec(ctx, `UPDATE chunk_vectors_g3 SET pages = NULL WHERE chunk_id = $1`, string(produced[0].ID))
		require.NoError(t, err)
		checkCoverage(1)

		_, err = pg.Pool.Exec(ctx, `UPDATE chunk_vectors_g3 SET pages = ARRAY[999] WHERE chunk_id = $1`, string(produced[0].ID))
		require.NoError(t, err)
		checkCoverage(1)

		state, err := pg.VectorState(ctx)
		require.NoError(t, err)
		require.Equal(t, pgstore.LegacyGeneration, state.Active, "failed target verification leaves live generation unchanged")
	})

	t.Run("hosted_source_to_postgres_target_fallback_stale_and_malformed", func(t *testing.T) {
		pg := vectorDB(t)
		pgTargetStore, err := pgstore.CreateGenerationVectorStore(t.Context(), pg, testDims, "g2")
		require.NoError(t, err)
		registerGeneration(t, pg, "g2", "postgres", pgstore.VectorTable("g2"), sourceModel)

		hosted, _ := memoryPinecone()
		// Doc A: snapshot has no extraction row, valid source record carries partial malformed + valid layout fields.
		docA := newDoc("alpha", "report", "Hosted fallback valid and malformed.")
		docA.Incarnation = uuid.NewString()
		chunksA := insertDoc(t, pg, docA)
		srcA := sourceChunks(docA, chunksA, sourceModel)
		srcA[0].VectorIncarnation = docA.Incarnation
		srcA[0].Metadata["pages"] = []int{2, 4}
		srcA[0].Metadata["table_ids"] = []string{"not-canonical"} // malformed optional provider field -> omit
		srcA[0].Metadata["row_ids"] = []string{"p2-t1-r3"}

		// Doc B: stale source incarnation must NOT copy provenance.
		docB := newDoc("alpha", "report", "Stale source record provenance.")
		docB.Incarnation = uuid.NewString()
		chunksB := insertDoc(t, pg, docB)
		srcB := sourceChunks(docB, chunksB, sourceModel)
		srcB[0].VectorIncarnation = uuid.NewString()
		srcB[0].Metadata["pages"] = []int{7}
		srcB[0].Metadata["table_ids"] = []string{"p7-t1"}

		ctx := t.Context()
		require.NoError(t, hosted.Upsert(ctx, "", append(srcA, srcB...)))

		opts := defaultOptions()
		opts.MaxEmbedCalls = 10
		s := &stager{
			corpus: pg, ledger: pg, target: pgTargetStore, generation: "g2",
			profile: testProfile(sourceModel), opts: opts, copyFrom: hosted, copyFromTagged: true,
			embed: &countingEmbedder{model: sourceModel},
		}
		rep, err := s.run(ctx)
		require.NoError(t, err)
		require.True(t, rep.Complete)
		require.Equal(t, 1, rep.Copied)
		require.Equal(t, 1, rep.Embedded)

		fetched, err := pgTargetStore.Fetch(ctx, []string{string(chunksA[0].ID), string(chunksB[0].ID)})
		require.NoError(t, err)
		require.Len(t, fetched, 2)
		require.Equal(t, []int{2, 4}, fetched[string(chunksA[0].ID)].Metadata["pages"])
		require.NotContains(t, fetched[string(chunksA[0].ID)].Metadata, "table_ids")
		require.Equal(t, []string{"p2-t1-r3"}, fetched[string(chunksA[0].ID)].Metadata["row_ids"])
		require.NotContains(t, fetched[string(chunksB[0].ID)].Metadata, "pages")
		require.NotContains(t, fetched[string(chunksB[0].ID)].Metadata, "table_ids")
		require.NotContains(t, fetched[string(chunksB[0].ID)].Metadata, "row_ids")

		vOpts := defaultOptions()
		vOpts.Verify, vOpts.SampleSize, vOpts.TopK, vOpts.ScopeDocuments, vOpts.MinRecall, vOpts.MaxP95 = true, 2, 2, 2, 1, time.Second
		v := &verifier{
			corpus: pg, ledger: pg, target: pgTargetStore, source: hosted, sourceTagged: true, generation: "g2",
			profile: testProfile(sourceModel), sourceProfile: testProfile(sourceModel), opts: vOpts, now: time.Now,
		}
		vRep, err := v.run(ctx)
		require.NoError(t, err)
		require.False(t, vRep.Passed)
		require.Contains(t, vRep.Failures, "only 1 of 2 sample queries ran with a usable baseline (1 unmeasurable)")
		require.Equal(t, 1, vRep.Sample.Queries)
		require.Equal(t, 1, vRep.Sample.Skipped)
		require.Equal(t, 1.0, vRep.Coverage.KeyCoverage)
		require.Equal(t, 1.0, vRep.Coverage.ProfileCoverage)

		// Unexpected provenance on target when snapshot lacks it and source is stale fails verify.
		_, err = pg.Pool.Exec(ctx, `UPDATE chunk_vectors_g2 SET pages = ARRAY[7] WHERE chunk_id = $1`, string(chunksB[0].ID))
		require.NoError(t, err)
		vRepFail, err := v.run(ctx)
		require.NoError(t, err)
		require.False(t, vRepFail.Passed)
		require.Equal(t, 1, vRepFail.Coverage.Mismatched)
	})

	t.Run("postgres_source_to_hosted_target_and_interrupted_retry", func(t *testing.T) {
		pg := vectorDB(t)
		live, err := pgstore.NewVectorStore(t.Context(), pg, testDims)
		require.NoError(t, err)
		doc := newDoc("alpha", "report", "PostgreSQL source to hosted target.")
		chunks := insertDoc(t, pg, doc)
		ctx := t.Context()
		src := sourceChunks(doc, chunks, sourceModel)
		src[0].Metadata["pages"] = []int{1, 2}
		src[0].Metadata["table_ids"] = []string{"p1-t1"}
		src[0].Metadata["row_ids"] = []string{"p1-t1-r1"}
		require.NoError(t, live.Upsert(ctx, "", src))

		hostedTarget, _ := memoryPinecone()
		registerGeneration(t, pg, "g_pc", "pinecone", "idx-pc", sourceModel)

		// Simulate interrupted write by opening an incomplete checkpoint before staging completes.
		_, err = pg.BeginGenerationDocument(ctx, "g_pc", doc.ID, []string{string(chunks[0].ID)})
		require.NoError(t, err)

		opts := defaultOptions()
		opts.MaxEmbedCalls = 10
		s := &stager{
			corpus: pg, ledger: pg, target: pineconeStore{hostedTarget}, generation: "g_pc",
			profile: testProfile(sourceModel), taggedTarget: true, opts: opts, copyFrom: live,
			embed: &countingEmbedder{model: sourceModel},
		}
		rep, err := s.run(ctx)
		require.NoError(t, err)
		require.True(t, rep.Complete)
		require.Equal(t, 1, rep.Copied)

		fetched, err := hostedTarget.Fetch(ctx, []string{string(chunks[0].ID)})
		require.NoError(t, err)
		require.Equal(t, []int{1, 2}, fetched[string(chunks[0].ID)].Metadata["pages"])
		require.Equal(t, []string{"p1-t1"}, fetched[string(chunks[0].ID)].Metadata["table_ids"])
		require.Equal(t, []string{"p1-t1-r1"}, fetched[string(chunks[0].ID)].Metadata["row_ids"])
	})
}

func TestLayoutFallbackUsesStagingSourceIdentity(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Layout evidence.")
	canonical := sourceChunks(doc, []domain.Chunk{{ID: domain.ChunkID(hexChunkID(doc.ID, 0)),
		DocumentID: domain.DocumentID(doc.ID), Text: doc.Texts[0], TokenCount: 2}}, sourceModel)[0]
	canonical.Metadata = nil
	canonical.VectorIncarnation = ""
	base := pgstore.DocumentRevision{ID: doc.ID, Incarnation: doc.Incarnation, DocType: doc.DocType, Collection: doc.Collection}
	requiredBare := base
	requiredBare.VectorIncarnationRequired = true
	tagged := requiredBare
	tagged.ScopedVectors = true
	tagged.VectorAttempt = doc.Attempt
	tagged.TenantID = uuid.NewString()
	optional := tagged
	optional.VectorIncarnationRequired = false
	optional.TenantID = ""
	manifested := tagged
	manifested.ActiveRevisionID = uuid.NewString()
	optionalManifest := manifested
	optionalManifest.VectorIncarnationRequired = false
	missingStamp := func(c *domain.Chunk) { c.VectorIncarnation = "" }
	wrongStamp := func(c *domain.Chunk) { c.VectorIncarnation = uuid.NewString() }
	for _, tc := range []struct {
		name         string
		revision     pgstore.DocumentRevision
		sourceTagged bool
		mutate       func(*domain.Chunk)
		accepted     bool
	}{
		{name: "bare missing optional tags", revision: base, sourceTagged: true, accepted: true},
		{name: "bare stale optional incarnation", revision: base, sourceTagged: true, mutate: wrongStamp},
		{name: "bare matching required incarnation", revision: requiredBare, sourceTagged: true, accepted: true},
		{name: "bare missing required incarnation", revision: requiredBare, sourceTagged: true, mutate: missingStamp},
		{name: "bare wrong required incarnation", revision: requiredBare, sourceTagged: true, mutate: wrongStamp},
		{name: "bare wrong tenant", revision: base, sourceTagged: true, mutate: func(c *domain.Chunk) { c.Metadata[domain.VectorTenantKey] = uuid.NewString() }},
		{name: "relational source without a wire stamp", revision: requiredBare, accepted: true},
		{name: "bare store for tagged snapshot", revision: tagged, accepted: true},
		{name: "tagged", revision: tagged, sourceTagged: true, accepted: true},
		{name: "tagged optional tenant and incarnation omitted", revision: optional, sourceTagged: true, accepted: true},
		{name: "tagged missing revision", revision: tagged, sourceTagged: true, mutate: func(c *domain.Chunk) { delete(c.Metadata, domain.VectorRevisionKey) }},
		{name: "tagged missing attempt", revision: tagged, sourceTagged: true, mutate: func(c *domain.Chunk) { delete(c.Metadata, domain.VectorAttemptKey) }},
		{name: "tagged missing tenant", revision: tagged, sourceTagged: true, mutate: func(c *domain.Chunk) { delete(c.Metadata, domain.VectorTenantKey) }},
		{name: "tagged missing required incarnation", revision: tagged, sourceTagged: true, mutate: missingStamp},
		{name: "manifested matching required incarnation", revision: manifested, sourceTagged: true, accepted: true},
		{name: "manifested missing required incarnation", revision: manifested, sourceTagged: true, mutate: missingStamp},
		{name: "manifested wrong required incarnation", revision: manifested, sourceTagged: true, mutate: wrongStamp},
		{name: "manifested missing optional incarnation", revision: optionalManifest, sourceTagged: true, accepted: true},
		{name: "manifested missing revision", revision: manifested, sourceTagged: true, mutate: func(c *domain.Chunk) { c.RevisionID = "" }},
		{name: "manifested wrong revision", revision: manifested, sourceTagged: true, mutate: func(c *domain.Chunk) { c.RevisionID = uuid.NewString() }},
		{name: "wrong document", revision: requiredBare, sourceTagged: true, mutate: func(c *domain.Chunk) { c.DocumentID = domain.DocumentID(uuid.NewString()) }},
		{name: "wrong chunk", revision: requiredBare, sourceTagged: true, mutate: func(c *domain.Chunk) { c.ID = domain.ChunkID(uuid.NewString()) }},
		{name: "wrong index", revision: requiredBare, sourceTagged: true, mutate: func(c *domain.Chunk) { c.Index++ }},
		{name: "wrong text", revision: requiredBare, sourceTagged: true, mutate: func(c *domain.Chunk) { c.Text += " Changed." }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := &pgstore.DocumentSnapshot{DocumentRevision: tc.revision, Chunks: []domain.Chunk{canonical}}
			src := canonical
			src.Metadata = map[string]any{
				domain.LayoutPagesKey:    []int{2},
				domain.LayoutTableIDsKey: []string{"invalid optional field"},
			}
			src.RevisionID = tc.revision.ActiveRevisionID
			if tc.sourceTagged && tc.revision.VectorIncarnationRequired {
				src.VectorIncarnation = tc.revision.Incarnation
			}
			if tc.sourceTagged && isTagged(tc.revision) {
				src.Metadata[domain.VectorRevisionKey] = tc.revision.Incarnation
				src.Metadata[domain.VectorAttemptKey] = tc.revision.VectorAttempt
				if tc.revision.TenantID != "" {
					src.Metadata[domain.VectorTenantKey] = tc.revision.TenantID
				}
			}
			if tc.mutate != nil {
				tc.mutate(&src)
			}
			sourceKey, err := vectorKey(tc.sourceTagged, tc.revision, canonical)
			require.NoError(t, err)
			for _, mode := range []struct {
				name    string
				reembed bool
			}{{name: "reusable vector"}, {name: "reembedded vector", reembed: true}} {
				t.Run(mode.name, func(t *testing.T) {
					record := src
					if mode.reembed {
						record.Embedding = nil
					}
					source := stubReader{chunks: map[string]domain.Chunk{sourceKey: record}}
					profile := testProfile(sourceModel)
					opts := defaultOptions()
					opts.MaxEmbedCalls = 1
					s := &stager{copyFrom: source, copyFromTagged: tc.sourceTagged, taggedTarget: tc.revision.ActiveRevisionID != "",
						profile: profile, opts: opts, embed: &countingEmbedder{model: sourceModel}}
					staged, copied, embedded, err := s.vectors(t.Context(), snap)
					require.NoError(t, err)
					if tc.accepted && !mode.reembed {
						require.Equal(t, 1, copied)
						require.Zero(t, embedded)
					} else {
						assert.Zero(t, copied)
						assert.Equal(t, 1, embedded)
					}
					if tc.accepted {
						require.Equal(t, []int{2}, staged[0].Metadata[domain.LayoutPagesKey])
					} else {
						assert.NotContains(t, staged[0].Metadata, domain.LayoutPagesKey)
					}
					require.NotContains(t, staged[0].Metadata, domain.LayoutTableIDsKey)

					targetKey, err := vectorKey(s.taggedTarget, tc.revision, canonical)
					require.NoError(t, err)
					assertCoverage := func(target domain.Chunk, wantMismatches int) {
						t.Helper()
						v := verifier{target: stubReader{chunks: map[string]domain.Chunk{targetKey: target}}, source: source,
							taggedTarget: s.taggedTarget, sourceTagged: tc.sourceTagged, profile: profile}
						var report verifyReport
						var sample []sampleChunk
						present, valid, err := v.compareDocument(t.Context(), snap, &report, map[string]scopeDoc{}, &sample)
						require.NoError(t, err)
						require.Equal(t, 1, present)
						require.Equal(t, 1-wantMismatches, valid)
						require.Equal(t, wantMismatches, report.Coverage.Mismatched)
					}
					assertCoverage(staged[0], 0)
					if !tc.accepted {
						// A target carrying the rejected source's page must fail verification.
						staged[0].Metadata[domain.LayoutPagesKey] = []int{2}
						assertCoverage(staged[0], 1)
					}
				})
			}
		})
	}
}

type layoutFailingReader struct {
	vectorReader
	fetch func(context.Context, []string) (map[string]domain.Chunk, error)
}

func (r layoutFailingReader) Fetch(ctx context.Context, keys []string) (map[string]domain.Chunk, error) {
	return r.fetch(ctx, keys)
}

func TestLayoutFallbackReadErrorPreservesEarlierBatches(t *testing.T) {
	doc := newDoc("alpha", "report", "Layout evidence.")
	snap := &pgstore.DocumentSnapshot{DocumentRevision: pgstore.DocumentRevision{ID: doc.ID, DocType: doc.DocType, Collection: doc.Collection}}
	records := make(map[string]domain.Chunk)
	for i := range pcstore.FetchBatchSize + 1 {
		chunk := domain.Chunk{ID: domain.ChunkID(hexChunkID(doc.ID, i)), DocumentID: domain.DocumentID(doc.ID), Index: i,
			Text: doc.Texts[0], TokenCount: 2}
		snap.Chunks = append(snap.Chunks, chunk)
		chunk.Embedding = embedWith(sourceModel, chunk.Text)
		chunk.Metadata = map[string]any{"doc_type": doc.DocType, "collection": doc.Collection, domain.LayoutPagesKey: []int{2}}
		records[string(chunk.ID)] = chunk
	}
	reader := stubReader{chunks: records}
	for name, readErr := range map[string]error{"transient": errors.New("provider unavailable"), "canceled": context.Canceled} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			source := layoutFailingReader{vectorReader: reader, fetch: func(ctx context.Context, ids []string) (map[string]domain.Chunk, error) {
				calls++
				if calls == 1 {
					return reader.Fetch(ctx, ids)
				}
				if errors.Is(readErr, context.Canceled) {
					cancel()
					return nil, ctx.Err()
				}
				return nil, readErr
			}}
			v := verifier{target: reader, source: source, profile: testProfile(sourceModel)}
			var report verifyReport
			var sample []sampleChunk
			present, valid, err := v.compareDocument(ctx, snap, &report, map[string]scopeDoc{}, &sample)
			require.ErrorIs(t, err, readErr)
			require.ErrorContains(t, err, "read source layout evidence")
			require.Equal(t, 2, calls)
			require.Equal(t, len(records), present)
			require.Equal(t, pcstore.FetchBatchSize, valid)
			require.Zero(t, report.Coverage.Mismatched)
		})
	}
}
