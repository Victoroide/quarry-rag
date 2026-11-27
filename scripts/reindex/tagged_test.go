package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func hostedPositionSnapshot(form string) (pgstore.DocumentSnapshot, []domain.Chunk) {
	if form == "manifested" {
		_, snap, live := manifestedSnapshot(uuid.NewString(), sourceModel)
		return snap, live
	}
	doc := newDoc("alpha", "report", "Position zero is stored first.", "Position one is stored second.")
	if form == "tagged" {
		doc = newTaggedDoc(doc.Collection, doc.DocType, doc.Texts...)
	} else {
		doc.Incarnation = doc.ID
	}
	chunks := chunksOf(doc)
	snap := snapshotOf(doc, chunks, "hosted-position-revision")
	snap.ScopedVectors, snap.VectorAttempt = doc.Tagged, doc.Attempt
	return snap, sourceChunks(doc, chunks, sourceModel)
}

func updateHostedRecord(t *testing.T, index *pcstore.MemoryIndex, key string, update func(*pinecone.Vector)) {
	t.Helper()
	response, err := index.FetchVectors(t.Context(), []string{key})
	require.NoError(t, err)
	vector := response.Vectors[key]
	require.NotNil(t, vector)
	update(vector)
	_, err = index.UpsertVectors(t.Context(), []*pinecone.Vector{vector})
	require.NoError(t, err)
}

func setHostedPosition(t *testing.T, index *pcstore.MemoryIndex, key string, value *structpb.Value) {
	t.Helper()
	updateHostedRecord(t, index, key, func(vector *pinecone.Vector) {
		if value == nil {
			delete(vector.Metadata.Fields, "chunk_index")
		} else {
			vector.Metadata.Fields["chunk_index"] = value
		}
	})
}

func TestStageHostedChunkPositionBeforeReuse(t *testing.T) {
	for _, tc := range []struct {
		name, form string
		index      int
		position   *structpb.Value
	}{
		{"missing", "tagged", 0, nil},
		{"string zero", "manifested", 0, structpb.NewStringValue("0")},
		{"NaN", "bare", 0, structpb.NewNumberValue(math.NaN())},
		{"infinite", "tagged", 1, structpb.NewNumberValue(math.Inf(1))},
		{"fraction at zero", "bare", 0, structpb.NewNumberValue(0.5)},
		{"fraction at one", "manifested", 1, structpb.NewNumberValue(1.5)},
		{"negative", "bare", 1, structpb.NewNumberValue(-1)},
		{"out of range", "tagged", 1, structpb.NewNumberValue(math.MaxInt32 + 1)},
		{"integer mismatch", "manifested", 1, structpb.NewNumberValue(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, live := hostedPositionSnapshot(tc.form)
			badKey, err := vectorKey(true, snap.DocumentRevision, snap.Chunks[tc.index])
			require.NoError(t, err)
			for i := range live {
				live[i].Metadata[domain.LayoutPagesKey] = []int{i + 1}
			}
			// Older tagged writes use revision and attempt metadata without a separate incarnation stamp.
			if tc.form == "tagged" {
				for i := range live {
					live[i].VectorIncarnation = ""
				}
			}
			src, sourceIndex := memoryPinecone()
			require.NoError(t, src.Upsert(t.Context(), "", live))
			setHostedPosition(t, sourceIndex, badKey, tc.position)

			dst, targetIndex := memoryPinecone()
			ledger := newMemoryManifestLedger()
			embed := &countingEmbedder{model: "replacement"}
			s := stager{
				corpus: snapshotCorpus{snap}, ledger: ledger, target: pineconeStore{dst}, opts: defaultOptions(),
				generation: "g2", taggedTarget: true, copyFromTagged: true, profile: testProfile(sourceModel), copyFrom: src, embed: embed,
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = s.run(ctx)
			require.ErrorIs(t, err, context.Canceled)
			rep, err := s.run(t.Context())
			require.NoError(t, err)
			require.False(t, rep.Complete)
			require.Equal(t, "embedding call budget", rep.Stopped)
			require.Zero(t, rep.Copied)
			require.Zero(t, embed.calls.Load())
			require.Empty(t, targetIndex.IDs())
			require.Empty(t, ledger.recorded)
			require.Empty(t, ledger.checkpoints)

			budgeted := s
			budgeted.opts.MaxEmbedCalls = 1
			budgetedTarget, _ := memoryPinecone()
			budgeted.target, budgeted.ledger = pineconeStore{budgetedTarget}, newMemoryManifestLedger()
			rep, err = budgeted.run(t.Context())
			require.NoError(t, err)
			require.True(t, rep.Complete)
			require.Equal(t, 1, rep.Copied)
			require.Equal(t, 1, rep.Embedded)
			require.EqualValues(t, 1, embed.calls.Load())
			other := 1 - tc.index
			goodKey, err := vectorKey(true, snap.DocumentRevision, snap.Chunks[other])
			require.NoError(t, err)
			got, err := budgetedTarget.Fetch(t.Context(), []string{badKey, goodKey})
			require.NoError(t, err)
			require.Len(t, got, 2)
			require.Equal(t, tc.index, got[badKey].Index)
			require.Equal(t, embedWith("replacement", live[tc.index].Text), got[badKey].Embedding)
			require.NotContains(t, got[badKey].Metadata, domain.LayoutPagesKey)
			require.Equal(t, live[other].Embedding, got[goodKey].Embedding)
			require.Equal(t, []int{other + 1}, got[goodKey].Metadata[domain.LayoutPagesKey])

			setHostedPosition(t, sourceIndex, badKey, structpb.NewNumberValue(float64(tc.index)))
			rep, err = s.run(t.Context())
			require.NoError(t, err)
			require.True(t, rep.Complete)
			require.Equal(t, 2, rep.Copied)
			require.Zero(t, rep.Embedded)
			rep, err = s.run(t.Context())
			require.NoError(t, err)
			require.True(t, rep.Complete)
			require.Equal(t, 1, rep.Skipped)
			require.EqualValues(t, 1, embed.calls.Load())
		})
	}

	t.Run("mixed documents", func(t *testing.T) {
		first, firstLive := hostedPositionSnapshot("bare")
		second, secondLive := hostedPositionSnapshot("bare")
		if first.ID > second.ID {
			first, second = second, first
			firstLive, secondLive = secondLive, firstLive
		}
		src, sourceIndex := memoryPinecone()
		require.NoError(t, src.Upsert(t.Context(), "", append(firstLive, secondLive...)))
		setHostedPosition(t, sourceIndex, string(secondLive[0].ID), nil)
		dst, targetIndex := memoryPinecone()
		ledger := newMemoryManifestLedger()
		s := stager{
			corpus: newSnapshotsCorpus(first, second), ledger: ledger, target: pineconeStore{dst}, opts: defaultOptions(),
			generation: "g2", taggedTarget: true, copyFromTagged: true, profile: testProfile(sourceModel), copyFrom: src,
		}
		rep, err := s.run(t.Context())
		require.NoError(t, err)
		require.False(t, rep.Complete)
		require.Equal(t, first.ID, rep.NextAfter)
		require.Equal(t, 1, rep.Staged)
		require.Equal(t, 2, rep.Copied)
		require.ElementsMatch(t, []string{string(firstLive[0].ID), string(firstLive[1].ID)}, targetIndex.IDs())
		require.True(t, ledger.checkpoints[first.ID].Complete)
		require.NotContains(t, ledger.recorded, second.ID)
	})
}

func assignDocTenant(t *testing.T, pg *pgstore.Client, d docSpec, tenant, principal string) {
	t.Helper()
	var rev int64
	require.NoError(t, pg.Pool.QueryRow(t.Context(), `SELECT revision FROM grant_catalog_policy_revision`).Scan(&rev))
	var sourceVersion string
	require.NoError(t, pg.Pool.QueryRow(t.Context(), `SELECT source_version FROM grant_catalog_source_snapshot($1)`, d.ID).Scan(&sourceVersion))
	batch := map[string]any{
		"batch_id":          uuid.NewString(),
		"expected_revision": rev,
		"tenants":           []map[string]any{{"tenant_id": tenant}},
		"collections":       []map[string]any{{"tenant_id": tenant, "collection": d.Collection}},
		"principals":        []map[string]any{{"tenant_id": tenant, "principal_id": principal, "issuer": "synthetic issuer", "subject": "owner", "enabled": true}},
		"assignments":       []map[string]any{{"tenant_id": tenant, "principal_id": principal, "collection": d.Collection, "document_id": d.ID, "source_version": sourceVersion}},
	}
	body, err := json.Marshal(batch)
	require.NoError(t, err)
	_, err = pg.ImportGrantBatch(t.Context(), bytes.NewReader(body), false)
	require.NoError(t, err)
}

func newTaggedFixture(t *testing.T, docs ...docSpec) *fixture {
	t.Helper()
	pg := vectorDB(t)
	legacy, err := pgstore.CreateGenerationVectorStore(t.Context(), pg, testDims, pgstore.LegacyGeneration)
	require.NoError(t, err)
	f := &fixture{pg: pg, legacy: legacy, chunks: map[string][]domain.Chunk{}}
	for _, d := range docs {
		f.add(t, d)
	}
	return f
}

func TestTaggedRefusalAndVectorKey(t *testing.T) {
	docID := uuid.NewString()
	incID := uuid.NewString()
	attID := uuid.NewString()
	revID := uuid.NewString()
	chunk := domain.Chunk{ID: domain.ChunkID(hexChunkID(docID, 0)), Index: 3}
	chunkID := string(chunk.ID)

	untagged := pgstore.DocumentRevision{ID: docID, Incarnation: incID}
	require.Empty(t, taggedRefusal(false, untagged))
	require.Empty(t, taggedRefusal(true, untagged))
	key, err := vectorKey(true, untagged, chunk)
	require.NoError(t, err)
	require.Equal(t, chunkID, key)

	tagged := pgstore.DocumentRevision{ID: docID, Incarnation: incID, ScopedVectors: true, VectorAttempt: attID}
	require.True(t, isTagged(tagged))
	require.Empty(t, taggedRefusal(true, tagged))
	require.Empty(t, taggedRefusal(false, tagged))
	require.Contains(t, taggedRefusal(true, pgstore.DocumentRevision{ID: docID, Incarnation: incID, ScopedVectors: true}), "attempt")
	require.Contains(t, taggedRefusal(true, pgstore.DocumentRevision{ID: docID, Incarnation: incID, ScopedVectors: true, VectorAttempt: attID, MutationPending: true}), "attempt")

	key, err = vectorKey(true, tagged, chunk)
	require.NoError(t, err)
	require.Equal(t, docID+"/"+incID+"/"+attID+"/"+chunkID, key)
	pgKey, err := vectorKey(false, tagged, chunk)
	require.NoError(t, err)
	require.Equal(t, chunkID, pgKey)

	// A document with an active hosted revision keeps its manifest keys whether or
	// not it is also marked tagged, and its attempt never decides its key.
	for _, manifested := range []pgstore.DocumentRevision{
		{ID: docID, Incarnation: incID, ActiveRevisionID: revID},
		{ID: docID, Incarnation: incID, ActiveRevisionID: revID, ScopedVectors: true, VectorAttempt: attID},
		{ID: docID, Incarnation: incID, ActiveRevisionID: revID, ScopedVectors: true, MutationPending: true},
	} {
		require.False(t, isTagged(manifested))
		require.Empty(t, taggedRefusal(true, manifested))
		require.Contains(t, taggedRefusal(false, manifested), "hosted vector revisions")
		for _, tagged := range []bool{true, false} {
			key, err = vectorKey(tagged, manifested, chunk)
			require.NoError(t, err)
			require.Equal(t, domain.FormatRevisionVectorID(domain.DocumentID(docID), revID, 3), key)
		}
	}
}

func TestStageTaggedSnapshotUnit(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "First tagged chunk.", "Second tagged chunk.")
	chunks := []domain.Chunk{
		{ID: domain.ChunkID(hexChunkID(doc.ID, 0)), DocumentID: domain.DocumentID(doc.ID), Index: 0, Text: doc.Texts[0], TokenCount: 3},
		{ID: domain.ChunkID(hexChunkID(doc.ID, 1)), DocumentID: domain.DocumentID(doc.ID), Index: 1, Text: doc.Texts[1], TokenCount: 3},
	}
	src, sourceIndex := memoryPinecone()
	tenant := uuid.NewString()
	source := sourceChunks(doc, chunks, sourceModel)
	for i := range source {
		source[i].VectorIncarnation = ""
		source[i].Metadata[domain.VectorTenantKey] = tenant
	}
	require.NoError(t, src.Upsert(t.Context(), "", source))
	// The serving worker sets the revision, attempt and tenant on tagged writes.
	// The separate incarnation field is omitted by the Pinecone writer.
	raw, err := sourceIndex.FetchVectors(t.Context(), sourceIndex.IDs())
	require.NoError(t, err)
	require.Len(t, raw.Vectors, len(chunks))
	for _, vector := range raw.Vectors {
		fields := vector.Metadata.GetFields()
		require.NotContains(t, fields, domain.VectorIncarnationKey)
		require.Equal(t, doc.Incarnation, fields[domain.VectorRevisionKey].GetStringValue())
		require.Equal(t, doc.Attempt, fields[domain.VectorAttemptKey].GetStringValue())
		require.Equal(t, tenant, fields[domain.VectorTenantKey].GetStringValue())
	}

	snapshot := pgstore.DocumentSnapshot{
		DocumentRevision: pgstore.DocumentRevision{
			ID: doc.ID, Incarnation: doc.Incarnation, DocType: doc.DocType, Collection: doc.Collection, TenantID: tenant,
			DeclaredChunks: len(chunks), Chunks: len(chunks), TextBytes: int64(len(doc.Texts[0]) + len(doc.Texts[1])),
			Revision: "tagged-rev-1", ScopedVectors: true, VectorAttempt: doc.Attempt,
		},
		Chunks: chunks,
	}
	target, targetIdx := memoryPinecone()
	led := &recordingLedger{}
	opts := defaultOptions()
	s := &stager{
		corpus: snapshotCorpus{snapshot}, ledger: led, target: pineconeStore{target},
		generation: "g2", taggedTarget: true, copyFromTagged: true,
		profile: testProfile(sourceModel), opts: opts, copyFrom: src,
	}
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.True(t, rep.Complete)
	require.Equal(t, 1, rep.Staged)
	require.Equal(t, 2, rep.Copied)
	require.Zero(t, rep.Embedded)

	wantKeys := make([]string, len(chunks))
	for i, c := range chunks {
		wantKeys[i] = doc.ID + "/" + doc.Incarnation + "/" + doc.Attempt + "/" + string(c.ID)
	}
	slices.Sort(wantKeys)
	require.Equal(t, wantKeys, targetIdx.IDs())
	fetched, err := target.Fetch(t.Context(), wantKeys)
	require.NoError(t, err)
	for _, key := range wantKeys {
		got := fetched[key]
		require.Equal(t, doc.Incarnation, got.VectorIncarnation)
		require.Equal(t, doc.Incarnation, got.Metadata[domain.VectorRevisionKey])
		require.Equal(t, doc.Attempt, got.Metadata[domain.VectorAttemptKey])
	}
}

func TestStageCopiesTaggedDocumentsUnderPublishedKeys(t *testing.T) {
	unowned := newTaggedDoc("alpha", "report", "First unowned tagged chunk.", "Second unowned tagged chunk.")
	owned := newTaggedDoc("beta", "note", "Owned tagged chunk.")
	tenantID := uuid.NewString()
	principalID := uuid.NewString()

	f := newTaggedFixture(t, unowned, owned)
	assignDocTenant(t, f.pg, owned, tenantID, principalID)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	// Also populate tenant_id in the source record for the owned document.
	ownedSource := sourceChunks(owned, f.chunks[owned.ID], sourceModel)
	ownedSource[0].Metadata[domain.VectorTenantKey] = tenantID
	require.NoError(t, pcstore.NewClientForIndex(h.stores.index("quarry"), testConfig(), "").Upsert(t.Context(), "", ownedSource))

	inv, err := h.run(options{Generation: "g2", Profile: h.profileFile("same.json", testProfile(sourceModel)), TargetStore: "pinecone", TargetIndex: "quarry-g2"})
	require.NoError(t, err)
	require.Empty(t, inv["blockers"])
	require.EqualValues(t, 0, inv["documents"].(map[string]any)["tagged_vectors"])
	require.EqualValues(t, 2, inv["plan"].(map[string]any)["documents_to_stage"])

	rep, err := h.run(options{Generation: "g2", Profile: h.profileFile("same.json", testProfile(sourceModel)), TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, rep["complete"])
	require.EqualValues(t, 2, rep["staged_documents"])
	require.EqualValues(t, 3, rep["copied_chunks"])
	require.EqualValues(t, 0, rep["embedded_chunks"])
	require.Zero(t, h.embedCalls())

	targetIdx := h.stores.index("quarry-g2")
	targetClient := pcstore.NewClientForIndex(targetIdx, testConfig(), "")
	var wantKeys []string
	wantTenant := map[string]string{}
	for _, d := range []docSpec{unowned, owned} {
		var docKeys []string
		for _, c := range f.chunks[d.ID] {
			k, keyErr := pcstore.TaggedVectorID(d.ID, d.Incarnation, d.Attempt, string(c.ID))
			require.NoError(t, keyErr)
			wantKeys = append(wantKeys, k)
			docKeys = append(docKeys, k)
			if d.ID == owned.ID {
				wantTenant[k] = tenantID
			}
		}
		slices.Sort(docKeys)
		cps, cpErr := f.pg.GenerationCheckpoints(t.Context(), "g2", []string{d.ID})
		require.NoError(t, cpErr)
		require.Equal(t, docKeys, cps[d.ID].ChunkIDs, "ledger stores physical target keys")
	}
	slices.Sort(wantKeys)
	require.Equal(t, wantKeys, targetIdx.IDs())

	fetched, err := targetClient.Fetch(t.Context(), wantKeys)
	require.NoError(t, err)
	require.Len(t, fetched, len(wantKeys))
	for _, d := range []docSpec{unowned, owned} {
		for _, c := range f.chunks[d.ID] {
			k, keyErr := pcstore.TaggedVectorID(d.ID, d.Incarnation, d.Attempt, string(c.ID))
			require.NoError(t, keyErr)
			got := fetched[k]
			require.Equal(t, domain.DocumentID(d.ID), got.DocumentID)
			require.Equal(t, d.Incarnation, got.VectorIncarnation)
			require.Equal(t, d.Incarnation, got.Metadata[domain.VectorRevisionKey])
			require.Equal(t, d.Attempt, got.Metadata[domain.VectorAttemptKey])
			require.Equal(t, d.DocType, got.Metadata["doc_type"])
			if d.Collection != "" {
				require.Equal(t, d.Collection, got.Metadata["collection"])
			}
			if wantTenant[k] != "" {
				require.Equal(t, wantTenant[k], got.Metadata[domain.VectorTenantKey])
			} else {
				require.NotContains(t, got.Metadata, domain.VectorTenantKey)
			}
			require.Equal(t, embedWith(sourceModel, c.Text), got.Embedding)
		}
	}
}

func TestStageRecomputesTaggedDocuments(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Recomputed tagged chunk one.", "Recomputed tagged chunk two.")
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()

	profile := h.profileFile("model-b.json", testProfile("model-b"))
	rep, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true, MaxEmbedCalls: 2})
	require.NoError(t, err)
	require.Equal(t, true, rep["complete"])
	require.EqualValues(t, 0, rep["copied_chunks"])
	require.EqualValues(t, 2, rep["embedded_chunks"])
	require.Equal(t, 2, h.embedCalls())

	targetIdx := h.stores.index("quarry-g2")
	targetClient := pcstore.NewClientForIndex(targetIdx, testConfig(), "")
	var wantKeys []string
	for _, c := range f.chunks[doc.ID] {
		k, keyErr := pcstore.TaggedVectorID(doc.ID, doc.Incarnation, doc.Attempt, string(c.ID))
		require.NoError(t, keyErr)
		wantKeys = append(wantKeys, k)
	}
	slices.Sort(wantKeys)
	require.Equal(t, wantKeys, targetIdx.IDs())
	fetched, err := targetClient.Fetch(t.Context(), wantKeys)
	require.NoError(t, err)
	for _, c := range f.chunks[doc.ID] {
		k, keyErr := pcstore.TaggedVectorID(doc.ID, doc.Incarnation, doc.Attempt, string(c.ID))
		require.NoError(t, keyErr)
		got := fetched[k]
		require.Equal(t, doc.Incarnation, got.VectorIncarnation)
		require.Equal(t, doc.Incarnation, got.Metadata[domain.VectorRevisionKey])
		require.Equal(t, doc.Attempt, got.Metadata[domain.VectorAttemptKey])
		require.Equal(t, embedWith("model-b", c.Text), got.Embedding)
	}
}

func TestStageMixedCorpus(t *testing.T) {
	tagged := newTaggedDoc("alpha", "report", "Tagged corpus chunk.")
	untagged := newDoc("alpha", "note", "Untagged corpus chunk.")
	f := newTaggedFixture(t, tagged, untagged)
	h := newHarness(t, f, "pinecone")
	h.attestSource()

	rep, err := h.run(options{Generation: "g2", Profile: h.profileFile("same.json", testProfile(sourceModel)), TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, rep["complete"])
	require.EqualValues(t, 2, rep["staged_documents"])
	require.EqualValues(t, 2, rep["copied_chunks"])

	taggedKey, err := pcstore.TaggedVectorID(tagged.ID, tagged.Incarnation, tagged.Attempt, string(f.chunks[tagged.ID][0].ID))
	require.NoError(t, err)
	untaggedKey := string(f.chunks[untagged.ID][0].ID)
	wantKeys := []string{taggedKey, untaggedKey}
	slices.Sort(wantKeys)
	targetIdx := h.stores.index("quarry-g2")
	require.Equal(t, wantKeys, targetIdx.IDs())

	fetched, err := pcstore.NewClientForIndex(targetIdx, testConfig(), "").Fetch(t.Context(), wantKeys)
	require.NoError(t, err)
	require.Equal(t, tagged.Attempt, fetched[taggedKey].Metadata[domain.VectorAttemptKey])
	require.NotContains(t, fetched[untaggedKey].Metadata, domain.VectorRevisionKey)
	require.NotContains(t, fetched[untaggedKey].Metadata, domain.VectorAttemptKey)
}

func TestTaggedDocumentWithoutAttemptIsRefused(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Missing attempt chunk.")
	doc.Attempt = ""
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	inv, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2"})
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(inv["blockers"]), "no determinable published vector attempt")
	require.EqualValues(t, 1, inv["documents"].(map[string]any)["tagged_vectors"])

	_, err = h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.ErrorContains(t, err, "staging is blocked")

	// Direct stager run also marks it unsupported and fails cleanly.
	client, _ := memoryPinecone()
	registerGeneration(t, f.pg, "g2", "pinecone", "quarry-g2", sourceModel)
	s := &stager{corpus: f.pg, ledger: f.pg, target: pineconeStore{client}, generation: "g2", taggedTarget: true, copyFromTagged: true, profile: testProfile(sourceModel), opts: defaultOptions()}
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{doc.ID}, rep.Unsupported)
	require.False(t, rep.Complete)

	vRep, err := h.run(verifyOptions("g2"))
	require.ErrorIs(t, err, errNotVerified)
	require.Contains(t, vRep["failures"], "1 tagged documents have no determinable published vector attempt")
}

func TestTaggedDocumentWithPendingMutationIsRefused(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Pending mutation chunk.")
	f := newTaggedFixture(t, doc)
	_, err := f.pg.Pool.Exec(t.Context(), `UPDATE documents SET vector_mutation_pending=TRUE WHERE id=$1`, doc.ID)
	require.NoError(t, err)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	inv, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2"})
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(inv["blockers"]), "no determinable published vector attempt")
	require.EqualValues(t, 1, inv["documents"].(map[string]any)["tagged_vectors"])

	_, err = h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.ErrorContains(t, err, "staging is blocked")

	client, _ := memoryPinecone()
	registerGeneration(t, f.pg, "g2", "pinecone", "quarry-g2", sourceModel)
	s := &stager{corpus: f.pg, ledger: f.pg, target: pineconeStore{client}, generation: "g2", taggedTarget: true, copyFromTagged: true, profile: testProfile(sourceModel), opts: defaultOptions()}
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{doc.ID}, rep.Unsupported)
	require.False(t, rep.Complete)

	vRep, err := h.run(verifyOptions("g2"))
	require.ErrorIs(t, err, errNotVerified)
	require.Contains(t, vRep["failures"], "1 tagged documents have no determinable published vector attempt")
}

func TestUncertainTaggedDocumentIsStaged(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Uncertain tagged chunk.")
	f := newTaggedFixture(t, doc)
	_, err := f.pg.Pool.Exec(t.Context(), `UPDATE documents SET vector_mutation_uncertain=TRUE WHERE id=$1`, doc.ID)
	require.NoError(t, err)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	rep, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, rep["complete"])
	require.EqualValues(t, 1, rep["staged_documents"])
	require.EqualValues(t, 1, rep["copied_chunks"])
}

func TestStageIgnoresStaleAttemptRecords(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Published attempt chunk.")
	staleOnly := newTaggedDoc("alpha", "report", "Only stale attempt exists.")
	f := newTaggedFixture(t, doc, staleOnly)
	h := newHarness(t, f, "pinecone")
	h.attestSource()

	liveIdx := h.stores.index("quarry")
	liveClient := pcstore.NewClientForIndex(liveIdx, testConfig(), "")
	// Delete published keys for staleOnly so only a stale attempt exists.
	for _, c := range f.chunks[staleOnly.ID] {
		k, err := pcstore.TaggedVectorID(staleOnly.ID, staleOnly.Incarnation, staleOnly.Attempt, string(c.ID))
		require.NoError(t, err)
		require.NoError(t, liveIdx.DeleteVectorsByID(t.Context(), []string{k}))
	}
	for _, d := range []docSpec{doc, staleOnly} {
		staleSpec := d
		staleSpec.Attempt = uuid.NewString()
		stale := sourceChunks(staleSpec, f.chunks[d.ID], "stale-model")
		require.NoError(t, liveClient.Upsert(t.Context(), "", stale))
	}

	profile := h.profileFile("same.json", testProfile(sourceModel))
	rep, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true, MaxEmbedCalls: 10})
	require.NoError(t, err)
	require.Equal(t, true, rep["complete"])
	require.EqualValues(t, 1, rep["copied_chunks"])
	require.EqualValues(t, 1, rep["embedded_chunks"])

	targetIdx := h.stores.index("quarry-g2")
	var wantKeys []string
	for _, d := range []docSpec{doc, staleOnly} {
		k, keyErr := pcstore.TaggedVectorID(d.ID, d.Incarnation, d.Attempt, string(f.chunks[d.ID][0].ID))
		require.NoError(t, keyErr)
		wantKeys = append(wantKeys, k)
	}
	slices.Sort(wantKeys)
	require.Equal(t, wantKeys, targetIdx.IDs())
	fetched, err := pcstore.NewClientForIndex(targetIdx, testConfig(), "").Fetch(t.Context(), wantKeys)
	require.NoError(t, err)
	for _, d := range []docSpec{doc, staleOnly} {
		c := f.chunks[d.ID][0]
		k, keyErr := pcstore.TaggedVectorID(d.ID, d.Incarnation, d.Attempt, string(c.ID))
		require.NoError(t, keyErr)
		require.Equal(t, embedWith(sourceModel, c.Text), fetched[k].Embedding)
	}
}

func TestPruneRemovesTaggedKeys(t *testing.T) {
	kept := newTaggedDoc("alpha", "report", "Kept tagged chunk.")
	removed := newTaggedDoc("alpha", "report", "Removed tagged chunk.")
	f := newTaggedFixture(t, kept, removed)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, 2, h.stores.index("quarry-g2").Len())

	_, err = f.pg.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, removed.ID)
	require.NoError(t, err)

	rep, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, rep["complete"])
	require.EqualValues(t, 1, rep["pruned_documents"])
	require.EqualValues(t, 1, rep["pruned_keys"])

	keptKey, err := pcstore.TaggedVectorID(kept.ID, kept.Incarnation, kept.Attempt, string(f.chunks[kept.ID][0].ID))
	require.NoError(t, err)
	require.Equal(t, []string{keptKey}, h.stores.index("quarry-g2").IDs())
	cps, err := f.pg.GenerationCheckpoints(t.Context(), "g2", []string{removed.ID})
	require.NoError(t, err)
	require.Empty(t, cps)
}

func TestRollbackKeepsCurrentTaggedKeys(t *testing.T) {
	kept := newTaggedDoc("alpha", "report", "Current tagged document.")
	deleted := newTaggedDoc("alpha", "report", "Deleted tagged document.")
	untagged := newDoc("alpha", "report", "Untagged document that stays current.")
	f := newTaggedFixture(t, kept, deleted, untagged)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	// Register g2 as an ever-active retired Pinecone generation.
	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(t.Context(), `UPDATE vector_generations SET state='retired', ever_active=TRUE WHERE name='g2'`)
	require.NoError(t, err)

	// Delete one document and seed stale/invalid tagged keys in the retired index.
	_, err = f.pg.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, deleted.ID)
	require.NoError(t, err)

	g2Idx := h.stores.index("quarry-g2")
	g2Client := pcstore.NewClientForIndex(g2Idx, testConfig(), "")
	chunkID := string(f.chunks[kept.ID][0].ID)
	currentKey, err := pcstore.TaggedVectorID(kept.ID, kept.Incarnation, kept.Attempt, chunkID)
	require.NoError(t, err)

	staleAttemptSpec := kept
	staleAttemptSpec.Attempt = uuid.NewString()
	staleIncSpec := kept
	staleIncSpec.Incarnation = uuid.NewString()
	threePartSpec := kept
	threePartSpec.Attempt = ""
	// The earlier untagged incarnation of the kept document left a record at the
	// bare chunk key, which a hosted target treats as stale for a tagged document.
	untaggedIncarnationSpec := kept
	untaggedIncarnationSpec.Tagged = false
	unledgeredDeleted := newTaggedDoc("alpha", "report", "Unledgered deleted doc.")
	unledgeredDeletedChunks := []domain.Chunk{{ID: domain.ChunkID(hexChunkID(unledgeredDeleted.ID, 0)), DocumentID: domain.DocumentID(unledgeredDeleted.ID), Index: 0, Text: unledgeredDeleted.Texts[0], TokenCount: 3}}
	for _, tc := range []struct {
		spec   docSpec
		chunks []domain.Chunk
	}{
		{staleAttemptSpec, f.chunks[kept.ID]},
		{staleIncSpec, f.chunks[kept.ID]},
		{threePartSpec, f.chunks[kept.ID]},
		{untaggedIncarnationSpec, f.chunks[kept.ID]},
		{unledgeredDeleted, unledgeredDeletedChunks},
	} {
		require.NoError(t, g2Client.Upsert(t.Context(), "", sourceChunks(tc.spec, tc.chunks, sourceModel)))
	}
	require.Equal(t, 8, g2Idx.Len())

	require.NoError(t, f.pg.FreezeWriters(t.Context(), "g2"))
	rep, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, rep["complete"])
	untaggedKey := string(f.chunks[untagged.ID][0].ID)
	require.ElementsMatch(t, []string{currentKey, untaggedKey}, g2Idx.IDs())
}

func TestPostgresTargetRefusesUnresolvedTaggedDocuments(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Tagged document.")
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	_, err := f.pg.Pool.Exec(t.Context(), `UPDATE documents SET vector_mutation_pending=TRUE WHERE id=$1`, doc.ID)
	require.NoError(t, err)
	profile := h.profileFile("same.json", testProfile(sourceModel))

	inv, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "postgres"})
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(inv["blockers"]), "no determinable published vector attempt")
	require.EqualValues(t, 1, inv["documents"].(map[string]any)["tagged_vectors"])

	_, err = h.run(options{Generation: "g2", Profile: profile, TargetStore: "postgres", Apply: true})
	require.ErrorContains(t, err, "staging is blocked")

	pgTarget(t, f.pg, "g2")
	vRep, err := h.run(verifyOptions("g2"))
	require.ErrorIs(t, err, errNotVerified)
	require.Contains(t, vRep["failures"], "1 tagged documents have no determinable published vector attempt")
}

func TestTaggedDocumentRespectsBudgets(t *testing.T) {
	d1 := newTaggedDoc("alpha", "report", "Chunk one.", "Chunk two.")
	d2 := newTaggedDoc("alpha", "report", "Chunk three.")
	f := newTaggedFixture(t, d1, d2)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	profile := h.profileFile("same.json", testProfile(sourceModel))

	// First run stops at -max-chunks 2 after staging only one document (or hits limit on first if d1 is 2 chunks and d2 is 1).
	opts := defaultOptions()
	opts.Generation, opts.Profile, opts.TargetStore, opts.TargetIndex, opts.Apply = "g2", profile, "pinecone", "quarry-g2", true
	opts.MaxChunks = 2
	rep, err := h.run(opts)
	require.NoError(t, err)
	require.Equal(t, "chunk or byte limit", rep["stopped"])
	require.LessOrEqual(t, rep["staged_chunks"], float64(2))

	// A tagged document alone exceeding -max-chunks is refused.
	twoChunk := newTaggedDoc("alpha", "report", "Big one.", "Big two.")
	f2 := newTaggedFixture(t, twoChunk)
	h2 := newHarness(t, f2, "pinecone")
	h2.attestSource()
	opts2 := defaultOptions()
	opts2.Generation, opts2.Profile, opts2.TargetStore, opts2.TargetIndex, opts2.Apply = "g2", h2.profileFile("same.json", testProfile(sourceModel)), "pinecone", "quarry-g2", true
	opts2.MaxChunks = 1
	_, err = h2.run(opts2)
	require.ErrorContains(t, err, "alone exceeds")

	// A tagged document alone exceeding -max-bytes is refused.
	opts2.MaxChunks = 100
	opts2.MaxBytes = 5
	_, err = h2.run(opts2)
	require.ErrorContains(t, err, "alone exceeds")
}

func TestTaggedHostedSourceMustMatchPublishedIdentity(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Source data to copy only with trusted ownership.")
	snap := incarnationSnapshot(doc)
	snap.Incarnation, snap.VectorAttempt, snap.ScopedVectors, snap.TenantID = doc.Incarnation, doc.Attempt, true, uuid.NewString()
	key, err := vectorKey(true, snap.DocumentRevision, snap.Chunks[0])
	require.NoError(t, err)
	otherID := func() *structpb.Value { return structpb.NewStringValue(uuid.NewString()) }
	for _, tc := range []struct {
		name, field string
		value       *structpb.Value
		required    bool
		unowned     bool
		copy        bool
	}{
		{name: "valid", copy: true},
		{name: "missing revision", field: domain.VectorRevisionKey},
		{name: "wrong revision", field: domain.VectorRevisionKey, value: otherID()},
		{name: "missing attempt", field: domain.VectorAttemptKey},
		{name: "wrong attempt", field: domain.VectorAttemptKey, value: otherID()},
		{name: "missing optional incarnation", field: domain.VectorIncarnationKey, copy: true},
		{name: "missing required incarnation", field: domain.VectorIncarnationKey, required: true},
		{name: "valid required incarnation", required: true, copy: true},
		{name: "wrong incarnation", field: domain.VectorIncarnationKey, value: otherID()},
		{name: "empty incarnation", field: domain.VectorIncarnationKey, value: structpb.NewStringValue("")},
		{name: "malformed incarnation", field: domain.VectorIncarnationKey, value: structpb.NewNumberValue(1)},
		{name: "missing required tenant", field: domain.VectorTenantKey},
		{name: "wrong tenant", field: domain.VectorTenantKey, value: otherID()},
		{name: "malformed tenant", field: domain.VectorTenantKey, value: structpb.NewBoolValue(true)},
		{name: "missing tenant on unowned document", unowned: true, copy: true},
		{name: "malformed tenant on unowned document", field: domain.VectorTenantKey, value: structpb.NewNullValue(), unowned: true},
		{name: "wrong document", field: "doc_id", value: otherID()},
		{name: "wrong text", field: "text", value: structpb.NewStringValue("different text")},
		{name: "wrong position", field: "chunk_index", value: structpb.NewNumberValue(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := snap
			snapshot.VectorIncarnationRequired = tc.required
			if tc.unowned {
				snapshot.TenantID = ""
			}
			chunk := sourceChunks(doc, snap.Chunks, sourceModel)[0]
			if snapshot.TenantID != "" {
				chunk.Metadata[domain.VectorTenantKey] = snapshot.TenantID
			}
			src, index := memoryPinecone()
			require.NoError(t, src.Upsert(t.Context(), "", []domain.Chunk{chunk}))
			if tc.field != "" {
				raw, err := index.FetchVectors(t.Context(), []string{key})
				require.NoError(t, err)
				vector := raw.Vectors[key]
				require.NotNil(t, vector)
				if tc.value == nil {
					delete(vector.Metadata.Fields, tc.field)
				} else {
					vector.Metadata.Fields[tc.field] = tc.value
				}
				_, err = index.UpsertVectors(t.Context(), []*pinecone.Vector{vector})
				require.NoError(t, err)
			}

			s := stager{copyFrom: src, copyFromTagged: true, profile: testProfile(sourceModel), opts: defaultOptions()}
			_, copied, embedded, err := s.vectors(t.Context(), &snapshot)
			if tc.copy {
				require.NoError(t, err)
				require.Equal(t, 1, copied)
				require.Zero(t, embedded)
				return
			}
			require.ErrorContains(t, err, "embedding call budget")
			s.embed, s.opts.MaxEmbedCalls = &countingEmbedder{model: sourceModel}, 1
			_, copied, embedded, err = s.vectors(t.Context(), &snapshot)
			require.NoError(t, err)
			require.Zero(t, copied)
			require.Equal(t, 1, embedded)
		})
	}
}

func TestPostgresTaggedTargetRecomputesOnProfileChange(t *testing.T) {
	doc := newTaggedDoc("alpha", "report", "Recomputed after changing the embedding model.")
	f := newTaggedFixture(t, doc)
	h := newHarness(t, f, "pinecone")
	h.attestSource()
	const model = "model-b"
	report, err := h.run(options{Generation: "g2", Profile: h.profileFile("changed.json", testProfile(model)), TargetStore: "postgres", Apply: true, MaxEmbedCalls: 1})
	require.NoError(t, err, "%v", report)
	require.EqualValues(t, 1, report["embedded_chunks"])
	require.Zero(t, report["copied_chunks"].(float64))
	require.Equal(t, 1, h.embedCalls())
	store, err := pgstore.OpenGenerationVectorStore(t.Context(), f.pg, testDims, "g2")
	require.NoError(t, err)
	key := string(f.chunks[doc.ID][0].ID)
	records, err := store.Fetch(t.Context(), []string{key})
	require.NoError(t, err)
	require.Equal(t, embedWith(model, doc.Texts[0]), records[key].Embedding)
}
