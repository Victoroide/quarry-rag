package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pg "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/stretchr/testify/require"
)

const (
	incarnationChunk = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherIncarnation = "11111111-1111-4111-8111-111111111111"
)

// recordingLedger keeps the document checkpoints of one run in memory.
type recordingLedger struct {
	ledger
	begun, completed []string
}

func (l *recordingLedger) GenerationCheckpoints(context.Context, string, []string) (map[string]pg.Checkpoint, error) {
	return map[string]pg.Checkpoint{}, nil
}

func (l *recordingLedger) BeginGenerationDocument(_ context.Context, _, document string, _ []string) ([]string, error) {
	l.begun = append(l.begun, document)
	return nil, nil
}

func (l *recordingLedger) CompleteGenerationDocument(_ context.Context, _, document, _, _ string, _ []string, _, _ int) error {
	l.completed = append(l.completed, document)
	return nil
}

func (l *recordingLedger) OrphanedGenerationDocuments(context.Context, string, int) ([]pg.Checkpoint, error) {
	return nil, nil
}

func (l *recordingLedger) HasOrphanedGenerationDocuments(context.Context, string) (bool, error) {
	return false, nil
}

func incarnationSnapshot(doc docSpec) pg.DocumentSnapshot {
	chunks := []domain.Chunk{{ID: incarnationChunk, DocumentID: domain.DocumentID(doc.ID), Index: 0, TokenCount: 3, Text: doc.Texts[0]}}
	return pg.DocumentSnapshot{
		DocumentRevision: pg.DocumentRevision{ID: doc.ID, Incarnation: doc.ID, DocType: doc.DocType, Collection: doc.Collection, DeclaredChunks: 1, Chunks: 1, TextBytes: int64(len(doc.Texts[0])), Revision: "revision-one"},
		Chunks:           chunks,
	}
}

func stageSnapshot(t *testing.T, snapshot pg.DocumentSnapshot, source vectorReader, embedder *countingEmbedder) (stageReport, vectorReader, *recordingLedger) {
	t.Helper()
	target, _ := memoryPinecone()
	led := &recordingLedger{}
	opts := defaultOptions()
	opts.MaxEmbedCalls = 10
	s := &stager{corpus: snapshotCorpus{snapshot}, ledger: led, target: pineconeStore{target}, generation: "g2", profile: testProfile(sourceModel), opts: opts, copyFrom: source, embed: embedder}
	rep, err := s.run(t.Context())
	require.NoError(t, err)
	return rep, target, led
}

func TestStagedRecordsNameTheDocumentIncarnation(t *testing.T) {
	doc := newDoc("alpha", "report", "A stored chunk of the document.")
	snapshot := incarnationSnapshot(doc)
	source := func(tag string) vectorReader {
		src, _ := memoryPinecone()
		chunks := sourceChunks(doc, snapshot.Chunks, sourceModel)
		chunks[0].VectorIncarnation = tag
		require.NoError(t, src.Upsert(t.Context(), "", chunks))
		return src
	}
	for _, tc := range []struct {
		name     string
		source   func() vectorReader
		copied   int
		embedded int
	}{
		{"computed from the stored text", func() vectorReader { return nil }, 0, 1},
		{"copied from a record without a tag", func() vectorReader { return source("") }, 1, 0},
		{"copied from a record of this incarnation", func() vectorReader { return source(doc.ID) }, 1, 0},
		{"a record of another incarnation is recomputed", func() vectorReader { return source(otherIncarnation) }, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			embedder := &countingEmbedder{model: sourceModel}
			rep, target, led := stageSnapshot(t, snapshot, tc.source(), embedder)
			require.True(t, rep.Complete)
			require.Equal(t, tc.copied, rep.Copied)
			require.Equal(t, tc.embedded, rep.Embedded)
			require.Equal(t, []string{doc.ID}, led.completed)
			stored, err := target.Fetch(t.Context(), []string{incarnationChunk})
			require.NoError(t, err)
			require.Contains(t, stored, incarnationChunk)
			require.Equal(t, doc.ID, stored[incarnationChunk].VectorIncarnation, "retrieval discards a record that does not name its document")
			require.Equal(t, embedWith(sourceModel, doc.Texts[0]), stored[incarnationChunk].Embedding)
		})
	}
}

func TestStagedRecordsCarryTenantLabelForOwnedDocuments(t *testing.T) {
	const (
		tenantA = "22222222-2222-4222-8222-222222222222"
		tenantB = "33333333-3333-4333-8333-333333333333"
	)
	doc := newDoc("alpha", "report", "A stored chunk of the document.")
	source := func(tenant string) vectorReader {
		src, _ := memoryPinecone()
		chunks := sourceChunks(doc, incarnationSnapshot(doc).Chunks, sourceModel)
		if tenant != "" {
			chunks[0].Metadata[domain.VectorTenantKey] = tenant
		}
		require.NoError(t, src.Upsert(t.Context(), "", chunks))
		return src
	}
	for _, tc := range []struct {
		name     string
		tenant   string
		source   func() vectorReader
		copied   int
		embedded int
	}{
		{"owned document computed from stored text", tenantA, func() vectorReader { return nil }, 0, 1},
		{"owned document copied from an unlabeled source", tenantA, func() vectorReader { return source("") }, 1, 0},
		{"owned document copied from its own tenant", tenantA, func() vectorReader { return source(tenantA) }, 1, 0},
		{"owned document recomputes a record from another tenant", tenantA, func() vectorReader { return source(tenantB) }, 0, 1},
		{"unowned document computed without a tenant label", "", func() vectorReader { return nil }, 0, 1},
		{"unowned document copied without a tenant label", "", func() vectorReader { return source("") }, 1, 0},
		{"unowned document recomputes an unexpected tenant label", "", func() vectorReader { return source(tenantA) }, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := incarnationSnapshot(doc)
			snapshot.TenantID = tc.tenant
			embedder := &countingEmbedder{model: sourceModel}
			rep, target, _ := stageSnapshot(t, snapshot, tc.source(), embedder)
			require.True(t, rep.Complete)
			require.Equal(t, tc.copied, rep.Copied)
			require.Equal(t, tc.embedded, rep.Embedded)
			stored, err := target.Fetch(t.Context(), []string{incarnationChunk})
			require.NoError(t, err)
			require.Contains(t, stored, incarnationChunk)
			if tc.tenant == "" {
				require.NotContains(t, stored[incarnationChunk].Metadata, domain.VectorTenantKey)
			} else {
				require.Equal(t, tc.tenant, stored[incarnationChunk].Metadata[domain.VectorTenantKey])
			}
			require.Equal(t, embedWith(sourceModel, doc.Texts[0]), stored[incarnationChunk].Embedding)
		})
	}
}

func TestTaggedVectorDocumentsAreNeverStaged(t *testing.T) {
	doc := newDoc("alpha", "report", "A document stored in the tagged format.")
	snapshot := incarnationSnapshot(doc)
	snapshot.ScopedVectors = true
	embedder := &countingEmbedder{model: sourceModel}
	rep, target, led := stageSnapshot(t, snapshot, nil, embedder)
	require.Equal(t, []string{doc.ID}, rep.Unsupported)
	require.False(t, rep.Complete, "a corpus with an unsupported document is not fully staged")
	require.Zero(t, rep.Staged)
	require.Empty(t, led.begun, "no checkpoint opens for an unsupported document")
	require.Zero(t, embedder.calls.Load())
	keys, _, err := target.ListIDs(t.Context(), "", 100)
	require.NoError(t, err)
	require.Empty(t, keys)
}

func verifyIncarnation(t *testing.T, snapshot pg.DocumentSnapshot, tagged bool, tag, recordTenant string) verifyReport {
	t.Helper()
	doc := docSpec{ID: snapshot.ID, Collection: snapshot.Collection, DocType: snapshot.DocType, Texts: []string{snapshot.Chunks[0].Text}}
	chunks := sourceChunks(doc, snapshot.Chunks, sourceModel)
	chunks[0].VectorIncarnation = tag
	if recordTenant != "" {
		chunks[0].Metadata[domain.VectorTenantKey] = recordTenant
	}
	src, _ := memoryPinecone()
	dst, _ := memoryPinecone()
	require.NoError(t, src.Upsert(t.Context(), "", chunks))
	require.NoError(t, dst.Upsert(t.Context(), "", chunks))
	opts := defaultOptions()
	opts.SampleSize, opts.ScopeDocuments, opts.MinRecall, opts.MaxP95, opts.TopK = 1, 1, 1, time.Second, 2
	v := verifier{corpus: snapshotCorpus{snapshot}, ledger: currentCheckpointLedger{doc: snapshot.ID, revision: snapshot.Revision, incarnation: snapshot.Incarnation}, target: dst, source: src,
		generation: "g2", profile: testProfile(sourceModel), sourceProfile: testProfile(sourceModel), taggedTarget: tagged, opts: opts, now: time.Now}
	rep, err := v.run(t.Context())
	require.NoError(t, err)
	return rep
}

func TestVerificationRequiresTheIncarnationOnARecordTargetThatCarriesIt(t *testing.T) {
	doc := newDoc("alpha", "report", "A stored chunk of the document.")
	snapshot := incarnationSnapshot(doc)
	for _, tc := range []struct {
		name   string
		tagged bool
		tag    string
		pass   bool
	}{
		{"the record names its document", true, doc.ID, true},
		{"the record has no tag", true, "", false},
		{"the record names another incarnation", true, otherIncarnation, false},
		{"a store without tags is not asked for one", false, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep := verifyIncarnation(t, snapshot, tc.tagged, tc.tag, "")
			require.Equal(t, tc.pass, rep.Passed, "%v", rep.Failures)
			if !tc.pass {
				require.Equal(t, 1, rep.Coverage.Mismatched)
				require.Less(t, rep.Coverage.ProfileCoverage, 1.0)
			}
		})
	}
}

func TestVerificationRequiresTenantLabelForOwnedDocumentsOnlyOnTaggedTargets(t *testing.T) {
	const (
		tenantA = "22222222-2222-4222-8222-222222222222"
		tenantB = "33333333-3333-4333-8333-333333333333"
	)
	doc := newDoc("alpha", "report", "A stored chunk of the document.")
	for _, tc := range []struct {
		name         string
		tagged       bool
		wantTenant   string
		recordTenant string
		pass         bool
	}{
		{"owned document on Pinecone carries matching tenant", true, tenantA, tenantA, true},
		{"owned document on Pinecone without tenant fails", true, tenantA, "", false},
		{"owned document on Pinecone with another tenant fails", true, tenantA, tenantB, false},
		{"owned document on PostgreSQL is not asked for tenant", false, tenantA, "", true},
		{"unowned document on Pinecone has no tenant", true, "", "", true},
		{"unowned document on Pinecone with unexpected tenant fails", true, "", tenantA, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := incarnationSnapshot(doc)
			snapshot.TenantID = tc.wantTenant
			rep := verifyIncarnation(t, snapshot, tc.tagged, doc.ID, tc.recordTenant)
			require.Equal(t, tc.pass, rep.Passed, "%v", rep.Failures)
			if !tc.pass {
				require.Equal(t, 1, rep.Coverage.Mismatched)
				require.Less(t, rep.Coverage.ProfileCoverage, 1.0)
			}
		})
	}
}

func TestVerificationFailsForTaggedVectorDocuments(t *testing.T) {
	doc := newDoc("alpha", "report", "A document stored in the tagged format.")
	snapshot := incarnationSnapshot(doc)
	snapshot.ScopedVectors = true
	rep := verifyIncarnation(t, snapshot, true, doc.ID, "")
	require.False(t, rep.Passed)
	require.Equal(t, 1, rep.Coverage.TaggedVectors)
	require.Contains(t, rep.Failures, "1 documents store tagged vectors, which a reindex does not stage")
}

func TestStagedRecordsPreserveLayoutProvenance(t *testing.T) {
	doc := newDoc("alpha", "report", "Region: EMEA | Revenue: 120")
	makeSource := func(incarnation string, zeroVector bool) vectorReader {
		src, _ := memoryPinecone()
		chunks := sourceChunks(doc, incarnationSnapshot(doc).Chunks, sourceModel)
		chunks[0].VectorIncarnation = incarnation
		if zeroVector {
			chunks[0].Embedding = make([]float32, testDims)
		}
		chunks[0].Metadata["pages"] = []int{2, 5}
		chunks[0].Metadata["table_ids"] = []string{"p2-t1"}
		chunks[0].Metadata["row_ids"] = []string{"p2-t1-r3"}
		require.NoError(t, src.Upsert(t.Context(), "", chunks))
		return src
	}

	for _, tc := range []struct {
		name           string
		snapProvenance bool
		source         func() vectorReader
		copied         int
		embedded       int
		wantProvenance bool
	}{
		{
			name:           "copied from a Pinecone source record",
			snapProvenance: false,
			source:         func() vectorReader { return makeSource(doc.ID, false) },
			copied:         1,
			embedded:       0,
			wantProvenance: true,
		},
		{
			name:           "recomputed from snapshot layout provenance",
			snapProvenance: true,
			source:         func() vectorReader { return nil },
			copied:         0,
			embedded:       1,
			wantProvenance: true,
		},
		{
			name:           "recomputed from invalid embedding keeps source provenance",
			snapProvenance: false,
			source:         func() vectorReader { return makeSource(doc.ID, true) },
			copied:         0,
			embedded:       1,
			wantProvenance: true,
		},
		{
			name:           "stale incarnation source provenance is not copied",
			snapProvenance: false,
			source:         func() vectorReader { return makeSource(otherIncarnation, false) },
			copied:         0,
			embedded:       1,
			wantProvenance: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := incarnationSnapshot(doc)
			if tc.snapProvenance {
				snapshot.Chunks[0].Metadata = map[string]any{
					"pages":     []int{2, 5},
					"table_ids": []string{"p2-t1"},
					"row_ids":   []string{"p2-t1-r3"},
				}
			}
			embedder := &countingEmbedder{model: sourceModel}
			rep, target, _ := stageSnapshot(t, snapshot, tc.source(), embedder)
			require.True(t, rep.Complete)
			require.Equal(t, tc.copied, rep.Copied)
			require.Equal(t, tc.embedded, rep.Embedded)

			fetched, err := target.Fetch(t.Context(), []string{incarnationChunk})
			require.NoError(t, err)
			require.Contains(t, fetched, incarnationChunk)
			queried, err := target.QueryFiltered(t.Context(), embedWith(sourceModel, doc.Texts[0]), 1, domain.VectorFilter{DocumentIDs: []string{doc.ID}})
			require.NoError(t, err)
			require.Len(t, queried, 1)

			for _, md := range []map[string]any{fetched[incarnationChunk].Metadata, queried[0].Chunk.Metadata} {
				if tc.wantProvenance {
					require.Equal(t, []int{2, 5}, md["pages"])
					require.Equal(t, []string{"p2-t1"}, md["table_ids"])
					require.Equal(t, []string{"p2-t1-r3"}, md["row_ids"])
				} else {
					require.NotContains(t, md, "pages")
					require.NotContains(t, md, "table_ids")
					require.NotContains(t, md, "row_ids")
				}
			}
		})
	}
}

func TestTaggedVectorDocumentsBlockStagingAndAreCounted(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "pinecone")
	_, err := f.pg.Pool.Exec(t.Context(), `UPDATE documents SET scoped_vectors=TRUE WHERE id=$1`, f.docs[0].ID)
	require.NoError(t, err)
	profile := h.profileFile("target.json", testProfile("model-b"))

	report, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "postgres"})
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(report["blockers"]), "1 completed documents store tagged vectors")
	require.EqualValues(t, 1, report["documents"].(map[string]any)["tagged_vectors"])
	require.EqualValues(t, 3, report["plan"].(map[string]any)["documents_to_stage"], "the tagged document is not planned")

	before := writeTableCounts(t, f.pg)
	_, err = h.run(options{Generation: "g2", Profile: profile, TargetStore: "postgres", Apply: true})
	require.ErrorContains(t, err, "staging is blocked")
	require.Equal(t, before, writeTableCounts(t, f.pg), "a blocked run writes nothing")
}
