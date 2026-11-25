package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pg "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/stretchr/testify/require"
)

func matchedQuerySnapshot() (docSpec, pg.DocumentSnapshot) {
	doc := newDoc("catalog", "report", "The first train stops at Riverside.", "The last bus stops at the library.")
	var chunks []domain.Chunk
	var textBytes int64
	for i, text := range doc.Texts {
		chunks = append(chunks, domain.Chunk{ID: domain.ChunkID(hexChunkID(doc.ID, i)), DocumentID: domain.DocumentID(doc.ID), Index: i, Text: text, TokenCount: 7})
		textBytes += int64(len(text))
	}
	return doc, pg.DocumentSnapshot{
		DocumentRevision: pg.DocumentRevision{ID: doc.ID, Incarnation: doc.ID, Collection: doc.Collection, DocType: doc.DocType, DeclaredChunks: len(chunks), Chunks: len(chunks), TextBytes: textBytes, Revision: "sample"},
		Chunks:           chunks,
	}
}

func TestSameProfileMatchedQueries(t *testing.T) {
	doc, snap := matchedQuerySnapshot()
	source := sourceChunks(doc, snap.Chunks, sourceModel)
	for _, name := range []string{"copied", "swapped"} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			src, _ := memoryPinecone()
			dst, _ := memoryPinecone()
			staged := slices.Clone(source)
			if name == "swapped" {
				staged[0].Embedding, staged[1].Embedding = staged[1].Embedding, staged[0].Embedding
			}
			require.NoError(t, src.Upsert(ctx, "", source))
			require.NoError(t, dst.Upsert(ctx, "", staged))
			opts := defaultOptions()
			opts.SampleSize, opts.TopK, opts.ScopeDocuments, opts.MinRecall, opts.MaxP95 = 2, 1, 1, 1, time.Second
			v := verifier{corpus: snapshotCorpus{snap}, ledger: currentCheckpointLedger{doc: doc.ID, incarnation: doc.ID, revision: "sample"}, target: dst, source: src, generation: "g2", profile: testProfile(sourceModel), sourceProfile: testProfile(sourceModel), opts: opts, now: time.Now}
			rep, err := v.run(ctx)
			require.NoError(t, err)
			matching := 0
			filter := domain.VectorFilter{DocumentIDs: []string{doc.ID}, Collection: doc.Collection}
			for _, sample := range source {
				// Generate the same request as a client using the declared unchanged profile.
				query := embedWith(sourceModel, sample.Text)
				baseline, err := src.QueryFiltered(ctx, query, opts.TopK, filter)
				require.NoError(t, err)
				observed, err := dst.QueryFiltered(ctx, query, opts.TopK, filter)
				require.NoError(t, err)
				require.Len(t, baseline, 1)
				require.Len(t, observed, 1)
				if baseline[0].Chunk.ID == observed[0].Chunk.ID {
					matching++
				}
			}
			t.Logf("queries=%d profileCoverage=%.1f forbidden=%d gateRecall=%.1f passed=%v matchedTop1=%d/%d", rep.Sample.Queries, rep.Coverage.ProfileCoverage, rep.Sample.ForbiddenHits, rep.Sample.MeanRecall, rep.Passed, matching, len(source))
			require.Equal(t, len(source), rep.Sample.Queries)
			require.Equal(t, 1.0, rep.Coverage.ProfileCoverage)
			require.Zero(t, rep.Sample.ForbiddenHits)
			require.Equal(t, float64(matching)/float64(len(source)), rep.Sample.MeanRecall)
			require.Equal(t, matching == len(source), rep.Passed)
		})
	}
}

func TestDifferentProfileMatchedQueriesRequireIndependentFixtures(t *testing.T) {
	doc, snap := matchedQuerySnapshot()
	sourceProfile, targetProfile := testProfile(sourceModel), testProfile("model-b")
	src, _ := memoryPinecone()
	dst, _ := memoryPinecone()
	staged := sourceChunks(doc, snap.Chunks, targetProfile.Model)
	require.NoError(t, src.Upsert(t.Context(), "", sourceChunks(doc, snap.Chunks, sourceProfile.Model)))
	require.NoError(t, dst.Upsert(t.Context(), "", staged))

	opts := defaultOptions()
	opts.SampleSize, opts.TopK, opts.ScopeDocuments, opts.MinRecall, opts.MaxP95 = 2, 1, 1, 1, time.Second
	v := verifier{corpus: snapshotCorpus{snap}, ledger: currentCheckpointLedger{doc: doc.ID, incarnation: doc.ID, revision: snap.Revision}, target: dst, source: src, generation: "g2", profile: targetProfile, sourceProfile: sourceProfile, opts: opts, now: time.Now}
	report, err := v.run(t.Context())
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Zero(t, report.Sample.Queries)
	require.Equal(t, sourceProfile.Hash(), report.Sample.SourceProfile)
	require.ElementsMatch(t, []string{string(snap.Chunks[0].ID), string(snap.Chunks[1].ID)}, report.Sample.ChunkIDs)

	fixtures := queryFixtures{
		SourceProfile: sourceProfile.Hash(),
		TargetProfile: targetProfile.Hash(),
		Queries:       make(map[string]queryVectors),
	}
	for _, chunk := range snap.Chunks {
		fixtures.Queries[string(chunk.ID)] = queryVectors{
			TextHash: fmt.Sprintf("%x", sha256.Sum256([]byte(chunk.Text))),
			Source:   embedWith(sourceProfile.Model, chunk.Text),
			Target:   embedWith(targetProfile.Model, chunk.Text),
		}
	}
	v.opts.QueryFixtures = filepath.Join(t.TempDir(), "queries.json")
	writeFixtures := func() {
		t.Helper()
		data, marshalErr := json.Marshal(fixtures)
		require.NoError(t, marshalErr)
		require.NoError(t, os.WriteFile(v.opts.QueryFixtures, data, 0o600))
	}
	writeFixtures()
	report, err = v.run(t.Context())
	require.NoError(t, err)
	require.True(t, report.Passed, "%v", report.Failures)
	require.Equal(t, 1.0, report.Sample.MeanRecall)

	staged[0].Embedding, staged[1].Embedding = staged[1].Embedding, staged[0].Embedding
	require.NoError(t, dst.Upsert(t.Context(), "", staged))
	report, err = v.run(t.Context())
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Equal(t, 2, report.Sample.Queries)
	require.Zero(t, report.Sample.MeanRecall)

	id := string(snap.Chunks[0].ID)
	stale := fixtures.Queries[id]
	stale.TextHash = ""
	fixtures.Queries[id] = stale
	writeFixtures()
	report, err = v.run(t.Context())
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Equal(t, 1, report.Sample.Queries)

	fixtures.TargetProfile = fixtures.SourceProfile
	writeFixtures()
	_, err = v.run(t.Context())
	require.ErrorContains(t, err, "fixture profiles differ")
}

func TestSameSpaceUsesCurrentSampleText(t *testing.T) {
	// A deterministic embedding of the input text, independent of both indexes.
	embed := func(s string) []float32 {
		v := make([]float32, testDims)
		for _, token := range strings.Fields(s) {
			switch token {
			case "train":
				v[0]++
			case "bus":
				v[1]++
			case "ferry":
				v[2]++
			}
		}
		return v
	}
	for _, tc := range []struct {
		name, sourceB, targetB string
		wantPass               bool
		wantMeasured           int
	}{
		{"both current", "bus", "bus", true, 2},
		{"bad target with current source", "bus", "ferry", false, 2},
		{"bad target with stale source", "bus ferry", "ferry", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, snap := matchedQuerySnapshot()
			doc.Texts = []string{"train train bus", "bus"}
			for i := range snap.Chunks {
				snap.Chunks[i].Text = doc.Texts[i]
			}
			source := sourceChunks(doc, snap.Chunks, sourceModel)
			target := sourceChunks(doc, snap.Chunks, sourceModel)
			source[1].Text = tc.sourceB
			for i := range source {
				source[i].Embedding = embed(source[i].Text)
				target[i].Embedding = embed(target[i].Text)
			}
			target[1].Embedding = embed(tc.targetB)
			src, _ := memoryPinecone()
			dst, _ := memoryPinecone()
			require.NoError(t, src.Upsert(t.Context(), "", source))
			require.NoError(t, dst.Upsert(t.Context(), "", target))
			opts := defaultOptions()
			opts.SampleSize, opts.TopK, opts.ScopeDocuments, opts.MinRecall, opts.MaxP95 = 2, 1, 1, 1, time.Second
			profile := testProfile(sourceModel)
			v := verifier{corpus: snapshotCorpus{snap}, ledger: currentCheckpointLedger{doc: doc.ID, revision: snap.Revision, incarnation: doc.ID}, target: dst, source: src, generation: "g2", profile: profile, sourceProfile: profile, opts: opts, now: time.Now}
			rep, err := v.run(t.Context())
			require.NoError(t, err)
			filter := domain.VectorFilter{DocumentIDs: []string{doc.ID}, Collection: doc.Collection}
			matching := 0
			for _, text := range doc.Texts {
				a, err := src.QueryFiltered(t.Context(), embed(text), 1, filter)
				require.NoError(t, err)
				require.Len(t, a, 1)
				b, err := dst.QueryFiltered(t.Context(), embed(text), 1, filter)
				require.NoError(t, err)
				require.Len(t, b, 1)
				if a[0].Chunk.ID == b[0].Chunk.ID {
					matching++
				}
			}
			t.Logf("actual matched top-1=%d/2; gate recall=%g, measured=%d, skipped=%d, coverage=%g, forbidden=%d, passed=%v", matching, rep.Sample.MeanRecall, rep.Sample.Queries, rep.Sample.Skipped, rep.Coverage.ProfileCoverage, rep.Sample.ForbiddenHits, rep.Passed)
			require.Equal(t, tc.wantPass, matching == 2)
			require.Equal(t, tc.wantPass, rep.Passed)
			require.Equal(t, tc.wantMeasured, rep.Sample.Queries)
			require.Equal(t, 2-tc.wantMeasured, rep.Sample.Skipped)
		})
	}
}
