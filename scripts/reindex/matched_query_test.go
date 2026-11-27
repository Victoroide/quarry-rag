package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pg "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type seedQueryReader struct {
	vectorReader
	read       func(context.Context, []string) (map[string]domain.Chunk, error)
	fetches    [][]string
	filters    []domain.VectorFilter
	embeddings [][]float32
}

func (r *seedQueryReader) Fetch(ctx context.Context, ids []string) (map[string]domain.Chunk, error) {
	r.fetches = append(r.fetches, slices.Clone(ids))
	if r.read != nil {
		return r.read(ctx, ids)
	}
	return r.vectorReader.Fetch(ctx, ids)
}

func (r *seedQueryReader) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	r.filters = append(r.filters, filter)
	r.embeddings = append(r.embeddings, slices.Clone(embedding))
	return r.vectorReader.QueryFiltered(ctx, embedding, topK, filter)
}

func TestSameSpaceSeedIdentityPolicy(t *testing.T) {
	for _, mode := range []struct {
		name, form     string
		hosted, tagged bool
	}{
		{"hosted bare", "bare", true, false},
		{"hosted tagged", "tagged", true, true},
		{"hosted manifested with scoped flag", "manifested", true, false},
		{"relational scoped", "tagged", false, false},
	} {
		t.Run(mode.name, func(t *testing.T) {
			snap, rows := hostedPositionSnapshot(mode.form)
			snap.TenantID = uuid.NewString()
			if snap.ActiveRevisionID != "" {
				snap.ScopedVectors = true
				snap.VectorAttempt = uuid.NewString()
			}
			want, valid := snap.Chunks[0], rows[0]
			valid.VectorIncarnation = snap.Incarnation
			valid.Metadata[domain.VectorTenantKey] = snap.TenantID
			if !mode.hosted {
				valid.VectorIncarnation = ""
				delete(valid.Metadata, domain.VectorRevisionKey)
				delete(valid.Metadata, domain.VectorAttemptKey)
				delete(valid.Metadata, domain.VectorTenantKey)
			}

			for _, tc := range []struct {
				name            string
				change          func(*domain.Chunk)
				stampRequired   bool
				found, accepted bool
			}{
				{"matching", nil, true, true, true},
				{"absent record", nil, false, false, false},
				{"wrong document", func(c *domain.Chunk) { c.DocumentID = "foreign" }, false, true, false},
				{"wrong logical chunk", func(c *domain.Chunk) { c.ID = "foreign" }, false, true, false},
				{"wrong index", func(c *domain.Chunk) { c.Index = 1 }, false, true, false},
				{"negative index", func(c *domain.Chunk) { c.Index = -1 }, false, true, false},
				{"wrong incarnation", func(c *domain.Chunk) { c.VectorIncarnation = "foreign" }, false, true, false},
				{"historical missing stamp", func(c *domain.Chunk) { c.VectorIncarnation = "" }, false, true, true},
				{"missing required stamp", func(c *domain.Chunk) { c.VectorIncarnation = "" }, true, true, !mode.hosted},
				{"wrong active revision", func(c *domain.Chunk) { c.RevisionID = "foreign" }, false, true, false},
				{"missing active revision", func(c *domain.Chunk) { c.RevisionID = "" }, false, true, snap.ActiveRevisionID == ""},
				{"correct tenant", func(c *domain.Chunk) { c.Metadata[domain.VectorTenantKey] = snap.TenantID }, false, true, true},
				{"absent tenant", func(c *domain.Chunk) { delete(c.Metadata, domain.VectorTenantKey) }, false, true, !mode.tagged},
				{"blank tenant", func(c *domain.Chunk) { c.Metadata[domain.VectorTenantKey] = "" }, false, true, !mode.tagged},
				{"foreign tenant", func(c *domain.Chunk) { c.Metadata[domain.VectorTenantKey] = "foreign" }, false, true, false},
				{"malformed tenant", func(c *domain.Chunk) { c.Metadata[domain.VectorTenantKey] = 12 }, false, true, false},
				{"wrong revision tag", func(c *domain.Chunk) { c.Metadata[domain.VectorRevisionKey] = "foreign" }, false, true, !mode.tagged},
				{"missing revision tag", func(c *domain.Chunk) { delete(c.Metadata, domain.VectorRevisionKey) }, false, true, !mode.tagged},
				{"wrong attempt tag", func(c *domain.Chunk) { c.Metadata[domain.VectorAttemptKey] = "foreign" }, false, true, !mode.tagged},
				{"missing attempt tag", func(c *domain.Chunk) { delete(c.Metadata, domain.VectorAttemptKey) }, false, true, !mode.tagged},
			} {
				t.Run(tc.name, func(t *testing.T) {
					source := valid
					source.Metadata = maps.Clone(valid.Metadata)
					if tc.change != nil {
						tc.change(&source)
					}
					snapshot := snap
					snapshot.VectorIncarnationRequired = tc.stampRequired
					identity := newSourceChunkIdentity(snapshot.DocumentRevision, want)
					require.Equal(t, tc.accepted, identity.matches(mode.hosted, source, tc.found))
					require.Equal(t, tc.accepted, sourceIdentityMatches(mode.hosted, &snapshot, want, source, tc.found))
				})
			}
			t.Run("stored text is checked by staging and layout fallback", func(t *testing.T) {
				source := valid
				source.Text += " Changed."
				identity := newSourceChunkIdentity(snap.DocumentRevision, want)
				require.True(t, identity.matches(mode.hosted, source, true))
				require.False(t, sourceIdentityMatches(mode.hosted, &snap, want, source, true))
			})
		})
	}
}

func TestSameSpaceVerificationSkipsMismatchedHostedSeeds(t *testing.T) {
	for _, tc := range []struct {
		name, form, field string
		decoded           bool
	}{
		{"foreign incarnation", "bare", domain.VectorIncarnationKey, true},
		{"foreign tenant", "bare", domain.VectorTenantKey, true},
		{"foreign logical ID", "bare", "chunk_id", true},
		{"foreign active revision", "bare", "revision_id", true},
		{"foreign document", "bare", "doc_id", true},
		{"manifest with foreign stamp", "manifested", domain.VectorIncarnationKey, true},
		{"tag inconsistent with key", "tagged", domain.VectorAttemptKey, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			snap, rows := hostedPositionSnapshot(tc.form)
			for i := range rows {
				rows[i].VectorIncarnation = snap.Incarnation
			}
			src, index := memoryPinecone()
			dst, _ := memoryPinecone()
			require.NoError(t, src.Upsert(ctx, "", rows))
			require.NoError(t, dst.Upsert(ctx, "", rows))
			source, target := &seedQueryReader{vectorReader: src}, &seedQueryReader{vectorReader: dst}
			v := hostedPositionVerifier(snap, src, dst)
			v.source, v.target = source, target
			var sample []sampleChunk
			_, _, err := v.compareDocument(ctx, &snap, &verifyReport{}, map[string]scopeDoc{}, &sample)
			require.NoError(t, err)
			key, goodKey := sample[0].sourceKey, sample[1].sourceKey
			updateHostedRecord(t, index, key, func(record *pinecone.Vector) {
				record.Metadata.Fields[tc.field] = structpb.NewStringValue(uuid.NewString())
			})
			decoded, err := src.Fetch(ctx, []string{key})
			require.NoError(t, err)
			_, present := decoded[key]
			require.Equal(t, tc.decoded, present)
			if present {
				require.False(t, sourceIdentityMatches(true, &snap, snap.Chunks[0], decoded[key], true))
			}
			queries, err := v.queryVectors(ctx, sample, &verifyReport{})
			require.NoError(t, err)
			require.NotContains(t, queries, sample[0].id)
			require.Contains(t, queries, sample[1].id)
			require.Equal(t, [][]string{{key, goodKey}}, source.fetches)
			source.fetches, target.fetches = nil, nil

			rep, err := v.run(ctx)
			require.NoError(t, err)
			require.False(t, rep.Passed)
			require.Contains(t, rep.Failures, "only 1 of 2 sample queries ran with a usable baseline (1 unmeasurable)")
			require.Equal(t, 1, rep.Sample.Queries)
			require.Equal(t, 1, rep.Sample.Skipped)
			require.Equal(t, 1.0, rep.Coverage.KeyCoverage)
			require.Equal(t, 1.0, rep.Coverage.ProfileCoverage)
			require.Zero(t, rep.Coverage.Missing)
			require.Zero(t, rep.Coverage.Mismatched)
			require.Zero(t, rep.Coverage.BadVectors)
			filter := domain.VectorFilter{DocumentIDs: []string{snap.ID}, Collection: snap.Collection}
			for _, reader := range []*seedQueryReader{source, target} {
				require.Len(t, reader.fetches, 1)
				require.ElementsMatch(t, []string{key, goodKey}, reader.fetches[0])
				require.Equal(t, []domain.VectorFilter{filter}, reader.filters)
				require.Equal(t, [][]float32{rows[1].Embedding}, reader.embeddings)
			}
			firstFetch := source.fetches[0]

			require.NoError(t, src.Upsert(ctx, "", rows[:1]))
			source.fetches, source.filters, target.filters = nil, nil, nil
			rep, err = v.run(ctx)
			require.NoError(t, err)
			require.True(t, rep.Passed, "%v", rep.Failures)
			require.Equal(t, 2, rep.Sample.Queries)
			require.Zero(t, rep.Sample.Skipped)
			require.Equal(t, [][]string{firstFetch}, source.fetches)
			require.Equal(t, []domain.VectorFilter{filter, filter}, source.filters)
			require.Equal(t, source.filters, target.filters)
		})
	}

	for _, tc := range []struct {
		name   string
		change func(map[string]domain.Chunk, string)
	}{
		{"no eligible record", func(records map[string]domain.Chunk, key string) { delete(records, key) }},
		{"wrong fetched map key", func(records map[string]domain.Chunk, key string) {
			records["unrequested"] = records[key]
			delete(records, key)
		}},
		{"wrong dimension", func(records map[string]domain.Chunk, key string) {
			c := records[key]
			c.Embedding = c.Embedding[:1]
			records[key] = c
		}},
		{"NaN", func(records map[string]domain.Chunk, key string) {
			records[key].Embedding[0] = float32(math.NaN())
		}},
		{"zero vector", func(records map[string]domain.Chunk, key string) { clear(records[key].Embedding) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, rows := hostedPositionSnapshot("bare")
			for i := range rows {
				rows[i].VectorIncarnation = snap.Incarnation
			}
			src, _ := memoryPinecone()
			dst, _ := memoryPinecone()
			require.NoError(t, src.Upsert(t.Context(), "", rows))
			require.NoError(t, dst.Upsert(t.Context(), "", rows))
			source := &seedQueryReader{vectorReader: src, read: func(ctx context.Context, ids []string) (map[string]domain.Chunk, error) {
				records, err := src.Fetch(ctx, ids)
				require.NoError(t, err)
				require.Len(t, ids, 1)
				tc.change(records, ids[0])
				return records, nil
			}}
			target := &seedQueryReader{vectorReader: dst}
			v := hostedPositionVerifier(snap, src, dst)
			v.source, v.target = source, target
			v.opts.SampleSize = 1
			rep, err := v.run(t.Context())
			require.NoError(t, err)
			require.False(t, rep.Passed)
			require.Zero(t, rep.Sample.Queries)
			require.Equal(t, 1, rep.Sample.Skipped)
			require.Equal(t, 1.0, rep.Coverage.ProfileCoverage)
			require.Len(t, source.fetches, 1)
			require.Empty(t, source.filters)
			require.Empty(t, target.filters)
		})
	}
}

func TestSameSpaceSeedFetchErrorsAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failBatch int
		cause     error
	}{
		{"permission denied", 1, status.Error(codes.PermissionDenied, "403")},
		{"temporary failure", 1, status.Error(codes.Unavailable, "temporarily unavailable")},
		{"canceled", 1, context.Canceled},
		{"deadline exceeded", 1, context.DeadlineExceeded},
		{"late batch", 2, status.Error(codes.Unavailable, "late fetch failed")},
		{"bounded batches", 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			switch {
			case errors.Is(tc.cause, context.Canceled):
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			case errors.Is(tc.cause, context.DeadlineExceeded):
				expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				ctx = expired
			}
			texts := make([]string, pcstore.FetchBatchSize+1)
			for i := range texts {
				texts[i] = fmt.Sprintf("Sample text number %d.", i)
			}
			doc := newDoc("alpha", "report", texts...)
			doc.Incarnation = doc.ID
			snap := snapshotOf(doc, chunksOf(doc), "seed-batch-revision")
			rows := sourceChunks(doc, snap.Chunks, sourceModel)
			for i := range rows {
				rows[i].VectorIncarnation = snap.Incarnation
			}
			src, _ := memoryPinecone()
			require.NoError(t, src.Upsert(t.Context(), "", rows))
			source := &seedQueryReader{vectorReader: src}
			source.read = func(ctx context.Context, ids []string) (map[string]domain.Chunk, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if len(source.fetches) == tc.failBatch {
					return nil, tc.cause
				}
				return src.Fetch(ctx, ids)
			}
			target := &seedQueryReader{vectorReader: src}
			v := hostedPositionVerifier(snap, src, src)
			v.source, v.target = source, target
			v.opts.SampleSize = len(rows)
			expected := map[string]scopeDoc{}
			var sample []sampleChunk
			_, _, err := v.compareDocument(t.Context(), &snap, &verifyReport{}, expected, &sample)
			require.NoError(t, err)
			require.Len(t, target.fetches, 2)
			if tc.cause == nil {
				// Internal sample entries without a source key retain the same-key lookup.
				sample[0].sourceKey = ""
			}
			queries, err := v.queryVectors(ctx, sample, &verifyReport{})
			if tc.cause == nil {
				require.NoError(t, err)
				require.Len(t, queries, len(sample))
				require.Equal(t, target.fetches, source.fetches)
				require.Contains(t, queries, sample[0].id)
				return
			}
			require.ErrorIs(t, err, tc.cause)
			require.ErrorContains(t, err, "read vectors")
			require.Nil(t, queries)
			require.Equal(t, target.fetches[:tc.failBatch], source.fetches)
			source.fetches = nil

			var rep verifyReport
			err = v.matchedQueries(ctx, snap.Revision, sample, []scopeDoc{{id: snap.ID, collection: snap.Collection}}, expected, &rep)
			require.ErrorIs(t, err, tc.cause)
			require.Len(t, source.fetches, tc.failBatch)
			require.Zero(t, rep.Sample.Queries)
			require.Zero(t, rep.Sample.Skipped)
			require.Empty(t, source.filters)
			require.Empty(t, target.filters)
		})
	}
}

func TestIndependentQueryFixtureDoesNotFetchSeeds(t *testing.T) {
	for _, tc := range []struct {
		name, targetModel string
		sourceQueryOnly   bool
	}{
		{"different spaces", "model-b", false},
		{"unattested same space", sourceModel, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			doc, snap := matchedQuerySnapshot()
			sourceRows := sourceChunks(doc, snap.Chunks, sourceModel)
			targetRows := sourceChunks(doc, snap.Chunks, tc.targetModel)
			src, _ := memoryPinecone()
			dst, _ := memoryPinecone()
			require.NoError(t, src.Upsert(ctx, "", sourceRows))
			require.NoError(t, dst.Upsert(ctx, "", targetRows))
			source, target := &seedQueryReader{vectorReader: src}, &seedQueryReader{vectorReader: dst}
			v := hostedPositionVerifier(snap, src, dst)
			v.source, v.target = source, target
			v.taggedTarget = false
			v.profile = testProfile(tc.targetModel)
			v.sourceQueryOnly = tc.sourceQueryOnly
			v.opts.TopK = 1
			v.opts.QueryFixtures = filepath.Join(t.TempDir(), "queries.json")
			fixtures := queryFixtures{
				SourceProfile: v.sourceProfile.Hash(), TargetProfile: v.profile.Hash(),
				Queries: make(map[string]queryVectors),
			}
			for i, c := range snap.Chunks {
				fixtures.Queries[string(c.ID)] = queryVectors{
					TextHash: fmt.Sprintf("%x", sha256.Sum256([]byte(c.Text))),
					Source:   sourceRows[i].Embedding, Target: targetRows[i].Embedding,
				}
			}
			writeFixtures := func() {
				t.Helper()
				data, err := json.Marshal(fixtures)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(v.opts.QueryFixtures, data, 0o600))
			}
			expected := map[string]scopeDoc{}
			var sample []sampleChunk
			_, _, err := v.compareDocument(ctx, &snap, &verifyReport{}, expected, &sample)
			require.NoError(t, err)
			docs := []scopeDoc{{id: doc.ID, collection: doc.Collection}}
			writeFixtures()
			var rep verifyReport
			require.NoError(t, v.matchedQueries(ctx, snap.Revision, sample, docs, expected, &rep))
			require.Equal(t, 2, rep.Sample.Queries)
			require.Equal(t, 1.0, rep.Sample.MeanRecall)
			require.Zero(t, rep.Sample.Skipped)
			require.Equal(t, fixtures.SourceProfile, rep.Sample.SourceProfile)
			require.ElementsMatch(t, []string{sample[0].id, sample[1].id}, rep.Sample.ChunkIDs)
			filter := domain.VectorFilter{DocumentIDs: []string{doc.ID}, Collection: doc.Collection}
			require.Equal(t, []domain.VectorFilter{filter, filter}, source.filters)
			require.Equal(t, source.filters, target.filters)

			stale := fixtures.Queries[sample[0].id]
			stale.TextHash = fmt.Sprintf("%x", sha256.Sum256([]byte("previous text")))
			fixtures.Queries[sample[0].id] = stale
			writeFixtures()
			rep = verifyReport{}
			require.NoError(t, v.matchedQueries(ctx, snap.Revision, sample, docs, expected, &rep))
			require.Equal(t, 1, rep.Sample.Queries)
			require.Equal(t, 1, rep.Sample.Skipped)

			fixtures.TargetProfile = testProfile("different-profile").Hash()
			writeFixtures()
			require.ErrorContains(t, v.matchedQueries(ctx, snap.Revision, sample, docs, expected, &verifyReport{}), "fixture profiles differ")
			v.sourceQueryOnly, v.profile = false, v.sourceProfile
			require.ErrorContains(t, v.matchedQueries(ctx, snap.Revision, sample, docs, expected, &verifyReport{}), "-query-fixtures is only needed")
			require.Empty(t, source.fetches)
			require.Len(t, source.filters, 3)
			require.Len(t, target.filters, 3)
		})
	}
}

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
