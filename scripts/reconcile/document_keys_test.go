package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pineconestore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

type documentKeyFakeIndex struct {
	requests []*pinecone.UpdateVectorsByMetadataRequest
	matched  map[string]int32
	failAt   int
	failErr  error
	nilAt    int
}

func (f *documentKeyFakeIndex) UpdateVectorsByMetadata(_ context.Context, req *pinecone.UpdateVectorsByMetadataRequest) (*pinecone.UpdateVectorsByMetadataResponse, error) {
	f.requests = append(f.requests, req)
	if f.failAt == len(f.requests) {
		if f.failErr != nil {
			return nil, f.failErr
		}
		return nil, errors.New("update failed")
	}
	if f.nilAt == len(f.requests) {
		return nil, nil
	}
	docID, _ := req.Metadata.AsMap()["doc_id"].(string)
	return &pinecone.UpdateVectorsByMetadataResponse{MatchedRecords: f.matched[docID]}, nil
}

func TestNoncanonicalSpellingsParseToTheSameUUID(t *testing.T) {
	id := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	spellings := noncanonicalSpellings(id)
	require.Len(t, spellings, 9)
	expected := []string{
		"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
		"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
		"{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}",
		"urn:uuid:a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		"urn:uuid:A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
		"URN:UUID:a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		"URN:UUID:A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
		"a0eebc999c0b4ef8bb6d6bb9bd380a11",
		"A0EEBC999C0B4EF8BB6D6BB9BD380A11",
	}
	require.Equal(t, expected, spellings)
	seen := make(map[string]struct{}, len(spellings))
	for _, s := range spellings {
		require.NotEqual(t, id.String(), s)
		require.NotContains(t, seen, s)
		seen[s] = struct{}{}
		parsed, err := uuid.Parse(s)
		require.NoError(t, err)
		require.Equal(t, id, parsed)
	}

	// An all-digit UUID has upper == lower, so canonical exclusion leaves four distinct forms.
	digitsOnly := uuid.MustParse(storedID)
	digitSpellings := noncanonicalSpellings(digitsOnly)
	require.Len(t, digitSpellings, 4)
	for _, s := range digitSpellings {
		require.NotEqual(t, digitsOnly.String(), s)
		parsed, err := uuid.Parse(s)
		require.NoError(t, err)
		require.Equal(t, digitsOnly, parsed)
	}
}

func FuzzNoncanonicalSpellings(f *testing.F) {
	f.Add(make([]byte, 16))
	f.Add([]byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) < 16 {
			return
		}
		id, err := uuid.FromBytes(raw[:16])
		require.NoError(t, err)
		spellings := noncanonicalSpellings(id)
		canonical := id.String()
		if canonical != strings.ToUpper(canonical) {
			require.Len(t, spellings, 9)
		} else {
			require.Len(t, spellings, 4)
		}
		seen := make(map[string]struct{}, len(spellings))
		for _, s := range spellings {
			require.NotEqual(t, canonical, s)
			require.NotContains(t, seen, s)
			seen[s] = struct{}{}
			parsed, parseErr := uuid.Parse(s)
			require.NoError(t, parseErr)
			require.Equal(t, id, parsed)
		}
	})
}

func TestCanonicalDocumentKeysDryRunAndApply(t *testing.T) {
	docOne := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	docZero := "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"
	docSkipped := "c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33"
	ids := []string{docOne, docZero, docSkipped}
	matched := map[string]int32{docOne: 3, docZero: 0}

	for _, apply := range []bool{false, true} {
		idx := &documentKeyFakeIndex{matched: matched}
		report, err := canonicalizeDocumentKeys(t.Context(), idx, ids, options{CanonicalDocumentIDs: true, Apply: apply, Limit: 2})
		require.NoError(t, err)
		require.Equal(t, 2, report.Examined)
		require.Equal(t, int64(3), report.MatchedVectors)
		require.Equal(t, docZero, report.ResumeAfter)
		require.Equal(t, []documentKeyMatch{{ID: docOne, Matched: 3}}, report.Documents)
		require.Len(t, idx.requests, 2)

		expectedSpellings := make([]any, 0, 9)
		for _, s := range noncanonicalSpellings(uuid.MustParse(docOne)) {
			expectedSpellings = append(expectedSpellings, s)
		}
		require.Equal(t, map[string]any{"doc_id": map[string]any{"$in": expectedSpellings}}, idx.requests[0].Filter.AsMap())
		require.Equal(t, map[string]any{"doc_id": docOne}, idx.requests[0].Metadata.AsMap())
		require.NotNil(t, idx.requests[0].DryRun)
		require.Equal(t, !apply, *idx.requests[0].DryRun)

		if apply {
			require.Equal(t, "apply", report.Mode)
			require.Equal(t, 1, report.Updated)
		} else {
			require.Equal(t, "inventory", report.Mode)
			require.Zero(t, report.Updated)
		}
	}
}

func TestCanonicalDocumentKeysReportIsBounded(t *testing.T) {
	ids := make([]string, 150)
	matched := make(map[string]int32, len(ids))
	for i := range ids {
		id := fmt.Sprintf("a0eebc99-9c0b-4ef8-bb6d-%012x", i+1)
		ids[i] = id
		matched[id] = 2
	}
	idx := &documentKeyFakeIndex{matched: matched}
	report, err := canonicalizeDocumentKeys(t.Context(), idx, ids, options{CanonicalDocumentIDs: true, Apply: true, Limit: 200})
	require.NoError(t, err)
	require.Equal(t, 150, report.Examined)
	require.Equal(t, int64(300), report.MatchedVectors)
	require.Equal(t, 150, report.Updated)
	require.Len(t, report.Documents, maxReportedDocuments)
	require.Equal(t, ids[0], report.Documents[0].ID)
	require.Equal(t, ids[99], report.Documents[99].ID)
	require.Equal(t, ids[149], report.ResumeAfter)
}

func TestCanonicalDocumentKeysReportsCappedDocuments(t *testing.T) {
	docCapped := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	docNormal := "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"
	idx := &documentKeyFakeIndex{matched: map[string]int32{
		docCapped: maxMetadataUpdate,
		docNormal: 4,
	}}
	report, err := canonicalizeDocumentKeys(t.Context(), idx, []string{docCapped, docNormal}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, int64(maxMetadataUpdate+4), report.MatchedVectors)
	require.Equal(t, 2, report.Updated)
	require.Equal(t, []documentKeyMatch{
		{ID: docCapped, Matched: maxMetadataUpdate, Capped: true},
		{ID: docNormal, Matched: 4, Capped: false},
	}, report.Documents)
}

func TestCanonicalDocumentKeysStopsOnFirstError(t *testing.T) {
	docOne := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	docTwo := "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"
	docThree := "c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33"
	failure := errors.New("metadata filter index missing")

	idx := &documentKeyFakeIndex{
		matched: map[string]int32{docOne: 2, docThree: 5},
		failAt:  2,
		failErr: failure,
	}
	report, err := canonicalizeDocumentKeys(t.Context(), idx, []string{docOne, docTwo, docThree}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10})
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, docTwo)
	require.ErrorContains(t, err, fmt.Sprintf("-after=%q", docOne))
	require.Len(t, idx.requests, 2)
	require.Equal(t, 1, report.Examined)
	require.Equal(t, int64(2), report.MatchedVectors)
	require.Equal(t, 1, report.Updated)
	require.Equal(t, docOne, report.ResumeAfter)

	// Failure on the first document reports the caller's -after cursor.
	idx = &documentKeyFakeIndex{failAt: 1, failErr: failure}
	report, err = canonicalizeDocumentKeys(t.Context(), idx, []string{docTwo}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10, After: docOne})
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, fmt.Sprintf("-after=%q", docOne))
	require.Zero(t, report.Examined)
	require.Equal(t, docOne, report.ResumeAfter)

	// Canceled context stops before issuing the next request.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	idx = &documentKeyFakeIndex{}
	report, err = canonicalizeDocumentKeys(ctx, idx, []string{docTwo, docThree}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10, After: docOne})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, docTwo)
	require.ErrorContains(t, err, fmt.Sprintf("-after=%q", docOne))
	require.Empty(t, idx.requests)
	require.Zero(t, report.Examined)
	require.Equal(t, docOne, report.ResumeAfter)
}

func TestCanonicalDocumentKeysFlag(t *testing.T) {
	opts, err := parseOptions([]string{"-canonical-document-ids"}, &bytes.Buffer{})
	require.NoError(t, err)
	require.True(t, opts.CanonicalDocumentIDs)
	require.False(t, opts.Apply)
	require.Equal(t, 100000, opts.Limit)

	const cursor = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	opts, err = parseOptions([]string{"-canonical-document-ids", "-apply", "-limit=5", "-after=" + strings.ToUpper(cursor)}, &bytes.Buffer{})
	require.NoError(t, err)
	require.True(t, opts.CanonicalDocumentIDs)
	require.True(t, opts.Apply)
	require.Equal(t, 5, opts.Limit)
	require.Equal(t, cursor, opts.After)

	for _, args := range [][]string{
		{"-canonical-document-ids", "-backfill-collections"},
		{"-canonical-document-ids", "-backfill-scoped"},
		{"-canonical-document-ids", "-source-uploads"},
		{"-canonical-document-ids", "-page=50"},
		{"-canonical-document-ids", "-after=invalid"},
		{"-canonical-document-ids", "-limit=0"},
		{"-canonical-document-ids", "-limit=-1"},
	} {
		_, err = parseOptions(args, &bytes.Buffer{})
		require.Error(t, err, "%v", args)
	}
}

func memoryVectorClient(dims int) (*pineconestore.Client, *pineconestore.MemoryIndex) {
	index := pineconestore.NewMemoryIndex(dims)
	cfg := &config.Config{
		PineconeUpsertBatchSize: 100,
		PineconeTimeout:         time.Second,
		PineconeMaxAttempts:     1,
		EmbeddingDimensions:     dims,
	}
	return pineconestore.NewClientForIndex(index, cfg, ""), index
}

func TestCanonicalDocumentKeysRestoreAllowlistedQueries(t *testing.T) {
	const docID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	client, memIdx := memoryVectorClient(3)
	queryVec := []float32{1, 0, 0}
	md, err := structpb.NewStruct(map[string]any{
		"doc_id":      strings.ToUpper(docID),
		"collection":  "archive",
		"doc_type":    "report",
		"chunk_index": float64(0),
		"text":        "stored chunk text",
		"token_count": float64(4),
		"kind":        "paragraph",
		"quality":     "high",
	})
	require.NoError(t, err)
	_, err = memIdx.UpsertVectors(t.Context(), []*pinecone.Vector{{
		Id:       "chunk-0",
		Values:   &queryVec,
		Metadata: md,
	}})
	require.NoError(t, err)

	beforeFetched, err := memIdx.FetchVectors(t.Context(), []string{"chunk-0"})
	require.NoError(t, err)
	beforeMeta := beforeFetched.Vectors["chunk-0"].Metadata.AsMap()

	// Before repair: collection-only finds the chunk, allowlisted query returns 0 hits.
	allowlisted, err := client.QueryFiltered(t.Context(), queryVec, 10, domain.VectorFilter{DocumentIDs: []string{docID}, Collection: "archive"})
	require.NoError(t, err)
	require.Empty(t, allowlisted)
	collectionOnly, err := client.QueryFiltered(t.Context(), queryVec, 10, domain.VectorFilter{Collection: "archive"})
	require.NoError(t, err)
	require.Len(t, collectionOnly, 1)

	// Dry run counts the match without modifying the index.
	dryReport, err := canonicalizeDocumentKeys(t.Context(), memIdx, []string{docID}, options{CanonicalDocumentIDs: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, "inventory", dryReport.Mode)
	require.Equal(t, int64(1), dryReport.MatchedVectors)
	require.Zero(t, dryReport.Updated)
	allowlisted, err = client.QueryFiltered(t.Context(), queryVec, 10, domain.VectorFilter{DocumentIDs: []string{docID}, Collection: "archive"})
	require.NoError(t, err)
	require.Empty(t, allowlisted)

	// Apply rewrites doc_id in place while leaving every other metadata field unchanged.
	applyReport, err := canonicalizeDocumentKeys(t.Context(), memIdx, []string{docID}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, "apply", applyReport.Mode)
	require.Equal(t, int64(1), applyReport.MatchedVectors)
	require.Equal(t, 1, applyReport.Updated)

	allowlisted, err = client.QueryFiltered(t.Context(), queryVec, 10, domain.VectorFilter{DocumentIDs: []string{docID}, Collection: "archive"})
	require.NoError(t, err)
	require.Len(t, allowlisted, 1)
	require.Equal(t, domain.DocumentID(docID), allowlisted[0].Chunk.DocumentID)

	collectionOnly, err = client.QueryFiltered(t.Context(), queryVec, 10, domain.VectorFilter{Collection: "archive"})
	require.NoError(t, err)
	require.Len(t, collectionOnly, 1)

	afterFetched, err := memIdx.FetchVectors(t.Context(), []string{"chunk-0"})
	require.NoError(t, err)
	afterMeta := afterFetched.Vectors["chunk-0"].Metadata.AsMap()
	require.Equal(t, docID, afterMeta["doc_id"])
	delete(beforeMeta, "doc_id")
	delete(afterMeta, "doc_id")
	require.Equal(t, beforeMeta, afterMeta)

	// Subsequent dry run converges to zero matches.
	cleanReport, err := canonicalizeDocumentKeys(t.Context(), memIdx, []string{docID}, options{CanonicalDocumentIDs: true, Limit: 10})
	require.NoError(t, err)
	require.Zero(t, cleanReport.MatchedVectors)
	require.Empty(t, cleanReport.Documents)
}

func TestCanonicalDocumentKeysRewriteOnlyAliases(t *testing.T) {
	targetID := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	otherID := uuid.MustParse("b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22")
	mixedCase := "A0eeBC99-9c0B-4ef8-Bb6d-6bb9bd380a11"
	memIdx := pineconestore.NewMemoryIndex(3)
	vec := []float32{1, 0, 0}

	upsert := func(id, docID, collection string) {
		md, err := structpb.NewStruct(map[string]any{"doc_id": docID, "collection": collection})
		require.NoError(t, err)
		_, err = memIdx.UpsertVectors(t.Context(), []*pinecone.Vector{{Id: id, Values: &vec, Metadata: md}})
		require.NoError(t, err)
	}

	spellings := noncanonicalSpellings(targetID)
	for i, spelling := range spellings {
		upsert(fmt.Sprintf("alias-%d", i), spelling, "c1")
	}
	upsert("target-canonical", targetID.String(), "c1")
	upsert("target-mixed", mixedCase, "c1")
	upsert("other-canonical", otherID.String(), "c2")
	upsert("other-upper", strings.ToUpper(otherID.String()), "c2")

	// A document whose vectors are all canonical reports zero matches.
	canonicalOnly := uuid.MustParse("c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33")
	upsert("clean-doc", canonicalOnly.String(), "c3")
	cleanReport, err := canonicalizeDocumentKeys(t.Context(), memIdx, []string{canonicalOnly.String()}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Zero(t, cleanReport.MatchedVectors)
	require.Zero(t, cleanReport.Updated)

	report, err := canonicalizeDocumentKeys(t.Context(), memIdx, []string{targetID.String()}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, int64(len(spellings)), report.MatchedVectors)
	require.Equal(t, 1, report.Updated)

	allIDs := memIdx.IDs()
	fetched, err := memIdx.FetchVectors(t.Context(), allIDs)
	require.NoError(t, err)
	for i := range spellings {
		id := fmt.Sprintf("alias-%d", i)
		require.Equal(t, targetID.String(), fetched.Vectors[id].Metadata.AsMap()["doc_id"])
		require.Equal(t, "c1", fetched.Vectors[id].Metadata.AsMap()["collection"])
	}
	require.Equal(t, targetID.String(), fetched.Vectors["target-canonical"].Metadata.AsMap()["doc_id"])
	require.Equal(t, mixedCase, fetched.Vectors["target-mixed"].Metadata.AsMap()["doc_id"], "mixed case is not enumerated and stays untouched")
	require.Equal(t, otherID.String(), fetched.Vectors["other-canonical"].Metadata.AsMap()["doc_id"])
	require.Equal(t, strings.ToUpper(otherID.String()), fetched.Vectors["other-upper"].Metadata.AsMap()["doc_id"])
}

func TestCanonicalKeysAreRemovedByDocumentDeletion(t *testing.T) {
	const docID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	client, memIdx := memoryVectorClient(3)
	vec := []float32{1, 0, 0}
	md, err := structpb.NewStruct(map[string]any{"doc_id": strings.ToUpper(docID), "collection": "archive"})
	require.NoError(t, err)
	_, err = memIdx.UpsertVectors(t.Context(), []*pinecone.Vector{{Id: "chunk-0", Values: &vec, Metadata: md}})
	require.NoError(t, err)

	// Before repair, canonical deletion leaves the upper-case vector behind.
	require.NoError(t, client.DeleteByDocID(t.Context(), docID))
	require.Equal(t, 1, memIdx.Len())

	_, err = canonicalizeDocumentKeys(t.Context(), memIdx, []string{docID}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10})
	require.NoError(t, err)

	require.NoError(t, client.DeleteByDocID(t.Context(), docID))
	require.Zero(t, memIdx.Len())
}

func TestCanonicalDocumentKeysRespectTheWriterBarrier(t *testing.T) {
	docID := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	for _, gateErr := range []error{domain.ErrWritersFrozen, domain.ErrGenerationMismatch} {
		idx := &documentKeyFakeIndex{matched: map[string]int32{docID: 1}}
		err := runMaintenance(t.Context(), fakeGate{err: gateErr}, options{CanonicalDocumentIDs: true, Limit: 10}, func() error {
			report, runErr := canonicalizeDocumentKeys(t.Context(), idx, []string{docID}, options{CanonicalDocumentIDs: true, Limit: 10})
			require.Equal(t, int64(1), report.MatchedVectors)
			return runErr
		})
		require.NoError(t, err, "dry run proceeds when %v", gateErr)
		require.Len(t, idx.requests, 1)

		idx = &documentKeyFakeIndex{matched: map[string]int32{docID: 1}}
		err = runMaintenance(t.Context(), fakeGate{err: gateErr}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10}, func() error {
			_, runErr := canonicalizeDocumentKeys(t.Context(), idx, []string{docID}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 10})
			return runErr
		})
		require.ErrorIs(t, err, gateErr)
		require.Empty(t, idx.requests, "apply refuses before sending any update")
	}
}

func TestCanonicalDocumentKeysRequireAHostedIndex(t *testing.T) {
	require.True(t, needsHostedIndex(options{CanonicalDocumentIDs: true}))
	require.True(t, needsHostedIndex(options{CanonicalDocumentIDs: true, Apply: true}))
	require.True(t, needsHostedIndex(options{}))
	require.True(t, needsHostedIndex(options{Apply: true}))
	require.False(t, needsHostedIndex(options{BackfillCollections: true}))
	require.True(t, needsHostedIndex(options{BackfillCollections: true, Apply: true}))
	require.False(t, needsHostedIndex(options{BackfillScoped: true}))
	require.True(t, needsHostedIndex(options{BackfillScoped: true, Apply: true}))
	require.False(t, needsHostedIndex(options{SourceUploads: true}))

	valid := &config.Config{
		VectorStore:       "pinecone",
		PineconeAPIKey:    "test-key",
		PineconeIndexName: "test-index",
	}
	for _, apply := range []bool{false, true} {
		opts := options{CanonicalDocumentIDs: true, Apply: apply}
		require.NoError(t, validateReconcileConfig(opts, valid))

		pgCfg := *valid
		pgCfg.VectorStore = "postgres"
		require.Error(t, validateReconcileConfig(opts, &pgCfg))

		fakeCfg := *valid
		fakeCfg.FakeMode = true
		require.Error(t, validateReconcileConfig(opts, &fakeCfg))

		noKeyCfg := *valid
		noKeyCfg.PineconeAPIKey = ""
		require.Error(t, validateReconcileConfig(opts, &noKeyCfg))

		noIndexCfg := *valid
		noIndexCfg.PineconeIndexName = ""
		require.Error(t, validateReconcileConfig(opts, &noIndexCfg))
	}
}

func TestStagingCanonicalDocumentKeys(t *testing.T) {
	indexName, apiKey := os.Getenv("QUARRY_PINECONE_STAGING_INDEX"), os.Getenv("PINECONE_API_KEY")
	if indexName == "" || apiKey == "" {
		t.Skip("requires QUARRY_PINECONE_STAGING_INDEX and PINECONE_API_KEY")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	sdk, err := pinecone.NewClient(pinecone.NewClientParams{ApiKey: apiKey})
	require.NoError(t, err)
	idx, err := sdk.DescribeIndex(ctx, indexName)
	require.NoError(t, err)
	require.NotNil(t, idx.Dimension)
	require.Positive(t, *idx.Dimension)

	namespace := "quarry-canonical-keys-" + uuid.NewString()
	conn, err := sdk.Index(pinecone.NewIndexConnParams{Host: idx.Host, Namespace: namespace})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(t.Context()), 15*time.Second)
		defer cleanupCancel()
		if cleanupErr := conn.DeleteAllVectorsInNamespace(cleanupCtx); cleanupErr != nil {
			t.Errorf("delete staging vectors: %v", cleanupErr)
		}
		require.NoError(t, conn.Close())
	})

	docID := uuid.NewString()
	const collection = "staging-collection"
	embedding := make([]float32, *idx.Dimension)
	embedding[0] = 1
	md, err := structpb.NewStruct(map[string]any{
		"doc_id":     strings.ToUpper(docID),
		"collection": collection,
	})
	require.NoError(t, err)
	_, err = conn.UpsertVectors(ctx, []*pinecone.Vector{{
		Id:       "staging-chunk-0",
		Values:   &embedding,
		Metadata: md,
	}})
	require.NoError(t, err)

	// Wait for the initial upsert to become visible to filtered queries/updates.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var dryReport documentKeyReport
	for {
		dryReport, err = canonicalizeDocumentKeys(ctx, conn, []string{docID}, options{CanonicalDocumentIDs: true, Limit: 1})
		require.NoError(t, err)
		if dryReport.MatchedVectors == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	require.Zero(t, dryReport.Updated)

	allowFilter, err := structpb.NewStruct(map[string]any{
		"doc_id":     map[string]any{"$in": []any{docID}},
		"collection": map[string]any{"$eq": collection},
	})
	require.NoError(t, err)
	beforeQuery, err := conn.QueryByVectorValues(ctx, &pinecone.QueryByVectorValuesRequest{
		Vector:          embedding,
		TopK:            5,
		IncludeMetadata: true,
		MetadataFilter:  allowFilter,
	})
	require.NoError(t, err)
	require.Empty(t, beforeQuery.Matches, "dry run must leave the non-canonical vector unchanged")

	applyReport, err := canonicalizeDocumentKeys(ctx, conn, []string{docID}, options{CanonicalDocumentIDs: true, Apply: true, Limit: 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), applyReport.MatchedVectors)
	require.Equal(t, 1, applyReport.Updated)

	pollCtx, pollCancel := context.WithTimeout(ctx, 60*time.Second)
	defer pollCancel()
	for {
		afterQuery, queryErr := conn.QueryByVectorValues(pollCtx, &pinecone.QueryByVectorValuesRequest{
			Vector:          embedding,
			TopK:            5,
			IncludeMetadata: true,
			MetadataFilter:  allowFilter,
		})
		require.NoError(t, queryErr)
		cleanReport, cleanErr := canonicalizeDocumentKeys(pollCtx, conn, []string{docID}, options{CanonicalDocumentIDs: true, Limit: 1})
		require.NoError(t, cleanErr)
		if len(afterQuery.Matches) == 1 && cleanReport.MatchedVectors == 0 {
			gotMeta := afterQuery.Matches[0].Vector.Metadata.AsMap()
			require.Equal(t, docID, gotMeta["doc_id"])
			require.Equal(t, collection, gotMeta["collection"])
			break
		}
		select {
		case <-pollCtx.Done():
			t.Fatal(pollCtx.Err())
		case <-ticker.C:
		}
	}
}
