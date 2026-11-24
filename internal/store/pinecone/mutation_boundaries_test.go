package pinecone

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	pg "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestSelectiveCleanupAvoidsUnindexedPredicate(t *testing.T) {
	doc := "doc"
	dirty := true
	queries := 0
	base := &fakeIndexConn{
		filterDelete: func(_ context.Context, f *structpb.Struct) error {
			if _, ok := f.Fields[domain.VectorRevisionKey]; ok {
				return nil
			}
			t.Error("broad filter")
			return nil
		},
		query: func(_ context.Context, _ *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			queries++
			if !dirty {
				return &pinecone.QueryVectorsResponse{}, nil
			}
			return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{legacyMatch("legacy")}}, nil
		},
		deleteIDs: func(_ context.Context, _ []string) error { dirty = false; return nil },
	}
	err := testClient(base).DeleteByDocID(domain.TrackVectorMutation(t.Context()), doc)
	t.Logf("selective non-indexed predicate matched nothing: err=%v queries=%d legacy present=%v", err, queries, dirty)
	require.NoError(t, err)
	require.False(t, dirty)
	require.Positive(t, queries)
}
func TestAcknowledgedTaggedFilterChecksPrefix(t *testing.T) {
	const prefix = "doc/rev/"
	id := prefix + "chunk"
	visible := true
	listCalls := 0
	filterCalls := 0
	base := &fakeIndexConn{filterDelete: func(_ context.Context, filter *structpb.Struct) error {
		require.Contains(t, filter.Fields, domain.VectorRevisionKey)
		filterCalls++
		return nil
	}}
	c := testClient(base)
	c.ownershipFilters = ownershipFilters{document: true, revision: true, attempt: true}
	c.defaultConn = listedIndexConn{base, func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		require.Equal(t, prefix, *req.Prefix)
		listCalls++
		if visible {
			return &pinecone.ListVectorsResponse{VectorIds: []*string{&id}}, nil
		}
		return &pinecone.ListVectorsResponse{}, nil
	}}
	first := c.DeleteByRevision(domain.TrackVectorMutation(t.Context()), "doc", "rev")
	t.Logf("acknowledged tagged filter with visible prefix: err=%v, filters=%d, lists=%d", first, filterCalls, listCalls)
	require.ErrorContains(t, first, "remain visible")
	require.Equal(t, 1, listCalls)
	require.Equal(t, 1, filterCalls)
	visible = false
	require.NoError(t, c.DeleteByRevision(domain.TrackVectorMutation(t.Context()), "doc", "rev"))
}

func TestUnsupportedListingUsesVerifiedQuery(t *testing.T) {
	const targetID, neighborID = "doc/rev/target", "doc/neighbor/chunk"
	live := map[string]string{targetID: "rev", neighborID: "neighbor"}
	queries, lists, deletes := 0, 0, 0
	index := &fakeIndexConn{
		filterDelete: func(context.Context, *structpb.Struct) error {
			return status.Error(codes.Unimplemented, "filter delete not supported")
		},
		query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			queries++
			require.True(t, req.IncludeMetadata)
			require.Equal(t, map[string]any{"doc_id": map[string]any{"$eq": "doc"}}, req.MetadataFilter.AsMap())
			var page pinecone.QueryVectorsResponse
			for id, revision := range live {
				match := legacyMatch(id)
				match.Vector.Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue(revision)
				page.Matches = append(page.Matches, match)
			}
			return &page, nil
		},
		deleteIDs: func(_ context.Context, ids []string) error {
			require.Equal(t, []string{targetID}, ids)
			deletes++
			delete(live, targetID)
			return nil
		},
	}
	c := testClient(index)
	c.defaultConn = listedIndexConn{index, func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		lists++
		return nil, status.Error(codes.Unimplemented, "list only supported for serverless indexes")
	}}
	require.NoError(t, c.DeleteByRevision(domain.TrackVectorMutation(t.Context()), "doc", "rev"))
	require.Equal(t, 1, lists)
	require.Equal(t, 2, queries)
	require.Equal(t, 1, deletes)
	require.Equal(t, map[string]string{neighborID: "neighbor"}, live)
}
func TestSelectiveCleanupRespectsMetadataResponseBudget(t *testing.T) {
	live := map[string]bool{}
	for i := 0; i < 700; i++ {
		live[fmt.Sprint(i)] = true
	}
	b := &fakeIndexConn{filterDelete: func(context.Context, *structpb.Struct) error {
		return status.Error(codes.InvalidArgument, "unindexed predicate")
	},
		query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			var page pinecone.QueryVectorsResponse
			for id := range live {
				if len(page.Matches) >= int(req.TopK) {
					break
				}
				m := legacyMatch(id)
				m.Vector.Metadata.Fields["doc_id"] = structpb.NewStringValue("doc")
				if req.IncludeMetadata {
					m.Vector.Metadata.Fields["text"] = structpb.NewStringValue(strings.Repeat("x", 8000))
				}
				page.Matches = append(page.Matches, m)
			}
			if req.IncludeMetadata && len(page.Matches)*8000 > 4*1024*1024 {
				return nil, status.Error(codes.OutOfRange, "result exceeds four MiB")
			}
			return &page, nil
		},
		deleteIDs: func(_ context.Context, ids []string) error {
			for _, id := range ids {
				delete(live, id)
			}
			return nil
		},
	}
	err := testClient(b).DeleteByDocID(domain.TrackVectorMutation(t.Context()), "doc")
	t.Logf("700 admissible 8KB chunks: remaining=%d err=%v", len(live), err)
	require.NoError(t, err)
	require.Empty(t, live)
}
func TestRechecksOwnershipBeforeEachUpsertBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL")
	}
	ctx := t.Context()
	db, err := pg.NewClient(ctx, pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { db.Pool.Close() })
	require.NoError(t, db.RunMigrations(ctx))
	id := domain.DocumentID(uuid.NewString())
	_, err = db.AcceptDocument(ctx, domain.AcceptRequest{
		Document: domain.Document{ID: id, Content: "two chunks"}, Fingerprint: chunker.Fingerprint("two chunks"), MaxPending: 2,
	})
	require.NoError(t, err)
	job, err := db.ClaimJob(ctx, "worker", time.Minute, 3)
	require.NoError(t, err)
	calls := 0
	b := &fakeIndexConn{upsert: func(call context.Context, v []*pinecone.Vector) (uint32, error) {
		calls++
		if calls == 1 {
			require.NoError(t, db.DeleteDocument(call, id))
		}
		require.Len(t, v, 1)
		return 1, nil
	}}
	c := testClient(b)
	c.batchSize = 1
	chunks := testChunks(2, 2, 10)
	for i := range chunks {
		chunks[i].DocumentID = id
	}
	err = db.MutateJobVectors(ctx, *job, func(call context.Context) error { return c.Upsert(call, "", chunks) })
	require.ErrorIs(t, err, domain.ErrJobNotOwned)
	t.Logf("after job removal during batch one: provider calls=%d, checkpoint=%v", calls, err)
	if calls != 1 {
		t.Error("second batch dispatched with no live job")
	}
}
func TestPartitionCandidatesReachVisibilityBeforeTrim(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL")
	}
	ctx := t.Context()
	db, err := pg.NewClient(ctx, pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(db.Pool.Close)
	require.NoError(t, db.RunMigrations(ctx))
	ids := make([]string, 10001)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	hitsByDoc := map[string]*pinecone.ScoredVector{}
	for n, doc := range []string{ids[0], ids[10000]} {
		id := uuid.NewString()
		var revision string
		require.NoError(t, db.Pool.QueryRow(ctx, "INSERT INTO documents(id,content,status,scoped_vectors) VALUES ($1,'text','completed',true) RETURNING incarnation::text", doc).Scan(&revision))
		require.NoError(t, db.InsertChunks(ctx, []domain.Chunk{{ID: domain.ChunkID(id), DocumentID: domain.DocumentID(doc), Text: "text"}}))
		score := float32(.8)
		if n == 0 {
			revision = uuid.NewString()
			score = 1
		}
		m := legacyMatch(doc + "/" + revision + "/" + id)
		m.Score = score
		m.Vector.Metadata.Fields["doc_id"] = structpb.NewStringValue(doc)
		m.Vector.Metadata.Fields["text"] = structpb.NewStringValue("text")
		m.Vector.Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue(revision)
		hitsByDoc[doc] = m
	}
	groups := 0
	b := &fakeIndexConn{query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		groups++
		doc := req.MetadataFilter.Fields["doc_id"].GetStructValue().Fields["$in"].GetListValue().Values[0].GetStringValue()
		return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{hitsByDoc[doc]}}, nil
	}}
	c := testClient(b)
	ret := retriever.NewRetriever(tracing.WrapVectorStore(NewDriver(c), "pinecone"), nil, .7, 1)
	vis := pg.Visibility{Pool: db.Pool}
	filter := domain.VectorFilter{DocumentIDs: ids}
	result, err := ret.RetrieveWithVisibility(ctx, []float32{1}, retriever.RetrievalModeGlobal, "", 1, filter, vis.VisibleEvidence)
	require.NoError(t, err)
	require.Equal(t, 2, groups)
	got := result.Chunks
	single, err := c.QueryFiltered(ctx, []float32{1}, 1, domain.VectorFilter{DocumentIDs: ids[10000:]})
	require.NoError(t, err)
	control, err := vis.VisibleEvidence(ctx, single)
	require.NoError(t, err)
	require.Len(t, control, 1)
	t.Logf("two allowlist groups returned both candidates; visible after adapter trim=%d, current-group control=%d", len(got), len(control))
	if len(got) != 1 {
		t.Error("known current candidate was discarded before visibility")
	}
}
func TestPartialUpsertAcknowledgmentIsRetryable(t *testing.T) {
	for _, count := range []uint32{1, 2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			calls := 0
			index := &fakeIndexConn{upsert: func(context.Context, []*pinecone.Vector) (uint32, error) {
				calls++
				return count, nil
			}}
			ctx := domain.TrackVectorMutation(t.Context())
			err := testClient(index).Upsert(ctx, "", testChunks(2, 2, 10))
			if count == 1 {
				require.ErrorContains(t, err, "acknowledged 1 of 2")
				require.True(t, domain.VectorMutationUncertain(err))
			} else {
				require.NoError(t, err)
				require.False(t, domain.VectorMutationUncertain(err))
			}
			require.Equal(t, 1, calls)
		})
	}
}

func TestTrackedUpsertClaimAndOutcome(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL")
	}
	db, err := pg.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(db.Pool.Close)
	require.NoError(t, db.RunMigrations(t.Context()))

	for _, code := range []codes.Code{codes.OK, codes.Unauthenticated, codes.PermissionDenied, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			ctx := t.Context()
			id := domain.DocumentID(uuid.NewString())
			_, err := db.AcceptDocument(ctx, domain.AcceptRequest{
				Document: domain.Document{ID: id, Content: string(id)}, CallerSupplied: true, Fingerprint: string(id), MaxPending: 2,
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, db.DeleteDocument(ctx, id)) }()
			job, err := db.ClaimJob(ctx, "worker", 2*time.Minute, 2)
			require.NoError(t, err)
			require.NotNil(t, job)
			chunks := testChunks(1, 2, 10)
			chunks[0].ID = domain.ChunkID(uuid.NewString())
			chunks[0].DocumentID = id
			chunks[0].Embedding[0] = 1

			reply := code
			calls := 0
			index := &fakeIndexConn{upsert: func(rpcCtx context.Context, records []*pinecone.Vector) (uint32, error) {
				calls++
				require.Len(t, records, 1)
				if code == codes.OK {
					deadline, ok := rpcCtx.Deadline()
					require.True(t, ok)
					require.LessOrEqual(t, time.Until(deadline), time.Second)
					var remaining float64
					require.NoError(t, db.Pool.QueryRow(rpcCtx, `SELECT EXTRACT(EPOCH FROM vector_mutation_until-clock_timestamp())::float8
						FROM documents WHERE id=$1 AND vector_mutation_pending`, id).Scan(&remaining))
					require.Positive(t, remaining)
					require.InDelta(t, time.Until(deadline).Seconds(), remaining, 0.25)
				}
				if reply != codes.OK {
					return 0, status.Error(reply, "provider rejected upsert")
				}
				return 1, nil
			}}
			client := testClient(index)
			outer, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			upsert := func(tracked context.Context) error { return client.Upsert(tracked, "", chunks) }
			require.Equal(t, code, status.Code(db.MutateJobVectors(outer, *job, upsert)))
			require.Equal(t, 1, calls)
			checkOutcome := func() {
				t.Helper()
				var pending, uncertain, deleteUncertain bool
				require.NoError(t, db.Pool.QueryRow(ctx, `SELECT vector_mutation_pending,vector_mutation_uncertain,vector_delete_uncertain
					FROM documents WHERE id=$1`, id).Scan(&pending, &uncertain, &deleteUncertain))
				require.False(t, pending)
				require.False(t, deleteUncertain)
				assert.Equal(t, code == codes.Unavailable, uncertain)
			}
			checkOutcome()
			if code != codes.OK {
				reply = codes.OK
				require.NoError(t, db.MutateJobVectors(outer, *job, upsert))
				require.Equal(t, 2, calls)
				checkOutcome()
			}
			published, err := db.CompleteJob(ctx, *job, chunks, domain.IndexObservation{})
			if code == codes.Unavailable {
				require.ErrorContains(t, err, "vector mutation is unresolved")
				require.False(t, published)
			} else {
				require.NoError(t, err)
				require.True(t, published)
			}
		})
	}
}

func TestUntaggedJobPublicationAfterHTTPWriteRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL")
	}
	db, err := pg.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(db.Pool.Close)
	require.NoError(t, db.RunMigrations(t.Context()))

	for _, tt := range []struct {
		name        string
		firstStatus int
		publishes   bool
	}{
		{name: "refused before connecting", publishes: true},
		{name: "throttled", firstStatus: http.StatusTooManyRequests, publishes: true},
		{name: "gateway timeout", firstStatus: http.StatusGatewayTimeout},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := visibilityClientWithFirstWriteFailure(t, tt.firstStatus)
			upsert := func(tracked context.Context, chunks []domain.Chunk) error {
				probe, upsertErr := client.UpsertWithVisibility(tracked, "", chunks)
				if upsertErr == nil {
					require.NotNil(t, probe)
				}
				return upsertErr
			}
			testUntaggedJobPublicationAfterWriteRetry(t, db, upsert, nil, tt.publishes)
		})
	}
}

func TestUntaggedJobPublicationAfterGRPCWriteRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL")
	}
	db, err := pg.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(db.Pool.Close)
	require.NoError(t, db.RunMigrations(t.Context()))

	for _, tt := range []struct {
		name     string
		received int32
	}{
		{name: "refused before opening a stream"},
		{name: "unavailable after receiving the write", received: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newGRPCWriteFixture(t)
			if tt.received != 0 {
				fixture.allowWrites(t)
				fixture.failAfterReceipt.Store(true)
			}
			upsert := func(ctx context.Context, chunks []domain.Chunk) error {
				return fixture.driver.Upsert(ctx, "", chunks)
			}
			checkFirst := func(err error) {
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Equal(t, tt.received, fixture.received.Load())
				if tt.received == 0 {
					require.Positive(t, fixture.dialRefusals.Load())
				}
				fixture.allowWrites(t)
			}
			testUntaggedJobPublicationAfterWriteRetry(t, db, upsert, checkFirst, tt.received == 0)
			require.Equal(t, tt.received+1, fixture.received.Load())
		})
	}
}

func testUntaggedJobPublicationAfterWriteRetry(t *testing.T, db *pg.Client, upsert func(context.Context, []domain.Chunk) error, checkFirst func(error), publishes bool) {
	t.Helper()
	ctx := t.Context()
	id := domain.DocumentID(uuid.NewString())
	_, err := db.AcceptDocument(ctx, domain.AcceptRequest{
		Document: domain.Document{ID: id, Content: string(id)}, CallerSupplied: true, Fingerprint: string(id), MaxPending: 2,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.DeleteDocument(ctx, id)) }()
	job, err := db.ClaimJob(ctx, "worker", 2*time.Minute, 2)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, id, job.Document.ID)
	require.False(t, job.ScopedVectors)

	chunks := testChunks(1, 2, 10)
	chunks[0].ID = domain.ChunkID(uuid.NewString())
	chunks[0].DocumentID = id
	chunks[0].Embedding[0] = 1
	write := func(ctx context.Context) error { return upsert(ctx, chunks) }

	firstErr := db.MutateJobVectors(ctx, *job, write)
	require.Error(t, firstErr)
	if checkFirst != nil {
		checkFirst(firstErr)
	}
	require.NoError(t, db.RetryJob(ctx, *job, 0, firstErr.Error()))
	retry, err := db.ClaimJob(ctx, "worker", 2*time.Minute, 2)
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Equal(t, id, retry.Document.ID)
	require.Equal(t, job.Attempts+1, retry.Attempts)
	require.False(t, retry.ScopedVectors)
	require.NoError(t, db.MutateJobVectors(ctx, *retry, write))

	published, err := db.CompleteJob(ctx, *retry, chunks, domain.IndexObservation{})
	if publishes {
		require.NoError(t, err)
		require.True(t, published)
	} else {
		require.ErrorContains(t, err, "vector mutation is unresolved")
		require.False(t, published)
	}
}

func TestRechecksOwnershipBeforeDeletingAnotherPage(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL")
	}
	ctx := t.Context()
	db, err := pg.NewClient(ctx, pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(db.Pool.Close)
	require.NoError(t, db.RunMigrations(ctx))
	id := domain.DocumentID(uuid.NewString())
	_, err = db.AcceptDocument(ctx, domain.AcceptRequest{
		Document: domain.Document{ID: id, Content: "two pages"}, Fingerprint: chunker.Fingerprint("two pages"), MaxPending: 2,
	})
	require.NoError(t, err)
	job, err := db.ClaimJob(ctx, "worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	deletes, queries := 0, 0
	index := &fakeIndexConn{
		query: func(ctx context.Context, _ *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			queries++
			if queries == 2 {
				if deleteErr := db.DeleteDocument(ctx, id); deleteErr != nil {
					return nil, deleteErr
				}
			}
			match := legacyMatch(fmt.Sprint(queries))
			match.Vector.Metadata.Fields["doc_id"] = structpb.NewStringValue(string(id))
			return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{match}}, nil
		},
		deleteIDs: func(context.Context, []string) error {
			deletes++
			return nil
		},
	}
	err = db.MutateJobVectors(ctx, *job, func(ctx context.Context) error {
		return testClient(index).DeleteByDocID(ctx, string(id))
	})
	require.ErrorIs(t, err, domain.ErrJobNotOwned)
	require.Equal(t, 2, queries)
	require.Equal(t, 1, deletes)
}
