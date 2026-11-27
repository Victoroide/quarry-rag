package pinecone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type fakeIndexConn struct {
	upsert       func(context.Context, []*pinecone.Vector) (uint32, error)
	query        func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error)
	filterDelete func(context.Context, *structpb.Struct) error
	deleteIDs    func(context.Context, []string) error
	deleteAll    func(context.Context) error
	fetch        func(context.Context, []string) (*pinecone.FetchVectorsResponse, error)
	list         func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error)
	closed       int
}

func (f *fakeIndexConn) FetchVectors(ctx context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
	if f.fetch != nil {
		return f.fetch(ctx, ids)
	}
	return &pinecone.FetchVectorsResponse{}, nil
}
func (f *fakeIndexConn) ListVectors(ctx context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
	if f.list != nil {
		return f.list(ctx, req)
	}
	return &pinecone.ListVectorsResponse{}, nil
}

func (f *fakeIndexConn) UpsertVectors(ctx context.Context, vectors []*pinecone.Vector) (uint32, error) {
	if f.upsert != nil {
		return f.upsert(ctx, vectors)
	}
	if len(vectors) > 1000 {
		return 0, errors.New("too many vectors")
	}
	return uint32(len(vectors)), nil // #nosec G115 -- The batch length is bounded above.
}
func (f *fakeIndexConn) QueryByVectorValues(ctx context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
	if f.query != nil {
		return f.query(ctx, req)
	}
	return &pinecone.QueryVectorsResponse{}, nil
}
func (f *fakeIndexConn) DeleteVectorsByFilter(ctx context.Context, filter *structpb.Struct) error {
	if f.filterDelete != nil {
		return f.filterDelete(ctx, filter)
	}
	return nil
}

func (f *fakeIndexConn) DeleteVectorsByID(ctx context.Context, ids []string) error {
	if f.deleteIDs != nil {
		return f.deleteIDs(ctx, ids)
	}
	return nil
}
func (f *fakeIndexConn) DeleteAllVectorsInNamespace(ctx context.Context) error {
	if f.deleteAll != nil {
		return f.deleteAll(ctx)
	}
	return nil
}
func (f *fakeIndexConn) Close() error { f.closed++; return nil }

func testClient(conn *fakeIndexConn) *Client {
	return &Client{defaultConn: conn, ownershipFilters: ownershipFilters{document: true}, batchSize: 100, timeout: time.Second, maxAttempts: 5, dims: 1024}
}

func testChunks(count, dims, textBytes int) []domain.Chunk {
	chunks := make([]domain.Chunk, count)
	for i := range chunks {
		chunks[i] = domain.Chunk{ID: domain.ChunkID(fmt.Sprintf("chunk-%d", i)), DocumentID: "doc", Index: i, Text: strings.Repeat("x", textBytes), Embedding: make([]float32, dims)}
	}
	return chunks
}

func TestUpsertSplitsByCountAndBytes(t *testing.T) {
	for _, batchSize := range []int{100, 1000} {
		t.Run(fmt.Sprint(batchSize), func(t *testing.T) {
			seen := make(map[string]bool)
			calls := 0
			conn := &fakeIndexConn{upsert: func(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
				calls++
				if len(vectors) == 0 || len(vectors) > batchSize {
					t.Fatalf("batch has %d vectors, limit %d", len(vectors), batchSize)
				}
				size := 0
				for _, v := range vectors {
					size += 4*len(*v.Values) + len(v.Id) + proto.Size(v.Metadata) + 32
					if seen[v.Id] {
						t.Fatalf("duplicate vector %s", v.Id)
					}
					seen[v.Id] = true
				}
				if size > 1572864 {
					t.Fatalf("batch estimate is %d bytes", size)
				}
				if len(vectors) > 1000 {
					return 0, errors.New("too many vectors")
				}
				return uint32(len(vectors)), nil // #nosec G115 -- The batch length is bounded above.
			}}
			client := testClient(conn)
			client.batchSize = batchSize
			if err := client.Upsert(t.Context(), "", testChunks(950, 1024, 2400)); err != nil {
				t.Fatal(err)
			}
			if len(seen) != 950 || calls < 2 {
				t.Fatalf("received %d vectors in %d batches", len(seen), calls)
			}
		})
	}
}

func TestUpsertRetriesTransientCodes(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				conn := &fakeIndexConn{upsert: func(ctx context.Context, _ []*pinecone.Vector) (uint32, error) {
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("missing per-attempt deadline")
					}
					calls++
					if calls < 3 {
						return 0, status.Error(code, "temporary")
					}
					return 1, nil
				}}
				if err := testClient(conn).Upsert(t.Context(), "", testChunks(1, 1024, 10)); err != nil {
					t.Fatal(err)
				}
				if calls != 3 {
					t.Fatalf("got %d attempts", calls)
				}
			})
		})
	}
}

func TestUpsertDoesNotRetryInvalidArgument(t *testing.T) {
	calls := 0
	conn := &fakeIndexConn{upsert: func(context.Context, []*pinecone.Vector) (uint32, error) {
		calls++
		return 0, status.Error(codes.InvalidArgument, "invalid")
	}}
	err := testClient(conn).Upsert(t.Context(), "", testChunks(1, 1024, 10))
	if status.Code(err) != codes.InvalidArgument || calls != 1 {
		t.Fatalf("attempts=%d error=%v", calls, err)
	}
}

func TestRetriesStopWhenContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0
		client := testClient(&fakeIndexConn{upsert: func(context.Context, []*pinecone.Vector) (uint32, error) {
			calls++
			cancel()
			return 0, status.Error(codes.Unavailable, "temporary")
		}})
		if err := client.Upsert(ctx, "", testChunks(1, 1024, 10)); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation: %v", err)
		}
		if calls != 1 {
			t.Fatalf("got %d attempts after cancellation", calls)
		}
		if err := client.Upsert(ctx, "", testChunks(1, 1024, 10)); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation: %v", err)
		}
		if calls != 1 {
			t.Fatal("called transport with canceled context")
		}
	})
}

func TestAttemptTimeoutAndLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		client := testClient(&fakeIndexConn{})
		err := client.do(t.Context(), 3, func(ctx context.Context) error { calls++; <-ctx.Done(); return ctx.Err() })
		if !errors.Is(err, context.DeadlineExceeded) || calls != 3 {
			t.Fatalf("attempts=%d error=%v", calls, err)
		}
	})
}

func TestRecordMetadataAbove40KBIsRejected(t *testing.T) {
	for _, text := range []string{strings.Repeat("x", 40960), strings.Repeat("\x01", 7000)} {
		calls := 0
		client := testClient(&fakeIndexConn{upsert: func(context.Context, []*pinecone.Vector) (uint32, error) { calls++; return 1, nil }})
		chunks := testChunks(2, 1024, 10)
		chunks[1].Text = text
		err := client.Upsert(t.Context(), "", chunks)
		if !errors.Is(err, ErrMetadataTooLarge) || calls != 0 {
			t.Fatalf("attempts=%d error=%v", calls, err)
		}
	}
}

func TestRecordMetadataAt40KBIsAccepted(t *testing.T) {
	md, err := json.Marshal(map[string]any{"doc_id": "doc", "chunk_index": float64(0), "text": "", "token_count": float64(0)})
	if err != nil {
		t.Fatal(err)
	}
	chunks := testChunks(1, 1024, 40960-len(md))
	if err := testClient(&fakeIndexConn{}).Upsert(t.Context(), "", chunks); err != nil {
		t.Fatal(err)
	}
}

func TestChunkFromManifestIncarnationMetadata(t *testing.T) {
	claim, revision := uuid.NewString(), uuid.NewString()
	chunks := testChunks(3, 1024, 0)
	for i := range chunks {
		chunks[i].RevisionID = revision
		chunks[i].VectorIncarnation = claim
		chunks[i].VectorID = domain.FormatRevisionVectorID(chunks[i].DocumentID, revision, chunks[i].Index)
	}
	var encoded []*pinecone.Vector
	var batches int
	client := testClient(&fakeIndexConn{upsert: func(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
		count := len(vectors)
		if uint64(count) > math.MaxUint32 {
			return 0, fmt.Errorf("batch contains too many vectors: %d", count)
		}
		batches++
		encoded = append(encoded, vectors...)
		return uint32(count), nil
	}})
	client.batchSize = 2
	require.NoError(t, client.Upsert(t.Context(), "", chunks))
	require.Equal(t, 2, batches)
	require.Len(t, encoded, len(chunks))
	for i, vector := range encoded {
		require.Equal(t, claim, vector.Metadata.Fields[domain.VectorIncarnationKey].GetStringValue())
		decoded, ok := chunkFromMetadata(vector.Id, vector.Metadata)
		require.True(t, ok)
		require.Equal(t, chunks[i].ID, decoded.ID)
		require.Equal(t, chunks[i].VectorID, decoded.VectorID)
		require.Equal(t, claim, decoded.VectorIncarnation)
		require.Equal(t, revision, decoded.RevisionID)
	}

	vector := encoded[0]
	for _, bad := range []*structpb.Value{
		structpb.NewStringValue(""), structpb.NewStringValue("not-a-uuid"), structpb.NewStringValue(uuid.Nil.String()),
		structpb.NewStringValue("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"), structpb.NewStringValue("urn:uuid:" + claim),
		structpb.NewStringValue(string([]byte{0xff})), structpb.NewBoolValue(true), structpb.NewNumberValue(1),
		structpb.NewNullValue(), structpb.NewListValue(&structpb.ListValue{}), structpb.NewStructValue(&structpb.Struct{}),
	} {
		vector.Metadata.Fields[domain.VectorIncarnationKey] = bad
		_, ok := chunkFromMetadata(vector.Id, vector.Metadata)
		require.False(t, ok, "reject supplied stamp %v", bad)
	}
	delete(vector.Metadata.Fields, domain.VectorIncarnationKey)
	historical, ok := chunkFromMetadata(vector.Id, vector.Metadata)
	require.True(t, ok)
	require.Empty(t, historical.VectorIncarnation)
	require.Equal(t, revision, historical.RevisionID)

	// Even an otherwise valid physical tagged ID cannot carry both identities.
	tagged := hostedHit(t, memberID(0), claim, scopeTenant, "tagged")
	tagged.Vector.Metadata.Fields["revision_id"] = structpb.NewStringValue(revision)
	_, ok = chunkFromMetadata(tagged.Vector.Id, tagged.Vector.Metadata)
	require.False(t, ok)

	last := encoded[len(encoded)-1]
	metadata, err := json.Marshal(last.Metadata.AsMap())
	require.NoError(t, err)
	lastChunk := &chunks[len(chunks)-1]
	lastChunk.Text = strings.Repeat("x", maxMetadataBytes-len(metadata))
	require.NoError(t, client.Upsert(t.Context(), "", chunks))
	require.Equal(t, 4, batches)
	lastChunk.Text += "x"
	require.ErrorIs(t, client.Upsert(t.Context(), "", chunks), ErrMetadataTooLarge)
	require.Equal(t, 4, batches, "an oversized later record prevents every batch")
}

func FuzzManifestIncarnationMetadata(f *testing.F) {
	const claim = "8a54a82c-4e81-43aa-95ea-c7ee1166b0d6"
	for _, seed := range []string{`"` + claim + `"`, `""`, `null`, `true`, `1`, `[]`, `{}`, `"\ud800"`, "invalid JSON"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		var parsed any
		if json.Unmarshal([]byte(input), &parsed) != nil {
			return
		}
		stamp, err := structpb.NewValue(parsed)
		if err != nil {
			return
		}
		md := &structpb.Struct{Fields: map[string]*structpb.Value{
			"revision_id": structpb.NewStringValue("revision"), domain.VectorIncarnationKey: stamp,
		}}
		got, ok := chunkFromMetadata("vector", md)
		label, isString := parsed.(string)
		id, parseErr := uuid.Parse(label)
		want := isString && parseErr == nil && id != uuid.Nil && id.String() == label
		require.Equal(t, want, ok)
		if want {
			require.Equal(t, label, got.VectorIncarnation)
		}
	})
}

func TestValidateIndexRejectsDimensionMismatch(t *testing.T) {
	idx := &pinecone.Index{Dimension: new(int32(512)), Metric: pinecone.Cosine, Status: &pinecone.IndexStatus{Ready: true}}
	if err := validateIndex(idx, &config.Config{EmbeddingDimensions: 1024}); err == nil {
		t.Fatal("accepted incompatible dimensions")
	}
}

func TestValidateIndexRejectsMissingDimension(t *testing.T) {
	idx := &pinecone.Index{Metric: pinecone.Cosine, Status: &pinecone.IndexStatus{Ready: true}}
	if err := validateIndex(idx, &config.Config{EmbeddingDimensions: 1024}); err == nil {
		t.Fatal("accepted missing dimension")
	}
}

func TestUpsertPreservesVectorValues(t *testing.T) {
	chunks := testChunks(2, 2, 10)
	chunks[0].Embedding = []float32{0.25, -0.5}
	chunks[1].Embedding = []float32{0.75, 1}
	calls := 0
	conn := &fakeIndexConn{upsert: func(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
		calls++
		if len(vectors) != 2 {
			t.Fatalf("got %d vectors", len(vectors))
		}
		for i, want := range [][]float32{{0.25, -0.5}, {0.75, 1}} {
			if vectors[i].Values == nil || len(*vectors[i].Values) != 2 {
				t.Fatalf("missing values for vector %d", i)
			}
			for j, value := range want {
				if (*vectors[i].Values)[j] != value {
					t.Fatalf("vector %d value %d: got %v, want %v", i, j, (*vectors[i].Values)[j], value)
				}
			}
		}
		return 2, nil
	}}
	if err := testClient(conn).Upsert(t.Context(), "", chunks); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("upsert called %d times", calls)
	}
}
func TestValidateIndexRejectsNonCosine(t *testing.T) {
	idx := &pinecone.Index{Dimension: new(int32(1024)), Metric: pinecone.Dotproduct, Status: &pinecone.IndexStatus{Ready: true}}
	if err := validateIndex(idx, &config.Config{EmbeddingDimensions: 1024}); err == nil {
		t.Fatal("accepted incompatible metric")
	}
}
func TestValidateIndexReadiness(t *testing.T) {
	idx := &pinecone.Index{Dimension: new(int32(1024)), Metric: pinecone.Cosine}
	cfg := &config.Config{EmbeddingDimensions: 1024}
	if err := validateIndex(idx, cfg); err == nil {
		t.Fatal("accepted missing status")
	}
	idx.Status = &pinecone.IndexStatus{}
	if err := validateIndex(idx, cfg); err == nil {
		t.Fatal("accepted unready index")
	}
	idx.Status.Ready = true
	if err := validateIndex(idx, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestClientReusesDefaultConnection(t *testing.T) {
	conn := &fakeIndexConn{}
	client := testClient(conn)
	dials := 0
	client.dial = func(string) (indexConn, error) { dials++; return &fakeIndexConn{}, nil }
	for range 2 {
		if err := client.Upsert(t.Context(), "", testChunks(1, 1024, 10)); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Query(t.Context(), "", []float32{1}, 1, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := client.QueryAllNamespaces(t.Context(), []float32{1}, 1, ""); err != nil {
			t.Fatal(err)
		}
		if err := client.Delete(t.Context(), "", []string{"id"}); err != nil {
			t.Fatal(err)
		}
	}
	if dials != 0 || conn.closed != 0 {
		t.Fatalf("dials=%d closes=%d", dials, conn.closed)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if conn.closed != 1 {
		t.Fatalf("connection closed %d times", conn.closed)
	}
}

func TestLegacyNamespaceConnectionRemainsOpen(t *testing.T) {
	client := testClient(&fakeIndexConn{})
	var conns []*fakeIndexConn
	client.dial = func(namespace string) (indexConn, error) {
		if namespace != "legacy" {
			t.Fatalf("namespace=%q", namespace)
		}
		conn := &fakeIndexConn{deleteAll: func(context.Context) error { return status.Error(codes.NotFound, "missing") }}
		conns = append(conns, conn)
		return conn, nil
	}
	for range 2 {
		if err := client.DeleteAllVectorsInNamespace(t.Context(), "legacy"); err != nil {
			t.Fatal(err)
		}
	}
	if len(conns) != 2 {
		t.Fatalf("dialed %d connections", len(conns))
	}
	for _, conn := range conns {
		if conn.closed != 0 {
			t.Fatalf("closed %d times", conn.closed)
		}
	}
}

func TestLegacyConnectionRemainsOpenOnError(t *testing.T) {
	conn := &fakeIndexConn{query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "bad")
	}}
	client := testClient(&fakeIndexConn{})
	client.dial = func(string) (indexConn, error) { return conn, nil }
	if _, err := client.Query(t.Context(), "legacy", []float32{1}, 1, ""); err == nil {
		t.Fatal("expected query failure")
	}
	if conn.closed != 0 {
		t.Fatalf("closed %d times", conn.closed)
	}
}

func TestQueryRetriesAtMostThreeTimes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		client := testClient(&fakeIndexConn{query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			calls++
			if !req.IncludeMetadata || req.MetadataFilter.GetFields()["doc_type"].GetStringValue() != "report" {
				t.Fatal("query options lost")
			}
			return nil, status.Error(codes.Unavailable, "temporary")
		}})
		if _, err := client.Query(t.Context(), "", []float32{1}, 1, "report"); status.Code(err) != codes.Unavailable || calls != 3 {
			t.Fatalf("attempts=%d error=%v", calls, err)
		}
	})
}

func legacyMatch(id string) *pinecone.ScoredVector {
	return &pinecone.ScoredVector{Vector: &pinecone.Vector{Id: id, Metadata: &structpb.Struct{
		Fields: map[string]*structpb.Value{"doc_id": structpb.NewStringValue("doc")},
	}}}
}

func TestFilterDeleteFallsBackToIDs(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.Unimplemented, codes.FailedPrecondition} {
		t.Run(code.String(), func(t *testing.T) {
			calls := 0
			conn := &fakeIndexConn{
				filterDelete: func(context.Context, *structpb.Struct) error { return status.Error(code, "unsupported") },
				query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
					calls++
					require.EqualValues(t, cleanupPageSize, req.TopK)
					require.True(t, req.IncludeMetadata)
					require.False(t, req.IncludeValues)
					require.Equal(t, map[string]any{
						"doc_id":                 map[string]any{"$eq": "doc"},
						domain.VectorRevisionKey: map[string]any{"$exists": false},
						domain.VectorAttemptKey:  map[string]any{"$exists": false},
					}, req.MetadataFilter.AsMap())
					if calls == 1 {
						return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{legacyMatch("first")}}, nil
					}
					return &pinecone.QueryVectorsResponse{}, nil
				},
				deleteIDs: func(_ context.Context, ids []string) error {
					require.Equal(t, []string{"first"}, ids)
					return nil
				},
			}
			c := testClient(conn)
			c.ownershipFilters = ownershipFilters{document: true, revision: true, attempt: true}
			require.NoError(t, c.DeleteByDocID(t.Context(), "doc"))
			require.Equal(t, 2, calls)
		})
	}
}

func TestFilterDeleteFallbackDeletesOnlyNewIDs(t *testing.T) {
	queries := 0
	var deletes [][]string
	conn := &fakeIndexConn{
		query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			queries++
			switch queries {
			case 1:
				return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{legacyMatch("first")}}, nil
			case 2:
				return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{legacyMatch("first"), legacyMatch("second")}}, nil
			default:
				return &pinecone.QueryVectorsResponse{}, nil
			}
		},
		deleteIDs: func(_ context.Context, ids []string) error {
			deletes = append(deletes, ids)
			return nil
		},
	}
	require.NoError(t, testClient(conn).DeleteByDocID(t.Context(), "doc"))
	require.Equal(t, [][]string{{"first"}, {"second"}}, deletes)
}

func TestDeleteRetriesAndMissingVectors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, missing := range []bool{false, true} {
			calls := 0
			client := testClient(&fakeIndexConn{filterDelete: func(context.Context, *structpb.Struct) error {
				calls++
				if calls == 1 {
					return status.Error(codes.Unavailable, "temporary")
				}
				if missing {
					return status.Error(codes.NotFound, "missing")
				}
				return nil
			}})
			client.ownershipFilters = ownershipFilters{document: true, revision: true, attempt: true}
			if err := client.DeleteByDocID(t.Context(), "doc"); err != nil || calls != 2 {
				t.Fatalf("attempts=%d error=%v", calls, err)
			}
		}
	})
}

func TestRoundTripsLayoutProvenanceMetadata(t *testing.T) {
	var stored []*pinecone.Vector
	conn := &fakeIndexConn{
		upsert: func(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
			stored = vectors
			return 1, nil
		},
		query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			return &pinecone.QueryVectorsResponse{
				Matches: []*pinecone.ScoredVector{{Vector: stored[0], Score: 0.9}},
			}, nil
		},
		fetch: func(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
			out := make(map[string]*pinecone.Vector, len(ids))
			for _, id := range ids {
				for _, v := range stored {
					if v.Id == id {
						out[id] = v
					}
				}
			}
			return &pinecone.FetchVectorsResponse{Vectors: out}, nil
		},
	}
	client := testClient(conn)
	err := client.Upsert(t.Context(), "", []domain.Chunk{{
		ID:         "c1",
		DocumentID: "doc-1",
		Text:       "Header: Val",
		Embedding:  []float32{1},
		Metadata: map[string]any{
			"doc_type":  "report",
			"pages":     []int{2, 5},
			"table_ids": []string{"p2-t1"},
			"row_ids":   []string{"p2-t1-r3"},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	require.Len(t, stored, 1)
	for key, field := range stored[0].Metadata.Fields {
		lv := field.GetListValue()
		if lv == nil {
			continue
		}
		for i, elem := range lv.GetValues() {
			_, isString := elem.GetKind().(*structpb.Value_StringValue)
			require.Truef(t, isString, "metadata list field %q[%d] must be a StringValue, got %T", key, i, elem.GetKind())
		}
	}
	var gotStoredPages []string
	for _, elem := range stored[0].Metadata.Fields["pages"].GetListValue().GetValues() {
		gotStoredPages = append(gotStoredPages, elem.GetStringValue())
	}
	require.Equal(t, []string{"2", "5"}, gotStoredPages)

	assertProvenance := func(md map[string]any) {
		t.Helper()
		pages, ok := md["pages"].([]int)
		if !ok || len(pages) != 2 || pages[0] != 2 || pages[1] != 5 {
			t.Fatalf("unexpected pages metadata: %#v", md["pages"])
		}
		tids, ok := md["table_ids"].([]string)
		if !ok || len(tids) != 1 || tids[0] != "p2-t1" {
			t.Fatalf("unexpected table_ids metadata: %#v", md["table_ids"])
		}
		rids, ok := md["row_ids"].([]string)
		if !ok || len(rids) != 1 || rids[0] != "p2-t1-r3" {
			t.Fatalf("unexpected row_ids metadata: %#v", md["row_ids"])
		}
	}

	got, err := client.Query(t.Context(), "", []float32{1}, 1, "")
	if err != nil || len(got) != 1 {
		t.Fatalf("query failed: %v, len=%d", err, len(got))
	}
	assertProvenance(got[0].Chunk.Metadata)

	fetched, err := client.Fetch(t.Context(), []string{"c1"})
	require.NoError(t, err)
	require.Contains(t, fetched, "c1")
	assertProvenance(fetched["c1"].Metadata)

	// Unparseable or non-string entries in pages are skipped on both Query and Fetch.
	malformed := &pinecone.Vector{
		Id:     "c2",
		Values: &[]float32{1},
		Metadata: &structpb.Struct{Fields: map[string]*structpb.Value{
			"doc_id": structpb.NewStringValue("doc-1"),
			"pages": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
				structpb.NewStringValue("1"),
				structpb.NewStringValue("not-a-page"),
				structpb.NewNumberValue(3),
				structpb.NewStringValue("4"),
			}}),
			"table_ids": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
				structpb.NewStringValue(""),
				structpb.NewStringValue("p1-t1"),
			}}),
		}},
	}
	matches := matchesToScoredChunks([]*pinecone.ScoredVector{{Vector: malformed}}, "")
	require.Len(t, matches, 1)
	require.Equal(t, []int{1, 4}, matches[0].Chunk.Metadata["pages"])
	require.Equal(t, []string{"p1-t1"}, matches[0].Chunk.Metadata["table_ids"])

	stored = append(stored, malformed)
	fetchedMalformed, err := client.Fetch(t.Context(), []string{"c2"})
	require.NoError(t, err)
	require.Equal(t, []int{1, 4}, fetchedMalformed["c2"].Metadata["pages"])
	require.Equal(t, []string{"p1-t1"}, fetchedMalformed["c2"].Metadata["table_ids"])
}

func TestRevisionedUpsertAndMatchRestoration(t *testing.T) {
	var sent []*pinecone.Vector
	conn := &fakeIndexConn{
		upsert: func(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
			sent = append(sent, vectors...)
			return uint32(len(vectors)), nil // #nosec G115 -- test batch length is small.
		},
		query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			matches := make([]*pinecone.ScoredVector, 0, len(sent))
			for _, v := range sent {
				matches = append(matches, &pinecone.ScoredVector{Vector: v, Score: 0.9})
			}
			return &pinecone.QueryVectorsResponse{Matches: matches}, nil
		},
	}
	client := testClient(conn)
	chunks := []domain.Chunk{{
		ID: "public-chunk-id", DocumentID: "doc-1", Index: 0, Text: "body",
		Embedding: []float32{1, 0}, RevisionID: "rev-1", VectorID: "doc-1#rev-1#0",
	}}
	if err := client.Upsert(t.Context(), "", chunks); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0].Id != "doc-1#rev-1#0" {
		t.Fatalf("unexpected upserted vector ID: %+v", sent)
	}
	fields := sent[0].Metadata.GetFields()
	if fields["revision_id"].GetStringValue() != "rev-1" || fields["chunk_id"].GetStringValue() != "public-chunk-id" {
		t.Fatalf("missing revision metadata: %+v", fields)
	}
	results, err := client.Query(t.Context(), "", []float32{1, 0}, 5, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Chunk.ID != "public-chunk-id" || results[0].Chunk.RevisionID != "rev-1" || results[0].Chunk.VectorID != "doc-1#rev-1#0" {
		t.Fatalf("unexpected restored chunk: %+v", results)
	}
}

func TestQueryFilteredKeepsBothRevisionsOfSamePublicChunk(t *testing.T) {
	staleMD, err := structpb.NewStruct(map[string]any{"doc_id": "doc-1", "chunk_id": "public-1", "revision_id": "rev-1", "text": "old"})
	if err != nil {
		t.Fatal(err)
	}
	activeMD, err := structpb.NewStruct(map[string]any{"doc_id": "doc-1", "chunk_id": "public-1", "revision_id": "rev-2", "text": "new"})
	if err != nil {
		t.Fatal(err)
	}
	conn := &fakeIndexConn{
		query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{
				{Vector: &pinecone.Vector{Id: "doc-1#rev-1#0", Metadata: staleMD}, Score: 0.99},
				{Vector: &pinecone.Vector{Id: "doc-1#rev-2#0", Metadata: activeMD}, Score: 0.90},
			}}, nil
		},
	}
	got, err := testClient(conn).QueryFiltered(t.Context(), []float32{1, 0}, 5, domain.VectorFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected both revisions before visibility check, got %d", len(got))
	}
}

func TestDeleteByRevisionAndPresenceChecks(t *testing.T) {
	var deleted [][]string
	var usedFilter bool
	conn := &fakeIndexConn{
		filterDelete: func(_ context.Context, filter *structpb.Struct) error {
			usedFilter = true
			if filter.GetFields()["revision_id"].GetStructValue().GetFields()["$eq"].GetStringValue() != "rev-1" {
				t.Fatalf("unexpected filter: %v", filter)
			}
			return status.Error(codes.Unimplemented, "filter delete unsupported")
		},
		list: func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
			if req.Prefix == nil || *req.Prefix != "doc-1#rev-1#" {
				return nil, status.Error(codes.InvalidArgument, "list is not supported on pod-based indexes")
			}
			id := "doc-1#rev-1#0"
			return &pinecone.ListVectorsResponse{VectorIds: []*string{&id}}, nil
		},
		deleteIDs: func(_ context.Context, ids []string) error {
			deleted = append(deleted, ids)
			return nil
		},
		fetch: func(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
			return &pinecone.FetchVectorsResponse{Vectors: map[string]*pinecone.Vector{
				ids[0]: {Id: ids[0]},
			}}, nil
		},
	}
	client := testClient(conn)
	if err := client.DeleteByManifestRevision(t.Context(), "doc-1", "rev-1", nil); err != nil {
		t.Fatal(err)
	}
	if !usedFilter || len(deleted) != 1 || deleted[0][0] != "doc-1#rev-1#0" {
		t.Fatalf("unexpected revision delete fallback: filter=%v deleted=%v", usedFilter, deleted)
	}
	// Pod-style unsupported list must report supported=false without error.
	_, _, supported, err := client.ListPrefixVectorIDs(t.Context(), "", "other#", 10, "")
	if err != nil || supported {
		t.Fatalf("expected unsupported list, supported=%v err=%v", supported, err)
	}
	present, err := client.FetchPresentVectorIDs(t.Context(), "", []string{"doc-1#rev-1#0", "doc-1#rev-1#1"})
	if err != nil || len(present) != 1 || present[0] != "doc-1#rev-1#0" {
		t.Fatalf("unexpected present IDs: %v err=%v", present, err)
	}
}
