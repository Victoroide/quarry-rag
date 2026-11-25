package pinecone

import (
	"context"
	"fmt"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func fetchedVector(t *testing.T, id string, values []float32, fields map[string]any) *pinecone.Vector {
	t.Helper()
	md, err := structpb.NewStruct(fields)
	require.NoError(t, err)
	return &pinecone.Vector{Id: id, Values: &values, Metadata: md}
}

func TestFetchBatchesKeysAndMapsStoredVectors(t *testing.T) {
	var batches [][]string
	conn := &fakeIndexConn{fetch: func(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
		batches = append(batches, append([]string(nil), ids...))
		out := map[string]*pinecone.Vector{}
		for _, id := range ids {
			if id == "absent" {
				continue
			}
			out[id] = fetchedVector(t, id, []float32{1, 2, 3}, map[string]any{
				"doc_id": "doc-1", "chunk_index": float64(4), "text": "stored text", "token_count": float64(7),
				"doc_type": "report", "collection": "archive", domain.VectorTenantKey: "tenant-1", "kind": "paragraph", "quality": "high",
			})
		}
		return &pinecone.FetchVectorsResponse{Vectors: out}, nil
	}}
	client := testClient(conn)
	ids := make([]string, 0, 2*FetchBatchSize+1)
	for i := range 2 * FetchBatchSize {
		ids = append(ids, fmt.Sprintf("id-%d", i))
	}
	ids = append(ids, "absent")
	got, err := client.Fetch(t.Context(), ids)
	require.NoError(t, err)
	require.Len(t, batches, 3)
	for _, batch := range batches {
		require.LessOrEqual(t, len(batch), FetchBatchSize)
	}
	require.Len(t, got, 2*FetchBatchSize, "an absent key is simply missing")
	chunk := got["id-3"]
	require.Equal(t, domain.ChunkID("id-3"), chunk.ID)
	require.Equal(t, domain.DocumentID("doc-1"), chunk.DocumentID)
	require.Equal(t, 4, chunk.Index)
	require.Equal(t, "stored text", chunk.Text)
	require.Equal(t, 7, chunk.TokenCount)
	require.Equal(t, []float32{1, 2, 3}, chunk.Embedding)
	require.Equal(t, map[string]any{"doc_type": "report", "collection": "archive", domain.VectorTenantKey: "tenant-1", "kind": "paragraph", "quality": "high"}, chunk.Metadata)
}

func TestFetchWithoutValuesIsAnError(t *testing.T) {
	conn := &fakeIndexConn{fetch: func(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
		return &pinecone.FetchVectorsResponse{Vectors: map[string]*pinecone.Vector{ids[0]: {Id: ids[0]}}}, nil
	}}
	_, err := testClient(conn).Fetch(t.Context(), []string{"id"})
	require.ErrorContains(t, err, "values")
}

func TestFetchReportsProviderErrors(t *testing.T) {
	conn := &fakeIndexConn{fetch: func(context.Context, []string) (*pinecone.FetchVectorsResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}}
	_, err := testClient(conn).Fetch(t.Context(), []string{"id"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	got, err := testClient(&fakeIndexConn{}).Fetch(t.Context(), nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestListIDsPagesAndDetectsMissingSupport(t *testing.T) {
	first, second := "a", "b"
	token := "next"
	calls := 0
	conn := &fakeIndexConn{list: func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		calls++
		require.NotNil(t, req.Limit)
		require.Equal(t, uint32(2), *req.Limit)
		if req.PaginationToken == nil {
			return &pinecone.ListVectorsResponse{VectorIds: []*string{&first, &second}, NextPaginationToken: &token}, nil
		}
		require.Equal(t, token, *req.PaginationToken)
		empty := ""
		return &pinecone.ListVectorsResponse{VectorIds: []*string{&first, &empty, nil}}, nil
	}}
	client := testClient(conn)
	ids, next, err := client.ListIDs(t.Context(), "", 2)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, ids)
	require.Equal(t, "next", next)
	ids, next, err = client.ListIDs(t.Context(), next, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, ids, "blank and nil keys are dropped")
	require.Empty(t, next)
	require.Equal(t, 2, calls)

	for _, bad := range []int{0, 1001} {
		_, _, err = client.ListIDs(t.Context(), "", bad)
		require.Error(t, err)
	}

	for name, listErr := range map[string]error{
		"unimplemented": status.Error(codes.Unimplemented, "unsupported"),
		"pod":           status.Error(codes.InvalidArgument, "List is only supported for serverless indexes"),
	} {
		t.Run(name, func(t *testing.T) {
			unsupported := testClient(&fakeIndexConn{list: func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
				return nil, listErr
			}})
			_, _, listErr := unsupported.ListIDs(t.Context(), "", 10)
			require.ErrorIs(t, listErr, ErrListingUnsupported)
		})
	}
	other := testClient(&fakeIndexConn{list: func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}})
	_, _, err = other.ListIDs(t.Context(), "", 10)
	require.NotErrorIs(t, err, ErrListingUnsupported)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestDeleteIDsUsesTheDefaultNamespace(t *testing.T) {
	var deleted []string
	conn := &fakeIndexConn{deleteIDs: func(_ context.Context, ids []string) error { deleted = append(deleted, ids...); return nil }}
	require.NoError(t, testClient(conn).Delete(t.Context(), "", []string{"a", "b"}))
	require.Equal(t, []string{"a", "b"}, deleted)
}
