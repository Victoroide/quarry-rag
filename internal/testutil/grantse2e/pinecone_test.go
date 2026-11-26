package grantse2e

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/Victoroide/quarry-rag/internal/testutil/pineconetest"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPineconeDataPlaneDispatch(t *testing.T) {
	fixture, conn, _ := newPinecone(t)
	ctx := t.Context()
	near, far := make([]float32, dimensions), make([]float32, dimensions)
	near[0], far[1] = 1, 1
	allowed, err := structpb.NewStruct(map[string]any{"doc_id": "allowed"})
	require.NoError(t, err)
	forbidden, err := structpb.NewStruct(map[string]any{"doc_id": "forbidden"})
	require.NoError(t, err)
	count, err := conn.UpsertVectors(ctx, []*pinecone.Vector{
		{Id: "far", Values: &far, Metadata: allowed},
		{Id: "near", Values: &near, Metadata: forbidden},
	})
	require.NoError(t, err)
	require.Equal(t, uint32(2), count)
	filter, err := structpb.NewStruct(map[string]any{"doc_id": map[string]any{"$in": []any{"allowed"}}})
	require.NoError(t, err)
	result, err := conn.QueryByVectorValues(ctx, &pinecone.QueryByVectorValuesRequest{
		Vector: near, TopK: 1, IncludeMetadata: true, MetadataFilter: filter,
	})
	require.NoError(t, err)
	require.Len(t, result.Matches, 1)
	require.Equal(t, "far", result.Matches[0].Vector.Id)
	require.Equal(t, float32(0), result.Matches[0].Score)
	require.Equal(t, []string{"far"}, fixture.Calls()[1].Keys)

	list, err := conn.ListVectors(ctx, &pinecone.ListVectorsRequest{})
	require.NoError(t, err)
	require.Len(t, list.VectorIds, 2)
	require.Equal(t, "far", *list.VectorIds[0])
	fetched, err := conn.FetchVectors(ctx, []string{"far"})
	require.NoError(t, err)
	require.Contains(t, fetched.Vectors, "far")
	require.Equal(t, "allowed", fetched.Vectors["far"].Metadata.Fields["doc_id"].GetStringValue())

	require.NoError(t, conn.DeleteVectorsByFilter(ctx, filter))
	require.Equal(t, []string{"near"}, fixture.IDs())
	require.NoError(t, conn.DeleteVectorsByID(ctx, []string{"near"}))
	require.Empty(t, fixture.IDs())
}

func TestPineconeDataPlaneRejectsInvalidRequests(t *testing.T) {
	fixture := &Pinecone{index: pcstore.NewMemoryIndex(dimensions)}
	for _, tc := range []struct {
		method, path, body string
		code               int
	}{
		{http.MethodPost, "/query", "{", http.StatusBadRequest},
		{http.MethodPost, "/query", "{}{}", http.StatusBadRequest},
		{http.MethodPost, "/query", `{"topK":1,"invalid":true}`, http.StatusBadRequest},
		{http.MethodPost, "/query", `{"namespace":"other","topK":1}`, http.StatusBadRequest},
		{http.MethodPost, "/query", `{"topK":1,"filter":{"other":{"$eq":"id"}}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", `{"topK":1,"filter":{"doc_id":{"$gt":"id"}}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", `{"topK":1,"filter":{"doc_id":{"$eq":0}}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", `{"topK":1,"filter":{"doc_id":{"$in":[false]}}}`, http.StatusBadRequest},
		{http.MethodPost, "/vectors/delete", `{"deleteAll":true}`, http.StatusBadRequest},
		{http.MethodGet, "/vectors/list?namespace=other", "", http.StatusBadRequest},
		{http.MethodGet, "/vectors/fetch?namespace=&ids=one&unexpected=1", "", http.StatusBadRequest},
		{http.MethodPost, "/vectors/unknown", "{}", http.StatusNotImplemented},
	} {
		req := httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, strings.NewReader(tc.body))
		out := httptest.NewRecorder()
		fixture.ServeHTTP(out, req)
		require.Equal(t, tc.code, out.Code, "%s %s %s", tc.method, tc.path, tc.body)
	}
	require.Empty(t, fixture.Calls())

	srv := httptest.NewServer(fixture)
	t.Cleanup(srv.Close)
	endpoint, err := url.Parse(srv.URL)
	require.NoError(t, err)
	namespaced := pineconetest.NewHTTPIndexConn(*endpoint, srv.Client(), "named")
	_, err = namespaced.ListVectors(t.Context(), &pinecone.ListVectorsRequest{})
	require.ErrorContains(t, err, "invalid namespace")
}
