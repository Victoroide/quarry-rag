package grantse2e

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/Victoroide/quarry-rag/internal/testutil/pineconetest"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPineconeDataPlaneDispatch(t *testing.T) {
	fixture, conn, _ := newPinecone(t)
	ctx := t.Context()
	near, far := make([]float32, dimensions), make([]float32, dimensions)
	near[0], far[1] = 1, 1
	allowed, err := structpb.NewStruct(map[string]any{"doc_id": "allowed", domain.VectorRevisionKey: "live"})
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
	scoped, err := structpb.NewStruct(map[string]any{"$and": []any{
		map[string]any{"doc_id": map[string]any{"$in": []any{"allowed"}}},
		map[string]any{"$or": []any{
			map[string]any{domain.VectorRevisionKey: map[string]any{"$in": []any{"live"}}},
			map[string]any{
				"revision_id":            map[string]any{"$exists": true},
				domain.VectorRevisionKey: map[string]any{"$exists": false},
			},
		}},
	}})
	require.NoError(t, err)
	result, err := conn.QueryByVectorValues(ctx, &pinecone.QueryByVectorValuesRequest{
		Vector: near, TopK: 1, IncludeMetadata: true, MetadataFilter: scoped,
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

func TestPineconeDataPlaneRanksScopedManifestAndTaggedVectors(t *testing.T) {
	const maxMembers = 10_000
	fixture, conn, host := newPinecone(t)
	driver := pcstore.NewDriver(pcstore.NewClientForIndex(conn, fixtureConfig("pinecone", ""), host))
	ctx := t.Context()
	scope := &domain.AuthorizedScope{
		TenantID: uuid.NewString(), Incarnations: make(map[string]string, maxMembers+1),
	}
	docs := make([]string, maxMembers+1)
	for i := range docs {
		docs[i] = uuid.NewString()
		scope.Incarnations[docs[i]] = uuid.NewString()
	}
	newChunk := func(doc string, secondary float32) domain.Chunk {
		vector := make([]float32, dimensions)
		vector[0], vector[1] = 1, secondary
		return domain.Chunk{
			ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(doc), Embedding: vector,
			Metadata: map[string]any{domain.VectorTenantKey: scope.TenantID, "collection": Collection, "doc_type": FileType},
		}
	}
	manifest := func(stamp string, secondary float32) domain.Chunk {
		chunk := newChunk(docs[0], secondary)
		chunk.RevisionID = uuid.NewString()
		chunk.VectorID = domain.FormatRevisionVectorID(chunk.DocumentID, chunk.RevisionID, chunk.Index)
		chunk.VectorIncarnation = stamp
		return chunk
	}
	tagged := func(doc string) domain.Chunk {
		chunk := newChunk(doc, 3)
		incarnation, attempt := scope.Incarnations[doc], uuid.NewString()
		chunk.Metadata[domain.VectorRevisionKey] = incarnation
		chunk.Metadata[domain.VectorAttemptKey] = attempt
		key, err := pcstore.TaggedVectorID(doc, incarnation, attempt, string(chunk.ID))
		require.NoError(t, err)
		chunk.VectorID = key
		return chunk
	}
	obsolete := manifest(uuid.NewString(), 0)
	active := manifest(scope.Incarnations[docs[0]], 1)
	historical := manifest("", 2)
	firstTagged, lastTagged := tagged(docs[0]), tagged(docs[maxMembers])
	require.NoError(t, driver.Upsert(ctx, "", []domain.Chunk{obsolete, active, historical, firstTagged, lastTagged}))
	query := obsolete.Embedding
	const topK = 3

	// Establish that the obsolete vector would occupy a ranked slot without scope.
	raw, err := conn.QueryByVectorValues(ctx, &pinecone.QueryByVectorValuesRequest{Vector: query, TopK: topK})
	require.NoError(t, err)
	require.Len(t, raw.Matches, topK)
	require.Equal(t, obsolete.VectorID, raw.Matches[0].Vector.Id)

	for _, members := range []int{1, maxMembers, maxMembers + 1} {
		t.Run(fmt.Sprintf("%d members", members), func(t *testing.T) {
			before := len(fixture.Calls())
			hits, queryErr := driver.QueryCandidates(t.Context(), query, topK, domain.VectorFilter{
				Authorized: scope, DocumentIDs: docs[:members], Collection: Collection, DocType: FileType,
			})
			require.NoError(t, queryErr)
			wantKeys := [][]string{{active.VectorID, historical.VectorID, firstTagged.VectorID}}
			wantIDs := []domain.ChunkID{active.ID, historical.ID, firstTagged.ID}
			if members > maxMembers {
				wantKeys = append(wantKeys, []string{lastTagged.VectorID})
				wantIDs = append(wantIDs, lastTagged.ID)
			}
			calls := fixture.Calls()[before:]
			require.Len(t, calls, len(wantKeys))
			for i, call := range calls {
				require.Equal(t, "/query", call.Path)
				require.Empty(t, call.Namespace)
				require.Equal(t, uint32(topK), call.TopK)
				require.Equal(t, wantKeys[i], call.Keys)
			}
			gotIDs := make([]domain.ChunkID, 0, len(hits))
			for _, hit := range hits {
				gotIDs = append(gotIDs, hit.Chunk.ID)
			}
			require.Equal(t, wantIDs, gotIDs)
		})
	}
}

func TestPineconeDataPlaneRejectsInvalidRequests(t *testing.T) {
	fixture := &Pinecone{index: pcstore.NewMemoryIndex(dimensions)}
	const query = `{"topK":1,"vector":[1,0,0,0,0,0,0,0],`
	for _, tc := range []struct {
		method, path, body string
		code               int
	}{
		{http.MethodPost, "/query", "{", http.StatusBadRequest},
		{http.MethodPost, "/query", "{}{}", http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"invalid":true}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"namespace":"other"}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"other":{"$eq":"id"}}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"doc_id":{"$gt":"id"}}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"doc_id":{"$eq":0}}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"doc_id":{"$in":[false]}}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"$and":[]}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"$or":{}}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"$or":[{}]}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"$or":[true]}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"$and":[{"$or":[{"doc_id":{"$in":[false]}}]}]}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"$and":[{"other":{"$eq":"id"}}]}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"$or":[{"` + domain.VectorIncarnationKey + `":{"$gt":"id"}}]}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"$or":[{"` + domain.VectorIncarnationKey + `":{"$exists":"false"}}]}}`, http.StatusBadRequest},
		{http.MethodPost, "/query", query + `"filter":{"$or":[{"` + domain.VectorIncarnationKey + `":{"$in":[0]}}]}}`, http.StatusBadRequest},
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

func TestPineconeDataPlaneRequestLimits(t *testing.T) {
	fixture := &Pinecone{index: pcstore.NewMemoryIndex(dimensions)}
	const query = `{"topK":1,"vector":[1,0,0,0,0,0,0,0]`
	for _, tc := range []struct{ name, path, body, message string }{
		{"query body cap", "/query", query + `}` + strings.Repeat(" ", 2<<20), "invalid data-plane request"},
		{"upsert body cap", "/vectors/upsert", `{"vectors":[{"id":"vector","values":[1,0,0,0,0,0,0,0]}]}` + strings.Repeat(" ", 1<<20), "invalid data-plane request"},
		{"delete body cap", "/vectors/delete", `{"ids":["vector"]}` + strings.Repeat(" ", 1<<20), "invalid data-plane request"},
		{"incarnation list cap", "/query", query + `,"filter":{"$or":[{"` + domain.VectorIncarnationKey + `":{"$in":[` + strings.Repeat(`"id",`, 10_000) + `"id"]}}]}}`, "invalid filter operand"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tc.path, strings.NewReader(tc.body))
			out := httptest.NewRecorder()
			fixture.ServeHTTP(out, req)
			require.Equal(t, http.StatusBadRequest, out.Code)
			require.Equal(t, tc.message+"\n", out.Body.String())
		})
	}
	require.Empty(t, fixture.Calls())
}

func FuzzPineconeDataPlaneQueryDispatch(f *testing.F) {
	f.Add([]byte(`{"namespace":"","vector":[1,0,0,0,0,0,0,0],"topK":1,"filter":{"doc_id":{"$in":["allowed"]}}}`))
	f.Add([]byte(`{"namespace":"other","topK":1}`))
	f.Add([]byte(`{"topK":1,"filter":{"doc_id":{"$gt":"id"}}}`))
	f.Add([]byte(`{"topK":1,"filter":{"collection":{"$eq":"archive"}}}`))
	f.Add([]byte(`{"vector":[`))
	f.Fuzz(func(t *testing.T, body []byte) {
		fixture := &Pinecone{index: pcstore.NewMemoryIndex(dimensions)}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", bytes.NewReader(body))
		out := httptest.NewRecorder()
		fixture.ServeHTTP(out, req)
		switch out.Code {
		case http.StatusOK:
			require.Len(t, fixture.Calls(), 1)
			require.Equal(t, "", fixture.Calls()[0].Namespace)
		case http.StatusBadRequest:
			require.Empty(t, fixture.Calls())
		default:
			t.Fatalf("unexpected status %d", out.Code)
		}
	})
}
