package pinecone

import (
	"context"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestUpsertWritesTheServerAuthoredTenantLabel(t *testing.T) {
	chunks := testChunks(2, 2, 10)
	chunks[0].Metadata = map[string]any{domain.VectorTenantKey: "tenant-a", "doc_type": "note"}
	var written []*pinecone.Vector
	conn := &fakeIndexConn{upsert: func(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
		written = vectors
		return uint32(len(vectors)), nil // #nosec G115 -- The batch length is bounded above.
	}}
	require.NoError(t, testClient(conn).Upsert(t.Context(), "", chunks))
	require.Len(t, written, 2)
	require.Equal(t, "tenant-a", written[0].Metadata.AsMap()[domain.VectorTenantKey])
	require.NotContains(t, written[1].Metadata.AsMap(), domain.VectorTenantKey, "service-mode vectors stay unlabeled")
}

func TestQueryReturnsTheTenantLabel(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"doc_id": "doc", "chunk_index": 0.0, "text": "t", "token_count": 1.0, domain.VectorTenantKey: "tenant-a"})
	require.NoError(t, err)
	conn := &fakeIndexConn{query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{{Vector: &pinecone.Vector{Id: "chunk", Metadata: metadata}, Score: 1}}}, nil
	}}
	results, err := testClient(conn).Query(t.Context(), "", make([]float32, 1024), 1, "")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "tenant-a", results[0].Chunk.Metadata[domain.VectorTenantKey])
}

func TestTaggedVectorID(t *testing.T) {
	id, err := TaggedVectorID("doc-1", "rev-1", "att-1", "chunk-1")
	require.NoError(t, err)
	require.Equal(t, "doc-1/rev-1/att-1/chunk-1", id)

	id, err = TaggedVectorID("doc-1", "rev-1", "", "chunk-1")
	require.NoError(t, err)
	require.Equal(t, "doc-1/rev-1/chunk-1", id)

	for _, args := range [][4]string{
		{"", "rev", "att", "chunk"},
		{"doc/slash", "rev", "att", "chunk"},
		{"doc", "rev/slash", "att", "chunk"},
		{"doc", "rev", "att/slash", "chunk"},
		{"doc", "rev", "att", "chunk/slash"},
	} {
		_, err = TaggedVectorID(args[0], args[1], args[2], args[3])
		require.Error(t, err)
	}
	_, err = TaggedVectorID("doc", "rev", "att", string(make([]byte, 510)))
	require.Error(t, err)
}
