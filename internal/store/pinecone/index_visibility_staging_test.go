package pinecone

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	pc "github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stagingHeaders struct {
	receipts []uint64
	indexed  []uint64
}

func (s *stagingHeaders) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return resp, err
	}
	if v, ok := parseVisibilityLSN(resp.Header, "x-pinecone-request-lsn"); ok && req.URL.Path == "/vectors/upsert" {
		s.receipts = append(s.receipts, v)
	}
	if v, ok := parseVisibilityLSN(resp.Header, "x-pinecone-max-indexed-lsn"); ok && req.URL.Path == "/query" {
		s.indexed = append(s.indexed, v)
	}
	return resp, nil
}

func TestStagingServerlessVisibilityHeaders(t *testing.T) {
	indexName, apiKey := os.Getenv("QUARRY_PINECONE_STAGING_INDEX"), os.Getenv("PINECONE_API_KEY")
	if indexName == "" || apiKey == "" {
		t.Skip("requires QUARRY_PINECONE_STAGING_INDEX and PINECONE_API_KEY")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	sdk, err := pc.NewClient(pc.NewClientParams{ApiKey: apiKey})
	require.NoError(t, err)
	idx, err := sdk.DescribeIndex(ctx, indexName)
	require.NoError(t, err)
	plane, err := newVisibilityPlane(idx, apiKey)
	require.NoError(t, err)
	require.NotNil(t, idx.Dimension)
	require.Positive(t, *idx.Dimension)
	namespace := "quarry-visibility-" + uuid.NewString()
	conn, err := sdk.Index(pc.NewIndexConnParams{Host: idx.Host, Namespace: namespace})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(t.Context()), 15*time.Second)
		defer cleanupCancel()
		if cleanupErr := conn.DeleteAllVectorsInNamespace(cleanupCtx); cleanupErr != nil {
			t.Errorf("delete staging vectors: %v", cleanupErr)
		}
		require.NoError(t, conn.Close())
	})
	headers := &stagingHeaders{}
	plane.http.Transport = headers
	c := &Client{visibility: plane, batchSize: 1, timeout: 10 * time.Second, maxAttempts: 2}
	embedding := make([]float32, *idx.Dimension)
	embedding[0] = 1
	probe, err := c.UpsertWithVisibility(ctx, namespace, []domain.Chunk{
		{ID: "synthetic-one", DocumentID: "synthetic-document", Embedding: embedding},
		{ID: "synthetic-two", DocumentID: "synthetic-document", Embedding: embedding},
	})
	require.NoError(t, err)
	require.Len(t, headers.receipts, 2)
	require.NotNil(t, probe)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		before := len(headers.indexed)
		result, probeErr := probe(ctx, embedding)
		require.NoError(t, probeErr)
		require.Len(t, headers.indexed, before+1)
		watermark := headers.indexed[before]
		maximum := max(headers.receipts[0], headers.receipts[1])
		t.Logf("write receipts %v, query watermark %d", headers.receipts, watermark)
		if watermark >= maximum {
			require.Equal(t, domain.VisibilityWritesIndexed, result)
			break
		}
		require.Empty(t, result)
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestStagingManifestIncarnationFilter(t *testing.T) {
	indexName, apiKey := os.Getenv("QUARRY_PINECONE_STAGING_INDEX"), os.Getenv("PINECONE_API_KEY")
	if indexName == "" || apiKey == "" {
		t.Skip("requires a disposable QUARRY_PINECONE_STAGING_INDEX and PINECONE_API_KEY")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	sdk, err := pc.NewClient(pc.NewClientParams{ApiKey: apiKey})
	require.NoError(t, err)
	idx, err := sdk.DescribeIndex(ctx, indexName)
	require.NoError(t, err)
	require.NotNil(t, idx.Dimension)
	require.Positive(t, *idx.Dimension)
	conn, err := sdk.Index(pc.NewIndexConnParams{Host: idx.Host})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := &Client{defaultConn: sdkIndexConn{conn}, batchSize: 100, timeout: 10 * time.Second, maxAttempts: 1}
	driver := NewDriver(client)

	doc, claim, tenant := uuid.NewString(), uuid.NewString(), uuid.NewString()
	fixture := func(stamp string, component float32) domain.Chunk {
		chunk := domain.Chunk{
			ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(doc),
			RevisionID: uuid.NewString(), VectorIncarnation: stamp, Embedding: make([]float32, *idx.Dimension),
			Metadata: map[string]any{domain.VectorTenantKey: tenant},
		}
		chunk.Embedding[0] = component
		chunk.VectorID = domain.FormatRevisionVectorID(chunk.DocumentID, chunk.RevisionID, chunk.Index)
		return chunk
	}
	stale, active := fixture(uuid.NewString(), 1), fixture(claim, -1)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(t.Context()), 15*time.Second)
		defer cleanupCancel()
		require.NoError(t, client.Delete(cleanupCtx, "", []string{stale.VectorID, active.VectorID}))
	})
	require.NoError(t, client.Upsert(ctx, "", []domain.Chunk{stale, active}))
	filter := domain.VectorFilter{DocumentIDs: []string{doc}}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		hits, queryErr := driver.QueryFiltered(ctx, stale.Embedding, 2, filter)
		require.NoError(collect, queryErr)
		require.Len(collect, hits, 2)
		require.Equal(collect, stale.ID, hits[0].Chunk.ID)
	}, time.Minute, time.Second)

	scope, ids := memberScope(maxInValues - 1)
	scope.TenantID = tenant
	scope.Incarnations[doc] = claim
	ids = append(ids, doc)
	filter.Authorized, filter.DocumentIDs = scope, ids
	hits, used, err := driver.QueryFilteredBudgeted(ctx, stale.Embedding, 1, filter, 1)
	require.NoError(t, err)
	require.Equal(t, 1, used)
	require.Len(t, hits, 1)
	require.Equal(t, active.ID, hits[0].Chunk.ID)
}
