package pinecone

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/Victoroide/quarry-rag/internal/testutil/pineconetest"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
)

// NewTestDriver supplies a fake index connection to the real Pinecone adapter.
func NewTestDriver(query func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error)) *Driver {
	return NewTestDriverWithUpsert(query, nil)
}

// NewTestDriverWithUpsert also records writes through the real adapter.
func NewTestDriverWithUpsert(query func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error), upsert func(context.Context, []*pinecone.Vector) (uint32, error)) *Driver {
	return NewDriver(testClient(&fakeIndexConn{query: query, upsert: upsert}))
}

// NewHTTPTestDriver builds a real Pinecone Driver backed by HTTP calls to a test server.
func NewHTTPTestDriver(baseURL string, httpClient *http.Client, dims int, useLSN bool) (*Driver, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	client := &Client{
		batchSize:   100,
		timeout:     2 * time.Second,
		maxAttempts: 3,
		dims:        dims,
		defaultConn: pineconetest.NewHTTPIndexConn(*parsed, httpClient, ""),
		dial: func(namespace string) (indexConn, error) {
			return pineconetest.NewHTTPIndexConn(*parsed, httpClient, namespace), nil
		},
	}
	if useLSN {
		client.visibility = &visibilityPlane{endpoint: *parsed, apiKey: "test-key", http: *httpClient}
	}
	return NewDriver(client), nil
}
