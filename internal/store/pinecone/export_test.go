package pinecone

import (
	"context"

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
