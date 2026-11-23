package pinecone

import (
	"context"

	"github.com/pinecone-io/go-pinecone/v5/pinecone"
)

// NewTestDriver supplies a fake index connection to the real Pinecone adapter.
func NewTestDriver(query func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error)) *Driver {
	return NewDriver(testClient(&fakeIndexConn{query: query}))
}
