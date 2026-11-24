package pinecone

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBudgetedQueryCountsPartitionsAndAttempts(t *testing.T) {
	ids := make([]string, maxInValues+1)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	filter := domain.VectorFilter{Collection: "archive", DocType: "guide", DocumentIDs: ids}
	for _, tt := range []struct {
		name     string
		budget   int
		want     []string
		exhausts bool
	}{
		{name: "skip whole search", budget: 1, exhausts: true},
		{name: "preserve later partition", budget: 2, want: []string{"0"}, exhausts: true},
		{name: "retry then search remaining partition", budget: 3, want: []string{"0", "0", fmt.Sprint(maxInValues)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls []string
				client := testClient(&fakeIndexConn{query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
					require.Equal(t, uint32(4), req.TopK)
					fields := req.MetadataFilter.GetFields()
					require.Equal(t, "archive", fields["collection"].GetStructValue().GetFields()["$eq"].GetStringValue())
					require.Equal(t, "guide", fields["doc_type"].GetStructValue().GetFields()["$eq"].GetStringValue())
					group := fields["doc_id"].GetStructValue().GetFields()["$in"].GetListValue().GetValues()
					start := group[0].GetStringValue()
					calls = append(calls, start)
					if len(calls) == 1 {
						return nil, status.Error(codes.Unavailable, "transient")
					}
					return &pinecone.QueryVectorsResponse{}, nil
				}})
				_, used, err := NewDriver(client).QueryFilteredBudgeted(t.Context(), []float32{1}, 4, filter, tt.budget)
				if tt.exhausts {
					require.ErrorIs(t, err, domain.ErrVectorQueryBudget)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tt.want, calls)
				require.Equal(t, len(calls), used)
			})
		})
	}
}

func TestBudgetedQueryPreventsTransparentTransportRetry(t *testing.T) {
	calls := 0
	guard := queryAttemptCredentials{}
	client := testClient(&fakeIndexConn{query: func(ctx context.Context, _ *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		calls++
		_, err := guard.GetRequestMetadata(ctx)
		require.NoError(t, err)
		_, err = guard.GetRequestMetadata(ctx)
		require.Equal(t, codes.Unavailable, status.Code(err))
		if calls == 1 {
			return nil, err
		}
		return &pinecone.QueryVectorsResponse{}, nil
	}})
	_, used, err := NewDriver(client).QueryFilteredBudgeted(t.Context(), []float32{1}, 4, domain.VectorFilter{}, 2)
	require.NoError(t, err)
	require.Equal(t, 2, used)
	require.Equal(t, used, calls)
	_, err = guard.GetRequestMetadata(t.Context())
	require.NoError(t, err)
	_, err = guard.GetRequestMetadata(t.Context())
	require.NoError(t, err)
}
