package pinecone

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func indexedClient(conn *fakeIndexConn) *Client {
	client := testClient(conn)
	client.ownershipFilters = ownershipFilters{document: true, revision: true, attempt: true}
	client.maxAttempts = 5
	return client
}

func TestObservedDeleteReportsAnAttemptThatALaterSuccessHides(t *testing.T) {
	var calls int
	conn := &fakeIndexConn{filterDelete: func(ctx context.Context, _ *structpb.Struct) error {
		calls++
		if calls == 1 {
			// The first attempt ran out of time after it may have reached the provider.
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		}
		return nil
	}}
	client := indexedClient(conn)
	client.timeout = 20 * time.Millisecond
	ctx, outcome := domain.ObserveVectorOutcome(t.Context())

	require.NoError(t, client.DeleteByDocID(ctx, "doc"))
	require.Equal(t, 2, calls, "the automatic retry stays enabled")
	require.True(t, outcome.Uncertain(), "the timed-out attempt must keep the identity held")
}

func TestObservedDeleteIgnoresAnAttemptRejectedBeforeAnySend(t *testing.T) {
	var calls int
	conn := &fakeIndexConn{filterDelete: func(context.Context, *structpb.Struct) error {
		calls++
		if calls == 1 {
			return domain.RejectedVectorMutation(status.Error(codes.Unavailable, "refused before any stream"))
		}
		return nil
	}}
	client := indexedClient(conn)
	ctx, outcome := domain.ObserveVectorOutcome(t.Context())

	require.NoError(t, client.DeleteByDocID(ctx, "doc"))
	require.Equal(t, 2, calls)
	require.False(t, outcome.Uncertain())
}

func TestObservedDeleteKeepsUncertaintyWhenEveryAttemptFails(t *testing.T) {
	conn := &fakeIndexConn{filterDelete: func(context.Context, *structpb.Struct) error {
		return status.Error(codes.Unavailable, "connection reset after the request was written")
	}}
	client := indexedClient(conn)
	client.maxAttempts = 2
	ctx, outcome := domain.ObserveVectorOutcome(t.Context())

	require.Error(t, client.DeleteByDocID(ctx, "doc"))
	require.True(t, outcome.Uncertain())
}

func TestUnobservedDeleteIsUnchanged(t *testing.T) {
	var calls int
	conn := &fakeIndexConn{filterDelete: func(context.Context, *structpb.Struct) error {
		calls++
		if calls == 1 {
			return status.Error(codes.Unavailable, "temporary")
		}
		return nil
	}}
	require.NoError(t, indexedClient(conn).DeleteByDocID(t.Context(), "doc"))
	require.Equal(t, 2, calls)
}

func TestLegacyDeleteTargetsOnlyUntaggedVectorsOfTheDocument(t *testing.T) {
	var filters []map[string]any
	conn := &fakeIndexConn{filterDelete: func(_ context.Context, filter *structpb.Struct) error {
		encoded, err := json.Marshal(filter.AsMap())
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		filters = append(filters, decoded)
		return nil
	}}
	require.NoError(t, indexedClient(conn).DeleteByDocID(t.Context(), "doc"))
	require.Equal(t, []map[string]any{{
		"doc_id":                 map[string]any{"$eq": "doc"},
		domain.VectorRevisionKey: map[string]any{"$exists": false},
		domain.VectorAttemptKey:  map[string]any{"$exists": false},
	}}, filters, "a delayed legacy filter must not match a tagged replacement")
}

func TestRevisionDeleteTargetsOneIncarnation(t *testing.T) {
	var filter map[string]any
	conn := &fakeIndexConn{filterDelete: func(_ context.Context, value *structpb.Struct) error {
		filter = value.AsMap()
		return nil
	}}
	require.NoError(t, indexedClient(conn).DeleteByRevision(t.Context(), "doc", "revision"))
	require.Equal(t, map[string]any{
		"doc_id":                 map[string]any{"$eq": "doc"},
		domain.VectorRevisionKey: map[string]any{"$eq": "revision"},
	}, filter)
}

func TestObservedDeleteTreatsADefiniteRejectionAsCertain(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.Unimplemented, codes.FailedPrecondition} {
		t.Run(code.String()+" falls back", func(t *testing.T) {
			var deleted []string
			conn := &fakeIndexConn{
				filterDelete: func(context.Context, *structpb.Struct) error {
					return status.Error(code, "filter deletion is not supported by this index")
				},
				query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
					if len(deleted) > 0 {
						return &pinecone.QueryVectorsResponse{}, nil
					}
					return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{legacyMatch("first")}}, nil
				},
				deleteIDs: func(_ context.Context, ids []string) error {
					deleted = append(deleted, ids...)
					return nil
				},
			}
			ctx, outcome := domain.ObserveVectorOutcome(t.Context())

			require.NoError(t, indexedClient(conn).DeleteByDocID(ctx, "doc"))
			require.Equal(t, []string{"first"}, deleted, "the verified fallback still deletes")
			require.False(t, outcome.Uncertain(), "an answer that proves the filter did not run holds nothing")
		})
	}
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied} {
		t.Run(code.String()+" fails", func(t *testing.T) {
			calls := 0
			conn := &fakeIndexConn{filterDelete: func(context.Context, *structpb.Struct) error {
				calls++
				return status.Error(code, "credentials rejected")
			}}
			ctx, outcome := domain.ObserveVectorOutcome(t.Context())

			require.Equal(t, code, status.Code(indexedClient(conn).DeleteByDocID(ctx, "doc")))
			require.Equal(t, 1, calls, "a rejected credential is not retried")
			require.False(t, outcome.Uncertain(), "a rotated key that rejected the call cannot have deleted anything")
		})
	}
}

func TestObservedDeleteKeepsADeadlineAfterSendUncertain(t *testing.T) {
	conn := &fakeIndexConn{filterDelete: func(context.Context, *structpb.Struct) error {
		return status.Error(codes.DeadlineExceeded, "no answer after the request was written")
	}}
	client := indexedClient(conn)
	client.maxAttempts = 1
	ctx, outcome := domain.ObserveVectorOutcome(t.Context())

	require.Error(t, client.DeleteByDocID(ctx, "doc"))
	require.True(t, outcome.Uncertain())
}

func TestObservedRejectionAfterAnUncertainAttemptKeepsTheHold(t *testing.T) {
	var calls int
	conn := &fakeIndexConn{filterDelete: func(context.Context, *structpb.Struct) error {
		calls++
		if calls == 1 {
			return status.Error(codes.Unavailable, "reset after the request was written")
		}
		return status.Error(codes.PermissionDenied, "key rotated")
	}}
	ctx, outcome := domain.ObserveVectorOutcome(t.Context())

	require.Error(t, indexedClient(conn).DeleteByDocID(ctx, "doc"))
	require.True(t, outcome.Uncertain(), "a later definite rejection must not erase an earlier uncertain attempt")
}
