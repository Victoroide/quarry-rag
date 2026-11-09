package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
)

type collectionFakeIndex struct {
	requests []*pinecone.UpdateVectorsByMetadataRequest
	failAt   int
}

func (f *collectionFakeIndex) UpdateVectorsByMetadata(_ context.Context, req *pinecone.UpdateVectorsByMetadataRequest) (*pinecone.UpdateVectorsByMetadataResponse, error) {
	f.requests = append(f.requests, req)
	if f.failAt == len(f.requests) {
		return nil, errors.New("update failed")
	}
	return &pinecone.UpdateVectorsByMetadataResponse{MatchedRecords: 2}, nil
}

func TestCollectionBackfillDryRunAndApply(t *testing.T) {
	docs := []collectionDocument{{ID: storedID, Collection: "archive"}, {ID: missingID, Collection: "private"}}
	for _, apply := range []bool{false, true} {
		idx := &collectionFakeIndex{}
		result, err := backfillCollections(t.Context(), idx, docs, options{Apply: apply, Limit: 1})
		require.NoError(t, err)
		require.Equal(t, docs[:1], result.Documents)
		if apply {
			require.Equal(t, 1, result.Updated)
			require.Len(t, idx.requests, 1)
			require.Equal(t, map[string]any{"doc_id": map[string]any{"$eq": storedID}}, idx.requests[0].Filter.AsMap())
			require.Equal(t, map[string]any{"collection": "archive"}, idx.requests[0].Metadata.AsMap())
			require.Nil(t, idx.requests[0].DryRun)
		} else {
			require.Empty(t, idx.requests)
			require.Zero(t, result.Updated)
		}
	}
	result, err := backfillCollections(t.Context(), nil, docs, options{Limit: 10})
	require.NoError(t, err)
	require.Equal(t, docs, result.Documents)
}

func TestCollectionBackfillStopsOnFirstError(t *testing.T) {
	docs := []collectionDocument{{ID: storedID, Collection: "archive"}, {ID: missingID, Collection: "private"}, {ID: "00000000-0000-4000-8000-000000000003", Collection: "third"}}
	idx := &collectionFakeIndex{failAt: 2}
	result, err := backfillCollections(t.Context(), idx, docs, options{Apply: true, Limit: 3})
	require.ErrorContains(t, err, "update failed")
	require.ErrorContains(t, err, missingID)
	require.ErrorContains(t, err, fmt.Sprintf("-after=%q", storedID))
	require.Len(t, idx.requests, 2)
	require.Equal(t, 1, result.Updated)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	idx = &collectionFakeIndex{}
	_, err = backfillCollections(ctx, idx, docs, options{Apply: true, Limit: 3})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, `-after=""`)
	_, err = backfillCollections(ctx, idx, docs[1:], options{Apply: true, Limit: 3, After: storedID})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, fmt.Sprintf("-after=%q", storedID))
	require.Empty(t, idx.requests)
}

func TestCollectionBackfillFlag(t *testing.T) {
	opts, err := parseOptions([]string{"-backfill-collections", "-apply", "-limit=2"}, &bytes.Buffer{})
	require.NoError(t, err)
	require.True(t, opts.BackfillCollections)
	require.True(t, opts.Apply)
	require.Equal(t, 2, opts.Limit)
	const cursor = "a0b1c2d3-e4f5-4678-9abc-def012345678"
	opts, err = parseOptions([]string{"-backfill-collections", "-after=" + strings.ToUpper(cursor)}, &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, cursor, opts.After)
	_, err = parseOptions([]string{"-after=" + storedID}, &bytes.Buffer{})
	require.Error(t, err)
	_, err = parseOptions([]string{"-backfill-collections", "-after=invalid"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "document UUID")
}
