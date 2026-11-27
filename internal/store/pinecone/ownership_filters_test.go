package pinecone

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAttemptWithoutRevisionIsAmbiguous(t *testing.T) {
	t.Run("verified delete", func(t *testing.T) {
		legacy := legacyMatch("legacy")
		tagged := legacyMatch("doc/rev/tagged")
		tagged.Vector.Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue("rev")
		bad := legacyMatch("logical-chunk")
		bad.Vector.Metadata.Fields[domain.VectorAttemptKey] = structpb.NewStringValue("tagged-owner")
		matches := []*pinecone.ScoredVector{legacy, tagged, bad}
		var deleted []string
		index := &fakeIndexConn{
			query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
				return &pinecone.QueryVectorsResponse{Matches: matches}, nil
			},
			deleteIDs: func(_ context.Context, ids []string) error {
				deleted = append(deleted, ids...)
				matches = []*pinecone.ScoredVector{tagged}
				return nil
			},
		}
		client := testClient(index)
		ctx := domain.TrackVectorMutation(t.Context())
		require.Error(t, client.DeleteByDocID(ctx, "doc"))
		require.Empty(t, deleted)
		require.Equal(t, []*pinecone.ScoredVector{legacy, tagged, bad}, matches)

		matches = []*pinecone.ScoredVector{legacy, tagged}
		require.NoError(t, client.DeleteByDocID(ctx, "doc"))
		require.Equal(t, []string{"legacy"}, deleted)
		require.Equal(t, []*pinecone.ScoredVector{tagged}, matches)
	})
	t.Run("upsert", func(t *testing.T) {
		upserts := 0
		index := &fakeIndexConn{upsert: func(context.Context, []*pinecone.Vector) (uint32, error) {
			upserts++
			return 1, nil
		}}
		chunks := testChunks(1, 2, 1)
		chunks[0].Metadata = map[string]any{domain.VectorAttemptKey: "tagged-owner"}
		require.Error(t, testClient(index).Upsert(t.Context(), "", chunks))
		require.Zero(t, upserts)
	})
}

type describeNamespaceFunc func(context.Context, string) (*pinecone.NamespaceDescription, error)

func (f describeNamespaceFunc) DescribeNamespace(ctx context.Context, name string) (*pinecone.NamespaceDescription, error) {
	return f(ctx, name)
}

func TestOwnershipFiltersUseEffectiveNamespace(t *testing.T) {
	indexSchema := &pinecone.MetadataSchema{Fields: map[string]pinecone.MetadataSchemaField{
		"doc_id": {Filterable: true}, domain.VectorRevisionKey: {Filterable: true},
	}}
	override := &pinecone.MetadataSchema{Fields: map[string]pinecone.MetadataSchemaField{
		domain.VectorRevisionKey: {Filterable: true}, domain.VectorAttemptKey: {Filterable: true},
	}}
	for _, tc := range []struct {
		name string
		spec *pinecone.IndexSpec
		ns   *pinecone.NamespaceDescription
		err  error
		want ownershipFilters
	}{
		{name: "index only", ns: &pinecone.NamespaceDescription{}, want: ownershipFilters{document: true, revision: true}},
		{name: "override", ns: &pinecone.NamespaceDescription{Schema: override}, want: ownershipFilters{revision: true, attempt: true}},
		{name: "new namespace", err: status.Error(codes.NotFound, "namespace missing"), want: ownershipFilters{document: true, revision: true}},
		{name: "unverified namespace", err: status.Error(codes.Unavailable, "namespace unavailable")},
		{name: "denied discovery", err: status.Error(codes.PermissionDenied, "namespace forbidden")},
		{name: "empty description"},
		{name: "empty namespace schema", ns: &pinecone.NamespaceDescription{Schema: &pinecone.MetadataSchema{}}},
		{
			name: "incomplete namespace fields",
			ns: &pinecone.NamespaceDescription{Schema: &pinecone.MetadataSchema{Fields: map[string]pinecone.MetadataSchemaField{
				"doc_id": {}, domain.VectorAttemptKey: {Filterable: true},
			}}},
			want: ownershipFilters{attempt: true},
		},
		{
			name: "all metadata", spec: &pinecone.IndexSpec{Serverless: &pinecone.ServerlessSpec{}},
			ns: &pinecone.NamespaceDescription{}, want: ownershipFilters{document: true, revision: true, attempt: true},
		},
		{
			name: "pod default", spec: &pinecone.IndexSpec{Pod: &pinecone.PodSpec{}},
			want: ownershipFilters{document: true, revision: true, attempt: true},
		},
		{
			name: "pod list", spec: &pinecone.IndexSpec{Pod: &pinecone.PodSpec{
				MetadataConfig: &pinecone.PodSpecMetadataConfig{Indexed: &[]string{"doc_id", domain.VectorAttemptKey}},
			}},
			want: ownershipFilters{document: true, attempt: true},
		},
		{
			name: "empty pod list", spec: &pinecone.IndexSpec{Pod: &pinecone.PodSpec{
				MetadataConfig: &pinecone.PodSpecMetadataConfig{Indexed: &[]string{}},
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := tc.spec
			if spec == nil {
				spec = &pinecone.IndexSpec{Serverless: &pinecone.ServerlessSpec{Schema: indexSchema}}
			}
			idx := &pinecone.Index{Spec: spec}
			describe := describeNamespaceFunc(func(_ context.Context, name string) (*pinecone.NamespaceDescription, error) {
				require.Nil(t, spec.Pod, "pods must use their indexed list")
				require.Equal(t, "__default__", name)
				return tc.ns, tc.err
			})
			filters, err := indexedOwnershipFields(t.Context(), idx, describe)
			switch {
			case spec.Pod != nil:
				require.NoError(t, err)
			case tc.err != nil && status.Code(tc.err) != codes.NotFound:
				require.Equal(t, status.Code(tc.err), status.Code(err))
			case tc.ns == nil && tc.err == nil:
				require.ErrorContains(t, err, "missing default namespace description")
			default:
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, filters)
		})
	}
}

func TestCleanupRetriesUnavailableNamespaceDescription(t *testing.T) {
	idx := &pinecone.Index{Spec: &pinecone.IndexSpec{Serverless: &pinecone.ServerlessSpec{
		Schema: &pinecone.MetadataSchema{Fields: map[string]pinecone.MetadataSchemaField{"doc_id": {Filterable: true}}},
	}}}
	descriptions, queries := 0, 0
	describe := describeNamespaceFunc(func(ctx context.Context, name string) (*pinecone.NamespaceDescription, error) {
		require.Equal(t, "__default__", name)
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), time.Second)
		descriptions++
		if descriptions <= 2 {
			return nil, status.Error(codes.Unavailable, "temporary namespace outage")
		}
		return &pinecone.NamespaceDescription{}, nil
	})
	index := &fakeIndexConn{query: func(_ context.Context, q *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		queries++
		require.Equal(t, map[string]any{"doc_id": map[string]any{"$eq": "doc"}}, q.MetadataFilter.AsMap())
		return &pinecone.QueryVectorsResponse{}, nil
	}}
	client := testClient(index)
	client.discoverOwnership = func(ctx context.Context) (ownershipFilters, error) {
		return indexedOwnershipFields(ctx, idx, describe)
	}
	require.Equal(t, codes.Unavailable, status.Code(client.DeleteByDocID(t.Context(), "doc")))
	require.Zero(t, queries)
	require.Equal(t, codes.Unavailable, status.Code(client.DeleteByDocID(t.Context(), "doc")))
	require.Zero(t, queries)
	require.NoError(t, client.DeleteByDocID(t.Context(), "doc"))
	require.NoError(t, client.DeleteByDocID(t.Context(), "doc"))
	require.Equal(t, 3, descriptions)
	require.Equal(t, 2, queries)
}

func TestTaggedCleanupWithoutDocumentFilterUsesPrefix(t *testing.T) {
	index := &fakeIndexConn{
		filterDelete: func(context.Context, *structpb.Struct) error { t.Fatal("unverified bulk filter"); return nil },
		query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			t.Fatal("unverified document query")
			return nil, nil
		},
	}
	client := testClient(index)
	client.ownershipFilters = ownershipFilters{revision: true, attempt: true}
	calls := 0
	client.defaultConn = listedIndexConn{index, func(_ context.Context, request *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		require.Equal(t, "doc/rev/attempt/", *request.Prefix)
		calls++
		if calls == 1 {
			return &pinecone.ListVectorsResponse{}, nil
		}
		return nil, status.Error(codes.Unimplemented, "listing unsupported")
	}}
	ctx := domain.TrackVectorMutation(t.Context())
	require.NoError(t, client.DeleteByAttempt(ctx, "doc", "rev", "attempt"))
	require.ErrorContains(t, client.DeleteByAttempt(ctx, "doc", "rev", "attempt"), "cannot verify cleanup ownership")
	require.Equal(t, 2, calls)
}

func TestCleanupRejectsUnsearchableDocumentKey(t *testing.T) {
	for _, indexed := range [][]string{{"doc_type"}, {domain.VectorRevisionKey}} {
		t.Run(indexed[0], func(t *testing.T) {
			dims := int32(1024)
			schema := &pinecone.Index{Dimension: &dims, Metric: pinecone.Cosine, Status: &pinecone.IndexStatus{Ready: true}, Spec: &pinecone.IndexSpec{Pod: &pinecone.PodSpec{MetadataConfig: &pinecone.PodSpecMetadataConfig{Indexed: &indexed}}}}
			require.NoError(t, validateIndex(schema, &config.Config{EmbeddingDimensions: 1024}))
			base := &fakeIndexConn{
				filterDelete: func(_ context.Context, f *structpb.Struct) error { require.Contains(t, f.Fields, "doc_id"); return nil },
				query: func(_ context.Context, q *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
					if q.MetadataFilter != nil && q.MetadataFilter.Fields["doc_id"] != nil {
						return &pinecone.QueryVectorsResponse{}, nil
					}
					return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{legacyMatch("legacy")}}, nil
				},
			}
			client := testClient(base)
			filters, err := indexedOwnershipFields(t.Context(), schema, nil)
			require.NoError(t, err)
			client.ownershipFilters = filters
			err = client.DeleteByDocID(domain.TrackVectorMutation(t.Context()), "doc")
			control, controlErr := base.QueryByVectorValues(t.Context(), &pinecone.QueryByVectorValuesRequest{TopK: 1, IncludeMetadata: true})
			require.NoError(t, controlErr)
			require.Len(t, control.Matches, 1)
			t.Logf("declared indexed fields=%v, cleanup=%v, unfiltered verification still sees=%s", indexed, err, control.Matches[0].Vector.Id)
			require.Error(t, err)
		})
	}
}

func TestLegacyVerificationOnAttemptOnlyOwnershipIndex(t *testing.T) {
	neighbors := make([]*pinecone.ScoredVector, cleanupPageSize)
	for i := range neighbors {
		match := legacyMatch(fmt.Sprintf("doc/rev/try/c%d", i))
		match.Vector.Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue("rev")
		match.Vector.Metadata.Fields[domain.VectorAttemptKey] = structpb.NewStringValue("try")
		neighbors[i] = match
	}
	legacy := []*pinecone.ScoredVector{legacyMatch("current")}
	deletes := 0
	index := &fakeIndexConn{
		filterDelete: func(context.Context, *structpb.Struct) error {
			t.Fatal("legacy filter deletion must exclude both ownership fields")
			return nil
		},
		query: func(_ context.Context, q *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			require.True(t, q.IncludeMetadata)
			fields := q.MetadataFilter.AsMap()
			require.Equal(t, map[string]any{"$eq": "doc"}, fields["doc_id"])
			require.NotContains(t, fields, domain.VectorRevisionKey)
			if _, hasAttempt := fields[domain.VectorAttemptKey]; !hasAttempt {
				return &pinecone.QueryVectorsResponse{Matches: neighbors}, nil
			}
			require.Equal(t, map[string]any{"$exists": false}, fields[domain.VectorAttemptKey])
			return &pinecone.QueryVectorsResponse{Matches: legacy}, nil
		},
		deleteIDs: func(_ context.Context, ids []string) error {
			require.Equal(t, []string{"current"}, ids)
			deletes++
			legacy = nil
			return nil
		},
	}
	client := testClient(index)
	client.ownershipFilters = ownershipFilters{document: true, attempt: true}
	ctx := domain.TrackVectorMutation(t.Context())
	require.NoError(t, client.DeleteByDocID(ctx, "doc"))
	require.NoError(t, client.DeleteByDocID(ctx, "doc"))
	require.Empty(t, legacy)
	require.Equal(t, 1, deletes)

	client.ownershipFilters.attempt = false
	require.ErrorContains(t, client.DeleteByDocID(ctx, "doc"), "full page")
	require.Equal(t, 1, deletes)
}

func TestLegacyVerificationPreservesExactOwnershipPredicates(t *testing.T) {
	const doc = "doc"
	var legacy = true
	filters, queries, broadened := 0, 0, 0
	neighbors := make([]*pinecone.ScoredVector, cleanupPageSize+3)
	for i := range neighbors {
		m := legacyMatch(fmt.Sprintf("doc/neighbor/chunk%d", i))
		m.Vector.Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue("neighbor")
		neighbors[i] = m
	}
	index := &fakeIndexConn{
		filterDelete: func(_ context.Context, f *structpb.Struct) error {
			filters++
			require.Equal(t, map[string]any{"$exists": false}, f.AsMap()[domain.VectorRevisionKey])
			require.Equal(t, map[string]any{"$exists": false}, f.AsMap()[domain.VectorAttemptKey])
			legacy = false
			return nil
		},
		query: func(_ context.Context, q *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			queries++
			_, revision := q.MetadataFilter.Fields[domain.VectorRevisionKey]
			_, attempt := q.MetadataFilter.Fields[domain.VectorAttemptKey]
			exact := revision && attempt
			var matches []*pinecone.ScoredVector
			if !exact {
				broadened++
				matches = append(matches, neighbors...)
			}
			if legacy {
				matches = append(matches, legacyMatch("legacy"))
			}
			return &pinecone.QueryVectorsResponse{Matches: matches[:min(len(matches), int(q.TopK))]}, nil
		},
		deleteIDs: func(_ context.Context, _ []string) error { t.Fatal("delete touched tagged neighbor"); return nil },
	}
	c := testClient(index)
	c.ownershipFilters = ownershipFilters{document: true, revision: true, attempt: true}
	err1 := c.DeleteByDocID(domain.TrackVectorMutation(t.Context()), doc)
	err2 := c.DeleteByDocID(domain.TrackVectorMutation(t.Context()), doc)
	t.Logf("acknowledged filter calls=%d, legacy remains=%v, tagged neighbors=%d, broad verification queries=%d/%d, retry=%v", filters, legacy, len(neighbors), broadened, queries, err2)
	predicate, err := structpb.NewStruct(map[string]any{"doc_id": map[string]any{"$eq": doc}, domain.VectorRevisionKey: map[string]any{"$exists": false}, domain.VectorAttemptKey: map[string]any{"$exists": false}})
	require.NoError(t, err)
	control, err := index.QueryByVectorValues(t.Context(), &pinecone.QueryByVectorValuesRequest{TopK: cleanupPageSize, MetadataFilter: predicate})
	require.NoError(t, err)
	require.Empty(t, control.Matches)
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, 2, filters)
	require.Equal(t, 3, queries)
	require.Zero(t, broadened)
	require.False(t, legacy)
}

func TestLegacyVerificationOnRevisionOnlyOwnershipIndex(t *testing.T) {
	neighbors := make([]*pinecone.ScoredVector, cleanupPageSize)
	for i := range neighbors {
		match := legacyMatch(fmt.Sprintf("doc/rev/chunk%d", i))
		match.Vector.Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue("rev")
		neighbors[i] = match
	}
	legacy := []*pinecone.ScoredVector{legacyMatch("legacy")}
	var deleted []string
	index := &fakeIndexConn{
		filterDelete: func(context.Context, *structpb.Struct) error {
			t.Fatal("legacy filter deletion must exclude both ownership fields")
			return nil
		},
		query: func(_ context.Context, q *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			fields := q.MetadataFilter.AsMap()
			require.Equal(t, map[string]any{"$eq": "doc"}, fields["doc_id"])
			require.NotContains(t, fields, domain.VectorAttemptKey)
			if _, hasRevision := fields[domain.VectorRevisionKey]; !hasRevision {
				return &pinecone.QueryVectorsResponse{Matches: neighbors}, nil
			}
			require.Equal(t, map[string]any{"$exists": false}, fields[domain.VectorRevisionKey])
			return &pinecone.QueryVectorsResponse{Matches: legacy}, nil
		},
		deleteIDs: func(_ context.Context, ids []string) error {
			deleted = append(deleted, ids...)
			legacy = nil
			return nil
		},
	}
	client := testClient(index)
	client.ownershipFilters = ownershipFilters{document: true, revision: true}
	ctx := domain.TrackVectorMutation(t.Context())
	require.NoError(t, client.DeleteByDocID(ctx, "doc"))
	require.Equal(t, []string{"legacy"}, deleted)
	require.Empty(t, legacy)

	ambiguous := legacyMatch("attempt-without-revision")
	ambiguous.Vector.Metadata.Fields[domain.VectorAttemptKey] = structpb.NewStringValue("owner")
	legacy = []*pinecone.ScoredVector{ambiguous}
	require.ErrorContains(t, client.DeleteByDocID(ctx, "doc"), "cannot verify untagged vector identity")
	require.Equal(t, []string{"legacy"}, deleted)
	require.Equal(t, []*pinecone.ScoredVector{ambiguous}, legacy)
}

func TestAttemptVerificationUsesExactAttemptPredicate(t *testing.T) {
	const doc, rev = "doc", "rev"
	old := make([]*pinecone.ScoredVector, cleanupPageSize)
	for i := range old {
		m := legacyMatch(fmt.Sprintf("doc/rev/old/c%d", i))
		m.Vector.Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue(rev)
		m.Vector.Metadata.Fields[domain.VectorAttemptKey] = structpb.NewStringValue("old")
		old[i] = m
	}
	var filtered []string
	var verified []string
	attemptPredicate := func(filter *structpb.Struct) string {
		fields := filter.AsMap()
		require.Equal(t, map[string]any{"$eq": doc}, fields["doc_id"])
		require.Equal(t, map[string]any{"$eq": rev}, fields[domain.VectorRevisionKey])
		return filter.Fields[domain.VectorAttemptKey].GetStructValue().GetFields()["$eq"].GetStringValue()
	}
	base := &fakeIndexConn{
		filterDelete: func(_ context.Context, filter *structpb.Struct) error {
			filtered = append(filtered, attemptPredicate(filter))
			return nil
		},
		query: func(_ context.Context, q *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			if attempt := attemptPredicate(q.MetadataFilter); attempt != "" {
				verified = append(verified, attempt)
				return &pinecone.QueryVectorsResponse{}, nil
			}
			return &pinecone.QueryVectorsResponse{Matches: old[:min(len(old), int(q.TopK))]}, nil
		},
		deleteIDs: func(context.Context, []string) error { t.Fatal("delete touched older attempt"); return nil },
	}
	c := testClient(base)
	c.ownershipFilters = ownershipFilters{document: true, revision: true, attempt: true}
	c.defaultConn = listedIndexConn{base, func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		return nil, status.Error(codes.Unimplemented, "listing unsupported")
	}}
	ctx := domain.TrackVectorMutation(t.Context())
	attempts := []string{"unused1", "unused2"}
	for _, attempt := range attempts {
		require.NoError(t, c.DeleteByAttempt(ctx, doc, rev, attempt))
	}
	require.Equal(t, attempts, filtered)
	require.Equal(t, attempts, verified)

	c.ownershipFilters.attempt = false
	require.ErrorContains(t, c.DeleteByAttempt(ctx, doc, rev, "unused3"), "full page")
	require.Equal(t, attempts, filtered)
	require.Equal(t, attempts, verified)
}
