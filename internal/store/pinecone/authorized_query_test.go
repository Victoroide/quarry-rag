package pinecone

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const scopeTenant = "00000000-0000-4000-8000-0000000000a0"

func memberID(i int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", i+1)
}

func memberIncarnation(i int) string {
	return fmt.Sprintf("10000000-0000-4000-8000-%012x", i+1)
}

func memberScope(count int) (scope *domain.AuthorizedScope, ids []string) {
	scope = &domain.AuthorizedScope{TenantID: scopeTenant, PrincipalID: "principal", Incarnations: make(map[string]string, count)}
	ids = make([]string, count)
	for i := range count {
		ids[i] = memberID(i)
		scope.Incarnations[ids[i]] = memberIncarnation(i)
	}
	return scope, ids
}

func hostedHit(t *testing.T, doc, incarnation, tenant, chunk string) *pinecone.ScoredVector {
	t.Helper()
	fields := map[string]any{"doc_id": doc, "text": "evidence " + chunk, domain.VectorTenantKey: tenant, domain.VectorRevisionKey: incarnation}
	id, err := TaggedVectorID(doc, incarnation, "", chunk)
	require.NoError(t, err)
	md, err := structpb.NewStruct(fields)
	require.NoError(t, err)
	return &pinecone.ScoredVector{Vector: &pinecone.Vector{Id: id, Metadata: md}, Score: .5}
}

func listValues(t *testing.T, filter *structpb.Struct, key string) []string {
	t.Helper()
	var out []string
	for _, v := range filter.GetFields()[key].GetStructValue().GetFields()["$in"].GetListValue().GetValues() {
		out = append(out, v.GetStringValue())
	}
	return out
}

func TestAuthorizedQuerySendsTenantAndIncarnationsInEveryRequest(t *testing.T) {
	scope, ids := memberScope(25000)
	var requests int
	conn := &fakeIndexConn{query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		requests++
		fields := make(map[string]*structpb.Value)
		clauses := req.MetadataFilter.GetFields()["$and"].GetListValue().GetValues()
		require.Len(t, clauses, 5)
		for _, clause := range clauses {
			for field, value := range clause.GetStructValue().GetFields() {
				fields[field] = value
			}
		}
		require.Equal(t, scopeTenant, fields[domain.VectorTenantKey].GetStructValue().GetFields()["$eq"].GetStringValue())
		identity := fields["$or"].GetListValue().GetValues()
		require.Len(t, identity, 2)
		manifest := identity[1].GetStructValue().GetFields()["$and"].GetListValue().GetValues()
		require.Len(t, manifest, 3)
		require.Equal(t, map[string]any{"revision_id": map[string]any{"$exists": true}}, manifest[0].GetStructValue().AsMap())
		require.Equal(t, map[string]any{domain.VectorRevisionKey: map[string]any{"$exists": false}}, manifest[1].GetStructValue().AsMap())
		stamps := manifest[2].GetStructValue().GetFields()["$or"].GetListValue().GetValues()
		require.Len(t, stamps, 2)
		require.Equal(t, map[string]any{domain.VectorIncarnationKey: map[string]any{"$exists": false}}, stamps[0].GetStructValue().AsMap())
		docs := listValues(t, &structpb.Struct{Fields: fields}, "doc_id")
		incarnations := listValues(t, identity[0].GetStructValue(), domain.VectorRevisionKey)
		require.LessOrEqual(t, len(docs), maxInValues)
		require.Equal(t, incarnations, listValues(t, stamps[1].GetStructValue(), domain.VectorIncarnationKey))
		// Leave headroom for the embedding and request framing on the gRPC transport.
		require.Less(t, proto.Size(req.MetadataFilter), 2*1024*1024)
		require.Equal(t, len(docs), len(incarnations), "each predicate describes the same documents")
		for i, doc := range docs {
			require.Equal(t, scope.Incarnations[doc], incarnations[i])
		}
		require.Equal(t, "report", fields["doc_type"].GetStructValue().GetFields()["$eq"].GetStringValue())
		require.Equal(t, "archive", fields["collection"].GetStructValue().GetFields()["$eq"].GetStringValue())
		return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{hostedHit(t, docs[0], incarnations[0], scopeTenant, "c")}}, nil
	}}
	driver := NewDriver(testClient(conn))
	filter := domain.VectorFilter{DocType: "report", Collection: "archive", DocumentIDs: ids, Authorized: scope}
	got, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, filter)
	require.NoError(t, err)
	require.Equal(t, 3, requests)
	require.Len(t, got, 3)
	requests = 0
	_, err = driver.QueryCandidates(t.Context(), []float32{1}, 3, filter)
	require.NoError(t, err)
	require.Equal(t, 3, requests)
	requests = 0
	_, used, err := driver.QueryFilteredBudgeted(t.Context(), []float32{1}, 3, filter, 3)
	require.NoError(t, err)
	require.Equal(t, 3, used)
	require.Equal(t, 3, requests)
}

func scopedFilterClause(filter *structpb.Struct, field string) *structpb.Struct {
	for _, clause := range filter.GetFields()["$and"].GetListValue().GetValues() {
		if fields := clause.GetStructValue(); fields.GetFields()[field] != nil {
			return fields
		}
	}
	return nil
}

func TestAuthorizedQueryNeverSearchesWithoutExplicitMembership(t *testing.T) {
	scope, _ := memberScope(2)
	for name, filter := range map[string]domain.VectorFilter{
		"omitted members": {Authorized: scope},
		"no tenant":       {Authorized: &domain.AuthorizedScope{PrincipalID: "p", Incarnations: scope.Incarnations}, DocumentIDs: []string{memberID(0)}},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			client := testClient(&fakeIndexConn{query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
				calls++
				return &pinecone.QueryVectorsResponse{}, nil
			}})
			driver := NewDriver(client)
			for _, search := range []func() error{
				func() error { _, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, filter); return err },
				func() error { _, err := driver.QueryCandidates(t.Context(), []float32{1}, 3, filter); return err },
				func() error {
					_, _, err := driver.QueryFilteredBudgeted(t.Context(), []float32{1}, 3, filter, 3)
					return err
				},
			} {
				require.ErrorIs(t, search(), domain.ErrGrantScopeUnsupported)
			}
			require.Zero(t, calls)
		})
	}
}

func TestAuthorizedQueryDropsUnlistedDocumentsAndSkipsEmptyPartitions(t *testing.T) {
	scope, _ := memberScope(1)
	calls := 0
	client := testClient(&fakeIndexConn{query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		calls++
		require.Equal(t, []string{memberID(0)}, listValues(t, scopedFilterClause(req.MetadataFilter, "doc_id"), "doc_id"), "an unlisted document never reaches a predicate")
		return &pinecone.QueryVectorsResponse{}, nil
	}})
	filter := domain.VectorFilter{Authorized: scope, DocumentIDs: []string{memberID(0), memberID(7), "not-a-uuid", uuid.NewString()}}
	_, err := NewDriver(client).QueryFiltered(t.Context(), []float32{1}, 3, filter)
	require.NoError(t, err)
	require.Equal(t, 1, calls)

	calls = 0
	filter.DocumentIDs = []string{memberID(7), "not-a-uuid"}
	got, err := NewDriver(client).QueryFiltered(t.Context(), []float32{1}, 3, filter)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Zero(t, calls, "no member remains, so no request is sent")
}

func TestManifestIncarnationMembership(t *testing.T) {
	scope, ids := memberScope(2)
	foreign := "00000000-0000-4000-8000-0000000000ee"
	client := testClient(&fakeIndexConn{query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		noTenant := hostedHit(t, ids[0], memberIncarnation(0), "", "no-tenant")
		delete(noTenant.Vector.Metadata.Fields, domain.VectorTenantKey)
		manifest := func(doc, tenant, chunk, stamp string) *pinecone.ScoredVector {
			const revision = "5b340356-6a61-4375-a0d2-a578731aaa7f"
			md, err := structpb.NewStruct(map[string]any{"doc_id": doc, "chunk_id": chunk, domain.VectorTenantKey: tenant, "revision_id": revision})
			require.NoError(t, err)
			if stamp != "" {
				md.Fields[domain.VectorIncarnationKey] = structpb.NewStringValue(stamp)
			}
			return &pinecone.ScoredVector{Vector: &pinecone.Vector{Id: domain.FormatRevisionVectorID(domain.DocumentID(doc), revision, 0), Metadata: md}, Score: .5}
		}
		contradictory := hostedHit(t, ids[0], memberIncarnation(0), scopeTenant, "both-identities")
		contradictory.Vector.Metadata.Fields["revision_id"] = structpb.NewStringValue("revision")
		noManifestTenant := manifest(ids[0], "", "missing-manifest-tenant", memberIncarnation(0))
		delete(noManifestTenant.Vector.Metadata.Fields, domain.VectorTenantKey)
		return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{
			hostedHit(t, ids[0], memberIncarnation(0), scopeTenant, "good"),
			hostedHit(t, ids[0], memberIncarnation(0), "00000000-0000-4000-8000-0000000000b0", "other-tenant"),
			hostedHit(t, ids[0], memberIncarnation(1), scopeTenant, "wrong-incarnation"),
			hostedHit(t, ids[0], "99999999-9999-4999-8999-999999999999", scopeTenant, "stale-incarnation"),
			hostedHit(t, foreign, memberIncarnation(0), scopeTenant, "foreign-document"),
			noTenant,
			hostedHit(t, ids[1], memberIncarnation(1), strings.ToUpper(scopeTenant), "case-folded-tenant"),
			manifest(ids[0], scopeTenant, "manifest", ""),
			manifest(ids[0], scopeTenant, "stamped", memberIncarnation(0)),
			manifest(ids[0], scopeTenant, "wrong-pair", memberIncarnation(1)),
			manifest(ids[0], "", "unlabeled-manifest", memberIncarnation(0)),
			manifest(ids[0], "foreign-tenant", "foreign-manifest", memberIncarnation(0)),
			manifest(foreign, scopeTenant, "unlisted-manifest", memberIncarnation(0)),
			noManifestTenant,
			contradictory,
		}}, nil
	}})
	got, err := NewDriver(client).QueryFiltered(t.Context(), []float32{1}, 10, domain.VectorFilter{Authorized: scope, DocumentIDs: ids})
	require.NoError(t, err)
	var kept []string
	for _, sc := range got {
		kept = append(kept, string(sc.Chunk.ID))
	}
	require.ElementsMatch(t, []string{"good", "case-folded-tenant", "manifest", "stamped"}, kept)
}

func TestManifestIncarnationFilterBeforeTopK(t *testing.T) {
	client, index := memoryClient(2)
	scope, ids := memberScope(2)
	manifest := func(id, stamp string, embedding []float32) domain.Chunk {
		chunk := memoryChunk(id, ids[0], embedding, "report", "archive")
		chunk.RevisionID = uuid.NewString()
		chunk.VectorIncarnation = stamp
		chunk.Metadata[domain.VectorTenantKey] = scopeTenant
		return chunk
	}
	obsolete := manifest("old-document-lifetime", memberIncarnation(2), []float32{1, 0})
	active := manifest("current", memberIncarnation(0), []float32{1, 1})
	require.NoError(t, client.Upsert(t.Context(), "", []domain.Chunk{obsolete, active}))

	// Check the provider's pre-ranking evaluation separately from the driver's
	// post-query membership gate.
	ctx := t.Context()
	query := []float32{1, 0}
	req := &pinecone.QueryByVectorValuesRequest{Vector: query, TopK: 1, IncludeMetadata: true}
	raw, err := index.QueryByVectorValues(ctx, req)
	require.NoError(t, err)
	require.Len(t, raw.Matches, 1)
	require.Equal(t, string(obsolete.ID), raw.Matches[0].Vector.Metadata.Fields["chunk_id"].GetStringValue())
	req.MetadataFilter = buildScopedFilter("report", ids, "archive", scopeTenant, []string{memberIncarnation(0), memberIncarnation(1)})
	raw, err = index.QueryByVectorValues(ctx, req)
	require.NoError(t, err)
	require.Len(t, raw.Matches, 1)
	require.Equal(t, string(active.ID), raw.Matches[0].Vector.Metadata.Fields["chunk_id"].GetStringValue())

	filter := domain.VectorFilter{Authorized: scope, DocumentIDs: ids, Collection: "archive", DocType: "report"}
	driver := NewDriver(client)
	kept, err := driver.QueryFiltered(ctx, query, 1, filter)
	require.NoError(t, err)
	require.Len(t, kept, 1)
	require.Equal(t, active.ID, kept[0].Chunk.ID)

	// Older writers leave the stamp absent. SQL decides which revision is active
	// when the provider returns both formats or two revisions of one incarnation.
	historical := manifest("historical-format", "", []float32{1, 0})
	priorRevision := manifest("previous-revision", memberIncarnation(0), []float32{1, 0})
	require.NoError(t, client.Upsert(ctx, "", []domain.Chunk{historical, priorRevision}))
	kept, err = driver.QueryFiltered(ctx, query, 4, filter)
	require.NoError(t, err)
	require.Len(t, kept, 3)
	require.ElementsMatch(t, []domain.ChunkID{historical.ID, priorRevision.ID, active.ID},
		[]domain.ChunkID{kept[0].Chunk.ID, kept[1].Chunk.ID, kept[2].Chunk.ID})
}

func TestManifestIncarnationPartitionFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fault  func(context.Context, context.CancelFunc) error
		calls  int
		code   codes.Code
		budget int
	}{
		{"later nonretryable error", func(context.Context, context.CancelFunc) error {
			return status.Error(codes.InvalidArgument, "provider rejected filter")
		}, 2, codes.InvalidArgument, 3},
		{"retry exhaustion", func(context.Context, context.CancelFunc) error {
			return status.Error(codes.Unavailable, "provider unavailable")
		}, 3, codes.Unavailable, 3},
		{"deadline", func(ctx context.Context, _ context.CancelFunc) error {
			<-ctx.Done()
			return ctx.Err()
		}, 2, codes.DeadlineExceeded, 2},
		{"cancellation", func(ctx context.Context, cancel context.CancelFunc) error {
			cancel()
			return ctx.Err()
		}, 2, codes.Canceled, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				scope, ids := memberScope(maxInValues + 1)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				calls := 0
				conn := &fakeIndexConn{query: func(ctx context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
					calls++
					docs := listValues(t, scopedFilterClause(req.MetadataFilter, "doc_id"), "doc_id")
					require.NotNil(t, scopedFilterClause(req.MetadataFilter, "$or"))
					if calls == 1 {
						require.Len(t, docs, maxInValues)
						hit := hostedHit(t, docs[0], memberIncarnation(0), scopeTenant, "first")
						return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{hit}}, nil
					}
					require.Equal(t, ids[maxInValues:], docs)
					return nil, tc.fault(ctx, cancel)
				}}
				client := testClient(conn)
				client.timeout = 100 * time.Millisecond
				filter := domain.VectorFilter{Authorized: scope, DocumentIDs: ids}
				hits, used, err := NewDriver(client).QueryFilteredBudgeted(ctx, []float32{1}, 2, filter, tc.budget)
				require.Error(t, err)
				switch tc.code {
				case codes.DeadlineExceeded:
					require.ErrorIs(t, err, context.DeadlineExceeded)
				case codes.Canceled:
					require.ErrorIs(t, err, context.Canceled)
				default:
					require.Equal(t, tc.code, status.Code(err))
				}
				require.Empty(t, hits)
				require.Equal(t, tc.calls, calls)
				require.Equal(t, calls, used)
			})
		})
	}
}

func TestManifestIncarnationMembershipDeduplicatesValues(t *testing.T) {
	scope, ids := memberScope(2)
	scope.Incarnations[ids[1]] = scope.Incarnations[ids[0]]
	documents, incarnations, members := authorizedMembers(scope, []string{ids[0], ids[0], ids[1]})
	require.Equal(t, ids, documents)
	require.Equal(t, []string{memberIncarnation(0)}, incarnations)
	require.Equal(t, scope.Incarnations, members)
}

func TestAuthorizedQueryMatchesAliasSpellingsExactly(t *testing.T) {
	scope, ids := memberScope(1)
	alias := strings.ToUpper(ids[0])
	client := testClient(&fakeIndexConn{query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		require.Equal(t, []string{alias}, listValues(t, scopedFilterClause(req.MetadataFilter, "doc_id"), "doc_id"), "the store's spelling is sent byte for byte")
		return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{hostedHit(t, alias, memberIncarnation(0), scopeTenant, "aliased")}}, nil
	}})
	got, err := NewDriver(client).QueryFiltered(t.Context(), []float32{1}, 10, domain.VectorFilter{Authorized: scope, DocumentIDs: []string{alias}})
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestHostedStoreDeclaresMembershipScope(t *testing.T) {
	var store domain.GrantScopedVectorStore = NewDriver(testClient(&fakeIndexConn{}))
	require.Equal(t, domain.GrantScopeMembership, store.GrantScope())
}

func TestUnscopedFiltersKeepTheirShape(t *testing.T) {
	require.Equal(t, buildFilter("report", []string{"one"}, "archive").AsMap(),
		buildScopedFilter("report", []string{"one"}, "archive", "", nil).AsMap())
	require.NotContains(t, buildFilter("", nil, "").AsMap(), domain.VectorTenantKey)
}

func FuzzAuthorizedMembers(f *testing.F) {
	f.Add(memberID(0), memberID(0), "x")
	f.Add(strings.ToUpper(memberID(1)), memberID(1), "")
	f.Add("", "00000000000040008000000000000001", "y")
	f.Add("{00000000-0000-4000-8000-000000000001}", memberID(0), "z")
	f.Fuzz(func(t *testing.T, id, member, incarnation string) {
		scope := &domain.AuthorizedScope{TenantID: scopeTenant, Incarnations: map[string]string{}}
		if parsed, err := uuid.Parse(member); err == nil {
			scope.Incarnations[parsed.String()] = incarnation
		}
		documents, incarnations, members := authorizedMembers(scope, []string{id, id})
		require.Len(t, documents, len(members))
		require.LessOrEqual(t, len(incarnations), len(documents))
		for _, document := range documents {
			want, ok := scope.IncarnationOf(document)
			require.True(t, ok, "only listed documents reach a predicate")
			require.Equal(t, want, members[document])
		}
		if len(documents) != 0 {
			parsed, err := uuid.Parse(id)
			require.NoError(t, err)
			require.Contains(t, scope.Incarnations, parsed.String())
		}
	})
}
