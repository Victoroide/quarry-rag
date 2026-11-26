package pinecone

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
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
		fields := req.MetadataFilter.GetFields()
		require.Equal(t, scopeTenant, fields[domain.VectorTenantKey].GetStructValue().GetFields()["$eq"].GetStringValue())
		docs, incarnations := listValues(t, req.MetadataFilter, "doc_id"), listValues(t, req.MetadataFilter, domain.VectorRevisionKey)
		require.LessOrEqual(t, len(docs), maxInValues)
		require.Equal(t, len(docs), len(incarnations), "each predicate describes the same documents")
		for i, doc := range docs {
			require.Equal(t, scope.Incarnations[doc], incarnations[i])
		}
		require.Equal(t, "report", fields["doc_type"].GetStructValue().GetFields()["$eq"].GetStringValue())
		return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{hostedHit(t, docs[0], incarnations[0], scopeTenant, "c")}}, nil
	}}
	driver := NewDriver(testClient(conn))
	filter := domain.VectorFilter{DocType: "report", DocumentIDs: ids, Authorized: scope}
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
		require.Equal(t, []string{memberID(0)}, listValues(t, req.MetadataFilter, "doc_id"), "an unlisted document never reaches a predicate")
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

func TestAuthorizedQueryTreatsHostedMetadataAsUntrusted(t *testing.T) {
	scope, ids := memberScope(2)
	foreign := "00000000-0000-4000-8000-0000000000ee"
	client := testClient(&fakeIndexConn{query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		noTenant := hostedHit(t, ids[0], memberIncarnation(0), "", "no-tenant")
		delete(noTenant.Vector.Metadata.Fields, domain.VectorTenantKey)
		return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{
			hostedHit(t, ids[0], memberIncarnation(0), scopeTenant, "good"),
			hostedHit(t, ids[0], memberIncarnation(0), "00000000-0000-4000-8000-0000000000b0", "other-tenant"),
			hostedHit(t, ids[0], memberIncarnation(1), scopeTenant, "wrong-incarnation"),
			hostedHit(t, ids[0], "99999999-9999-4999-8999-999999999999", scopeTenant, "stale-incarnation"),
			hostedHit(t, foreign, memberIncarnation(0), scopeTenant, "foreign-document"),
			noTenant,
			hostedHit(t, ids[1], memberIncarnation(1), strings.ToUpper(scopeTenant), "case-folded-tenant"),
		}}, nil
	}})
	got, err := NewDriver(client).QueryFiltered(t.Context(), []float32{1}, 10, domain.VectorFilter{Authorized: scope, DocumentIDs: ids})
	require.NoError(t, err)
	var kept []string
	for _, sc := range got {
		kept = append(kept, string(sc.Chunk.ID))
	}
	require.ElementsMatch(t, []string{"good", "case-folded-tenant"}, kept)
}

func TestAuthorizedQueryMatchesAliasSpellingsExactly(t *testing.T) {
	scope, ids := memberScope(1)
	alias := strings.ToUpper(ids[0])
	client := testClient(&fakeIndexConn{query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		require.Equal(t, []string{alias}, listValues(t, req.MetadataFilter, "doc_id"), "the store's spelling is sent byte for byte")
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
