package pinecone

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// hostedRecord is one vector of the exact hosted double.
type hostedRecord struct {
	document, incarnation, tenant, chunk string
	score                                float32
}

// exactHostedIndex answers a request the way the hosted index must: it applies
// every equality and membership predicate to the whole corpus, then ranks the
// matches by score and identifier. It keeps no state between requests.
type exactHostedIndex struct {
	t       *testing.T
	records []hostedRecord
	// predicates records the number of values of each membership predicate.
	predicates [][2]int
}

func (x *exactHostedIndex) query(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
	fields := req.MetadataFilter.GetFields()
	tenant := fields[domain.VectorTenantKey].GetStructValue().GetFields()["$eq"].GetStringValue()
	documents := listValues(x.t, req.MetadataFilter, "doc_id")
	incarnations := listValues(x.t, req.MetadataFilter, domain.VectorRevisionKey)
	x.predicates = append(x.predicates, [2]int{len(documents), len(incarnations)})
	documentSet, incarnationSet := map[string]bool{}, map[string]bool{}
	for _, id := range documents {
		documentSet[id] = true
	}
	for _, id := range incarnations {
		incarnationSet[id] = true
	}
	var matches []hostedRecord
	for _, record := range x.records {
		if record.tenant == tenant && documentSet[record.document] && incarnationSet[record.incarnation] {
			matches = append(matches, record)
		}
	}
	rankHosted(matches)
	matches = matches[:min(len(matches), int(req.TopK))]
	out := make([]*pinecone.ScoredVector, 0, len(matches))
	for _, record := range matches {
		out = append(out, scoredHostedHit(x.t, record))
	}
	return &pinecone.QueryVectorsResponse{Matches: out}, nil
}

func rankHosted(records []hostedRecord) {
	slices.SortFunc(records, func(a, b hostedRecord) int {
		switch {
		case a.score > b.score:
			return -1
		case a.score < b.score:
			return 1
		}
		return strings.Compare(a.chunk, b.chunk)
	})
}

func scoredHostedHit(t *testing.T, record hostedRecord) *pinecone.ScoredVector {
	t.Helper()
	hit := hostedHit(t, record.document, record.incarnation, record.tenant, record.chunk)
	hit.Score = record.score
	return hit
}

// TestHostedSetSearchEqualsTheExhaustiveAuthorizedResult compares the real
// adapter, partitioned and merged, with a ranking of the whole authorized scope.
// Scores tie in groups, so the order of ties decides equality. A foreign tenant
// and unlisted documents score higher than every member and must never appear.
func TestHostedSetSearchEqualsTheExhaustiveAuthorizedResult(t *testing.T) {
	t.Run("tagged filter equivalence", func(t *testing.T) {
		doc, incarnation := memberID(0), memberIncarnation(0)
		flat, err := structpb.NewStruct(map[string]any{
			domain.VectorTenantKey:   map[string]any{"$eq": scopeTenant},
			"collection":             map[string]any{"$eq": "team"},
			"doc_type":               map[string]any{"$eq": "report"},
			"doc_id":                 map[string]any{"$in": []any{doc}},
			domain.VectorRevisionKey: map[string]any{"$in": []any{incarnation}},
		})
		require.NoError(t, err)
		grouped := buildScopedFilter("report", []string{doc}, "team", scopeTenant, []string{incarnation})
		match := func(filter *structpb.Struct, field, value string) bool {
			metadata, err := structpb.NewStruct(map[string]any{
				domain.VectorTenantKey: scopeTenant, "collection": "team", "doc_type": "report",
				"doc_id": doc, domain.VectorRevisionKey: incarnation,
			})
			require.NoError(t, err)
			if field != "" {
				metadata.Fields[field] = structpb.NewStringValue(value)
			}
			found, err := matchesFilter(metadata, filter)
			require.NoError(t, err)
			return found
		}
		for _, filter := range []*structpb.Struct{flat, grouped} {
			docs, incarnations := hostedFilterMembership(t, filter)
			require.Equal(t, []string{doc}, docs)
			require.Equal(t, []string{incarnation}, incarnations)
			require.Equal(t, scopeTenant, hostedFilterEquality(filter, domain.VectorTenantKey))
			require.Equal(t, "team", hostedFilterEquality(filter, "collection"))
			require.Equal(t, "report", hostedFilterEquality(filter, "doc_type"))
			require.True(t, match(filter, "", ""), "all predicates match")
			for field, mismatch := range map[string]string{
				domain.VectorTenantKey: "foreign", "collection": "other", "doc_type": "note",
				"doc_id": memberID(1), domain.VectorRevisionKey: memberIncarnation(1),
			} {
				require.False(t, match(filter, field, mismatch), "%s is required", field)
			}
		}
	})
	for _, members := range []int{9999, 10000, 10001, 20001} {
		t.Run(fmt.Sprint(members), func(t *testing.T) {
			scope, ids := memberScope(members)
			index := &exactHostedIndex{t: t}
			for i, id := range ids {
				index.records = append(index.records, hostedRecord{document: id, incarnation: scope.Incarnations[id], tenant: scopeTenant,
					chunk: fmt.Sprintf("c%06d", i), score: float32(i%7) / 10})
			}
			authorized := slices.Clone(index.records)
			for i := range 500 {
				index.records = append(index.records,
					hostedRecord{document: uuid.NewString(), incarnation: uuid.NewString(), tenant: scopeTenant, chunk: fmt.Sprintf("a%06d", i), score: .99},
					hostedRecord{document: ids[i], incarnation: uuid.NewString(), tenant: scopeTenant, chunk: fmt.Sprintf("b%06d", i), score: .98},
					hostedRecord{document: ids[i], incarnation: scope.Incarnations[ids[i]], tenant: "00000000-0000-4000-8000-0000000000ff", chunk: fmt.Sprintf("d%06d", i), score: .97})
			}
			rankHosted(authorized)
			driver := NewTestDriver(index.query)
			for _, topK := range []int{1, 3, 10, 25} {
				index.predicates = nil
				requestScope := *scope
				requestScope.Budget = domain.NewSearchBudget(10, 10)
				got, err := driver.QueryFiltered(t.Context(), []float32{1}, topK, domain.VectorFilter{DocumentIDs: ids, Authorized: &requestScope})
				require.NoError(t, err)
				require.Len(t, got, topK)
				for i, record := range authorized[:topK] {
					require.Equal(t, domain.ChunkID(record.chunk), got[i].Chunk.ID, "top %d, position %d", topK, i)
					require.Equal(t, float64(record.score), got[i].Score)
				}
				partitions, _ := driver.SearchPartitions(members)
				require.Len(t, index.predicates, partitions)
				for _, predicate := range index.predicates {
					require.LessOrEqual(t, predicate[0], maxInValues)
					require.Equal(t, predicate[0], predicate[1], "the document and incarnation predicates describe the same documents")
				}
			}
			// Candidates keep each partition's hits, so a verification step can run
			// before any merge. After the same merge they equal the exhaustive ranking.
			requestScope := *scope
			requestScope.Budget = domain.NewSearchBudget(10, 10)
			candidates, err := driver.QueryCandidates(t.Context(), []float32{1}, 10, domain.VectorFilter{DocumentIDs: ids, Authorized: &requestScope})
			require.NoError(t, err)
			merged := make([]hostedRecord, 0, len(candidates))
			for _, sc := range candidates {
				merged = append(merged, hostedRecord{chunk: string(sc.Chunk.ID), score: float32(sc.Score)})
			}
			rankHosted(merged)
			for i := range 10 {
				require.Equal(t, authorized[i].chunk, merged[i].chunk)
			}
		})
	}
}

func TestHostedSetSearchKeepsTenantAndLabelsOnEveryRequestAt100000Members(t *testing.T) {
	scope, ids := memberScope(100000)
	scope.Budget = domain.NewSearchBudget(10, 10)
	var seen []string
	index := &exactHostedIndex{t: t}
	driver := NewTestDriver(func(ctx context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		fields := req.MetadataFilter.GetFields()
		require.Equal(t, scopeTenant, fields[domain.VectorTenantKey].GetStructValue().GetFields()["$eq"].GetStringValue())
		require.Equal(t, "team", fields["collection"].GetStructValue().GetFields()["$eq"].GetStringValue())
		require.Equal(t, "report", fields["doc_type"].GetStructValue().GetFields()["$eq"].GetStringValue())
		seen = append(seen, structpb.NewStructValue(req.MetadataFilter).String())
		return index.query(ctx, req)
	})
	_, err := driver.QueryFiltered(t.Context(), []float32{1}, 5, domain.VectorFilter{DocType: "report", Collection: "team", DocumentIDs: ids, Authorized: scope})
	require.NoError(t, err)
	require.Len(t, seen, 10, "a global pass over the largest set makes at most ten hosted searches")
	require.Equal(t, 10, scope.Budget.Used())
}
