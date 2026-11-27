package pinecone

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// hostedFilterClause reads a predicate that applies to every result.
func hostedFilterClause(filter *structpb.Struct, field string) *structpb.Struct {
	if filter.GetFields()[field] != nil {
		return filter
	}
	return scopedFilterClause(filter, field)
}

func hostedFilterEquality(filter *structpb.Struct, field string) string {
	return hostedFilterClause(filter, field).GetFields()[field].GetStructValue().GetFields()["$eq"].GetStringValue()
}

func hostedFilterMembership(t *testing.T, filter *structpb.Struct) (documents, incarnations []string) {
	t.Helper()
	documents = listValues(t, hostedFilterClause(filter, "doc_id"), "doc_id")
	require.NotEmpty(t, documents, "hosted filter must contain a nonempty doc_id $in predicate")

	const revision = domain.VectorRevisionKey
	clause := hostedFilterClause(filter, revision)
	if clause == nil {
		// The tagged alternative uses $in. The manifest alternative uses
		// $exists on the same field and carries no incarnation list.
		identity := hostedFilterClause(filter, "$or").GetFields()["$or"].GetListValue()
		for _, alternative := range identity.GetValues() {
			fields := alternative.GetStructValue()
			if fields.GetFields()[revision].GetStructValue().GetFields()["$in"] != nil {
				clause = fields
				break
			}
		}
	}
	incarnations = listValues(t, clause, revision)
	require.NotEmpty(t, incarnations, "hosted filter must contain a nonempty %s $in predicate", revision)
	return documents, incarnations
}
