package pinecone

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	maxInValues = 10000
	// Pinecone caps query results at 4 MB and 10,000 matches. Allow 40 KiB
	// of metadata, up to 512 four-byte ID characters, and 1 KiB of framing
	// per match. Refill requests omit vector values.
	maxRefillTopK = min(10_000, 4_000_000/(maxMetadataBytes+512*4+1024))
)

func buildFilter(docType string, ids []string, collection string) *structpb.Struct {
	return buildScopedFilter(docType, ids, collection, "", nil)
}

// buildScopedFilter binds both vector formats to the same tenant and document
// membership. Tagged vectors must also match one of the listed incarnations.
func buildScopedFilter(docType string, ids []string, collection, tenant string, incarnations []string) *structpb.Struct {
	fields := make(map[string]*structpb.Value)
	if collection != "" {
		fields["collection"] = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$eq": structpb.NewStringValue(collection)}})
	}
	if docType != "" {
		fields["doc_type"] = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$eq": structpb.NewStringValue(docType)}})
	}
	if ids != nil {
		values := make([]*structpb.Value, len(ids))
		for i, id := range ids {
			values[i] = structpb.NewStringValue(id)
		}
		fields["doc_id"] = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$in": structpb.NewListValue(&structpb.ListValue{Values: values})}})
	}
	if tenant != "" {
		fields[domain.VectorTenantKey] = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$eq": structpb.NewStringValue(tenant)}})
		values := make([]*structpb.Value, len(incarnations))
		for i, incarnation := range incarnations {
			values[i] = structpb.NewStringValue(incarnation)
		}
		tagged := map[string]*structpb.Value{
			domain.VectorRevisionKey: structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$in": structpb.NewListValue(&structpb.ListValue{Values: values})}}),
		}
		manifest := map[string]*structpb.Value{
			"revision_id":            structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$exists": structpb.NewBoolValue(true)}}),
			domain.VectorRevisionKey: structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$exists": structpb.NewBoolValue(false)}}),
		}
		identity := structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewStructValue(&structpb.Struct{Fields: tagged}),
			structpb.NewStructValue(&structpb.Struct{Fields: manifest}),
		}})
		clauses := make([]*structpb.Value, 0, len(fields)+1)
		for _, field := range []string{domain.VectorTenantKey, "doc_id", "collection", "doc_type"} {
			if condition := fields[field]; condition != nil {
				clauses = append(clauses, structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{field: condition}}))
			}
		}
		clauses = append(clauses, structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$or": identity}}))
		fields = map[string]*structpb.Value{"$and": structpb.NewListValue(&structpb.ListValue{Values: clauses})}
	}
	if len(fields) == 0 {
		return nil
	}
	return &structpb.Struct{Fields: fields}
}

// partitionCount is the number of requests a member list needs when each
// predicate carries at most maxInValues values.
func partitionCount(members int) int {
	if members <= 0 {
		return 0
	}
	return 1 + (members-1)/maxInValues
}

// QueryFiltered searches the default namespace with document scope predicates.
func (p *Client) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	if filter.MatchesNothing() {
		return []domain.ScoredChunk{}, nil
	}
	type hitKey struct {
		id          domain.ChunkID
		revision    string
		attempt     string
		manifestRev string
	}
	merged := make(map[hitKey]domain.ScoredChunk)
	if err := p.queryPartitions(ctx, embedding, topK, filter, nil, func(chunks []domain.ScoredChunk) {
		for _, sc := range chunks {
			revision, _ := sc.Chunk.Metadata[domain.VectorRevisionKey].(string)
			attempt, _ := sc.Chunk.Metadata[domain.VectorAttemptKey].(string)
			key := hitKey{id: sc.Chunk.ID, revision: revision, attempt: attempt, manifestRev: sc.Chunk.RevisionID}
			if prev, ok := merged[key]; !ok || sc.Score > prev.Score {
				merged[key] = sc
			}
		}
	}); err != nil {
		return nil, err
	}
	chunks := make([]domain.ScoredChunk, 0, len(merged))
	for _, sc := range merged {
		chunks = append(chunks, sc)
	}
	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].Score == chunks[j].Score {
			return chunks[i].Chunk.ID < chunks[j].Chunk.ID
		}
		return chunks[i].Score > chunks[j].Score
	})
	return chunks[:min(topK, len(chunks))], nil
}

func (p *Client) queryCandidates(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, budget *int) ([]domain.ScoredChunk, error) {
	var candidates []domain.ScoredChunk
	if err := p.queryPartitions(ctx, embedding, topK, filter, budget, func(chunks []domain.ScoredChunk) {
		candidates = append(candidates, chunks...)
	}); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (p *Client) queryPartitions(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, budget *int, accept func([]domain.ScoredChunk)) error {
	if filter.MatchesNothing() {
		return nil
	}
	if topK < 1 || uint64(topK) > math.MaxUint32 {
		return fmt.Errorf("pinecone: topK must be between 1 and %d", uint64(math.MaxUint32))
	}
	if filter.Authorized != nil && (filter.DocumentIDs == nil || filter.Authorized.TenantID == "") {
		// A hosted search has no relation to join, so an authorized scope must
		// name its tenant and its members. Anything else would search globally.
		return domain.ErrGrantScopeUnsupported
	}
	partitions := max(1, partitionCount(len(filter.DocumentIDs)))
	if budget != nil && *budget < partitions {
		return domain.ErrVectorQueryBudget
	}
	// A request budget counts every attempt of the whole request, and the
	// refill budget counts one search. They never apply to the same search.
	var requestBudget *domain.SearchBudget
	var beforeSearch func(context.Context) error
	if filter.Authorized != nil {
		requestBudget, beforeSearch = filter.Authorized.Budget, filter.Authorized.BeforeSearch
		if requestBudget != nil && budget != nil {
			return domain.ErrGrantScopeUnsupported
		}
	}
	for part := range partitions {
		start := part * maxInValues
		ids := filter.DocumentIDs[start:min(start+maxInValues, len(filter.DocumentIDs))]
		metadataFilter := buildFilter(filter.DocType, ids, filter.Collection)
		var members map[string]string
		if filter.Authorized != nil {
			var incarnations []string
			ids, incarnations, members = authorizedMembers(filter.Authorized, ids)
			if len(ids) == 0 {
				continue
			}
			metadataFilter = buildScopedFilter(filter.DocType, ids, filter.Collection, filter.Authorized.TenantID, incarnations)
		}
		req := &pinecone.QueryByVectorValuesRequest{Vector: embedding, TopK: uint32(topK), IncludeMetadata: true, MetadataFilter: metadataFilter}
		if beforeSearch != nil {
			// Each batch re-establishes the credential, principal and set before
			// it sends the scope to the hosted index.
			if err := beforeSearch(ctx); err != nil {
				return err
			}
		}
		var partitionBudget *int
		limited := false
		remainingPartitions := partitions - part - 1
		reserved := 0
		switch {
		case requestBudget != nil:
			allowed, err := requestBudget.Acquire(p.queryAttempts())
			if err != nil {
				return err
			}
			reserved, partitionBudget = allowed, &allowed
		case budget != nil:
			// Keep one attempt for each later partition before allowing retries.
			available := *budget - remainingPartitions
			limited = available < p.queryAttempts()
			partitionBudget = &available
		}
		chunks, err := p.queryNamespace(ctx, "", req, partitionBudget)
		if requestBudget != nil {
			requestBudget.Release(reserved, reserved-*partitionBudget)
		}
		if budget != nil && partitionBudget != nil {
			*budget = *partitionBudget + remainingPartitions
			if limited && *partitionBudget == 0 && err != nil && retryable(err) {
				err = errors.Join(err, domain.ErrVectorQueryBudget)
			}
		}
		if err != nil {
			return err
		}
		if filter.Authorized != nil {
			chunks = withinMembers(filter.Authorized, members, chunks)
		}
		accept(chunks)
	}
	return nil
}

// authorizedMembers keeps the documents of a partition that the scope lists
// and pairs each with the incarnation it may read. A document the scope does
// not list is dropped, so it never reaches a hosted predicate. The members map
// is keyed by the exact spelling sent to the provider.
func authorizedMembers(scope *domain.AuthorizedScope, ids []string) (documents, incarnations []string, members map[string]string) {
	members = make(map[string]string, len(ids))
	documents = make([]string, 0, len(ids))
	incarnations = make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		incarnation, ok := scope.IncarnationOf(id)
		if _, repeated := members[id]; !ok || repeated {
			continue
		}
		if _, duplicate := seen[incarnation]; !duplicate {
			seen[incarnation] = struct{}{}
			incarnations = append(incarnations, incarnation)
		}
		members[id] = incarnation
		documents = append(documents, id)
	}
	return documents, incarnations, members
}

// withinMembers rejects any hit whose own metadata contradicts the request.
// Hosted metadata is untrusted. Membership and tenant must match for both
// vector formats, and SQL checks the active revision of each manifest hit.
func withinMembers(scope *domain.AuthorizedScope, members map[string]string, chunks []domain.ScoredChunk) []domain.ScoredChunk {
	kept := chunks[:0:0]
	for _, sc := range chunks {
		incarnation, listed := members[string(sc.Chunk.DocumentID)]
		revision, _ := sc.Chunk.Metadata[domain.VectorRevisionKey].(string)
		tagged := sc.Chunk.RevisionID == "" && revision == incarnation
		if listed && scope.SameTenant(sc.Chunk.Metadata) && (tagged || domain.ManifestIdentity(sc.Chunk)) {
			kept = append(kept, sc)
		}
	}
	return kept
}
