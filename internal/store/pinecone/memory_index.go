package pinecone

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// MemoryIndex is an in-memory cosine index that follows the semantics the
// service relies on: upserts replace whole records and reject a wrong
// dimension, queries apply equality and membership filters before ranking,
// and keys can be fetched, listed in ascending order and deleted. It serves
// tests and offline tools that need an index without a hosted account.
type MemoryIndex struct {
	mu      sync.Mutex
	dims    int
	records map[string]*pinecone.Vector
}

var _ IndexConn = (*MemoryIndex)(nil)

// NewMemoryIndex creates an empty index of the given dimension.
func NewMemoryIndex(dimensions int) *MemoryIndex {
	return &MemoryIndex{dims: dimensions, records: make(map[string]*pinecone.Vector)}
}

// Len returns the number of stored records.
func (m *MemoryIndex) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.records)
}

// IDs returns the stored keys in ascending order.
func (m *MemoryIndex) IDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sortedIDs()
}

func (m *MemoryIndex) sortedIDs() []string {
	ids := make([]string, 0, len(m.records))
	for id := range m.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func cloneVector(v *pinecone.Vector, values, metadata bool) *pinecone.Vector {
	out := &pinecone.Vector{Id: v.Id}
	if values && v.Values != nil {
		copied := slices.Clone(*v.Values)
		out.Values = &copied
	}
	if metadata && v.Metadata != nil {
		out.Metadata = proto.Clone(v.Metadata).(*structpb.Struct)
	}
	return out
}

// UpsertVectors stores or replaces records.
func (m *MemoryIndex) UpsertVectors(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
	count := len(vectors)
	if uint64(count) > math.MaxUint32 {
		return 0, fmt.Errorf("batch contains too many vectors: %d", count)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range vectors {
		if v.Values == nil || len(*v.Values) != m.dims {
			return 0, fmt.Errorf("vector dimension does not match the index dimension %d", m.dims)
		}
	}
	for _, v := range vectors {
		m.records[v.Id] = cloneVector(v, true, true)
	}
	return uint32(count), nil
}

func matchesFilter(md, filter *structpb.Struct) (bool, error) {
	for field, condition := range filter.GetFields() {
		operators := condition.GetStructValue().GetFields()
		if len(operators) == 0 {
			return false, fmt.Errorf("filter on %q needs an operator", field)
		}
		have := md.GetFields()[field]
		for operator, want := range operators {
			switch operator {
			case "$eq":
				if have == nil || have.GetStringValue() != want.GetStringValue() {
					return false, nil
				}
			case "$in":
				found := false
				for _, candidate := range want.GetListValue().GetValues() {
					found = found || (have != nil && candidate.GetStringValue() == have.GetStringValue())
				}
				if !found {
					return false, nil
				}
			case "$exists":
				if (have != nil) != want.GetBoolValue() {
					return false, nil
				}
			default:
				return false, fmt.Errorf("unsupported filter operator %q", operator)
			}
		}
	}
	return true, nil
}

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// QueryByVectorValues ranks the records that satisfy the metadata filter.
func (m *MemoryIndex) QueryByVectorValues(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
	if len(req.Vector) != m.dims {
		return nil, fmt.Errorf("query dimension %d does not match the index dimension %d", len(req.Vector), m.dims)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	type hit struct {
		v     *pinecone.Vector
		score float64
	}
	var hits []hit
	for _, id := range m.sortedIDs() {
		v := m.records[id]
		if req.MetadataFilter != nil {
			ok, err := matchesFilter(v.Metadata, req.MetadataFilter)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		hits = append(hits, hit{v, cosine(req.Vector, *v.Values)})
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	if len(hits) > int(req.TopK) {
		hits = hits[:req.TopK]
	}
	res := &pinecone.QueryVectorsResponse{}
	for _, h := range hits {
		res.Matches = append(res.Matches, &pinecone.ScoredVector{Vector: cloneVector(h.v, req.IncludeValues, req.IncludeMetadata), Score: float32(h.score)})
	}
	return res, nil
}

// FetchVectors returns the stored records; absent keys are omitted.
func (m *MemoryIndex) FetchVectors(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := &pinecone.FetchVectorsResponse{Vectors: make(map[string]*pinecone.Vector)}
	for _, id := range ids {
		if v, ok := m.records[id]; ok {
			res.Vectors[id] = cloneVector(v, true, true)
		}
	}
	return res, nil
}

// ListVectors pages through the keys in ascending order; the token is the last key returned.
func (m *MemoryIndex) ListVectors(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	limit := 100
	if req.Limit != nil {
		limit = int(*req.Limit)
	}
	after := ""
	if req.PaginationToken != nil {
		after = *req.PaginationToken
	}
	res := &pinecone.ListVectorsResponse{}
	for _, id := range m.sortedIDs() {
		if id <= after {
			continue
		}
		if len(res.VectorIds) == limit {
			last := *res.VectorIds[len(res.VectorIds)-1]
			res.NextPaginationToken = &last
			break
		}
		key := id
		res.VectorIds = append(res.VectorIds, &key)
	}
	return res, nil
}

// DeleteVectorsByID removes records; absent keys are ignored.
func (m *MemoryIndex) DeleteVectorsByID(_ context.Context, ids []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		delete(m.records, id)
	}
	return nil
}

// DeleteVectorsByFilter removes every record that satisfies the filter.
func (m *MemoryIndex) DeleteVectorsByFilter(_ context.Context, filter *structpb.Struct) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, v := range m.records {
		ok, err := matchesFilter(v.Metadata, filter)
		if err != nil {
			return err
		}
		if ok {
			delete(m.records, id)
		}
	}
	return nil
}

// DeleteAllVectorsInNamespace empties the index.
func (m *MemoryIndex) DeleteAllVectorsInNamespace(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.records)
	return nil
}

// Close releases nothing.
func (*MemoryIndex) Close() error { return nil }
