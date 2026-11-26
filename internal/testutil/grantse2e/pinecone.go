package grantse2e

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/Victoroide/quarry-rag/internal/testutil/pineconetest"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// Pinecone implements the default-namespace fixture data plane.
type Pinecone struct {
	index       *pcstore.MemoryIndex
	mu          sync.Mutex
	calls       []PineconeCall
	extra       []*pinecone.Vector
	gate        *queryBarrier
	failDeletes bool
}

// PineconeCall records the actual request and affected or returned keys.
type PineconeCall struct {
	Path, Namespace string
	Filter          map[string]any
	TopK            uint32
	Keys            []string
}

type queryBarrier struct{ started, canceled chan struct{} }

func newPinecone(t *testing.T) (*Pinecone, *pineconetest.HTTPIndexConn, string) {
	t.Helper()
	fixture := &Pinecone{index: pcstore.NewMemoryIndex(dimensions)}
	srv := httptest.NewServer(fixture)
	t.Cleanup(srv.Close)
	endpoint, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return fixture, pineconetest.NewHTTPIndexConn(*endpoint, srv.Client(), ""), endpoint.Host
}

// Calls returns a snapshot of the data-plane history.
func (p *Pinecone) Calls() []PineconeCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.calls)
}

func (p *Pinecone) remember(call PineconeCall) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, call)
}

// IDs reads the fixture index's authoritative vector keys.
func (p *Pinecone) IDs() []string { return p.index.IDs() }

// Put seeds consistent or deliberately stale provider records.
func (p *Pinecone) Put(t *testing.T, vectors ...*pinecone.Vector) {
	t.Helper()
	count, err := p.index.UpsertVectors(t.Context(), vectors)
	require.NoError(t, err)
	require.Equal(t, len(vectors), int(count))
}

// Remove removes selected records for a fault-injection scenario.
func (p *Pinecone) Remove(t *testing.T, ids ...string) {
	t.Helper()
	ctx := context.WithoutCancel(t.Context())
	require.NoError(t, p.index.DeleteVectorsByID(ctx, ids))
}

// Record returns the single seeded chunk for a document.
func (p *Pinecone) Record(t *testing.T, document string) *pinecone.Vector {
	t.Helper()
	var ids []string
	for _, key := range p.IDs() {
		if strings.HasPrefix(key, document+"/") {
			ids = append(ids, key)
		}
	}
	require.Len(t, ids, 1, "document %s", document)
	got, err := p.index.FetchVectors(t.Context(), ids)
	require.NoError(t, err)
	require.Contains(t, got.Vectors, ids[0])
	return got.Vectors[ids[0]]
}

// CloneTagged constructs a self-consistent provider record with another lifetime or attempt.
func (*Pinecone) CloneTagged(t *testing.T, vector *pinecone.Vector, incarnation, attempt string) *pinecone.Vector {
	t.Helper()
	document, _, _, chunk, ok := pcstore.ParseTaggedVectorID(vector.Id)
	require.True(t, ok)
	key, err := pcstore.TaggedVectorID(document, incarnation, attempt, chunk)
	require.NoError(t, err)
	fields := vector.Metadata.AsMap()
	fields[domain.VectorRevisionKey], fields[domain.VectorAttemptKey] = incarnation, attempt
	metadata, err := structpb.NewStruct(fields)
	require.NoError(t, err)
	values := slices.Clone(*vector.Values)
	return &pinecone.Vector{Id: key, Values: &values, Metadata: metadata}
}

// SetField changes one returned provider metadata field and persists the record.
func (p *Pinecone) SetField(t *testing.T, vector *pinecone.Vector, field, value string) {
	t.Helper()
	vector.Metadata.Fields[field] = structpb.NewStringValue(value)
	p.Put(t, vector)
}

// AppendQueryHits simulates a provider returning candidates outside its own filter.
func (p *Pinecone) AppendQueryHits(vectors ...*pinecone.Vector) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.extra = slices.Clone(vectors)
}

// FailDeletes injects provider failure while leaving records untouched.
func (p *Pinecone) FailDeletes(fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failDeletes = fail
}

// BlockQuery stops the next query until the HTTP context is canceled.
func (p *Pinecone) BlockQuery() (started, canceled <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gate = &queryBarrier{started: make(chan struct{}), canceled: make(chan struct{})}
	return p.gate.started, p.gate.canceled
}

type pineconePayload struct {
	Namespace       string           `json:"namespace"`
	Vectors         []pineconeVector `json:"vectors"`
	Vector          []float32        `json:"vector"`
	TopK            uint32           `json:"topK"`
	IncludeValues   bool             `json:"includeValues"`
	IncludeMetadata bool             `json:"includeMetadata"`
	Filter          map[string]any   `json:"filter"`
	IDs             []string         `json:"ids"`
	DeleteAll       bool             `json:"deleteAll"`
}

type pineconeVector struct {
	ID       string         `json:"id"`
	Values   []float32      `json:"values,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func fromSDK(v *pinecone.Vector) pineconeVector {
	out := pineconeVector{ID: v.Id}
	if v.Values != nil {
		out.Values = *v.Values
	}
	if v.Metadata != nil {
		out.Metadata = v.Metadata.AsMap()
	}
	return out
}

// ServeHTTP validates the wire request before delegating to the ranked index.
func (p *Pinecone) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var out any
	var err error
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost && (path == "/query" || path == "/vectors/upsert" || path == "/vectors/delete"):
		var body pineconePayload
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err = dec.Decode(&body); err != nil || dec.Decode(new(any)) != io.EOF || body.Namespace != "" {
			http.Error(w, "invalid data-plane request", http.StatusBadRequest)
			return
		}
		switch path {
		case "/query":
			out, err = p.query(r.Context(), body)
		case "/vectors/upsert":
			out, err = p.upsert(r.Context(), body)
		case "/vectors/delete":
			err = p.delete(r.Context(), body)
		}
	case r.Method == http.MethodGet && (path == "/vectors/list" || path == "/vectors/fetch"):
		out, err = p.read(r)
	default:
		http.Error(w, "unsupported data-plane operation", http.StatusNotImplemented)
		return
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return
	case errors.Is(err, errDeleteUnavailable):
		http.Error(w, "synthetic delete unavailable", http.StatusServiceUnavailable)
	case err != nil:
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(out); err != nil {
			return
		}
	}
}

func (p *Pinecone) upsert(ctx context.Context, body pineconePayload) (any, error) {
	if len(body.Vectors) == 0 || !validPayload(body, "upsert") {
		return nil, errors.New("invalid upsert")
	}
	vectors := make([]*pinecone.Vector, 0, len(body.Vectors))
	var ids []string
	for _, v := range body.Vectors {
		md, err := structpb.NewStruct(v.Metadata)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, &pinecone.Vector{Id: v.ID, Values: &v.Values, Metadata: md})
		ids = append(ids, v.ID)
	}
	count, err := p.index.UpsertVectors(ctx, vectors)
	if err != nil {
		return nil, err
	}
	p.remember(PineconeCall{Path: "/vectors/upsert", Namespace: body.Namespace, Keys: ids})
	return map[string]uint32{"upsertedCount": count}, nil
}

func (p *Pinecone) query(ctx context.Context, body pineconePayload) (any, error) {
	if body.TopK == 0 || body.TopK > 10_000 || !validPayload(body, "query") {
		return nil, errors.New("invalid query")
	}
	filter, err := filterShape(body.Filter)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	gate, extra := p.gate, slices.Clone(p.extra)
	p.gate = nil
	p.mu.Unlock()
	if gate != nil {
		p.remember(PineconeCall{Path: "/query", Namespace: body.Namespace, Filter: body.Filter, TopK: body.TopK})
		close(gate.started)
		<-ctx.Done()
		close(gate.canceled)
		return nil, ctx.Err()
	}
	result, err := p.index.QueryByVectorValues(ctx, &pinecone.QueryByVectorValuesRequest{
		Vector: body.Vector, TopK: body.TopK, MetadataFilter: filter,
		IncludeMetadata: body.IncludeMetadata, IncludeValues: body.IncludeValues,
	})
	if err != nil {
		return nil, err
	}
	type match struct {
		pineconeVector
		Score float32 `json:"score"`
	}
	var matches []match
	var keys []string
	for _, hit := range result.Matches {
		matches = append(matches, match{pineconeVector: fromSDK(hit.Vector), Score: hit.Score})
		keys = append(keys, hit.Vector.Id)
	}
	for _, v := range extra {
		matches = append(matches, match{pineconeVector: fromSDK(v), Score: 1})
		keys = append(keys, v.Id)
	}
	p.remember(PineconeCall{Path: "/query", Namespace: body.Namespace, Filter: body.Filter, TopK: body.TopK, Keys: keys})
	return map[string]any{"matches": matches}, nil
}

var errDeleteUnavailable = errors.New("fixture delete unavailable")

func (p *Pinecone) delete(ctx context.Context, body pineconePayload) error {
	if !validPayload(body, "delete") || body.DeleteAll || (len(body.Filter) > 0) == (len(body.IDs) > 0) {
		return errors.New("invalid or unscoped deletion")
	}
	filter, err := filterShape(body.Filter)
	if err != nil {
		return err
	}
	p.mu.Lock()
	fail := p.failDeletes
	p.mu.Unlock()
	if fail {
		p.remember(PineconeCall{Path: "/vectors/delete", Namespace: body.Namespace, Filter: body.Filter})
		return errDeleteUnavailable
	}
	before := p.IDs()
	if len(body.IDs) > 0 {
		err = p.index.DeleteVectorsByID(ctx, body.IDs)
	} else {
		err = p.index.DeleteVectorsByFilter(ctx, filter)
	}
	if err != nil {
		return err
	}
	remaining := p.IDs()
	deleted := slices.DeleteFunc(before, func(key string) bool { return slices.Contains(remaining, key) })
	p.remember(PineconeCall{Path: "/vectors/delete", Namespace: body.Namespace, Filter: body.Filter, Keys: deleted})
	return nil
}

func (p *Pinecone) read(r *http.Request) (any, error) {
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || !slices.Equal(params["namespace"], []string{""}) {
		return nil, errors.New("invalid namespace")
	}
	if r.URL.Path == "/vectors/fetch" {
		if len(params["ids"]) == 0 || len(params) != 2 {
			return nil, errors.New("invalid fetch parameters")
		}
		result, err := p.index.FetchVectors(r.Context(), params["ids"])
		if err != nil {
			return nil, err
		}
		vectors := make(map[string]pineconeVector, len(result.Vectors))
		for id, v := range result.Vectors {
			vectors[id] = fromSDK(v)
		}
		p.remember(PineconeCall{Path: r.URL.Path, Keys: slices.Sorted(maps.Keys(vectors))})
		return map[string]any{"vectors": vectors}, nil
	}
	input := &pinecone.ListVectorsRequest{}
	for key, values := range params {
		if len(values) != 1 {
			return nil, errors.New("duplicate list parameter")
		}
		value := values[0]
		switch key {
		case "namespace":
		case "prefix":
			input.Prefix = &value
		case "paginationToken":
			input.PaginationToken = &value
		case "limit":
			limit, err := strconv.ParseUint(value, 10, 32)
			if err != nil || limit == 0 || limit > 1000 {
				return nil, errors.New("invalid list limit")
			}
			n := uint32(limit)
			input.Limit = &n
		default:
			return nil, errors.New("unknown list parameter")
		}
	}
	result, err := p.index.ListVectors(r.Context(), input)
	if err != nil {
		return nil, err
	}
	vectors := make([]pineconeVector, 0, len(result.VectorIds))
	var keys []string
	for _, id := range result.VectorIds {
		vectors = append(vectors, pineconeVector{ID: *id})
		keys = append(keys, *id)
	}
	p.remember(PineconeCall{Path: r.URL.Path, Keys: keys})
	out := map[string]any{"vectors": vectors}
	if result.NextPaginationToken != nil {
		out["pagination"] = map[string]string{"next": *result.NextPaginationToken}
	}
	return out, nil
}

func validPayload(body pineconePayload, action string) bool {
	hasQuery := len(body.Vector) > 0 || body.TopK != 0 || body.IncludeValues || body.IncludeMetadata
	hasDelete := len(body.IDs) > 0 || body.DeleteAll
	switch action {
	case "query":
		return len(body.Vectors) == 0 && !hasDelete
	case "upsert":
		return !hasQuery && !hasDelete && len(body.Filter) == 0
	default:
		return !hasQuery && len(body.Vectors) == 0
	}
}

func filterShape(fields map[string]any) (*structpb.Struct, error) {
	for key, condition := range fields {
		switch key {
		case "doc_id", "collection", "doc_type", domain.VectorTenantKey, domain.VectorRevisionKey, domain.VectorAttemptKey:
		default:
			return nil, errors.New("unknown filter field")
		}
		operators, ok := condition.(map[string]any)
		if !ok || len(operators) != 1 {
			return nil, errors.New("invalid filter condition")
		}
		for op, operand := range operators {
			if !validOperand(op, operand) {
				return nil, errors.New("invalid filter operand")
			}
		}
	}
	return structpb.NewStruct(fields)
}

func validOperand(op string, operand any) bool {
	switch op {
	case "$eq":
		_, ok := operand.(string)
		return ok
	case "$exists":
		_, ok := operand.(bool)
		return ok
	case "$in":
		values, ok := operand.([]any)
		if !ok || len(values) == 0 || len(values) > 10_000 {
			return false
		}
		for _, value := range values {
			if _, ok := value.(string); !ok {
				return false
			}
		}
		return true
	default:
		return false
	}
}
