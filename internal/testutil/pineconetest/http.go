// Package pineconetest provides an HTTP transport for Pinecone SDK types.
package pineconetest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// HTTPIndexConn sends data-plane operations to a test server.
type HTTPIndexConn struct {
	endpoint  url.URL
	http      *http.Client
	namespace string
}

// NewHTTPIndexConn builds a connection for one namespace.
func NewHTTPIndexConn(endpoint url.URL, httpClient *http.Client, namespace string) *HTTPIndexConn {
	return &HTTPIndexConn{endpoint: endpoint, http: httpClient, namespace: namespace}
}

type httpVectorPayload struct {
	ID       string         `json:"id"`
	Values   []float32      `json:"values,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// UpsertVectors stores records in the configured namespace.
func (c *HTTPIndexConn) UpsertVectors(ctx context.Context, vectors []*pinecone.Vector) (uint32, error) {
	payload := struct {
		Namespace string              `json:"namespace"`
		Vectors   []httpVectorPayload `json:"vectors"`
	}{
		Namespace: c.namespace,
		Vectors:   make([]httpVectorPayload, 0, len(vectors)),
	}
	for _, v := range vectors {
		var values []float32
		if v.Values != nil {
			values = *v.Values
		}
		var meta map[string]any
		if v.Metadata != nil {
			meta = v.Metadata.AsMap()
		}
		payload.Vectors = append(payload.Vectors, httpVectorPayload{ID: v.Id, Values: values, Metadata: meta})
	}
	var out struct {
		UpsertedCount uint32 `json:"upsertedCount"`
	}
	if err := c.postJSON(ctx, "/vectors/upsert", payload, &out); err != nil {
		return 0, err
	}
	return out.UpsertedCount, nil
}

// QueryByVectorValues sends a ranked vector query and decodes its matches.
func (c *HTTPIndexConn) QueryByVectorValues(ctx context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
	var filterMap map[string]any
	if req.MetadataFilter != nil {
		filterMap = req.MetadataFilter.AsMap()
	}
	payload := struct {
		Namespace       string         `json:"namespace"`
		Vector          []float32      `json:"vector"`
		TopK            uint32         `json:"topK"`
		IncludeValues   bool           `json:"includeValues"`
		IncludeMetadata bool           `json:"includeMetadata"`
		Filter          map[string]any `json:"filter,omitempty"`
	}{
		Namespace:       c.namespace,
		Vector:          req.Vector,
		TopK:            req.TopK,
		IncludeValues:   req.IncludeValues,
		IncludeMetadata: req.IncludeMetadata,
		Filter:          filterMap,
	}
	var raw struct {
		Matches []struct {
			ID       string         `json:"id"`
			Score    float32        `json:"score"`
			Values   []float32      `json:"values"`
			Metadata map[string]any `json:"metadata"`
		} `json:"matches"`
	}
	if err := c.postJSON(ctx, "/query", payload, &raw); err != nil {
		return nil, err
	}
	matches := make([]*pinecone.ScoredVector, 0, len(raw.Matches))
	for _, m := range raw.Matches {
		var meta *pinecone.Metadata
		if m.Metadata != nil {
			meta, _ = structpb.NewStruct(m.Metadata)
		}
		var values *[]float32
		if len(m.Values) > 0 {
			cp := append([]float32(nil), m.Values...)
			values = &cp
		}
		matches = append(matches, &pinecone.ScoredVector{
			Vector: &pinecone.Vector{Id: m.ID, Values: values, Metadata: meta},
			Score:  m.Score,
		})
	}
	return &pinecone.QueryVectorsResponse{Matches: matches}, nil
}

// DeleteVectorsByFilter removes records matching the supplied metadata.
func (c *HTTPIndexConn) DeleteVectorsByFilter(ctx context.Context, filter *structpb.Struct) error {
	var filterMap map[string]any
	if filter != nil {
		filterMap = filter.AsMap()
	}
	payload := struct {
		Namespace string         `json:"namespace"`
		Filter    map[string]any `json:"filter,omitempty"`
	}{Namespace: c.namespace, Filter: filterMap}
	return c.postJSON(ctx, "/vectors/delete", payload, nil)
}

// DeleteVectorsByID removes the supplied physical record IDs.
func (c *HTTPIndexConn) DeleteVectorsByID(ctx context.Context, ids []string) error {
	payload := struct {
		Namespace string   `json:"namespace"`
		IDs       []string `json:"ids,omitempty"`
	}{Namespace: c.namespace, IDs: ids}
	return c.postJSON(ctx, "/vectors/delete", payload, nil)
}

// DeleteAllVectorsInNamespace requests removal of the entire namespace.
func (c *HTTPIndexConn) DeleteAllVectorsInNamespace(ctx context.Context) error {
	payload := struct {
		Namespace string `json:"namespace"`
		DeleteAll bool   `json:"deleteAll"`
	}{Namespace: c.namespace, DeleteAll: true}
	return c.postJSON(ctx, "/vectors/delete", payload, nil)
}

// FetchVectors looks up records by their physical IDs.
func (c *HTTPIndexConn) FetchVectors(ctx context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
	q := url.Values{}
	q.Set("namespace", c.namespace)
	for _, id := range ids {
		q.Add("ids", id)
	}
	var raw struct {
		Vectors map[string]httpVectorPayload `json:"vectors"`
	}
	if err := c.getJSON(ctx, "/vectors/fetch?"+q.Encode(), &raw); err != nil {
		return nil, err
	}
	out := make(map[string]*pinecone.Vector, len(raw.Vectors))
	for id, v := range raw.Vectors {
		var meta *pinecone.Metadata
		if v.Metadata != nil {
			meta, _ = structpb.NewStruct(v.Metadata)
		}
		out[id] = &pinecone.Vector{Id: id, Metadata: meta}
	}
	return &pinecone.FetchVectorsResponse{Vectors: out}, nil
}

// ListVectors returns a page of physical IDs for the namespace.
func (c *HTTPIndexConn) ListVectors(ctx context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
	q := url.Values{}
	q.Set("namespace", c.namespace)
	if req.Prefix != nil {
		q.Set("prefix", *req.Prefix)
	}
	if req.Limit != nil {
		q.Set("limit", strconv.FormatUint(uint64(*req.Limit), 10))
	}
	if req.PaginationToken != nil {
		q.Set("paginationToken", *req.PaginationToken)
	}
	var raw struct {
		Vectors []struct {
			ID string `json:"id"`
		} `json:"vectors"`
		Pagination *struct {
			Next string `json:"next"`
		} `json:"pagination"`
	}
	if err := c.getJSON(ctx, "/vectors/list?"+q.Encode(), &raw); err != nil {
		return nil, err
	}
	items := make([]*string, 0, len(raw.Vectors))
	for _, v := range raw.Vectors {
		id := v.ID
		items = append(items, &id)
	}
	var next *string
	if raw.Pagination != nil && raw.Pagination.Next != "" {
		n := raw.Pagination.Next
		next = &n
	}
	return &pinecone.ListVectorsResponse{VectorIds: items, NextPaginationToken: next}, nil
}

// Close leaves the caller-owned HTTP client open.
func (c *HTTPIndexConn) Close() error { return nil }

func (c *HTTPIndexConn) postJSON(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	u := c.endpoint
	u.Path = strings.TrimRight(u.Path, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.doJSON(req, out)
}

func (c *HTTPIndexConn) getJSON(ctx context.Context, pathAndQuery string, out any) error {
	rel, err := url.Parse(pathAndQuery)
	if err != nil {
		return err
	}
	u := c.endpoint
	u.Path = strings.TrimRight(u.Path, "/") + rel.Path
	u.RawQuery = rel.RawQuery
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return err
	}
	return c.doJSON(req, out)
}

func (c *HTTPIndexConn) doJSON(req *http.Request, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	if resp.StatusCode == http.StatusNotImplemented {
		return status.Error(codes.Unimplemented, string(data))
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return status.Error(codes.Unavailable, fmt.Sprintf("http %d: %s", resp.StatusCode, string(data)))
	}
	if resp.StatusCode >= 400 {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("http %d: %s", resp.StatusCode, string(data)))
	}
	if out != nil && len(bytes.TrimSpace(data)) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}
