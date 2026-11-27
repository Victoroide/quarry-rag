package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const bareTestID = "0123456789abcdef0123456789abcdef"

func supersededTestVector(t *testing.T, id string, fields map[string]any) *pinecone.Vector {
	t.Helper()
	md, err := structpb.NewStruct(fields)
	require.NoError(t, err)
	return &pinecone.Vector{Id: id, Metadata: md}
}

func TestSelectSupersededUnrevisionedVectors(t *testing.T) {
	tagged, err := pcstore.TaggedVectorID(storedID, missingID, storedID, bareTestID)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, id, key string
		value         any
		want          bool
	}{
		{name: "bare", id: bareTestID, want: true},
		{name: "retired incarnation", id: bareTestID, key: domain.VectorIncarnationKey, value: missingID, want: true},
		{name: "current incarnation", id: bareTestID, key: domain.VectorIncarnationKey, value: storedID, want: true},
		{name: "malformed incarnation", id: bareTestID, key: domain.VectorIncarnationKey, value: 42},
		{name: "empty incarnation", id: bareTestID, key: domain.VectorIncarnationKey, value: ""},
		{name: "legacy revision", id: bareTestID, key: "revision_id", value: ""},
		{name: "active or writing revision", id: bareTestID, key: domain.VectorRevisionKey, value: storedID},
		{name: "null revision", id: bareTestID, key: domain.VectorRevisionKey},
		{name: "tracked attempt", id: bareTestID, key: domain.VectorAttemptKey, value: map[string]any{}},
		{name: "qualified revision ID", id: domain.FormatRevisionVectorID(storedID, missingID, 0)},
		{name: "qualified attempt ID", id: tagged},
		{name: "hyphenated ID", id: storedID},
		{name: "uppercase ID", id: strings.ToUpper(bareTestID)},
		{name: "empty ID"},
		{name: "nonhex ID", id: strings.Repeat("z", 32)},
		{name: "missing document", id: bareTestID, key: "doc_id", value: nil},
		{name: "malformed document", id: bareTestID, key: "doc_id", value: "owner"},
		{name: "mistyped document", id: bareTestID, key: "doc_id", value: 42},
		{name: "document alias", id: bareTestID, key: "doc_id", value: "{" + storedID + "}", want: true},
		{name: "invalid alias wrapper", id: bareTestID, key: "doc_id", value: "[" + storedID + "]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md := map[string]any{"doc_id": storedID}
			if tc.key != "" {
				md[tc.key] = tc.value
			}
			got := supersededVectorDocument(tc.id, supersededTestVector(t, tc.id, md))
			if tc.want {
				require.Equal(t, storedID, got)
			} else {
				require.Empty(t, got)
			}
		})
	}
	require.Empty(t, supersededVectorDocument(bareTestID, &pinecone.Vector{Id: bareTestID}))
}

func FuzzSupersededVectorReference(f *testing.F) {
	f.Add(bareTestID, storedID, "", "")
	f.Add(storedID+":"+bareTestID, missingID, domain.VectorRevisionKey, "")
	f.Fuzz(func(t *testing.T, key, owner, metadataKey, metadataValue string) {
		fields := map[string]*structpb.Value{"doc_id": structpb.NewStringValue(owner)}
		if metadataKey != "" {
			fields[metadataKey] = structpb.NewStringValue(metadataValue)
		}
		got := supersededVectorDocument(key, &pinecone.Vector{Id: key, Metadata: &structpb.Struct{Fields: fields}})
		if got == "" {
			return
		}
		require.Len(t, key, 32)
		require.Equal(t, strings.ToLower(key), key)
		require.NoError(t, uuid.Validate(fields["doc_id"].GetStringValue()))
		id, err := uuid.Parse(got)
		require.NoError(t, err)
		require.Equal(t, id.String(), got)
		for _, field := range []string{"revision_id", domain.VectorRevisionKey, domain.VectorAttemptKey} {
			require.NotContains(t, fields, field)
		}
	})
}

type supersededTestIndex struct {
	reconcileFakeIndex
	list      func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error)
	vectors   map[string]*pinecone.Vector
	fetch     func(context.Context, []string) (*pinecone.FetchVectorsResponse, error)
	deleted   [][]string
	deleteErr error
	onDelete  func()
}

func (i *supersededTestIndex) ListVectors(ctx context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
	return i.list(ctx, req)
}

func (i *supersededTestIndex) FetchVectors(ctx context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
	if i.fetch != nil {
		return i.fetch(ctx, ids)
	}
	return i.fetchListedVectors(ids), nil
}

func (i *supersededTestIndex) fetchListedVectors(ids []string) *pinecone.FetchVectorsResponse {
	got := make(map[string]*pinecone.Vector)
	for _, id := range ids {
		if v, ok := i.vectors[id]; ok {
			got[id] = v
		}
	}
	return &pinecone.FetchVectorsResponse{Namespace: defaultVectorNamespace, Vectors: got}
}

func (i *supersededTestIndex) deleteVectors(ids []string) error {
	i.deleted = append(i.deleted, ids)
	if i.onDelete != nil {
		i.onDelete()
	}
	if i.deleteErr == nil {
		for _, id := range ids {
			delete(i.vectors, id)
		}
	}
	return i.deleteErr
}

func supersededTestList(token string, ids ...string) *pinecone.ListVectorsResponse {
	page := &pinecone.ListVectorsResponse{Namespace: defaultVectorNamespace}
	for _, id := range ids {
		page.VectorIds = append(page.VectorIds, new(id))
	}
	if token != "" {
		page.NextPaginationToken = &token
	}
	return page
}

func supersededTestCursor(token string) string {
	return "scope." + base64.RawURLEncoding.EncodeToString([]byte(token))
}

func TestReconcileSupersededUnrevisionedPaging(t *testing.T) {
	ctx := t.Context()
	lookup := func(context.Context, []string) (map[string]bool, error) {
		return map[string]bool{storedID: true}, nil
	}
	scope := "bound-index"
	live := bareTestID
	stale := strings.Repeat("b", 32)
	next := strings.Repeat("c", 32)
	index := &supersededTestIndex{
		vectors: map[string]*pinecone.Vector{
			live: supersededTestVector(t, live, map[string]any{"doc_id": storedID}),
			next: supersededTestVector(t, next, map[string]any{"doc_id": storedID}),
		},
		list: func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
			switch {
			case req.PaginationToken == nil:
				require.Equal(t, uint32(3), *req.Limit)
				return supersededTestList("second", stale, live, live), nil
			case *req.PaginationToken == "second":
				require.Equal(t, uint32(1), *req.Limit)
				return supersededTestList("third"), nil
			default:
				require.Equal(t, "third", *req.PaginationToken)
				require.Equal(t, uint32(1), *req.Limit)
				return supersededTestList("", next), nil
			}
		},
	}
	opts := options{Page: 8, Limit: 3, ScanLimit: 4}
	result, err := reconcileSuperseded(ctx, index, lookup, nil, scope, opts)
	require.NoError(t, err)
	require.Equal(t, []string{live, next}, result.Sample)
	require.Equal(t, 4, result.Scanned)
	require.Zero(t, result.Deleted)
	require.Empty(t, index.deleted)

	opts.ScanLimit = 3
	result, err = reconcileSuperseded(ctx, index, lookup, nil, scope, opts)
	require.NoError(t, err)
	require.Equal(t, 1, result.Selected)
	require.NotEmpty(t, result.NextPageToken)
	opts.PageToken = result.NextPageToken
	_, err = reconcileSuperseded(ctx, index, lookup, nil, "other-index", opts)
	require.ErrorContains(t, err, "bound index")
	opts.Limit = 1
	result, err = reconcileSuperseded(ctx, index, lookup, nil, scope, opts)
	require.NoError(t, err)
	require.Equal(t, []string{next}, result.Sample)
	require.Empty(t, result.NextPageToken)
}

func TestSupersededListFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		page *pinecone.ListVectorsResponse
		err  error
		want string
	}{
		{"nil", nil, nil, "invalid vector listing"},
		{"unsupported", nil, status.Error(codes.Unimplemented, "list"), "listing"},
		{"repeat", supersededTestList("same", bareTestID), nil, "repeated pagination token"},
		{"malformed", supersededTestList("bad\ntoken", bareTestID), nil, "malformed pagination token"},
		{"namespace", &pinecone.ListVectorsResponse{Namespace: storedID}, nil, "invalid vector listing"},
		{"overfull", supersededTestList("", bareTestID, bareTestID), nil, "invalid vector listing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := &supersededTestIndex{list: func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
				return tc.page, tc.err
			}}
			lookup := func(context.Context, []string) (map[string]bool, error) { return nil, nil }
			opts := options{Apply: true, Page: 1, ScanLimit: 2, Limit: 1}
			_, err := reconcileSuperseded(t.Context(), index, lookup, func(context.Context, []string) (bool, error) {
				return false, errors.New("unexpected deletion")
			}, "scope", opts)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestSupersededFetchFailurePreservesPartialProgress(t *testing.T) {
	next, last := strings.Repeat("b", 32), strings.Repeat("a", 32)
	index := &supersededTestIndex{vectors: make(map[string]*pinecone.Vector)}
	for _, id := range []string{bareTestID, next, last} {
		index.vectors[id] = supersededTestVector(t, id, map[string]any{"doc_id": storedID})
	}
	index.deleteHook = index.deleteVectors
	index.list = func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		if req.PaginationToken == nil {
			return supersededTestList("second", bareTestID), nil
		}
		require.Equal(t, "second", *req.PaginationToken)
		return supersededTestList("third", next, bareTestID, last), nil
	}
	var fetches [][]string
	unavailable := status.Error(codes.Unavailable, "fetch unavailable")
	fail := true
	index.fetch = func(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
		fetches = append(fetches, slices.Clone(ids))
		if len(ids) > 1 {
			return nil, supersededTestSizeError()
		}
		if fail && ids[0] == last {
			return nil, unavailable
		}
		return index.fetchListedVectors(ids), nil
	}
	var lookups [][]string
	lookup := func(_ context.Context, docs []string) (map[string]bool, error) {
		lookups = append(lookups, slices.Clone(docs))
		return map[string]bool{storedID: true}, nil
	}
	apply := func(ctx context.Context, ids []string) (bool, error) {
		err := index.DeleteVectorsById(ctx, ids)
		return err == nil, err
	}
	opts := options{Apply: true, Page: 3, Limit: 4, ScanLimit: 4}
	result, err := reconcileSuperseded(t.Context(), index, lookup, apply, "scope", opts)
	require.ErrorIs(t, err, unavailable)
	token, tokenErr := decodeSupersededPageToken("scope", result.NextPageToken)
	require.NoError(t, tokenErr)
	require.Equal(t, "second", token)
	require.Equal(t, 1, result.Selected)
	require.Equal(t, 4, result.Scanned)
	require.Equal(t, 1, result.Deleted)
	require.Equal(t, []string{bareTestID}, result.Sample)
	require.Equal(t, [][]string{{bareTestID}}, index.deleted)
	require.Equal(t, [][]string{{bareTestID}, {next, last}, {next}, {last}}, fetches)
	require.Equal(t, [][]string{nil, {storedID}}, lookups)

	opts.PageToken, opts.ScanLimit = result.NextPageToken, 3
	fetches, lookups, fail = nil, nil, false
	result, err = reconcileSuperseded(t.Context(), index, lookup, apply, "scope", opts)
	require.NoError(t, err)
	require.Equal(t, []string{next, last}, result.Sample)
	require.Equal(t, 2, result.Selected)
	require.Equal(t, 2, result.Deleted)
	require.Equal(t, 3, result.Scanned)
	require.Equal(t, [][]string{{bareTestID}, {next, last}}, index.deleted)
	require.Equal(t, [][]string{{next, bareTestID, last}, {next}, {bareTestID, last}, {bareTestID}, {last}}, fetches)
	require.Equal(t, [][]string{nil, {storedID, storedID}}, lookups)
	token, tokenErr = decodeSupersededPageToken("scope", result.NextPageToken)
	require.NoError(t, tokenErr)
	require.Equal(t, "third", token)

	data, err := json.Marshal(report{})
	require.NoError(t, err)
	require.NotContains(t, string(data), "superseded_unrevisioned_vectors")
	require.NotContains(t, string(data), "next_page_token")
}

func supersededTestSizeError() error {
	return status.Error(codes.ResourceExhausted, "grpc: received message larger than max (4194305 vs. 4194304)")
}

func TestSupersededFetchLeafValidation(t *testing.T) {
	first, second, absent := bareTestID, strings.Repeat("b", 32), strings.Repeat("a", 32)
	for _, tc := range []struct {
		name   string
		change func(*pinecone.FetchVectorsResponse) *pinecone.FetchVectorsResponse
		want   string
	}{
		{"foreign sibling", func(res *pinecone.FetchVectorsResponse) *pinecone.FetchVectorsResponse {
			res.Vectors[first] = supersededTestVector(t, first, map[string]any{"doc_id": storedID})
			return res
		}, "unexpected record"},
		{"wrapper namespace", func(res *pinecone.FetchVectorsResponse) *pinecone.FetchVectorsResponse {
			res.Namespace = "other"
			return res
		}, "bound default namespace"},
		{"nil response", func(*pinecone.FetchVectorsResponse) *pinecone.FetchVectorsResponse { return nil }, "bound default namespace"},
		{"nil record", func(res *pinecone.FetchVectorsResponse) *pinecone.FetchVectorsResponse {
			res.Vectors[second] = nil
			return res
		}, "unexpected record"},
		{"mismatched id", func(res *pinecone.FetchVectorsResponse) *pinecone.FetchVectorsResponse {
			res.Vectors[second].Id = absent
			return res
		}, "unexpected record"},
		{"revisioned record and absent id", func(res *pinecone.FetchVectorsResponse) *pinecone.FetchVectorsResponse {
			res.Vectors[second].Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue(storedID)
			return res
		}, ""},
		{"invalid owner", func(res *pinecone.FetchVectorsResponse) *pinecone.FetchVectorsResponse {
			res.Vectors[second].Metadata.Fields["doc_id"] = structpb.NewNumberValue(42)
			return res
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := &supersededTestIndex{
				vectors: map[string]*pinecone.Vector{
					first:  supersededTestVector(t, first, map[string]any{"doc_id": storedID}),
					second: supersededTestVector(t, second, map[string]any{"doc_id": missingID}),
				},
			}
			page := supersededTestList("next", first, second, absent)
			index.list = func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
				return page, nil
			}
			var calls [][]string
			index.fetch = func(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
				calls = append(calls, slices.Clone(ids))
				switch {
				case len(ids) == 3:
					return nil, supersededTestSizeError()
				case ids[0] == second:
					return tc.change(index.fetchListedVectors(ids)), nil
				default:
					return index.fetchListedVectors(ids), nil
				}
			}
			var lookups, deletes [][]string
			lookup := func(_ context.Context, docs []string) (map[string]bool, error) {
				lookups = append(lookups, docs)
				return map[string]bool{storedID: true, missingID: true}, nil
			}
			remove := func(_ context.Context, ids []string) (bool, error) {
				deletes = append(deletes, ids)
				return true, nil
			}
			opts := options{Page: 3, ScanLimit: 3, Limit: 3, PageToken: supersededTestCursor("start")}
			report, err := reconcileSuperseded(t.Context(), index, lookup, remove, "scope", opts)
			require.Equal(t, [][]string{{first, second, absent}, {first}, {second, absent}}, calls)
			require.Equal(t, 3, report.Scanned)
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				require.Equal(t, [][]string{nil}, lookups)
				require.Empty(t, deletes)
				require.Zero(t, report.Selected)
				require.Empty(t, report.Sample)
				require.Equal(t, opts.PageToken, report.NextPageToken)
				return
			}
			require.NoError(t, err)
			require.Equal(t, [][]string{nil, {storedID}}, lookups)
			require.Equal(t, [][]string{{first}}, deletes)
			require.Equal(t, []string{first}, report.Sample)
			require.Equal(t, supersededTestCursor("next"), report.NextPageToken)
		})
	}
}

func TestSupersededFetchStopsOnNonSizeError(t *testing.T) {
	second, third, fourth := strings.Repeat("b", 32), strings.Repeat("c", 32), strings.Repeat("d", 32)
	page := supersededTestList("", bareTestID, second, third, fourth)
	dbFailure := errors.New("owner lookup denied")
	for _, tc := range []struct {
		name        string
		err         error
		failAt      int
		wantFetches int
	}{
		{"permission denied", status.Error(codes.PermissionDenied, "fetch denied"), 1, 1},
		{"provider quota", status.Error(codes.ResourceExhausted, "read units exhausted"), 1, 1},
		{"unavailable left", status.Error(codes.Unavailable, "left unavailable"), 2, 2},
		{"deadline right", status.Error(codes.DeadlineExceeded, "right deadline"), 3, 3},
		{"owner lookup failure", dbFailure, 0, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := &supersededTestIndex{
				vectors: map[string]*pinecone.Vector{bareTestID: supersededTestVector(t, bareTestID, map[string]any{"doc_id": storedID})},
				list: func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
					return page, nil
				},
			}
			var fetches, lookups int
			index.fetch = func(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
				fetches++
				switch fetches {
				case tc.failAt:
					return nil, tc.err
				case 1:
					return nil, supersededTestSizeError()
				default:
					return index.fetchListedVectors(ids), nil
				}
			}
			lookup := func(_ context.Context, docs []string) (map[string]bool, error) {
				lookups++
				if len(docs) != 0 {
					return nil, dbFailure
				}
				return nil, nil
			}
			opts := options{Page: 4, ScanLimit: 4, Limit: 4, PageToken: supersededTestCursor("start")}
			report, err := reconcileSuperseded(t.Context(), index, lookup, func(context.Context, []string) (bool, error) {
				t.Fatal("deleted from failed page")
				return false, nil
			}, "scope", opts)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.wantFetches, fetches)
			wantLookups := 1
			if tc.failAt == 0 {
				wantLookups++
			}
			require.Equal(t, wantLookups, lookups)
			require.Equal(t, supersededReport{Scanned: 4, NextPageToken: opts.PageToken}, report)
		})
	}

	for _, when := range []string{"before fetch", "after oversized response", "between leaves", "after last leaf"} {
		t.Run(when, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wantFetches := map[string]int{"before fetch": 0, "after oversized response": 1, "between leaves": 2, "after last leaf": 3}[when]
			if wantFetches == 0 {
				cancel()
			}
			var fetches int
			index := &supersededTestIndex{
				vectors: map[string]*pinecone.Vector{bareTestID: supersededTestVector(t, bareTestID, map[string]any{"doc_id": storedID})},
			}
			index.fetch = func(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
				fetches++
				if fetches == wantFetches {
					cancel()
				}
				if fetches == 1 {
					return nil, supersededTestSizeError()
				}
				return index.fetchListedVectors(ids), nil
			}
			_, err := selectSupersededPage(ctx, index, func(context.Context, []string) (map[string]bool, error) {
				t.Fatal("looked up owners from canceled page")
				return nil, nil
			}, page.VectorIds, make(map[string]bool))
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, wantFetches, fetches)
		})
	}
	t.Run("expired deadline before fetch", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		_, err := selectSupersededPage(ctx, nil, nil, page.VectorIds, make(map[string]bool))
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestSupersededFetchBisectionBound(t *testing.T) {
	ids := []string{bareTestID, strings.Repeat("b", 32), strings.Repeat("c", 32), strings.Repeat("d", 32), strings.Repeat("e", 32)}
	index := &supersededTestIndex{vectors: make(map[string]*pinecone.Vector)}
	for _, id := range ids {
		index.vectors[id] = supersededTestVector(t, id, map[string]any{"doc_id": storedID})
	}
	var fetches [][]string
	index.fetch = func(_ context.Context, requested []string) (*pinecone.FetchVectorsResponse, error) {
		fetches = append(fetches, slices.Clone(requested))
		if len(requested) > 1 {
			return nil, supersededTestSizeError()
		}
		return index.fetchListedVectors(requested), nil
	}
	page := supersededTestList("", ids...)
	selected, err := selectSupersededPage(t.Context(), index, func(context.Context, []string) (map[string]bool, error) {
		return map[string]bool{storedID: true}, nil
	}, page.VectorIds, make(map[string]bool))
	require.NoError(t, err)
	require.Equal(t, ids, selected)
	require.Len(t, fetches, 2*len(ids)-1)
	require.Equal(t, [][]string{ids, ids[:2], ids[:1], ids[1:2], ids[2:], ids[2:3], ids[3:], ids[3:4], ids[4:]}, fetches)
}

func TestSupersededFetchReceiveSizeError(t *testing.T) {
	const decompressed = "grpc: message after decompression larger than max (4194305 vs. 4194304)"
	detailed, err := status.New(codes.ResourceExhausted, decompressed).WithDetails(&emptypb.Empty{})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"raw", supersededTestSizeError(), true},
		{"decompressed with length", status.Error(codes.ResourceExhausted, decompressed), true},
		{"decompressed without length", status.Error(codes.ResourceExhausted, "grpc: received message after decompression larger than max 4194304"), true},
		{"wrong status", status.Error(codes.Unavailable, decompressed), false},
		{"status details", detailed.Err(), false},
		{"wrapped", fmt.Errorf("provider wrapper: %w", supersededTestSizeError()), false},
		{"plain error", errors.New(decompressed), false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isFetchReceiveSizeError(tc.err))
		})
	}
	for _, message := range []string{
		"grpc: received message larger than max (4194305 vs. 4194303)",
		"grpc: received message larger than max (4194303 vs. 4194304)",
		"grpc: received message larger than max (4194304 vs. 4194304)",
		"grpc: received message larger than max (18446744073709551616 vs. 4194304)",
		"grpc: received message larger than max (4194305 vs. 18446744073709551616)",
		"grpc: received message after decompression larger than max 18446744073709551616",
		"grpc: received message after decompression larger than max 4194305",
		"grpc: received message larger than max (+4194305 vs. 4194304)",
		"grpc: received message larger than max (04194305 vs. 4194304)",
		"grpc: received message larger than max (4194305  vs. 4194304)",
		"grpc: received message larger than max (4194305 vs. 4194304)\n",
		"provider: " + decompressed,
		decompressed + " for this project",
		"grpc: trying to send message larger than max (4194305 vs. 4194304)",
		"grpc: received message larger than max length allowed on current machine (4194305 vs. 4194304)",
		"provider quota exceeded (4194305 vs. 4194304)",
	} {
		require.False(t, isFetchReceiveSizeError(status.Error(codes.ResourceExhausted, message)), "%q", message)
	}
}

func FuzzSupersededFetchReceiveSizeError(f *testing.F) {
	f.Add(uint64(4194305), uint64(4194304), true, "")
	f.Add(uint64(4194303), uint64(4194304), false, "")
	f.Add(uint64(4194305), uint64(4194305), true, "\n")
	f.Fuzz(func(t *testing.T, size, limit uint64, decompressed bool, suffix string) {
		diagnostic := "received message"
		if decompressed {
			diagnostic = "message after decompression"
		}
		message := fmt.Sprintf("grpc: %s larger than max (%d vs. %d)%s", diagnostic, size, limit, suffix)
		want := size > limit && limit == 4194304 && suffix == ""
		require.Equal(t, want, isFetchReceiveSizeError(status.Error(codes.ResourceExhausted, message)))
	})
}

func TestSupersededOptions(t *testing.T) {
	opts, err := parseOptions(nil, t.Output())
	require.NoError(t, err)
	require.False(t, opts.SupersededUnrevisioned)
	opts, err = parseOptions([]string{"-superseded-unrevisioned"}, t.Output())
	require.NoError(t, err)
	require.Equal(t, 100000, opts.ScanLimit)
	confirm := "-confirm-target-drained=" + storedID
	sender := "-confirm-sender-stopped=" + missingID
	apply := []string{"-superseded-unrevisioned", "-apply"}
	opts, err = parseOptions(apply, io.Discard)
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, opts.ConfirmTargetDrained)
	require.Equal(t, uuid.Nil, opts.ConfirmSenderStopped)
	opts, err = parseOptions(append(apply, confirm, sender), t.Output())
	require.NoError(t, err)
	require.Equal(t, uuid.MustParse(storedID), opts.ConfirmTargetDrained)
	require.Equal(t, uuid.MustParse(missingID), opts.ConfirmSenderStopped)
	for _, flags := range [][]string{
		{"-scan-limit=1"}, {"-page-token=token"}, {"-apply", confirm, sender},
		{"-superseded-unrevisioned", confirm, sender},
		append(apply, confirm, sender, "-page-token=token"),
		append(apply, confirm, sender, "-page-token="),
		append(apply, confirm), append(apply, sender),
		append(apply, confirm, sender, confirm), append(apply, sender, confirm, sender),
		append(apply, sender, "-confirm-target-drained"),
		append(apply, confirm, "-confirm-sender-stopped"),
		{"-superseded-unrevisioned", "-scan-limit=0"},
	} {
		_, err := parseOptions(flags, t.Output())
		require.Error(t, err, fmt.Sprint(flags))
	}
	for _, value := range []string{"", "true", "false", "malformed", uuid.Nil.String(),
		strings.ToUpper("abcdefab-cdef-4abc-8def-abcdefabcdef"), strings.ReplaceAll(storedID, "-", ""),
		"urn:uuid:" + storedID, "{" + storedID + "}", strings.Repeat("a", 4096)} {
		_, err := parseOptions(append(apply, sender, "-confirm-target-drained="+value), io.Discard)
		require.Error(t, err)
		_, err = parseOptions(append(apply, confirm, "-confirm-sender-stopped="+value), io.Discard)
		require.Error(t, err)
	}
	for _, mode := range []string{"-backfill-collections", "-backfill-scoped", "-canonical-document-ids", "-source-uploads"} {
		_, err := parseOptions([]string{"-superseded-unrevisioned", mode}, t.Output())
		require.Error(t, err, mode)
	}
}

func FuzzSupersededRecoveryOptions(f *testing.F) {
	f.Add(storedID, missingID, false)
	f.Add(storedID, missingID, true)
	f.Add(uuid.Nil.String(), "", false)
	f.Add("{"+storedID+"}", "true", false)
	f.Fuzz(func(t *testing.T, reservation, sender string, duplicate bool) {
		args := []string{"-superseded-unrevisioned", "-apply", "-confirm-target-drained=" + reservation, "-confirm-sender-stopped=" + sender}
		if duplicate {
			args = append(args, args[3])
		}
		opts, err := parseOptions(args, io.Discard)
		reservationID, reservationErr := uuid.Parse(reservation)
		senderID, senderErr := uuid.Parse(sender)
		if duplicate || reservationErr != nil || senderErr != nil || reservationID == uuid.Nil || senderID == uuid.Nil ||
			reservationID.String() != reservation || senderID.String() != sender {
			require.Error(t, err)
			return
		}
		require.NoError(t, err)
		require.Equal(t, reservationID, opts.ConfirmTargetDrained)
		require.Equal(t, senderID, opts.ConfirmSenderStopped)
	})
}
