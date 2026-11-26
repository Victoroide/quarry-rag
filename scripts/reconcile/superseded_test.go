package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	fetchErr  error
	deleted   [][]string
	deleteErr error
	onDelete  func()
}

func (i *supersededTestIndex) ListVectors(ctx context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
	return i.list(ctx, req)
}

func (i *supersededTestIndex) FetchVectors(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
	if i.fetchErr != nil {
		return nil, i.fetchErr
	}
	got := make(map[string]*pinecone.Vector)
	for _, id := range ids {
		if v, ok := i.vectors[id]; ok {
			got[id] = v
		}
	}
	return &pinecone.FetchVectorsResponse{Namespace: defaultVectorNamespace, Vectors: got}, nil
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
	index := &supersededTestIndex{
		vectors: map[string]*pinecone.Vector{bareTestID: supersededTestVector(t, bareTestID, map[string]any{"doc_id": storedID})},
	}
	index.deleteHook = index.deleteVectors
	next := strings.Repeat("b", 32)
	index.list = func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		if req.PaginationToken == nil {
			return supersededTestList("second", bareTestID), nil
		}
		index.fetchErr = errors.New("fetch unavailable")
		return supersededTestList("", next), nil
	}
	lookup := func(context.Context, []string) (map[string]bool, error) {
		return map[string]bool{storedID: true}, nil
	}
	apply := func(ctx context.Context, ids []string) (bool, error) {
		err := index.DeleteVectorsById(ctx, ids)
		return err == nil, err
	}
	opts := options{Apply: true, Page: 1, Limit: 2, ScanLimit: 2}
	result, err := reconcileSuperseded(t.Context(), index, lookup, apply, "scope", opts)
	require.ErrorContains(t, err, "fetch unavailable")
	token, tokenErr := decodeSupersededPageToken("scope", result.NextPageToken)
	require.NoError(t, tokenErr)
	require.Equal(t, "second", token)
	require.Equal(t, 1, result.Selected)
	require.Equal(t, 2, result.Scanned)
	require.Equal(t, 1, result.Deleted)
	require.Equal(t, []string{bareTestID}, result.Sample)
	require.Equal(t, [][]string{{bareTestID}}, index.deleted)

	data, err := json.Marshal(report{})
	require.NoError(t, err)
	require.NotContains(t, string(data), "superseded_unrevisioned_vectors")
	require.NotContains(t, string(data), "next_page_token")
}

func TestSupersededOptions(t *testing.T) {
	opts, err := parseOptions(nil, t.Output())
	require.NoError(t, err)
	require.False(t, opts.SupersededUnrevisioned)
	opts, err = parseOptions([]string{"-superseded-unrevisioned"}, t.Output())
	require.NoError(t, err)
	require.Equal(t, 100000, opts.ScanLimit)
	confirm := "-confirm-target-drained=" + storedID
	opts, err = parseOptions([]string{"-superseded-unrevisioned", "-apply", confirm}, t.Output())
	require.NoError(t, err)
	require.Equal(t, uuid.MustParse(storedID), opts.ConfirmTargetDrained)
	for _, flags := range [][]string{
		{"-scan-limit=1"}, {"-page-token=token"}, {"-apply", confirm},
		{"-superseded-unrevisioned", confirm},
		{"-superseded-unrevisioned", "-apply", confirm, "-page-token=token"},
		{"-superseded-unrevisioned", "-apply", "-confirm-target-drained"},
		{"-superseded-unrevisioned", "-apply", "-confirm-target-drained=true"},
		{"-superseded-unrevisioned", "-apply", "-confirm-target-drained="},
		{"-superseded-unrevisioned", "-apply", "-confirm-target-drained=" + uuid.Nil.String()},
		{"-superseded-unrevisioned", "-scan-limit=0"},
	} {
		_, err := parseOptions(flags, t.Output())
		require.Error(t, err, fmt.Sprint(flags))
	}
	for _, mode := range []string{"-backfill-collections", "-backfill-scoped", "-canonical-document-ids", "-source-uploads"} {
		_, err := parseOptions([]string{"-superseded-unrevisioned", mode}, t.Output())
		require.Error(t, err, mode)
	}
}
