package pinecone

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type listedIndexConn struct {
	*fakeIndexConn
	list func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error)
}

func (c listedIndexConn) ListVectors(ctx context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
	return c.list(ctx, req)
}

func TestRevisionCleanupListsEntireQualifiedPrefix(t *testing.T) {
	base := &fakeIndexConn{filterDelete: func(context.Context, *structpb.Struct) error {
		return status.Error(codes.Unimplemented, "filter unavailable")
	}}
	var deleted []string
	base.deleteIDs = func(_ context.Context, ids []string) error { deleted = append(deleted, ids...); return nil }
	c := testClient(base)
	c.defaultConn = listedIndexConn{base, func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		if req.Prefix == nil || *req.Prefix != "document/revision/" {
			t.Fatalf("unqualified list request: %+v", req)
		}
		if req.PaginationToken == nil {
			if len(deleted) == 2 {
				return &pinecone.ListVectorsResponse{}, nil
			}
			first, next := "document/revision/first", "second-page"
			return &pinecone.ListVectorsResponse{VectorIds: []*string{&first}, NextPaginationToken: &next}, nil
		}
		if *req.PaginationToken != "second-page" {
			t.Fatalf("unexpected token: %s", *req.PaginationToken)
		}
		last := "document/revision/last"
		return &pinecone.ListVectorsResponse{VectorIds: []*string{&last}}, nil
	}}
	if err := c.DeleteByRevision(t.Context(), "document", "revision"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(deleted, ",") != "document/revision/first,document/revision/last" {
		t.Fatalf("incomplete enumeration: %v", deleted)
	}
}

func TestRevisionCleanupRequiresEmptyPrefixAfterDelete(t *testing.T) {
	const prefix = "document/incarnation/"
	vectorID := prefix + "chunk"
	visible := true
	acknowledgments := 0
	base := &fakeIndexConn{
		filterDelete: func(context.Context, *structpb.Struct) error {
			return status.Error(codes.Unimplemented, "filter unavailable")
		},
		deleteIDs: func(_ context.Context, ids []string) error {
			require.Equal(t, []string{vectorID}, ids)
			acknowledgments++
			visible = acknowledgments == 1
			return nil
		},
	}
	pine := testClient(base)
	pine.defaultConn = listedIndexConn{base, func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		require.Equal(t, prefix, *req.Prefix)
		if visible {
			return &pinecone.ListVectorsResponse{VectorIds: []*string{&vectorID}}, nil
		}
		return &pinecone.ListVectorsResponse{}, nil
	}}
	require.ErrorContains(t, pine.DeleteByRevision(t.Context(), "document", "incarnation"), "remain visible")
	require.True(t, visible)
	require.NoError(t, pine.DeleteByRevision(t.Context(), "document", "incarnation"))
	require.False(t, visible)
}

func TestRevisionCleanupProgressBeyondHundredPages(t *testing.T) {
	const prefix = "doc/old/"
	const total = 10001
	const neighbor = "doc/new/10000"
	live := map[string]bool{neighbor: true}
	for i := range total {
		live[prefix+strconv.Itoa(i)] = true
	}
	base := &fakeIndexConn{
		filterDelete: func(context.Context, *structpb.Struct) error {
			return status.Error(codes.Unimplemented, "filter delete unavailable")
		},
		deleteIDs: func(_ context.Context, ids []string) error {
			for _, id := range ids {
				delete(live, id)
			}
			return nil
		},
	}
	failLastPage := true
	fromStart := 0
	c := testClient(base)
	c.defaultConn = listedIndexConn{base, func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		require.Equal(t, prefix, *req.Prefix)
		require.EqualValues(t, 100, *req.Limit)
		var offset int
		if req.PaginationToken == nil {
			fromStart++
		} else {
			var err error
			offset, err = strconv.Atoi(*req.PaginationToken)
			require.NoError(t, err)
		}
		if failLastPage && offset == total-1 {
			return nil, status.Error(codes.Unavailable, "page 101 unavailable")
		}
		var page pinecone.ListVectorsResponse
		for offset < total && len(page.VectorIds) < int(*req.Limit) {
			if id := prefix + strconv.Itoa(offset); live[id] {
				page.VectorIds = append(page.VectorIds, &id)
			}
			offset++
		}
		if offset < total {
			page.NextPaginationToken = new(strconv.Itoa(offset))
		}
		return &page, nil
	}}
	require.ErrorContains(t, c.DeleteByRevision(t.Context(), "doc", "old"), "page 101 unavailable")
	require.Equal(t, map[string]bool{prefix + "10000": true, neighbor: true}, live)
	failLastPage = false
	require.NoError(t, c.DeleteByRevision(t.Context(), "doc", "old"))
	require.Equal(t, map[string]bool{neighbor: true}, live)
	require.Equal(t, 3, fromStart)
}

func TestRevisionCleanupRejectsInvalidListing(t *testing.T) {
	for _, scenario := range []string{"repeated token", "other revision", "empty suffix"} {
		t.Run(scenario, func(t *testing.T) {
			const prefix = "doc/old/"
			var deleted []string
			base := &fakeIndexConn{
				filterDelete: func(context.Context, *structpb.Struct) error {
					return status.Error(codes.Unimplemented, "filter unavailable")
				},
				deleteIDs: func(_ context.Context, ids []string) error {
					deleted = append(deleted, ids...)
					return nil
				},
			}
			c := testClient(base)
			c.defaultConn = listedIndexConn{base, func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
				if req.PaginationToken == nil {
					return &pinecone.ListVectorsResponse{
						VectorIds: []*string{new(prefix + "first")}, NextPaginationToken: new("next"),
					}, nil
				}
				page := &pinecone.ListVectorsResponse{VectorIds: []*string{new(prefix + "second")}}
				switch scenario {
				case "repeated token":
					page.NextPaginationToken = new("next")
				case "other revision":
					page.VectorIds[0] = new("doc/other/chunk")
				case "empty suffix":
					page.VectorIds[0] = new(prefix)
				}
				return page, nil
			}}
			require.Error(t, c.DeleteByRevision(t.Context(), "doc", "old"))
			require.Equal(t, []string{prefix + "first"}, deleted)
		})
	}
}

func TestLegacyCleanupOnSelectiveIndex(t *testing.T) {
	for _, scenario := range []string{"verified", "ambiguous", "full stale page"} {
		t.Run(scenario, func(t *testing.T) {
			const doc = "document"
			legacy := func(id, docID string) *pinecone.ScoredVector {
				return &pinecone.ScoredVector{Vector: &pinecone.Vector{Id: id, Metadata: &structpb.Struct{
					Fields: map[string]*structpb.Value{"doc_id": structpb.NewStringValue(docID)},
				}}}
			}
			tagged := legacy("document/current/chunk", doc)
			tagged.Vector.Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue("current")
			live := map[string]bool{tagged.Vector.Id: true}
			page := []*pinecone.ScoredVector{legacy("legacy", doc), tagged}
			page[0].Vector.Metadata.Fields[domain.VectorIncarnationKey] = structpb.NewStringValue("current-untagged")
			live["legacy"] = true
			if scenario == "ambiguous" {
				page[0] = legacy("unverified", "")
			}
			if scenario == "full stale page" {
				page = nil
				for i := range cleanupPageSize {
					id := fmt.Sprint(i)
					live[id] = true
					page = append(page, legacy(id, doc))
				}
				live["hidden"] = true
			}
			conn := &fakeIndexConn{
				filterDelete: func(_ context.Context, filter *structpb.Struct) error {
					require.Equal(t, map[string]any{"$exists": false}, filter.AsMap()[domain.VectorRevisionKey])
					return status.Error(codes.InvalidArgument, "document_revision is not indexed")
				},
				query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
					require.True(t, req.IncludeMetadata)
					require.Equal(t, map[string]any{"doc_id": map[string]any{"$eq": doc}}, req.MetadataFilter.AsMap())
					if scenario == "verified" && !live["legacy"] {
						return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{tagged}}, nil
					}
					return &pinecone.QueryVectorsResponse{Matches: page}, nil
				},
				deleteIDs: func(_ context.Context, ids []string) error {
					for _, id := range ids {
						delete(live, id)
					}
					return nil
				},
			}
			err := testClient(conn).DeleteByDocID(domain.TrackVectorMutation(t.Context()), doc)
			switch scenario {
			case "verified":
				require.NoError(t, err)
				require.Equal(t, map[string]bool{tagged.Vector.Id: true}, live)
			case "ambiguous":
				require.ErrorContains(t, err, "cannot verify")
				require.True(t, live["legacy"])
			case "full stale page":
				require.ErrorContains(t, err, "full page")
				require.True(t, live["hidden"])
				require.False(t, live[fmt.Sprint(cleanupPageSize-1)])
			}
			require.True(t, live[tagged.Vector.Id])
		})
	}
}

func TestTaggedVectorsKeepLogicalIDsAndSurfaceUnknownReply(t *testing.T) {
	const revision = "current"
	chunk := testChunks(1, 2, 5)[0]
	chunk.DocumentID = "document"
	chunk.Metadata = map[string]any{domain.VectorRevisionKey: revision}
	calls := 0
	conn := &fakeIndexConn{upsert: func(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
		calls++
		require.Len(t, vectors, 1)
		vec := vectors[0]
		require.Equal(t, "document/current/"+string(chunk.ID), vec.Id)
		require.Equal(t, revision, vec.Metadata.Fields[domain.VectorRevisionKey].GetStringValue())
		hits := matchesToScoredChunks([]*pinecone.ScoredVector{{Vector: vec}}, "")
		require.Len(t, hits, 1)
		require.Equal(t, chunk.ID, hits[0].Chunk.ID)
		require.Equal(t, revision, hits[0].Chunk.Metadata[domain.VectorRevisionKey])
		return 0, status.Error(codes.DeadlineExceeded, "unknown reply")
	}}
	err := testClient(conn).Upsert(domain.TrackVectorMutation(t.Context()), "", []domain.Chunk{chunk})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Equal(t, 1, calls)
}

func FuzzTaggedVectorID(f *testing.F) {
	f.Add("doc", "revision", "chunk")
	f.Add("doc", "revision", "nested/chunk")
	f.Add("doc/neighbor", "revision", "chunk")
	f.Fuzz(func(t *testing.T, doc, revision, id string) {
		_, err := physicalVectorID(domain.Chunk{
			ID: domain.ChunkID(id), DocumentID: domain.DocumentID(doc),
			Metadata: map[string]any{domain.VectorAttemptKey: revision},
		})
		require.Error(t, err)

		target := vectorTarget{document: doc, revision: revision}
		logical, ok := target.logicalID(doc + "/" + revision + "/" + id)
		if doc == "" || revision == "" || id == "" ||
			strings.Contains(doc, "/") || strings.Contains(revision, "/") || strings.Contains(id, "/") {
			require.False(t, ok)
			return
		}
		require.True(t, ok)
		require.Equal(t, id, logical)
	})
}

func TestTrackedRevisionDeleteRetriesRepeatedFullPage(t *testing.T) {
	const doc, revision = "document", "retired"
	filterCalls := 0
	const total = cleanupPageSize + 1
	visible := make(map[string]bool, total)
	for i := range total {
		visible[fmt.Sprintf("%s/%s/%d", doc, revision, i)] = true
	}
	stale := true
	conn := &fakeIndexConn{
		filterDelete: func(_ context.Context, filter *structpb.Struct) error {
			filterCalls++
			require.Equal(t, map[string]any{
				"doc_id": map[string]any{"$eq": doc}, domain.VectorRevisionKey: map[string]any{"$eq": revision},
			}, filter.AsMap())
			return status.Error(codes.Unimplemented, "filter deletion unavailable")
		},
		query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			require.True(t, req.IncludeMetadata)
			var page pinecone.QueryVectorsResponse
			for i := range total {
				match := legacyMatch(fmt.Sprintf("%s/%s/%d", doc, revision, i))
				match.Vector.Metadata.Fields["doc_id"] = structpb.NewStringValue(doc)
				match.Vector.Metadata.Fields[domain.VectorRevisionKey] = structpb.NewStringValue(revision)
				if stale && i < cleanupPageSize || !stale && visible[match.Vector.Id] {
					page.Matches = append(page.Matches, match)
				}
			}
			return &page, nil
		},
		deleteIDs: func(_ context.Context, ids []string) error {
			for _, id := range ids {
				delete(visible, id)
			}
			return nil
		},
	}
	ctx := domain.TrackVectorMutation(t.Context())
	c := testClient(conn)
	c.ownershipFilters = ownershipFilters{document: true, revision: true, attempt: true}
	c.defaultConn = listedIndexConn{conn, func(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		return nil, status.Error(codes.Unimplemented, "listing unsupported")
	}}
	require.ErrorContains(t, c.DeleteByRevision(ctx, doc, revision), "full page of stale results")
	require.Equal(t, map[string]bool{fmt.Sprintf("%s/%s/%d", doc, revision, cleanupPageSize): true}, visible)
	stale = false
	require.NoError(t, c.DeleteByRevision(ctx, doc, revision))
	require.Empty(t, visible)
	require.Equal(t, 2, filterCalls)
}

func TestScopedSearchKeepsBothIncarnationsForVisibilityCheck(t *testing.T) {
	current := &pinecone.Vector{Id: "doc/current/chunk", Metadata: &structpb.Struct{
		Fields: map[string]*structpb.Value{"doc_id": structpb.NewStringValue("doc"), domain.VectorRevisionKey: structpb.NewStringValue("current")},
	}}
	previous := &pinecone.Vector{Id: "doc/previous/chunk", Metadata: &structpb.Struct{
		Fields: map[string]*structpb.Value{"doc_id": structpb.NewStringValue("doc"), domain.VectorRevisionKey: structpb.NewStringValue("previous")},
	}}
	conn := &fakeIndexConn{query: func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{
			{Vector: previous, Score: 1}, {Vector: current, Score: 0.8},
		}}, nil
	}}
	hits, err := testClient(conn).QueryFiltered(t.Context(), []float32{1}, 2, domain.VectorFilter{DocumentIDs: []string{"doc"}})
	require.NoError(t, err)
	require.Len(t, hits, 2)
	require.Equal(t, hits[0].Chunk.ID, hits[1].Chunk.ID)
	require.Equal(t, "current", hits[1].Chunk.Metadata[domain.VectorRevisionKey])
}

func TestDelayedHostedMutationsKeepReplacementPhysicalVector(t *testing.T) {
	for _, operation := range []string{"upsert", "cleanup"} {
		t.Run(operation, func(t *testing.T) {
			const docID, oldRevision, newRevision = "document", "original", "replacement"
			const oldID, newID = "document/original/chunk", "document/replacement/chunk"
			entered, resume := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			index := make(map[string]*pinecone.Vector)
			var mu sync.Mutex
			conn := &fakeIndexConn{
				upsert: func(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
					if operation == "upsert" && vectors[0].Id == oldID {
						close(entered)
						<-resume
					}
					mu.Lock()
					defer mu.Unlock()
					var upserted uint32
					for _, vector := range vectors {
						index[vector.Id] = vector
						upserted++
					}
					return upserted, nil
				},
				filterDelete: func(_ context.Context, filter *structpb.Struct) error {
					close(entered)
					<-resume
					mu.Lock()
					defer mu.Unlock()
					for id, entry := range index {
						fields := filter.GetFields()
						document := fields["doc_id"].GetStructValue().GetFields()["$eq"].GetStringValue()
						revision := fields[domain.VectorRevisionKey].GetStructValue().GetFields()["$eq"].GetStringValue()
						md := entry.Metadata.GetFields()
						if md["doc_id"].GetStringValue() == document && md[domain.VectorRevisionKey].GetStringValue() == revision {
							delete(index, id)
						}
					}
					return nil
				},
			}
			c := testClient(conn)
			c.ownershipFilters = ownershipFilters{document: true, revision: true, attempt: true}
			ctx := domain.TrackVectorMutation(t.Context())
			chunk := func(revision string, value float32) domain.Chunk {
				return domain.Chunk{ID: "chunk", DocumentID: docID, Text: "same body", Embedding: []float32{value},
					Metadata: map[string]any{domain.VectorRevisionKey: revision}}
			}
			if operation == "cleanup" {
				require.NoError(t, c.Upsert(ctx, "", []domain.Chunk{chunk(oldRevision, 0)}))
			}
			done := make(chan error, 1)
			go func() {
				if operation == "upsert" {
					done <- c.Upsert(ctx, "", []domain.Chunk{chunk(oldRevision, 0)})
				} else {
					done <- c.DeleteByRevision(ctx, docID, oldRevision)
				}
			}()
			select {
			case <-entered:
			case err := <-done:
				t.Fatalf("provider call never reached its delayed effect: %v", err)
			case <-time.After(time.Second):
				t.Fatal("provider call never reached its delayed effect")
			}
			require.NoError(t, c.Upsert(ctx, "", []domain.Chunk{chunk(newRevision, 1)}))
			unblock()
			require.NoError(t, <-done)
			require.Contains(t, index, newID)
			require.Equal(t, []float32{1}, *index[newID].Values)
			if operation == "cleanup" {
				require.NotContains(t, index, oldID)
			} else {
				require.Len(t, index, 2)
			}
		})
	}
}
