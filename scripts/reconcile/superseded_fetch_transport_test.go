package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const supersededDeleteMethod = "/VectorService/Delete"

func supersededWireIndex(t *testing.T, opts options, handle func(string, map[protowire.Number][]string) ([]byte, error)) *pinecone.IndexConnection {
	t.Helper()
	listener, listenErr := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, listenErr)
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		req := &emptypb.Empty{}
		if err := stream.RecvMsg(req); err != nil {
			return err
		}
		method, _ := grpc.MethodFromServerStream(stream)
		fields, err := supersededWireStrings(req.ProtoReflect().GetUnknown())
		if err != nil {
			return err
		}
		wire, err := handle(method, fields)
		if err != nil {
			return err
		}
		reply := &emptypb.Empty{}
		reply.ProtoReflect().SetUnknown(wire)
		return stream.SendMsg(reply)
	}))
	done := make(chan struct{})
	t.Cleanup(func() {
		server.Stop()
		<-done
	})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	pc, err := pinecone.NewClient(pinecone.NewClientParams{ApiKey: "local-test"})
	require.NoError(t, err)
	host := (&url.URL{Scheme: "http", Host: listener.Addr().String()}).String()
	conn, err := pc.Index(pinecone.NewIndexConnParams{Host: host}, reconcileIndexDialOptions(opts)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn.WithNamespace(defaultVectorNamespace)
}

func supersededWireStrings(wire []byte) (map[protowire.Number][]string, error) {
	fields := make(map[protowire.Number][]string)
	for len(wire) > 0 {
		number, kind, n := protowire.ConsumeField(wire)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		if kind == protowire.BytesType {
			_, _, tagSize := protowire.ConsumeTag(wire)
			value, _ := protowire.ConsumeString(wire[tagSize:n])
			fields[number] = append(fields[number], value)
		}
		wire = wire[n:]
	}
	return fields, nil
}

// The SDK's 2025-10 vector messages are internal to its Go module.
func supersededWireBytes(wire []byte, field protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(wire, field, protowire.BytesType), value)
}

func supersededWireList(ids []string, namespace, next string) []byte {
	wire := supersededWireBytes(nil, 3, []byte(namespace))
	for _, id := range ids {
		wire = supersededWireBytes(wire, 1, supersededWireBytes(nil, 1, []byte(id)))
	}
	if next != "" {
		wire = supersededWireBytes(wire, 2, supersededWireBytes(nil, 1, []byte(next)))
	}
	return wire
}

func supersededWireFetch(ids []string, namespace string, values, metadata []byte) []byte {
	wire := supersededWireBytes(nil, 2, []byte(namespace))
	for _, id := range ids {
		vector := supersededWireBytes(nil, 1, []byte(id))
		vector = supersededWireBytes(vector, 2, values)
		vector = supersededWireBytes(vector, 3, metadata)
		entry := supersededWireBytes(nil, 1, []byte(id))
		entry = supersededWireBytes(entry, 2, vector)
		wire = supersededWireBytes(wire, 1, entry)
	}
	return wire
}

func supersededWirePad(wire []byte, size int) []byte {
	// The padding for the boundary cases is several MiB, so removing the tag
	// and length prefix does not change the width of the encoded length.
	const field = 15
	extra := size - len(wire)
	if extra < 0 {
		return nil
	}
	payload := extra - protowire.SizeTag(field) - protowire.SizeVarint(uint64(extra))
	if payload < 0 {
		return nil
	}
	return supersededWireBytes(wire, field, make([]byte, payload))
}

func supersededWireMetadata(t *testing.T, padding int) []byte {
	t.Helper()
	md, err := structpb.NewStruct(map[string]any{"doc_id": storedID, "text": strings.Repeat("m", padding)})
	require.NoError(t, err)
	wire, err := proto.Marshal(md)
	require.NoError(t, err)
	return wire
}

func TestSupersededFetchPageSplitsByReceiveSize(t *testing.T) {
	const receiveLimit = 4 * 1024 * 1024
	for _, tc := range []struct {
		name        string
		records     int
		padding     int
		parentBytes int
		fetchCounts []int
		wantCode    codes.Code
		quota       bool
	}{
		{name: "empty"},
		{name: "one fitting", records: 1, fetchCounts: []int{1}},
		{name: "fitting dense", records: 100, fetchCounts: []int{100}},
		{name: "oversized dense", records: 1000, fetchCounts: []int{1000, 500, 500}},
		{name: "oversized metadata", records: 100, padding: 36 * 1024, fetchCounts: []int{100, 50, 50}},
		{name: "exact receive limit", records: 2, parentBytes: receiveLimit, fetchCounts: []int{2}},
		{name: "one byte over", records: 2, parentBytes: receiveLimit + 1, fetchCounts: []int{2, 1, 1}},
		{name: "oversized singleton", records: 1, parentBytes: receiveLimit + 1, fetchCounts: []int{1}, wantCode: codes.ResourceExhausted},
		{name: "quota", records: 2, fetchCounts: []int{2}, wantCode: codes.ResourceExhausted, quota: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids := make([]string, tc.records)
			for i := range ids {
				ids[i] = fmt.Sprintf("%032x", len(ids)-i)
			}
			values := make([]byte, 1536*4)
			metadata := supersededWireMetadata(t, tc.padding)
			type fetchCall struct {
				ids  []string
				size int
			}
			var mu sync.Mutex
			var fetches []fetchCall
			opts := options{SupersededUnrevisioned: true, Page: 1000, Limit: 1000, ScanLimit: max(1, tc.records), PageToken: supersededTestCursor("start")}
			index := supersededWireIndex(t, opts, func(method string, fields map[protowire.Number][]string) ([]byte, error) {
				switch method {
				case supersededListMethod:
					if !slices.Equal(fields[4], []string{defaultVectorNamespace}) || !slices.Equal(fields[3], []string{"start"}) {
						return nil, status.Error(codes.InvalidArgument, "unexpected list binding")
					}
					next := ""
					if len(ids) > 0 {
						next = "next"
					}
					return supersededWireList(ids, "", next), nil
				case supersededFetchMethod:
					if !slices.Equal(fields[2], []string{defaultVectorNamespace}) {
						return nil, status.Error(codes.InvalidArgument, "unexpected fetch binding")
					}
					requested := fields[1]
					wire := supersededWireFetch(requested, defaultVectorNamespace, values, metadata)
					if tc.parentBytes != 0 && len(requested) == tc.records {
						wire = supersededWirePad(wire, tc.parentBytes)
					}
					mu.Lock()
					fetches = append(fetches, fetchCall{ids: requested, size: len(wire)})
					mu.Unlock()
					if tc.quota {
						return nil, status.Error(codes.ResourceExhausted, "read unit quota exhausted")
					}
					return wire, nil
				default:
					return nil, status.Error(codes.Unimplemented, "unexpected vector method")
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			var lookups int
			got, err := reconcileSuperseded(ctx, index, func(_ context.Context, docs []string) (map[string]bool, error) {
				if len(docs) > 0 {
					require.Len(t, docs, tc.records)
					lookups++
				}
				return map[string]bool{storedID: true}, nil
			}, nil, "scope", opts)
			if tc.wantCode != codes.OK {
				require.Equal(t, tc.wantCode, status.Code(err), "%v", err)
				if tc.quota {
					require.ErrorContains(t, err, "read unit quota exhausted")
				} else {
					require.ErrorContains(t, err, "reducing -page cannot help this record")
				}
				require.Equal(t, supersededReport{Scanned: tc.records, NextPageToken: opts.PageToken}, got)
				require.Zero(t, lookups)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.records, got.Scanned)
				require.Equal(t, tc.records, got.Selected)
				require.Zero(t, got.Deleted)
				if tc.records == 0 {
					require.Empty(t, got.NextPageToken)
					require.Empty(t, got.Sample)
					require.Zero(t, lookups)
				} else {
					require.Equal(t, supersededTestCursor("next"), got.NextPageToken)
					require.Equal(t, ids[:min(supersededSampleLimit, len(ids))], got.Sample)
					require.Equal(t, 1, lookups)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, fetches, len(tc.fetchCounts))
			for i, count := range tc.fetchCounts {
				require.Len(t, fetches[i].ids, count)
			}
			if len(fetches) == 0 {
				return
			}
			require.Equal(t, ids, fetches[0].ids)
			if tc.parentBytes != 0 {
				require.Equal(t, tc.parentBytes, fetches[0].size)
			}
			if len(fetches) == 3 {
				require.Greater(t, fetches[0].size, receiveLimit)
				require.Equal(t, ids[:len(ids)/2], fetches[1].ids)
				require.Equal(t, ids[len(ids)/2:], fetches[2].ids)
				require.LessOrEqual(t, fetches[1].size, receiveLimit)
				require.LessOrEqual(t, fetches[2].size, receiveLimit)
			}
		})
	}
}

func TestSupersededFetchWireNamespace(t *testing.T) {
	ids := []string{bareTestID, strings.Repeat("b", 32)}
	metadata := supersededWireMetadata(t, 0)
	for _, tc := range []struct {
		name, listNamespace, rightNamespace string
		wantFetches                         int
		wantScanned                         int
	}{
		{"foreign list", "other", "", 0, 0},
		{"foreign right leaf", defaultVectorNamespace, "other", 3, 2},
		{"empty aliases", "", "", 3, 2},
		{"explicit default alias", defaultVectorNamespace, defaultVectorNamespace, 3, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var fetches, deletes int
			opts := options{SupersededUnrevisioned: true, Apply: true, Page: 2, ScanLimit: 2, Limit: 2, PageToken: supersededTestCursor("start")}
			index := supersededWireIndex(t, opts, func(method string, fields map[protowire.Number][]string) ([]byte, error) {
				mu.Lock()
				defer mu.Unlock()
				switch method {
				case supersededListMethod:
					return supersededWireList(ids, tc.listNamespace, "next"), nil
				case supersededFetchMethod:
					fetches++
					requested := fields[1]
					namespace := ""
					if len(requested) == 1 && requested[0] == ids[1] {
						namespace = tc.rightNamespace
					}
					wire := supersededWireFetch(requested, namespace, nil, metadata)
					if len(requested) > 1 {
						wire = supersededWirePad(wire, 4*1024*1024+1)
					}
					return wire, nil
				case supersededDeleteMethod:
					deletes++
					if !slices.Equal(fields[1], ids) || !slices.Equal(fields[3], []string{defaultVectorNamespace}) {
						return nil, status.Error(codes.InvalidArgument, "unexpected delete binding")
					}
					return nil, nil
				default:
					return nil, status.Error(codes.Unimplemented, "unexpected vector method")
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			var ownerLookups int
			got, err := reconcileSuperseded(ctx, index, func(_ context.Context, docs []string) (map[string]bool, error) {
				if len(docs) > 0 {
					ownerLookups++
				}
				return map[string]bool{storedID: true}, nil
			}, func(ctx context.Context, selected []string) (bool, error) {
				require.Equal(t, ids, selected)
				err := index.DeleteVectorsById(ctx, selected)
				return err == nil, err
			}, "scope", opts)
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, tc.wantFetches, fetches)
			require.Equal(t, tc.wantScanned, got.Scanned)
			if tc.listNamespace == "other" || tc.rightNamespace == "other" {
				require.Equal(t, codes.DataLoss, status.Code(err), "%v", err)
				require.Equal(t, supersededReport{Scanned: tc.wantScanned, NextPageToken: opts.PageToken}, got)
				require.Zero(t, ownerLookups)
				require.Zero(t, deletes)
				return
			}
			require.NoError(t, err)
			require.Equal(t, supersededReport{Selected: 2, Deleted: 2, Scanned: 2, Sample: ids, NextPageToken: supersededTestCursor("next")}, got)
			require.Equal(t, 1, ownerLookups)
			require.Equal(t, 1, deletes)
		})
	}
	t.Run("other modes leave their namespace handling unchanged", func(t *testing.T) {
		modes := [][]string{nil, {"-apply"}, {"-backfill-collections", "-apply"}, {"-backfill-scoped"}, {"-canonical-document-ids"}, {"-source-uploads"}}
		for _, args := range modes {
			opts, err := parseOptions(args, t.Output())
			require.NoError(t, err)
			require.Empty(t, reconcileIndexDialOptions(opts), "%v", args)
		}
		index := supersededWireIndex(t, options{}, func(_ string, _ map[protowire.Number][]string) ([]byte, error) {
			return supersededWireList(ids, "other", ""), nil
		})
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		list, err := index.ListVectors(ctx, &pinecone.ListVectorsRequest{Limit: new(uint32(2))})
		require.NoError(t, err)
		require.Equal(t, defaultVectorNamespace, list.Namespace)
	})
}

func TestScopedBackfillManifestFetchTransport(t *testing.T) {
	for _, mode := range []string{"split", "foreign", "later failure", "single overflow", "quota"} {
		t.Run(mode, func(t *testing.T) {
			doc := scopedManifest(1, 2)
			ids := doc.Hosted.VectorIDs
			fake := &scopedFakeIndex{}
			fake.add(doc)
			var mu sync.Mutex
			var fetches [][]string
			var updates []string
			opts := options{BackfillScoped: true, Apply: true, Limit: 1}
			index := supersededWireIndex(t, opts, func(method string, fields map[protowire.Number][]string) ([]byte, error) {
				mu.Lock()
				defer mu.Unlock()
				switch method {
				case supersededFetchMethod:
					if !slices.Equal(fields[2], []string{defaultVectorNamespace}) {
						return nil, status.Error(codes.InvalidArgument, "wrong fetch namespace")
					}
					requested := fields[1]
					fetches = append(fetches, requested)
					switch {
					case mode == "quota":
						return nil, status.Error(codes.ResourceExhausted, "request quota exceeded")
					case len(requested) == 2 || mode == "single overflow":
						return supersededWirePad(nil, supersededFetchReceiveLimit+1), nil
					case requested[0] == ids[1] && mode == "later failure":
						return nil, status.Error(codes.Unavailable, "right batch unavailable")
					}
					namespace := ""
					if mode == "foreign" && requested[0] == ids[1] {
						namespace = "foreign"
					}
					values := []byte{0, 0, 128, 63, 0, 0, 0, 0, 0, 0, 0, 0}
					md, err := structpb.NewStruct(fake.vectors[requested[0]])
					if err != nil {
						return nil, err
					}
					metadata, err := proto.Marshal(md)
					if err != nil {
						return nil, err
					}
					return supersededWireFetch(requested, namespace, values, metadata), nil
				case "/VectorService/Update":
					if len(fields[1]) != 1 || len(fields[3]) != 1 || !slices.Equal(fields[4], []string{defaultVectorNamespace}) ||
						len(fields[2]) != 0 || len(fields[5]) != 0 || len(fields[6]) != 0 {
						return nil, status.Error(codes.InvalidArgument, "expected an exact metadata-only update")
					}
					id := fields[1][0]
					md := &structpb.Struct{}
					if err := proto.Unmarshal([]byte(fields[3][0]), md); err != nil {
						return nil, err
					}
					if len(md.Fields) != 2 || md.Fields[domain.VectorTenantKey].GetStringValue() != doc.TenantID ||
						md.Fields[domain.VectorIncarnationKey].GetStringValue() != doc.Incarnation || fake.vectors[id] == nil {
						return nil, status.Error(codes.InvalidArgument, "unexpected metadata update")
					}
					updates = append(updates, id)
					for key, value := range md.AsMap() {
						fake.vectors[id][key] = value
					}
					return nil, nil
				default:
					return nil, status.Error(codes.Unimplemented, method)
				}
			})
			source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
			report, err := backfillScoped(t.Context(), source, hostedVectors{idx: index, dimensions: scopedFixtureDimensions}, opts)
			mu.Lock()
			defer mu.Unlock()
			passes := [][]string{ids, ids[:1], ids[1:]}
			switch mode {
			case "split":
				require.NoError(t, err)
				require.Equal(t, 1, report.Verified)
				require.Equal(t, append(slices.Clone(passes), passes...), fetches)
				require.Equal(t, ids, updates)
				require.Equal(t, []string{doc.DocumentID}, source.published)
				return
			case "foreign":
				require.Equal(t, codes.DataLoss, status.Code(err))
			case "later failure":
				require.Equal(t, codes.Unavailable, status.Code(err))
			case "single overflow":
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
				passes = passes[:2]
			case "quota":
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
				passes = passes[:1]
			}
			require.Equal(t, passes, fetches)
			require.Empty(t, updates)
			require.Empty(t, source.published)
			require.Empty(t, report.ResumeAfter)
		})
	}
}
