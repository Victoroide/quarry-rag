package pinecone

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	pc "github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSelectiveMetadataStagingConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "skip when both names absent", env: map[string]string{"PINECONE_API_KEY": "private-key"}},
		{name: "included only", env: map[string]string{selectiveIncludedEnv: "private-index"}, want: "set both"},
		{name: "excluded only", env: map[string]string{selectiveExcludedEnv: "private-index"}, want: "set both"},
		{name: "explicitly empty", env: map[string]string{selectiveIncludedEnv: ""}, want: "set both"},
		{name: "same names", env: map[string]string{selectiveIncludedEnv: "private-index", selectiveExcludedEnv: "private-index", "PINECONE_API_KEY": "private-key"}, want: "distinct"},
		{name: "missing key", env: map[string]string{selectiveIncludedEnv: "private-index", selectiveExcludedEnv: "other-index"}, want: "PINECONE_API_KEY"},
		{name: "valid", env: map[string]string{selectiveIncludedEnv: "private-index", selectiveExcludedEnv: "other-index", "PINECONE_API_KEY": "private-key"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, enabled, err := readSelectiveStagingConfig(func(key string) (string, bool) {
				value, ok := tc.env[key]
				return value, ok
			})
			require.Equal(t, tc.name == "valid", enabled)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
				require.NotContains(t, err.Error(), "private-index")
				require.NotContains(t, err.Error(), "private-key")
			}
			if enabled {
				require.True(t, cfg.names[0] == tc.env[selectiveIncludedEnv] && cfg.names[1] == tc.env[selectiveExcludedEnv])
			}
		})
	}

	for _, tc := range []struct {
		name   string
		change func(*pc.Index, *pc.MetadataSchema)
		want   string
	}{
		{name: "valid pair"},
		{name: "same resolved host", change: func(idx *pc.Index, _ *pc.MetadataSchema) { idx.Host = "PRIVATE-HOST.invalid." }, want: "distinct resolved hosts"},
		{name: "dimension mismatch", change: func(idx *pc.Index, _ *pc.MetadataSchema) { *idx.Dimension = 2 }, want: "matching dimensions"},
		{name: "invalid dimension", change: func(idx *pc.Index, _ *pc.MetadataSchema) { *idx.Dimension = 0 }, want: "ready serverless dense cosine"},
		{name: "nonserverless", change: func(idx *pc.Index, _ *pc.MetadataSchema) { idx.Spec.Serverless = nil }, want: "ready serverless dense cosine"},
		{name: "cloud mismatch", change: func(idx *pc.Index, _ *pc.MetadataSchema) { idx.Spec.Serverless.Cloud = pc.Gcp }, want: "matching cloud and region"},
		{name: "region mismatch", change: func(idx *pc.Index, _ *pc.MetadataSchema) { idx.Spec.Serverless.Region = "eu-west-1" }, want: "matching cloud and region"},
		{name: "missing region", change: func(idx *pc.Index, _ *pc.MetadataSchema) { idx.Spec.Serverless.Region = "" }, want: "matching cloud and region"},
		{name: "read capacity mode mismatch", change: func(idx *pc.Index, _ *pc.MetadataSchema) {
			idx.Spec.Serverless.ReadCapacity = &pc.ReadCapacity{OnDemand: &pc.ReadCapacityOnDemand{}}
		}, want: "matching read capacity"},
		{name: "read capacity node mismatch", change: func(idx *pc.Index, _ *pc.MetadataSchema) {
			*idx.Spec.Serverless.ReadCapacity.Dedicated.NodeType = "b2"
		}, want: "matching read capacity"},
		{name: "read capacity scaling mismatch", change: func(idx *pc.Index, _ *pc.MetadataSchema) {
			*idx.Spec.Serverless.ReadCapacity.Dedicated.Scaling.Manual.Replicas = 2
		}, want: "matching read capacity"},
		{name: "dedicated capacity still scaling", change: func(idx *pc.Index, _ *pc.MetadataSchema) {
			idx.Spec.Serverless.ReadCapacity.Dedicated.Status = pc.ReadCapacityStatus{State: "Scaling", CurrentReplicas: new(int32(1))}
		}, want: "dedicated read capacity must be Ready"},
		{name: "ready read capacity counters may differ", change: func(idx *pc.Index, _ *pc.MetadataSchema) {
			idx.Spec.Serverless.ReadCapacity.Dedicated.Status.CurrentReplicas = new(int32(2))
		}},
		{name: "embedding mode mismatch", change: func(idx *pc.Index, _ *pc.MetadataSchema) { idx.Embed = nil }, want: "matching embedding configuration"},
		{name: "embedding model mismatch", change: func(idx *pc.Index, _ *pc.MetadataSchema) { idx.Embed.Model = "other-model" }, want: "matching embedding configuration"},
		{name: "embedding parameter mismatch", change: func(idx *pc.Index, _ *pc.MetadataSchema) {
			(*idx.Embed.ReadParameters)["truncate"] = "NONE"
		}, want: "matching embedding configuration"},
		{name: "extra field", change: func(_ *pc.Index, schema *pc.MetadataSchema) {
			schema.Fields["private-field"] = pc.MetadataSchemaField{Filterable: true}
		}, want: "expected filterable fields"},
		{name: "missing shared field", change: func(_ *pc.Index, schema *pc.MetadataSchema) { delete(schema.Fields, "doc_id") }, want: "expected filterable fields"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := selectiveStagingConfig{names: [2]string{"private-index", "other-index"}, key: "private-key"}
			reads := 0
			err := withSelectiveStagingTargets(t.Context(), cfg, func(_ context.Context, name string) (*pc.Index, *pc.MetadataSchema, error) {
				reads++
				host := "private-host.invalid"
				idx := &pc.Index{
					Name: name, Host: host, Dimension: new(int32(1)), Metric: pc.Cosine, VectorType: "dense",
					Status: &pc.IndexStatus{Ready: true},
					Spec: &pc.IndexSpec{Serverless: &pc.ServerlessSpec{
						Cloud: pc.Aws, Region: "us-east-1", ReadCapacity: &pc.ReadCapacity{Dedicated: &pc.ReadCapacityDedicated{
							Status:   pc.ReadCapacityStatus{State: "Ready", CurrentReplicas: new(int32(1))},
							NodeType: new("b1"), Scaling: &pc.ReadCapacityScaling{Manual: &pc.ReadCapacityManualScaling{
								Replicas: new(int32(1)), Shards: new(int32(1)),
							}},
						}},
					}},
					Embed: &pc.IndexEmbed{Model: "fixture-model", ReadParameters: new(map[string]any{"truncate": "END"})},
				}
				schema := &pc.MetadataSchema{Fields: selectiveStagingFields(true)}
				if name == cfg.names[1] {
					idx.Host = "other-host.invalid"
					schema.Fields = selectiveStagingFields(false)
					if tc.change != nil {
						tc.change(idx, schema)
					}
				}
				return idx, schema, nil
			}, func(_ [2]selectiveStagingTarget) {
				require.Empty(t, tc.want, "fixture operations started before validation passed")
				require.Equal(t, 2, reads, "both targets must be discovered before fixture operations")
			})
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
				require.NotContains(t, err.Error(), "private-host")
				require.NotContains(t, err.Error(), "private-field")
			}
		})
	}

	t.Run("discovery errors redact provider data", func(t *testing.T) {
		cfg := selectiveStagingConfig{names: [2]string{"private-index", "other-index"}, key: "private-key"}
		err := withSelectiveStagingTargets(t.Context(), cfg, func(context.Context, string) (*pc.Index, *pc.MetadataSchema, error) {
			return nil, nil, errors.New("private-index private-host private-key private-project")
		}, func([2]selectiveStagingTarget) { t.Fatal("fixture operations started after failed discovery") })
		require.ErrorContains(t, err, "discovery")
		require.NotContains(t, err.Error(), "private-")
	})

	for _, expired := range []bool{false, true} {
		name := "canceled parent"
		if expired {
			name = "expired parent"
		}
		t.Run(name, func(t *testing.T) {
			var parent context.Context
			var cancel context.CancelFunc
			if expired {
				parent, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			} else {
				parent, cancel = context.WithCancel(t.Context())
			}
			cancel()
			conn := selectiveCleanupStub{check: func(ctx context.Context, ids []string) {
				assert.NoError(t, ctx.Err())
				deadline, ok := ctx.Deadline()
				assert.True(t, ok)
				assert.Positive(t, time.Until(deadline))
				assert.LessOrEqual(t, time.Until(deadline), 30*time.Second)
				assert.Equal(t, []string{"synthetic-id"}, ids)
			}}
			write := selectiveStagingWrite{conn: conn, acknowledged: true, ready: true}
			results := cleanupSelectiveStaging(parent, []selectiveStagingWrite{write, write}, []string{"synthetic-id"})
			require.Equal(t, []error{nil, nil}, results)
		})
	}
}

func TestSelectiveStagingReadCapacityDiscovery(t *testing.T) {
	t.Setenv("PINECONE_CONTROLLER_HOST", "")
	dedicated := func(node, scaling, manual, status string) string {
		return fmt.Sprintf(`,"read_capacity":{"mode":"Dedicated","dedicated":{"node_type":%q,"scaling":%q,"manual":%s}%s}`,
			node, scaling, manual, status)
	}
	const manual = `{"shards":1,"replicas":2}`
	const readyStatus = `,"status":{"state":"Ready","current_shards":1,"current_replicas":2}`
	const onDemand = `,"read_capacity":{"mode":"OnDemand","status":{"state":"Ready"}}`
	ready := dedicated("b1", "Manual", manual, readyStatus)
	for _, tc := range []struct {
		name, included, excluded, want string
	}{
		{name: "matching ready dedicated", included: ready, excluded: dedicated("b1", "Manual", manual,
			`,"status":{"state":"Ready","current_shards":2,"current_replicas":3}`)},
		{name: "matching on demand", included: onDemand, excluded: onDemand},
		{name: "scaling excluded", included: ready, excluded: dedicated("b1", "Manual", manual,
			`,"status":{"state":"Scaling","current_shards":1,"current_replicas":1}`), want: "excluded target dedicated read capacity must be Ready"},
		{name: "migrating included", included: dedicated("b1", "Manual", manual,
			`,"status":{"state":"Migrating"}`), excluded: ready, want: "included target dedicated read capacity must be Ready"},
		{name: "error", included: ready, excluded: dedicated("b1", "Manual", manual,
			`,"status":{"state":"Error","error_message":"private-provider-detail"}`), want: "dedicated read capacity must be Ready"},
		{name: "missing status", included: ready, excluded: dedicated("b1", "Manual", manual, ""), want: "dedicated read capacity must be Ready"},
		{name: "distinct unknown raw modes", included: `,"read_capacity":{"mode":"private-mode-a"}`,
			excluded: `,"read_capacity":{"mode":"private-mode-b"}`, want: "recognized, complete read capacity"},
		{name: "missing mode", included: `,"read_capacity":{}`, excluded: `,"read_capacity":{}`, want: "recognized, complete read capacity"},
		{name: "omitted from both", want: "recognized, complete read capacity"},
		{name: "omitted from one", included: onDemand, want: "excluded target must describe a recognized, complete read capacity"},
		{name: "dedicated settings omitted", included: ready, excluded: `,"read_capacity":{"mode":"Dedicated","status":{"state":"Ready"}}`, want: "recognized, complete read capacity"},
		{name: "missing node type", included: ready, excluded: dedicated("", "Manual", manual, readyStatus), want: "recognized, complete read capacity"},
		{name: "unknown scaling", included: ready, excluded: dedicated("b1", "private-scaling", manual, readyStatus), want: "recognized, complete read capacity"},
		{name: "missing configured size", included: ready, excluded: dedicated("b1", "Manual", `null`, readyStatus), want: "recognized, complete read capacity"},
		{name: "omitted manual settings", included: ready, excluded: `,"read_capacity":{"mode":"Dedicated","dedicated":{"node_type":"b1","scaling":"Manual"},"status":{"state":"Ready"}}`, want: "recognized, complete read capacity"},
		{name: "SDK case-insensitive manual settings", included: ready, excluded: `,"READ_CAPACITY":{"MODE":"Dedicated","DEDICATED":{"SCALING":"Manual"}}`, want: "recognized, complete read capacity"},
		{name: "missing replicas", included: ready, excluded: dedicated("b1", "Manual", `{"shards":1}`, readyStatus), want: "recognized, complete read capacity"},
		{name: "missing shards", included: ready, excluded: dedicated("b1", "Manual", `{"replicas":2}`, readyStatus), want: "recognized, complete read capacity"},
		{name: "paused index", included: ready, excluded: dedicated("b1", "Manual", `{"shards":1,"replicas":0}`, readyStatus), want: "recognized, complete read capacity"},
		{name: "invalid shards", included: ready, excluded: dedicated("b1", "Manual", `{"shards":-1,"replicas":2}`, readyStatus), want: "recognized, complete read capacity"},
		{name: "configured shards differ", included: ready, excluded: dedicated("b1", "Manual", `{"shards":2,"replicas":2}`, readyStatus), want: "matching read capacity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := selectiveStagingConfig{names: [2]string{"private-included", "private-excluded"}, key: "private-key"}
			capacity := [2]string{tc.included, tc.excluded}
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, http.MethodGet, req.Method)
				for i, name := range cfg.names {
					host := name + ".invalid"
					switch {
					case req.URL.Host == "api.pinecone.io" && req.URL.Path == "/indexes/"+name:
						require.Equal(t, "2025-10", req.Header.Get("X-Pinecone-Api-Version"))
						body := fmt.Sprintf(`{"name":%q,"host":%q,"dimension":1,"metric":"cosine","vector_type":"dense",
							"status":{"ready":true},"spec":{"serverless":{"cloud":"aws","region":"us-east-1"%s}}}`,
							name, host, capacity[i])
						return jsonResponse(req, nil, body), nil
					case req.URL.Host == host && req.URL.Path == "/namespaces/"+selectiveNamespace:
						require.Equal(t, selectiveDescribeAPI, req.Header.Get("X-Pinecone-Api-Version"))
						body, err := json.Marshal(map[string]any{
							"name":   selectiveNamespace,
							"schema": map[string]any{"fields": selectiveStagingFields(i == 0)},
						})
						require.NoError(t, err)
						return jsonResponse(req, nil, string(body)), nil
					}
				}
				t.Fatalf("unexpected discovery request: %s", req.URL)
				return nil, nil
			})
			client := &http.Client{Transport: transport}
			sdk, err := newSelectiveStagingSDK(client, cfg.key)
			require.NoError(t, err)
			called := false
			err = withSelectiveStagingTargets(t.Context(), cfg, func(ctx context.Context, name string) (*pc.Index, *pc.MetadataSchema, error) {
				return describeSelectiveStagingTarget(ctx, sdk, client, cfg.key, name)
			}, func([2]selectiveStagingTarget) {
				called = true
			})
			if tc.want == "" {
				require.NoError(t, err)
				require.True(t, called)
				return
			}
			require.False(t, called, "fixture operations started before read capacity validation passed")
			require.ErrorContains(t, err, tc.want)
			require.NotContains(t, err.Error(), "private-")
		})
	}
}

func TestSelectiveStagingQueryFixtureCount(t *testing.T) {
	for name, count := range map[string]int{"empty": 0, "over limit": 10_001, "first valid": 1, "last valid": 10_000} {
		t.Run(name, func(t *testing.T) {
			fixture := selectiveStagingFixture{ids: make([]string, count)}
			valid := count >= 1 && count <= 10_000
			conn := &fakeIndexConn{query: func(_ context.Context, req *pc.QueryByVectorValuesRequest) (*pc.QueryVectorsResponse, error) {
				require.True(t, valid, "query started with a fixture count outside Pinecone's supported range")
				require.EqualValues(t, count, req.TopK)
				return &pc.QueryVectorsResponse{Namespace: selectiveNamespace}, nil
			}}
			_, code, err := fixture.query(t.Context(), conn, nil)
			if valid {
				require.NoError(t, err)
				require.Equal(t, codes.OK, code)
			} else {
				require.ErrorContains(t, err, "fixture count")
				require.Equal(t, codes.InvalidArgument, code)
			}
		})
	}
}

func TestSelectiveStagingDescription(t *testing.T) {
	for _, tc := range []struct {
		name       string
		input      string
		indexLevel bool
		wantFields []string
		wantErr    string
	}{
		{name: "omitted namespace schema inherits", input: `{"name":"__default__"}`},
		{name: "namespace override", input: `{"name":"__default__","schema":{"fields":{"doc_id":{"filterable":true}}}}`, wantFields: []string{"doc_id"}},
		{
			name: "index descriptions include typed fields", indexLevel: true, wantFields: []string{"doc_id"},
			input: `{"name":"fixture","schema":{"fields":{"doc_id":{"filterable":true},"vector":{"type":"dense_vector"}}}}`,
		},
		{name: "no name", input: `{}`, wantErr: "invalid describe response"},
		{name: "multiple objects", input: `{"name":"__default__"}{}`, wantErr: "invalid describe response"},
		{name: "duplicate schema", input: `{"name":"__default__","schema":null,"schema":{}}`, wantErr: "invalid describe response"},
		{
			name: "invalid field is not silently omitted", wantErr: "invalid filterable field",
			input: `{"name":"__default__","schema":{"fields":{"private-field":{"filterable":false}}}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			description, err := decodeSelectiveDescription(strings.NewReader(tc.input))
			if err == nil {
				var schema *pc.MetadataSchema
				schema, err = description.metadataSchema(tc.indexLevel)
				if err == nil && tc.wantFields == nil {
					require.Nil(t, schema)
				} else if err == nil {
					require.Len(t, schema.Fields, len(tc.wantFields))
					for _, field := range tc.wantFields {
						require.True(t, schema.Fields[field].Filterable)
					}
				}
			}
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
				require.NotContains(t, err.Error(), "private-field")
			}
		})
	}
}

func FuzzSelectiveStagingDescription(f *testing.F) {
	f.Add(`{"name":"__default__","schema":{"fields":{"doc_id":{"filterable":true}}}}`, false)
	f.Add(`{"name":"fixture","schema":{"fields":{"vector":{"type":"dense_vector"}}}}`, true)
	f.Add(`{"name":"__default__","schema":{"fields":{"doc_id":null}}}`, false)
	f.Fuzz(func(t *testing.T, input string, indexLevel bool) {
		description, err := decodeSelectiveDescription(strings.NewReader(input))
		if err != nil {
			return
		}
		require.NotEmpty(t, description.Name)
		schema, err := description.metadataSchema(indexLevel)
		if err == nil && schema != nil {
			for _, field := range schema.Fields {
				require.True(t, field.Filterable)
			}
		}
	})
}

func TestSelectiveStagingQueryEvidence(t *testing.T) {
	fixture := newSelectiveStagingFixture(t, 1)
	other := newSelectiveStagingFixture(t, 1)
	require.NotEqual(t, fixture.ids[0], other.ids[0])
	require.Len(t, fixture.ids, 9)
	response := &pc.QueryVectorsResponse{
		Namespace: selectiveNamespace,
		Matches: []*pc.ScoredVector{
			{Vector: fixture.vectors[1]},
			{Vector: fixture.vectors[0]},
		},
	}
	var queryErr error
	conn := &fakeIndexConn{query: func(_ context.Context, req *pc.QueryByVectorValuesRequest) (*pc.QueryVectorsResponse, error) {
		require.Same(t, fixture.compound, req.MetadataFilter)
		require.Equal(t, []float32{1}, req.Vector)
		require.EqualValues(t, len(fixture.ids), req.TopK)
		return response, queryErr
	}}
	ids, code, err := fixture.query(t.Context(), conn, fixture.compound)
	require.NoError(t, err)
	require.Equal(t, codes.OK, code)
	require.Equal(t, fixture.ids[:2], ids)

	response.Matches[0].Vector = &pc.Vector{Id: "private-provider-id"}
	ids, _, err = fixture.query(t.Context(), conn, fixture.compound)
	require.Empty(t, ids)
	require.ErrorContains(t, err, "unexpected or duplicate ID")
	require.NotContains(t, err.Error(), "private-provider-id")

	queryErr = status.Error(codes.ResourceExhausted, "private-provider-error")
	ids, code, err = fixture.query(t.Context(), conn, fixture.compound)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Equal(t, codes.ResourceExhausted, code)
}

func TestSelectiveStagingCleanupEvidence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		acknowledged bool
		ready        bool
		visible      bool
		appliesLate  bool
		deleteErr    error
		fetchErr     error
		wantErr      string
	}{
		{name: "upsert applies after an acknowledged delete and empty fetch", appliesLate: true, wantErr: "upsert outcome unknown"},
		{name: "partially visible fixture", acknowledged: true, visible: true, appliesLate: true, wantErr: "full fixture visibility was unconfirmed"},
		{name: "delete times out", acknowledged: true, ready: true, visible: true, deleteErr: status.Error(codes.DeadlineExceeded, "private-address"), wantErr: "exact ID deletion failed, status=DeadlineExceeded"},
		{name: "fetch times out", acknowledged: true, ready: true, visible: true, fetchErr: status.Error(codes.DeadlineExceeded, "private-address"), wantErr: "cleanup verification fetch failed, status=DeadlineExceeded"},
		{name: "acknowledged visible write is removed", acknowledged: true, ready: true, visible: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids := []string{"synthetic-visible", "synthetic-late"}
			vectors := map[string]*pc.Vector{}
			if tc.visible {
				vectors[ids[0]] = &pc.Vector{Id: ids[0]}
			}
			if tc.ready {
				vectors[ids[1]] = &pc.Vector{Id: ids[1]}
			}
			var calls []string
			conn := selectiveCleanupStub{
				check: func(_ context.Context, gotIDs []string) { assert.Equal(t, ids, gotIDs) },
				deleteIDs: func() error {
					calls = append(calls, "delete")
					if tc.deleteErr == nil {
						clear(vectors)
					}
					return tc.deleteErr
				},
				fetch: func() (*pc.FetchVectorsResponse, error) {
					calls = append(calls, "fetch")
					return &pc.FetchVectorsResponse{Namespace: selectiveNamespace, Vectors: vectors}, tc.fetchErr
				},
			}
			write := selectiveStagingWrite{conn: conn, acknowledged: tc.acknowledged, ready: tc.ready}
			results := cleanupSelectiveStaging(t.Context(), []selectiveStagingWrite{write}, ids)
			require.Len(t, results, 1)
			if tc.deleteErr == nil {
				require.Equal(t, []string{"delete", "fetch"}, calls)
				require.Empty(t, vectors)
			} else {
				require.Equal(t, []string{"delete"}, calls)
			}
			if tc.appliesLate {
				vectors[ids[1]] = &pc.Vector{Id: ids[1]}
			}
			if tc.wantErr == "" {
				require.NoError(t, results[0])
				return
			}
			require.ErrorContains(t, results[0], tc.wantErr)
			require.ErrorContains(t, results[0], "tear down the disposable index")
			require.NotContains(t, results[0].Error(), "private-address")
		})
	}
}

type selectiveCleanupStub struct {
	check     func(context.Context, []string)
	deleteIDs func() error
	fetch     func() (*pc.FetchVectorsResponse, error)
}

func (c selectiveCleanupStub) DeleteVectorsByID(ctx context.Context, ids []string) error {
	c.check(ctx, ids)
	if c.deleteIDs != nil {
		return c.deleteIDs()
	}
	return nil
}

func (c selectiveCleanupStub) FetchVectors(ctx context.Context, ids []string) (*pc.FetchVectorsResponse, error) {
	c.check(ctx, ids)
	if c.fetch != nil {
		return c.fetch()
	}
	return &pc.FetchVectorsResponse{Namespace: selectiveNamespace}, nil
}
