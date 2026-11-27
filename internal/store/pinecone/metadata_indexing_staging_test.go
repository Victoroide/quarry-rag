package pinecone

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	pc "github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	selectiveIncludedEnv    = "QUARRY_PINECONE_SELECTIVE_INCLUDED_INDEX"
	selectiveExcludedEnv    = "QUARRY_PINECONE_SELECTIVE_EXCLUDED_INDEX"
	selectiveNamespace      = "__default__"
	selectiveControllerHost = "api.pinecone.io"
	selectiveDescribeAPI    = "2026-07"
	selectiveMaxTopK        = 10_000
	selectiveDescribeMax    = 64 << 10
)

type selectiveStagingConfig struct {
	names [2]string
	key   string
}

func readSelectiveStagingConfig(lookup func(string) (string, bool)) (selectiveStagingConfig, bool, error) {
	included, hasIncluded := lookup(selectiveIncludedEnv)
	excluded, hasExcluded := lookup(selectiveExcludedEnv)
	if !hasIncluded && !hasExcluded {
		return selectiveStagingConfig{}, false, nil
	}
	included, excluded = strings.TrimSpace(included), strings.TrimSpace(excluded)
	if included == "" || excluded == "" {
		return selectiveStagingConfig{}, false, fmt.Errorf("set both %s and %s", selectiveIncludedEnv, selectiveExcludedEnv)
	}
	if strings.EqualFold(included, excluded) {
		return selectiveStagingConfig{}, false, errors.New("selective staging requires distinct index names")
	}
	key, _ := lookup("PINECONE_API_KEY")
	if strings.TrimSpace(key) == "" {
		return selectiveStagingConfig{}, false, errors.New("selective staging requires PINECONE_API_KEY")
	}
	return selectiveStagingConfig{names: [2]string{included, excluded}, key: key}, true, nil
}

type selectiveStagingTarget struct {
	label string
	index *pc.Index
	plane *visibilityPlane
}

type selectiveDiscoveryError string

func (e selectiveDiscoveryError) Error() string { return string(e) }

func selectiveStagingFields(includesIncarnation bool) map[string]pc.MetadataSchemaField {
	fields := make(map[string]pc.MetadataSchemaField)
	for _, field := range []string{"collection", "doc_id", "doc_type", domain.VectorRevisionKey, "revision_id", domain.VectorTenantKey, domain.VectorAttemptKey} {
		fields[field] = pc.MetadataSchemaField{Filterable: true}
	}
	if includesIncarnation {
		fields[domain.VectorIncarnationKey] = pc.MetadataSchemaField{Filterable: true}
	}
	return fields
}

func withSelectiveStagingTargets(
	ctx context.Context, cfg selectiveStagingConfig,
	describe func(context.Context, string) (*pc.Index, *pc.MetadataSchema, error),
	use func([2]selectiveStagingTarget),
) error {
	var targets [2]selectiveStagingTarget
	var hosts [2]string
	for i, label := range [...]string{"included", "excluded"} {
		idx, schema, err := describe(ctx, cfg.names[i])
		if err != nil {
			// Only the controlled diagnostics from discovery may reach test output.
			if detail, ok := errors.AsType[selectiveDiscoveryError](err); ok {
				return fmt.Errorf("%s target discovery failed: %w", label, detail)
			}
			return fmt.Errorf("%s target discovery failed", label)
		}
		if idx == nil || idx.Name != cfg.names[i] || idx.Dimension == nil || *idx.Dimension < 1 || *idx.Dimension > 20_000 ||
			idx.Spec == nil || idx.Spec.Serverless == nil || idx.Spec.Pod != nil || idx.Spec.BYOC != nil ||
			idx.Status == nil || !idx.Status.Ready || idx.Metric != pc.Cosine || idx.VectorType != "dense" {
			return fmt.Errorf("%s target must be a ready serverless dense cosine vector index", label)
		}
		if capacityErr := validateSelectiveReadCapacity(idx.Spec.Serverless.ReadCapacity); capacityErr != nil {
			return fmt.Errorf("%s target %w", label, capacityErr)
		}
		plane, err := newVisibilityPlane(idx, cfg.key)
		if err != nil || plane.endpoint.Port() != "" {
			return fmt.Errorf("%s target has an invalid resolved host", label)
		}
		if schema == nil || !matchingSelectiveStagingFields(schema, i == 0) {
			return fmt.Errorf("%s default namespace must declare exactly the expected filterable fields", label)
		}
		hosts[i] = strings.ToLower(strings.TrimSuffix(plane.endpoint.Hostname(), "."))
		targets[i] = selectiveStagingTarget{label: label, index: idx, plane: plane}
	}
	if hosts[0] == hosts[1] {
		return errors.New("selective staging requires distinct resolved hosts")
	}
	if *targets[0].index.Dimension != *targets[1].index.Dimension {
		return errors.New("selective staging requires matching dimensions")
	}
	included, excluded := targets[0].index, targets[1].index
	left, right := included.Spec.Serverless, excluded.Spec.Serverless
	if left.Cloud == "" || left.Region == "" || left.Cloud != right.Cloud || left.Region != right.Region {
		return errors.New("selective staging requires matching cloud and region")
	}
	if !matchingSelectiveReadCapacity(left.ReadCapacity, right.ReadCapacity) {
		return errors.New("selective staging requires matching read capacity configuration")
	}
	if !reflect.DeepEqual(included.Embed, excluded.Embed) {
		return errors.New("selective staging requires matching embedding configuration")
	}
	use(targets)
	return nil
}

func validateSelectiveReadCapacity(capacity *pc.ReadCapacity) error {
	incomplete := selectiveDiscoveryError("must describe a recognized, complete read capacity configuration")
	if capacity == nil || (capacity.OnDemand == nil) == (capacity.Dedicated == nil) {
		return incomplete
	}
	if capacity.OnDemand != nil {
		return nil
	}
	dedicated := capacity.Dedicated
	if dedicated.NodeType == nil || strings.TrimSpace(*dedicated.NodeType) == "" ||
		dedicated.Scaling == nil || dedicated.Scaling.Manual == nil {
		return incomplete
	}
	size := dedicated.Scaling.Manual
	if size.Shards == nil || *size.Shards < 1 || size.Replicas == nil || *size.Replicas < 1 {
		return incomplete
	}
	if dedicated.Status.State != "Ready" {
		return errors.New("dedicated read capacity must be Ready before running the comparison")
	}
	return nil
}

// Both configurations have passed validateSelectiveReadCapacity.
func matchingSelectiveReadCapacity(left, right *pc.ReadCapacity) bool {
	if left.OnDemand != nil || right.OnDemand != nil {
		return left.OnDemand != nil && right.OnDemand != nil
	}
	leftSize, rightSize := left.Dedicated.Scaling.Manual, right.Dedicated.Scaling.Manual
	return *left.Dedicated.NodeType == *right.Dedicated.NodeType &&
		*leftSize.Shards == *rightSize.Shards && *leftSize.Replicas == *rightSize.Replicas
}

func matchingSelectiveStagingFields(schema *pc.MetadataSchema, includesIncarnation bool) bool {
	want := selectiveStagingFields(includesIncarnation)
	if len(schema.Fields) != len(want) {
		return false
	}
	for key := range want {
		if !schema.Fields[key].Filterable {
			return false
		}
	}
	return true
}

func TestStagingSelectiveMetadataIndexing(t *testing.T) {
	cfg, enabled, err := readSelectiveStagingConfig(os.LookupEnv)
	require.NoError(t, err)
	if !enabled {
		t.Skip("requires two disposable selectively indexed targets")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	httpClient := newSelectiveStagingHTTPClient(nil)
	defer httpClient.CloseIdleConnections()
	sdk, err := newSelectiveStagingSDK(httpClient, cfg.key)
	require.True(t, err == nil, "could not configure the Pinecone SDK")
	describe := func(ctx context.Context, name string) (*pc.Index, *pc.MetadataSchema, error) {
		return describeSelectiveStagingTarget(ctx, sdk, httpClient, cfg.key, name)
	}
	err = withSelectiveStagingTargets(ctx, cfg, describe, func(targets [2]selectiveStagingTarget) {
		fixture := newSelectiveStagingFixture(t, int(*targets[0].index.Dimension))
		var connections [2]*pc.IndexConnection
		for i, target := range targets {
			conn, connErr := sdk.Index(pc.NewIndexConnParams{Host: target.index.Host})
			require.Truef(t, connErr == nil, "%s target SDK connection failed", target.label)
			t.Cleanup(func() {
				if closeErr := conn.Close(); closeErr != nil {
					t.Errorf("%s target SDK connection close failed", target.label)
				}
			})
			connections[i] = conn
		}
		var touched []selectiveStagingWrite
		t.Cleanup(func() {
			for i, err := range cleanupSelectiveStaging(ctx, touched, fixture.ids) {
				if err != nil {
					t.Errorf("%s target cleanup: %v", targets[i].label, err)
					continue
				}
				t.Logf("%s cleanup confirmed for %d synthetic IDs", targets[i].label, len(fixture.ids))
			}
		})
		for i, target := range targets {
			conn := connections[i]
			// The provider may apply a write even when the acknowledgement fails.
			touched = append(touched, selectiveStagingWrite{conn: selectiveStagingCleanupAdapter{conn}})
			client := &Client{visibility: target.plane, batchSize: len(fixture.vectors), timeout: 10 * time.Second, maxAttempts: 1}
			probe, writeErr := client.upsertForVisibility(ctx, "", fixture.vectors)
			require.Truef(t, writeErr == nil, "%s target synthetic upsert failed, status=%s", target.label, status.Code(writeErr))
			touched[i].acknowledged = true
			baselineErr := fixture.waitForReadiness(ctx, conn, probe)
			require.NoErrorf(t, baselineErr, "%s target baseline inconclusive", target.label)
			touched[i].ready = true
		}
		fixture.checkFilters(ctx, t, connections)
	})
	require.NoError(t, err)
}

func newSelectiveStagingHTTPClient(transport http.RoundTripper) *http.Client {
	return &http.Client{
		Transport:     transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func newSelectiveStagingSDK(client *http.Client, key string) (*pc.Client, error) {
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	// DescribeIndex is the only SDK REST call in this check.
	sdkClient := *client
	sdkClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := transport.RoundTrip(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			return resp, err
		}
		body := resp.Body
		defer func() { _ = body.Close() }()
		data, err := io.ReadAll(io.LimitReader(body, selectiveDescribeMax+1))
		var description struct {
			Spec struct {
				Serverless *struct {
					ReadCapacity struct {
						Mode      string `json:"mode"`
						Dedicated struct {
							Scaling string    `json:"scaling"`
							Manual  *struct{} `json:"manual"`
						} `json:"dedicated"`
					} `json:"read_capacity"`
				} `json:"serverless"`
				Pod  *struct{} `json:"pod"`
				BYOC *struct{} `json:"byoc"`
			} `json:"spec"`
		}
		if err != nil || len(data) > selectiveDescribeMax ||
			json.Unmarshal(data, &description, json.MatchCaseInsensitiveNames(true)) != nil {
			return nil, selectiveDiscoveryError("invalid SDK describe-index response")
		}
		spec := description.Spec
		if spec.Serverless == nil || spec.Pod != nil || spec.BYOC != nil {
			return nil, selectiveDiscoveryError("resolved index is not a serverless target")
		}
		capacity := spec.Serverless.ReadCapacity
		// SDK v5 dereferences the manual settings when the strategy is Manual.
		if capacity.Mode == "Dedicated" && strings.EqualFold(capacity.Dedicated.Scaling, "Manual") && capacity.Dedicated.Manual == nil {
			return nil, selectiveDiscoveryError("target must describe a recognized, complete read capacity configuration")
		}
		resp.Body = io.NopCloser(bytes.NewReader(data))
		return resp, nil
	})
	return pc.NewClient(pc.NewClientParams{ApiKey: key, Host: "https://" + selectiveControllerHost, RestClient: &sdkClient})
}

func describeSelectiveStagingTarget(ctx context.Context, sdk *pc.Client, client *http.Client, key, name string) (*pc.Index, *pc.MetadataSchema, error) {
	idx, err := sdk.DescribeIndex(ctx, name)
	if err != nil {
		if detail, ok := errors.AsType[selectiveDiscoveryError](err); ok {
			return nil, nil, detail
		}
		return nil, nil, selectiveDiscoveryError("SDK describe-index failed")
	}
	if idx == nil || idx.Name != name {
		return nil, nil, selectiveDiscoveryError("SDK describe-index did not confirm the configured index")
	}
	plane, err := newVisibilityPlane(idx, key)
	if err != nil || plane.endpoint.Port() != "" {
		return nil, nil, selectiveDiscoveryError("resolved index is not a serverless target with a valid host")
	}
	endpoint := plane.endpoint
	endpoint.Path = "/namespaces/" + selectiveNamespace
	namespace, err := getSelectiveDescription(ctx, client, key, endpoint)
	if err != nil {
		return nil, nil, selectiveDiscoveryError("describe namespace: " + err.Error())
	}
	if namespace.Name != selectiveNamespace {
		return nil, nil, selectiveDiscoveryError("describe namespace did not identify the default namespace")
	}
	if namespace.Schema != nil {
		schema, schemaErr := namespace.metadataSchema(false)
		return idx, schema, schemaErr
	}
	endpoint = url.URL{Scheme: "https", Host: selectiveControllerHost, Path: "/indexes/" + name, RawPath: "/indexes/" + url.PathEscape(name)}
	index, err := getSelectiveDescription(ctx, client, key, endpoint)
	if err != nil {
		return nil, nil, selectiveDiscoveryError("describe index: " + err.Error())
	}
	if index.Name != name || index.Host != idx.Host {
		return nil, nil, selectiveDiscoveryError("describe index did not confirm the SDK target")
	}
	schema, err := index.metadataSchema(true)
	return idx, schema, err
}

type selectiveDescription struct {
	Name   string `json:"name"`
	Host   string `json:"host"`
	Schema *struct {
		Fields map[string]struct {
			Filterable *bool  `json:"filterable"`
			Type       string `json:"type"`
		} `json:"fields"`
	} `json:"schema"`
}

func getSelectiveDescription(ctx context.Context, client *http.Client, key string, endpoint url.URL) (selectiveDescription, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), http.NoBody)
	if err != nil {
		return selectiveDescription{}, errors.New("could not prepare describe request")
	}
	req.Header.Set("Api-Key", key)
	req.Header.Set("X-Pinecone-Api-Version", selectiveDescribeAPI)
	resp, err := client.Do(req)
	if err != nil {
		return selectiveDescription{}, errors.New("describe transport failed")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return selectiveDescription{}, fmt.Errorf("describe returned HTTP %d", resp.StatusCode)
	}
	return decodeSelectiveDescription(resp.Body)
}

func decodeSelectiveDescription(body io.Reader) (selectiveDescription, error) {
	data, err := io.ReadAll(io.LimitReader(body, selectiveDescribeMax+1))
	var description selectiveDescription
	if err != nil || len(data) > selectiveDescribeMax || json.Unmarshal(data, &description) != nil || description.Name == "" {
		return selectiveDescription{}, errors.New("invalid describe response")
	}
	return description, nil
}

func (d selectiveDescription) metadataSchema(indexLevel bool) (*pc.MetadataSchema, error) {
	if d.Schema == nil {
		return nil, nil
	}
	schema := &pc.MetadataSchema{Fields: make(map[string]pc.MetadataSchemaField)}
	for key, field := range d.Schema.Fields {
		if indexLevel && field.Type != "" && field.Filterable == nil {
			// Index descriptions include typed search fields alongside metadata declarations.
			continue
		}
		if field.Filterable == nil || !*field.Filterable {
			return nil, selectiveDiscoveryError("invalid filterable field declaration")
		}
		schema.Fields[key] = pc.MetadataSchemaField{Filterable: true}
	}
	return schema, nil
}

type selectiveStagingFixture struct {
	vectors     []*pc.Vector
	ids         []string
	allDocs     *structpb.Struct
	compound    *structpb.Struct
	incarnation string
	embedding   []float32
}

func newSelectiveStagingFixture(t *testing.T, dimension int) selectiveStagingFixture {
	t.Helper()
	run, doc, otherDoc := uuid.NewString(), uuid.NewString(), uuid.NewString()
	tenant, incarnation, staleIncarnation := uuid.NewString(), uuid.NewString(), uuid.NewString()
	collection, docType := "fixture-collection", "fixture-type"
	embedding := make([]float32, dimension)
	embedding[0] = 1
	metadata := func(doc, tenant, collection, docType string) map[string]any {
		return map[string]any{"doc_id": doc, domain.VectorTenantKey: tenant, "collection": collection, "doc_type": docType}
	}
	fixture := selectiveStagingFixture{
		allDocs:     buildFilter("", []string{doc, otherDoc}, ""),
		compound:    buildScopedFilter(docType, []string{doc}, collection, tenant, []string{incarnation}),
		incarnation: incarnation, embedding: embedding,
	}
	add := func(label string, fields map[string]any) {
		meta, err := structpb.NewStruct(fields)
		require.NoError(t, err)
		id := run + "/" + label
		fixture.vectors = append(fixture.vectors, &pc.Vector{Id: id, Values: &embedding, Metadata: meta})
		fixture.ids = append(fixture.ids, id)
	}
	tagged := func(tag string) map[string]any {
		fields := metadata(doc, tenant, collection, docType)
		fields[domain.VectorRevisionKey] = tag
		return fields
	}
	manifestRevision := uuid.NewString()
	manifest := func(stamp string) map[string]any {
		fields := metadata(doc, tenant, collection, docType)
		fields["revision_id"] = manifestRevision
		if stamp != "" {
			fields[domain.VectorIncarnationKey] = stamp
		}
		return fields
	}
	add("tagged-current", tagged(incarnation))
	add("tagged-stale", tagged(staleIncarnation))
	add("manifest-current", manifest(incarnation))
	add("manifest-stale", manifest(staleIncarnation))
	add("manifest-historical", manifest(""))
	for _, negative := range []struct{ label, field, value string }{
		{"foreign-tenant", domain.VectorTenantKey, uuid.NewString()},
		{"wrong-document", "doc_id", otherDoc},
		{"wrong-collection", "collection", "other-collection"},
		{"wrong-type", "doc_type", "other-type"},
	} {
		fields := manifest(incarnation)
		fields[negative.field] = negative.value
		add(negative.label, fields)
	}
	return fixture
}

type selectiveStagingQuerier interface {
	QueryByVectorValues(context.Context, *pc.QueryByVectorValuesRequest) (*pc.QueryVectorsResponse, error)
}

func (f selectiveStagingFixture) query(ctx context.Context, conn selectiveStagingQuerier, filter *structpb.Struct) ([]string, codes.Code, error) {
	count := len(f.ids)
	if count < 1 || count > selectiveMaxTopK {
		return nil, codes.InvalidArgument, fmt.Errorf("fixture count must be between 1 and %d", selectiveMaxTopK)
	}
	r, err := conn.QueryByVectorValues(ctx, &pc.QueryByVectorValuesRequest{
		Vector: f.embedding, TopK: uint32(count), IncludeMetadata: true, MetadataFilter: filter,
	})
	if err != nil {
		return nil, status.Code(err), nil
	}
	if r == nil || r.Namespace != selectiveNamespace {
		return nil, codes.OK, errors.New("invalid query response namespace")
	}
	ids := make([]string, 0, len(r.Matches))
	for _, match := range r.Matches {
		if match == nil || match.Vector == nil || !slices.Contains(f.ids, match.Vector.Id) || slices.Contains(ids, match.Vector.Id) {
			return nil, codes.OK, errors.New("query response contained an unexpected or duplicate ID")
		}
		ids = append(ids, match.Vector.Id)
	}
	slices.Sort(ids)
	return ids, codes.OK, nil
}

func (f selectiveStagingFixture) waitForReadiness(ctx context.Context, conn *pc.IndexConnection, probe domain.VisibilityProbe) error {
	readyCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	return waitSelectiveStaging(readyCtx, func(ctx context.Context) (bool, error) {
		if probe != nil {
			visibility, err := probe(ctx, f.embedding)
			if err != nil || visibility == domain.VisibilityUnknown {
				return false, fmt.Errorf("visibility probe inconclusive, status=%s", status.Code(err))
			}
			if visibility != domain.VisibilityWritesIndexed {
				return false, nil
			}
		}
		fetched, err := conn.FetchVectors(ctx, f.ids)
		if err != nil {
			return false, fmt.Errorf("unfiltered fetch failed, status=%s", status.Code(err))
		}
		if fetched == nil || fetched.Namespace != selectiveNamespace {
			return false, errors.New("unfiltered fetch returned an unexpected namespace")
		}
		for _, id := range f.ids {
			if fetched.Vectors[id] == nil {
				return false, nil
			}
		}
		ids, code, err := f.query(ctx, conn, f.allDocs)
		if err != nil {
			return false, err
		}
		if code != codes.OK {
			return false, fmt.Errorf("doc_id baseline query failed, status=%s", code)
		}
		return len(ids) == len(f.ids), nil
	})
}

func (f selectiveStagingFixture) checkFilters(ctx context.Context, t *testing.T, conns [2]*pc.IndexConnection) {
	t.Helper()
	docScope := structpb.NewStructValue(f.allDocs)
	condition := func(predicate *structpb.Value) *structpb.Struct {
		return filterGroup("$and", docScope, filterField(domain.VectorIncarnationKey, predicate)).GetStructValue()
	}
	runPrefix := strings.SplitN(f.ids[0], "/", 2)[0] + "/"
	for _, probe := range []struct {
		name   string
		filter *structpb.Struct
		labels []string
	}{
		{
			name: "$in", filter: condition(filterField("$in", structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{structpb.NewStringValue(f.incarnation)},
			}))),
			labels: []string{"foreign-tenant", "manifest-current", "wrong-collection", "wrong-document", "wrong-type"},
		},
		{
			name: "$exists:false", filter: condition(filterField("$exists", structpb.NewBoolValue(false))),
			labels: []string{"manifest-historical", "tagged-current", "tagged-stale"},
		},
		{name: "compound", filter: f.compound, labels: []string{"manifest-current", "manifest-historical", "tagged-current"}},
	} {
		want := make([]string, 0, len(probe.labels))
		for _, label := range probe.labels {
			want = append(want, runPrefix+label)
		}
		for i, label := range [...]string{"included", "excluded"} {
			ids, code, err := f.query(ctx, conns[i], probe.filter)
			t.Logf("%s %s (SDK 2025-10), status=%s count=%d ids=%v", label, probe.name, code, len(ids), ids)
			require.NoError(t, err)
			if i == 0 {
				require.Equal(t, codes.OK, code, "included target control must pass")
				require.Equal(t, want, ids, "included target control must return the exact synthetic IDs")
				continue
			}
			require.Truef(t, ctx.Err() == nil && (code == codes.OK || code == codes.InvalidArgument),
				"excluded target result is inconclusive, status=%s", code)
		}
	}
}

func waitSelectiveStaging(ctx context.Context, ready func(context.Context) (bool, error)) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		ok, err := ready(ctx)
		if err != nil {
			return err
		}
		if ok {
			return ctx.Err()
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
		}
	}
	return ctx.Err()
}

type selectiveStagingCleanupConn interface {
	DeleteVectorsByID(context.Context, []string) error
	FetchVectors(context.Context, []string) (*pc.FetchVectorsResponse, error)
}

type selectiveStagingCleanupAdapter struct{ *pc.IndexConnection }

func (c selectiveStagingCleanupAdapter) DeleteVectorsByID(ctx context.Context, ids []string) error {
	return c.DeleteVectorsById(ctx, ids)
}

type selectiveStagingWrite struct {
	conn         selectiveStagingCleanupConn
	acknowledged bool
	ready        bool
}

func cleanupSelectiveStaging(parent context.Context, writes []selectiveStagingWrite, ids []string) []error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 30*time.Second)
	defer cancel()
	results := make([]error, len(writes))
	var workers sync.WaitGroup
	for i, write := range writes {
		workers.Go(func() {
			if err := deleteSelectiveStaging(ctx, write, ids); err != nil {
				results[i] = fmt.Errorf("cleanup inconclusive, tear down the disposable index: %w", err)
			}
		})
	}
	workers.Wait()
	return results
}

func deleteSelectiveStaging(ctx context.Context, write selectiveStagingWrite, ids []string) error {
	if err := write.conn.DeleteVectorsByID(ctx, ids); err != nil {
		return fmt.Errorf("exact ID deletion failed, status=%s", status.Code(err))
	}
	err := waitSelectiveStaging(ctx, func(ctx context.Context) (bool, error) {
		got, err := write.conn.FetchVectors(ctx, ids)
		if err != nil {
			return false, fmt.Errorf("cleanup verification fetch failed, status=%s", status.Code(err))
		}
		if got == nil || got.Namespace != selectiveNamespace {
			return false, errors.New("cleanup verification fetch returned an unexpected namespace")
		}
		return len(got.Vectors) == 0, nil
	})
	if err != nil {
		return err
	}
	// An empty fetch cannot rule out an unresolved upsert applying after deletion.
	if !write.acknowledged {
		return errors.New("upsert outcome unknown and synthetic IDs could still appear")
	}
	if !write.ready {
		return errors.New("full fixture visibility was unconfirmed before deletion and synthetic IDs could still appear")
	}
	return nil
}
