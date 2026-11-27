// Package pinecone provides the Pinecone vector store client.
package pinecone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	maxUpsertBytes   = 1572864
	maxMetadataBytes = 40 * 1024
)

// Reserve response space for IDs, scores and protocol framing.
const cleanupPageSize = 4_000_000 / (maxMetadataBytes + 2048)

// Client wraps the official Pinecone Go SDK.
// All namespaces share one connection.
type Client struct {
	client                *pinecone.Client
	host                  string
	cfg                   *config.Config
	defaultConn           indexConn
	ownershipMu           sync.Mutex
	ownershipFilters      ownershipFilters
	discoverOwnership     func(context.Context) (ownershipFilters, error)
	dial                  func(namespace string) (indexConn, error)
	batchSize             int
	timeout               time.Duration
	maxAttempts           int
	dims                  int
	visibility            *visibilityPlane
	closeOnce             sync.Once
	closeErr              error
	fixtureVectorIdentity string
}

// NewPineconeClient creates a new Pinecone client and connects to the index.
func NewPineconeClient(ctx context.Context, cfg *config.Config) (*Client, error) {
	pc, err := pinecone.NewClient(pinecone.NewClientParams{ApiKey: cfg.PineconeAPIKey})
	if err != nil {
		return nil, fmt.Errorf("pinecone: create client: %w", err)
	}
	return newPineconeClient(ctx, cfg, pc)
}

func newPineconeClient(ctx context.Context, cfg *config.Config, pc *pinecone.Client, dialOpts ...grpc.DialOption) (*Client, error) {
	startupCtx, cancel := context.WithTimeout(ctx, cfg.PineconeTimeout)
	defer cancel()
	idx, err := pc.DescribeIndex(startupCtx, cfg.PineconeIndexName)
	if err != nil {
		return nil, fmt.Errorf("pinecone: describe index %q: %w", cfg.PineconeIndexName, err)
	}
	if validationErr := validateIndex(idx, cfg); validationErr != nil {
		return nil, validationErr
	}
	dialOpts = append(dialOpts, grpc.WithStatsHandler(mutationDeliveryObserver{}), grpc.WithChainUnaryInterceptor(classifyMutationDelivery))
	dialOpts = append(dialOpts, WithSingleWriterTransport())
	if cfg.HostedVisibilityRefill {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(queryAttemptCredentials{}))
	}
	var visibility *visibilityPlane
	if cfg.IndexVisibilityMode == "lsn" {
		visibility, err = newVisibilityPlane(idx, cfg.PineconeAPIKey)
		if err != nil {
			return nil, err
		}
	}
	defaultConn, err := pc.Index(pinecone.NewIndexConnParams{Host: idx.Host}, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("pinecone: connect to default namespace: %w", err)
	}
	p := newClient(pc, cfg, defaultConn)
	p.visibility = visibility
	// Fixture writes use the default namespace of the resolved index.
	p.fixtureVectorIdentity = domain.FixtureVectorIdentity("pinecone", cfg.PineconeIndexName, idx.Host)
	p.host = idx.Host
	p.discoverOwnership = func(ctx context.Context) (ownershipFilters, error) {
		return indexedOwnershipFields(ctx, idx, defaultConn)
	}
	slog.InfoContext(ctx, "pinecone connected", "index", cfg.PineconeIndexName, "host", idx.Host, "dimensions", *idx.Dimension)
	return p, nil
}

type ownershipFilters struct {
	document bool
	revision bool
	attempt  bool
}

type namespaceDescriber interface {
	DescribeNamespace(context.Context, string) (*pinecone.NamespaceDescription, error)
}

func indexedOwnershipFields(ctx context.Context, idx *pinecone.Index, conn namespaceDescriber) (ownershipFilters, error) {
	if idx.Spec == nil {
		return ownershipFilters{}, nil
	}
	if pod := idx.Spec.Pod; pod != nil {
		if pod.MetadataConfig == nil || pod.MetadataConfig.Indexed == nil {
			return ownershipFilters{document: true, revision: true, attempt: true}, nil
		}
		fields := *pod.MetadataConfig.Indexed
		return ownershipFilters{
			document: slices.Contains(fields, "doc_id"),
			revision: slices.Contains(fields, domain.VectorRevisionKey),
			attempt:  slices.Contains(fields, domain.VectorAttemptKey),
		}, nil
	}
	var schema *pinecone.MetadataSchema
	switch {
	case idx.Spec.Serverless != nil:
		schema = idx.Spec.Serverless.Schema
	case idx.Spec.BYOC != nil:
		schema = idx.Spec.BYOC.Schema
	default:
		return ownershipFilters{}, nil
	}
	ns, err := conn.DescribeNamespace(ctx, "__default__")
	switch status.Code(err) {
	case codes.OK:
		if ns == nil {
			return ownershipFilters{}, errors.New("pinecone: missing default namespace description")
		}
		if ns.Schema != nil {
			schema = ns.Schema
		}
	case codes.NotFound:
		// A new default namespace inherits its index's schema.
	default:
		return ownershipFilters{}, fmt.Errorf("pinecone: describe default namespace: %w", err)
	}
	if schema == nil {
		return ownershipFilters{document: true, revision: true, attempt: true}, nil
	}
	return ownershipFilters{
		document: schema.Fields["doc_id"].Filterable,
		revision: schema.Fields[domain.VectorRevisionKey].Filterable,
		attempt:  schema.Fields[domain.VectorAttemptKey].Filterable,
	}, nil
}

func (p *Client) cleanupOwnership(ctx context.Context) (ownershipFilters, error) {
	p.ownershipMu.Lock()
	filters, discover := p.ownershipFilters, p.discoverOwnership
	p.ownershipMu.Unlock()
	if discover == nil {
		return filters, nil
	}

	discoveryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	filters, err := discover(discoveryCtx)
	if err != nil {
		return ownershipFilters{}, err
	}
	p.ownershipMu.Lock()
	defer p.ownershipMu.Unlock()
	if p.discoverOwnership != nil {
		p.ownershipFilters, p.discoverOwnership = filters, nil
	}
	return p.ownershipFilters, nil
}

func newClient(pc *pinecone.Client, cfg *config.Config, root *pinecone.IndexConnection) *Client {
	return &Client{
		client: pc, cfg: cfg, defaultConn: sdkIndexConn{root}, batchSize: cfg.PineconeUpsertBatchSize,
		timeout: cfg.PineconeTimeout, maxAttempts: cfg.PineconeMaxAttempts, dims: cfg.EmbeddingDimensions,
		dial: func(namespace string) (indexConn, error) {
			return sdkIndexConn{root.WithNamespace(namespace)}, nil
		},
	}
}

// FixtureVectorIdentity identifies the concrete default index used for fixtures.
func (p *Client) FixtureVectorIdentity() string { return p.fixtureVectorIdentity }

// Host returns the physical index host resolved at startup.
func (p *Client) Host() string { return p.host }

func (p *Client) conn(namespace string) (indexConn, func(), error) {
	if namespace == "" {
		return p.defaultConn, func() {}, nil
	}
	conn, err := p.dial(namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("pinecone: connect to namespace %q: %w", namespace, err)
	}
	// Derived connections share the default transport and must not be closed.
	return conn, func() {}, nil
}

// Upsert inserts or updates vectors in the specified namespace.
func (p *Client) Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error {
	_, err := p.UpsertWithVisibility(ctx, namespace, chunks)
	return err
}

// UpsertWithVisibility inserts vectors and returns an attempt-bound probe when enabled and supported.
func (p *Client) UpsertWithVisibility(ctx context.Context, namespace string, chunks []domain.Chunk) (domain.VisibilityProbe, error) {
	vectors := make([]*pinecone.Vector, 0, len(chunks))
	sizes := make([]int, 0, len(chunks))
	for _, c := range chunks {
		if len(c.Embedding) == 0 {
			continue
		}
		mdMap := map[string]any{
			"doc_id": string(c.DocumentID), "chunk_index": float64(c.Index),
			"text": c.Text, "token_count": float64(c.TokenCount),
		}
		if c.VectorIncarnation != "" {
			mdMap[domain.VectorIncarnationKey] = c.VectorIncarnation
		}
		if c.RevisionID != "" {
			mdMap["revision_id"] = c.RevisionID
			mdMap["chunk_id"] = string(c.ID)
		}
		for _, key := range []string{"doc_type", "kind", "quality", "collection", domain.VectorTenantKey, domain.VectorRevisionKey, domain.VectorAttemptKey, domain.LayoutPagesKey, domain.LayoutTableIDsKey, domain.LayoutRowIDsKey} {
			if value, ok := c.Metadata[key]; ok {
				mdMap[key] = pineconeMetadataValue(value)
			}
		}
		encoded, err := json.Marshal(mdMap)
		if err != nil {
			return nil, fmt.Errorf("pinecone: serialize metadata for chunk %s: %w", c.ID, err)
		}
		if len(encoded) > maxMetadataBytes {
			return nil, fmt.Errorf("%w: chunk %s (%d bytes)", ErrMetadataTooLarge, c.ID, len(encoded))
		}
		md, err := structpb.NewStruct(mdMap)
		if err != nil {
			return nil, fmt.Errorf("pinecone: build metadata for chunk %s: %w", c.ID, err)
		}
		id := c.VectorID
		if id == "" {
			var idErr error
			id, idErr = physicalVectorID(c)
			if idErr != nil {
				return nil, idErr
			}
		} else if len(id) > 512 {
			return nil, fmt.Errorf("pinecone: chunk %s vector ID exceeds 512 bytes", c.ID)
		}
		size := 4*len(c.Embedding) + len(id) + proto.Size(md) + 32
		if size > maxUpsertBytes {
			return nil, fmt.Errorf("pinecone: chunk %s exceeds the upsert byte limit", c.ID)
		}
		vectors = append(vectors, &pinecone.Vector{Id: id, Values: &c.Embedding, Metadata: md})
		sizes = append(sizes, size)
	}
	if len(vectors) == 0 {
		return nil, nil
	}
	if p.visibility != nil {
		return p.upsertForVisibility(ctx, namespace, vectors)
	}
	return nil, p.upsertGRPC(ctx, namespace, vectors, sizes)
}

func (p *Client) upsertGRPC(ctx context.Context, namespace string, vectors []*pinecone.Vector, sizes []int) error {
	conn, release, err := p.conn(namespace)
	if err != nil {
		return err
	}
	defer release()
	for start := 0; start < len(vectors); {
		end, size := start, 0
		for end < len(vectors) && end-start < p.batchSize && size+sizes[end] <= maxUpsertBytes {
			size += sizes[end]
			end++
		}
		if err := p.mutate(ctx, false, func(attemptCtx context.Context) error {
			count, upsertErr := conn.UpsertVectors(attemptCtx, vectors[start:end])
			if upsertErr != nil {
				return upsertErr
			}
			if int64(count) != int64(end-start) {
				return fmt.Errorf("pinecone: acknowledged %d of %d upserted vectors", count, end-start)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("pinecone: upsert %d vectors to namespace %q: %w", end-start, namespace, err)
		}
		start = end
	}
	attrs := []any{"vectors", len(vectors)}
	if !tracing.RedactIdentifiers(ctx) {
		attrs = append(attrs, "namespace", namespace)
	}
	slog.DebugContext(ctx, "pinecone vectors upserted", attrs...)
	return nil
}

// Query searches for similar vectors in the specified namespace.
func (p *Client) Query(ctx context.Context, namespace string, embedding []float32, topK int, docTypeFilter string) ([]domain.ScoredChunk, error) {
	if topK < 1 || uint64(topK) > math.MaxUint32 {
		return nil, fmt.Errorf("pinecone: topK must be between 1 and %d", uint64(math.MaxUint32))
	}
	req := &pinecone.QueryByVectorValuesRequest{Vector: embedding, TopK: uint32(topK), IncludeMetadata: true}
	if docTypeFilter != "" {
		req.MetadataFilter = &structpb.Struct{Fields: map[string]*structpb.Value{"doc_type": structpb.NewStringValue(docTypeFilter)}}
	}
	return p.queryNamespace(ctx, namespace, req, nil)
}

func (p *Client) queryNamespace(ctx context.Context, namespace string, req *pinecone.QueryByVectorValuesRequest, budget *int) ([]domain.ScoredChunk, error) {
	conn, release, err := p.conn(namespace)
	if err != nil {
		return nil, err
	}
	defer release()
	res, err := p.queryMatches(ctx, conn, req, budget)
	if err != nil {
		return nil, fmt.Errorf("pinecone: query namespace %q: %w", namespace, err)
	}
	return matchesToScoredChunks(res, namespace), nil
}

func (p *Client) queryMatches(ctx context.Context, conn indexConn, req *pinecone.QueryByVectorValuesRequest, budget *int) ([]*pinecone.ScoredVector, error) {
	var res *pinecone.QueryVectorsResponse
	attempts := p.queryAttempts()
	if budget != nil {
		attempts = min(attempts, *budget)
	}
	err := p.do(ctx, attempts, func(attemptCtx context.Context) error {
		if budget != nil {
			*budget--
			attemptCtx = singleTransportAttempt(attemptCtx)
		}
		var queryErr error
		res, queryErr = conn.QueryByVectorValues(attemptCtx, req)
		return queryErr
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errors.New("pinecone: missing query response")
	}
	return res.Matches, nil
}

func (p *Client) queryAttempts() int { return min(3, p.maxAttempts) }

// QueryAllNamespaces searches the default namespace containing corpus vectors.
func (p *Client) QueryAllNamespaces(ctx context.Context, embedding []float32, topK int, docTypeFilter string) ([]domain.ScoredChunk, error) {
	return p.Query(ctx, "", embedding, topK, docTypeFilter)
}

// Delete removes vectors by ID from the specified namespace.
func (p *Client) Delete(ctx context.Context, namespace string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	conn, release, err := p.conn(namespace)
	if err != nil {
		return err
	}
	defer release()
	for start := 0; start < len(ids); start += 1000 {
		batch := ids[start:min(start+1000, len(ids))]
		if err := p.mutate(ctx, true, func(attemptCtx context.Context) error {
			return ignoreNotFound(DispatchWriterMutation(attemptCtx, namespace, func(ctx context.Context) error {
				return conn.DeleteVectorsByID(ctx, batch)
			}))
		}); err != nil {
			return fmt.Errorf("pinecone: delete %d vectors from namespace %q: %w", len(batch), namespace, err)
		}
	}
	return nil
}

// DeleteAllVectorsInNamespace deletes all vectors from the specified namespace.
func (p *Client) DeleteAllVectorsInNamespace(ctx context.Context, namespace string) error {
	conn, release, err := p.conn(namespace)
	if err != nil {
		return err
	}
	defer release()
	if err := p.mutate(ctx, true, func(attemptCtx context.Context) error {
		return ignoreNotFound(DispatchWriterMutation(attemptCtx, namespace, func(ctx context.Context) error {
			return conn.DeleteAllVectorsInNamespace(ctx)
		}))
	}); err != nil {
		return fmt.Errorf("pinecone: delete all vectors in namespace %q: %w", namespace, err)
	}
	if tracing.RedactIdentifiers(ctx) {
		slog.DebugContext(ctx, "pinecone namespace cleared")
	} else {
		slog.DebugContext(ctx, "pinecone namespace cleared", "namespace", namespace)
	}
	return nil
}

// DeleteByDocID removes untagged vectors for a document from the default namespace.
func (p *Client) DeleteByDocID(ctx context.Context, docID string) error {
	return p.deleteDocumentVectors(ctx, vectorTarget{document: docID})
}

// DeleteByRevision removes vectors for one incarnation from the default namespace.
func (p *Client) DeleteByRevision(ctx context.Context, docID, revision string) error {
	if !validVectorIDPart(docID) || !validVectorIDPart(revision) {
		return errors.New("pinecone: invalid document revision target")
	}
	return p.deleteDocumentVectors(ctx, vectorTarget{document: docID, revision: revision})
}

// DeleteByAttempt removes vectors that belong to a single claim.
func (p *Client) DeleteByAttempt(ctx context.Context, docID, revision, attempt string) error {
	if !validVectorIDPart(docID) || !validVectorIDPart(revision) || !validVectorIDPart(attempt) {
		return errors.New("pinecone: invalid vector attempt target")
	}
	return p.deleteDocumentVectors(ctx, vectorTarget{document: docID, revision: revision, attempt: attempt})
}

type vectorTarget struct {
	document string
	revision string
	attempt  string
}

func (t vectorTarget) prefix() string {
	prefix := vectorIDPrefix(t.document, t.revision)
	if t.attempt != "" {
		prefix += t.attempt + "/"
	}
	return prefix
}

// filter builds the narrowest verified predicate and reports whether it
// identifies the whole target without relying on physical ID validation.
func (f ownershipFilters) filter(target vectorTarget) (*structpb.Struct, bool, error) {
	if !f.document {
		return nil, false, nil
	}
	fields := map[string]any{"doc_id": map[string]any{"$eq": target.document}}
	var exact bool
	if target.revision == "" {
		exact = f.revision && f.attempt
		if f.revision {
			fields[domain.VectorRevisionKey] = map[string]any{"$exists": false}
		}
		if f.attempt {
			fields[domain.VectorAttemptKey] = map[string]any{"$exists": false}
		}
	} else {
		if f.revision {
			fields[domain.VectorRevisionKey] = map[string]any{"$eq": target.revision}
		}
		if target.attempt != "" && f.attempt {
			fields[domain.VectorAttemptKey] = map[string]any{"$eq": target.attempt}
		}
		exact = f.revision && (target.attempt == "" || f.attempt)
	}
	filter, err := structpb.NewStruct(fields)
	return filter, exact, err
}

func (p *Client) deleteDocumentVectors(ctx context.Context, target vectorTarget) error {
	if !validVectorIDPart(target.document) {
		return errors.New("pinecone: invalid document cleanup target")
	}
	ownership, err := p.cleanupOwnership(ctx)
	if err != nil {
		return err
	}
	filter, exact, err := ownership.filter(target)
	if err != nil {
		return fmt.Errorf("pinecone: build cleanup filter: %w", err)
	}
	acknowledged := false
	if exact {
		filterErr := p.mutate(ctx, true, func(ctx context.Context) error {
			return ignoreNotFound(DispatchWriterMutation(ctx, "", func(ctx context.Context) error {
				return p.defaultConn.DeleteVectorsByFilter(ctx, filter)
			}))
		})
		switch status.Code(filterErr) {
		case codes.OK:
			acknowledged = true
		case codes.InvalidArgument, codes.Unimplemented, codes.FailedPrecondition:
			// A provider can support metadata search without filter deletion.
		default:
			return filterErr
		}
	}
	if lister, ok := p.defaultConn.(vectorIDLister); ok && target.revision != "" {
		err := p.deleteRevisionByList(ctx, lister, target, acknowledged)
		if !errors.Is(err, errVectorListingUnsupported) {
			return err
		}
	}
	if filter == nil {
		return errors.New("pinecone: cannot verify cleanup ownership without an indexed doc_id or prefix listing")
	}
	return p.deleteByDocumentMatches(ctx, filter, target, acknowledged)
}

var errVectorListingUnsupported = errors.New("pinecone: vector listing unsupported")

type vectorIDLister interface {
	ListVectors(context.Context, *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error)
}

func vectorIDPrefix(docID, revision string) string { return docID + "/" + revision + "/" }

func validVectorIDPart(part string) bool { return part != "" && !strings.Contains(part, "/") }

// TaggedVectorID formats a revision- and attempt-qualified hosted vector ID.
func TaggedVectorID(docID, revision, attempt, chunkID string) (string, error) {
	if !validVectorIDPart(docID) || !validVectorIDPart(revision) || !validVectorIDPart(chunkID) {
		return "", errors.New("pinecone: invalid tagged vector identity")
	}
	target := vectorTarget{document: docID, revision: revision}
	if attempt != "" {
		if !validVectorIDPart(attempt) {
			return "", errors.New("pinecone: invalid vector attempt")
		}
		target.attempt = attempt
	}
	id := target.prefix() + chunkID
	if len(id) > 512 {
		return "", errors.New("pinecone: tagged vector ID exceeds 512 bytes")
	}
	return id, nil
}

func physicalVectorID(chunk domain.Chunk) (string, error) {
	id := string(chunk.ID)
	value, tagged := chunk.Metadata[domain.VectorRevisionKey]
	attemptValue, hasAttempt := chunk.Metadata[domain.VectorAttemptKey]
	if !tagged {
		if hasAttempt {
			return "", errors.New("pinecone: vector attempt requires a revision")
		}
		return id, nil
	}
	revision, _ := value.(string)
	var attempt string
	if hasAttempt {
		var ok bool
		attempt, ok = attemptValue.(string)
		if !ok || attempt == "" {
			return "", errors.New("pinecone: invalid vector attempt")
		}
	}
	return TaggedVectorID(string(chunk.DocumentID), revision, attempt, id)
}

func (t vectorTarget) logicalID(physicalID string) (string, bool) {
	if !validVectorIDPart(t.document) || !validVectorIDPart(t.revision) {
		return "", false
	}
	prefix := t.prefix()
	id, ok := strings.CutPrefix(physicalID, prefix)
	return id, ok && validVectorIDPart(id)
}

func (p *Client) deleteRevisionByList(ctx context.Context, lister vectorIDLister, target vectorTarget, checkingCompletion bool) error {
	prefix := target.prefix()
	req := &pinecone.ListVectorsRequest{Prefix: &prefix, Limit: new(uint32(100))}
	tokens := make(map[string]struct{})
	firstPage := true
	for {
		var listed *pinecone.ListVectorsResponse
		if err := p.do(ctx, min(3, p.maxAttempts), func(callCtx context.Context) error {
			var listErr error
			listed, listErr = lister.ListVectors(callCtx, req)
			return listErr
		}); err != nil {
			if firstPage && status.Code(err) == codes.Unimplemented {
				return errVectorListingUnsupported
			}
			return fmt.Errorf("list revision vectors: %w", err)
		}
		firstPage = false
		if listed == nil || len(listed.VectorIds) > int(*req.Limit) {
			return errors.New("pinecone: missing revision listing")
		}
		lastPage := listed.NextPaginationToken == nil || *listed.NextPaginationToken == ""
		empty := len(listed.VectorIds) == 0 && lastPage
		if req.PaginationToken == nil && empty {
			return nil
		}
		if checkingCompletion {
			return errors.New("pinecone: revision vectors remain visible after deletion")
		}
		ids := make([]string, 0, len(listed.VectorIds))
		for _, id := range listed.VectorIds {
			if id == nil {
				return errors.New("pinecone: missing vector ID in revision listing")
			}
			suffix, qualified := strings.CutPrefix(*id, prefix)
			if !qualified || !validListedSuffix(suffix, target.attempt == "") {
				return errors.New("pinecone: unexpected vector ID in revision listing")
			}
			ids = append(ids, *id)
		}
		if !lastPage {
			token := *listed.NextPaginationToken
			if _, seen := tokens[token]; seen {
				return errors.New("pinecone: revision listing did not advance")
			}
			tokens[token] = struct{}{}
		}
		if err := p.Delete(ctx, "", ids); err != nil {
			return err
		}
		req.PaginationToken = listed.NextPaginationToken
		if lastPage {
			// A listing can shift after deletions. Confirm from the beginning before
			// completing the checkpoint.
			req.PaginationToken = nil
			checkingCompletion = true
		}
	}
}

func validListedSuffix(suffix string, allowAttempt bool) bool {
	if attempt, id, found := strings.Cut(suffix, "/"); found {
		return allowAttempt && validVectorIDPart(attempt) && validVectorIDPart(id)
	}
	return validVectorIDPart(suffix)
}

func (p *Client) deleteByDocumentMatches(ctx context.Context, filter *structpb.Struct, target vectorTarget, acknowledged bool) error {
	// Every successful full page removes new IDs. Bound the operation by its
	// call budget even when a selective namespace contains many records.
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	vector := make([]float32, p.dims)
	vector[0] = 1
	req := &pinecone.QueryByVectorValuesRequest{Vector: vector, TopK: cleanupPageSize, MetadataFilter: filter, IncludeMetadata: true}
	deleted := make(map[string]struct{})
	for {
		matches, err := p.queryMatches(ctx, p.defaultConn, req, nil)
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(matches))
		remaining := false
		for _, match := range matches {
			id, belongs, verifyErr := deletionMatch(match, target)
			if verifyErr != nil {
				return verifyErr
			}
			if !belongs {
				continue
			}
			remaining = true
			if _, seen := deleted[id]; !seen {
				ids = append(ids, id)
				deleted[id] = struct{}{}
			}
		}
		if acknowledged && remaining {
			return errors.New("pinecone: document vectors remain visible after deletion")
		}
		if len(ids) == 0 {
			if len(matches) >= int(req.TopK) {
				return errors.New("pinecone: a full page of stale results obscures remaining vectors")
			}
			if remaining {
				return errors.New("pinecone: document vectors remain visible after deletion")
			}
			return nil
		}
		if err := p.Delete(ctx, "", ids); err != nil {
			return err
		}
	}
}

// deletionMatch verifies provider results before using their physical IDs.
func deletionMatch(match *pinecone.ScoredVector, target vectorTarget) (id string, belongs bool, err error) {
	if match == nil || match.Vector == nil {
		return "", false, errors.New("pinecone: cannot verify vector identity for cleanup")
	}
	v := match.Vector
	fields := v.Metadata.GetFields()
	if !validVectorIDPart(target.document) || fields["doc_id"].GetStringValue() != target.document {
		return "", false, errors.New("pinecone: cannot verify document identity for cleanup")
	}
	if value, tagged := fields[domain.VectorRevisionKey]; tagged {
		observed := vectorTarget{document: target.document, revision: value.GetStringValue()}
		if attempt, present := fields[domain.VectorAttemptKey]; present {
			observed.attempt = attempt.GetStringValue()
			if !validVectorIDPart(observed.attempt) {
				return "", false, errors.New("pinecone: cannot verify vector attempt for cleanup")
			}
		}
		if _, ok := observed.logicalID(v.Id); !ok {
			return "", false, errors.New("pinecone: cannot verify tagged vector identity for cleanup")
		}
		return v.Id, target.revision == observed.revision && (target.attempt == "" || target.attempt == observed.attempt), nil
	}
	if _, hasAttempt := fields[domain.VectorAttemptKey]; hasAttempt || !validVectorIDPart(v.Id) {
		return "", false, errors.New("pinecone: cannot verify untagged vector identity for cleanup")
	}
	return v.Id, target.revision == "", nil
}

func ignoreNotFound(err error) error {
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// DeleteVectorIDs removes exact external vector IDs from namespace.
func (p *Client) DeleteVectorIDs(ctx context.Context, namespace string, ids []string) error {
	return p.Delete(ctx, namespace, ids)
}

// DeleteByManifestRevision deletes vectors belonging only to (docID, revisionID),
// using exact IDs when supplied or falling back to revision-scoped filtering.
func (p *Client) DeleteByManifestRevision(ctx context.Context, docID, revisionID string, knownIDs []string) error {
	if revisionID == "" {
		return errors.New("pinecone: revision_id is required for revision-targeted deletion")
	}
	if len(knownIDs) > 0 {
		return p.Delete(ctx, "", knownIDs)
	}
	filter, err := structpb.NewStruct(map[string]any{
		"doc_id":      map[string]any{"$eq": docID},
		"revision_id": map[string]any{"$eq": revisionID},
	})
	if err != nil {
		return fmt.Errorf("pinecone: build revision filter: %w", err)
	}
	err = p.mutate(ctx, true, func(attemptCtx context.Context) error {
		return ignoreNotFound(DispatchWriterMutation(attemptCtx, "", func(ctx context.Context) error {
			return p.defaultConn.DeleteVectorsByFilter(ctx, filter)
		}))
	})
	switch status.Code(err) {
	case codes.InvalidArgument, codes.Unimplemented, codes.FailedPrecondition:
		prefix := domain.RevisionVectorPrefix(domain.DocumentID(docID), revisionID)
		listedErr := p.deleteByRevisionPrefix(ctx, "", prefix)
		if listedErr == nil {
			return nil
		}
		if !errors.Is(listedErr, errPrefixListUnsupported) {
			return fmt.Errorf("pinecone: delete vectors by revision %s: %w", revisionID, listedErr)
		}
		err = p.deleteByManifestQuery(ctx, docID, revisionID, filter)
	}
	if err != nil {
		return fmt.Errorf("pinecone: delete vectors by revision %s: %w", revisionID, err)
	}
	return nil
}

func (p *Client) deleteByManifestQuery(ctx context.Context, docID, revisionID string, filter *structpb.Struct) error {
	vector := make([]float32, p.dims)
	vector[0] = 1
	req := &pinecone.QueryByVectorValuesRequest{Vector: vector, TopK: cleanupPageSize, MetadataFilter: filter, IncludeMetadata: true}
	deleted := make(map[string]struct{})
	for {
		matches, err := p.queryMatches(ctx, p.defaultConn, req, nil)
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(matches))
		for _, match := range matches {
			if match == nil || match.Vector == nil {
				continue
			}
			fields := match.Vector.Metadata.GetFields()
			if fields["doc_id"].GetStringValue() != docID || fields["revision_id"].GetStringValue() != revisionID {
				continue
			}
			if _, ok := deleted[match.Vector.Id]; !ok {
				deleted[match.Vector.Id] = struct{}{}
				ids = append(ids, match.Vector.Id)
			}
		}
		if len(ids) == 0 {
			return nil
		}
		if err := p.Delete(ctx, "", ids); err != nil {
			return err
		}
		if len(matches) < int(req.TopK) {
			return nil
		}
	}
}

var errPrefixListUnsupported = errors.New("pinecone: prefix listing is unsupported")

func (p *Client) deleteByRevisionPrefix(ctx context.Context, namespace, prefix string) error {
	var token string
	seen := make(map[string]bool)
	for {
		ids, next, supported, err := p.ListPrefixVectorIDs(ctx, namespace, prefix, 100, token)
		if err != nil {
			return err
		}
		if !supported {
			return errPrefixListUnsupported
		}
		if len(ids) > 0 {
			if err := p.Delete(ctx, namespace, ids); err != nil {
				return err
			}
		}
		if next == "" {
			return nil
		}
		if seen[next] {
			return errors.New("pinecone: vector listing repeated a pagination token")
		}
		seen[next] = true
		token = next
	}
}

// FetchPresentVectorIDs returns the subset of ids currently stored in namespace.
func (p *Client) FetchPresentVectorIDs(ctx context.Context, namespace string, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	conn, release, err := p.conn(namespace)
	if err != nil {
		return nil, err
	}
	defer release()
	var present []string
	for start := 0; start < len(ids); start += 1000 {
		batch := ids[start:min(start+1000, len(ids))]
		var res *pinecone.FetchVectorsResponse
		if err := p.do(ctx, p.queryAttempts(), func(attemptCtx context.Context) error {
			var fetchErr error
			res, fetchErr = conn.FetchVectors(attemptCtx, batch)
			return fetchErr
		}); err != nil {
			return nil, fmt.Errorf("pinecone: fetch vectors in namespace %q: %w", namespace, err)
		}
		if res == nil {
			continue
		}
		for _, id := range batch {
			if res.Vectors[id] != nil {
				present = append(present, id)
			}
		}
	}
	return present, nil
}

// ListPrefixVectorIDs lists one page of external IDs matching prefix when supported.
func (p *Client) ListPrefixVectorIDs(ctx context.Context, namespace, prefix string, limit int, token string) (ids []string, nextToken string, supported bool, err error) {
	if limit < 1 || limit > 1000 {
		return nil, "", false, fmt.Errorf("pinecone: list limit must be between 1 and 1000")
	}
	conn, release, err := p.conn(namespace)
	if err != nil {
		return nil, "", false, err
	}
	defer release()
	page := uint32(limit)
	req := &pinecone.ListVectorsRequest{Prefix: &prefix, Limit: &page}
	if token != "" {
		req.PaginationToken = &token
	}
	var res *pinecone.ListVectorsResponse
	if doErr := p.do(ctx, p.queryAttempts(), func(attemptCtx context.Context) error {
		var listErr error
		res, listErr = conn.ListVectors(attemptCtx, req)
		return listErr
	}); doErr != nil {
		if IsListUnsupported(doErr) {
			return nil, "", false, nil
		}
		return nil, "", true, fmt.Errorf("pinecone: list prefix %q in namespace %q: %w", prefix, namespace, doErr)
	}
	if res == nil {
		return nil, "", true, nil
	}
	out := make([]string, 0, len(res.VectorIds))
	for _, id := range res.VectorIds {
		if id != nil && *id != "" {
			out = append(out, *id)
		}
	}
	next := ""
	if res.NextPaginationToken != nil {
		next = *res.NextPaginationToken
	}
	return out, next, true, nil
}

// IsListUnsupported reports whether a Pinecone index error indicates that vector
// listing is unsupported (such as pod-based indexes).
func IsListUnsupported(err error) bool {
	if status.Code(err) == codes.Unimplemented {
		return true
	}
	if status.Code(err) != codes.InvalidArgument && status.Code(err) != codes.FailedPrecondition {
		return false
	}
	message := strings.ToLower(status.Convert(err).Message())
	return strings.Contains(message, "list") && (strings.Contains(message, "not supported") ||
		(strings.Contains(message, "only supported") && strings.Contains(message, "serverless")))
}

// Ping verifies connectivity to the Pinecone index.
func (p *Client) Ping(ctx context.Context) error {
	return p.do(ctx, min(3, p.maxAttempts), func(attemptCtx context.Context) error {
		_, err := p.client.DescribeIndex(attemptCtx, p.cfg.PineconeIndexName)
		if err != nil {
			return fmt.Errorf("pinecone: ping: %w", err)
		}
		return nil
	})
}

// chunkFromMetadata decodes a stored record into its logical chunk. It reports
// false for a record whose incarnation, revision or attempt tags are not
// self-consistent, so that no reader treats it as a retrievable chunk.
func chunkFromMetadata(id string, md *structpb.Struct) (domain.Chunk, bool) {
	return chunkFromMetadataAtIndex(id, md, int(md.GetFields()["chunk_index"].GetNumberValue()))
}

func chunkFromMetadataAtIndex(id string, md *structpb.Struct, index int) (domain.Chunk, bool) {
	chunk := domain.Chunk{ID: domain.ChunkID(id), VectorID: id, Index: index}
	if md == nil {
		return chunk, true
	}
	fields := md.GetFields()
	if v, ok := fields["chunk_id"]; ok && v.GetStringValue() != "" {
		chunk.ID = domain.ChunkID(v.GetStringValue())
	}
	if v, ok := fields["revision_id"]; ok && v.GetStringValue() != "" {
		chunk.RevisionID = v.GetStringValue()
	}
	if v, ok := fields["doc_id"]; ok {
		chunk.DocumentID = domain.DocumentID(v.GetStringValue())
	}
	if v, ok := fields["text"]; ok {
		chunk.Text = v.GetStringValue()
	}
	if v, ok := fields["token_count"]; ok {
		chunk.TokenCount = int(v.GetNumberValue())
	}
	if v, ok := fields[domain.VectorIncarnationKey]; ok {
		chunk.VectorIncarnation = v.GetStringValue()
		if chunk.VectorIncarnation == "" || (chunk.RevisionID != "" && !domain.ValidManifestIncarnation(chunk.VectorIncarnation)) {
			return chunk, false
		}
	}
	chunk.Metadata = make(map[string]any)
	for _, key := range []string{"doc_type", "kind", "quality", "collection", domain.VectorRevisionKey, domain.VectorAttemptKey} {
		if v, ok := fields[key]; ok {
			chunk.Metadata[key] = v.GetStringValue()
		}
	}
	if tenant, present := fields[domain.VectorTenantKey]; present {
		// Retain the type of a supplied label so it cannot decode as an
		// empty string and match a document with no tenant.
		chunk.Metadata[domain.VectorTenantKey] = tenant.AsInterface()
	}
	if value, tagged := fields[domain.VectorRevisionKey]; tagged {
		if chunk.RevisionID != "" {
			return chunk, false
		}
		target := vectorTarget{document: string(chunk.DocumentID), revision: value.GetStringValue()}
		if attempt, present := fields[domain.VectorAttemptKey]; present {
			target.attempt = attempt.GetStringValue()
			if !validVectorIDPart(target.attempt) {
				return chunk, false
			}
		}
		logical, ok := target.logicalID(id)
		if !ok {
			return chunk, false
		}
		chunk.ID = domain.ChunkID(logical)
	} else if _, hasAttempt := fields[domain.VectorAttemptKey]; hasAttempt {
		return chunk, false
	}
	if pages := layoutPagesFromMetadata(fields[domain.LayoutPagesKey]); len(pages) > 0 {
		chunk.Metadata[domain.LayoutPagesKey] = pages
	}
	if ids := layoutIDsFromMetadata(fields[domain.LayoutTableIDsKey], domain.ValidateLayoutTableIDs); len(ids) > 0 {
		chunk.Metadata[domain.LayoutTableIDsKey] = ids
	}
	if ids := layoutIDsFromMetadata(fields[domain.LayoutRowIDsKey], domain.ValidateLayoutRowIDs); len(ids) > 0 {
		chunk.Metadata[domain.LayoutRowIDsKey] = ids
	}
	return chunk, true
}

func layoutListFromMetadata(value *structpb.Value) []*structpb.Value {
	list, ok := value.GetKind().(*structpb.Value_ListValue)
	if !ok || list == nil {
		return nil
	}
	return list.ListValue.GetValues()
}

func layoutPagesFromMetadata(value *structpb.Value) []int {
	items := layoutListFromMetadata(value)
	if len(items) == 0 || len(items) > domain.MaxPDFPages {
		return nil
	}
	pages := make([]int, len(items))
	for i, item := range items {
		text, ok := item.GetKind().(*structpb.Value_StringValue)
		if !ok || text == nil {
			return nil
		}
		page, err := strconv.ParseUint(text.StringValue, 10, 16)
		if err != nil || page < 1 || page > domain.MaxPDFPages || strconv.FormatUint(page, 10) != text.StringValue {
			return nil
		}
		pages[i] = int(page)
	}
	valid, err := domain.ValidateLayoutPages(pages)
	if err != nil {
		return nil
	}
	return valid
}

func layoutIDsFromMetadata(value *structpb.Value, validate func([]string) ([]string, error)) []string {
	items := layoutListFromMetadata(value)
	if len(items) == 0 || len(items) > domain.MaxLayoutProvenanceIDs {
		return nil
	}
	ids := make([]string, len(items))
	for i, item := range items {
		text, ok := item.GetKind().(*structpb.Value_StringValue)
		if !ok || text == nil || text.StringValue == "" {
			return nil
		}
		ids[i] = text.StringValue
	}
	valid, err := validate(ids)
	if err != nil {
		return nil
	}
	return valid
}

func matchesToScoredChunks(matches []*pinecone.ScoredVector, namespace string) []domain.ScoredChunk {
	scored := make([]domain.ScoredChunk, 0, len(matches))
	for _, m := range matches {
		if m == nil || m.Vector == nil {
			continue
		}
		chunk, ok := chunkFromMetadata(m.Vector.Id, m.Vector.Metadata)
		if !ok {
			continue
		}
		scored = append(scored, domain.ScoredChunk{
			Chunk:     chunk,
			Score:     float64(m.Score),
			Namespace: namespace,
		})
	}
	return scored
}

func pineconeMetadataValue(v any) any {
	switch list := v.(type) {
	case []int:
		out := make([]any, len(list))
		for i, n := range list {
			out[i] = strconv.Itoa(n)
		}
		return out
	case []string:
		out := make([]any, len(list))
		for i, s := range list {
			out[i] = s
		}
		return out
	default:
		return v
	}
}

// Close releases the shared default connection.
func (p *Client) Close() error {
	p.closeOnce.Do(func() { p.closeErr = p.defaultConn.Close() })
	return p.closeErr
}
