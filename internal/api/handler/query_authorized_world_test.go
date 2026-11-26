package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

// Two tenants, three principals and four documents. The hidden document scores
// highest for every query and carries a preview that names an entity, so any
// path that read it would change the answer or the route.
const (
	tenantA = "00000000-0000-4000-8000-0000000000a0"
	tenantB = "00000000-0000-4000-8000-0000000000b0"
	userA1  = "00000000-0000-4000-8000-0000000000a1"
	userA2  = "00000000-0000-4000-8000-0000000000a2"
	userB1  = "00000000-0000-4000-8000-0000000000b1"

	docVisible = "00000000-0000-4000-8000-000000000001"
	docShared  = "00000000-0000-4000-8000-000000000002"
	docHidden  = "00000000-0000-4000-8000-000000000003"
	docOther   = "00000000-0000-4000-8000-000000000004"
	docMissing = "00000000-0000-4000-8000-0000000000ff"

	hiddenText  = "SECRET hidden archive text for zeta."
	visibleText = "Archive alpha policy visible text."
	sharedText  = "Archive beta guide shared text."
)

type grantSet struct {
	id, revision, owner, tenant string
	members                     map[string]string
	expired                     bool
}

type grantDocument struct {
	id, incarnation, tenant, collection, docType, text, preview string
	score                                                       float64
	readers                                                     map[string]bool
}

func (d *grantDocument) chunk() domain.Chunk {
	return domain.Chunk{ID: domain.ChunkID(d.id + "-chunk"), DocumentID: domain.DocumentID(d.id), Text: d.text,
		Metadata: map[string]any{"collection": d.collection, "doc_type": d.docType}}
}

// grantWorld is a recording double for the catalog, the readable relation and
// the evidence checks. It applies the same rules as the SQL: a document is
// readable only by an active principal of its tenant that currently holds a grant.
type grantWorld struct {
	mu        sync.Mutex
	documents []*grantDocument
	disabled  map[string]bool
	revision  int64
	// stale makes ReadState report an older revision, as a lagging read would.
	stale int64

	deriveErr, stateErr, evidenceErr error
	trustErr                         error

	// sets are the document sets a principal owns. A set is usable only by its
	// owner, before it expires, and only for the incarnations it names.
	sets map[string]*grantSet

	derived  []domain.ReadScopeRequest
	previews [][]string
	log      []string
	// onCheck runs after each recorded authority check with its ordinal.
	onCheck func(kind string, n int)
	checks  map[string]int
}

func newGrantWorld() *grantWorld {
	return &grantWorld{
		revision: 7,
		disabled: map[string]bool{},
		checks:   map[string]int{},
		sets:     map[string]*grantSet{},
		documents: []*grantDocument{
			{id: docVisible, incarnation: "11111111-1111-4111-8111-111111111111", tenant: tenantA, collection: "archive", docType: "guide",
				text: visibleText, preview: "Alpha Records office", score: .90, readers: map[string]bool{userA1: true}},
			{id: docShared, incarnation: "22222222-2222-4222-8222-222222222222", tenant: tenantA, collection: "archive", docType: "guide",
				text: sharedText, preview: "Beta Guides office", score: .80, readers: map[string]bool{userA1: true, userA2: true}},
			{id: docHidden, incarnation: "33333333-3333-4333-8333-333333333333", tenant: tenantA, collection: "archive", docType: "guide",
				text: hiddenText, preview: "Zephyr Industries confidential", score: .99, readers: map[string]bool{userA2: true}},
			{id: docOther, incarnation: "44444444-4444-4444-8444-444444444444", tenant: tenantB, collection: "archive", docType: "guide",
				text: visibleText, preview: "Alpha Records office", score: .98, readers: map[string]bool{userB1: true}},
		},
	}
}

func (w *grantWorld) record(kind string) {
	w.mu.Lock()
	w.log = append(w.log, kind)
	w.checks[kind]++
	n, hook := w.checks[kind], w.onCheck
	w.mu.Unlock()
	if hook != nil {
		hook(kind, n)
	}
}

func (w *grantWorld) send(kind string) { w.record("send:" + kind) }

func (w *grantWorld) document(id string) *grantDocument {
	for _, d := range w.documents {
		if d.id == id {
			return d
		}
	}
	return nil
}

func (w *grantWorld) readable(p domain.Principal, d *grantDocument) bool {
	return d != nil && !w.disabled[p.ID] && d.tenant == p.TenantID && d.readers[p.ID]
}

func (w *grantWorld) revoke(principal, document string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.document(document).readers, principal)
	w.revision++
}

func (w *grantWorld) disable(principal string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.disabled[principal] = true
	w.revision++
}

func (w *grantWorld) logSnapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.log)
}

func (w *grantWorld) DeriveReadScope(_ context.Context, p domain.Principal, req domain.ReadScopeRequest) (domain.ReadScope, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.derived = append(w.derived, domain.ReadScopeRequest{DocumentIDs: slices.Clone(req.DocumentIDs), Collection: req.Collection, DocType: req.DocType, Membership: req.Membership, DocumentSetID: req.DocumentSetID})
	if w.deriveErr != nil {
		return domain.ReadScope{}, w.deriveErr
	}
	if w.disabled[p.ID] {
		return domain.ReadScope{}, domain.ErrReadDenied
	}
	var set *grantSet
	if req.DocumentSetID != "" {
		if set = w.usableSet(p, req.DocumentSetID); set == nil {
			return domain.ReadScope{}, domain.ErrDocumentSetNotFound
		}
	}
	var docs []domain.ReadableDocument
	for _, d := range w.documents {
		if !w.readable(p, d) || (req.DocumentIDs != nil && !slices.Contains(req.DocumentIDs, d.id)) ||
			(req.Collection != "" && d.collection != req.Collection) || (req.DocType != "" && d.docType != req.DocType) ||
			(set != nil && set.members[d.id] != d.incarnation) {
			continue
		}
		docs = append(docs, domain.ReadableDocument{ID: d.id, Incarnation: d.incarnation})
	}
	slices.SortFunc(docs, func(a, b domain.ReadableDocument) int { return strings.Compare(a.ID, b.ID) })
	scope := domain.ReadScope{Revision: w.revision, Empty: len(docs) == 0, Documents: docs}
	if set != nil {
		scope.DocumentSet = &domain.DocumentSetRef{ID: set.id, Revision: set.revision, ExpiresAt: time.Now().Add(time.Hour)}
	}
	if req.Membership <= 0 {
		scope.Documents = nil
	} else if len(docs) > req.Membership {
		scope.Exceeded, scope.Documents = true, nil
	}
	return scope, nil
}

func (w *grantWorld) ReadState(_ context.Context, p domain.Principal) (domain.ReadState, error) {
	w.record("state")
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stateErr != nil {
		return domain.ReadState{}, w.stateErr
	}
	revision := w.revision
	if w.stale != 0 {
		revision = w.stale
	}
	return domain.ReadState{Active: !w.disabled[p.ID], Revision: revision}, nil
}

func (w *grantWorld) Evidence(p domain.Principal, collection, docType string) domain.SubjectEvidence {
	e := &grantEvidence{w: w, principal: p, collection: collection, docType: docType}
	return domain.SubjectEvidence{Visibility: e, Documents: e, Previews: e}
}

// usableSet returns the set only for its live owner. The caller holds the lock.
func (w *grantWorld) usableSet(p domain.Principal, id string) *grantSet {
	set := w.sets[id]
	if set == nil || set.expired || set.owner != p.ID || set.tenant != p.TenantID || w.disabled[p.ID] {
		return nil
	}
	return set
}

func (w *grantWorld) DocumentSetState(_ context.Context, p domain.Principal, ref domain.DocumentSetRef) (domain.ReadState, bool, error) {
	w.record("state")
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stateErr != nil {
		return domain.ReadState{}, false, w.stateErr
	}
	revision := w.revision
	if w.stale != 0 {
		revision = w.stale
	}
	set := w.usableSet(p, ref.ID)
	return domain.ReadState{Active: !w.disabled[p.ID], Revision: revision}, set != nil && set.revision == ref.Revision, nil
}

func (w *grantWorld) EvidenceInSet(p domain.Principal, ref domain.DocumentSetRef, collection, docType string) domain.SubjectEvidence {
	e := &grantEvidence{w: w, principal: p, collection: collection, docType: docType, setID: ref.ID}
	return domain.SubjectEvidence{Visibility: e, Documents: e, Previews: e}
}

type grantEvidence struct {
	w                   *grantWorld
	principal           domain.Principal
	collection, docType string
	setID               string
}

func (e *grantEvidence) inScope(d *grantDocument) bool {
	if d == nil || !e.w.readable(e.principal, d) || (e.collection != "" && d.collection != e.collection) || (e.docType != "" && d.docType != e.docType) {
		return false
	}
	if e.setID == "" {
		return true
	}
	set := e.w.usableSet(e.principal, e.setID)
	return set != nil && set.members[d.id] == d.incarnation
}

func (e *grantEvidence) VisibleEvidence(ctx context.Context, candidates []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	visible, _, err := e.VisibleEvidenceWithPreviews(ctx, candidates, nil)
	return visible, err
}

func (e *grantEvidence) VisibleEvidenceWithPreviews(_ context.Context, candidates []domain.ScoredChunk, probeIDs []string) ([]domain.ScoredChunk, map[string]string, error) {
	e.w.record("evidence")
	e.w.mu.Lock()
	defer e.w.mu.Unlock()
	if e.w.evidenceErr != nil {
		return nil, nil, e.w.evidenceErr
	}
	if probeIDs != nil {
		e.w.previews = append(e.w.previews, slices.Clone(probeIDs))
	}
	var visible []domain.ScoredChunk
	previews := map[string]string{}
	for _, c := range candidates {
		d := e.w.document(string(c.Chunk.DocumentID))
		if !e.inScope(d) || c.Chunk.Text != d.text {
			continue
		}
		if revision, tagged := c.Chunk.Metadata[domain.VectorRevisionKey].(string); tagged && revision != d.incarnation {
			continue
		}
		c.ObservedIncarnation = d.incarnation
		metadata := map[string]any{}
		for k, v := range c.Chunk.Metadata {
			metadata[k] = v
		}
		metadata["doc_type"] = d.docType
		c.Chunk.Metadata = metadata
		visible = append(visible, c)
		if slices.Contains(probeIDs, d.id) {
			previews[d.id] = d.preview
		}
	}
	return visible, previews, nil
}

func (e *grantEvidence) VisibleDocumentIDs(_ context.Context, ids []string) (map[string]struct{}, error) {
	e.w.mu.Lock()
	defer e.w.mu.Unlock()
	visible := map[string]struct{}{}
	for _, id := range ids {
		if e.inScope(e.w.document(id)) {
			visible[id] = struct{}{}
		}
	}
	return visible, nil
}

func (e *grantEvidence) DocumentPreviews(_ context.Context, ids []string) (map[string]string, error) {
	e.w.mu.Lock()
	defer e.w.mu.Unlock()
	e.w.previews = append(e.w.previews, slices.Clone(ids))
	previews := map[string]string{}
	for _, id := range ids {
		if d := e.w.document(id); e.inScope(d) {
			previews[id] = d.preview
		}
	}
	return previews, nil
}

// grantStore is a recording vector store that applies a filter the way each
// real backend does and keeps every filter it was asked to apply.
type grantStore struct {
	domain.VectorStore
	w    *grantWorld
	kind domain.GrantScopeKind
	// noSets makes a relation store unable to join a set's membership.
	noSets bool
	// partitionSize splits a hosted search into requests of this many members.
	// Zero keeps one request.
	partitionSize int
	// hostedCalls counts the simulated hosted requests of every search.
	hostedCalls int

	mu      sync.Mutex
	filters []domain.VectorFilter
	// hostile hits are returned whatever the filter says, as a misbehaving
	// or stale backend could.
	hostile []domain.ScoredChunk
	// failSearch returns an error for the numbered search (1-based).
	failSearch map[int]error
	afterQuery func(n int)
	unscoped   int
}

func (s *grantStore) GrantScope() domain.GrantScopeKind { return s.kind }

func (s *grantStore) SupportsDocumentSets() bool { return !s.noSets }

func (s *grantStore) SearchPartitions(members int) (int, bool) {
	if s.partitionSize <= 0 {
		return min(members, 1), true
	}
	return (members + s.partitionSize - 1) / s.partitionSize, true
}

func (s *grantStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	s.mu.Lock()
	s.unscoped++
	s.mu.Unlock()
	return []domain.ScoredChunk{s.w.document(docHidden).hit(false)}, nil
}

func (d *grantDocument) hit(tagged bool) domain.ScoredChunk {
	c := d.chunk()
	if tagged {
		c.Metadata[domain.VectorTenantKey] = d.tenant
		c.Metadata[domain.VectorRevisionKey] = d.incarnation
	}
	return domain.ScoredChunk{Chunk: c, Score: d.score}
}

func (s *grantStore) QueryFiltered(ctx context.Context, _ []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.w.send("search")
	s.mu.Lock()
	s.filters = append(s.filters, filter)
	n := len(s.filters)
	failure := s.failSearch[n]
	s.mu.Unlock()
	if failure != nil {
		return nil, failure
	}
	if err := s.simulateHostedRequests(ctx, filter); err != nil {
		return nil, err
	}
	if filter.Authorized == nil {
		s.mu.Lock()
		s.unscoped++
		s.mu.Unlock()
	} else if s.kind == domain.GrantScopeMembership && filter.DocumentIDs == nil {
		return nil, domain.ErrGrantScopeUnsupported
	}
	s.w.mu.Lock()
	var hits []domain.ScoredChunk
	for _, d := range s.w.documents {
		if s.admits(filter, d) {
			hits = append(hits, d.hit(s.kind == domain.GrantScopeMembership))
		}
	}
	s.w.mu.Unlock()
	hits = append(hits, s.hostile...)
	slices.SortStableFunc(hits, func(a, b domain.ScoredChunk) int {
		switch {
		case a.Score > b.Score:
			return -1
		case a.Score < b.Score:
			return 1
		}
		return 0
	})
	if len(hits) > topK {
		hits = hits[:topK]
	}
	if s.afterQuery != nil {
		s.afterQuery(n)
	}
	return hits, nil
}

// simulateHostedRequests sends one request per partition of a membership search
// the way the hosted store does: it checks the authority before each and
// reserves its attempt from the request budget.
func (s *grantStore) simulateHostedRequests(ctx context.Context, filter domain.VectorFilter) error {
	if s.kind != domain.GrantScopeMembership || filter.Authorized == nil || len(filter.DocumentIDs) == 0 {
		return nil
	}
	partitions, _ := s.SearchPartitions(len(filter.DocumentIDs))
	for range partitions {
		if gate := filter.Authorized.BeforeSearch; gate != nil {
			if err := gate(ctx); err != nil {
				return err
			}
		}
		if budget := filter.Authorized.Budget; budget != nil {
			allowed, err := budget.Acquire(1)
			if err != nil {
				return err
			}
			budget.Release(allowed, allowed)
		}
		s.mu.Lock()
		s.hostedCalls++
		s.mu.Unlock()
	}
	return nil
}

func (s *grantStore) admits(filter domain.VectorFilter, d *grantDocument) bool {
	if scope := filter.Authorized; scope != nil {
		switch s.kind {
		case domain.GrantScopeRelation:
			if !s.w.readable(domain.Principal{ID: scope.PrincipalID, TenantID: scope.TenantID}, d) {
				return false
			}
			if scope.DocumentSet != nil {
				set := s.w.sets[scope.DocumentSet.ID]
				if set == nil || set.expired || set.owner != scope.PrincipalID || set.members[d.id] != d.incarnation {
					return false
				}
			}
		case domain.GrantScopeMembership:
			if incarnation, ok := scope.IncarnationOf(d.id); !ok || incarnation != d.incarnation || d.tenant != scope.TenantID {
				return false
			}
		}
	}
	return (filter.DocumentIDs == nil || slices.Contains(filter.DocumentIDs, d.id)) &&
		(filter.Collection == "" || d.collection == filter.Collection) && (filter.DocType == "" || d.docType == filter.DocType)
}

type grantEmbedder struct {
	domain.EmbeddingProvider
	w       *grantWorld
	mu      sync.Mutex
	queries []string
}

func (e *grantEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	e.w.send("embed")
	e.mu.Lock()
	e.queries = append(e.queries, text)
	e.mu.Unlock()
	return []float32{1}, nil
}

// grantConverser records every prompt sent to the model and can act between frames.
type grantConverser struct {
	llm.FakeConverser
	w       *grantWorld
	mu      sync.Mutex
	prompts []string
	deltas  []string
	// beforeFrame runs before the numbered delta (1-based) is handed to the handler.
	beforeFrame func(n int)
	// afterGenerate runs once a non-streaming answer exists and before it is released.
	afterGenerate func()
	// afterFrames runs after the last delta and before the stream is completed.
	afterFrames func()
}

func (c *grantConverser) record(messages []brtypes.Message) {
	var b strings.Builder
	for _, m := range messages {
		for _, block := range m.Content {
			if text, ok := llm.BlockText(block); ok {
				b.WriteString(text)
			}
		}
	}
	c.mu.Lock()
	c.prompts = append(c.prompts, b.String())
	c.mu.Unlock()
}

func (c *grantConverser) answer(text string) *bedrockruntime.ConverseOutput {
	return deadlineOutput(text, brtypes.StopReasonEndTurn)
}

func (c *grantConverser) ConverseWithModelAndSystem(_ context.Context, _, _ string, messages []brtypes.Message, _ float32, _ int) (*bedrockruntime.ConverseOutput, error) {
	c.w.send("llm")
	c.record(messages)
	if c.afterGenerate != nil {
		c.afterGenerate()
	}
	return c.answer("Answer one [1]. Answer two."), nil
}

func (c *grantConverser) ConverseStreamWithModelAndSystem(_ context.Context, _, _ string, messages []brtypes.Message, _ float32, _ int, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
	c.w.send("llm")
	c.record(messages)
	for i, part := range []string{"Answer one [1]. ", "Answer two. ", "Answer three."} {
		if c.beforeFrame != nil {
			c.beforeFrame(i + 1)
		}
		if err := fn(part); err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.deltas = append(c.deltas, part)
		c.mu.Unlock()
	}
	if c.afterFrames != nil {
		c.afterFrames()
	}
	return c.answer("Answer one [1]. Answer two. Answer three."), nil
}

type grantRig struct {
	w         *grantWorld
	store     *grantStore
	embedder  *grantEmbedder
	converser *grantConverser
	handler   http.HandlerFunc

	mu       sync.Mutex
	reranked [][]string
	onRerank func()
	cfg      *config.Config
}

func newGrantRig(t *testing.T, kind domain.GrantScopeKind, tune func(*config.Config)) *grantRig {
	t.Helper()
	return newGrantRigWith(t, kind, tune, nil, nil)
}

// newGrantRigWith also lets a test change the store before the handler reads
// its capabilities, and wrap the authority the handler is given.
func newGrantRigWith(t *testing.T, kind domain.GrantScopeKind, tune func(*config.Config), prepare func(*grantRig), wrap func(*grantWorld) domain.ReadAuthority) *grantRig {
	t.Helper()
	w := newGrantWorld()
	rig := &grantRig{w: w, store: &grantStore{w: w, kind: kind}, embedder: &grantEmbedder{w: w}, converser: &grantConverser{w: w}}
	cfg := streamQueryConfig()
	cfg.QueryConcurrency, cfg.QueryQueueTimeout = 4, time.Hour
	if tune != nil {
		tune(cfg)
	}
	rig.cfg = cfg
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(rig.converser)
	if prepare != nil {
		prepare(rig)
	}
	ret := retriever.NewRetriever(rig.store, rig.embedder, .7, 10)
	reranker := queryReranker(func(_ context.Context, _ string, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		w.send("rerank")
		ids := make([]string, 0, len(chunks))
		for _, c := range chunks {
			ids = append(ids, string(c.Chunk.DocumentID))
		}
		rig.mu.Lock()
		rig.reranked = append(rig.reranked, ids)
		hook := rig.onRerank
		rig.mu.Unlock()
		if hook != nil {
			hook()
		}
		return chunks, nil
	})
	var authority domain.ReadAuthority = w
	if wrap != nil {
		authority = wrap(w)
	}
	rig.handler = QueryHandler(ret, amb, cfg, nil, WithReranker(reranker), WithAuthorizedRetrieval(authority))
	return rig
}

func principalOf(id string) domain.Principal {
	if id == userB1 {
		return domain.Principal{ID: id, TenantID: tenantB}
	}
	return domain.Principal{ID: id, TenantID: tenantA}
}

func (rig *grantRig) request(ctx context.Context, principal, body, accept string, expires time.Time) (*http.Request, context.CancelFunc) {
	if expires.IsZero() {
		expires = time.Now().Add(time.Hour)
	}
	ctx, cancel := context.WithDeadline(ctx, expires)
	r := streamQueryRequest(ctx, body)
	r.Header.Set("Accept", accept)
	subject := middleware.VerifiedSubject{Principal: principalOf(principal), ExpiresAt: expires, RecheckTrust: func() error {
		rig.w.mu.Lock()
		defer rig.w.mu.Unlock()
		return rig.w.trustErr
	}}
	return r.WithContext(middleware.ContextWithSubject(ctx, subject)), cancel
}

type frameRecorder struct {
	*httptest.ResponseRecorder
	w *grantWorld
}

func (f *frameRecorder) Flush() {
	f.w.record("frame")
	f.ResponseRecorder.Flush()
}

func (rig *grantRig) serve(t *testing.T, principal, body, accept string) *httptest.ResponseRecorder {
	t.Helper()
	return rig.serveWith(t, principal, body, accept, time.Time{})
}

func (rig *grantRig) serveWith(t *testing.T, principal, body, accept string, expires time.Time) *httptest.ResponseRecorder {
	t.Helper()
	req, cancel := rig.request(t.Context(), principal, body, accept, expires)
	defer cancel()
	rec := &frameRecorder{ResponseRecorder: httptest.NewRecorder(), w: rig.w}
	rig.handler.ServeHTTP(rec, req)
	return rec.ResponseRecorder
}

func (rig *grantRig) providerCalls() int {
	log := rig.w.logSnapshot()
	n := 0
	for _, entry := range log {
		if strings.HasPrefix(entry, "send:") {
			n++
		}
	}
	return n
}

// everythingSent is every byte that reached a provider: query embeddings, filters
// and model prompts.
func (rig *grantRig) everythingSent() string {
	rig.embedder.mu.Lock()
	rig.converser.mu.Lock()
	defer rig.embedder.mu.Unlock()
	defer rig.converser.mu.Unlock()
	rig.mu.Lock()
	defer rig.mu.Unlock()
	var b strings.Builder
	for _, q := range rig.embedder.queries {
		b.WriteString(q + "\n")
	}
	for _, p := range rig.converser.prompts {
		b.WriteString(p + "\n")
	}
	for _, ids := range rig.reranked {
		b.WriteString(strings.Join(ids, ",") + "\n")
	}
	return b.String()
}

var errDatabase = errors.New("connection refused")
