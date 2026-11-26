package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/stretchr/testify/require"
)

const (
	wireTenantA = "00000000-0000-4000-8000-0000000000a0"
	wireTenantB = "00000000-0000-4000-8000-0000000000b0"
	wireUserA   = "00000000-0000-4000-8000-0000000000a1"
	wireUserB   = "00000000-0000-4000-8000-0000000000b1"

	wireDocA      = "00000000-0000-4000-8000-000000000001"
	wireDocHidden = "00000000-0000-4000-8000-000000000002"
	wireDocB      = "00000000-0000-4000-8000-000000000003"
)

type wireDoc struct {
	id, incarnation, tenant, text string
	score                         float64
	readers                       map[string]bool
}

type wireAuthority struct {
	mu   sync.Mutex
	docs []*wireDoc
}

func newWireAuthority() *wireAuthority {
	return &wireAuthority{
		docs: []*wireDoc{
			{id: wireDocA, incarnation: "11111111-1111-4111-8111-111111111111", tenant: wireTenantA, text: "Alpha handbook approved for user A.", score: .85, readers: map[string]bool{wireUserA: true}},
			{id: wireDocHidden, incarnation: "22222222-2222-4222-8222-222222222222", tenant: wireTenantA, text: "Confidential executive memo hidden from user A.", score: .99, readers: map[string]bool{}},
			{id: wireDocB, incarnation: "33333333-3333-4333-8333-333333333333", tenant: wireTenantB, text: "Beta tenant secret isolated from tenant A.", score: .97, readers: map[string]bool{wireUserB: true}},
		},
	}
}

func (a *wireAuthority) DeriveReadScope(_ context.Context, p domain.Principal, req domain.ReadScopeRequest) (domain.ReadScope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var docs []domain.ReadableDocument
	for _, d := range a.docs {
		if d.tenant != p.TenantID || !d.readers[p.ID] {
			continue
		}
		if req.DocumentIDs != nil && !slices.Contains(req.DocumentIDs, d.id) {
			continue
		}
		docs = append(docs, domain.ReadableDocument{ID: d.id, Incarnation: d.incarnation})
	}
	return domain.ReadScope{Revision: 1, Empty: len(docs) == 0, Documents: docs}, nil
}

func (a *wireAuthority) ReadState(context.Context, domain.Principal) (domain.ReadState, error) {
	return domain.ReadState{Active: true, Revision: 1}, nil
}

func (a *wireAuthority) Evidence(p domain.Principal, _, _ string) domain.SubjectEvidence {
	e := &wireEvidence{a: a, p: p}
	return domain.SubjectEvidence{Visibility: e, Documents: e, Previews: e}
}

type wireEvidence struct {
	a *wireAuthority
	p domain.Principal
}

func (e *wireEvidence) VisibleEvidence(ctx context.Context, c []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	visible, _, err := e.VisibleEvidenceWithPreviews(ctx, c, nil)
	return visible, err
}

func (e *wireEvidence) VisibleEvidenceWithPreviews(_ context.Context, candidates []domain.ScoredChunk, _ []string) ([]domain.ScoredChunk, map[string]string, error) {
	e.a.mu.Lock()
	defer e.a.mu.Unlock()
	var visible []domain.ScoredChunk
	for _, c := range candidates {
		for _, d := range e.a.docs {
			if d.id == string(c.Chunk.DocumentID) && d.tenant == e.p.TenantID && d.readers[e.p.ID] && c.Chunk.Text == d.text {
				c.ObservedIncarnation = d.incarnation
				visible = append(visible, c)
				break
			}
		}
	}
	return visible, map[string]string{}, nil
}

func (e *wireEvidence) VisibleDocumentIDs(_ context.Context, ids []string) (map[string]struct{}, error) {
	e.a.mu.Lock()
	defer e.a.mu.Unlock()
	out := map[string]struct{}{}
	for _, id := range ids {
		for _, d := range e.a.docs {
			if d.id == id && d.tenant == e.p.TenantID && d.readers[e.p.ID] {
				out[id] = struct{}{}
			}
		}
	}
	return out, nil
}

func (e *wireEvidence) DocumentPreviews(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

type wireVectorStore struct {
	a       *wireAuthority
	kind    domain.GrantScopeKind
	mu      sync.Mutex
	filters []domain.VectorFilter
}

func (s *wireVectorStore) GrantScope() domain.GrantScopeKind { return s.kind }
func (*wireVectorStore) Upsert(context.Context, string, []domain.Chunk) error {
	return nil
}
func (*wireVectorStore) Query(context.Context, string, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}
func (*wireVectorStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}
func (*wireVectorStore) Ping(context.Context) error                                { return nil }
func (*wireVectorStore) DeleteByDocID(context.Context, string) error               { return nil }
func (*wireVectorStore) DeleteAllVectorsInNamespace(context.Context, string) error { return nil }

func (s *wireVectorStore) QueryFiltered(_ context.Context, _ []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.mu.Lock()
	s.filters = append(s.filters, filter)
	s.mu.Unlock()
	if filter.Authorized == nil {
		return nil, domain.ErrGrantScopeUnsupported
	}
	s.a.mu.Lock()
	defer s.a.mu.Unlock()
	var hits []domain.ScoredChunk
	for _, d := range s.a.docs {
		if d.tenant != filter.Authorized.TenantID {
			continue
		}
		switch s.kind {
		case domain.GrantScopeRelation:
			if !d.readers[filter.Authorized.PrincipalID] {
				continue
			}
		case domain.GrantScopeMembership:
			inc, ok := filter.Authorized.IncarnationOf(d.id)
			if !ok || inc != d.incarnation || (filter.DocumentIDs != nil && !slices.Contains(filter.DocumentIDs, d.id)) {
				continue
			}
		}
		md := map[string]any{"doc_type": "guide"}
		if s.kind == domain.GrantScopeMembership {
			md[domain.VectorTenantKey] = d.tenant
			md[domain.VectorRevisionKey] = d.incarnation
		}
		hits = append(hits, domain.ScoredChunk{
			Chunk:               domain.Chunk{ID: domain.ChunkID(d.id + "-c1"), DocumentID: domain.DocumentID(d.id), Text: d.text, Metadata: md},
			Score:               d.score,
			ObservedIncarnation: d.incarnation,
		})
	}
	if len(hits) > topK {
		hits = hits[:topK]
	}
	return hits, nil
}

type wirePostgresStore struct{ *wireVectorStore }

func (*wirePostgresStore) TransactionalVectors() {}
func (*wirePostgresStore) ObservesVectorIncarnation() bool {
	return true
}
func (s *wirePostgresStore) QueryFilteredWithExactness(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	chunks, err := s.QueryFiltered(ctx, embedding, topK, filter)
	return chunks, true, err
}

type wirePineconeStore struct{ *wireVectorStore }

func (*wirePineconeStore) VisibilityRefillLimit() int  { return 10 }
func (*wirePineconeStore) ManagesVectorDispatch() bool { return true }
func (*wirePineconeStore) DeleteByRevision(context.Context, string, string) error {
	return nil
}
func (*wirePineconeStore) DeleteByAttempt(context.Context, string, string, string) error {
	return nil
}
func (*wirePineconeStore) UpsertWithVisibility(context.Context, string, []domain.Chunk) (domain.VisibilityProbe, error) {
	return nil, nil
}
func (s *wirePineconeStore) QueryCandidates(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.QueryFiltered(ctx, embedding, topK, filter)
}
func (s *wirePineconeStore) QueryFilteredBudgeted(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, _ int) ([]domain.ScoredChunk, int, error) {
	chunks, err := s.QueryFiltered(ctx, embedding, topK, filter)
	return chunks, 1, err
}

func TestServerAssemblyPreservesConcreteStoreCapabilities(t *testing.T) {
	emb := embedder.NewFakeEmbedder(4)
	for _, tc := range []struct {
		name        string
		cfg         *config.Config
		store       domain.VectorStore
		wantKind    domain.GrantScopeKind
		wantCapable bool
	}{
		{"postgres", &config.Config{VectorStore: "postgres", LocalRetrievalThreshold: 0.7, MaxChunksPerQuery: 10}, &pgstore.VectorStore{}, domain.GrantScopeRelation, true},
		{"pinecone", &config.Config{VectorStore: "pinecone", LocalRetrievalThreshold: 0.7, MaxChunksPerQuery: 10}, pcstore.NewDriver(nil), domain.GrantScopeMembership, true},
		{"fake memory", &config.Config{FakeMode: true, LocalRetrievalThreshold: 0.7, MaxChunksPerQuery: 10}, memory.NewFakeVectorStore(), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assembled, err := assembleQueryComponents(tc.cfg, tc.store, emb)
			require.NoError(t, err)
			kind, capable := assembled.retriever.GrantScope()
			require.Equal(t, tc.wantCapable, capable)
			require.Equal(t, tc.wantKind, kind)
		})
	}
}

func TestServerAssemblyServesAuthorizedQueryEndToEnd(t *testing.T) {
	for _, tc := range []struct {
		name        string
		vectorStore string
		kind        domain.GrantScopeKind
		makeStore   func(*wireVectorStore) domain.VectorStore
		wantStatus  int
		fakeMode    bool
	}{
		{
			name:        "postgres assembly",
			vectorStore: "postgres",
			kind:        domain.GrantScopeRelation,
			makeStore:   func(base *wireVectorStore) domain.VectorStore { return &wirePostgresStore{base} },
			wantStatus:  http.StatusOK,
		},
		{
			name:        "pinecone assembly",
			vectorStore: "pinecone",
			kind:        domain.GrantScopeMembership,
			makeStore:   func(base *wireVectorStore) domain.VectorStore { return &wirePineconeStore{base} },
			wantStatus:  http.StatusOK,
		},
		{
			name:       "fake memory fails closed",
			fakeMode:   true,
			wantStatus: http.StatusServiceUnavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var promptsMu sync.Mutex
			var prompts []string
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/embeddings":
					_, _ = fmt.Fprint(w, `{"data":[{"index":0,"embedding":[1.0,0.0]}]}`)
				case "/chat/completions":
					var payload struct {
						Messages []struct {
							Content string `json:"content"`
						} `json:"messages"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
					promptsMu.Lock()
					for _, m := range payload.Messages {
						prompts = append(prompts, m.Content)
					}
					promptsMu.Unlock()
					_, _ = fmt.Fprint(w, `{"choices":[{"index":0,"message":{"content":"Approved policy answer."},"finish_reason":"stop"}]}`)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer provider.Close()

			cfg := &config.Config{
				VectorStore:             tc.vectorStore,
				FakeMode:                tc.fakeMode,
				LLMProvider:             "openai",
				EmbeddingProvider:       "openai",
				OpenAIBaseURL:           provider.URL,
				OpenAIChatModel:         "chat",
				OpenAIEmbeddingModel:    "embed",
				EmbeddingDimensions:     2,
				EmbedConcurrency:        1,
				EmbedTimeout:            2 * time.Second,
				LocalRetrievalThreshold: 0.7,
				MaxChunksPerQuery:       10,
				QueryConcurrency:        4,
				QueryTimeout:            5 * time.Second,
				SynthesisTimeout:        5 * time.Second,
			}

			authority := newWireAuthority()
			var store domain.VectorStore
			var recorder *wireVectorStore
			if tc.fakeMode {
				store = memory.NewFakeVectorStore()
			} else {
				recorder = &wireVectorStore{a: authority, kind: tc.kind}
				store = tc.makeStore(recorder)
			}

			emb, err := newEmbeddingProvider(t.Context(), cfg)
			require.NoError(t, err)
			amb, err := newSynthesisProvider(t.Context(), cfg)
			require.NoError(t, err)
			assembled, err := assembleQueryComponents(cfg, store, emb)
			require.NoError(t, err)

			h := middleware.Tracing(handler.QueryHandler(assembled.retriever, amb, cfg, nil, handler.WithAuthorizedRetrieval(authority)), false)
			req := httptest.NewRequestWithContext(
				middleware.ContextWithSubject(t.Context(), middleware.VerifiedSubject{
					Principal: domain.Principal{ID: wireUserA, TenantID: wireTenantA},
					ExpiresAt: time.Now().Add(time.Minute),
				}),
				http.MethodPost,
				"/query",
				strings.NewReader(`{"query":"What does the handbook say?","mode":"global"}`),
			)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
			if tc.wantStatus != http.StatusOK {
				promptsMu.Lock()
				require.Empty(t, prompts)
				promptsMu.Unlock()
				return
			}

			var resp handler.QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Equal(t, 1, resp.ChunksUsed)
			require.Equal(t, wireDocA, resp.ScoredChunks[0].DocID)
			require.Contains(t, rec.Body.String(), "Alpha handbook approved for user A.")
			require.NotContains(t, rec.Body.String(), "Confidential executive memo")
			require.NotContains(t, rec.Body.String(), "Beta tenant secret")

			promptsMu.Lock()
			joinedPrompts := strings.Join(prompts, "\n")
			promptsMu.Unlock()
			require.Contains(t, joinedPrompts, "Alpha handbook approved for user A.")
			require.NotContains(t, joinedPrompts, "Confidential executive memo")
			require.NotContains(t, joinedPrompts, "Beta tenant secret")

			recorder.mu.Lock()
			require.NotEmpty(t, recorder.filters)
			for _, f := range recorder.filters {
				require.NotNil(t, f.Authorized)
				require.Equal(t, wireTenantA, f.Authorized.TenantID)
				require.Equal(t, wireUserA, f.Authorized.PrincipalID)
			}
			recorder.mu.Unlock()
		})
	}
}
