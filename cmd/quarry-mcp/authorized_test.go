package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	serviceconfig "github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

const (
	aliceDoc = "00000000-0000-4000-8000-0000000000a1"
	bobDoc   = "00000000-0000-4000-8000-0000000000b1"
	aliceID  = "00000000-0000-4000-8000-0000000000aa"
	bobID    = "00000000-0000-4000-8000-0000000000bb"
	tenant   = "00000000-0000-4000-8000-0000000000cc"
)

// readableAuthority lists, for each principal, the documents it may read.
// Revoking removes a document from the live set between calls.
type readableAuthority struct {
	mu       sync.Mutex
	readable map[string][]string
	texts    map[string]string
}

func (a *readableAuthority) DeriveReadScope(_ context.Context, p domain.Principal, req domain.ReadScopeRequest) (domain.ReadScope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var docs []domain.ReadableDocument
	for _, id := range a.readable[p.ID] {
		if req.DocumentIDs == nil || slices.Contains(req.DocumentIDs, id) {
			docs = append(docs, domain.ReadableDocument{ID: id, Incarnation: "incarnation-" + id})
		}
	}
	return domain.ReadScope{Revision: 1, Empty: len(docs) == 0, Documents: docs}, nil
}

func (a *readableAuthority) ReadState(context.Context, domain.Principal) (domain.ReadState, error) {
	return domain.ReadState{Active: true, Revision: 1}, nil
}

type readableEvidence struct {
	a *readableAuthority
	p domain.Principal
}

func (a *readableAuthority) Evidence(p domain.Principal, _, _ string) domain.SubjectEvidence {
	e := readableEvidence{a: a, p: p}
	return domain.SubjectEvidence{Visibility: e, Documents: e, Previews: e}
}

func (e readableEvidence) VisibleEvidence(ctx context.Context, c []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	visible, _, err := e.VisibleEvidenceWithPreviews(ctx, c, nil)
	return visible, err
}

func (e readableEvidence) VisibleEvidenceWithPreviews(_ context.Context, candidates []domain.ScoredChunk, _ []string) ([]domain.ScoredChunk, map[string]string, error) {
	e.a.mu.Lock()
	defer e.a.mu.Unlock()
	var visible []domain.ScoredChunk
	for _, c := range candidates {
		if slices.Contains(e.a.readable[e.p.ID], string(c.Chunk.DocumentID)) && c.Chunk.Text == e.a.texts[string(c.Chunk.DocumentID)] {
			visible = append(visible, c)
		}
	}
	return visible, map[string]string{}, nil
}

func (e readableEvidence) VisibleDocumentIDs(_ context.Context, ids []string) (map[string]struct{}, error) {
	e.a.mu.Lock()
	defer e.a.mu.Unlock()
	out := map[string]struct{}{}
	for _, id := range ids {
		if slices.Contains(e.a.readable[e.p.ID], id) {
			out[id] = struct{}{}
		}
	}
	return out, nil
}

func (e readableEvidence) DocumentPreviews(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

// membershipStore returns every document whatever the filter says, so only the
// handler's scope and checks stand between a principal and the other's text.
type membershipStore struct {
	domain.VectorStore
	a *readableAuthority
}

func (membershipStore) GrantScope() domain.GrantScopeKind { return domain.GrantScopeRelation }

func (s membershipStore) QueryFiltered(_ context.Context, _ []float32, _ int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	if filter.Authorized == nil {
		panic("an authorized query reached the store without its scope")
	}
	var hits []domain.ScoredChunk
	for id, text := range s.a.texts {
		hits = append(hits, domain.ScoredChunk{Chunk: domain.Chunk{ID: domain.ChunkID(id + "-chunk"), DocumentID: domain.DocumentID(id), Text: text, Metadata: map[string]any{"doc_type": "guide"}}, Score: .9})
	}
	return hits, nil
}

func TestQueryToolReturnsOnlyAuthorizedEvidenceToEachSubject(t *testing.T) {
	const service = "fixture-service"
	authority := &readableAuthority{
		readable: map[string][]string{aliceID: {aliceDoc}, bobID: {bobDoc}},
		texts:    map[string]string{aliceDoc: "Alice policy text.", bobDoc: "Bob secret text."},
	}
	expires := time.Now().Add(3 * time.Minute)
	tokens := map[string]domain.Principal{
		subjectFixture(expires, 1): {ID: aliceID, TenantID: tenant},
		subjectFixture(expires, 2): {ID: bobID, TenantID: tenant},
	}
	cfg := &serviceconfig.Config{MaxChunksPerQuery: 10, QueryTimeout: time.Minute, QueryAnswerPolicy: "evidence_only"}
	ret := retriever.NewRetriever(membershipStore{a: authority}, embedder.NewFakeEmbedder(8), .7, 10)
	query := handler.QueryHandler(ret, nil, cfg, nil, handler.WithAuthorizedRetrieval(authority))
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, known := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !known || r.Header.Get("X-Service-Token") != service {
			http.Error(w, `{"error":"unauthorized","message":"invalid or missing credentials"}`, http.StatusUnauthorized)
			return
		}
		verified := middleware.VerifiedSubject{Principal: principal, ExpiresAt: expires}
		query(w, r.WithContext(middleware.ContextWithSubject(r.Context(), verified)))
	}))
	t.Cleanup(api.Close)

	connect := func(version int) *mcp.ClientSession {
		file := filepath.Join(t.TempDir(), "subject-token")
		replaceSubjectFile(t, file, subjectFixture(expires, version), 0o600)
		return connectServer(t, config{baseURL: api.URL, token: service, subjectTokenFile: file, timeout: 5 * time.Second})
	}
	call := func(cs *mcp.ClientSession, args map[string]any) *mcp.CallToolResult {
		t.Helper()
		args["query"], args["mode"], args["answer_mode"] = "What do the policies say?", "global", "evidence"
		result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: args})
		require.NoError(t, err)
		return result
	}
	textsOf := func(result *mcp.CallToolResult) string {
		t.Helper()
		data, err := json.Marshal(result)
		require.NoError(t, err)
		return string(data)
	}

	alice, bob := connect(1), connect(2)
	aliceResult, bobResult := call(alice, map[string]any{}), call(bob, map[string]any{})
	require.False(t, aliceResult.IsError)
	require.False(t, bobResult.IsError)
	require.Contains(t, textsOf(aliceResult), "Alice policy text.")
	require.NotContains(t, textsOf(aliceResult), "Bob secret text.")
	require.NotContains(t, textsOf(aliceResult), bobDoc)
	require.Contains(t, textsOf(bobResult), "Bob secret text.")
	require.NotContains(t, textsOf(bobResult), "Alice policy text.")

	// A foreign identifier in the tool arguments cannot widen the subject's scope.
	foreign := call(alice, map[string]any{"allowed_document_ids": []string{bobDoc}})
	require.False(t, foreign.IsError)
	require.NotContains(t, textsOf(foreign), "Bob secret text.")
	require.Contains(t, textsOf(foreign), `"finish_reason":"no_evidence"`)

	// A revocation reaches the next call.
	authority.mu.Lock()
	authority.readable[aliceID] = nil
	authority.mu.Unlock()
	revoked := call(alice, map[string]any{})
	require.False(t, revoked.IsError)
	require.NotContains(t, textsOf(revoked), "Alice policy text.")
	require.Contains(t, textsOf(revoked), `"finish_reason":"no_evidence"`)
}
