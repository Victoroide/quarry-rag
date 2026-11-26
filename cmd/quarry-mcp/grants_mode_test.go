package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	serviceapi "github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	serviceconfig "github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/Victoroide/quarry-rag/internal/worker"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type mcpDocRecord struct {
	id, incarnation, tenant, owner, text string
}

type mcpGrantsBackend struct {
	domain.ChunkStore
	mu         sync.Mutex
	rev        int64
	principals map[string]domain.Principal
	docs       map[string]*mcpDocRecord
	jobs       []*domain.Job
	vectors    map[string][]domain.Chunk
}

func newMCPGrantsBackend() *mcpGrantsBackend {
	return &mcpGrantsBackend{
		rev:        1,
		principals: make(map[string]domain.Principal),
		docs:       make(map[string]*mcpDocRecord),
		vectors:    make(map[string][]domain.Chunk),
	}
}

func (*mcpGrantsBackend) Ping(context.Context) error                      { return nil }
func (*mcpGrantsBackend) CorpusAuthzMode(context.Context) (string, error) { return "grants", nil }
func (*mcpGrantsBackend) RecordSecurityAudit(context.Context, domain.SecurityAuditEvent) error {
	return nil
}

func (b *mcpGrantsBackend) ResolvePrincipal(_ context.Context, issuer, subject string) (domain.Principal, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, ok := b.principals[issuer+"\x00"+subject]
	return p, ok, nil
}

func (b *mcpGrantsBackend) AuthorizeOwnedIngest(_ context.Context, p domain.Principal, _ string, supplied domain.DocumentID) (domain.OwnedTarget, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if supplied == "" {
		return domain.OwnedTarget{Decision: domain.OwnedNew}, nil
	}
	d, ok := b.docs[string(supplied)]
	if !ok || d.tenant != p.TenantID || d.owner != p.ID {
		return domain.OwnedTarget{Decision: domain.OwnedNotFound}, nil
	}
	return domain.OwnedTarget{Decision: domain.OwnedExisting, Incarnation: d.incarnation}, nil
}

func (b *mcpGrantsBackend) AcceptDocument(_ context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if req.Owner == nil {
		return domain.AcceptResult{Outcome: domain.AcceptUnauthorized}, domain.ErrUnauthorized
	}
	id := string(req.Document.ID)
	if existing, ok := b.docs[id]; ok {
		return domain.AcceptResult{DocumentID: req.Document.ID, Outcome: domain.AcceptDuplicate, Incarnation: existing.incarnation}, nil
	}
	b.rev++
	b.docs[id] = &mcpDocRecord{
		id: id, incarnation: req.Owner.Incarnation, tenant: req.Owner.Principal.TenantID, owner: req.Owner.Principal.ID, text: req.Document.Content,
	}
	b.jobs = append(b.jobs, &domain.Job{Document: req.Document, Incarnation: req.Owner.Incarnation, Tenant: req.Owner.Principal.TenantID, Attempts: 1})
	return domain.AcceptResult{DocumentID: req.Document.ID, Outcome: domain.AcceptAccepted, Incarnation: req.Owner.Incarnation}, nil
}

func (b *mcpGrantsBackend) ClaimJob(context.Context, string, time.Duration, int) (*domain.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.jobs) == 0 {
		return nil, nil
	}
	j := b.jobs[0]
	b.jobs = b.jobs[1:]
	return j, nil
}
func (*mcpGrantsBackend) HeartbeatJob(context.Context, domain.Job, time.Duration) (bool, error) {
	return true, nil
}
func (*mcpGrantsBackend) CompleteJob(context.Context, domain.Job, []domain.Chunk, domain.IndexObservation) (bool, error) {
	return true, nil
}
func (*mcpGrantsBackend) RetryJob(context.Context, domain.Job, time.Duration, string) error {
	return nil
}
func (*mcpGrantsBackend) FailJob(context.Context, domain.Job, string) error { return nil }
func (*mcpGrantsBackend) ReleaseJob(context.Context, domain.Job) error      { return nil }

func (b *mcpGrantsBackend) ListScopedDocumentsPage(_ context.Context, p domain.Principal, _ string, limit int, _ string, includeIncarnation bool) (domain.ScopedDocumentPage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var docs []domain.DocumentSummary
	for _, d := range b.docs {
		if d.tenant == p.TenantID && d.owner == p.ID {
			s := domain.DocumentSummary{ID: d.id, Status: "ready"}
			if includeIncarnation {
				s.Incarnation = d.incarnation
			}
			docs = append(docs, s)
		}
	}
	if len(docs) > limit {
		docs = docs[:limit]
	}
	return domain.ScopedDocumentPage{Active: true, Revision: b.rev, Documents: docs}, nil
}

func (b *mcpGrantsBackend) GetScopedDocumentSummary(_ context.Context, p domain.Principal, id domain.DocumentID, includeIncarnation bool) (domain.ScopedDocumentSummary, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.docs[string(id)]
	if !ok || d.tenant != p.TenantID || d.owner != p.ID {
		return domain.ScopedDocumentSummary{Active: true, Revision: b.rev}, nil
	}
	s := domain.DocumentSummary{ID: d.id, Status: "ready"}
	if includeIncarnation {
		s.Incarnation = d.incarnation
	}
	return domain.ScopedDocumentSummary{Active: true, Revision: b.rev, Document: &s}, nil
}

func (b *mcpGrantsBackend) ScopedPolicyState(context.Context, domain.Principal) (domain.ScopedPolicyState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return domain.ScopedPolicyState{Active: true, Revision: b.rev}, nil
}

func (b *mcpGrantsBackend) BeginScopedDeletion(ctx context.Context, p domain.Principal, _ time.Time, id domain.DocumentID, ops domain.DeleteOperations) (incarnation string, complete bool, err error) {
	b.mu.Lock()
	d, ok := b.docs[string(id)]
	if !ok || d.tenant != p.TenantID || d.owner != p.ID {
		b.mu.Unlock()
		return "", false, domain.ErrDocumentNotFound
	}
	if d.incarnation != ops.ExpectedIncarnation {
		b.mu.Unlock()
		return "", false, domain.ErrDeletionPreconditionFailed
	}
	inc := d.incarnation
	delete(b.docs, string(id))
	b.rev++
	b.mu.Unlock()
	if ops.RevisionVectors != nil {
		_ = ops.RevisionVectors(ctx, string(id), inc)
	}
	return inc, true, nil
}
func (*mcpGrantsBackend) ResumeScopedDeletion(context.Context, domain.DocumentID, string, domain.DeleteOperations) error {
	return nil
}

func (b *mcpGrantsBackend) DeriveReadScope(_ context.Context, p domain.Principal, req domain.ReadScopeRequest) (domain.ReadScope, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var docs []domain.ReadableDocument
	for _, d := range b.docs {
		if d.tenant == p.TenantID && d.owner == p.ID && (req.DocumentIDs == nil || slices.Contains(req.DocumentIDs, d.id)) {
			docs = append(docs, domain.ReadableDocument{ID: d.id, Incarnation: d.incarnation})
		}
	}
	return domain.ReadScope{Revision: b.rev, Empty: len(docs) == 0, Documents: docs}, nil
}

func (b *mcpGrantsBackend) ReadState(context.Context, domain.Principal) (domain.ReadState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return domain.ReadState{Active: true, Revision: b.rev}, nil
}

func (b *mcpGrantsBackend) Evidence(p domain.Principal, _, _ string) domain.SubjectEvidence {
	e := mcpGrantsEvidence{b: b, p: p}
	return domain.SubjectEvidence{Visibility: e, Documents: e, Previews: e}
}

type mcpGrantsEvidence struct {
	b *mcpGrantsBackend
	p domain.Principal
}

func (e mcpGrantsEvidence) VisibleEvidence(ctx context.Context, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	v, _, err := e.VisibleEvidenceWithPreviews(ctx, chunks, nil)
	return v, err
}

func (e mcpGrantsEvidence) VisibleEvidenceWithPreviews(_ context.Context, chunks []domain.ScoredChunk, _ []string) ([]domain.ScoredChunk, map[string]string, error) {
	e.b.mu.Lock()
	defer e.b.mu.Unlock()
	var out []domain.ScoredChunk
	for _, c := range chunks {
		if d, ok := e.b.docs[string(c.Chunk.DocumentID)]; ok && d.tenant == e.p.TenantID && d.owner == e.p.ID {
			c.ObservedIncarnation = d.incarnation
			out = append(out, c)
		}
	}
	return out, map[string]string{}, nil
}

func (e mcpGrantsEvidence) VisibleDocumentIDs(_ context.Context, ids []string) (map[string]struct{}, error) {
	e.b.mu.Lock()
	defer e.b.mu.Unlock()
	out := make(map[string]struct{})
	for _, id := range ids {
		if d, ok := e.b.docs[id]; ok && d.tenant == e.p.TenantID && d.owner == e.p.ID {
			out[id] = struct{}{}
		}
	}
	return out, nil
}
func (mcpGrantsEvidence) DocumentPreviews(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (*mcpGrantsBackend) GrantScope() domain.GrantScopeKind { return domain.GrantScopeRelation }
func (b *mcpGrantsBackend) Upsert(_ context.Context, _ string, chunks []domain.Chunk) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range chunks {
		b.vectors[string(c.DocumentID)] = append(b.vectors[string(c.DocumentID)], c)
	}
	return nil
}
func (*mcpGrantsBackend) Query(context.Context, string, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}
func (*mcpGrantsBackend) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}
func (b *mcpGrantsBackend) QueryFiltered(_ context.Context, _ []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	if filter.Authorized == nil {
		return nil, errors.New("missing scope")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var hits []domain.ScoredChunk
	for docID, chunks := range b.vectors {
		d, ok := b.docs[docID]
		if !ok || d.tenant != filter.Authorized.TenantID || d.owner != filter.Authorized.PrincipalID {
			continue
		}
		for _, c := range chunks {
			hits = append(hits, domain.ScoredChunk{Chunk: c, Score: 0.9, ObservedIncarnation: d.incarnation})
		}
	}
	if len(hits) > topK {
		hits = hits[:topK]
	}
	return hits, nil
}
func (b *mcpGrantsBackend) DeleteByDocID(_ context.Context, docID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.vectors, docID)
	return nil
}
func (*mcpGrantsBackend) DeleteAllVectorsInNamespace(context.Context, string) error { return nil }

func TestMCPAllSixToolsWorkInGrantsMode(t *testing.T) {
	issuer := (&url.URL{Scheme: "https", Host: "issuer.mcp.test"}).String()
	const (
		audience         = "quarry-mcp-test"
		fixtureSvcHeader = "mcp-service-token"
		aliceSub         = "mcp-sub-alice"
		bobSub           = "mcp-sub-bob"
		tenantB          = "00000000-0000-4000-8000-0000000000dd"
	)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwk := jose.JSONWebKey{Key: key.Public(), KeyID: "mcp-k1"}
	jwksPath := filepath.Join(t.TempDir(), "jwks.json")
	jwksData, err := json.Marshal(map[string]any{"expires_at": time.Now().Add(time.Hour), "keys": []jose.JSONWebKey{jwk}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(jwksPath, jwksData, 0o600))

	signJWT := func(sub string) string {
		now := time.Now()
		exp := now.Add(3 * time.Minute).Truncate(time.Second)
		claims, mErr := json.Marshal(map[string]any{
			"iss": issuer, "aud": audience, "sub": sub,
			"client_id": "mcp-client", "jti": uuid.NewString(), "iat": now.Unix(), "exp": exp.Unix(),
		})
		require.NoError(t, mErr)
		signer, sErr := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithType("at+jwt").WithHeader("kid", jwk.KeyID))
		require.NoError(t, sErr)
		sig, sErr := signer.Sign(claims)
		require.NoError(t, sErr)
		tok, sErr := sig.CompactSerialize()
		require.NoError(t, sErr)
		return tok
	}

	backend := newMCPGrantsBackend()
	backend.principals[issuer+"\x00"+aliceSub] = domain.Principal{ID: aliceID, TenantID: tenant}
	backend.principals[issuer+"\x00"+bobSub] = domain.Principal{ID: bobID, TenantID: tenantB}

	verifier, err := subjectauth.NewVerifierWithObserver(issuer, audience, jwksPath, func(subjectauth.TrustEvent) error { return nil })
	require.NoError(t, err)
	require.NoError(t, verifier.ValidateTrust())

	emb := embedder.NewFakeEmbedder(8)
	cfg := &serviceconfig.Config{
		AuthzMode:            serviceconfig.AuthzModeGrants,
		ServiceTokens:        []string{fixtureSvcHeader},
		IngestContentDedup:   domain.ContentDedupOff,
		MaxUploadBytes:       4 << 20,
		WorkerQueueSize:      16,
		MaxChunksPerQuery:    10,
		QueryConcurrency:     4,
		QueryTimeout:         15 * time.Second,
		HealthCacheTTL:       time.Millisecond,
		VectorStore:          "postgres",
		BedrockFallbackModel: "fake-model",
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(tracing.WrapConverser(llm.NewFakeConverser(), "offline", cfg.BedrockFallbackModel))
	ret := retriever.NewRetriever(backend, emb, 0, 10)
	chk := chunker.NewChunker(128, 16)
	stages := []pipeline.Stage{pipeline.NewChunkStage(chk), pipeline.NewEmbedStage(emb, 1), pipeline.NewPersistStage(backend)}
	w := worker.New(backend, stages, backend, worker.Options{Workers: 1, PollInterval: 10 * time.Millisecond, RedactTelemetry: true})
	w.Start(t.Context())
	t.Cleanup(w.Shutdown)

	router := serviceapi.NewRouter(&serviceapi.RouterDeps{
		Config:          cfg,
		VectorStore:     backend,
		ChunkStore:      backend,
		Retriever:       ret,
		Ambassador:      amb,
		Worker:          w,
		PineconeDriver:  backend,
		SubjectVerifier: verifier,
		SubjectResolver: backend,
		ReadAuthority:   backend,
		AuditRecorder:   backend,
	})
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	root := t.TempDir()
	uploadFile := filepath.Join(root, "alice-notes.txt")
	require.NoError(t, os.WriteFile(uploadFile, []byte("Alice private file notes."), 0o600))

	connectAs := func(sub string) *mcp.ClientSession {
		tokPath := filepath.Join(t.TempDir(), "subject-token")
		replaceSubjectFile(t, tokPath, signJWT(sub), 0o600)
		return connectServer(t, config{baseURL: srv.URL, token: fixtureSvcHeader, subjectTokenFile: tokPath, fileRoots: []string{root}, timeout: 10 * time.Second})
	}
	alice := connectAs(aliceSub)
	bob := connectAs(bobSub)

	// 1. quarry_ingest_text
	resText, err := alice.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "quarry_ingest_text",
		Arguments: map[string]any{"content": "Alice text handbook content."},
	})
	require.NoError(t, err)
	require.False(t, resText.IsError)
	var ingText struct {
		DocumentID string `json:"document_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(resText.Content[0].(*mcp.TextContent).Text), &ingText))

	// 2. quarry_ingest_file
	resFile, err := alice.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "quarry_ingest_file",
		Arguments: map[string]any{"path": uploadFile},
	})
	require.NoError(t, err)
	require.False(t, resFile.IsError)

	// Wait for chunks.
	require.Eventually(t, func() bool {
		backend.mu.Lock()
		defer backend.mu.Unlock()
		return len(backend.vectors[ingText.DocumentID]) > 0
	}, 3*time.Second, 20*time.Millisecond)

	// 3. quarry_list_documents
	resList, err := alice.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_list_documents", Arguments: map[string]any{"limit": 10}})
	require.NoError(t, err)
	require.False(t, resList.IsError)
	require.Contains(t, resList.Content[0].(*mcp.TextContent).Text, ingText.DocumentID)

	bobList, err := bob.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_list_documents", Arguments: map[string]any{"limit": 10}})
	require.NoError(t, err)
	require.NotContains(t, bobList.Content[0].(*mcp.TextContent).Text, ingText.DocumentID)

	// 4. quarry_get_document
	resGet, err := alice.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_get_document", Arguments: map[string]any{"document_id": ingText.DocumentID}})
	require.NoError(t, err)
	require.False(t, resGet.IsError)
	bobGet, err := bob.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_get_document", Arguments: map[string]any{"document_id": ingText.DocumentID}})
	require.NoError(t, err)
	require.True(t, bobGet.IsError)

	// 5. quarry_query
	marshalResult := func(r *mcp.CallToolResult) string {
		data, mErr := json.Marshal(r)
		require.NoError(t, mErr)
		return string(data)
	}
	resQuery, err := alice.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "quarry_query",
		Arguments: map[string]any{"query": "What is in Alice handbook?", "mode": "global", "answer_mode": "evidence"},
	})
	require.NoError(t, err)
	require.False(t, resQuery.IsError)
	require.Contains(t, marshalResult(resQuery), ingText.DocumentID)

	bobQuery, err := bob.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "quarry_query",
		Arguments: map[string]any{"query": "What is in Alice handbook?", "mode": "global", "answer_mode": "evidence"},
	})
	require.NoError(t, err)
	require.NotContains(t, marshalResult(bobQuery), ingText.DocumentID)

	// 6. quarry_delete_document
	backend.mu.Lock()
	inc := backend.docs[ingText.DocumentID].incarnation
	backend.mu.Unlock()
	bobDel, err := bob.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_delete_document", Arguments: map[string]any{"document_id": ingText.DocumentID, "incarnation": inc}})
	require.NoError(t, err)
	require.True(t, bobDel.IsError)

	resDel, err := alice.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_delete_document", Arguments: map[string]any{"document_id": ingText.DocumentID, "incarnation": inc}})
	require.NoError(t, err)
	require.False(t, resDel.IsError)
	require.Contains(t, resDel.Content[0].(*mcp.TextContent).Text, `"deleted":true`)
}
