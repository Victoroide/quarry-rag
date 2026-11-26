package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/google/uuid"
)

type benchAuthority struct {
	scopeIDs []domain.ReadableDocument
	calls    atomic.Int64
}

func (a *benchAuthority) DeriveReadScope(_ context.Context, _ domain.Principal, req domain.ReadScopeRequest) (domain.ReadScope, error) {
	a.calls.Add(1)
	docs := a.scopeIDs
	if req.Membership > 0 && len(docs) > req.Membership {
		return domain.ReadScope{Revision: 1, Exceeded: true}, nil
	}
	if req.Membership == 0 && req.DocumentIDs == nil {
		docs = nil
	}
	return domain.ReadScope{Revision: 1, Empty: len(a.scopeIDs) == 0, Documents: docs}, nil
}

func (*benchAuthority) ReadState(context.Context, domain.Principal) (domain.ReadState, error) {
	return domain.ReadState{Active: true, Revision: 1}, nil
}

func (*benchAuthority) Evidence(domain.Principal, string, string) domain.SubjectEvidence {
	e := benchEvidence{}
	return domain.SubjectEvidence{Visibility: e, Documents: e, Previews: e}
}

type benchEvidence struct{}

func (benchEvidence) VisibleEvidence(_ context.Context, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	return chunks, nil
}
func (benchEvidence) VisibleEvidenceWithPreviews(_ context.Context, chunks []domain.ScoredChunk, _ []string) ([]domain.ScoredChunk, map[string]string, error) {
	return chunks, map[string]string{}, nil
}
func (benchEvidence) VisibleDocumentIDs(context.Context, []string) (map[string]struct{}, error) {
	return map[string]struct{}{}, nil
}
func (benchEvidence) DocumentPreviews(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

type benchVectorStore struct {
	kind  domain.GrantScopeKind
	calls atomic.Int64
}

func (v *benchVectorStore) GrantScope() domain.GrantScopeKind { return v.kind }
func (*benchVectorStore) Ping(context.Context) error          { return nil }
func (*benchVectorStore) Upsert(context.Context, string, []domain.Chunk) error {
	return nil
}
func (*benchVectorStore) Query(context.Context, string, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}
func (*benchVectorStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}

func (v *benchVectorStore) QueryFiltered(_ context.Context, _ []float32, _ int, _ domain.VectorFilter) ([]domain.ScoredChunk, error) {
	v.calls.Add(1)
	return nil, nil
}

type countingBenchEmbedder struct {
	base  *embedder.FakeEmbedder
	calls atomic.Int64
}

func (e *countingBenchEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.calls.Add(1)
	return e.base.Embed(ctx, text)
}

func (e *countingBenchEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.calls.Add(int64(len(texts)))
	out := make([][]float32, len(texts))
	for i, text := range texts {
		vec, err := e.base.Embed(ctx, text)
		if err != nil {
			return nil, err
		}
		out[i] = vec
	}
	return out, nil
}

const benchMaxAllowedDocumentIDs = 25000

func BenchmarkQueryHandlerAuthzModes(b *testing.B) {
	docPool := make([]domain.ReadableDocument, benchMaxAllowedDocumentIDs)
	idPool := make([]string, benchMaxAllowedDocumentIDs)
	for i := range benchMaxAllowedDocumentIDs {
		id := uuid.NewString()
		idPool[i] = id
		docPool[i] = domain.ReadableDocument{ID: id, Incarnation: id}
	}
	principal := domain.Principal{ID: "00000000-0000-4000-8000-000000000001", TenantID: "00000000-0000-4000-8000-000000000002"}

	for _, size := range []int{0, 1, 10000, 25000} {
		for _, tc := range []struct {
			name   string
			grants bool
			kind   domain.GrantScopeKind
			accept string
		}{
			{"service_json", false, domain.GrantScopeRelation, "application/json"},
			{"grants_relation_json", true, domain.GrantScopeRelation, "application/json"},
			{"grants_membership_json", true, domain.GrantScopeMembership, "application/json"},
			{"service_sse", false, domain.GrantScopeRelation, "text/event-stream"},
			{"grants_relation_sse", true, domain.GrantScopeRelation, "text/event-stream"},
		} {
			b.Run(fmt.Sprintf("scope=%d/%s", size, tc.name), func(b *testing.B) {
				emb := &countingBenchEmbedder{base: embedder.NewFakeEmbedder(8)}
				vs := &benchVectorStore{kind: tc.kind}
				auth := &benchAuthority{scopeIDs: docPool[:size]}
				ret := retriever.NewRetriever(vs, emb, 0, 10)
				cfg := &config.Config{MaxChunksPerQuery: 10, QueryConcurrency: 4, QueryAnswerPolicy: "evidence_only", MaxAllowedDocumentIDs: benchMaxAllowedDocumentIDs}
				var opts []handler.QueryOption
				if tc.grants {
					opts = append(opts, handler.WithAuthorizedRetrieval(auth))
				}
				h := handler.QueryHandler(ret, nil, cfg, nil, opts...)

				reqMap := map[string]any{"query": "benchmark query", "mode": "global", "answer_mode": "evidence"}
				if !tc.grants {
					reqMap["allowed_document_ids"] = slices.Clone(idPool[:size])
				}
				body, err := json.Marshal(reqMap)
				if err != nil {
					b.Fatal(err)
				}

				b.ReportAllocs()
				expiresAt := time.Now().Add(time.Hour)
				b.ResetTimer()
				for range b.N {
					req := httptest.NewRequestWithContext(b.Context(), http.MethodPost, "/query", bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Accept", tc.accept)
					if tc.grants {
						req = req.WithContext(middleware.ContextWithSubject(req.Context(), middleware.VerifiedSubject{
							Principal: principal, ExpiresAt: expiresAt,
						}))
					}
					rec := httptest.NewRecorder()
					h(rec, req)
					if rec.Code != http.StatusOK {
						b.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(auth.calls.Load())/float64(b.N), "auth_calls/op")
				b.ReportMetric(float64(vs.calls.Load())/float64(b.N), "vector_calls/op")
				b.ReportMetric(float64(emb.calls.Load())/float64(b.N), "embed_calls/op")
			})
		}
	}
}
