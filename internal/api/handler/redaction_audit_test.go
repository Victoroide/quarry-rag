package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/stretchr/testify/require"
)

type auditMock struct {
	mu     sync.Mutex
	err    error
	events []domain.SecurityAuditEvent
}

func (a *auditMock) RecordSecurityAudit(_ context.Context, event domain.SecurityAuditEvent) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.events = append(a.events, event)
	return nil
}

type modeChunkStore struct {
	*healthChunkStore
	mode string
	err  error
}

func (s *modeChunkStore) CorpusAuthzMode(context.Context) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	return s.mode, nil
}

func TestReadinessEnforcesCorpusModeAndTrust(t *testing.T) {
	cs := &modeChunkStore{healthChunkStore: &healthChunkStore{}, mode: "grants"}
	trustErr := error(nil)
	// Each probe lifetime caches the corpus comparison, so every scenario
	// starts from fresh probes.
	check := func() int {
		probes := NewProbes(&healthVectorStore{}, cs, nil, time.Minute)
		probes.SetAuthzChecks("grants", func() error { return trustErr })
		rec := httptest.NewRecorder()
		ReadinessHandler(probes)(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", http.NoBody))
		return rec.Code
	}

	require.Equal(t, http.StatusOK, check())

	trustErr = errors.New("expired jwks")
	require.Equal(t, http.StatusServiceUnavailable, check())
	trustErr = nil

	cs.mode = "service"
	require.Equal(t, http.StatusServiceUnavailable, check())

	cs.mode = "grants"
	cs.err = errors.New("db unavailable")
	require.Equal(t, http.StatusServiceUnavailable, check())
}

type countingModeStore struct {
	*modeChunkStore
	reads atomic.Int64
}

func (s *countingModeStore) CorpusAuthzMode(ctx context.Context) (string, error) {
	s.reads.Add(1)
	return s.modeChunkStore.CorpusAuthzMode(ctx)
}

func TestReadinessCachesCorpusModeForTheProbeLifetime(t *testing.T) {
	for _, mode := range []string{"service", "grants"} {
		t.Run(mode, func(t *testing.T) {
			cs := &countingModeStore{modeChunkStore: &modeChunkStore{healthChunkStore: &healthChunkStore{}, mode: mode}}
			probes := NewProbes(&healthVectorStore{}, cs, nil, time.Minute)
			probes.SetAuthzChecks(mode, func() error { return nil })
			var wg sync.WaitGroup
			for range 50 {
				wg.Go(func() {
					rec := httptest.NewRecorder()
					ReadinessHandler(probes)(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", http.NoBody))
					require.Equal(t, http.StatusOK, rec.Code)
				})
			}
			wg.Wait()
			require.Equal(t, int64(1), cs.reads.Load(), "readiness must not query the corpus mode on every probe")
		})
	}
}

type failingScopedStore struct {
	domain.ChunkStore
}

func (failingScopedStore) ListScopedDocumentsPage(context.Context, domain.Principal, string, int, string, bool) (domain.ScopedDocumentPage, error) {
	return domain.ScopedDocumentPage{Revision: 7, Active: true, Documents: []domain.DocumentSummary{{ID: "00000000-0000-4000-8000-000000000011"}}}, nil
}

func (failingScopedStore) GetScopedDocumentSummary(context.Context, domain.Principal, domain.DocumentID, bool) (domain.ScopedDocumentSummary, error) {
	return domain.ScopedDocumentSummary{}, errors.New("db lookup error")
}

func (failingScopedStore) ScopedPolicyState(context.Context, domain.Principal) (domain.ScopedPolicyState, error) {
	return domain.ScopedPolicyState{Active: true, Revision: 7}, nil
}

func (failingScopedStore) WithScopedDocumentFile(context.Context, domain.Principal, domain.DocumentID, func(context.Context, string) error) (domain.ScopedFileResult, error) {
	return domain.ScopedFileResult{}, errors.New("file lookup error")
}

func (failingScopedStore) BeginScopedDeletion(context.Context, domain.Principal, time.Time, domain.DocumentID, domain.DeleteOperations) (incarnation string, complete bool, err error) {
	return "", false, errors.New("delete failed")
}

func (failingScopedStore) ResumeScopedDeletion(context.Context, domain.DocumentID, string, domain.DeleteOperations) error {
	return nil
}

type failingAuditAuthority struct{}

func (failingAuditAuthority) DeriveReadScope(context.Context, domain.Principal, domain.ReadScopeRequest) (domain.ReadScope, error) {
	return domain.ReadScope{Revision: 3, Empty: true}, nil
}

func (failingAuditAuthority) ReadState(context.Context, domain.Principal) (domain.ReadState, error) {
	return domain.ReadState{Active: true, Revision: 3}, nil
}

func (failingAuditAuthority) Evidence(domain.Principal, string, string) domain.SubjectEvidence {
	e := emptyScopedEvidence{}
	return domain.SubjectEvidence{Visibility: e, Documents: e, Previews: e}
}

type emptyScopedEvidence struct{}

func (emptyScopedEvidence) VisibleEvidence(_ context.Context, c []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	return c, nil
}
func (emptyScopedEvidence) VisibleEvidenceWithPreviews(_ context.Context, c []domain.ScoredChunk, _ []string) ([]domain.ScoredChunk, map[string]string, error) {
	return c, map[string]string{}, nil
}
func (emptyScopedEvidence) VisibleDocumentIDs(context.Context, []string) (map[string]struct{}, error) {
	return map[string]struct{}{}, nil
}
func (emptyScopedEvidence) DocumentPreviews(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

type scopedQueryVectorStore struct {
	domain.VectorStore
	kind domain.GrantScopeKind
}

func (s scopedQueryVectorStore) GrantScope() (domain.GrantScopeKind, bool) { return s.kind, true }

type failingEmbedder struct {
	calls int
}

func (e *failingEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.calls++
	return nil, errors.New("should not be called when audit fails")
}
func (e *failingEmbedder) EmbedBatch(context.Context, []string) ([][]float32, error) {
	e.calls++
	return nil, errors.New("should not be called when audit fails")
}
func (e *failingEmbedder) Dimensions() int { return 3 }

func TestScopedHandlersRedactDocumentIDsFromLogsAndFailClosedOnAudit(t *testing.T) {
	const docID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	logs := captureLeveledLogs(t)
	audit := &auditMock{}
	subj := middleware.VerifiedSubject{
		Principal: domain.Principal{ID: "00000000-0000-4000-8000-000000000001", TenantID: "00000000-0000-4000-8000-000000000002"},
		ExpiresAt: time.Now().Add(time.Minute),
	}
	withSubject := func(r *http.Request) *http.Request {
		ctx := tracing.WithRedactIdentifiers(middleware.ContextWithSubject(r.Context(), subj), true)
		return r.WithContext(ctx)
	}

	// Status lookup error must not log document_id.
	statusH := DocumentStatusHandler(failingScopedStore{}, WithScopedStatus(), WithStatusAudit(audit))
	req := withSubject(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents/"+docID, http.NoBody))
	req.SetPathValue("doc_id", docID)
	rec := httptest.NewRecorder()
	statusH(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	// Delete failure must not log document_id.
	delH := DeleteHandler(failingScopedStore{}, nil, nil, WithScopedDeletion(), WithDeleteAudit(audit))
	reqDel := withSubject(httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+docID, http.NoBody))
	reqDel.SetPathValue("doc_id", docID)
	reqDel.Header.Set("X-Document-Incarnation", "11111111-1111-4111-8111-111111111111")
	recDel := httptest.NewRecorder()
	delH(recDel, reqDel)
	require.Equal(t, http.StatusInternalServerError, recDel.Code)

	require.NotContains(t, logs.String(), docID)

	// Failing audit mutation fails closed on document listing and query before data or provider calls.
	audit.err = errors.New("audit table unavailable")
	listH := ListDocumentsHandler(failingScopedStore{}, WithScopedListing(), WithListingAudit(audit))
	for _, target := range []string{"/documents", "/documents?limit=10"} {
		recList := httptest.NewRecorder()
		listH(recList, withSubject(httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, http.NoBody)))
		require.Equal(t, http.StatusServiceUnavailable, recList.Code)
	}

	emb := &failingEmbedder{}
	ret := retriever.NewRetriever(scopedQueryVectorStore{kind: domain.GrantScopeRelation}, emb, 0.7, 5)
	cfg := &config.Config{FakeMode: true, MaxChunksPerQuery: 5, QueryConcurrency: 1}
	amb := llm.NewAmbassador(cfg)
	queryH := QueryHandler(ret, amb, cfg, nil, WithAuthorizedRetrieval(failingAuditAuthority{}), WithQueryAudit(audit))
	rawBody, err := json.Marshal(struct {
		Query string `json:"query"`
	}{Query: "classified prompt"})
	require.NoError(t, err)
	qReq := withSubject(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", bytes.NewReader(rawBody)))
	qReq.Header.Set("Content-Type", "application/json")
	qRec := httptest.NewRecorder()
	queryH(qRec, qReq)
	require.Equal(t, http.StatusServiceUnavailable, qRec.Code)
	require.Equal(t, 0, emb.calls)
}
