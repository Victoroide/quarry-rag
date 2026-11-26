package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
)

const (
	grantsTenantA      = "10000000-0000-4000-8000-0000000000a0"
	grantsTenantB      = "20000000-0000-4000-8000-0000000000b0"
	grantsOwnerA       = "10000000-0000-4000-8000-0000000000a1"
	grantsReaderA      = "10000000-0000-4000-8000-0000000000a2"
	grantsOtherA       = "10000000-0000-4000-8000-0000000000a3"
	grantsOwnerB       = "20000000-0000-4000-8000-0000000000b1"
	grantsQuarantineID = "99999999-0000-4000-8000-000000000099"
)

type grantsStoredDoc struct {
	id             string
	incarnation    string
	tenant         string
	ownerID        string
	collection     string
	content        string
	s3Key          *string
	lastError      *string
	readGrants     map[string]bool
	downloadGrants map[string]bool
	quarantined    bool
}

type grantsMemoryStore struct {
	domain.ChunkStore
	mu          sync.Mutex
	corpusMode  string
	revision    int64
	auditFail   atomic.Bool
	modeReads   atomic.Int64
	auditEvents []domain.SecurityAuditEvent
	principals  map[string]domain.Principal
	docs        map[string]*grantsStoredDoc
	jobs        []*domain.Job
}

func newGrantsMemoryStore() *grantsMemoryStore {
	lastErr := "pq: raw internal error text"
	return &grantsMemoryStore{
		corpusMode: "grants",
		revision:   1,
		principals: make(map[string]domain.Principal),
		docs: map[string]*grantsStoredDoc{
			grantsQuarantineID: {
				id:          grantsQuarantineID,
				incarnation: "99999999-1111-4111-8111-000000000099",
				content:     "Quarantined legacy unassigned content that must stay invisible.",
				lastError:   &lastErr,
				quarantined: true,
			},
		},
	}
}

func (s *grantsMemoryStore) Ping(context.Context) error { return nil }

func (s *grantsMemoryStore) CorpusAuthzMode(context.Context) (string, error) {
	s.modeReads.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.corpusMode, nil
}

func (s *grantsMemoryStore) ResolvePrincipal(_ context.Context, issuer, subject string) (domain.Principal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.principals[issuer+"\x00"+subject]
	return p, ok, nil
}

func (s *grantsMemoryStore) RecordSecurityAudit(_ context.Context, event domain.SecurityAuditEvent) error {
	if s.auditFail.Load() {
		return errors.New("audit unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auditEvents = append(s.auditEvents, event)
	return nil
}

func (s *grantsMemoryStore) AuthorizeOwnedIngest(_ context.Context, p domain.Principal, _ string, supplied domain.DocumentID) (domain.OwnedTarget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if supplied == "" {
		return domain.OwnedTarget{Decision: domain.OwnedNew}, nil
	}
	doc, exists := s.docs[string(supplied)]
	if !exists || doc.quarantined || doc.tenant != p.TenantID || doc.ownerID != p.ID {
		return domain.OwnedTarget{Decision: domain.OwnedNotFound}, nil
	}
	return domain.OwnedTarget{Decision: domain.OwnedExisting, Incarnation: doc.incarnation}, nil
}

func (s *grantsMemoryStore) AcceptDocument(_ context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.corpusMode != "grants" {
		return domain.AcceptResult{Outcome: domain.AcceptUnauthorized}, domain.ErrAuthzModeMismatch
	}
	if req.Owner == nil {
		return domain.AcceptResult{Outcome: domain.AcceptUnauthorized}, domain.ErrUnauthorized
	}
	docID := string(req.Document.ID)
	if existing, ok := s.docs[docID]; ok {
		if existing.tenant != req.Owner.Principal.TenantID || existing.ownerID != req.Owner.Principal.ID {
			return domain.AcceptResult{Outcome: domain.AcceptUnauthorized}, domain.ErrUnauthorized
		}
		return domain.AcceptResult{DocumentID: req.Document.ID, Outcome: domain.AcceptDuplicate, Incarnation: existing.incarnation}, nil
	}
	s.revision++
	s.docs[docID] = &grantsStoredDoc{
		id:             docID,
		incarnation:    req.Owner.Incarnation,
		tenant:         req.Owner.Principal.TenantID,
		ownerID:        req.Owner.Principal.ID,
		collection:     req.Collection,
		content:        req.Document.Content,
		s3Key:          req.Document.S3Key,
		readGrants:     map[string]bool{req.Owner.Principal.ID: true},
		downloadGrants: map[string]bool{req.Owner.Principal.ID: true},
	}
	s.jobs = append(s.jobs, &domain.Job{
		Document:    req.Document,
		Incarnation: req.Owner.Incarnation,
		Tenant:      req.Owner.Principal.TenantID,
		Attempts:    1,
	})
	rev := s.revision
	s.auditEvents = append(s.auditEvents, domain.SecurityAuditEvent{
		TenantID:       req.Owner.Principal.TenantID,
		PrincipalID:    req.Owner.Principal.ID,
		PolicyRevision: &rev,
		Action:         "ingest",
		Outcome:        "allow",
		Reason:         "accepted",
		DocumentID:     docID,
		Incarnation:    req.Owner.Incarnation,
	})
	return domain.AcceptResult{DocumentID: req.Document.ID, Outcome: domain.AcceptAccepted, Incarnation: req.Owner.Incarnation}, nil
}

func (s *grantsMemoryStore) ClaimJob(context.Context, string, time.Duration, int) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.jobs) == 0 {
		return nil, nil
	}
	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	return job, nil
}

func (s *grantsMemoryStore) HeartbeatJob(context.Context, domain.Job, time.Duration) (bool, error) {
	return true, nil
}
func (s *grantsMemoryStore) CompleteJob(context.Context, domain.Job, []domain.Chunk, domain.IndexObservation) (bool, error) {
	return true, nil
}
func (s *grantsMemoryStore) RetryJob(context.Context, domain.Job, time.Duration, string) error {
	return nil
}
func (s *grantsMemoryStore) FailJob(context.Context, domain.Job, string) error { return nil }
func (s *grantsMemoryStore) ReleaseJob(context.Context, domain.Job) error      { return nil }

func (d *grantsStoredDoc) readableBy(p domain.Principal) bool {
	if d.quarantined || d.tenant != p.TenantID {
		return false
	}
	return d.ownerID == p.ID || d.readGrants[p.ID]
}

func (d *grantsStoredDoc) downloadableBy(p domain.Principal) bool {
	if !d.readableBy(p) {
		return false
	}
	return d.ownerID == p.ID || d.downloadGrants[p.ID]
}

func (s *grantsMemoryStore) ListScopedDocumentsPage(_ context.Context, p domain.Principal, after string, limit int, collection string, includeIncarnation bool) (domain.ScopedDocumentPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for id, d := range s.docs {
		if d.readableBy(p) && (collection == "" || d.collection == collection) && (after == "" || id > after) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]domain.DocumentSummary, 0, len(ids))
	for _, id := range ids {
		d := s.docs[id]
		summary := domain.DocumentSummary{ID: d.id, Collection: d.collection, Status: "ready", LastError: d.lastError}
		if includeIncarnation {
			summary.Incarnation = d.incarnation
		}
		out = append(out, summary)
	}
	return domain.ScopedDocumentPage{Active: true, Revision: s.revision, Documents: out}, nil
}

func (s *grantsMemoryStore) GetScopedDocumentSummary(_ context.Context, p domain.Principal, id domain.DocumentID, includeIncarnation bool) (domain.ScopedDocumentSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.docs[string(id)]
	if !ok || !d.readableBy(p) {
		return domain.ScopedDocumentSummary{Active: true, Revision: s.revision}, nil
	}
	summary := domain.DocumentSummary{ID: d.id, Collection: d.collection, Status: "ready", LastError: d.lastError}
	if includeIncarnation {
		summary.Incarnation = d.incarnation
	}
	return domain.ScopedDocumentSummary{Active: true, Revision: s.revision, Document: &summary}, nil
}

func (s *grantsMemoryStore) ScopedPolicyState(context.Context, domain.Principal) (domain.ScopedPolicyState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return domain.ScopedPolicyState{Active: true, Revision: s.revision}, nil
}

func (s *grantsMemoryStore) WithScopedDocumentFile(ctx context.Context, p domain.Principal, id domain.DocumentID, sign func(context.Context, string) error) (domain.ScopedFileResult, error) {
	s.mu.Lock()
	d, ok := s.docs[string(id)]
	if !ok || !d.downloadableBy(p) {
		s.mu.Unlock()
		return domain.ScopedFileResult{Active: true, Found: false}, nil
	}
	if d.s3Key == nil {
		s.mu.Unlock()
		return domain.ScopedFileResult{Active: true, Found: false}, nil
	}
	key := *d.s3Key
	inc := d.incarnation
	rev := s.revision
	s.mu.Unlock()

	if err := sign(ctx, key); err != nil {
		return domain.ScopedFileResult{Active: true, Found: true}, err
	}
	s.mu.Lock()
	s.auditEvents = append(s.auditEvents, domain.SecurityAuditEvent{
		TenantID:       p.TenantID,
		PrincipalID:    p.ID,
		PolicyRevision: &rev,
		Action:         "file_sign",
		Outcome:        "allow",
		Reason:         "authorized",
		DocumentID:     string(id),
		Incarnation:    inc,
	})
	s.mu.Unlock()
	return domain.ScopedFileResult{Active: true, Found: true}, nil
}

func (s *grantsMemoryStore) BeginScopedDeletion(ctx context.Context, p domain.Principal, _ time.Time, id domain.DocumentID, ops domain.DeleteOperations) (incarnation string, complete bool, err error) {
	s.mu.Lock()
	d, ok := s.docs[string(id)]
	if !ok || d.quarantined || d.tenant != p.TenantID || d.ownerID != p.ID {
		s.mu.Unlock()
		return "", false, domain.ErrDocumentNotFound
	}
	if ops.ExpectedIncarnation == "" {
		s.mu.Unlock()
		return "", false, domain.ErrDeletionPreconditionRequired
	}
	if d.incarnation != ops.ExpectedIncarnation {
		s.mu.Unlock()
		return "", false, domain.ErrDeletionPreconditionFailed
	}
	inc := d.incarnation
	rev := s.revision
	s3Key := ""
	if d.s3Key != nil {
		s3Key = *d.s3Key
	}
	delete(s.docs, string(id))
	s.revision++
	s.auditEvents = append(s.auditEvents, domain.SecurityAuditEvent{
		TenantID:       p.TenantID,
		PrincipalID:    p.ID,
		PolicyRevision: &rev,
		Action:         "document_delete",
		Outcome:        "allow",
		Reason:         "authorized",
		DocumentID:     string(id),
		Incarnation:    inc,
	})
	s.mu.Unlock()

	if ops.RevisionVectors != nil {
		if err := ops.RevisionVectors(ctx, string(id), inc); err != nil {
			return inc, false, err
		}
	}
	if s3Key != "" && ops.Source != nil {
		if err := ops.Source(ctx, s3Key); err != nil {
			return inc, false, err
		}
	}
	return inc, true, nil
}

func (s *grantsMemoryStore) ResumeScopedDeletion(context.Context, domain.DocumentID, string, domain.DeleteOperations) error {
	return nil
}

func (s *grantsMemoryStore) DeriveReadScope(_ context.Context, p domain.Principal, req domain.ReadScopeRequest) (domain.ReadScope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var docs []domain.ReadableDocument
	for id, d := range s.docs {
		if !d.readableBy(p) {
			continue
		}
		if req.Collection != "" && d.collection != req.Collection {
			continue
		}
		if req.DocumentIDs != nil && !slices.Contains(req.DocumentIDs, id) {
			continue
		}
		docs = append(docs, domain.ReadableDocument{ID: id, Incarnation: d.incarnation})
	}
	slices.SortFunc(docs, func(a, b domain.ReadableDocument) int { return strings.Compare(a.ID, b.ID) })
	return domain.ReadScope{Revision: s.revision, Empty: len(docs) == 0, Documents: docs}, nil
}

func (s *grantsMemoryStore) ReadState(context.Context, domain.Principal) (domain.ReadState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return domain.ReadState{Active: true, Revision: s.revision}, nil
}

func (s *grantsMemoryStore) Evidence(p domain.Principal, collection, _ string) domain.SubjectEvidence {
	e := grantsEvidence{s: s, p: p, collection: collection}
	return domain.SubjectEvidence{Visibility: e, Documents: e, Previews: e}
}

type grantsEvidence struct {
	s          *grantsMemoryStore
	p          domain.Principal
	collection string
}

func (e grantsEvidence) VisibleEvidence(ctx context.Context, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	visible, _, err := e.VisibleEvidenceWithPreviews(ctx, chunks, nil)
	return visible, err
}

func (e grantsEvidence) VisibleEvidenceWithPreviews(_ context.Context, candidates []domain.ScoredChunk, _ []string) ([]domain.ScoredChunk, map[string]string, error) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	var out []domain.ScoredChunk
	for _, c := range candidates {
		d, ok := e.s.docs[string(c.Chunk.DocumentID)]
		if ok && d.readableBy(e.p) && (e.collection == "" || d.collection == e.collection) {
			c.ObservedIncarnation = d.incarnation
			out = append(out, c)
		}
	}
	return out, map[string]string{}, nil
}

func (e grantsEvidence) VisibleDocumentIDs(_ context.Context, ids []string) (map[string]struct{}, error) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	out := make(map[string]struct{})
	for _, id := range ids {
		if d, ok := e.s.docs[id]; ok && d.readableBy(e.p) && (e.collection == "" || d.collection == e.collection) {
			out[id] = struct{}{}
		}
	}
	return out, nil
}

func (e grantsEvidence) DocumentPreviews(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

type grantsVectorStore struct {
	mu      sync.Mutex
	store   *grantsMemoryStore
	vectors map[string][]domain.Chunk
}

func newGrantsVectorStore(store *grantsMemoryStore) *grantsVectorStore {
	return &grantsVectorStore{store: store, vectors: make(map[string][]domain.Chunk)}
}

func (*grantsVectorStore) GrantScope() domain.GrantScopeKind { return domain.GrantScopeRelation }
func (*grantsVectorStore) Ping(context.Context) error        { return nil }

func (v *grantsVectorStore) Upsert(_ context.Context, _ string, chunks []domain.Chunk) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, c := range chunks {
		key := string(c.DocumentID)
		v.vectors[key] = append(v.vectors[key], c)
	}
	return nil
}

func (*grantsVectorStore) Query(context.Context, string, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}
func (*grantsVectorStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}

func (v *grantsVectorStore) QueryFiltered(_ context.Context, _ []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	if filter.Authorized == nil {
		return nil, domain.ErrGrantScopeUnsupported
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	var hits []domain.ScoredChunk
	principal := domain.Principal{ID: filter.Authorized.PrincipalID, TenantID: filter.Authorized.TenantID}
	for docID, chunks := range v.vectors {
		if filter.DocumentIDs != nil && !slices.Contains(filter.DocumentIDs, docID) {
			continue
		}
		d, ok := v.store.docs[docID]
		if !ok || !d.readableBy(principal) {
			continue
		}
		for _, c := range chunks {
			hits = append(hits, domain.ScoredChunk{Chunk: c, Score: 0.95, ObservedIncarnation: d.incarnation})
		}
	}
	if len(hits) > topK {
		hits = hits[:topK]
	}
	return hits, nil
}

func (v *grantsVectorStore) DeleteByDocID(_ context.Context, docID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.vectors, docID)
	return nil
}

func (v *grantsVectorStore) DeleteAllVectorsInNamespace(context.Context, string) error { return nil }

func TestGrantsModeEndToEndIsolationCutoverAuditAndRedaction(t *testing.T) {
	issuer := (&url.URL{Scheme: "https", Host: "issuer.grants.test"}).String()
	const (
		audience      = "quarry-api"
		fixtureHeader = "grants-fixture-auth-val"
		textAlpha     = "CONFIDENTIAL_ALPHA_EXCERPT"
		textBeta      = "CONFIDENTIAL_BETA_EXCERPT"
		promptAlpha   = "SENSITIVE_USER_PROMPT_ALPHA"
		fileNameAlpha = "confidential-alpha-upload.txt"
		subjectAlpha  = "ext-raw-subject-alpha-001"
		subjectAlpha2 = "ext-raw-subject-alpha-reader"
		subjectAlpha3 = "ext-raw-subject-alpha-other"
		subjectBeta   = "ext-raw-subject-beta-001"
	)

	var logBuf bytes.Buffer
	prevLog := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	t.Cleanup(func() { slog.SetDefault(prevLog) })

	prevTracer := otel.GetTracerProvider()
	spanRec := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRec))
	otel.SetTracerProvider(tracerProvider)
	t.Cleanup(func() {
		require.NoError(t, tracerProvider.Shutdown(context.WithoutCancel(t.Context())))
		otel.SetTracerProvider(prevTracer)
	})

	key1, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	key2, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwk1 := jose.JSONWebKey{Key: key1.Public(), KeyID: "k1"}
	jwk2 := jose.JSONWebKey{Key: key2.Public(), KeyID: "k2"}

	jwksPath := filepath.Join(t.TempDir(), "jwks.json")
	writeJWKS := func(exp time.Time, keys ...jose.JSONWebKey) {
		t.Helper()
		raw, mErr := json.Marshal(map[string]any{"expires_at": exp, "keys": keys})
		require.NoError(t, mErr)
		require.NoError(t, os.WriteFile(jwksPath, raw, 0o600))
	}
	writeJWKS(time.Now().Add(time.Hour), jwk1)

	signToken := func(k *rsa.PrivateKey, kid, sub string, ttl time.Duration) client.SubjectCredential {
		t.Helper()
		now := time.Now()
		exp := now.Add(ttl).Truncate(time.Second)
		claims, mErr := json.Marshal(map[string]any{
			"iss": issuer, "aud": audience, "sub": sub,
			"client_id": "mcp-test", "jti": uuid.NewString(), "iat": now.Unix(), "exp": exp.Unix(),
		})
		require.NoError(t, mErr)
		signer, sErr := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: k}, (&jose.SignerOptions{}).WithType("at+jwt").WithHeader("kid", kid))
		require.NoError(t, sErr)
		sig, sErr := signer.Sign(claims)
		require.NoError(t, sErr)
		tok, sErr := sig.CompactSerialize()
		require.NoError(t, sErr)
		return client.SubjectCredential{Token: tok, ExpiresAt: exp}
	}

	s3Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", "\"etag-1\"")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s3Srv.Close)
	t.Setenv("AWS_ENDPOINT_URL_S3", s3Srv.URL)
	s3Client, err := s3store.New(t.Context(), &config.Config{AWSRegion: "us-east-1", AWSAccessKeyID: "test-key", AWSSecretKey: "test-secret", S3Bucket: "grants-bucket"})
	require.NoError(t, err)

	store := newGrantsMemoryStore()
	store.principals[issuer+"\x00"+subjectAlpha] = domain.Principal{ID: grantsOwnerA, TenantID: grantsTenantA}
	store.principals[issuer+"\x00"+subjectAlpha2] = domain.Principal{ID: grantsReaderA, TenantID: grantsTenantA}
	store.principals[issuer+"\x00"+subjectAlpha3] = domain.Principal{ID: grantsOtherA, TenantID: grantsTenantA}
	store.principals[issuer+"\x00"+subjectBeta] = domain.Principal{ID: grantsOwnerB, TenantID: grantsTenantB}

	verifier, err := subjectauth.NewVerifierWithObserver(issuer, audience, jwksPath, func(ev subjectauth.TrustEvent) error {
		return store.RecordSecurityAudit(t.Context(), domain.SecurityAuditEvent{Action: ev.Action, Outcome: ev.Outcome, Reason: ev.Reason})
	})
	require.NoError(t, err)
	require.NoError(t, verifier.ValidateTrust())

	vs := newGrantsVectorStore(store)
	emb := embedder.NewFakeEmbedder(8)
	cfg := &config.Config{
		AuthzMode:            config.AuthzModeGrants,
		ServiceTokens:        []string{fixtureHeader},
		IngestContentDedup:   domain.ContentDedupOff,
		MaxUploadBytes:       4 << 20,
		WorkerQueueSize:      16,
		MaxChunksPerQuery:    10,
		QueryConcurrency:     4,
		QueryTimeout:         15 * time.Second,
		HealthCacheTTL:       time.Millisecond,
		VectorStore:          "postgres",
		Tracing:              config.TracingConfig{CaptureContent: false},
		BedrockFallbackModel: "fake-model",
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(tracing.WrapConverser(llm.NewFakeConverser(), "offline", cfg.BedrockFallbackModel))
	ret := retriever.NewRetriever(vs, emb, 0, 10)
	chk := chunker.NewChunker(128, 16)
	stages := []pipeline.Stage{pipeline.NewChunkStage(chk), pipeline.NewEmbedStage(emb, 1), pipeline.NewPersistStage(vs)}
	w := worker.New(store, stages, vs, worker.Options{Workers: 1, PollInterval: 10 * time.Millisecond, RedactTelemetry: true})
	w.Start(t.Context())
	t.Cleanup(w.Shutdown)

	metricsSvc := metrics.NewService(nil)
	router := NewRouter(&RouterDeps{
		VectorStore:     vs,
		ChunkStore:      store,
		Retriever:       ret,
		Ambassador:      amb,
		Config:          cfg,
		Worker:          w,
		S3Client:        s3Client,
		PineconeDriver:  vs,
		Metrics:         metricsSvc,
		SubjectVerifier: verifier,
		SubjectResolver: store,
		ReadAuthority:   store,
		AuditRecorder:   store,
	})
	apiSrv := httptest.NewServer(router)
	t.Cleanup(apiSrv.Close)

	newClientFor := func(k *rsa.PrivateKey, kid, sub string, ttl time.Duration) *client.Client {
		cred := signToken(k, kid, sub, ttl)
		c, cErr := client.New(apiSrv.URL, client.WithServiceToken(fixtureHeader),
			client.WithSubjectCredentialProvider(func(context.Context) (client.SubjectCredential, error) { return cred, nil }))
		require.NoError(t, cErr)
		return c
	}

	probeStatus := func(path string) int {
		t.Helper()
		req, rErr := http.NewRequestWithContext(t.Context(), http.MethodGet, apiSrv.URL+path, http.NoBody)
		require.NoError(t, rErr)
		resp, dErr := apiSrv.Client().Do(req)
		require.NoError(t, dErr)
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	// Verify readiness in grants mode, plus liveness & disabled fixtures.
	require.Equal(t, http.StatusOK, probeStatus("/readyz"))

	// Corpus mode mismatch fails /readyz and data routes with 503 while keeping /livez at 200.
	store.mu.Lock()
	store.corpusMode = "service"
	store.mu.Unlock()
	// Readiness caches the corpus comparison for the probe lifetime.
	time.Sleep(10 * cfg.HealthCacheTTL)
	require.Equal(t, http.StatusServiceUnavailable, probeStatus("/readyz"))
	require.Equal(t, http.StatusOK, probeStatus("/livez"))
	ownerAClient := newClientFor(key1, "k1", subjectAlpha, 2*time.Minute)
	_, err = ownerAClient.ListDocuments(t.Context(), client.ListOptions{})
	require.Error(t, err)

	// Restore corpus mode to grants.
	store.mu.Lock()
	store.corpusMode = "grants"
	store.mu.Unlock()

	// Reverse mismatch: service-mode replica seeing grants corpus fails /readyz & data routes with 503.
	svcRouter := NewRouter(&RouterDeps{Config: &config.Config{ServiceTokens: []string{fixtureHeader}, AuthzMode: config.AuthzModeService}, ChunkStore: store, VectorStore: vs})
	rec := httptest.NewRecorder()
	svcRouter.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", http.NoBody))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	// Owner A ingests a file (multipart) and Owner B ingests text.
	ownerBClient := newClientFor(key1, "k1", subjectBeta, 2*time.Minute)
	ingA, err := ownerAClient.IngestFile(t.Context(), client.FileUpload{Filename: fileNameAlpha, Reader: strings.NewReader(textAlpha)})
	require.NoError(t, err)
	ingB, err := ownerBClient.Ingest(t.Context(), client.IngestRequest{Content: textBeta})
	require.NoError(t, err)

	// Wait for background worker to persist chunks into vector store.
	require.Eventually(t, func() bool {
		vs.mu.Lock()
		defer vs.mu.Unlock()
		return len(vs.vectors[ingA.DocumentID]) > 0 && len(vs.vectors[ingB.DocumentID]) > 0
	}, 3*time.Second, 20*time.Millisecond)

	// Owner A grants read (no download) to Reader A, keeping Other A and Tenant B excluded.
	store.mu.Lock()
	store.docs[ingA.DocumentID].readGrants[grantsReaderA] = true
	store.mu.Unlock()

	// Quarantined doc and cross-tenant doc stay invisible (`not_found` / absent).
	_, err = ownerAClient.GetDocument(t.Context(), grantsQuarantineID)
	require.Error(t, err)
	_, err = ownerBClient.GetDocument(t.Context(), ingA.DocumentID)
	require.Error(t, err)

	pageA, err := ownerAClient.ListDocumentsWithIncarnation(t.Context(), client.ListOptions{Limit: 10})
	require.NoError(t, err)
	require.Len(t, pageA.Documents, 1)
	require.Equal(t, ingA.DocumentID, pageA.Documents[0].ID)

	// Presigned file download URL returns for Owner A with TTL <= credential remaining & <= 900s.
	fileRes, err := ownerAClient.DocumentFileURL(t.Context(), ingA.DocumentID)
	require.NoError(t, err)
	require.Contains(t, fileRes.URL, ingA.DocumentID)
	require.LessOrEqual(t, fileRes.ExpiresIn, 900)

	// Same-tenant Reader A can query and read status, but cannot sign file or delete.
	readerAClient := newClientFor(key1, "k1", subjectAlpha2, 2*time.Minute)
	qReader, err := readerAClient.Query(t.Context(), client.QueryRequest{Query: promptAlpha, Mode: "global", AnswerMode: client.AnswerModeEvidence})
	require.NoError(t, err)
	require.NotEmpty(t, qReader.ScoredChunks)
	for _, sc := range qReader.ScoredChunks {
		require.Equal(t, ingA.DocumentID, sc.DocID)
	}
	_, err = readerAClient.DocumentFileURL(t.Context(), ingA.DocumentID)
	require.Error(t, err)
	_, err = readerAClient.DeleteDocumentIncarnation(t.Context(), ingA.DocumentID, pageA.Documents[0].Incarnation)
	require.Error(t, err)

	// Generated JSON + SSE query work for Tenant A without ever leaking Tenant B.
	qGen, err := ownerAClient.Query(t.Context(), client.QueryRequest{Query: promptAlpha, Mode: "global", AnswerMode: client.AnswerModeGenerated})
	require.NoError(t, err)
	require.NotEmpty(t, qGen.Answer)

	var sseEvents []string
	err = ownerAClient.QueryStream(t.Context(), client.QueryRequest{Query: promptAlpha, Mode: "global", AnswerMode: client.AnswerModeEvidence}, func(ev client.Event) error {
		sseEvents = append(sseEvents, ev.Type)
		require.NotContains(t, string(ev.Data), ingB.DocumentID)
		require.NotContains(t, string(ev.Data), textBeta)
		return nil
	})
	require.NoError(t, err)
	require.Contains(t, sseEvents, "context")

	// Live revocation: remove Reader A grant -> next query yields zero evidence.
	store.mu.Lock()
	delete(store.docs[ingA.DocumentID].readGrants, grantsReaderA)
	store.revision++
	store.mu.Unlock()
	qRevoked, err := readerAClient.Query(t.Context(), client.QueryRequest{Query: promptAlpha, Mode: "global", AnswerMode: client.AnswerModeEvidence})
	require.NoError(t, err)
	require.Empty(t, qRevoked.ScoredChunks)

	// Same-tenant Other A without grant gets empty listing & cannot inspect doc.
	otherAClient := newClientFor(key1, "k1", subjectAlpha3, 2*time.Minute)
	otherList, err := otherAClient.ListDocuments(t.Context(), client.ListOptions{Limit: 10})
	require.NoError(t, err)
	require.Empty(t, otherList.Documents)
	_, err = otherAClient.GetDocument(t.Context(), ingA.DocumentID)
	require.Error(t, err)

	// Disabled/missing principal receives 403 and fails.
	store.mu.Lock()
	delete(store.principals, issuer+"\x00"+subjectAlpha3)
	store.mu.Unlock()
	_, err = otherAClient.ListDocuments(t.Context(), client.ListOptions{Limit: 10})
	require.Error(t, err)

	// Key rotation: rotate JWKS to k2 only -> k1 immediately rejected; k2 accepted; missing JWKS -> 503 readiness + routes.
	writeJWKS(time.Now().Add(time.Hour), jwk2)
	_, err = ownerAClient.ListDocuments(t.Context(), client.ListOptions{Limit: 10})
	require.Error(t, err)
	ownerARotated := newClientFor(key2, "k2", subjectAlpha, 2*time.Minute)
	_, err = ownerARotated.ListDocuments(t.Context(), client.ListOptions{Limit: 10})
	require.NoError(t, err)

	// Owner A deletes own doc with incarnation.
	delRes, err := ownerARotated.DeleteDocumentIncarnation(t.Context(), ingA.DocumentID, pageA.Documents[0].Incarnation)
	require.NoError(t, err)
	require.True(t, delRes.Deleted)

	// Unavailable audit mutation fails closed.
	store.auditFail.Store(true)
	_, err = ownerARotated.ListDocuments(t.Context(), client.ListOptions{Limit: 10})
	require.Error(t, err)
	store.auditFail.Store(false)

	// Unavailable verifier fails readiness and routes closed.
	require.NoError(t, os.Remove(jwksPath))
	require.Equal(t, http.StatusServiceUnavailable, probeStatus("/readyz"))

	// Assert sensitive PII, identifiers, and credentials never entered logs, spans, metrics, or audit records.
	var metricBuf bytes.Buffer
	require.NoError(t, metricsSvc.Registry.WriteText(&metricBuf))
	metricsBody := metricBuf.String()
	require.NotContains(t, metricsBody, ingA.DocumentID)
	require.NotContains(t, metricsBody, promptAlpha)

	logText := logBuf.String()
	for _, sensitive := range []string{textAlpha, textBeta, promptAlpha, fileNameAlpha, subjectAlpha, subjectBeta, fixtureHeader, ingA.DocumentID, ingB.DocumentID, fileRes.URL, url.QueryEscape(fileRes.URL)} {
		require.NotContains(t, logText, sensitive, "log leaked %q", sensitive)
	}
	for _, sp := range spanRec.Ended() {
		for _, attr := range sp.Attributes() {
			val := attr.Value.String()
			for _, sensitive := range []string{textAlpha, textBeta, promptAlpha, fileNameAlpha, subjectAlpha, subjectBeta, fixtureHeader, ingA.DocumentID, ingB.DocumentID} {
				require.NotContains(t, val, sensitive, "span attr %s leaked %q", attr.Key, sensitive)
			}
		}
	}
	store.mu.Lock()
	auditJSON, err := json.Marshal(store.auditEvents)
	store.mu.Unlock()
	require.NoError(t, err)
	for _, sensitive := range []string{textAlpha, textBeta, promptAlpha, fileNameAlpha, subjectAlpha, subjectBeta, fixtureHeader} {
		require.NotContains(t, string(auditJSON), sensitive, "audit leaked %q", sensitive)
	}
	require.Contains(t, string(auditJSON), "trust_loaded")
	require.Contains(t, string(auditJSON), "trust_rotated")
	require.Contains(t, string(auditJSON), "ingest")
	require.Contains(t, string(auditJSON), "file_sign")
	require.Contains(t, string(auditJSON), "document_delete")
	require.Contains(t, string(auditJSON), "query")
	require.Contains(t, string(auditJSON), "document_list")
	require.Contains(t, string(auditJSON), "document_status")
}

type grantsFailureStore struct {
	*grantsMemoryStore
	err         error
	panicOnList bool
}

func (s *grantsFailureStore) ListScopedDocumentsPage(ctx context.Context, _ domain.Principal, _ string, _ int, _ string, _ bool) (domain.ScopedDocumentPage, error) {
	if s.panicOnList {
		panic(s.err)
	}
	db := tracing.NewQueryTracer()
	ctx = db.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 'synthetic-input-excerpt'"})
	db.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: s.err})
	return domain.ScopedDocumentPage{}, s.err
}

func (s *grantsFailureStore) ListDocuments(context.Context) ([]domain.DocumentRef, error) {
	return nil, s.err
}

type grantsFailureVectors struct {
	*grantsVectorStore
	err error
}

func (v grantsFailureVectors) Ping(context.Context) error { return v.err }

type grantsFailureModel struct {
	*llm.FakeConverser
	err error
}

func (m grantsFailureModel) ConverseWithModelAndSystem(context.Context, string, string, []brtypes.Message, float32, int) (*bedrockruntime.ConverseOutput, error) {
	return nil, m.err
}
func (m grantsFailureModel) ConverseStreamWithModelAndSystem(context.Context, string, string, []brtypes.Message, float32, int, func(string) error) (*bedrockruntime.ConverseOutput, error) {
	return nil, m.err
}

func TestGrantsRequestFailureTelemetryRedaction(t *testing.T) {
	const (
		doc       = "f40fe629-f807-4f5c-b47a-781421bcfe76"
		chunk     = "synthetic-chunk-key"
		object    = "synthetic-object-key"
		excerpt   = "synthetic-input-excerpt"
		requestID = "test-correlated-request"
		subject   = "failure-test-subject"
		token     = "grants-test-service-token"
	)
	canaries := []string{doc, chunk, object, excerpt}
	failure := &pgconn.PgError{Code: "22000", Message: strings.Join(canaries[:2], " "), Detail: strings.Join(canaries[2:], " ")}

	var logs bytes.Buffer
	prevLog := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	t.Cleanup(func() { slog.SetDefault(prevLog) })
	prevTracer := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
		otel.SetTracerProvider(prevTracer)
	})

	issuer := (&url.URL{Scheme: "https", Host: "failure-test.invalid"}).String()
	const audience = "quarry-api"
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwks, err := json.Marshal(map[string]any{
		"expires_at": time.Now().Add(time.Hour),
		"keys":       []jose.JSONWebKey{{Key: key.Public(), KeyID: "key"}},
	})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "jwks.json")
	require.NoError(t, os.WriteFile(path, jwks, 0o600))
	verifier, err := subjectauth.NewVerifier(issuer, audience, path)
	require.NoError(t, err)
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithType("at+jwt").WithHeader("kid", "key"))
	require.NoError(t, err)
	now := time.Now()
	claims, err := json.Marshal(map[string]any{
		"iss": issuer, "aud": audience, "sub": subject, "client_id": "test-mcp",
		"jti": uuid.NewString(), "iat": now.Unix(), "exp": now.Add(time.Minute).Unix(),
	})
	require.NoError(t, err)
	signed, err := signer.Sign(claims)
	require.NoError(t, err)
	credential, err := signed.CompactSerialize()
	require.NoError(t, err)

	objectCalls := atomic.Int32{}
	objectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		objectCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "<Error><Code>InvalidRequest</Code><Message>"+strings.Join(canaries, " ")+"</Message></Error>")
	}))
	t.Cleanup(objectServer.Close)
	t.Setenv("AWS_ENDPOINT_URL_S3", objectServer.URL)
	objects, err := s3store.New(t.Context(), &config.Config{AWSRegion: "us-east-1", AWSAccessKeyID: "key", AWSSecretKey: "secret", S3Bucket: "fixture-bucket"})
	require.NoError(t, err)
	store := &grantsFailureStore{grantsMemoryStore: newGrantsMemoryStore(), err: failure}
	store.principals[issuer+"\x00"+subject] = domain.Principal{ID: grantsOwnerA, TenantID: grantsTenantA}
	objectKey := object
	store.docs[doc] = &grantsStoredDoc{id: doc, incarnation: uuid.NewString(), tenant: grantsTenantA, ownerID: grantsOwnerA, s3Key: &objectKey}
	vectors := newGrantsVectorStore(store.grantsMemoryStore)
	require.NoError(t, vectors.Upsert(t.Context(), "", []domain.Chunk{{
		ID: chunk, DocumentID: doc, Text: excerpt, Embedding: make([]float32, 8),
	}}))
	probeVectors := grantsFailureVectors{grantsVectorStore: vectors, err: failure}
	cfg := &config.Config{
		AuthzMode: config.AuthzModeGrants, ServiceTokens: []string{token}, Tracing: config.TracingConfig{CaptureContent: true},
		MaxUploadBytes: 4 << 20, IngestContentDedup: domain.ContentDedupOff, WorkerQueueSize: 16,
		MaxChunksPerQuery: 10, QueryConcurrency: 1, QueryTimeout: 5 * time.Second,
		BedrockFallbackModel: "fixture-model", VectorStore: "postgres",
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(tracing.WrapConverser(grantsFailureModel{FakeConverser: llm.NewFakeConverser(), err: errors.New(strings.Join(canaries, "\n"))}, "fixture", "fixture-model"))
	ret := retriever.NewRetriever(vectors, embedder.NewFakeEmbedder(8), 0, 10)
	serviceMetrics := metrics.NewService(nil)
	deps := &RouterDeps{
		ChunkStore: store, ReadAuthority: store, SubjectResolver: store, AuditRecorder: store, VectorStore: probeVectors,
		PineconeDriver: vectors, S3Client: objects, Retriever: ret, Ambassador: amb, Config: cfg,
		SubjectVerifier: verifier, Metrics: serviceMetrics, Worker: worker.New(store, nil, nil, worker.Options{}),
		Probes: handler.NewProbes(probeVectors, store, nil, time.Minute),
	}
	router := NewRouter(deps)
	send := func(h http.Handler, method, path, accept, contentType string, body io.Reader) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequestWithContext(t.Context(), method, path, body)
		r.Header.Set("Authorization", "Bearer "+credential)
		r.Header.Set("X-Service-Token", token)
		r.Header.Set("X-Request-ID", requestID)
		r.Header.Set("Content-Type", contentType)
		r.Header.Set("Accept", accept)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		require.Equal(t, requestID, w.Header().Get("X-Request-ID"))
		return w
	}
	list := send(router, http.MethodGet, "/documents?limit=10", "", "", nil)
	require.Equal(t, http.StatusInternalServerError, list.Code)
	require.Contains(t, list.Body.String(), "failed to list documents")
	store.panicOnList = true
	panicked := send(router, http.MethodGet, "/documents?limit=10", "", "", nil)
	require.Equal(t, http.StatusInternalServerError, panicked.Code)
	require.Contains(t, panicked.Body.String(), "internal server error")
	store.panicOnList = false

	beforeProbe := len(recorder.Ended())
	probe := send(router, http.MethodGet, "/health", "", "", nil)
	require.Equal(t, http.StatusServiceUnavailable, probe.Code)
	require.Len(t, recorder.Ended(), beforeProbe)
	require.Contains(t, logs.String(), "health probe failed")

	var form bytes.Buffer
	writer := multipart.NewWriter(&form)
	file, err := writer.CreateFormFile("file", "fixture.txt")
	require.NoError(t, err)
	_, err = io.WriteString(file, excerpt)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	upload := send(router, http.MethodPost, "/ingest", "", writer.FormDataContentType(), &form)
	require.Equal(t, http.StatusServiceUnavailable, upload.Code)
	require.Contains(t, upload.Body.String(), "storage")
	require.EqualValues(t, 1, objectCalls.Load())

	// A malformed endpoint fails AWS presigning locally before any request is sent.
	t.Setenv("AWS_ENDPOINT_URL_S3", "%zz/"+object)
	brokenSigner, err := s3store.New(t.Context(), &config.Config{AWSRegion: "us-east-1", AWSAccessKeyID: "key", AWSSecretKey: "secret", S3Bucket: "fixture-bucket"})
	require.NoError(t, err)
	fileDeps := *deps
	fileDeps.S3Client = brokenSigner
	signResponse := send(NewRouter(&fileDeps), http.MethodGet, "/documents/"+doc+"/file", "", "", nil)
	require.Equal(t, http.StatusInternalServerError, signResponse.Code)
	require.Contains(t, signResponse.Body.String(), "failed to generate download URL")
	require.EqualValues(t, 1, objectCalls.Load())

	queryBody := `{"query":"synthetic question","mode":"global","answer_mode":"generated"}`
	jsonQuery := send(router, http.MethodPost, "/query", "application/json", "application/json", strings.NewReader(queryBody))
	require.Equal(t, http.StatusInternalServerError, jsonQuery.Code)
	require.Contains(t, jsonQuery.Body.String(), "query processing failed")
	sseQuery := send(router, http.MethodPost, "/query", "text/event-stream", "application/json", strings.NewReader(queryBody))
	require.Equal(t, http.StatusOK, sseQuery.Code)
	require.Contains(t, sseQuery.Body.String(), "event: error")
	require.NotContains(t, sseQuery.Body.String(), "event: done")
	visibleFrame, terminalFrame, ok := strings.Cut(sseQuery.Body.String(), "event: error")
	require.True(t, ok)
	require.Contains(t, visibleFrame, doc)
	require.Contains(t, visibleFrame, excerpt)
	require.Contains(t, terminalFrame, "query processing failed")

	for _, message := range []string{"list scoped documents page", "panic recovered", "upload source file", "presign scoped document file", "query processing failed"} {
		require.Contains(t, logs.String(), message)
	}
	require.Contains(t, logs.String(), requestID)
	require.Contains(t, logs.String(), "operation failed")
	hasError := make(map[string]bool)
	for _, span := range recorder.Ended() {
		dump := fmt.Sprint(span.Name(), span.Status(), span.Events(), span.Attributes())
		for _, canary := range canaries {
			require.NotContains(t, dump, canary)
		}
		if span.Status().Code == codes.Error {
			hasError[span.Name()] = true
			require.Equal(t, "operation failed", span.Status().Description)
		}
	}
	require.True(t, hasError["db.query"])
	require.True(t, hasError["query.synthesis"])
	for _, canary := range canaries {
		require.NotContains(t, logs.String(), canary)
		require.NotContains(t, terminalFrame, canary)
		for _, response := range []*httptest.ResponseRecorder{list, panicked, probe, upload, signResponse, jsonQuery} {
			require.NotContains(t, response.Body.String(), canary)
		}
	}
	require.NotContains(t, logs.String(), `"stack":`)
	require.NotContains(t, logs.String(), `"panic":`)
	require.Contains(t, store.auditEvents, domain.SecurityAuditEvent{
		RequestID: requestID, TenantID: grantsTenantA, PrincipalID: grantsOwnerA, Action: "document_list", Outcome: "error", Reason: "internal",
	})
	require.Contains(t, store.auditEvents, domain.SecurityAuditEvent{
		RequestID: requestID, TenantID: grantsTenantA, PrincipalID: grantsOwnerA, Action: "file_sign", Outcome: "error", Reason: "presign_failed", DocumentID: doc,
	})
	var metricOutput bytes.Buffer
	require.NoError(t, serviceMetrics.Registry.WriteText(&metricOutput))
	httpSeries := make(map[string]bool)
	for line := range strings.SplitSeq(metricOutput.String(), "\n") {
		if strings.HasPrefix(line, "quarry_http_requests_total{") {
			httpSeries[line] = true
		}
	}
	require.Equal(t, map[string]bool{
		`quarry_http_requests_total{route="GET /documents",code="500"} 2`:               true,
		`quarry_http_requests_total{route="GET /health",code="503"} 1`:                  true,
		`quarry_http_requests_total{route="POST /ingest",code="503"} 1`:                 true,
		`quarry_http_requests_total{route="GET /documents/{doc_id}/file",code="500"} 1`: true,
		`quarry_http_requests_total{route="POST /query",code="500"} 1`:                  true,
		`quarry_http_requests_total{route="POST /query",code="200"} 1`:                  true,
	}, httpSeries)
	require.NotContains(t, metricOutput.String(), requestID)
	for _, canary := range canaries {
		require.NotContains(t, metricOutput.String(), canary)
	}

	otel.SetTracerProvider(noop.NewTracerProvider())
	logs.Reset()
	list = send(router, http.MethodGet, "/documents", "", "", nil)
	require.Equal(t, http.StatusInternalServerError, list.Code)
	require.Contains(t, logs.String(), "operation failed")
	require.NotContains(t, logs.String(), doc)

	store.corpusMode = config.AuthzModeService
	service := NewRouter(&RouterDeps{Config: &config.Config{ServiceTokens: []string{token}}, ChunkStore: store})
	logs.Reset()
	legacy := send(service, http.MethodGet, "/documents", "", "", nil)
	require.GreaterOrEqual(t, legacy.Code, 500)
	require.Contains(t, logs.String(), doc)
}
