// Package grantse2e assembles restricted, offline grants-mode integration fixtures.
package grantse2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/Victoroide/quarry-rag/internal/worker"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
)

// Synthetic values shared by the HTTP and stdio fixtures.
const (
	Audience      = "quarry-grants-test"
	TokenLifetime = 4 * time.Minute
	Collection    = "shared-archive"
	FileType      = "archive-file"
	TextType      = "archive-text"
	Filename      = "private-grants-fixture.txt"
	Prompt        = "Which amber records are in the fixture archive?"
	Bucket        = "grants-fixture-bucket"
	AccessKey     = "grants-fixture-access-key"
	SecretKey     = "grants-fixture-secret-key"
	generation    = "grants_test"
	dimensions    = 8
)

// Backends lists the vector adapters exercised by the fixture.
var Backends = [...]string{"postgres", "pinecone"}

// Actor is a catalog principal and its synthetic signed subject credential.
type Actor struct {
	Tenant, ID, Subject string
	Credential          client.SubjectCredential
}

type deletionActivator func(*pgstore.Client, context.Context) (domain.DeletionActivation, error)

// Fixture wires two replicas to one isolated corpus through separate pools.
type Fixture struct {
	Owner, Reader, Other, Downloader, Revocable, Foreign Actor
	Admin, Operator, Auditor                             *pgstore.Client
	Serving                                              [2]*pgstore.Client
	Servers                                              [2]*httptest.Server
	Metrics                                              [2]*metrics.Service
	OperatorLogin, ServiceToken, LegacyID                string
	workers                                              [2]*worker.Worker
	Model                                                *Model
	Revision                                             int64
	Backend                                              string

	Pinecone         *Pinecone
	indexHost        string
	index            pcstore.IndexConn
	key              *rsa.PrivateKey
	issuer, jwksPath string
}

// New starts the corpus, verifies the service-mode gates, and activates grants.
func New(t *testing.T, backend string, activateDeletion deletionActivator, setup ...func()) *Fixture {
	t.Helper()
	dsn := pgtest.StartVector(t)
	for _, configure := range setup {
		configure()
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	f := &Fixture{
		ServiceToken: uuid.NewString(), Backend: backend,
		issuer:   (&url.URL{Scheme: "https", Host: "issuer.grants.test"}).String(),
		jwksPath: filepath.Join(t.TempDir(), "jwks.json"),
		Model:    &Model{},
	}
	var err error
	f.key, err = rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwks, err := json.Marshal(map[string]any{
		"expires_at": time.Now().Add(time.Hour),
		"keys":       []jose.JSONWebKey{{Key: f.key.Public(), KeyID: "fixture"}},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f.jwksPath, jwks, 0o600))

	tenantA, tenantB := uuid.NewString(), uuid.NewString()
	f.Owner = f.actor(t, tenantA)
	f.Reader = f.actor(t, tenantA)
	f.Other = f.actor(t, tenantA)
	f.Downloader = f.actor(t, tenantA)
	f.Revocable = f.actor(t, tenantA)
	f.Foreign = f.actor(t, tenantB)

	f.Admin = open(ctx, t, dsn, "", "")
	require.NoError(t, f.Admin.RunMigrations(ctx))
	f.Exec(t, "REVOKE CREATE ON SCHEMA public FROM PUBLIC")
	_, err = pgstore.NewVectorStore(ctx, f.Admin, dimensions)
	require.NoError(t, err)
	cfg := fixtureConfig(backend, f.ServiceToken)
	host := f.prepareVectors(ctx, t, cfg)
	password := uuid.NewString()
	roles := make(map[string]string)
	for _, name := range []string{"quarry_service", "quarry_operator", "quarry_auditor"} {
		role := pgtest.Role(t, dsn, name)
		roles[name] = role
		f.Exec(t, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" LOGIN PASSWORD '"+password+"'")
	}
	docsDir := filepath.Join("..", "..", "docs")
	grant := func(guide, marker string) {
		sql := pgtest.PublishedSQL(t, docsDir, guide, marker)
		sql = strings.ReplaceAll(sql, "<generation>", generation)
		f.Exec(t, pgtest.ReplaceRoles(sql, roles))
	}
	grant("grant-catalog.md", "GRANT USAGE ON SCHEMA public")
	grant("grant-catalog.md", "ON public.chunk_vectors TO quarry_service")
	if backend == "postgres" {
		grant("deployment.md", "ON public.chunk_vectors_<generation> TO quarry_service")
	}
	f.OperatorLogin = roles["quarry_operator"]
	f.Operator = open(ctx, t, dsn, f.OperatorLogin, password)
	f.Auditor = open(ctx, t, dsn, roles["quarry_auditor"], password)

	for i := range f.Serving {
		f.Serving[i] = open(ctx, t, dsn, roles["quarry_service"], password)
	}
	serving := f.Serving[0]
	serving.SetAuthzMode(config.AuthzModeService)
	store, target := cfg.VectorTarget()
	require.NoError(t, serving.PinVectorGeneration(ctx, generation, store, target, host, cfg.EmbeddingSettings()))
	f.checkServicePrivileges(ctx, t)
	f.seedLegacy(ctx, t)
	f.provision(t)
	s3 := newObjects(ctx, t)
	if backend == "postgres" {
		f.checkQualifiedServiceDeletion(ctx, t, cfg, s3, activateDeletion)
	} else {
		// The hosted fixture exercises tagged vectors in the default namespace.
		marker, err := activateDeletion(f.Admin, ctx)
		require.NoError(t, err)
		require.False(t, marker.ActivatedAt.IsZero())
	}
	for i := range f.Servers {
		f.Servers[i] = f.startReplica(t, cfg, host, s3, i)
	}

	resp := f.Raw(t, 0, http.MethodGet, "/readyz", "", "", nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.Status, "%s", resp.Body)
	resp = f.Raw(t, 1, http.MethodGet, "/documents", f.ServiceToken, f.Owner.Credential.Token, nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.Status, "%s", resp.Body)
	f.checkAdmissionCutover(t)
	f.checkServiceFence(t, cfg, s3)
	return f
}

func (f *Fixture) checkAdmissionCutover(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	const content = "Owned admission across the corpus cutover."
	body, err := json.Marshal(client.IngestRequest{Content: content, DocType: TextType, Collection: Collection})
	require.NoError(t, err)
	for i := range f.Servers {
		resp := f.Raw(t, i, http.MethodPost, "/ingest", f.ServiceToken, f.Owner.Credential.Token, body)
		RequireDenied(t, resp, http.StatusServiceUnavailable, "unavailable", "authorization mode mismatch")
		var documentInserted, jobInserted bool
		require.NoError(t, f.Admin.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM documents WHERE id <> $1),
			EXISTS (SELECT FROM ingest_jobs)`, f.LegacyID).Scan(&documentInserted, &jobInserted))
		require.False(t, documentInserted, "replica %d inserted a document before cutover", i)
		require.False(t, jobInserted, "replica %d inserted a job before cutover", i)
	}

	activated, err := f.Operator.ActivateGrants(ctx, false)
	require.NoError(t, err)
	require.Equal(t, f.Revision+1, activated.Revision)
	f.Revision = activated.Revision
	var admitted [len(f.Servers)]string
	// Leave workers idle until the committed admissions and pending jobs can be read together.
	for i, replica := range f.Serving {
		require.NoError(t, replica.WritersOpen(ctx))
		resp := f.Raw(t, i, http.MethodGet, "/documents", f.ServiceToken, f.Owner.Credential.Token, nil)
		require.Equal(t, http.StatusOK, resp.Status, "%s", resp.Body)
		f.VerifyReady(t, i)

		resp = f.Raw(t, i, http.MethodPost, "/ingest", f.ServiceToken, f.Owner.Credential.Token, body)
		require.Equal(t, http.StatusAccepted, resp.Status, "%s", resp.Body)
		var accepted client.IngestResponse
		require.NoError(t, json.Unmarshal(resp.Body, &accepted))
		require.Equal(t, "processing", accepted.Status)
		id, err := uuid.Parse(accepted.DocumentID)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, id)
		var tenant, principal, collection, documentStatus, jobState string
		require.NoError(t, f.Admin.Pool.QueryRow(ctx, `SELECT o.tenant_id::text, o.principal_id::text,
			o.collection, d.status, j.state FROM documents d
			JOIN grant_catalog_document_ownership o ON o.document_id=d.id AND o.incarnation=d.incarnation
			JOIN ingest_jobs j ON j.document_id=d.id WHERE d.id=$1`, accepted.DocumentID).
			Scan(&tenant, &principal, &collection, &documentStatus, &jobState))
		require.Equal(t, f.Owner.Tenant, tenant)
		require.Equal(t, f.Owner.ID, principal)
		require.Equal(t, Collection, collection)
		require.Equal(t, "processing", documentStatus)
		require.Equal(t, "pending", jobState)
		admitted[i] = accepted.DocumentID
	}
	for _, w := range f.workers {
		w.Start(t.Context())
	}
	for i, id := range admitted {
		doc := f.Wait(t, f.Owner, i, id, content)
		result, err := f.Client(t, i, f.Owner).DeleteDocumentIncarnation(ctx, doc.ID, doc.Incarnation)
		require.NoError(t, err)
		require.True(t, result.Deleted)
		f.RequireFinalized(t, doc)

		var deletionRevision int64
		require.NoError(t, f.Admin.Pool.QueryRow(ctx, `SELECT revision FROM grant_catalog_policy_audit
			WHERE document_id=$1 AND incarnation=$2`, doc.ID, doc.Incarnation).Scan(&deletionRevision))
		require.Equal(t, activated.Revision+int64(i+1), deletionRevision, "replica %d ownership removal", i)
	}
	// Finalizing each owned deletion advances the revision used by the next grant import.
	var committedRevision int64
	require.NoError(t, f.Admin.Pool.QueryRow(ctx, `SELECT revision FROM grant_catalog_policy_revision
		WHERE singleton`).Scan(&committedRevision))
	require.Equal(t, activated.Revision+int64(len(admitted)), committedRevision)
	f.Revision = committedRevision
}

func fixtureConfig(backend, serviceToken string) *config.Config {
	return &config.Config{
		AuthzMode: config.AuthzModeGrants, ServiceToken: serviceToken, S3Bucket: Bucket,
		VectorStore: backend, VectorGeneration: generation,
		PineconeIndexName: "fixture-index", PineconeTimeout: 5 * time.Second, PineconeMaxAttempts: 1, PineconeUpsertBatchSize: 100,
		EmbeddingProvider: domain.EmbeddingProviderBedrock, BedrockEmbedModel: "fixture-embedding",
		EmbeddingModelRevision: "fixture-v1", EmbeddingDimensions: dimensions,
		ExtractionChunkVersion: "fixture-v1", ChunkTokenSize: 128, ChunkOverlap: 16,
		BedrockFallbackModel: "fixture-synthesis", IngestContentDedup: domain.ContentDedupOff,
		WorkerQueueSize: 16, MaxUploadBytes: 1 << 20, ExtractConcurrency: 2,
		MaxChunksPerQuery: 8, LocalRetrievalThreshold: .1, QueryConcurrency: 4,
		QueryTimeout: 20 * time.Second, SynthesisTimeout: 15 * time.Second,
		QueryAnswerPolicy: config.QueryAnswerPolicyBoth, HealthCacheTTL: time.Millisecond,
		Tracing: config.TracingConfig{CaptureContent: true},
	}
}

func (f *Fixture) prepareVectors(ctx context.Context, t *testing.T, cfg *config.Config) string {
	t.Helper()
	var host string
	switch cfg.VectorStore {
	case "postgres":
		_, err := pgstore.CreateGenerationVectorStore(ctx, f.Admin, dimensions, generation, pgstore.WithExactScopeMax(32))
		require.NoError(t, err)
	case "pinecone":
		f.Pinecone, f.index, host = newPinecone(t)
		f.indexHost = host
	default:
		t.Fatalf("unknown vector backend %q", cfg.VectorStore)
	}
	store, target := cfg.VectorTarget()
	f.activateGeneration(ctx, t, cfg, store, target, host)
	return host
}

func (f *Fixture) activateGeneration(ctx context.Context, t *testing.T, cfg *config.Config, store, target, host string) {
	t.Helper()
	_, err := f.Admin.RegisterGeneration(ctx, pgstore.Generation{
		Name: generation, Store: store, Target: target, Host: host,
		Profile: cfg.EmbeddingSettings(), Origin: pgstore.OriginRecorded,
	})
	require.NoError(t, err)
	require.NoError(t, f.Admin.FreezeWriters(ctx, generation))
	const digest = "empty-fixture-corpus"
	require.NoError(t, f.Admin.RecordVerification(ctx, generation, digest, json.RawMessage(`{"passed":true}`)))
	require.NoError(t, f.Admin.ActivateGeneration(ctx, generation, digest, 0))
	require.NoError(t, f.Admin.ThawWriters(ctx))
}

func open(ctx context.Context, t *testing.T, dsn, role, password string) *pgstore.Client {
	t.Helper()
	conn, err := pgstore.NewClient(ctx, dsn, pgstore.WithQueryTracer(tracing.NewQueryTracer()), func(c *pgxpool.Config) {
		c.MaxConns = 4
		c.ConnConfig.RuntimeParams["search_path"] = "public"
		if role != "" {
			c.ConnConfig.User, c.ConnConfig.Password = role, password
		}
	})
	require.NoError(t, err)
	t.Cleanup(conn.Pool.Close)
	return conn
}

func (f *Fixture) checkServicePrivileges(ctx context.Context, t *testing.T) {
	t.Helper()
	serving := f.Serving[0]
	_, err := serving.ActivateGrants(ctx, false)
	require.ErrorContains(t, err, "42501")
	for _, stmt := range []string{
		"SELECT audit_id FROM grant_catalog_security_audit",
		"INSERT INTO grant_catalog_security_audit (action, outcome, reason) VALUES ('trust_key_load', 'allow', 'direct')",
	} {
		_, err := serving.Pool.Exec(ctx, stmt)
		var denied *pgconn.PgError
		require.ErrorAs(t, err, &denied)
		require.Equal(t, "42501", denied.Code)
	}
}

func (f *Fixture) seedLegacy(ctx context.Context, t *testing.T) {
	t.Helper()
	serving := f.Serving[0]
	f.LegacyID = string(f.acceptLegacy(ctx, t, serving).Document.ID)
}

func (f *Fixture) acceptLegacy(ctx context.Context, t *testing.T, serving *pgstore.Client) *domain.Job {
	t.Helper()
	req := domain.AcceptRequest{
		Document: domain.Document{ID: domain.DocumentID(uuid.NewString()), Content: "Private unassigned legacy excerpt."},
		DocType:  TextType, ContentDedup: domain.ContentDedupOff, MaxPending: 16,
	}
	result, err := serving.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	job, err := serving.ClaimJob(ctx, "legacy-fixture", time.Minute, 1)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, req.Document.ID, job.Document.ID)
	ok, err := serving.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	return job
}

func (f *Fixture) checkQualifiedServiceDeletion(ctx context.Context, t *testing.T, cfg *config.Config, s3 *s3store.Client, activateDeletion deletionActivator) {
	t.Helper()
	serving := f.Serving[0]
	doc := f.acceptLegacy(ctx, t, serving)
	service := *cfg
	service.AuthzMode = config.AuthzModeService
	vector := f.openVector(ctx, t, cfg, serving)
	deps := f.routerDeps(t, &service, serving, vector, s3, 0)
	router := api.NewRouter(deps)
	server := httptest.NewServer(router)
	defer server.Close()
	c, err := client.New(server.URL, client.WithServiceToken(f.ServiceToken), client.WithTimeout(15*time.Second))
	require.NoError(t, err)
	mode, err := c.GetDeletionMode(ctx)
	require.NoError(t, err)
	require.Equal(t, "inactive", mode.DeletionMode)
	id := string(doc.Document.ID)
	_, err = c.DeleteDocumentIncarnation(ctx, id, doc.Incarnation)
	var precondition *client.APIError
	require.ErrorAs(t, err, &precondition)
	require.Equal(t, http.StatusPreconditionFailed, precondition.StatusCode)
	marker, err := activateDeletion(f.Admin, ctx)
	require.NoError(t, err)
	require.False(t, marker.ActivatedAt.IsZero())
	mode, err = c.GetDeletionMode(ctx)
	require.NoError(t, err)
	require.True(t, mode.Active())
	result, err := c.DeleteDocumentIncarnation(ctx, id, doc.Incarnation)
	require.NoError(t, err)
	require.True(t, result.Deleted)
	require.Equal(t, id, result.DocID)
	// Service mode can reuse an ordinary identity. The earlier incarnation
	// must remain fenced even when the document ID is live again.
	replacement, err := c.Ingest(ctx, client.IngestRequest{
		DocumentID: id, Content: "Replacement service document.", DocType: TextType,
	})
	require.NoError(t, err)
	require.Equal(t, id, replacement.DocumentID)
	status, err := c.GetDocumentWithIncarnation(ctx, id)
	require.NoError(t, err)
	require.NotEqual(t, doc.Incarnation, status.Incarnation)
	_, err = c.DeleteDocumentIncarnation(ctx, id, doc.Incarnation)
	require.ErrorAs(t, err, &precondition)
	require.Equal(t, http.StatusPreconditionFailed, precondition.StatusCode)
	result, err = c.DeleteDocumentIncarnation(ctx, id, status.Incarnation)
	require.NoError(t, err)
	require.True(t, result.Deleted)
}

func (f *Fixture) startReplica(t *testing.T, cfg *config.Config, host string, s3 *s3store.Client, replica int) *httptest.Server {
	t.Helper()
	parent := t.Context()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	conn := f.Serving[replica]
	conn.SetAuthzMode(config.AuthzModeGrants)
	store, target := cfg.VectorTarget()
	require.NoError(t, conn.PinVectorGeneration(ctx, generation, store, target, host, cfg.EmbeddingSettings()))
	vector := f.openVector(ctx, t, cfg, conn)
	verifier, err := subjectauth.NewVerifierWithObserver(f.issuer, Audience, f.jwksPath, func(event subjectauth.TrustEvent) error {
		auditCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
		defer cancel()
		return conn.RecordSecurityAudit(auditCtx, domain.SecurityAuditEvent{
			Action: event.Action, Outcome: event.Outcome, Reason: event.Reason,
		})
	})
	require.NoError(t, err)
	require.NoError(t, verifier.ValidateTrust())
	deps := f.routerDeps(t, cfg, conn, vector, s3, replica)
	deps.SubjectVerifier, deps.SubjectResolver = verifier, conn
	deps.ReadAuthority = pgstore.GrantReader{Pool: conn.Pool}
	deps.AuditRecorder = conn
	f.workers[replica] = deps.Worker
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(parent), 15*time.Second)
		defer cancel()
		require.NoError(t, deps.Worker.ShutdownContext(shutdown))
	})
	server := httptest.NewServer(api.NewRouter(deps))
	t.Cleanup(server.Close)
	return server
}

// StopReplicas drains HTTP and workers before a telemetry snapshot.
func (f *Fixture) StopReplicas(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	for i, server := range f.Servers {
		server.Close()
		require.NoError(t, f.workers[i].ShutdownContext(ctx))
	}
}

func (f *Fixture) openVector(ctx context.Context, t *testing.T, cfg *config.Config, conn *pgstore.Client) domain.VectorStore {
	t.Helper()
	if f.index != nil {
		pc := pcstore.NewClientForIndex(f.index, cfg, f.indexHost)
		t.Cleanup(func() { require.NoError(t, pc.Close()) })
		return tracing.WrapVectorStore(pcstore.NewDriver(pc), "pinecone")
	}
	vector, err := pgstore.OpenGenerationVectorStore(ctx, conn, dimensions, generation, pgstore.WithExactScopeMax(32))
	require.NoError(t, err)
	require.NoError(t, vector.EnsureExactScopeIndexes(ctx))
	vector.RouteCompletion()
	return tracing.WrapVectorStore(vector, "postgresql")
}

func (f *Fixture) routerDeps(t *testing.T, cfg *config.Config, conn *pgstore.Client, vector domain.VectorStore, s3 *s3store.Client, replica int) *api.RouterDeps {
	t.Helper()
	emb := tracing.WrapEmbedder(fixtureEmbedder{}, "fixture", cfg.BedrockEmbedModel)
	deleter, ok := vector.(domain.VectorDeleter)
	require.True(t, ok)
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(tracing.WrapConverser(f.Model, "fixture", cfg.BedrockFallbackModel))
	serviceMetrics := metrics.NewService(nil)
	f.Metrics[replica] = serviceMetrics
	w := worker.New(conn, []pipeline.Stage{
		pipeline.NewChunkStage(chunker.NewChunker(cfg.ChunkTokenSize, cfg.ChunkOverlap)),
		pipeline.NewEmbedStage(emb, 1),
		pipeline.NewPersistStage(vector),
	}, deleter, worker.Options{
		Owner: uuid.NewString(), Workers: 1, PollInterval: 50 * time.Millisecond,
		JobTimeout: 15 * time.Second, RedactTelemetry: true, Metrics: serviceMetrics,
	})
	return &api.RouterDeps{
		Config: cfg, ChunkStore: conn, VectorStore: vector, PGPool: conn.Pool, S3Client: s3,
		Retriever:  retriever.NewRetriever(vector, emb, cfg.LocalRetrievalThreshold, cfg.MaxChunksPerQuery),
		Probes:     f.probes(cfg, conn, vector, s3),
		Ambassador: amb, PineconeDriver: deleter, Metrics: serviceMetrics, Worker: w, LegacyDeletionReservations: true,
	}
}

func (f *Fixture) probes(cfg *config.Config, conn *pgstore.Client, vector domain.VectorStore, s3 *s3store.Client) *handler.Probes {
	if f.index != nil {
		// The injected SDK connection exposes the data plane only. Probe it
		// directly because Driver.Ping uses the hosted control plane.
		vector = pineconeProbe{VectorStore: vector, conn: f.index}
	}
	return handler.NewProbes(vector, conn, s3, cfg.HealthCacheTTL)
}

type pineconeProbe struct {
	domain.VectorStore
	conn pcstore.IndexConn
}

func (p pineconeProbe) Ping(ctx context.Context) error {
	one := uint32(1)
	_, err := p.conn.ListVectors(ctx, &pinecone.ListVectorsRequest{Limit: &one})
	return err
}

func (f *Fixture) checkServiceFence(t *testing.T, cfg *config.Config, s3 *s3store.Client) {
	t.Helper()
	// Service-mode gates read the same marker as a grants replica.
	service := *cfg
	service.AuthzMode = config.AuthzModeService
	conn := f.Serving[0]
	vector := f.openVector(t.Context(), t, cfg, conn)
	router := api.NewRouter(&api.RouterDeps{
		Config: &service, ChunkStore: conn, S3Client: s3, VectorStore: vector,
		Probes: f.probes(cfg, conn, vector, s3),
	})
	for _, path := range []string{"/readyz", "/documents"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody)
		req.Header.Set("X-Service-Token", f.ServiceToken)
		out := httptest.NewRecorder()
		router.ServeHTTP(out, req)
		require.Equal(t, http.StatusServiceUnavailable, out.Code, "%s", out.Body.String())
	}
}

func (f *Fixture) actor(t *testing.T, tenant string) Actor {
	t.Helper()
	a := Actor{Tenant: tenant, ID: uuid.NewString(), Subject: "fixture-subject-" + uuid.NewString()}
	a.Credential = f.Sign(t, a, Audience, TokenLifetime)
	return a
}

// Sign creates a token for an actor, optionally with incorrect claims.
func (f *Fixture) Sign(t testing.TB, actor Actor, audience string, ttl time.Duration) client.SubjectCredential {
	t.Helper()
	now := time.Now()
	expiry := now.Add(ttl).Truncate(time.Second)
	iat := now
	if ttl < 0 {
		iat = expiry.Add(-time.Minute)
	}
	raw, err := json.Marshal(map[string]any{
		"iss": f.issuer, "sub": actor.Subject, "aud": audience, "client_id": Audience,
		"iat": iat.Unix(), "exp": expiry.Unix(), "jti": uuid.NewString(),
	})
	require.NoError(t, err)
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: f.key},
		(&jose.SignerOptions{}).WithType("at+jwt").WithHeader("kid", "fixture"))
	require.NoError(t, err)
	signed, err := signer.Sign(raw)
	require.NoError(t, err)
	token, err := signed.CompactSerialize()
	require.NoError(t, err)
	return client.SubjectCredential{Token: token, ExpiresAt: expiry}
}

// Client returns a bounded Go HTTP client for an actor at a replica.
func (f *Fixture) Client(t testing.TB, replica int, actor Actor) *client.Client {
	t.Helper()
	c, err := client.New(f.Servers[replica].URL, client.WithTimeout(25*time.Second), client.WithServiceToken(f.ServiceToken),
		client.WithSubjectCredentialProvider(func(context.Context) (client.SubjectCredential, error) {
			return actor.Credential, nil
		}))
	require.NoError(t, err)
	return c
}

// Response holds a buffered raw response for wire assertions.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// Raw sends a bounded request while allowing independently absent auth headers.
func (f *Fixture) Raw(t *testing.T, replica int, method, path, service, token string, body []byte, opts ...client.RequestOption) Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, f.Servers[replica].URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	if service != "" {
		req.Header.Set("X-Service-Token", service)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	for _, opt := range opts {
		opt(req)
	}
	resp, err := f.Servers[replica].Client().Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	require.NoError(t, err)
	return Response{Status: resp.StatusCode, Header: resp.Header, Body: data}
}

// Exec installs or removes fixture-only schema objects through the owner login.
func (f *Fixture) Exec(t testing.TB, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
	defer cancel()
	_, err := f.Admin.Pool.Exec(ctx, sql, args...)
	require.NoError(t, err)
}

func newObjects(ctx context.Context, t *testing.T) *s3store.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead, http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case http.MethodPut:
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				http.Error(w, "fixture body unreadable", http.StatusBadRequest)
				return
			}
			w.Header().Set("ETag", "\"fixture-etag\"")
		default:
			http.Error(w, "fixture method unsupported", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("AWS_ENDPOINT_URL_S3", server.URL)
	storage, err := s3store.New(ctx, &config.Config{
		AWSRegion: "us-east-1", AWSAccessKeyID: AccessKey, AWSSecretKey: SecretKey, S3Bucket: Bucket,
	})
	require.NoError(t, err)
	return storage
}
