package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// deletionStores records every call that reaches a vector or object store. A
// hook runs after the call is recorded, so an error models an applied
// operation whose reply was lost.
type deletionStores struct {
	mu    sync.Mutex
	calls []string
	hooks map[string]func(context.Context) error
}

func newDeletionStores() *deletionStores {
	return &deletionStores{hooks: map[string]func(context.Context) error{}}
}

func (s *deletionStores) record(ctx context.Context, kind, argument string) error {
	s.mu.Lock()
	s.calls = append(s.calls, kind+":"+argument)
	hook := s.hooks[kind]
	s.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	return nil
}

func (s *deletionStores) hook(kind string, hook func(context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooks[kind] = hook
}

func (s *deletionStores) count(kind string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, call := range s.calls {
		if strings.HasPrefix(call, kind+":") {
			n++
		}
	}
	return n
}

func (s *deletionStores) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *deletionStores) sources() []string {
	var sources []string
	for _, call := range s.all() {
		if strings.HasPrefix(call, "source:") {
			sources = append(sources, call)
		}
	}
	return sources
}

func (s *deletionStores) DeleteAllVectorsInNamespace(ctx context.Context, namespace string) error {
	return s.record(ctx, "namespace", namespace)
}

func (s *deletionStores) DeleteByDocID(ctx context.Context, id string) error {
	return s.record(ctx, "vectors", id)
}

func (s *deletionStores) DeleteByRevision(ctx context.Context, id, revision string) error {
	return s.record(ctx, "revision", id+"/"+revision)
}

func (s *deletionStores) Delete(ctx context.Context, key string) error {
	return s.record(ctx, "source", key)
}

func (s *deletionStores) PresignGet(ctx context.Context, key string) (string, error) {
	return "signed:" + key, s.record(ctx, "sign", key)
}

func failOnce(message string) func(context.Context) error {
	var calls atomic.Int32
	return func(context.Context) error {
		if calls.Add(1) == 1 {
			return errors.New(message)
		}
		return nil
	}
}

func activateDeletion(t *testing.T, c *pgstore.Client) {
	t.Helper()
	_, err := c.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
}

func otherDatabaseClient(t *testing.T, c *pgstore.Client, opts ...pgstore.Option) *pgstore.Client {
	t.Helper()
	other, err := pgstore.NewClient(t.Context(), c.Pool.Config().ConnString(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, other.Close(context.Background())) })
	return other
}

func storedIncarnation(t *testing.T, c *pgstore.Client, id string) string {
	t.Helper()
	var incarnation string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, id).Scan(&incarnation))
	return incarnation
}

// acceptDeletable admits a caller-supplied document and returns its
// incarnation. A scoped document records the tagged vector format, which the
// active marker permits.
func acceptDeletable(t *testing.T, c *pgstore.Client, id, content string, scoped bool, key string) string {
	t.Helper()
	req := domain.AcceptRequest{
		Document: domain.Document{ID: domain.DocumentID(id), Content: content}, CallerSupplied: true,
		Fingerprint: chunker.ScopedFingerprint("", content), MaxPending: 100, SupportsScopedVectors: scoped,
	}
	if key != "" {
		req.Document.S3Key = &key
	}
	result, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	var stored bool
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT scoped_vectors FROM documents WHERE id=$1`, id).Scan(&stored))
	require.Equal(t, scoped, stored, "the tagged format is recorded only once the database is activated; use markTagged before activation")
	return storedIncarnation(t, c, id)
}

// markTagged records the tagged vector format on a live document of a database
// that has not been activated, where admission never selects it.
func markTagged(t *testing.T, c *pgstore.Client, id string) {
	t.Helper()
	tag, err := c.Pool.Exec(t.Context(), `UPDATE documents SET scoped_vectors=TRUE WHERE id=$1`, id)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

type deletionReceiptRow struct {
	step      int
	finalized bool
	held      bool
	source    *string
	claim     *string
}

func receiptOf(t *testing.T, c *pgstore.Client, id string) deletionReceiptRow {
	t.Helper()
	var r deletionReceiptRow
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT step,finalized,held,source_key,claim::text FROM document_deletions WHERE document_id=$1`, id).
		Scan(&r.step, &r.finalized, &r.held, &r.source, &r.claim))
	return r
}

func receiptCount(t *testing.T, c *pgstore.Client) int {
	t.Helper()
	var n int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM document_deletions`).Scan(&n))
	return n
}

func documentState(t *testing.T, c *pgstore.Client, id string) string {
	t.Helper()
	var state string
	if err := c.Pool.QueryRow(t.Context(), `SELECT status FROM documents WHERE id=$1`, id).Scan(&state); err != nil {
		return ""
	}
	return state
}

// deletionRoutes names the stores behind the document routes; a nil member
// models an unconfigured store.
type deletionRoutes struct {
	vectors domain.VectorDeleter
	objects cleanup.ObjectDeleter
	signer  handler.FileSigner
	options []handler.DeleteOption
}

func fullRoutes(stores *deletionStores) deletionRoutes {
	return deletionRoutes{vectors: stores, objects: stores, signer: stores}
}

func deletionMux(c domain.ChunkStore, token string, routes deletionRoutes) *http.ServeMux {
	tokens := []string{token}
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /documents/{doc_id}", middleware.RequireServiceTokens(tokens, handler.DeleteHandler(c, routes.vectors, routes.objects, routes.options...)))
	mux.HandleFunc("GET /documents/{doc_id}", middleware.RequireServiceTokens(tokens, handler.DocumentStatusHandler(c)))
	mux.HandleFunc("GET /documents/{doc_id}/file", middleware.RequireServiceTokens(tokens, handler.FileHandler(c, routes.signer)))
	mux.HandleFunc("GET /documents", middleware.RequireServiceTokens(tokens, handler.ListDocumentsHandler(c)))
	mux.HandleFunc("POST /ingest", middleware.RequireServiceTokens(tokens, handler.IngestHandler(nil, c, nil, 1<<20)))
	return mux
}

func deletionServer(t *testing.T, c domain.ChunkStore, token string, routes deletionRoutes) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(deletionMux(c, token, routes))
	t.Cleanup(server.Close)
	return server
}

type httpReply struct {
	code       int
	body       string
	retryAfter string
}

func doRequest(t *testing.T, server *httptest.Server, method, path, token string, headers map[string]string, body string) httpReply {
	t.Helper()
	var reader io.Reader = http.NoBody
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, reader)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("X-Service-Token", token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return httpReply{code: resp.StatusCode, body: string(data), retryAfter: resp.Header.Get("Retry-After")}
}

func sendDelete(t *testing.T, server *httptest.Server, token, id, incarnation string) httpReply {
	t.Helper()
	headers := map[string]string{}
	if incarnation != "" {
		headers["X-Document-Incarnation"] = incarnation
	}
	return doRequest(t, server, http.MethodDelete, "/documents/"+id, token, headers, "")
}

func TestHTTPDeleteRetryCannotRemoveReplacement(t *testing.T) {
	c := durableDatabase(t)
	other := otherDatabaseClient(t, c)
	activateDeletion(t, c)
	stores := newDeletionStores()
	stores.hook("revision", failOnce("delete response lost"))
	first := deletionServer(t, c, "token", fullRoutes(stores))
	second := deletionServer(t, other, "token", fullRoutes(stores))
	id := uuid.NewString()
	oldKey, newKey := "source/"+id+"/1", "source/"+id+"/2"
	original := acceptDeletable(t, c, id, "first lifetime", true, oldKey)

	reply := sendDelete(t, first, "token", id, original)
	require.Equal(t, http.StatusInternalServerError, reply.code, reply.body)
	require.NotContains(t, reply.body, "delete response lost")
	require.Equal(t, http.StatusOK, sendDelete(t, second, "token", id, original).code)
	require.Equal(t, http.StatusOK, sendDelete(t, first, "token", id, original).code, "a matching retry acknowledges the completed deletion")

	replacement := acceptDeletable(t, other, id, "second lifetime", true, newKey)
	require.NotEqual(t, original, replacement)
	calls := stores.all()
	require.Equal(t, http.StatusPreconditionFailed, sendDelete(t, first, "token", id, original).code)
	require.Equal(t, http.StatusPreconditionRequired, sendDelete(t, first, "token", id, "").code)
	require.Equal(t, calls, stores.all(), "a stale or unqualified retry must not touch the replacement")
	stored, err := other.GetDocumentS3Key(t.Context(), domain.DocumentID(id))
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, newKey, *stored)
	require.Equal(t, 2, stores.count("revision"))
	require.Equal(t, 1, stores.count("source"))

	status := doRequest(t, second, http.MethodGet, "/documents/"+id+"?include_incarnation=true", "token", nil, "")
	require.Equal(t, http.StatusOK, status.code)
	var summary client.DocumentIncarnationSummary
	require.NoError(t, json.Unmarshal([]byte(status.body), &summary))
	require.Equal(t, replacement, summary.Incarnation)
	require.Equal(t, http.StatusOK, sendDelete(t, second, "token", id, summary.Incarnation).code)
	require.Equal(t, []string{"source:" + oldKey, "source:" + newKey}, stores.sources())
}

func TestDeleteResumesAtEveryExternalCheckpoint(t *testing.T) {
	c := durableDatabase(t)
	retry := otherDatabaseClient(t, c)
	activateDeletion(t, c)
	const token = "token"
	for _, tc := range []struct {
		failing string
		scoped  bool
		step    int
		counts  map[string]int
	}{
		{failing: "namespace", step: 0, counts: map[string]int{"namespace": 2, "vectors": 1, "revision": 0, "source": 1}},
		{failing: "vectors", step: 1, counts: map[string]int{"namespace": 1, "vectors": 2, "revision": 0, "source": 1}},
		{failing: "revision", scoped: true, step: 1, counts: map[string]int{"namespace": 0, "vectors": 0, "revision": 2, "source": 1}},
		{failing: "source", scoped: true, step: 2, counts: map[string]int{"namespace": 0, "vectors": 0, "revision": 1, "source": 2}},
	} {
		t.Run(tc.failing, func(t *testing.T) {
			id, key := uuid.NewString(), "original/"+tc.failing
			content := "document being removed through " + tc.failing
			incarnation := acceptDeletable(t, c, id, content, tc.scoped, key)
			require.NoError(t, c.UpdateDocumentStatus(t.Context(), domain.DocumentID(id), "completed"))
			stores := newDeletionStores()
			stores.hook(tc.failing, failOnce("provider reply lost"))
			first := deletionServer(t, c, token, fullRoutes(stores))
			second := deletionServer(t, retry, token, fullRoutes(stores))
			visibility := pgstore.Visibility{Pool: c.Pool}
			visible, err := visibility.VisibleDocumentIDs(t.Context(), []string{id})
			require.NoError(t, err)
			require.Contains(t, visible, id, "control: a completed document is visible")
			control := doRequest(t, first, http.MethodPost, "/ingest", token, nil, `{"content":"`+content+`"}`)
			require.Equal(t, http.StatusOK, control.code, "control: a live document matches shared content: "+control.body)

			require.Equal(t, http.StatusInternalServerError, sendDelete(t, first, token, id, incarnation).code)
			receipt := receiptOf(t, c, id)
			require.Equal(t, tc.step, receipt.step)
			require.False(t, receipt.finalized)
			require.Nil(t, receipt.claim)
			require.NotNil(t, receipt.source)
			require.Equal(t, key, *receipt.source)
			require.Equal(t, "deleting", documentState(t, c, id))

			// At the failure boundary the retired document stays invisible.
			require.Equal(t, http.StatusNotFound, doRequest(t, second, http.MethodGet, "/documents/"+id, token, nil, "").code)
			list := doRequest(t, second, http.MethodGet, "/documents", token, nil, "")
			require.Equal(t, http.StatusOK, list.code)
			require.NotContains(t, list.body, id)
			require.Equal(t, http.StatusNotFound, doRequest(t, second, http.MethodGet, "/documents/"+id+"/file", token, nil, "").code)
			require.Zero(t, stores.count("sign"), "no signature after the intent committed")
			visible, err = visibility.VisibleDocumentIDs(t.Context(), []string{id})
			require.NoError(t, err)
			require.Empty(t, visible)
			reserved := doRequest(t, second, http.MethodPost, "/ingest", token, nil, `{"document_id":"`+id+`","content":"replacement"}`)
			require.Equal(t, http.StatusConflict, reserved.code, reserved.body)
			require.Contains(t, reserved.body, "reserved by a deletion")
			shared := doRequest(t, second, http.MethodPost, "/ingest", token, nil, `{"content":"`+content+`"}`)
			require.Equal(t, http.StatusAccepted, shared.code, "shared content must not attach to a retired document: "+shared.body)
			require.NotContains(t, shared.body, id)

			// Storage that is genuinely missing fails before any further effect.
			calls := stores.all()
			noSource := deletionServer(t, retry, token, deletionRoutes{vectors: stores})
			reply := sendDelete(t, noSource, token, id, incarnation)
			require.Equal(t, http.StatusServiceUnavailable, reply.code, reply.body)
			require.Contains(t, reply.body, `"error":"storage"`)
			require.Equal(t, calls, stores.all())

			require.Equal(t, http.StatusOK, sendDelete(t, second, token, id, incarnation).code)
			for kind, want := range tc.counts {
				require.Equal(t, want, stores.count(kind), kind)
			}
			require.Empty(t, documentState(t, c, id))
			require.True(t, receiptOf(t, c, id).finalized)
			require.Equal(t, http.StatusNotFound, sendDelete(t, first, token, id, "").code, "an unqualified request for an absent UUID")
			require.Equal(t, http.StatusOK, sendDelete(t, first, token, id, incarnation).code, "a matching retry acknowledges completion")
			require.Equal(t, http.StatusNotFound, sendDelete(t, first, token, id, uuid.NewString()).code)
		})
	}
}

func TestDeletionRetainsCheckpointsAfterFinalCommitRollsBack(t *testing.T) {
	c := durableDatabase(t)
	activateDeletion(t, c)
	stores := newDeletionStores()
	server := deletionServer(t, c, "token", fullRoutes(stores))
	id, key := uuid.NewString(), "commit-boundary-source"
	incarnation := acceptDeletable(t, c, id, "commit boundary", true, key)
	_, err := c.Pool.Exec(t.Context(), `
		CREATE FUNCTION reject_document_delete() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'final commit rejected'; END $$;
		CREATE CONSTRAINT TRIGGER reject_document_delete AFTER DELETE ON documents
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_document_delete()`)
	require.NoError(t, err)

	require.Equal(t, http.StatusInternalServerError, sendDelete(t, server, "token", id, incarnation).code)
	receipt := receiptOf(t, c, id)
	require.Equal(t, 3, receipt.step)
	require.False(t, receipt.finalized)
	require.NotNil(t, receipt.source)
	require.Equal(t, key, *receipt.source)
	require.Equal(t, "deleting", documentState(t, c, id))
	require.Equal(t, 1, stores.count("source"))

	_, err = c.Pool.Exec(t.Context(), `DROP TRIGGER reject_document_delete ON documents`)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, sendDelete(t, server, "token", id, incarnation).code)
	require.Equal(t, 1, stores.count("source"), "acknowledged source cleanup is not repeated after a final commit fault")
	require.Equal(t, 1, stores.count("revision"))
	require.True(t, receiptOf(t, c, id).finalized)
}

// lostCommitConn drops the PostgreSQL CommandComplete message after COMMIT succeeds.
type lostCommitConn struct {
	net.Conn
	armed *atomic.Bool
	tail  []byte
}

var commitComplete = []byte{'C', 0, 0, 0, 11, 'C', 'O', 'M', 'M', 'I', 'T', 0}

func (c *lostCommitConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n == 0 || !c.armed.Load() {
		return n, err
	}
	c.tail = append(c.tail, p[:n]...)
	if bytes.Contains(c.tail, commitComplete) && c.armed.CompareAndSwap(true, false) {
		_ = c.Close()
		return 0, io.ErrUnexpectedEOF
	}
	c.tail = append(c.tail[:0], c.tail[max(0, len(c.tail)-len(commitComplete)+1):]...)
	return n, err
}

func TestDeleteResolvesLostFinalCommitReply(t *testing.T) {
	observer := durableDatabase(t)
	activateDeletion(t, observer)
	var armed atomic.Bool
	faulted := otherDatabaseClient(t, observer, func(cfg *pgxpool.Config) {
		dial := cfg.ConnConfig.DialFunc
		cfg.MaxConns = 1
		cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dial(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &lostCommitConn{Conn: conn, armed: &armed}, nil
		}
	})
	stores := newDeletionStores()
	id, key := uuid.NewString(), "lost-commit-reply-source"
	incarnation := acceptDeletable(t, observer, id, "lost reply", true, key)
	server := deletionServer(t, faulted, "token", fullRoutes(stores))
	// Arm the fault when the last external step returns, so the lost reply
	// belongs to the transaction that removes the document.
	stores.hook("source", func(context.Context) error {
		armed.Store(true)
		return nil
	})
	reply := sendDelete(t, server, "token", id, incarnation)
	require.Equal(t, http.StatusOK, reply.code, reply.body)
	require.False(t, armed.Load(), "did not lose the final COMMIT response")
	require.Equal(t, 1, stores.count("source"), "acknowledged source cleanup is not repeated")
	require.Equal(t, 1, stores.count("revision"))
	require.Empty(t, documentState(t, observer, id))
	require.True(t, receiptOf(t, observer, id).finalized)
	require.Equal(t, http.StatusNotFound, sendDelete(t, deletionServer(t, observer, "token", fullRoutes(stores)), "token", id, "").code)
}

func TestDeleteContractOnInactiveAndActiveDatabases(t *testing.T) {
	c := durableDatabase(t)
	stores := newDeletionStores()
	server := deletionServer(t, c, "permitted", fullRoutes(stores))
	newDocument := func(content string) string {
		id := uuid.NewString()
		require.NoError(t, c.InsertDocument(t.Context(), domain.Document{ID: domain.DocumentID(id), Content: content}))
		require.NoError(t, c.UpdateDocumentStatus(t.Context(), domain.DocumentID(id), "completed"))
		return id
	}
	live, legacy := newDocument("live"), newDocument("legacy")

	// Inactive: a premature qualifier creates no intent and the bare request
	// keeps addressing the live UUID.
	require.Equal(t, http.StatusPreconditionFailed, sendDelete(t, server, "permitted", live, storedIncarnation(t, c, live)).code)
	require.Equal(t, "completed", documentState(t, c, live))
	require.Zero(t, receiptCount(t, c))
	require.Equal(t, http.StatusOK, sendDelete(t, server, "permitted", legacy, "").code)
	require.Empty(t, documentState(t, c, legacy))
	active, err := c.DeletionActive(t.Context())
	require.NoError(t, err)
	require.False(t, active, "serving requests never activates the database")

	activateDeletion(t, c)
	incarnation := storedIncarnation(t, c, live)
	calls := stores.all()
	for _, tc := range []struct {
		name, token, incarnation string
		target                   string
		want                     int
	}{
		{name: "missing token", incarnation: incarnation, target: live, want: http.StatusUnauthorized},
		{name: "wrong token", token: "denied", incarnation: incarnation, target: live, want: http.StatusUnauthorized},
		{name: "live bare request", token: "permitted", target: live, want: http.StatusPreconditionRequired},
		{name: "mismatched lifetime", token: "permitted", incarnation: uuid.NewString(), target: live, want: http.StatusPreconditionFailed},
		{name: "malformed lifetime", token: "permitted", incarnation: "not-a-uuid", target: live, want: http.StatusBadRequest},
		{name: "absent bare request", token: "permitted", target: uuid.NewString(), want: http.StatusNotFound},
		{name: "absent qualified request", token: "permitted", incarnation: incarnation, target: uuid.NewString(), want: http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sendDelete(t, server, tc.token, tc.target, tc.incarnation).code)
			require.Equal(t, calls, stores.all(), "a refused request must not reach a store")
			require.Equal(t, "completed", documentState(t, c, live), "a refused request must not change the document")
			require.Zero(t, receiptCount(t, c), "a refused request must not write a receipt")
		})
	}

	// Required storage that is not configured is refused before any mutation.
	keyed := uuid.NewString()
	keyedIncarnation := acceptDeletable(t, c, keyed, "stored original", true, "original/"+keyed)
	for name, routes := range map[string]deletionRoutes{
		"no object storage": {vectors: stores},
		"no vector storage": {objects: stores},
		"neither store":     {},
	} {
		t.Run(name, func(t *testing.T) {
			reply := sendDelete(t, deletionServer(t, c, "permitted", routes), "permitted", keyed, keyedIncarnation)
			require.Equal(t, http.StatusServiceUnavailable, reply.code, reply.body)
			require.Contains(t, reply.body, `"error":"storage"`)
			require.Equal(t, calls, stores.all())
			require.Equal(t, "processing", documentState(t, c, keyed))
			require.Zero(t, receiptCount(t, c))
		})
	}
	require.Equal(t, http.StatusOK, sendDelete(t, server, "permitted", live, incarnation).code)
	require.Equal(t, http.StatusOK, sendDelete(t, server, "permitted", live, incarnation).code, "a matching retry while absent")
}

func TestDeleteWithdrawsRetiredEvidenceFromJSONAndSSE(t *testing.T) {
	c := durableDatabase(t)
	activateDeletion(t, c)
	index := memory.NewFakeVectorStore()
	e := embedder.NewFakeEmbedder(256)
	w := durableWorker(c, index, e)
	w.Start(t.Context())
	t.Cleanup(w.Shutdown)
	var failDelete atomic.Bool
	vectors := &pendingDeletionVectors{FakeVectorStore: index, fail: &failDelete}
	cfg := &config.Config{
		FakeMode: true, ServiceToken: "evidence", BedrockFallbackModel: "offline",
		MaxUploadBytes: 1 << 20, MaxChunksPerQuery: 10, WorkerQueueSize: 10, ExtractConcurrency: 1,
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	server := httptest.NewServer(api.NewRouter(&api.RouterDeps{
		Config: cfg, Worker: w, ChunkStore: c, VectorStore: index, PineconeDriver: vectors, PGPool: c.Pool,
		Retriever: retriever.NewRetriever(index, e, 0, 10), Ambassador: amb,
	}))
	t.Cleanup(server.Close)
	reader, err := client.New(server.URL, client.WithServiceToken(cfg.ServiceToken))
	require.NoError(t, err)
	id := uuid.NewString()
	submit := func(text string) {
		t.Helper()
		reply := doRequest(t, server, http.MethodPost, "/ingest", cfg.ServiceToken, nil, `{"document_id":"`+id+`","content":"`+text+`"}`)
		require.Equal(t, http.StatusAccepted, reply.code, reply.body)
		require.Eventually(t, func() bool {
			doc, lookupErr := reader.GetDocument(t.Context(), id)
			return lookupErr == nil && doc.Status == "completed"
		}, 10*time.Second, 10*time.Millisecond)
	}
	query := client.QueryRequest{Query: "policy", AllowedDocumentIDs: []string{id}, Mode: "global"}
	const oldText, newText = "The old policy says blue folders.", "The new policy says green folders."
	submit(oldText)
	before, err := reader.Query(t.Context(), query)
	require.NoError(t, err)
	require.Len(t, before.ScoredChunks, 1)
	require.Equal(t, oldText, before.ScoredChunks[0].Text)
	incarnation := storedIncarnation(t, c, id)

	// The cleanup fails before it removes any vector, so the retired chunk
	// stays in the index while its intent is committed.
	failDelete.Store(true)
	require.Equal(t, http.StatusInternalServerError, doRequest(t, server, http.MethodDelete, "/documents/"+id, cfg.ServiceToken,
		map[string]string{"X-Document-Incarnation": incarnation}, "").code)
	hits, err := index.QueryAllNamespaces(t.Context(), make([]float32, 256), 10, "")
	require.NoError(t, err)
	require.Len(t, hits, 1, "the retired vector is still in the index")
	withdrawn, err := reader.Query(t.Context(), query)
	require.NoError(t, err)
	require.Empty(t, withdrawn.ScoredChunks)
	require.Equal(t, client.TokenUsage{}, withdrawn.TokenUsage, "no model call for retired evidence")
	var events []string
	require.NoError(t, reader.QueryStream(t.Context(), query, func(event client.Event) error {
		events = append(events, string(event.Data))
		return nil
	}))
	require.NotContains(t, strings.Join(events, " "), oldText)
	evidenceOnly := query
	evidenceOnly.AnswerMode = client.AnswerModeEvidence
	only, err := reader.Query(t.Context(), evidenceOnly)
	require.NoError(t, err)
	require.Empty(t, only.ScoredChunks)

	failDelete.Store(false)
	require.Equal(t, http.StatusOK, doRequest(t, server, http.MethodDelete, "/documents/"+id, cfg.ServiceToken,
		map[string]string{"X-Document-Incarnation": incarnation}, "").code)
	submit(newText)
	current, err := reader.Query(t.Context(), query)
	require.NoError(t, err)
	require.Len(t, current.ScoredChunks, 1)
	require.Equal(t, newText, current.ScoredChunks[0].Text, "the replacement owns its vectors")
	require.NotContains(t, current.Answer, oldText)
}

// pendingDeletionVectors fails a document-wide delete before it removes anything.
type pendingDeletionVectors struct {
	*memory.FakeVectorStore
	fail *atomic.Bool
}

func (v *pendingDeletionVectors) DeleteByDocID(ctx context.Context, id string) error {
	if v.fail.Load() {
		return errors.New("provider unavailable")
	}
	return v.FakeVectorStore.DeleteByDocID(ctx, id)
}

type sourceObjects struct {
	mu   sync.Mutex
	keys map[string]bool
}

func (s *sourceObjects) Upload(_ context.Context, id, _, _ string, _ extractor.FileType, _ io.Reader) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[id] = true
	return id, nil
}

func (s *sourceObjects) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.keys, key)
	return nil
}

func (s *sourceObjects) held() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := make(map[string]bool, len(s.keys))
	for key := range s.keys {
		held[key] = true
	}
	return held
}

// deleteBeforeAdmission runs a deletion once between the handler's preflight
// and the admission that follows it.
type deleteBeforeAdmission struct {
	*pgstore.Client
	once func()
}

func (s deleteBeforeAdmission) AcceptDocument(ctx context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	s.once()
	return s.Client.AcceptDocument(ctx, req)
}

func TestMultipartRestagesSourceAfterDeletion(t *testing.T) {
	c := durableDatabase(t)
	activateDeletion(t, c)
	id := uuid.NewString()
	objects := &sourceObjects{keys: map[string]bool{}}
	send := func(store domain.ChunkStore) *httptest.ResponseRecorder {
		body := new(bytes.Buffer)
		w := multipart.NewWriter(body)
		require.NoError(t, w.WriteField("document_id", id))
		part, err := w.CreateFormFile("file", "source.txt")
		require.NoError(t, err)
		_, err = io.WriteString(part, "A stored document with a source file.")
		require.NoError(t, err)
		require.NoError(t, w.Close())
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", body)
		req.Header.Set("Content-Type", w.FormDataContentType())
		rec := httptest.NewRecorder()
		handler.IngestHandler(nil, store, objects, 1<<20)(rec, req)
		return rec
	}
	first := send(c)
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	old, err := c.GetDocumentS3Key(t.Context(), domain.DocumentID(id))
	require.NoError(t, err)
	require.NotNil(t, old)
	require.NoError(t, c.UpdateDocumentStatus(t.Context(), domain.DocumentID(id), "completed"))
	require.Equal(t, http.StatusOK, send(c).Code)
	require.Len(t, objects.held(), 1)

	incarnation := storedIncarnation(t, c, id)
	once := sync.OnceFunc(func() {
		require.NoError(t, cleanup.RemoveIncarnation(t.Context(), c, nil, objects, cleanup.Targets{}, domain.DocumentID(id), incarnation))
	})
	second := send(deleteBeforeAdmission{Client: c, once: once})
	require.Equal(t, http.StatusAccepted, second.Code, second.Body.String())
	replacement, err := c.GetDocumentS3Key(t.Context(), domain.DocumentID(id))
	require.NoError(t, err)
	require.NotNil(t, replacement)
	require.NotEqual(t, *old, *replacement, "the replacement stages its own source")
	require.Equal(t, map[string]bool{*replacement: true}, objects.held(), "the retired source is gone and the new one stays")
	require.NotEqual(t, incarnation, storedIncarnation(t, c, id))
}

func TestDeleteSurvivesClientDisconnectAndGracefulShutdown(t *testing.T) {
	c := durableDatabase(t)
	activateDeletion(t, c)
	stores := newDeletionStores()
	id, key := uuid.NewString(), "disconnect-source"
	incarnation := acceptDeletable(t, c, id, "disconnect", true, key)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	stores.hook("source", func(ctx context.Context) error {
		once.Do(func() { close(entered) })
		<-release
		return ctx.Err()
	})
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: deletionMux(c, "token", fullRoutes(stores)), ReadHeaderTimeout: time.Second}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	ctx, disconnect := context.WithCancel(t.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://"+listener.Addr().String()+"/documents/"+id, http.NoBody)
	require.NoError(t, err)
	req.Header.Set("X-Service-Token", "token")
	req.Header.Set("X-Document-Incarnation", incarnation)
	clientDone := make(chan error, 1)
	go func() {
		resp, doErr := http.DefaultClient.Do(req)
		if doErr == nil {
			_ = resp.Body.Close()
		}
		clientDone <- doErr
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("cleanup never reached the source store")
	}
	disconnect()
	require.Error(t, <-clientDone)
	shutdown := make(chan error, 1)
	go func() {
		drain, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		shutdown <- server.Shutdown(drain)
	}()
	require.Equal(t, 2, receiptOf(t, c, id).step, "the source step is the one in flight")
	close(release)
	require.NoError(t, <-shutdown, "the drain waits for the deletion to finish")
	require.Equal(t, 1, stores.count("source"))
	require.Empty(t, documentState(t, c, id))
	require.True(t, receiptOf(t, c, id).finalized)
	require.ErrorIs(t, <-served, http.ErrServerClosed)
}

func TestConcurrentDuplicateDeletesRunEachStepOnce(t *testing.T) {
	c := durableDatabase(t)
	other := otherDatabaseClient(t, c)
	activateDeletion(t, c)
	stores := newDeletionStores()
	id := uuid.NewString()
	incarnation := acceptDeletable(t, c, id, "concurrent duplicates", true, "concurrent-source")
	stores.hook("revision", func(context.Context) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	servers := []*httptest.Server{deletionServer(t, c, "token", fullRoutes(stores)), deletionServer(t, other, "token", fullRoutes(stores))}
	codes := make(chan int, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() { codes <- sendDelete(t, servers[i%2], "token", id, incarnation).code })
	}
	wg.Wait()
	close(codes)
	ok := 0
	for code := range codes {
		switch code {
		case http.StatusOK:
			ok++
		case http.StatusServiceUnavailable:
		default:
			t.Errorf("unexpected status %d", code)
		}
	}
	require.Positive(t, ok)
	require.Equal(t, http.StatusOK, sendDelete(t, servers[0], "token", id, incarnation).code)
	require.Equal(t, 1, stores.count("revision"))
	require.Equal(t, 1, stores.count("source"))
	require.True(t, receiptOf(t, c, id).finalized)
}

func TestDeleteDuringProcessingDoesNotResurrectOnAnActiveDatabase(t *testing.T) {
	c := durableDatabase(t)
	activateDeletion(t, c)
	v := memory.NewFakeVectorStore()
	e := durableBlockingEmbedder{make(chan struct{}, 1), make(chan struct{})}
	w := durableWorker(c, v, e)
	rec := durableSubmit(t, w, c, `{"content":"Delete this document during processing."}`)
	require.Equal(t, http.StatusAccepted, rec.Code)
	var response handler.IngestResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	w.Start(t.Context())
	select {
	case <-e.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("processing did not start")
	}
	incarnation := storedIncarnation(t, c, response.DocumentID)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+response.DocumentID, http.NoBody)
	req.SetPathValue("doc_id", response.DocumentID)
	req.Header.Set("X-Document-Incarnation", incarnation)
	deleted := httptest.NewRecorder()
	handler.DeleteHandler(c, v, nil, handler.WithCleanupTargets(cleanup.Targets{InMemory: true}))(deleted, req)
	require.Equal(t, http.StatusOK, deleted.Code, deleted.Body.String())
	close(e.finish)
	w.Shutdown()
	exists, err := c.DocumentExists(t.Context(), domain.DocumentID(response.DocumentID))
	require.NoError(t, err)
	require.False(t, exists)
	chunks, err := v.Query(t.Context(), "", []float32{1}, 10, "")
	require.NoError(t, err)
	require.Empty(t, chunks, "the retired worker must not write after the intent")
	var chunkRows int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunks WHERE document_id=$1`, response.DocumentID).Scan(&chunkRows))
	require.Zero(t, chunkRows)
}

func TestConcurrentReuseNeverTouchesTheReplacement(t *testing.T) {
	c := durableDatabase(t)
	other := otherDatabaseClient(t, c)
	activateDeletion(t, c)
	stores := newDeletionStores()
	stores.hook("source", func(context.Context) error {
		time.Sleep(150 * time.Millisecond)
		return nil
	})
	server := deletionServer(t, c, "token", fullRoutes(stores))
	id := uuid.NewString()
	oldKey, newKey := "source/"+id+"/original", "source/"+id+"/replacement"
	original := acceptDeletable(t, c, id, "original lifetime", true, oldKey)

	deleted := make(chan int, 1)
	go func() { deleted <- sendDelete(t, server, "token", id, original).code }()
	deadline := time.Now().Add(20 * time.Second)
	for documentState(t, c, id) != "deleting" {
		require.True(t, time.Now().Before(deadline), "the intent never committed")
		time.Sleep(2 * time.Millisecond)
	}
	// A replacement is refused while the deletion is unfinished and accepted
	// as soon as its receipt is complete.
	replacement := domain.AcceptRequest{
		Document: domain.Document{ID: domain.DocumentID(id), Content: "replacement lifetime", S3Key: &newKey}, CallerSupplied: true,
		Fingerprint: chunker.ScopedFingerprint("", "replacement lifetime"), MaxPending: 100, SupportsScopedVectors: true,
	}
	refused := 0
	for {
		result, err := other.AcceptDocument(t.Context(), replacement)
		require.NoError(t, err)
		if result.Outcome == domain.AcceptDeletionConflict {
			refused++
			require.True(t, time.Now().Before(deadline), "the replacement was never admitted")
			time.Sleep(5 * time.Millisecond)
			continue
		}
		require.Equal(t, domain.AcceptAccepted, result.Outcome)
		break
	}
	require.Equal(t, http.StatusOK, <-deleted)
	require.Positive(t, refused, "the admission raced an unfinished deletion")

	successor := storedIncarnation(t, c, id)
	require.NotEqual(t, original, successor)
	require.Equal(t, http.StatusPreconditionFailed, sendDelete(t, server, "token", id, original).code, "a stale retry is rejected while a replacement lifetime is live")
	require.Equal(t, "processing", documentState(t, c, id))
	stored, err := c.GetDocumentS3Key(t.Context(), domain.DocumentID(id))
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, newKey, *stored, "the replacement keeps its own file")
	require.Equal(t, []string{"source:" + oldKey}, stores.sources())
	for _, call := range stores.all() {
		require.NotContains(t, call, successor, "no operation may name the replacement's incarnation")
	}
}
