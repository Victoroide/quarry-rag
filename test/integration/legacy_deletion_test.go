package integration

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	deletionLockOperationTimeout = 10 * time.Second
	deletionLockCleanupTimeout   = 10 * time.Second
)

// legacyRoutes composes the production handlers with the reservation guard that
// the server entry point enables.
func legacyRoutes(stores *deletionStores) deletionRoutes {
	routes := fullRoutes(stores)
	routes.options = []handler.DeleteOption{
		handler.WithLegacyReservations(),
		handler.WithCleanupTargets(cleanup.Targets{SourceBucket: "originals", VectorIndex: "index"}),
	}
	return routes
}

type legacyReceiptRow struct {
	kind      string
	held      bool
	finalized bool
	claimLive bool
}

func legacyReceiptOf(ctx context.Context, t *testing.T, c *pgstore.Client, id string) legacyReceiptRow {
	t.Helper()
	var r legacyReceiptRow
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT kind,held,finalized,COALESCE(claim_until>clock_timestamp(),FALSE)
		FROM document_deletions WHERE document_id=$1`, id).Scan(&r.kind, &r.held, &r.finalized, &r.claimLive))
	return r
}

func boundedLegacyReceiptOf(t *testing.T, c *pgstore.Client, id string) legacyReceiptRow {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	return legacyReceiptOf(ctx, t, c, id)
}

func admission(t *testing.T, c *pgstore.Client, id, content string) domain.AcceptOutcome {
	t.Helper()
	return admissionWithContext(t.Context(), t, c, id, content)
}

func admissionWithContext(ctx context.Context, t *testing.T, c *pgstore.Client, id, content string) domain.AcceptOutcome {
	t.Helper()
	result, err := c.AcceptDocument(ctx, domain.AcceptRequest{
		Document: domain.Document{ID: domain.DocumentID(id), Content: content}, CallerSupplied: true,
		Fingerprint: chunker.ScopedFingerprint("", content), MaxPending: 100,
	})
	require.NoError(t, err)
	return result.Outcome
}

// terminateActivationHolders simulates lost sessions holding the shared
// activation lock in this fixture database.
func terminateActivationHolders(t *testing.T, c *pgstore.Client) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	n, err := endDeletionActivationHolders(ctx, c.Pool)
	cancel()
	require.NoError(t, err, "terminate activation lock holders in the local fixture")
	return n
}

func endDeletionActivationHolders(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	rows, err := pool.Query(ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND mode='ShareLock'
		AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
		AND classid::bigint=$1 AND objid::bigint=$2 AND objsubid=1`,
		(pgstore.DeletionActivationLockID>>32)&0xffffffff, pgstore.DeletionActivationLockID&0xffffffff)
	if err != nil {
		return 0, fmt.Errorf("query activation lock holders in the local fixture: %w", err)
	}
	defer rows.Close()

	var pids []int32
	for rows.Next() {
		var pid int32
		if err := rows.Scan(&pid); err != nil {
			return 0, fmt.Errorf("scan activation lock holder in the local fixture: %w", err)
		}
		pids = append(pids, pid)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read activation lock holders in the local fixture: %w", err)
	}

	for _, pid := range pids {
		var ended bool
		if err := pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&ended); err != nil {
			return 0, fmt.Errorf("terminate activation lock holder PID %d in the local fixture: %w", pid, err)
		}
		if !ended {
			return 0, fmt.Errorf("activation lock holder PID %d in the local fixture was not terminated", pid)
		}
	}
	return len(pids), nil
}

func holdSiblingActivationLock(t *testing.T, c *pgstore.Client) pgx.Tx {
	t.Helper()
	address := pgtest.Sibling(t, c.Pool.Config().ConnString())
	conn := openDeletionLockConn(t, address, "sibling")
	tx, err := beginSiblingActivationLock(t.Context(), conn)
	require.NoError(t, err)
	return tx
}

func beginSiblingActivationLock(ctx context.Context, conn *pgx.Conn) (pgx.Tx, error) {
	beginCtx, cancelBegin := context.WithTimeout(ctx, deletionLockOperationTimeout)
	tx, err := conn.Begin(beginCtx)
	cancelBegin()
	if err != nil {
		return nil, fmt.Errorf("begin transaction in the sibling fixture: %w", err)
	}

	lockCtx, cancelLock := context.WithTimeout(ctx, deletionLockOperationTimeout)
	_, err = tx.Exec(lockCtx, `SELECT pg_advisory_xact_lock_shared($1)`, pgstore.DeletionActivationLockID)
	cancelLock()
	if err == nil {
		return tx, nil
	}

	lockErr := fmt.Errorf("acquire shared activation lock in the sibling fixture: %w", err)
	cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), deletionLockCleanupTimeout)
	defer cancelCleanup()
	if err := tx.Rollback(cleanupCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return nil, errors.Join(lockErr, fmt.Errorf("roll back transaction in the sibling fixture: %w", err))
	}
	return nil, lockErr
}

func openDeletionLockConn(t *testing.T, address, label string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	conn, err := pgx.Connect(ctx, address)
	cancel()
	if err != nil {
		t.Fatalf("dial %s fixture connection: %v", label, deletionLockConnectError{cause: err})
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), deletionLockCleanupTimeout)
		defer cancel()
		if err := conn.Close(cleanupCtx); err != nil {
			t.Errorf("close %s fixture connection: %v", label, err)
		}
	})
	return conn
}

func openDeletionLockClient(t *testing.T, address string, opts ...pgstore.Option) *pgstore.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	client, err := pgstore.NewClient(ctx, address, opts...)
	cancel()
	if err != nil {
		t.Fatalf("open local fixture client: %v", deletionLockConnectError{cause: err})
	}
	t.Cleanup(func() { require.NoError(t, client.Close(context.Background())) })
	return client
}

// pgx connection errors can contain the DSN or user. Retain the cause for error
// inspection and keep the diagnostics independent of the connection settings.
type deletionLockConnectError struct{ cause error }

func (e deletionLockConnectError) Error() string {
	var configErr *pgconn.ParseConfigError
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(e.cause, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case errors.Is(e.cause, context.Canceled):
		return context.Canceled.Error()
	case errors.As(e.cause, &configErr):
		return "invalid connection configuration"
	case errors.As(e.cause, &pgErr):
		return "server rejected the connection (SQLSTATE " + pgErr.SQLState() + ")"
	default:
		return "connection failed"
	}
}

func (e deletionLockConnectError) Unwrap() error { return e.cause }

func TestBareDeleteBeforeActivationReservesFirstAndRepeatReportsAbsence(t *testing.T) {
	c := durableDatabase(t)
	stores := newDeletionStores()
	id := uuid.NewString()
	key := "source/" + id + "/original"
	acceptDeletable(t, c, id, "legacy document", false, key)
	var atFirstCall legacyReceiptRow
	stores.hook("namespace", func(ctx context.Context) error {
		atFirstCall = legacyReceiptOf(ctx, t, c, id)
		return nil
	})
	server := deletionServer(t, c, "token", legacyRoutes(stores))

	require.Equal(t, http.StatusOK, sendDelete(t, server, "token", id, "").code)

	require.Equal(t, legacyReceiptRow{kind: "legacy_reservation", held: true, finalized: false, claimLive: true}, atFirstCall,
		"the reservation is durable before the first external call")
	require.Equal(t, []string{"namespace:" + id, "vectors:" + id, "source:" + key, "vectors:" + id}, stores.all())
	final := legacyReceiptOf(t.Context(), t, c, id)
	require.True(t, final.finalized)
	require.True(t, final.held, "historical document-wide hosted effects stay held")
	require.Equal(t, http.StatusNotFound, sendDelete(t, server, "token", id, "").code, "a repeat reports absence")
	require.Equal(t, domain.AcceptDeletionConflict, admission(t, c, id, "replacement"))
}

func TestIDOnlyDeleteAcrossActivationFollowsTheActivatedContract(t *testing.T) {
	c := durableDatabase(t)
	stale := deletionServer(t, c, "token", legacyRoutes(newDeletionStores()))
	stores := newDeletionStores()
	id := uuid.NewString()
	incarnation := acceptDeletable(t, c, id, "live document", false, "")
	activateDeletion(t, c)
	restarted := deletionServer(t, otherDatabaseClient(t, c), "token", legacyRoutes(stores))

	for name, server := range map[string]*httptest.Server{"stale replica": stale, "restarted replica": restarted} {
		reply := sendDelete(t, server, "token", id, "")
		require.Equal(t, http.StatusPreconditionRequired, reply.code, name+": "+reply.body)
	}
	require.Empty(t, stores.all())
	require.Equal(t, http.StatusPreconditionFailed, sendDelete(t, restarted, "token", id, uuid.NewString()).code, "a wrong token")
	require.Empty(t, stores.all())
	require.Equal(t, http.StatusOK, sendDelete(t, restarted, "token", id, incarnation).code, "the original token")
	require.Equal(t, http.StatusNotFound, sendDelete(t, stale, "token", id, "").code)
}

func TestQualifiedDeleteBeforeActivationFailsClosed(t *testing.T) {
	c := durableDatabase(t)
	stores := newDeletionStores()
	id := uuid.NewString()
	incarnation := acceptDeletable(t, c, id, "live document", false, "")
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	for _, value := range []string{incarnation, "", "latest"} {
		require.Equal(t, http.StatusPreconditionFailed, doRequest(t, server, http.MethodDelete, "/documents/"+id, "token",
			map[string]string{"X-Document-Incarnation": value}, "").code)
	}
	require.Empty(t, stores.all(), "a qualifier never becomes an ID-only deletion")
	require.Equal(t, "processing", documentState(t, c, id))
}

func TestPreMigrationBareRetryCannotRemoveAReplacement(t *testing.T) {
	c := durableDatabase(t)
	stores := newDeletionStores()
	id := uuid.NewString()
	acceptDeletable(t, c, id, "first lifetime", false, "source/"+id+"/old")
	// An executable that predates receipts removed the first lifetime and recorded nothing.
	_, err := c.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, id)
	require.NoError(t, err)
	activateDeletion(t, c)
	newKey := "source/" + id + "/new"
	replacement := acceptDeletable(t, c, id, "multipart replacement", true, newKey)
	server := deletionServer(t, c, "token", legacyRoutes(stores))

	require.Equal(t, http.StatusPreconditionRequired, sendDelete(t, server, "token", id, "").code, "the delayed bare request")
	require.Empty(t, stores.all(), "no vector or object call reaches the replacement")
	stored, err := c.GetDocumentS3Key(t.Context(), domain.DocumentID(id))
	require.NoError(t, err)
	require.Equal(t, newKey, *stored)
	require.Equal(t, replacement, storedIncarnation(t, c, id))
	require.Zero(t, receiptCount(t, c), "there was no receipt and the request created none")
}

func TestFirstHostedFailureKeepsTheLiveRowVisibleUntilTheQualifiedIntentCommits(t *testing.T) {
	c := durableDatabase(t)
	stores := newDeletionStores()
	stores.hook("namespace", failOnce("provider unavailable"))
	id := uuid.NewString()
	key := "source/" + id + "/original"
	const content = "document whose first hosted call fails"
	req := domain.AcceptRequest{
		Document: domain.Document{ID: domain.DocumentID(id), Content: content, S3Key: &key}, CallerSupplied: true,
		Fingerprint: chunker.ScopedFingerprint("", content), MaxPending: 100, ContentDedup: domain.ContentDedupCollection,
	}
	accepted, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	incarnation := storedIncarnation(t, c, id)
	require.NoError(t, c.UpdateDocumentStatus(t.Context(), domain.DocumentID(id), "completed"))
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	duplicate := func() domain.AcceptOutcome {
		// Content matching applies to an upload that supplies no document_id.
		other := req
		other.CallerSupplied = false
		other.Document = domain.Document{ID: domain.DocumentID(uuid.NewString()), Content: content}
		result, dupErr := c.AcceptDocument(t.Context(), other)
		require.NoError(t, dupErr)
		return result.Outcome
	}
	live := func(phase string) {
		require.Equal(t, http.StatusOK, doRequest(t, server, http.MethodGet, "/documents/"+id, "token", nil, "").code, phase+" status")
		require.Equal(t, http.StatusOK, doRequest(t, server, http.MethodGet, "/documents/"+id+"/file", "token", nil, "").code, phase+" signer")
		require.Equal(t, domain.AcceptDuplicate, duplicate(), phase+" duplicate")
	}

	require.Equal(t, http.StatusInternalServerError, sendDelete(t, server, "token", id, "").code)
	require.Equal(t, "legacy_reservation", legacyReceiptOf(t.Context(), t, c, id).kind)
	require.False(t, legacyReceiptOf(t.Context(), t, c, id).claimLive)
	live("before activation")
	activateDeletion(t, c)
	live("after activation, before the qualified retry")

	// The retry converts the reservation and commits the visibility fence, then fails at a later step.
	stores.hook("vectors", failOnce("provider unavailable"))
	require.Equal(t, http.StatusInternalServerError, sendDelete(t, server, "token", id, incarnation).code)
	require.Equal(t, "qualified_intent", legacyReceiptOf(t.Context(), t, c, id).kind)
	require.Equal(t, "deleting", documentState(t, c, id))
	require.Equal(t, http.StatusNotFound, doRequest(t, server, http.MethodGet, "/documents/"+id, "token", nil, "").code, "status after the fence")
	require.Equal(t, http.StatusNotFound, doRequest(t, server, http.MethodGet, "/documents/"+id+"/file", "token", nil, "").code, "the signer is silent")
	require.NotEqual(t, domain.AcceptDuplicate, duplicate(), "a retiring document is no duplicate source")
	require.Equal(t, domain.AcceptDeletionConflict, admission(t, c, id, "conflicting admission"))
	require.Equal(t, http.StatusOK, sendDelete(t, server, "token", id, incarnation).code)
}

func TestHostedFinalCallOutcomesKeepTheIdentityReserved(t *testing.T) {
	uncertain := status.Error(codes.DeadlineExceeded, "final call timed out")
	rejected := domain.RejectedVectorMutation(status.Error(codes.InvalidArgument, "answered before any effect"))
	hidden := func(attempt error) func(context.Context) error {
		return func(ctx context.Context) error {
			domain.NoteVectorAttempt(ctx, attempt)
			return nil
		}
	}
	for _, tc := range []struct {
		name   string
		scoped bool
		final  func(context.Context) error
		status int
		held   bool
	}{
		// Untagged hosted vectors are held whatever happens, so these cases cannot show a note.
		{name: "untagged timeout", final: func(context.Context) error { return uncertain }, status: http.StatusInternalServerError, held: true},
		{name: "untagged retry succeeds after a hidden timeout", final: hidden(uncertain), status: http.StatusOK, held: true},
		// A scoped document is held only when an attempt may have reached the provider.
		{name: "scoped timeout", scoped: true, final: func(context.Context) error { return uncertain }, status: http.StatusInternalServerError, held: true},
		{name: "scoped retry succeeds after a hidden timeout", scoped: true, final: hidden(uncertain), status: http.StatusOK, held: true},
		{name: "scoped retry after a definite rejection", scoped: true, final: hidden(rejected), status: http.StatusOK, held: false},
		{name: "scoped clean final call", scoped: true, final: func(context.Context) error { return nil }, status: http.StatusOK, held: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := durableDatabase(t)
			stores := newDeletionStores()
			id := uuid.NewString()
			acceptDeletable(t, c, id, "hosted legacy document", false, "")
			kind := "vectors"
			if tc.scoped {
				// Admission selects the tagged format only after activation, which ID-only deletion no longer serves.
				markTagged(t, c, id)
				kind = "revision"
			}
			var mu sync.Mutex
			calls := 0
			stores.hook(kind, func(ctx context.Context) error {
				mu.Lock()
				calls++
				call := calls
				mu.Unlock()
				if call == 2 {
					return tc.final(ctx)
				}
				return nil
			})
			server := deletionServer(t, c, "token", legacyRoutes(stores))

			require.Equal(t, tc.status, sendDelete(t, server, "token", id, "").code)

			receipt := legacyReceiptOf(t.Context(), t, c, id)
			require.Equal(t, tc.held, receipt.held)
			require.Equal(t, tc.status == http.StatusOK, receipt.finalized)
			if tc.held || !receipt.finalized {
				require.Equal(t, domain.AcceptDeletionConflict, admission(t, c, id, "replacement"), "the identity stays reserved")
			} else {
				require.Equal(t, domain.AcceptAccepted, admission(t, c, id, "replacement"), "a clean scoped cleanup releases normally")
			}
		})
	}
}

func blockFinalHostedCall(requestCtx context.Context, stores *deletionStores) (entered <-chan struct{}, unblock func()) {
	arrival, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	stores.hook("vectors", func(ctx context.Context) error {
		if calls.Add(1) == 2 {
			close(arrival)
			// The handler detaches cleanup from cancellation of the HTTP request.
			select {
			case <-release:
			case <-requestCtx.Done():
				return context.Cause(requestCtx)
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		return nil
	})
	return arrival, sync.OnceFunc(func() { close(release) })
}

func TestHostedDeleteReportsEarlyVectorFailure(t *testing.T) {
	c := durableDatabase(t)
	stores := newDeletionStores()
	id := uuid.NewString()
	acceptDeletable(t, c, id, "hosted legacy document with an early provider failure", false, "")
	stores.hook("namespace", failOnce("first hosted call failed"))

	requestCtx, cancelRequest := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancelRequest()
	entered, unblock := blockFinalHostedCall(requestCtx, stores)
	defer unblock()
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	results := make(chan deletionHTTPResult, 1)
	startDeletionRequest(results, "hosted DELETE "+id, func() (httpReply, error) {
		return readDeleteReply(requestCtx, server, "token", id, "")
	})
	entryCtx, cancelEntry := context.WithTimeout(t.Context(), 30*time.Second)
	err := waitDeletionArrivals(entryCtx, entered, results, 1, "before the final hosted call")
	cancelEntry()

	require.ErrorContains(t, err, "before the final hosted call")
	require.ErrorContains(t, err, "HTTP 500")
	require.Equal(t, []string{"namespace:" + id}, stores.all(), "the final provider gate was never reached")
}

func TestLostSessionBeforeTheFinalHostedCallKeepsTheHold(t *testing.T) {
	c := durableDatabase(t)
	sibling := holdSiblingActivationLock(t, c)
	activator := otherDatabaseClient(t, c)
	stores := newDeletionStores()
	id := uuid.NewString()
	acceptDeletable(t, c, id, "hosted legacy document", false, "")
	requestCtx, cancelRequest := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancelRequest()
	entered, unblock := blockFinalHostedCall(requestCtx, stores)
	defer unblock()
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	results := make(chan deletionHTTPResult, 1)
	startDeletionRequest(results, "hosted DELETE "+id, func() (httpReply, error) {
		return readDeleteReply(requestCtx, server, "token", id, "")
	})
	entryCtx, cancelEntry := context.WithTimeout(t.Context(), 30*time.Second)
	entryErr := waitDeletionArrivals(entryCtx, entered, results, 1, "before the final hosted call")
	cancelEntry()
	require.NoError(t, entryErr)

	// The row is retired and the last call has not been sent.
	stateCtx, cancelState := context.WithTimeout(t.Context(), 10*time.Second)
	state, err := readDocumentState(stateCtx, c, id)
	cancelState()
	require.NoError(t, err)
	require.Empty(t, state)
	activationCtx, cancelActivation := context.WithCancel(t.Context())
	defer cancelActivation()
	activated := make(chan error, 1)
	go func() {
		_, activationErr := activator.ActivateDeletion(activationCtx)
		activated <- activationErr
	}()
	select {
	case activationErr := <-activated:
		require.NoError(t, activationErr, "activation returned while the cleanup held the lock")
		t.Fatal("a healthy activation committed under the cleanup")
	case <-time.After(500 * time.Millisecond):
	}
	require.Equal(t, 1, terminateActivationHolders(t, activator), "only the guarding session ends")
	var alive bool
	siblingCtx, cancelSibling := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	siblingErr := sibling.QueryRow(siblingCtx, `SELECT TRUE`).Scan(&alive)
	cancelSibling()
	require.NoError(t, siblingErr, "query the sibling fixture session holding the activation lock")
	require.True(t, alive)
	select {
	case activationErr := <-activated:
		require.NoError(t, activationErr, "activation after the lock disappeared")
	case <-time.After(10 * time.Second):
		t.Fatal("activation did not proceed after the lock disappeared")
	}
	admissionCtx, cancelAdmission := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelAdmission()
	require.Equal(t, domain.AcceptDeletionConflict, admissionWithContext(admissionCtx, t, activator, id, "replacement"), "the durable hold still blocks the identity")
	cancelAdmission()

	unblock()
	replyCtx, cancelReply := context.WithTimeout(t.Context(), 10*time.Second)
	reply, err := waitDeletionReply(replyCtx, results, "after releasing the final hosted call")
	cancelReply()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, reply.code, reply.body)
	receipt := boundedLegacyReceiptOf(t, c, id)
	require.True(t, receipt.finalized)
	require.True(t, receipt.held)
	require.Equal(t, domain.AcceptDeletionConflict, admission(t, c, id, "replacement"))
}

// smallPoolDatabase opens a migrated client whose pool holds at most maxConns connections.
func smallPoolDatabase(t *testing.T, maxConns int) *pgstore.Client {
	t.Helper()
	u, err := url.Parse(startPostgres(t))
	require.NoError(t, err)
	q := u.Query()
	q.Set("pool_max_conns", strconv.Itoa(maxConns))
	u.RawQuery = q.Encode()
	c, err := pgstore.NewClient(t.Context(), u.String())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(t.Context()))
	return c
}

// blockFirstCalls pauses the first n calls of a provider kind until unblocked,
// signaling each arrival on entered.
func blockFirstCalls(fixtureCtx context.Context, stores *deletionStores, kind string, n int) (entered <-chan struct{}, unblock func()) {
	arrivals, release := make(chan struct{}, n), make(chan struct{})
	var calls atomic.Int32
	stores.hook(kind, func(ctx context.Context) error {
		if int(calls.Add(1)) > n {
			return nil
		}
		arrivals <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-fixtureCtx.Done():
			return context.Cause(fixtureCtx)
		}
		return nil
	})
	return arrivals, sync.OnceFunc(func() { close(release) })
}

func advisoryLocks(t *testing.T, c *pgstore.Client) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	n, err := countDeletionAdvisoryLocks(ctx, c.Pool)
	cancel()
	require.NoError(t, err, "count advisory locks in the local fixture")
	return n
}

func countDeletionAdvisoryLocks(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var n int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted
		AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&n)
	return n, err
}

func TestLegacyDeletionProbesIgnoreSiblingActivationLock(t *testing.T) {
	address := startPostgres(t)
	c := openDeletionLockClient(t, address)

	sibling := openDeletionLockConn(t, pgtest.Sibling(t, address), "sibling")
	siblingLockCtx, cancelSiblingLock := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	_, err := sibling.Exec(siblingLockCtx, `SELECT pg_advisory_lock_shared($1)`, pgstore.DeletionActivationLockID)
	cancelSiblingLock()
	require.NoError(t, err, "acquire shared activation lock in the sibling fixture")

	require.Zero(t, advisoryLocks(t, c))
	require.Zero(t, terminateActivationHolders(t, c))

	local := openDeletionLockConn(t, address, "local")
	lockCtx, cancelLock := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	_, err = local.Exec(lockCtx, `SELECT pg_advisory_lock_shared($1)`, pgstore.DeletionActivationLockID)
	cancelLock()
	require.NoError(t, err, "acquire shared activation lock in the local fixture")
	require.Equal(t, 1, advisoryLocks(t, c), "only the fixture session's activation lock is counted")

	var localUnlocked bool
	unlockCtx, cancelUnlock := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	err = local.QueryRow(unlockCtx, `SELECT pg_advisory_unlock_shared($1)`, pgstore.DeletionActivationLockID).Scan(&localUnlocked)
	cancelUnlock()
	require.NoError(t, err, "unlock shared activation lock in the local fixture")
	require.True(t, localUnlocked, "the fixture session held the activation lock")
	require.Zero(t, advisoryLocks(t, c), "only the sibling session still holds the activation lock")

	var unlocked bool
	siblingUnlockCtx, cancelSiblingUnlock := context.WithTimeout(t.Context(), deletionLockOperationTimeout)
	err = sibling.QueryRow(siblingUnlockCtx, `SELECT pg_advisory_unlock_shared($1)`, pgstore.DeletionActivationLockID).Scan(&unlocked)
	cancelSiblingUnlock()
	require.NoError(t, err, "unlock shared activation lock in the sibling fixture")
	require.True(t, unlocked, "the sibling session still holds the activation lock")
}

func startGuardedDeletionBatch(ctx context.Context, server *httptest.Server, ids []string) <-chan deletionHTTPResult {
	replies := make(chan deletionHTTPResult, len(ids))
	for _, id := range ids {
		startDeletionRequest(replies, "guarded DELETE "+id, func() (httpReply, error) {
			return readDeleteReply(ctx, server, "token", id, "")
		})
	}
	return replies
}

func TestGuardedIDOnlyDeleteRejectsBurstWith503BusyBeforeLocks(t *testing.T) {
	c := durableDatabase(t)
	c.SetLegacyDeletionConcurrency(2)
	stores := newDeletionStores()
	ids := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	for _, id := range ids {
		acceptDeletable(t, c, id, "burst document "+id, false, "")
	}
	untouched := documentState(t, c, ids[2])
	require.NotEmpty(t, untouched)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	entered, unblock := blockFirstCalls(ctx, stores, "namespace", 2)
	defer unblock()
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	replies := startGuardedDeletionBatch(ctx, server, ids[:2])
	entryCtx, cancelEntry := context.WithTimeout(ctx, 30*time.Second)
	err := waitDeletionArrivals(entryCtx, entered, replies, 2, "before the burst DELETE provider calls")
	cancelEntry()
	require.NoError(t, err)
	locks := advisoryLocks(t, c)
	// Other fixtures can take locks on this server between samples.
	holdSiblingActivationLock(t, c)

	refusedCtx, cancelRefused := context.WithTimeout(t.Context(), 10*time.Second)
	refused, err := readDeleteReply(refusedCtx, server, "token", ids[2], "")
	cancelRefused()
	require.NoError(t, err, "DELETE refused while both slots are occupied")
	require.Equal(t, http.StatusServiceUnavailable, refused.code, refused.body)
	require.Equal(t, "5", refused.retryAfter)
	require.Contains(t, refused.body, `"busy"`)
	require.Equal(t, 2, stores.count("namespace"), "the refused request made no provider call")
	require.Equal(t, locks, advisoryLocks(t, c), "the refused request took no lock")
	stateCtx, cancelState := context.WithTimeout(t.Context(), 10*time.Second)
	state, err := readDocumentState(stateCtx, c, ids[2])
	cancelState()
	require.NoError(t, err, "read the refused document state")
	require.Equal(t, untouched, state, "the refused document is untouched")
	var receipts int
	receiptCtx, cancelReceipt := context.WithTimeout(t.Context(), 10*time.Second)
	err = c.Pool.QueryRow(receiptCtx, `SELECT count(*) FROM document_deletions WHERE document_id=$1`, ids[2]).Scan(&receipts)
	cancelReceipt()
	require.NoError(t, err, "read the refused document receipt count")
	require.Zero(t, receipts, "the refused request reserved nothing")

	unblock()
	replyCtx, cancelReply := context.WithTimeout(ctx, 10*time.Second)
	defer cancelReply()
	for range 2 {
		reply, replyErr := waitDeletionReply(replyCtx, replies, "after releasing the burst DELETE providers")
		require.NoError(t, replyErr)
		require.Equal(t, http.StatusOK, reply.code, reply.body)
	}
	cancelReply()
	retryCtx, cancelRetry := context.WithTimeout(t.Context(), 10*time.Second)
	reply, err := readDeleteReply(retryCtx, server, "token", ids[2], "")
	cancelRetry()
	require.NoError(t, err, "retry DELETE after releasing a slot")
	require.Equal(t, http.StatusOK, reply.code, "a retry succeeds once a slot is free: "+reply.body)
}

func TestSlowGuardedDeletionsDoNotExhaustSmallPool(t *testing.T) {
	c := smallPoolDatabase(t, 2)
	c.SetLegacyDeletionConcurrency(2)
	stores := newDeletionStores()
	ids := []string{uuid.NewString(), uuid.NewString()}
	for _, id := range ids {
		acceptDeletable(t, c, id, "slow document "+id, false, "")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	entered, unblock := blockFirstCalls(ctx, stores, "namespace", 2)
	defer unblock()
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	replies := startGuardedDeletionBatch(ctx, server, ids)
	entryCtx, cancelEntry := context.WithTimeout(ctx, 30*time.Second)
	err := waitDeletionArrivals(entryCtx, entered, replies, 2, "before the small-pool DELETE provider calls")
	cancelEntry()
	require.NoError(t, err)

	const batches = 2
	type request struct {
		operation  string
		method     string
		path       string
		payload    string
		wantStatus int
		result     chan deletionHTTPResult
	}
	requests := make([]request, 0, 3*batches)
	for i := range batches {
		requests = append(requests,
			request{
				operation: fmt.Sprintf("POST /ingest %d", i+1), method: http.MethodPost, path: "/ingest",
				payload: `{"content":"admitted during slow deletions ` + uuid.NewString() + `"}`, wantStatus: http.StatusAccepted,
			},
			request{
				operation: fmt.Sprintf("GET /documents %d", i+1), method: http.MethodGet, path: "/documents?limit=100",
				wantStatus: http.StatusOK,
			},
			request{
				operation: fmt.Sprintf("GET document status %d", i+1), method: http.MethodGet, path: "/documents/" + uuid.NewString(),
				wantStatus: http.StatusNotFound,
			},
		)
	}

	// Both guarded deletions are inside a provider call. Other traffic on the
	// same two-connection pool shares one deadline across requests and replies.
	trafficCtx, cancelTraffic := context.WithTimeout(ctx, 5*time.Second)
	defer cancelTraffic()
	for i := range requests {
		request := &requests[i]
		request.result = make(chan deletionHTTPResult, 1)
		startDeletionRequest(request.result, request.operation, func() (httpReply, error) {
			return readHTTPReply(trafficCtx, server, request.method, request.path, "token", nil, request.payload)
		})
	}
	for _, request := range requests {
		reply, err := waitDeletionReply(trafficCtx, request.result, "while the small-pool DELETE providers are paused")
		require.NoError(t, err, request.operation)
		require.NoError(t, checkSmallPoolReply(reply, request.wantStatus), request.operation)
	}
	require.NoError(t, context.Cause(trafficCtx), "small-pool traffic exceeded its deadline")
	cancelTraffic()
	require.Zero(t, c.Pool.Stat().AcquiredConns(), "the guarded deletions hold no pooled connection")

	unblock()
	replyCtx, cancelReply := context.WithTimeout(ctx, 10*time.Second)
	defer cancelReply()
	for range 2 {
		reply, err := waitDeletionReply(replyCtx, replies, "after releasing the small-pool DELETE providers")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, reply.code, reply.body)
	}
}

// pausedMemory pauses the second document-wide delete, the one sent after the
// metadata removal, and leaves every other call to the real in-memory index.
type pausedMemory struct {
	*memory.FakeVectorStore
	fixtureDone  <-chan struct{}
	fixtureCause func() error
	mu           sync.Mutex
	calls        int
	entered      chan struct{}
	release      chan struct{}
}

func newPausedMemory(ctx context.Context, index *memory.FakeVectorStore) (vectors *pausedMemory, release func()) {
	vectors = &pausedMemory{
		FakeVectorStore: index,
		fixtureDone:     ctx.Done(),
		fixtureCause:    func() error { return context.Cause(ctx) },
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	return vectors, sync.OnceFunc(func() { close(vectors.release) })
}

func (v *pausedMemory) DeleteByDocID(ctx context.Context, id string) error {
	v.mu.Lock()
	v.calls++
	call := v.calls
	v.mu.Unlock()
	if call == 2 {
		close(v.entered)
		select {
		case <-v.release:
			if cause := v.fixtureCause(); cause != nil {
				return cause
			}
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
		case <-v.fixtureDone:
			return v.fixtureCause()
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return v.FakeVectorStore.DeleteByDocID(ctx, id)
}

func offlineAPI(t *testing.T, c *pgstore.Client, w *worker.Worker, vectors *pausedMemory) *httptest.Server {
	t.Helper()
	cfg := &config.Config{
		FakeMode: true, ServiceToken: "offline", BedrockFallbackModel: "offline",
		MaxUploadBytes: 1 << 20, MaxChunksPerQuery: 10, WorkerQueueSize: 10, ExtractConcurrency: 1,
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	embeddings := embedder.NewFakeEmbedder(256)
	server := httptest.NewServer(api.NewRouter(&api.RouterDeps{
		Config: cfg, Worker: w, ChunkStore: c, VectorStore: vectors, PineconeDriver: vectors, PGPool: c.Pool,
		Retriever: retriever.NewRetriever(vectors, embeddings, 0, 10), Ambassador: amb, LegacyDeletionReservations: true,
	}))
	t.Cleanup(server.Close)
	return server
}

func TestPausedMemoryDeleteHoldsTheIdentityUntilItsOwnerFinishes(t *testing.T) {
	c := durableDatabase(t)
	second := otherDatabaseClient(t, c)
	index := memory.NewFakeVectorStore()
	e := embedder.NewFakeEmbedder(256)
	w := durableWorker(c, index, e)
	w.Start(t.Context())
	t.Cleanup(w.Shutdown)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	vectors, release := newPausedMemory(ctx, index)
	defer release()
	stale := offlineAPI(t, c, w, vectors)
	// Another API shares the database and the original worker but owns no index.
	otherVectors, releaseOther := newPausedMemory(ctx, memory.NewFakeVectorStore())
	defer releaseOther()
	other := offlineAPI(t, second, w, otherVectors)
	id := uuid.NewString()
	submit := func(server *httptest.Server, text string) httpReply {
		submitCtx, cancelSubmit := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancelSubmit()
		reply, err := readHTTPReply(submitCtx, server, http.MethodPost, "/ingest", "offline", nil, `{"document_id":"`+id+`","content":"`+text+`"}`)
		require.NoError(t, err, "submit ingest for the memory deletion fixture")
		return reply
	}
	reply := submit(stale, "The old policy says blue folders.")
	require.Equal(t, http.StatusAccepted, reply.code, reply.body)
	durableStatus(t, c, id, "completed")

	deleted := make(chan deletionHTTPResult, 1)
	startDeletionRequest(deleted, "offline DELETE "+id, func() (httpReply, error) {
		return readDeleteReply(ctx, stale, "offline", id, "")
	})
	entryCtx, cancelEntry := context.WithTimeout(ctx, 30*time.Second)
	err := waitDeletionArrivals(entryCtx, vectors.entered, deleted, 1, "before the final offline DELETE provider call")
	cancelEntry()
	require.NoError(t, err)
	stateCtx, cancelState := context.WithTimeout(t.Context(), 10*time.Second)
	state, err := readDocumentState(stateCtx, c, id)
	cancelState()
	require.NoError(t, err, "read document state during the final offline DELETE provider call")
	require.Empty(t, state, "metadata is gone while the local delete is outstanding")
	receipt := boundedLegacyReceiptOf(t, c, id)
	require.False(t, receipt.finalized)
	require.False(t, receipt.held, "a clean memory cleanup needs no permanent hold")

	// A concurrent bare caller neither starts a second mutation nor clears the hold.
	duplicateCtx, cancelDuplicate := context.WithTimeout(t.Context(), 10*time.Second)
	duplicate, err := readDeleteReply(duplicateCtx, stale, "offline", id, "")
	cancelDuplicate()
	require.NoError(t, err, "duplicate DELETE while the memory owner is paused")
	require.Equal(t, http.StatusServiceUnavailable, duplicate.code, duplicate.body)
	require.False(t, boundedLegacyReceiptOf(t, c, id).finalized)

	// The guarding session is lost and an operator activates from a second pool.
	require.Equal(t, 1, terminateActivationHolders(t, second))
	activationCtx, cancelActivation := context.WithTimeout(t.Context(), 10*time.Second)
	_, err = second.ActivateDeletion(activationCtx)
	cancelActivation()
	require.NoError(t, err, "activate after the memory owner's session is lost")

	require.Equal(t, http.StatusConflict, submit(stale, "The stale policy says red folders.").code, "the stale service")
	require.Equal(t, http.StatusConflict, submit(other, "The other policy says red folders.").code, "another API")
	require.False(t, boundedLegacyReceiptOf(t, c, id).finalized)

	release()
	replyCtx, cancelReply := context.WithTimeout(ctx, 10*time.Second)
	reply, err = waitDeletionReply(replyCtx, deleted, "after releasing the final offline DELETE provider call")
	cancelReply()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, reply.code, reply.body)
	final := boundedLegacyReceiptOf(t, c, id)
	require.True(t, final.finalized, "the owner recorded the end of its last effect on a fresh connection")
	require.False(t, final.held)

	const newText = "The new policy says green folders."
	reuse := submit(stale, newText)
	require.Equal(t, http.StatusAccepted, reuse.code, "reuse succeeds immediately after a normal completion: "+reuse.body)
	durableStatus(t, c, id, "completed")
	hits, err := index.QueryAllNamespaces(t.Context(), make([]float32, 256), 10, "")
	require.NoError(t, err)
	require.Len(t, hits, 1, "the newly indexed vector survives")
	require.Equal(t, newText, hits[0].Chunk.Text)
	var chunks int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunks WHERE document_id=$1`, id).Scan(&chunks))
	require.Positive(t, chunks)
}

func TestMemoryOwnerCompletesOnlyItsOwnReservation(t *testing.T) {
	c := durableDatabase(t)
	index := memory.NewFakeVectorStore()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	vectors, release := newPausedMemory(ctx, index)
	defer release()
	id := uuid.NewString()
	acceptDeletable(t, c, id, "offline document", false, "")
	routes := deletionRoutes{vectors: vectors, options: []handler.DeleteOption{
		handler.WithLegacyReservations(), handler.WithCleanupTargets(cleanup.Targets{InMemory: true, Memory: index}),
	}}
	owner := deletionServer(t, c, "token", routes)
	// Another process sees an empty local index. It cannot release the reservation.
	foreignIndex := memory.NewFakeVectorStore()
	foreign := deletionServer(t, otherDatabaseClient(t, c), "token", deletionRoutes{vectors: foreignIndex, options: []handler.DeleteOption{
		handler.WithLegacyReservations(), handler.WithCleanupTargets(cleanup.Targets{InMemory: true, Memory: foreignIndex}),
	}})
	deleted := make(chan deletionHTTPResult, 1)
	startDeletionRequest(deleted, "memory owner DELETE "+id, func() (httpReply, error) {
		return readDeleteReply(ctx, owner, "token", id, "")
	})
	entryCtx, cancelEntry := context.WithTimeout(ctx, 30*time.Second)
	err := waitDeletionArrivals(entryCtx, vectors.entered, deleted, 1, "before the final memory owner DELETE provider call")
	cancelEntry()
	require.NoError(t, err)

	foreignCtx, cancelForeign := context.WithTimeout(t.Context(), 10*time.Second)
	reply, err := readDeleteReply(foreignCtx, foreign, "token", id, "")
	cancelForeign()
	require.NoError(t, err, "foreign DELETE while the memory owner is paused")
	require.Equal(t, http.StatusNotFound, reply.code, "the row is gone, and an empty index elsewhere proves nothing")
	require.False(t, boundedLegacyReceiptOf(t, c, id).finalized)
	admissionCtx, cancelAdmission := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelAdmission()
	require.Equal(t, domain.AcceptDeletionConflict, admissionWithContext(admissionCtx, t, c, id, "replacement"))
	cancelAdmission()

	release()
	replyCtx, cancelReply := context.WithTimeout(ctx, 10*time.Second)
	reply, err = waitDeletionReply(replyCtx, deleted, "after releasing the final memory owner DELETE provider call")
	cancelReply()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, reply.code, reply.body)
	require.True(t, boundedLegacyReceiptOf(t, c, id).finalized)
}
