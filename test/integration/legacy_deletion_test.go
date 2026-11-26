package integration

import (
	"context"
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
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func admission(t *testing.T, c *pgstore.Client, id, content string) domain.AcceptOutcome {
	t.Helper()
	result, err := c.AcceptDocument(t.Context(), domain.AcceptRequest{
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
	rows, err := c.Pool.Query(t.Context(), `SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND mode='ShareLock'
		AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
		AND classid::bigint=$1 AND objid::bigint=$2 AND objsubid=1`,
		(pgstore.DeletionActivationLockID>>32)&0xffffffff, pgstore.DeletionActivationLockID&0xffffffff)
	require.NoError(t, err)
	var pids []int32
	for rows.Next() {
		var pid int32
		require.NoError(t, rows.Scan(&pid))
		pids = append(pids, pid)
	}
	require.NoError(t, rows.Err())
	for _, pid := range pids {
		var ended bool
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT pg_terminate_backend($1)`, pid).Scan(&ended))
		require.True(t, ended)
	}
	return len(pids)
}

func holdSiblingActivationLock(t *testing.T, c *pgstore.Client) pgx.Tx {
	t.Helper()
	address := pgtest.Sibling(t, c.Pool.Config().ConnString())
	conn, err := pgx.Connect(t.Context(), address)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	tx, err := conn.Begin(t.Context())
	require.NoError(t, err)
	_, err = tx.Exec(t.Context(), `SELECT pg_advisory_xact_lock_shared($1)`, pgstore.DeletionActivationLockID)
	require.NoError(t, err)
	return tx
}

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

type hostedDeleteResult struct {
	reply httpReply
	err   error
}

func startHostedDelete(ctx context.Context, server *httptest.Server, id string) <-chan hostedDeleteResult {
	results := make(chan hostedDeleteResult, 1)
	go func() {
		reply, err := readHTTPReply(ctx, server, http.MethodDelete, "/documents/"+id, "token", nil, "")
		results <- hostedDeleteResult{reply: reply, err: err}
	}()
	return results
}

func waitForHostedDeleteBarrier(ctx context.Context, entered <-chan struct{}, results <-chan hostedDeleteResult) error {
	select {
	case <-entered:
		return nil
	case result := <-results:
		if result.err != nil {
			return fmt.Errorf("DELETE failed before the final hosted call: %w", result.err)
		}
		return fmt.Errorf("DELETE replied before the final hosted call: HTTP %d", result.reply.code)
	case <-ctx.Done():
		return fmt.Errorf("waiting for the final hosted call: %w", ctx.Err())
	}
}

func waitForHostedDeleteReply(ctx context.Context, results <-chan hostedDeleteResult) (httpReply, error) {
	select {
	case result := <-results:
		if result.err != nil {
			return httpReply{}, fmt.Errorf("DELETE failed after releasing the final hosted call: %w", result.err)
		}
		return result.reply, nil
	case <-ctx.Done():
		return httpReply{}, fmt.Errorf("waiting for DELETE after releasing the final hosted call: %w", ctx.Err())
	}
}

func blockFinalHostedCall(requestCtx context.Context, stores *deletionStores) (entered <-chan struct{}, unblock func()) {
	arrival, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	stores.hook("vectors", func(context.Context) error {
		if calls.Add(1) == 2 {
			close(arrival)
			// The handler detaches cleanup from cancellation of the HTTP request.
			select {
			case <-release:
			case <-requestCtx.Done():
				return requestCtx.Err()
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

	requestCtx, cancelRequest := context.WithCancel(t.Context())
	defer cancelRequest()
	entered, unblock := blockFinalHostedCall(requestCtx, stores)
	defer unblock()
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	results := startHostedDelete(requestCtx, server, id)
	entryCtx, cancelEntry := context.WithTimeout(t.Context(), 30*time.Second)
	err := waitForHostedDeleteBarrier(entryCtx, entered, results)
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
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	defer cancelRequest()
	entered, unblock := blockFinalHostedCall(requestCtx, stores)
	defer unblock()
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	results := startHostedDelete(requestCtx, server, id)
	entryCtx, cancelEntry := context.WithTimeout(t.Context(), 30*time.Second)
	entryErr := waitForHostedDeleteBarrier(entryCtx, entered, results)
	cancelEntry()
	require.NoError(t, entryErr)

	// The row is retired and the last call has not been sent.
	require.Empty(t, documentState(t, c, id))
	activationCtx, cancelActivation := context.WithCancel(t.Context())
	defer cancelActivation()
	activated := make(chan error, 1)
	go func() {
		_, err := activator.ActivateDeletion(activationCtx)
		activated <- err
	}()
	select {
	case err := <-activated:
		require.NoError(t, err, "activation returned while the cleanup held the lock")
		t.Fatal("a healthy activation committed under the cleanup")
	case <-time.After(500 * time.Millisecond):
	}
	require.Equal(t, 1, terminateActivationHolders(t, activator), "only the guarding session ends")
	var alive bool
	require.NoError(t, sibling.QueryRow(t.Context(), `SELECT TRUE`).Scan(&alive), "the sibling session still holds its lock")
	require.True(t, alive)
	select {
	case err := <-activated:
		require.NoError(t, err, "activation after the lock disappeared")
	case <-time.After(10 * time.Second):
		t.Fatal("activation did not proceed after the lock disappeared")
	}
	require.Equal(t, domain.AcceptDeletionConflict, admission(t, activator, id, "replacement"), "the durable hold still blocks the identity")

	unblock()
	replyCtx, cancelReply := context.WithTimeout(t.Context(), 10*time.Second)
	reply, err := waitForHostedDeleteReply(replyCtx, results)
	cancelReply()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, reply.code, reply.body)
	receipt := legacyReceiptOf(t.Context(), t, c, id)
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
func blockFirstCalls(stores *deletionStores, kind string, n int) (entered <-chan struct{}, unblock func()) {
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
			return ctx.Err()
		}
		return nil
	})
	return arrivals, sync.OnceFunc(func() { close(release) })
}

func awaitArrivals(t *testing.T, entered <-chan struct{}, n int) {
	t.Helper()
	for range n {
		select {
		case <-entered:
		case <-time.After(30 * time.Second):
			t.Fatal("a guarded deletion never reached its provider call")
		}
	}
}

func advisoryLocks(t *testing.T, c *pgstore.Client) int {
	t.Helper()
	var n int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted
		AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&n))
	return n
}

func TestLegacyDeletionProbesIgnoreSiblingActivationLock(t *testing.T) {
	ctx := t.Context()
	address := startPostgres(t)
	c, err := pgstore.NewClient(ctx, address)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })

	sibling, err := pgx.Connect(ctx, pgtest.Sibling(t, address))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sibling.Close(context.Background())) })
	_, err = sibling.Exec(ctx, `SELECT pg_advisory_lock_shared($1)`, pgstore.DeletionActivationLockID)
	require.NoError(t, err)

	require.Zero(t, advisoryLocks(t, c))
	require.Zero(t, terminateActivationHolders(t, c))

	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	local, err := pgx.Connect(lockCtx, address)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, local.Close(cleanupCtx))
	})
	_, err = local.Exec(lockCtx, `SELECT pg_advisory_lock_shared($1)`, pgstore.DeletionActivationLockID)
	require.NoError(t, err)
	require.Equal(t, 1, advisoryLocks(t, c), "only the fixture session's activation lock is counted")
	var localUnlocked bool
	require.NoError(t, local.QueryRow(lockCtx, `SELECT pg_advisory_unlock_shared($1)`, pgstore.DeletionActivationLockID).Scan(&localUnlocked))
	require.True(t, localUnlocked, "the fixture session held the activation lock")
	require.Zero(t, advisoryLocks(t, c), "only the sibling session still holds the activation lock")

	var unlocked bool
	require.NoError(t, sibling.QueryRow(ctx, `SELECT pg_advisory_unlock_shared($1)`, pgstore.DeletionActivationLockID).Scan(&unlocked))
	require.True(t, unlocked, "the sibling session still holds the activation lock")
}

func TestGuardedIDOnlyDeleteRejectsBurstWith503BusyBeforeLocks(t *testing.T) {
	c := durableDatabase(t)
	c.SetLegacyDeletionConcurrency(2)
	stores := newDeletionStores()
	ids := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	for _, id := range ids {
		acceptDeletable(t, c, id, "burst document "+id, false, "")
	}
	entered, unblock := blockFirstCalls(stores, "namespace", 2)
	defer unblock()
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	untouched := documentState(t, c, ids[2])
	require.NotEmpty(t, untouched)
	replies := make(chan httpReply, 2)
	for _, id := range ids[:2] {
		go func() { replies <- sendDelete(t, server, "token", id, "") }()
	}
	awaitArrivals(t, entered, 2)
	locks := advisoryLocks(t, c)
	// Other fixtures can take locks on this server between samples.
	holdSiblingActivationLock(t, c)

	refused := sendDelete(t, server, "token", ids[2], "")
	require.Equal(t, http.StatusServiceUnavailable, refused.code, refused.body)
	require.Equal(t, "5", refused.retryAfter)
	require.Contains(t, refused.body, `"busy"`)
	require.Equal(t, 2, stores.count("namespace"), "the refused request made no provider call")
	require.Equal(t, locks, advisoryLocks(t, c), "the refused request took no lock")
	require.Equal(t, untouched, documentState(t, c, ids[2]), "the refused document is untouched")
	var receipts int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM document_deletions WHERE document_id=$1`, ids[2]).Scan(&receipts))
	require.Zero(t, receipts, "the refused request reserved nothing")

	unblock()
	for range 2 {
		require.Equal(t, http.StatusOK, (<-replies).code)
	}
	require.Equal(t, http.StatusOK, sendDelete(t, server, "token", ids[2], "").code, "a retry succeeds once a slot is free")
}

func TestSlowGuardedDeletionsDoNotExhaustSmallPool(t *testing.T) {
	c := smallPoolDatabase(t, 2)
	c.SetLegacyDeletionConcurrency(2)
	stores := newDeletionStores()
	ids := []string{uuid.NewString(), uuid.NewString()}
	for _, id := range ids {
		acceptDeletable(t, c, id, "slow document "+id, false, "")
	}
	entered, unblock := blockFirstCalls(stores, "namespace", 2)
	defer unblock()
	server := deletionServer(t, c, "token", legacyRoutes(stores))
	replies := make(chan httpReply, 2)
	for _, id := range ids {
		go func() { replies <- sendDelete(t, server, "token", id, "") }()
	}
	awaitArrivals(t, entered, 2)

	// Both guarded deletions are inside a provider call. Other traffic on the
	// same two-connection pool finishes well inside its own deadline.
	started := time.Now()
	var wg sync.WaitGroup
	results := make(chan httpReply, 6)
	for range 2 {
		wg.Go(func() {
			results <- doRequest(t, server, http.MethodPost, "/ingest", "token", nil, `{"content":"admitted during slow deletions `+uuid.NewString()+`"}`)
		})
		wg.Go(func() { results <- doRequest(t, server, http.MethodGet, "/documents", "token", nil, "") })
		wg.Go(func() {
			results <- doRequest(t, server, http.MethodGet, "/documents/"+uuid.NewString(), "token", nil, "")
		})
	}
	wg.Wait()
	close(results)
	for reply := range results {
		require.Less(t, reply.code, http.StatusInternalServerError, reply.body)
		require.NotEqual(t, http.StatusServiceUnavailable, reply.code, reply.body)
	}
	require.Less(t, time.Since(started), 5*time.Second)
	require.Zero(t, c.Pool.Stat().AcquiredConns(), "the guarded deletions hold no pooled connection")

	unblock()
	for range 2 {
		require.Equal(t, http.StatusOK, (<-replies).code)
	}
}

// pausedMemory pauses the second document-wide delete, the one sent after the
// metadata removal, and leaves every other call to the real in-memory index.
type pausedMemory struct {
	*memory.FakeVectorStore
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (v *pausedMemory) DeleteByDocID(ctx context.Context, id string) error {
	v.mu.Lock()
	v.calls++
	call := v.calls
	v.mu.Unlock()
	if call == 2 {
		close(v.entered)
		<-v.release
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
	vectors := &pausedMemory{FakeVectorStore: index, entered: make(chan struct{}), release: make(chan struct{})}
	stale := offlineAPI(t, c, w, vectors)
	// Another API shares the database and the original worker but owns no index.
	other := offlineAPI(t, second, w, &pausedMemory{FakeVectorStore: memory.NewFakeVectorStore(), entered: make(chan struct{}), release: make(chan struct{})})
	id := uuid.NewString()
	submit := func(server *httptest.Server, text string) httpReply {
		return doRequest(t, server, http.MethodPost, "/ingest", "offline", nil, `{"document_id":"`+id+`","content":"`+text+`"}`)
	}
	reply := submit(stale, "The old policy says blue folders.")
	require.Equal(t, http.StatusAccepted, reply.code, reply.body)
	durableStatus(t, c, id, "completed")

	deleted := make(chan httpReply, 1)
	go func() { deleted <- sendDelete(t, stale, "offline", id, "") }()
	<-vectors.entered
	require.Empty(t, documentState(t, c, id), "metadata is gone while the local delete is outstanding")
	receipt := legacyReceiptOf(t.Context(), t, c, id)
	require.False(t, receipt.finalized)
	require.False(t, receipt.held, "a clean memory cleanup needs no permanent hold")

	// A concurrent bare caller neither starts a second mutation nor clears the hold.
	require.Equal(t, http.StatusServiceUnavailable, sendDelete(t, stale, "offline", id, "").code)
	require.False(t, legacyReceiptOf(t.Context(), t, c, id).finalized)

	// The guarding session is lost and an operator activates from a second pool.
	require.Equal(t, 1, terminateActivationHolders(t, second))
	require.Eventually(t, func() bool {
		_, err := second.ActivateDeletion(t.Context())
		return err == nil
	}, 10*time.Second, 25*time.Millisecond)

	require.Equal(t, http.StatusConflict, submit(stale, "The stale policy says red folders.").code, "the stale service")
	require.Equal(t, http.StatusConflict, submit(other, "The other policy says red folders.").code, "another API")
	require.False(t, legacyReceiptOf(t.Context(), t, c, id).finalized)

	close(vectors.release)
	require.Equal(t, http.StatusOK, (<-deleted).code)
	final := legacyReceiptOf(t.Context(), t, c, id)
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
	vectors := &pausedMemory{FakeVectorStore: index, entered: make(chan struct{}), release: make(chan struct{})}
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
	deleted := make(chan httpReply, 1)
	go func() { deleted <- sendDelete(t, owner, "token", id, "") }()
	<-vectors.entered

	require.Equal(t, http.StatusNotFound, sendDelete(t, foreign, "token", id, "").code, "the row is gone, and an empty index elsewhere proves nothing")
	require.False(t, legacyReceiptOf(t.Context(), t, c, id).finalized)
	require.Equal(t, domain.AcceptDeletionConflict, admission(t, c, id, "replacement"))

	close(vectors.release)
	require.Equal(t, http.StatusOK, (<-deleted).code)
	require.True(t, legacyReceiptOf(t.Context(), t, c, id).finalized)
}
