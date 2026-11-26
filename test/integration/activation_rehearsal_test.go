package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const rehearsalToken = "rehearsal-token"

// rehearsalService is the document routes and the mode check on one database
// connection pool, as one replica of the current build serves them.
func rehearsalService(t *testing.T, c *pgstore.Client, stores *deletionStores) *httptest.Server {
	t.Helper()
	mux := deletionMux(c, rehearsalToken, legacyRoutes(stores))
	mux.HandleFunc("GET /deletion-mode", middleware.RequireServiceTokens([]string{rehearsalToken}, handler.DeletionModeHandler(c)))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func deletionModeOf(t *testing.T, server *httptest.Server) string {
	t.Helper()
	reply := doRequest(t, server, http.MethodGet, "/deletion-mode", rehearsalToken, nil, "")
	require.Equal(t, http.StatusOK, reply.code, reply.body)
	var body handler.DeletionModeResponse
	require.NoError(t, json.Unmarshal([]byte(reply.body), &body))
	return body.DeletionMode
}

func activateWithCommand(t *testing.T, bin, dbURL string) domain.DeletionActivation {
	t.Helper()
	out, err := runCleanupCLI(t, bin, dbURL, "postgres", "", "--activate-deletion")
	require.NoError(t, err, string(out))
	var activation domain.DeletionActivation
	require.NoError(t, json.Unmarshal(out, &activation), string(out))
	return activation
}

// TestActivationRehearsalAcrossOldAndNewExecutables walks one database through
// schema 009, the expanded inactive schema, explicit activation and a restart,
// with separately built old and current executables.
func TestActivationRehearsalAcrossOldAndNewExecutables(t *testing.T) {
	dbURL := pgtest.StartVector(t)
	preUpgradeBin := buildPreUpgradeRouter(t)
	preUpgradeCleanupBin := buildPreUpgradeCleanup(t)
	cleanupBin := buildCleanupBinary(t)
	ctx := t.Context()
	db, err := pgstore.NewClient(ctx, dbURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	applyPreDeletionMigrations(t, db)

	seed := func(content string) string {
		id := uuid.NewString()
		_, execErr := db.Pool.Exec(ctx, `INSERT INTO documents(id,content,status) VALUES($1,$2,'completed')`, id, content)
		require.NoError(t, execErr)
		return id
	}
	preUpgradeOnly, bareDocument, qualifiedDocument := seed("removed by the old executable"), seed("removed by a bare request"), seed("removed by a qualified request")

	// Schema 009: the old executable deletes by ID.
	runPreUpgradeDelete(t, preUpgradeBin, dbURL, preUpgradeOnly, http.StatusOK)
	require.Empty(t, documentState(t, db, preUpgradeOnly))

	// Expanded and inactive: nothing about the contract changed.
	require.NoError(t, db.RunMigrations(ctx))
	_, err = pgstore.NewVectorStore(ctx, db, 3)
	require.NoError(t, err)
	stores := newDeletionStores()
	stale := rehearsalService(t, db, stores)
	require.Equal(t, handler.DeletionModeInactive, deletionModeOf(t, stale))
	require.Equal(t, http.StatusPreconditionFailed, sendDelete(t, stale, rehearsalToken, qualifiedDocument, qualifiedDocument).code,
		"a qualified request cannot reach an inactive database")
	require.Empty(t, stores.all())
	require.Equal(t, "completed", documentState(t, db, qualifiedDocument))
	require.Equal(t, http.StatusOK, sendDelete(t, stale, rehearsalToken, bareDocument, "").code)
	require.Empty(t, documentState(t, db, bareDocument))
	require.Equal(t, http.StatusNotFound, sendDelete(t, stale, rehearsalToken, bareDocument, "").code)
	inactiveCalls := stores.all()

	// Explicit activation, twice, with local settings that differ between the
	// two runs. The same instant comes back and the marker never moves.
	t.Setenv("RETRYABLE_DELETION", "true")
	first := activateWithCommand(t, cleanupBin, dbURL)
	require.False(t, first.AlreadyActive)
	t.Setenv("RETRYABLE_DELETION", "false")
	time.Sleep(20 * time.Millisecond)
	second := activateWithCommand(t, cleanupBin, dbURL)
	require.True(t, second.AlreadyActive)
	require.True(t, second.ActivatedAt.Equal(first.ActivatedAt))
	var marker time.Time
	require.NoError(t, db.Pool.QueryRow(ctx, `SELECT activated_at FROM document_deletion_activation`).Scan(&marker))
	require.True(t, marker.Equal(first.ActivatedAt))

	// A stale replica and a restarted one read the marker from the database.
	restarted := rehearsalService(t, otherDatabaseClient(t, db), stores)
	for name, server := range map[string]*httptest.Server{"stale replica": stale, "restarted replica": restarted} {
		require.Equal(t, handler.DeletionModeActive, deletionModeOf(t, server), name)
		require.Equal(t, http.StatusPreconditionRequired, sendDelete(t, server, rehearsalToken, qualifiedDocument, "").code, name+": a bare live request")
		require.Equal(t, http.StatusPreconditionFailed, sendDelete(t, server, rehearsalToken, qualifiedDocument, uuid.NewString()).code, name+": the wrong incarnation")
	}
	require.Equal(t, inactiveCalls, stores.all(), "a refused request touches no store")
	require.Equal(t, "completed", documentState(t, db, qualifiedDocument))
	require.Equal(t, http.StatusOK, sendDelete(t, restarted, rehearsalToken, qualifiedDocument, storedIncarnation(t, db, qualifiedDocument)).code)
	require.Empty(t, documentState(t, db, qualifiedDocument))

	// The old router and cleanup command each bypass the marker on their own
	// sacrificial row.
	sacrificial := uuid.NewString()
	acceptDeletable(t, db, sacrificial, "removed by the old router", false, "")
	runPreUpgradeDelete(t, preUpgradeBin, dbURL, sacrificial, http.StatusOK)
	require.Empty(t, documentState(t, db, sacrificial), "the old router bypasses the boundary")

	sacrificial = uuid.NewString()
	acceptDeletable(t, db, sacrificial, "removed by the old cleanup command", false, "")
	out, err := runCleanupCLI(t, preUpgradeCleanupBin, dbURL, "postgres", "", sacrificial)
	require.NoError(t, err, string(out))
	require.Empty(t, documentState(t, db, sacrificial), "the old cleanup command bypasses the boundary")
	require.Equal(t, handler.DeletionModeActive, deletionModeOf(t, restarted))

	// The deletion-aware build preserves the row and sends no source or vector
	// deletion when a bare request reaches it after activation.
	protected := uuid.NewString()
	key := "source/" + protected + "/original"
	acceptDeletable(t, db, protected, "kept by the activated boundary", false, key)
	stores = newDeletionStores()
	rolledBack := rehearsalService(t, otherDatabaseClient(t, db), stores)
	require.Equal(t, http.StatusPreconditionRequired, sendDelete(t, rolledBack, rehearsalToken, protected, "").code)
	require.Equal(t, "processing", documentState(t, db, protected))
	require.Empty(t, stores.all())
}
