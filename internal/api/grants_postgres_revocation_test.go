package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/grantse2e"
	"github.com/stretchr/testify/require"
)

func TestGrantsPostgresReplicasObserveCommittedRevocation(t *testing.T) {
	for _, backend := range grantse2e.Backends {
		t.Run(backend, func(t *testing.T) {
			f := grantse2e.New(t, backend, (*pgstore.Client).ActivateDeletion)
			docs := f.Seed(t)
			actor := f.Revocable
			replicaB := f.Client(t, 1, actor)
			page, err := replicaB.ListDocumentsWithIncarnation(t.Context(), client.ListOptions{Limit: 1})
			require.NoError(t, err)
			require.Len(t, page.Documents, 1)
			require.NotNil(t, page.NextAfter)
			cursor := *page.NextAfter
			require.Equal(t, page.Documents[0].ID, cursor)
			target, retained := docs.Second, docs.First
			if cursor == docs.Second.ID {
				target, retained = docs.First, docs.Second
			}
			next := client.ListOptions{Limit: 2, After: cursor}
			requirePage(t, replicaB, next, target.ID)
			signed, err := replicaB.DocumentFileURL(t.Context(), target.ID)
			require.NoError(t, err)
			require.Contains(t, signed.URL, target.ID)
			_, err = replicaB.DeleteDocumentIncarnation(t.Context(), target.ID, target.Incarnation)
			requireAPIError(t, err, http.StatusNotFound, "not_found")

			t.Run("grant removal", func(t *testing.T) {
				holdStream(t, f, replicaB, target, func() { f.Grant(t, actor, false, false, target) })
				requirePage(t, replicaB, next)
				requirePage(t, replicaB, client.ListOptions{}, retained.ID)
				_, err := replicaB.DocumentFileURL(t.Context(), target.ID)
				requireAPIError(t, err, http.StatusNotFound, "not_found")
				f.Grant(t, actor, true, true, target)
				requirePage(t, replicaB, next, target.ID)
			})

			t.Run("principal disable with signing in flight", func(t *testing.T) {
				holdStream(t, f, replicaB, target, func() { disableDuringSigning(t, f, replicaB, target) })
				_, err := replicaB.ListDocuments(t.Context(), next)
				requireAPIError(t, err, http.StatusForbidden, "forbidden")
				_, err = replicaB.ListDocuments(t.Context(), client.ListOptions{})
				requireAPIError(t, err, http.StatusForbidden, "forbidden")
				_, err = replicaB.DocumentFileURL(t.Context(), target.ID)
				requireAPIError(t, err, http.StatusForbidden, "forbidden")
				resp, err := f.Client(t, 1, f.Reader).Query(t.Context(), grantse2e.Query("global", client.AnswerModeEvidence))
				require.NoError(t, err)
				requireEvidence(t, resp.ScoredChunks, docs.First)
				file, err := f.Client(t, 1, f.Owner).DocumentFileURL(t.Context(), target.ID)
				require.NoError(t, err)
				require.Contains(t, file.URL, target.ID)
			})
		})
	}
}

func holdStream(t *testing.T, f *grantse2e.Fixture, c *client.Client, doc grantse2e.Document, commit func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	gate := f.Model.PauseNextStream()
	result := make(chan error, 1)
	contextFrame := make(chan client.QueryStreamContext, 1)
	var events []string
	go func() {
		req := grantse2e.Query("global", client.AnswerModeGenerated)
		req.AllowedDocumentIDs = []string{doc.ID}
		result <- c.QueryStream(ctx, req, func(event client.Event) error {
			events = append(events, event.Type)
			if event.Type == "context" {
				var frame client.QueryStreamContext
				if err := json.Unmarshal(event.Data, &frame); err != nil {
					return err
				}
				contextFrame <- frame
			}
			return nil
		})
	}()
	select {
	case frame := <-contextFrame:
		requireEvidence(t, frame.ScoredChunks, doc)
	case err := <-result:
		t.Fatalf("stream ended before context: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-gate.Entered:
	case err := <-result:
		t.Fatalf("stream ended before provider barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	commit()
	close(gate.Release)
	select {
	case err := <-result:
		var streamErr *client.StreamError
		require.ErrorAs(t, err, &streamErr)
		require.Equal(t, "forbidden", streamErr.Code)
		require.Equal(t, "access denied", streamErr.Message)
		require.Equal(t, []string{"context", "error"}, events)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func disableDuringSigning(t *testing.T, f *grantse2e.Fixture, c *client.Client, doc grantse2e.Document) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	// Hold the signing transaction after the signer and principal share lock
	// have succeeded, immediately before the allow audit can be committed.
	f.Exec(t, `CREATE FUNCTION hold_signing_audit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_advisory_xact_lock(283, 1); RETURN NEW; END; $$;
		CREATE TRIGGER hold_signing_audit BEFORE INSERT ON grant_catalog_security_audit
		FOR EACH ROW WHEN (NEW.action='file_sign' AND NEW.outcome='allow')
		EXECUTE FUNCTION hold_signing_audit()`)
	defer f.Exec(t, `DROP TRIGGER hold_signing_audit ON grant_catalog_security_audit; DROP FUNCTION hold_signing_audit()`)
	conn, err := f.Admin.Pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(283, 1)")
	require.NoError(t, err)

	type signResult struct {
		file *client.FileURL
		err  error
	}
	signed := make(chan signResult, 1)
	go func() {
		file, err := c.DocumentFileURL(ctx, doc.ID)
		signed <- signResult{file: file, err: err}
	}()
	var signPID int32
	require.Eventually(t, func() bool {
		err := f.Admin.Pool.QueryRow(ctx, `SELECT pid FROM pg_locks WHERE database = (SELECT oid FROM pg_database WHERE datname=current_database())
			AND locktype='advisory' AND classid=283 AND objid=1 AND objsubid=2 AND NOT granted`).Scan(&signPID)
		return err == nil
	}, 3*time.Second, 15*time.Millisecond, "signing transaction did not reach the audit gate")

	batch := f.DisableBatch(t, f.Revocable)
	type importResult struct {
		value pgstore.GrantImportResult
		err   error
	}
	imported := make(chan importResult, 1)
	go func() {
		value, err := f.Operator.ImportGrantBatch(ctx, bytes.NewReader(batch), false)
		imported <- importResult{value: value, err: err}
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		err := f.Admin.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database()
			AND usename=$1 AND $2=ANY(pg_blocking_pids(pid)))`, f.OperatorLogin, signPID).Scan(&blocked)
		return err == nil && blocked
	}, 3*time.Second, 15*time.Millisecond, "operator disable did not wait for signing")
	require.NoError(t, tx.Commit(ctx))
	select {
	case result := <-signed:
		require.NoError(t, result.err)
		require.Contains(t, result.file.URL, doc.ID)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case result := <-imported:
		require.NoError(t, result.err)
		require.Equal(t, f.Revision+1, result.value.Revision)
		f.Revision = result.value.Revision
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
