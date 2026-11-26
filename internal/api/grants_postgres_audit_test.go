package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/testutil/grantse2e"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestGrantsPostgresAuditAndRedaction(t *testing.T) {
	for _, backend := range grantse2e.Backends {
		t.Run(backend, func(t *testing.T) {
			var logs *lockedGrantsLog
			var spans *tracetest.SpanRecorder
			f := grantse2e.New(t, backend, func() { logs, spans = captureGrantsTelemetry(t) })
			docs := f.Seed(t)
			path := "/documents/" + docs.First.ID
			body, err := json.Marshal(grantse2e.Query("global", client.AnswerModeGenerated))
			require.NoError(t, err)
			var objectKey string
			require.NoError(t, f.Admin.Pool.QueryRow(t.Context(), "SELECT s3_key FROM documents WHERE id=$1", docs.First.ID).Scan(&objectKey))
			var chunkID string
			require.NoError(t, f.Admin.Pool.QueryRow(t.Context(), "SELECT id::text FROM chunks WHERE document_id=$1", docs.First.ID).Scan(&chunkID))
			signer := f.Client(t, 1, f.Owner)
			signed, err := signer.DocumentFileURL(t.Context(), docs.First.ID)
			require.NoError(t, err)

			for _, route := range []struct {
				action, method, path string
				actor                grantse2e.Actor
				body                 []byte
			}{
				{"document_list", http.MethodGet, "/documents?limit=2", f.Reader, nil},
				{"document_status", http.MethodGet, path, f.Reader, nil},
				{"query", http.MethodPost, "/query", f.Reader, body},
				{"file_sign", http.MethodGet, path + "/file", f.Owner, nil},
			} {
				resp := f.Raw(t, 1, route.method, route.path, f.ServiceToken, route.actor.Credential.Token, route.body)
				require.Equal(t, http.StatusOK, resp.Status, "%s", resp.Body)
				requireAudit(t, f, resp.Header.Get("X-Request-ID"), route.action, "allow", route.actor, f.Revision)
				t.Run(route.action+" fails closed", func(t *testing.T) {
					rejectAudit(t, f, route.action)
					failed := f.Raw(t, 1, route.method, route.path, f.ServiceToken, route.actor.Credential.Token, route.body)
					if route.action == "file_sign" {
						grantse2e.RequireDenied(t, failed, http.StatusInternalServerError, "internal", "failed to look up document")
					} else {
						grantse2e.RequireDenied(t, failed, http.StatusServiceUnavailable, "unavailable", "audit logging unavailable")
					}
					requireNoAllowAudit(t, f, failed.Header.Get("X-Request-ID"), route.action)
				})
			}

			t.Run("ingest rolls back", func(t *testing.T) {
				rejectAudit(t, f, "ingest")
				before := documentAndJobCounts(t, f)
				resp := f.Raw(t, 0, http.MethodPost, "/ingest", f.ServiceToken, f.Owner.Credential.Token,
					[]byte(`{"content":"Synthetic content that must remain uncommitted."}`))
				grantse2e.RequireDenied(t, resp, http.StatusServiceUnavailable, "unavailable", "document store unavailable")
				require.Equal(t, before, documentAndJobCounts(t, f))
				requireNoAllowAudit(t, f, resp.Header.Get("X-Request-ID"), "ingest")
			})

			t.Run("delete rolls back", func(t *testing.T) {
				rejectAudit(t, f, "document_delete")
				resp := f.Raw(t, 0, http.MethodDelete, path, f.ServiceToken, f.Owner.Credential.Token, nil, incarnation(docs.First))
				grantse2e.RequireDenied(t, resp, http.StatusInternalServerError, "internal", "failed to delete document")
				var state string
				var intent bool
				require.NoError(t, f.Admin.Pool.QueryRow(t.Context(), `SELECT status,
					EXISTS(SELECT 1 FROM document_deletions WHERE document_id=$1)
					FROM documents WHERE id=$1`, docs.First.ID).Scan(&state, &intent))
				require.Equal(t, "completed", state)
				require.False(t, intent)
				f.RequireStoredVectors(t, docs.First, true)
				requireNoAllowAudit(t, f, resp.Header.Get("X-Request-ID"), "document_delete")
			})

			ingested := f.Raw(t, 0, http.MethodPost, "/ingest", f.ServiceToken, f.Owner.Credential.Token,
				[]byte(`{"content":"Committed synthetic audit document."}`))
			require.Equal(t, http.StatusAccepted, ingested.Status, "%s", ingested.Body)
			requireAudit(t, f, ingested.Header.Get("X-Request-ID"), "ingest", "allow", f.Owner, f.Revision)
			var accepted client.IngestResponse
			require.NoError(t, json.Unmarshal(ingested.Body, &accepted))
			f.Wait(t, f.Owner, 0, accepted.DocumentID, "Committed synthetic audit document.")
			deleted := f.Raw(t, 0, http.MethodDelete, path, f.ServiceToken, f.Owner.Credential.Token, nil, incarnation(docs.First))
			require.Equal(t, http.StatusOK, deleted.Status, "%s", deleted.Body)
			requireAudit(t, f, deleted.Header.Get("X-Request-ID"), "document_delete", "allow", f.Owner, f.Revision)
			auth := f.Raw(t, 1, http.MethodGet, "/documents", f.ServiceToken, "", nil)
			require.Equal(t, http.StatusUnauthorized, auth.Status)
			requireAudit(t, f, auth.Header.Get("X-Request-ID"), "subject_auth", "deny", grantse2e.Actor{}, f.Revision+1)

			for _, row := range []struct {
				action, operator string
				revision         int64
			}{
				{"grant_import", f.OperatorLogin, 1},
				{"corpus_cutover", f.OperatorLogin, 2},
				{"trust_key_load", "", 1},
			} {
				var id int64
				require.NoError(t, f.Auditor.Pool.QueryRow(t.Context(), `SELECT audit_id FROM grant_catalog_security_audit
					WHERE request_id='' AND outcome='allow' AND action=$1 AND COALESCE(operator::text,'')=$2
					AND policy_revision=$3 ORDER BY audit_id LIMIT 1`, row.action, row.operator, row.revision).Scan(&id))
				require.Positive(t, id)
			}
			f.StopReplicas(t)
			requireGrantsTelemetry(t, f, docs, logs, spans, objectKey, chunkID, signed.URL)
		})
	}
}

func incarnation(doc grantse2e.Document) client.RequestOption {
	return func(req *http.Request) { req.Header.Set("X-Document-Incarnation", doc.Incarnation) }
}

func rejectAudit(t *testing.T, f *grantse2e.Fixture, action string) {
	t.Helper()
	f.Exec(t, `CREATE FUNCTION reject_fixture_audit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'fixture audit interruption'; END; $$;
		CREATE TRIGGER reject_fixture_audit BEFORE INSERT ON grant_catalog_security_audit
		FOR EACH ROW WHEN (NEW.action='`+action+`' AND NEW.outcome='allow') EXECUTE FUNCTION reject_fixture_audit()`)
	t.Cleanup(func() {
		f.Exec(t, `DROP TRIGGER reject_fixture_audit ON grant_catalog_security_audit; DROP FUNCTION reject_fixture_audit()`)
	})
}

func documentAndJobCounts(t *testing.T, f *grantse2e.Fixture) [2]int {
	t.Helper()
	var counts [2]int
	require.NoError(t, f.Admin.Pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM documents),
		(SELECT count(*) FROM ingest_jobs)`).Scan(&counts[0], &counts[1]))
	return counts
}

func requireAudit(t *testing.T, f *grantse2e.Fixture, requestID, action, outcome string, actor grantse2e.Actor, revision int64) {
	t.Helper()
	require.NotEmpty(t, requestID)
	var id, rev int64
	var tenant, principal, reason string
	require.NoError(t, f.Auditor.Pool.QueryRow(t.Context(), `SELECT audit_id, policy_revision,
		COALESCE(tenant_id::text,''), COALESCE(principal_id::text,''), reason
		FROM grant_catalog_security_audit WHERE request_id=$1 AND action=$2 AND outcome=$3`,
		requestID, action, outcome).Scan(&id, &rev, &tenant, &principal, &reason))
	require.Positive(t, id)
	require.Equal(t, revision, rev)
	require.Equal(t, actor.Tenant, tenant)
	require.Equal(t, actor.ID, principal)
	require.NotEmpty(t, reason)
}

func requireNoAllowAudit(t *testing.T, f *grantse2e.Fixture, requestID, action string) {
	t.Helper()
	require.NotEmpty(t, requestID)
	var found bool
	require.NoError(t, f.Auditor.Pool.QueryRow(t.Context(), `SELECT EXISTS(
		SELECT 1 FROM grant_catalog_security_audit WHERE request_id=$1 AND action=$2 AND outcome='allow')`,
		requestID, action).Scan(&found))
	require.False(t, found)
}

type lockedGrantsLog struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *lockedGrantsLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.Write(p)
}

func (b *lockedGrantsLog) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.String()
}

func captureGrantsTelemetry(t *testing.T) (*lockedGrantsLog, *tracetest.SpanRecorder) {
	t.Helper()
	logs := &lockedGrantsLog{}
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	t.Cleanup(func() { slog.SetDefault(oldLogger) })
	oldTracer := otel.GetTracerProvider()
	spans := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		defer otel.SetTracerProvider(oldTracer)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		require.NoError(t, provider.Shutdown(ctx))
	})
	return logs, spans
}

func requireGrantsTelemetry(t *testing.T, f *grantse2e.Fixture, docs grantse2e.Documents, logs *lockedGrantsLog, spans *tracetest.SpanRecorder, secrets ...string) {
	t.Helper()
	secrets = append(secrets, grantse2e.Prompt, grantse2e.Answer, grantse2e.AccessKey, grantse2e.SecretKey, f.ServiceToken, f.LegacyID)
	for _, actor := range []grantse2e.Actor{f.Owner, f.Reader, f.Other, f.Downloader, f.Revocable, f.Foreign} {
		secrets = append(secrets, actor.Tenant, actor.ID, actor.Subject, actor.Credential.Token)
	}
	for _, doc := range []grantse2e.Document{docs.First, docs.Second, docs.Text, docs.Foreign} {
		secrets = append(secrets, doc.ID, doc.Incarnation, doc.Text)
		if doc.FileName != nil {
			secrets = append(secrets, *doc.FileName)
		}
	}
	var output bytes.Buffer
	log := logs.String()
	require.Contains(t, log, `"method":"GET","path":"/documents/{doc_id}"`)
	output.WriteString(log)
	ended := spans.Ended()
	require.NotEmpty(t, ended)
	for _, span := range ended {
		_, err := fmt.Fprintln(&output, span.Name(), span.Status(), span.Attributes(), span.Events())
		require.NoError(t, err)
	}
	var series bytes.Buffer
	for _, registry := range f.Metrics {
		require.NoError(t, registry.Registry.WriteText(&series))
	}
	for _, label := range []string{
		`quarry_http_requests_total{route="GET /documents/{doc_id}",code="200"}`,
		`quarry_ingest_jobs_total{result="completed"}`, `quarry_query_mode_total{mode="GLOBAL"}`,
	} {
		require.Contains(t, series.String(), label)
	}
	output.Write(series.Bytes())
	for _, secret := range secrets {
		require.NotEmpty(t, secret)
		require.NotContains(t, output.String(), secret)
	}
}
