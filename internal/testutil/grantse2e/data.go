package grantse2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Document pairs the public identity and completed lifetime with its input.
type Document struct {
	client.DocumentIncarnationSummary
	Text string
}

// Documents holds the four seeded documents and their completed lifetimes.
type Documents struct {
	First, Second, Text, Foreign Document
}

func (f *Fixture) principal(actor Actor, enabled bool) map[string]any {
	return map[string]any{"tenant_id": actor.Tenant, "principal_id": actor.ID,
		"issuer": f.issuer, "subject": actor.Subject, "enabled": enabled}
}

func (f *Fixture) provision(t *testing.T) {
	t.Helper()
	var tenants, principals, collections, permissions []map[string]any
	for _, owner := range []Actor{f.Owner, f.Foreign} {
		tenants = append(tenants, map[string]any{"tenant_id": owner.Tenant})
		for _, collection := range []string{"", Collection} {
			collections = append(collections, map[string]any{"tenant_id": owner.Tenant, "collection": collection})
			permissions = append(permissions, map[string]any{
				"tenant_id": owner.Tenant, "principal_id": owner.ID, "collection": collection, "can_create": true,
			})
		}
	}
	for _, actor := range []Actor{f.Owner, f.Reader, f.Other, f.Downloader, f.Revocable, f.Foreign} {
		principals = append(principals, f.principal(actor, true))
	}
	f.Import(t, "tenants", tenants, "principals", principals, "collections", collections, "create_permissions", permissions)
}

// Import submits an operator batch and carries forward the committed revision.
func (f *Fixture) Import(t *testing.T, group string, entries []map[string]any, more ...any) {
	t.Helper()
	batch := map[string]any{"batch_id": uuid.NewString(), "expected_revision": f.Revision, group: entries}
	for i := 0; i < len(more); i += 2 {
		batch[more[i].(string)] = more[i+1]
	}
	body, err := json.Marshal(batch)
	require.NoError(t, err)
	result, err := f.Operator.ImportGrantBatch(t.Context(), bytes.NewReader(body), false)
	require.NoError(t, err)
	require.Equal(t, f.Revision+1, result.Revision)
	require.False(t, result.Replayed)
	f.Revision = result.Revision
}

// Grant changes read/download access to the specified completed incarnations.
func (f *Fixture) Grant(t *testing.T, actor Actor, read, download bool, docs ...Document) {
	t.Helper()
	var entries []map[string]any
	for _, doc := range docs {
		entries = append(entries, map[string]any{"tenant_id": actor.Tenant, "principal_id": actor.ID,
			"document_id": doc.ID, "incarnation": doc.Incarnation, "can_read": read, "can_download": download})
	}
	f.Import(t, "grants", entries)
}

// DisableBatch creates a principal update without doing database work.
func (f *Fixture) DisableBatch(t *testing.T, actor Actor) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"batch_id": uuid.NewString(), "expected_revision": f.Revision,
		"principals": []map[string]any{f.principal(actor, false)},
	})
	require.NoError(t, err)
	return body
}

// Seed sends real JSON and multipart requests, waits for both worker pools,
// and grants scoped access only after the completed lifetimes are known.
func (f *Fixture) Seed(t *testing.T) Documents {
	t.Helper()
	first := f.Ingest(t, f.Owner, 0, Collection, Filename, "Amber archive records are kept in the northern cabinet.")
	distractor := "Obsidian archive records are kept in the southern cabinet."
	second := f.Ingest(t, f.Owner, 0, Collection, "other-"+Filename, distractor)
	text := f.Ingest(t, f.Owner, 0, "", "", "Cerulean default collection records use a different filing cabinet.")
	foreign := f.Ingest(t, f.Foreign, 1, Collection, "", distractor)
	f.Grant(t, f.Reader, true, false, first)
	f.Grant(t, f.Downloader, true, true, first)
	f.Grant(t, f.Revocable, true, true, first, second)
	return Documents{First: first, Second: second, Text: text, Foreign: foreign}
}

// Ingest submits a document and waits for the status and queue to agree.
func (f *Fixture) Ingest(t *testing.T, actor Actor, replica int, collection, filename, text string) Document {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	c := f.Client(t, replica, actor)
	meta := map[string]any{"title": "Archive records"}
	var response *client.IngestResponse
	var err error
	switch {
	case filename != "":
		response, err = c.IngestFile(ctx, client.FileUpload{Reader: strings.NewReader(text),
			Filename: filename, DocType: FileType, Collection: collection, Metadata: meta})
	default:
		response, err = c.Ingest(ctx, client.IngestRequest{Content: text, DocType: TextType, Collection: collection, Metadata: meta})
	}
	require.NoError(t, err)
	require.Equal(t, "processing", response.Status)
	_, err = uuid.Parse(response.DocumentID)
	require.NoError(t, err)
	return f.Wait(t, actor, replica, response.DocumentID, text)
}

// Wait requires a committed completion with no remaining job.
func (f *Fixture) Wait(t *testing.T, actor Actor, replica int, id, input string) Document {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	c := f.Client(t, replica, actor)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var state string
	var lastError *string
	var pending, completed bool
	for {
		err := f.Admin.Pool.QueryRow(ctx, `SELECT status, last_error, completed_at IS NOT NULL,
			EXISTS (SELECT FROM ingest_jobs WHERE document_id=documents.id) FROM documents WHERE id=$1`, id).
			Scan(&state, &lastError, &completed, &pending)
		require.NoError(t, err, "document %s, last state %q, pending %v", id, state, pending)
		require.NotEqual(t, "failed", state, "document %s failed: %v", id, lastError)
		if state == "completed" && completed && !pending {
			status, err := c.GetDocumentWithIncarnation(ctx, id)
			require.NoError(t, err)
			require.Equal(t, id, status.ID)
			require.Equal(t, "completed", status.Status)
			inc, err := uuid.Parse(status.Incarnation)
			require.NoError(t, err)
			require.NotEqual(t, uuid.Nil, inc)
			return Document{DocumentIncarnationSummary: *status, Text: input}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("document %s state %q, completed %v, pending job %v, last error %v: %v", id, state, completed, pending, lastError, ctx.Err())
		case <-ticker.C:
		}
	}
}

// RequireFinalized checks durable deletion independently of the API response.
func (f *Fixture) RequireFinalized(t *testing.T, doc Document) {
	t.Helper()
	var finalized, missing, chunksGone bool
	require.NoError(t, f.Admin.Pool.QueryRow(t.Context(), `SELECT finalized,
		NOT EXISTS (SELECT FROM documents WHERE id=$1),
		NOT EXISTS (SELECT FROM chunks WHERE document_id=$1)
		FROM document_deletions WHERE document_id=$1 AND incarnation=$2`, doc.ID, doc.Incarnation).
		Scan(&finalized, &missing, &chunksGone))
	require.True(t, finalized && missing && chunksGone)
}

// RequireStoredVectors checks the selected vector store's persisted state.
func (f *Fixture) RequireStoredVectors(t *testing.T, doc Document, want bool) {
	t.Helper()
	var exists bool
	require.NoError(t, f.Admin.Pool.QueryRow(t.Context(), "SELECT EXISTS (SELECT FROM "+f.vectorTable()+" WHERE document_id=$1)", doc.ID).Scan(&exists))
	require.Equal(t, want, exists, "document %s", doc.ID)
}

func (*Fixture) vectorTable() string { return "chunk_vectors_" + generation }

// Query selects the shared deterministic query with appropriate citations.
func Query(mode string, answer client.AnswerMode) client.QueryRequest {
	return client.QueryRequest{Query: Prompt, Mode: mode, AnswerMode: answer, IncludeCitations: answer == client.AnswerModeGenerated}
}

// RequireDenied compares the response to the bounded authorization envelope.
func RequireDenied(t *testing.T, resp Response, status int, code, message string) {
	t.Helper()
	require.Equal(t, status, resp.Status, "%s", resp.Body)
	require.Equal(t, "private, no-store", resp.Header.Get("Cache-Control"))
	data, err := json.Marshal(client.ErrorResponse{Error: code, Message: message})
	require.NoError(t, err)
	require.JSONEq(t, string(data), string(resp.Body))
}

// VerifyReady waits for the readiness cache to observe the committed marker.
func (f *Fixture) VerifyReady(t *testing.T, replica int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.Servers[replica].URL+"/readyz", http.NoBody)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := f.Servers[replica].Client().Do(req.Clone(ctx))
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, 6*time.Second, 20*time.Millisecond)
}
