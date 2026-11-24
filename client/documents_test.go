package client_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocuments(t *testing.T) {
	next := "00000000-0000-4000-8000-000000000003"
	summary := client.DocumentSummary{ID: "00000000-0000-4000-8000-000000000001", DocType: "guide", Status: "completed", ChunkCount: 2, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "synthetic-token", r.Header.Get("X-Service-Token"))
		switch r.Method + " " + r.URL.Path {
		case "GET /api/documents":
			assert.Equal(t, "after=00000000-0000-4000-8000-000000000002&limit=2", r.URL.RawQuery)
			writeJSON(t, w, client.DocumentPage{Documents: []client.DocumentSummary{summary}, Count: 1, NextAfter: &next})
		case "GET /api/documents/00000000-0000-4000-8000-000000000001":
			writeJSON(t, w, summary)
		case "DELETE /api/documents/00000000-0000-4000-8000-000000000001":
			writeJSON(t, w, client.DeleteResponse{Deleted: true, DocID: "00000000-0000-4000-8000-000000000001"})
		case "GET /api/documents/00000000-0000-4000-8000-000000000001/file":
			writeJSON(t, w, client.FileURL{URL: "/synthetic-download", ExpiresIn: 900})
		default:
			t.Errorf("unexpected route: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	page, err := c.ListDocuments(t.Context(), client.ListOptions{Limit: 2, After: "00000000-0000-4000-8000-000000000002"})
	require.NoError(t, err)
	assert.Equal(t, []client.DocumentSummary{summary}, page.Documents)
	assert.Equal(t, &next, page.NextAfter)
	got, err := c.GetDocument(t.Context(), "00000000-0000-4000-8000-000000000001")
	require.NoError(t, err)
	assert.Equal(t, &summary, got)
	deleted, err := c.DeleteDocument(t.Context(), "00000000-0000-4000-8000-000000000001")
	require.NoError(t, err)
	assert.Equal(t, &client.DeleteResponse{Deleted: true, DocID: "00000000-0000-4000-8000-000000000001"}, deleted)
	file, err := c.DocumentFileURL(t.Context(), "00000000-0000-4000-8000-000000000001")
	require.NoError(t, err)
	assert.Equal(t, &client.FileURL{URL: "/synthetic-download", ExpiresIn: 900}, file)
}

func TestListDocumentsDefaultsToPage(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "limit=100", r.URL.RawQuery)
		writeJSON(t, w, client.DocumentPage{Documents: []client.DocumentSummary{}})
	})
	_, err := c.ListDocuments(t.Context(), client.ListOptions{})
	require.NoError(t, err)
}

func TestDocumentIDIsOnePathSegment(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/documents/id%2Fwith%3Freserved%23characters", r.URL.EscapedPath())
		assert.Empty(t, r.URL.RawQuery)
		writeJSON(t, w, client.DocumentSummary{})
	})
	_, err := c.GetDocument(t.Context(), "id/with?reserved#characters")
	require.NoError(t, err)
}

func TestListDocumentsCollectionEncoding(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "café & reports", r.URL.Query().Get("collection"))
		require.Equal(t, "100", r.URL.Query().Get("limit"))
		writeJSON(t, w, client.DocumentPage{Documents: []client.DocumentSummary{}})
	})
	_, err := c.ListDocuments(t.Context(), client.ListOptions{Collection: "café & reports"})
	require.NoError(t, err)
}

func TestDocumentIncarnationReads(t *testing.T) {
	const id = "00000000-0000-4000-8000-000000000001"
	const incarnation = "2d43ca14-76bc-4f72-bf85-e7a607a1197d"
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "synthetic-token", r.Header.Get("X-Service-Token"))
		summary := map[string]string{"document_id": id, "incarnation": incarnation}
		switch r.URL.Path {
		case "/api/documents":
			assert.Equal(t, "after=cursor&collection=archive&include_incarnation=true&limit=100", r.URL.RawQuery)
			writeJSON(t, w, map[string]any{"documents": []any{summary}, "count": 1, "next_after": id})
		case "/api/documents/" + id:
			assert.Equal(t, "include_incarnation=true", r.URL.RawQuery)
			writeJSON(t, w, summary)
		default:
			t.Errorf("unexpected route: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	summary, err := c.GetDocumentWithIncarnation(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, id, summary.ID)
	require.Equal(t, incarnation, summary.Incarnation)
	page, err := c.ListDocumentsWithIncarnation(t.Context(), client.ListOptions{Collection: "archive", After: "cursor"})
	require.NoError(t, err)
	require.Equal(t, 1, page.Count)
	require.Equal(t, &summary.ID, page.NextAfter)
	require.Equal(t, []client.DocumentIncarnationSummary{*summary}, page.Documents)
}
