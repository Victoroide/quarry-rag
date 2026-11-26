package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	mcpDeleteID          = "00000000-0000-4000-8000-000000000001"
	mcpDeleteIncarnation = "2d43ca14-76bc-4f72-bf85-e7a607a1197d"
)

type deleteRecorder struct {
	mu      sync.Mutex
	headers [][]string
}

func (r *deleteRecorder) record(values []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.headers = append(r.headers, values)
}

func (r *deleteRecorder) requests() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]string(nil), r.headers...)
}

func deleteToolServer(t *testing.T) (*mcp.ClientSession, *deleteRecorder) {
	t.Helper()
	recorder := new(deleteRecorder)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE /documents/"+mcpDeleteID, r.Method+" "+r.URL.Path)
		recorder.record(r.Header.Values("X-Document-Incarnation"))
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(client.DeleteResponse{Deleted: true, DocID: mcpDeleteID}))
	}))
	t.Cleanup(s.Close)
	return connectServer(t, config{baseURL: s.URL, token: "synthetic-token", timeout: time.Second}), recorder
}

func TestDeleteToolSchemaKeepsTheIDOnlyContract(t *testing.T) {
	cs, _ := deleteToolServer(t)
	list, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	for _, tool := range list.Tools {
		if tool.Name != "quarry_delete_document" {
			continue
		}
		encoded, err := json.Marshal(tool.InputSchema)
		require.NoError(t, err)
		var schema struct {
			Required   []string                  `json:"required"`
			Properties map[string]map[string]any `json:"properties"`
		}
		require.NoError(t, json.Unmarshal(encoded, &schema))
		assert.Equal(t, []string{"document_id"}, schema.Required, "a caller that sends only the ID still validates")
		require.Contains(t, schema.Properties, "incarnation")
		assert.Equal(t, "string", schema.Properties["incarnation"]["type"], "a null value is not an absent one")
		assert.EqualValues(t, 36, schema.Properties["incarnation"]["minLength"])
		assert.EqualValues(t, 36, schema.Properties["incarnation"]["maxLength"])
		assert.NotEmpty(t, schema.Properties["incarnation"]["description"])
		return
	}
	t.Fatal("quarry_delete_document is not listed")
}

func TestDeleteToolWithoutIncarnationSendsTheOriginalRequest(t *testing.T) {
	cs, recorder := deleteToolServer(t)
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_delete_document", Arguments: map[string]any{"document_id": mcpDeleteID}})
	require.NoError(t, err)
	require.False(t, result.IsError, "%v", result.Content)
	require.Equal(t, [][]string{nil}, recorder.requests(), "one request without the qualifier")
}

func TestDeleteToolWithIncarnationQualifiesTheRequestOnce(t *testing.T) {
	cs, recorder := deleteToolServer(t)
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_delete_document", Arguments: map[string]any{"document_id": mcpDeleteID, "incarnation": mcpDeleteIncarnation}})
	require.NoError(t, err)
	require.False(t, result.IsError, "%v", result.Content)
	require.Equal(t, [][]string{{mcpDeleteIncarnation}}, recorder.requests())
}

func TestDeleteToolRejectsAnIncarnationThatCouldBecomeIDOnly(t *testing.T) {
	cs, recorder := deleteToolServer(t)
	for name, value := range map[string]any{
		"empty":        "",
		"null":         nil,
		"not a UUID":   "not-a-uuid",
		"unhyphenated": "2d43ca1476bc4f72bf85e7a607a1197d",
		"padded":       " " + mcpDeleteIncarnation,
		"number":       36,
		"list":         []string{mcpDeleteIncarnation},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_delete_document", Arguments: map[string]any{"document_id": mcpDeleteID, "incarnation": value}})
			if err == nil {
				require.True(t, result.IsError, "the call must fail")
			}
		})
	}
	require.Empty(t, recorder.requests(), "a rejected qualifier never reaches the service as an ID-only deletion")
}

func TestDeleteToolReportsTheActivatedContractWithoutRetrying(t *testing.T) {
	var requests int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPreconditionRequired)
		_, _ = w.Write([]byte(`{"error":"precondition","message":"X-Document-Incarnation is required to delete a live document"}`))
	}))
	defer s.Close()
	cs := connectServer(t, config{baseURL: s.URL, token: "synthetic-token", timeout: time.Second})
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_delete_document", Arguments: map[string]any{"document_id": mcpDeleteID}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, 1, requests)
}
