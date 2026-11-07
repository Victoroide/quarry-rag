package client_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	want := client.QueryRequest{Query: "synthetic question", Mode: "local", FilterDocType: "guide", MaxChunks: 4, AllowedDocumentIDs: []string{"00000000-0000-4000-8000-000000000001"}}
	response := client.QueryResponse{Answer: "synthetic answer", Mode: "LOCAL", ChunksUsed: 1, ContextChunks: 1, FinishReason: "stop", ContextTokens: 20, StageTimings: map[string]int64{"synthesis": 1}, TokenUsage: client.TokenUsage{InputTokens: 10, OutputTokens: 3, TotalTokens: 13}, ScoredChunks: []client.ScoredChunkResponse{{DocID: "00000000-0000-4000-8000-000000000001", DocType: "guide", ChunkIndex: 2, Score: 0.9, Text: "synthetic evidence", Kind: "body", Quality: "high"}}}
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/query", r.URL.Path)
		var got client.QueryRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		assert.Equal(t, want, got)
		writeJSON(t, w, response)
	})
	got, err := c.Query(t.Context(), want)
	require.NoError(t, err)
	assert.Equal(t, &response, got)
}

func TestQueryPreservesEmptyAllowlist(t *testing.T) {
	for _, ids := range [][]string{nil, {}} {
		c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			var body map[string]json.RawMessage
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if ids == nil {
				assert.NotContains(t, body, "allowed_document_ids")
			} else {
				assert.JSONEq(t, `[]`, string(body["allowed_document_ids"]))
			}
			writeJSON(t, w, client.QueryResponse{})
		})
		_, err := c.Query(t.Context(), client.QueryRequest{Query: "synthetic question", AllowedDocumentIDs: ids})
		require.NoError(t, err)
	}
}
