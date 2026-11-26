package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	serviceapi "github.com/Victoroide/quarry-rag/internal/api"
	serviceconfig "github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	foreignSet = "20000000-0000-4000-8000-000000000001"
	absentSet  = "20000000-0000-4000-8000-0000000000ff"
)

func TestQueryToolSchemaOffersTheDocumentSetReference(t *testing.T) {
	cs := connectServer(t, config{baseURL: "http://127.0.0.1:1", token: "token", timeout: time.Second})
	tools, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	for _, tool := range tools.Tools {
		if tool.Name != "quarry_query" {
			continue
		}
		data, err := json.Marshal(tool.InputSchema)
		require.NoError(t, err)
		var schema struct {
			Properties map[string]struct {
				Type        json.RawMessage `json:"type"`
				Description string          `json:"description"`
			} `json:"properties"`
			Required []string `json:"required"`
		}
		require.NoError(t, json.Unmarshal(data, &schema))
		property, ok := schema.Properties["document_set_id"]
		require.True(t, ok)
		require.JSONEq(t, `"string"`, string(property.Type))
		require.Contains(t, property.Description, "grants no access")
		require.NotContains(t, schema.Required, "document_set_id", "the reference is optional")
		for _, forbidden := range []string{"owner", "revision", "tenant", "batch"} {
			for name := range schema.Properties {
				require.NotContains(t, name, forbidden, "a client cannot supply %s", forbidden)
			}
		}
		return
	}
	require.Fail(t, "quarry_query is not registered")
}

func TestQueryToolSendsTheReferenceAndHidesWhetherItIsForeign(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body client.QueryRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		seen = append(seen, body.DocumentSetID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte(`{"error":"not_found","message":"document set not found"}`))
		assert.NoError(t, err)
	}))
	defer server.Close()
	cs := connectServer(t, config{baseURL: server.URL, token: "token", timeout: time.Second})
	var texts []string
	for _, set := range []string{foreignSet, absentSet} {
		result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: map[string]any{"query": "Archive?", "document_set_id": set}})
		require.NoError(t, err)
		require.True(t, result.IsError)
		text := result.Content[0].(*mcp.TextContent).Text
		require.Contains(t, text, "404")
		require.NotContains(t, text, set, "the error never echoes the reference")
		texts = append(texts, text)
	}
	require.Equal(t, []string{foreignSet, absentSet}, seen)
	require.Equal(t, texts[0], texts[1], "a foreign reference reads like an absent one")
}

func TestQueryToolOmitsAnEmptyReference(t *testing.T) {
	var raw map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&raw))
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(client.QueryResponse{Answer: "ok", Mode: "GLOBAL", FinishReason: "stop"}))
	}))
	defer server.Close()
	cs := connectServer(t, config{baseURL: server.URL, token: "token", timeout: time.Second})
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: map[string]any{"query": "Archive?"}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	require.NotContains(t, raw, "document_set_id")
}

func TestQueryToolReferenceIsRefusedByAServiceModeDeployment(t *testing.T) {
	emb := embedder.NewFakeEmbedder(8)
	store := memory.NewFakeVectorStore()
	server := httptest.NewServer(serviceapi.NewRouter(&serviceapi.RouterDeps{
		Config:      &serviceconfig.Config{ServiceToken: "token", DocumentSetsEnabled: true},
		VectorStore: store,
		Retriever:   retriever.NewRetriever(store, emb, .7, 2),
	}))
	defer server.Close()
	cs := connectServer(t, config{baseURL: server.URL, token: "token", timeout: time.Second})
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: map[string]any{"query": "Archive?", "document_set_id": foreignSet}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Contains(t, result.Content[0].(*mcp.TextContent).Text, "400")
	require.Contains(t, result.Content[0].(*mcp.TextContent).Text, "document_set_id is not supported")
}
