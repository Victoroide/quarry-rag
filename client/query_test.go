package client_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryDecodesOptionalRerankScore(t *testing.T) {
	for _, field := range []string{"", `,"rerank_score":0`, `,"rerank_score":0.75`} {
		c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
			_, err := w.Write([]byte(`{"scored_chunks":[{"score":0.9` + field + `}]}`))
			require.NoError(t, err)
		})
		got, err := c.Query(t.Context(), client.QueryRequest{Query: "archive"})
		require.NoError(t, err)
		require.Equal(t, .9, got.ScoredChunks[0].Score)
		if field == "" {
			require.Nil(t, got.ScoredChunks[0].RerankScore)
		} else {
			require.NotNil(t, got.ScoredChunks[0].RerankScore)
			if field == `,"rerank_score":0` {
				require.Zero(t, *got.ScoredChunks[0].RerankScore)
			} else {
				require.Equal(t, .75, *got.ScoredChunks[0].RerankScore)
			}
		}
	}
}

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

func TestQueryAcceptLanguage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   []client.RequestOption
		header string
	}{
		{"default", nil, ""},
		{"regional", []client.RequestOption{client.WithAcceptLanguage("es-MX")}, "es-MX"},
		{"weighted", []client.RequestOption{client.WithAcceptLanguage("en;q=0.2, fr-CA;q=0.9")}, "en;q=0.2, fr-CA;q=0.9"},
		{"last option", []client.RequestOption{client.WithAcceptLanguage("es"), client.WithAcceptLanguage("pt-BR")}, "pt-BR"},
		{"empty", []client.RequestOption{client.WithAcceptLanguage("")}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, tc.header, r.Header.Get("Accept-Language"))
				assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
				writeJSON(t, w, client.QueryResponse{})
			})
			_, err := c.Query(t.Context(), client.QueryRequest{Query: "synthetic question"}, tc.opts...)
			require.NoError(t, err)
		})
	}
}

func TestQueryAcceptLanguageIsPerRequest(t *testing.T) {
	wantHeader := "es"
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, wantHeader, r.Header.Get("Accept-Language"))
		writeJSON(t, w, client.QueryResponse{})
	})
	_, err := c.Query(t.Context(), client.QueryRequest{Query: "synthetic question"}, client.WithAcceptLanguage("es"))
	require.NoError(t, err)
	wantHeader = ""
	_, err = c.Query(t.Context(), client.QueryRequest{Query: "synthetic question"})
	require.NoError(t, err)
}

func TestQueryRejectsNilRequestOption(t *testing.T) {
	c := newClient(t, func(http.ResponseWriter, *http.Request) {
		t.Error("invalid request option reached the server")
	})
	got, err := c.Query(t.Context(), client.QueryRequest{Query: "synthetic question"}, nil)
	require.EqualError(t, err, "request option is required")
	require.Nil(t, got)
}

func TestQueryCitationContract(t *testing.T) {
	for _, citations := range []string{`[]`, `[{"excerpt":1,"doc_id":"source","chunk_index":4,"offset":8,"verified":false,"overlap":0.25}]`} {
		t.Run(citations, func(t *testing.T) {
			c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				var body map[string]json.RawMessage
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.Equal(t, "true", string(body["include_citations"]))
				_, err := w.Write([]byte(`{"answer":"Archive [1]","scored_chunks":[{"doc_id":"source","chunk_index":4,"excerpt":1,"truncated":true}],"citations":` + citations + `}`))
				assert.NoError(t, err)
			})
			var req client.QueryRequest
			require.NoError(t, json.Unmarshal([]byte(`{"query":"archive","include_citations":true}`), &req))
			got, err := c.Query(t.Context(), req)
			require.NoError(t, err)
			encoded, err := json.Marshal(got)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(encoded, &fields))
			require.JSONEq(t, citations, string(fields["citations"]))
			var chunks []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(fields["scored_chunks"], &chunks))
			require.Equal(t, "1", string(chunks[0]["excerpt"]))
			require.Equal(t, "true", string(chunks[0]["truncated"]))
		})
	}
}

func TestQueryCollectionEncoding(t *testing.T) {
	encoded, err := json.Marshal(client.QueryRequest{Query: "archive", Collection: "café"})
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(encoded, &body))
	require.Equal(t, "café", body["collection"])
}
