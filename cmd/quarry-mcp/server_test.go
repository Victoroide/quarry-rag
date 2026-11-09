package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stdioPipes(t *testing.T) (input, output, logs *os.File) {
	t.Helper()
	inR, inW, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = inR.Close(); _ = inW.Close() })
	outR, outW, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = outR.Close(); _ = outW.Close() })
	errR, errW, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = errR.Close(); _ = errW.Close() })
	stdin, stdout, stderr := os.Stdin, os.Stdout, os.Stderr
	os.Stdin, os.Stdout, os.Stderr = inR, outW, errW
	t.Cleanup(func() { os.Stdin, os.Stdout, os.Stderr = stdin, stdout, stderr })
	deadline := time.Now().Add(5 * time.Second)
	require.NoError(t, inW.SetWriteDeadline(deadline))
	require.NoError(t, outR.SetReadDeadline(deadline))
	require.NoError(t, errR.SetReadDeadline(deadline))
	return inW, outR, errR
}

func TestStdioSession(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assert.Equal(t, "GET /documents/00000000-0000-4000-8000-000000000001", r.Method+" "+r.URL.Path)
		assert.Equal(t, "synthetic-token", r.Header.Get("X-Service-Token"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"document_id":"00000000-0000-4000-8000-000000000001","doc_type":"guide","collection":"","status":"processing","chunk_count":0,"file_name":null,"file_type":null,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","completed_at":null}`)
	}))
	defer api.Close()
	t.Setenv("QUARRY_URL", strings.TrimPrefix(api.URL, "http://"))
	t.Setenv("QUARRY_SERVICE_TOKEN", "synthetic-token")
	for _, name := range []string{"QUARRY_TIMEOUT", "QUARRY_MCP_FILE_ROOTS", "QUARRY_MCP_READ_ONLY"} {
		t.Setenv(name, "")
	}
	input, output, logs := stdioPipes(t)
	status := make(chan int, 1)
	go func() { status <- run() }()
	finished := false
	t.Cleanup(func() {
		_ = input.Close()
		if !finished {
			select {
			case <-status:
			case <-time.After(5 * time.Second):
				t.Error("stdio session did not stop after EOF")
			}
		}
	})
	responses := bufio.NewReader(output)
	call := func(message string) json.RawMessage {
		t.Helper()
		_, writeErr := io.WriteString(input, message+"\n")
		require.NoError(t, writeErr)
		line, readErr := responses.ReadBytes('\n')
		require.NoError(t, readErr)
		var envelope struct {
			JSONRPC string          `json:"jsonrpc"`
			Result  json.RawMessage `json:"result"`
			Error   json.RawMessage `json:"error"`
		}
		require.NoError(t, json.Unmarshal(line, &envelope))
		assert.Equal(t, "2.0", envelope.JSONRPC)
		assert.Empty(t, envelope.Error)
		require.NotEmpty(t, envelope.Result)
		return envelope.Result
	}
	var initialized mcp.InitializeResult
	require.NoError(t, json.Unmarshal(call(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`), &initialized))
	assert.Equal(t, "quarry", initialized.ServerInfo.Name)
	_, err := io.WriteString(input, `{"jsonrpc":"2.0","method":"notifications/initialized"}`+"\n")
	require.NoError(t, err)
	var listed mcp.ListToolsResult
	require.NoError(t, json.Unmarshal(call(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`), &listed))
	assert.Len(t, listed.Tools, 5)
	assert.Zero(t, calls.Load())
	var result struct {
		IsError           bool `json:"isError"`
		StructuredContent struct {
			Status     string  `json:"status"`
			Collection *string `json:"collection"`
		} `json:"structuredContent"`
	}
	require.NoError(t, json.Unmarshal(call(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"quarry_get_document","arguments":{"document_id":"00000000-0000-4000-8000-000000000001"}}}`), &result))
	assert.False(t, result.IsError)
	assert.Equal(t, "processing", result.StructuredContent.Status)
	require.NotNil(t, result.StructuredContent.Collection)
	assert.Empty(t, *result.StructuredContent.Collection)
	require.NoError(t, input.Close())
	select {
	case code := <-status:
		finished = true
		assert.Zero(t, code)
	case <-time.After(5 * time.Second):
		t.Fatal("stdio session did not stop after EOF")
	}
	require.NoError(t, os.Stdout.Close())
	require.NoError(t, os.Stderr.Close())
	rest, err := io.ReadAll(responses)
	require.NoError(t, err)
	assert.Empty(t, rest, "stdout carries only protocol messages")
	diagnostics, err := io.ReadAll(logs)
	require.NoError(t, err)
	assert.NotContains(t, string(diagnostics), "synthetic-token")
	assert.EqualValues(t, 1, calls.Load())
}

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	t.Setenv("QUARRY_URL", "")
	t.Setenv("QUARRY_SERVICE_TOKEN", "")
	_, output, logs := stdioPipes(t)
	assert.Equal(t, 2, run())
	require.NoError(t, os.Stdout.Close())
	require.NoError(t, os.Stderr.Close())
	stdout, err := io.ReadAll(output)
	require.NoError(t, err)
	assert.Empty(t, stdout)
	decoder := json.NewDecoder(logs)
	var record map[string]any
	require.NoError(t, decoder.Decode(&record))
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "invalid configuration", record["msg"])
	assert.Equal(t, "QUARRY_URL is required", record["error"])
	assert.ErrorIs(t, decoder.Decode(&record), io.EOF, "stderr must contain exactly one record")
}

func connectServer(t *testing.T, cfg config) *mcp.ClientSession {
	t.Helper()
	s, err := newServer(cfg)
	require.NoError(t, err)
	ct, st := mcp.NewInMemoryTransports()
	ss, err := s.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ss.Close()) })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cs.Close()) })
	assert.Equal(t, "quarry", cs.InitializeResult().ServerInfo.Name)
	assert.NotEmpty(t, cs.InitializeResult().ServerInfo.Version)
	return cs
}

func TestToolRegistration(t *testing.T) {
	for _, tc := range []struct {
		name     string
		roots    bool
		readOnly bool
		count    int
	}{{"full", true, false, 6}, {"without files", false, false, 5}, {"read only", true, true, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
			defer s.Close()
			cfg := config{baseURL: s.URL, timeout: time.Second, readOnly: tc.readOnly}
			if tc.roots {
				cfg.fileRoots = []string{t.TempDir()}
			}
			cs := connectServer(t, cfg)
			list, err := cs.ListTools(t.Context(), nil)
			require.NoError(t, err)
			require.Len(t, list.Tools, tc.count)
			assert.Zero(t, calls.Load(), "initialization and discovery must not contact the API")
			expectedNames := []string{"quarry_query", "quarry_list_documents", "quarry_get_document"}
			if !tc.readOnly {
				expectedNames = append(expectedNames, "quarry_ingest_text", "quarry_delete_document")
				if tc.roots {
					expectedNames = append(expectedNames, "quarry_ingest_file")
				}
			}
			var actualNames []string
			for _, tool := range list.Tools {
				actualNames = append(actualNames, tool.Name)
				require.NotNil(t, tool.Annotations)
				require.NotNil(t, tool.Annotations.OpenWorldHint)
				assert.False(t, *tool.Annotations.OpenWorldHint)
				assert.NotNil(t, tool.InputSchema)
				assert.NotNil(t, tool.OutputSchema)
				data, err := json.Marshal(tool.InputSchema)
				require.NoError(t, err)
				var schema struct {
					Properties map[string]struct{ Description string }
				}
				require.NoError(t, json.Unmarshal(data, &schema))
				require.NotEmpty(t, schema.Properties)
				for name, property := range schema.Properties {
					assert.NotEmpty(t, property.Description, "%s.%s needs a description", tool.Name, name)
				}
				if tool.Name == "quarry_ingest_text" || tool.Name == "quarry_ingest_file" {
					assert.Contains(t, schema.Properties, "collection")
					assert.NotEmpty(t, schema.Properties["collection"].Description)
				}
				switch tool.Name {
				case "quarry_query", "quarry_list_documents", "quarry_get_document":
					assert.True(t, tool.Annotations.ReadOnlyHint)
				case "quarry_ingest_text", "quarry_ingest_file":
					assert.False(t, tc.readOnly)
					assert.False(t, tool.Annotations.ReadOnlyHint)
					require.NotNil(t, tool.Annotations.DestructiveHint)
					assert.False(t, *tool.Annotations.DestructiveHint)
					assert.True(t, tool.Annotations.IdempotentHint)
				case "quarry_delete_document":
					assert.False(t, tc.readOnly)
					require.NotNil(t, tool.Annotations.DestructiveHint)
					assert.True(t, *tool.Annotations.DestructiveHint)
					assert.True(t, tool.Annotations.IdempotentHint)
				default:
					t.Errorf("unexpected tool %q", tool.Name)
				}
			}
			assert.ElementsMatch(t, expectedNames, actualNames)
		})
	}
}

func TestListToolPaginationAndBounds(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assert.Equal(t, "GET /documents", r.Method+" "+r.URL.Path)
		assert.Equal(t, "after=00000000-0000-4000-8000-000000000002&limit=2", r.URL.RawQuery)
		_, _ = io.WriteString(w, `{"documents":[],"count":0,"next_after":"00000000-0000-4000-8000-000000000003"}`)
	}))
	defer s.Close()
	cs := connectServer(t, config{baseURL: s.URL, timeout: time.Second})
	listed, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	for _, tool := range listed.Tools {
		if tool.Name != "quarry_list_documents" {
			continue
		}
		data, marshalErr := json.Marshal(tool.InputSchema)
		require.NoError(t, marshalErr)
		var schema struct {
			Properties map[string]struct {
				Minimum int
				Maximum int
				Default int
			}
		}
		require.NoError(t, json.Unmarshal(data, &schema))
		assert.Equal(t, 1, schema.Properties["limit"].Minimum)
		assert.Equal(t, 1000, schema.Properties["limit"].Maximum)
		assert.Equal(t, 100, schema.Properties["limit"].Default)
	}
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_list_documents", Arguments: map[string]any{"limit": 2, "after": "00000000-0000-4000-8000-000000000002"}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	data, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	assert.JSONEq(t, `{"documents":[],"next_after":"00000000-0000-4000-8000-000000000003"}`, string(data))
	for _, limit := range []int{0, -1, 1001} {
		result, err = cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_list_documents", Arguments: map[string]any{"limit": limit}})
		require.NoError(t, err)
		assert.True(t, result.IsError)
	}
	assert.EqualValues(t, 1, calls.Load())
}

func TestQueryToolCitations(t *testing.T) {
	for _, tc := range []struct {
		name, citations string
		include         bool
	}{
		{"requested", `[{"excerpt":1,"doc_id":"source","chunk_index":4,"offset":8,"verified":false,"overlap":0.25}]`, true},
		{"empty", `[]`, true},
		{"default", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "POST /query", r.Method+" "+r.URL.Path)
				var body map[string]json.RawMessage
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				if tc.include {
					assert.Equal(t, "true", string(body["include_citations"]))
				} else {
					assert.NotContains(t, body, "include_citations")
				}
				response := `{"answer":"Archive [1]","scored_chunks":[{"doc_id":"source","chunk_index":4,"excerpt":1,"truncated":true}]`
				if tc.include {
					response += `,"citations":` + tc.citations
				}
				_, err := io.WriteString(w, response+"}")
				assert.NoError(t, err)
			}))
			defer s.Close()
			cs := connectServer(t, config{baseURL: s.URL, timeout: time.Second})
			arguments := map[string]any{"query": "archive"}
			if tc.include {
				arguments["include_citations"] = true
			}
			result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: arguments})
			require.NoError(t, err)
			require.False(t, result.IsError, "%v", result.Content)
			data, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &fields))
			if tc.include {
				assert.JSONEq(t, tc.citations, string(fields["citations"]))
			} else {
				assert.NotContains(t, fields, "citations")
			}
			var chunks []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(fields["chunks"], &chunks))
			require.Len(t, chunks, 1)
			assert.Equal(t, "1", string(chunks[0]["excerpt"]))
			assert.Equal(t, "true", string(chunks[0]["truncated"]))
		})
	}
}

func TestFileToolRejectsEscapeWithoutHTTPCall(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer s.Close()
	path := filepath.Join(t.TempDir(), "outside.txt")
	syntheticFile(t, path)
	cs := connectServer(t, config{baseURL: s.URL, timeout: time.Second, fileRoots: []string{t.TempDir()}})
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_ingest_file", Arguments: map[string]any{"path": path}})
	require.NoError(t, err)
	assert.True(t, result.IsError)
	assert.Nil(t, result.StructuredContent)
	assert.Equal(t, "file must be inside an allowed root", result.Content[0].(*mcp.TextContent).Text)
	assert.Zero(t, calls.Load())
}

func TestUnreachableServiceIsToolError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	s.Close()
	cs := connectServer(t, config{baseURL: s.URL, token: "synthetic-token", timeout: time.Second})
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: map[string]any{"query": "synthetic question"}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	assert.Nil(t, result.StructuredContent)
	text := result.Content[0].(*mcp.TextContent).Text
	assert.True(t, strings.HasPrefix(text, "service unreachable: "), "unexpected error: %s", text)
	assert.NotContains(t, text, "synthetic-token")
}

func TestFileToolExplainsRejections(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer s.Close()
	root := t.TempDir()
	unknown := filepath.Join(root, "sample.pem")
	syntheticFile(t, unknown)
	directory := filepath.Join(root, "directory.md")
	require.NoError(t, os.Mkdir(directory, 0o700))
	cs := connectServer(t, config{baseURL: s.URL, timeout: time.Second, fileRoots: []string{root}})
	for _, tc := range []struct{ path, message string }{
		{unknown, "file extension must be pdf, docx, doc, txt, md or markdown"},
		{filepath.Join(root, "missing.md"), "file is unavailable"},
		{directory, "path must identify a regular file"},
	} {
		t.Run(tc.message, func(t *testing.T) {
			result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_ingest_file", Arguments: map[string]any{"path": tc.path}})
			require.NoError(t, err)
			require.True(t, result.IsError)
			assert.Nil(t, result.StructuredContent)
			assert.Equal(t, tc.message, result.Content[0].(*mcp.TextContent).Text)
		})
	}
	assert.Zero(t, calls.Load())
}

func TestToolsCallHTTPAPI(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sample.md")
	syntheticFile(t, path)
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assert.Equal(t, "synthetic-token", r.Header.Get("X-Service-Token"))
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /prefix/query":
			var body client.QueryRequest
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			assert.Equal(t, client.QueryRequest{Query: "synthetic question", Mode: "local", FilterDocType: "guide", MaxChunks: 2, AllowedDocumentIDs: []string{}}, body)
			assert.NoError(t, json.NewEncoder(w).Encode(client.QueryResponse{Answer: "synthetic answer", Mode: "LOCAL", FinishReason: "stop", ChunksUsed: 1, ContextChunks: 1, TokenUsage: client.TokenUsage{InputTokens: 10}, ScoredChunks: []client.ScoredChunkResponse{{DocID: "00000000-0000-4000-8000-000000000001", DocType: "guide", ChunkIndex: 2, Score: 0.9, Text: "synthetic evidence"}}}))
		case "GET /prefix/documents":
			assert.Equal(t, "limit=100", r.URL.RawQuery)
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"documents": []domain.DocumentSummary{{ID: "00000000-0000-4000-8000-000000000001", Collection: "tenant-a", Status: "processing"}}, "count": 1, "next_after": nil}))
		case "GET /prefix/documents/00000000-0000-4000-8000-000000000001":
			assert.NoError(t, json.NewEncoder(w).Encode(domain.DocumentSummary{ID: "00000000-0000-4000-8000-000000000001", Collection: "tenant-a", Status: "processing"}))
		case "DELETE /prefix/documents/00000000-0000-4000-8000-000000000001":
			assert.NoError(t, json.NewEncoder(w).Encode(client.DeleteResponse{Deleted: true, DocID: "00000000-0000-4000-8000-000000000001"}))
		case "POST /prefix/ingest":
			if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
				f, header, err := r.FormFile("file")
				if !assert.NoError(t, err) {
					return
				}
				defer func() {
					assert.NoError(t, f.Close())
					if r.MultipartForm != nil {
						assert.NoError(t, r.MultipartForm.RemoveAll())
					}
				}()
				data, err := io.ReadAll(f)
				assert.NoError(t, err)
				assert.Equal(t, "sample.md", header.Filename)
				assert.Equal(t, "synthetic file content", string(data))
				assert.Equal(t, "guide", r.FormValue("doc_type"))
				assert.Equal(t, []string{"tenant-a"}, r.MultipartForm.Value["collection"])
				assert.Equal(t, "00000000-0000-4000-8000-000000000001", r.FormValue("document_id"))
				assert.JSONEq(t, `{"category":"example"}`, r.FormValue("metadata"))
			} else {
				var body client.IngestRequest
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.Equal(t, client.IngestRequest{Content: "synthetic text", DocumentID: "00000000-0000-4000-8000-000000000001", DocType: "guide", Metadata: map[string]any{"category": "example"}, Collection: "tenant-a"}, body)
			}
			w.WriteHeader(http.StatusAccepted)
			assert.NoError(t, json.NewEncoder(w).Encode(client.IngestResponse{DocumentID: "00000000-0000-4000-8000-000000000001", Status: "processing"}))
		default:
			t.Errorf("unexpected route %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer s.Close()
	cs := connectServer(t, config{baseURL: s.URL + "/prefix", token: "synthetic-token", timeout: time.Second, fileRoots: []string{root}})
	for _, tc := range []struct {
		name     string
		args     map[string]any
		expected map[string]any
	}{
		{"quarry_query", map[string]any{"query": "synthetic question", "mode": "local", "filter_doc_type": "guide", "max_chunks": 2, "allowed_document_ids": []string{}}, map[string]any{"answer": "synthetic answer", "mode": "LOCAL", "finish_reason": "stop", "chunks_used": float64(1), "context_chunks": float64(1), "token_usage": map[string]any{"input_tokens": float64(10), "output_tokens": float64(0), "total_tokens": float64(0)}, "chunks": []any{map[string]any{"doc_id": "00000000-0000-4000-8000-000000000001", "doc_type": "guide", "chunk_index": float64(2), "score": 0.9, "text": "synthetic evidence"}}}},
		{"quarry_list_documents", map[string]any{}, map[string]any{"next_after": nil}},
		{"quarry_get_document", map[string]any{"document_id": "00000000-0000-4000-8000-000000000001"}, map[string]any{"document_id": "00000000-0000-4000-8000-000000000001", "collection": "tenant-a", "status": "processing"}},
		{"quarry_ingest_text", map[string]any{"content": "synthetic text", "document_id": "00000000-0000-4000-8000-000000000001", "doc_type": "guide", "metadata": map[string]any{"category": "example"}, "collection": "tenant-a"}, map[string]any{"document_id": "00000000-0000-4000-8000-000000000001", "status": "processing"}},
		{"quarry_ingest_file", map[string]any{"path": path, "document_id": "00000000-0000-4000-8000-000000000001", "doc_type": "guide", "metadata": map[string]any{"category": "example"}, "collection": "tenant-a"}, map[string]any{"document_id": "00000000-0000-4000-8000-000000000001", "status": "processing"}},
		{"quarry_delete_document", map[string]any{"document_id": "00000000-0000-4000-8000-000000000001"}, map[string]any{"deleted": true, "doc_id": "00000000-0000-4000-8000-000000000001"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: tc.name, Arguments: tc.args})
			require.NoError(t, err)
			require.False(t, result.IsError, "%v", result.Content)
			encoded, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			var body map[string]any
			require.NoError(t, json.Unmarshal(encoded, &body))
			for key, want := range tc.expected {
				assert.Equal(t, want, body[key], "field %s", key)
			}
			if tc.name == "quarry_list_documents" {
				documents, ok := body["documents"].([]any)
				require.True(t, ok)
				require.Len(t, documents, 1)
				document, ok := documents[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "tenant-a", document["collection"])
			}
		})
	}
	assert.EqualValues(t, 6, calls.Load())
}

func TestAPIFailureIsToolError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"busy","message":"synthetic-token unavailable"}`)
	}))
	defer s.Close()
	cs := connectServer(t, config{baseURL: s.URL, token: "synthetic-token", timeout: time.Second})
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: map[string]any{"query": "synthetic question"}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	text := result.Content[0].(*mcp.TextContent).Text
	assert.Contains(t, text, "503 busy:")
	assert.Contains(t, text, "unavailable")
	assert.Contains(t, text, "retry after 3s")
	assert.NotContains(t, text, "synthetic-token")
	assert.Nil(t, result.StructuredContent)
}
