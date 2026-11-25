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
	serviceapi "github.com/Victoroide/quarry-rag/internal/api"
	serviceconfig "github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
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
	subject := subjectFixture(time.Now().Add(time.Minute), 1)
	file := filepath.Join(t.TempDir(), "subject-token")
	replaceSubjectFile(t, file, subject, 0o600)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assert.Equal(t, "GET /documents/00000000-0000-4000-8000-000000000001", r.Method+" "+r.URL.Path)
		assert.Equal(t, "synthetic-token", r.Header.Get("X-Service-Token"))
		assert.Equal(t, "Bearer "+subject, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"document_id":"00000000-0000-4000-8000-000000000001","doc_type":"guide","collection":"","status":"processing","chunk_count":0,"file_name":null,"file_type":null,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","completed_at":null}`)
	}))
	defer api.Close()
	t.Setenv("QUARRY_URL", strings.TrimPrefix(api.URL, "http://"))
	t.Setenv("QUARRY_SERVICE_TOKEN", "synthetic-token")
	t.Setenv("QUARRY_SUBJECT_TOKEN_FILE", file)
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
	assert.NotContains(t, string(diagnostics), subject)
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
					assert.Contains(t, schema.Properties["document_id"].Description, "policy")
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
	observedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	summary := domain.DocumentSummary{ID: "00000000-0000-4000-8000-000000000001", Collection: "tenant-a", Status: "completed", IndexVisibility: domain.VisibilityWritesIndexed, IndexVisibilityObservedAt: &observedAt}
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
			assert.Equal(t, "include_index_visibility=true&limit=100", r.URL.RawQuery)
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"documents": []domain.DocumentSummary{summary}, "count": 1, "next_after": nil}))
		case "GET /prefix/documents/00000000-0000-4000-8000-000000000001":
			assert.Equal(t, "include_index_visibility=true", r.URL.RawQuery)
			assert.NoError(t, json.NewEncoder(w).Encode(summary))
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
		{"quarry_list_documents", map[string]any{"include_index_visibility": true}, map[string]any{"next_after": nil}},
		{"quarry_get_document", map[string]any{"document_id": "00000000-0000-4000-8000-000000000001", "include_index_visibility": true}, map[string]any{"document_id": "00000000-0000-4000-8000-000000000001", "collection": "tenant-a", "status": "completed", "index_visibility": "writes_indexed", "index_visibility_observed_at": observedAt.Format(time.RFC3339)}},
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
				assert.Equal(t, "writes_indexed", document["index_visibility"])
				assert.Equal(t, observedAt.Format(time.RFC3339), document["index_visibility_observed_at"])
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

func TestIngestQueueFullIsRetryableToolError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/ingest", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"queue","message":"ingest queue is full"}`)
	}))
	defer s.Close()
	cs := connectServer(t, config{baseURL: s.URL, timeout: time.Second})
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
		Name: "quarry_ingest_text",
		Arguments: map[string]any{
			"content":     "synthetic text",
			"document_id": "00000000-0000-4000-8000-000000000001",
		},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Nil(t, result.StructuredContent)
	require.Equal(t, "503 queue: ingest queue is full; retry after 30s", result.Content[0].(*mcp.TextContent).Text)
}

func TestCollectionTools(t *testing.T) {
	var queries, lists atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/query":
			queries.Add(1)
			var body map[string]any
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			assert.Equal(t, "archive", body["collection"])
			_, _ = io.WriteString(w, `{"answer":"synthetic","scored_chunks":[]}`)
		case "/documents":
			lists.Add(1)
			assert.Equal(t, "archive", r.URL.Query().Get("collection"))
			_, _ = io.WriteString(w, `{"documents":[],"next_after":null}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer s.Close()
	cs := connectServer(t, config{baseURL: s.URL, timeout: time.Second})
	tools, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	for _, tool := range tools.Tools {
		if tool.Name != "quarry_query" && tool.Name != "quarry_list_documents" {
			continue
		}
		data, err := json.Marshal(tool.InputSchema)
		require.NoError(t, err)
		var schema struct{ Properties map[string]any }
		require.NoError(t, json.Unmarshal(data, &schema))
		require.Contains(t, schema.Properties, "collection")
	}
	for _, name := range []string{"quarry_query", "quarry_list_documents"} {
		args := map[string]any{"collection": "archive"}
		if name == "quarry_query" {
			args["query"] = "archive"
		}
		result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
		require.NoError(t, err)
		require.False(t, result.IsError)
	}
	require.Equal(t, int32(1), queries.Load())
	require.Equal(t, int32(1), lists.Load())
}

func TestQueryToolRespectsEvidencePolicy(t *testing.T) {
	const docID = "00000000-0000-4000-8000-000000000001"
	const evidence = "Fold the paper. <b>Sample</b> [label](#section)"
	emb := embedder.NewFakeEmbedder(8)
	embedding, err := emb.Embed(t.Context(), "Archive?")
	require.NoError(t, err)
	store := memory.NewFakeVectorStore()
	require.NoError(t, store.Upsert(t.Context(), "", []domain.Chunk{{ID: "chunk", DocumentID: docID, Index: 2, Text: evidence, Embedding: embedding, Metadata: map[string]any{"doc_type": "guide"}}}))
	server := httptest.NewServer(serviceapi.NewRouter(&serviceapi.RouterDeps{
		Config:      &serviceconfig.Config{QueryAnswerPolicy: "evidence_only", ServiceToken: "token"},
		VectorStore: store,
		Retriever:   retriever.NewRetriever(store, emb, .7, 2),
	}))
	defer server.Close()
	cs := connectServer(t, config{baseURL: server.URL, token: "token", timeout: time.Second})

	for _, mode := range []string{"evidence", "", "generated"} {
		t.Run(mode, func(t *testing.T) {
			args := map[string]any{"query": "Archive?", "mode": "global", "allowed_document_ids": []string{docID}}
			if mode != "" {
				args["answer_mode"] = mode
			}
			result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: args})
			require.NoError(t, err)
			fallback := result.Content[0].(*mcp.TextContent).Text
			if mode == "generated" {
				require.True(t, result.IsError)
				require.Contains(t, fallback, "403 forbidden")
				require.Nil(t, result.StructuredContent)
				return
			}
			require.False(t, result.IsError, fallback)
			require.NotContains(t, fallback, "Fold the paper")
			data, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			var got queryOutput
			require.NoError(t, json.Unmarshal(data, &got))
			require.Empty(t, got.Answer)
			require.Equal(t, "evidence_only", got.FinishReason)
			require.Zero(t, got.TokenUsage)
			require.Nil(t, got.Citations)
			require.Len(t, got.Chunks, 1)
			require.Equal(t, docID, got.Chunks[0].DocID)
			require.Equal(t, 2, got.Chunks[0].ChunkIndex)
			require.Equal(t, evidence, got.Chunks[0].Text)
		})
	}
}

func TestQueryToolReportsEmbeddingInputLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "embedding.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"provider":"openai","model":"embed","revision":"r1","max_characters":6}`), 0o600))
	profile, err := embeddinginput.Load(path, embeddinginput.Target{Provider: "openai", Model: "embed"})
	require.NoError(t, err)
	store := memory.NewFakeVectorStore()
	server := httptest.NewServer(serviceapi.NewRouter(&serviceapi.RouterDeps{
		Config:      &serviceconfig.Config{ServiceToken: "token"},
		VectorStore: store,
		Retriever:   retriever.NewRetriever(store, embedder.NewFakeEmbedder(8), .7, 2, retriever.WithEmbeddingInputProfile(profile)),
	}))
	defer server.Close()
	cs := connectServer(t, config{baseURL: server.URL, token: "token", timeout: time.Second})
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: map[string]any{"query": "archive details"}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Nil(t, result.StructuredContent)
	require.Contains(t, result.Content[0].(*mcp.TextContent).Text, "400 embedding_input_too_long:")
}

func TestQueryToolStrictBudgetFields(t *testing.T) {
	for _, tc := range []struct {
		name, extraJSON, wantMethod string
		wantTokens                  int
		wantPresent                 bool
	}{
		{"strict", `,"count_method":"openai_cl100k_chat","model_input_tokens":512`, "openai_cl100k_chat", 512, true},
		{"legacy", "", "", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, err := io.WriteString(w, `{"answer":"Ok","finish_reason":"stop","mode":"LOCAL","chunks_used":1,"context_chunks":1,"token_usage":{"input_tokens":512,"output_tokens":4,"total_tokens":516},"scored_chunks":[]`+tc.extraJSON+`}`)
				assert.NoError(t, err)
			}))
			defer s.Close()
			cs := connectServer(t, config{baseURL: s.URL, timeout: time.Second})
			res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_query", Arguments: map[string]any{"query": "archive"}})
			require.NoError(t, err)
			require.False(t, res.IsError)
			data, err := json.Marshal(res.StructuredContent)
			require.NoError(t, err)
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &raw))
			if !tc.wantPresent {
				assert.NotContains(t, raw, "count_method")
				assert.NotContains(t, raw, "model_input_tokens")
				return
			}
			var got queryOutput
			require.NoError(t, json.Unmarshal(data, &got))
			assert.Equal(t, tc.wantMethod, got.CountMethod)
			assert.Equal(t, tc.wantTokens, got.ModelInputTokens)
		})
	}
}
