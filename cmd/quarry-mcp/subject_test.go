package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func subjectFixture(expiry time.Time, version int) string {
	encode := base64.RawURLEncoding.EncodeToString
	return encode([]byte(`{"alg":"RS256","typ":"at+jwt","kid":"fixture"}`)) + "." +
		encode(fmt.Appendf(nil, `{"exp":%d,"jti":"version-%d"}`, expiry.Unix(), version)) + "." + encode([]byte("signature fixture"))
}

func replaceSubjectFile(t *testing.T, path, contents string, mode os.FileMode) {
	t.Helper()
	require.NoError(t, os.WriteFile(path+".new", []byte(contents), mode))
	require.NoError(t, os.Rename(path+".new", path))
}

func TestSubjectCredentialFileAcrossMCPTools(t *testing.T) {
	const service = "fixture-mcp-service"
	const doc = "00000000-0000-4000-8000-000000000001"
	dir := t.TempDir()
	file := filepath.Join(dir, "subject-token")
	path := filepath.Join(dir, "synthetic.md")
	syntheticFile(t, path)
	expires := time.Now().Add(3 * time.Minute)
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		subject := subjectFixture(expires, int(n))
		assert.Equal(t, "Bearer "+subject, r.Header.Get("Authorization"))
		assert.Equal(t, service, r.Header.Get("X-Service-Token"))
		assert.NotContains(t, r.URL.String(), subject)
		assert.NotContains(t, r.URL.String(), service)
		if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
			part, _, err := r.FormFile("file")
			if assert.NoError(t, err) {
				assert.NoError(t, part.Close())
				assert.NoError(t, r.MultipartForm.RemoveAll())
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		assert.NoError(t, json.NewEncoder(w).Encode(client.ErrorResponse{Error: "unauthorized", Message: service + " " + subject}))
	}))
	t.Cleanup(api.Close)
	cfg, err := loadConfig(func(key string) string {
		return map[string]string{"QUARRY_URL": api.URL, "QUARRY_SERVICE_TOKEN": service, "QUARRY_SUBJECT_TOKEN_FILE": file, "QUARRY_MCP_FILE_ROOTS": dir}[key]
	})
	require.NoError(t, err)
	cs := connectServer(t, cfg)
	listed, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 6)
	for _, tool := range listed.Tools {
		schema, err := json.Marshal(tool.InputSchema)
		require.NoError(t, err)
		for _, key := range []string{`"authorization"`, `"subject"`, `"subject_token"`, `"tenant_id"`, `"service_token"`} {
			require.NotContains(t, string(schema), key)
		}
	}
	for index, tool := range []struct {
		name string
		args map[string]any
	}{
		{"quarry_query", map[string]any{"query": "synthetic query"}},
		{"quarry_list_documents", map[string]any{}},
		{"quarry_get_document", map[string]any{"document_id": doc}},
		{"quarry_ingest_text", map[string]any{"content": "synthetic text"}},
		{"quarry_ingest_file", map[string]any{"path": path}},
		{"quarry_delete_document", map[string]any{"document_id": doc}},
	} {
		t.Run(tool.name, func(t *testing.T) {
			params := &mcp.CallToolParams{Name: tool.name, Arguments: tool.args}
			result, err := cs.CallTool(t.Context(), params)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Contains(t, result.Content[0].(*mcp.TextContent).Text, "subject credential unavailable")
			replaceSubjectFile(t, file, subjectFixture(time.Now().Add(-time.Minute), index), 0o600)
			result, err = cs.CallTool(t.Context(), params)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Contains(t, result.Content[0].(*mcp.TextContent).Text, "subject credential unavailable")
			require.EqualValues(t, index, calls.Load())
			token := subjectFixture(expires, index+1)
			replaceSubjectFile(t, file, token+"\n", 0o600)
			result, err = cs.CallTool(t.Context(), params)
			require.NoError(t, err)
			require.True(t, result.IsError)
			message := result.Content[0].(*mcp.TextContent).Text
			require.Contains(t, message, "401 unauthorized")
			require.NotContains(t, message, token)
			require.NotContains(t, message, service)
			require.EqualValues(t, index+1, calls.Load())
			require.NoError(t, os.Remove(file))
		})
	}
}

func TestSubjectFileBoundsAndFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subject-token")
	valid := subjectFixture(time.Now().Add(time.Minute), 1)
	for _, tc := range []struct{ name, content string }{
		{"empty", ""},
		{"oversized", strings.Repeat("x", 8195)},
		{"multiple tokens", valid + "\n" + valid},
		{"invalid", "not-a-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replaceSubjectFile(t, path, tc.content, 0o600)
			_, err := subjectTokenFile(path)(t.Context())
			require.ErrorIs(t, err, client.ErrSubjectCredential)
			require.NotContains(t, err.Error(), path)
		})
	}
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o700))
	_, err := subjectTokenFile(path)(t.Context())
	require.ErrorIs(t, err, client.ErrSubjectCredential)
	// Expiry is carried separately for the client to check again immediately before sending.
	other := filepath.Join(t.TempDir(), "fresh")
	replaceSubjectFile(t, other, valid+"\r\n", 0o600)
	got, err := subjectTokenFile(other)(t.Context())
	require.NoError(t, err)
	require.Equal(t, valid, got.Token)
	require.True(t, time.Now().Before(got.ExpiresAt))
}

func TestSubjectMCPRejectsRedirects(t *testing.T) {
	service := rand.Text()
	subject := subjectFixture(time.Now().Add(time.Minute), 1)
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetCalls.Add(1) }))
	t.Cleanup(target.Close)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, service, r.Header.Get("X-Service-Token"))
		assert.Equal(t, "Bearer "+subject, r.Header.Get("Authorization"))
		_, err := io.Copy(io.Discard, r.Body)
		assert.NoError(t, err)
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(api.Close)
	file := filepath.Join(t.TempDir(), "subject-token")
	replaceSubjectFile(t, file, subject, 0o600)
	cs := connectServer(t, config{baseURL: api.URL, token: service, subjectTokenFile: file})
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "quarry_list_documents", Arguments: map[string]any{}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Contains(t, result.Content[0].(*mcp.TextContent).Text, "307")
	require.Zero(t, targetCalls.Load())
}
