package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/testutil/grantse2e"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

const grantsStdioHelper = "QUARRY_TEST_GRANTS_STDIO_HELPER"

func TestGrantsStdioHelper(t *testing.T) {
	if os.Getenv(grantsStdioHelper) != "1" {
		return
	}
	os.Exit(run())
}

func TestMCPStdioGrantsThroughPostgres(t *testing.T) {
	for _, backend := range grantse2e.Backends {
		t.Run(backend, func(t *testing.T) {
			f := grantse2e.New(t, backend)
			docs := f.Seed(t)
			root := t.TempDir()
			filePath := filepath.Join(root, "private-mcp-source.txt")
			const fileText = "The indigo dispatch register is filed by week."
			const textContent = "The red dispatch register is filed by month."
			require.NoError(t, os.WriteFile(filePath, []byte(fileText), 0o600))
			owner := startGrantsStdio(t, f, f.Owner, 0, root)
			forbidden := startGrantsStdio(t, f, f.Other, 1, root)

			toolNames := []string{
				"quarry_ingest_text", "quarry_ingest_file", "quarry_list_documents",
				"quarry_get_document", "quarry_query", "quarry_delete_document",
			}
			for _, process := range []*grantsStdio{owner, forbidden} {
				var tools mcp.ListToolsResult
				process.request(t, "tools/list", nil, &tools)
				names := make([]string, 0, len(tools.Tools))
				for _, tool := range tools.Tools {
					names = append(names, tool.Name)
				}
				require.ElementsMatch(t, toolNames, names)
			}

			text := textInput{Content: textContent, Collection: grantse2e.Collection}
			file := fileInput{Path: filePath, Collection: grantse2e.Collection, DocType: grantse2e.FileType}
			textAccepted := toolOutput[client.IngestResponse](t, owner.tool(t, toolNames[0], text))
			fileAccepted := toolOutput[client.IngestResponse](t, owner.tool(t, toolNames[1], file))
			require.Equal(t, "processing", textAccepted.Status)
			require.Equal(t, "processing", fileAccepted.Status)
			textDoc := f.Wait(t, f.Owner, 0, textAccepted.DocumentID, textContent)
			fileDoc := f.Wait(t, f.Owner, 0, fileAccepted.DocumentID, fileText)

			for _, input := range []struct {
				tool string
				args any
			}{{toolNames[0], text}, {toolNames[1], file}} {
				requireToolError(t, forbidden.tool(t, input.tool, input.args), "403 forbidden: access denied")
			}

			list := listInput{Limit: 20}
			requireStdioPage(t, toolOutput[listOutput](t, owner.tool(t, toolNames[2], list)),
				docs.First.ID, docs.Second.ID, docs.Text.ID, textDoc.ID, fileDoc.ID)
			requireStdioPage(t, toolOutput[listOutput](t, forbidden.tool(t, toolNames[2], list)))

			get := getDocumentInput{documentInput: documentInput{DocumentID: fileDoc.ID}}
			got := toolOutput[client.DocumentSummary](t, owner.tool(t, toolNames[3], get))
			require.Equal(t, fileDoc.ID, got.ID)
			require.Equal(t, "completed", got.Status)
			requireToolError(t, forbidden.tool(t, toolNames[3], get), "404 not_found: document not found")
			foreign := getDocumentInput{documentInput: documentInput{DocumentID: docs.Foreign.ID}}
			requireToolError(t, owner.tool(t, toolNames[3], foreign), "404 not_found: document not found")

			query := queryInput{Query: grantse2e.Prompt, Mode: "global",
				AnswerMode: client.AnswerModeEvidence, AllowedDocumentIDs: []string{fileDoc.ID, docs.Foreign.ID}}
			gotQuery := toolOutput[queryOutput](t, owner.tool(t, toolNames[4], query))
			require.Equal(t, client.FinishReasonEvidenceOnly, gotQuery.FinishReason)
			require.Len(t, gotQuery.Chunks, 1)
			require.Equal(t, fileDoc.ID, gotQuery.Chunks[0].DocID)
			require.Equal(t, fileText, gotQuery.Chunks[0].Text)
			require.Empty(t, toolOutput[queryOutput](t, forbidden.tool(t, toolNames[4], query)).Chunks)

			del := deleteInput{documentInput: documentInput{DocumentID: fileDoc.ID}, Incarnation: &fileDoc.Incarnation}
			requireToolError(t, forbidden.tool(t, toolNames[5], del), "404 not_found: document not found")
			f.Grant(t, f.Other, true, false, fileDoc)
			requireStdioPage(t, toolOutput[listOutput](t, forbidden.tool(t, toolNames[2], list)), fileDoc.ID)
			require.Equal(t, fileDoc.ID, toolOutput[client.DocumentSummary](t, forbidden.tool(t, toolNames[3], get)).ID)
			f.Grant(t, f.Other, false, false, fileDoc)
			requireStdioPage(t, toolOutput[listOutput](t, forbidden.tool(t, toolNames[2], list)))
			requireToolError(t, forbidden.tool(t, toolNames[3], get), "404 not_found: document not found")

			deleted := toolOutput[client.DeleteResponse](t, owner.tool(t, toolNames[5], del))
			require.True(t, deleted.Deleted)
			require.Equal(t, fileDoc.ID, deleted.DocID)
			f.RequireFinalized(t, fileDoc)
			f.RequireStoredVectors(t, fileDoc, false)
			forbidden.close(t)
			owner.close(t)
		})
	}
}

func requireStdioPage(t *testing.T, page listOutput, want ...string) {
	t.Helper()
	var ids []string
	for _, doc := range page.Documents {
		ids = append(ids, doc.ID)
	}
	require.ElementsMatch(t, want, ids)
	require.Nil(t, page.NextAfter)
}

type grantsStdio struct {
	cmd     *exec.Cmd
	in      io.WriteCloser
	out     *bufio.Reader
	stderr  *bytes.Buffer
	cancel  context.CancelFunc
	closed  bool
	id      int
	secrets []string
}

func startGrantsStdio(t *testing.T, f *grantse2e.Fixture, actor grantse2e.Actor, replica int, root string) *grantsStdio {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	credentialPath := filepath.Join(t.TempDir(), "subject-token")
	require.NoError(t, os.WriteFile(credentialPath, []byte(actor.Credential.Token), 0o600))
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestGrantsStdioHelper$")
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = append(cmd.Environ(), grantsStdioHelper+"=1",
		"QUARRY_URL="+f.Servers[replica].URL, "QUARRY_SERVICE_TOKEN="+f.ServiceToken,
		"QUARRY_SUBJECT_TOKEN_FILE="+credentialPath, "QUARRY_MCP_FILE_ROOTS="+root,
		"QUARRY_MCP_READ_ONLY=false", "QUARRY_TIMEOUT=12s")
	in, err := cmd.StdinPipe()
	require.NoError(t, err)
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	require.NoError(t, cmd.Start())
	proc := &grantsStdio{cmd: cmd, in: in, out: bufio.NewReader(out), stderr: stderr, cancel: cancel,
		secrets: []string{f.ServiceToken, actor.Credential.Token, actor.Subject, credentialPath}}
	t.Cleanup(func() {
		if !proc.closed {
			cancel()
			_ = proc.in.Close()
			_ = proc.cmd.Wait()
		}
	})
	var initialized mcp.InitializeResult
	proc.request(t, "initialize", map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{},
		"clientInfo": map[string]string{"name": "quarry-test", "version": "1"}}, &initialized)
	require.Equal(t, "quarry", initialized.ServerInfo.Name)
	require.NoError(t, json.NewEncoder(in).Encode(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}))
	return proc
}

func (p *grantsStdio) request(t *testing.T, method string, params any, response any) {
	t.Helper()
	p.id++
	message := map[string]any{"jsonrpc": "2.0", "id": p.id, "method": method}
	if params != nil {
		message["params"] = params
	}
	require.NoError(t, json.NewEncoder(p.in).Encode(message))
	line, err := p.out.ReadBytes('\n')
	require.NoError(t, err, "reading %s", method)
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	require.NoError(t, json.Unmarshal(line, &envelope))
	require.Equal(t, "2.0", envelope.JSONRPC)
	require.Equal(t, strconv.Itoa(p.id), string(envelope.ID))
	require.Empty(t, envelope.Error)
	require.NotEmpty(t, envelope.Result)
	require.NoError(t, json.Unmarshal(envelope.Result, response))
}

func (p *grantsStdio) tool(t *testing.T, name string, args any) mcp.CallToolResult {
	t.Helper()
	var result mcp.CallToolResult
	p.request(t, "tools/call", map[string]any{"name": name, "arguments": args}, &result)
	return result
}

func (p *grantsStdio) close(t *testing.T) {
	t.Helper()
	require.NoError(t, p.in.Close())
	rest, readErr := io.ReadAll(io.LimitReader(p.out, 64<<10))
	if len(rest) != 0 {
		p.cancel()
	}
	err := p.cmd.Wait()
	p.closed = true
	p.cancel()
	require.NoError(t, readErr)
	require.Empty(t, rest, "stdout after protocol EOF")
	require.NoError(t, err)
	for _, secret := range p.secrets {
		require.NotContains(t, p.stderr.String(), secret)
	}
}

func toolOutput[T any](t *testing.T, result mcp.CallToolResult) T {
	t.Helper()
	require.False(t, result.IsError, "%v", result.Content)
	require.NotNil(t, result.StructuredContent)
	body, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	var output T
	require.NoError(t, json.Unmarshal(body, &output))
	return output
}

func requireToolError(t *testing.T, result mcp.CallToolResult, want string) {
	t.Helper()
	require.True(t, result.IsError)
	require.Nil(t, result.StructuredContent)
	require.Len(t, result.Content, 1)
	text, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	require.Equal(t, want, text.Text)
}
