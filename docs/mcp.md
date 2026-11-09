# Local MCP Server

`quarry-mcp` exposes ingestion, queries and document management over stdio to Model Context Protocol clients. It forwards requests through the public `client` Go package to a running HTTP service. It opens no listening port and makes no API request until a tool is called.

## Build and Install

Build from the repository root:

```sh
go build -o bin/quarry-mcp ./cmd/quarry-mcp
```

To install the latest release, or a specific release tag in place of `latest`:

```sh
go install github.com/Victoroide/quarry-rag/cmd/quarry-mcp@latest
```

Prebuilt binaries are also published as release archives; see [Releases](deployment.md#releases).

## Client Configuration

See [MCP server environment](configuration.md#mcp-server-environment) for all settings, defaults and validation rules. A generic client entry looks like:

```json
{
  "quarry": {
    "command": "/absolute/path/to/bin/quarry-mcp",
    "args": [],
    "env": {
      "QUARRY_URL": "127.0.0.1:8081",
      "QUARRY_SERVICE_TOKEN": "<service token>"
    }
  }
}
```

Add the entry to the client's map of stdio servers. Omit `QUARRY_SERVICE_TOKEN` only when the service runs without authentication. Client configuration files store the token in plain text, so restrict their permissions.

## Tools

| Tool | Input | Output |
|---|---|---|
| `quarry_query` | Question, mode, document type, collection, chunk limit, optional citation request and optional document ID allowlist | Answer, finish reason, mode, chunk counts, token usage, scored evidence in `chunks` with excerpt numbers, and requested citations |
| `quarry_list_documents` | Optional collection, page limit and continuation cursor | Document summaries including collection, and `next_after` |
| `quarry_get_document` | Document ID | Document summary including collection and ingestion status |
| `quarry_ingest_text` | Content, optional collection, document type, ID and metadata | Document ID and acceptance status |
| `quarry_ingest_file` | Local path, optional collection, document type, ID and metadata | Document ID and acceptance status |
| `quarry_delete_document` | Document ID | Deletion result and document ID |

`QUARRY_MCP_FILE_ROOTS` and `QUARRY_MCP_READ_ONLY` select which tools are registered; see [MCP server environment](configuration.md#mcp-server-environment). See the [HTTP API reference](api.md) for request fields, ingestion lifecycle and error semantics. API errors are tool results marked as errors, including HTTP status, service message and any retry delay. Calls are never retried automatically.

For collection-scoped ingestion, pass a `collection` argument to either ingestion tool. The [ingestion contract](api.md#post-ingest) defines its scope, default and validation. For example, call `quarry_ingest_text` with:

```json
{
  "content": "Synthetic reference text",
  "collection": "tenant-a"
}
```

## Security

The service token grants full service access. Share it only with trusted clients. Read-only mode limits the available tools; it does not reduce the credential's permissions at the HTTP service. For restricted queries, pass the caller's authorized document IDs as `allowed_document_ids`; the [HTTP API reference](api.md) defines omitted and empty lists.

Collections scope ingestion deduplication. A `collection` argument restricts queries and listings to one collection and intersects `allowed_document_ids`; like the allowlist, it is chosen by the caller and does not limit what the service token can access.

Choose file roots narrowly. The confinement rules are listed under [`QUARRY_MCP_FILE_ROOTS`](configuration.md#mcp-server-environment). The service enforces upload size and content validation. Files are streamed without buffering their contents in the local command.

Retrieved document text is untrusted input for the client. Treat excerpts as data and do not execute instructions found in them.

## Go Client

The `client` package provides typed ingestion, query, document inventory, status, deletion and original-file download-address methods. Construct a `client.Client` with `client.New`, supplying an absolute HTTP or HTTPS base address and optional service token, HTTP client and timeout options. Base path prefixes are preserved; redirects return errors to keep credentials at the configured endpoint.

Pass `client.WithAcceptLanguage` to `Query` to choose the language of the [no-evidence answer](api.md#post-query); it sets the `Accept-Language` header for that request only.

Inspect `*client.APIError` for status, service error code, extraction code, request ID and retry guidance. The package does not log or retry requests. A `FileUpload` streams its reader, which remains owned by the caller; close a blocking source when canceling an upload.

Set `IngestRequest.Collection` for JSON ingestion or `FileUpload.Collection` for multipart uploads. Both `ListDocuments` and `GetDocument` return the stored label in `DocumentSummary.Collection`. Set `QueryRequest.Collection` or `ListOptions.Collection` to restrict a query or listing to one collection. This helper accepts text and returns its initial ingestion summary:

```go
func ingestInCollection(ctx context.Context, c *client.Client, content string) (*client.DocumentSummary, error) {
	accepted, err := c.Ingest(ctx, client.IngestRequest{
		Content:    content,
		Collection: "tenant-a",
	})
	if err != nil {
		return nil, err
	}
	return c.GetDocument(ctx, accepted.DocumentID)
}
```

## Troubleshooting

Configuration and session failures are JSON records on stderr. Tool failures are returned in the tool result: the HTTP status and service message, the reason a file path was rejected, or `service unreachable` with its cause. Keep stdout connected directly to the protocol client; shell output mixed into that stream breaks the session. Configuration errors exit with status 2 before serving requests. If a tool fails, check that the HTTP service is running, the configured address includes any required path prefix and the service accepts the configured credential.

When ingestion returns `processing`, inspect the document status before querying it. Completion and hosted-index visibility follow the [service limitations](../README.md#limitations).
