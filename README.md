# Quarry

[![CI](https://github.com/Victoroide/quarry-rag/actions/workflows/ci.yml/badge.svg)](https://github.com/Victoroide/quarry-rag/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Victoroide/quarry-rag/actions/workflows/codeql.yml/badge.svg)](https://github.com/Victoroide/quarry-rag/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Victoroide/quarry-rag/badge)](https://scorecard.dev/viewer/?uri=github.com/Victoroide/quarry-rag)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Victoroide/quarry-rag)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/Victoroide/quarry-rag.svg)](https://pkg.go.dev/github.com/Victoroide/quarry-rag)
[![Release](https://img.shields.io/github/v/release/Victoroide/quarry-rag)](https://github.com/Victoroide/quarry-rag/releases/latest)

Quarry (`quarry-rag`) is a domain-agnostic retrieval-augmented generation (RAG) microservice written in Go. It provides HTTP endpoints for backend applications and distributed services to ingest textual documents (raw JSON or multipart PDF, DOCX, DOC files), generate vector embeddings, persist document metadata, and answer natural-language queries grounded in the indexed corpus.

## Features

- **Durable document ingestion**: Accepts text or PDF, DOCX, DOC and UTF-8 or UTF-16 plain text uploads into PostgreSQL before responding, with content verification, atomic deduplication, parallel processing, leased retries and restart recovery.
- **Adaptive retrieval**: Evaluates query complexity and similarity signals to route between LOCAL and GLOBAL vector search in Pinecone.
- **Tenant and document filtering**: Supports metadata filtering by document type and query-time document ID allowlists enforced before synthesis.
- **Grounded synthesis**: Generates answers in the language of the incoming question using AWS Bedrock models when evidence is available; see the [query API](docs/api.md#post-query) for empty results.
- **Offline development mode**: Runs fully offline with deterministic in-memory vector storage and mock model providers via `GO_RAG_FAKE_MODE`.
- **Service authentication**: Restricts operational and query routes with constant-time `X-Service-Token` verification.
- **Tracing**: optional OpenTelemetry export over OTLP/HTTP, including LangSmith, for requests, pipeline stages, model calls and storage operations.
- **MCP server**: `quarry-mcp` exposes ingestion, queries and document management to MCP clients over stdio.

## Architecture Overview

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/diagrams/system-architecture-dark.svg">
  <img alt="Quarry service architecture. The Go client and local stdio MCP server call the HTTP API. Public probes serve GET /health, GET /livez and GET /readyz. Token-protected routes serve ingestion, queries and document operations. Ingestion normally extracts binary uploads in a sandbox, decodes text uploads in process, optionally saves originals in S3, and accepts documents and durable jobs in PostgreSQL. The ingestion processor claims a job, chunks text, embeds with Bedrock Titan V2, persists vectors in Pinecone, then completes the job and saves chunks in PostgreSQL. Queries decompose, route LOCAL or GLOBAL with Bedrock embeddings, retrieve scoped visible evidence from Pinecone, and synthesize with Bedrock Converse. A representative ingestion span flows through OpenTelemetry to optional OTLP and LangSmith exporters. Document handlers serve GET /documents, GET /documents/{doc_id}, GET /documents/{doc_id}/file and DELETE /documents/{doc_id}." src="docs/diagrams/system-architecture-light.svg">
</picture>

## Requirements

- Go at the version in [go.mod](go.mod) to build from source; the badge shows it, and [CONTRIBUTING.md](CONTRIBUTING.md) lists the tool versions.
- PostgreSQL 14 or later in every mode.
- Docker for the full test suite and image builds.
- For external services, a Pinecone index, AWS Bedrock access to the embedding and synthesis models, and an optional S3 bucket. [Deployment](docs/deployment.md) covers index settings, model identifiers and permissions.

## Installation

- **Container image:** `docker pull ghcr.io/victoroide/quarry-rag:<version>`, where `<version>` is a release number such as `0.1.0`, for `linux/amd64` and `linux/arm64`.
- **Release archives:** `quarry-server` for Linux and macOS, and `quarry-mcp` for Linux, macOS and Windows, each for `amd64` and `arm64`. [Releases](docs/deployment.md#releases) describes checksum and provenance verification.
- **MCP server:** install `quarry-mcp` with `go install` or from a release archive; see [Local MCP Server](docs/mcp.md#build-and-install).
- **From source:** `go build -o bin/server ./cmd/server` with the Go version in [go.mod](go.mod).

## Quick Start

### Offline Fake Mode

Offline fake mode executes the service without cloud credentials by using an in-memory vector store, deterministic mock embeddings, and local model synthesis. A PostgreSQL database is still required for schema migrations and metadata storage.

1. Start a local PostgreSQL instance:

```sh
docker run --detach --name rag-dev-db \
  --env POSTGRES_DB=rag \
  --env POSTGRES_USER=rag \
  --env POSTGRES_PASSWORD=rag \
  --publish 127.0.0.1:5432:5432 \
  postgres:16.13-alpine3.23
```

2. Run the service from source:

```sh
export GO_RAG_FAKE_MODE=true
export DATABASE_URL="host=127.0.0.1 port=5432 user=rag password=rag dbname=rag sslmode=disable"
export SERVICE_TOKEN="dev-token"
go run ./cmd/server
```

3. In another terminal, check readiness:

```sh
curl localhost:8081/readyz
```

The response is `{"status":"ready"}`.

### Docker Execution

To run the containerized application using Docker Compose:

1. Copy the example configuration:

```sh
cp .env.example .env
```

Set `PINECONE_INDEX_NAME=quarry-rag` to the name of your existing index. External-service mode requires this value and has no implicit default; offline mode does not need it. Set `S3_BUCKET=your-owned-bucket` in `.env` for original file storage, and optionally set `S3_EXPECTED_BUCKET_OWNER` to the owner's 12-digit AWS account ID. Leaving `S3_BUCKET` empty disables original file storage.

2. Start the container in detached mode:

```sh
docker-compose up --build -d
```

## Usage

### Ingest a Document

```sh
curl -X POST localhost:8081/ingest \
  -H "X-Service-Token: dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "content": "The storage engine distributes records across replicated partitions for fault tolerance.",
    "doc_type": "technical_spec"
  }'
```

### Check Ingestion Status

Replace `<document_id>` with the identifier returned by ingestion:

```sh
curl "localhost:8081/documents/<document_id>" -H "X-Service-Token: dev-token"
```

Ingestion is asynchronous: query after `status` changes from `processing` to `completed`; failed documents report `last_error`.

### Query the Corpus

```sh
curl -X POST localhost:8081/query \
  -H "X-Service-Token: dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "How does the storage engine achieve fault tolerance?",
    "mode": "auto"
  }'
```

## Configuration

The service reads configuration only from environment variables. [Configuration](docs/configuration.md) lists every variable with its default and bounds, and [.env.example](.env.example) is a starting point for Docker Compose.

## Operations

`GET /livez` and `GET /readyz` serve liveness and readiness probes, and `GET /health` reports dependency status. Setting `METRICS_ADDR` starts a separate Prometheus-format metrics listener. Logs are JSON on stderr and carry each request's `X-Request-ID`. [Deployment](docs/deployment.md) covers logging, probes, metrics, shutdown and token rotation, and [Tracing](docs/deployment.md#tracing) covers optional trace export, including LangSmith, sampling and content capture.

## Limits

JSON ingestion accepts at most 1,000,000 bytes of decoded content. Its request-body cap is 8,097,152 bytes, allowing sixfold JSON escaping plus metadata overhead; queries accept a 1 MiB body. Every upload format enforces the same decoded-text limit. PDF extraction also enforces an 8 MiB decoded-stream budget. Oversized bodies return 413; extraction-limit failures return 422.

The runtime image uses numeric UID/GID `10001:10001`. A failed automatic routing probe falls back to GLOBAL retrieval.

## Limitations

- **Scanned and image-only PDFs need external OCR.** The extractor reads embedded text and returns 422 with `no_text` when it finds no readable text; no OCR engine is included. Extract text before submitting these documents.
- **PDF layout fidelity depends on the parser.** Plain-text extraction does not reconstruct table structure or guarantee reading order and spacing in complex layouts. Check extracted content when layout carries meaning.
- **Encrypted DOC files and Word 6/95 binaries are unsupported.** The DOC reader returns 422 with `encrypted` or `unsupported_version`, and the API accepts no decryption password. Convert these documents before uploading them.
- **New documents can take a moment to become searchable.** Completion waits up to [`INDEX_VISIBILITY_TIMEOUT`](docs/configuration.md#environment-variables) for the hosted index to return a document's last written vector. If the wait ends first, the document completes anyway, the service logs a warning and counts it, and the document becomes searchable once the index catches up. After deletion, the PostgreSQL visibility check excludes the document from new answers even if vectors remain in the index temporarily.
- **Ingestion processing is at least once.** Expired leases can cause an attempt to repeat, and a crash between external writes and cleanup can leave orphaned vectors. PostgreSQL and the vector index do not share a transaction; vectors without a completed document are excluded from answers. With ingestion paused, `make reconcile` previews eligible cleanup; deletion requires `ARGS="-apply"` and preserves processing documents and retryable jobs.
- **Prompt injection remains possible.** Excerpt delimiters, character sanitization and synthesis instructions reduce exposure, but hostile text in authorized documents can still influence generated answers.
- **Token counts are estimates.** The service uses text heuristics instead of a provider tokenizer. Chunk token estimates and the 30,000-byte chunk cap bound input size, but do not guarantee exact token counts or acceptance by every configured provider.
- **Individual-user authorization belongs to the caller.** Service credentials authenticate trusted applications. Those applications must supply each user's `allowed_document_ids`; `REQUIRE_DOCUMENT_ALLOWLIST` can require that field, but the service does not determine which documents a user may read.
- **Content deduplication can reveal that identical content exists within a collection.** A request without `document_id` can return any document in its `collection` with identical content, including one created with an explicit `document_id`. Give each tenant or user its own collection when content must stay private between them.
- **Concurrency limits apply per replica.** Query admission, embedding and extraction limits are process-local, so total concurrency grows with the replica count. Configure replica counts, retries and per-process limits to fit upstream quotas.
- **The no-evidence answer covers six languages.** Empty results skip synthesis and return a fixed message in English, Spanish, Portuguese, French, German or Italian, chosen from `Accept-Language`; other languages receive English unless closely related to a supported language. See the [query API](docs/api.md#post-query).
- **Document allowlists have size limits.** `MAX_ALLOWED_DOCUMENT_IDS` defaults to 25,000 entries before deduplication, and the 1 MiB query-body cap also applies. These bounds limit request size and the number of vector queries needed for large scopes.
- **Offline mode does not measure hosted behavior.** Its vector writes are immediately consistent and its answer generator echoes prompt context. It exercises contracts and storage limits, while answer quality and hosted-index latency require external-service testing.

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | Package organization, sequence diagrams, database entity-relationship model, and runtime lifecycles |
| [API Reference](docs/api.md) | Route definitions, authentication headers, request and response structures, and HTTP status codes |
| [MCP Server](docs/mcp.md) | Local stdio command, client setup, tools and Go client package |
| [Configuration](docs/configuration.md) | Environment variables, validation constraints, default parameters, and database schema migrations |
| [Deployment](docs/deployment.md) | Container image build, non-root user execution, external service requirements, and token rotation |
| [Development](docs/development.md) | Repository directory structure, unit and integration test suites, continuous integration workflows, operational scripts, and fake mode mechanics |

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before opening a pull request.

With the prerequisites listed in CONTRIBUTING.md available, run the full host checks and build:

```sh
make ci
```

`make test` runs the full race suite and needs a Docker daemon for isolated database tests. The Docker `test` stage runs the short suite without a daemon. Delete explicit documents with `make cleanup IDS="<uuid> <uuid>"`.

## Security

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md), which also states the deployment boundaries the service relies on.

## License

[MIT](LICENSE)
