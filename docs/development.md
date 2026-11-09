# Development

For contribution guidelines, environment setup, Conventional Commits formatting, and pull request procedures, refer to CONTRIBUTING.md. For vulnerability reporting procedures, refer to SECURITY.md.

---

## Repository Layout

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="diagrams/repository-layout-dark.svg">
  <img alt="Repository layout, from the directory listing to depth three. client/ Go client for the HTTP API. cmd/ Executable entry points. cmd/quarry-mcp/ Local MCP server over stdio. cmd/server/ Wiring and graceful shutdown. docs/ Service documentation. docs/diagrams/ Architecture, schema and layout. docs/diagrams/src/ Diagram placement sources. internal/ Private service packages. internal/api/ HTTP router. internal/api/handler/ Request handlers. internal/api/middleware/ Authentication, logs and metrics. internal/awsconf/ AWS credentials and retry policy. internal/chunker/ Text windows and fingerprints. internal/cleanup/ Document deletion across stores. internal/config/ Configuration and validation. internal/domain/ Shared types and interfaces. internal/embedder/ Titan V2 and offline embeddings. internal/extractor/ File parsing and process isolation. internal/llm/ Answer routing and Converse. internal/logctx/ Request log attributes. internal/metrics/ Prometheus text metrics. internal/ocr/ Asynchronous PDF text detection. internal/pipeline/ Ingestion and query stages. internal/retriever/ LOCAL and GLOBAL retrieval. internal/store/ Storage adapters. internal/store/memory/ Offline vector index. internal/store/pinecone/ Hosted vector index. internal/store/postgres/ Documents, chunks and durable jobs. internal/store/s3/ Original files and signed downloads. internal/testutil/ Test support. internal/testutil/pgtest/ Disposable database containers. internal/textutil/ Unicode normalization. internal/tracing/ OpenTelemetry export and span helpers. internal/worker/ Leased ingestion and retry handling. scripts/ Operational commands. scripts/cleanup/ Delete selected documents. scripts/langeval/ Answer language and latency checks. scripts/reconcile/ Find and remove orphan vectors. scripts/simulate/ Exercise the service API. scripts/validate/ Check retrieval behavior. test/ Service test suites. test/integration/ Database and API integration. test/smoke/ Opt-in live service checks. test/unit/ Package behavior checks. tools/ Documentation support. tools/diagrams/ Vector image sources and checks. tools/diagrams/fonts/ Bundled typefaces and license. The highlight follows cmd/server, internal/api, internal/pipeline, internal/retriever, internal/llm and internal/store." src="diagrams/repository-layout-light.svg">
</picture>

The tree includes directories containing tracked files through depth three and omits `.git`, `.github`, `vendor` and `testdata`. The highlight follows the request path through the entry point, API, pipeline, retrieval, answer synthesis and storage packages.

Regenerate and check the images after changing routes, schema or tracked directories:

```sh
make diagrams
make diagrams-check
```

Placement is recorded in `docs/diagrams/src/`, with content and validation in `tools/diagrams/`. The checks cover source drift, geometry, text, contrast and reproducible output. The outlined images respect the reader's reduced-motion preference.

---

## Test Suites

The tests are organized into four suites:

Host targets mirror the full validation path:

```sh
make vet
make lint
make test
make ci
```

`make ci` runs vet, lint, the full race suite and the diagram check in order, then builds the runtime image. `docker build --target test .` runs the short suite without requiring a nested Docker daemon.

### 1. Unit Tests

Unit tests cover domain functions, data parsing, HTTP behavior and pipeline logic using local implementations. They live beside the packages under `internal/` as well as in `test/unit/`. The short suite runs these tests and skips database-backed integration tests:

```sh
go test -short -race -count=1 ./...
```

Scope of unit tests:
- **Chunker (`chunker_test.go`)**: Validates word window boundaries, token approximation formulas, HTML sanitization, and SHA-256 fingerprint generation.
- **Extractor (`test/unit/extractor_test.go`, `internal/extractor/*_test.go`, `internal/api/handler/ingest_detect_test.go`)**: Covers synthetic PDF, DOCX and plain text, content detection and extension mismatches, stable extraction error codes, decoded-text and XML limits, parser isolation, and cancellation.
- **DOC (`internal/extractor/doc_test.go`)**: Covers Word 97–2003 piece tables, Windows-1252 and UTF-16 text, nested field codes, corrupt structures, encrypted and older files, and decoded-text limits.
- **Fake Mode (`fakemode_test.go`)**: Verifies deterministic in-memory vector storage, mock embeddings, and fake converser outputs.
- **Named Entities (`named_entity_test.go`)**: Tests proper noun phrase extraction, stop-word filtering, and Unicode capitalization handling.
- **Retriever (`test/unit/retriever_test.go`, `internal/retriever/retriever_test.go`, `internal/pipeline/local_test.go`)**: Covers scoped retrieval, document selection, similarity thresholds and the LOCAL top-up when fewer than three chunks are returned.
- **Router (`router_test.go`)**: Validates model selection decisions across task types, context token thresholds, and synthesis modes.

### 2. Integration Tests

Integration tests verify database persistence, transactional rollbacks, schema migrations, and pipeline execution. Tests in `test/integration/`, `internal/store/postgres/` and `scripts/reconcile/postgres_test.go` use Testcontainers to start isolated PostgreSQL containers automatically during execution.

```sh
go test -race -count=1 ./test/integration/...
go test -race -count=1 ./internal/store/postgres/...
```

Docker must be running locally to permit Testcontainers to manage the ephemeral PostgreSQL database instance. Database-backed tests skip under `go test -short` with an explicit daemon requirement message; CI runs the full suite without `-short`. The offline query test ingests through HTTP, polls document status until completion, and verifies cited evidence and document restrictions through the router.

### 3. Smoke Tests

Smoke tests run against live, deployed instances and external cloud services (AWS Bedrock, Pinecone, PostgreSQL, and S3). Smoke tests are excluded by default to avoid unexpected cloud consumption or accidental mutation of configured datasets. Accepted documents already have persisted metadata; bounded deferred deletion also removes asynchronous ingestion jobs.

To opt in to smoke testing against a configured service:

```sh
RUN_SMOKE=1 go test ./test/smoke/... -count=1 -v -timeout 120s
```

### 4. Fuzz Tests

Fuzz targets cover format detection, document extraction, chunking, prompt sanitization, prompt markers and JSON request decoding. Their seed corpora execute as part of `go test ./...`. To explore generated inputs for every target locally:

```sh
make fuzz FUZZTIME=2m
```

Failing inputs are saved under the affected package's `testdata/fuzz/<Target>/` directory. Keep each reproducer with its fix so ordinary tests also check that regression.

---

## Continuous Integration

| Workflow | Trigger | Checks |
|---|---|---|
| [CI](../.github/workflows/ci.yml) | Pushes to `main` and pull requests | Lint; Test with race detection; Build service and runtime image; Check reachable vulnerabilities; Check vector diagrams. |
| [CodeQL](../.github/workflows/codeql.yml) | Pushes to `main`, pull requests targeting `main` and weekly | Analyze Go for security issues. |
| [Dependency review](../.github/workflows/dependency-review.yml) | Pull requests | Review dependency changes; reject vulnerabilities of high severity or above. |
| [Scorecard](../.github/workflows/scorecard.yml) | Pushes to `main`, weekly, branch protection changes and manual dispatch | Publish Scorecard results, retain the SARIF artifact for five days and upload it to code scanning. |
| [Release](../.github/workflows/release.yml) | Version tags matching `v*.*.*` | Builds archives and the runtime image, signs provenance, and publishes the release only after both builds and attestations succeed. |
| [Release check](../.github/workflows/release-check.yml) | Pull requests changing Go sources, module dependencies, the Dockerfile or release scripts and workflows | Checks script behavior, builds archives and verifies checksums and bundled licenses. |
| [Fuzz](../.github/workflows/fuzz.yml) | Weekly and manual dispatch | Fuzz each target separately; upload failing inputs as artifacts retained for 14 days. |

## Releases

Before publishing `vX.Y.Z`, move the entries under `## [Unreleased]` in
`CHANGELOG.md` to `## [X.Y.Z] - YYYY-MM-DD` and leave a new empty Unreleased
section above it. Merge that change, then create and push an annotated tag:

```sh
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

The release workflow publishes nothing, neither the image nor the release, for
tags whose CHANGELOG section is missing or empty.
Tags containing a hyphen publish a prerelease. Asset names and verification
commands are documented in [Deployment](deployment.md#releases).

To check packaging locally without publishing:

```sh
bash .github/scripts/release-test.sh
.github/scripts/build-release.sh v0.0.0-check "$(mktemp -d)"
actionlint .github/workflows/release.yml .github/workflows/release-check.yml
```

---

## Operational Scripts

The `scripts/` directory contains standalone administrative and evaluation tools:

### Document Cleanup (`scripts/cleanup`)

Accepts one or more document UUIDs as command-line arguments and removes their records from the configured vector backend, S3, and PostgreSQL using one shared set of clients:

```sh
make cleanup IDS="<document-uuid-1> <document-uuid-2>"
```

### Orphan Reconciliation (`scripts/reconcile`)

Use the service environment configuration, including `DATABASE_URL`,
`PINECONE_API_KEY` and `PINECONE_INDEX_NAME`, with hosted mode enabled. The database
must already have its ingestion job migrations applied. Pause ingestion and other
index writers during reconciliation so document checks and namespace counts stay
valid through deletion. Always run a dry run first:

```sh
make reconcile
make reconcile ARGS="-limit=10000 -page=100"
```

The command prints JSON with `orphan_vectors`, `orphan_namespaces` and up to 20
sample identifiers prefixed with `vector:` or `namespace:`. Vector counts cover
the default namespace. A vector is selected if `doc_id` is missing or invalid,
the document row is missing, or its document has status `failed` with no
`ingest_jobs` row. Retryable failures and processing documents are preserved.
Only UUID-named legacy namespaces with no document row are selected; other
namespace names are ignored.

`-limit` defaults to 100000 and bounds selected default-namespace vectors plus
the vector counts of whole selected namespaces. A namespace larger than the
remaining budget is skipped, and an empty namespace consumes one budget entry.
The listing page size defaults to 100 and accepts 1–1000. Listing stops at the
end of the default namespace or when the selection budget is full; healthy
vectors do not consume that budget. Pod-based indexes that do not support
vector listing report the limitation on stderr and reconcile namespaces only.
Other read errors stop the command before deletion.

After reviewing the dry run, explicitly enable deletion with the same bounds:

```sh
make reconcile ARGS="-apply -limit=10000 -page=100"
```

Default-namespace IDs are deleted in batches of at most 1000. Legacy namespaces
are deleted as a whole. PostgreSQL rows and source objects are preserved. A
delete error stops further deletions and returns a nonzero exit status; earlier
successful deletions remain applied. Rerun the dry run after the index has
converged to verify the remaining selections.

### Collection Metadata Backfill

For documents ingested with a collection before collection filtering was available,
label their existing default-namespace vectors once after upgrading. Until then,
collection-filtered queries omit those documents without an error, while
collection-filtered listings, which read PostgreSQL, already include them. Pause
ingestion and other index writers, then inspect the selected documents:

```sh
make reconcile ARGS="-backfill-collections"
make reconcile ARGS="-backfill-collections -apply"
```

This mode skips orphan cleanup. It selects documents with non-empty collections
in ascending UUID order and prints their `document_id` and `collection` in a
`documents` array, plus `updated_documents`. A dry run reads only PostgreSQL and
makes no index calls. With `-apply`, each document updates vectors whose `doc_id`
exactly matches its identifier, setting only the `collection` metadata key.
The `-limit` flag bounds documents in this mode (100000 by default); `-page` has
no effect. Each run stops after 30 minutes. To continue after `-limit`, an
interruption or an error, pass `-after` with the last printed `document_id` or
the value named in the error; without it, a run starts at the first identifier.
Reapplying the same label is idempotent.

An update error stops further updates and returns a nonzero exit status; earlier
successful updates remain applied. Metadata updates are eventually consistent,
so allow them to propagate before relying on collection-filtered queries.

### Language Evaluation (`scripts/langeval`)

Measures whether generated answers match the language of the input question (e.g., English or Spanish) across a fixed evaluation query set, recording token usage and latency:

```sh
go run ./scripts/langeval
go run ./scripts/langeval -tag baseline
go run ./scripts/langeval -tag after
go run ./scripts/langeval -compare baseline,after
```

### End-to-End Simulation (`scripts/simulate`)

Simulates a sequential series of client interactions against a running instance on port 8081, verifying health checks, JSON ingestion, duplicate detection, vector query modes, and document deletion:

```sh
go run ./scripts/simulate/main.go
```

### Retrieval Validation (`scripts/validate`)

Dispatches predefined queries to a running instance to verify that routing logic correctly assigns LOCAL or GLOBAL retrieval modes and returns non-empty result sets:

```sh
go run ./scripts/validate/main.go
```

---

## Offline Fake Mode Internals

Setting `GO_RAG_FAKE_MODE=true` allows the entire microservice and its HTTP endpoints to run locally without connecting to AWS Bedrock, Pinecone, or S3.

### Implementation Architecture

When `FakeMode` is active in `cmd/server/main.go`:
1. **Vector Store**: Instantiates `internal/store/memory.FakeVectorStore`, which stores vector embeddings in a thread-safe Go map (`sync.RWMutex`). Vector distance calculations use cosine similarity computed in memory.
2. **Embedding Provider**: Instantiates `internal/embedder.FakeEmbedder`, generating deterministic 1024-dimensional float arrays based on hashing input texts.
3. **LLM Converser**: Instantiates `internal/llm.FakeConverser`, returning structured responses without invoking external APIs.
4. **Object Storage**: S3 client initialization is skipped. Multipart file ingestion extracts text directly in memory and continues through chunking and indexing without storing the raw binary.
5. **Relational Storage**: PostgreSQL is still utilized via `DATABASE_URL` for real schema migrations, metadata storage, and duplicate fingerprint tracking.

Durable ingestion tests use disposable PostgreSQL databases and the offline vector store to cover restart recovery, terminal failures, deletion during processing, NUL handling and source upload failures. Lease and atomic acceptance tests live in `internal/store/postgres/jobs_test.go`; the short suite skips database-backed tests.

The offline vector store enforces the hosted index's per-record limits: flat metadata values must be strings, numbers, booleans or lists of strings, serialized vector metadata must fit within 40,960 bytes, and vector identifiers must fit within 512 bytes. The metadata size includes chunk text and the required fields sent by the hosted adapter. The first stored embedding fixes the index dimension across all namespaces; later embeddings must match. Filtered queries accept at most 10,000 results. Upserts have no per-call batch limit because the hosted adapter batches requests. Offline writes are immediately consistent; the hosted index is eventually consistent.
