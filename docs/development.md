# Development

For contribution guidelines, environment setup, Conventional Commits formatting, and pull request procedures, refer to CONTRIBUTING.md. For vulnerability reporting procedures, refer to SECURITY.md.

---

## Repository Layout

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="diagrams/repository-layout-dark.svg">
  <img alt="Repository layout, from the directory listing to depth three. client/ Go client for the HTTP API. cmd/ Executable entry points. cmd/quarry-mcp/ Local MCP server over stdio. cmd/server/ Wiring and graceful shutdown. docs/ Service documentation. docs/diagrams/ Architecture, schema and layout. docs/diagrams/src/ Diagram placement sources. internal/ Private service packages. internal/api/ HTTP router. internal/api/handler/ Request handlers. internal/api/middleware/ Authentication, logs and metrics. internal/awsconf/ AWS credentials and retry policy. internal/chunker/ Text windows and fingerprints. internal/cleanup/ Document deletion across stores. internal/config/ Configuration and validation. internal/credential/ Token files and JWT parsing. internal/domain/ Shared types and interfaces. internal/embedder/ Titan V2 and offline embeddings. internal/embeddinginput/ Model input profiles. internal/evaluation/ Versioned corpus and evidence labels. internal/extractor/ File parsing and process isolation. internal/llm/ Answer routing and Converse. internal/logctx/ Request log attributes. internal/metrics/ Prometheus text metrics. internal/ocr/ Asynchronous PDF text detection. internal/pipeline/ Ingestion and query stages. internal/retriever/ LOCAL and GLOBAL retrieval. internal/store/ Storage adapters. internal/store/memory/ Offline vector index. internal/store/pinecone/ Hosted vector index. internal/store/postgres/ Documents, chunks and durable jobs. internal/store/s3/ Original files and signed downloads. internal/subjectauth/ Delegated access token verification. internal/testutil/ Test support. internal/testutil/docxtest/ Word package fixtures. internal/testutil/pgtest/ Disposable database containers. internal/textutil/ Unicode normalization. internal/tracing/ OpenTelemetry export and span helpers. internal/wordprocessingml/ Word schema identifiers. internal/worker/ Leased ingestion and retry handling. scripts/ Operational commands. scripts/cleanup/ Delete selected documents. scripts/evidenceeval/ Scoped evidence and visibility checks. scripts/fuzz/ Discover and exercise Go fuzz targets. scripts/grant-catalog/ Import tenant grants. scripts/langeval/ Answer language and latency checks. scripts/reconcile/ Find and remove orphan vectors. scripts/simulate/ Exercise the service API. scripts/validate/ Check retrieval behavior. test/ Service test suites. test/integration/ Database and API integration. test/smoke/ Opt-in live service checks. test/unit/ Package behavior checks. tools/ Repository tools. tools/diagrams/ Vector image sources and checks. tools/diagrams/fonts/ Bundled typefaces and license. tools/imagecheck/ Runtime image and license verification. The highlight follows cmd/server, internal/api, internal/pipeline, internal/retriever, internal/llm and internal/store." src="diagrams/repository-layout-light.svg">
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

`make ci` runs vet, lint, the full race suite and the diagram check in order, then builds the runtime image. `docker build --target test .` runs the root module's short suite without requiring a nested Docker daemon. The host suite checks that the build context includes the Go test files and repository files they name with literal paths. Add a `.dockerignore` exception when a test needs another file from `docs/`.

### 1. Unit Tests

Unit tests cover domain functions, data parsing, HTTP behavior and pipeline logic using local implementations. They live beside the packages under `internal/` as well as in `test/unit/`. The short suite runs these tests and skips database-backed integration tests:

```sh
go test -short -race -count=1 ./...
```

The `internal/api/handler` tests use `awaitSignal` to wait for asynchronous events up to the test deadline. The stream disconnect fixture waits for the provider to acknowledge readiness before canceling the client. Queue and health probe concurrency checks use `testing/synctest` to observe blocked goroutines before their timers expire.

Scope of unit tests:
- **Chunker**: Validates word window boundaries, token estimates, HTML sanitization, and SHA-256 fingerprints. `bounds_test.go` checks the clamp result for each raw window and checks aggregate bytes and chunk count for each effective window and text family.
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

Docker must be running locally to permit Testcontainers to manage the ephemeral PostgreSQL database instance. Database-backed tests skip under `go test -short` with an explicit daemon requirement message; CI runs the full suite without `-short`. The cleanup container that Testcontainers starts exits, by default, after a test process has had no database container for ten seconds. `internal/testutil/pgtest` gives each test process its own cleanup container, unless `TESTCONTAINERS_SESSION_ID` is set, so that concurrent packages cannot end each other's. It also waits up to a minute for Docker to finish removing an exited one and starts the database again, so a test that begins during the removal does not fail. The offline query test ingests through HTTP, polls document status until completion, and verifies cited evidence and document restrictions through the router.

### 3. Smoke Tests

Smoke tests run against live, deployed instances and external cloud services (AWS Bedrock, Pinecone, PostgreSQL, and S3). Smoke tests are excluded by default to avoid unexpected cloud consumption or accidental mutation of configured datasets. Accepted documents already have persisted metadata; bounded deferred deletion also removes asynchronous ingestion jobs.

To opt in to smoke testing against a configured service:

```sh
RUN_SMOKE=1 go test ./test/smoke/... -count=1 -v -timeout 120s
```

### 4. Fuzz Tests

The full race suite executes each fuzz target's seed corpus on pull requests and pushes to `main`. The targets cover Unicode entity matching, calibration profile parsing and Pinecone write receipt header parsing. To exercise generated inputs locally, use `make fuzz`. It discovers targets from active test files in the root module and applies the `FUZZTIME` budget to each target. The default is 30 seconds per target.

```sh
make fuzz FUZZTIME=2m
```

Failing inputs are saved under the affected package's `testdata/fuzz/<Target>/` directory. Keep each reproducer with its fix so ordinary tests also check that regression.

---

## Continuous Integration

| Workflow | Trigger | Checks |
|---|---|---|
| [CI](../.github/workflows/ci.yml) | Pushes to `main` and pull requests | Lint; Test with race detection; Build service and runtime image for both published platforms and check them; Check reachable vulnerabilities; Check vector diagrams. |
| [CodeQL](../.github/workflows/codeql.yml) | Pushes to `main`, pull requests targeting `main` and a daily schedule at 04:31 UTC | Analyze Go for security issues. |
| [Dependency review](../.github/workflows/dependency-review.yml) | Pull requests | Review dependency changes; reject vulnerabilities of high severity or above. |
| [Scorecard](../.github/workflows/scorecard.yml) | Pushes to `main`, weekly, branch protection changes and manual dispatch | Publish Scorecard results, retain the SARIF artifact for five days and upload it to code scanning. |
| [Release](../.github/workflows/release.yml) | Version tags matching `v*.*.*` | Builds archives and the runtime image, signs provenance, and publishes the release only after both builds and attestations succeed. |
| [Release check](../.github/workflows/release-check.yml) | Pull requests | Checks [release packaging](#releases) when release inputs may have changed. Cancels superseded checks for the same pull request. |
| [Fuzz](../.github/workflows/fuzz.yml) | Daily and manual dispatch | Exercise every discovered target for five seconds per target, with two minutes per target on Mondays and by default on manual dispatch. Longer campaigns group targets by package and split jobs at the runner time limit. Manual dispatch accepts a per-target duration up to 5h56m. Failing inputs are retained for 14 days. |

### Runtime Image Job

The image job builds the `runtime` target for `linux/amd64` and `linux/arm64` without publishing it. It checks the effective filesystem of each image after applying its layers. The server must be the sole entry point, match the target architecture and be executable by UID/GID `10001:10001`. The check also requires CA certificates and timezone data, and compares every installed license and notice against the project's license and the files collected for that target from the pinned build environment.

The job compiles every package with `go build ./...` only for events the CodeQL analysis does not build: pull requests that target another branch and any other trigger. Pushes to `main` and pull requests targeting `main` rely on the CodeQL build.

Layers are restored from the `ci-runtime` scope of the GitHub Actions cache on every event and exported to it only by pushes to `main`. Release builds neither read nor write that scope. The `test` and `lint` Dockerfile targets are not part of CI.

Run the same check locally with a builder that supports multi-platform OCI exports:

```sh
make image-check
```

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

Pull request preflight checks the packaging fixtures once and verifies the real
archives in two parallel target groups. Each group checks its archives,
checksums, executables, README and target-specific licenses. **Check release
archives** checks that the two manifests cover every command and target exactly
once. This applies to additions, renames and deletions of packaged inputs such
as migrations and license notices, as well as changes whose effect is uncertain.

To check packaging locally without publishing:

```sh
bash .github/scripts/release-test.sh
dist=$(mktemp -d)
.github/scripts/build-release.sh v0.0.0-check "$dist"
.github/scripts/check-release.sh archives v0.0.0-check "$dist"
actionlint .github/workflows/release.yml .github/workflows/release-check.yml
```

To check a subset, append the same `os/arch` arguments to both archive commands.
A version tag uses the full target set.

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

### Evidence Evaluation

`go run ./scripts/evidenceeval` replays the frozen, purpose-written fixture in
`internal/evaluation`. It prints JSON and uses no deployment or credentials.
Version 1 has four documents and six queries, including two English–Spanish
pairs and two no-answer queries. One document repeats the title and text of
another in a separate collection. The empty-scope query grants access to zero
documents.

For a hosted comparison, select staging deployments with real model providers
and equal retrieval settings where possible. Freeze the model revisions,
embedding dimensions, chunking, relevance and context limits, reranking, output
limit and seed where configurable. Record the deployment configuration revision
with each target. Provide a JSON array of objects with these fields:

| Field | Value |
| --- | --- |
| `name`, `base_url` | Unique lowercase target name and absolute API base address. |
| `token_env` | Name of the environment variable holding the service token. Leave empty for an unauthenticated staging instance. |
| `backend` | `postgres` or `pinecone`. |
| `embedding_provider`, `embedding_model`, `dimensions` | Embedding provider, exact model identifier or revision, and output dimensions. |
| `answer_provider`, `answer_models` | Synthesis provider and all routed model identifiers or revisions. |
| `reranker`, `settings_revision` | Reranking model identifier or `off`, and a reference to the frozen deployment settings. |

```sh
RUN_SMOKE=1 go run ./scripts/evidenceeval --live --create --delete \
  --targets staging-targets.json --baseline current --collection evidence-qa \
  --ledger-dir ./evaluation-ledgers --max-requests 120 --instrument
```

Set `EVALUATION_COLLECTION_PREFIX` to the selected prefix on the staging
deployment. The evaluator derives fixture IDs from a fresh namespace per target
and scopes every query by collection and document allowlist. Keep the ledger
directory private. Each ledger has an exclusive process lock and stores an
invocation receipt and pending identity before submission.

The authenticated `/evaluation/fixtures` routes require
`X-Quarry-Evaluation-Fixture` and `X-Quarry-Evaluation-Receipt`. An older server
returns an ordinary route error. The fixture routes accept only the built-in
JSON fixtures. PostgreSQL creates a reservation and receipt with the admitted
document in the same transaction. Fixture UUIDs remain reserved after
deletion so delayed operations cannot target a replacement. Before enabling
fixture admission, upgrade every API replica, ingest worker and operator
cleanup binary that shares the stores. Older binaries can bypass the fixture
guards. Clean up active fixtures before rolling back an upgraded writer.

Apply the [serving-role upgrade](grant-catalog.md#upgrade-an-existing-serving-role)
after migration 019. Grant the staging deployment's serving login receipt
access only in the database that sets `EVALUATION_COLLECTION_PREFIX`:

```sql
GRANT SELECT, INSERT, UPDATE ON public.evaluation_fixtures TO quarry_service;
```

Status uses `X-Quarry-Evaluation-State` to distinguish missing, borrowed, owned,
retired and durably deleted fixtures. An owned or deleted response carries its
`X-Document-Incarnation`. The evaluator pins that revision and supplies it with
the receipt on DELETE. Running ingestion defers cleanup. Workers and cleanup
use the vector store bound at fixture admission, including the resolved
Pinecone index and its default namespace. Cleanup deletes only that
namespace's entries for the fixture ID and never sweeps a namespace named after
the document. If cleanup reports `store_mismatch`, restore the original vector
configuration and retry. For older receipts with an unknown binding, verify the
original index and remaining IDs operationally. Offline fake mode refuses
fixture admission, because its memory vectors cannot outlive the admitting
process. Use PostgreSQL vectors or Pinecone.

A lost or failed reply retains the ID until receipt-qualified recovery confirms
the outcome. When a pending submission appears absent or borrowed, the
evaluator uses `X-Quarry-Evaluation-Reconcile: absent` without an incarnation.
The service retires fixture admission atomically without affecting any ordinary
document that occupies that ID. A delayed admission cannot create a fixture
after retirement.
If admission already won, recovery records the owned incarnation for deletion
on a later attempt.

`--delete` uses the remaining shared cap. The report includes the ledger path
and IDs still pending or accepted. A later cleanup takes a target file with
just the matching deployment and its own explicit cap:

```sh
RUN_SMOKE=1 go run ./scripts/evidenceeval --delete \
  --targets cleanup-target.json --cleanup <ledger-file> --max-requests 16
```

Each preflight, ingest, status check, query, and delete consumes one operation
before dispatch. The client uses fresh HTTP/1 connections and makes no
automatic retries. Check polling uses `--poll-deadline` (30 seconds by default)
per accepted document and shares the absolute cap. Provider calls, retries and
prices inside the deployment are unmeasured. The response's synthesis usage is
reported separately when available. Supplying `--max-tokens` or `--max-spend`
with a positive value stops before dispatch because the full provider charge
cannot be bounded through the API.

The staging diagnostic is available on generated JSON responses with
[the configured collection prefix](configuration.md#environment-variables).
The evaluator requests excerpt matches with `--instrument` and supplies its
fixture namespace and invocation receipt. For a nonempty explicit fixture
allowlist, the service verifies that every ID has an active reservation for
that receipt and current incarnation, then checks retained chunks in the
same snapshot. It returns `X-Quarry-Evaluation-Evidence` when verification
succeeds. An empty allowlist discloses no matches. Its value is unpadded base64url JSON with span
IDs, 1-based excerpt numbers and retained excerpt lengths in runes. An empty
array represents an observed response with no matching span. An absent or
invalid observation leaves coverage unmeasured. The diagnostic never includes
snippet text.

A hosted result requires a successful scoped answer, matching
`X-Quarry-Evaluation-Execution` and `X-Quarry-Evaluation-Upstreams` from the
staging service over certificate-verified TLS. The second header records the
authenticated TLS origins used for both embedding and synthesis. The service
recognizes the public OpenAI and standard regional Bedrock Runtime origins.
Compatible endpoints without recognized provenance are labeled
`live_unverified`. Fake deployments are labeled `offline`.

The report includes these measurements per target and query slice:

- Gold-document recall at K counts unique relevant documents among the first
  K returned chunks in service order. Preview span recall counts verbatim
  labeled spans visible in those public previews. Post-trim coverage uses only
  the staging observation, including matches beyond the public preview.
- Reported p50 and p95 use completed requests. Visibility uses a current
  scoped chunk that matches the fixture body. Completed fixtures remain
  eligible for quality queries when their visibility check fails or is
  censored. Client-observed delay starts at the first completed status response.
  Completion-event bounds add the age of the completion on the PostgreSQL
  clock and bound uncertainty by the status request's client round trip.
  The report includes separate censor bounds and marks event timing unmeasured
  when the clock observation is unavailable.
- Rates include numerators, denominators and descriptive Wilson intervals.
  Error and result counts show which queries contribute to those denominators.
  For comparisons, publish the planned sample size, per-target result counts
  and the matched-query baseline before drawing conclusions. The paired
  queries and this small corpus limit generalization.
  Paired rates for coverage and human judgments use the shared measured
  queries for each metric, with a separate sample count.

To label generated claims, opt in with `--include-snippets`. Review each
answer against its authorized passages and the accepted answers in the frozen
fixture. Count atomic factual claims and mark claims without support in the
available evidence. Independently record whether the answer meets the gold
answer or abstention in the requested language. Omit an observation when the
available evidence is insufficient to judge it. Save the labels as a JSON
array with `target`, `query`, `response_sha256`, `claims`, `unsupported`, and
`accepted` for each reviewed observation:

```sh
go run ./scripts/evidenceeval --annotate-report results.json --reviews labels.json
```

This updates the reported human-label rates without contacting the deployment.
Raw answers and previews require `--include-snippets` on the output command.

### End-to-End Simulation (`scripts/simulate`)

Simulates a sequential series of client interactions against a running instance on port 8081, verifying health checks, JSON ingestion, upload identity, vector query modes, and document deletion. The simulator expects the default `collection` policy.

```sh
go run ./scripts/simulate/main.go
```

Set the expected policy when the service uses `off`. The simulation verifies
the additional upload gets its own ID and deletes that document.

```sh
INGEST_CONTENT_DEDUP=off go run ./scripts/simulate/main.go
```

### Retrieval Validation (`scripts/validate`)

Dispatches predefined queries to a running instance to verify that routing logic correctly assigns LOCAL or GLOBAL retrieval modes and returns non-empty result sets:

```sh
go run ./scripts/validate/main.go
```

---

## Offline Fake Mode Internals

Start the service with the [offline quick start](../README.md#offline-fake-mode).

### Implementation Architecture

- `memory.FakeVectorStore` keeps vectors in a thread-safe Go map and computes
  cosine similarity in memory.
- `embedder.FakeEmbedder` hashes the input text to produce deterministic
  embeddings with the configured `EMBEDDING_DIMENSIONS`. Its constructor falls
  back to 1,024 for nonpositive arguments. Server startup validates the
  configured dimension before calling the constructor.
- `llm.FakeConverser` returns local responses.
- Multipart ingestion decodes recognized plain text in process. PDF, DOCX and
  DOC use the [isolated parser](architecture.md#extraction-boundaries) by
  default. Offline ingestion skips OCR and retains no original files.
- The service applies its normal migrations to the [metadata database](deployment.md#postgresql-metadata-and-chunk-store).

Durable ingestion tests use disposable PostgreSQL databases and the offline vector store to cover restart recovery, terminal failures, deletion during processing, NUL handling and source upload failures. Lease and atomic acceptance tests live in `internal/store/postgres/jobs_test.go`; the short suite skips database-backed tests.

The offline vector store enforces the hosted index's per-record limits: flat metadata values must be strings, numbers, booleans or lists of strings, serialized vector metadata must fit within 40,960 bytes, and vector identifiers must fit within 512 bytes. The metadata size includes chunk text and the required fields sent by the hosted adapter. The first stored embedding fixes the index dimension across all namespaces; later embeddings must match. Filtered queries accept at most 10,000 results. Upserts have no per-call batch limit because the hosted adapter batches requests. Offline writes are immediately consistent; the hosted index is eventually consistent.
