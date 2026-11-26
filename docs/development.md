# Development

For contribution guidelines, environment setup, Conventional Commits formatting, and pull request procedures, refer to CONTRIBUTING.md. For vulnerability reporting procedures, refer to SECURITY.md.

---

## Repository Layout

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="diagrams/repository-layout-dark.svg">
  <img alt="Repository layout, from the directory listing to depth three. client/ Go client for the HTTP API. cmd/ Executable entry points. cmd/quarry-mcp/ Local MCP server over stdio. cmd/server/ Wiring and graceful shutdown. docs/ Service documentation. docs/diagrams/ Architecture, schema and layout. docs/diagrams/src/ Diagram placement sources. internal/ Private service packages. internal/api/ HTTP router. internal/api/handler/ Request handlers. internal/api/middleware/ Authentication, logs and metrics. internal/awsconf/ AWS credentials and retry policy. internal/chunker/ Text windows and fingerprints. internal/citationsupport/ Citation verifier. internal/cleanup/ Document deletion across stores. internal/config/ Configuration and validation. internal/credential/ Token files and JWT parsing. internal/domain/ Shared types and interfaces. internal/embedder/ Titan V2 and offline embeddings. internal/embeddinginput/ Model input profiles. internal/evaluation/ Versioned corpus and evidence labels. internal/extractor/ File parsing and process isolation. internal/llm/ Answer routing and Converse. internal/logctx/ Request log attributes. internal/metrics/ Prometheus text metrics. internal/ocr/ Asynchronous PDF text detection. internal/pipeline/ Ingestion and query stages. internal/retriever/ LOCAL and GLOBAL retrieval. internal/store/ Storage adapters. internal/store/memory/ Offline vector index. internal/store/pinecone/ Hosted vector index. internal/store/postgres/ Documents, chunks and durable jobs. internal/store/s3/ Original files and signed downloads. internal/subjectauth/ Delegated access token verification. internal/testutil/ Test support. internal/testutil/docxtest/ Word package fixtures. internal/testutil/pgtest/ Disposable test databases. internal/textutil/ Unicode normalization. internal/tracing/ OpenTelemetry export and span helpers. internal/wordprocessingml/ Word schema identifiers. internal/worker/ Leased ingestion and retry handling. scripts/ Operational commands. scripts/citationsupport/ Calibration. scripts/cleanup/ Delete selected documents. scripts/evidenceeval/ Scoped evidence and visibility checks. scripts/fuzz/ Discover and exercise Go fuzz targets. scripts/grant-catalog/ Import tenant grants. scripts/langeval/ Answer language and latency checks. scripts/reconcile/ Find and remove orphan vectors. scripts/reindex/ Stage and verify vector generations. scripts/simulate/ Exercise the service API. scripts/validate/ Check retrieval behavior. test/ Service test suites. test/integration/ Database and API integration. test/smoke/ Opt-in live service checks. test/unit/ Package behavior checks. tools/ Repository tools. tools/diagrams/ Vector image sources and checks. tools/diagrams/fonts/ Bundled typefaces and license. tools/imagecheck/ Runtime image and license verification. The highlight follows cmd/server, internal/api, internal/pipeline, internal/retriever, internal/llm and internal/store." src="diagrams/repository-layout-light.svg">
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

The `internal/api/handler` tests use `awaitSignal` to wait for asynchronous events up to the test deadline. The stream disconnect fixture waits for the provider to acknowledge readiness before canceling the client. Queue and health probe concurrency checks and the Pinecone attempt-deadline visibility check use `testing/synctest`, so their timers do not depend on runner speed; the visibility check serves requests through an in-memory transport.

Scope of unit tests:
- **Chunker**: Validates word window boundaries, token estimates, HTML sanitization, and SHA-256 fingerprints. `bounds_test.go` checks the clamp result for each raw window and checks aggregate bytes and chunk count for each effective window and text family.
- **Extractor (`test/unit/extractor_test.go`, `internal/extractor/*_test.go`, `internal/api/handler/ingest_detect_test.go`)**: Covers synthetic PDF, DOCX and plain text, content detection and extension mismatches, stable extraction error codes, decoded-text and XML limits, parser isolation, and cancellation.
- **DOC (`internal/extractor/doc_test.go`)**: Covers Word 97–2003 piece tables, Windows-1252 and UTF-16 text, nested field codes, corrupt structures, encrypted and older files, and decoded-text limits.
- **Fake Mode (`fakemode_test.go`)**: Verifies deterministic in-memory vector storage, mock embeddings, and fake converser outputs.
- **Named Entities (`named_entity_test.go`)**: Tests proper noun phrase extraction, stop-word filtering, and Unicode capitalization handling.
- **Retriever (`test/unit/retriever_test.go`, `internal/retriever/retriever_test.go`, `internal/pipeline/local_test.go`)**: Covers scoped retrieval, document selection, similarity thresholds and the LOCAL top-up when fewer than three chunks are returned.
- **Router (`router_test.go`)**: Validates model selection decisions across task types, context token thresholds, and synthesis modes.

### 2. Integration Tests

Integration tests verify database persistence, transactional rollbacks, schema migrations, and pipeline execution. Database-backed tests in `test/integration/`, `internal/store/postgres/`, `internal/store/pinecone/`, `internal/cleanup/`, `scripts/reconcile/` and `scripts/grant-catalog/` receive an empty database from `internal/testutil/pgtest`.

```sh
go test -race -count=1 ./test/integration/...
go test -race -count=1 ./internal/store/postgres/...
```

Database-backed tests skip under `go test -short` even when shared services are configured. CI runs the full suite. By default every test database runs in its own disposable Testcontainers container and requires Docker. The cleanup container that Testcontainers starts exits, by default, after a test process has had no database container for ten seconds. `internal/testutil/pgtest` gives each test process its own cleanup container, unless `TESTCONTAINERS_SESSION_ID` is set, so that concurrent packages cannot end each other's. It also waits up to a minute for Docker to finish removing an exited one and starts the database again, so a test that begins during the removal does not fail.

To reuse running services instead, set both `QUARRY_TEST_POSTGRES_ADMIN_DSN` and `QUARRY_TEST_PGVECTOR_ADMIN_DSN` to PostgreSQL URLs for superusers on an ordinary PostgreSQL 16 server and on a server where the `vector` extension and the `es_ES.utf8` system locale are available. Each URL must specify a host and an administrator, either in the user info or the `user` query parameter. An omitted maintenance database defaults to `postgres`. Set a database in the path or use the `dbname` or `database` query parameter to override it. A single host defaults to port 5432.

Each test then gets a new empty database, created from `template0` with a random name and dropped when the test ends, even if its setup failed, together with the databases and roles the test requested through `pgtest.Sibling`, `pgtest.SiblingLatin1` and `pgtest.Role`. Connections that outlive the test are closed by the drop. Setting only one variable, an invalid URL or an unreachable service fails the test. Only the two test settings supply administrator addresses. Tests that touch cluster-wide state take their role names from `pgtest.Role` and scope lock observations to their own database, so any order and any number of concurrent fixtures stay isolated.

The offline query test ingests through HTTP, polls document status until completion, and verifies cited evidence and document restrictions through the router.

### 3. Smoke Tests

Smoke tests run against live, deployed instances and external cloud services (AWS Bedrock, Pinecone, PostgreSQL, and S3). Smoke tests are excluded by default to avoid unexpected cloud consumption or accidental mutation of configured datasets. Accepted documents already have persisted metadata; bounded deferred deletion also removes asynchronous ingestion jobs.

To opt in to smoke testing against a configured service:

```sh
RUN_SMOKE=1 go test ./test/smoke/... -count=1 -v -timeout 120s
```

The versioned-bucket check writes and deletes objects under `documents/smoke/` in `S3_BUCKET`, so it also requires `SMOKE_S3_VERSIONED=1` and a disposable bucket with versioning enabled. It verifies that a write reports a version, that a plain delete leaves the version behind a marker, and that deleting by version ID removes both, which [source upload reclamation](#source-upload-reclamation-scriptsreconcile) relies on.

### 4. Fuzz Tests

The full race suite executes each fuzz target's seed corpus on pull requests and pushes to `main`. The targets cover Unicode entity matching, calibration profile parsing and Pinecone write receipt header parsing. To exercise generated inputs locally, use `make fuzz`. It discovers targets from active test files in the root module and applies the `FUZZTIME` budget to each target. The default is 30 seconds per target.

The source-upload reconciliation target exercises the object keys accepted by reclamation.

```sh
make fuzz FUZZTIME=2m
```

Failing inputs are saved under the affected package's `testdata/fuzz/<Target>/` directory. Keep each reproducer with its fix so ordinary tests also check that regression.

---

## Continuous Integration

| Workflow | Trigger | Checks |
|---|---|---|
| [CI](../.github/workflows/ci.yml) | Pushes to `main` and pull requests | Lint; Test with race detection against PostgreSQL and pgvector services; Build service and runtime image for both published platforms and check them; Check reachable vulnerabilities; Check vector diagrams. |
| [CodeQL](../.github/workflows/codeql.yml) | Pushes to `main`, pull requests targeting `main` and a daily schedule at 04:31 UTC | Analyze Go for security issues. |
| [Dependency review](../.github/workflows/dependency-review.yml) | Pull requests | Review dependency changes; reject vulnerabilities of high severity or above. |
| [Scorecard](../.github/workflows/scorecard.yml) | Pushes to `main`, weekly, branch protection changes and manual dispatch | Publish Scorecard results, retain the SARIF artifact for five days and upload it to code scanning. |
| [Release](../.github/workflows/release.yml) | Version tags matching `v*.*.*` | Builds archives and the runtime image, signs provenance, and publishes the release only after both builds and attestations succeed. |
| [Release check](../.github/workflows/release-check.yml) | Pull requests | Checks [release packaging](#releases) when release inputs may have changed. Cancels superseded checks for the same pull request. |
| [Fuzz](../.github/workflows/fuzz.yml) | Daily and manual dispatch | Exercise every discovered target for five seconds per target, with two minutes per target on Mondays and by default on manual dispatch. Longer campaigns group targets by package and split jobs at the runner time limit. Manual dispatch accepts a per-target duration up to 5h56m. Failing inputs are retained for 14 days. |
| [Database fixtures](../.github/workflows/fixtures.yml) | Weekly and manual dispatch | Run representative PostgreSQL and pgvector tests with disposable containers, shuffled, in separate invocations. |

The race job runs `go test -race -count=1 -json ./...` against a PostgreSQL service and a pgvector service pinned to the images that `pgtest` starts locally. It also tests `.github/scripts/testreport` with race detection in a separate step because `./...` skips directories beginning with a dot. The report command fails the job when a package does not build, fails, has no result or reuses a cached one. Its summary lists package results, the slowest tests and subtests separately, skips, packages without test files, fixture setup and cleanup times, and the state of both caches. The module cache and the race build objects are restored under separate keys, and only pushes to `main` save them, so pull requests build cold when no entry matches.

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

Accepts document UUIDs to remove records from the configured vector backend, S3 and PostgreSQL, or inspects, quarantines and releases held identities using one shared set of clients:

```sh
make cleanup IDS="<document-uuid-1> <document-uuid-2>"
make cleanup IDS="--list-held"
make cleanup IDS="--quarantine <document-uuid>"
make cleanup IDS="--release-held <document-uuid> <incarnation-uuid>"
```

A bare UUID addresses the live document, which is the only form that works before the database activates [checkpointed deletion](api.md#checkpointed-deletion). `UUID@incarnation` addresses only the lifetime read earlier with `include_incarnation=true`. The incarnation must be one canonical UUID; an empty, repeated or malformed value stops the command before any deletion, and it is never treated as a bare UUID. With `GO_RAG_FAKE_MODE` the vectors live in the running service, so deletion and `--release-held` forward each request once to that service on `SERVER_PORT` with the configured service token.

See [quarantine and hold release](deployment.md#quarantine-and-hold-release) for inspecting held receipts, quarantining absent UUIDs and releasing verified holds.

PostgreSQL cleanup binds an existing `chunk_vectors` table on first use and refuses when it is missing; Pinecone cleanup and applied reconciliation require the binding a serving replica records.

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
sample identifiers prefixed with `vector:`, `namespace:` or `retired:`. Vector
counts cover the default namespace. A vector is selected if `doc_id` is missing
or invalid, the document row is missing, or its document has status `failed`
with no `ingest_jobs` row. Retryable failures and processing documents are
preserved. A vector tagged with an active, writing or cleanup-tracked
`revision_id` is preserved even if the document row was deleted or reused until
its revision cleanup finishes. Only UUID-named legacy namespaces with no document
row are selected; other namespace names are ignored.

Retired hosted vector revisions appear as `retired_revisions` and
`retired_vectors`, with `retired:` samples that name the revision. A revision is
unresolved when verification still found residue after every recorded ID was
deleted. `unresolved_revisions` counts those, and `unresolved_vectors` counts the
IDs the revisions recorded. With `-apply`, `swept_revisions` and `swept_vectors`
report the cleanups completed by this run, and `sweep_error` carries the last
residue or provider failure the sweep deferred. A deferred cleanup never stops
the orphan scan or hides the report. A claim or database failure ends the run
with a nonzero exit status.

`-limit` defaults to 100000 and bounds selected default-namespace vectors,
retired cleanup vectors drained during `-apply`, and the vector counts of whole
selected namespaces. A namespace larger than the remaining budget is skipped, and
an empty namespace consumes one budget entry.
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

### Source Upload Reclamation (`scripts/reconcile`)

Use `-source-uploads` to reclaim original files recorded by
[`SOURCE_UPLOAD_JOURNAL`](configuration.md#environment-variables)
that no document adopted. Configure `DATABASE_URL`, `S3_BUCKET`, `AWS_REGION`
and AWS credentials. Set `S3_EXPECTED_BUCKET_OWNER` to enforce the expected
account. The command runs independently of serving and vector configuration
and can run during ingestion. Each version listing is scoped to a recorded
object key in the configured bucket, and the command matches the full key.
Prerequisites and rollout are in the
[deployment guide](deployment.md#source-upload-journal).

```sh
make reconcile ARGS="-source-uploads"
make reconcile ARGS="-source-uploads -apply"
```

Without `-apply` the command only reads: it changes no database row and writes
or deletes no object. It reads the journal and the recorded keys' versions and
retention state, then prints what `-apply` would do. With `-apply` it claims
uploads that are unadopted and past their lease and `-min-age`, rechecks that no
document or deletion receipt names the key, and deletes every version and
delete marker of that key by version ID. A claimed row fences acceptance: a
stalled upload that reaches acceptance afterwards fails with `503` and is
retried by its caller.

Before dispatching a PUT, the writer marks its journal row as uncertain. When the
PUT returns, the writer accounts for each attempt the SDK sent. An attempt is
accounted for when S3 acknowledged it, when S3 refused it with a redirect or
client error that carries an `x-amz-request-id` header, or when it failed
before a connection was established, so none of it was sent. A credential or
signing failure sends no attempt. A timeout, a lost response, a server error, a
refusal without that header or any other outcome can still produce a version.
The writer records the row as settled only when every attempt is accounted for.
While the key is empty, reconciliation keeps rechecking it at the pace the
reasons below give. It drops the row after a clean recheck at least `-settle`
after the claim only when the writer never dispatched a PUT or recorded it as
settled.

A row pending with `unacknowledged_write` never settles on its own. It records
a PUT with an unknown outcome and keeps its key under exact-key rechecks. A late
version is still found and removed on a later check.

| Flag | Default | Meaning |
|---|---|---|
| `-source-uploads` | off | Selects this mode; it cannot be combined with `-backfill-collections`, `-after` or `-page`. |
| `-apply` | off | Performs the removals; without it nothing is written. |
| `-limit` | 1000 | Maximum uploads examined in one run. `limited` in the report is true when more were due. |
| `-min-age` | `1h` | Uploads younger than this stay untouched, even after their lease. |
| `-settle` | `30m` | Minimum time between claim and settlement for accounted-for writes (at least `1m`). |
| `-max-bytes` | 1 GiB | Maximum object bytes deleted in one run; a version that would exceed it stays pending. |
| `-batch` | 100 | Uploads claimed per round, 1 to 1000. |

The command prints JSON: `mode` (`inventory` or `apply`), `examined`, `limited`,
`deleted_versions`, `deleted_bytes`, `settled` and `pending`; `uploads`, the
first 100 examined uploads with `upload_id`, `key`, `acknowledged`, `write_pending`, `action`
(`delete`, `settle` or `pending`), `reason`, `versions`, `removals` and `bytes`; and
`journal`, the bucket's rows counted by `state`, `reason`, `acknowledged` and
`write_pending` with the oldest creation time. `write_pending` identifies a PUT
with at least one possible delivery outstanding, even when `acknowledged` is
true. Per-upload `removals` and `bytes` count versions
and bytes selected in inventory mode or confirmed deleted in apply mode. Both
modes process rows in due-time order and share one byte budget across rows.
Apply reserves bytes before sending each delete and keeps the reservation even
if the response fails. A partial removal reports its confirmed counts and stays
pending with the reason. Inventory leaves the aggregate deletion counters at zero.

An upload stays `pending`, with one of these reasons, until the uncertainty
ends. `awaiting_settlement` rechecks every `-settle`/3. Every other reason
retries after `-settle`/3 doubled for up to six steps, at most one day (about
10.7 hours with the default `-settle`).

| Reason | Meaning |
|---|---|
| `awaiting_settlement` | The object was removed or is absent; a clean recheck can drop the row when its writes are accounted for. |
| `unacknowledged_write` | The object is absent, and at least one PUT could still land. |
| `legal_hold`, `retention_until=<time>` | A version is protected; no version of the key is deleted. |
| `versions_unlisted`, `versions_truncated` | The key's version history could not be read, its listing was indeterminate, or it exceeded 100 entries. |
| `version_id_missing` | A version or delete marker lacks an ID, so the entire key stays pending. |
| `version_size_invalid` | A data version has a missing or negative size, so the entire key stays pending. |
| `lock_unknown` | The protection of a version could not be read; nothing is deleted. |
| `delete_failed`, `delete_failed:<code>` | A delete returned an error. A later check reinspects the key. |
| `byte_limit` | The next version would exceed `-max-bytes`. |
| `referenced_by_document` | A document or deletion receipt names the key; it is never deleted. |
| `unrecognized_key` | The key is not `documents/{type}/{document}/{upload}/{file}` for its own row. |

A database error stops the command with a nonzero status; an S3 error leaves
the affected upload pending. Rerun the inventory until `journal` shows no
`staged` or `claimed` rows beyond uploads that are legitimately in flight,
protected or pending with `unacknowledged_write`.

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

### Document Key Canonicalization

Vectors written into a hosted index by external tools can store `doc_id` in a
UUID format other than the canonical lower-case form. Until those keys are
rewritten, allowlisted queries, `DeleteByDocID`, `-backfill-collections` and
`-backfill-scoped` do not match them, while collection-only queries still see
them. Run this mode before `-backfill-collections` and `-backfill-scoped`,
which match `doc_id` exactly:

```sh
make reconcile ARGS="-canonical-document-ids"
make reconcile ARGS="-canonical-document-ids -apply"
```

This mode skips orphan cleanup and requires a configured Pinecone index in both
dry run and apply. An index that does not index `doc_id` for filtering rejects
the first request and stops the run. Without `-apply`, each request sends a
provider dry-run request (`DryRun: true`) that counts matching vectors without
changing them. With `-apply`, the command pins the active vector generation,
holds the writer barrier, and sets only `doc_id` to the canonical lower-case
UUID, leaving `collection`, `doc_type`, text and ownership fields unchanged.

For each document in PostgreSQL in ascending UUID order, regardless of status,
the command matches the nine uniform-case non-canonical spellings that UUID
parsers accept: upper-case hyphenated, braced lower and upper case,
`urn:uuid:` and `URN:UUID:` prefixes with lower- and upper-case digits, and
32-digit lower and upper case without hyphens. Mixed-case hex digits are not
enumerated and stay unchanged, though the orphan reconciler still maps them to
their document.

The command prints JSON with `mode` (`inventory` or `apply`),
`examined_documents`, `matched_vectors`, `updated_documents`, `documents` (the
first 100 matching documents with `document_id`, `matched_vectors`, and
`capped` when a document reaches the provider's 100,000-record per-request
limit), and `resume_after`. `-limit` bounds examined documents (100000 by
default); `-page` is rejected. Each run stops after 30 minutes. Pass `-after`
with `resume_after` or the cursor named in an error to continue. Any provider
error stops further updates with a nonzero exit status; earlier updates remain
applied. Metadata updates are eventually consistent, so rerun the dry run until
`matched_vectors` is zero. To verify against a serverless staging index in an
isolated namespace, run
`QUARRY_PINECONE_STAGING_INDEX=<index> PINECONE_API_KEY=<key> go test -v -run TestStagingCanonicalDocumentKeys ./scripts/reconcile`.

### Scoped Metadata Backfill

Documents assigned through the [grant catalog](grant-catalog.md#assign-historical-documents)
stay unreadable until their stored data is verified. Pause ingestion and other
index writers, then run the command with the operator login, which needs
execution on the two backfill functions:

```sh
make reconcile ARGS="-backfill-scoped"
make reconcile ARGS="-backfill-scoped -apply"
```

This mode skips orphan cleanup. It walks completed, assigned documents that are
not yet readable in ascending UUID order and prints only counts: `examined`,
`verified`, `quarantined` and `changed`, plus `resume_after`, the last document
it finished. A document with an unresolved vector mutation is skipped until the
mutation settles. A dry run reads only PostgreSQL. With `-apply`, both the
PostgreSQL and hosted paths pin the configured `VECTOR_GENERATION` and target
profile against PostgreSQL's active binding and hold the writer barrier for the
pass, refusing while writers are frozen or when the active generation differs.
Each document is verified from its current stored chunks. On `VECTOR_STORE=postgres`
the active generation's vector table must hold exactly those chunks. On the hosted index every current chunk
must exist under its stored identifier with its `doc_id`, `chunk_index`, `text`
and `token_count`, and in the format the document was written in, without a
conflicting `document_incarnation`. The command then labels those vectors
by identifier: `tenant_id` for a document that already carries revision and
attempt tags, and `tenant_id` plus `document_incarnation` for an untagged one.
It reads the labels back and publishes readiness only if the document row and
chunk list still match the verified snapshot. Vectors of stale or replaced
chunks and legacy namespaces never receive labels, so no scoped search can
return them.

A document that fails verification counts as `quarantined` and a document that
changed after it was read counts as `changed`. Both stay unreadable. Re-ingest a
quarantined document and refresh its assignment, or refresh the assignment of a
changed one, then run the mode again. The `-limit` flag bounds documents (100000 by default) and `-page`
has no effect. Pass `-after` with `resume_after` or the value named in an error
to continue after an interruption. Labeling and publishing are idempotent, so a
rerun can repeat any document safely.

An update error stops further updates and returns a nonzero exit status; earlier
successful updates remain applied. Metadata updates are eventually consistent,
so allow them to propagate before relying on collection-filtered queries.

### Citation Verifier Calibration

Quarry ships with zero enabled language pairs. Before enabling a production profile, publish a digest-pinned evaluation with blinded native-speaker labels. Split development and held-out data by document before fitting thresholds. Freeze the verifier protocol, language identification, thresholds and evidence selection, including body truncation, before collecting held-out predictions. Use equal counts of `supported`, `contradicted`, and `insufficient` labels within each candidate reference pair. Include closely matched factual contrasts, such as different numbers or negations with otherwise identical wording, and bilingual paraphrases for cross-language profiles.

The calibration file contains `model`, `blinded_native_labels`, `development_documents`, and `pairs`. Group every eligible held-out example by its reference `claim_language`, `source_language`, and `evidence_mode` (`single` or `set`). Include reference pairs without candidate `thresholds` so their misroutes can count against the profiles that issue predictions. Use opaque IDs for each underlying document, including translated or duplicate copies. Keep held-out documents disjoint across samples in the replay and from development.

Each `held_out` entry has `documents`, the actual `evidence_count` from the request, an optional `label` and an optional `category`. Use `benign_contrast` and `bilingual_paraphrase` for the corresponding labeled examples. Preserve each raw verifier result in `prediction` with its predicted `claim_language`, `evidence_languages`, and `scores`, including results with incorrect language identification. Omit the prediction when the verifier returned no usable result. An unlabeled example can still have a prediction.

Run `go run ./scripts/citationsupport -input /path/to/calibration.json` and publish the output with the frozen evaluation. The gate requires labels and in-profile assessments for at least half the eligible examples in the reference group. For each candidate profile, `confirmed_supported_precision` counts every supported prediction issued by that profile, including incoming language misroutes. `reference_confirmed_supported_precision` counts only those it issued on the matching reference languages and evidence mode. Both metrics count every supported prediction in their population as a trial, including unlabeled examples, and count only confirmed supports as successes. Each one-sided 95% exact lower bound must reach 0.90. The report also publishes the issuer's labeled supported count, assessed coverage and per-class recall for the qualified live policy by reference pair. Keep profiles disabled when the evidence is missing or fails qualification.

### Vector Reindex (`scripts/reindex`)

Moves completed documents to a new vector target, a vector generation, without changing document or chunk identities. PostgreSQL stays the source of truth: the command reads documents, chunks, document types and collections from it and never writes the `documents`, `chunks`, `document_fingerprints` or `ingest_jobs` tables. The [deployment guide](deployment.md#changing-models-or-vector-stores) gives the order of operations; this section is the command reference.

Use the live deployment's `DATABASE_URL`, `VECTOR_STORE`, `VECTOR_GENERATION` and Pinecone settings to identify the source. For staging with model calls, configure the embedding provider for the target profile. The database must already have migration 016 and an active target binding. Without `-apply` every mode only reads. The command reports JSON on stdout and exits with a nonzero status on errors. See [vector reindex database privileges](deployment.md#vector-reindex-database-privileges) when running with a non-owner maintenance login.

```sh
make reindex ARGS="-generation g2 -profile profile.json"
make reindex ARGS="-generation g2 -profile profile.json -apply -max-embed-calls 50000"
```

**Target.** `-generation` accepts 1 to 24 lowercase letters, digits or underscores, starting with a letter. A PostgreSQL target uses its own `chunk_vectors_<generation>` table. Pinecone needs `-target-store pinecone` and `-target-index` naming an existing, ready cosine index of the profile's dimension. `-target-store` defaults to `VECTOR_STORE`. The command protects the live target recorded in PostgreSQL, even when the legacy profile is unknown, and requires the configured source to match it. Each generation keeps one store, target and profile.

**Profile.** `-profile` names a JSON object. Unknown, duplicate and missing fields are rejected.

| Field | Meaning |
|---|---|
| `provider` | `bedrock` or `openai`. |
| `model` | Model identifier as sent to the provider. |
| `model_revision` | Full versioned Bedrock foundation model ID, or the immutable deployment assertion in `EMBEDDING_MODEL_REVISION`. |
| `dimensions` | 256, 512 or 1024 for Bedrock; 1 to 4096 for OpenAI-compatible providers; at most 2000 for PostgreSQL. |
| `metric` | `cosine`. |
| `extraction_chunk_version` | Configured text identity, formed as `<EXTRACTION_CHUNK_VERSION>/size=<CHUNK_TOKEN_SIZE>/overlap=<CHUNK_OVERLAP>/ocr=<OCR_PROVIDER>/doc=<mask>/docx=<mask>`. When configured, the embedding input profile appends an `/input=` fingerprint of its limits and tokenizer. Inventory reports the expected identity when it differs from the profile. A known profile with different text settings requires a separate re-extraction from the original files. |

The story masks sum the selected story values. For DOC, footnotes, endnotes, headers, footers and comments have values 1, 2, 4, 8 and 16. The DOC body is always included and contributes zero. For DOCX, body, footnotes, endnotes, headers, footers and comments have values 1, 2, 4, 8, 16 and 32. The default masks are DOC 0 and DOCX 7.

**Modes.**

| Invocation | Effect |
|---|---|
| none | Inventory: counts documents, chunks and checkpoints, reports blockers and the embedding calls staging could need. Opens no vector store and constructs no embedding provider. |
| `-apply` | Stage: register the generation, create the target table when PostgreSQL, and write vectors. |
| `-verify` | Compare the target with PostgreSQL and the current target. With `-apply` and the freeze, record a passing result. |
| `-freeze` | Pause admission, job claims, deletions and maintenance. With `-apply`, wait until the freeze settles. |
| `-thaw` | Resume writers. |
| `-activate` | Make a verified generation the one every replica must serve. |
| `-attest-source` | Record the profile the operator states for the legacy target. |

At most one of the mode flags can be given, and each write needs `-apply`.

**Copy or compute.** Reuse requires a recorded or attested source profile in the same embedding space and a retrievable, finite, nonzero vector of the declared dimension for the same document, position and text. When source vectors are unavailable or unsuitable, staging can recompute them from the stored chunks within the embedding budget. Each charged Bedrock call uses one transport attempt. Both attestation and model calls require the full profile to match the configured deployment. Document type and collection come from PostgreSQL. Pinecone generations record the resolved physical host alongside the index name. Inventory displays the recorded host without opening the index. A credential rotation may retain the same host. Reusing or replacing an index name at a different host fails the identity check.

**Bounds and cost.** `-max-embed-calls` defaults to 0. Staging stops before writing a document that exceeds the available budget. `-max-documents` (1000) counts scanned documents, including current checkpoints, and limits orphan cleanup. `-max-chunks` (100000) bounds chunk loads, writes and key scans. `-max-bytes` (128 MiB) bounds stored chunk text loaded into the command. `-batch` (100), `-page` (100) and `-timeout` (30 minutes) bound requests and elapsed time. For staging, pass the returned `next_after` as `-after`. If the frozen rollback key scan stopped, also pass `next_prune_after` as `-prune-after` during the same freeze. Verification requires all checks to fit within one invocation.

**Checkpoints.** Every staged document has a ledger row keyed to the generation and a source revision, which hashes the document's incarnation, type, collection, owning tenant when assigned, active hosted vector revision when manifested, and chunk keys, positions, token counts and text. The row records the intent before the first write, so an interruption never strands a key; vectors are upserted under their target keys: the chunk key, `<docID>#<revisionID>#<index>` with `revision_id` and `chunk_id` metadata on a Pinecone target when the document has an active hosted revision, or the revision- and attempt-qualified key with its ownership tags on a Pinecone target when the document is stored in the tagged vector format; stale keys of a changed document are removed; and completion is recorded last. A document deleted, failed, being deleted or held is pruned from the target. A document deleted and recreated under the same UUID has a new incarnation and is restaged. A document whose `chunk_count` differs from its stored chunks is reported as inconsistent and blocks completion. Staged Pinecone records name the document incarnation, which retrieval requires, and carry `tenant_id` when the document is assigned in the grant catalog; a source record that names another incarnation or tenant is recomputed. For a tagged document, staging reads the published revision and attempt from PostgreSQL and its source revision includes the attempt, so a re-published document is staged again; a tagged document whose published attempt cannot be determined, for example while a vector mutation is in progress, blocks staging and verification. A document with an active hosted revision keeps its manifest keys whether or not it is also tagged. A PostgreSQL target cannot yet hold tagged documents or documents with an active hosted revision, which block staging and verification there. Documents without stored original files are counted in the inventory; the command never reads them.

Staging, applied verification and activation have exclusive ownership of a target while they run. Use a PostgreSQL connection that preserves the server session for those operations. An unacknowledged mutation blocks subsequent staging and approval after a disconnect. Use `-confirm-target-drained` with staging only after establishing that the outstanding request can no longer change the target, then resume from the beginning of the incomplete document or key page. If the request cannot be resolved, stage a fresh isolated target.

**Verification.** Counts alone prove nothing, so `-verify` walks every eligible document and requires, for every expected target key, a target record with the profile's dimension and PostgreSQL's document, chunk, position, text, token count, type and collection (and, on Pinecone, the document incarnation, the active hosted revision when manifested, the published revision and attempt in the record key and tags when tagged, and the owning tenant when assigned), and no record outside the expected set. It then runs matched queries: for a deterministic sample of `-sample-size` stored chunks it queries the current target and the staged target with vectors independent of the staged records, scoped to the chunk's document and up to `-scope-documents` (5) others of its collection, and compares the top `-top-k` (10) results. It never sends an unscoped query. Thresholds have no defaults and must be declared: `-sample-size`, `-min-recall` (mean overlap with the current target's results) and `-max-p95` (95th percentile target query latency). The target passes only with 100% key and profile coverage, no out-of-scope record, no result outside a query's scope, the declared recall and latency, and a full sample. A target that cannot list its keys, such as a pod-based Pinecone index, cannot pass. A hosted index may lag its writes, so allow it to converge before verifying. The report lists every failure.

For recorded profiles in the same embedding space, verification uses the current target's vector on both stores after checking the sampled chunk's document, position and text. For different spaces or an unattested legacy target, run verification without `-apply` to get `sample.chunk_ids`, `sample.source_profile_hash` and `profile_hash`. When the legacy profile is unknown, run verification with the live replicas' serving encoder settings, including the exact model revision and effective text settings. The resulting `sample.source_profile_query_only` means the hash identifies the query encoder. The baseline still queries the bound live target, and all current staging checkpoints must show that every vector was recomputed.

Embed each selected chunk's stored text independently with the source query encoder and the recorded target encoder. Supply the results as a JSON object via `-query-fixtures`. Set `source_profile_hash` and `target_profile_hash` to the reported hashes, and key `queries` by chunk ID. Each entry contains `text_sha256` (the lowercase hex SHA-256 of the stored text) and the numeric `source` and `target` embeddings. Verification bounds the fixture file by `-max-bytes` and makes no provider calls. If the sample changes during catch-up, regenerate the fixtures for the new sample before recording approval.

**Freeze and activation.** `-freeze -apply -generation g2` drains admitted work at the admission, claim, deletion and maintenance barriers before recording the freeze. The command then waits until no job holds a live lease, no deletion receipt is open and `-quiesce` (at least 60 seconds) has elapsed. When open receipts remain, inventory, `-freeze -apply` timeouts, `-activate` previews and activation errors report up to 20 document IDs (`open_deletion_ids`, with `+<N> more` when truncated; inventory does not block live staging). See [Changing Models or Vector Stores](deployment.md#changing-models-or-vector-stores) for the thaw, qualified-retry and re-freeze recovery procedure. An applied verification revokes the previous approval at the start. Each sampled query needs usable scoped results from the current target, and approval requires a complete scan and sample under the same freeze. `-activate -apply` requires a current approval and the same digest of every eligible document and revision. Traffic moves when replicas start with `VECTOR_GENERATION` naming the new generation.

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
configuration and retry. Offline fake mode refuses
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
