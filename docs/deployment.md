# Deployment

## Logging

The service emits JSON logs to stderr at the minimum severity set by
[`LOG_LEVEL`](configuration.md#environment-variables). Each access log includes `method`,
`path`, `status`, `bytes`, `duration_ms`, `remote_addr` and `request_id`. The path
excludes the query string; the remote address is the connecting peer, including
its port. `bytes` counts response body bytes written, and `duration_ms` measures
elapsed request handling time in milliseconds.

The `X-Request-ID` response header carries the correlation value. Structured logs
written with the request context include it as `request_id`; startup and other
logs without that context omit it. Recovered panics are logged at error severity
with a stack trace. Panics before a response starts produce a JSON 500 and an
access log with status 500. If headers or a partial body were already sent,
recovery preserves that response and the access log records its original status.
Aborted handlers propagate to the HTTP server without a recovered panic error.

## Metrics

Set [`METRICS_ADDR`](configuration.md#environment-variables) to a listen address such as
`127.0.0.1:9090` to serve `GET /metrics` separately from the API. Bind it to a
private interface and never expose it publicly. The listener has no
service-token authentication; restrict monitoring access with network policy or
firewall rules. Container port publishing must preserve that private access.
The listener uses bounded HTTP timeouts and stops with the API during
[graceful shutdown](architecture.md#graceful-shutdown-sequence). See
[Metrics](configuration.md#metrics) for metric series, labels and histogram buckets.

## Tracing

Select the OTLP exporter and configure a collector's HTTP trace endpoint using
the [tracing settings](configuration.md#tracing). For LangSmith, enable its
destination and provide an API key, optionally selecting a project and the
region's or self-hosted API address shown in LangSmith. No collector is required
when exporting directly to LangSmith. In an agent-based LangSmith workspace, set
the project to the target agent environment's tracer session name, because the
service addresses traces by project.

Content capture sends prompts, questions, document excerpts, embedding inputs and
answers outside the process. Enable it only for data that may be sent to every
configured destination. Without it, query spans contain byte counts and a short
digest rather than question text; model spans retain model settings and token
counts. Export failures emit a warning without credentials or response bodies;
LangSmith HTTP failures include only the response status code.

Use the configured sampler to control export volume. Parent-based sampling
follows incoming W3C trace context; ratio sampling can reduce newly created
traces. LangSmith discards spans whose parent it never receives, so it receives a
request that continues an incoming trace as a root span in the same trace; the
generic destination keeps the caller's parent. Request IDs remain available as
span attributes. Ingestion acceptance and background processing create separate
traces correlated by `document_id`.
Health probes are excluded from request tracing, and unparented database polling
does not create traces. Pending spans flush at the end of
[graceful shutdown](architecture.md#graceful-shutdown-sequence).

## Container Image

The service is packaged using a multi-stage Docker build that outputs a minimal, non-root runtime container based on Alpine Linux.

### Build Stages

Both base images are pinned by multi-platform index digest in the Dockerfile. Dependabot updates their tags and digests together.

1. **`base`**: Builds on `golang:1.27.3-alpine3.23`, installs compilation tools (`gcc`, `musl-dev`, `git`), and downloads Go modules.
2. **`test`**: Runs static checks (`go vet ./...`) and the short suite with data race instrumentation (`go test -short ./... -count=1 -race`); Docker-backed tests run in full on the host or CI runner.
3. **`lint`**: Compiles and executes `golangci-lint` (v2.15.0) against `.golangci.yml`.
4. **`builder`**: Compiles the statically linked binary (`CGO_ENABLED=0 go build -o /bin/server ./cmd/server`) and collects the license and notice files of the Go standard library and every linked module.
5. **`runtime`**: Uses the Alpine Linux runtime pinned in the Dockerfile. Installs CA certificates and timezone data, creates a dedicated system user and group (`app`, numeric UID/GID `10001:10001`), copies the compiled server binary containing the embedded migrations, and sets the non-root user as the execution user.

For host verification, `make vet`, `make lint` and `make test` run directly with the local Go toolchain; the full test suite needs a Docker daemon. The [`make ci` description](development.md#test-suites) lists the combined checks.

To compile the runtime image:

```sh
docker build --target runtime -t quarry-rag:local .
```

To run the container with an environment file:

```sh
docker run -d \
  --name quarry-rag \
  --user 10001:10001 \
  --env-file .env \
  -p 8081:8081 \
  quarry-rag:local
```

---

## Releases

Tagged releases contain an archive for every main package under `cmd/`. The
binary for `cmd/server` is `quarry-server`; other command names gain the
`quarry-` prefix unless they already have it. For example, `cmd/quarry-mcp`
produces `quarry-mcp`.

| Asset | Contents and targets |
|-------|----------------------|
| `<binary>_<version>_<os>_<arch>.tar.gz` | Linux and macOS (`linux`, `darwin`), each on `amd64` and `arm64` |
| `<binary>_<version>_windows_<arch>.zip` | Every command except `quarry-server`, on `amd64` and `arm64` |
| `quarry-rag_<version>_checksums.txt` | SHA-256 checksums for every archive |
| `quarry-rag_<version>.intoto.jsonl` | Sigstore bundle containing signed SLSA build provenance for the archives and checksum file |

Each archive has one top-level directory matching its filename without the
archive extension. It contains the binary (`.exe` on Windows), `README.md`, the
project `LICENSE` and the license and notice files of the Go standard library and
every linked module under `licenses/`.
The runtime image carries these notices at `/usr/share/licenses/quarry-rag`.
The provenance provides SLSA build level 2; the runtime image digest is also
attested and stored in the container registry.

Download and verify a release, setting `tag` to its tag:

```sh
tag=v0.1.0
version=${tag#v}
dir=$(mktemp -d)
gh release download "$tag" --repo Victoroide/quarry-rag --dir "$dir"
cd "$dir"
sha256sum -c "quarry-rag_${version}_checksums.txt"
for f in *.tar.gz *.zip "quarry-rag_${version}_checksums.txt"; do
  test -f "$f" || continue
  gh attestation verify "$f" --repo Victoroide/quarry-rag \
    --bundle "quarry-rag_${version}.intoto.jsonl" \
    --signer-workflow Victoroide/quarry-rag/.github/workflows/release.yml \
    --source-ref "refs/tags/$tag" --deny-self-hosted-runners
done
gh attestation verify "quarry-server_${version}_linux_amd64.tar.gz" --repo Victoroide/quarry-rag
tar -tzf "quarry-server_${version}_linux_amd64.tar.gz" | grep -c '/licenses/'
docker buildx imagetools inspect "ghcr.io/victoroide/quarry-rag:$version"
gh attestation verify "oci://ghcr.io/victoroide/quarry-rag:$version" --repo Victoroide/quarry-rag
```

The license count must be greater than zero. The image manifest must include
`linux/amd64` and `linux/arm64`. On macOS, use `shasum -a 256 -c` in place of
`sha256sum -c` if needed.

## Document Extraction Processes

Each concurrent multipart upload starts at most one extraction child process. The service binary must be able to execute itself; the runtime filesystem and process policy must allow this.

Size container memory to at least `base + EXTRACT_CONCURRENCY × EXTRACT_MEMORY_LIMIT_MB`, where `base` covers the server, buffered uploads and background ingestion. The child memory setting is a soft Go limit; allow additional headroom for runtime overhead and temporary allocations. For child process sandboxing, stack limits, timeouts and termination semantics, see [Extraction Boundaries](architecture.md#extraction-boundaries).

## Concurrency Limits

Query admission, embedding and extraction limits are process-local, so the cluster-wide maximum is each per-replica value multiplied by the replica count. `QUERY_CONCURRENCY` bounds concurrent queries and therefore concurrent synthesis calls. `EMBED_CONCURRENCY` bounds concurrent embedding calls from ingestion and queries together. `EXTRACT_CONCURRENCY` bounds concurrent multipart uploads, which use local CPU and memory but no model quota.

When replicas share an account quota expressed in concurrent requests, the largest per-replica value that stays within it is:

`per-replica limit = floor(account quota / replicas)`

Apply this formula to `QUERY_CONCURRENCY` and `EMBED_CONCURRENCY` with the quotas of the models they call, and to `EXTRACT_CONCURRENCY` when the deployment sets its own cluster-wide upload limit. Each result must stay within the variable's [bounds](configuration.md#environment-variables); a result below 1 means the quota cannot support that many replicas. Retries run inside these limits, so they add requests but not concurrency.

## Required External Services

In production deployments (`GO_RAG_FAKE_MODE=false`), the microservice connects to four external cloud services, configured via environment variables.

### 1. PostgreSQL (Metadata and Chunk Store)

- **Version**: PostgreSQL 14 or later.
- **Extensions**: `pgcrypto` (applied automatically by migration 001).
- **Configuration**: Provided via `DATABASE_URL`.
- **Purpose**: Stores document records, status indicators, chunk texts, token counts, and SHA-256 content fingerprints.
- **Operations**: Versioned startup migrations, duplicate fingerprint lookups, chunk insertion transactions, document status tracking, and inventory listings. Replicas serialize migrations using an advisory lock; see [Schema Migrations](configuration.md#schema-migrations).

### 2. Pinecone (Vector Index)

- **Configuration**: `PINECONE_API_KEY` and an explicit `PINECONE_INDEX_NAME`, for example `quarry-rag`. Both are required when external services are enabled; the index name has no default. `PINECONE_ENVIRONMENT` is ignored and logs a startup warning when non-empty; remove it from deployment configuration.
- **API Version**: The v5 Go SDK uses Pinecone API version `2025-10`.
- **Startup Requirements**: The index dimension must equal [`EMBEDDING_DIMENSIONS`](configuration.md#environment-variables), the metric must be `cosine`, and the index must be ready. Startup fails if any requirement is unmet.
- **Request Limits**: Upserts use at most [`PINECONE_UPSERT_BATCH_SIZE`](configuration.md#environment-variables) records and 1.5 MiB of estimated encoded data per batch. `PINECONE_TIMEOUT` bounds each attempt, and `PINECONE_MAX_ATTEMPTS` limits write and deletion attempts.
- **Deletion**: If metadata-filter deletion returns `InvalidArgument`, `Unimplemented`, or `FailedPrecondition`, the service queries the default namespace with an exact `doc_id` filter and deletes the returned identifiers in batches of at most 1,000. Each identifier is deleted once per operation, so stale query results do not repeat deletions. Enumeration stops when no new identifiers appear or after deleting a result with fewer than 1,000 matches, and otherwise fails after 10 rounds. Missing vectors and namespaces are accepted as already deleted.
- **Purpose**: High-dimensional vector search. Chunks are upserted only into the default namespace (`""`), which the SDK maps to `__default__` and the console displays as `__default__`, with `doc_id` metadata identifying each document. Per-document namespaces created by earlier versions are removed when their document is deleted. All namespace operations share one connection, which closes only when the client shuts down.

#### SDK Deployment Verification

Before upgrading, record the UUID of a completed, searchable staging document in the default namespace. Deploy to staging first, with `GO_RAG_FAKE_MODE=false`, and expose the staging server at `localhost:8081`. Configure the test environment with the staging service token and the same external service settings as the staging server.

Before promoting the upgrade, the opt-in smoke suite must pass against the deployment's own index with `RUN_SMOKE=1` and `SMOKE_EXISTING_DOCUMENT_ID` set to a document ingested before the upgrade. The Pinecone service determines whether the legacy empty namespace (`""`) and `__default__` refer to the same data; the SDK mapping alone does not establish their equivalence.

Run the smoke suite with that preexisting document UUID:

```sh
RUN_SMOKE=1 SMOKE_EXISTING_DOCUMENT_ID="<pre-upgrade-document-uuid>" \
  go test ./test/smoke/... -count=1 -v -timeout 600s
```

Confirm that `TestSmoke_PreexistingDocumentRemainsRetrievable` passes without skipping: the direct index check must find the preexisting document's vectors, and a service query restricted to that document must return at least one chunk from it. If the direct check succeeds but the service reports `no_evidence`, check `MIN_RELEVANCE_SCORE` and the query before retrying. This check reads the document without reingesting or deleting it. The other smoke checks create and delete their own documents. Promote only after the staging suite passes. To roll back, revert the SDK upgrade and redeploy; stored documents and vectors are unaffected.

### 3. AWS Bedrock (Embedding and LLM Models)

- **Configuration**: `AWS_REGION` and model identifiers (`BEDROCK_EMBED_MODEL`, `BEDROCK_PRIMARY_MODEL`, `BEDROCK_FALLBACK_MODEL`). Static `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are optional and must be configured together; temporary static credentials also accept `AWS_SESSION_TOKEN`.
- **Credentials**: IAM roles are supported. Without static keys, AWS clients use the default credential chain, including environment credentials, shared configuration files, web identity, container roles and instance roles.
- **Query and proxy timeouts**: `QUERY_CONCURRENCY` bounds admitted queries [per replica](#concurrency-limits), and `QUERY_QUEUE_TIMEOUT` limits admission waits. Each admitted query has a [`QUERY_TIMEOUT`](configuration.md#environment-variables) budget, with a nested `SYNTHESIS_TIMEOUT`. Set proxy and load balancer response timeouts to at least `HTTP_WRITE_TIMEOUT`. Keep adequate margin for reading request bodies and waiting for admission when configuring write timeouts.
- **Retries and Deadlines**: Bedrock and S3 use the same retry policy, with [`AWS_MAX_ATTEMPTS`](configuration.md#environment-variables) attempts, up to 10 seconds of backoff, no client-side retry quota and retries for `ModelTimeoutException` and `ModelNotReadyException`. The SDK is the only transport retry layer; the ambassador makes one client call per operation. Each embedding call has an `EMBED_TIMEOUT` deadline, including waiting for a concurrency slot and retries.
- **Required Model Access**:
  - Titan Embed Text V2 (`amazon.titan-embed-text-v2:0`): generates 1024-dimensional normalized text embeddings.
  - Nova Micro (`amazon.nova-micro-v1:0`): fast, low-latency reasoning and synthesis.
  - Llama 4 Maverick (`us.meta.llama4-maverick-17b-instruct-v1:0`): synthesis and comparison across multiple retrieved chunks.

### 4. AWS S3 (Raw Object Storage)

- **Configuration**: Set `S3_BUCKET` explicitly to a bucket you control, using the same AWS credentials configured for Bedrock. Set optional `S3_EXPECTED_BUCKET_OWNER` to the owner's 12-digit AWS account ID. An empty bucket disables original file storage and logs a startup warning.
- **Startup Verification**: Client initialization and `HeadBucket` share a 10-second deadline. Access denied (403), a missing bucket (404), or an expected-owner mismatch prevents startup. Transient errors log a warning and allow startup to continue; an initialized client remains available for later requests.
- **Purpose**: Stores original binary uploads (PDF, DOCX, DOC) and generates presigned GET download links valid for up to 15 minutes. A download signed with temporary credentials stops working when those credentials expire, which can be before 15 minutes. Downloads use attachment disposition with the stored filename and an extension-derived content type. Downloaders need no expected-owner header.
- **IAM Permissions Required**:
  - `s3:PutObject` on `arn:aws:s3:::{bucket}/documents/*`
  - `s3:GetObject` on `arn:aws:s3:::{bucket}/documents/*`
  - `s3:DeleteObject` on `arn:aws:s3:::{bucket}/documents/*`
  - `s3:ListBucket` on `arn:aws:s3:::{bucket}`, required by the startup and health `HeadBucket` checks.

---

## Vector Index Maintenance

Use `make reconcile` during a maintenance window to inspect orphaned vectors and
legacy document namespaces. Configure the same PostgreSQL database and Pinecone
index as the deployment, ensure the ingestion job migrations have been applied,
and pause ingestion and other index writers until reconciliation finishes.
Always inspect a dry run before applying deletions:

```sh
make reconcile ARGS="-limit=10000 -page=100"
make reconcile ARGS="-apply -limit=10000 -page=100"
```

Without `-apply`, the command only reads and prints JSON counts and a bounded
sample. It preserves stored documents, processing documents and failed documents
with retry jobs. Default-namespace vectors with missing or invalid `doc_id`, a
missing document row, or a terminally failed document are eligible. UUID-named
legacy namespaces are eligible only when no document row exists.

The default limit is 100000 vectors across selected default-namespace IDs and
the reported counts of whole legacy namespaces; namespaces that exceed the
remaining budget are skipped. Counts are eventually consistent, so use a quiet
index and rerun the dry run after deletion has propagated. Pod-based indexes
without vector listing support reconcile legacy namespaces only and report that
limitation on stderr. See the operational scripts documentation for page sizes,
output fields and partial deletion failures.

## Health Probes and Readiness

The service exposes three unauthenticated probe endpoints:

- Use `GET /livez` for startup and liveness checks.
- Use `GET /readyz` for readiness checks.
- Use `GET /health` for diagnostics.

See [Endpoints](api.md#endpoints) for probe responses and status codes.

---

## Graceful Shutdown

Set the termination grace period to at least `SHUTDOWN_DRAIN_DELAY + 40s` to allow traffic removal, HTTP requests, worker completion and lease release to settle before sending `SIGKILL`. No preStop hook is needed. See [Graceful Shutdown Sequence](architecture.md#graceful-shutdown-sequence) for the internal shutdown order and queue recovery.

---

## Service Token Rotation

The service accepts any token configured in [`SERVICE_TOKENS` or `SERVICE_TOKEN`](configuration.md#environment-variables). To rotate tokens without downtime:

1. Add the new token to `SERVICE_TOKENS`, keep the old token configured, and roll the deployment. Wait until every replica accepts both tokens before updating callers.
2. Move callers to the new token and verify authenticated requests succeed.
3. Remove the old token from `SERVICE_TOKENS` and `SERVICE_TOKEN`, wherever configured, and roll the deployment again. After all replicas restart, the old token receives `401 Unauthorized`.
