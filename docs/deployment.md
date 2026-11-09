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

PostgreSQL connections also add up across replicas. Each replica opens one pool of at most `pool_max_conns` connections, set in [`DATABASE_URL`](configuration.md#environment-variables); startup migrations use this pool. Maintenance connections are administrative sessions plus the separate pools of `make cleanup` and `make reconcile`, each capped by the same setting. The total must fit within `max_connections` less the server's reserved slots, such as `superuser_reserved_connections`:

`replicas × pool_max_conns + maintenance connections ≤ max_connections − reserved slots`

## Required External Services

In production deployments (`GO_RAG_FAKE_MODE=false`), the microservice uses PostgreSQL and AWS Bedrock, with a selectable vector backend and optional S3 storage. See [configuration](configuration.md#environment-variables) for backend selection and credentials. [PDF OCR](#optional-pdf-ocr) adds optional text detection.

### 1. PostgreSQL (Metadata and Chunk Store)

- **Version**: PostgreSQL 14 or later.
- **Extensions**: `pgcrypto` (applied automatically by migration 001).
- **Configuration**: Provided via `DATABASE_URL`.
- **Purpose**: Stores document records, status indicators, chunk texts, token counts, and SHA-256 content fingerprints.
- **Operations**: Versioned startup migrations, duplicate fingerprint lookups, chunk insertion transactions, document status tracking, and inventory listings. Replicas serialize migrations using an advisory lock; see [Schema Migrations](configuration.md#schema-migrations).

### 2. Pinecone (Vector Index)

- **Configuration**: `PINECONE_API_KEY` and an explicit `PINECONE_INDEX_NAME`, for example `quarry-rag`. Required settings depend on the selected backend; see [configuration](configuration.md#environment-variables). The index name has no default. `PINECONE_ENVIRONMENT` is ignored and logs a startup warning when non-empty; remove it from deployment configuration.
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

### PostgreSQL Vectors

The PostgreSQL vector backend requires pgvector 0.8 or later installed on the database server and a database role allowed to create its tables and indexes. The role must also be allowed to create the `vector` extension unless an administrator creates it beforehand; an existing extension requires no extension-creation privilege. The standard PostgreSQL image in the offline example does not include this extension; use a PostgreSQL distribution with pgvector available. Base PostgreSQL requirements remain as described above. The [vector schema](architecture.md#postgresql-vector-schema) is initialized at startup.

Changing backends does not copy existing embeddings. Re-ingest documents with new identities in the target backend, or migrate their vectors before switching query traffic. Existing completed documents are not automatically queued again. Use the same backend across replicas sharing an ingestion queue. Bedrock access is still required for embeddings and synthesis; the PostgreSQL backend removes the Pinecone dependency only. Hosted-index reconciliation applies to Pinecone deployments.

### 3. AWS Bedrock (Embedding and LLM Models)

- **Guardrail permission**: When [optional guardrails](configuration.md#environment-variables) are enabled, grant `bedrock:ApplyGuardrail` on the target guardrail ARN, `arn:aws:bedrock:{region}:{account-id}:guardrail/{guardrail-id}`, in addition to model invocation permissions. Create the guardrail and configure its prompt-attack and contextual grounding policies before enabling it in the service.
- **Configuration**: `AWS_REGION` and model identifiers (`BEDROCK_EMBED_MODEL`, `BEDROCK_PRIMARY_MODEL`, `BEDROCK_FALLBACK_MODEL`). Static `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are optional and must be configured together; temporary static credentials also accept `AWS_SESSION_TOKEN`.
- **Credentials**: IAM roles are supported. Without static keys, AWS clients use the default credential chain, including environment credentials, shared configuration files, web identity, container roles and instance roles.
- **Query and proxy timeouts**: Admitted queries run under the [query time budget](architecture.md#query-time-budget). Set proxy and load balancer response timeouts to at least `HTTP_WRITE_TIMEOUT`. Keep adequate margin for reading request bodies and waiting for admission when configuring write timeouts.
- **Retries and Deadlines**: Bedrock and S3 use the same retry policy: the SDK's standard retries with [`AWS_MAX_ATTEMPTS`](configuration.md#environment-variables) attempts, a 10-second maximum backoff, no client-side retry quota, and `ModelTimeoutException` and `ModelNotReadyException` added to the retryable errors. Each embedding call has an [`EMBED_TIMEOUT`](configuration.md#environment-variables) deadline. See the [query time budget](architecture.md#query-time-budget) for the single transport retry layer.
- **Required Model Access**:
  - Titan Embed Text V2 (`amazon.titan-embed-text-v2:0`): generates 1024-dimensional normalized text embeddings.
  - Nova Micro (`amazon.nova-micro-v1:0`): fast, low-latency reasoning and synthesis.
  - Llama 4 Maverick (`us.meta.llama4-maverick-17b-instruct-v1:0`): synthesis and comparison across multiple retrieved chunks.
- **Reranking permissions**: Optional [reranking](api.md#post-query) requires `bedrock:Rerank` with `Resource: "*"` and `bedrock:InvokeModel` scoped to the configured reranking model ARN. Enable access to that model in the configured AWS region before querying.

### OpenAI-compatible providers

Select chat and embedding backends independently using the [provider configuration](configuration.md#environment-variables). Supply an explicit base address including the API version prefix required by your server, and model names supported by that server. Configure secrets through your deployment's secret store. Pinecone and PostgreSQL remain required in external-service mode. When both backends use compatible endpoints and reranking and original-file storage are disabled, startup does not initialize AWS clients.

Chat sends JSON to `/chat/completions`; embeddings send JSON to `/embeddings` beneath the configured base address. Chat requests send `max_tokens` and `temperature`; use a chat model that accepts both. Embedding requests ask for float encoding and do not request a dimension count; set `EMBEDDING_DIMENSIONS` to the model's output size. An existing index must use the same embedding model and vector size; changing the embedding model requires re-embedding the corpus into a matching index.

Synchronous queries use one chat completion request. Streamed queries use the streaming chat API with usage requested, deliver text deltas as they arrive, and require a finish reason followed by the `[DONE]` marker. Usage omitted by a compatible server remains unavailable and query token counts stay zero. Malformed events, premature EOF, upstream error events and callback failures stop the stream without a successful final response; a caller cancellation closes the upstream body. HTTP 429 and 503 map to the existing `upstream_throttled` response. The HTTP clients do not retry or follow redirects, and discard upstream error bodies. Caller deadlines bound requests; embedding concurrency and call deadlines use the same limits as Bedrock.

OpenAI-compatible tracing sets `gen_ai.system` and `gen_ai.provider.name` to `openai` and records model, finish reason and reported token counts.

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

## Optional PDF OCR

Enable the [OCR configuration](configuration.md#environment-variables) to accept
[image-only PDFs](api.md#image-only-pdf-ocr). The S3 bucket and Textract endpoint
must be in the same AWS Region. The service uses its shared AWS credential chain
and retry policy. Grant `textract:StartDocumentTextDetection` and
`textract:GetDocumentTextDetection` on `*`; Textract reads the original with the
caller's existing `s3:GetObject` permission. SSE-KMS objects also need
`kms:Decrypt` on the object's key. Polling requires no SNS topic or notification role. Enable OCR only after every replica runs a release with OCR support, and let pending OCR documents finish before rolling back; replicas without it fail those documents with `no chunks produced`.

Textract asynchronous PDF detection supports at most 3,000 pages and 500 MB per
document. The service's [upload and decoded-text limits](api.md#post-ingest)
still apply and can be lower. Detection adds latency and is billed per page;
Amazon's US West (Oregon) example charges $0.0015 per page for the first million
pages in a month. Rates vary by region and volume; check current Amazon Textract
pricing before enabling it. S3 and other ingestion charges also apply. Detection
can continue in AWS after a local timeout or deletion, and incurred charges are
not canceled by stopping ingestion.

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

Without `-apply`, the command only reads. Counts are eventually consistent, so
run it against a quiet index. The [operational scripts documentation](development.md#orphan-reconciliation-scriptsreconcile)
describes which vectors and namespaces are selected, default limits, page sizes,
output fields, index limitations, partial deletion failures and verification
after deletion.

If documents already have collection labels, complete the
[collection metadata backfill](development.md#collection-metadata-backfill) once
after upgrading to enable collection-filtered retrieval of their existing vectors.

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
