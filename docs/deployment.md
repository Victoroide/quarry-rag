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

1. **`base`**: Uses the Go image pinned in the [Dockerfile](../Dockerfile) for the requested platform, installs compilation tools (`gcc`, `musl-dev`, `git`), and downloads Go modules for the `test` and `lint` stages.
2. **`test`**: Runs static checks (`go vet ./...`) and the short suite with data race instrumentation (`go test -short ./... -count=1 -race`); Docker-backed tests run in full on the host or CI runner.
3. **`lint`**: Compiles and executes `golangci-lint` (v2.15.0) against `.golangci.yml`.
4. **`builder`**: Uses a native build-host stage with its own cached module-download layer. It cross-compiles the statically linked binary for the requested platform and collects the license and notice files of the Go standard library and every module linked for that target.
5. **`runtime`**: Uses the Alpine Linux runtime pinned in the Dockerfile for the target platform. Installs CA certificates and timezone data, creates a dedicated system user and group (`app`, numeric UID/GID `10001:10001`), copies the compiled server binary containing the embedded migrations, and sets the non-root user as the execution user.

A multi-platform build therefore compiles each architecture natively. Only the final stage, which installs the runtime packages, runs on the target platform and needs emulation when it differs from the build host.

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

## Content Deduplication Rollout

Deploy the release with migration 009 and leave `INGEST_CONTENT_DEDUP=collection`
until every replica that accepts uploads or consumes the shared queue has been
upgraded. Drain and stop earlier replicas before setting
`INGEST_CONTENT_DEDUP=off` on all ingest replicas. A remaining replica in
`collection` mode can still match previously shared content.

Switch back to `collection` on upgraded replicas to restore matching for new
uploads.

Previously returned IDs and existing shared mappings remain valid. Treat an
explicit-ID conflict as observable and apply authorization at the trusted
caller. With content matching disabled, repeated implicit uploads can each
consume database, vector, and original-file storage and incur separate
processing charges.

### Downgrading

Switch all upgraded replicas to `collection`. Pause ingestion traffic or
block explicit retries for retained private UUIDs at the trusted caller. Let
in-flight acceptances finish, then keep upgraded workers running to drain
pending and leased private jobs. Check the shared database before starting
earlier workers.

```sql
SELECT NOT EXISTS (
    SELECT 1
    FROM ingest_jobs AS j
    JOIN documents AS d ON d.id = j.document_id
    WHERE d.content_dedup = 'off'
) AS private_queue_drained;
```

Start earlier workers only when the check returns `true`. Keep the pause or
retry block in place throughout mixed-version queue consumption. Even in
`collection` mode, an upgraded replica can requeue a failed private UUID.
Earlier workers attempt to register a shared fingerprint at completion, and the
database rejects that write for private documents. Retained private IDs require
upgraded replicas for explicit retries.

## Concurrency Limits

Query admission, embedding and extraction limits are process-local, so the cluster-wide maximum is each per-replica value multiplied by the replica count. `QUERY_CONCURRENCY` bounds concurrent queries and therefore concurrent synthesis calls. `EMBED_CONCURRENCY` bounds concurrent embedding calls from ingestion and queries together. `EXTRACT_CONCURRENCY` bounds concurrent multipart uploads, which use local CPU and memory but no model quota.

When replicas share an account quota expressed in concurrent requests, the largest per-replica value that stays within it is:

`per-replica limit = floor(account quota / replicas)`

Apply this formula to `QUERY_CONCURRENCY` and `EMBED_CONCURRENCY` with the quotas of the models they call, and to `EXTRACT_CONCURRENCY` when the deployment sets its own cluster-wide upload limit. Each result must stay within the variable's [bounds](configuration.md#environment-variables); a result below 1 means the quota cannot support that many replicas. Retries run inside these limits, so they add requests but not concurrency.

PostgreSQL connections also add up across replicas. Each replica opens one pool of at most `pool_max_conns` connections, set in [`DATABASE_URL`](configuration.md#environment-variables); startup migrations use this pool. Maintenance connections are administrative sessions plus the separate pools of `make cleanup` and `make reconcile`, each capped by the same setting. The total must fit within `max_connections` less the server's reserved slots, such as `superuser_reserved_connections`:

`replicas × pool_max_conns + maintenance connections ≤ max_connections − reserved slots`

## Query and Proxy Timeouts

Admitted queries follow the [query time budget](architecture.md#query-time-budget).
Set proxy and load balancer response timeouts to at least `HTTP_WRITE_TIMEOUT`.
Allow time to read request bodies and wait for admission when configuring the
write timeout.

## Required External Services

Every deployment needs [PostgreSQL for metadata and chunks](#postgresql-metadata-and-chunk-store).
Outside offline fake mode, select a vector store, an embedding provider and a
chat provider independently. Use the matrix to provision dependencies, then set
the [environment variables](configuration.md#environment-variables) for the
selected services.

| `VECTOR_STORE` | `EMBEDDING_PROVIDER` | `LLM_PROVIDER` | Additional prerequisites |
|---|---|---|---|
| `pinecone` | `bedrock` | `bedrock` | Pinecone index and key, AWS access for Bedrock embeddings and chat |
| `pinecone` | `bedrock` | `openai` | Pinecone index and key, AWS access for Bedrock embeddings, compatible chat endpoint |
| `pinecone` | `openai` | `bedrock` | Pinecone index and key, compatible embeddings endpoint, AWS access for Bedrock chat |
| `pinecone` | `openai` | `openai` | Pinecone index and key, compatible embeddings and chat endpoint |
| `postgres` | `bedrock` | `bedrock` | pgvector, AWS access for Bedrock embeddings and chat |
| `postgres` | `bedrock` | `openai` | pgvector, AWS access for Bedrock embeddings, compatible chat endpoint |
| `postgres` | `openai` | `bedrock` | pgvector, compatible embeddings endpoint, AWS access for Bedrock chat |
| `postgres` | `openai` | `openai` | pgvector, compatible embeddings and chat endpoint |

With `VECTOR_STORE=postgres`, both providers set to `openai` and the options
below disabled, leave the AWS region and credentials and the Pinecone index
name and key unset. See the [compatible chat, streaming and embedding
protocols](#openai-compatible-providers) for what the endpoint must support.
For a Bedrock provider, configure [AWS credentials](#aws-credentials) and
grant [access to the selected models](#aws-bedrock-models).

Apply the following prerequisites for each optional feature you enable.

- **Stored originals** require a private [S3 bucket](#aws-s3-original-file-storage)
  and [AWS access](#aws-credentials).
- **Image-only PDF OCR** requires [Textract](#optional-pdf-ocr) and the S3 source bucket.
- **Reranking** requires [AWS access to the reranking model](#bedrock-reranking),
  even when chat and embeddings both use compatible providers.
- **Guardrails** require [Bedrock chat and guardrail permissions](#bedrock-guardrails).
  Pairing a guardrail with `LLM_PROVIDER=openai` fails configuration validation,
  even when embeddings use Bedrock.

For `GO_RAG_FAKE_MODE=true`, provision only the metadata database. See the
[offline setup](../README.md#offline-fake-mode).

### PostgreSQL Metadata and Chunk Store

Set `DATABASE_URL` to a PostgreSQL 14 or later database. It stores document
records, ingestion status, chunk text and token counts, and SHA-256 content
fingerprints. [Startup migrations](configuration.md#schema-migrations) create
the schema and enable `pgcrypto`.

### Pinecone Vector Index

- Set `PINECONE_API_KEY` and `PINECONE_INDEX_NAME` for the hosted index.
- **API Version**: The v5 Go SDK uses Pinecone API version `2025-10`. With `INDEX_VISIBILITY_MODE=lsn`, ingestion upserts and visibility probes use the `2026-07` HTTP data plane. Both pin the unique index host and use the SDK's `__default__` namespace for ingestion.
- **Startup Requirements**: The index dimension must equal [`EMBEDDING_DIMENSIONS`](configuration.md#environment-variables), the metric must be `cosine`, and the index must be ready. Startup fails if any requirement is unmet.
- **Request Limits**: Upserts use at most [`PINECONE_UPSERT_BATCH_SIZE`](configuration.md#environment-variables) records per batch. The 1.5 MiB batch limit uses an estimate for SDK requests and the serialized size for HTTP requests. `PINECONE_TIMEOUT` bounds each attempt, and `PINECONE_MAX_ATTEMPTS` limits write and deletion attempts.
- **Deletion**: If metadata-filter deletion returns `InvalidArgument`, `Unimplemented`, or `FailedPrecondition`, the service queries the default namespace with an exact `doc_id` filter and deletes the returned identifiers in batches of at most 1,000. Each identifier is deleted once per operation, so stale query results do not repeat deletions. Enumeration stops when no new identifiers appear or after deleting a result with fewer than 1,000 matches, and otherwise fails after 10 rounds. Missing vectors and namespaces are accepted as already deleted.
- **Purpose**: High-dimensional vector search. Ingested chunks use `doc_id` metadata to identify each document. Per-document namespaces created by earlier versions are removed when their document is deleted. SDK namespace operations share one connection, which closes when the client shuts down.

#### Index Visibility Rollout

Deploy the readers and migration 012 before enabling `lsn` on all replicas that
share the ingestion queue. Earlier workers can still complete jobs with a null
observation. Drain or pause the queue while switching modes if every new job must
record an LSN observation. Pinecone startup rejects `lsn` for indexes outside its
serverless service. To roll back, disable `lsn` and allow active waits to finish.
Both modes use the same job states and can read the same stored documents.

Before enabling `lsn`, set `QUARRY_PINECONE_STAGING_INDEX` to a serverless staging
index and provide its `PINECONE_API_KEY`. Verify both write receipts and the query
watermark in an isolated namespace. The check deletes its test vectors.

```sh
GOMAXPROCS=4 go test -p 2 -count=1 -v ./internal/store/pinecone -run '^TestStagingServerlessVisibilityHeaders$'
```

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

Install pgvector 0.8 or later on the PostgreSQL server, for example by using
the `pgvector/pgvector:0.8.7-pg16` image or installing the pgvector package for
your PostgreSQL major version. The stock PostgreSQL image in the offline quick
start lacks pgvector. For a data-only serving login, follow the
[owner preparation and role instructions](grant-catalog.md#database-roles).
Deployments that initialize pgvector at service startup need a serving login
with the privileges described under
[vector schema initialization](architecture.md#postgresql-vector-schema).

With the default `PGVECTOR_EXACT_SCOPE_MAX=0`, startup skips the exact-scope
composite indexes. With exact search enabled, startup validates both indexes and
builds missing ones concurrently. On a populated database, prebuild them before
enabling exact search so the builds can finish outside the 30-second startup
deadline. Run the catalog check with the application's search path and a
superuser or a role with effective `pg_read_all_stats` privileges. Inspect both
names in the schema that owns `chunk_vectors`.

```sql
SELECT n.nspname AS vector_schema, expected.name, object.relkind,
       i.indrelid = v.oid AS on_vector_table,
       i.indisvalid AS valid, i.indisready AS ready, i.indislive AS live,
       CASE WHEN pg_has_role('pg_read_all_stats', 'USAGE') THEN
           EXISTS (SELECT FROM pg_stat_progress_create_index p
                   WHERE p.index_relid = object.oid)
       END AS building,
       pg_get_indexdef(i.indexrelid) AS definition
FROM pg_class v
JOIN pg_namespace n ON n.oid = v.relnamespace
CROSS JOIN (VALUES ('idx_chunk_vectors_collection_doc_type'),
                   ('idx_chunk_vectors_doc_type_document_id')) AS expected(name)
LEFT JOIN pg_class object ON object.relnamespace = v.relnamespace
                         AND object.relname = expected.name
LEFT JOIN pg_index i ON i.indexrelid = object.oid
WHERE v.oid = 'chunk_vectors'::regclass;
```

For each existing object, verify `relkind = 'i'` and `on_vector_table = true`.
Both indexes must be valid, ready and live. Compare their definitions with the
nonunique B-tree statements below, including the key order. Both use default
operator classes, collations and sort order. An expression, included column or
predicate makes the definition incompatible.

For each missing index, run the matching statement separately outside a
transaction block. Replace `public` with the reported vector schema if needed,
then repeat the catalog check.

```sql
SET statement_timeout = 0;
CREATE INDEX CONCURRENTLY idx_chunk_vectors_collection_doc_type ON public.chunk_vectors USING btree (collection, doc_type, document_id);
CREATE INDEX CONCURRENTLY idx_chunk_vectors_doc_type_document_id ON public.chunk_vectors USING btree (doc_type, document_id);
```

If startup reports a name conflict or incompatible definition, inspect that
object and resolve the conflict before enabling exact search. If `building` is
true, wait for the build to finish. A null value means progress visibility is
insufficient. Have a privileged operator repeat the check in that case.

Before recovering an interrupted build, confirm it belongs to `chunk_vectors`,
matches the intended definition and has `valid = false`, `live = true` and
`building = false`. The table owner can then drop the schema-qualified index
with `DROP INDEX CONCURRENTLY` and run the matching create statement as a
separate command. Repeat the catalog check before enabling exact search.

### Changing Models or Vector Stores

A chat model change can reuse the existing vectors when the embedding model and
its output dimensions stay the same. When changing the embedding model,
re-embed the corpus into a matching index before switching query traffic, even
if the dimensions stay the same.

When changing only the vector store, migrate the existing vectors or re-ingest
documents with new identities in the target store before switching query
traffic. A configuration change leaves completed documents in their current
state. Use the same vector store and embedding model across replicas sharing an
ingestion queue.

### AWS Credentials

For any selected AWS service, set `AWS_REGION` and give the service access to
the corresponding resources. AWS clients use the default credential chain,
including shared configuration, web identity and IAM roles. Supply
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` together when using static
credentials. Include `AWS_SESSION_TOKEN` when those credentials are temporary.

### AWS Retries and Deadlines

Bedrock and S3 use the SDK's standard retries with
[`AWS_MAX_ATTEMPTS`](configuration.md#environment-variables) attempts, a
10-second maximum calculated backoff and no client-side retry quota. The service
adds `ModelTimeoutException` and `ModelNotReadyException` to the retryable
errors. A valid `x-amz-retry-after` header can extend the calculated backoff
by up to five seconds, subject to the request deadline. Each embedding call
has an [`EMBED_TIMEOUT`](configuration.md#environment-variables) deadline.
See the [query time budget](architecture.md#query-time-budget) for the transport
retry layer.

### AWS Bedrock Models

- For Bedrock embeddings, grant `bedrock:InvokeModel` for `BEDROCK_EMBED_MODEL`.
- For Bedrock chat, grant `bedrock:InvokeModel` for the models selected by the
  synthesis policy from `BEDROCK_PRIMARY_MODEL` and `BEDROCK_FALLBACK_MODEL`.
  Streamed answers also require `bedrock:InvokeModelWithResponseStream` for those
  models. When [`SYNTHESIS_INPUT_PROFILE`](configuration.md#environment-variables)`=strict`,
  also grant `bedrock:CountTokens` for the foundation models selected for
  [strict synthesis](configuration.md#strict-synthesis-budgets). For a
  geographic inference profile, including the default fallback,
  grant the applicable invoke actions on the profile ARN in the source Region
  and on the foundation model ARN in the source Region and every destination
  Region for that source Region.

#### Bedrock Guardrails

Create the guardrail with its prompt-attack and contextual grounding policies
before enabling it. Grant `bedrock:ApplyGuardrail` on its ARN
(`arn:aws:bedrock:{region}:{account-id}:guardrail/{guardrail-id}`) in addition
to the chat model's invocation permissions.

#### Bedrock Reranking

Optional [reranking](api.md#post-query) requires `bedrock:Rerank` with
`Resource: "*"` and `bedrock:InvokeModel` scoped to the model specified by
`RERANK_MODEL_ARN`. Enable access to that model in the configured AWS region.

### OpenAI-compatible providers

Both compatible clients use the same `OPENAI_BASE_URL` and optional
`OPENAI_API_KEY`. Include the API version prefix required by the endpoint and
set the model name for each selected compatible provider.

Chat sends JSON to `/chat/completions` with `max_tokens` and `temperature`.
See [strict synthesis budgets](configuration.md#strict-synthesis-budgets) for
deployment matching.
Embeddings send JSON to `/embeddings` and request float encoding. Both paths are
relative to the configured API base. Set `EMBEDDING_DIMENSIONS` to the model's
output size.

Synchronous queries use one chat completion request. Streamed queries use the streaming chat API with usage requested, deliver text deltas as they arrive, and require a finish reason followed by the `[DONE]` marker. Usage omitted by a compatible server remains unavailable and query token counts stay zero. Malformed events, premature EOF, upstream error events and callback failures stop the stream without a successful final response; a caller cancellation closes the upstream body. HTTP 429 and 503 map to the existing `upstream_throttled` response. The HTTP clients do not retry or follow redirects, and discard upstream error bodies. Caller deadlines bound requests; embedding concurrency and call deadlines use the same limits as Bedrock.

OpenAI-compatible tracing sets `gen_ai.system` and `gen_ai.provider.name` to `openai` and records model, finish reason and reported token counts.

### AWS S3 Original File Storage

- Set `S3_BUCKET` to a bucket you control. Use `S3_EXPECTED_BUCKET_OWNER` to
  verify ownership when needed.
- **Startup Verification**: Client initialization and `HeadBucket` share a 10-second deadline. Access denied (403), a missing bucket (404), or an expected-owner mismatch prevents startup. Transient errors log a warning and allow startup to continue; an initialized client remains available for later requests.
- S3 stores originals for [accepted multipart uploads](api.md#variant-b-multipart-form-upload),
  including recognized plain-text files and binary documents. Clients request
  [presigned GET URLs for downloads](api.md#get-documentsdoc_idfile). Temporary
  signing credentials limit new download requests to the end of their AWS
  session. Revoking the signing principal's read permission, deleting or
  deactivating its signing key, or tightening an applicable IAM or bucket
  policy can also end access early. Downloaders need no expected-owner header.
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

### Mixed PDF OCR Rollout

Deploy migration 015 with `OCR_PDF_MODE=image_only`. If the serving login and
metadata owner are separate, have the metadata owner apply the
`document_pdf_extractions` grant from [role provisioning](grant-catalog.md#database-roles)
before starting the upgraded binary. This grant is required even when keeping
`image_only` because listing and job claims read the table. Upgrade every replica
that accepts uploads or consumes the queue, and stop earlier consumers before
enabling `OCR_PDF_MODE=mixed`. Keep Textract configured on the processing
replicas. The database refuses new mixed work while an incompatible consumer
holds a live lease. While mixed work is queued, only compatible consumers can
start new leases on that queue. Each claim receives its own lease identifier,
including a reclaim by the same replica after release or terminal retry.

Mixed mode retains the parser's 1,000-page cap on the original. Textract bills
for every submitted page, including pages whose text Quarry already has.

To roll back, return upload replicas to `image_only`, let in-flight uploads
finish and keep compatible Textract workers running until the mixed queue
drains. Confirm `SELECT count(*) FROM ingest_jobs WHERE pdf_required` returns
zero before starting older binaries. Retain migration 015 and its data.
Keep UUID retries for documents accepted in mixed mode on binaries that
support migration 015 to preserve original-byte identity matching.

## Vector Index Maintenance

For a Pinecone deployment, use `make reconcile` during a maintenance window to
inspect orphaned vectors and legacy document namespaces. Configure the same
PostgreSQL database and Pinecone index as the deployment. Apply the ingestion
job migrations and pause ingestion and other index writers until reconciliation
finishes.
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
