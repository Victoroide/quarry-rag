# Deployment

## Logging

The service emits JSON logs to stderr at the minimum severity set by
[`LOG_LEVEL`](configuration.md#environment-variables). Each access log includes `method`,
`path`, `status`, `bytes`, `duration_ms`, `remote_addr` and `request_id`. The path
excludes the query string; the remote address is the connecting peer, including
its port. `bytes` counts response body bytes written, and `duration_ms` measures
elapsed request handling time in milliseconds.

The `X-Request-ID` response header carries the correlation value. In `AUTHZ_MODE=grants`, access logs record route templates (for example `/documents/{doc_id}` or `unmatched`) instead of raw path UUIDs, and query and ingestion logs omit document IDs, S3 keys, signed URLs, prompts, and text excerpts. Structured logs
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

Every base image is pinned by multi-platform index digest in the `FROM` line that uses it, so scanners see each pin without resolving build arguments. The `base` and `build-base` stages repeat the same Go image reference, a unit test keeps them identical, and Dependabot updates the tag and digest on each line together.

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
tag=v0.2.0
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

PostgreSQL connections also add up across processes. Each process opens one pool of at most `pool_max_conns` connections, set in [`DATABASE_URL`](configuration.md#environment-variables); startup migrations use this pool. Some operations also hold a dedicated session outside the pool, so that a lock never waits for a pooled connection. Each admitted document deletion holds one until its request finishes, and a replica admits at most `pool_max_conns` deletions at a time. Each guarded ID-only deletion ([legacy deletion reservations](#legacy-deletion-reservations)) also holds a session of its own for its whole cleanup, and a replica runs at most `LEGACY_DELETION_CONCURRENCY` of them (`guarded deletions` below). `make cleanup`, an applied `make reconcile`, and a `make reindex` command while it holds its generation lock each add one session to their own pool, and `make cleanup` adds a second (`maintenance sessions` below). Administrative sessions count as well. The total must fit within `max_connections` less the reserved slots (`superuser_reserved_connections` and `reserved_connections`), and within any per-role or per-database connection limit:

`replicas × (2 × pool_max_conns + guarded deletions) + Σ (maintenance pool_max_conns + maintenance sessions) + administrative sessions ≤ max_connections − reserved slots`

For example, three replicas with `pool_max_conns=16` can use 108 connections when each runs the default four guarded deletions. One `make reindex` command with `pool_max_conns=4` and one administrative session raise the total to 114, more than PostgreSQL's default `max_connections` of 100 less its 3 reserved superuser slots allows. Lower `pool_max_conns` or raise `max_connections`.

With hosted manifests disabled, admission to a hosted or offline backend that
supports incarnation-tagged vectors holds a shared lock on the activation row
until the admission transaction ends.

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

#### PostgreSQL writer journal durability

Quarry commits deletion receipts, upload intents, worker claims, hosted
manifests, staging write reservations and exact cleanup batches before the
corresponding provider work. A crash can lose these records when PostgreSQL
acknowledges a commit before flushing WAL. Losing an acknowledged record can
make a retry unsafe, even when the provider completed the request.

Use the [DSN setting](configuration.md#schema-migrations) on every server,
worker and journal-writing maintenance process, including cleanup, reconciliation,
and reindex. Enable `fsync` and `full_page_writes` on the primary and every
eligible standby. Storage must honor flush requests. The setting covers future
commits, including single statements outside explicit transactions. Keep the
same connection settings and provider shutdown procedure across a mixed-version
rollout or supported executable rollback. Previously acknowledged commits keep
the guarantees of the configuration under which they completed.

- **Primary-only durability** accepts `on`, `remote_apply`, `remote_write` and
  `local`. Each waits for local WAL flush. A primary-only check leaves candidate
  durability unverified.
- **Synchronous standby durability** uses `on` to wait for remote WAL flush
  or `remote_apply` to wait for standby replay and query visibility. Use direct
  PostgreSQL connections and put `statement_timeout=0` in every writer DSN in
  this profile. Keep the application operation deadlines. PostgreSQL can end a
  synchronous replication wait on query cancellation after locally committing
  and return a warning alongside a successful command. A client deadline can
  also leave the commit outcome ambiguous. Avoid operator query cancellation
  during journal writes and treat interrupted waits as recovery events.

For the synchronous preflight, designate one directly connected physical
standby with a permanent replication slot and a unique `application_name`.
Configure `synchronous_standby_names` with that name alone, as
`FIRST 1 (name)` or as `1 (name)`. Use 1–63 ASCII letters, digits or underscores,
beginning with a letter or underscore. The reserved names `first` and `any`
are excluded. Match the exact application name supplied by the failover
controller. Multiple candidates, cascading replicas, wildcard configurations
and managed failover topologies require their own equivalent evidence.

Set `DATABASE_URL` to the exact writing role, database and connection settings
for each deployed writer. Run the applicable check with that login.

```sh
go run ./scripts/postgres-durability -mode local
go run ./scripts/postgres-durability -mode ha-session
```

For the topology check, set `DATABASE_URL` to a separate monitoring login on
the intended primary with effective `pg_read_all_stats` access. Supply the
actual promotion candidate. This checks the observed stream and the active
physical slot. Verify WAL settings and storage on the candidate independently.

```sh
go run ./scripts/postgres-durability -mode ha-topology -standby quarry_standby
```

The ten-second command deadline covers connection settings, startup and
read-only observations. The check uses at most one connection at a time and
prints one JSON object with `mode`, `status` and `reason`. The executable exits
0 when the selected check passes, 1 for a failed or unavailable observation,
and 2 for invalid invocation. Build the executable when automation needs its
exact exit status.
Repeat after topology or connection changes. A pass describes the sampled
session or stream at that moment. Promotion still requires confirming the
intended cluster, timeline and candidate through the failover controller. A
successful ping or a journal row visible on the old primary proves nothing
about its presence on the candidate.

Remote flush adds storage and network wait. Replay acknowledgment can also
wait for standby apply. Without the eligible standby, synchronous commits can
wait until it returns. Fence all senders before disabling synchronous
replication, replacing the eligible standby, canceling a wait, or changing
write routing to work around an outage.

1. For controlled failover, stop writers and capture the primary WAL flush
   position. Fence the former primary and verify that the named candidate
   replayed through that position on the same timeline before promotion.
2. After primary loss, fence it before permitting reconnection. Promote only
   the designated standby whose policy preserved acknowledged commits.
   Where available, verify that its WAL position covers the last acknowledged
   deployment journal watermark on the same lineage.
3. Before allowing provider writes, restore and verify the synchronous
   topology. When a canceled wait, failed drill, stale replica, restore or
   uncertain WAL history could have lost a commit, stop all dispatchers.
   Inventory the original provider targets and outstanding sends, verify
   provider and SQL outcomes, and reconcile the exact receipts before resuming
   writers or freeze and cutover. Follow the recovery commands in the relevant
   journal section. Preserve journal state until its outcome is known.

In staging, rehearse a provider-free mixed-version rollout and primary loss
before the new DSNs reach every process. Measure p50 and p95 for the same
provider-free journal write before and after enabling verified standby flush.
Record the topology, settings and measured latency with the release evidence.

### Pinecone Vector Index

- Set `PINECONE_API_KEY` and `PINECONE_INDEX_NAME` for the hosted index.
- **API Version**: The v5 Go SDK uses Pinecone API version `2025-10`. With `INDEX_VISIBILITY_MODE=lsn`, ingestion upserts and visibility probes use the `2026-07` HTTP data plane. Both pin the unique index host and use the SDK's `__default__` namespace for ingestion.
- **Startup Requirements**: The index dimension must equal [`EMBEDDING_DIMENSIONS`](configuration.md#environment-variables), the metric must be `cosine`, and the index must be ready. Startup fails if any requirement is unmet.
- **Request Limits**: Upserts use at most [`PINECONE_UPSERT_BATCH_SIZE`](configuration.md#environment-variables) records per batch. The 1.5 MiB batch limit uses an estimate for SDK requests and the serialized size for HTTP requests. `PINECONE_TIMEOUT` bounds each attempt. Each tracked worker mutation uses one RPC deadline for its durable claim and transport attempt. Explicit authentication and permission rejections are recorded as definite outcomes. Untracked writes and deletes use up to `PINECONE_MAX_ATTEMPTS` attempts, and queries use at most three. Retries use full-jitter exponential backoff from 200 ms to a cap of 5 seconds and stop on parent cancellation.
- **Deletion**: Before document cleanup, the adapter checks which ownership fields the default namespace indexes. If discovery fails, the next cleanup retries within `PINECONE_TIMEOUT`. On a selective index, it queries an indexed `doc_id` and every usable ownership predicate with bounded metadata pages, then deletes only verified physical IDs. Tagged cleanup can verify the exact prefix using listing. An unverified document predicate without exact prefix listing, remaining matches or an obscured full page leave cleanup pending for retry.
- **Purpose**: High-dimensional vector search. Chunks are upserted only into the default namespace (`""`), which the SDK maps to `__default__` and the console displays as `__default__`, with `doc_id` metadata identifying each document. Per-document namespaces created by earlier versions are removed when their document is deleted. SDK namespace operations share one connection, which closes only when the client shuts down.

#### Metadata Indexing

Keep Pinecone's all-metadata indexing default for new deployments. If you
select fields, make the following union filterable in `__default__` for the
retrieval and cleanup modes you use:

| Use | Filterable metadata |
| --- | --- |
| Service searches by document type or collection | `doc_type`, `collection` |
| Document allowlist and document-set searches, canonical ID and collection backfills, and document cleanup | `doc_id` |
| Grants searches by tenant and vector publication | `tenant_id`, `document_revision`, `revision_id`, `document_incarnation` |
| Manifest revision cleanup using metadata | `doc_id`, `revision_id` |
| Exact tagged revision and attempt cleanup | `doc_id`, `document_revision`, `vector_attempt` |

Keep `document_incarnation` filterable when grants readers can encounter hosted
manifests. The filter tests both the allowed stamp and its absence on historical
records. Turning off manifest writes can leave those readers active. Manifest
revision cleanup uses both fields in the table for delete-by-filter and its
query fallback. This applies to historical manifests and current manifested
writes, including deployments with grants retrieval or new manifest writes
disabled. Cleanup can delete verified physical IDs directly or list and delete
IDs under the exact revision prefix where listing is supported. Pinecone can
return stored metadata for evidence checks independently of which fields are
filterable.

On compatible serverless vector indexes, a namespace schema overrides the index
schema. If the namespace omits its schema, it inherits the index rules. An
explicitly indexed metadata field has `filterable: true`. For a serverless
target, inspect the provisioning API at version `2026-07`. The SDK still sends
retrieval queries at `2025-10`, and the optional LSN path has its own data-plane
version as described above.

With `PINECONE_INDEX_NAME` and `PINECONE_API_KEY` set for the target, run:

```sh
index=$(curl -fsS --proto '=https' --proto-default https \
  -H "Api-Key: ${PINECONE_API_KEY:?}" -H 'X-Pinecone-Api-Version: 2026-07' \
  "api.pinecone.io/indexes/${PINECONE_INDEX_NAME:?}") &&
host=$(printf '%s\n' "$index" | jq -er '.host') &&
printf '%s\n' "$index" | jq '.schema' &&
namespace=$(curl -fsS --proto '=https' --proto-default https \
  -H "Api-Key: ${PINECONE_API_KEY:?}" -H 'X-Pinecone-Api-Version: 2026-07' \
  "${host:?}/namespaces/__default__") &&
printf '%s\n' "$namespace" | jq '.schema'
```

For a legacy pod target, inspect `spec.pod.metadata_config.indexed` on the index
description. An omitted list uses the all-metadata default.

```sh
index=$(curl -fsS --proto '=https' --proto-default https \
  -H "Api-Key: ${PINECONE_API_KEY:?}" -H 'X-Pinecone-Api-Version: 2025-10' \
  "api.pinecone.io/indexes/${PINECONE_INDEX_NAME:?}") &&
printf '%s\n' "$index" | jq '.spec.pod.metadata_config.indexed'
```

If discovery is denied, unsupported, or ambiguous, resolve the target's
configuration with its owner and complete the staging check before promotion.
An empty query result alone cannot establish compatibility. If the effective
schema lacks a required field, provision a fresh index with the required fields
or the all-metadata default. Use the writer freeze, reindex, verification, and
cutover commands in [Changing Models or Vector Stores](#changing-models-or-vector-stores)
to replace the target while preserving publication and authorization checks.

For the staging check, source `PINECONE_API_KEY` from a staging credential and
set `QUARRY_PINECONE_SELECTIVE_INCLUDED_INDEX` and
`QUARRY_PINECONE_SELECTIVE_EXCLUDED_INDEX` to two separately provisioned,
disposable serverless dense cosine vector indexes. Provision both through the
same path with matching dimension, cloud, region, read capacity mode and
configuration, and embedding model and settings where applicable. Use the full
field list above for the included index and omit only
`document_incarnation` for the excluded index. Confirm the resulting schemas
in `__default__`, which Pinecone reserves as the default namespace. Selective
indexing must be available on a provisioning path that supports Quarry's
Vectors API.

The SDK index description and the provisioning fallback both use HTTPS on the
public controller at `api.pinecone.io`. The SDK's `PINECONE_CONTROLLER_HOST`
override does not apply to this comparison. The check requires the SDK's
`2025-10` index description to report an explicit, complete read capacity
configuration for each target. With Dedicated capacity, both targets must
report `Ready` and have matching node type, scaling strategy, and configured
shard and replica counts. Retry after both targets are ready.

```sh
GOMAXPROCS=4 go test -p 2 -count=2 -v ./internal/store/pinecone -run '^TestStagingSelectiveMetadataIndexing$'
```

Each check confirms both schemas before writing and uses fresh synthetic IDs.
Keep the provider status and sorted synthetic match IDs for the inclusion,
absence, and complete grants filters. The included index must return exactly
the expected controls. Transport, permission, throttling, and freshness
failures leave the comparison inconclusive. Each check attempts exact ID deletion
on every target it may have written to. Cleanup is confirmed after a fully
acknowledged and visible write, followed by an acknowledged delete and an empty
fetch. If cleanup is inconclusive, tear down the affected disposable indexes to
remove any residual records. Tear down the remaining disposable indexes after
both checks and manage the staging credential through your normal operator
process. The effect of omitting
`document_incarnation` is unverified on serverless and unmeasured on legacy pods.

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

#### Hosted Vector Manifest Rollout

See [writer journal durability](#postgresql-writer-journal-durability) before
enabling synchronous failover for hosted manifests.

Apply migration 025 and the `hosted_vector_revision_visible` grant before
starting upgraded processes, whether or not `HOSTED_VECTOR_MANIFEST` is enabled:
every query asks that predicate whether a hit belongs to the document's active
revision. A serving login that leaves the flag off needs no privilege on the
three manifest tables, because document deletion and job failure retire
revisions in owner-defined triggers. Only replicas that set
`HOSTED_VECTOR_MANIFEST=true` need the
[manifest table grants](grant-catalog.md#database-roles)
and run the sweeper.

When a document has an active hosted revision, readers require hits to match
that revision, including for documents configured to use scoped vectors.
Manifest hits carry the hosted revision ID alone for publication identity.
Evidence reads reject hits that also supply tagged revision or attempt fields.
With explicit grants membership, Pinecone can exclude stamped manifest vectors
from obsolete incarnations before ranking. See [Metadata Indexing](#metadata-indexing)
for the namespace requirements. It uses up to 10,000 incarnation values per
existing request partition. Revisions from the same incarnation and
historical vectors without a stamp can still occupy candidate slots until SQL
checks the active revision. Service queries without explicit grants membership
apply the incarnation and revision checks in SQL.
For documents without an active hosted revision, evidence reads continue to
validate unrevisioned hits against the document's persisted vector format.
Upgrade every replica to run the revision-aware readers before enabling
`HOSTED_VECTOR_MANIFEST=true`.
Enable the flag on one replica first and confirm that
`quarry_hosted_vector_cleanups_total` reports completions.

Revision-aware readers support both stamped and historical manifest vectors during
a rolling upgrade or rollback. Historical unstamped hits can pass only if their
active publication did not require a stamp. Rolling back to an older revision-aware
reader retains SQL validation and gives up the pre-ranking manifest incarnation
filter. Existing vectors need no rewrite. To stamp historical vectors, use the
existing vector reindex workflow or publish a new ingestion
revision. The incarnation field counts toward Pinecone's 40 KiB metadata limit
per vector.

To verify Pinecone's pre-ranking filter on a disposable vector index, set
`QUARRY_PINECONE_STAGING_INDEX` and `PINECONE_API_KEY` and run
`go test ./internal/store/pinecone -run '^TestStagingManifestIncarnationFilter$'`.
The test writes and deletes two synthetic records and checks a full partition
with top-K set to one.

On both serverless and pod-based Pinecone indexes, a manifested retry of an
unscoped ingestion can publish after an unacknowledged legacy upsert or an
abandoned upsert claim expires. The revisions use separate vector IDs, and
readers exclude legacy hits after activation. A live mutation claim or an
uncertain legacy delete still blocks publication. An acknowledged legacy cleanup
leaves earlier uncertainty intact for deletion safeguards and reconciliation.
Legacy workers still apply their original publication guard. Finish upgrading
the workers before relying on this recovery. This recovery requires no schema
change, and rollback preserves the manifest reader checks.

To roll back, set `HOSTED_VECTOR_MANIFEST=false` and keep at least one replica
that has the flag enabled and its grants until `scripts/reconcile` reports no
`retired_revisions`. Retired revisions wait in the database, and only a
replica with the flag enabled sweeps them. Without one, run `make reconcile
ARGS="-apply"` against the same configuration to sweep them from the command
line. Follow the hosted manifest drain procedure below before switching to
PostgreSQL.

An attempt that was interrupted before the rollback and then completed without
manifests leaves its vectors in the index. Readers never return them, and they
are removed when the document is deleted or fails.

#### SDK Deployment Verification

Verify an SDK upgrade on staging before promoting it. The [staging procedure](#smoke-testing-a-staging-deployment) lists the prerequisites, including the preexisting document to record before upgrading, and the exact invocation. The Pinecone service determines whether the legacy empty namespace (`""`) and `__default__` refer to the same data; the SDK mapping alone does not establish their equivalence.

Confirm that `TestSmoke_PreexistingDocumentRemainsRetrievable` passes without skipping: the direct index check must find the preexisting document's vectors, and a service query restricted to that document must return at least one chunk from it. If the direct check succeeds but the service reports `no_evidence`, check `MIN_RELEVANCE_SCORE` and the query before retrying. This check reads the document without reingesting or deleting it. The other smoke checks create and delete their own documents. Promote only after the staging suite passes. To roll back, revert the SDK upgrade inside a deletion-aware build and redeploy, as [rollback after activation](#rollback-after-activation) requires; stored documents and vectors are unaffected.

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

The [writer journal durability policy](#postgresql-writer-journal-durability)
covers staging writes and exact active cleanup batches.

A chat model change can reuse the existing vectors when the embedding model and
its output dimensions stay the same. Changing the embedding model, its revision
or its dimensions, or changing the vector store, moves the corpus to a new
vector generation: a target staged beside the live one that keeps every
document and chunk identity, so saved allowlists, citations and fingerprints stay
valid. A configuration change alone leaves completed documents in their current
state and never copies them. Use the same vector store and embedding model
across replicas sharing an ingestion queue. [`make reindex`](development.md#vector-reindex-scriptsreindex)
is the command; this section gives the order.

Before starting, run a release that contains migration 016 on every replica and
worker. Earlier releases ignore the writer freeze and the active-generation pin,
so finish that rollout first. Start a replica with the actual live configuration
to bind the physical legacy target before invoking reindex. Provision the target:
for Pinecone, a ready cosine index of the new
dimension; for PostgreSQL, let a maintenance login with schema creation and
table reference privileges create `chunk_vectors_<generation>`, or pre-create
the table and its indexes as the metadata owner. See
[vector reindex database privileges](#vector-reindex-database-privileges) for
the login and privilege requirements.

Before applying migration 041, stop pre-upgrade staging executables and all
processes that can still dispatch mutations to the staging target. Apply the
migration before invoking the updated maintenance commands. On rollback, keep
the migration and use the updated maintenance executable or leave staging and
recovery disabled.

Before confirming an unacknowledged staging write, stop all dispatch-capable
senders for the bound target. Obtain provider-side evidence that the exact
outstanding mutation can no longer change that target. A database disconnect,
context error or empty read is insufficient settlement evidence. Migration 041
assigns an identity to each legacy pending row without establishing settlement.
Keep the journal held when evidence is unavailable.

Before cutting a tagged corpus over to PostgreSQL, upgrade every serving,
worker and cleanup binary to a release that supports tagged documents on
pgvector. Finish catch-up and verification under the freeze before routing
traffic to the new generation. Apply the pgvector deletion grant to restricted
serving and cleanup logins as part of the [serving role setup](grant-catalog.md#database-roles).

1. Write the profile of the target: provider, model, exact revision,
   dimensions, `cosine` and the extraction and chunk version of the stored
   chunks. Use the extraction and chunk settings that produced the stored text.
   Run the inventory and resolve every blocker. The advisory
   `documents.deleting_or_held` count includes each existing document once if
   its status is `deleting` or its document UUID has a held or unfinalized
   deletion receipt. A counted document can also appear in a status total.
   `open_deletions` counts all unfinalized receipts, including those whose
   document is absent. The `open_deletion_ids` cap below limits only the list.
   Inventory reads the document and receipt counts in separate read-committed
   statements, so concurrent cleanup can commit between the two reads.
2. If you know what produced the legacy vectors, configure every legacy replica
   with that profile and record it with `-attest-source -apply`. Attestation
   waits for admitted transactions and rejects while running job attempts
   remain. After attestation, only replicas pinned to that profile can admit or
   claim work. If the historical profile is unknown, re-embed every stored
   chunk and prepare independent query fixtures using the current serving
   query encoder and the target encoder. Configure the replacement embedding
   deployment for staging and supply `-max-embed-calls` whenever staging
   needs to recompute vectors.
3. Stage with `-apply` while the service keeps serving. Repeat until a run
   reports `complete: true`. Resume a bounded run with the cursor it reports.
4. Freeze writers for the target generation and wait for the settling time.
   Reads continue and [writes pause](api.md#writer-pause). Freeze waits for
   unfinalized deletion receipts to clear from `document_deletions`, and activation
   refuses to switch while any remain. Both report up to 20 document IDs of open receipts
   (`open_deletion_ids` in reports; inventory also reports `open_deletions` and
   `open_deletion_ids` without blocking live staging). Freezing and activation
   also refuse a target in a different vector store than an active
   [evaluation fixture](development.md#evidence-evaluation), because only workers
   attached to the fixture's store can lease its job or delete it by receipt.
   Delete each such fixture by its original receipt first, or thaw, delete and
   freeze again. A receipt with an unknown binding blocks as well; resolve it
   as that section describes.

   If open deletion receipts block a freeze or activation, clear them without
   bypassing the freeze:

   1. Thaw writers: `make reindex ARGS="-thaw -apply"`.
   2. Run `make cleanup IDS="--list-held"` and find each reported
      `document_id`. Use the recorded `incarnation` and confirm it matches the
      incarnation saved before deletion, if available. Use `document_present`
      to decide how to finish the receipt. The status endpoint returns `404`
      both for a hidden `deleting` row and for an absent row.

      - If `document_present=true`, repeat the deletion with the saved
        incarnation via `X-Document-Incarnation`,
        `Client.DeleteDocumentIncarnation`, or
        `make cleanup IDS="<document-uuid>@<incarnation-uuid>"`.
      - If `document_present=false` and the receipt is an unfinished
        `legacy_reservation`, a qualified retry returns `404` when checkpointed
        deletion is active. Follow [Quarantine and Hold Release](#quarantine-and-hold-release)
        to pause affected mutators, including pre-upgrade processes, and verify
        settlement in the original backend. Confirm that no claim is active and
        that the configured backend and source match the receipt before running
        `make cleanup IDS="--release-held <document-uuid> <incarnation-uuid>"`
        with the permissions and credentials for that backend and source. The
        existing receipt already reserves the UUID. Use `--quarantine` only
        for an absent UUID that requires quarantine, as described in that
        procedure.
      - If the identity differs, the receipt is missing, or the absent row
        has an unsupported open receipt, stop and investigate before taking
        any recovery action.

      A finalized quarantine hold can still prevent UUID reuse while allowing
      reindex to proceed.
   3. After finishing the reported open receipts, freeze writers again with
      `make reindex ARGS="-freeze -apply -generation <name>"`. If the report
      lists more open receipts, repeat recovery before resuming catch-up.
5. Start staging again from the beginning and finish catch-up. Verify with declared
   thresholds and `-apply` to record the result. For different embedding spaces
   or an unattested legacy target, [prepare matched query fixtures](development.md#vector-reindex-scriptsreindex).
   Preview with
   `make reindex ARGS="-activate -generation <name>"`. A report with `ready: true`
   and `applied: false` indicates that the advisory checks passed. Rerun with
   `-apply` to switch. The database checks again under lock. A confirmed switch
   reports `ready: true` and `applied: true` and leaves the freeze in force.

   If activation reports `ready: false`, resolve the reported reasons before
   retrying. Freeze for the requested generation if it lacks a matching freeze.
   If a different generation is frozen, thaw before freezing for the target.
   If the freeze is still settling, wait until the selected `-quiesce` interval
   has elapsed since freezing. Let live ingestion leases finish or expire and
   resolve open deletion receipts as described above. If verification is missing
   or the corpus changed, finish catch-up and verify again. If the requested
   generation is already active, continue with deployment in step 6. If
   activation could not be confirmed, check the active generation before retrying.
   If writing the JSON report fails during a hosted-manifest refusal, the
   command reports both errors.
6. Deploy every replica sharing the queue with `VECTOR_GENERATION` set to the new
   generation, the matching embedding deployment and extraction settings, and
   `PINECONE_INDEX_NAME` for a Pinecone target. Apply any [serving grants](grant-catalog.md#database-roles)
   for the promoted table before starting those replicas. A replica on any other
   generation, or with settings that differ from the recorded ones, cannot start
   or cannot write.
7. Resume writers with `-thaw -apply`.

Keep the old target for rollback. Changes made after writes resumed exist only
in the new target, so catch the old target up before switching back: freeze for
the old generation, stage into it, verify, activate it and redeploy the old
settings. Frozen catch-up also compares every retired target key with PostgreSQL
to remove stale records written during its earlier active period.
For a tagged corpus, keep the upgraded binaries and current metadata throughout
rollback so that readers can check the current published attempt and cleanup
can resume recorded receipts.
For an unattested legacy source, keep the original target intact and prepare a
separate rollback generation with a known profile if you need to return to the
previous serving encoder.

#### Drain hosted manifests before PostgreSQL

Apply migration 047 before inventory or activation of a PostgreSQL generation,
including a rollback to PostgreSQL. Inventory shows a
`hosted_manifest_cutover` object with writing, active and retired manifest
counts and a separate cleanup queue count that can overlap the retired count.
Activation requires all four counts to be zero. Inventory counts are advisory
for staging. Completed documents with active hosted revisions still need to
meet the PostgreSQL corpus compatibility checks before staging.

Keep the original Pinecone index, host, namespace and credentials available
until the drain is verified. Resume or settle writing attempts on that source.
For an abandoned document, use the authorized, incarnation-qualified deletion
path against the original source. Drain retired work there with the existing
manifest sweeper or applied reconciliation. Let cleanup leases, drain windows
and retries finish. Confirm that old writers can no longer send and that their
submitted provider requests have settled. If writers are frozen, thaw first
and then freeze, catch up and verify again after cleanup.

Keep the switch blocked during outages and while provider settlement or cleanup
is unresolved. If previous hosted switches left work with uncertain index
ownership, establish the original target before applying a sweep. Repair
orphaned work on the proven original target without deleting journal rows
directly. An installation that already switched and stranded work before the
upgrade requires incident recovery on that target. Zero counts cover the
current journal; work whose records were lost or previously declared converged
requires separate verification.

#### Vector reindex database privileges

Run `make reindex` as the metadata schema owner, or provision a dedicated
maintenance login (`quarry_reindex` in the examples) with `CONNECT` on the
metadata database and the metadata schema first in `search_path`. Substitute
your metadata schema for `public` and your maintenance login name for
`quarry_reindex`.

Every `make reindex` mode (inventory, staging, freeze, verification, activation,
thaw and source attestation) requires these grants:

```sql
GRANT USAGE ON SCHEMA public TO quarry_reindex;
GRANT SELECT ON public.documents, public.chunks, public.document_deletions,
  public.ingest_jobs, public.evaluation_fixtures, public.document_pdf_extractions,
  public.hosted_vector_revisions TO quarry_reindex;
GRANT SELECT, INSERT, UPDATE ON public.vector_generations TO quarry_reindex;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.vector_generation_documents TO quarry_reindex;
GRANT SELECT, UPDATE ON public.vector_state TO quarry_reindex;
GRANT EXECUTE ON FUNCTION public.grant_catalog_document_tenant(uuid, uuid) TO quarry_reindex;
GRANT EXECUTE ON FUNCTION public.hosted_manifest_cutover_counts() TO quarry_reindex;
```

PostgreSQL checks `EXECUTE` privilege on `grant_catalog_document_tenant(uuid, uuid)`
when initializing every revision query used by inventory, staging, verification
and activation, even when no document is assigned in the grant catalog or
`documents` is empty.

When `VECTOR_STORE=postgres` is the source or target, also grant vector table
access:

- **Legacy source or rollback target (`chunk_vectors`):** Grant `SELECT` to read
  source vectors or verify against the legacy PostgreSQL store, or
  `SELECT, INSERT, UPDATE, DELETE` when staging a rollback into `legacy` (omit
  `INSERT, UPDATE, DELETE` if never rolling back to `legacy`). Before rollback,
  the table owner must prepare its base indexes and, when exact search is
  enabled, the [exact-scope indexes](#postgresql-vectors), because staging
  validates them and creates any missing indexes:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON public.chunk_vectors TO quarry_reindex;
```

- **Generation tables (`chunk_vectors_<generation>`):** Grant `SELECT` on a
  promoted generation when it is the source of a later reindex; retain the
  maintenance login's access after cutover or have the table owner grant it
  again. For a staging or rollback target, grant `SELECT, INSERT, UPDATE, DELETE`.
  Ensure pgvector
  0.8 or later is already installed by a database administrator or schema owner
  if the maintenance login lacks extension creation privileges. Choose one of
  two provisioning patterns:
  - **Pre-created by the metadata owner (recommended when non-owner roles lack
    schema `CREATE` per [grant catalog database roles](grant-catalog.md#database-roles)):**
    Have the metadata owner create `chunk_vectors_<generation>` with the
    [vector schema](architecture.md#postgresql-vector-schema) columns and its
    HNSW (`vector_cosine_ops`), `document_id` and `collection` indexes (plus
    the two exact-scope indexes from [PostgreSQL vectors](#postgresql-vectors)
    when `PGVECTOR_EXACT_SCOPE_MAX` is positive), then grant table access to
    the maintenance login (and to `quarry_service` before cutover). When the
    table and matching indexes already exist, `make reindex` verifies and skips
    creation, so the maintenance login needs neither `CREATE` on the schema nor
    `REFERENCES` on `documents`:

```sql
CREATE TABLE public.chunk_vectors_<generation> (
  chunk_id TEXT PRIMARY KEY,
  document_id UUID NOT NULL REFERENCES public.documents(id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL,
  doc_type TEXT NOT NULL,
  collection TEXT NOT NULL DEFAULT '',
  text TEXT NOT NULL,
  kind TEXT,
  quality TEXT,
  token_count INTEGER NOT NULL,
  embedding vector(<dimensions>) NOT NULL,
  pages INTEGER[] CONSTRAINT quarry_layout_pages_check
    CHECK (pages IS NULL OR (cardinality(pages) BETWEEN 1 AND 1000 AND array_ndims(pages) = 1 AND array_lower(pages, 1) = 1)),
  table_ids TEXT[] CONSTRAINT quarry_layout_table_ids_check
    CHECK (table_ids IS NULL OR (cardinality(table_ids) BETWEEN 1 AND 250000 AND array_ndims(table_ids) = 1 AND array_lower(table_ids, 1) = 1)),
  row_ids TEXT[] CONSTRAINT quarry_layout_row_ids_check
    CHECK (row_ids IS NULL OR (cardinality(row_ids) BETWEEN 1 AND 250000 AND array_ndims(row_ids) = 1 AND array_lower(row_ids, 1) = 1))
);
CREATE INDEX idx_chunk_vectors_<generation>_embedding ON public.chunk_vectors_<generation> USING hnsw (embedding vector_cosine_ops);
CREATE INDEX idx_chunk_vectors_<generation>_document_id ON public.chunk_vectors_<generation> (document_id);
CREATE INDEX idx_chunk_vectors_<generation>_collection ON public.chunk_vectors_<generation> (collection);
CREATE INDEX idx_chunk_vectors_<generation>_collection_doc_type ON public.chunk_vectors_<generation> (collection, doc_type, document_id);
CREATE INDEX idx_chunk_vectors_<generation>_doc_type_document_id ON public.chunk_vectors_<generation> (doc_type, document_id);
```

Use a standalone table and retain the three named layout checks exactly as
shown, validated and enforced. Opening a vector generation checks this schema
using PostgreSQL catalog metadata. Initialization checks it before preparing
indexes.

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON public.chunk_vectors_<generation> TO quarry_reindex;
```

  - **Created by the maintenance login itself:** When the maintenance login
    creates `chunk_vectors_<generation>` and its indexes directly during
    staging, also grant schema creation and foreign-key reference privileges:

```sql
GRANT CREATE ON SCHEMA public TO quarry_reindex;
GRANT REFERENCES ON public.documents TO quarry_reindex;
```

The maintenance login owns a self-created table and its indexes. Before cutover,
it must build any exact-scope indexes and grant serving access:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON public.chunk_vectors_<generation> TO quarry_service;
```

This pattern grants schema `CREATE` to a non-owner role, which the grant catalog
role provisioning forbids. Deployments following that provisioning must use the
pre-created pattern.

#### Superseded vector cleanup

Apply migration 042 before using `make reconcile ARGS="-superseded-unrevisioned -apply"`.
Use the metadata schema owner or the login with the
[vector reindex database privileges](#vector-reindex-database-privileges).
The mode reads the documents, jobs, deletion receipts, active revision
pointers and manifests. For a restricted maintenance login, add these grants
to the reindex grants. Serving roles require no additional privileges.

```sql
GRANT SELECT ON public.hosted_vector_manifests TO quarry_reindex;
GRANT EXECUTE ON FUNCTION public.vector_writer_state(), public.document_deletion_reserved(uuid) TO quarry_reindex;
```

1. Stop legacy and external writers that bypass the database writer gate,
   including any direct upsert or delete jobs attached to the index.
2. Run `make reindex ARGS="-freeze -apply -generation <active-generation>"`
   and wait for the freeze to settle for at least 60 seconds, with no running
   job leases or open deletion receipts. Use the current active generation,
   such as `legacy`, for this freeze.
3. Preview with `make reconcile ARGS="-superseded-unrevisioned"`, then use
   `make reconcile ARGS="-superseded-unrevisioned -apply"` to delete exact IDs.
   The cleanup holds the generation lock and leaves the freeze as it found it.
   `-page` bounds both the number of IDs listed and the number selected per
   page. A response that exceeds the receive limit triggers smaller exact-ID
   fetches, which can add billed requests to that page. A failed listing,
   fetch, or ownership lookup stops the scan and preserves the input cursor
   in `next_page_token`. Fetching stops on an oversized single record, a
   quota error, or any other provider error. A smaller page can reduce
   multi-record response sizes and the amount of work per page.
4. If deletion or its checkpoint fails, keep writers frozen and disable the
   old maintenance dispatch and restart paths. The next apply reports any
   pending batch with its generation, reservation UUID, sender UUID and exact
   IDs. Correlate the sender UUID in the invocation logs with its execution
   supervisor. Verify that the process exited or that network egress or
   provider credentials permanently prevent it from sending to the target.
   Credential revocation is sufficient only if it covers every route the
   sender can use, including shared credentials.
   A stop signal, disconnected database session, paused container, expired
   deadline, absence of in-flight requests or PID alone cannot establish exit.
   For a sender UUID assigned by migration or missing invocation logs, stop
   and verify every process that could dispatch cleanup against the bound
   target, including older maintenance executables.
5. After establishing shutdown, confirm with Pinecone that every request
   already submitted for the recorded IDs on the pinned index and default
   namespace has settled. A timeout or a fetch that omits the IDs cannot
   establish settlement. If either confirmation is unavailable, retain the
   freeze and reservation. With both confirmations, run
   `make reconcile ARGS="-superseded-unrevisioned -apply -confirm-sender-stopped=<sender-uuid> -confirm-target-drained=<reservation-uuid>"`.
   This clears only the matching batch and starts from the beginning to
   re-evaluate candidates. Omit `-page-token`. A stale pair stops the command
   before it scans. Every reservation has a new UUID, and each invocation
   has its own sender UUID. Confirm any later batch separately, even when its
   exact IDs are identical.
6. After the final pass and provider convergence, run
   `make reindex ARGS="-thaw -apply"` and resume external writers. Thaw and
   activation are blocked while a batch is pending. During binary rollback,
   retain migration 042 and keep an updated reconciliation executable
   available for recovery.

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

### Source Upload Journal

Verify [writer journal durability](#postgresql-writer-journal-durability)
for deployments that require upload intents to survive primary loss.

By default an original is written to S3 before PostgreSQL accepts its document.
A process that stops between the two steps, or a compensating delete that
fails, leaves an object that no document references. The service cannot tell it
from the original of a job that is merely delayed, so it never deletes objects
by age or prefix.

`SOURCE_UPLOAD_JOURNAL=true` makes the owner durable first. Each multipart
upload stages a PostgreSQL row naming the bucket, the generated object key, the
collection and an upload ID before the object is written. Staging and all PUT
attempts share a five-minute deadline, and the row has a 15-minute lease.
Acceptance adopts the row in the transaction that inserts the document, so a
document and its journal row
commit together; if reclamation has claimed the row first, acceptance fails
with `503` and the caller retries the upload. The version ID that S3 returns is
recorded. Unadopted journaled uploads stay available for reclamation after an
acceptance failure, including when the commit outcome is unknown.
Reclamation only considers objects recorded in the journal.

- **Rollout.** Deploy a release with migration 014 to every replica with the
  journal off. Apply the database grants below after the migration and before
  enabling the journal on every replica. A replica that predates the setting
  writes unjournaled originals, which reclamation never selects. Before rolling
  back to a release without the journal, run reclamation until its inventory
  lists no `staged` rows and no `claimed` rows other than those that are
  protected, referenced by a document or pending with `unacknowledged_write`.
  Older releases ignore every `source_uploads` row, and a later journaled
  release resumes their rechecks, as described in the
  [reclamation reference](development.md#source-upload-reclamation-scriptsreconcile).
  Upgrade the reconciliation command before enabling journaled owned uploads.
  Keep that version of the command available during rollback so it can continue
  reclaiming incarnation-scoped keys.
- **Bucket versioning.** Deleting a key in a versioned bucket only adds a delete
  marker and keeps every version, so a delete is not erasure. Reclamation lists
  the exact recorded key and deletes each version and marker by its version ID.
  A lifecycle rule for noncurrent versions can also remove them but does not
  replace reclamation.
- **Object Lock and legal holds.** A version under retention or a legal hold is
  never deleted, and no other version of its key is deleted either. It stays
  `claimed` with its reason in the inventory until the protection ends.
- **IAM.** The role that runs reclamation needs
  `s3:ListBucketVersions` on `arn:aws:s3:::{bucket}`, plus `s3:GetObjectVersion`,
  `s3:GetObjectRetention`, `s3:GetObjectLegalHold` and `s3:DeleteObjectVersion`
  on `arn:aws:s3:::{bucket}/documents/*`. Do not grant bucket-wide deletion or
  `s3:BypassGovernanceRetention`.

#### Source upload journal database grants

For a new installation or an upgrade, use the metadata owner to grant the
serving login access to the journal after migration 014. Substitute your
metadata schema for `public` and your login names for the examples.

```sql
GRANT SELECT, INSERT, UPDATE ON public.source_uploads TO quarry_service;
```

Provision a separate maintenance login for reclamation. Both the serving and
maintenance logins need `CONNECT` on the metadata database and the metadata
schema first in their PostgreSQL search path. For reconciliation, set
`DATABASE_URL` to the maintenance login. Grant these privileges for inventory.

```sql
GRANT USAGE ON SCHEMA public TO quarry_source_maintenance;
GRANT SELECT ON public.source_uploads TO quarry_source_maintenance;
GRANT SELECT (s3_key) ON public.documents TO quarry_source_maintenance;
GRANT SELECT (source_key) ON public.document_deletions TO quarry_source_maintenance;
```

Add the following privileges if the maintenance login will also run `-apply`.

```sql
GRANT UPDATE, DELETE ON public.source_uploads TO quarry_source_maintenance;
```

Reclamation runs while ingestion continues. Inspect the inventory before
applying:

```sh
make reconcile ARGS="-source-uploads"
make reconcile ARGS="-source-uploads -apply"
```

The [reclamation reference](development.md#source-upload-reclamation-scriptsreconcile)
describes the bounds, the report and the reasons an upload stays pending.

---

## Optional PDF OCR

Enable the [OCR configuration](configuration.md#environment-variables) to accept
[image-only PDFs](api.md#image-only-pdf-ocr). The S3 bucket and Textract endpoint
must be in the same AWS Region. The service uses its shared AWS credential chain
and retry policy. Grant `textract:StartDocumentTextDetection` and
`textract:GetDocumentTextDetection` on `*` for `image_only` and `mixed` OCR, and
`textract:StartDocumentAnalysis` and `textract:GetDocumentAnalysis` on `*` when
enabling [`PDF_LAYOUT_MODE=textract`](configuration.md#environment-variables);
Textract reads the original with the caller's existing `s3:GetObject` permission. SSE-KMS objects also need
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

### PDF Layout Analysis Rollout

Apply migration `040_layout_pdf_claim_capability.sql` before any upload replica
sets `PDF_LAYOUT_MODE=textract`, and ensure the serving login holds the
`document_pdf_extractions` `SELECT` grant from
[role provisioning](grant-catalog.md#database-roles). Before applying migration
`040`, pause layout admissions (`PDF_LAYOUT_MODE=plain`) and drain or release
any running unexpired layout leases:

```sql
SELECT count(*) FROM ingest_jobs j
JOIN document_pdf_extractions p ON p.document_id = j.document_id
WHERE j.state = 'running' AND j.lease_until > clock_timestamp()
  AND p.profile = 'quarry.pdf.layout.v1';
```

The migration takes the `ingest_jobs` table lock required for `ALTER TABLE` and
aborts without modifying the schema or invalidating live attempts if any
unexpired layout lease is still running; pending, failed, and expired rows do
not block it.

Worker claim filtering uses the OCR interfaces configured on each worker.
`PDF_LAYOUT_MODE` controls new admissions. A worker with text detection
support can skip queued layout jobs and continue claiming compatible mixed
or ordinary jobs. Workers with neither text detection nor layout analysis
remain subject to the queue-wide PDF guard. Pending layout work waits for a
worker with layout analysis support. Processing that work requires
`textract:StartDocumentAnalysis` and `textract:GetDocumentAnalysis` permissions.
Layout admission fails closed while migration `040` is absent.

In a mixed-version deployment after migration `040`, an older binary running the
legacy pre-`040` claim query cannot lease layout jobs, including retries with
saved checkpoints. Its update consumes no attempt or token, so it receives no
job whenever a layout job sits at the head of its legacy query. Keep at least
one upgraded layout-capable worker running so queued layout jobs make progress.
Layout mode sends eligible new PDFs (including born-digital pages) to Textract
`StartDocumentAnalysis` (`LAYOUT` and `TABLES`) and applies Textract's document
analysis pricing per page. Offline execution (`GO_RAG_FAKE_MODE=true`) invokes
neither S3 nor Textract.

To roll back without down-migrating, set `PDF_LAYOUT_MODE=plain` on all upload
replicas, retain migration `040` (`layout_claim_attempt` and its trigger), and
keep at least one layout-capable worker running until no queued layout jobs
remain (confirming `SELECT count(*) FROM ingest_jobs j JOIN
document_pdf_extractions p ON p.document_id = j.document_id WHERE
p.profile = 'quarry.pdf.layout.v1'` returns zero) before rolling back worker
binaries.

For PostgreSQL vector storage, use a coordinated writer cutover for migration
039. Pause ingestion, vector maintenance, and reindex and drain in-flight jobs
and database sessions. Stop older vector writers and generation creators,
including their scheduled invocations, before the schema owner migrates.
Disable their database logins and terminate any remaining sessions before
resuming with upgraded writers. If a login was shared, issue fresh credentials
to the upgraded processes and retire the old credentials. Keep schema
ownership and creation privileges with the upgraded owner-side deployment path.
This prevents an older upsert from retaining arrays from the previous contents
of a vector row and prevents older generation DDL from creating tables without
the provenance columns.

Existing rows acquire nullable columns without a backfill. Older readers and
explicit-column append-only inserts remain compatible. Use upgraded writers to
replace existing keys, including during an application rollback. Keep the
vector write and generation-creation paths paused if the rollback also requires
older writer binaries. To fill in provenance, stage a fresh target generation
or re-ingest the source documents. Completed reindex checkpoints can skip
unchanged documents.

If an older creator produced a table after migration 039 was recorded, or a
custom table has missing or incompatible layout checks, pause generation
creation and table changes for a maintenance window. Retire any old writer
credentials as above. As table owner, correct malformed arrays, and restore
the named checks from the generation DDL. Move inherited or partitioned
targets to standalone tables. To reapply the migration's repair under the
migration lock, connect as schema owner from the release tree and run the
following in one session. Set the search path to the affected metadata
schema followed by the extension schema if they differ. This preserves
compatible data and leaves migration history intact.

```sql
\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(8175547842735702017);
\i internal/store/postgres/migrations/039_vector_layout_provenance.sql
COMMIT;
```

If validation fails, roll back the transaction and repair the reported
definition or invalid data before retrying. Open the repaired generation with
the upgraded binary before resuming writes. The catalog check runs when the
generation opens. Apply later owner DDL in a maintenance window and reopen
before resuming service. Reindex from a fresh checkpoint
or re-ingest affected documents if an older upsert may already have retained
stale provenance.

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

## Activating Checkpointed Deletion

See [writer journal durability](#postgresql-writer-journal-durability) for
the deployment settings that protect checkpointed deletion receipts.

A new database starts with checkpointed deletion inactive. In service mode, bare
`DELETE` requests, `make cleanup <uuid>` and the MCP tool address the live document
by UUID until activation. Activating [checkpointed deletion](api.md#checkpointed-deletion) is a separate, one-way, database-wide operation that an operator performs once every mutating executable can honor it and outstanding provider work is settled or quarantined.

### Upgrade order

1. **Apply the additive schema and prepare callers.** Apply pending migrations as the schema owner and apply the [serving-role upgrade grants](grant-catalog.md#upgrade-an-existing-serving-role) before starting upgraded processes. For hosted and offline vectors, include the migration 030 grant in every authorization mode. Prepare clients to opt in to each document's incarnation with `include_incarnation=true`. Service-mode callers use bare deletion until step 4.
2. **Pause and retire pre-upgrade processes.** Pause ingestion, deletion and index maintenance at every entry point. Stop and drain every API, worker, `make cleanup` or `make reconcile` invocation, scheduled job and client that could reissue an operation through an endpoint that predates the marker. Such a process ignores the marker and every receipt, and it can delete by UUID after activation. Reads can stay available where that is safe. Deploy this release with the marker still unset. Keep the old writers away during preparation too, because an old process ignores the SQL hold.
3. **Account for outstanding provider work.** For every hosted upsert and every hosted delete that a pre-upgrade process issued, including retries by processes that have exited, collect the document UUID, the original index and the namespace. On a Pinecone serverless namespace where a response recorded an `x-pinecone-request-lsn`, query the same namespace until its `x-pinecone-max-indexed-lsn` reaches that value. For a lost reply with no sequence number, obtain independent confirmation from the provider or keep the UUID fenced. An empty read, an elapsed drain interval, a later request's success and a released database lock cannot settle an earlier attempt.
   - While writers are paused, [quarantine](#quarantine-and-hold-release) every known absent UUID with unresolved effects, even when its process left no receipt. Keep writes and deletions paused for live UUIDs with unresolved effects.
   - Postpone activation, and any reuse of the affected UUIDs, if the affected UUIDs cannot be enumerated, the pause cannot be enforced, a live pre-upgrade executable remains, or the target index has neither a usable scoped metadata filter nor supported prefix listing.
   - Verify that the object bucket and the vector index are the ones the receipts name, and keep both available through recovery. Inventory the operations that this release's own bare deletions issue before activation as well: they are reserved, and a held reservation needs the same settlement.
4. **Activate, then switch callers.** Run the [activation command](#activate-checkpointed-deletion). Check the mode through every replica, including one that was running before activation and one started afterwards. Switch callers to the incarnation they saved earlier, then resume the writes you paused. Identities that stay held remain fenced until a verified release.
5. **Keep the boundary.** Follow [rollback after activation](#rollback-after-activation). Old executables stay retired and the marker is never cleared.

### Activate checkpointed deletion

```sh
make cleanup IDS="--activate-deletion"
```

The command is an exclusive action: it accepts no document, no other flag and no value. It reads the service environment, so it runs against the database of the deployment it activates. It must run as the schema owner. The restricted serving login has no privilege on the marker table and fails with a permission error, so keep it that way.

It takes the activation lock exclusively. That lock is shared by every ID-only cleanup, so activation waits for each healthy cleanup to return from its last external call, and cleanups that start meanwhile run after it as activated requests. The command is bounded by two minutes. If a cleanup outlasts that, the command exits with an error and the marker stays unset, so pause deletion before activating. Under the lock the command sets the marker once from the database clock and prints it:

```json
{"activated_at":"2026-11-26T18:30:00.123456Z","already_active":false}
```

Repeating the command, from any host, changes nothing and prints the recorded instant with `"already_active":true`. After a lost reply or a crash, repeat it to learn the outcome. A cleanup whose database session was lost holds no lock, so activation can commit while its last provider call is outstanding. Its durable reservation keeps the UUID protected until that call is completed.

Nothing else writes the marker. Server startup, HTTP requests, `make cleanup` deletions, the MCP tool and the smoke tests only read it, and no environment variable in a service or test shell can authorize the transition. Every compatible process reads the marker from the database for each decision, with its local settings unset or different from other replicas. If the lookup fails, it reports an error and never falls back to an ID-only deletion.

From activation, documents that a replica admits store incarnation-tagged vectors in a hosted or in-process vector backend that can delete one incarnation's vectors, and ID-only requests for a live UUID receive `428`. PostgreSQL vectors need no tag, because they leave with the document in one transaction. A deployment with `HOSTED_VECTOR_MANIFEST` enabled keeps manifested revisions, which already fence vectors by exact IDs.

### Smoke testing a staging deployment

This procedure is the single home of the prerequisites and the invocation for the opt-in smoke suite. Run it against a staging deployment whose database is separate from production, and record the UUID of a completed, searchable document that predates the upgrade.

1. Expose the staging server at `localhost:8081`. Configure the test environment with the staging service token and the same external service settings as the staging server.
2. While the target is inactive, run only the checks that write no documents:

   ```sh
   RUN_SMOKE=1 SMOKE_EXISTING_DOCUMENT_ID="<pre-upgrade-document-uuid>" \
     go test ./test/smoke/... -count=1 -v -timeout 600s \
     -run 'TestSmoke_(HealthIncludesS3|UnsupportedFileType|PreexistingDocumentRemainsRetrievable)$'
   ```

3. Activate the staging database with the [activation command](#activate-checkpointed-deletion) and confirm `GET /deletion-mode` reports `active`.
4. Run the whole suite with the same variables and without `-run`.

The tests that write documents ask the target service for its mode before their first ingest. When it reports `inactive`, or cannot answer, they stop with an error before any ingest or delete, and the marker stays unset. They never activate the database, whatever the test process's environment contains. Each test gives its document a unique identifier and registers cleanup only after the service accepted that request. It reads the document's incarnation once, from the accepted document, and sends every cleanup attempt as an authenticated qualified `DELETE` to that service with the saved value. A creation whose outcome is ambiguous, or an incarnation that cannot be read, never leads to a lookup of the current document: the failure names the document so an operator can remove it deliberately. Replacements that reuse an identifier are left alone, and the preexisting document is only read.

### Rollback after activation

- **Before activation, holds and tagged data.** With the marker unset and no reservation, hold or tagged document in the database, the binary can be reverted. The additive migrations and their checksums stay in place.
- **With holds.** Once reservations, quarantines or held receipts exist, keep a build that reads them at admission and the cleanup command that releases them.
- **With tagged data.** Once tagged documents exist, keep workers and readers that understand the tagged format.
- **After activation.** Keep the marker and deploy only a build that enforces the incarnation contract on every path above, or pause writes and repair forward. Never redeploy a pre-upgrade API, worker, cleanup command, reconcile command or client: it ignores the marker and every receipt, and a bare request deletes a live document by UUID. Keep those executables retired.
- **SDK rollback.** Reverting the Pinecone SDK dependency is supported only inside a deletion-aware service build. The rebuilt image keeps the activated contract, so a bare `DELETE` of a live UUID still returns `428` and leaves its row, source object and vectors in place.

## Legacy Deletion Reservations

The [writer journal durability policy](#postgresql-writer-journal-durability)
also applies to dedicated legacy cleanup sessions.

ID-only deletion keeps working while the database has not activated [checkpointed deletion](api.md#checkpointed-deletion). The [reservation behavior](api.md#id-only-deletion-across-activation) closes the window in which an activation, or a lost database session, lets a replacement UUID overtake the last external call of a legacy cleanup. The server entry point and `make cleanup` enable it, and a hosted reservation is released with the [quarantine and release procedure](#quarantine-and-hold-release) below.

- Activation takes the same lock exclusively. It waits for every healthy ID-only cleanup, then commits. A cleanup whose database session was lost does not hold the lock, so activation can commit while the last provider call is outstanding. The durable reservation then blocks admission of that UUID with `409` until the call completes, and the original request completes it on a new connection.
- A live row with a reservation, for example after the first hosted call failed, still serves status, signed file addresses and same-content duplicates until an activated request qualifies the original incarnation. That request converts the reservation and fences the document.
- Each in-flight guarded ID-only cleanup holds a session of its own outside the pool for its whole cleanup, so a slow provider never takes a pooled connection from queries, admissions or worker leases. A replica runs at most [`LEGACY_DELETION_CONCURRENCY`](configuration.md#environment-variables) of them, by default a quarter of `pool_max_conns` and at least one. A further ID-only request returns `503` `busy` with `Retry-After: 5` before it takes a lock or a connection, and succeeds when repeated after a cleanup has finished. Qualified requests are not counted. The standalone command runs one cleanup at a time. See [PostgreSQL connections](#concurrency-limits).
- A qualified request sent to an endpoint that predates the header can be treated as an ID-only request. Send qualified deletions only after every endpoint in the pool enforces the qualifier, and keep ID-only clients until then.

### Quarantine and Hold Release

Operators can inspect, quarantine and release held identities in either activation mode without setting the activation marker:

1. **Inspect held and unfinished receipts.** Run `make cleanup IDS="--list-held"` to view each held or unfinished `document_id`, `incarnation`, `kind`, backend (`vector_mode`, `vector_index`, `memory_owner`), `source_key`, `source_bucket`, `phase`, `document_present` and active `claim` state.
2. **Pause mutating callers before quarantining.** Stop writers, deletion callers and pre-upgrade processes that could issue unreceipted requests. Quarantine known absent UUIDs with `make cleanup IDS="--quarantine <document-uuid>"` (accepts one or more bare UUIDs; rejects live documents). Quarantine acquires the admission lock and creates a fresh recovery `incarnation` only when no receipt exists or the prior receipt was already released; on an existing held or unfinished receipt (including an unfinished legacy reservation whose document row was deleted by an older executable), it preserves the recorded incarnation, source, backend, progress and claim and returns the effective pair. Keep affected live documents paused until their external operations settle.
3. **Verify provider settlement.** Before releasing any hold, stop all processes that can issue writes or cleanup for that UUID and establish settlement in the original index and namespace. On Pinecone serverless, when a write or delete response recorded an `x-pinecone-request-lsn`, query the same namespace until `x-pinecone-max-indexed-lsn` reaches that value; for unacknowledged attempts without a sequence number, confirm settlement directly with the provider. Reconcile any residual vectors while the UUID remains held.
4. **Release the verified identity.** Run `make cleanup IDS="--release-held <document-uuid> <incarnation-uuid>"`. Release checks that no live document row exists, no live claim is active, and the configured source bucket, vector index and backend match the receipt. For a Pinecone receipt with `vector_mode=legacy`, set `PINECONE_INDEX_NAME` to the exact recorded `vector_index` and configure the active `VECTOR_GENERATION`. The command holds the writer barrier and releases without contacting Pinecone. Configuration still requires a nonempty `PINECONE_API_KEY`, which this release leaves unused. An unfinished legacy reservation with a pending `source_key` requires the recorded S3 bucket and credentials to delete that exact object before clearing the hold. Scoped revision holds still use the verified provider to replay the recorded revision. For `GO_RAG_FAKE_MODE=true`, `--release-held` routes `DELETE /documents/{doc_id}?release_held=true` (`client.ReleaseHeldOfflineDeletion`) to the owning offline server. If that server terminated and its local index is destroyed, pass `--offline-owner-terminated=<owner-uuid>` (or `unknown` if no owner was recorded) to recover administratively against PostgreSQL and S3. The serving process records its owner on every qualified and ID-only offline deletion. A hold made by `--quarantine`, which runs out of process, and a receipt written by an earlier release have no recorded owner, so they are released only with `--offline-owner-terminated=unknown` after the owning service has stopped. Any mismatch, live document, active claim or source failure leaves the hold intact.

   If finalization fails after the transaction begins, the release uses a three-second timeout for rollback, then makes a best-effort attempt to clear its own claim token before returning the original error. If the claim commit outcome is uncertain, the release also attempts to clear its own claim token. The hold and any pending source cleanup stay intact after an uncertain claim commit. Retry with the same document UUID and incarnation. If PostgreSQL cannot confirm claim cleanup, the claim can remain busy until it expires.

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

---

## Grants Mode Operations and Benchmarks

In `AUTHZ_MODE=grants`, Quarry enforces delegated subject credentials and tenant/document grants inside the service alongside `X-Service-Token`. In `AUTHZ_MODE=service`, authorization scopes (`allowed_document_ids`) remain the caller's responsibility; in `grants` mode, callers present both `X-Service-Token` and a short-lived signed JWT bearer token (`Authorization: Bearer <jwt>`).

### Grants cutover

Before enabling grants, apply metadata migrations and complete the
[serving-role upgrade](grant-catalog.md#upgrade-an-existing-serving-role).
Deploy receipt-capable service replicas that take the corpus writer lock.
Close mutating ingress and fence every older executable, including builds
that only read the migration 029 marker, using network policy and separate
PostgreSQL logins. A mixed rollout is unsafe while an older build can write.
Drain in-flight requests, workers and outstanding provider work under the
[checkpointed deletion upgrade order](#upgrade-order). Provider mutations may
already be in flight when the database fences publication. Complete any required
vector reindex under [Changing Models or Vector Stores](#changing-models-or-vector-stores)
before permitting the new replicas to write. A PostgreSQL reindex target for a
corpus with incarnation-tagged vectors requires a build that can stage and
delete those records.

As the schema owner, [activate checkpointed deletion](#activate-checkpointed-deletion)
and verify that the deletion-mode endpoint reports active. With ingress still
closed, use the separate [operator preview and activation](grant-catalog.md#corpus-cutover-and-security-audit)
to switch the corpus to grants. Start replicas with `AUTHZ_MODE=grants` and
check readiness before reopening ingress.

After upgrading an already-grants corpus whose deletion marker is unset, reads
and queries remain available and live scoped deletion returns `412`. Follow the
same fencing and drain procedure, then activate and verify checkpointed deletion
to restore scoped deletion. The corpus stays in grants throughout.

### Identity, Keys, Retention and Storage Boundaries

- **Trusted issuance and key rotation**: Provision principals into the grant catalog before issuing bearer tokens for them. Distribute issuer public keys to replicas in the local `AUTHZ_JWKS_FILE` (`0600`) with bounded key-set expiry. Rotate keys with overlap via atomic rename (`temp` then `rename`); removing a key invalidates tokens signed with it on the very next check across both `/readyz` and data routes, while missing or expired trust files fail `/readyz` and all data routes closed with `503`.
- **Cross-replica revocation**: Replicas recheck policy before inventory batches, status responses, file signing, deletion and each SSE query disclosure frame. Follow the [serving-role upgrade procedure](grant-catalog.md#upgrade-an-existing-serving-role) for scoped inventory revisions. During a rolling upgrade or binary rollback, older replicas still use the catalog-wide revision for inventory and can retry on unrelated imports. Measure inventory retry rates after all serving replicas are upgraded.
- **Metrics, telemetry and S3 storage**: Keep `METRICS_ADDR` bound to a private operator interface. In `grants` mode telemetry forces content capture off and omits document IDs, object keys, presigned URLs, raw `sub`, prompts, excerpts, and membership lists from logs, traces and metrics. Audit rows carry internal document and incarnation UUIDs, but no credentials, raw subjects, signed addresses or text. Original objects remain private in `S3_BUCKET`, and presigned URLs returned by `GET /documents/{doc_id}/file` are bounded to the lesser of `900s` or the remaining bearer token lifetime.
- **Upstream provider retention**: Vector indexes (`pinecone` when used), embedding providers, rerankers, and LLM endpoints still process submitted text outside Quarry during ingest or synthesis; configure provider data-retention and zero-retention agreements to match tenant sensitivity, and use `answer_mode=evidence` when synthesis must be omitted.

### Benchmarks: `service` vs `grants`

No backend comparison figures are published yet. No Pinecone figures are published.

To measure PostgreSQL pgvector query paths (`service` vs `grants` across 0, 1, 10,000, and 25,000 documents) with Docker available:

```sh
go test -run=^$ -bench=BenchmarkPGVectorAuthzModes -benchmem ./internal/store/postgres
```

To compare live Pinecone vector queries (`service` allowlists vs `grants` tenant+incarnation filters) with live credentials:

```sh
QUARRY_BENCHMARK_LIVE=1 PINECONE_API_KEY=<key> PINECONE_INDEX_NAME=<index> go test -run=^$ -bench=BenchmarkPineconeAuthzModes -benchmem ./internal/store/pinecone
```
