# Configuration

Configuration parameters are loaded exclusively from environment variables at startup via `internal/config/config.go`. Startup fails if required variables are missing, configured values are outside their allowed bounds, or numbers, booleans and durations cannot be parsed. Malformed values are errors rather than silent fallbacks.

## Environment Variables

The table below lists the environment variables recognized by the service:

| Variable | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `AWS_ACCESS_KEY_ID` | string | `""` | Set together with `AWS_SECRET_ACCESS_KEY`, or leave both empty | Optional static AWS access key ID. When both keys are empty, AWS clients use the default credential chain. |
| `AWS_MAX_ATTEMPTS` | integer | `4` | 1 to 10 | Maximum AWS request attempts, including the first attempt. Standard retries have a 10-second maximum backoff, no client-side retry quota, and include `ModelTimeoutException` and `ModelNotReadyException`. |
| `AWS_REGION` | string | `""` | Valid AWS region identifier | AWS region used for AWS Bedrock API calls and S3 object storage. Required when `GO_RAG_FAKE_MODE=false`. |
| `AWS_SECRET_ACCESS_KEY` | string | `""` | Set together with `AWS_ACCESS_KEY_ID`, or leave both empty | Optional static AWS secret access key. |
| `AWS_SESSION_TOKEN` | string | `""` | Optional string | Session token for temporary static AWS credentials. |
| `BEDROCK_EMBED_MODEL` | string | `"amazon.titan-embed-text-v2:0"` | Non-empty string | Bedrock model identifier used for generating text embeddings. |
| `BEDROCK_FALLBACK_MODEL` | string | `"us.meta.llama4-maverick-17b-instruct-v1:0"` | Non-empty string | Bedrock model identifier for answer synthesis and fallback routing. |
| `BEDROCK_PRIMARY_MODEL` | string | `"amazon.nova-micro-v1:0"` | Non-empty string | Bedrock model identifier for low-latency reasoning and fast answer synthesis. |
| `CHUNK_OVERLAP` | integer | `64` | >= 0 and < `CHUNK_TOKEN_SIZE` | Maximum estimated tokens retained from the previous chunk; overlap is reduced when needed to include new text within both limits. |
| `CHUNK_TOKEN_SIZE` | integer | `512` | 2 to 8192 | Maximum estimated tokens per text chunk. Dense scripts count one token per character; chunks never exceed 30,000 bytes. |
| `DATABASE_URL` | string | `""` | Valid PostgreSQL connection string | PostgreSQL connection string for document metadata and chunk storage. Always required in all modes. Defaults to 16 pooled connections and a 30-second statement timeout; override with `pool_max_conns` and `statement_timeout`. |
| `EMBEDDING_DIMENSIONS` | integer | `1024` | 256, 512, or 1024 | Output vector dimensions requested from Bedrock Titan Embed V2. Responses with a different length or non-finite values are rejected. |
| `EMBED_CONCURRENCY` | integer | `10` | 1 to 50 | Maximum number of concurrent embedding requests dispatched to AWS Bedrock. |
| `EMBED_TIMEOUT` | duration | `15s` | 1s to 2m | Deadline for each embedding call, including waiting for a concurrency slot and all retries. An earlier caller deadline takes precedence. |
| `EXTRACT_CONCURRENCY` | integer | `2` | 1 to 64 | Maximum concurrent multipart handlers, including upload reads and extraction. |
| `EXTRACT_ISOLATION` | boolean | `true` | Boolean | Extract PDF, DOCX and DOC text in a separate process. Disabling this removes crash isolation and the child process limits. |
| `EXTRACT_MEMORY_LIMIT_MB` | integer | `512` | 64 to 8192 | Soft Go memory limit per extraction child in MiB. This is not a hard process memory cap. |
| `EXTRACT_QUEUE_TIMEOUT` | duration | `5s` | 0s to 60s | Maximum wait for an upload slot; zero rejects immediately when full. |
| `EXTRACT_TIMEOUT` | duration | `30s` | 1s to 10m | Maximum wall-clock time for each isolated extraction. |
| `GO_RAG_FAKE_MODE` | boolean | `false` | Boolean (`true`, `false`, `1`, `0`) | Runs the service in offline fake mode using an in-memory vector store and mock model generators without AWS or Pinecone credentials. Never enable in production deployments. |
| `HEALTH_CACHE_TTL` | duration | `10s` | 1s to 5m | Lifetime of the dependency snapshot shared by diagnostics and readiness. Concurrent requests share one refresh; failures are cached too. |
| `HTTP_IDLE_TIMEOUT` | duration | `60s` | Positive | Idle keep-alive connection timeout. |
| `HTTP_READ_TIMEOUT` | duration | `60s` | 5s to 30m | Maximum time to read a complete HTTP request, including the body. |
| `HTTP_WRITE_TIMEOUT` | duration | `QUERY_TIMEOUT + 15s` | Greater than `QUERY_TIMEOUT` | Response write deadline; defaults to 75s with the default query budget. |
| `INDEX_VISIBILITY_TIMEOUT` | duration | `30s` | 0s to 5m; less than `INGEST_JOB_TIMEOUT` | Maximum wait after vector writes for search to return the document's last embedded chunk; zero disables waiting. Timeout logs a warning and allows completion. |
| `INGEST_JOB_TIMEOUT` | duration | `15m` | 1m to 2h; greater than lease | Maximum processing time per attempt. |
| `INGEST_LEASE` | duration | `2m` | 30s to 30m | Lease duration, renewed every third of this interval. |
| `INGEST_MAX_ATTEMPTS` | integer | `5` | 1 to 20 | Maximum processing attempts before recording terminal failure. |
| `INGEST_POLL_INTERVAL` | duration | `1s` | 100ms to 1m | Polling interval for ready jobs across replicas. |
| `INGEST_WORKERS` | integer | `2` | 1 to 32 | Parallel document claim loops per replica. |
| `LOCAL_RETRIEVAL_THRESHOLD` | float64 | `0.70` | 0.0 to 1.0 | Cosine similarity threshold for routing single-need queries to LOCAL retrieval mode. |
| `LOG_LEVEL` | string | `info` | `debug`, `info`, `warn`, `error` (case-insensitive, trimmed) | Minimum severity emitted to stderr as JSON. Access logs use info; recovered panics use error. |
| `MAX_ALLOWED_DOCUMENT_IDS` | integer | `25000` | 1 to 100000 | Maximum allowlist entries per query before deduplication; the 1 MiB query body limit also applies. Vector queries split lists into groups of at most 10000 IDs and merge the top results. |
| `MAX_CHUNKS_PER_QUERY` | integer | `10` | 1 to 100 | Maximum chunks retrieved per query. Requests with omitted or zero `max_chunks` use this value; larger requests are capped at this value. |
| `MAX_UPLOAD_BYTES` | int64 | `20971520` (20 MiB) | 1 to 104857600 (100 MiB) | Maximum accepted request payload size for multipart file uploads in bytes. |
| `METRICS_ADDR` | string | `""` (disabled) | Optional TCP listen address | Separate metrics listener; bind to a private interface and never expose publicly. |
| `MIN_RELEVANCE_SCORE` | float64 | `0` | 0 to 1, finite | Absolute evidence score floor applied before adaptive synthesis trimming; zero disables this floor. |
| `PINECONE_API_KEY` | string | `""` | Non-empty string | API key for authenticating with Pinecone vector indices. Required when `GO_RAG_FAKE_MODE=false`. |
| `PINECONE_ENVIRONMENT` | string | `""` | Ignored | Has no effect and is not stored in configuration. A non-empty value adds the startup warning `PINECONE_ENVIRONMENT is ignored`. Remove it from deployment configuration. |
| `PINECONE_INDEX_NAME` | string | `""` | Required when `GO_RAG_FAKE_MODE=false` | Target Pinecone index, for example `quarry-rag`. There is no implicit default; missing values prevent startup with external services. Optional in offline mode. |
| `PINECONE_MAX_ATTEMPTS` | integer | `5` | 1 to 10 | Maximum vector write and deletion attempts, including the first attempt; queries use at most three attempts. |
| `PINECONE_TIMEOUT` | duration | `10s` | 1s to 2m | Timeout for each vector index request attempt. |
| `PINECONE_UPSERT_BATCH_SIZE` | integer | `100` | 1 to 1000 | Maximum records per vector upsert; each batch also has a 1.5 MiB estimated size limit. |
| `QUERY_CONCURRENCY` | integer | `32` | 1 to 1024 | Maximum concurrent queries per handler instance. |
| `QUERY_QUEUE_TIMEOUT` | duration | `2s` | 0s to 60s | Maximum wait for query admission; zero rejects immediately when all slots are occupied. |
| `QUERY_TIMEOUT` | duration | `60s` | 5s to 10m | Total retrieval and synthesis budget after query admission; earlier caller deadlines take precedence. |
| `REQUIRE_DOCUMENT_ALLOWLIST` | boolean | `false` | Boolean | Require a non-null `allowed_document_ids` field on every query; an empty list is accepted and skips embedding and search. |
| `S3_BUCKET` | string | `""` | Explicit bucket name | S3 bucket for original uploads. Empty disables source file storage with a startup warning. Startup checks bucket access within 10 seconds; a 403 or 404 prevents startup, while transient errors produce a warning and startup continues. |
| `S3_EXPECTED_BUCKET_OWNER` | string | `""` | Empty or exactly 12 ASCII digits | Optional AWS account ID expected to own the bucket. Sent with uploads, deletions and bucket checks; omitted from presigned downloads. |
| `SERVER_PORT` | string | `"8081"` | 1 to 65535 | TCP port on which the HTTP server listens. |
| `SERVICE_TOKEN` | string | `""` | No whitespace or control characters after trimming | Shared secret merged with `SERVICE_TOKENS`. At least one token is required when `GO_RAG_FAKE_MODE=false`. |
| `SERVICE_TOKENS` | comma-separated strings | `""` | No whitespace or control characters within a token after trimming | Additional secrets accepted in `X-Service-Token` for rotation. Tokens are trimmed, empty entries are dropped and duplicates are removed across both variables. Startup warns with only the count of tokens shorter than 32 bytes; use secrets of at least 32 bytes. An empty combined list leaves routes open only in offline mode. |
| `SHUTDOWN_DRAIN_DELAY` | duration | `5s` | 0s to 60s | Delay after readiness starts returning draining, before HTTP shutdown. A second termination signal skips the remaining delay. |
| `SYNTHESIS_MODEL` | string | `"fallback"` | `"primary"`, `"fallback"`, or `"hybrid"` | Model routing policy for query answer synthesis: `"fallback"` uses Llama 4 Maverick, `"primary"` uses Nova Micro, and `"hybrid"` dynamically selects between models based on query complexity. |
| `SYNTHESIS_TIMEOUT` | duration | `45s` | Positive and below `QUERY_TIMEOUT` | Synthesis call budget, including SDK retries. |
| `WORKER_QUEUE_SIZE` | integer | `1000` | 1 to 100000 | Maximum outstanding persisted ingestion jobs across replicas, including running jobs. |

---

## Tracing

Tracing is disabled unless a destination is explicitly enabled. Generic OTLP and
LangSmith can be enabled together. Other OTLP exporter variables (including TLS
and compression) and batch processor variables
defined by the OpenTelemetry Go SDK pass through to the generic destination and
processors respectively. LangSmith uses its own endpoint, headers, system trust
store, protobuf encoding and 10-second export timeout, independent of generic
exporter settings.

| Variable | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `LANGSMITH_API_KEY` | string | `""` | Required when LangSmith is enabled, unless the SDK is disabled; no whitespace or control characters | LangSmith authentication key. Never logged. |
| `LANGSMITH_ENDPOINT` | string | LangSmith US API address | Absolute HTTP or HTTPS API address, without credentials, query or fragment | Regional or self-hosted API address from LangSmith; self-hosted addresses include `/api/v1`. The service appends `/otel/v1/traces`. |
| `LANGSMITH_PROJECT` | string | `""` | No control characters | Optional `Langsmith-Project` header; omitted to use the destination's default project. |
| `LANGSMITH_TRACING` | boolean | `false` | Boolean | Enables LangSmith export independently of the generic exporter. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | string | SDK loopback collector address | SDK OTLP/HTTP endpoint | Base collector address; the SDK appends the trace path. Used only for the generic destination. |
| `OTEL_EXPORTER_OTLP_HEADERS` | string | `""` | SDK comma-separated, percent-encoded key-value pairs | Generic destination headers. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | string | `http/protobuf` | `http/protobuf` or `http/json` when tracing is enabled | Generic payload encoding. gRPC is unsupported. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | integer | `10000` | SDK timeout in milliseconds | Generic export deadline. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | string | SDK trace endpoint derived from the base address | SDK OTLP/HTTP endpoint | Complete trace address including its path; takes precedence over the base address. |
| `OTEL_EXPORTER_OTLP_TRACES_HEADERS` | string | Unset | SDK header encoding | Trace-specific generic headers; overrides the generic header setting. |
| `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | string | Inherits generic protocol | `http/protobuf` or `http/json` when tracing is enabled | Trace-specific generic encoding. |
| `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` | integer | Inherits generic timeout | SDK timeout in milliseconds | Trace-specific generic export deadline. |
| `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT` | boolean | `false` | Boolean | Adds question, prompt, document excerpt, embedding input and answer text to recorded spans. |
| `OTEL_RESOURCE_ATTRIBUTES` | string | `""` | SDK comma-separated key-value pairs | Overrides resource attributes, including the default service name and build version. |
| `OTEL_SDK_DISABLED` | boolean | `false` | Boolean | Disables both destinations even when otherwise enabled. |
| `OTEL_SERVICE_NAME` | string | `quarry-rag` | SDK service name | Overrides the resource service name, taking precedence over resource attributes. |
| `OTEL_TRACES_EXPORTER` | string | `none` | `none` or `otlp` | Explicitly enables the generic OTLP/HTTP exporter. Other values fail startup. |
| `OTEL_TRACES_SAMPLER` | string | `parentbased_always_on` | `always_on`, `always_off`, `traceidratio`, `parentbased_always_on`, `parentbased_always_off` or `parentbased_traceidratio`; other values fail startup when tracing is enabled | Selects sampling for both destinations. |
| `OTEL_TRACES_SAMPLER_ARG` | string | Unset | A ratio from 0 to 1 for ratio samplers; malformed values fail startup when tracing is enabled | Ratio for ratio-based sampling. |

---

## MCP Server Environment

The local stdio command reads these variables separately from the HTTP service. Invalid settings prevent startup. See [MCP server usage](mcp.md).

| Variable | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `QUARRY_MCP_FILE_ROOTS` | path list | `""` (disabled) | Existing directories separated by the OS path-list separator | Allowed roots for file ingestion; roots and file paths are resolved through symlinks. Only regular files with `.pdf`, `.docx`, `.doc`, `.txt`, `.md` or `.markdown` extensions are accepted, case-insensitively. |
| `QUARRY_MCP_READ_ONLY` | boolean | `false` | Boolean | Registers only query, list and get tools. |
| `QUARRY_SERVICE_TOKEN` | string | `""` | No whitespace or control characters after trimming | Sent only in the `X-Service-Token` request header. |
| `QUARRY_TIMEOUT` | duration | `90s` | 1s to 10m | HTTP request timeout, including upload and response transfer. The default exceeds the service's default write timeout. |
| `QUARRY_URL` | string | Required | Absolute HTTP or HTTPS address, or `host:port` for plain HTTP; no credentials, query or fragment | HTTP API base address; any path prefix is preserved. |

## Schema Migrations

Database schema migrations are SQL files named with a three-digit version and a descriptive suffix, such as `006_add_index.sql`. Every file under `internal/store/postgres/migrations` matching `NNN_name.sql` is embedded in the binary automatically. Versions must be unique and are applied in ascending order by `internal/store/postgres/migrate.go`.

On startup, the runner acquires a PostgreSQL transaction-scoped advisory lock and applies missing versions in one transaction. The `schema_migrations` table records each version, filename, SHA-256 checksum and application timestamp. Concurrent replicas wait for the lock before checking this table. Subsequent startups verify checksums and skip applied SQL, avoiding table locks on document data when no migration is pending. Never edit an applied migration; add a new numbered file instead. A checksum mismatch stops startup. Existing databases without migration history apply the original idempotent files once and record them without removing stored documents.

Application connections default to `pool_max_conns=16` and `statement_timeout=30000` (milliseconds). Explicit settings in `DATABASE_URL` override these defaults, including `statement_timeout=0` to disable the timeout. Both URI query parameters and keyword connection strings are supported; for example, `host=localhost dbname=rag pool_max_conns=24 statement_timeout=10000` sets 24 connections and a 10-second statement timeout. The migration transaction disables statement and lock timeouts while waiting for the advisory lock, then uses a 15-second lock timeout when applying SQL. Transaction-local settings do not change application connection settings after commit or rollback.

| File | Description |
|---|---|
| `001_init.sql` | Enables the `pgcrypto` extension. Creates the `documents` table, the `chunks` table with foreign key cascading deletion (`ON DELETE CASCADE`), and the `document_fingerprints` deduplication table with indexing. |
| `002_add_file_storage.sql` | Adds nullable columns `file_name`, `file_type`, and `s3_key` to `documents` to track raw uploaded files stored in S3. |
| `003_add_content_preview.sql` | Adds the `content_preview` column to `documents`, backfills existing rows with the first 120 characters of `content`, and creates a functional index on `LOWER(content_preview)`. |
| `004_add_doc_type.sql` | Adds the `doc_type` column to `documents` and backfills values from `metadata->>'doc_type'`. |
| `005_document_default.sql` | Sets the default column value for `doc_type` in `documents` to `'document'`. |
| `006_ingest_jobs.sql` | Adds durable ingestion jobs, lease and failure tracking, completion timestamps and document fingerprints. Normalizes previously persisted processing rows with fingerprints to completed and requeues other processing rows. |
| `007_document_collections.sql` | Adds collection labels to documents and an index on `(collection, id)`; existing rows belong to the default collection. |

## Metrics

Set `METRICS_ADDR` to enable `GET /metrics` on a separate listener. The main API
listener does not serve metrics. Bind the metrics listener to a private interface
and restrict access to the monitoring system; it has no service-token check.

Metrics use Prometheus text format. Request counts use `route` and `code` labels;
request duration uses `route`. Routes are matched method and path patterns, or
`unmatched` for requests with no matching pattern. Pipeline durations use `stage`,
ingestion outcomes use `result`, and query counts use the effective `mode`
(`LOCAL` or `GLOBAL`). All durations are in seconds. Histograms use boundaries
0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60 and 120 seconds,
plus the cumulative positive-infinity bucket.

`quarry_ingest_jobs_pending` counts database rows in the `pending` state,
including delayed retries and excluding running leases. Concurrent scrapes share
a 10-second cache, including failed refreshes. A failed refresh retains the last
successful value, or reports `NaN` before any successful refresh. This gauge is
shared across replicas through the database; do not sum it across replicas.
Other counters and histograms describe the current process and reset on restart.

`quarry_index_visibility_waits_total{result}` counts visibility waits by outcome:
`visible` when search returns the document's last vector, and `timeout` when the
wait expires or worker shutdown ends it first. Disabled waits and canceled
attempts are not counted. The `persist` stage duration includes the wait.
