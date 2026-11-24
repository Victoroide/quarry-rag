# Configuration

Configuration parameters are loaded exclusively from environment variables at startup via `internal/config/config.go`. Startup fails if required variables are missing, configured values are outside their allowed bounds, or numbers, booleans and durations cannot be parsed. Malformed values are errors rather than silent fallbacks.

## Environment Variables

The table below lists the environment variables recognized by the service:

| Variable | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `AUTHZ_AUDIENCE` | string | `""` | Quarry-specific audience reserved at the issuer | Staged [subject authority](grant-catalog.md#subject-identity) setting. |
| `AUTHZ_ISSUER` | string | `""` | Exact HTTPS issuer without credentials, query or fragment | Staged subject authority setting. |
| `AUTHZ_JWKS_FILE` | path | `""` | Local public key set | Staged subject authority setting. |
| `AUTHZ_MODE` | string | `service` | `service` or unset | Other values prevent startup. |
| `AWS_ACCESS_KEY_ID` | string | `""` | Set together with `AWS_SECRET_ACCESS_KEY`, or leave both empty | Optional static AWS access key ID. When both keys are empty, AWS clients use the default credential chain. |
| `AWS_MAX_ATTEMPTS` | integer | `4` | 1 to 10 | Maximum AWS request attempts, including the first attempt. See [AWS retries](deployment.md#aws-retries-and-deadlines) for retry policy details. |
| `AWS_REGION` | string | `""` | Valid AWS region identifier | AWS region used for AWS Bedrock API calls, Bedrock reranking and S3 object storage. Required outside offline mode when either provider uses Bedrock, reranking is enabled, or original-file storage is enabled. |
| `AWS_SECRET_ACCESS_KEY` | string | `""` | Set together with `AWS_ACCESS_KEY_ID`, or leave both empty | Optional static AWS secret access key. |
| `AWS_SESSION_TOKEN` | string | `""` | Optional string | Session token for temporary static AWS credentials. |
| `BEDROCK_EMBED_MODEL` | string | `"amazon.titan-embed-text-v2:0"` | Non-empty string | Bedrock model identifier used for generating text embeddings. |
| `BEDROCK_FALLBACK_MODEL` | string | `"us.meta.llama4-maverick-17b-instruct-v1:0"` | Non-empty string | Bedrock model identifier for answer synthesis and fallback routing. |
| `BEDROCK_GUARDRAIL_ID` | string | `""` (disabled) | Requires `LLM_PROVIDER=bedrock`; set together with `BEDROCK_GUARDRAIL_VERSION`; no whitespace or control characters | Optional Bedrock guardrail identifier attached to Converse and ConverseStream requests. See [synthesis behavior](architecture.md#query-stages) and [required permissions](deployment.md#bedrock-guardrails). |
| `BEDROCK_GUARDRAIL_VERSION` | string | `""` (disabled) | Set together with `BEDROCK_GUARDRAIL_ID`; `DRAFT` or a positive decimal integer without leading zeros | Guardrail version to apply. |
| `BEDROCK_PRIMARY_MODEL` | string | `"amazon.nova-micro-v1:0"` | Non-empty string | Bedrock model identifier for low-latency reasoning and fast answer synthesis. |
| `CHUNK_OVERLAP` | integer | `64` | 0 to `CHUNK_TOKEN_SIZE/2` (integer division) | Maximum estimated tokens retained from the previous chunk. Each chunk starts at least halfway through the previous one, in estimated tokens and bytes, so total chunk text is at most twice the normalized input. |
| `CHUNK_TOKEN_SIZE` | integer | `512` | 2 to 8192 | Maximum estimated tokens per text chunk. Dense scripts count one token per character; chunks never exceed 30,000 bytes. |
| `DATABASE_URL` | string | `""` | Valid PostgreSQL connection string | PostgreSQL connection string for document metadata and chunk storage. Always required in all modes. Pool size and statement timeout defaults are described under [Schema Migrations](#schema-migrations). |
| `DOC_STORIES` | comma-separated set | `body` | `body`, `footnotes`, `endnotes`, `headers`, `footers`, `comments` | Selects optional [Word binary stories](api.md#word-binary-extraction) for new uploads. The body is always included. Empty entries, duplicate names, and unknown names are configuration errors. |
| `DOCX_STORIES` | comma-separated set | `body,footnotes,endnotes` | Any nonempty subset of `body`, `footnotes`, `endnotes`, `headers`, `footers`, `comments`. Names are case-sensitive, surrounding whitespace is trimmed, and duplicate or empty entries are invalid. Unset or empty uses the default. | Keep the same set on all replicas that accept uploads. See [DOCX story selection](api.md#docx-story-selection). |
| `EMBEDDING_DIMENSIONS` | integer | `1024` | Bedrock: 256, 512, or 1024; OpenAI-compatible: 1 to 4096 | Embedding vector dimensions: requested from Bedrock Titan; with an OpenAI-compatible provider, the model's output size. Responses with a different length, missing values or non-finite values are rejected. Must match the vector index dimensions; the PostgreSQL vector backend accepts at most 2000. |
| `EMBEDDING_INPUT_PROFILE` | file path | `""` (disabled) | Local JSON profile for the configured embedding provider and model | See [Embedding input profiles](#embedding-input-profiles). |
| `EMBEDDING_PROVIDER` | string | `bedrock` | `bedrock` or `openai` | Embedding backend; selected independently of the synthesis provider. |
| `EMBED_CONCURRENCY` | integer | `10` | 1 to 50 | Maximum number of concurrent embedding requests dispatched to the selected provider. |
| `EMBED_TIMEOUT` | duration | `15s` | 1s to 2m | Deadline for each embedding call after acquiring a concurrency slot, including retries when supported. The caller context also bounds waiting for a slot. An earlier caller deadline takes precedence. |
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
| `INDEX_VISIBILITY_MODE` | string | `sample` | `sample`, `lsn` | In Pinecone, `sample` searches for the last embedded chunk, and `lsn` checks the serverless index's write receipts and query watermark. |
| `INDEX_VISIBILITY_TIMEOUT` | duration | `30s` | 0s to 5m; less than `INGEST_JOB_TIMEOUT` | Maximum Pinecone visibility wait after vector writes. Zero disables waiting. A timeout logs a warning and allows completion. |
| `HOSTED_VISIBILITY_REFILL` | bool | `false` | Boolean | Enable [visibility refill](architecture.md#hosted-visibility-refill) for Pinecone queries. Independent of `INDEX_VISIBILITY_MODE`. |
| `HOSTED_VISIBILITY_EXTRA_REQUESTS` | integer | `1` | Non-negative | Maximum additional Pinecone requests per enabled query. Zero keeps the current-chunk checks and allows no additional requests. |
| `INGEST_CONTENT_DEDUP` | string | `collection` | `collection` or `off` | Selects the [document identity policy](api.md#document-identity) for new uploads. See [rollout and rollback](deployment.md#content-deduplication-rollout). |
| `INGEST_JOB_TIMEOUT` | duration | `15m` | 1m to 2h; greater than lease | Maximum processing time per attempt. |
| `INGEST_LEASE` | duration | `2m` | 30s to 30m | Lease duration, renewed every third of this interval. |
| `INGEST_MAX_ATTEMPTS` | integer | `5` | 1 to 20 | Maximum processing attempts before recording terminal failure. |
| `INGEST_POLL_INTERVAL` | duration | `1s` | 100ms to 1m | Polling interval for ready jobs across replicas. |
| `INGEST_WORKERS` | integer | `2` | 1 to 32 | Parallel document claim loops per replica. |
| `LLM_PROVIDER` | string | `bedrock` | `bedrock` or `openai` | Answer synthesis backend. Bedrock guardrails with `openai` fail startup validation. |
| `LOCAL_RETRIEVAL_THRESHOLD` | float64 | `0.70` | 0.0 to 1.0 | Cosine similarity threshold for routing single-need queries to LOCAL retrieval mode. |
| `LOG_LEVEL` | string | `info` | `debug`, `info`, `warn`, `error` (case-insensitive, trimmed) | Minimum severity emitted to stderr as JSON. Access logs use info; recovered panics use error. |
| `MAX_ALLOWED_DOCUMENT_IDS` | integer | `25000` | 1 to 100000 | Maximum allowlist entries per query before deduplication; the 1 MiB query body limit also applies. Vector queries split lists into groups of at most 10000 IDs and merge the top results. |
| `MAX_CHUNKS_PER_QUERY` | integer | `10` | 1 to 100 | Maximum chunks retrieved per query. Requests with omitted or zero `max_chunks` use this value; larger requests are capped at this value. |
| `MAX_UPLOAD_BYTES` | int64 | `20971520` (20 MiB) | 1 to 104857600 (100 MiB) | Maximum accepted request payload size for multipart file uploads in bytes. |
| `METRICS_ADDR` | string | `""` (disabled) | Optional TCP listen address | Separate metrics listener. See [Metrics](deployment.md#metrics) for listener exposure guidance. |
| `MIN_RELEVANCE_SCORE` | float64 | `0` | 0 to 1, finite | Absolute relevance floor before adaptive context selection. Zero disables this floor. |
| `OCR_PDF_MODE` | string | `image_only` | `image_only` or `mixed`; `mixed` requires `OCR_PROVIDER=textract`, `EXTRACT_ISOLATION=true` and `GO_RAG_FAKE_MODE=false` | Selects [whole-document](api.md#image-only-pdf-ocr) or [page-aware](api.md#mixed-pdf-ocr) detection. Follow the [rollout procedure](deployment.md#mixed-pdf-ocr-rollout) before enabling `mixed`. |
| `OCR_PROVIDER` | string | `none` | `none` or `textract`; `textract` requires `S3_BUCKET` | Enables PDF OCR. Offline fake mode keeps parser-only ingestion. |
| `OCR_TIMEOUT` | duration | `10m` | Positive; below `INGEST_JOB_TIMEOUT` when OCR is enabled | Total Textract start, polling and result pagination budget per ingestion attempt. Earlier job cancellation takes precedence. |
| `OPENAI_API_KEY` | secret string | `""` | Optional; no whitespace or control characters | Bearer credential read only from configuration. Sent only to the configured endpoint; redirects are rejected. Keep it in secret storage and out of logs. |
| `OPENAI_BASE_URL` | string | `""` | Required when either provider is `openai`; absolute HTTP or HTTPS address without credentials, query or fragment | Explicit API base address, including its version prefix when required. Endpoint paths are appended to this address; there is no implicit public endpoint. Use HTTPS for remote endpoints. |
| `OPENAI_CHAT_MODEL` | string | `""` | Required when `LLM_PROVIDER=openai` | Chat completion model name; replaces Bedrock synthesis routing for this provider. |
| `OPENAI_EMBEDDING_MODEL` | string | `""` | Required when `EMBEDDING_PROVIDER=openai` | Embedding model name. Choose a model and dimensions supported by the endpoint and vector index. |
| `PGVECTOR_EXACT_SCOPE_MAX` | integer | `0` (disabled) | `0` to `100000` | PostgreSQL vectors only. Sets the matching completed chunk limit for exact cosine search on filtered queries and routing probes. See [PostgreSQL rollout](deployment.md#postgresql-vectors) and [vector search](architecture.md#postgresql-vector-schema). |
| `PINECONE_API_KEY` | string | `""` | Non-empty string | API key for authenticating with Pinecone vector indices. Required when `GO_RAG_FAKE_MODE=false` and `VECTOR_STORE=pinecone`. |
| `PINECONE_ENVIRONMENT` | string | `""` | Ignored | Has no effect and is not stored in configuration. A non-empty value adds the startup warning `PINECONE_ENVIRONMENT is ignored`. Remove it from deployment configuration. |
| `PINECONE_INDEX_NAME` | string | `""` | Required when `GO_RAG_FAKE_MODE=false` and `VECTOR_STORE=pinecone` | Target Pinecone index, for example `quarry-rag`. There is no implicit default. Optional with PostgreSQL vectors or offline mode. |
| `PINECONE_MAX_ATTEMPTS` | integer | `5` | 1 to 10 | See [Pinecone request limits and retries](deployment.md#pinecone-vector-index). |
| `PINECONE_TIMEOUT` | duration | `10s` | 1s to 2m | Timeout for each vector index request attempt. |
| `PINECONE_UPSERT_BATCH_SIZE` | integer | `100` | 1 to 1000 | Maximum records per vector upsert; each batch also has a 1.5 MiB estimated size limit. |
| `QUERY_CONCURRENCY` | integer | `32` | 1 to 1024 | Maximum concurrent queries per handler instance. |
| `QUERY_QUEUE_TIMEOUT` | duration | `2s` | 0s to 60s | Maximum wait for query admission; zero rejects immediately when all slots are occupied. |
| `QUERY_ANSWER_POLICY` | string | `both` | `both`, `evidence_only` | Deployment policy for [query answer modes](api.md#evidence-only-queries). |
| `EVALUATION_COLLECTION_PREFIX` | string | empty | 1–40 lowercase ASCII letters, digits or hyphens, starting with a letter or digit | Enables the [staging fixture routes and query diagnostic](development.md#evidence-evaluation) for a dedicated collection prefix. |
| `NO_EVIDENCE_CATALOG_FILE` | path | `""` | Empty or a readable local file | Optional [no-evidence translation catalog](#no-evidence-translation-catalog). |
| `QUERY_TIMEOUT` | duration | `60s` | 5s to 10m | Total query budget after admission. Earlier caller deadlines take precedence. |
| `REQUIRE_DOCUMENT_ALLOWLIST` | boolean | `false` | Boolean | Require a non-null `allowed_document_ids` field on every query; an empty list is accepted and skips embedding and search. |
| `RERANK_MODEL_ARN` | string | `""` | Empty, or an `arn:` value without whitespace or control characters | Enables [reranking before synthesis](api.md#post-query); empty disables it. Ignored in offline fake mode. See [Bedrock permissions](deployment.md#bedrock-reranking). |
| `RERANK_TIMEOUT` | duration | `5s` | 1s to 60s | Reranking call budget, including SDK retries and paginated results; earlier query or caller deadlines take precedence. |
| `ROUTING_EMBEDDING_REVISION` | string | `""` | Immutable revision label up to 256 bytes with no spaces or control characters | Identifies the deployed OpenAI-compatible embedding revision for [profile matching](routing-calibration.md). |
| `ROUTING_MARGIN` | float64 | calibrated profile margin | 0 to 1, finite | Minimum score lead used for `evidence` routing. Overrides the matching profile when set. Ignored under `legacy`. |
| `ROUTING_POLICY` | string | `"legacy"` | `legacy` or `evidence` | Automatic query routing policy; see [RoutingStage](architecture.md#query-stages). `legacy` preserves single-document threshold and structural heuristics; `evidence` requires calibration matching the active embedding profile and defaults conservatively to `GLOBAL` when missing or mismatched. |
| `ROUTING_PROFILE_FILE` | string | `""` | Existing regular JSON file up to 1 MiB | Optional [calibration profile set](routing-calibration.md) for `ROUTING_POLICY=evidence`. Ignored under `legacy`. |
| `S3_BUCKET` | string | `""` | Explicit bucket name | S3 bucket for original uploads. Empty disables source file storage with a startup warning. Startup checks bucket access within 10 seconds; a 403 or 404 prevents startup, while transient errors produce a warning and startup continues. |
| `S3_EXPECTED_BUCKET_OWNER` | string | `""` | Empty or exactly 12 ASCII digits | Optional AWS account ID expected to own the bucket. Sent with uploads, deletions and bucket checks; omitted from presigned downloads. |
| `SERVER_PORT` | string | `"8081"` | 1 to 65535 | TCP port on which the HTTP server listens. |
| `SERVICE_TOKEN` | string | `""` | No whitespace or control characters after trimming | Shared secret merged with `SERVICE_TOKENS`; see [Service Token Rotation](deployment.md#service-token-rotation). At least one token is required when `GO_RAG_FAKE_MODE=false`. |
| `SERVICE_TOKENS` | comma-separated strings | `""` | No whitespace or control characters within a token after trimming | Additional secrets accepted in `X-Service-Token` for rotation; see [Service Token Rotation](deployment.md#service-token-rotation). Tokens are trimmed, empty entries are dropped and duplicates are removed across both variables. Startup warns with only the count of tokens shorter than 32 bytes; use secrets of at least 32 bytes. An empty combined list leaves routes open only in offline mode. |
| `SHUTDOWN_DRAIN_DELAY` | duration | `5s` | 0s to 60s | Delay after readiness starts returning draining, before HTTP shutdown. A second termination signal skips the remaining delay. |
| `SYNTHESIS_CONTEXT_TOKENS` | integer | `2400` | 256 to 100000 | Maximum sum of estimated tokens in sanitized excerpt bodies. Generated answers also include prompt instructions, the question and markers. |
| `SYNTHESIS_INPUT_PROFILE` | string | `""` | `""` or `"strict"` | Enables [strict synthesis budgets](#strict-synthesis-budgets). Incompatible with `GO_RAG_FAKE_MODE=true`. |
| `SYNTHESIS_OPENAI_DEPLOYMENT` | string | `""` | `chatml-v1\|<model-revision>\|<base-address>` | Explicit deployment match for strict OpenAI-compatible synthesis. |
| `SYNTHESIS_MODEL` | string | `"fallback"` | `"primary"`, `"fallback"`, or `"hybrid"` | Bedrock model routing policy for query answer synthesis: `"fallback"` uses Llama 4 Maverick, `"primary"` uses Nova Micro, and `"hybrid"` dynamically selects between models based on query complexity. |
| `SYNTHESIS_RELATIVE_SCORE_FLOOR` | float64 | `0.55` | 0 to 1, finite | Minimum candidate score relative to the best retrieved score. |
| `SYNTHESIS_TIMEOUT` | duration | `45s` | Positive and below `QUERY_TIMEOUT` | Synthesis call budget, including SDK retries with Bedrock. |
| `CITATION_SUPPORT_MODEL` | string | empty | Immutable `name@sha256:<digest>` | Enables [semantic citation assessment](#citation-verifier) for requests that ask for citations. |
| `CITATION_SUPPORT_TIMEOUT` | duration | `2s` | Positive and below `QUERY_TIMEOUT` | Optional assessment budget within the query deadline. |
| `CITATION_SUPPORT_MAX_PAIRS` | int | `8` | 1 to 32 | Distinct claim and excerpt-body pairs per answer. |
| `CITATION_SUPPORT_ENDPOINT` | string | empty | Absolute verifier address | An empty value uses HTTP on `127.0.0.1:8091` at `/citation-support/v1`. An external address requires HTTPS and a bearer token. |
| `CITATION_SUPPORT_TOKEN` | secret | empty | Bearer token | Authenticates the verifier request. Optional for literal loopback. |
| `CITATION_SUPPORT_CALIBRATION_FILE` | path | empty | Local JSON file, up to 8 MiB | Loads the [held-out evaluation](development.md#citation-verifier-calibration) at startup. |
| `VECTOR_STORE` | string | `"pinecone"` | `pinecone` or `postgres` | Persistent vector backend; offline fake mode always uses memory. PostgreSQL vectors require the [deployment prerequisites](deployment.md#postgresql-vectors). |
| `WORKER_QUEUE_SIZE` | integer | `1000` | 1 to 100000 | Maximum outstanding persisted ingestion jobs across replicas, including pending, delayed and running jobs. |

---

## Embedding input profiles

Set `EMBEDDING_INPUT_PROFILE` to the path of a UTF-8 JSON file. Mount the profile
and any tokenizer artifact as read-only files accessible to the service. Startup
validates both files locally. An empty setting retains estimated chunking.
Profiles require a configured provider and are unavailable in offline fake mode.

Every profile declares `provider`, `model` and `revision`. The first two must match
the embedding provider and model settings. Use the provider's revision identifier
when one is available. For a compatible endpoint, bind the revision to the
deployment that serves the configured model ID and update the profile when the
deployment changes. Every declared allowance is a positive integer.
`max_bytes` measures UTF-8 text bytes and `max_characters` counts Unicode code
points. The service checks every declared allowance.

For Titan Text Embeddings V2, use the published character ceiling or a lower
allowance. Byte-only profiles still enforce the 50,000-character ceiling. The
model also has an 8,192-token window. This profile checks characters and
provides no exact token counts.

```json
{
  "provider": "bedrock",
  "model": "amazon.titan-embed-text-v2:0",
  "revision": "0",
  "max_characters": 50000
}
```

For the public OpenAI embeddings endpoint, `text-embedding-3-small`,
`text-embedding-3-large` and `text-embedding-ada-002` support exact counting with
`cl100k_base`. Use the published model ID as the revision for these IDs and set
`max_tokens` to at most 8192. Obtain the published `cl100k_base.tiktoken` vocabulary
out of band and specify its path, relative to the profile or absolute. Startup
checks its SHA-256 against the published digest. Exact mode rejects other
endpoints and model IDs. For a compatible server with documented character or
byte limits, declare those limits and omit `max_tokens` and the tokenizer.
Exact profiles include the version of the local Unicode 16 pre-tokenizer in
their fingerprint.

```json
{
  "provider": "openai",
  "model": "text-embedding-3-small",
  "revision": "text-embedding-3-small",
  "max_tokens": 8192,
  "tokenizer": {
    "type": "tiktoken",
    "version": "cl100k_base",
    "sha256": "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7",
    "artifact": "cl100k_base.tiktoken"
  }
}
```

Ingestion applies the profile before embedding or persisting chunks. It splits
normalized text at Unicode 17 grapheme boundaries, preferring nearby word
boundaries and omitting overlap when splitting to meet the profile. It can
revisit earlier boundaries when the first split leaves text that cannot fit.
Ingestion fails if no valid partition fits the limits and the 10,000-chunk cap,
or if the provider rejects an input for its model window. A model-window
rejection ends the attempt without rechunking from the provider response. See
[query errors](api.md#post-query) for the query response.

Profiled chunk IDs incorporate the chunking algorithm version, profile
fingerprint and chunk settings. Activate one profile across all replicas that
share an ingestion queue. Drain pending and in-flight work before changing
profile or rolling back so attempts use consistent settings. Existing completed
documents retain their stored chunks and IDs. To rebuild them, replay stored
document text into an isolated Quarry deployment with a fresh metadata database
and vector target. Supply each original document UUID, collection, document
type and caller metadata. The [document identity rules](api.md#document-identity)
explain when a submission resolves to an existing document instead of queuing
new work. Before cutover, check that replacement chunk IDs and vectors are
present and that queries with citations resolve to the rebuilt chunk indices.
Copying stored chunks to another vector backend preserves their geometry.

## Strict synthesis budgets

With `SYNTHESIS_INPUT_PROFILE=strict`, startup checks every reachable synthesis
model. With the setting empty, queries use the existing excerpt estimate.

For Bedrock, choose an exact model or inference profile ID and source Region
from the table. Startup sends one synthetic Converse-form `CountTokens`
request to the regional Bedrock Runtime endpoint for each distinct reachable
model. The request includes text guardrail blocks when configured. The
configured deployment must accept that shape and return a valid count before
the server starts. Custom runtime endpoints are unsupported. Strict mode
requires replacing the default fallback model. See [Bedrock IAM
permissions](deployment.md#aws-bedrock-models).

| Bedrock model or inference profile ID | Context window | Source Regions for startup validation | Inference routing |
|---|---:|---|---|
| `amazon.nova-micro-v1:0` | 128,000 | `us-east-1`, `eu-west-2`, `ap-southeast-2` | In-Region |
| `us.anthropic.claude-haiku-4-5-20251001-v1:0` | 200,000 | `us-east-1`, `us-east-2`, `us-west-1`, `us-west-2`, `ca-central-1` | Geographic cross-Region |

For OpenAI-compatible generation, set `SYNTHESIS_OPENAI_DEPLOYMENT` to
`chatml-v1|<model-revision>|<base-address>` using the exact revision and
`OPENAI_BASE_URL` with trailing slashes removed. Set this only for a deployment
whose immutable revision uses the stated tokenizer, window and ChatML v1 text
framing in both JSON and streaming requests. The deployment must encode roles
and contents as ordinary text, add three framing tokens per message and three
assistant priming tokens, and apply no additional input transformations or
instructions. Local counting uses the pre-tokenization and merge ranks of
tiktoken 0.14.0. Floating aliases and Bedrock guardrail settings are
unsupported in this configuration.

| Model revision | Tokenizer | Context window |
|---|---|---:|
| `gpt-4-0613` | `cl100k_base` | 8,192 |
| `gpt-4o-2024-08-06` | `o200k_base` | 128,000 |
| `gpt-4o-mini-2024-07-18` | `o200k_base` | 128,000 |

See [query stages](architecture.md#query-stages) for fitting and
[POST /query](api.md#post-query) for the response fields and statuses.

## No-evidence translation catalog

Leave `NO_EVIDENCE_CATALOG_FILE` empty to use the built-in translations. Point it to a local UTF-8 JSON object to add languages for [no-evidence answers](api.md#post-query). Keys are BCP 47 tags checked against the IANA Language Subtag Registry dated 2026-11-18, and values are operator-authored plain text. An extended-language subtag must follow its registered prefix, as in `zh-cmn`. Each tag is canonicalized with the registry's preferred values, so `zh-hnm` and `hnm`, or the deprecated `dek` and `sqm`, name one translation and one `Content-Language` value.

Unicode locale attributes, keyword groups and transformed-content fields are ordered canonically. Repeated Unicode locale attributes count once, and the second character of each Unicode locale keyword must be a letter. A keyword with the sole value `true` is equivalent to the keyword alone. Unicode locale keyword and transformed-content field values use CLDR 49 aliases. For example, `ar-u-ca-islamicc` uses `ar-u-ca-islamic-civil`, and `ar-t-m0-names` uses `ar-t-m0-prprname`. Private-use subtags retain their identifiers, with case normalized to lowercase.

```json
{
  "ar": "لم نجد معلومات ذات صلة للإجابة عن هذا السؤال.",
  "ja": "この質問に答えるための関連情報が見つかりませんでした。"
}
```

Use a regular file of up to 64 KiB with at most 128 additional translations. Canonical tags can contain up to 1,024 bytes and each decoded translation can contain up to 1 KiB of UTF-8 text. The server rejects invalid UTF-8 or JSON, invalid tags, duplicate canonical tags including built-in tags, and messages consisting solely of whitespace or invisible characters. It rejects control characters, bidi embeddings, overrides and isolates. Right-to-left text, standalone direction marks and script joining marks are preserved in valid messages.

The server reads the file once at startup. An unreadable or invalid catalog prevents startup. Deploy the same file on every replica and restart the server to apply changes.

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

## Citation Verifier

Serve the digest-pinned multilingual verifier within the deployment's authorized data boundary. The endpoint and token come from service configuration. The service sends a single POST with JSON and denies redirects. Configure egress rules for the allowed deployment-owned address.

The verifier protocol sends `model`, `pairs`, and `claims`. Each pair has a generated `claim` and the exact sanitized, possibly truncated excerpt `body` that reached synthesis. Each entry in `claims` is an array of zero-based pair indices for one claim and its complete evidence set. The batch contains at most 32 claim groups and occupies at most 32 KiB as encoded JSON. The request includes no question, document identifiers or other metadata. A joint set must fit as a whole.

Return HTTP 200 with the same digest-pinned `model` and a `results` array in claim order. Each result has a `claim_language` and an `evidence_languages` array in the referenced pair order. Use canonical BCP 47 tags and `und` when the language cannot be established. Supply `scores` with `supported`, `contradicted`, and `insufficient` probabilities that sum to one, or `null` to abstain. The response is capped at 32 KiB. Mixed source languages within a set abstain. Single excerpts and joint sets require their own calibration.

The service ignores verifier settings when the model is empty or `GO_RAG_FAKE_MODE` is enabled. With a model and no qualifying calibration profiles, citation support reports `unchecked` without a verifier request. A malformed configured file or model digest mismatch fails startup when verifier settings are active.

## MCP Server Environment

The local stdio command reads these variables separately from the HTTP service. Invalid settings prevent startup. See [MCP server usage](mcp.md).

| Variable | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `QUARRY_MCP_FILE_ROOTS` | path list | `""` (disabled) | Existing directories separated by the OS path-list separator | Allowed roots for file ingestion; roots and file paths are resolved through symlinks. Only regular files with `.pdf`, `.docx`, `.doc`, `.txt`, `.md` or `.markdown` extensions are accepted, case-insensitively. |
| `QUARRY_MCP_READ_ONLY` | boolean | `false` | Boolean | Registers only query, list and get tools. |
| `QUARRY_SERVICE_TOKEN` | string | `""` | No whitespace or control characters after trimming | Sent only in the `X-Service-Token` request header. |
| `QUARRY_SUBJECT_TOKEN_FILE` | path | `""` | Protected regular file | Optional source of the [stdio subject credential](mcp.md#subject-credential). |
| `QUARRY_TIMEOUT` | duration | `90s` | 1s to 10m | HTTP request timeout, including upload and response transfer. The default exceeds the service's default write timeout. |
| `QUARRY_URL` | string | Required | Absolute HTTP or HTTPS address, or `host:port` for plain HTTP; no credentials, query or fragment | HTTP API base address; any path prefix is preserved. |

## Schema Migrations

Database schema migrations are SQL files named with a three-digit version and a descriptive suffix, such as `006_add_index.sql`. Every file under `internal/store/postgres/migrations` matching `NNN_name.sql` is embedded in the binary automatically. Versions must be unique and are applied in ascending order by `internal/store/postgres/migrate.go`.

On startup, the runner acquires a PostgreSQL transaction-scoped advisory lock and applies missing versions in one transaction. The standalone cleanup command uses the same runner with a 30-second migration deadline before starting cleanup. The `schema_migrations` table records each version, filename, SHA-256 checksum and application timestamp. Concurrent replicas wait for the lock before checking this table. Subsequent startups verify checksums and skip applied SQL, avoiding table locks on document data when no migration is pending. Never edit an applied migration; add a new numbered file instead. A checksum mismatch stops startup. Existing databases without migration history apply the original idempotent files once and record them without removing stored documents.

Application connections default to `pool_max_conns=16` and `statement_timeout=30000` (milliseconds). Explicit settings in `DATABASE_URL` override these defaults, including `statement_timeout=0` to disable the timeout. Both URI query parameters and keyword connection strings are supported; for example, `host=localhost dbname=rag pool_max_conns=24 statement_timeout=10000` sets 24 connections and a 10-second statement timeout. The migration transaction disables statement and lock timeouts while waiting for the advisory lock, then uses a 15-second lock timeout when applying SQL. Transaction-local settings do not change application connection settings after commit or rollback.

When `sslcert` and `sslkey` point to files, keep both files readable after startup so new PostgreSQL connections can pick up rotated client certificates and keys during TLS handshakes.

| File | Description |
|---|---|
| `001_init.sql` | Enables the `pgcrypto` extension. Creates the `documents` table, the `chunks` table with foreign key cascading deletion (`ON DELETE CASCADE`), and the `document_fingerprints` deduplication table with indexing. |
| `002_add_file_storage.sql` | Adds nullable columns `file_name`, `file_type`, and `s3_key` to `documents` to track raw uploaded files stored in S3. |
| `003_add_content_preview.sql` | Adds the `content_preview` column to `documents`, backfills existing rows with the first 120 characters of `content`, and creates a functional index on `LOWER(content_preview)`. |
| `004_add_doc_type.sql` | Adds the `doc_type` column to `documents` and backfills values from `metadata->>'doc_type'`. |
| `005_document_default.sql` | Sets the default column value for `doc_type` in `documents` to `'document'`. |
| `006_ingest_jobs.sql` | Adds durable ingestion jobs, lease and failure tracking, completion timestamps and document fingerprints. Normalizes previously persisted processing rows with fingerprints to completed and requeues other processing rows. |
| `007_document_collections.sql` | Adds collection labels to documents and an index on `(collection, id)`; existing rows belong to the default collection. |
| `008_document_text_source.sql` | Adds `text_source` to documents with the default `parser`, including existing rows; see [image-only PDF OCR](api.md#image-only-pdf-ocr). |
| `009_content_dedup.sql` | Stores the upload's policy on the document referenced by each job, defaults existing rows to `collection`, and prevents private documents from registering shared fingerprints. |
| `010_document_deletions.sql` | Adds a nullable incarnation column with a random UUID default for future inserts, an empty deletion receipt table and a singleton with a null activation timestamp. It leaves existing rows in place. |
| `011_grant_catalog.sql` | Installs the [operator grant catalog](grant-catalog.md) with an empty policy at revision zero. |
| `012_index_visibility.sql` | Adds index visibility observations to documents and a lease token to ingestion jobs. |
| `013_vector_mutations.sql` | Records each document's vector format, published attempt and vector mutation state. |
| `015_mixed_pdf_ocr.sql` | Records page manifests, versioned extraction provenance, and coverage. Adds a lease constraint for jobs that require a mixed-PDF worker. |
| `017_evaluation_fixtures.sql` | Adds durable receipts and UUID reservations for the [opt-in evaluation fixtures](development.md#evidence-evaluation). |
| `018_evaluation_vector_identity.sql` | Binds new fixture receipts to their vector store and permits reservations for durably retired submissions. |
| `019_evaluation_fixture_guards.sql` | Moves the reservation checks used by ordinary admission, leasing and removal into owner-defined predicates, so a restricted serving login needs no access to the receipt table. |

The service can roll back to the previous binary with migration 010 and its
checksum still recorded. After applying migration 019, follow the
[serving-role upgrade procedure](grant-catalog.md#upgrade-an-existing-serving-role)
for restricted logins. For deployments with evaluation fixtures, follow the
[fixture upgrade and rollback order](development.md#evidence-evaluation).
See [document deletion](api.md#delete-documentsdoc_id) for fixture reservations.

This release keeps production admissions on the untagged format and leaves the activation marker null. Drain in-progress worker mutations before rolling back the binary. While production data uses only the untagged format, the additive migrations and their checksums can stay in place.

The optional vector schema is initialized separately after base migrations, under the same advisory lock; see the [vector data model](architecture.md#postgresql-vector-schema).

## Metrics

Set `METRICS_ADDR` to enable `GET /metrics` on a separate listener. The main API
listener does not serve metrics. See [Metrics](deployment.md#metrics) for listener exposure guidance.

Metrics use Prometheus text format:

- `quarry_http_requests_total{route,code}` counts HTTP responses. A [streamed query answer](api.md#streamed-query-answers) that fails after its first event keeps code `200`; the failure is logged as `query processing failed`.
- `quarry_http_request_duration_seconds{route}` observes request duration.
- `quarry_pipeline_stage_duration_seconds{stage}` observes executed ingestion and
  retrieval stages, including stages that return an error.
- `quarry_ingest_jobs_total{result}` counts successful `completed`, `retried`,
  `failed` and `released` transitions made by the ingestion processor. Rejected or
  failed state updates do not increment counters. Expired-lease cleanup performed
  by the claim query is outside these processor outcome counters.
- `quarry_ingest_jobs_pending` counts pending rows in `ingest_jobs`, including
  delayed retries and excluding running jobs. Concurrent scrapes share a 10-second
  cache per process, including failed refreshes. A failed refresh retains the last
  good value, or `NaN` initially. This gauge is shared across replicas through the
  database; do not sum it across replicas.
- `quarry_query_mode_total{mode}` counts each routed query once using its effective
  `LOCAL` or `GLOBAL` mode, including queries that subsequently fail synthesis.
- `quarry_hosted_visibility_refill_total{result}` counts each applicable outcome
  once per completed retrieval. `budget_exhausted` means the request allowance
  prevented an optional partition or retry attempt, `upstream_error` means an
  optional search failed without hitting that limit, and `underfill` means
  visibility filtering left fewer than the requested number of chunks after
  recovery. A retrieval may have multiple outcomes.
- `quarry_query_route_reason_total{policy,mode,reason}` counts each routed query using
  its active routing policy (`legacy` or `evidence`), effective mode, and bounded
  route reason (`empty_scope`, `explicit_global`, `explicit_local`, `probe_failed`,
  `empty_probe`, `missing_calibration`, `multi_need`, `ambiguous_entities`,
  `negated_entity`, `named_entity`, `ambiguous_margin`, `score_threshold`,
  `strong_lead`, `low_score`).
- `quarry_rerank_total{result}` counts optional reranking attempts: `reranked` for
  success and `fallback` for errors, invalid results or an expired `RERANK_TIMEOUT`.
  Disabled reranking, empty candidate lists and queries canceled or timed out during
  reranking are not counted.
- `quarry_index_visibility_waits_total{result}` counts started visibility waits.
  `visible` means the sample was found. `writes_indexed` and `unknown` are LSN
  probe outcomes. `timeout` counts expired waits and interrupted worker shutdowns.
  The `persist` stage duration includes the wait.
- `quarry_pgvector_search_total{strategy,status}` counts PostgreSQL vector
  searches by `strategy` (`exact`, `hnsw`, `probe`) and `status` (`ok`,
  `partial`, `timeout`, `canceled`, `error`). On `hnsw`, `partial` indicates fewer
  results than `topK`. Explicit caller cancellation is recorded as `canceled`.
  A PostgreSQL cancellation with an active caller is recorded according to the
  [query timeout policy](api.md#post-query).

The `route` label holds the matched method and path pattern, or `unmatched` for
requests with no matching pattern. All durations are in seconds. Histograms use
boundaries 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60 and 120
seconds, plus the cumulative positive-infinity bucket. Counters and histograms
describe the current process and reset on restart.
