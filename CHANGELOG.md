# Changelog

Notable changes are grouped by release.

## [Unreleased]

### Added

- Vector reindex supports tagged documents in both directions between Pinecone and PostgreSQL, including retrieval and checkpointed deletion after PostgreSQL cutover.

## [0.2.0] - 2026-11-26

### Upgrade from 0.1.0

1. As the schema owner, apply the pending PostgreSQL migrations `009` through
   `030` in order. Apply the [serving-role upgrade grants](docs/grant-catalog.md#upgrade-an-existing-serving-role)
   before starting upgraded processes. For hosted and offline vector stores,
   grant restricted serving logins execution of `document_deletion_active_shared`
   after migration 030 and before starting upgraded processes in either
   authorization mode. Admission that may select the tagged vector format uses
   the function even while checkpointed deletion is inactive.
2. Prepare callers to save each document's incarnation with
   `include_incarnation=true`. Pause mutations and retire pre-upgrade
   executables. Deploy 0.2.0 with the deletion marker unset. Settle or quarantine
   outstanding hosted effects by UUID, index and namespace before activation.
3. As the schema owner, run `make cleanup IDS=--activate-deletion`. Verify the
   mode on every replica and switch callers before resuming writes. After
   activation, retain the marker and deploy only compatible builds. See the
   [checkpointed deletion upgrade order](docs/deployment.md#upgrade-order).

If changing embedding models or vector stores, finish upgrading all replicas
and workers before running `make reindex`. Stage the target, freeze writers and
wait for quiescence, then catch up, verify and activate the generation. Deploy
all replicas sharing the queue with the new generation and matching settings
before thawing writers. See [changing models or vector stores](docs/deployment.md#changing-models-or-vector-stores).

### Added

- Opt-in `make reconcile ARGS=-superseded-unrevisioned` finds and removes bare Pinecone vectors superseded by an active hosted manifest, with a durable cleanup journal protecting the writer freeze during apply.
- `make reconcile ARGS=-canonical-document-ids` rewrites hosted vector document IDs written in another UUID spelling, such as upper case, so allowlisted queries, deletions and backfills match them.
- Public `AUTHZ_MODE=service|grants` configuration switch (default `service`) activating per-subject tenant and document grant enforcement across HTTP data routes, `cmd/server`, the worker, Go client, and stdio MCP (`QUARRY_SUBJECT_TOKEN_FILE`).
- Existing restricted serving roles must receive EXECUTE on `grant_catalog_corpus_authz_mode()` after migration 029 and before upgraded processes start; see the serving-role upgrade procedure.
- Fenced corpus cutover in migration `029_authz_corpus_mode.sql` and `scripts/grant-catalog -activate-grants` (with `-dry-run`), enforcing corpus-mode parity on `/readyz`, HTTP routes, and database gates (`ErrAuthzModeMismatch`) while keeping unassigned historical documents quarantined and invisible in `grants` mode.
- Restricted PostgreSQL security audit table `grant_catalog_security_audit` (`SELECT`-only for `quarry_auditor`), mirroring policy catalog audit events and failing closed on missing trust audit or query/list/status audit writes.
- Benchmarks comparing `service` and `grants` authorization paths across scopes `0`, `1`, `10,000`, and `25,000` (`BenchmarkQueryHandlerAuthzModes`, `BenchmarkPGVectorAuthzModes`, and opt-in live `BenchmarkPineconeAuthzModes`).

- `make cleanup` lists held deletion receipts, quarantines absent UUIDs and releases verified holds, and `Client.ReleaseHeldOfflineDeletion` releases a hold on the offline service; see [quarantine and hold release](docs/deployment.md#quarantine-and-hold-release).
- Qualified document deletion for callers: `Client.DeleteDocumentIncarnation`, `UUID@incarnation` arguments for `make cleanup` and an optional `incarnation` for `quarry_delete_document`. An empty or malformed incarnation is rejected and never becomes an ID-only request.
- Legacy deletion reservations commit a durable receipt before ID-only cleanup mutates a vector store, so activating checkpointed deletion or losing a database session cannot let a replacement overtake the last call. The server and `make cleanup` enable them; see [legacy deletion reservations](docs/deployment.md#legacy-deletion-reservations).
- `make cleanup IDS="--activate-deletion"` is the only operation that activates checkpointed deletion. It is exclusive, waits for healthy ID-only cleanups, sets the database marker once and reports the recorded instant when repeated; see [activating checkpointed deletion](docs/deployment.md#activate-checkpointed-deletion).
- `GET /deletion-mode` and `Client.GetDeletionMode` report the mode recorded in the service's own database for operators and smoke checks. They only read; see [GET /deletion-mode](docs/api.md#get-deletion-mode).
- Queries with `include_citations` can set `preserve_code` to keep Markdown code unchanged and exclude bracketed numbers in code from citations, including through the Go client and MCP query tool.
- Opt-in revision-fenced hosted vector manifests (`HOSTED_VECTOR_MANIFEST`, disabled by default), durable write-intent and cleanup tombstone tables (`025_hosted_vector_manifest.sql`), lease-fenced revision activation, reader revision visibility gating, `quarry_hosted_vector_cleanups_total` metrics, and a leased background and `scripts/reconcile` cleanup sweeper (`HOSTED_VECTOR_SWEEP_*`, `HOSTED_VECTOR_DRAIN_DELAY`, `HOSTED_VECTOR_CONVERGENCE_DELAY`). Restricted serving logins need execution on `hosted_vector_revision_visible` after the migration, and only replicas that enable the flag need access to the new tables; see the [serving-role upgrade procedure](docs/grant-catalog.md#upgrade-an-existing-serving-role).
- A PostgreSQL grant catalog and operator import for staging tenant-bound document ownership and direct grants.
- Optional [Word binary stories](docs/api.md#word-binary-extraction) for `.doc` uploads.
- Optional [strict synthesis budgeting](docs/configuration.md#strict-synthesis-budgets) measures complete model input, reserves answer space and reports the measured input count.
- `DOCX_STORIES` lets deployments opt in to indexing DOCX headers, footers and comments.
- An internal owned ingestion path binds private documents, jobs, original objects and vectors to a verified tenant, owner and document incarnation, and a scoped backfill verifies historical assignments before they become readable. See [grant catalog](docs/grant-catalog.md#owned-ingestion).
- An internal authorized retrieval path confines every search, routing preview, model input and response to the verified principal's current grants on pgvector and Pinecone, and rechecks them before each disclosure, including server-sent frames (migration 026). Both paths are available only through internal entry points. See [grant catalog](docs/grant-catalog.md#authorized-retrieval).
- Internal scoped handlers authorize document inventory, status, assertion-bounded original-file signing and owner-only checkpointed deletion against the grant catalog. No public route enables them. See [grant catalog](docs/grant-catalog.md#scoped-document-operations).
- Staged delegated access-token verification with request-scoped subject credentials for the Go client and stdio MCP.
- Document status and inventory can include the ingestion-time index visibility observation and timestamp on request, with opt-in Pinecone serverless watermark checks.

- Opt-in evidence-based automatic retrieval routing (`ROUTING_POLICY=evidence`, `ROUTING_MARGIN`, `ROUTING_PROFILE_FILE`) using visible per-document score aggregation, entity ambiguity and exclusion matching, calibrated margins, and bounded `quarry_query_route_reason_total` metrics.
- `INGEST_CONTENT_DEDUP=off` lets deployments disable content matching across upload IDs and retain explicit UUID retries.
- Optional OCR of image-only PDF uploads through Amazon Textract (`OCR_PROVIDER=textract`, requires `S3_BUCKET`; migration 008).
- Opt-in page-aware OCR recovers scanned pages in mixed PDFs, including baseline, extended and progressive JPEG scans and ICC-based color spaces. Request recorded extraction coverage in document summaries with `include_pdf_coverage=true`.
- Opt-in Textract PDF layout and table extraction (`PDF_LAYOUT_MODE=textract`) preserving reading order, labeled table rows with merged headers, and document-scoped chunk provenance (`pages`, `table_ids`, `row_ids`).
- Optional PostgreSQL vector store (`VECTOR_STORE=postgres`, pgvector) that writes vectors in the completion transaction.
- The README shows release and Go reference badges and describes installing released artifacts.
- Queries report which retrieved chunks reached synthesis (`excerpt`, `truncated`) and, with `include_citations`, return citations checked against their excerpts.
- Opt-in semantic citation support reports calibrated claim assessments, cited evidence sets and citation coverage in generated query results.
- Queries and document listings accept a `collection` filter applied inside vector search; `make reconcile ARGS=-backfill-collections` labels vectors written before this release.
- `docs/openapi.yaml` describes the HTTP API in OpenAPI 3.1, and a test keeps it in sync with the router and handler types.
- Optional Bedrock Guardrails for answer synthesis (`BEDROCK_GUARDRAIL_ID`, `BEDROCK_GUARDRAIL_VERSION`), with the question and excerpts marked for prompt-attack and grounding checks.
- `POST /query` streams answers as server-sent events when the request accepts `text/event-stream`.
- Optional Bedrock reranking of retrieved chunks before synthesis (`RERANK_MODEL_ARN`).
- OpenAI-compatible chat and embedding providers (`LLM_PROVIDER`, `EMBEDDING_PROVIDER`, `OPENAI_BASE_URL`) for local or self-hosted models.
- Evidence-only queries return scoped excerpts without answer generation over JSON, SSE, the Go client and MCP.
- Pinecone queries can opt into bounded recovery of live chunks displaced by stale index hits.
- Optional source upload journal (`SOURCE_UPLOAD_JOURNAL=true`, migration 014) that records each original file's owner in PostgreSQL before it is written to S3, and `make reconcile ARGS="-source-uploads"`, which inventories and, with `-apply`, reclaims originals that no document adopted, including versions in versioned buckets, while preserving held versions and referenced objects.
- `make reindex` moves completed documents to a new vector target without changing document or chunk IDs: it inventories read-only by default, stages into a separate PostgreSQL table or Pinecone index with durable per-document checkpoints, reuses vectors only when the source target and profile are proven and recomputes the rest within a declared call budget, verifies key, scope, recall and latency criteria, and activates a generation under a writer freeze.
- `VECTOR_GENERATION` selects the vector target recorded for a migration and pins every replica of a shared ingestion queue to the active generation and its recorded profile (migration 016). While writers are frozen for a migration, ingestion and deletion return 503 `frozen`, and `make cleanup` and `make reconcile -apply` stop.
- Authenticated document status and inventory can include stable incarnations on request. [Document deletion](docs/api.md#delete-documentsdoc_id) returns `412` for reserved selectors and `400` for malformed queries.
- Optional embedding input profiles enforce model-specific allowances, split overlong ingestion chunks and reject overlong queries before embedding.
- Optional exact cosine search for small filtered pgvector scopes (`PGVECTOR_EXACT_SCOPE_MAX`, disabled by default) with bounded single-snapshot probing and `quarry_pgvector_search_total` metrics.
- The [evidence evaluation](docs/development.md#evidence-evaluation) replays a labeled fixture locally and compares scoped retrieval, answer support and index visibility against opted-in staging deployments with receipt-qualified fixture cleanup.
- Deployments can add fixed no-evidence translations through a [local catalog](docs/configuration.md#no-evidence-translation-catalog).
- Optional diversity-aware synthesis context selection (`SYNTHESIS_CONTEXT_POLICY=diverse`, defaulting to `ranked`) to reduce repeated passages and pack complementary evidence within `SYNTHESIS_CONTEXT_TOKENS`.

### Changed

- Guarded ID-only deletions, which legacy deletion reservations introduce, run on a PostgreSQL session outside the pool, so slow provider calls no longer hold pooled connections. A replica runs at most `LEGACY_DELETION_CONCURRENCY` of them (default a quarter of `pool_max_conns`, at least one), answers further requests with `503` `busy` and `Retry-After: 5` before it takes a lock or a connection, and the [connection sizing formula](docs/deployment.md#concurrency-limits) counts the extra sessions.
- **Breaking after activation.** While the database is inactive, bare `DELETE`, `make cleanup <uuid>` and the MCP tool keep addressing the live UUID. After an operator runs `make cleanup IDS="--activate-deletion"`, deletion of a live document requires its saved incarnation (`428` without it and `412` for the wrong lifetime). Subsequent admissions record incarnation-tagged vectors where supported. See [activating checkpointed deletion](docs/deployment.md#activating-checkpointed-deletion).
- Smoke tests that write documents require an activated target, check it through `GET /deletion-mode` before their first ingest, never activate it and remove only their own documents with a qualified request through the service; see the [staging procedure](docs/deployment.md#smoke-testing-a-staging-deployment).
- `make reindex` freeze, activation and inventory reports now include up to 20 document IDs of open deletion receipts blocking quiescence, and the [deployment guide](docs/deployment.md#changing-models-or-vector-stores) documents how to thaw, complete qualified retries and re-freeze.
- Deployment and grant-catalog documentation specifies the PostgreSQL privileges required by a non-owner `make reindex` maintenance login, including `EXECUTE` on `grant_catalog_document_tenant`, `ingest_jobs`, `evaluation_fixtures`, `document_pdf_extractions`, and pre-created or self-created generation vector tables.
- `DELETE /documents/{doc_id}` accepts `release_held=true` for offline hold release and rejects empty, repeated, malformed or non-literal values with `400`; see [document deletion](docs/api.md#delete-documentsdoc_id).
- Release archives are checked for platform coverage and package contents before publication. See [release packaging](docs/development.md#releases).
- PostgreSQL vector queries handle server query timeouts consistently across locales.
- `make cleanup` with `GO_RAG_FAKE_MODE` forwards each deletion once to the running service on `SERVER_PORT`, because that service owns the in-memory vectors.
- Local and scheduled fuzzing discover all active targets, and CI exercises them nightly.
- The file download API and Go client describe `expires_in` as the requested lifetime and explain why access can end earlier.
- The no-evidence answer follows `Accept-Language` for English, Spanish, Portuguese, French, German and Italian and sets `Content-Language`.
- Documentation keeps each operational fact in one document and links to it elsewhere.
- Synthesis context is selected by `SYNTHESIS_CONTEXT_TOKENS` estimated tokens and `SYNTHESIS_RELATIVE_SCORE_FLOOR` instead of at most 5 excerpts (8 for multi-part questions) cut near 1,200 characters.
- Synthesis excerpt selection now drops empty or control-only bodies before choosing the leading passage in every mode (returning `no_evidence` with zero synthesis calls, `chunks_used: 0` and empty `scored_chunks` when none remain), skips later candidates with `NaN` scores, stops adding zero-token fragments once the token budget is spent and caps output at 100 excerpts.
- Deployment documentation includes PostgreSQL connections in the cross-replica concurrency limits.
- Documentation diagrams animate one step at a time at a slower pace, keep each highlight lit before it fades, and pause briefly before repeating.
- The runtime image uses Alpine 3.24.4.
- Multi-platform image builds compile each target architecture on the build host instead of under emulation, and CI checks the executable, user, entry point and notices of both published platforms.
- AWS retries distinguish throttling from transient errors and honor service-provided retry delays.
- Textract OCR uses Smithy RPC v2 CBOR by default.
- PostgreSQL connections reload file-backed client certificates and keys on TLS certificate requests.

### Fixed

- Grants replicas wait for a successful trust-file audit before accepting newly loaded or rotated keys.
- Mixed PDF JPEG inspection skips bounded runs of extraneous non-marker bytes between segments (up to 64 bytes per gap and 256 bytes per image) instead of rejecting valid scans as `unparseable`.
- `preserve_code` citation cleanup tracks fence-like openers indented four or more spaces until their matching closing line and closes two- or three-space list-item fences across four- to six-space closing lines, preventing nested fence straddles from hiding later citations.
- Document deletion can checkpoint each external step under a durable, incarnation-qualified receipt before it removes the document, so a retry resumes the same lifetime instead of returning `404` or reaching a replacement. A database uses [checkpointed deletion](docs/api.md#checkpointed-deletion) only after explicit activation. Migration `023_deletion_predicates.sql` adds owner-defined predicates, and restricted serving logins need the [upgrade grants](docs/grant-catalog.md#upgrade-an-existing-serving-role) before they run upgraded processes.
- Ingestion checks the current document incarnation and lease before vector mutations. Queries validate the document lifetime before using search hits for routing or evidence.
- Failed-document retries respect the persisted ingestion queue limit.
- The API reference distinguishes JSON responses from streamed query answers and explains how callers recognize stream completion or interruption.
- Deployment prerequisites now follow the selected vector store, model providers and optional AWS integrations. Offline setup documents the database, embedding dimensions and multipart parser behavior.
- The ingestion and query sequence diagrams in the architecture documentation render again.
- The architecture documentation lists the tracing middleware in the request middleware order.
- Every Dockerfile base image is pinned by digest in the `FROM` line that uses it, so dependency scanners recognize each pin. The resolved images are unchanged.
- **Breaking:** Startup rejects `CHUNK_OVERLAP` above `CHUNK_TOKEN_SIZE/2`, which was previously accepted, and chunk output now grows linearly with document size. Chunks keep the source spacing between sentences instead of adding a space after full-width terminators such as `。`.

### Security

- In `AUTHZ_MODE=grants`, access logs log route templates rather than raw path identifiers, OpenTelemetry tracing forces content capture off and omits `document_id`, object storage errors/debug logs omit `s3_key`, and startup rejects incompatible debug/unsafe combinations (`GO_RAG_FAKE_MODE=true`, empty `SERVICE_TOKEN`/`SERVICE_TOKENS`, invalid JWKS/issuer/audience, `INGEST_CONTENT_DEDUP!=off`, tracing message content capture, or evaluation fixture prefixes).

- Build Quarry with Go 1.27.4 to fix GO-2026-6859 in the HTTP server.
- Update pgx to 5.13.0, which fixes GHSA-w9rp-vfrh-q8vv in row decoding.

## [0.1.0] - 2026-11-09

### Added

- OpenSSF Scorecard analysis publishes results for `main`, and the README shows CI, CodeQL, Scorecard, Go version and license badges with requirements, usage, configuration, operations, contributing and security sections.
- Ingestion accepts an optional `collection` that scopes content deduplication; document summaries report it (migration 007).
- Tagged releases publish archives for each command with checksums, third-party licenses and signed build provenance, and attest the runtime image; base images are pinned by digest.
- Optional OpenTelemetry trace export over OTLP/HTTP, including LangSmith, for requests, pipeline stages, model calls and storage operations; disabled by default and without message content unless enabled.
- `quarry-mcp`, a local stdio Model Context Protocol server for ingestion, queries and document management, and the `client` package, a typed Go client for the HTTP API.
- Fuzz tests cover format detection, document extraction, chunking, prompt sanitization, prompt markers and JSON request decoding, and run weekly in CI.
- Optional Prometheus-format metrics on `METRICS_ADDR` for requests, pipeline stages, ingestion jobs and routing modes.
- `make reconcile` lists, and with `ARGS=-apply` removes, vectors and legacy namespaces that no stored document references.
- Requests carry an `X-Request-ID`, accepted from callers or generated, that appears in structured access and panic logs; `LOG_LEVEL` sets verbosity.
- Multipart uploads accept UTF-8 and UTF-16 plain text (`.txt`, `.md`).
- `SERVICE_TOKENS` accepts several comma-separated tokens for rotation without downtime; tokens are trimmed and compared by digest in constant time.
- `GET /documents/{doc_id}` reports ingestion status, timestamps and the last error; `GET /documents` accepts `limit` and `after` and reports `status`, `chunk_count` and `created_at`.
- Use the AWS default credential chain when static keys are not configured, with
  optional `AWS_SESSION_TOKEN`.
- Multipart uploads accept a `metadata` form field.
- `GET /livez` and `GET /readyz`, with readiness failing during a configurable shutdown drain delay.
- JSON and multipart document ingestion with content deduplication, asynchronous
  processing and caller-supplied document identifiers.
- Text extraction, normalization, semantic chunking and chunk quality metadata.
- Local and global retrieval with document type filters, scored evidence and
  context-based answer generation.
- Document reference listing, deletion and temporary original-file downloads.
- Local offline mode, health probes and environment-based configuration.
- Automated checks, dependency updates, security analysis and version-tagged
  multi-architecture runtime images.

### Changed

- Ingestion waits up to `INDEX_VISIBILITY_TIMEOUT` (30 s by default) for new vectors to become searchable before marking a document completed.
- **Breaking:** `PINECONE_INDEX_NAME` has no implicit default and is required when external services are enabled; missing values prevent startup. Offline mode does not require it.
- The architecture, data model and repository layout diagrams are animated vector images with sequential flow highlights and reduced-motion support.
- Examples use the index name `quarry-rag`.
- `PINECONE_ENVIRONMENT` is ignored, is no longer stored in configuration, and emits a startup warning when non-empty.
- Logs use leveled JSON records with structured attributes; unused routing code was removed.
- The offline vector store rejects records the hosted index would reject (metadata above 40 KB, unsupported metadata types, mismatched dimensions).
- Use Pinecone API version 2025-10 through the v5 Go SDK; namespace operations share one connection.
- Uploads are identified by content as well as extension; mismatches return 415; extraction errors return fixed messages with a `code` field.
- LOCAL retrieval draws evidence from the routed document (named entity or best match) and tops up from the authorized scope when it has fewer than 3 matching chunks; explicit `global` skips the routing probe.
- Query failures return 504, 503 (with `Retry-After`) or 502 with generic messages; responses report `finish_reason`; HTTP server timeouts are configurable.

- AWS calls share one retry policy (`AWS_MAX_ATTEMPTS`, no client-side retry
  quota, `ModelTimeoutException` retried); embedding calls time out after
  `EMBED_TIMEOUT`.
- JSON requests with unknown fields or duplicate top-level keys return 400; labels are limited to 128 bytes without control or format characters; metadata is limited to 64 KiB and depth 8; `max_chunks` is capped by `MAX_CHUNKS_PER_QUERY`.
- `WORKER_QUEUE_SIZE` limits pending jobs (default 1000); identical in-flight submissions return the existing identifier with 202; resubmitting the content of a failed document retries it; source file upload failures return 503.

- Content deduplication applies only to requests without `document_id`;
  fingerprint lookup failures return 503.
- `allowed_document_ids` entries must be UUIDs (at most
  `MAX_ALLOWED_DOCUMENT_IDS`); an empty list returns the no-information answer
  without embedding or search; GLOBAL retrieval issues a single vector query.

- New documents are indexed only in the default vector namespace; per-document
  namespaces created by earlier versions are still removed on deletion.
- Database connections default to 16 pooled connections and a 30-second statement
  timeout unless `DATABASE_URL` sets them.
- Deleting a document with a stored file returns 503 when file storage is not
  configured; deletion continues after the caller disconnects; presigned
  downloads are served as attachments.
- Startup fails when the vector index dimension, metric or readiness does not
  match the configuration.
- Document labels default to `document` when omitted and are never inferred from
  content; migration 005 changes the database default without relabelling rows.
- Evidence-backed answers follow the question's language. Empty authorized
  results return a fixed English no-information answer without synthesis and
  with zero synthesis token usage.
- Blank content or queries, unknown retrieval modes, `max_chunks` outside 0–100,
  and trailing JSON values return 400.
- JSON bodies over 8,097,152 bytes for ingestion or 1 MiB for queries return 413.
  Ingestion retains its 1,000,000-byte decoded-content limit; the body allowance
  includes worst-case JSON escaping and metadata overhead.
- Extraction-limit failures return 422, including PDF text above 1,000,000 bytes,
  PDF work-budget exhaustion and DOCX document XML above 16 MiB.
- Document lookup and external deletion failures return 500 rather than a false
  missing-document or success result; already-missing vector namespaces are
  accepted on retries.
- Response snippets and document previews truncate at UTF-8 character boundaries.
- Startup rejects malformed numbers and booleans, unsupported synthesis policies
  and embedding dimensions, and values outside documented configuration bounds.
- Automatic retrieval falls back to GLOBAL if its similarity probe fails.
- The runtime uses numeric UID/GID 10001; local validation targets run on the host,
  and the Docker test stage runs the short suite without a Docker daemon.
- The deletion command requires explicit document UUIDs, accepts them through
  `make cleanup IDS="<uuid> ..."`, and reuses connections across documents.

### Deprecated

- The `neon` field of `GET /health`; read `postgres`.

### Fixed

- DOCX extraction keeps text around text boxes, avoids duplicated alternate content, supports strict Open XML and includes footnotes and endnotes.
- Panicking requests appear in the access log with status 500 when no response has started; aborted handlers are not reported as errors.
- Bound chunks by estimated tokens and 30,000 bytes, splitting unspaced scripts, long tokens and long sentences; stop emitting chunks that repeat only the previous chunk's overlap.
- Require `CHUNK_TOKEN_SIZE` to be at least 2, the minimum estimate for a non-dense character.
- Bound each query by `QUERY_TIMEOUT` and its synthesis call by `SYNTHESIS_TIMEOUT`, limit concurrent queries, and retry upstream calls in a single layer.

- `GET /documents` returns an empty array instead of `null` and streams large listings.
- Preserve superscripts, fractions, circled numbers and text between comparison operators during ingestion; keep carriage-return paragraph breaks; strip HTML only from documents that contain markup.
- Reject embeddings whose length differs from `EMBEDDING_DIMENSIONS`.
- Extract legacy `.doc` text through the document piece table, keeping accented and non-Latin characters and dropping field codes and binary noise; encrypted and pre-Word 97 files return 422.
- Retain source files for caller-owned documents with identical text and after uncertain acceptance results; bound acceptance independently of caller cancellation.
- Clear existing vectors before each ingestion attempt and again after document deletion; perform terminal cleanup under the live lease before recording failure.
- Reject NUL characters in document labels before storage.

- Persist accepted documents and their ingestion jobs in PostgreSQL before responding; jobs survive restarts, are retried with backoff under leases, and failures are recorded with `last_error`.
- Remove NUL characters from document content and reject them in metadata before storage.
- Embed chunks concurrently and process documents in parallel (`INGEST_WORKERS`).

- Keep caller-supplied document identifiers: identical resubmissions return the
  same identifier, conflicting content returns 409, malformed identifiers return
  400, and existing documents are never overwritten.
- Restricted queries no longer lose authorized evidence to documents outside
  the allowlist.

- Apply each schema migration once under a PostgreSQL advisory lock and record it
  in `schema_migrations`, so concurrent replicas start safely and restarts take
  no table locks.
- Write vectors in batches of at most 100 records by default and 1.5 MiB with
  retries, so documents near the 1,000,000-byte limit no longer exceed the vector
  index request limit; reuse one index connection; fall back to identifier-based
  deletion when metadata-filter deletion is rejected.
- Wait for active requests and current ingestion attempts before closing stores;
  release unfinished leases at the shutdown deadline.
- Persist failure status with a bounded independent context after cancellation.
- Roll back failed chunk batches, refresh conflicting chunks, propagate metadata
  errors and isolate entity caches by database connection pool.
- Preserve DOCX text across formatting runs, bound PDF stream expansion, and
  retain chunk quality metadata without sharing mutable caller maps.
- Remove multipart temporary files and source uploads rejected by a full queue.
- Defer live-test deletion until asynchronous ingestion has stored the document.
- Report panic responses as JSON and log the status actually written.

### Security

- Upgrade `github.com/klauspost/compress` for GO-2026-6865, `golang.org/x/net` for GO-2026-6866 and GO-2026-6860, and `golang.org/x/crypto` for GO-2026-5932.
- Wrap retrieved excerpts in per-request markers, place the question before them, and strip invisible characters from stored chunks before synthesis.
- Stop logging query text; logs record its length and digest.

- Remove invisible Unicode format characters, such as tag characters and bidirectional isolates, from ingested text.
- Cache dependency checks behind `GET /health` so unauthenticated requests cannot multiply calls to the vector index, database or file storage.
- Apply document allowlists and type filters inside every vector query, including
  the routing probe, and derive entity routing only from authorized candidates.

- Remove the implicit `S3_BUCKET` default, verify bucket access and an optional
  expected owner at startup, and send the expected owner on storage writes.
- Exclude chunks of deleted, failed and unfinished documents from query results
  and answer generation.
- Reject DOCX, DOC and text extractions above 1,000,000 bytes and bound legacy DOC
  stream reads to the uploaded file size.
- Parse uploaded PDF, DOCX and DOC files in a separate process with time, memory
  and concurrency limits, so a malformed file cannot stop the service.
- Service token authentication for document and query routes, with startup checks
  that reject missing tokens outside offline mode.
- Document allowlists applied before answer generation for restricted queries.
- Upgrade affected dependencies to remove reachable vulnerability findings and
  enforce header, idle and request timeout boundaries.
