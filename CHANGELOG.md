# Changelog

Notable changes are grouped by release.

## [Unreleased]

### Added

- The README shows release and Go reference badges and describes installing released artifacts.
- Queries report which retrieved chunks reached synthesis (`excerpt`, `truncated`) and, with `include_citations`, return citations checked against their excerpts.
- Queries and document listings accept a `collection` filter applied inside vector search; `make reconcile ARGS=-backfill-collections` labels vectors written before this release.

### Changed

- The no-evidence answer follows `Accept-Language` for English, Spanish, Portuguese, French, German and Italian and sets `Content-Language`.
- Documentation keeps each operational fact in one document and links to it elsewhere.

### Fixed

- **Breaking:** Startup rejects `CHUNK_OVERLAP` above `CHUNK_TOKEN_SIZE/2`, which was previously accepted, and chunk output now grows linearly with document size. Chunks keep the source spacing between sentences instead of adding a space after full-width terminators such as `。`.

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
