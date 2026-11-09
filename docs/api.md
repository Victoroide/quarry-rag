# API Reference

The [OpenAPI 3.1 description](openapi.yaml) provides machine-readable routes and
payload schemas. This reference owns endpoint behavior and limits.

All API responses use `Content-Type: application/json`. Error responses follow a uniform structured JSON format:

```json
{
  "error": "<error_category>",
  "message": "<descriptive_message>"
}
```

File content verification and extraction errors also include a stable `code`
field. Their messages are fixed; parser details are recorded in service logs.

JSON request bodies must be objects. Unknown request fields and duplicate
top-level keys return 400 with `unknown field "<name>"` or
`duplicate field "<name>"`. Trailing data after the object also returns 400.

Document labels (`doc_type`, `filter_doc_type` and `collection`) must be valid
UTF-8, are trimmed and normalized to Unicode NFC, limited to 128 UTF-8 bytes,
and must contain no control or Unicode format characters, including NUL. An
empty `doc_type` uses the default `document`; an empty query filter imposes no
document type restriction. On ingestion, an omitted or empty `collection` selects
the default collection (`""`); on queries and listings it imposes no collection
restriction.

Metadata is stored only in PostgreSQL. Its serialized JSON representation is
limited to 65,536 bytes (64 KiB), including JSON escaping, with at most eight
nested object or array levels counting the root object as level one. Keys and
string values must contain no NUL characters. Arbitrary metadata keys are
accepted; they do not become vector search filters.

## Request IDs

Send an optional `X-Request-ID` header to correlate a request with its logs. The
service accepts 1 to 128 ASCII letters, digits, periods, underscores, colons or
hyphens (`^[A-Za-z0-9._:-]{1,128}$`). Missing or invalid values are replaced with
a generated UUIDv7. Every response echoes the accepted or generated value in
`X-Request-ID`, including authentication errors and probe responses. Access logs
and recovered panic logs include the same value as `request_id`.

## Authentication

When `SERVICE_TOKEN` or `SERVICE_TOKENS` is configured, all mutating and sensitive endpoints require the `X-Service-Token` HTTP header matching any configured token:

- `POST /ingest`
- `POST /query`
- `GET /documents`
- `GET /documents/{doc_id}`
- `GET /documents/{doc_id}/file`
- `DELETE /documents/{doc_id}`

Requests missing or presenting an empty or incorrect token receive `401 Unauthorized`. Each request hashes the header with SHA-256 and compares it against every configured digest in constant time, without stopping at a match. An empty combined token list is allowed only in offline mode and leaves service routes open. The `GET /livez`, `GET /readyz` and `GET /health` endpoints are public and do not require authentication. To change tokens without downtime, follow [Service Token Rotation](deployment.md#service-token-rotation).

---

## Endpoints

### GET /livez

Returns HTTP 200 with `{"status":"ok"}` while the HTTP server is serving requests, including during the shutdown drain delay. It makes no dependency calls. Use this endpoint for startup and liveness checks.

### GET /readyz

Returns HTTP 200 with `{"status":"ready"}` when the cached PostgreSQL check succeeds. Returns HTTP 503 with `{"status":"unavailable"}` when PostgreSQL is unreachable, or `{"status":"draining"}` during shutdown. Vector index and file storage failures do not fail readiness. Use this endpoint for readiness checks.

### GET /health

Returns cached diagnostic status for backing data stores. Results are shared with readiness and may be up to [`HEALTH_CACHE_TTL`](configuration.md#environment-variables) old. On expiry, concurrent callers share one refresh, with checks running concurrently under separate 5-second timeouts. Use this endpoint for diagnostics.

- **Method**: `GET`
- **Path**: `/health`
- **Auth**: None (public)
- **Request Body**: None

#### Responses

- **200 OK**: All dependency probes responded within the 5-second timeout.
- **503 Service Unavailable**: One or more backing stores failed to respond within the timeout.

Response payload fields:

| Field | Type | Description |
|---|---|---|
| `status` | string | `"ok"` if all dependencies respond, or `"degraded"` if any probe fails |
| `pinecone` | string | `"ok"` or `"unreachable"` |
| `postgres` | string | `"ok"` or `"unreachable"` |
| `neon` | string | Deprecated alias of `postgres`, retained for compatibility |
| `s3` | string | `"ok"` or `"unreachable"`; `"ok"` when file storage is disabled |

#### Example

```sh
curl localhost:8081/health
```

---

### POST /ingest

Ingests textual content or document files for chunking, embedding, vector indexing, and relational storage. HTTP 202 means the parsed content or [pending OCR input](#image-only-pdf-ocr) and ingestion job have committed to PostgreSQL and survive restarts. Processing completes asynchronously. Content NUL characters are removed before validation and fingerprinting; metadata keys or values containing NUL return 400 `metadata must not contain NUL characters`.

A document's `status` becomes `completed` after its vectors and chunks are stored and search returns its last vector. When the [visibility wait](architecture.md#ingestion-stages) is disabled or ends first, the document completes anyway and can take a moment to become searchable.

- **Method**: `POST`
- **Path**: `/ingest`
- **Auth**: Required (`X-Service-Token`)

#### Variant A: JSON Request

- **Content-Type**: `application/json` (accepts standard parameters such as `charset=utf-8`)
- **Body Limit**: 8,097,152 bytes (six times the decoded-content limit plus 2 MiB of overhead)

Request fields:

| Field | Type | Required | Description |
|---|---|---|---|
| `content` | string | yes | Document text content (maximum 1,000,000 bytes after JSON decoding) |
| `collection` | string | no | Collection label that scopes content deduplication, using the label rules above |
| `doc_type` | string | no | Document category label. Defaults to `"document"` if omitted |
| `document_id` | string | no | Optional document UUID, retained in canonical form. Omitted or empty values generate a fresh UUID; malformed non-empty values return 400 |
| `metadata` | object | no | Optional key-value metadata stored only in PostgreSQL, subject to the size, nesting and NUL limits above |

Response (New Document):
- **Status**: `202 Accepted`

```json
{
  "document_id": "8a3e792c-5df6-4b6e-a612-4f7db4959db2",
  "status": "processing"
}
```

Response (Duplicate Document):
- **Status**: `200 OK`

When `document_id` is supplied, a completed document with the same content
fingerprint returns `already_ingested` with that same identifier. In-flight duplicates return 202 `processing` with the same identifier. Failed duplicates return 202 and requeue that document. Different
content or a different collection under an existing identifier returns 409 with
`{"error":"conflict","message":"document_id already exists with different content"}`.
Existing documents are never overwritten.

Content deduplication across identifiers applies only when `document_id` is
omitted or empty, and only within the request's collection. A new
caller-supplied identifier is retained even if another document has identical
content. Document or fingerprint lookup failures return 503 with
`{"error":"unavailable","message":"document store unavailable"}`.

```json
{
  "document_id": "8a3e792c-5df6-4b6e-a612-4f7db4959db2",
  "status": "already_ingested"
}
```

Error Status Codes:

| Status | Error Category | Condition |
|---|---|---|
| `400 Bad Request` | `validation` | Content field missing, empty, blank whitespace, or exceeding 1,000,000 decoded bytes; malformed JSON, unknown fields, duplicate top-level keys or trailing data; invalid label or metadata; non-empty `document_id` is not a UUID (`document_id must be a UUID`) |
| `401 Unauthorized` | `unauthorized` | Missing or invalid `X-Service-Token` header |
| `409 Conflict` | `conflict` | Supplied `document_id` already exists with different content |
| `413 Payload Too Large` | `validation` | JSON request payload exceeds 8,097,152 bytes |
| `415 Unsupported Media Type` | `validation` | Content-Type is not `application/json` or `multipart/form-data` |
| `503 Service Unavailable` | `queue` | Persisted ingestion queue is full (`WORKER_QUEUE_SIZE` reached); `Retry-After: 30` |
| `503 Service Unavailable` | `unavailable` | Document lookup or atomic acceptance failed |

Example:

```sh
curl -X POST localhost:8081/ingest \
  -H "X-Service-Token: dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Operational procedures for network routing nodes...",
    "doc_type": "handbook",
    "metadata": {
      "author": "infrastructure-team",
      "version": "1.4"
    }
  }'
```

#### Variant B: Multipart Form Upload

- **Content-Type**: `multipart/form-data`
- **Body Limit**: Bounded by [`MAX_UPLOAD_BYTES`](configuration.md#environment-variables)

Form fields:

| Field | Type | Required | Description |
|---|---|---|---|
| `file` | file binary | yes | File to extract and ingest. Accepted formats: PDF, DOCX, DOC and UTF-8 or BOM-marked UTF-16 plain text (`.txt`, `.md`, `.markdown`) |
| `collection` | string | no | Collection label with the same scope, default and validation rules as JSON ingestion; repeated fields return 400 |
| `doc_type` | string | no | Document category label. Defaults to `"document"` if omitted |
| `document_id` | string | no | Optional document UUID with the same identity and conflict rules as JSON requests, applied to extracted text or [OCR input](#image-only-pdf-ocr) |
| `metadata` | JSON object string | no | Optional metadata with the same limits as JSON ingestion; duplicate top-level keys, trailing data and repeated metadata fields return 400. The stored object includes `source_file` set to the uploaded filename, overriding a caller value, and must still fit the size limit |

Uploads are identified by content. PDF headers must appear within the first
1,024 bytes; DOCX packages must contain a main document part or an
officeDocument relationship; DOC files must contain a WordDocument entry.
Plain text requires valid UTF-8 without NUL in an 8 KiB sample, or a UTF-16 BOM.
Known extensions must match the detected format; mismatches return 415. Files
with no extension or an unknown extension use the detected format. Content
that cannot be identified returns 415.

Plain text is decoded in process. Extraction strips a UTF-8 BOM, decodes
BOM-marked UTF-16, replaces invalid encoding sequences, removes NUL and trims
surrounding whitespace. The decoded text must fit the 1,000,000-byte limit.
PDF, DOCX and DOC extraction uses an isolated process.

DOCX extraction supports body text, text boxes, footnotes and endnotes in
transitional and strict Open XML documents. Headers, footers and comments are
not extracted. All extracted parts share the 1,000,000-byte text limit.

DOC extraction supports the main document text in Word 97–2003 binary documents;
headers, footers, footnotes and comments are not extracted. Word 6/95 documents
and encrypted files return 422.

New uploads return 202 after durable acceptance with the retained or generated identifier. Completed duplicates return 200 `already_ingested`; in-flight duplicates return 202 with the same identifier; failed duplicates return 202 and are requeued. Identity and deduplication follow the JSON ingestion rules above, with the [OCR input exception](#image-only-pdf-ocr). A configured source store must accept the original upload before document acceptance. New source objects are removed when acceptance does not retain them.

Error Status Codes:

| Status | Error Category | Condition |
|---|---|---|
| `400 Bad Request` | `validation` | Form `file` part missing or read error; invalid label or metadata; non-empty `document_id` is not a UUID |
| `401 Unauthorized` | `unauthorized` | Missing or invalid `X-Service-Token` header |
| `409 Conflict` | `conflict` | Supplied `document_id` already exists with different extracted text or OCR input |
| `413 Payload Too Large` | `validation` | Upload exceeds `MAX_UPLOAD_BYTES` |
| `415 Unsupported Media Type` | `extraction` | Extension and content disagree, or content has no supported type |
| `422 Unprocessable Entity` | `extraction` | Document text extraction failed (e.g., malformed binary structure, extracted PDF, DOCX, DOC or text exceeding 1,000,000 bytes, expanded DOCX XML exceeding 16 MiB, a DOC stream declaring a size larger than the file, or PDF extraction work exceeding its limits) |
| `422 Unprocessable Entity` | `extraction` | Isolated parser failure: `document could not be parsed`; extraction timeout: `document extraction exceeded the time limit` |
| `503 Service Unavailable` | `busy` | All upload slots remain occupied beyond `EXTRACT_QUEUE_TIMEOUT`: `too many concurrent uploads`, with `Retry-After: 5` |
| `503 Service Unavailable` | `extraction` | The extraction child cannot be started: `document extraction is unavailable` |
| `503 Service Unavailable` | `storage` | Source upload failed: `source file storage is unavailable`; `Retry-After: 30` |
| `503 Service Unavailable` | `queue` | Persisted ingestion queue is full; removes a newly uploaded source object; `Retry-After: 30` |
| `503 Service Unavailable` | `unavailable` | Document lookup or atomic acceptance failed |

Content verification and extraction error codes:

| Code | Status | Fixed message |
|---|---|---|
| `type_mismatch` | 415 | `file extension does not match its content` |
| `unsupported_type` | 415 | `unsupported file type; accepted formats: pdf, docx, doc, txt, md, markdown` |
| `too_large` | 422 | `extracted text exceeds the size limit` |
| `timeout` | 422 | `document extraction exceeded the time limit` |
| `unparseable` | 422 | `document could not be parsed` |
| `no_text` | 422 | `document contains no readable text` |
| `encrypted` | 422 | `encrypted documents are not supported` |
| `unsupported_version` | 422 | `document version is not supported` |
| `unavailable` | 503 | `document extraction is unavailable` |

With OCR disabled, a PDF without a readable text layer returns 422 with
`{"error":"extraction","message":"document contains no readable text","code":"no_text"}`.

Example:

```sh
curl -X POST localhost:8081/ingest \
  -H "X-Service-Token: dev-token" \
  -F "file=@technical_paper.pdf" \
  -F "doc_type=research" \
  -F 'metadata={"author":"research-team"}'
```

#### Image-only PDF OCR

With [OCR enabled](configuration.md#environment-variables), a PDF whose parser
returns `no_text` (fewer than 10 bytes of embedded text after trimming
whitespace) is uploaded to S3 and accepted with HTTP 202 `processing`. PDFs with
more embedded text, including partly scanned documents, are ingested from that
text without OCR.
PostgreSQL initially stores empty content and `text_source = 'ocr_pending'`.
Deduplication and caller-supplied identifier conflicts use the collection-scoped
SHA-256 of the original PDF bytes, preserving that identity after text detection.
Other formats and extraction errors keep their existing behavior. If the
original cannot be stored, acceptance returns 503 `storage`.

During ingestion, Textract text detection runs against that original while the
job lease is renewed. All result pages are read; detected lines are ordered by
page, then top and left position, and joined with newlines. The detected content
and preview are saved with `text_source = 'ocr'` before chunking, embedding and
indexing. A retry uses saved text when available, and retries before persistence
reuse the detection request for that S3 object. A requeued failed document
reuses the same original and request, so a failed detection can return the same
result; delete the document and upload it again to request a new detection. Empty results fail ingestion with
`last_error = 'no_text'`; partial or failed detection and document or decoded-text
limit errors also fail ingestion. Poll [document status](#get-documentsdoc_id)
for the outcome. The decoded-text limit above still applies; provider limits,
permissions and billing are described in [deployment](deployment.md#optional-pdf-ocr).

---

### POST /query

Executes vector similarity search and synthesizes an answer grounded in the retrieved document corpus.

- **Method**: `POST`
- **Path**: `/query`
- **Auth**: Required (`X-Service-Token`)
- **Content-Type**: `application/json`
- **Body Limit**: 1 MiB (1,048,576 bytes)

Request fields:

| Field | Type | Required | Description |
|---|---|---|---|
| `query` | string | yes | Natural-language query string (1 to 10,000 decoded bytes) |
| `mode` | string | no | Retrieval mode: `"auto"` (default), `"local"`, or `"global"`. Case-insensitive. `local` answers mainly from the most relevant authorized document (named entity or best match), topped up from the authorized scope when that document has fewer than 3 matching chunks. With no visible candidate, `local` uses `global`. Explicit `global` skips the routing probe |
| `filter_doc_type` | string | no | Restricts vector retrieval to chunks matching the given `doc_type`, using the label normalization and limits above |
| `collection` | string | no | Exact collection label, using the label rules above; omitted or empty searches all collections. Applied inside vector retrieval and routing, then checked against completed PostgreSQL documents. Intersects the type filter and document allowlist; vectors written before collection filtering need the [collection metadata backfill](development.md#collection-metadata-backfill) |
| `max_chunks` | int | no | Requested chunks to retrieve (0 to 100), capped by [`MAX_CHUNKS_PER_QUERY`](configuration.md#environment-variables). Omitted or zero uses that configured maximum |
| `include_citations` | bool | no | Request numbered inline citations and lexical checks against the excerpts sent to synthesis; defaults to false |
| `allowed_document_ids` | array of strings | no | Tenant/role allowlist of UUIDs, at most [`MAX_ALLOWED_DOCUMENT_IDS`](configuration.md#environment-variables) entries. IDs are canonicalized and deduplicated. Omitted or `null` means unrestricted unless `REQUIRE_DOCUMENT_ALLOWLIST=true`, which makes a non-null field mandatory. An empty list (`[]`) returns the no-information answer without embedding or search. The allowlist is applied inside vector search and routing |

Response fields (200 OK):

| Field | Type | Description |
|---|---|---|
| `finish_reason` | string | Completion reason when available: `stop`, `max_tokens`, `filtered`, `context_exceeded`, or `no_evidence` |
| `answer` | string | Grounded answer in the question's language when evidence exists; localized fixed no-information answer otherwise, as described below |
| `mode` | string | Retrieval mode executed: `"LOCAL"` or `"GLOBAL"`; automatic routing reflects only authorized, visible candidate documents |
| `chunks_used` | int | Count of chunks from completed documents remaining after visibility, allowlist and minimum relevance filtering |
| `context_chunks` | int | Count of chunks retained after score thresholding and passed into the LLM context (optional) |
| `context_tokens` | int | Estimated token count of the prompt context (optional) |
| `early_exit` | bool | Indicates whether query execution completed early (optional) |
| `stage_timings_ms` | map[string]int64 | Execution timings for pipeline stages in milliseconds (optional) |
| `token_usage` | object | LLM token usage metrics (`input_tokens`, `output_tokens`, `total_tokens`) |
| `scored_chunks` | array | Retrieved chunks ordered by reranking relevance when available, otherwise vector similarity |
| `citations` | array | Citation checks; present whenever `include_citations` is true, including an empty array when no markers or evidence remain; otherwise omitted |

Each `citations` entry contains:

| Field | Type | Description |
|---|---|---|
| `excerpt` | int | One-based index of the excerpt sent to synthesis |
| `doc_id` | string | Source document identifier |
| `chunk_index` | int | Source chunk index |
| `offset` | int | Zero-based Unicode code point index of the marker's `[` in the cleaned JSON answer or unchanged streamed answer; references in one list marker share the offset |
| `verified` | bool | Whether the rounded lexical overlap is at least 0.30; this is not an entailment check |
| `overlap` | number | Fraction of distinct sentence tokens also found in the excerpt, rounded to two decimal places; zero for a sentence without tokens |

For JSON responses with `include_citations`, markers match `[n]` or comma-separated lists of one-to-three-digit numbers, such as `[1, 2]`; adjacent markers are allowed. Numbers outside the excerpt range are removed. A marker with no valid numbers is removed with one preceding space. Retained markers list each valid number once, as comma-separated decimal numbers. Other bracketed text is unchanged. Without this option the answer is unchanged.

The referencing sentence begins after the previous `.`, `!`, `?`, `。`, `！`, `？` or newline before the marker and excludes other citation markers. Tokens are maximal runs of letters, combining marks and digits after NFC normalization and lowercasing, ignoring invisible format characters; runs of at least three code points or containing a digit are kept. Dense-script runs (Han, Hiragana, Katakana, Hangul, Thai, Lao, Khmer, Myanmar and Tibetan) use character bigrams, and script-neutral letters such as `ー` stay in the run they follow. Lexical checks use the actual sanitized, possibly truncated excerpt body rather than the response snippet.

Malformed allowlist entries return 400 `allowed_document_ids must contain UUIDs`; a missing or null required allowlist returns 400 `allowed_document_ids is required`. Lists above `MAX_ALLOWED_DOCUMENT_IDS` return 400 before deduplication.

If retrieval, the document allowlist, or `MIN_RELEVANCE_SCORE` leaves no evidence, the answer is a fixed message selected from the optional `Accept-Language` request header. Supported language tags are `en`, `es`, `pt`, `fr`, `de` and `it`; regional tags such as `es-MX`, `pt-BR` and `fr-CA` match their base language, and quality weights determine preference. When the header lists no supported language, closely related languages such as Galician (`gl`) or Luxembourgish (`lb`) receive the related supported language. Missing, empty, malformed or unmatched headers, and headers longer than 1,024 bytes, use English: `No relevant information was found to answer this question.` The response sets `Content-Language` to the selected base tag. No synthesis model is called, `token_usage` fields are zero, `early_exit` is true and `finish_reason` is `no_evidence`. Evidence-backed synthesis follows the question's language and does not set `Content-Language`.

A transient similarity-probe failure selects GLOBAL retrieval in automatic mode instead of failing the query; embedding or subsequent retrieval failures use the error mapping below.

Document visibility is checked against PostgreSQL before answer generation. Older processing rows with a stored fingerprint are treated as completed for compatibility. Failed, unfinished and missing documents contribute no chunks; a visibility lookup failure uses the error mapping below. The same empty-evidence response applies when visibility filtering removes all chunks.

`MIN_RELEVANCE_SCORE=0` disables the absolute relevance floor; positive values remove lower-scoring chunks before synthesis trimming.

Optional [Bedrock reranking](configuration.md#environment-variables) submits the original question and up to 100 authorized, visible candidate texts after retrieval and before synthesis context selection. Candidates are sorted by descending relevance; ties retain vector order. Reranking changes only the order: `MIN_RELEVANCE_SCORE` and `SYNTHESIS_RELATIVE_SCORE_FLOOR` still compare vector similarity, and candidates below the relative floor are skipped rather than ending selection. A failed, timed-out or invalid reranking response preserves vector order and omits reranking scores; a warning and [outcome counter](configuration.md#metrics) record the fallback. The `rerank` entry in `stage_timings_ms` measures this step.

Each admitted query runs under `QUERY_TIMEOUT`; synthesis has the shorter `SYNTHESIS_TIMEOUT` budget. Earlier client deadlines take precedence. A canceled request produces no further response bytes. Nonempty truncated or filtered answers retain their completion reason; empty synthesis text uses the `synthesis` error category below.

Scored chunk fields:

| Field | Type | Description |
|---|---|---|
| `doc_id` | string | Document UUID the chunk belongs to |
| `doc_type` | string | Document category label |
| `chunk_index` | int | Position index of the chunk within the source document |
| `score` | float64 | Cosine similarity score |
| `rerank_score` | float64 | Bedrock relevance score, including zero; omitted when reranking is disabled or fails. Does not replace `score` |
| `text` | string | Chunk text excerpt, truncated safely to a maximum of 600 runes without splitting UTF-8 characters |
| `kind` | string | Classification category if populated during ingestion (optional) |
| `quality` | string | Quality rating if populated during ingestion (optional) |
| `excerpt` | int | One-based prompt excerpt number; omitted when this chunk did not reach synthesis |
| `truncated` | bool | True when the excerpt body was cut before synthesis; omitted otherwise. This describes synthesis truncation, independently of the response snippet limit |

#### Streamed query answers

Send `Accept: text/event-stream` to receive server-sent events; JSON remains the default. `text/event-stream` with any positive `q` value selects streaming, regardless of other listed types; wildcards such as `*/*` keep JSON. Each event is framed as `event: <name>`, a `data: <JSON>` line, and a blank line. Text, including newlines, is escaped inside JSON. Each frame is flushed, and the response sets `Content-Type: text/event-stream`, `Cache-Control: no-cache` and `X-Accel-Buffering: no`. Bedrock generation uses ConverseStream, OpenAI-compatible generation uses the [streaming chat API](deployment.md#openai-compatible-providers), and offline generation emits words incrementally.

| Event | Payload | When |
|---|---|---|
| `context` | `mode`, `chunks_used`, `context_chunks`, `scored_chunks` | Once, after evidence filtering and synthesis excerpt selection, before generation |
| `delta` | `{"text":"..."}` | Incremental answer text; concatenate `text` values in order |
| `done` | `finish_reason`, `token_usage`, `context_tokens`, `stage_timings_ms`, optional `early_exit`, and `citations` when requested | Successful completion; timings include synthesis |
| `error` | `{"error":"...","message":"..."}` | A failure after the first frame; error categories and messages follow the table below |

The context event uses the scored chunk fields above, including `excerpt` and `truncated`, so it identifies which retrieved chunks reached synthesis. Zero context counts are included. With `include_citations`, the final event always contains a citation array, including `[]` when no valid references remain. Citation checks run on the complete generated answer. Streaming preserves all text and markers as generated; checks omit out-of-range excerpt numbers and report each valid number in a marker once. The lexical check and offset definition above apply.

A no-evidence response sends `context`, one localized `delta`, and `done`, with zero token usage and context counts, `early_exit: true`, and `finish_reason: "no_evidence"`. Its `Content-Language` header follows the language selection described above.

Authentication, JSON validation, body and authorization limits, admission, deadlines, metrics and tracing apply to both formats. Errors before the first frame retain the JSON status codes below. After the stream starts, HTTP status remains 200 and a failure ends with `error` instead of `done`. A disconnected client cancels generation and closes the upstream stream. A stream that closes without `done` is incomplete; discard it or retry the query as a new request.

For example, add `-N -H "Accept: text/event-stream"` to the query command to display frames as they arrive:

```text
event: context
data: {"mode":"GLOBAL","chunks_used":1,"context_chunks":1,"scored_chunks":[{"doc_id":"example-document","doc_type":"guide","chunk_index":0,"score":0.9,"text":"Archive evidence","excerpt":1}]}

event: delta
data: {"text":"Archive evidence [1]."}

event: done
data: {"finish_reason":"stop","token_usage":{"input_tokens":100,"output_tokens":6,"total_tokens":106},"context_tokens":100,"stage_timings_ms":{"query_decomposition":0,"retrieval_routing":1,"retrieval":2,"synthesis":5},"citations":[{"excerpt":1,"doc_id":"example-document","chunk_index":0,"offset":17,"verified":true,"overlap":1}]}

```

The [Go client](mcp.md#go-client) reads this stream with `QueryStream`.

Error Status Codes:

| Status | Error Category | Condition |
|---|---|---|
| `400 Bad Request` | `validation` | Query missing, empty, or whitespace; query exceeding 10,000 decoded bytes; mode not auto, local, or global; max_chunks outside 0-100; invalid document type filter or collection; malformed JSON, unknown fields, duplicate top-level keys or trailing data |
| `401 Unauthorized` | `unauthorized` | Missing or invalid `X-Service-Token` header |
| `413 Payload Too Large` | `validation` | Request body exceeds the 1 MiB limit |
| `415 Unsupported Media Type` | `validation` | Content-Type is not `application/json` |
| `504 Gateway Timeout` | `timeout` | The query or synthesis deadline expired: `query processing timed out` |
| `503 Service Unavailable` | `overloaded` | Query admission exceeded `QUERY_QUEUE_TIMEOUT`: `too many concurrent queries`; `Retry-After: 1` |
| `503 Service Unavailable` | `upstream_throttled` | Upstream throttling or exhausted quota: `upstream service is busy`; `Retry-After: 2` |
| `502 Bad Gateway` | `synthesis` | Synthesis returned no text: `answer synthesis returned no text` |
| `500 Internal Server Error` | `pipeline` | Other pipeline failures: `query processing failed`; internal error details are recorded only in server logs |

Example:

```sh
curl -X POST localhost:8081/query \
  -H "X-Service-Token: dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Which routing algorithms are supported in version 2?",
    "mode": "auto",
    "filter_doc_type": "handbook",
    "max_chunks": 5
  }'
```

---

### GET /documents

Returns document summaries for inventory verification and orphan vector reconciliation. PostgreSQL returns summaries in ascending UUID order, including processing, completed and failed documents.

- **Method**: `GET`
- **Path**: `/documents`
- **Auth**: Required (`X-Service-Token`)
- **Request Body**: None

Optional query parameters:

| Parameter | Type | Description |
|---|---|---|
| `limit` | int | Page size from 1 to 1000 |
| `after` | string | UUID cursor, exclusive; requires `limit` |
| `collection` | string | Exact collection label, using the label rules above; omitted or empty lists all collections. Filters both streaming and paginated listings before ordering and limiting |

Without pagination parameters, PostgreSQL streams the full listing without buffering every row. The response writes `documents` before `count`; an empty listing is `{"documents":[],"count":0}`. If a stream fails after it begins, its JSON is incomplete; discard the partial result and retry. Stores without summary support retain the reference listing and return an empty array when no documents exist.

Response fields (200 OK):

| Field | Type | Description |
|---|---|---|
| `documents` | array | Document summaries; always an array, including when empty |
| `count` | int | Number of documents returned in this response |
| `next_after` | string or null | Present for paginated responses; last UUID when the page contains `limit` documents, otherwise null |

Use `next_after` as the next request's `after` value and keep the same `limit` and `collection`. A full final page may lead to one empty page before `next_after` becomes null. Each request reads the current inventory; concurrent ingestion and deletion can change later pages.

Document summary fields:

| Field | Type | Description |
|---|---|---|
| `document_id` | string | Document UUID |
| `collection` | string | Collection label, always present; `""` for the default collection |
| `doc_type` | string | Document category label |
| `s3_key` | string | S3 object key, or omitted if no binary file was stored |
| `status` | string | Ingestion state: `processing`, `completed` or `failed` |
| `chunk_count` | int | Number of stored chunks |
| `file_name` | string or null | Original uploaded filename |
| `file_type` | string or null | Original uploaded file type |
| `created_at` | string | Creation timestamp in RFC 3339 format |
| `updated_at` | string | Latest ingestion transition timestamp in RFC 3339 format |
| `completed_at` | string or null | Completion timestamp in RFC 3339 format |
| `last_error` | string | Last ingestion error, included only for failed documents when recorded |

Error Status Codes:

| Status | Error Category | Condition |
|---|---|---|
| `400 Bad Request` | `validation` | Invalid `limit`, `after` or `collection`; duplicate pagination or `collection` parameters; `after` without `limit`; or a malformed query string |
| `401 Unauthorized` | `unauthorized` | Missing or invalid `X-Service-Token` header |
| `500 Internal Server Error` | `internal` | Database query failure |

Example:

```sh
curl localhost:8081/documents \
  -H "X-Service-Token: dev-token"

curl 'localhost:8081/documents?limit=100&after=8a3e792c-5df6-4b6e-a612-4f7db4959db2' \
  -H "X-Service-Token: dev-token"
```

---

### GET /documents/{doc_id}

Returns one document summary with the fields described above. Poll this endpoint after ingestion to observe completion or failure. Document content and caller metadata are excluded.

- **Method**: `GET`
- **Path**: `/documents/{doc_id}`
- **Auth**: Required (`X-Service-Token`)
- **Response**: The document summary object (200 OK)

| Status | Error Category | Condition |
|---|---|---|
| `400 Bad Request` | `validation` | `doc_id` is not a UUID |
| `401 Unauthorized` | `unauthorized` | Missing or invalid `X-Service-Token` header |
| `404 Not Found` | `not_found` | The document does not exist |
| `500 Internal Server Error` | `internal` | Database lookup failure |
| `503 Service Unavailable` | `unavailable` | The configured store does not support document summaries |

---

### GET /documents/{doc_id}/file

Generates a presigned S3 URL for downloading the original binary file associated with an ingested document. Downloads are served as attachments using the stored filename and a content type derived from its extension.

- **Method**: `GET`
- **Path**: `/documents/{doc_id}/file`
- **Auth**: Required (`X-Service-Token`)
- **Parameters**: `doc_id` (path parameter, must be a valid UUID)
- **Request Body**: None

Response fields (200 OK):

| Field | Type | Description |
|---|---|---|
| `url` | string | Presigned GET URL generated for the S3 object |
| `expires_in` | int | URL validity duration in seconds (always 900 seconds = 15 minutes) |

Error Status Codes:

| Status | Error Category | Condition |
|---|---|---|
| `400 Bad Request` | `validation` | Invalid path or `doc_id` is not a valid UUID |
| `401 Unauthorized` | `unauthorized` | Missing or invalid `X-Service-Token` header |
| `404 Not Found` | `not_found` | S3 storage not configured or no file stored for the requested document |
| `500 Internal Server Error` | `internal` | Database lookup failure or error generating presigned URL |

Example:

```sh
curl localhost:8081/documents/8a3e792c-5df6-4b6e-a612-4f7db4959db2/file \
  -H "X-Service-Token: dev-token"
```

---

### DELETE /documents/{doc_id}

Deletes a document and all related artifacts across Pinecone, S3, and PostgreSQL.

Deletion continues after the caller disconnects, with a separate 60-second deadline. When a stored file exists but file storage is not configured, deletion returns `503 Service Unavailable` before removing any vectors or metadata, with `{"error":"storage","message":"stored file cannot be deleted because file storage is not configured"}`.

- **Method**: `DELETE`
- **Path**: `/documents/{doc_id}`
- **Auth**: Required (`X-Service-Token`)
- **Parameters**: `doc_id` (path parameter, must be a valid UUID)
- **Request Body**: None

Deletion ordering:
1. Removes the legacy Pinecone document namespace (`doc_id`) when present.
2. Deletes vector entries in the Pinecone default namespace where metadata `doc_id` matches.
3. Deletes the original uploaded file from S3 if an `s3_key` exists.
4. Deletes the document record from PostgreSQL (cascading foreign keys remove rows in `chunks` and `document_fingerprints`).

If an error occurs while deleting vectors or object storage, PostgreSQL metadata is preserved so the operation can be retried, and the request returns `500 Internal Server Error`.

After deletion returns successfully, subsequent queries exclude the document from answers and scored chunks immediately, even while vector deletion is still propagating through the index.

Response fields (200 OK):

| Field | Type | Description |
|---|---|---|
| `deleted` | bool | Always `true` on successful deletion |
| `doc_id` | string | UUID of the deleted document |

Error Status Codes:

| Status | Error Category | Condition |
|---|---|---|
| `400 Bad Request` | `validation` | Invalid path or `doc_id` is not a valid UUID |
| `401 Unauthorized` | `unauthorized` | Missing or invalid `X-Service-Token` header |
| `404 Not Found` | `not_found` | Document does not exist in PostgreSQL |
| `500 Internal Server Error` | `internal` | Database query failure or external deletion error in Pinecone or S3 |
| `503 Service Unavailable` | `storage` | A stored file exists but file storage is not configured; no data is deleted |

Example:

```sh
curl -X DELETE localhost:8081/documents/8a3e792c-5df6-4b6e-a612-4f7db4959db2 \
  -H "X-Service-Token: dev-token"
```
