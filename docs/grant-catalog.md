# Grant catalog

The grant catalog stages tenant-bound access in the metadata database. Ownership
imports remain pending until document validation can publish readiness.

## Subject identity

Public routes use service authentication. The staged verifier is available
internally for handlers that enforce grants. It requires one valid
`X-Service-Token` and one `Authorization` bearer header. Each header must
contain a single credential.

The verifier accepts RFC 9068 access tokens with an `at+jwt` media type, an
RS256 signature and a local `kid`. The issuer must match exactly and the
audience must be the configured value, as a string or a single-element array.
The subject, client ID, token ID, issued-at and expiry claims are required.
Tokens have a positive lifetime of up to five minutes. Admission checks allow
up to 30 seconds of clock skew and honor `nbf` when present. Disclosure checks
use the exact expiry without skew. The same valid token can be used for more
than one request. Compact tokens are limited to 8 KiB. Keep the Quarry audience
reserved for delegated subject tokens at the issuer and exclude client-credentials
grants from issuing tokens for it. Keep issuer subject identifiers stable.

The operator supplies a regular local JWKS file with a top-level `keys` array
and an RFC 3339 `expires_at` timestamp. The file must be at most 64 KiB and
contain at most 16 public RSA signing keys of at least 2,048 bits with distinct
`kid` values. Keep the file and its directory writable only by the trusted
operator. On Windows, enforce those write restrictions with filesystem ACLs.
Export the public RSA `kty`, `n`, `e` and `kid` fields. When present,
`use`, `alg` and `key_ops` must identify RS256 signature verification. Rotate by
atomically replacing the file on each replica. Include both keys during overlap,
then remove the retired key. A missing, unreadable, invalid or expired file
makes the authority unavailable. After resolving the principal, the gate
checks that the current key set is still valid and contains the signing key.

Use TLS between clients and trusted ingress, with protected transport from
ingress to Quarry. The verified, case-sensitive issuer and subject select the
active provisioned principal and its tenant in PostgreSQL. Caller-supplied
headers, token roles and email have no effect on that lookup. The internal
gate returns generic `401` for invalid credentials, `403` for an absent or
disabled principal and `503` for unavailable authority. The gate carries only
the internal principal, tenant and exact expiry into the request context and
bounds that context's deadline at the credential expiry.

## Database roles

Use the metadata schema owner to apply migrations before starting a restricted
serving login. Put the corpus schema first in each login's PostgreSQL search
path. The schema owner needs `USAGE` and `CREATE` there and ownership of its
existing metadata tables. A database administrator can preinstall `pgcrypto`.
Catalog upgrades then require only these schema and table privileges. Keep the
owner credential off request-serving and operator hosts.

```sh
go run ./scripts/grant-catalog -migrate
```

Set `DATABASE_URL` to the appropriate login for each command. Provision separate
logins for serving, each operator and audit access. Substitute the corpus schema
for `public` in the example. Before granting catalog execution, revoke existing
schema `CREATE` grants from `PUBLIC` and every non-owner role, including roles
from which a login inherits privileges. Check the schema ACL for other grantees
and inspect existing objects before trusting a previously writable schema.
Use a dedicated owner-controlled corpus schema if other applications need to
create objects in `public`.

```sql
REVOKE CREATE ON SCHEMA public FROM PUBLIC, quarry_service, quarry_operator, quarry_auditor;
SELECT acl.grantee::regrole AS role_with_create
FROM pg_namespace AS schema
CROSS JOIN LATERAL aclexplode(coalesce(schema.nspacl, acldefault('n', schema.nspowner))) AS acl
WHERE schema.nspname = 'public' AND acl.privilege_type = 'CREATE';
SELECT login, has_schema_privilege(login, 'public', 'CREATE') AS can_create
FROM (VALUES ('quarry_service'), ('quarry_operator'), ('quarry_auditor')) AS roles(login);
```

Proceed when the ACL lists only the schema owner and each login returns
`false` for `can_create`. The metadata owner then applies the role grants.

```sql
GRANT USAGE ON SCHEMA public TO quarry_service, quarry_operator, quarry_auditor;
GRANT SELECT ON public.schema_migrations TO quarry_service;
GRANT EXECUTE ON FUNCTION public.vector_bind_legacy(text, text, text),
  public.vector_writer_state() TO quarry_service;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.documents, public.chunks,
  public.ingest_jobs, public.document_fingerprints TO quarry_service;
GRANT SELECT, INSERT, UPDATE ON public.document_pdf_extractions TO quarry_service;
GRANT SELECT, INSERT, UPDATE ON public.document_deletions TO quarry_service;
GRANT EXECUTE ON FUNCTION public.evaluation_fixture_claimable(uuid, text),
  public.evaluation_fixture_reserved(uuid), public.document_deletion_active(),
  public.document_deletion_reserved(uuid) TO quarry_service;
GRANT SELECT (revision) ON public.grant_catalog_policy_revision TO quarry_operator;
GRANT EXECUTE ON FUNCTION public.grant_catalog_source_snapshot(uuid) TO quarry_operator;
GRANT EXECUTE ON FUNCTION public.grant_catalog_import_batch(text, boolean) TO quarry_operator;
GRANT EXECUTE ON FUNCTION public.grant_catalog_authorize_ingest(uuid, uuid, text, uuid),
  public.grant_catalog_lock_ingest_permission(uuid, uuid, text),
  public.grant_catalog_bind_accepted_document(uuid, uuid, text, uuid, uuid, boolean),
  public.grant_catalog_publish_accepted_document(uuid, uuid),
  public.grant_catalog_document_tenant(uuid, uuid),
  public.grant_catalog_policy_state(uuid, uuid),
  public.grant_catalog_list_documents(uuid, uuid, uuid, integer, text, boolean),
  public.grant_catalog_get_document(uuid, uuid, uuid, boolean),
  public.grant_catalog_authorize_file(uuid, uuid, uuid),
  public.grant_catalog_authorize_delete(uuid, uuid, uuid, uuid),
  public.grant_catalog_record_deletion_owner(uuid, uuid, uuid, uuid),
  public.grant_catalog_read_revision(uuid, uuid) TO quarry_service;
GRANT SELECT ON public.grant_catalog_active_principals, public.grant_catalog_readable_documents TO quarry_service;
GRANT EXECUTE ON FUNCTION public.grant_catalog_backfill_candidates(uuid, integer),
  public.grant_catalog_publish_verified_document(uuid, uuid, text, text) TO quarry_operator;
GRANT SELECT ON public.grant_catalog_policy_audit TO quarry_auditor;
```

If serving replicas will enable the source upload journal, also apply the
[source upload journal database grants](deployment.md#source-upload-journal-database-grants).

For pgvector, preinstall the extension as a database administrator or use an
owner login with extension installation privileges. Make the extension schema
visible to the owner and serving logins. Use the configured embedding dimension
when the metadata owner prepares the table and indexes.

```sh
go run ./scripts/grant-catalog -migrate -vector-dimensions 256
```

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON public.chunk_vectors TO quarry_service;
```

The metadata owner applies the vector function grants after migrating a new or
existing corpus. On first startup, the serving process opens the configured
target and binds its identity through the owner-installed initialization
function. Subsequent startups validate the recorded binding. The writer-state
function lets serving transactions participate in the freeze. Restrict direct
generation table access, freezes, verification approval and activation to the
maintenance login.

Before starting serving replicas for a promoted pgvector generation, the
metadata owner grants the same table privileges on that generation's
`chunk_vectors_<generation>` table. If exact-scope search is enabled, prepare
its indexes with the owner login before switching replicas. The serving
process validates the provisioned dimension and indexes without requiring
ownership or schema creation privileges.

After renaming a corpus schema, reconnect with the new schema first in the
search path. Catalog entry points and document cleanup stay with the renamed
corpus.

The serving login needs no catalog table privilege. Its only catalog access is
reading `grant_catalog_active_principals` and `grant_catalog_readable_documents`
and executing the owned-ingestion, scoped document-operation and
authorized-retrieval functions granted above, each of which runs with the
schema owner's rights. The catalog owner and database
administrators have administrative access. Give each operator an internal
database login name and avoid shared operator logins. The audit records the
authenticated PostgreSQL session login, revision, batch UUID, payload digest
and timestamp. Deleting an assigned document records the login and deleted
identity alongside the resulting revision. Keep the audit and backups
restricted and retain the audit, including import receipts, with the catalog.

The serving login needs no privilege on the `evaluation_fixtures` receipts. The
[staging evaluation guide](development.md#evidence-evaluation) lists the one
receipt grant that applies only to a deployment that enables fixtures.

### Upgrade an existing serving role

After the schema owner applies migrations `019_evaluation_fixture_guards.sql`
and `023_deletion_predicates.sql`, grant the serving login execution on the
four functions and access to the deletion receipts before starting upgraded
processes. Ordinary admission, job leasing, removal and the deletion-mode
check use these functions even when evaluation fixtures are disabled and
checkpointed deletion is inactive. The login reads the activation marker only
through `document_deletion_active` and needs no privilege on its table.
Substitute the deployed schema and serving role names as needed.

```sql
GRANT SELECT, INSERT, UPDATE ON public.document_deletions TO quarry_service;
GRANT EXECUTE ON FUNCTION public.evaluation_fixture_claimable(uuid, text),
  public.evaluation_fixture_reserved(uuid), public.document_deletion_active(),
  public.document_deletion_reserved(uuid) TO quarry_service;
```

After applying migration `020_owned_ingestion.sql`, grant reading
`grant_catalog_active_principals` and execution of the five serving functions
and the two operator functions before enabling
[owned ingestion](#owned-ingestion) or the scoped backfill. After applying
migration `024_document_operations.sql`, grant execution of the six scoped
document-operation functions (`grant_catalog_policy_state`,
`grant_catalog_list_documents`, `grant_catalog_get_document`,
`grant_catalog_authorize_file`, `grant_catalog_authorize_delete` and
`grant_catalog_record_deletion_owner`) before enabling
[scoped document operations](#scoped-document-operations). After applying
`026_authorized_retrieval.sql`, grant reading
`grant_catalog_readable_documents` and execution of
`grant_catalog_read_revision` before enabling
[authorized retrieval](#authorized-retrieval). Service-mode ingestion, job
leasing and queries need none of them until a document is assigned.

## Owned ingestion

Owned ingestion is the internal ingestion path for a verified principal. No
public route enables it yet, so the HTTP contract is unchanged. A handler that
uses it requires the verified subject and refuses every upload unless
`INGEST_CONTENT_DEDUP=off` and the store supports owners.

Acceptance runs in this order, and every step that can reveal or change shared
state follows the authorization:

1. The request body is parsed under the usual size limits. For multipart
   requests the whole body is read before any decision, each field may appear
   once, and query parameters never supply a field, so field order cannot change
   what is enforced. Metadata cannot use the names that carry ownership or
   index labels: tenant, owner, principal, incarnation, document and chunk
   identifiers, collection, document type, the vector revision and attempt tags,
   and the stored chunk fields. The match ignores case and covers top-level keys.
2. The principal's create permission on the tenant-local collection is
   checked. Without it the request receives `403`.
3. A server-generated UUID creates a new document. A supplied UUID is usable
   only as the principal's pending reservation in the same collection or as the
   principal's own current document, which is a retry. Every other supplied UUID
   receives the same `404`, whether it is missing, unreserved, deleted,
   unassigned or owned by someone else, and nothing else runs. The catalog
   stores no reservation expiry, so a reservation stays usable while it is
   pending.
4. Only then does the service detect the file type, extract text, defer OCR and
   store the original. The object key names the document, the intended
   incarnation and the attempt. A refused or uncertain acceptance removes only
   the attempt's own object, and keeps it when the stored key proves the
   acceptance committed.

The acceptance transaction holds the principal and the collection's create
permission until it commits, so a revocation waits for it or is already
visible. It stores the document with private identity and the intended
incarnation, queues one job and records the ownership with origin `accepted`
and readiness unset. It then spends the reservation. Revocation before the
commit returns `403`, and a document replaced since authorization returns
`404`. A subject credential that has expired when the transaction checks it
returns `401`, and the attempt's object is removed. An owner's retry meets the usual rules: a completed document is a
duplicate, a processing one is in progress, a failed one is requeued only with
current create permission and queue capacity, and different content or
collection is a conflict. An owner's retry of a historical PDF whose stored
identity is its parser text receives `409`. Only the owner sees these outcomes.
No step reads or registers a shared content fingerprint, including retries of
historical documents and job completion.

The job carries the tenant and incarnation read from the catalog, never a
subject token, so an accepted job may finish after the user's token expires.
Every hosted vector of an owned document carries a server-authored `tenant_id`
label beside the revision and attempt tags that already fence its writes and
cleanup to one incarnation, so a retired lease or incarnation can neither
publish into nor remove a replacement's data. Completion publishes readiness
for accepted documents in the same transaction that completes them.

## Scoped document operations

Scoped document handlers authorize inventory, status, original-file signing and
deletion for a verified principal. No public route enables them yet, so the
service-mode HTTP contracts stay unchanged. Every scoped response and error sets
`Cache-Control: private, no-store`, and handlers refuse to fall back to
unscoped stores (`503`).

- **Inventory and status:** Listing and status lookups apply the same-tenant
  ownership or `read` grant inside SQL before ordering, counting, limiting or
  streaming, exposing accepted documents in `processing`, `failed` and
  `completed` states and verified historical assignments while excluding
  unverified legacy rows and documents in `deleting` state. An omitted or empty
  `collection` filters across the caller's tenant without granting default
  collection access, and `after` acts only as an exclusive UUID cursor. Policy
  revision, principal status and token expiry are rechecked before releasing
  each page or bounded streaming batch. Raw `s3_key` values are omitted and
  internal `last_error` messages on failed rows are mapped to a bounded public
  category (`no_text`, `ocr_result_expired`, `ocr_text_too_large`,
  `embedding_limit`, `no_chunks`, `ocr_failed` or `processing_failed`).
- **Original-file signing:** Shared row locks verify active principal status
  and ownership or `read` plus `download` grants directly before signing. The
  requested presigned URL lifetime is the smaller of `900` seconds and the whole
  seconds remaining before token expiry, and signing is refused (`401`) when
  fewer than one second remains. A missing, unassigned, unverified, denied,
  read-only, deleting, absent-original or storage-disabled lookup returns the
  same `404` envelope with zero signing calls.
- **Owner-only deletion:** Only the active document owner may delete (`404` for
  grantees and non-owners). Authorization and the incarnation-pinned
  `document_deletions` intent commit atomically under the caller's request and
  credential deadline before detached cross-store cleanup runs; cancellation or
  token expiry before commit leaves the document intact and makes zero vector or
  object-store calls, while a committed intent finishes cleanup for that exact
  incarnation without affecting any replacement UUID.

## Authorized retrieval

Authorized retrieval is the internal query path for a verified principal. No
public route enables it yet, so the HTTP contract is unchanged. A handler that
uses it requires the verified subject and a vector store that enforces read
scopes. Without both it refuses every request with `503` before anything is
embedded. A store without the capability, including the in-memory fake, is
refused, and no branch of a scoped request ever falls back to an unfiltered
search.

### Request scope

A request derives its scope from the verified tenant and principal, the
documents the principal may read now and the current policy revision. A
document is readable when its principal is active, its current incarnation has
a ready ownership row held by the principal or a direct `read` grant, and the
document is complete. The scope is then intersected with `allowed_document_ids`,
`collection` and `filter_doc_type`.

- An omitted or `null` allowlist uses every readable document, unless
  `REQUIRE_DOCUMENT_ALLOWLIST` requires one, which is a `400`.
- An explicit empty list matches nothing and never widens the scope.
- Well-formed UUIDs that are foreign, deleted or unknown drop out without an
  error.
- An empty intersection returns the localized no-evidence response in both
  answer modes with no embedding, vector, reranking or generation call.

### Search

The scope applies to the similarity probe, entity previews, explicit and
automatic routing, the selected LOCAL document, GLOBAL retrieval, sparse LOCAL
top-up, the hosted refill and the fallback after a failed probe.

- pgvector joins `grant_catalog_readable_documents` inside the ranking
  statement for the exact-scope probe, exact ranking and approximate ranking,
  so grants and the current incarnation limit the rows before ordering and
  `LIMIT`. Nothing lists the membership, so `MAX_ALLOWED_DOCUMENT_IDS` limits
  only the inline allowlist.
- Pinecone sends the tenant, the allowed incarnations and the allowed document
  IDs in every request to the default namespace, split into requests of at most
  10,000 values per predicate. Before embedding, the service lists the
  readable members and returns `422` with `scope_too_large` when they exceed
  `MAX_ALLOWED_DOCUMENT_IDS` (25,000 by default). It never truncates the list.
  Only vectors that carry the revision and tenant tags can match, so a
  historical untagged vector never does.

The query is embedded only after the scope is known to be non-empty and within
capacity. Both backends use the same candidate checks afterward.

### Candidate and preview checks

Hosted hits are untrusted references. A hit whose tenant label contradicts the
scope, or that lacks the incarnation tag of a listed member, is dropped on
return. Every remaining candidate is then matched against the completed
PostgreSQL chunks, joined to the principal's readable relation, for identity,
body digest, category and incarnation before scores merge or any preview is
read. Previews are loaded only for readable documents, so a document the
principal cannot read can neither change the route nor appear in a diagnostic.
The response and model input use the verified fields. Probe candidates that
influenced the route, previews and counts are checked again at release.

### Disclosure checks

Immediately before each reranker or synthesis request, each JSON release and
each `context`, `delta` and `done` frame, the service checks the credential's
exact expiry without skew, the signing key's trust, the principal's standing and
the policy revision from the primary database, and every document and preview
that contributed. A response with no evidence is checked the same way. A
revision lower than one already observed is a lagging read and counts as
unavailable authority.

| Outcome | Before the first byte | After streaming began |
|---|---|---|
| Credential expired | `401` `unauthorized` | terminal `error` event, `unauthorized` |
| Grant, principal, document or preview changed | `403` `forbidden` | terminal `error` event, `forbidden` |
| Database, trust data or scope capability unavailable | `503` `unavailable` | terminal `error` event, `unavailable` |
| Scope above hosted capacity | `422` `scope_too_large` | not applicable |

Each outcome has a fixed message and carries no identifier or count. A failure
cancels upstream work, and nothing from the rejected disclosure is sent. The
decision for a send is bounded by its immediately preceding check. A read that
was authorized at dispatch can still be in flight when a grant changes, so a
revocation stops the next disclosure, not one already delivered or a request
already accepted by a model provider. Evidence-only queries never call a
reranker or a model. Responses are `Cache-Control: private, no-store`, and the
service keeps no shared retrieval, answer or preview cache. The Go client and the
stdio tools return only what the response carries.

### Residual channels

A shared approximate index can change recall or latency as its contents change
even when every returned row is authorized. Measure recall, p50 and p95
query-start and frame latency, and database statements per request for the
corpus, backend and scope sizes of the deployment, and compare them with
service mode. Use exact search over the authorized relation or separate indexes
for independent search behavior. Provider-side prompt caching and anything
already sent to a model provider are outside the service's control.

## Assign historical documents

An assignment of an existing document keeps the document's stored identity
policy, so a historical retry never registers a shared match once the document
is assigned. Its ownership starts unreadable and is published only by the
[scoped backfill](development.md#scoped-metadata-backfill), which verifies the
current chunks and vectors and requires the document row to match the assignment
snapshot. A change to the row or its chunks after the snapshot, including a
retry, leaves the document unreadable until the operator refreshes the
assignment. Vectors without a validated tenant and incarnation, including those
in legacy namespaces, are never eligible.

## Prepare a batch

Read the policy revision and capture a source snapshot for each document to
assign. Use an explicit tenant, owner and collection even when the collection
label is empty. The source version hashes the current document row, including
its effective incarnation. It binds the assignment to that snapshot. Content
deduplication and other stored document properties retain their service-mode
values. Chunk, object and index validation occur separately before readiness.

```sql
SELECT revision FROM grant_catalog_policy_revision;
SELECT * FROM grant_catalog_source_snapshot('10000000-0000-4000-8000-000000000004');
```

Pass one JSON object on standard input. The two required header fields are
`batch_id` and `expected_revision`. Each group is optional and at least one
entry is required across all groups. The synthetic example shows every group.
Use the incarnation and source version returned by the source snapshot in
place of the example values.

```json
{
  "batch_id": "10000000-0000-4000-8000-000000000001",
  "expected_revision": 0,
  "tenants": [
    {"tenant_id": "10000000-0000-4000-8000-000000000002"}
  ],
  "principals": [
    {"tenant_id": "10000000-0000-4000-8000-000000000002", "principal_id": "10000000-0000-4000-8000-000000000003", "issuer": "example-issuer", "subject": "owner", "enabled": true},
    {"tenant_id": "10000000-0000-4000-8000-000000000002", "principal_id": "10000000-0000-4000-8000-000000000008", "issuer": "example-issuer", "subject": "reader", "enabled": true}
  ],
  "collections": [
    {"tenant_id": "10000000-0000-4000-8000-000000000002", "collection": ""}
  ],
  "create_permissions": [
    {"tenant_id": "10000000-0000-4000-8000-000000000002", "principal_id": "10000000-0000-4000-8000-000000000003", "collection": "", "can_create": true}
  ],
  "assignments": [
    {"tenant_id": "10000000-0000-4000-8000-000000000002", "principal_id": "10000000-0000-4000-8000-000000000003", "collection": "", "document_id": "10000000-0000-4000-8000-000000000004", "source_version": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
  ],
  "grants": [
    {"tenant_id": "10000000-0000-4000-8000-000000000002", "principal_id": "10000000-0000-4000-8000-000000000008", "document_id": "10000000-0000-4000-8000-000000000004", "incarnation": "10000000-0000-4000-8000-000000000005", "can_read": true, "can_download": false}
  ],
  "reservations": [
    {"tenant_id": "10000000-0000-4000-8000-000000000002", "principal_id": "10000000-0000-4000-8000-000000000003", "collection": "", "document_id": "10000000-0000-4000-8000-000000000006"}
  ]
}
```

An exact issuer and subject pair binds to one principal and tenant. The
principal's tenant and identity are immutable. Set `enabled` to `false` to
disable its access. A collection is local to its tenant. Its create permission
permits uploads and retries for otherwise eligible documents. Owners can read,
sign, delete and retry their ready documents. Direct read grants cover query,
list and status, and adding download permission permits signing. Set both
grant flags to `false` to remove the grant. Set `can_create` to `false` to
revoke collection create permission.

Assignments require an enabled owner and the exact stored collection. A
pending assignment refresh updates the collection and source snapshot together
for the same owner and incarnation. A reservation requires an enabled principal
and an unused UUID. The catalog allocates a single intended incarnation per
reservation. An assignment spends a reservation only for the matching UUID,
incarnation, tenant, principal and collection. An explicit assignment of a
different incarnation marks the old reservation as superseded. Deleting an
assigned document removes its ownership and direct grants in the same
transaction. The incarnation history and reservation records survive UUID reuse.
Before truncating the document table, remove assigned rows through audited
deletion.

Use lowercase canonical, nonzero UUIDs, exact case for identity and collection
values, and a nonnegative signed 64-bit integer for the expected revision.
Each identity field is 1 to 1,024 UTF-8 bytes. Collection labels are 0 to 128
UTF-8 bytes. Create permissions and reservations require canonical service
labels in UTF-8 databases. Use stream-safe NFC, omit leading or trailing whitespace, and
exclude control and format characters. The empty label is valid. Collection and
assignment entries support exact historical labels. Both operator entry points
limit raw input to 256 KiB, reject duplicate JSON fields, and allow 500 entries
total. Use each target once per group.

## Validate, apply and recover

```sh
go run ./scripts/grant-catalog -dry-run < batch.json
go run ./scripts/grant-catalog < batch.json
```

Direct SQL and command previews use one database snapshot for the receipt,
revision and reference checks. A new batch returns the observed revision.
A previously committed batch returns its recorded revision
with `replayed: true`. Applying a valid batch commits all entries, the incremented revision
and the audit receipt together. Every new committed batch advances the revision once,
even when entries restate current values. The database rejects a stale revision
or an exhausted counter. The import command allows 30 seconds for database
work with a 25-second statement timeout and a five-second lock timeout.

```json
{"revision":0,"dry_run":true,"replayed":false}
{"revision":1,"dry_run":false,"replayed":false}
```

Retain the submitted batch until its outcome is confirmed. After an interrupted
or uncertain response, resubmit the same batch under the same login. A
committed batch returns its recorded revision with `replayed: true`. A changed
payload or login using that UUID is rejected. If the batch never committed and
its revision is stale, read the current revision, capture fresh snapshots, then
prepare and preview a new batch with a new UUID. Invalid and failed batches
leave policy and revision intact. An unexpected SQLSTATE can be investigated
from the operator's database records without copying batch contents into logs.
