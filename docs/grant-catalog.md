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
makes the authority unavailable. Each trust check enforces the file policy and
hashes the bytes it reads. A stalled filesystem read can delay its caller while
other checks of unchanged trust data proceed independently. Each replica
serializes a trust change through a candidate reread, a successful
`trust_key_load` allow audit and a final reread of the same unexpired bytes
before using them. Subject admissions and readiness checks return `503` during
a pending transition or when trust data is unavailable. A subsequent check can
retry, and liveness continues to report process availability. Replicas can use
the same trust files and audit schema during an upgrade. Roll back only to a
build that audits trust changes before publishing them. After resolving the
principal, the gate checks that the current key set is still valid and contains
the signing key.

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
  public.document_deletion_active_shared(),
  public.document_deletion_reserved(uuid), public.hosted_vector_revision_visible(uuid, uuid) TO quarry_service;
GRANT SELECT (revision) ON public.grant_catalog_policy_revision TO quarry_operator;
GRANT EXECUTE ON FUNCTION public.grant_catalog_source_snapshot(uuid) TO quarry_operator;
GRANT EXECUTE ON FUNCTION public.grant_catalog_import_batch(text, boolean) TO quarry_operator;
GRANT EXECUTE ON FUNCTION public.grant_catalog_authorize_ingest(uuid, uuid, text, uuid),
  public.grant_catalog_lock_ingest_permission(uuid, uuid, text),
  public.grant_catalog_bind_accepted_document(uuid, uuid, text, uuid, uuid, boolean),
  public.grant_catalog_publish_accepted_document(uuid, uuid),
  public.grant_catalog_document_tenant(uuid, uuid),
  public.grant_catalog_policy_state(uuid, uuid),
  public.grant_catalog_inventory_policy_state(uuid, uuid),
  public.grant_catalog_list_documents(uuid, uuid, uuid, integer, text, boolean),
  public.grant_catalog_get_document(uuid, uuid, uuid, boolean),
  public.grant_catalog_authorize_file(uuid, uuid, uuid),
  public.grant_catalog_lock_file_principal(uuid, uuid),
  public.grant_catalog_authorize_file_advisory(uuid, uuid, uuid),
  public.grant_catalog_authorize_delete(uuid, uuid, uuid, uuid),
  public.grant_catalog_record_deletion_owner(uuid, uuid, uuid, uuid),
  public.grant_catalog_read_revision(uuid, uuid),
  public.grant_catalog_corpus_authz_mode(),
  public.grant_catalog_record_security_audit(text, uuid, uuid, bigint, text, text, text, uuid, uuid) TO quarry_service;
GRANT SELECT ON public.grant_catalog_active_principals, public.grant_catalog_readable_documents,
  public.grant_catalog_document_sets_live, public.grant_catalog_document_set_readable TO quarry_service;
GRANT EXECUTE ON FUNCTION public.grant_catalog_backfill_candidates(uuid, integer),
  public.grant_catalog_publish_verified_document(uuid, uuid, text, text),
  public.grant_catalog_activate_grants(boolean),
  public.grant_catalog_create_document_set(uuid, uuid, integer, text, uuid[], integer),
  public.grant_catalog_purge_document_sets(integer) TO quarry_operator;
GRANT SELECT ON public.grant_catalog_policy_audit, public.grant_catalog_security_audit TO quarry_auditor;
```

If serving replicas will enable the source upload journal, also apply the
[source upload journal database grants](deployment.md#source-upload-journal-database-grants).

Replicas that set `HOSTED_VECTOR_MANIFEST=true` also record and sweep hosted
vector revisions, so they need access to the three tables that hold them. Apply
this block only for those replicas. Retirement runs in owner-defined triggers,
and readers consult revisions through `hosted_vector_revision_visible`.

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON public.hosted_vector_manifests,
  public.hosted_vector_revisions, public.hosted_vector_cleanups TO quarry_service;
```

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

Before deploying binaries that support tagged documents on pgvector, apply the
following grant to restricted pgvector serving and qualified cleanup logins.
They check for active and in-progress hosted revisions before recording a local
deletion receipt for a tagged document.

```sql
GRANT SELECT (document_id, incarnation, state) ON public.hosted_vector_manifests TO quarry_service;
```

Whole-table `SELECT, INSERT, UPDATE, DELETE` grants on `chunk_vectors` and
`chunk_vectors_<generation>` automatically cover the `pages`, `table_ids`, and
`row_ids` columns added by migration `039_vector_layout_provenance.sql`.
Installations that grant column-level privileges instead of whole-table access
must grant `SELECT`, `INSERT`, and `UPDATE` on the layout columns (alongside
`DELETE` on the table) for serving and maintenance roles before starting
upgraded binaries:

```sql
-- Column-restricted serving or maintenance upgrade for migration 039
GRANT SELECT (chunk_id, document_id, chunk_index, doc_type, collection, text, kind, quality, token_count, embedding, pages, table_ids, row_ids),
  INSERT (chunk_id, document_id, chunk_index, doc_type, collection, text, kind, quality, token_count, embedding, pages, table_ids, row_ids),
  UPDATE (document_id, chunk_index, doc_type, collection, text, kind, quality, token_count, embedding, pages, table_ids, row_ids),
  DELETE ON public.chunk_vectors TO quarry_service;
```

The metadata owner applies the vector function grants after migrating a new or
existing corpus. On first startup, the serving process opens the configured
target and binds its identity through the owner-installed initialization
function. Subsequent startups validate the recorded binding. The writer-state
function lets serving transactions participate in the freeze. Restrict direct
generation table access, freezes, verification approval and activation to the
maintenance login. A non-owner maintenance login running `make reindex` also
requires `EXECUTE` on `grant_catalog_document_tenant(uuid, uuid)` along with
the schema, metadata, generation and vector table privileges listed in
[vector reindex database privileges](deployment.md#vector-reindex-database-privileges).

Before starting serving replicas for a promoted pgvector generation, the
table owner grants the same table privileges on that generation's
`chunk_vectors_<generation>` table. If exact-scope search is enabled, prepare
its indexes with the table owner login before switching replicas. The serving
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

Apply each step after the schema owner applies the named migration and before
starting upgraded processes. Substitute the deployed schema and serving role
names as needed.

- Migrations `019_evaluation_fixture_guards.sql` and
  `023_deletion_predicates.sql` add owner-defined predicates and the deletion
  receipts. Ordinary admission, job leasing, removal and the deletion-mode check
  use them even when evaluation fixtures are disabled and checkpointed deletion
  is inactive. Guarded ID-only deletion can also insert and update receipts.
  The serving login reads the activation marker through owner-defined functions
  and needs no privilege on the marker table.
- Migration `025_hosted_vector_manifest.sql` adds the predicate that readers use
  to match a hit with the document's active hosted revision. Every query uses
  it, even when `HOSTED_VECTOR_MANIFEST` is disabled. Replicas that enable the
  flag also need the full manifest write and sweep grants shown above.

```sql
-- Serving role upgrade for migrations 019, 023 and 025
GRANT SELECT, INSERT, UPDATE ON public.document_deletions TO quarry_service;
GRANT EXECUTE ON FUNCTION public.evaluation_fixture_claimable(uuid, text),
  public.evaluation_fixture_reserved(uuid), public.document_deletion_active(),
  public.document_deletion_reserved(uuid), public.hosted_vector_revision_visible(uuid, uuid) TO quarry_service;
```

The serving login already holds `SELECT` on `public.document_pdf_extractions`
(shown above), which the `SECURITY INVOKER` layout claim trigger added in
`040_layout_pdf_claim_capability.sql` also uses; no new runtime grants are
required beyond those base privileges.

After applying `030_deletion_mode_lock.sql` and before starting this release
with a hosted or offline vector store in any authorization mode, grant execution
of `document_deletion_active_shared`. Admission to a backend that supports
incarnation-tagged vectors reads the marker through this function when
`HOSTED_VECTOR_MANIFEST` is off, even while checkpointed deletion is inactive.
Admission with PostgreSQL vectors does not call this function.

```sql
-- Serving role upgrade for migration 030
GRANT EXECUTE ON FUNCTION public.document_deletion_active_shared() TO quarry_service;
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
[authorized retrieval](#authorized-retrieval). These grants are needed before enabling their respective features.

After applying `029_authz_corpus_mode.sql`, grant the serving login execution of
`grant_catalog_corpus_authz_mode()` before starting upgraded processes. Readiness,
data routes and the writer gate read it even with `AUTHZ_MODE=service`. Grant
`grant_catalog_record_security_audit(text, uuid, uuid, bigint, text, text, text, uuid, uuid)`
before enabling grants mode.

```sql
-- Serving role upgrade for migration 029
GRANT EXECUTE ON FUNCTION public.grant_catalog_corpus_authz_mode() TO quarry_service;
GRANT EXECUTE ON FUNCTION public.grant_catalog_record_security_audit(text, uuid, uuid, bigint, text, text, text, uuid, uuid) TO quarry_service;
```

After applying `031_document_sets.sql`, grant the serving login read access to the
two live set relations before enabling [document sets](#document-sets), and
grant each operator execution of the two set functions. Neither login receives
any privilege on the set tables.

```sql
-- Role upgrade for migration 031
GRANT SELECT ON public.grant_catalog_document_sets_live, public.grant_catalog_document_set_readable TO quarry_service;
GRANT EXECUTE ON FUNCTION public.grant_catalog_create_document_set(uuid, uuid, integer, text, uuid[], integer),
  public.grant_catalog_purge_document_sets(integer) TO quarry_operator;
```

Pause operator grant imports and let in-flight imports finish before applying
`034_scoped_file_advisory.sql`. Apply the migration as schema owner, grant the
following permissions, then deploy upgraded serving replicas. Operator imports
can resume after the migration and keep their existing entry point. Keep the
existing file authorization grant for older replicas and rollback.

```sql
GRANT EXECUTE ON FUNCTION public.grant_catalog_lock_file_principal(uuid, uuid),
  public.grant_catalog_authorize_file_advisory(uuid, uuid, uuid) TO quarry_service;
```

After applying `037_inventory_policy_revisions.sql`, grant the serving login
execution of the new inventory state function before starting upgraded
processes. Keep execution of `grant_catalog_policy_state` for older replicas
and binary rollback. Both inventory revision tables are private.
During database-owner maintenance on a serving corpus, use ordinary catalog
row writes with the inventory constraint deferred and the capture triggers
enabled.

```sql
GRANT EXECUTE ON FUNCTION public.grant_catalog_inventory_policy_state(uuid, uuid) TO quarry_service;
```

## Owned ingestion

In `AUTHZ_MODE=grants`, `POST /ingest` uses owned ingestion for a verified
principal. This path requires `INGEST_CONTENT_DEDUP=off` and a store that
supports owners.

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
   incarnation and the attempt. With `SOURCE_UPLOAD_JOURNAL=true`, an owned
   multipart upload stages a journal row before writing that key and leaves
   unadopted uploads for [reclamation](development.md#source-upload-reclamation-scriptsreconcile).
   With the journal disabled, a refused acceptance removes only its own
   attempt's object. After an uncertain acceptance, it keeps the object when
   the stored key matches the attempt or the lookup fails.

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
collection is a conflict. In mixed PDF OCR mode, an owner's retry of a
historical PDF whose stored identity is its parser text uses the bounded legacy
parser once more after acceptance confirms current ownership and create
permission. The second acceptance checks both again. Matching text follows the
usual retry rules, and different or missing text is a conflict. Only the owner
sees these outcomes.
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
deletion for a verified principal in grants mode. Every scoped response and error sets
`Cache-Control: private, no-store`, and handlers refuse to fall back to
unscoped stores (`503`).

- **Inventory and status:** Listing and status lookups apply the same-tenant
  ownership or `read` grant inside SQL before ordering, counting, limiting or
  streaming, exposing accepted documents in `processing`, `failed` and
  `completed` states and verified historical assignments while excluding
  unverified legacy rows and documents in `deleting` state. An omitted or empty
  `collection` filters across the caller's tenant without granting default
  collection access, and `after` acts only as an exclusive UUID cursor. Each
  tenant and principal has an inventory revision that starts at zero and
  advances in the transaction that changes their identity, activity, effective
  create rights, grants, ownership or document visibility. The primary
  database recheck compares this revision and principal activity before
  releasing a page, status lookup or streaming batch. Imports, query
  authorization and audit continue to use the catalog-wide revision. Raw
  `s3_key` values are omitted and
  internal `last_error` messages on failed rows are mapped to a bounded public
  category (`no_text`, `ocr_result_expired`, `ocr_text_too_large`,
  `embedding_limit`, `no_chunks`, `ocr_failed` or `processing_failed`).
- **Original-file signing:** The store checks active principal status and
  ownership or `read` plus `download` grants directly before signing. Before
  checkpointed deletion is active, shared document and principal row locks
  protect this check. Once active, signing takes shared advisory locks for
  the document intent key and then the tenant/principal key. It reads
  authorization in a fresh statement after both waits and holds the keys
  through the commit of the mandatory allow audit. Checkpointed deletion takes
  the document key exclusively. Grant imports take distinct affected principal
  keys exclusively in sorted order before locking documents or policy revision.
  A mutation that wins the key is visible to the subsequent signing check.
  A mutation that waits for signing can take effect only after the signing
  transaction ends. Already issued presigned addresses retain their original
  expiry. The advisory path avoids authorization tuple-lock writes and their
  multixacts, while the allow audit still writes WAL and commits. Apply
  principal and grant changes through the operator import. For trusted
  owner-only SQL that bypasses these locks, drain scoped signing and keep it
  paused until the change has committed. The requested presigned URL lifetime
  is the smaller of `900` seconds and the whole
  seconds remaining before token expiry, and signing is refused (`401`) when
  fewer than one second remains. A missing, unassigned, unverified, denied,
  read-only, deleting, absent-original or storage-disabled lookup returns the
  same `404` envelope with zero signing calls.
- **Owner-only deletion:** See [the grants-mode API contract](api.md#delete-documentsdoc_id).
  Authorization and the incarnation-pinned
  `document_deletions` intent commit atomically under the caller's request and
  credential deadline before detached cross-store cleanup runs; confirmed
  cancellation or token expiry before commit leaves the document intact and makes
  zero vector or object-store calls (`503` or `401`). When the intent `COMMIT`
  reply is lost, a bounded outcome check inspects the PostgreSQL transaction
  status, server instance identity and exact `(document_id, incarnation)`
  qualified intent receipt on the primary writer: a confirmed commit resumes
  detached cleanup for that exact incarnation without affecting any replacement
  UUID (and credential expiry during detached cleanup does not reclassify a
  committed intent to `401`), a confirmed rollback keeps the precommit failure,
  and an unconfirmed outcome returns a retryable `500` (`commit_unconfirmed` in
  the security audit log) without exposing internal identifiers or touching
  external stores.

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

## Document sets

A document set is an immutable, expiring list of document incarnations that one principal can select in a query. It lets a caller name up to 100,000 documents with one UUID, where an inline `allowed_document_ids` list would compete with the 1 MiB request body and the [`MAX_ALLOWED_DOCUMENT_IDS`](configuration.md#environment-variables) limit. The [query contract](api.md#document-sets) defines the request field and its responses. A set is only a selector. It gives its holder no permission, and every use intersects it with the owner's live read grants.

Sets are off by default. `DOCUMENT_SETS_ENABLED=true` requires `AUTHZ_MODE=grants` and a vector store that can confine a search to a set, which is pgvector or Pinecone. A shared application credential identifies the calling tier and never a person, so in `service` mode two end users behind one credential could not be told apart and a reference would become a transferable capability. Keep a large selection in the calling application until the deployment verifies subjects.

Enable the setting only after every replica runs a build that accepts `document_set_id`. An older build rejects the field as unknown, so a request that reaches it fails with `400`.

### Create a set

Run the separate operator command with the operator login in `DATABASE_URL`. It reads a manifest from standard input and prints the new reference as JSON.

```sh
go run ./scripts/document-sets -tenant <tenant UUID> -owner <principal UUID> \
  -ttl 1h -import-key nightly-report < manifest.txt
```

The manifest holds one document UUID per line, in the hyphenated form, with LF or CRLF line endings. An empty manifest creates an empty set. The command refuses a blank line, a value that is not a UUID in its 36-character hyphenated form and a nil UUID. It refuses a manifest of more than 8 MiB, and more raw entries than `DOCUMENT_SET_MAX_MEMBERS`, before it canonicalizes anything. It then deduplicates the identifiers and connects. All of this happens before any database work.

The server assigns the set an opaque UUID and an immutable revision, and records the tenant, the owning principal, the operator login and the expiry. `-ttl` defaults to one hour and cannot exceed 24 hours. Creation is one transaction. Every member must be a completed document that the owner may read now, and its current incarnation is stored with composite tenant constraints and unique membership. An unknown, foreign, unfinished or unreadable document fails the whole creation with one message that does not say which member or why, so no partial set is ever published. An owner that is unknown or disabled in the tenant also fails creation.

- An owner can hold at most 16 unexpired sets. Creations for one owner run one at a time, so concurrent calls cannot exceed the limit.
- `-import-key` names one set per owner. An identical retry, meaning the same members, returns the original reference with `replayed: true` and its original expiry. Changed members under the same key fail with `import key conflict`. A key is free again once its set has expired.
- Each creation also removes up to four of the owner's expired sets. `-purge-expired` alone removes expired sets of every owner in bounded steps and prints how many it removed.

### Use a set

The service resolves the reference by tenant and authenticated principal. An inactive principal receives the usual `403` before any set is resolved. A missing, foreign, deleted or expired set returns the same `404`. Deleting a document removes it from every set, and a document recreated under the same UUID has a new incarnation that no set names.

The documents searched are the set's membership intersected with the principal's live read grants, each document's current incarnation, the inline list, the collection, the type and the completion rules. The same relations decide candidate checks, routing previews and every later release, so a document outside the set can neither be previewed nor released even if a store returned it.

- pgvector joins the set, the live grants and the document incarnation inside the ranking statement, before ordering and `LIMIT`. Nothing lists the membership, and the exact-scope probe, exact ranking and approximate ranking use the same join. The result is exact in scope. Selective sets can still return fewer eligible chunks than requested or miss nearer eligible chunks when the planner uses an approximate index.
- Pinecone receives the intersection as an explicit member list, in partitions of at most 10,000 documents. Every request carries the tenant and any collection and type filter, and each predicate holds at most 10,000 values. The query embeds once. The service searches one partition at a time and buffers at most the requested number of hits per partition, then merges the verified hits by descending vector score and ascending chunk ID, deduplicates across partitions and truncates only after merging. The optional hosted refill is not used for a set query.
- Each request spends from one budget of `DOCUMENT_SET_MAX_BATCHES` hosted attempts, counting the probe, the local pass, the global pass, the sparse top-up and every retry. Transport retries are disabled, so every retry is counted. The route is priced before anything is embedded: its first attempts never exceed the priced total, and retries may spend only what that total leaves. See the [query contract](api.md#document-sets) for the pricing and the `422` that an unaffordable explicit route returns.
- A timeout or failed partition fails the whole request.

Before each hosted batch, each reranker or model request, each JSON release and each streamed frame, the service checks the credential, key trust, principal and policy revision, and that the set is still live under the same revision. The database clock decides expiry, and a set past its recorded expiry by the service's own clock is also unusable. A set that lapses mid-request ends it with the same `404` as before the first byte, or with the terminal `error` event once streaming began. Nothing from the rejected disclosure is sent.

### Tune pgvector exact search

Use the pgvector document-set recall evaluation in the development guide to
decide whether to set `PGVECTOR_EXACT_SCOPE_MAX` on deployment data. A member
can account for multiple eligible chunks, so the cutoff uses chunk counts.
The serving index is cosine HNSW. The comparison also measures a separate,
fixture-only cosine IVFFlat index. Natural plans may fall back to non-ANN
paths, and forced index figures describe a different planner setting.

For the reference rule, consider each positive cutoff among 100, 1,000,
10,000 and 100,000 chunks. Include every nonempty configured-strategy cell at
k=50 with eligible chunks at or below the cutoff, across both layouts and
both sample types. At least one such cell must exist. Every included cell
must select exact search, have perfect strict recall, have zero timeouts, and
meet a p95 full-query latency of 50 ms or less. At least one of those cells
must have mean strict recall below 0.95 on verified HNSW plans at the default
threshold. The evaluation reports the largest passing cutoff, or 0 when
none passes. The 50 ms budget is a reference for the fixture. Profile the
same rule with deployment data and its latency budget.

The default is 0. Reference corpus measurements are pending.

## Assign historical documents

An assignment of an existing document keeps the document's stored identity
policy, so a historical retry never registers a shared match once the document
is assigned. Its ownership starts unreadable and is published only by the
[scoped backfill](development.md#scoped-metadata-backfill), which verifies the
current chunks and vectors. Refresh the assignment if a bound document property
changes. Verify again if the chunk set changes and wait for outstanding vector
mutations to settle before publishing readiness. Vectors require a validated
tenant and incarnation, including vectors in legacy namespaces.

## Prepare a batch

Read the policy revision and capture a source snapshot for each document to
assign. Use an explicit tenant, owner and collection even when the collection
label is empty. The source version is an opaque SHA-256 digest encoded as 64
lowercase hexadecimal characters. It binds the assignment to the following
document properties.

- Document ID, effective incarnation (the document ID for a historical row with
  no stored incarnation), collection and document type.
- Fingerprint, content deduplication policy, content, metadata, content preview
  and text source.
- Original filename, file type and object key.
- Status, chunk count, creation and completion timestamps, scoped vector
  format, vector attempt and the required-incarnation flag.

The digest preserves the distinction between null and empty values. Unrelated
schema additions, assignment markers, untagged-vector stamps, visibility
observations, error text, generic update timestamps and lease or vector mutation
bookkeeping leave it stable.

```sql
SELECT revision FROM grant_catalog_policy_revision;
SELECT * FROM grant_catalog_source_snapshot('10000000-0000-4000-8000-000000000004');
```

### Upgrade the source version

Before applying migration 043, pause source snapshots, grant imports and scoped
backfill. Apply the migration before rolling out compatible binaries. Replicas
sharing the database then use the same digest through the existing SQL functions.
Older binaries accept the same digest format and ignore migration history for
versions they do not embed. Existing ready access, grants and audit records are preserved. A committed batch
can still be replayed with the same login, batch ID and equivalent JSON payload.

For an unapplied batch or a pending imported assignment, read the current policy
revision and take fresh snapshots. For a pending import, confirm the owner,
tenant and incarnation still match the assignment and use the current collection.
Preview and apply a fresh batch ID to record the refresh before rerunning scoped
verification. If the source snapshot or policy revision changes, recapture and
validate it before submitting again. For a pre-upgrade assignment that is
already ready, omit its assignment entry from new batches and supply the current
incarnation on any grant entries. Resume the paused jobs after the cutover.

Keep migration 043 applied when rolling back the binary. Restoring a database
backup taken before the migration restores the old digest and policy state.
Prepare fresh batches for uncommitted work against the restored database. Keep
the view in place while import or backfill jobs are active.

### Batch format

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

---

## Corpus cutover and security audit

Migration `029_authz_corpus_mode.sql` records the corpus-level authorization mode
in `grant_catalog_corpus_mode` (`service` by default, fenced to `grants` once activated)
and creates the append-only security audit log `grant_catalog_security_audit`:

- Serving replicas check `grant_catalog_corpus_authz_mode()` on `/readyz`, HTTP data routes,
  and database gates (`ErrAuthzModeMismatch`). Both `AUTHZ_MODE=service` and
  `AUTHZ_MODE=grants` replicas fail readiness and admissions whenever the replica configuration
  differs from the database corpus marker. Readiness caches the comparison for
  `HEALTH_CACHE_TTL`; data routes and writer gates check it independently.
- Follow the [grants cutover deployment sequence](deployment.md#grants-cutover)
  before previewing or activating grants. Migration 038 requires checkpointed
  deletion to be active for both operations, including a replay.
- Preview and activate `grants` mode with the operator command (see [Database roles](#database-roles)):

```sh
go run ./scripts/grant-catalog -activate-grants -dry-run
go run ./scripts/grant-catalog -activate-grants
```

Activating `grants` locks the catalog, advances `grant_catalog_policy_revision`, records a
`corpus_cutover` row in `grant_catalog_security_audit`, and is idempotent if re-run
(`already_active: true`). A failed activation leaves mode, policy revision and
audit unchanged. Historical documents without verified ownership remain quarantined
and invisible in `grants` mode until reconciled via [Assign historical documents](#assign-historical-documents).

### Recovery and retention

For rollback, close ingress and keep both activation markers. Redeploy only a
build that enforces scoped incarnation-qualified deletion with `AUTHZ_MODE=grants`.
Service-mode replicas remain unready. Returning to service-only trust requires a
separately authorized trust-boundary change: an isolated legacy corpus, or an
operator migration and a trusted gateway enforcing every operation.

A restored backup carries its own marker. A backup from before cutover leaves
grants replicas unready: re-fence older binaries, re-apply the final ownership
delta, and follow the [grants cutover sequence](deployment.md#grants-cutover)
before reopening ingress. A backup from after cutover keeps service replicas fenced. An interrupted cutover rolls back its
single transaction, leaving the marker at `service`; rerunning is idempotent
and reports `already_active` once committed.

The deployment determines audit retention. Only the schema owner can prune or
archive audit rows by `recorded_at`; serving, operator and auditor logins cannot
delete them.

`grant_catalog_security_audit` stores bounded actor fields (`tenant_id`, `principal_id`, or
`operator`), `policy_revision`, `action`, `outcome`, bounded `reason`, and `request_id` without
raw `sub`, credentials, presigned URLs, or text excerpts:

- Starting with migration `035`, `grant_catalog_record_security_audit(...)`
  accepts only `ingest`, `query`, `document_list`, `document_status`, `file_sign`,
  `document_delete`, `subject_auth`, and `trust_key_load` with an outcome of
  `allow`, `deny`, or `error`. The function leaves `operator` NULL on runtime rows.
- Ingest acceptance, file signing, and deletion intents record allow events in
  their PostgreSQL transactions. Subject authentication, queries, and document
  listing and status fail closed if required allow audit records fail, and
  record denials or errors.
- Owner-defined operator functions and the policy-audit mirror (`mirror_policy_audit`) write
  `corpus_cutover` and `grant_import` rows with a named operator in the same
  transaction as the change. A historical row with either action and a NULL
  `operator` provides no evidence of an operator action. The mirror also records
  ownership deletion as `document_delete` with reason `ownership_deleted` and
  the acting login in `operator`. A serving deletion intent has a NULL `operator`.
- Grant `SELECT` on `grant_catalog_security_audit` to `quarry_auditor` only. The
  schema owner can modify audit rows. The serving login writes runtime events
  through the audit function.

### Operational telemetry

In grants mode, operational error fields in logs and trace exceptions and status
descriptions use three categories: `operation failed`, `operation canceled`, and
`operation timed out`. SQL traces omit statement text and record only a recognized
SQL verb or `OTHER`. Panic and extraction child diagnostics use fixed log messages.
Grants-mode telemetry omits document, object, and upload identifiers and disables
content capture. API responses, restricted audit records, and durable ingestion
and cleanup diagnostics keep their existing formats.

Upgrade every grants-mode replica and worker to apply this behavior across the
deployment. A binary rollback restores that binary's previous diagnostic rendering.
Telemetry already exported stays subject to the destination's retention policy.
