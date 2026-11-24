# Grant catalog

The grant catalog stages tenant-bound access in the metadata database. Quarry
serves HTTP, Go client and MCP requests under the existing service credentials.
ownership imports remain pending until document validation can publish
readiness.

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
GRANT SELECT, INSERT, UPDATE, DELETE ON public.documents, public.chunks,
  public.ingest_jobs, public.document_fingerprints TO quarry_service;
GRANT SELECT (revision) ON public.grant_catalog_policy_revision TO quarry_operator;
GRANT EXECUTE ON FUNCTION public.grant_catalog_source_snapshot(uuid) TO quarry_operator;
GRANT EXECUTE ON FUNCTION public.grant_catalog_import_batch(text, boolean) TO quarry_operator;
GRANT SELECT ON public.grant_catalog_policy_audit TO quarry_auditor;
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

The serving process validates the provisioned vector dimensions and indexes
without requiring ownership or schema creation privileges.

After renaming a corpus schema, reconnect with the new schema first in the
search path. Catalog entry points and document cleanup stay with the renamed
corpus.

The serving login needs zero catalog privileges. The catalog owner and database
administrators have administrative access. Give each operator an internal
database login name and avoid shared operator logins. The audit records the
authenticated PostgreSQL session login, revision, batch UUID, payload digest
and timestamp. Deleting an assigned document records the login and deleted
identity alongside the resulting revision. Keep the audit and backups
restricted and retain the audit, including import receipts, with the catalog.

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
