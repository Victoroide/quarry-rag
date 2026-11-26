-- Track the owner who committed a qualified deletion intent so retries with
-- X-Document-Incarnation after finalization only acknowledge that owner.
CREATE TABLE grant_catalog_deletion_receipts (
    document_id UUID NOT NULL,
    incarnation UUID NOT NULL,
    tenant_id UUID NOT NULL,
    principal_id UUID NOT NULL,
    PRIMARY KEY (document_id, incarnation),
    FOREIGN KEY (document_id, incarnation) REFERENCES grant_catalog_assigned_incarnations (document_id, incarnation),
    FOREIGN KEY (tenant_id, principal_id) REFERENCES grant_catalog_principals (tenant_id, principal_id)
);

-- Report whether a principal is active and the current policy revision in one snapshot.
CREATE FUNCTION grant_catalog_policy_state(p_tenant UUID, p_principal UUID)
RETURNS TABLE (active BOOLEAN, policy_revision BIGINT)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT EXISTS (
        SELECT 1 FROM grant_catalog_active_principals p
        WHERE p.tenant_id = p_tenant AND p.principal_id = p_principal
    ),
    (SELECT r.revision FROM grant_catalog_policy_revision r WHERE r.singleton);
END;

-- List authorized summaries inside the caller's tenant before sorting, counting
-- or paginating. Raw object keys are never returned.
CREATE FUNCTION grant_catalog_list_documents(
    p_tenant UUID, p_principal UUID, p_after UUID, p_limit INTEGER, p_collection TEXT, p_include_incarnation BOOLEAN)
RETURNS TABLE (
    id TEXT, doc_type TEXT, collection TEXT, status TEXT, chunk_count INTEGER,
    file_name TEXT, file_type TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ, last_error TEXT, index_visibility TEXT,
    index_visibility_observed_at TIMESTAMPTZ, pdf_coverage TEXT, incarnation TEXT
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT d.id::text, d.doc_type, d.collection, d.status, d.chunk_count,
        d.file_name, d.file_type, d.created_at, d.updated_at,
        d.completed_at, d.last_error, COALESCE(d.index_visibility, 'unknown'),
        d.index_visibility_observed_at,
        COALESCE((SELECT e.coverage FROM document_pdf_extractions e WHERE e.document_id = d.id), ''),
        CASE WHEN p_include_incarnation THEN COALESCE(d.incarnation, d.id)::text ELSE '' END
    FROM documents d
    JOIN grant_catalog_document_ownership o
      ON o.document_id = d.id AND o.incarnation = COALESCE(d.incarnation, d.id)
     AND o.collection = d.collection
    JOIN grant_catalog_active_principals p
      ON p.tenant_id = p_tenant AND p.principal_id = p_principal
    WHERE d.status <> 'deleting' AND d.grant_catalog_assigned
      AND o.tenant_id = p_tenant
      AND (o.ready OR o.origin = 'accepted')
      AND (o.principal_id = p_principal OR EXISTS (
          SELECT 1 FROM grant_catalog_document_grants g
          WHERE g.tenant_id = o.tenant_id AND g.document_id = o.document_id
            AND g.incarnation = o.incarnation AND g.principal_id = p_principal AND g.can_read
      ))
      AND (p_after IS NULL OR d.id > p_after)
      AND (p_collection IS NULL OR p_collection = '' OR o.collection = p_collection)
    ORDER BY d.id
    LIMIT LEAST(GREATEST(p_limit, 1), 1000);
END;

-- Look up one authorized document summary without returning raw object keys.
CREATE FUNCTION grant_catalog_get_document(
    p_tenant UUID, p_principal UUID, p_document UUID, p_include_incarnation BOOLEAN)
RETURNS TABLE (
    id TEXT, doc_type TEXT, collection TEXT, status TEXT, chunk_count INTEGER,
    file_name TEXT, file_type TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ, last_error TEXT, index_visibility TEXT,
    index_visibility_observed_at TIMESTAMPTZ, pdf_coverage TEXT, incarnation TEXT
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT d.id::text, d.doc_type, d.collection, d.status, d.chunk_count,
        d.file_name, d.file_type, d.created_at, d.updated_at,
        d.completed_at, d.last_error, COALESCE(d.index_visibility, 'unknown'),
        d.index_visibility_observed_at,
        COALESCE((SELECT e.coverage FROM document_pdf_extractions e WHERE e.document_id = d.id), ''),
        CASE WHEN p_include_incarnation THEN COALESCE(d.incarnation, d.id)::text ELSE '' END
    FROM documents d
    JOIN grant_catalog_document_ownership o
      ON o.document_id = d.id AND o.incarnation = COALESCE(d.incarnation, d.id)
     AND o.collection = d.collection
    JOIN grant_catalog_active_principals p
      ON p.tenant_id = p_tenant AND p.principal_id = p_principal
    WHERE d.id = p_document AND d.status <> 'deleting' AND d.grant_catalog_assigned
      AND o.tenant_id = p_tenant
      AND (o.ready OR o.origin = 'accepted')
      AND (o.principal_id = p_principal OR EXISTS (
          SELECT 1 FROM grant_catalog_document_grants g
          WHERE g.tenant_id = o.tenant_id AND g.document_id = o.document_id
            AND g.incarnation = o.incarnation AND g.principal_id = p_principal AND g.can_read
      ));
END;

-- Authorize owner or read-plus-download access for one live document while
-- holding shared locks in documents -> grant_catalog_principals order.
CREATE FUNCTION grant_catalog_authorize_file(p_tenant UUID, p_principal UUID, p_document UUID)
RETURNS TABLE (active BOOLEAN, s3_key TEXT)
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT d.id FROM documents d WHERE d.id = p_document FOR SHARE;

    SELECT EXISTS (
        SELECT 1 FROM grant_catalog_principals pr
        WHERE pr.tenant_id = p_tenant AND pr.principal_id = p_principal AND pr.enabled
        FOR SHARE
    ),
    (
        SELECT d.s3_key
        FROM documents d
        JOIN grant_catalog_document_ownership o
          ON o.document_id = d.id AND o.incarnation = COALESCE(d.incarnation, d.id)
         AND o.collection = d.collection AND o.tenant_id = p_tenant
         AND (o.ready OR o.origin = 'accepted')
        JOIN grant_catalog_principals pr
          ON pr.tenant_id = p_tenant AND pr.principal_id = p_principal AND pr.enabled
        WHERE d.id = p_document AND d.status <> 'deleting' AND d.grant_catalog_assigned
          AND (o.principal_id = p_principal OR EXISTS (
              SELECT 1 FROM grant_catalog_document_grants g
              WHERE g.tenant_id = o.tenant_id AND g.document_id = o.document_id
                AND g.incarnation = o.incarnation AND g.principal_id = p_principal
                AND g.can_read AND g.can_download
          ))
    );
END;

-- Authorize owner-only deletion or an owner's qualified retry after finalization.
-- Callers lock documents FOR UPDATE before invoking this function.
CREATE FUNCTION grant_catalog_authorize_delete(
    p_tenant UUID, p_principal UUID, p_document UUID, p_expected_incarnation UUID)
RETURNS TABLE (active BOOLEAN, authorized BOOLEAN, finalized_match BOOLEAN)
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT TRUE FROM grant_catalog_principals pr
    WHERE pr.tenant_id = p_tenant AND pr.principal_id = p_principal AND pr.enabled
    FOR SHARE;

    SELECT
        EXISTS (
            SELECT 1 FROM grant_catalog_active_principals pr
            WHERE pr.tenant_id = p_tenant AND pr.principal_id = p_principal
        ),
        EXISTS (
            SELECT 1 FROM documents d
            JOIN grant_catalog_document_ownership o
              ON o.document_id = d.id AND o.incarnation = COALESCE(d.incarnation, d.id)
             AND o.collection = d.collection
            JOIN grant_catalog_active_principals pr
              ON pr.tenant_id = p_tenant AND pr.principal_id = p_principal
            WHERE d.id = p_document AND d.grant_catalog_assigned
              AND o.tenant_id = p_tenant AND o.principal_id = p_principal
              AND (o.ready OR o.origin = 'accepted')
        ),
        (p_expected_incarnation IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.id = p_document)
            AND EXISTS (
                SELECT 1 FROM document_deletions r
                JOIN grant_catalog_deletion_receipts o
                  ON o.document_id = r.document_id AND o.incarnation = r.incarnation
                JOIN grant_catalog_active_principals pr
                  ON pr.tenant_id = p_tenant AND pr.principal_id = p_principal
                WHERE r.document_id = p_document AND r.incarnation = p_expected_incarnation
                  AND r.kind = 'qualified_intent' AND r.finalized
                  AND o.tenant_id = p_tenant AND o.principal_id = p_principal
            )
        );
END;

-- Record the verified owner of a committed deletion intent so qualified retries
-- after document removal stay restricted to that owner.
CREATE FUNCTION grant_catalog_record_deletion_owner(
    p_tenant UUID, p_principal UUID, p_document UUID, p_incarnation UUID)
RETURNS VOID
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    INSERT INTO grant_catalog_deletion_receipts (document_id, incarnation, tenant_id, principal_id)
    SELECT p_document, p_incarnation, p_tenant, p_principal
    WHERE EXISTS (
        SELECT 1 FROM grant_catalog_document_ownership o
        WHERE o.document_id = p_document AND o.incarnation = p_incarnation
          AND o.tenant_id = p_tenant AND o.principal_id = p_principal
    )
    ON CONFLICT (document_id, incarnation) DO NOTHING;
END;

-- Remove PUBLIC and inherited default access from the new catalog table and functions.
DO $$
DECLARE
    catalog_schema NAME := current_schema();
    catalog_namespace OID;
    permission RECORD;
BEGIN
    SELECT oid INTO STRICT catalog_namespace FROM pg_namespace WHERE nspname = catalog_schema;
    FOR permission IN
        SELECT 'TABLE' AS kind, format('%I.%I', catalog_schema, c.relname) AS identity, acl.grantee
        FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) acl
        WHERE c.relnamespace = catalog_namespace
          AND c.oid = 'grant_catalog_deletion_receipts'::regclass
          AND acl.grantee <> c.relowner
        UNION ALL
        SELECT 'FUNCTION', format('%I.%I(%s)', catalog_schema, p.proname, pg_get_function_identity_arguments(p.oid)), acl.grantee
        FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE p.pronamespace = catalog_namespace AND p.oid IN (
            'grant_catalog_policy_state(uuid,uuid)'::regprocedure,
            'grant_catalog_list_documents(uuid,uuid,uuid,integer,text,boolean)'::regprocedure,
            'grant_catalog_get_document(uuid,uuid,uuid,boolean)'::regprocedure,
            'grant_catalog_authorize_file(uuid,uuid,uuid)'::regprocedure,
            'grant_catalog_authorize_delete(uuid,uuid,uuid,uuid)'::regprocedure,
            'grant_catalog_record_deletion_owner(uuid,uuid,uuid,uuid)'::regprocedure
        ) AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON %s %s FROM %s CASCADE',
            permission.kind, permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
