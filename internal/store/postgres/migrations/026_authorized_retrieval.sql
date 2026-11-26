-- Retrieval for a verified subject reads what the principal may use through
-- these owner-defined entry points, so a restricted serving login needs no
-- privilege on the catalog tables. Only completed documents whose current
-- incarnation carries a ready ownership row are listed. A disabled principal,
-- a revoked grant, a replaced or deleted document and an unassigned legacy
-- document all drop out of the relation without any further check.
CREATE VIEW grant_catalog_readable_documents AS
SELECT a.tenant_id, a.principal_id, a.document_id, a.incarnation, a.collection, d.doc_type
FROM grant_catalog_document_access a
JOIN documents d ON d.id = a.document_id AND COALESCE(d.incarnation, d.id) = a.incarnation
    AND d.status = 'completed';

-- The policy revision a read was decided at. The result is NULL unless the
-- principal is active in its tenant, so one statement answers both questions.
CREATE FUNCTION grant_catalog_read_revision(p_tenant UUID, p_principal UUID)
RETURNS BIGINT
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT r.revision FROM grant_catalog_policy_revision r
    WHERE EXISTS (
        SELECT 1 FROM grant_catalog_active_principals p
        WHERE p.tenant_id = p_tenant AND p.principal_id = p_principal
    );
END;

-- Remove PUBLIC and inherited default access from the new relation and entry
-- point. The schema owner grants them to the serving login.
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
        WHERE c.relnamespace = catalog_namespace AND c.oid = 'grant_catalog_readable_documents'::regclass
            AND acl.grantee <> c.relowner
        UNION ALL
        SELECT 'FUNCTION', format('%I.%I(%s)', catalog_schema, p.proname, pg_get_function_identity_arguments(p.oid)), acl.grantee
        FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE p.pronamespace = catalog_namespace AND p.oid = 'grant_catalog_read_revision(uuid,uuid)'::regprocedure
            AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON %s %s FROM %s CASCADE',
            permission.kind, permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
