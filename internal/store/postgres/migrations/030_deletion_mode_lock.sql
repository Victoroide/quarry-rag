-- Admission that may select the tagged vector format reads the activation
-- marker while holding its row in share mode, so activation waits for every
-- such admission that read the old mode, and an admission that starts during
-- an activation waits for its outcome. The function runs as the schema owner,
-- so a restricted serving login needs no privilege on the marker table.
CREATE FUNCTION document_deletion_active_shared() RETURNS BOOLEAN
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT activated_at IS NOT NULL FROM document_deletion_activation WHERE singleton FOR SHARE;
END;

-- Remove PUBLIC and inherited default access. The schema owner grants execution
-- to the serving login.
DO $$
DECLARE
    deletion_schema NAME := current_schema();
    deletion_namespace OID;
    permission RECORD;
BEGIN
    SELECT oid INTO STRICT deletion_namespace FROM pg_namespace WHERE nspname = deletion_schema;
    FOR permission IN
        SELECT format('%I.%I(%s)', deletion_schema, p.proname, pg_get_function_identity_arguments(p.oid)) AS identity, acl.grantee
        FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE p.pronamespace = deletion_namespace
          AND p.oid = 'document_deletion_active_shared()'::regprocedure
          AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON FUNCTION %s FROM %s CASCADE',
            permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
