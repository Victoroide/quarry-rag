-- Writer transactions use a shared lock on the existing corpus mode marker.
-- Cutover takes the exclusive lock, so it waits for writers that saw the old
-- mode and later writers see the committed mode before mutating data.
CREATE FUNCTION grant_catalog_corpus_authz_mode_shared() RETURNS TEXT
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT authz_mode FROM grant_catalog_corpus_mode WHERE singleton FOR SHARE;
END;

-- Remove PUBLIC and inherited default access. The schema owner grants execution
-- to the serving login.
DO $$
DECLARE
    corpus_schema NAME := current_schema();
    corpus_namespace OID;
    permission RECORD;
BEGIN
    SELECT oid INTO STRICT corpus_namespace FROM pg_namespace WHERE nspname = corpus_schema;
    FOR permission IN
        SELECT format('%I.%I(%s)', corpus_schema, p.proname, pg_get_function_identity_arguments(p.oid)) AS identity, acl.grantee
        FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE p.pronamespace = corpus_namespace
          AND p.oid = 'grant_catalog_corpus_authz_mode_shared()'::regprocedure
          AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON FUNCTION %s FROM %s CASCADE',
            permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
