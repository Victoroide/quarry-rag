-- Keep the writer's SQL body so PostgreSQL binds the validator and audit
-- tables at definition time. Those references survive a corpus schema rename.
-- The private PL/pgSQL validator supplies the runtime errors.
CREATE FUNCTION grant_catalog_validate_security_audit(p_action TEXT, p_outcome TEXT)
RETURNS VOID
LANGUAGE plpgsql CALLED ON NULL INPUT
SET search_path = pg_catalog, pg_temp
AS $validator$
BEGIN
    IF p_action IS NULL OR p_action COLLATE pg_catalog."C" NOT IN (
        'ingest', 'query', 'document_list', 'document_status', 'file_sign',
        'document_delete', 'subject_auth', 'trust_key_load'
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid runtime audit action';
    END IF;
    IF p_outcome IS NULL OR p_outcome COLLATE pg_catalog."C" NOT IN ('allow', 'deny', 'error') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid runtime audit outcome';
    END IF;
END;
$validator$;

REVOKE ALL ON FUNCTION grant_catalog_validate_security_audit(TEXT, TEXT) FROM PUBLIC;

DO $permissions$
DECLARE
    validator REGPROCEDURE := 'grant_catalog_validate_security_audit(text,text)'::regprocedure;
    writer REGPROCEDURE := 'grant_catalog_record_security_audit(text,uuid,uuid,bigint,text,text,text,uuid,uuid)'::regprocedure;
    writer_owner NAME;
    grantee OID;
BEGIN
    FOR grantee IN
        SELECT DISTINCT acl.grantee
        FROM pg_catalog.pg_proc p CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) acl
        WHERE p.oid = validator AND acl.grantee <> p.proowner
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %I CASCADE',
            validator, pg_catalog.pg_get_userbyid(grantee));
    END LOOP;

    SELECT pg_catalog.pg_get_userbyid(proowner) INTO STRICT writer_owner
    FROM pg_catalog.pg_proc WHERE oid = writer;
    IF writer_owner <> current_user THEN
        EXECUTE pg_catalog.format('ALTER FUNCTION %s OWNER TO %I', validator, writer_owner);
    END IF;
END;
$permissions$;

CREATE OR REPLACE FUNCTION grant_catalog_record_security_audit(
    p_request_id TEXT, p_tenant UUID, p_principal UUID, p_policy_revision BIGINT,
    p_action TEXT, p_outcome TEXT, p_reason TEXT, p_document_id UUID, p_incarnation UUID)
RETURNS VOID
LANGUAGE sql CALLED ON NULL INPUT SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT grant_catalog_validate_security_audit(p_action, p_outcome);
    INSERT INTO grant_catalog_security_audit (
        request_id, tenant_id, principal_id, policy_revision,
        action, outcome, reason, document_id, incarnation
    )
    VALUES (
        COALESCE(p_request_id, ''),
        p_tenant,
        p_principal,
        COALESCE(p_policy_revision, (SELECT revision FROM grant_catalog_policy_revision WHERE singleton)),
        p_action,
        p_outcome,
        p_reason,
        p_document_id,
        p_incarnation
    );
END;
