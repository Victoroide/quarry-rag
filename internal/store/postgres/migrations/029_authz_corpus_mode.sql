-- Persist the corpus authorization mode and structured security audit log.
-- Mode-aware binaries check the marker for readiness and admission; an
-- operator transaction switches the corpus from service to grants once
-- legacy data and older replicas are fenced.
CREATE TABLE grant_catalog_corpus_mode (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    authz_mode TEXT NOT NULL DEFAULT 'service' CHECK (authz_mode IN ('service', 'grants')),
    activated_at TIMESTAMPTZ,
    activated_by NAME
);
INSERT INTO grant_catalog_corpus_mode DEFAULT VALUES;

CREATE TABLE grant_catalog_security_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    request_id TEXT NOT NULL DEFAULT '' CHECK (octet_length(request_id) <= 128),
    tenant_id UUID,
    principal_id UUID,
    operator NAME,
    policy_revision BIGINT,
    action TEXT NOT NULL CHECK (action IN (
        'corpus_cutover', 'grant_import', 'trust_key_load',
        'ingest', 'query', 'document_list', 'document_status',
        'file_sign', 'document_delete', 'subject_auth'
    )),
    outcome TEXT NOT NULL CHECK (outcome IN ('allow', 'deny', 'error')),
    reason TEXT NOT NULL CHECK (octet_length(reason) <= 128),
    document_id UUID,
    incarnation UUID
);

-- Report the active corpus authorization mode to restricted serving logins
-- without exposing direct privileges on catalog tables. A missing row yields
-- NULL so callers fail closed.
CREATE FUNCTION grant_catalog_corpus_authz_mode() RETURNS TEXT
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT authz_mode FROM grant_catalog_corpus_mode WHERE singleton;
END;

-- Record a security audit event for sensitive runtime operations and key loads
-- without storing credentials, raw subjects, signed addresses or text.
CREATE FUNCTION grant_catalog_record_security_audit(
    p_request_id TEXT, p_tenant UUID, p_principal UUID, p_policy_revision BIGINT,
    p_action TEXT, p_outcome TEXT, p_reason TEXT, p_document_id UUID, p_incarnation UUID)
RETURNS VOID
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
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

-- Mirror policy audit rows (grant imports and ownership deletions) into the
-- unified security audit log in the same transaction.
CREATE FUNCTION grant_catalog_mirror_policy_audit() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    IF NEW.batch_id IS NOT NULL THEN
        EXECUTE format(
            'INSERT INTO %I.grant_catalog_security_audit (operator, policy_revision, action, outcome, reason) VALUES ($1, $2, $3, $4, $5)',
            TG_TABLE_SCHEMA
        ) USING NEW.operator, NEW.revision, 'grant_import', 'allow', 'batch_committed';
    ELSE
        EXECUTE format(
            'INSERT INTO %I.grant_catalog_security_audit (operator, policy_revision, action, outcome, reason, document_id, incarnation) VALUES ($1, $2, $3, $4, $5, $6, $7)',
            TG_TABLE_SCHEMA
        ) USING NEW.operator, NEW.revision, 'document_delete', 'allow', 'ownership_deleted', NEW.document_id, NEW.incarnation;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER mirror_policy_audit AFTER INSERT ON grant_catalog_policy_audit
    FOR EACH ROW EXECUTE FUNCTION grant_catalog_mirror_policy_audit();

CREATE FUNCTION grant_catalog_activate_grants_impl(p_dry_run BOOLEAN)
RETURNS TABLE (resulting_revision BIGINT, already_active BOOLEAN)
LANGUAGE plpgsql AS $$
DECLARE
    current_mode TEXT;
    current_revision BIGINT;
BEGIN
    IF p_dry_run IS NULL THEN
        RAISE SQLSTATE 'QG001' USING MESSAGE = 'invalid preview flag';
    END IF;
    IF p_dry_run THEN
        SELECT m.authz_mode INTO STRICT current_mode FROM grant_catalog_corpus_mode m WHERE m.singleton;
        SELECT r.revision INTO STRICT current_revision FROM grant_catalog_policy_revision r WHERE r.singleton;
        RETURN QUERY SELECT current_revision, current_mode = 'grants';
        RETURN;
    END IF;
    SELECT m.authz_mode INTO STRICT current_mode FROM grant_catalog_corpus_mode m WHERE m.singleton FOR UPDATE;
    SELECT r.revision INTO STRICT current_revision FROM grant_catalog_policy_revision r WHERE r.singleton FOR UPDATE;
    IF current_mode = 'grants' THEN
        RETURN QUERY SELECT current_revision, TRUE;
        RETURN;
    END IF;
    PERFORM grant_catalog_check_revision(current_revision, current_revision);
    UPDATE grant_catalog_corpus_mode
    SET authz_mode = 'grants', activated_at = clock_timestamp(), activated_by = session_user
    WHERE singleton;
    UPDATE grant_catalog_policy_revision
    SET revision = revision + 1
    WHERE singleton
    RETURNING revision INTO STRICT current_revision;
    INSERT INTO grant_catalog_security_audit (operator, policy_revision, action, outcome, reason)
    VALUES (session_user, current_revision, 'corpus_cutover', 'allow', 'grants_activated');
    RETURN QUERY SELECT current_revision, FALSE;
END;
$$;

-- Activate grants mode atomically behind the schema anchor, advancing the
-- policy revision and writing the cutover audit record in one transaction.
CREATE FUNCTION grant_catalog_activate_grants(p_dry_run BOOLEAN)
RETURNS TABLE (resulting_revision BIGINT, already_active BOOLEAN)
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp SET timezone = 'UTC' SET datestyle = 'ISO, YMD'
BEGIN ATOMIC
    SELECT grant_catalog_enter('grant_catalog_corpus_mode'::regclass);
    SELECT * FROM grant_catalog_activate_grants_impl(p_dry_run);
END;

-- Remove PUBLIC and inherited default access from the new tables and functions.
-- The schema owner grants serving, operator and auditor privileges explicitly.
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
        WHERE c.relnamespace = catalog_namespace AND c.oid IN (
            'grant_catalog_corpus_mode'::regclass,
            'grant_catalog_security_audit'::regclass
        ) AND acl.grantee <> c.relowner
        UNION ALL
        SELECT 'FUNCTION', format('%I.%I(%s)', catalog_schema, p.proname, pg_get_function_identity_arguments(p.oid)), acl.grantee
        FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE p.pronamespace = catalog_namespace AND p.oid IN (
            'grant_catalog_corpus_authz_mode()'::regprocedure,
            'grant_catalog_record_security_audit(text,uuid,uuid,bigint,text,text,text,uuid,uuid)'::regprocedure,
            'grant_catalog_mirror_policy_audit()'::regprocedure,
            'grant_catalog_activate_grants_impl(boolean)'::regprocedure,
            'grant_catalog_activate_grants(boolean)'::regprocedure
        ) AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON %s %s FROM %s CASCADE',
            permission.kind, permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
