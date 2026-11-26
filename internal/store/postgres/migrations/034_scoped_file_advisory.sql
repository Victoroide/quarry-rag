-- Scoped file signing and grant imports share the two-integer advisory
-- lock class 0x71726673 (see locks.go). The document intent key uses bigint.
CREATE FUNCTION grant_catalog_file_principal_key(p_tenant UUID, p_principal UUID)
RETURNS INTEGER
LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT hashtext(p_tenant::text || ':' || p_principal::text);
END;

CREATE FUNCTION grant_catalog_lock_file_principal(p_tenant UUID, p_principal UUID)
RETURNS VOID
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT pg_advisory_xact_lock_shared(1903322739, grant_catalog_file_principal_key(p_tenant, p_principal));
END;

-- The caller must hold both shared advisory keys before reading in a fresh
-- statement. Keep the row-locked entry point for older replicas and legacy deletion.
CREATE FUNCTION grant_catalog_authorize_file_advisory(p_tenant UUID, p_principal UUID, p_document UUID)
RETURNS TABLE (active BOOLEAN, s3_key TEXT)
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT EXISTS (
        SELECT 1 FROM grant_catalog_principals pr
        WHERE pr.tenant_id = p_tenant AND pr.principal_id = p_principal AND pr.enabled
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

CREATE OR REPLACE FUNCTION grant_catalog_import_impl(p_raw TEXT, p_dry_run BOOLEAN)
RETURNS TABLE (resulting_revision BIGINT, replayed BOOLEAN)
LANGUAGE plpgsql AS $$
DECLARE
    p_payload JSONB;
    batch UUID;
    expected BIGINT;
    payload_hash BYTEA;
    saved_revision BIGINT;
    entries grant_catalog_import_entry[];
    current_revision BIGINT;
    principal_key INTEGER;
BEGIN
    IF p_dry_run IS NULL THEN
        RAISE SQLSTATE 'QG001' USING MESSAGE = 'invalid preview flag';
    END IF;
    p_payload := grant_catalog_decode_import(p_raw);
    IF p_dry_run THEN
        RETURN QUERY SELECT * FROM grant_catalog_preview(p_payload);
        RETURN;
    END IF;
    SELECT h.batch, h.expected, h.payload_hash INTO STRICT batch, expected, payload_hash
    FROM grant_catalog_import_header(p_payload) h;
    saved_revision := grant_catalog_committed_revision(batch, payload_hash);
    IF saved_revision IS NOT NULL THEN
        RETURN QUERY SELECT saved_revision, TRUE;
        RETURN;
    END IF;
    entries := grant_catalog_parse_import(p_payload);

    -- Acquire distinct lock keys in signed order before touching document rows
    -- or policy revision. Hash collisions only serialize unrelated principals.
    FOR principal_key IN
        SELECT DISTINCT grant_catalog_file_principal_key(e.tenant_id, e.principal_id) AS lock_key
        FROM unnest(entries) e
        WHERE e.entry_group IN ('principals', 'grants')
        ORDER BY lock_key
    LOOP
        PERFORM pg_advisory_xact_lock(1903322739, principal_key);
    END LOOP;

    -- Document deletion locks its source row before removing ownership and advancing policy.
    PERFORM d.document_id FROM grant_catalog_source_documents d
    WHERE d.document_id IN (SELECT e.document_id FROM unnest(entries) e WHERE e.entry_group IN ('assignments', 'grants'))
    ORDER BY d.document_id FOR NO KEY UPDATE;
    SELECT revision INTO STRICT current_revision FROM grant_catalog_policy_revision FOR UPDATE;
    saved_revision := grant_catalog_committed_revision(batch, payload_hash);
    IF saved_revision IS NOT NULL THEN
        RETURN QUERY SELECT saved_revision, TRUE;
        RETURN;
    END IF;
    PERFORM grant_catalog_check_revision(current_revision, expected);
    PERFORM grant_catalog_validate_import(entries);

    PERFORM grant_catalog_apply_import(entries);
    UPDATE grant_catalog_policy_revision SET revision = revision + 1 RETURNING revision INTO STRICT current_revision;
    INSERT INTO grant_catalog_policy_audit (revision, operator, batch_id, payload_hash)
    VALUES (current_revision, session_user, batch, payload_hash);
    RETURN QUERY SELECT current_revision, FALSE;
END;
$$;

-- Remove PUBLIC and inherited default access. The schema owner grants execution
-- to the serving login.
DO $$
DECLARE
    catalog_schema NAME := current_schema();
    catalog_namespace OID;
    permission RECORD;
BEGIN
    SELECT oid INTO STRICT catalog_namespace FROM pg_namespace WHERE nspname = catalog_schema;
    FOR permission IN
        SELECT format('%I.%I(%s)', catalog_schema, p.proname, pg_get_function_identity_arguments(p.oid)) AS identity, acl.grantee
        FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE p.pronamespace = catalog_namespace
          AND p.oid IN (
              'grant_catalog_file_principal_key(uuid,uuid)'::regprocedure,
              'grant_catalog_lock_file_principal(uuid,uuid)'::regprocedure,
              'grant_catalog_authorize_file_advisory(uuid,uuid,uuid)'::regprocedure
          )
          AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON FUNCTION %s FROM %s CASCADE',
            permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
