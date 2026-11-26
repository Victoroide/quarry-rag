-- Subject-bound document sets. A set is an immutable list of document
-- incarnations that one principal may select in a query. It is created by an
-- operator in one transaction and is only a selector: every use intersects it
-- with the owner's live read grants, so a set never adds access and cannot
-- resurrect a revoked, deleted or recreated document.
CREATE TABLE grant_catalog_document_sets (
    set_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    owner_principal_id UUID NOT NULL,
    revision UUID NOT NULL DEFAULT gen_random_uuid(),
    import_key TEXT COLLATE "C" CHECK (import_key IS NULL OR octet_length(import_key) BETWEEN 1 AND 128),
    content_digest BYTEA NOT NULL CHECK (octet_length(content_digest) = 32),
    member_count INTEGER NOT NULL CHECK (member_count BETWEEN 0 AND 100000),
    created_by NAME NOT NULL DEFAULT session_user,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > created_at AND expires_at <= created_at + INTERVAL '24 hours'),
    UNIQUE (set_id, tenant_id),
    FOREIGN KEY (tenant_id, owner_principal_id) REFERENCES grant_catalog_principals (tenant_id, principal_id)
);

-- An import key names one set per owner, so a retry finds the original.
CREATE UNIQUE INDEX grant_catalog_document_sets_import_key
    ON grant_catalog_document_sets (tenant_id, owner_principal_id, import_key) WHERE import_key IS NOT NULL;
CREATE INDEX grant_catalog_document_sets_owner_expiry
    ON grant_catalog_document_sets (tenant_id, owner_principal_id, expires_at);
CREATE INDEX grant_catalog_document_sets_expiry ON grant_catalog_document_sets (expires_at);

-- Membership names the incarnation that was readable when the set was created.
-- Deleting the document deletes the member, and a recreated document has a new
-- incarnation that no set contains.
CREATE TABLE grant_catalog_document_set_members (
    set_id UUID NOT NULL,
    tenant_id UUID NOT NULL,
    document_id UUID NOT NULL,
    incarnation UUID NOT NULL,
    PRIMARY KEY (set_id, document_id),
    FOREIGN KEY (set_id, tenant_id) REFERENCES grant_catalog_document_sets (set_id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, document_id, incarnation) REFERENCES grant_catalog_document_ownership
        (tenant_id, document_id, incarnation) ON DELETE CASCADE
);
CREATE INDEX grant_catalog_document_set_members_incarnation
    ON grant_catalog_document_set_members (tenant_id, document_id, incarnation);

-- A set resolves only before it expires and only while its owner is active. The
-- database clock decides expiry, so every replica agrees on the instant.
CREATE VIEW grant_catalog_document_sets_live AS
SELECT s.set_id, s.tenant_id, s.owner_principal_id, s.revision, s.expires_at
FROM grant_catalog_document_sets s
JOIN grant_catalog_active_principals p ON p.tenant_id = s.tenant_id AND p.principal_id = s.owner_principal_id
WHERE s.expires_at > clock_timestamp();

-- The documents of a live set that its owner may read now at the incarnation the
-- set names. The shape matches grant_catalog_readable_documents plus the set,
-- so a ranking statement can join it in place of that relation.
CREATE VIEW grant_catalog_document_set_readable AS
SELECT s.set_id, s.tenant_id, s.owner_principal_id AS principal_id, m.document_id, m.incarnation, g.collection, g.doc_type
FROM grant_catalog_document_sets_live s
JOIN grant_catalog_document_set_members m ON m.set_id = s.set_id AND m.tenant_id = s.tenant_id
JOIN grant_catalog_readable_documents g ON g.tenant_id = s.tenant_id AND g.principal_id = s.owner_principal_id
    AND g.document_id = m.document_id AND g.incarnation = m.incarnation;

CREATE FUNCTION grant_catalog_create_document_set_impl(
    p_tenant UUID, p_owner UUID, p_ttl_seconds INTEGER, p_import_key TEXT, p_documents UUID[], p_max_members INTEGER)
RETURNS TABLE (result_set UUID, result_revision UUID, result_expires TIMESTAMPTZ, result_members INTEGER, result_replayed BOOLEAN)
LANGUAGE plpgsql AS $$
DECLARE
    ids UUID[];
    digest BYTEA;
    started TIMESTAMPTZ;
    existing grant_catalog_document_sets%ROWTYPE;
    created grant_catalog_document_sets%ROWTYPE;
    active INTEGER;
    inserted BIGINT;
BEGIN
    IF p_tenant IS NULL OR p_owner IS NULL OR p_documents IS NULL OR p_max_members IS NULL OR p_ttl_seconds IS NULL
       OR p_max_members NOT BETWEEN 1 AND 100000 OR p_ttl_seconds NOT BETWEEN 1 AND 86400
       OR (p_import_key IS NOT NULL AND octet_length(p_import_key) NOT BETWEEN 1 AND 128)
       OR COALESCE(array_ndims(p_documents), 1) <> 1
       OR cardinality(p_documents) > p_max_members
       OR EXISTS (SELECT FROM unnest(p_documents) AS u(id) WHERE u.id IS NULL) THEN
        RAISE SQLSTATE 'QD001' USING MESSAGE = 'invalid document set request';
    END IF;

    -- Creations for one owner run one at a time, and a concurrent change to the
    -- owner waits for the creation or is already visible to it.
    PERFORM 1 FROM grant_catalog_principals p
    WHERE p.tenant_id = p_tenant AND p.principal_id = p_owner AND p.enabled
    FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RAISE SQLSTATE 'QD005' USING MESSAGE = 'unknown or inactive owner';
    END IF;

    -- Expired rows leave in small steps, so no creation pays for an unbounded cleanup.
    DELETE FROM grant_catalog_document_sets s WHERE s.set_id IN (
        SELECT x.set_id FROM grant_catalog_document_sets x
        WHERE x.tenant_id = p_tenant AND x.owner_principal_id = p_owner AND x.expires_at <= clock_timestamp()
        ORDER BY x.expires_at LIMIT 4 FOR UPDATE SKIP LOCKED);

    SELECT array_agg(d.id ORDER BY d.id) INTO ids FROM (SELECT DISTINCT u.id FROM unnest(p_documents) AS u(id)) d;
    ids := COALESCE(ids, ARRAY[]::UUID[]);
    digest := sha256(convert_to(array_to_string(ids, ','), 'UTF8'));

    IF p_import_key IS NOT NULL THEN
        SELECT s.* INTO existing FROM grant_catalog_document_sets s
        WHERE s.tenant_id = p_tenant AND s.owner_principal_id = p_owner AND s.import_key = p_import_key
        FOR UPDATE;
        IF FOUND THEN
            IF existing.expires_at <= clock_timestamp() THEN
                -- An expired set frees its key, so a later import creates a new reference.
                DELETE FROM grant_catalog_document_sets s WHERE s.set_id = existing.set_id;
            ELSIF existing.content_digest = digest THEN
                RETURN QUERY SELECT existing.set_id, existing.revision, existing.expires_at, existing.member_count, TRUE;
                RETURN;
            ELSE
                RAISE SQLSTATE 'QD002' USING MESSAGE = 'import key conflict';
            END IF;
        END IF;
    END IF;

    SELECT count(*) INTO active FROM grant_catalog_document_sets s
    WHERE s.tenant_id = p_tenant AND s.owner_principal_id = p_owner AND s.expires_at > clock_timestamp();
    IF active >= 16 THEN
        RAISE SQLSTATE 'QD003' USING MESSAGE = 'active document set limit reached';
    END IF;

    started := clock_timestamp();
    INSERT INTO grant_catalog_document_sets (tenant_id, owner_principal_id, import_key, content_digest, member_count, created_at, expires_at)
    VALUES (p_tenant, p_owner, p_import_key, digest, cardinality(ids), started, started + make_interval(secs => p_ttl_seconds))
    RETURNING * INTO created;

    -- Every member must be a completed document the owner may read now. The
    -- failure is the same for an unknown, foreign, unfinished or unreadable
    -- identifier, and the raise discards the whole set.
    INSERT INTO grant_catalog_document_set_members (set_id, tenant_id, document_id, incarnation)
    SELECT created.set_id, p_tenant, g.document_id, g.incarnation
    FROM unnest(ids) AS u(id)
    JOIN grant_catalog_readable_documents g ON g.tenant_id = p_tenant AND g.principal_id = p_owner AND g.document_id = u.id;
    GET DIAGNOSTICS inserted = ROW_COUNT;
    IF inserted <> cardinality(ids) THEN
        RAISE SQLSTATE 'QD004' USING MESSAGE = 'unknown or unauthorized document set member';
    END IF;

    RETURN QUERY SELECT created.set_id, created.revision, created.expires_at, created.member_count, FALSE;
END;
$$;

CREATE FUNCTION grant_catalog_create_document_set(
    p_tenant UUID, p_owner UUID, p_ttl_seconds INTEGER, p_import_key TEXT, p_documents UUID[], p_max_members INTEGER)
RETURNS TABLE (result_set UUID, result_revision UUID, result_expires TIMESTAMPTZ, result_members INTEGER, result_replayed BOOLEAN)
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT grant_catalog_enter('grant_catalog_document_sets'::regclass);
    SELECT * FROM grant_catalog_create_document_set_impl(p_tenant, p_owner, p_ttl_seconds, p_import_key, p_documents, p_max_members);
END;

-- Remove expired sets in a bounded step. An operator repeats the call until it returns zero.
CREATE FUNCTION grant_catalog_purge_document_sets(p_limit INTEGER) RETURNS INTEGER
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    WITH doomed AS (
        SELECT s.set_id FROM grant_catalog_document_sets s
        WHERE s.expires_at <= clock_timestamp()
        ORDER BY s.expires_at
        LIMIT LEAST(GREATEST(COALESCE(p_limit, 0), 0), 100)
        FOR UPDATE SKIP LOCKED
    ), gone AS (
        DELETE FROM grant_catalog_document_sets s USING doomed d WHERE s.set_id = d.set_id RETURNING 1
    )
    SELECT count(*)::INTEGER FROM gone;
END;

-- Remove PUBLIC and inherited default access from the new objects. The schema
-- owner grants the views to the serving login and the functions to the operator.
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
            'grant_catalog_document_sets'::regclass, 'grant_catalog_document_set_members'::regclass,
            'grant_catalog_document_sets_live'::regclass, 'grant_catalog_document_set_readable'::regclass
        ) AND acl.grantee <> c.relowner
        UNION ALL
        SELECT 'FUNCTION', format('%I.%I(%s)', catalog_schema, p.proname, pg_get_function_identity_arguments(p.oid)), acl.grantee
        FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE p.pronamespace = catalog_namespace AND p.oid IN (
            'grant_catalog_create_document_set_impl(uuid,uuid,integer,text,uuid[],integer)'::regprocedure,
            'grant_catalog_create_document_set(uuid,uuid,integer,text,uuid[],integer)'::regprocedure,
            'grant_catalog_purge_document_sets(integer)'::regprocedure
        ) AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON %s %s FROM %s CASCADE',
            permission.kind, permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
