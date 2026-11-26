-- A missing row means revision zero. Keep counters when principals disappear
-- so a reused identity can never match an earlier inventory snapshot.
CREATE TABLE grant_catalog_inventory_revisions (
    tenant_id UUID NOT NULL,
    principal_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    PRIMARY KEY (tenant_id, principal_id)
);

CREATE TABLE grant_catalog_inventory_revision_pending (
    xact_id XID8 NOT NULL,
    tenant_id UUID NOT NULL,
    principal_id UUID NOT NULL,
    PRIMARY KEY (xact_id, tenant_id, principal_id)
);

CREATE FUNCTION grant_catalog_enqueue_inventory_revision(p_tenant UUID, p_principal UUID)
RETURNS VOID
LANGUAGE sql
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    INSERT INTO grant_catalog_inventory_revision_pending (xact_id, tenant_id, principal_id)
    VALUES (pg_current_xact_id(), p_tenant, p_principal)
    ON CONFLICT DO NOTHING;
END;

CREATE FUNCTION grant_catalog_capture_inventory_principal() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND
        (OLD.tenant_id, OLD.principal_id, OLD.issuer, OLD.subject, OLD.enabled) IS NOT DISTINCT FROM
        (NEW.tenant_id, NEW.principal_id, NEW.issuer, NEW.subject, NEW.enabled) THEN
        RETURN NULL;
    END IF;
    IF TG_OP <> 'INSERT' THEN
        EXECUTE format('SELECT %I.grant_catalog_enqueue_inventory_revision($1,$2)', TG_TABLE_SCHEMA)
        USING OLD.tenant_id, OLD.principal_id;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        EXECUTE format('SELECT %I.grant_catalog_enqueue_inventory_revision($1,$2)', TG_TABLE_SCHEMA)
        USING NEW.tenant_id, NEW.principal_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION grant_catalog_capture_inventory_create() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND
        (OLD.tenant_id, OLD.principal_id, OLD.collection, OLD.can_create) IS NOT DISTINCT FROM
        (NEW.tenant_id, NEW.principal_id, NEW.collection, NEW.can_create) THEN
        RETURN NULL;
    END IF;
    IF TG_OP <> 'INSERT' AND OLD.can_create THEN
        EXECUTE format('SELECT %I.grant_catalog_enqueue_inventory_revision($1,$2)', TG_TABLE_SCHEMA)
        USING OLD.tenant_id, OLD.principal_id;
    END IF;
    IF TG_OP <> 'DELETE' AND NEW.can_create THEN
        EXECUTE format('SELECT %I.grant_catalog_enqueue_inventory_revision($1,$2)', TG_TABLE_SCHEMA)
        USING NEW.tenant_id, NEW.principal_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION grant_catalog_capture_inventory_grant() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND
        (OLD.tenant_id, OLD.document_id, OLD.incarnation, OLD.principal_id, OLD.can_read, OLD.can_download) IS NOT DISTINCT FROM
        (NEW.tenant_id, NEW.document_id, NEW.incarnation, NEW.principal_id, NEW.can_read, NEW.can_download) THEN
        RETURN NULL;
    END IF;
    -- Ownership already supplies read and download. A cascaded grant
    -- deletion may run after its ownership row is gone.
    EXECUTE format('
        WITH grants(tenant_id, document_id, incarnation, principal_id, can_read) AS (
            VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5::boolean),
                   ($6::uuid,$7::uuid,$8::uuid,$9::uuid,$10::boolean)
        )
        SELECT %1$I.grant_catalog_enqueue_inventory_revision(g.tenant_id,g.principal_id)
        FROM grants g
        WHERE g.can_read AND NOT EXISTS (
            SELECT 1 FROM %1$I.grant_catalog_document_ownership o
            WHERE (o.tenant_id, o.document_id, o.incarnation, o.principal_id) =
                  (g.tenant_id, g.document_id, g.incarnation, g.principal_id)
        )', TG_TABLE_SCHEMA)
    USING OLD.tenant_id, OLD.document_id, OLD.incarnation, OLD.principal_id, OLD.can_read,
          NEW.tenant_id, NEW.document_id, NEW.incarnation, NEW.principal_id, NEW.can_read;
    RETURN NULL;
END;
$$;

CREATE FUNCTION grant_catalog_capture_inventory_ownership() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND
        (OLD.tenant_id, OLD.document_id, OLD.incarnation, OLD.principal_id, OLD.collection, (OLD.ready OR OLD.origin = 'accepted')) IS NOT DISTINCT FROM
        (NEW.tenant_id, NEW.document_id, NEW.incarnation, NEW.principal_id, NEW.collection, (NEW.ready OR NEW.origin = 'accepted')) THEN
        RETURN NULL;
    END IF;

    EXECUTE format('
        WITH owners(tenant_id, document_id, incarnation, principal_id) AS (
            VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid), ($5::uuid,$6::uuid,$7::uuid,$8::uuid)
        ), affected AS (
            SELECT o.tenant_id, o.principal_id FROM owners o WHERE o.tenant_id IS NOT NULL
            UNION
            SELECT g.tenant_id, g.principal_id
            FROM owners o JOIN %1$I.grant_catalog_document_grants g USING (tenant_id, document_id, incarnation)
            WHERE g.can_read
        )
        SELECT %1$I.grant_catalog_enqueue_inventory_revision(a.tenant_id,a.principal_id) FROM affected a',
        TG_TABLE_SCHEMA)
    USING OLD.tenant_id, OLD.document_id, OLD.incarnation, OLD.principal_id,
          NEW.tenant_id, NEW.document_id, NEW.incarnation, NEW.principal_id;
    RETURN NULL;
END;
$$;

CREATE FUNCTION grant_catalog_capture_inventory_document() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    -- Publication can update ownership without locking the document. Lock the
    -- matching incarnation before evaluating readiness so a publication that
    -- commits first cannot leave this later visibility change unrecorded.
    EXECUTE format('
        WITH matching_ownership AS MATERIALIZED (
            SELECT o.tenant_id, o.document_id, o.incarnation, o.principal_id, o.collection, o.ready, o.origin
            FROM %1$I.grant_catalog_document_ownership o
            WHERE o.document_id = $1 AND o.incarnation = $2
            FOR SHARE
        ), owners AS (
            SELECT o.tenant_id, o.document_id, o.incarnation, o.principal_id
            FROM matching_ownership o
            WHERE (o.ready OR o.origin = ''accepted'')
              AND (($3 AND o.collection = $4) OR ($5 AND o.collection = $6))
        ), affected AS (
            SELECT o.tenant_id, o.principal_id FROM owners o
            UNION
            SELECT g.tenant_id, g.principal_id
            FROM owners o JOIN %1$I.grant_catalog_document_grants g USING (tenant_id, document_id, incarnation)
            WHERE g.can_read
        )
        SELECT %1$I.grant_catalog_enqueue_inventory_revision(a.tenant_id,a.principal_id) FROM affected a',
        TG_TABLE_SCHEMA)
    USING NEW.id, COALESCE(NEW.incarnation, NEW.id),
          OLD.grant_catalog_assigned AND OLD.status <> 'deleting', OLD.collection,
          NEW.grant_catalog_assigned AND NEW.status <> 'deleting', NEW.collection;
    RETURN NULL;
END;
$$;

CREATE TRIGGER capture_inventory_principal
    AFTER INSERT OR UPDATE OR DELETE ON grant_catalog_principals
    FOR EACH ROW EXECUTE FUNCTION grant_catalog_capture_inventory_principal();
CREATE TRIGGER capture_inventory_create
    AFTER INSERT OR UPDATE OR DELETE ON grant_catalog_create_permissions
    FOR EACH ROW EXECUTE FUNCTION grant_catalog_capture_inventory_create();
CREATE TRIGGER capture_inventory_grant
    AFTER INSERT OR UPDATE OR DELETE ON grant_catalog_document_grants
    FOR EACH ROW EXECUTE FUNCTION grant_catalog_capture_inventory_grant();
CREATE TRIGGER capture_inventory_ownership
    AFTER INSERT OR UPDATE OR DELETE ON grant_catalog_document_ownership
    FOR EACH ROW EXECUTE FUNCTION grant_catalog_capture_inventory_ownership();
CREATE TRIGGER capture_inventory_document
    AFTER UPDATE OF collection, grant_catalog_assigned, status ON documents
    FOR EACH ROW WHEN (
        (OLD.grant_catalog_assigned OR NEW.grant_catalog_assigned) AND
        ((OLD.collection, OLD.grant_catalog_assigned, (OLD.status = 'deleting')) IS DISTINCT FROM
         (NEW.collection, NEW.grant_catalog_assigned, (NEW.status = 'deleting')))
    ) EXECUTE FUNCTION grant_catalog_capture_inventory_document();

CREATE FUNCTION grant_catalog_flush_inventory_revisions() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    -- The first deferred event drains the transaction's distinct keys. Take
    -- counter locks in the same order across transactions, after policy writes.
    EXECUTE format('
        WITH pending AS (
            DELETE FROM %1$I.grant_catalog_inventory_revision_pending WHERE xact_id = $1
            RETURNING tenant_id, principal_id
        )
        INSERT INTO %1$I.grant_catalog_inventory_revisions AS r (tenant_id, principal_id, revision)
        SELECT tenant_id, principal_id, 1 FROM pending ORDER BY tenant_id, principal_id
        ON CONFLICT (tenant_id, principal_id) DO UPDATE SET revision = r.revision + 1',
        TG_TABLE_SCHEMA) USING NEW.xact_id;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER flush_inventory_revisions
    AFTER INSERT ON grant_catalog_inventory_revision_pending
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION grant_catalog_flush_inventory_revisions();

-- Both revisions come from the same snapshot as the active-principal lookup.
CREATE FUNCTION grant_catalog_inventory_policy_state(p_tenant UUID, p_principal UUID)
RETURNS TABLE (active BOOLEAN, policy_revision BIGINT, catalog_revision BIGINT)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT EXISTS (
        SELECT 1 FROM grant_catalog_active_principals p
        WHERE p.tenant_id = p_tenant AND p.principal_id = p_principal
    ),
    COALESCE((
        SELECT r.revision FROM grant_catalog_inventory_revisions r
        WHERE r.tenant_id = p_tenant AND r.principal_id = p_principal
    ), 0),
    (SELECT r.revision FROM grant_catalog_policy_revision r WHERE r.singleton);
END;

-- Remove PUBLIC and inherited default access. Only the state function is
-- granted explicitly to serving logins when they upgrade.
DO $$
DECLARE
    catalog_schema NAME := current_schema();
    permission RECORD;
BEGIN
    FOR permission IN
        SELECT 'TABLE' AS kind, format('%I.%I', catalog_schema, c.relname) AS identity, acl.grantee
        FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) acl
        WHERE c.oid IN ('grant_catalog_inventory_revisions'::regclass, 'grant_catalog_inventory_revision_pending'::regclass)
          AND acl.grantee <> c.relowner
        UNION ALL
        SELECT 'FUNCTION', format('%I.%I(%s)', catalog_schema, p.proname, pg_get_function_identity_arguments(p.oid)), acl.grantee
        FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE p.oid IN (
            'grant_catalog_enqueue_inventory_revision(uuid,uuid)'::regprocedure,
            'grant_catalog_capture_inventory_principal()'::regprocedure,
            'grant_catalog_capture_inventory_create()'::regprocedure,
            'grant_catalog_capture_inventory_grant()'::regprocedure,
            'grant_catalog_capture_inventory_ownership()'::regprocedure,
            'grant_catalog_capture_inventory_document()'::regprocedure,
            'grant_catalog_flush_inventory_revisions()'::regprocedure,
            'grant_catalog_inventory_policy_state(uuid,uuid)'::regprocedure
        ) AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON %s %s FROM %s CASCADE',
            permission.kind, permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
