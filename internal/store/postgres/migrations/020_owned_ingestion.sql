-- Origin separates documents accepted under a verified owner from historical
-- assignments. Only accepted documents publish their own readiness, because
-- historical ones stay quarantined until their stored data is verified.
ALTER TABLE grant_catalog_document_ownership
    ADD COLUMN origin TEXT NOT NULL DEFAULT 'imported' CHECK (origin IN ('imported', 'accepted'));

-- Decide, before any extraction or external write, what an upload may use.
-- The outcome is the same for every identity the caller cannot use.
CREATE FUNCTION grant_catalog_authorize_ingest(p_tenant UUID, p_principal UUID, p_collection TEXT, p_document UUID)
RETURNS TABLE (outcome TEXT, target_incarnation UUID)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    WITH owned AS (
        SELECT o.incarnation FROM grant_catalog_document_ownership o
        JOIN documents d ON d.id = o.document_id AND COALESCE(d.incarnation, d.id) = o.incarnation
            AND d.grant_catalog_assigned
        WHERE o.document_id = p_document AND o.tenant_id = p_tenant AND o.principal_id = p_principal
    ), reserved AS (
        SELECT r.incarnation FROM grant_catalog_document_reservations r
        WHERE r.document_id = p_document AND r.tenant_id = p_tenant AND r.principal_id = p_principal
            AND r.collection = p_collection AND r.state = 'pending'
            AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.id = p_document)
    )
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM grant_catalog_collection_creators c
            WHERE c.tenant_id = p_tenant AND c.principal_id = p_principal AND c.collection = p_collection
        ) THEN 'forbidden'
        WHEN p_document IS NULL THEN 'new'
        WHEN EXISTS (SELECT 1 FROM owned) THEN 'existing'
        WHEN EXISTS (SELECT 1 FROM reserved) THEN 'reserved'
        ELSE 'missing'
    END,
    COALESCE((SELECT incarnation FROM owned), (SELECT incarnation FROM reserved));
END;

-- Hold the create permission and the principal so a revocation waits for the
-- acceptance transaction instead of landing between its check and its commit.
CREATE FUNCTION grant_catalog_lock_ingest_permission(p_tenant UUID, p_principal UUID, p_collection TEXT)
RETURNS BOOLEAN
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT COALESCE((
        SELECT TRUE FROM grant_catalog_create_permissions cp
        JOIN grant_catalog_principals pr ON pr.tenant_id = cp.tenant_id AND pr.principal_id = cp.principal_id
        WHERE cp.tenant_id = p_tenant AND cp.principal_id = p_principal AND cp.collection = p_collection
            AND cp.can_create AND pr.enabled
        LIMIT 1 FOR SHARE OF cp, pr
    ), FALSE);
END;

CREATE FUNCTION grant_catalog_may_bind(
    p_tenant UUID, p_principal UUID, p_collection TEXT, p_document UUID, p_incarnation UUID, p_reserved BOOLEAN)
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT EXISTS (
        SELECT 1 FROM grant_catalog_collection_creators c
        WHERE c.tenant_id = p_tenant AND c.principal_id = p_principal AND c.collection = p_collection
            AND EXISTS (
                SELECT 1 FROM documents d
                WHERE d.id = p_document AND d.incarnation = p_incarnation AND d.collection = p_collection
            )
            AND (NOT p_reserved OR EXISTS (
                SELECT 1 FROM grant_catalog_document_reservations r
                WHERE r.document_id = p_document AND r.incarnation = p_incarnation
                    AND r.tenant_id = p_tenant AND r.principal_id = p_principal
                    AND r.collection = p_collection AND r.state = 'pending'
            ))
    );
END;

-- Assign the inserted document to its owner in the caller's transaction. Each
-- statement repeats the permission, reservation and identity guard, and the
-- result reports whether the owner is now bound to exactly this incarnation.
CREATE FUNCTION grant_catalog_bind_accepted_document(
    p_tenant UUID, p_principal UUID, p_collection TEXT, p_document UUID, p_incarnation UUID, p_reserved BOOLEAN)
RETURNS BOOLEAN
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp SET timezone = 'UTC' SET datestyle = 'ISO, YMD'
BEGIN ATOMIC
    INSERT INTO grant_catalog_assigned_incarnations (document_id, incarnation)
    SELECT p_document, p_incarnation
    WHERE grant_catalog_may_bind(p_tenant, p_principal, p_collection, p_document, p_incarnation, p_reserved);

    INSERT INTO grant_catalog_document_ownership
        (document_id, incarnation, tenant_id, principal_id, collection, source_version, origin)
    SELECT s.document_id, s.incarnation, p_tenant, p_principal, p_collection, s.source_version, 'accepted'
    FROM grant_catalog_source_documents s
    WHERE s.document_id = p_document AND s.incarnation = p_incarnation
        AND EXISTS (
            SELECT 1 FROM grant_catalog_assigned_incarnations a
            WHERE a.document_id = p_document AND a.incarnation = p_incarnation
        )
        AND grant_catalog_may_bind(p_tenant, p_principal, p_collection, p_document, p_incarnation, p_reserved);

    UPDATE documents SET grant_catalog_assigned = TRUE
    WHERE id = p_document AND incarnation = p_incarnation AND NOT grant_catalog_assigned
        AND EXISTS (
            SELECT 1 FROM grant_catalog_document_ownership o
            WHERE o.document_id = p_document AND o.incarnation = p_incarnation
                AND o.tenant_id = p_tenant AND o.principal_id = p_principal AND o.origin = 'accepted'
        );

    UPDATE grant_catalog_document_reservations SET state = 'spent'
    WHERE p_reserved AND document_id = p_document AND incarnation = p_incarnation AND state = 'pending'
        AND EXISTS (
            SELECT 1 FROM grant_catalog_document_ownership o
            WHERE o.document_id = p_document AND o.incarnation = p_incarnation
                AND o.tenant_id = p_tenant AND o.principal_id = p_principal AND o.origin = 'accepted'
        );

    SELECT EXISTS (
        SELECT 1 FROM documents d
        JOIN grant_catalog_document_ownership o ON o.document_id = d.id AND o.incarnation = d.incarnation
        WHERE d.id = p_document AND d.incarnation = p_incarnation AND d.grant_catalog_assigned
            AND o.tenant_id = p_tenant AND o.principal_id = p_principal AND o.origin = 'accepted'
    );
END;

-- A completed accepted document is complete by construction: its chunks and
-- vectors were written for this incarnation under a lease the same transaction checks.
CREATE FUNCTION grant_catalog_publish_accepted_document(p_document UUID, p_incarnation UUID)
RETURNS VOID
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    UPDATE grant_catalog_document_ownership SET ready = TRUE
    WHERE document_id = p_document AND incarnation = p_incarnation AND origin = 'accepted' AND NOT ready;
END;

-- Serving logins need no catalog privilege to learn the tenant that pins a job.
CREATE FUNCTION grant_catalog_document_tenant(p_document UUID, p_incarnation UUID)
RETURNS UUID
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT o.tenant_id FROM grant_catalog_document_ownership o
    WHERE o.document_id = p_document AND o.incarnation = p_incarnation;
END;

-- List quarantined historical assignments with their authoritative chunk
-- identities, spelled as the vector stores hold them (32 hexadecimal digits),
-- per-chunk content digests, token counts and the vector format each document
-- was written in. A document with an unresolved vector mutation is skipped
-- until the mutation settles. The batch is ordered by document so an
-- interrupted run resumes from the last identity it finished.
CREATE FUNCTION grant_catalog_backfill_candidates(p_after UUID, p_limit INTEGER)
RETURNS TABLE (
    document_id UUID, incarnation UUID, tenant_id UUID, source_version TEXT,
    chunk_ids TEXT[], chunk_text_digests TEXT[], chunk_token_counts INTEGER[],
    chunk_digest TEXT, scoped_vectors BOOLEAN, vector_attempt UUID)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT o.document_id, o.incarnation, o.tenant_id, o.source_version,
        COALESCE(c.ids, ARRAY[]::TEXT[]),
        COALESCE(c.text_digests, ARRAY[]::TEXT[]),
        COALESCE(c.token_counts, ARRAY[]::INTEGER[]),
        c.digest,
        d.scoped_vectors, d.vector_attempt
    FROM grant_catalog_document_ownership o
    JOIN documents d ON d.id = o.document_id AND COALESCE(d.incarnation, d.id) = o.incarnation
        AND d.grant_catalog_assigned AND d.status = 'completed'
        AND NOT d.vector_mutation_pending AND NOT d.vector_mutation_uncertain
    LEFT JOIN LATERAL (
        SELECT array_agg(replace(ch.id::TEXT, '-', '') ORDER BY ch.chunk_index) AS ids,
            array_agg(encode(sha256(convert_to(ch.text, 'UTF8')), 'hex') ORDER BY ch.chunk_index) AS text_digests,
            array_agg(ch.token_count ORDER BY ch.chunk_index) AS token_counts,
            encode(sha256(convert_to(COALESCE(string_agg(
                ch.chunk_index::TEXT || ':' || replace(ch.id::TEXT, '-', '') || ':' || ch.token_count::TEXT || ':' || encode(sha256(convert_to(ch.text, 'UTF8')), 'hex'),
                ',' ORDER BY ch.chunk_index), ''), 'UTF8')), 'hex') AS digest
        FROM chunks ch WHERE ch.document_id = d.id
    ) c ON TRUE
    WHERE o.origin = 'imported' AND NOT o.ready AND (p_after IS NULL OR o.document_id > p_after)
    ORDER BY o.document_id
    LIMIT LEAST(GREATEST(p_limit, 1), 50);
END;

-- Publish readiness only for the exact document row and chunk set that were
-- verified. The source version depends on time zone and date style, so both match the snapshot function. Any change since the batch was read leaves the document quarantined.
CREATE FUNCTION grant_catalog_publish_verified_document(
    p_document UUID, p_incarnation UUID, p_source_version TEXT, p_chunk_digest TEXT)
RETURNS BOOLEAN
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp SET timezone = 'UTC' SET datestyle = 'ISO, YMD'
BEGIN ATOMIC
    UPDATE grant_catalog_document_ownership o SET ready = TRUE
    FROM grant_catalog_source_documents s
    WHERE o.document_id = p_document AND o.incarnation = p_incarnation AND o.origin = 'imported' AND NOT o.ready
        AND o.source_version = p_source_version
        AND s.document_id = o.document_id AND s.incarnation = o.incarnation AND s.source_version = p_source_version
        AND EXISTS (
            SELECT 1 FROM documents d
            WHERE d.id = o.document_id AND d.status = 'completed'
                AND NOT d.vector_mutation_pending AND NOT d.vector_mutation_uncertain
                AND d.chunk_count = (SELECT count(*) FROM chunks ch WHERE ch.document_id = d.id)
        )
        AND encode(sha256(convert_to(COALESCE((
            SELECT string_agg(
                ch.chunk_index::TEXT || ':' || replace(ch.id::TEXT, '-', '') || ':' || ch.token_count::TEXT || ':' || encode(sha256(convert_to(ch.text, 'UTF8')), 'hex'),
                ',' ORDER BY ch.chunk_index)
            FROM chunks ch WHERE ch.document_id = o.document_id), ''), 'UTF8')), 'hex') = p_chunk_digest;

    SELECT EXISTS (
        SELECT 1 FROM grant_catalog_document_ownership o
        WHERE o.document_id = p_document AND o.incarnation = p_incarnation AND o.ready
    );
END;

-- Remove PUBLIC and inherited default access from the new entry points. The
-- schema owner grants execution to the serving and operator logins.
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
        WHERE p.pronamespace = catalog_namespace AND p.oid IN (
            'grant_catalog_authorize_ingest(uuid,uuid,text,uuid)'::regprocedure,
            'grant_catalog_lock_ingest_permission(uuid,uuid,text)'::regprocedure,
            'grant_catalog_may_bind(uuid,uuid,text,uuid,uuid,boolean)'::regprocedure,
            'grant_catalog_bind_accepted_document(uuid,uuid,text,uuid,uuid,boolean)'::regprocedure,
            'grant_catalog_publish_accepted_document(uuid,uuid)'::regprocedure,
            'grant_catalog_document_tenant(uuid,uuid)'::regprocedure,
            'grant_catalog_backfill_candidates(uuid,integer)'::regprocedure,
            'grant_catalog_publish_verified_document(uuid,uuid,text,text)'::regprocedure
        ) AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON FUNCTION %s FROM %s CASCADE',
            permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
