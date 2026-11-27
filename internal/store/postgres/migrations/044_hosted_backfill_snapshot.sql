-- Bind hosted verification to the active pointer and its ordered manifest.
CREATE FUNCTION grant_catalog_hosted_backfill_snapshot(p_document UUID)
RETURNS TABLE (
    manifest_present BOOLEAN, manifest_valid BOOLEAN, revision_id UUID,
    vector_ids TEXT[], manifest_chunk_ids TEXT[], manifest_digest TEXT)
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    WITH snapshot AS (
        SELECT r.active_revision_id, r.incarnation AS pointer_incarnation,
            m.state, m.document_id, m.incarnation, m.collection,
            m.vector_namespace, m.id_prefix, m.vector_ids, m.chunk_ids,
            (r.document_id IS NOT NULL OR EXISTS (
                SELECT 1 FROM hosted_vector_manifests intent
                WHERE intent.document_id = d.id AND intent.state IN ('writing', 'active')
            )) AS present,
            COALESCE(
                r.incarnation = COALESCE(d.incarnation, d.id)
                AND m.document_id = d.id AND m.incarnation = r.incarnation
                AND m.state = 'active' AND m.collection = d.collection
                AND m.vector_namespace IN ('', '__default__')
                AND m.id_prefix = d.id::TEXT || '#' || r.active_revision_id::TEXT || '#'
                AND NOT EXISTS (SELECT 1 FROM hosted_vector_cleanups x WHERE x.revision_id = m.revision_id)
                AND array_ndims(m.vector_ids) = 1 AND array_lower(m.vector_ids, 1) = 1
                AND array_ndims(m.chunk_ids) = 1 AND array_lower(m.chunk_ids, 1) = 1
                AND cardinality(m.vector_ids) = cardinality(m.chunk_ids)
                AND cardinality(m.vector_ids) > 0
                AND NOT EXISTS (SELECT 1 FROM unnest(m.vector_ids) v(id) WHERE v.id IS NULL)
                AND NOT EXISTS (SELECT 1 FROM unnest(m.chunk_ids) c(id) WHERE c.id IS NULL),
                FALSE
            ) AS active_valid
        FROM documents d
        LEFT JOIN hosted_vector_revisions r ON r.document_id = d.id
        LEFT JOIN hosted_vector_manifests m ON m.revision_id = r.active_revision_id
        WHERE d.id = p_document
    )
    SELECT s.present, NOT s.present OR s.active_valid, s.active_revision_id,
        CASE WHEN s.active_valid THEN s.vector_ids ELSE ARRAY[]::TEXT[] END,
        CASE WHEN s.active_valid THEN ARRAY(
            SELECT replace(c.id::TEXT, '-', '')
            FROM unnest(s.chunk_ids) WITH ORDINALITY AS c(id, ord) ORDER BY c.ord
        ) ELSE ARRAY[]::TEXT[] END,
        CASE WHEN NOT s.present OR s.active_valid THEN
            encode(sha256(convert_to(
                (CASE WHEN s.present THEN
                    jsonb_build_array('quarry.hosted-backfill.v1'::TEXT, s.active_revision_id,
                        s.pointer_incarnation, s.state, s.document_id, s.incarnation, s.collection,
                        s.vector_namespace, s.id_prefix, s.vector_ids, s.chunk_ids)
                ELSE jsonb_build_array('quarry.hosted-backfill.v1'::TEXT, NULL::UUID) END)::TEXT,
                'UTF8')), 'hex')
        END
    FROM snapshot s;
END;

CREATE FUNCTION grant_catalog_hosted_backfill_candidates(p_after UUID, p_limit INTEGER)
RETURNS TABLE (
    document_id UUID, incarnation UUID, tenant_id UUID, source_version TEXT,
    chunk_ids TEXT[], chunk_text_digests TEXT[], chunk_token_counts INTEGER[],
    chunk_digest TEXT, scoped_vectors BOOLEAN, vector_attempt UUID,
    manifest_present BOOLEAN, manifest_valid BOOLEAN, revision_id UUID,
    vector_ids TEXT[], manifest_chunk_ids TEXT[], manifest_digest TEXT,
    source_current BOOLEAN, declared_chunks INTEGER, chunk_indexes INTEGER[],
    deletion_reserved BOOLEAN, collection TEXT)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    WITH candidates AS (
        SELECT o.document_id, o.incarnation, o.tenant_id, o.source_version,
            d.scoped_vectors, d.vector_attempt, d.chunk_count, d.collection
        FROM grant_catalog_document_ownership o
        JOIN documents d ON d.id = o.document_id AND COALESCE(d.incarnation, d.id) = o.incarnation
            AND d.grant_catalog_assigned AND d.status = 'completed'
            AND NOT d.vector_mutation_pending AND NOT d.vector_mutation_uncertain
        WHERE o.origin = 'imported' AND NOT o.ready AND (p_after IS NULL OR o.document_id > p_after)
        ORDER BY o.document_id
        LIMIT LEAST(GREATEST(p_limit, 1), 50)
    )
    SELECT d.document_id, d.incarnation, d.tenant_id, d.source_version,
        COALESCE(c.ids, ARRAY[]::TEXT[]),
        COALESCE(c.text_digests, ARRAY[]::TEXT[]),
        COALESCE(c.token_counts, ARRAY[]::INTEGER[]),
        c.digest, d.scoped_vectors, d.vector_attempt,
        m.manifest_present, m.manifest_valid, m.revision_id, m.vector_ids, m.manifest_chunk_ids, m.manifest_digest,
        COALESCE(d.source_version = s.source_version, FALSE), d.chunk_count,
        COALESCE(c.indexes, ARRAY[]::INTEGER[]), document_deletion_reserved(d.document_id), d.collection
    FROM candidates d
    LEFT JOIN grant_catalog_source_documents s ON s.document_id = d.document_id AND s.incarnation = d.incarnation
    LEFT JOIN LATERAL grant_catalog_hosted_backfill_snapshot(d.document_id) m ON TRUE
    CROSS JOIN LATERAL (
        SELECT array_agg(replace(ch.id::TEXT, '-', '') ORDER BY ch.chunk_index, ch.id) AS ids,
            array_agg(h.text_digest ORDER BY ch.chunk_index, ch.id) AS text_digests,
            array_agg(ch.token_count ORDER BY ch.chunk_index, ch.id) AS token_counts,
            array_agg(ch.chunk_index ORDER BY ch.chunk_index, ch.id) AS indexes,
            encode(sha256(convert_to(COALESCE(string_agg(
                ch.chunk_index::TEXT || ':' || replace(ch.id::TEXT, '-', '') || ':' || ch.token_count::TEXT || ':' || h.text_digest,
                ',' ORDER BY ch.chunk_index, ch.id), ''), 'UTF8')), 'hex') AS digest
        FROM chunks ch
        CROSS JOIN LATERAL (SELECT encode(sha256(convert_to(ch.text, 'UTF8')), 'hex') AS text_digest) h
        WHERE ch.document_id = d.document_id
    ) c
    ORDER BY d.document_id;
END;

CREATE FUNCTION grant_catalog_publish_hosted_backfill(
    p_document UUID, p_incarnation UUID, p_tenant UUID, p_source_version TEXT,
    p_chunk_digest TEXT, p_manifest_digest TEXT)
RETURNS BOOLEAN
LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp SET timezone = 'UTC' SET datestyle = 'ISO, YMD'
BEGIN ATOMIC
    -- A compatible service writer can hold the generation barrier too. Read the
    -- current snapshot in a separate statement after any row lock wait.
    SELECT d.id FROM documents d
    WHERE d.id = p_document AND COALESCE(d.incarnation, d.id) = p_incarnation
    FOR NO KEY UPDATE;
    SELECT r.active_revision_id FROM hosted_vector_revisions r
    WHERE r.document_id = p_document
    FOR SHARE;
    SELECT m.revision_id FROM hosted_vector_manifests m
    WHERE m.revision_id = (
        SELECT r.active_revision_id FROM hosted_vector_revisions r WHERE r.document_id = p_document
    )
    FOR SHARE;

    WITH eligible AS MATERIALIZED (
        SELECT o.document_id, o.ready
        FROM grant_catalog_document_ownership o
        JOIN documents d ON d.id = o.document_id AND COALESCE(d.incarnation, d.id) = o.incarnation
        JOIN grant_catalog_source_documents s ON s.document_id = o.document_id AND s.incarnation = o.incarnation
        CROSS JOIN LATERAL grant_catalog_hosted_backfill_snapshot(d.id) m
        CROSS JOIN LATERAL (
            SELECT count(*) AS count,
                array_agg(replace(ch.id::TEXT, '-', '') ORDER BY ch.chunk_index, ch.id) AS ids,
                array_agg(ch.chunk_index ORDER BY ch.chunk_index, ch.id) AS indexes,
                encode(sha256(convert_to(COALESCE(string_agg(
                    ch.chunk_index::TEXT || ':' || replace(ch.id::TEXT, '-', '') || ':' || ch.token_count::TEXT || ':' || encode(sha256(convert_to(ch.text, 'UTF8')), 'hex'),
                    ',' ORDER BY ch.chunk_index, ch.id), ''), 'UTF8')), 'hex') AS digest
            FROM chunks ch WHERE ch.document_id = d.id
        ) c
        WHERE o.document_id = p_document AND o.incarnation = p_incarnation
            AND o.tenant_id = p_tenant AND o.origin = 'imported'
            AND o.source_version = p_source_version AND s.source_version = p_source_version
            AND d.grant_catalog_assigned AND d.status = 'completed'
            AND NOT d.vector_mutation_pending AND NOT d.vector_mutation_uncertain
            AND NOT document_deletion_reserved(d.id)
            AND c.count > 0 AND c.count = d.chunk_count AND c.digest = p_chunk_digest
            AND m.manifest_valid AND m.manifest_digest = NULLIF(p_manifest_digest, '')
            AND (NOT m.manifest_present OR (
                c.ids = m.manifest_chunk_ids
                AND c.indexes = ARRAY(SELECT i FROM generate_series(0, cardinality(c.indexes) - 1) i ORDER BY i)
            ))
    ), published AS (
        UPDATE grant_catalog_document_ownership o SET ready = TRUE
        FROM eligible e WHERE o.document_id = e.document_id AND NOT o.ready
        RETURNING o.document_id
    )
    SELECT EXISTS (SELECT 1 FROM published) OR EXISTS (SELECT 1 FROM eligible WHERE ready);
END;

-- Entry points are granted explicitly to the catalog operator.
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
            'grant_catalog_hosted_backfill_snapshot(uuid)'::regprocedure,
            'grant_catalog_hosted_backfill_candidates(uuid,integer)'::regprocedure,
            'grant_catalog_publish_hosted_backfill(uuid,uuid,uuid,text,text,text)'::regprocedure
        ) AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON FUNCTION %s FROM %s CASCADE',
            permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
