CREATE TABLE hosted_vector_manifests (
    revision_id UUID PRIMARY KEY,
    document_id UUID NOT NULL,
    incarnation UUID NOT NULL,
    attempt INTEGER NOT NULL,
    lease_token UUID,
    collection TEXT NOT NULL DEFAULT '',
    vector_namespace TEXT NOT NULL DEFAULT '',
    id_prefix TEXT NOT NULL,
    vector_ids TEXT[] NOT NULL,
    chunk_ids UUID[] NOT NULL,
    state TEXT NOT NULL DEFAULT 'writing' CHECK (state IN ('writing', 'active', 'retired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ
);

CREATE INDEX idx_hosted_vector_manifests_doc_state ON hosted_vector_manifests (document_id, state);
CREATE UNIQUE INDEX idx_hosted_vector_manifests_doc_active ON hosted_vector_manifests (document_id) WHERE state = 'active';
CREATE UNIQUE INDEX idx_hosted_vector_manifests_lease ON hosted_vector_manifests (document_id, incarnation, attempt, lease_token) WHERE lease_token IS NOT NULL;

CREATE TABLE hosted_vector_revisions (
    document_id UUID PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    incarnation UUID NOT NULL,
    active_revision_id UUID NOT NULL REFERENCES hosted_vector_manifests(revision_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE hosted_vector_cleanups (
    revision_id UUID PRIMARY KEY REFERENCES hosted_vector_manifests(revision_id),
    document_id UUID NOT NULL,
    incarnation UUID NOT NULL,
    collection TEXT NOT NULL DEFAULT '',
    vector_namespace TEXT NOT NULL DEFAULT '',
    id_prefix TEXT NOT NULL,
    vector_ids TEXT[] NOT NULL,
    next_offset INTEGER NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'verifying', 'unresolved')),
    attempts INTEGER NOT NULL DEFAULT 0,
    convergence_checks INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    lease_owner TEXT,
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason TEXT NOT NULL DEFAULT 'superseded' CHECK (reason IN ('superseded', 'failed', 'deleted', 'abandoned'))
);

CREATE INDEX idx_hosted_vector_cleanups_ready ON hosted_vector_cleanups (next_attempt_at) WHERE state IN ('pending', 'verifying', 'unresolved');
CREATE INDEX idx_hosted_vector_cleanups_doc ON hosted_vector_cleanups (document_id);

-- Readers ask whether a vector belongs to the document's active revision through
-- this owner-defined predicate, so a restricted serving login needs no privilege
-- on the revision table. The body binds the installed table when it is created.
-- A document without an active revision accepts only unrevisioned vectors.
CREATE FUNCTION hosted_vector_revision_visible(p_document_id UUID, p_revision_id UUID) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT p_revision_id IS NOT DISTINCT FROM
        (SELECT r.active_revision_id FROM hosted_vector_revisions r WHERE r.document_id = p_document_id);
END;

-- Deleting a document and failing its job both abandon every external vector the
-- document still owns. The trigger runs as the schema owner and binds the
-- installed tables, so no serving login needs a privilege on them, whether or
-- not the feature is enabled. Retired intents wait in hosted_vector_cleanups; the
-- sweeper enforces the drain window when it claims them.
CREATE FUNCTION retire_hosted_vector_manifests() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    abandoned_document UUID;
    abandoned_reason TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        abandoned_document := OLD.id;
        abandoned_reason := 'deleted';
    ELSE
        abandoned_document := NEW.id;
        abandoned_reason := 'failed';
        EXECUTE format('DELETE FROM %I.hosted_vector_revisions WHERE document_id = $1', TG_TABLE_SCHEMA) USING abandoned_document;
    END IF;
    EXECUTE format(
        'WITH retired AS (
            UPDATE %I.hosted_vector_manifests
               SET state = ''retired'',
                   retired_at = COALESCE(retired_at, statement_timestamp())
             WHERE document_id = $1 AND state IN (''writing'', ''active'')
            RETURNING revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids, retired_at
        )
        INSERT INTO %I.hosted_vector_cleanups (
            revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids,
            next_attempt_at, retired_at, reason
        )
        SELECT revision_id, document_id, incarnation, collection, vector_namespace, id_prefix, vector_ids,
               statement_timestamp(), retired_at, $2
        FROM retired
        ON CONFLICT (revision_id) DO NOTHING',
        TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
    ) USING abandoned_document, abandoned_reason;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER documents_retire_hosted_vectors
    BEFORE DELETE ON documents
    FOR EACH ROW EXECUTE FUNCTION retire_hosted_vector_manifests();

CREATE TRIGGER documents_fail_hosted_vectors
    AFTER UPDATE OF status ON documents
    FOR EACH ROW WHEN (NEW.status = 'failed' AND OLD.status IS DISTINCT FROM 'failed')
    EXECUTE FUNCTION retire_hosted_vector_manifests();

-- A document that owns manifested revisions is deleted by exact revision, so the
-- deletion receipt records that vector target beside the existing ones.
ALTER TABLE document_deletions DROP CONSTRAINT document_deletions_vector_mode_check;
ALTER TABLE document_deletions ADD CONSTRAINT document_deletions_vector_mode_check
    CHECK (vector_mode IN ('local', 'legacy', 'revision', 'memory', 'memory_revision', 'manifest'));
