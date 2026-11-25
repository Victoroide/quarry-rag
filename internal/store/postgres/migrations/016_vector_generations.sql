CREATE TABLE vector_generations (
    name TEXT PRIMARY KEY CHECK (name ~ '^[a-z][a-z0-9_]{0,23}$'),
    store TEXT NOT NULL CHECK (store IN ('postgres', 'pinecone')),
    target TEXT NOT NULL CHECK (target <> ''),
    host TEXT NOT NULL DEFAULT '' CHECK ((store = 'pinecone') = (host <> '')),
    profile JSONB NOT NULL,
    profile_hash TEXT NOT NULL,
    profile_origin TEXT NOT NULL CHECK (profile_origin IN ('recorded', 'attested')),
    state TEXT NOT NULL DEFAULT 'staging' CHECK (state IN ('staging', 'verified', 'active', 'retired')),
    corpus_digest TEXT,
    verification JSONB,
    verified_at TIMESTAMPTZ,
    ever_active BOOLEAN NOT NULL DEFAULT FALSE,
    pending_write BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_vector_generations_target ON vector_generations (store, target);

CREATE UNIQUE INDEX idx_vector_generations_host ON vector_generations (host) WHERE host <> '';

CREATE TABLE vector_generation_documents (
    generation TEXT NOT NULL REFERENCES vector_generations(name) ON DELETE CASCADE,
    document_id UUID NOT NULL,
    source_revision TEXT NOT NULL,
    incarnation UUID,
    chunk_ids TEXT[] NOT NULL,
    complete BOOLEAN NOT NULL DEFAULT FALSE,
    copied INTEGER NOT NULL DEFAULT 0,
    embedded INTEGER NOT NULL DEFAULT 0,
    checkpointed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_vector_generation_documents_key ON vector_generation_documents (generation, document_id);

CREATE TABLE vector_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    active_generation TEXT NOT NULL DEFAULT 'legacy' CHECK (active_generation ~ '^[a-z][a-z0-9_]{0,23}$'),
    active_profile_hash TEXT NOT NULL DEFAULT '',
    frozen BOOLEAN NOT NULL DEFAULT FALSE,
    frozen_at TIMESTAMPTZ,
    freeze_generation TEXT,
    legacy_store TEXT CHECK (legacy_store IN ('postgres', 'pinecone')),
    legacy_target TEXT CHECK ((legacy_store IS NULL) = (legacy_target IS NULL) AND legacy_target IS DISTINCT FROM ''),
    legacy_host TEXT NOT NULL DEFAULT '' CHECK (COALESCE(legacy_store = 'pinecone', FALSE) = (legacy_host <> ''))
);

INSERT INTO vector_state DEFAULT VALUES;

-- Bind once after the configured target has opened. The owner can provision
-- the binding before rollout or delegate first use to the serving role.
CREATE FUNCTION vector_bind_legacy(p_store TEXT, p_target TEXT, p_host TEXT)
RETURNS VOID LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    UPDATE vector_state SET legacy_store = p_store, legacy_target = p_target, legacy_host = p_host
    WHERE active_generation = 'legacy' AND legacy_store IS NULL;
END;

-- Only the singleton is locked, and the caller's transaction keeps the lock.
CREATE FUNCTION vector_writer_state()
RETURNS TABLE (active TEXT, frozen BOOLEAN, profile_hash TEXT, store TEXT, target TEXT, host TEXT)
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT s.active_generation, s.frozen, s.active_profile_hash,
           COALESCE(g.store, s.legacy_store, ''), COALESCE(g.target, s.legacy_target, ''),
           COALESCE(g.host, s.legacy_host)
    FROM vector_state s LEFT JOIN vector_generations g ON g.name = s.active_generation
    FOR SHARE OF s;
END;

REVOKE ALL ON FUNCTION vector_bind_legacy(TEXT, TEXT, TEXT), vector_writer_state() FROM PUBLIC;
