CREATE TABLE vector_generations (
    name TEXT PRIMARY KEY CHECK (name ~ '^[a-z][a-z0-9_]{0,23}$'),
    store TEXT NOT NULL CHECK (store IN ('postgres', 'pinecone')),
    target TEXT NOT NULL CHECK (target <> ''),
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
    frozen BOOLEAN NOT NULL DEFAULT FALSE,
    frozen_at TIMESTAMPTZ,
    freeze_generation TEXT,
    legacy_store TEXT CHECK (legacy_store IN ('postgres', 'pinecone')),
    legacy_target TEXT CHECK ((legacy_store IS NULL) = (legacy_target IS NULL) AND legacy_target IS DISTINCT FROM '')
);

INSERT INTO vector_state DEFAULT VALUES;
