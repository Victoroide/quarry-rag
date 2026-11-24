CREATE TABLE document_deletions (
    document_id UUID PRIMARY KEY,
    incarnation UUID NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('legacy_reservation', 'qualified_intent', 'quarantine')),
    source_key TEXT,
    source_bucket TEXT,
    vector_mode TEXT NOT NULL CHECK (vector_mode IN ('local', 'legacy', 'revision', 'memory', 'memory_revision')),
    vector_index TEXT,
    vector_namespace TEXT,
    memory_owner UUID,
    step INTEGER NOT NULL DEFAULT 0 CHECK (step BETWEEN 0 AND 3),
    held BOOLEAN NOT NULL DEFAULT FALSE,
    finalized BOOLEAN NOT NULL DEFAULT FALSE,
    claim UUID,
    claim_until TIMESTAMPTZ
);

CREATE TABLE document_deletion_activation (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    activated_at TIMESTAMPTZ
);

INSERT INTO document_deletion_activation DEFAULT VALUES;

ALTER TABLE documents ADD COLUMN incarnation UUID;
ALTER TABLE documents ALTER COLUMN incarnation SET DEFAULT gen_random_uuid();
