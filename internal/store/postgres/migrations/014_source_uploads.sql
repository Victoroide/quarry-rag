CREATE TABLE source_uploads (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL,
    collection TEXT NOT NULL,
    bucket TEXT NOT NULL CHECK (length(bucket) BETWEEN 1 AND 255),
    object_key TEXT NOT NULL CHECK (length(object_key) BETWEEN 1 AND 1024),
    state TEXT NOT NULL DEFAULT 'staged' CHECK (state IN ('staged', 'linked', 'claimed')),
    version_id TEXT CHECK (length(version_id) <= 1024),
    acknowledged_at TIMESTAMPTZ,
    write_state TEXT NOT NULL DEFAULT 'not_started' CHECK (write_state IN ('not_started', 'uncertain', 'settled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    linked_document_id UUID REFERENCES documents (id) ON DELETE CASCADE CHECK ((state = 'linked') = (linked_document_id IS NOT NULL)),
    linked_at TIMESTAMPTZ,
    claim UUID,
    claim_until TIMESTAMPTZ,
    claimed_at TIMESTAMPTZ CHECK ((state = 'claimed') = (claimed_at IS NOT NULL)),
    checks INTEGER NOT NULL DEFAULT 0,
    last_checked_at TIMESTAMPTZ,
    next_check_at TIMESTAMPTZ,
    reason TEXT CHECK (length(reason) <= 256)
);

CREATE UNIQUE INDEX source_uploads_object ON source_uploads (bucket, object_key);
CREATE INDEX source_uploads_staged_expiry ON source_uploads (expires_at) WHERE state = 'staged';
CREATE INDEX source_uploads_claimed_check ON source_uploads (next_check_at) WHERE state = 'claimed';
