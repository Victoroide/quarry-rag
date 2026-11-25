ALTER TABLE documents
    ADD COLUMN index_visibility TEXT,
    ADD COLUMN index_visibility_observed_at TIMESTAMPTZ CONSTRAINT documents_index_visibility_check CHECK (
        (index_visibility IS NULL AND index_visibility_observed_at IS NULL)
        OR (index_visibility IS NOT NULL AND index_visibility_observed_at IS NOT NULL
            AND index_visibility IN ('unknown', 'disabled', 'timeout', 'sample_seen', 'writes_indexed', 'transactional'))
    );

ALTER TABLE ingest_jobs ADD COLUMN lease_token UUID;
