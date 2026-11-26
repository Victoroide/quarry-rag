ALTER TABLE vector_generations ADD COLUMN pending_write_id UUID;

UPDATE vector_generations SET pending_write_id = gen_random_uuid() WHERE pending_write;

ALTER TABLE vector_generations
    ADD CONSTRAINT vector_generations_pending_write_identity
    CHECK (pending_write = (pending_write_id IS NOT NULL)
        AND (pending_write_id IS NULL OR pending_write_id <> '00000000-0000-0000-0000-000000000000'::uuid));
