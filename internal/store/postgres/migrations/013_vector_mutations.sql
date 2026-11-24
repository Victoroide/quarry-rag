ALTER TABLE documents
    ADD COLUMN scoped_vectors BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN vector_attempt UUID,
    ADD COLUMN vector_incarnation_required BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN vector_mutation_deletes BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN vector_delete_uncertain BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN vector_mutation_pending BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN vector_mutation_uncertain BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN vector_mutation_claim UUID,
    ADD COLUMN vector_mutation_until TIMESTAMPTZ;
