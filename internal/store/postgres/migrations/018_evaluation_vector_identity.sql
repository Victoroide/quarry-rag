ALTER TABLE evaluation_fixtures
    ADD COLUMN vector_identity TEXT CHECK (active_document_id IS NULL OR incarnation IS NOT NULL),
    ALTER COLUMN incarnation DROP NOT NULL;

-- An intent retired before admission never acquired an ordinary incarnation.
CREATE OR REPLACE FUNCTION prevent_evaluation_fixture_reuse() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.id = OLD.id AND NEW.incarnation IS NOT DISTINCT FROM OLD.incarnation THEN
        RETURN NEW;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.id::text, 487011));
    IF EXISTS (SELECT 1 FROM evaluation_fixtures WHERE document_id = NEW.id AND incarnation IS NOT NULL) THEN
        RAISE EXCEPTION 'evaluation fixture document identity is reserved' USING ERRCODE = '23505';
    END IF;
    RETURN NEW;
END;
$$;
