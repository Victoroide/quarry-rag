CREATE TABLE evaluation_fixtures (
    document_id UUID PRIMARY KEY CHECK ((get_byte(uuid_send(document_id), 6) >> 4) = 5),
    receipt UUID NOT NULL,
    incarnation UUID NOT NULL,
    active_document_id UUID UNIQUE REFERENCES documents(id) CHECK (active_document_id IS NULL OR active_document_id = document_id)
);

CREATE FUNCTION prevent_evaluation_fixture_reuse() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.id = OLD.id AND NEW.incarnation IS NOT DISTINCT FROM OLD.incarnation THEN
        RETURN NEW;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.id::text, 487011));
    IF EXISTS (SELECT 1 FROM evaluation_fixtures WHERE document_id = NEW.id) THEN
        RAISE EXCEPTION 'evaluation fixture document identity is reserved' USING ERRCODE = '23505';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER prevent_evaluation_fixture_reuse
BEFORE INSERT OR UPDATE OF id, incarnation ON documents
FOR EACH ROW WHEN ((get_byte(uuid_send(NEW.id), 6) >> 4) = 5)
EXECUTE FUNCTION prevent_evaluation_fixture_reuse();
