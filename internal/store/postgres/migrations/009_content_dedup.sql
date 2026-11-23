ALTER TABLE documents
    ADD COLUMN content_dedup TEXT NOT NULL DEFAULT 'collection'
        CHECK (content_dedup IN ('collection', 'off'));

CREATE FUNCTION check_document_fingerprint_policy() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM documents WHERE id = NEW.document_id AND content_dedup = 'off') THEN
        RAISE check_violation USING MESSAGE = 'private documents cannot register shared fingerprints';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER document_fingerprint_policy
    BEFORE INSERT OR UPDATE OF document_id ON document_fingerprints
    FOR EACH ROW EXECUTE FUNCTION check_document_fingerprint_policy();
