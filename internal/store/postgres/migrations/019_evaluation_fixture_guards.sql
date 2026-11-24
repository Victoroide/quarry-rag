-- Ordinary serving paths consult fixture reservations through these owner-defined
-- predicates, so a request-serving login needs no privilege on the receipt table.
-- Each body binds the installed table when it is created instead of resolving
-- names through the caller's search path.
CREATE FUNCTION evaluation_fixture_reserved(p_document_id UUID) RETURNS BOOLEAN
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT EXISTS (SELECT 1 FROM evaluation_fixtures WHERE document_id = p_document_id AND incarnation IS NOT NULL);
END;

-- A fixture job may be leased only by a worker attached to its bound vector store.
-- A NULL identity names a worker with no binding, which never leases fixture work.
CREATE FUNCTION evaluation_fixture_claimable(p_document_id UUID, p_vector_identity TEXT) RETURNS BOOLEAN
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT NOT EXISTS (
        SELECT 1 FROM evaluation_fixtures
        WHERE active_document_id = p_document_id
          AND (p_vector_identity IS NULL OR vector_identity IS NULL OR vector_identity <> p_vector_identity));
END;

CREATE OR REPLACE FUNCTION prevent_evaluation_fixture_reuse() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    reserved BOOLEAN;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.id = OLD.id AND NEW.incarnation IS NOT DISTINCT FROM OLD.incarnation THEN
        RETURN NEW;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.id::text, 487011));
    EXECUTE format('SELECT %I.evaluation_fixture_reserved($1)', TG_TABLE_SCHEMA) INTO reserved USING NEW.id;
    IF reserved THEN
        RAISE EXCEPTION 'evaluation fixture document identity is reserved' USING ERRCODE = '23505';
    END IF;
    RETURN NEW;
END;
$$;
