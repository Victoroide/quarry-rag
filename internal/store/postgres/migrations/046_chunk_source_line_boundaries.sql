ALTER TABLE chunks ADD COLUMN source_line_boundaries integer[],
    ADD CONSTRAINT chunks_source_line_boundaries_size
    CHECK (cardinality(source_line_boundaries) <= octet_length(text)) NOT VALID;

-- A writer that predates the sidecar can still replace a chunk body in place.
-- Its old coordinates cannot describe the new body.
CREATE FUNCTION clear_replaced_chunk_source_line_boundaries() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    NEW.source_line_boundaries := NULL;
    RETURN NEW;
END
$$;

CREATE TRIGGER chunks_clear_replaced_source_line_boundaries
BEFORE UPDATE OF text ON chunks
FOR EACH ROW WHEN (
    OLD.source_line_boundaries IS NOT NULL
    AND (OLD.text COLLATE "C") IS DISTINCT FROM (NEW.text COLLATE "C")
    AND OLD.source_line_boundaries IS NOT DISTINCT FROM NEW.source_line_boundaries
)
EXECUTE FUNCTION clear_replaced_chunk_source_line_boundaries();
