-- Ordinary admission and deletion read the activation marker and the deletion
-- reservations through these owner-defined predicates, so a restricted serving
-- login needs no privilege on the receipt tables while checkpointed deletion
-- is inactive. Each body binds the installed table when it is created instead
-- of resolving names through the caller's search path. A missing marker row
-- yields NULL, which callers treat as an error rather than as an answer.
CREATE FUNCTION document_deletion_active() RETURNS BOOLEAN
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT activated_at IS NOT NULL FROM document_deletion_activation WHERE singleton;
END;

-- An unfinished or held deletion keeps its public UUID from being admitted again.
CREATE FUNCTION document_deletion_reserved(p_document_id UUID) RETURNS BOOLEAN
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT EXISTS (SELECT 1 FROM document_deletions WHERE document_id = p_document_id AND (held OR NOT finalized));
END;
