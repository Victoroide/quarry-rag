-- Historical untagged vectors carry no document lifetime. A query compares the
-- transaction that last left a document's vectors untagged with a snapshot
-- taken before its search, instead of reading every historical document first.
-- A null stamp marks a row that was untagged before this column existed or
-- whose vectors are tagged.
ALTER TABLE documents ADD COLUMN untagged_since xid8;

CREATE FUNCTION documents_stamp_untagged() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF NEW.scoped_vectors OR NEW.vector_incarnation_required THEN
        NEW.untagged_since := NULL;
    ELSIF TG_OP = 'INSERT' OR OLD.scoped_vectors OR OLD.vector_incarnation_required THEN
        NEW.untagged_since := pg_current_xact_id();
    ELSE
        NEW.untagged_since := OLD.untagged_since;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER documents_stamp_untagged
BEFORE INSERT OR UPDATE OF scoped_vectors, vector_incarnation_required, untagged_since ON documents
FOR EACH ROW EXECUTE FUNCTION documents_stamp_untagged();
