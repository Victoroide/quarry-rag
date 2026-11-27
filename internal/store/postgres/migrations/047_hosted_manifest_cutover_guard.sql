CREATE FUNCTION hosted_manifest_cutover_counts()
RETURNS TABLE (writing BIGINT, active BIGINT, retired BIGINT, cleanups BIGINT)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT count(*) FILTER (WHERE m.state = 'writing'),
           count(*) FILTER (WHERE m.state = 'active'),
           count(*) FILTER (WHERE m.state = 'retired'),
           (SELECT count(*) FROM hosted_vector_cleanups)
    FROM hosted_vector_manifests m;
END;

CREATE FUNCTION guard_hosted_manifest_cutover() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    destination_store TEXT;
    outstanding RECORD;
BEGIN
    IF OLD.active_generation = NEW.active_generation THEN
        RETURN NEW;
    END IF;

    EXECUTE format('SELECT store FROM %I.vector_generations WHERE name = $1', TG_TABLE_SCHEMA)
        INTO destination_store USING NEW.active_generation;
    IF destination_store IS NULL AND NEW.active_generation = 'legacy' THEN
        destination_store := NEW.legacy_store;
    END IF;
    IF destination_store IS DISTINCT FROM 'postgres' THEN
        RETURN NEW;
    END IF;

    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'PostgreSQL generation activation requires READ COMMITTED'
            USING ERRCODE = '55000', CONSTRAINT = 'hosted_manifest_cutover_guard';
    END IF;

    -- The row update already holds the singleton lock. This query starts a
    -- fresh snapshot so it sees intents that committed while the update waited.
    EXECUTE format('SELECT * FROM %I.hosted_manifest_cutover_counts()', TG_TABLE_SCHEMA)
        INTO outstanding;
    IF outstanding.writing > 0 OR outstanding.active > 0 OR
       outstanding.retired > 0 OR outstanding.cleanups > 0 THEN
        RAISE EXCEPTION 'drain outstanding hosted manifest work on the original source before activating PostgreSQL'
            USING ERRCODE = '55000', CONSTRAINT = 'hosted_manifest_cutover_guard';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION guard_hosted_manifest_insert() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    active_store TEXT;
BEGIN
    -- Already claimed hosted writers may journal even during a freeze. Hold
    -- SHARE until commit so a concurrent switch must see the journal or win.
    EXECUTE format('SELECT store FROM %I.vector_writer_state()', TG_TABLE_SCHEMA);
    -- The first call may have waited for a switch that also registered its
    -- target. Resolve the binding again after locking with a fresh snapshot.
    EXECUTE format('SELECT store FROM %I.vector_writer_state()', TG_TABLE_SCHEMA)
        INTO active_store;
    IF active_store = 'postgres' THEN
        RAISE EXCEPTION 'cannot insert a hosted vector manifest while a PostgreSQL generation is active'
            USING ERRCODE = '55000';
    END IF;
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION hosted_manifest_cutover_counts(), guard_hosted_manifest_cutover(),
    guard_hosted_manifest_insert() FROM PUBLIC;

CREATE TRIGGER hosted_manifest_cutover_guard
    BEFORE UPDATE OF active_generation ON vector_state
    FOR EACH ROW EXECUTE FUNCTION guard_hosted_manifest_cutover();

CREATE TRIGGER hosted_manifest_insert_guard
    BEFORE INSERT ON hosted_vector_manifests
    FOR EACH STATEMENT EXECUTE FUNCTION guard_hosted_manifest_insert();
