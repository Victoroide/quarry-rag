CREATE OR REPLACE FUNCTION guard_hosted_manifest_cutover() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    destination_store TEXT;
    first_binding BOOLEAN := FALSE;
    active_generation TEXT;
    legacy_store TEXT;
    already_registered BOOLEAN;
    outstanding RECORD;
BEGIN
    IF TG_WHEN = 'AFTER' THEN
        -- Only an actual change can advance the row version. A stale snapshot
        -- must fail to lock the old version that omits the new route.
        EXECUTE format('UPDATE %I.vector_state SET active_profile_hash = active_profile_hash
            WHERE singleton AND active_generation = $1 AND legacy_store IS NULL', TG_TABLE_SCHEMA)
            USING NEW.name;
        RETURN NULL;
    END IF;

    IF TG_TABLE_NAME = 'vector_state' THEN
        IF OLD.active_generation IS DISTINCT FROM NEW.active_generation THEN
            EXECUTE format('SELECT store FROM %I.vector_generations WHERE name = $1', TG_TABLE_SCHEMA)
                INTO destination_store USING NEW.active_generation;
            IF destination_store IS NULL AND NEW.active_generation = 'legacy' THEN
                destination_store := NEW.legacy_store;
            END IF;
        ELSIF NEW.active_generation = 'legacy' AND OLD.legacy_store IS NULL AND NEW.legacy_store = 'postgres' THEN
            -- The registered route takes precedence even when the physical
            -- binding is unset. A startup can fill it for an existing route.
            EXECUTE format('SELECT store FROM %I.vector_generations WHERE name = $1', TG_TABLE_SCHEMA)
                INTO destination_store USING NEW.active_generation;
            IF destination_store = 'postgres' THEN
                RETURN NEW;
            END IF;
            destination_store := 'postgres';
            first_binding := TRUE;
        ELSE
            RETURN NEW;
        END IF;
    ELSE
        EXECUTE format('SELECT active_generation, legacy_store FROM %I.vector_state WHERE singleton FOR UPDATE', TG_TABLE_SCHEMA)
            INTO active_generation, legacy_store;
        IF active_generation IS DISTINCT FROM NEW.name OR legacy_store IS NOT NULL THEN
            RETURN NEW;
        END IF;

        IF TG_OP = 'INSERT' THEN
            -- The cleanup reservation guard serializes registry key writes on
            -- the singleton. Let PostgreSQL resolve either registry conflict;
            -- an upsert that changes the route reaches the UPDATE trigger.
            EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.vector_generations
                WHERE name = $1 OR (store = $2 AND target = $3))', TG_TABLE_SCHEMA)
                INTO already_registered USING NEW.name, NEW.store, NEW.target;
            IF already_registered THEN
                RETURN NEW;
            END IF;
        END IF;
        destination_store := 'postgres';
        first_binding := TRUE;
    END IF;

    IF destination_store IS DISTINCT FROM 'postgres' THEN
        RETURN NEW;
    END IF;

    IF current_setting('transaction_isolation') <> 'read committed' THEN
        IF first_binding THEN
            RAISE EXCEPTION 'first PostgreSQL legacy binding requires READ COMMITTED'
                USING ERRCODE = '55000', CONSTRAINT = 'hosted_manifest_cutover_guard';
        END IF;
        RAISE EXCEPTION 'PostgreSQL generation activation requires READ COMMITTED'
            USING ERRCODE = '55000', CONSTRAINT = 'hosted_manifest_cutover_guard';
    END IF;

    -- Both triggers hold the singleton lock. Take a fresh snapshot after the
    -- lock so any hosted intent that committed while waiting is visible.
    EXECUTE format('SELECT * FROM %I.hosted_manifest_cutover_counts()', TG_TABLE_SCHEMA)
        INTO outstanding;
    IF outstanding.writing > 0 OR outstanding.active > 0 OR
       outstanding.retired > 0 OR outstanding.cleanups > 0 THEN
        IF first_binding THEN
            RAISE EXCEPTION 'drain outstanding hosted manifest work on the original source before binding legacy to PostgreSQL'
                USING ERRCODE = '55000', CONSTRAINT = 'hosted_manifest_cutover_guard';
        END IF;
        RAISE EXCEPTION 'drain outstanding hosted manifest work on the original source before activating PostgreSQL'
            USING ERRCODE = '55000', CONSTRAINT = 'hosted_manifest_cutover_guard';
    END IF;

    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION guard_hosted_manifest_cutover() FROM PUBLIC;

DROP TRIGGER hosted_manifest_cutover_guard ON vector_state;
CREATE TRIGGER hosted_manifest_cutover_guard
    BEFORE UPDATE OF active_generation, legacy_store ON vector_state
    FOR EACH ROW EXECUTE FUNCTION guard_hosted_manifest_cutover();

CREATE TRIGGER hosted_manifest_legacy_registration_guard
    BEFORE INSERT ON vector_generations
    FOR EACH ROW WHEN (NEW.name = 'legacy' AND NEW.store = 'postgres')
    EXECUTE FUNCTION guard_hosted_manifest_cutover();

CREATE TRIGGER hosted_manifest_legacy_registration_update_guard
    BEFORE UPDATE OF name, store ON vector_generations
    FOR EACH ROW WHEN (NEW.name = 'legacy' AND NEW.store = 'postgres'
        AND (OLD.name IS DISTINCT FROM NEW.name OR OLD.store IS DISTINCT FROM NEW.store))
    EXECUTE FUNCTION guard_hosted_manifest_cutover();

CREATE TRIGGER hosted_manifest_legacy_registration_fence
    AFTER INSERT ON vector_generations
    FOR EACH ROW WHEN (NEW.name = 'legacy' AND NEW.store = 'postgres')
    EXECUTE FUNCTION guard_hosted_manifest_cutover();

CREATE TRIGGER hosted_manifest_legacy_registration_update_fence
    AFTER UPDATE OF name, store ON vector_generations
    FOR EACH ROW WHEN (NEW.name = 'legacy' AND NEW.store = 'postgres'
        AND (OLD.name IS DISTINCT FROM NEW.name OR OLD.store IS DISTINCT FROM NEW.store))
    EXECUTE FUNCTION guard_hosted_manifest_cutover();

-- The existing BEFORE trigger fences other transactions. The AFTER trigger
-- also sees a binding made while evaluating this statement's input.
CREATE TRIGGER hosted_manifest_insert_recheck
    AFTER INSERT ON hosted_vector_manifests
    FOR EACH ROW EXECUTE FUNCTION guard_hosted_manifest_insert();
