ALTER TABLE vector_state ADD COLUMN active_cleanup_sender UUID;

UPDATE vector_state SET active_cleanup_sender=gen_random_uuid()
WHERE active_cleanup_reservation IS NOT NULL;

ALTER TABLE vector_state ADD CONSTRAINT vector_state_active_cleanup_sender CHECK (
    (active_cleanup_reservation IS NULL) = (active_cleanup_sender IS NULL)
    AND (active_cleanup_sender IS NULL OR active_cleanup_sender <> '00000000-0000-0000-0000-000000000000'::uuid)
);

CREATE OR REPLACE FUNCTION guard_active_cleanup_state() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF OLD.active_cleanup_reservation IS NOT NULL AND (
        ROW(NEW.singleton, NEW.active_generation, NEW.active_profile_hash, NEW.frozen,
            NEW.frozen_at, NEW.freeze_generation, NEW.legacy_store, NEW.legacy_target, NEW.legacy_host)
        IS DISTINCT FROM
        ROW(OLD.singleton, OLD.active_generation, OLD.active_profile_hash, OLD.frozen,
            OLD.frozen_at, OLD.freeze_generation, OLD.legacy_store, OLD.legacy_target, OLD.legacy_host)
        OR (NEW.active_cleanup_reservation IS NOT NULL AND
            ROW(NEW.active_cleanup_reservation, NEW.active_cleanup_ids, NEW.active_cleanup_sender) IS DISTINCT FROM
            ROW(OLD.active_cleanup_reservation, OLD.active_cleanup_ids, OLD.active_cleanup_sender))
    ) THEN
        RAISE EXCEPTION 'pending active vector cleanup must be resolved first' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
