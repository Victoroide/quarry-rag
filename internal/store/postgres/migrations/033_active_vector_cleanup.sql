ALTER TABLE vector_state
    ADD COLUMN active_cleanup_ids TEXT[],
    ADD COLUMN active_cleanup_reservation UUID,
    ADD CONSTRAINT vector_state_active_cleanup_frozen CHECK (
        (active_cleanup_ids IS NULL AND active_cleanup_reservation IS NULL) OR (
            active_cleanup_ids IS NOT NULL AND active_cleanup_reservation IS NOT NULL
            AND active_cleanup_reservation <> '00000000-0000-0000-0000-000000000000'::UUID
            AND cardinality(active_cleanup_ids) BETWEEN 1 AND 1000
            AND frozen AND frozen_at IS NOT NULL
            AND freeze_generation IS NOT DISTINCT FROM active_generation
        )
    );

CREATE FUNCTION guard_active_cleanup_state() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF OLD.active_cleanup_reservation IS NOT NULL AND (
        ROW(NEW.singleton, NEW.active_generation, NEW.active_profile_hash, NEW.frozen,
            NEW.frozen_at, NEW.freeze_generation, NEW.legacy_store, NEW.legacy_target, NEW.legacy_host)
        IS DISTINCT FROM
        ROW(OLD.singleton, OLD.active_generation, OLD.active_profile_hash, OLD.frozen,
            OLD.frozen_at, OLD.freeze_generation, OLD.legacy_store, OLD.legacy_target, OLD.legacy_host)
        OR (NEW.active_cleanup_reservation IS NOT NULL AND
            ROW(NEW.active_cleanup_reservation, NEW.active_cleanup_ids) IS DISTINCT FROM
            ROW(OLD.active_cleanup_reservation, OLD.active_cleanup_ids))
    ) THEN
        RAISE EXCEPTION 'pending active vector cleanup must be resolved first' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER vector_state_guard_active_cleanup
    BEFORE UPDATE ON vector_state
    FOR EACH ROW EXECUTE FUNCTION guard_active_cleanup_state();

-- Registered bindings live on a different row. Serialize changes to that
-- binding with the cleanup reservation, leaving generation state updates alone.
CREATE FUNCTION guard_active_cleanup_binding() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    active_name TEXT;
    pending BOOLEAN;
BEGIN
    IF TG_OP = 'UPDATE' AND
        ROW(NEW.name, NEW.store, NEW.target, NEW.host, NEW.profile, NEW.profile_hash)
        IS NOT DISTINCT FROM
        ROW(OLD.name, OLD.store, OLD.target, OLD.host, OLD.profile, OLD.profile_hash)
    THEN
        RETURN NEW;
    END IF;
    EXECUTE format('SELECT active_generation, active_cleanup_reservation IS NOT NULL FROM %I.vector_state WHERE singleton FOR UPDATE',
        TG_TABLE_SCHEMA) INTO active_name, pending;
    IF pending AND ((TG_OP <> 'INSERT' AND OLD.name = active_name)
        OR (TG_OP <> 'DELETE' AND NEW.name = active_name))
    THEN
        RAISE EXCEPTION 'pending active vector cleanup must be resolved first' USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER vector_generations_guard_active_cleanup
    BEFORE INSERT OR UPDATE OF name, store, target, host, profile, profile_hash OR DELETE ON vector_generations
    FOR EACH ROW EXECUTE FUNCTION guard_active_cleanup_binding();

DO $$
DECLARE
    permission RECORD;
BEGIN
    FOR permission IN
        SELECT format('%I.%I()', n.nspname, p.proname) AS identity, acl.grantee
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE n.nspname = current_schema()
          AND p.proname IN ('guard_active_cleanup_state', 'guard_active_cleanup_binding')
          AND p.pronargs = 0 AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON FUNCTION %s FROM %s CASCADE',
            permission.identity,
            CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$$;
