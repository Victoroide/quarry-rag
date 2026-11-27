ALTER TABLE vector_state ADD COLUMN writer_operations_enabled BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE vector_writer_operations (
    operation_id UUID PRIMARY KEY CHECK (operation_id <> '00000000-0000-0000-0000-000000000000'),
    sender_id UUID NOT NULL CHECK (sender_id <> '00000000-0000-0000-0000-000000000000'),
    writer_role NAME NOT NULL,
    generation TEXT NOT NULL CHECK (generation ~ '^[a-z][a-z0-9_]{0,23}$'),
    kind TEXT NOT NULL CHECK (kind IN ('delete', 'held_release', 'legacy_hosted_release', 'reconcile')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    target_reference TEXT NOT NULL CHECK (
        octet_length(target_reference) BETWEEN 1 AND 512 AND target_reference !~ '[^a-zA-Z0-9._:-]'
    ),
    pending_provider BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE FUNCTION vector_writer_operations_enabled() RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT writer_operations_enabled FROM vector_state WHERE singleton;
END;

CREATE FUNCTION vector_writer_operation_mark(p_operation UUID, p_sender UUID) RETURNS BOOLEAN
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    WITH marked AS (
        UPDATE vector_writer_operations SET pending_provider=TRUE
        WHERE operation_id=p_operation AND sender_id=p_sender AND writer_role=session_user
          AND generation=(SELECT active_generation FROM vector_state WHERE singleton)
        RETURNING operation_id
    )
    SELECT EXISTS(SELECT 1 FROM marked);
END;

CREATE FUNCTION vector_writer_operation_finish(p_operation UUID, p_sender UUID) RETURNS BOOLEAN
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    WITH finished AS (
        DELETE FROM vector_writer_operations
        WHERE operation_id=p_operation AND sender_id=p_sender AND writer_role=session_user
          AND generation=(SELECT active_generation FROM vector_state WHERE singleton)
        RETURNING operation_id
    )
    SELECT EXISTS(SELECT 1 FROM finished);
END;

CREATE FUNCTION vector_writer_operation_drain_status()
RETURNS TABLE (pending_count BIGINT, operation_ids UUID[])
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
BEGIN ATOMIC
    SELECT (SELECT count(*) FROM vector_writer_operations),
           ARRAY(SELECT operation_id FROM vector_writer_operations ORDER BY started_at, operation_id LIMIT 20);
END;

DO $migration$
DECLARE
    metadata_schema NAME := current_schema();
    permission RECORD;
BEGIN
    EXECUTE format($ddl$
        CREATE FUNCTION %1$I.vector_writer_operation_begin(
            p_operation UUID, p_sender UUID, p_generation TEXT, p_kind TEXT, p_target TEXT
        ) RETURNS BOOLEAN
        LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $function$
        DECLARE
            state %1$I.vector_state%%ROWTYPE;
        BEGIN
            SELECT * INTO STRICT state FROM %1$I.vector_state WHERE singleton FOR SHARE;
            IF NOT state.writer_operations_enabled OR state.frozen OR p_generation IS DISTINCT FROM state.active_generation THEN
                RETURN FALSE;
            END IF;
            INSERT INTO %1$I.vector_writer_operations(operation_id, sender_id, writer_role, generation, kind, target_reference)
            VALUES (p_operation, p_sender, session_user, p_generation, p_kind, p_target);
            RETURN TRUE;
        END;
        $function$;
    $ddl$, metadata_schema);

    EXECUTE format($ddl$
        CREATE FUNCTION %1$I.require_drained_vector_writer_operations() RETURNS TRIGGER
        LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $function$
        BEGIN
            IF OLD.writer_operations_enabled IS DISTINCT FROM NEW.writer_operations_enabled
                AND NOT pg_has_role(session_user, current_user, 'MEMBER') THEN
                RAISE EXCEPTION 'changing writer operation mode requires the metadata schema owner'
                    USING ERRCODE = '42501';
            END IF;
            IF OLD.writer_operations_enabled AND (NOT NEW.writer_operations_enabled OR (NEW.frozen AND NOT OLD.frozen)) THEN
                IF current_setting('transaction_isolation') <> 'read committed'
                    OR EXISTS(SELECT 1 FROM %1$I.vector_writer_operations) THEN
                    RAISE EXCEPTION 'vector writer operations must drain under read committed isolation'
                        USING ERRCODE = '23514', CONSTRAINT = 'vector_writer_operations_drained';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $function$;
    $ddl$, metadata_schema);

    FOR permission IN
        SELECT format('%I.%I(%s)', metadata_schema, p.proname, pg_get_function_identity_arguments(p.oid)) AS identity, acl.grantee
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
        WHERE n.nspname = metadata_schema AND p.proname IN (
            'vector_writer_operations_enabled', 'vector_writer_operation_begin', 'vector_writer_operation_mark',
            'vector_writer_operation_finish', 'vector_writer_operation_drain_status', 'require_drained_vector_writer_operations'
        ) AND acl.grantee <> p.proowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON FUNCTION %s FROM %s CASCADE',
            permission.identity, CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;

    FOR permission IN
        SELECT acl.grantee FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) acl
        WHERE n.nspname = metadata_schema AND c.relname = 'vector_writer_operations' AND acl.grantee <> c.relowner
    LOOP
        EXECUTE format('REVOKE ALL PRIVILEGES ON TABLE %I.vector_writer_operations FROM %s CASCADE',
            metadata_schema, CASE WHEN permission.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(permission.grantee)) END);
    END LOOP;
END;
$migration$;

CREATE TRIGGER require_drained_vector_writer_operations
BEFORE UPDATE ON vector_state FOR EACH ROW EXECUTE FUNCTION require_drained_vector_writer_operations();
