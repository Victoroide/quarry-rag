-- Add nullable typed layout provenance arrays to existing schema-local pgvector
-- tables without installing extensions or modifying tables in other schemas.
DO $$
DECLARE
    target_schema NAME := current_schema();
    target_namespace OID;
    docs_oid OID;
    docs_id_attnum SMALLINT;
    vector_type_oid OID;
    tbl RECORD;
    is_valid BOOLEAN;
    col_spec RECORD;
    existing_column RECORD;
    constraint_name TEXT;
BEGIN
    SELECT oid INTO STRICT target_namespace
    FROM pg_catalog.pg_namespace
    WHERE nspname = target_schema;

    SELECT c.oid, a.attnum
    INTO STRICT docs_oid, docs_id_attnum
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_attribute a
      ON a.attrelid = c.oid AND a.attname = 'id' AND NOT a.attisdropped
    WHERE c.relnamespace = target_namespace
      AND c.relname = 'documents'
      AND c.relkind = 'r';

    SELECT t.oid
    INTO vector_type_oid
    FROM pg_catalog.pg_extension e
    JOIN pg_catalog.pg_type t
      ON t.typnamespace = e.extnamespace AND t.typname = 'vector'
    WHERE e.extname = 'vector';

    FOR tbl IN
        SELECT DISTINCT reg.target, c.oid AS rel_oid, c.relkind
        FROM (
            SELECT target FROM vector_generations WHERE store = 'postgres'
            UNION
            SELECT legacy_target AS target
            FROM vector_state
            WHERE legacy_store = 'postgres' AND legacy_target IS NOT NULL
        ) reg
        JOIN pg_catalog.pg_class c
          ON c.relnamespace = target_namespace AND c.relname = reg.target
    LOOP
        IF tbl.relkind = 'p' OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_inherits i
            WHERE i.inhrelid = tbl.rel_oid OR i.inhparent = tbl.rel_oid
        ) THEN
            RAISE EXCEPTION 'registered pgvector target %.% uses inheritance or partitioning; move its vectors to a standalone table before applying migration 039', target_schema, tbl.target;
        END IF;
        is_valid := tbl.relkind = 'r'
            AND tbl.target ~ '^chunk_vectors(_[a-z][a-z0-9_]{0,23})?$'
            AND vector_type_oid IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = tbl.rel_oid
                  AND a.attname = 'embedding'
                  AND NOT a.attisdropped
                  AND a.atttypid = vector_type_oid
            )
            AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_constraint con
                  ON con.conrelid = tbl.rel_oid
                 AND con.contype = 'f'
                 AND con.confrelid = docs_oid
                 AND con.confdeltype = 'c'
                 AND con.conkey = ARRAY[a.attnum]
                 AND con.confkey = ARRAY[docs_id_attnum]
                WHERE a.attrelid = tbl.rel_oid
                  AND a.attname = 'document_id'
                  AND NOT a.attisdropped
            );
        IF NOT is_valid THEN
            RAISE EXCEPTION 'incompatible registered pgvector target %.%', target_schema, tbl.target;
        END IF;
    END LOOP;

    IF vector_type_oid IS NULL THEN
        RETURN;
    END IF;

    FOR tbl IN
        SELECT c.oid AS rel_oid, c.relname
        FROM pg_catalog.pg_class c
        WHERE c.relnamespace = target_namespace
          AND c.relkind = 'r'
          AND NOT EXISTS (
              SELECT 1 FROM pg_catalog.pg_inherits i
              WHERE i.inhrelid = c.oid OR i.inhparent = c.oid
          )
          AND c.relname ~ '^chunk_vectors(_[a-z][a-z0-9_]{0,23})?$'
          AND EXISTS (
              SELECT 1
              FROM pg_catalog.pg_attribute a
              WHERE a.attrelid = c.oid
                AND a.attname = 'embedding'
                AND NOT a.attisdropped
                AND a.atttypid = vector_type_oid
          )
          AND EXISTS (
              SELECT 1
              FROM pg_catalog.pg_attribute a
              JOIN pg_catalog.pg_constraint con
                ON con.conrelid = c.oid
               AND con.contype = 'f'
               AND con.confrelid = docs_oid
               AND con.confdeltype = 'c'
               AND con.conkey = ARRAY[a.attnum]
               AND con.confkey = ARRAY[docs_id_attnum]
              WHERE a.attrelid = c.oid
                AND a.attname = 'document_id'
                AND NOT a.attisdropped
          )
        ORDER BY c.relname
    LOOP
        -- Prevent new inheritance links while checking and changing this table.
        EXECUTE format('LOCK TABLE ONLY %I.%I IN ACCESS EXCLUSIVE MODE', target_schema, tbl.relname);
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
            WHERE c.oid = tbl.rel_oid AND c.relnamespace = target_namespace AND c.relname = tbl.relname
        ) THEN
            RAISE EXCEPTION 'pgvector target %.% changed during migration 039; retry after schema changes finish', target_schema, tbl.relname;
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_inherits i
            WHERE i.inhrelid = tbl.rel_oid OR i.inhparent = tbl.rel_oid
        ) THEN
            IF EXISTS (SELECT 1 FROM vector_generations WHERE store = 'postgres' AND target = tbl.relname)
                OR EXISTS (SELECT 1 FROM vector_state WHERE legacy_store = 'postgres' AND legacy_target = tbl.relname)
            THEN
                RAISE EXCEPTION 'registered pgvector target %.% uses inheritance or partitioning; move its vectors to a standalone table before applying migration 039', target_schema, tbl.relname;
            END IF;
            CONTINUE;
        END IF;

        FOR col_spec IN
            SELECT * FROM (VALUES
                ('pages', 'INTEGER[]', 'integer[]'::regtype, 1000),
                ('table_ids', 'TEXT[]', 'text[]'::regtype, 250000),
                ('row_ids', 'TEXT[]', 'text[]'::regtype, 250000)
            ) AS specs(col_name, sql_type, expected_oid, max_len)
        LOOP
            SELECT a.atttypid, a.attnotnull, a.attgenerated, a.attidentity, a.atthasdef
            INTO existing_column
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = tbl.rel_oid
              AND a.attname = col_spec.col_name
              AND NOT a.attisdropped;

            IF NOT FOUND THEN
                EXECUTE format(
                    'ALTER TABLE ONLY %I.%I ADD COLUMN %I %s',
                    target_schema, tbl.relname, col_spec.col_name, col_spec.sql_type
                );
            ELSE
                IF existing_column.atttypid <> col_spec.expected_oid OR existing_column.attnotnull
                    OR existing_column.attgenerated <> '' OR existing_column.attidentity <> '' THEN
                    RAISE EXCEPTION 'incompatible layout provenance column %.%.%; convert it to an ordinary nullable % with no default before applying migration 039',
                        target_schema, tbl.relname, col_spec.col_name, col_spec.sql_type;
                END IF;
                IF existing_column.atthasdef THEN
                    EXECUTE format(
                        'ALTER TABLE ONLY %I.%I ALTER COLUMN %I DROP DEFAULT',
                        target_schema, tbl.relname, col_spec.col_name
                    );
                END IF;
            END IF;

            -- Reinstall the reserved constraint so its definition is never inferred
            -- from partial or unvalidated checks. ADD validates the existing rows.
            constraint_name := 'quarry_layout_' || col_spec.col_name || '_check';
            IF EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint con
                JOIN pg_catalog.pg_attribute a
                  ON a.attrelid = con.conrelid AND a.attname = col_spec.col_name AND NOT a.attisdropped
                WHERE con.conrelid = tbl.rel_oid AND con.conname = constraint_name
                  AND (con.contype <> 'c' OR con.conkey IS DISTINCT FROM ARRAY[a.attnum])
            ) THEN
                RAISE EXCEPTION 'reserved layout constraint name %.%.% is in use; rename that constraint before applying migration 039',
                    target_schema, tbl.relname, constraint_name;
            END IF;
            EXECUTE format(
                'ALTER TABLE ONLY %I.%I DROP CONSTRAINT IF EXISTS %I, ADD CONSTRAINT %I CHECK (%I IS NULL OR (cardinality(%I) BETWEEN 1 AND %s AND array_ndims(%I) = 1 AND array_lower(%I, 1) = 1))',
                target_schema, tbl.relname, constraint_name, constraint_name,
                col_spec.col_name, col_spec.col_name, col_spec.max_len,
                col_spec.col_name, col_spec.col_name
            );
        END LOOP;
    END LOOP;
END;
$$;
