package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type exactScopeIndex struct {
	name string
	keys []string
}

var exactScopeIndexes = [...]exactScopeIndex{
	{name: "idx_chunk_vectors_collection_doc_type", keys: []string{"collection", "doc_type", "document_id"}},
	{name: "idx_chunk_vectors_doc_type_document_id", keys: []string{"doc_type", "document_id"}},
}

type vectorTable struct {
	oid    uint32
	schema string
}

func (t vectorTable) identifier() string {
	return pgx.Identifier{t.schema, "chunk_vectors"}.Sanitize()
}

func (idx exactScopeIndex) createSQL(table vectorTable) string {
	// PostgreSQL puts an index in its table's schema.
	return fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s USING btree (%s)",
		pgx.Identifier{idx.name}.Sanitize(), table.identifier(), strings.Join(idx.keys, ", "))
}

// ensureExactScopeIndexes validates the probe indexes and builds missing indexes
// concurrently under a dedicated session lock.
func (c *Client) ensureExactScopeIndexes(ctx context.Context) error {
	conn, err := c.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, unlockErr := conn.Exec(cleanupCtx, `SELECT pg_advisory_unlock($1)`, vectorIndexLockID); unlockErr != nil {
			_ = conn.Conn().Close(cleanupCtx)
		}
	}()
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, vectorIndexLockID); err != nil {
		return fmt.Errorf("acquire index initialization lock: %w", err)
	}

	var table vectorTable
	err = conn.QueryRow(ctx, `SELECT c.oid, n.nspname
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE c.oid = 'chunk_vectors'::regclass`).Scan(&table.oid, &table.schema)
	if err != nil {
		return fmt.Errorf("resolve vector table: %w", err)
	}
	for _, idx := range exactScopeIndexes {
		exists, checkErr := checkExactScopeIndex(ctx, conn, table, idx)
		if checkErr != nil {
			return checkErr
		}
		if exists {
			continue
		}
		name := pgx.Identifier{table.schema, idx.name}.Sanitize()
		if _, execErr := conn.Exec(ctx, idx.createSQL(table)); execErr != nil {
			return fmt.Errorf("could not build index %s: %w (inspect the schema for a name conflict or an interrupted build)", name, execErr)
		}
		exists, checkErr = checkExactScopeIndex(ctx, conn, table, idx)
		if checkErr != nil {
			return checkErr
		}
		if !exists {
			return fmt.Errorf("index %s missing after creation; inspect the schema before retrying", name)
		}
	}
	return nil
}

func checkExactScopeIndex(ctx context.Context, conn *pgxpool.Conn, table vectorTable, idx exactScopeIndex) (bool, error) {
	var belongsToTable, compatible, valid, ready, live, building, canInspectAllBuilds bool
	err := conn.QueryRow(ctx, `SELECT
 COALESCE(c.relkind = 'i' AND i.indrelid = $3, false),
 COALESCE(am.amname = 'btree' AND NOT i.indisunique
   AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnatts = i.indnkeyatts
   AND i.indnkeyatts = cardinality($4::text[])
   AND ARRAY(
     SELECT a.attname::text
     FROM unnest(i.indkey::smallint[], i.indclass::oid[], i.indcollation::oid[], i.indoption::smallint[])
          WITH ORDINALITY AS k(attnum, opclass, collation_oid, options, position)
     JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
     JOIN pg_catalog.pg_opclass op ON op.oid = k.opclass
       AND op.opcdefault AND op.opcintype = a.atttypid
     WHERE k.options = 0 AND k.collation_oid = a.attcollation
     ORDER BY k.position
   ) = $4::text[], false),
 COALESCE(i.indisvalid, false), COALESCE(i.indisready, false), COALESCE(i.indislive, false),
 EXISTS (SELECT FROM pg_catalog.pg_stat_progress_create_index p WHERE p.index_relid = c.oid),
 pg_catalog.pg_has_role('pg_read_all_stats', 'USAGE')
 FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
 LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
 WHERE n.nspname = $1 AND c.relname = $2`, table.schema, idx.name, table.oid, idx.keys).
		Scan(&belongsToTable, &compatible, &valid, &ready, &live, &building, &canInspectAllBuilds)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	name := pgx.Identifier{table.schema, idx.name}.Sanitize()
	if err != nil {
		return false, fmt.Errorf("inspect index %s: %w", name, err)
	}
	if !belongsToTable {
		return false, fmt.Errorf("object-name conflict for %s: expected an index on %s", name, table.identifier())
	}
	if !compatible {
		return false, fmt.Errorf("index %s has an incompatible definition; expected %#q", name, idx.createSQL(table))
	}
	if !live || building || (valid && !ready) {
		return false, fmt.Errorf("index %s is unavailable; inspect its state and any concurrent maintenance before retrying", name)
	}
	if !valid {
		// PostgreSQL can hide another login's index from the progress view.
		if !canInspectAllBuilds {
			return false, fmt.Errorf("index %s is invalid; have a role with pg_read_all_stats privileges inspect for an active build before retrying", name)
		}
		return false, fmt.Errorf("index %s is invalid; execute %#q and then %#q separately",
			name, "DROP INDEX CONCURRENTLY "+name, idx.createSQL(table))
	}
	return true, nil
}
