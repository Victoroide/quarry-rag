package postgres

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type pgLayoutProvenance struct {
	pages    []int32
	tableIDs []string
	rowIDs   []string
}

func layoutProvenanceToSQL(md map[string]any) (pgLayoutProvenance, error) {
	lp, err := domain.ExtractLayoutProvenance(md)
	if err != nil {
		return pgLayoutProvenance{}, err
	}
	var out pgLayoutProvenance
	if len(lp.Pages) > 0 {
		pages := make([]int32, len(lp.Pages))
		for i, p := range lp.Pages {
			if p < 1 || p > domain.MaxPDFPages {
				return pgLayoutProvenance{}, fmt.Errorf("postgres: invalid layout field %q: page out of range", domain.LayoutPagesKey)
			}
			pages[i] = int32(p)
		}
		out.pages = pages
	}
	if len(lp.TableIDs) > 0 {
		out.tableIDs = lp.TableIDs
	}
	if len(lp.RowIDs) > 0 {
		out.rowIDs = lp.RowIDs
	}
	return out, nil
}

func validateLayoutArray[T any](raw pgtype.Array[T], field string, maxLen int) error {
	if len(raw.Elements) == 0 {
		return nil
	}
	if len(raw.Dims) != 1 || raw.Dims[0].LowerBound != 1 {
		return fmt.Errorf("postgres: invalid layout field %q: expected a one-dimensional array with lower bound 1", field)
	}
	if len(raw.Elements) > maxLen {
		return fmt.Errorf("postgres: invalid layout field %q: exceeds maximum length of %d", field, maxLen)
	}
	return nil
}

func applyLayoutProvenanceFromSQL(md map[string]any, rawPages pgtype.Array[pgtype.Int4], rawTableIDs, rawRowIDs pgtype.Array[pgtype.Text]) error {
	if err := validateLayoutArray(rawPages, domain.LayoutPagesKey, domain.MaxPDFPages); err != nil {
		return err
	}
	pages := make([]int, len(rawPages.Elements))
	for i, p := range rawPages.Elements {
		if !p.Valid {
			return fmt.Errorf("postgres: invalid layout field %q: NULL array element", domain.LayoutPagesKey)
		}
		pages[i] = int(p.Int32)
	}
	validPages, err := domain.ValidateLayoutPages(pages)
	if err != nil {
		return fmt.Errorf("postgres: invalid layout field %q: %w", domain.LayoutPagesKey, err)
	}
	if len(validPages) > 0 {
		md[domain.LayoutPagesKey] = validPages
	}

	for _, field := range []struct {
		key      string
		raw      pgtype.Array[pgtype.Text]
		validate func([]string) ([]string, error)
	}{
		{domain.LayoutTableIDsKey, rawTableIDs, domain.ValidateLayoutTableIDs},
		{domain.LayoutRowIDsKey, rawRowIDs, domain.ValidateLayoutRowIDs},
	} {
		if err := validateLayoutArray(field.raw, field.key, domain.MaxLayoutProvenanceIDs); err != nil {
			return err
		}
		ids := make([]string, len(field.raw.Elements))
		for i, id := range field.raw.Elements {
			if !id.Valid {
				return fmt.Errorf("postgres: invalid layout field %q: NULL array element", field.key)
			}
			ids[i] = id.String
		}
		validIDs, err := field.validate(ids)
		if err != nil {
			return fmt.Errorf("postgres: invalid layout field %q: %w", field.key, err)
		}
		if len(validIDs) > 0 {
			md[field.key] = validIDs
		}
	}
	return nil
}

type layoutSchemaQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const maxLayoutCheckBytes = 1024

type vectorLayoutCheck struct {
	column  string
	pattern *regexp.Regexp
}

var vectorLayoutFields = [...]vectorLayoutCheck{
	newVectorLayoutCheck(domain.LayoutPagesKey, domain.MaxPDFPages),
	newVectorLayoutCheck(domain.LayoutTableIDsKey, domain.MaxLayoutProvenanceIDs),
	newVectorLayoutCheck(domain.LayoutRowIDsKey, domain.MaxLayoutProvenanceIDs),
}

// newVectorLayoutCheck matches PostgreSQL's pretty deparse of the canonical
// CHECK. It permits only formatting differences within that token sequence.
func newVectorLayoutCheck(column string, maxLength int) vectorLayoutCheck {
	const space = `[ \t\r\n\f\v]`
	ws := space + "*"
	word := space + "+"
	identifier := func(name string) string {
		return `(?:` + name + `|"` + name + `")`
	}
	catalog := identifier("pg_catalog") + ws + `\.` + ws
	fn := func(name string) string {
		return `(?:` + catalog + `)?` + identifier(name) + ws
	}
	operator := func(symbol string) string {
		return `(?:` + symbol + `|OPERATOR` + ws + `\(` + ws + catalog + symbol + ws + `\))`
	}
	col := identifier(column)
	unaryArgs := `\(` + ws + col + ws + `\)`
	lowerArgs := `\(` + ws + col + ws + `,` + ws + `1` + ws + `\)`
	compare := func(symbol string) string { return ws + operator(symbol) + ws }
	and := word + "AND" + word
	pattern := `\A` + ws + col + word + "IS" + word + "NULL" + word + "OR" + word +
		fn("cardinality") + unaryArgs + compare(">=") + "1" + and +
		fn("cardinality") + unaryArgs + compare("<=") + strconv.Itoa(maxLength) + and +
		fn("array_ndims") + unaryArgs + compare("=") + "1" + and +
		fn("array_lower") + lowerArgs + compare("=") + "1" + ws + `\z`
	return vectorLayoutCheck{column: column, pattern: regexp.MustCompile(pattern)}
}

func (c vectorLayoutCheck) matches(expression string) bool {
	return len(expression) <= maxLayoutCheckBytes && c.pattern.MatchString(expression)
}

type layoutCatalogColumn struct {
	validType, ordinaryNullable bool
	hasCheck                    bool
	expression                  *string
}

func validateVectorLayoutSchema(ctx context.Context, db layoutSchemaQuerier, table string) error {
	// Pinned built-ins have no pg_depend entry. A same-named user function
	// or operator can deparse identically under a different search path.
	const query = `SELECT a.attname,
		CASE a.attname
			WHEN 'pages' THEN a.atttypid = 'pg_catalog.int4[]'::pg_catalog.regtype
			ELSE a.atttypid = 'pg_catalog.text[]'::pg_catalog.regtype
		END AS valid_type,
		NOT a.attnotnull AND NOT a.atthasdef AND a.attgenerated = '' AND a.attidentity = '' AS ordinary_nullable,
		c.oid IS NOT NULL AS has_check,
		CASE WHEN c.contype = 'c' AND c.conkey = ARRAY[a.attnum]
			AND c.convalidated AND c.conparentid = 0 AND c.conislocal AND c.coninhcount = 0 AND NOT c.connoinherit
			AND COALESCE((pg_catalog.to_jsonb(c) ->> 'conenforced')::pg_catalog.bool, TRUE)
			AND r.relkind = 'r'
			AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid = r.oid OR i.inhparent = r.oid)
			AND NOT EXISTS (
				SELECT 1 FROM pg_catalog.pg_depend d
				WHERE d.classid = 'pg_catalog.pg_constraint'::pg_catalog.regclass AND d.objid = c.oid
				  AND d.refclassid IN ('pg_catalog.pg_proc'::pg_catalog.regclass, 'pg_catalog.pg_operator'::pg_catalog.regclass)
			)
		THEN pg_catalog.left(pg_catalog.pg_get_expr(c.conbin, c.conrelid, true), $2::pg_catalog.int4)
		END AS check_expression
	FROM pg_catalog.pg_class r
	JOIN pg_catalog.pg_attribute a ON a.attrelid = r.oid
	LEFT JOIN pg_catalog.pg_constraint c ON c.conrelid = r.oid
		AND c.conname = 'quarry_layout_' || a.attname || '_check'
	WHERE r.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', pg_catalog.current_schema(), $1::pg_catalog.text))
		AND a.attname IN ('pages', 'table_ids', 'row_ids')
		AND NOT a.attisdropped`
	rows, err := db.Query(ctx, query, table, maxLayoutCheckBytes+1)
	if err != nil {
		return fmt.Errorf("postgres: inspect layout schema on %q: %w", table, err)
	}
	defer rows.Close()

	columns := make(map[string][]layoutCatalogColumn, len(vectorLayoutFields))
	for rows.Next() {
		var name string
		var col layoutCatalogColumn
		if scanErr := rows.Scan(&name, &col.validType, &col.ordinaryNullable, &col.hasCheck, &col.expression); scanErr != nil {
			return fmt.Errorf("postgres: inspect layout schema on %q: %w", table, scanErr)
		}
		columns[name] = append(columns[name], col)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("postgres: inspect layout schema on %q: %w", table, rowsErr)
	}
	const recovery = "apply database migrations or follow migration 039 recovery in the deployment guide"
	for _, field := range vectorLayoutFields {
		matches := columns[field.column]
		if len(matches) == 0 {
			return fmt.Errorf("postgres: vector table %q is missing layout column %q; %s", table, field.column, recovery)
		}
		col := matches[0]
		if !col.validType || !col.ordinaryNullable {
			return fmt.Errorf("postgres: vector table %q has incompatible layout column %q; %s", table, field.column, recovery)
		}
		check := "quarry_layout_" + field.column + "_check"
		if !col.hasCheck {
			return fmt.Errorf("postgres: vector table %q is missing layout check %q; %s", table, check, recovery)
		}
		if len(matches) != 1 || col.expression == nil || !field.matches(*col.expression) {
			return fmt.Errorf("postgres: vector table %q has incompatible layout check %q; %s", table, check, recovery)
		}
	}
	return nil
}
