package postgres

import (
	"context"
	"fmt"

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

type layoutColumnQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func validateVectorLayoutColumns(ctx context.Context, db layoutColumnQuerier, table string) error {
	rows, err := db.Query(ctx, `SELECT attname,
		CASE attname
			WHEN 'pages' THEN atttypid = 'integer[]'::regtype
			ELSE atttypid = 'text[]'::regtype
		END AS valid_type,
		NOT attnotnull AND NOT atthasdef AND attgenerated = '' AND attidentity = '' AS ordinary_nullable
	FROM pg_catalog.pg_attribute
	WHERE attrelid = to_regclass(format('%I.%I', current_schema(), $1::text))
		AND attname IN ('pages', 'table_ids', 'row_ids')
		AND NOT attisdropped`, table)
	if err != nil {
		return fmt.Errorf("postgres: inspect layout columns on %q: %w", table, err)
	}
	defer rows.Close()

	seen := make(map[string]bool, 3)
	for rows.Next() {
		var name string
		var validType, ordinaryNullable bool
		if scanErr := rows.Scan(&name, &validType, &ordinaryNullable); scanErr != nil {
			return fmt.Errorf("postgres: inspect layout columns on %q: %w", table, scanErr)
		}
		if !validType || !ordinaryNullable {
			return fmt.Errorf("postgres: vector table %q has incompatible layout column %q; apply database migrations or follow migration 039 recovery in the deployment guide", table, name)
		}
		seen[name] = true
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("postgres: inspect layout columns on %q: %w", table, rowsErr)
	}
	for _, col := range []string{domain.LayoutPagesKey, domain.LayoutTableIDsKey, domain.LayoutRowIDsKey} {
		if !seen[col] {
			return fmt.Errorf("postgres: vector table %q is missing layout column %q; apply database migrations or follow migration 039 recovery in the deployment guide", table, col)
		}
	}
	return nil
}
