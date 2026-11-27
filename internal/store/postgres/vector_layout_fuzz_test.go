package postgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func FuzzVectorLayoutMetadata(f *testing.F) {
	f.Add(int32(1), int32(2), "p1-t1", "p1-t1-r1", uint8(0))
	f.Add(int32(0), int32(-1), "p01-t1", "p1-t1-r01", uint8(1))
	f.Add(int32(domain.MaxPDFPages), int32(domain.MaxPDFPages+1), "p1000-t999", "p1000-t999-r1", uint8(2))
	f.Add(int32(3), int32(3), "p1-t1", "p1-t1-r2", uint8(3))
	f.Add(int32(1), int32(1<<30), "p1-t"+strings.Repeat("1", 32), "p1-t1\x00-r1", uint8(4))
	f.Add(int32(2), int32(4), "p1-t١", "p1-t1-r\n1", uint8(5))

	f.Fuzz(func(t *testing.T, p1, p2 int32, rawTable, rawRow string, mode uint8) {
		if len(rawTable) > 128 || len(rawRow) > 128 {
			t.Skip()
		}

		var md map[string]any
		switch mode % 6 {
		case 0:
			md = nil
		case 1:
			md = map[string]any{
				"pages":     nil,
				"table_ids": []string{},
				"row_ids":   []string(nil),
			}
		case 2:
			// Wrong metadata value types must always fail strict extraction and never panic.
			md = map[string]any{
				"pages":     []string{fmt.Sprint(p1)},
				"table_ids": []int{int(p1)},
				"row_ids":   rawRow,
			}
		case 3:
			// Continued table ID referencing an earlier page plus duplicates.
			md = map[string]any{
				"pages":     []int{int(p1), int(p2), int(p1)},
				"table_ids": []string{rawTable, rawTable},
				"row_ids":   []string{rawRow, rawRow},
			}
		case 4:
			// Max and max+1 boundary checks for pages.
			over := make([]int, domain.MaxPDFPages+1)
			for i := range over {
				over[i] = 1
			}
			_, err := domain.ValidateLayoutPages(over)
			require.Error(t, err)
			md = map[string]any{
				"pages":     []int{int(p1), int(p2)},
				"table_ids": []string{rawTable},
				"row_ids":   []string{rawRow},
			}
		case 5:
			// Oversized ID list boundary check.
			overIDs := make([]string, domain.MaxLayoutProvenanceIDs+1)
			_, err := domain.ValidateLayoutTableIDs(overIDs)
			require.Error(t, err)
			md = map[string]any{
				"pages":     []int{int(p1)},
				"table_ids": []string{rawTable},
				"row_ids":   []string{rawRow},
			}
		}

		// Strict write conversion must never panic or leak duplicates/overflow.
		sqlVal, writeErr := layoutProvenanceToSQL(md)
		if writeErr == nil {
			seenPages := make(map[int32]bool, len(sqlVal.pages))
			for _, p := range sqlVal.pages {
				require.GreaterOrEqual(t, p, int32(1))
				require.LessOrEqual(t, p, int32(domain.MaxPDFPages))
				require.False(t, seenPages[p])
				seenPages[p] = true
			}
			seenTables := make(map[string]bool, len(sqlVal.tableIDs))
			for _, id := range sqlVal.tableIDs {
				require.LessOrEqual(t, len(id), domain.MaxLayoutProvenanceIDBytes)
				require.False(t, seenTables[id])
				seenTables[id] = true
			}
			seenRows := make(map[string]bool, len(sqlVal.rowIDs))
			for _, id := range sqlVal.rowIDs {
				require.LessOrEqual(t, len(id), domain.MaxLayoutProvenanceIDBytes)
				require.False(t, seenRows[id])
				seenRows[id] = true
			}
		}

		// Read conversion on arbitrary DB slices must fail closed on invalid entries and omit empty/nil.
		twoElements := []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}}
		decoded := make(map[string]any)
		readErr := applyLayoutProvenanceFromSQL(decoded, pgtype.Array[pgtype.Int4]{Dims: twoElements, Elements: []pgtype.Int4{{Int32: p1, Valid: true}, {Int32: p2, Valid: true}}, Valid: true},
			layoutTextPair(rawTable, twoElements), layoutTextPair(rawRow, twoElements))
		if readErr == nil {
			pages, ok := decoded["pages"].([]int)
			require.True(t, ok)
			require.NotEmpty(t, pages)
			tables, ok := decoded["table_ids"].([]string)
			require.True(t, ok)
			require.Len(t, tables, 1)
			rows, ok := decoded["row_ids"].([]string)
			require.True(t, ok)
			require.Len(t, rows, 1)
		}

		emptyDecoded := make(map[string]any)
		require.NoError(t, applyLayoutProvenanceFromSQL(emptyDecoded, pgtype.Array[pgtype.Int4]{}, pgtype.Array[pgtype.Text]{Valid: true}, pgtype.Array[pgtype.Text]{}))
		require.Empty(t, emptyDecoded)

		// Best-effort copy omits malformed fields while preserving valid ones.
		copied := make(map[string]any)
		domain.CopyValidLayoutProvenance(copied, md)
		_, checkErr := domain.ExtractLayoutProvenance(copied)
		require.NoError(t, checkErr)
	})
}

func layoutTextPair(value string, dims []pgtype.ArrayDimension) pgtype.Array[pgtype.Text] {
	element := pgtype.Text{String: value, Valid: true}
	return pgtype.Array[pgtype.Text]{Dims: dims, Elements: []pgtype.Text{element, element}, Valid: true}
}

func TestDecodeLayoutArrayShapes(t *testing.T) {
	codecs := pgtype.NewMap()
	for _, field := range []struct {
		name, element string
		oid           uint32
	}{
		{domain.LayoutPagesKey, "1", pgtype.Int4ArrayOID},
		{domain.LayoutTableIDsKey, "p1-t1", pgtype.TextArrayOID},
		{domain.LayoutRowIDsKey, "p1-t1-r1", pgtype.TextArrayOID},
	} {
		for _, tc := range []struct {
			name  string
			value []byte
			valid bool
		}{
			{"null", nil, true},
			{"empty", []byte("{}"), true},
			{"one dimension", fmt.Appendf(nil, "{%s}", field.element), true},
			{"matrix", fmt.Appendf(nil, "{{%s,%s},{%s,%s}}", field.element, field.element, field.element, field.element), false},
			{"lower bound", fmt.Appendf(nil, "[0:0]={%s}", field.element), false},
			{"null element", fmt.Appendf(nil, "{%s,NULL}", field.element), false},
		} {
			t.Run(field.name+"/"+tc.name, func(t *testing.T) {
				var pages pgtype.Array[pgtype.Int4]
				var tables, rows pgtype.Array[pgtype.Text]
				var array any
				switch field.name {
				case domain.LayoutPagesKey:
					array = &pages
				case domain.LayoutTableIDsKey:
					array = &tables
				default:
					array = &rows
				}
				require.NoError(t, codecs.Scan(field.oid, pgtype.TextFormatCode, tc.value, array))
				binary, err := codecs.Encode(field.oid, pgtype.BinaryFormatCode, array, nil)
				require.NoError(t, err)
				require.NoError(t, codecs.Scan(field.oid, pgtype.BinaryFormatCode, binary, array))
				metadata := map[string]any{}
				err = applyLayoutProvenanceFromSQL(metadata, pages, tables, rows)
				if tc.valid {
					require.NoError(t, err)
					if tc.name == "one dimension" {
						require.Contains(t, metadata, field.name)
					} else {
						require.Empty(t, metadata)
					}
				} else {
					require.ErrorContains(t, err, field.name)
					require.Empty(t, metadata)
				}
			})
		}
	}
}

func TestLayoutSQLPageBounds(t *testing.T) {
	for _, page := range []int{0, -1, domain.MaxPDFPages + 1, int(^uint(0) >> 1)} {
		_, err := layoutProvenanceToSQL(map[string]any{domain.LayoutPagesKey: []int{page}})
		require.ErrorContains(t, err, domain.LayoutPagesKey)
	}
	provenance, err := layoutProvenanceToSQL(map[string]any{domain.LayoutPagesKey: []int{1, domain.MaxPDFPages}})
	require.NoError(t, err)
	require.Equal(t, []int32{1, domain.MaxPDFPages}, provenance.pages)
}
