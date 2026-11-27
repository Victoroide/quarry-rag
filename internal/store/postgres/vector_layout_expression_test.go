package postgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const canonicalPagesDeparse = "pages IS NULL OR cardinality(pages) >= 1 AND cardinality(pages) <= 1000 AND array_ndims(pages) = 1 AND array_lower(pages, 1) = 1"

func FuzzCanonicalVectorLayoutExpression(f *testing.F) {
	// Generated expressions combine harmless formatting changes. Insertions
	// after the expression must never be ignored.
	f.Add(uint32(0), "")
	f.Add(^uint32(0), "")
	f.Add(uint32(1), " OR true")
	f.Add(uint32(2), " /* trailing comment */")
	f.Add(uint32(3), " AND pages IS NOT NULL")
	f.Fuzz(func(t *testing.T, formatting uint32, suffix string) {
		valid := canonicalPagesDeparse
		if formatting&1 != 0 {
			valid = strings.ReplaceAll(valid, "pages", `"pages"`)
		}
		if formatting&2 != 0 {
			valid = strings.NewReplacer(
				"cardinality(", `pg_catalog."cardinality"(`,
				"array_ndims(", `"pg_catalog".array_ndims(`,
				"array_lower(", `"pg_catalog"."array_lower"(`,
			).Replace(valid)
		}
		if formatting&4 != 0 {
			valid = strings.NewReplacer(
				">=", "OPERATOR(pg_catalog.>=)",
				"<=", `OPERATOR("pg_catalog".<=)`,
				" = ", " OPERATOR(pg_catalog.=) ",
			).Replace(valid)
		}
		if formatting&8 != 0 {
			valid = " \n " + strings.ReplaceAll(valid, " ", "\t\n") + "\r "
		}
		check := vectorLayoutFields[0]
		require.True(t, check.matches(valid), valid)
		if strings.Trim(suffix, " \t\r\n\f\v") != "" {
			require.False(t, check.matches(valid+suffix))
		}
		for _, malformed := range []string{
			strings.Replace(valid, "array_lower", "array_upper", 1),
			"pages IS NULL OR cardinality(pages) >= 1 AND cardinality(pages) <= 1000 AND array_ndims(pages) = 1",
			`"pages IS NULL OR cardinality(pages)" >= 1 AND cardinality(pages) <= 1000 AND array_ndims(pages) = 1 AND array_lower(pages, 1) = 1`,
			strings.Replace(canonicalPagesDeparse, "pages IS NULL OR cardinality(pages) >= 1", "(pages IS NULL OR cardinality(pages) >= 1)", 1),
			strings.Replace(canonicalPagesDeparse, "IS NULL", "ISNULL", 1),
			strings.Replace(canonicalPagesDeparse, "1000", "1 000", 1),
			strings.Repeat(" ", 2048) + canonicalPagesDeparse,
		} {
			require.False(t, check.matches(malformed), malformed)
		}
	})
}
