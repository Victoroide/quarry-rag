package domain

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLayoutProvenanceValidationAndDeduplication(t *testing.T) {
	p, err := ExtractLayoutProvenance(map[string]any{
		"pages":     []int{5, 2, 5, 1, 2},
		"table_ids": []string{"p2-t1", "p1-t2", "p2-t1"},
		"row_ids":   []string{"p2-t1-r3", "p1-t2-r1", "p2-t1-r3"},
	})
	require.NoError(t, err)
	require.Equal(t, []int{5, 2, 1}, p.Pages)
	require.Equal(t, []string{"p2-t1", "p1-t2"}, p.TableIDs)
	require.Equal(t, []string{"p2-t1-r3", "p1-t2-r1"}, p.RowIDs)

	// Continued table on an earlier page is valid even when pages lists only the later page.
	continued, err := ExtractLayoutProvenance(map[string]any{
		"pages":     []int{4},
		"table_ids": []string{"p2-t1"},
		"row_ids":   []string{"p2-t1-r7"},
	})
	require.NoError(t, err)
	require.Equal(t, []int{4}, continued.Pages)
	require.Equal(t, []string{"p2-t1"}, continued.TableIDs)
	require.Equal(t, []string{"p2-t1-r7"}, continued.RowIDs)

	// Missing, nil, and empty slices produce nil fields.
	empty, err := ExtractLayoutProvenance(map[string]any{
		"pages":     nil,
		"table_ids": []string{},
		"row_ids":   []string(nil),
	})
	require.NoError(t, err)
	require.Nil(t, empty.Pages)
	require.Nil(t, empty.TableIDs)
	require.Nil(t, empty.RowIDs)

	// Boundaries: max length and max ID bytes pass; +1 fails.
	maxPages := make([]int, MaxPDFPages)
	for i := range maxPages {
		maxPages[i] = i + 1
	}
	gotPages, err := ValidateLayoutPages(maxPages)
	require.NoError(t, err)
	require.Len(t, gotPages, MaxPDFPages)
	_, err = ValidateLayoutPages(append(maxPages, 1))
	require.ErrorContains(t, err, "pages")

	maxIDs := make([]string, MaxLayoutProvenanceIDs)
	for i := range maxIDs {
		maxIDs[i] = fmt.Sprintf("p1-t%d", i+1)
	}
	gotIDs, err := ValidateLayoutTableIDs(maxIDs)
	require.NoError(t, err)
	require.Len(t, gotIDs, MaxLayoutProvenanceIDs)
	_, err = ValidateLayoutTableIDs(append(maxIDs, "p1-t1"))
	require.ErrorContains(t, err, "table_ids")

	exact32 := "p1-t" + strings.Repeat("1", MaxLayoutProvenanceIDBytes-4)
	require.Len(t, exact32, MaxLayoutProvenanceIDBytes)
	gotExact, err := ValidateLayoutTableIDs([]string{exact32})
	require.NoError(t, err)
	require.Equal(t, []string{exact32}, gotExact)
	_, err = ValidateLayoutTableIDs([]string{exact32 + "1"})
	require.ErrorContains(t, err, "table_ids")
}

func TestExtractLayoutProvenanceRejectsMalformedAndHidesContent(t *testing.T) {
	secret := "SENSITIVE_DOCUMENT_BODY"
	for _, tc := range []struct {
		name  string
		md    map[string]any
		field string
	}{
		{"wrong pages type", map[string]any{"pages": []string{"1", secret}}, "pages"},
		{"wrong table_ids type", map[string]any{"table_ids": []int{1}}, "table_ids"},
		{"wrong row_ids type", map[string]any{"row_ids": secret}, "row_ids"},
		{"zero page", map[string]any{"pages": []int{0}}, "pages"},
		{"page above max", map[string]any{"pages": []int{MaxPDFPages + 1}}, "pages"},
		{"negative page", map[string]any{"pages": []int{-1}}, "pages"},
		{"table leading zero", map[string]any{"table_ids": []string{"p01-t1"}}, "table_ids"},
		{"table zero id", map[string]any{"table_ids": []string{"p1-t0"}}, "table_ids"},
		{"table unicode digit", map[string]any{"table_ids": []string{"p1-t١"}}, "table_ids"},
		{"table control char", map[string]any{"table_ids": []string{"p1-t1\n" + secret}}, "table_ids"},
		{"row missing suffix", map[string]any{"row_ids": []string{"p1-t1"}}, "row_ids"},
		{"row page above max", map[string]any{"row_ids": []string{"p1001-t1-r1"}}, "row_ids"},
		{"row extra suffix", map[string]any{"row_ids": []string{"p1-t1-r1-" + secret}}, "row_ids"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ExtractLayoutProvenance(tc.md)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.field)
			require.NotContains(t, err.Error(), secret)
		})
	}
}

func TestCopyValidLayoutProvenancePreservesValidAndSnapshotFields(t *testing.T) {
	dst := map[string]any{"pages": []int{3}}
	src := map[string]any{
		"pages":     []int{1},
		"table_ids": []string{"bad"},
		"row_ids":   []string{"p1-t1-r2", "p1-t1-r2"},
	}
	CopyValidLayoutProvenance(dst, src)
	require.Equal(t, []int{3}, dst["pages"])
	require.NotContains(t, dst, "table_ids")
	require.Equal(t, []string{"p1-t1-r2"}, dst["row_ids"])
}
