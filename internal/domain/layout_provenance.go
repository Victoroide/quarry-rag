package domain

import (
	"fmt"
	"slices"
)

const (
	// LayoutPagesKey is the chunk metadata key recording one-based PDF page numbers.
	LayoutPagesKey = "pages"
	// LayoutTableIDsKey is the chunk metadata key recording document-scoped table IDs.
	LayoutTableIDsKey = "table_ids"
	// LayoutRowIDsKey is the chunk metadata key recording document-scoped table row IDs.
	LayoutRowIDsKey = "row_ids"

	// MaxLayoutProvenanceIDs bounds table_ids and row_ids per vector row (Textract block limit).
	MaxLayoutProvenanceIDs = 250_000
	// MaxLayoutProvenanceIDBytes bounds the length of one canonical table or row identifier.
	MaxLayoutProvenanceIDBytes = 32
)

// LayoutProvenance holds validated, deduplicated, source-ordered layout coordinates.
type LayoutProvenance struct {
	Pages    []int
	TableIDs []string
	RowIDs   []string
}

// ValidateLayoutPages validates and deduplicates one-based PDF page numbers in source order.
// Empty or nil input returns nil.
func ValidateLayoutPages(pages []int) ([]int, error) {
	if len(pages) == 0 {
		return nil, nil
	}
	if len(pages) > MaxPDFPages {
		return nil, fmt.Errorf("%s exceeds maximum length of %d", LayoutPagesKey, MaxPDFPages)
	}
	seen := make(map[int]struct{}, len(pages))
	out := make([]int, 0, len(pages))
	for _, p := range pages {
		if p < 1 || p > MaxPDFPages {
			return nil, fmt.Errorf("%s contains out-of-range page", LayoutPagesKey)
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out, nil
}

// ValidateLayoutTableIDs validates and deduplicates canonical p<page>-t<table> tokens in source order.
// Empty or nil input returns nil.
func ValidateLayoutTableIDs(ids []string) ([]string, error) {
	return validateLayoutIDs(LayoutTableIDsKey, ids, false)
}

// ValidateLayoutRowIDs validates and deduplicates canonical p<page>-t<table>-r<row> tokens in source order.
// Empty or nil input returns nil.
func ValidateLayoutRowIDs(ids []string) ([]string, error) {
	return validateLayoutIDs(LayoutRowIDsKey, ids, true)
}

func validateLayoutIDs(field string, ids []string, requireRow bool) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > MaxLayoutProvenanceIDs {
		return nil, fmt.Errorf("%s exceeds maximum length of %d", field, MaxLayoutProvenanceIDs)
	}
	capHint := min(len(ids), 1024)
	seen := make(map[string]struct{}, capHint)
	out := make([]string, 0, capHint)
	for _, id := range ids {
		if !validLayoutToken(id, requireRow) {
			return nil, fmt.Errorf("%s contains an invalid identifier", field)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

func validLayoutToken(id string, requireRow bool) bool {
	if id == "" || len(id) > MaxLayoutProvenanceIDBytes || id[0] != 'p' {
		return false
	}
	page, pos, ok := scanPositiveDecimalBounded(id, 1, MaxPDFPages)
	if !ok || page < 1 || page > MaxPDFPages {
		return false
	}
	if pos+2 > len(id) || id[pos] != '-' || id[pos+1] != 't' {
		return false
	}
	pos, ok = scanPositiveDecimalDigits(id, pos+2)
	if !ok {
		return false
	}
	if !requireRow {
		return pos == len(id)
	}
	if pos+2 > len(id) || id[pos] != '-' || id[pos+1] != 'r' {
		return false
	}
	pos, ok = scanPositiveDecimalDigits(id, pos+2)
	return ok && pos == len(id)
}

func scanPositiveDecimalDigits(s string, start int) (int, bool) {
	if start >= len(s) || s[start] < '1' || s[start] > '9' {
		return 0, false
	}
	pos := start + 1
	for pos < len(s) && s[pos] >= '0' && s[pos] <= '9' {
		pos++
	}
	return pos, true
}

func scanPositiveDecimalBounded(s string, start, maxVal int) (val, pos int, ok bool) {
	if start >= len(s) || s[start] < '1' || s[start] > '9' {
		return 0, 0, false
	}
	pos = start
	for pos < len(s) && s[pos] >= '0' && s[pos] <= '9' {
		val = val*10 + int(s[pos]-'0')
		if val > maxVal {
			return 0, 0, false
		}
		pos++
	}
	return val, pos, true
}

// ExtractLayoutPages validates the optional pages metadata entry strictly.
func ExtractLayoutPages(md map[string]any) ([]int, error) {
	val, ok := md[LayoutPagesKey]
	if !ok || val == nil {
		return nil, nil
	}
	raw, ok := val.([]int)
	if !ok {
		return nil, fmt.Errorf("invalid layout metadata field %q: expected []int", LayoutPagesKey)
	}
	pages, err := ValidateLayoutPages(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid layout metadata field %q: %w", LayoutPagesKey, err)
	}
	return pages, nil
}

// ExtractLayoutTableIDs validates the optional table_ids metadata entry strictly.
func ExtractLayoutTableIDs(md map[string]any) ([]string, error) {
	return extractLayoutStringField(md, LayoutTableIDsKey, ValidateLayoutTableIDs)
}

// ExtractLayoutRowIDs validates the optional row_ids metadata entry strictly.
func ExtractLayoutRowIDs(md map[string]any) ([]string, error) {
	return extractLayoutStringField(md, LayoutRowIDsKey, ValidateLayoutRowIDs)
}

func extractLayoutStringField(md map[string]any, key string, validate func([]string) ([]string, error)) ([]string, error) {
	val, ok := md[key]
	if !ok || val == nil {
		return nil, nil
	}
	raw, ok := val.([]string)
	if !ok {
		return nil, fmt.Errorf("invalid layout metadata field %q: expected []string", key)
	}
	items, err := validate(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid layout metadata field %q: %w", key, err)
	}
	return items, nil
}

// ExtractLayoutProvenance validates all three layout provenance fields strictly.
// Any malformed present value returns an error naming the invalid field without document content.
func ExtractLayoutProvenance(md map[string]any) (LayoutProvenance, error) {
	if len(md) == 0 {
		return LayoutProvenance{}, nil
	}
	pages, err := ExtractLayoutPages(md)
	if err != nil {
		return LayoutProvenance{}, err
	}
	tableIDs, err := ExtractLayoutTableIDs(md)
	if err != nil {
		return LayoutProvenance{}, err
	}
	rowIDs, err := ExtractLayoutRowIDs(md)
	if err != nil {
		return LayoutProvenance{}, err
	}
	return LayoutProvenance{
		Pages:    pages,
		TableIDs: tableIDs,
		RowIDs:   rowIDs,
	}, nil
}

// CopyValidLayoutProvenance copies valid, nonempty layout fields from src into dst
// for keys absent in dst, omitting any malformed optional field while preserving valid ones.
func CopyValidLayoutProvenance(dst, src map[string]any) {
	if len(src) == 0 || dst == nil {
		return
	}
	if _, exists := dst[LayoutPagesKey]; !exists {
		if pages, err := ExtractLayoutPages(src); err == nil && len(pages) > 0 {
			dst[LayoutPagesKey] = slices.Clone(pages)
		}
	}
	if _, exists := dst[LayoutTableIDsKey]; !exists {
		if tableIDs, err := ExtractLayoutTableIDs(src); err == nil && len(tableIDs) > 0 {
			dst[LayoutTableIDsKey] = slices.Clone(tableIDs)
		}
	}
	if _, exists := dst[LayoutRowIDsKey]; !exists {
		if rowIDs, err := ExtractLayoutRowIDs(src); err == nil && len(rowIDs) > 0 {
			dst[LayoutRowIDsKey] = slices.Clone(rowIDs)
		}
	}
}
