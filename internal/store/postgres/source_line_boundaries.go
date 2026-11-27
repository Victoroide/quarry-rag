package postgres

import (
	"math"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5/pgtype"
)

func encodeSourceLineBoundaries(text string, marks []domain.SourceLineBoundary) []int32 {
	if marks == nil || !domain.ValidateSourceLineBoundaries(text, marks) {
		return nil
	}
	out := make([]int32, 0, len(marks))
	for _, mark := range marks {
		at := mark.Offset
		if at <= 0 || at > math.MaxInt32 {
			return nil
		}
		offset := int32(at)
		if mark.Kind == domain.SourceParagraphBreak {
			offset = -offset
		}
		out = append(out, offset)
	}
	return out
}

func decodeSourceLineBoundaries(text string, raw pgtype.Array[pgtype.Int4]) []domain.SourceLineBoundary {
	n := len(raw.Elements)
	if !raw.Valid || n > len(text) {
		return nil
	}
	if n == 0 && len(raw.Dims) == 0 {
		return []domain.SourceLineBoundary{}
	}
	if len(raw.Dims) != 1 || int64(raw.Dims[0].Length) != int64(n) ||
		(n > 0 && raw.Dims[0].LowerBound != 1) {
		return nil
	}
	marks := make([]domain.SourceLineBoundary, 0, n)
	for _, value := range raw.Elements {
		offset := value.Int32
		if !value.Valid || offset == 0 || offset == math.MinInt32 {
			return nil
		}
		kind := domain.SourceLineBreak
		if offset < 0 {
			offset, kind = -offset, domain.SourceParagraphBreak
		}
		marks = append(marks, domain.SourceLineBoundary{Offset: int(offset), Kind: kind})
	}
	if !domain.ValidateSourceLineBoundaries(text, marks) {
		return nil
	}
	return marks
}
