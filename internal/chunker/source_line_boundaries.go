package chunker

import (
	"context"
	"strings"
	"unicode"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

type unitBoundaries struct {
	inside []domain.SourceLineBoundary
	gap    []domain.SourceLineBoundary
	folded domain.SourceLineBoundaryKind
}

// Index each source gap once. The same unit can occur in overlapping windows.
func indexUnitBoundaries(ctx context.Context, text string, units []textUnit, marks []domain.SourceLineBoundary) ([]unitBoundaries, error) {
	index := make([]unitBoundaries, len(units))
	for i, unit := range units {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		gapEnd := boundaryPrefix(marks, unit.byteStart)
		if i > 0 && gapEnd > 0 {
			start := units[i-1].byteEnd
			if unit.separator == text[start:unit.byteStart] {
				index[i].gap = marks[:gapEnd]
			} else {
				// Paragraph joins and long-sentence word gaps can emit
				// fewer bytes than the original separators.
				kind, _ := domain.SourceBoundaryBetween(marks[:gapEnd], start, unit.byteStart)
				if kind == domain.SourceParagraphBreak && unit.separator == "\n\n" ||
					kind == domain.SourceLineBreak && unit.separator == " " {
					index[i].folded = kind
				}
			}
		}
		marks = marks[gapEnd:]
		end := boundaryPrefix(marks, unit.byteEnd)
		index[i].inside = marks[:end]
		marks = marks[end:]
	}
	return index, ctx.Err()
}

func boundaryPrefix(marks []domain.SourceLineBoundary, end int) int {
	n := 0
	for n < len(marks) && marks[n].Offset < end {
		n++
	}
	return n
}

func boundaryFitsWindow(mark domain.SourceLineBoundary, start, end int) bool {
	width := 1
	if mark.Kind == domain.SourceParagraphBreak {
		width = 2
	}
	return start < mark.Offset && mark.Offset < end-width
}

func attachSourceLineBoundaries(chunk *domain.Chunk, marks []domain.SourceLineBoundary) {
	if marks == nil {
		return
	}
	kept := marks[:0]
	if len(marks) > 0 {
		trimmedLeft := strings.TrimLeftFunc(chunk.Text, unicode.IsSpace)
		left := len(chunk.Text) - len(trimmedLeft)
		right := left + len(strings.TrimRightFunc(trimmedLeft, unicode.IsSpace))
		// Windows may end inside whitespace. Drop each clipped mark so the
		// remaining interior marks can still describe the original separators.
		for _, mark := range marks {
			if boundaryFitsWindow(mark, left, right) {
				kept = append(kept, mark)
			}
		}
	}
	if domain.ValidateSourceLineBoundaries(chunk.Text, kept) {
		chunk.SourceLineBoundaries = kept
	}
}
