package textutil

import (
	"bytes"
	"context"
	"unicode"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

// NormalizeForIngestWithLineBoundaries returns the normalized text and the
// offsets of retained separators in that text.
func NormalizeForIngestWithLineBoundaries(ctx context.Context, raw string) (string, []domain.SourceLineBoundary, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	s := normalizeIngestPrefix(raw)
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	paragraphs := paragraphBreakRe.FindAllStringIndex(s, -1)
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}

	out := make([]byte, 0, len(s))
	marks := make([]domain.SourceLineBoundary, 0, len(paragraphs))
	from := 0
	for _, interval := range paragraphs {
		var err error
		out, marks, err = appendNormalizedLine(ctx, out, marks, s[from:interval[0]])
		if err != nil {
			return "", nil, err
		}
		marks = append(marks, domain.SourceLineBoundary{Offset: len(out), Kind: domain.SourceParagraphBreak})
		out = append(out, '\n', '\n')
		from = interval[1]
	}
	out, marks, err := appendNormalizedLine(ctx, out, marks, s[from:])
	if err != nil {
		return "", nil, err
	}
	// Trimming can remove whole lines when their only contents were whitespace.
	left := len(out) - len(bytes.TrimLeftFunc(out, unicode.IsSpace))
	right := left + len(bytes.TrimRightFunc(out[left:], unicode.IsSpace))
	marks = rebaseBoundaries(marks, 0, left, right, 0)
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	return string(out[left:right]), marks, nil
}

// Within a paragraph, a run of spaces, tabs and isolated LF emits one space.
func appendNormalizedLine(ctx context.Context, out []byte, marks []domain.SourceLineBoundary, line string) ([]byte, []domain.SourceLineBoundary, error) {
	start, firstMark := len(out), len(marks)
	var inSpace, sawBreak bool
	for i := range len(line) {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		b := line[i]
		if b == ' ' || b == '\t' || b == '\n' {
			if !inSpace {
				out = append(out, ' ')
				inSpace = true
			}
			if b == '\n' && !sawBreak {
				marks = append(marks, domain.SourceLineBoundary{Offset: len(out) - 1, Kind: domain.SourceLineBreak})
				sawBreak = true
			}
			continue
		}
		inSpace, sawBreak = false, false
		out = append(out, b)
	}
	left := len(out) - len(bytes.TrimLeftFunc(out[start:], unicode.IsSpace))
	right := left + len(bytes.TrimRightFunc(out[left:], unicode.IsSpace))
	marks = rebaseBoundaries(marks, firstMark, left, right, start)
	return append(out[:start], out[left:right]...), marks, nil
}

func rebaseBoundaries(marks []domain.SourceLineBoundary, first, left, right, base int) []domain.SourceLineBoundary {
	kept := marks[:first]
	for _, mark := range marks[first:] {
		if mark.Offset >= left && mark.Offset < right {
			mark.Offset += base - left
			kept = append(kept, mark)
		}
	}
	return kept
}
