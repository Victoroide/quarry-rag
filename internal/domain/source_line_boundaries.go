package domain

import (
	"sort"
	"strings"
	"unicode"
)

// SourceLineBoundaryKind describes the separator emitted during ingestion.
type SourceLineBoundaryKind uint8

const (
	// SourceLineBreak marks the space emitted for a retained source line break.
	SourceLineBreak SourceLineBoundaryKind = iota + 1
	// SourceParagraphBreak marks the first LF of a retained paragraph separator.
	SourceParagraphBreak
)

// SourceLineBoundary points to the first byte of a separator in chunk text.
type SourceLineBoundary struct {
	Offset int
	Kind   SourceLineBoundaryKind
}

// ValidateSourceLineBoundaries checks a complete sidecar against its text.
func ValidateSourceLineBoundaries(text string, boundaries []SourceLineBoundary) bool {
	if len(boundaries) > len(text) {
		return false
	}
	if len(boundaries) == 0 {
		return true
	}
	trimmedLeft := strings.TrimLeftFunc(text, unicode.IsSpace)
	left := len(text) - len(trimmedLeft)
	right := left + len(strings.TrimRightFunc(trimmedLeft, unicode.IsSpace))
	previousEnd := left + 1
	var paragraphStart, paragraphEnd int
	for _, mark := range boundaries {
		at := mark.Offset
		if at < previousEnd || at >= right-1 {
			return false
		}
		switch mark.Kind {
		case SourceLineBreak:
			if text[at] != ' ' || text[at-1] == ' ' || text[at+1] == ' ' {
				return false
			}
			previousEnd = at + 1
		case SourceParagraphBreak:
			if at >= right-2 || text[at:at+2] != "\n\n" {
				return false
			}
			// Blank lines containing other Unicode whitespace can leave
			// consecutive paragraph pairs after their contents are trimmed.
			if at >= paragraphEnd {
				paragraphStart, paragraphEnd = at, at+2
				for paragraphStart > 0 && text[paragraphStart-1] == '\n' {
					paragraphStart--
				}
				for paragraphEnd < len(text) && text[paragraphEnd] == '\n' {
					paragraphEnd++
				}
			}
			if (at-paragraphStart)%2 != 0 {
				return false
			}
			previousEnd = at + 2
		default:
			return false
		}
	}
	return true
}

// SourceBoundaryBetween finds the strongest mark in a half-open byte range.
// Boundaries must be sorted by offset.
func SourceBoundaryBetween(boundaries []SourceLineBoundary, left, right int) (SourceLineBoundaryKind, bool) {
	if left >= right {
		return 0, false
	}
	start := sort.Search(len(boundaries), func(i int) bool { return boundaries[i].Offset >= left })
	var kind SourceLineBoundaryKind
	for _, mark := range boundaries[start:] {
		if mark.Offset >= right {
			break
		}
		if mark.Kind == SourceParagraphBreak {
			return mark.Kind, true
		}
		kind = mark.Kind
	}
	return kind, kind != 0
}
