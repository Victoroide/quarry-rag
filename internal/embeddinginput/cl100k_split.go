package embeddinginput

import (
	"context"
	"sort"
	"unicode/utf8"
)

// cl100kSplitVersion binds the cl100k_base pattern and its Unicode classes
// as used by the reference tiktoken 0.12 and 0.14 wheels.
const cl100kSplitVersion = "cl100k-base-pretokenizer-unicode16-v1"

type cl100kClass uint8

const (
	clOther cl100kClass = iota
	clLetter
	clNumber
	clSpace
)

func cl100kRuneClass(r rune) cl100kClass {
	if r < utf8.RuneSelf {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z':
			return clLetter
		case r >= '0' && r <= '9':
			return clNumber
		case r == ' ', r >= '\t' && r <= '\r':
			return clSpace
		default:
			return clOther
		}
	}
	i := sort.Search(len(cl100kUnicode16), func(i int) bool { return cl100kUnicode16[i].last >= r })
	if i < len(cl100kUnicode16) && cl100kUnicode16[i].first <= r {
		return cl100kUnicode16[i].class
	}
	return clOther
}

func cl100kContraction(text string) int {
	if len(text) < 2 || text[0] != '\'' {
		return 0
	}
	r, size := utf8.DecodeRuneInString(text[1:])
	if r == 'ſ' {
		return 1 + size
	}
	if r >= utf8.RuneSelf {
		return 0
	}
	switch r | 0x20 {
	case 's', 'd', 'm', 't':
		return 2
	case 'l', 'v', 'r':
		if len(text) > 2 {
			last := text[2] | 0x20
			if (r|0x20 == 'l' && last == 'l') || (r|0x20 != 'l' && last == 'e') {
				return 3
			}
		}
	}
	return 0
}

// cl100kPiece implements the ordered alternatives and possessive quantifiers
// of the reference cl100k_base regular expression. text is nonempty valid UTF-8.
func cl100kPiece(ctx context.Context, text string) (int, error) {
	if n := cl100kContraction(text); n != 0 {
		return n, nil
	}
	first, width := utf8.DecodeRuneInString(text)
	kind := cl100kRuneClass(first)
	second, _ := utf8.DecodeRuneInString(text[width:])
	nextKind := cl100kRuneClass(second)
	switch {
	case kind == clLetter:
		return cl100kRun(ctx, text, width, clLetter)
	case kind != clNumber && first != '\r' && first != '\n' && nextKind == clLetter:
		return cl100kRun(ctx, text, width, clLetter)
	case kind == clNumber:
		end := width
		for i := 1; i < 3 && end < len(text); i++ {
			r, size := utf8.DecodeRuneInString(text[end:])
			if cl100kRuneClass(r) != clNumber {
				break
			}
			end += size
		}
		return end, nil
	case kind == clOther || (first == ' ' && width < len(text) && nextKind == clOther):
		end, err := cl100kRun(ctx, text, width, clOther)
		if err != nil {
			return 0, err
		}
		for end < len(text) && (text[end] == '\r' || text[end] == '\n') {
			end++
		}
		return end, nil
	default:
		return cl100kWhitespace(ctx, text, width)
	}
}

func cl100kRun(ctx context.Context, text string, start int, class cl100kClass) (int, error) {
	nextCheck := start + 1024
	end := start
	for end < len(text) {
		if end >= nextCheck {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			nextCheck = end + 1024
		}
		r, size := utf8.DecodeRuneInString(text[end:])
		if cl100kRuneClass(r) != class {
			break
		}
		end += size
	}
	return end, nil
}

func cl100kWhitespace(ctx context.Context, text string, width int) (int, error) {
	previous, newline, end := 0, 0, 0
	nextCheck := 1024
	for end < len(text) {
		if end >= nextCheck {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			nextCheck = end + 1024
		}
		r, size := utf8.DecodeRuneInString(text[end:])
		if cl100kRuneClass(r) != clSpace {
			break
		}
		previous, end = end, end+size
		if r == '\r' || r == '\n' {
			newline = end
		}
	}
	switch {
	case end == len(text):
		return end, nil
	case newline != 0:
		return newline, nil
	case previous != 0:
		return previous, nil
	default:
		return width, nil
	}
}
