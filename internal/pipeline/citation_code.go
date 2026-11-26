package pipeline

import (
	"sort"
	"strings"
)

// asciiPunctuation lists the characters a backslash can escape in Markdown.
const asciiPunctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"

// citationCodeRanges returns the sorted, non-overlapping byte ranges of
// Markdown code in text: fenced code blocks and single-line inline code spans
// as defined by CommonMark. Indented code blocks, fences indented four or more
// spaces, fences inside block quotes and HTML code elements are not recognized.
// Fence-like openers indented four or more spaces are still tracked until a
// matching closing line so their closing fences cannot start a stray block, and
// list-item fences opened with two or three spaces close across four- to
// six-space closing lines. Lines end at a line feed, so a carriage return
// before it belongs to the line.
func citationCodeRanges(text string) [][2]int {
	var ranges [][2]int
	var fence struct {
		char                  byte
		indent, length, start int
		open                  bool
	}
	var unrecognized struct {
		char   byte
		length int
		open   bool
	}
	for lineStart := 0; lineStart < len(text); {
		lineEnd := len(text)
		if i := strings.IndexByte(text[lineStart:], '\n'); i >= 0 {
			lineEnd = lineStart + i
		}
		line := text[lineStart:lineEnd]
		indent, char, length := scanFenceLine(line)
		switch {
		case fence.open:
			maxCloserIndent := 3
			if fence.indent >= 2 {
				maxCloserIndent = fence.indent + 3
			}
			if indent <= maxCloserIndent && isFenceCloser(line, indent, char, length, fence.char, fence.length) {
				ranges = append(ranges, [2]int{fence.start, lineStart + indent + length})
				fence.open = false
			}
		case unrecognized.open:
			if isFenceCloser(line, indent, char, length, unrecognized.char, unrecognized.length) {
				unrecognized.open = false
			}
		case indent <= 3 && isFenceOpener(line, indent, char, length):
			fence.char, fence.indent, fence.length, fence.start, fence.open = char, indent, length, lineStart+indent, true
		case indent >= 4 && isFenceOpener(line, indent, char, length):
			unrecognized.char, unrecognized.length, unrecognized.open = char, length, true
		default:
			ranges = appendInlineCode(ranges, line, lineStart)
		}
		lineStart = lineEnd + 1
	}
	if fence.open {
		ranges = append(ranges, [2]int{fence.start, len(text)})
	}
	return ranges
}

// scanFenceLine measures leading space indentation and a run of backtick or
// tilde characters at the first non-space byte. A zero length means the line
// has no such delimiter run.
func scanFenceLine(line string) (indent int, char byte, length int) {
	for indent < len(line) && line[indent] == ' ' {
		indent++
	}
	if indent == len(line) || (line[indent] != '`' && line[indent] != '~') {
		return 0, 0, 0
	}
	char = line[indent]
	for indent+length < len(line) && line[indent+length] == char {
		length++
	}
	return indent, char, length
}

func isFenceCloser(line string, indent int, char byte, length int, openChar byte, openLength int) bool {
	return char == openChar && length >= openLength && strings.Trim(line[indent+length:], " \t\r") == ""
}

func isFenceOpener(line string, indent int, char byte, length int) bool {
	return length >= 3 && (char != '`' || !strings.Contains(line[indent+length:], "`"))
}

type backtickRun struct{ start, length int }

// appendInlineCode appends the code spans of one line. A backtick run that is
// not escaped opens a span closed by the next run of exactly the same length on
// the same line; backslashes are literal inside a span, and an opener without a
// closer is literal text. Closers come from a per-length index, so the cost is
// linear in the line length plus a logarithmic lookup per opener.
func appendInlineCode(ranges [][2]int, line string, base int) [][2]int {
	if !strings.Contains(line, "`") {
		return ranges
	}
	var runs []backtickRun
	byLength := make(map[int][]int)
	for i := 0; i < len(line); {
		if line[i] != '`' {
			i++
			continue
		}
		j := i
		for j < len(line) && line[j] == '`' {
			j++
		}
		byLength[j-i] = append(byLength[j-i], len(runs))
		runs = append(runs, backtickRun{start: i, length: j - i})
		i = j
	}
	current := 0
	for i := 0; i < len(line); {
		switch {
		case line[i] == '\\' && i+1 < len(line) && strings.IndexByte(asciiPunctuation, line[i+1]) >= 0:
			i += 2
		case line[i] == '`':
			for runs[current].start+runs[current].length <= i {
				current++
			}
			runEnd := runs[current].start + runs[current].length
			// An escaped first backtick leaves a shorter opener inside its run.
			length := runEnd - i
			candidates := byLength[length]
			next := sort.SearchInts(candidates, current+1)
			if next == len(candidates) {
				i = runEnd
				continue
			}
			end := runs[candidates[next]].start + length
			ranges = append(ranges, [2]int{base + i, base + end})
			i = end
		default:
			i++
		}
	}
	return ranges
}

// inCitationCode reports whether the byte offset lies inside one of the ranges.
func inCitationCode(pos int, ranges [][2]int) bool {
	i := sort.Search(len(ranges), func(i int) bool { return ranges[i][1] > pos })
	return i < len(ranges) && ranges[i][0] <= pos
}
