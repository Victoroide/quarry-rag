package pipeline

import (
	"sort"
	"strings"
)

// asciiPunctuation lists the characters a backslash can escape in Markdown.
const asciiPunctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"

// citationCodeRanges returns sorted, non-overlapping byte ranges for fences
// on their own lines and single-line inline spans. It tracks one simple list
// item at a time. Lines are separated by LF, with an optional preceding CR.
func citationCodeRanges(text string) [][2]int {
	var ranges [][2]int
	var fence citationFence
	var paragraph citationParagraph
	var quote citationQuote
	var html citationHTMLBlock
	listMargin := 0
	for lineStart := 0; lineStart < len(text); {
		lineEnd := len(text)
		if i := strings.IndexByte(text[lineStart:], '\n'); i >= 0 {
			lineEnd = lineStart + i
		}
		line := text[lineStart:lineEnd]
		indentBytes, indent := scanCodeIndent(line, 0)
		content := line[indentBytes:]
		start := lineStart
		lineStart = lineEnd + 1
		if blankCodeContent(content) {
			paragraph = noCitationParagraph
			quote = citationQuote{}
			if html == citationHTMLUntilBlank {
				html = ""
			}
			continue
		}

		// HTML context only affects paragraph and list admission. Preserve the
		// existing fence and inline-code recognition within HTML blocks.
		inHTML := html != ""
		html = html.afterLine(content)
		char, length := scanFenceDelimiter(content)
		if fence.length != 0 {
			dedented := indent < fence.contentMargin()
			if !dedented && !fence.closes(indent, char, length, content) {
				continue
			}
			if !fence.suppressed {
				end := start
				if !dedented {
					end += indentBytes + length
				}
				ranges = append(ranges, [2]int{fence.start, end})
			}
			fence = citationFence{}
			if !dedented {
				continue
			}
			// The dedent belongs to the outside scanner.
		}

		if indent < listMargin {
			listMargin = 0
		}
		if isFenceOpener(content, char, length) {
			fence = citationFence{char: char, length: length, start: start + indentBytes, suppressed: indent >= 4}
			if indent <= listMargin+3 {
				paragraph = noCitationParagraph
				quote = citationQuote{}
				fence.listMargin = listMargin
			}
			continue
		}
		ranges = appendInlineCode(ranges, line, start)
		if inHTML {
			continue
		}
		if listMargin == 0 && indent <= 3 {
			if content[0] == '>' {
				paragraph = quote.line(content, indent)
				continue
			}
			if html = citationHTMLStart(content, paragraph == topLevelCitationParagraph); html != "" {
				paragraph = noCitationParagraph
				quote = citationQuote{}
				html = html.afterLine(content)
				continue
			}
		}
		listMargin, paragraph = citationLineContext(content, indent, listMargin, paragraph)
		if paragraph != quoteCitationParagraph {
			quote = citationQuote{}
		}
	}
	if fence.length != 0 && !fence.suppressed {
		ranges = append(ranges, [2]int{fence.start, len(text)})
	}
	return ranges
}

type citationFence struct {
	char                      byte
	length, start, listMargin int
	suppressed                bool
}

func (f citationFence) standaloneIndented() bool {
	return f.suppressed && f.listMargin == 0
}

func (f citationFence) contentMargin() int {
	if f.standaloneIndented() {
		return 4
	}
	return f.listMargin
}

func (f citationFence) closes(indent int, char byte, length int, content string) bool {
	return (f.standaloneIndented() || indent <= f.listMargin+3) &&
		char == f.char && length >= f.length && strings.Trim(content[length:], " \t\r") == ""
}

// scanCodeIndent returns the number of leading indentation bytes and the
// resulting visual column. Tabs advance to the next four-column stop.
func scanCodeIndent(line string, column int) (bytes, columns int) {
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case ' ':
			column++
		case '\t':
			column += 4 - column%4
		default:
			return i, column
		}
	}
	return len(line), column
}

func blankCodeContent(content string) bool {
	return content == "" || content == "\r"
}

type citationParagraph uint8

const (
	noCitationParagraph citationParagraph = iota
	topLevelCitationParagraph
	listCitationParagraph
	quoteCitationParagraph
)

func citationLineContext(content string, indent, margin int, paragraph citationParagraph) (int, citationParagraph) {
	if indent > margin+3 {
		// Indented text may continue an open paragraph but cannot open one.
		return margin, paragraph
	}
	interrupting := paragraph == topLevelCitationParagraph
	if citationParagraphBoundary(content, interrupting || (margin != 0 && paragraph == listCitationParagraph)) {
		return margin, noCitationParagraph
	}
	if margin != 0 {
		return margin, listCitationParagraph
	}
	if itemMargin, body := citationListStart(content, indent, interrupting); itemMargin != 0 {
		if body == "" || citationParagraphBoundary(body, false) {
			return itemMargin, noCitationParagraph
		}
		return itemMargin, listCitationParagraph
	}
	if paragraph == listCitationParagraph || paragraph == quoteCitationParagraph {
		// Lazy prose can continue a container paragraph without restoring
		// the list fence margin after a nonblank dedent.
		return 0, paragraph
	}
	return 0, topLevelCitationParagraph
}

// citationListStart runs after block boundaries such as thematic breaks have
// been checked. Only an ordered marker whose value is one can interrupt a
// top-level paragraph. After a list item, an ordered marker may start at any
// value, including a sibling with the same delimiter.
func citationListStart(content string, indent int, interrupting bool) (margin int, body string) {
	width := 1
	switch content[0] {
	case '-', '+', '*':
	default:
		digits := 0
		for digits < min(9, len(content)) && content[digits] >= '0' && content[digits] <= '9' {
			digits++
		}
		if digits == 0 || digits == len(content) || (content[digits] != '.' && content[digits] != ')') {
			return 0, ""
		}
		if interrupting && strings.TrimLeft(content[:digits], "0") != "1" {
			return 0, ""
		}
		width += digits
	}

	markerEnd := indent + width
	padding, margin := scanCodeIndent(content[width:], markerEnd)
	if padding == 0 || blankCodeContent(content[width+padding:]) {
		return 0, ""
	}
	if margin > markerEnd+4 {
		return markerEnd + 1, ""
	}
	return margin, content[width+padding:]
}

func citationParagraphBoundary(content string, paragraphOpen bool) bool {
	line := strings.TrimSuffix(content, "\r")
	switch line[0] {
	case '#':
		length := len(line) - len(strings.TrimLeft(line, "#"))
		return length <= 6 && (length == len(line) || line[length] == ' ' || line[length] == '\t')
	case '-', '=', '*', '_':
		mark := line[0]
		if paragraphOpen && (mark == '-' || mark == '=') && strings.TrimRight(strings.TrimLeft(line, line[:1]), " \t") == "" {
			return true
		}
		if mark == '=' {
			return false
		}
		count := 0
		for i := range len(line) {
			switch line[i] {
			case mark:
				count++
			case ' ', '\t':
			default:
				return false
			}
		}
		return count >= 3
	default:
		return false
	}
}

func scanFenceDelimiter(content string) (char byte, length int) {
	char = content[0]
	if char == '`' || char == '~' {
		for length < len(content) && content[length] == char {
			length++
		}
	}
	return char, length
}

func isFenceOpener(content string, char byte, length int) bool {
	return length >= 3 && (char != '`' || !strings.Contains(content[length:], "`"))
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
