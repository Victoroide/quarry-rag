package pipeline

import (
	"regexp"
	"strings"
)

// citationQuote tracks whether unmarked prose can continue a quote lazily.
// Its fence and HTML state do not contribute to the reported code ranges.
type citationQuote struct {
	fence     citationFence
	html      citationHTMLBlock
	paragraph bool
}

func (q *citationQuote) line(content string, indent int) citationParagraph {
	body := content[1:]
	// The quote marker consumes at most one following column. The rest
	// counts as indentation within the quote, including a partial tab.
	padding, column := scanCodeIndent(body, indent+1)
	innerIndent := max(0, column-indent-2)
	body = body[padding:]
	if blankCodeContent(body) {
		q.paragraph = false
		if q.html == citationHTMLUntilBlank {
			q.html = ""
		}
		return noCitationParagraph
	}
	if q.html != "" {
		q.html = q.html.afterLine(body)
		return noCitationParagraph
	}
	char, length := scanFenceDelimiter(body)
	if q.fence.length != 0 {
		if q.fence.closes(innerIndent, char, length, body) {
			q.fence = citationFence{}
		}
		return noCitationParagraph
	}
	if innerIndent <= 3 {
		switch {
		case isFenceOpener(body, char, length):
			q.fence = citationFence{char: char, length: length}
			q.paragraph = false
		case citationParagraphBoundary(body, q.paragraph):
			q.paragraph = false
		default:
			html := citationHTMLStart(body, q.paragraph)
			q.html = html.afterLine(body)
			q.paragraph = html == ""
		}
	}
	if q.paragraph {
		return quoteCitationParagraph
	}
	return noCitationParagraph
}

// Except for the two named modes, a nonempty value is a literal terminator.
type citationHTMLBlock string

const (
	citationHTMLUntilBlank citationHTMLBlock = "blank line"
	citationHTMLRawTag     citationHTMLBlock = "raw end tag"
)

var (
	citationRawHTMLEnd      = regexp.MustCompile(`</(?:[pP][rR][eE]|[sS][cC][rR][iI][pP][tT]|[sS][tT][yY][lL][eE]|[tT][eE][xX][tT][aA][rR][eE][aA])>`)
	citationCompleteHTMLTag = regexp.MustCompile(`^<(?:/[A-Za-z][A-Za-z0-9-]*[ \t]*|` +
		`[A-Za-z][A-Za-z0-9-]*(?:[ \t]+[A-Za-z_:][A-Za-z0-9_.:-]*` +
		`(?:[ \t]*=[ \t]*(?:"[^"\r]*"|'[^'\r]*'|[^ \t\r"'=<>\x60]+))?)*[ \t]*/?)>[ \t]*$`)
)

func (block citationHTMLBlock) afterLine(content string) citationHTMLBlock {
	switch block {
	case "", citationHTMLUntilBlank:
		return block
	case citationHTMLRawTag:
		if citationRawHTMLEnd.MatchString(content) {
			return ""
		}
	default:
		if strings.Contains(content, string(block)) {
			return ""
		}
	}
	return block
}

// citationHTMLStart recognizes the CommonMark HTML block starts that can
// affect whether a subsequent list marker is part of an open paragraph.
func citationHTMLStart(content string, paragraphOpen bool) citationHTMLBlock {
	if content[0] != '<' {
		return ""
	}
	line := strings.TrimSuffix(content, "\r")
	switch {
	case strings.HasPrefix(line, "<!--"):
		return "-->"
	case strings.HasPrefix(line, "<?"):
		return "?>"
	case strings.HasPrefix(line, "<![CDATA["):
		return "]]>"
	case strings.HasPrefix(line, "<!") && len(line) > 2 && asciiLetter(line[2]):
		return ">"
	}
	tag, closing := strings.CutPrefix(line[1:], "/")
	nameEnd := 0
	for nameEnd < len(tag) && (asciiLetter(tag[nameEnd]) || tag[nameEnd] >= '0' && tag[nameEnd] <= '9' || tag[nameEnd] == '-') {
		nameEnd++
	}
	tail := tag[nameEnd:]
	boundary := tail == "" || strings.ContainsRune(" \t>", rune(tail[0]))
	switch strings.ToLower(tag[:nameEnd]) {
	case "pre", "script", "style", "textarea":
		if !closing && boundary {
			return citationHTMLRawTag
		}
		if !closing {
			return ""
		}
	case "address", "article", "aside", "base", "basefont", "blockquote", "body", "caption", "center",
		"col", "colgroup", "dd", "details", "dialog", "dir", "div", "dl", "dt", "fieldset", "figcaption",
		"figure", "footer", "form", "frame", "frameset", "h1", "h2", "h3", "h4", "h5", "h6", "head",
		"header", "hr", "html", "iframe", "legend", "li", "link", "main", "menu", "menuitem", "nav",
		"noframes", "ol", "optgroup", "option", "p", "param", "search", "section", "summary", "table",
		"tbody", "td", "tfoot", "th", "thead", "title", "tr", "track", "ul":
		if boundary || strings.HasPrefix(tail, "/>") {
			return citationHTMLUntilBlank
		}
	}
	if !paragraphOpen && citationCompleteHTMLTag.MatchString(line) {
		return citationHTMLUntilBlank
	}
	return ""
}

func asciiLetter(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}
