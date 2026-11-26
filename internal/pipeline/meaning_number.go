package pipeline

import (
	"slices"
	"strings"
	"unicode"
)

type numberClass uint8

const (
	numberValue numberClass = iota
	numberQuantity
	numberDate
	numberVersion
	numberPage
	numberCopyright
)

func (c numberClass) incidental() bool {
	return c == numberPage || c == numberCopyright
}

type numberLexemeKind uint8

const (
	numberPunctuation numberLexemeKind = iota
	numberDigits
	numberWord
)

type numberLexeme struct {
	start, end int
	kind       numberLexemeKind
	upper      bool
}

type numberGrammar struct {
	runes   []rune
	lexemes []numberLexeme
}

// Each stored excerpt line has its own lexical context. Normalize word case
// for alignment after recording the case of each word's initial rune.
func (x *meaningSignals) extractNumbers() {
	var buffer [64]numberLexeme
	lexemes := buffer[:0]
	start := 0
	for i, r := range x.runes {
		if r == '\n' {
			lexemes = x.extractLineNumbers(x.runes[start:i], start, lexemes[:0])
			start = i + 1
		}
	}
	x.extractLineNumbers(x.runes[start:], start, lexemes[:0])
}

func (x *meaningSignals) extractLineNumbers(runes []rune, offset int, lexemes []numberLexeme) []numberLexeme {
	if !slices.ContainsFunc(runes, unicode.IsDigit) {
		for i, r := range runes {
			runes[i] = unicode.ToLower(r)
		}
		return lexemes
	}
	for i := 0; i < len(runes); {
		start := i
		r := runes[i]
		i++
		if r == ' ' {
			continue
		}
		kind := numberPunctuation
		runes[start] = unicode.ToLower(r)
		switch {
		case unicode.IsDigit(r):
			kind = numberDigits
			for i < len(runes) && unicode.IsDigit(runes[i]) {
				i++
			}
		case unicode.IsLetter(r) || unicode.IsMark(r):
			kind = numberWord
			for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsMark(runes[i])) {
				i++
			}
			for j := start + 1; j < i; j++ {
				runes[j] = unicode.ToLower(runes[j])
			}
		}
		lexemes = append(lexemes, numberLexeme{start: start, end: i, kind: kind, upper: kind == numberWord && (unicode.IsUpper(r) || unicode.IsTitle(r))})
	}
	g := numberGrammar{runes: runes, lexemes: lexemes}
	for i := 0; i < len(lexemes); {
		if lexemes[i].kind != numberDigits {
			i++
			continue
		}
		class, end := g.classify(i)
		token := numberToken{start: offset + lexemes[i].start, class: class}
		for _, l := range lexemes[i:end] {
			if l.kind != numberDigits {
				continue
			}
			run := string(runes[l.start:l.end])
			token.runs = append(token.runs, run)
			if x.numbers == nil {
				x.numbers = make(map[string]struct{})
			}
			x.numbers[run] = struct{}{}
			token.end = offset + l.end
		}
		x.tokens = append(x.tokens, token)
		i = end
	}
	return lexemes
}

func (g numberGrammar) is(at int, kind numberLexemeKind) bool {
	return at >= 0 && at < len(g.lexemes) && g.lexemes[at].kind == kind
}

func (g numberGrammar) text(at int) string {
	if at < 0 || at >= len(g.lexemes) {
		return ""
	}
	l := g.lexemes[at]
	return string(g.runes[l.start:l.end])
}

func (g numberGrammar) mark(at int) rune {
	if g.is(at, numberPunctuation) {
		return g.runes[g.lexemes[at].start]
	}
	return 0
}

func (g numberGrammar) attached(left, right int) bool {
	return left >= 0 && right < len(g.lexemes) && g.lexemes[left].end == g.lexemes[right].start
}

func (g numberGrammar) digits(at int) int {
	if !g.is(at, numberDigits) {
		return 0
	}
	return g.lexemes[at].end - g.lexemes[at].start
}

func isNumberRangeDash(r rune) bool {
	return r == '-' || r == '–' || r == '—'
}

// Numeric expressions join compact separator/digit pairs. Free expressions
// also join fractions with spaces around the slash.
func (g numberGrammar) expressionEnd(at int, separators string, spacedFraction bool) int {
	end := at + 1
	for g.is(end+1, numberDigits) && strings.ContainsRune(separators, g.mark(end)) &&
		(spacedFraction && g.mark(end) == '/' || g.attached(end-1, end) && g.attached(end, end+1)) {
		end += 2
	}
	return end
}

func (g numberGrammar) classify(at int) (class numberClass, next int) {
	const separators = ".,:/-–—"
	switch g.text(at - 1) {
	case "v", "version":
		if g.wordBoundary(at - 1) {
			return numberVersion, g.expressionEnd(at, separators, false)
		}
	}
	if end := g.pageEnd(at); end != at {
		return numberPage, end
	}
	if end := g.copyrightEnd(at); end != at {
		return numberCopyright, end
	}
	end := g.expressionEnd(at, separators, true)
	if g.quantity(at, end) {
		return numberQuantity, end
	}
	if g.date(at) {
		return numberDate, end
	}
	return numberValue, end
}

func (g numberGrammar) date(at int) bool {
	separator := g.mark(at + 1)
	if (separator == '-' || separator == '/' || separator == '.') && g.mark(at+3) == separator &&
		g.attached(at, at+1) && g.attached(at+1, at+2) && g.attached(at+2, at+3) && g.attached(at+3, at+4) {
		a, b, c := g.digits(at), g.digits(at+2), g.digits(at+4)
		return b >= 1 && b <= 2 && (a == 4 && c >= 1 && c <= 2 || a >= 1 && a <= 2 && c == 4)
	}
	if g.digits(at) <= 2 && g.digits(at+2) == 4 {
		return !g.attached(at, at+1) && !g.attached(at+1, at+2) && isNumberMonth(g.text(at+1))
	}
	return g.digits(at) == 4 && g.digits(at-2) >= 1 && g.digits(at-2) <= 2 &&
		!g.attached(at-2, at-1) && !g.attached(at-1, at) && isNumberMonth(g.text(at-1))
}

func isNumberMonth(word string) bool {
	switch word {
	case "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december":
		return true
	}
	return false
}

func (g numberGrammar) quantity(at, end int) bool {
	after := g.mark(end)
	if unicode.Is(unicode.Sc, g.mark(at-1)) || unicode.Is(unicode.Sc, after) ||
		strings.ContainsRune("°%‰‱", after) {
		return true
	}
	if g.is(end, numberWord) && g.attached(end-1, end) {
		return true
	}
	// Standalone unit symbols also take a space. Symbols that double as
	// words in reference phrases, such as "in", require attachment.
	switch g.text(end) {
	case "kg", "mg", "g", "km", "cm", "mm", "ml", "hz", "khz", "mhz", "ghz", "pa", "kpa", "mpa", "kwh", "mwh",
		"m", "s", "h", "l", "kw", "mw", "bar", "rpm", "ms", "ns", "μs":
		return true
	case "k", "v", "w", "n", "a", "t":
		return g.lexemes[end].upper
	case "usd", "eur", "gbp", "jpy", "cny", "krw":
		return true
	}
	return false
}

// A page span needs a label, suffix, or matched frame. Validate each
// proposed continuation before consuming its digits.
func (g numberGrammar) pageEnd(at int) int {
	head := g.expressionEnd(at, ".", false)
	label := g.pagePrefix(at)
	explicit := label >= 0 && g.citationIntroduced(label)
	if isNumberRangeDash(g.mark(head)) && g.is(head+1, numberDigits) {
		end := g.expressionEnd(head+1, ".", false)
		end = g.pageVolumeLetter(end, explicit)
		if g.pageEndpoint(head+1, end, at) {
			page := g.finishPage(at, end, label >= 0, explicit, true)
			// In a compact reference a comma can open a separate count.
			compactComma := g.mark(end) == ',' && g.is(end+1, numberDigits) &&
				g.attached(head-1, head) && g.attached(head, head+1) && g.attached(end-1, end) && g.attached(end, end+1)
			if page > end || page == end && (compactComma || g.pageContinuation(end, explicit)) {
				return page
			}
		}
	}
	return g.finishPage(at, g.pageVolumeLetter(head, explicit), label >= 0, explicit, false)
}

func (g numberGrammar) finishPage(at, end int, prefix, explicit, rangeEndpoint bool) int {
	if suffix := g.pageSuffix(at, end); suffix != 0 {
		if suffix == g.lexemes[end].end {
			return g.pageTotal(at, end+1, explicit, false)
		}
		return end + 1
	}
	if g.framedPage(at, end) {
		return g.pageTotal(at, end+1, explicit, false)
	}
	if prefix && !g.numberAttachment(at, end) {
		return g.pageTotal(at, end, explicit, rangeEndpoint)
	}
	if g.mark(end) == '/' && g.is(end+1, numberDigits) && g.pageSuffix(at, end+2) != 0 {
		return end + 3
	}
	return at
}

func (g numberGrammar) pagePrefix(at int) int {
	label := at - 1
	for g.mark(label) == ':' || g.mark(label) == '#' {
		label--
	}
	switch {
	case g.mark(label) == '.' && g.attached(label-1, label):
		label--
		switch g.text(label) {
		case "p", "pp", "pg", "pág", "pag", "s":
			if g.wordBoundary(label) {
				return label
			}
		}
	case g.is(label, numberWord):
		switch g.text(label) {
		case "page", "pages", "página", "páginas", "pagina", "pagine", "seite", "seiten":
			if g.wordBoundary(label) {
				return label
			}
		}
	}
	return -1
}

func (g numberGrammar) citationIntroduced(label int) bool {
	before := label - 1
	switch g.text(before) {
	case "to", "à", "a":
		before--
	case ".":
		before--
	}
	switch g.text(before) {
	case "see", "refer", "consult", "compare", "read", "cf", "véase", "consulte", "ver", "siehe", "voir", "vedi":
		return g.wordBoundary(before)
	}
	return false
}

func (g numberGrammar) pageVolumeLetter(end int, explicit bool) int {
	if explicit && g.text(end) == "l" && g.lexemes[end].upper && !g.attached(end-1, end) &&
		(isPageTotalConnector(g.text(end+1)) || isDocumentPreposition(g.text(end+1))) && g.documentPhrase(end+2) {
		return end + 1
	}
	return end
}

func (g numberGrammar) wordBoundary(word int) bool {
	return !g.attached(word-1, word) || g.is(word-1, numberPunctuation)
}

// A suffix can start an unsegmented word in East Asian text.
func (g numberGrammar) pageSuffix(start, end int) int {
	if !g.is(end, numberWord) {
		return 0
	}
	before := g.text(start - 1)
	after := g.lexemes[end]
	for _, label := range []string{"ページ", "頁", "페이지", "쪽", "页"} {
		if label == "页" && !strings.HasSuffix(before, "第") {
			continue
		}
		marker := []rune(label)
		if len(marker) <= after.end-after.start && slices.Equal(g.runes[after.start:after.start+len(marker)], marker) {
			return after.start + len(marker)
		}
	}
	return 0
}

func (g numberGrammar) framedPage(at, end int) bool {
	dash := g.mark(at - 1)
	return isNumberRangeDash(dash) && g.mark(end) == dash &&
		(!g.is(at-2, numberDigits) || !g.attached(at-2, at-1)) &&
		(!g.is(end+1, numberDigits) || !g.attached(end, end+1))
}

func (g numberGrammar) pageEndpoint(at, end, referenceStart int) bool {
	return g.pageSuffix(referenceStart, end) != 0 || g.referenceEndpoint(at, end)
}

// A comma group or an unambiguous date is its own numeric expression,
// even where the first run would fit the shape of a reference endpoint.
func (g numberGrammar) referenceEndpoint(at, end int) bool {
	return !g.numberAttachment(at, end) && (g.mark(at+1) == '.' || !g.date(at))
}

func (g numberGrammar) numberAttachment(at, end int) bool {
	commaGroup := g.mark(end) == ',' && g.digits(end+1) == 3 && g.attached(end-1, end) && g.attached(end, end+1)
	return commaGroup || g.quantity(at, end)
}

func (g numberGrammar) pageContinuation(end int, explicit bool) bool {
	if g.referenceBoundary(end) {
		return true
	}
	switch g.mark(end) {
	case ',', ':':
		return explicit && g.citationTail(end+1)
	}
	switch word := g.text(end); {
	case isPageTotalConnector(word), isDocumentPreposition(word):
		return g.documentPhrase(end + 1)
	case explicit:
		return g.citationTail(end)
	}
	return false
}

func (g numberGrammar) referenceBoundary(at int) bool {
	if at == len(g.lexemes) || isNumberRangeDash(g.mark(at)) {
		return true
	}
	switch g.mark(at) {
	case '.', '。':
		return !g.is(at+1, numberDigits) || !g.attached(at, at+1)
	case ';', '!', '?', ')', ']', '}', '|':
		return true
	}
	return false
}

func (g numberGrammar) citationTail(at int) bool {
	switch g.text(at) {
	case "inclusive", "inclusively", "inclus", "einschließlich":
		at++
	}
	if g.referenceBoundary(at) {
		return true
	}
	switch g.text(at) {
	case "for", "about", "regarding", "and", "or", "as", "para", "pour", "für", "per", "et", "und", "y", "e":
		return g.is(at+1, numberWord)
	}
	return false
}

func isDocumentPreposition(word string) bool {
	switch word {
	case "in", "im", "dans", "en", "nel", "nella":
		return true
	}
	return false
}

// A document must head the locator, rather than modify a counted noun.
// Apostrophes are already folded, including in possessives.
func (g numberGrammar) documentPhrase(at int) bool {
	for i := at; i < at+4 && g.is(i, numberWord); i++ {
		if g.mark(i+1) == '\'' {
			return false
		}
		switch g.text(i) {
		case "manual", "guide", "handbook", "book", "document", "report", "appendix", "chapter", "section", "volume",
			"libro", "documento", "capítulo", "anexo", "guía", "apéndice", "rapport", "livre", "annexe", "tome",
			"manuel", "kapitel", "anhang", "handbuch", "bericht", "buch", "band", "capitolo", "allegato", "volumen":
			return g.documentHeadEnd(i + 1)
		}
	}
	return false
}

func (g numberGrammar) documentHeadEnd(end int) bool {
	if !g.is(end, numberWord) || isDocumentPreposition(g.text(end)) || g.citationTail(end) {
		return true
	}
	return g.citationPredicate(end)
}

// A locator or a page used as a subject can precede a predicate.
func (g numberGrammar) citationPredicate(at int) bool {
	switch g.text(at) {
	case "is", "are", "was", "were", "has", "have", "will",
		"contains", "describes", "illustrates", "explains", "provides", "includes",
		"presents", "identifies", "defines", "summarizes", "specifies":
		return g.is(at+1, numberWord)
	}
	return false
}

func isPageTotalConnector(word string) bool {
	switch word {
	case "of", "de", "von", "di", "sur":
		return true
	}
	return false
}

func (g numberGrammar) pageTotal(at, end int, explicit, rangeEndpoint bool) int {
	total := end + 1
	if !g.is(total, numberDigits) {
		return end
	}
	slash := g.mark(end) == '/'
	if !slash && (!isPageTotalConnector(g.text(end)) || g.attached(end-1, end) || g.attached(end, total)) {
		return end
	}
	after := g.pageVolumeLetter(total+1, explicit)
	if !g.pageEndpoint(total, after, at) {
		return end
	}
	if g.pageSuffix(at, after) != 0 {
		return after + 1
	}
	// A labeled slash total can precede a separate expression. For a
	// proposed range, require the denominator to finish the reference.
	if isPageTotalConnector(g.text(after)) && g.is(after+1, numberDigits) ||
		slash && !rangeEndpoint && (g.mark(after) == '/' || g.mark(after) == '.' || g.mark(after) == ':') ||
		g.pageContinuation(after, explicit) || !slash && g.citationPredicate(after) {
		return after
	}
	return end
}

// copyright = label year [dash (year | shortYear) attribution]
// The range must end at a notice boundary or a recognizable attribution.
func (g numberGrammar) copyrightEnd(at int) int {
	if g.digits(at) != 4 || !g.copyrightPrefix(at) || g.quantity(at, at+1) {
		return at
	}
	last := at + 2
	if isNumberRangeDash(g.mark(at+1)) && (g.digits(last) == 4 || g.digits(last) == 2) &&
		g.referenceEndpoint(last, last+1) && g.copyrightAttribution(last+1) {
		return last + 1
	}
	return at + 1
}

func (g numberGrammar) copyrightPrefix(at int) bool {
	label := at - 1
	for g.mark(label) == ':' || g.mark(label) == '#' {
		label--
	}
	return g.mark(label) == '©' || g.text(label) == "copyright" && g.wordBoundary(label) ||
		g.mark(label) == ')' && g.text(label-1) == "c" && g.mark(label-2) == '(' &&
			g.attached(label-2, label-1) && g.attached(label-1, label)
}

func (g numberGrammar) copyrightAttribution(at int) bool {
	if g.referenceBoundary(at) || g.rightsReservation(at) {
		return true
	}
	if !g.is(at, numberWord) {
		return false
	}

	// Scan the name as a phrase so a comma or the casing of one word
	// cannot turn the start of a new statement into an attribution.
	end, capitalized, words := at, 0, 0
	nameCase, dense := true, false
	for g.is(end, numberWord) && !g.rightsReservation(end) {
		word := g.text(end)
		if isAttributionClauseWord(word) {
			return false
		}
		words++
		if g.lexemes[end].upper {
			capitalized++
		} else if !isNameParticle(word) {
			nameCase = false
		}
		dense = dense || unicode.In(g.runes[g.lexemes[end].start], unicode.Han, unicode.Hangul, unicode.Hiragana, unicode.Katakana)
		end++
		mark := g.mark(end)
		if (mark == '-' || mark == '\'') && g.is(end+1, numberWord) &&
			g.attached(end-1, end) && g.attached(end, end+1) {
			end++
		}
	}

	if g.rightsReservation(end) {
		return true
	}
	organization := words > 1 && isOrganizationDesignator(g.text(end-1))
	if g.mark(end) == ',' && isOrganizationDesignator(g.text(end+1)) {
		organization = true
		end += 2
		if g.rightsReservation(end) {
			return true
		}
	}
	if !g.referenceBoundary(end) {
		return false
	}
	if isAttributionClauseWord(g.text(end+1)) || !isNumberRangeDash(g.mark(end)) && g.is(end+1, numberDigits) {
		return false
	}
	if g.is(end+1, numberWord) && g.mark(end) == ';' {
		return organization
	}
	return organization || dense || nameCase &&
		(capitalized >= 2 || capitalized == 1 && (end == len(g.lexemes) || g.mark(end) == '.'))
}

func (g numberGrammar) rightsReservation(at int) bool {
	switch g.mark(at) {
	case '.', ',', ':', ';':
		at++
	}
	return g.text(at) == "all" && g.text(at+1) == "rights" && g.text(at+2) == "reserved" &&
		!g.is(at+3, numberWord) && !g.is(at+3, numberDigits)
}

func isNameParticle(word string) bool {
	switch word {
	case "the", "of", "and", "de", "del", "da", "di", "du", "des", "der", "den", "van", "von", "la", "le":
		return true
	}
	return false
}

func isOrganizationDesignator(word string) bool {
	switch word {
	case "company", "corporation", "inc", "ltd", "llc", "gmbh", "office", "association", "foundation", "university":
		return true
	}
	return false
}

func isAttributionClauseWord(word string) bool {
	switch word {
	case "are", "were", "was", "is", "been", "being", "have", "has", "had", "will", "would", "should", "only", "plus":
		return true
	}
	return false
}
