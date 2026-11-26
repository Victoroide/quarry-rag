package pipeline

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/citationsupport"
	"github.com/Victoroide/quarry-rag/internal/domain"
)

// CitationVerifier assesses a bounded batch for the configured model version.
type CitationVerifier interface {
	Version() string
	Verify(context.Context, citationsupport.Batch) ([]citationsupport.Status, error)
}

// WithCitationSupport enables assessment after requested citations are parsed.
func (s *SynthesisStage) WithCitationSupport(verifier CitationVerifier) *SynthesisStage {
	s.citationVerifier = verifier
	return s
}

type citedSentence struct {
	text      string
	citations []int
	ambiguous bool
}

func citationClaims(answer string, citations []Citation, cleanup *citationCleanup) []citedSentence {
	references := make(map[int][]int)
	for i, citation := range citations {
		offset := citation.markerByte
		if cleanup != nil {
			var valid bool
			offset, valid = cleanup.origins[offset]
			if !valid {
				continue
			}
		}
		references[offset] = append(references[offset], i)
	}
	var sentences []citedSentence
	var sentence strings.Builder
	var cited []int
	var quotes quotations
	brackets, parentheses, codeDelimiter := 0, 0, 0
	hasText, ambiguous, quotedTerminal, quotedEnd := false, false, false, false
	spacePending := false
	flush := func() {
		if hasText {
			sentences = append(sentences, citedSentence{
				text: sentence.String(), citations: cited,
				ambiguous: ambiguous || brackets != 0 || parentheses != 0 || len(quotes) != 0 || codeDelimiter != 0,
			})
		}
		sentence.Reset()
		spacePending = false
		cited, quotes, hasText, ambiguous, quotedTerminal, quotedEnd = nil, nil, false, false, false, false
		brackets, parentheses, codeDelimiter = 0, 0, 0
	}

	markers := citationMarker.FindAllStringIndex(answer, -1)
	for pos := 0; pos < len(answer); {
		if pos == 0 || answer[pos-1] == '\n' || answer[pos-1] == '\r' {
			pos += claimListPrefix(answer[pos:])
			if pos == len(answer) {
				break
			}
		}
		if len(markers) > 0 && pos == markers[0][0] {
			end := markers[0][1]
			markers = markers[1:]
			if brackets == 0 && codeDelimiter == 0 {
				// A citation-shaped token inside a quotation can be literal text.
				// Retain citation accounting and withhold assessment of the sentence.
				ambiguous = ambiguous || len(quotes) > 0
				if hasText {
					cited = append(cited, references[pos]...)
				}
				spacePending = sentence.Len() > 0
				pos = end
				continue
			}
			if cleanup != nil {
				// The scanner keeps this marker as literal text. Abstain if JSON
				// displays a different expression after legacy marker cleanup.
				_, edited := cleanup.editedMarkers[pos]
				ambiguous = ambiguous || edited
			}
		}
		r, size := utf8.DecodeRuneInString(answer[pos:])
		switch {
		case r == '\n' || r == '\r':
			line := afterClaimLineBreak(answer[pos:])
			lineStart := len(answer) - len(line)
			next, _ := utf8.DecodeRuneInString(strings.TrimLeftFunc(line, horizontalSpace))
			quotedContinuation := quotedEnd && unicode.IsLower(next)
			parentheticalContinuation := parentheses > 0
			if hasText && brackets == 0 && isClaimContinuation(line) && (!quotedEnd || quotedContinuation || parentheticalContinuation) {
				if codeDelimiter > 0 || len(quotes) > 0 {
					sentence.WriteString(answer[pos:lineStart])
				} else {
					spacePending = true
				}
				ambiguous = ambiguous || (!quotedContinuation && !parentheticalContinuation)
				quotedEnd = false
			} else {
				flush()
			}
			pos = lineStart
			continue
		case brackets == 0 && r == '`':
			// A code span closes only on a backtick run of the opening width.
			size = len(answer[pos:]) - len(strings.TrimLeft(answer[pos:], "`"))
			switch codeDelimiter {
			case size:
				codeDelimiter = 0
			case 0:
				codeDelimiter = size
			}
		case codeDelimiter > 0:
			hasText = hasText || unicode.IsLetter(r) || unicode.IsNumber(r)
		case len(quotes) == 0 && unicode.IsSpace(r):
			spacePending = sentence.Len() > 0
			pos += size
			continue
		case brackets == 0 && r == '\\' && pos+size < len(answer) && quotes.escapes(answer[pos+size]):
			// Outside a code span, an escaped backslash or backtick is literal
			// text, and so is an escaped quote inside a straight double-quoted string.
			size++
		case r == '[':
			brackets++
		case r == ']' && brackets > 0:
			brackets--
		case brackets == 0 && (r == '(' || r == '（'):
			parentheses++
		case brackets == 0 && parentheses > 0 && (r == ')' || r == '）'):
			parentheses--
		case brackets == 0 && quotes.accepts(r):
			before, _ := utf8.DecodeLastRuneInString(answer[:pos])
			next, _ := utf8.DecodeRuneInString(answer[pos+size:])
			// A backslash the escape rule left unconsumed may escape this mark in
			// a convention the scanner cannot decide, so the literal is uncertain.
			ambiguous = ambiguous || oddBackslashes(answer[:pos])
			if quotes.mark(r, before, next) && quotedTerminal {
				following := afterMarkers(answer, pos+size, markers)
				ambiguous = ambiguous || uncertainAfterQuotation(following)
				quotedEnd = following == '\n' || following == '\r'
				quotedTerminal = false
			}
		case brackets == 0 && len(quotes) > 0 && unicode.Is(unicode.Sentence_Terminal, r):
			quotedTerminal = true
		case brackets == 0 && parentheses == 0 && unicode.Is(unicode.Sentence_Terminal, r):
			boundary := true
			if r == '.' {
				var uncertain bool
				boundary, uncertain = claimPeriod(answer, pos, len(cited) != 0)
				ambiguous = ambiguous || uncertain
			}
			if boundary {
				flush()
				pos += size
				continue
			}
		case brackets == 0 && (unicode.IsLetter(r) || unicode.IsNumber(r)):
			hasText = true
		}
		if spacePending {
			sentence.WriteByte(' ')
			spacePending = false
		}
		sentence.WriteString(answer[pos : pos+size])
		pos += size
	}
	flush()
	return sentences
}

// quoteClosers lists the marks that can close each opening quotation mark.
// Several marks open in one convention and close in another, so a mark that
// fits the innermost open quotation closes it and any other opens a new one.
var quoteClosers = map[rune]string{
	'"': `"`, '\'': "'", '“': "”", '”': "”", '„': "“”", '‘': "’", '‚': "‘’",
	'«': "»", '»': "«", '‹': "›", '›': "‹",
	'「': "」", '『': "』", '《': "》", '〈': "〉", '｢': "｣", '〝': "〞〟",
}

// quotations holds the closers of the open quotations, innermost last. Terminal
// punctuation inside a quotation does not end the sentence around it.
type quotations []string

func (q quotations) accepts(r rune) bool {
	if _, opens := quoteClosers[r]; opens {
		return true
	}
	return len(q) > 0 && strings.ContainsRune(q[len(q)-1], r)
}

// escapes reports whether a backslash before the byte next keeps it literal.
// A straight double quote is escapable only inside a straight double-quoted
// string; elsewhere a backslash before it is prose.
func (q quotations) escapes(next byte) bool {
	if next == '\\' || next == '`' {
		return true
	}
	return next == '"' && len(q) > 0 && q[len(q)-1] == `"`
}

// oddBackslashes reports whether text ends in an unpaired backslash.
func oddBackslashes(text string) bool {
	return (len(text)-len(strings.TrimRight(text, `\`)))%2 == 1
}

// mark records a quotation mark and reports whether it closed the outermost
// quotation. Adjacent word characters distinguish single quotes from apostrophes.
func (q *quotations) mark(r, before, next rune) (closedOutermost bool) {
	n := len(*q)
	closes := n > 0 && strings.ContainsRune((*q)[n-1], r)
	if r == '\'' {
		wordBefore := unicode.IsLetter(before) || unicode.IsNumber(before) || unicode.IsMark(before)
		wordAfter := unicode.IsLetter(next) || unicode.IsNumber(next) || unicode.IsMark(next)
		if (wordBefore && wordAfter) || (!closes && (wordBefore || unicode.IsSpace(next) || next == utf8.RuneError)) {
			return false
		}
	}
	if closes {
		if r == '’' && (unicode.IsLetter(next) || unicode.IsNumber(next)) {
			return false
		}
		*q = (*q)[:n-1]
		return n == 1
	}
	if closers, opens := quoteClosers[r]; opens {
		*q = append(*q, closers)
	}
	return false
}

// uncertainAfterQuotation reports whether text that follows a quotation holding
// terminal punctuation may start a new sentence on the same line.
func uncertainAfterQuotation(next rune) bool {
	_, opensQuotation := quoteClosers[next]
	return (unicode.IsLetter(next) && !unicode.IsLower(next)) || unicode.IsNumber(next) || opensQuotation
}

// afterMarkers returns the first rune at or after from that is neither
// horizontal space nor part of a citation marker.
func afterMarkers(answer string, from int, markers [][]int) rune {
	for {
		from = len(answer) - len(strings.TrimLeftFunc(answer[from:], horizontalSpace))
		for len(markers) > 0 && markers[0][0] < from {
			markers = markers[1:]
		}
		if len(markers) == 0 || markers[0][0] != from {
			r, _ := utf8.DecodeRuneInString(answer[from:])
			return r
		}
		from = markers[0][1]
	}
}

func horizontalSpace(r rune) bool {
	return r == '\t' || unicode.Is(unicode.Zs, r)
}

func afterClaimLineBreak(text string) string {
	if strings.HasPrefix(text, "\r\n") {
		return text[2:]
	}
	return text[1:]
}

func isClaimContinuation(line string) bool {
	trimmed := strings.TrimLeftFunc(line, horizontalSpace)
	return trimmed != "" && trimmed[0] != '\n' && trimmed[0] != '\r' && claimListPrefix(trimmed) == 0
}

func claimListPrefix(line string) int {
	trimmed := strings.TrimLeftFunc(line, horizontalSpace)
	marker := strings.TrimLeftFunc(trimmed, unicode.IsDigit)
	r, size := utf8.DecodeRuneInString(marker)
	if len(marker) < len(trimmed) {
		if r != '.' && r != ')' {
			return 0
		}
	} else if !strings.ContainsRune("-*+•", r) {
		return 0
	}
	after := strings.TrimLeftFunc(marker[size:], horizontalSpace)
	if len(after) == len(marker)-size {
		return 0
	}
	return len(line) - len(after)
}

// Short tokens can be abbreviations even without capitalization or internal
// periods.
const maxAbbreviationLetters = 3

// allowsLeadingPoint recognizes positions where a token can start with a point,
// as in .5, $.50, -.25 or .env.
func allowsLeadingPoint(before rune, none bool) bool {
	return none || unicode.IsSpace(before) || unicode.In(before, unicode.Sc, unicode.Ps, unicode.Pi) || strings.ContainsRune("+-−±", before)
}

// claimPeriod keeps decimals, dotted names, abbreviations and personal initials
// together. A period that cannot be told from an abbreviation or an ellipsis
// joins its neighbors: the join is retained for coverage and skipped for egress.
func claimPeriod(text string, pos int, cited bool) (boundary, uncertain bool) {
	before, beforeSize := utf8.DecodeLastRuneInString(text[:pos])
	suffix := text[pos+1:]
	immediate, _ := utf8.DecodeRuneInString(suffix)
	if (unicode.IsLetter(before) && unicode.IsLetter(immediate)) || (unicode.IsDigit(before) && unicode.IsDigit(immediate)) ||
		((unicode.IsLetter(immediate) || unicode.IsDigit(immediate)) && allowsLeadingPoint(before, beforeSize == 0)) {
		return false, false
	}
	if before == '.' || immediate == '.' {
		return false, true
	}
	following := strings.TrimLeftFunc(suffix, horizontalSpace)
	if strings.HasPrefix(following, "\n") || strings.HasPrefix(following, "\r") {
		if line := afterClaimLineBreak(following); isClaimContinuation(line) {
			following = strings.TrimLeftFunc(line, horizontalSpace)
		}
	}
	next, _ := utf8.DecodeRuneInString(following)
	word := text[:pos]
	if i := strings.LastIndexFunc(word, func(r rune) bool { return r != '.' && !unicode.IsLetter(r) }); i >= 0 {
		_, size := utf8.DecodeRuneInString(word[i:])
		word = word[i+size:]
	}
	if strings.Contains(word, ".") && dottedInitials(word) {
		if unicode.IsLower(next) || unicode.IsDigit(next) || next == '[' {
			return false, false
		}
		if unicode.IsLetter(next) {
			return false, true
		}
	}
	if unicode.IsUpper(next) && utf8.RuneCountInString(word) == 1 && unicode.IsUpper(before) {
		return false, false
	}
	switch strings.ToLower(word) {
	case "dr", "mr", "mrs", "ms", "prof", "sr", "sra", "dra", "st":
		if unicode.IsLetter(next) {
			return false, false
		}
	case "approx", "cf", "co", "dept", "est", "etc", "fig", "inc", "ltd", "no", "pp", "rev", "vol", "vs":
		if unicode.IsLetter(next) || unicode.IsDigit(next) || next == '[' {
			return false, true
		}
	}
	if (unicode.IsUpper(next) || unicode.IsDigit(next)) && abbreviationLike(word) {
		return false, true
	}
	if !cited && unicode.IsLower(next) && (unicode.IsLetter(before) || unicode.IsNumber(before)) {
		return false, true
	}
	return true, false
}

// abbreviationLike identifies tokens that may be titles, shortened words or
// initials when the next token starts with a capital or digit.
func abbreviationLike(word string) bool {
	initial, _ := utf8.DecodeRuneInString(word)
	return word != "" && (unicode.IsUpper(initial) || unicode.IsTitle(initial) || strings.Contains(word, ".") ||
		utf8.RuneCountInString(word) <= maxAbbreviationLetters)
}

func dottedInitials(word string) bool {
	for part := range strings.SplitSeq(word, ".") {
		r, size := utf8.DecodeRuneInString(part)
		if !unicode.IsLetter(r) || len(part) != size {
			return false
		}
	}
	return true
}

func (s *SynthesisStage) finalizeCitations(ctx context.Context, qc *QueryContext) error {
	answer := qc.FinalAnswer
	var cleanup *citationCleanup
	var literal [][2]int
	if qc.Stream == nil {
		if s.citationVerifier != nil {
			cleanup = &citationCleanup{origins: make(map[int]int), editedMarkers: make(map[int]struct{})}
		}
		qc.FinalAnswer, literal = cleanCitationMarkersInto(answer, len(qc.Excerpts), cleanup, qc.PreserveCode)
	} else if qc.PreserveCode {
		literal = citationCodeRanges(answer)
	}
	qc.Citations = verifyCitationMarkers(qc.FinalAnswer, qc.Excerpts, literal)
	if s.citationVerifier == nil {
		return nil
	}

	timeout := s.cfg.CitationSupportTimeout
	if timeout <= 0 {
		timeout = citationsupport.DefaultTimeout
	}
	verifyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	batch, assessments := s.prepareCitationBatch(qc, citationClaims(answer, qc.Citations, cleanup))
	if len(batch.Claims) == 0 || verifyCtx.Err() != nil {
		return ctx.Err()
	}
	if err := qc.CheckEvidenceScope(verifyCtx); err != nil {
		if verifyCtx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	statuses, err := s.citationVerifier.Verify(verifyCtx, batch)
	if err == nil && verifyCtx.Err() == nil && len(statuses) == len(assessments) {
		for i, status := range statuses {
			switch status {
			case citationsupport.Supported, citationsupport.Contradicted, citationsupport.Insufficient:
				for _, assessment := range assessments[i] {
					assessment.Status = status
				}
			}
		}
	}
	return ctx.Err()
}

func (s *SynthesisStage) prepareCitationBatch(qc *QueryContext, sentences []citedSentence) (citationsupport.Batch, [][]*citationsupport.Assessment) {
	limit := s.cfg.CitationSupportMaxPairs
	if limit <= 0 {
		limit = citationsupport.DefaultMaxPairs
	}
	limit = min(limit, citationsupport.MaxPairs)
	version := s.citationVerifier.Version()
	newAssessment := func(evidence []int) *citationsupport.Assessment {
		return &citationsupport.Assessment{Status: citationsupport.Unchecked, Method: citationsupport.Method, Version: version, Evidence: evidence}
	}
	for i := range qc.Citations {
		qc.Citations[i].Support = newAssessment([]int{qc.Citations[i].Excerpt})
	}
	qc.CitationCoverage = &citationsupport.Coverage{Sentences: len(sentences)}
	var batch citationsupport.Batch
	var assessments [][]*citationsupport.Assessment
	groups := make(map[string]int)
	for _, sentence := range sentences {
		if len(sentence.citations) == 0 {
			continue
		}
		qc.CitationCoverage.CitedSentences++
		evidence := make([]int, 0, min(len(sentence.citations), len(qc.Excerpts)))
		for _, i := range sentence.citations {
			evidence = append(evidence, qc.Citations[i].Excerpt)
		}
		slices.Sort(evidence)
		evidence = slices.Compact(evidence)
		assessment := newAssessment(evidence)
		for _, i := range sentence.citations {
			qc.Citations[i].Support = assessment
		}
		if sentence.ambiguous {
			continue
		}
		pairs, indices := citationPairs(batch.Pairs, sentence.text, evidence, qc.Excerpts, limit)
		if indices == nil {
			continue
		}
		key := fmt.Sprint(indices)
		if index, exists := groups[key]; exists {
			assessments[index] = append(assessments[index], assessment)
			continue
		}
		next := citationsupport.Batch{Pairs: pairs, Claims: append(batch.Claims, indices)}
		if !citationsupport.FitsBatch(version, next) {
			continue
		}
		groups[key] = len(batch.Claims)
		batch = next
		assessments = append(assessments, []*citationsupport.Assessment{assessment})
	}
	return batch, assessments
}

func citationPairs(existing []citationsupport.Pair, claim string, evidence []int, excerpts []domain.ScoredChunk, limit int) (pairs []citationsupport.Pair, indices []int) {
	pairs = slices.Clone(existing)
	indices = make([]int, 0, min(len(evidence), limit))
	for _, number := range evidence {
		body := excerpts[number-1].Chunk.Text
		if strings.TrimSpace(body) == "" || len(claim)+len(body) > citationsupport.MaxBatchBytes {
			return nil, nil
		}
		pair := citationsupport.Pair{Claim: claim, Body: body}
		index := slices.Index(pairs, pair)
		if index < 0 {
			if len(pairs) == limit {
				return nil, nil
			}
			index = len(pairs)
			pairs = append(pairs, pair)
		}
		if !slices.Contains(indices, index) {
			indices = append(indices, index)
		}
	}
	slices.Sort(indices)
	return pairs, indices
}
