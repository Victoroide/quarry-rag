package pipeline

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
	"golang.org/x/text/width"
)

// stopWords are common English words that should not be treated as named entities
// even when capitalised (e.g. at the start of a sentence or title).
var stopWords = map[string]bool{
	"a": true, "an": true, "the": true, "and": true, "or": true,
	"for": true, "in": true, "on": true, "at": true, "to": true,
	"of": true, "with": true, "by": true, "from": true, "is": true,
	"are": true, "was": true, "were": true, "be": true, "been": true,
	"document": true, "reference": true, "guide": true, "report": true,
	"title": true, "section": true, "summary": true, "overview": true,
	"what": true, "how": true, "which": true, "who": true, "find": true, "list": true,

	"describe": true, "tell": true, "compare": true, "best": true,
}

// evidenceStopWords adds Spanish title/question words and folded variants for evidence routing.
var evidenceStopWords = map[string]bool{
	"el": true, "la": true, "los": true, "las": true, "un": true, "una": true,
	"para": true, "por": true, "con": true, "en": true, "sobre": true, "sin": true,
	"documento": true, "documentos": true, "referencia": true, "guia": true,
	"informe": true, "informes": true, "reporte": true, "titulo": true,
	"seccion": true, "resumen": true, "que": true, "cual": true, "cuales": true,
	"como": true, "quien": true, "donde": true, "cuando": true, "buscar": true,
	"listar": true, "compara": true, "comparar": true, "mejor": true,
}

func isEvidenceStopWord(word string) bool {
	clean := strings.Trim(word, ":;,.-!?¡¿\"'«»“”‘’()[]{}（）「」『』、。！？；：")
	clean = strings.TrimSuffix(strings.TrimSuffix(clean, "'s"), "’s")
	if isStopWord(clean) {
		return true
	}
	folded := foldLatinMarks(strings.ToLower(width.Fold.String(norm.NFC.String(clean))))
	return stopWords[folded] || evidenceStopWords[folded]
}

// isStopWord returns true if the lowercased word is a generic stop word.
func isStopWord(word string) bool {
	// Strip trailing punctuation before checking.
	clean := strings.TrimRight(strings.ToLower(word), ":;,.-!?")
	return stopWords[clean]
}

// ExtractNamedEntity finds a proper noun phrase with at least two capitalised,
// non-stop words so generic document titles are ignored.
func ExtractNamedEntity(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}

	words := strings.Fields(text)

	i := 0
	for i < len(words) {
		word := words[i]

		// Skip stop words and non-capitalised words.
		if isStopWord(word) || !isCapitalised(word) {
			i++
			continue
		}

		// Start collecting an entity phrase.
		var entityWords []string
		entityWords = append(entityWords, word)
		properWords := 1
		i++

		for i < len(words) {
			next := words[i]
			lower := strings.ToLower(next)
			// Allow name connectors (de, del, la, von).
			isConnector := lower == "de" || lower == "del" || lower == "la" || lower == "von"
			if isConnector || (isCapitalised(next) && !isStopWord(next)) {
				entityWords = append(entityWords, next)
				if !isConnector {
					properWords++
				}
				i++
			} else {
				break
			}
		}

		// Only accept phrases of 2+ non-connector words as proper entities.
		if properWords >= 2 {
			return strings.ToLower(strings.Join(entityWords, " "))
		}

		// Single capitalised word — skip, too ambiguous.
	}

	return ""
}

// isCapitalised returns true if the first rune of word is an uppercase letter.
func isCapitalised(word string) bool {
	if word == "" {
		return false
	}
	r, _ := utf8.DecodeRuneInString(word)
	return unicode.IsLetter(r) && unicode.IsUpper(r)
}

// MatchNamedEntity selects the strongest whole-word match in candidate order.
func MatchNamedEntity(query string, previews map[string]string, order []string) (entity, docID string) {
	queryTokens := entityTokens(query)
	bestClass, bestLength := 0, 0
	for _, id := range order {
		candidate := ExtractNamedEntity(previews[id])
		tokens := entityTokens(candidate)
		if len(tokens) == 0 {
			continue
		}
		matchClass, matchLength := 0, 0
		for start := 0; start+len(tokens) <= len(queryTokens); start++ {
			matches := true
			for i, token := range tokens {
				if queryTokens[start+i] != token {
					matches = false
					break
				}
			}
			if matches {
				matchClass = 2
				matchLength = utf8.RuneCountInString(candidate)
				break
			}
		}
		if matchClass == 0 {
			for _, token := range tokens {
				if utf8.RuneCountInString(token) <= 2 {
					continue
				}
				for _, word := range queryTokens {
					if word == token {
						matchClass = 1
						matchLength = max(matchLength, utf8.RuneCountInString(token))
					}
				}
			}
		}
		if matchClass > bestClass || (matchClass == bestClass && matchLength > bestLength) {
			entity, docID = candidate, id
			bestClass, bestLength = matchClass, matchLength
		}
	}
	return entity, docID
}

func entityTokens(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
}

// EntityMatch summarizes evidence-mode entity matching across visible candidate previews.
type EntityMatch struct {
	Entity          string
	DocumentID      string
	Ambiguous       bool
	Negated         bool
	MatchedDocs     int
	NegativeChecked bool
	VariantMatch    bool
}

type entityQueryToken struct {
	norm     string
	clauseID int
}

type docEntityCandidate struct {
	docID      string
	entity     string
	matchClass int // 3 = exact full phrase, 2 = variant or 2+ contiguous sub-phrase
	spanStart  int
	spanEnd    int
	tokenCount int
	runeLen    int
	variant    bool
}

var nameConnectors = map[string]bool{
	"de": true, "del": true, "la": true, "von": true, "van": true, "der": true,
}

// MatchNamedEntityEvidence evaluates named-entity matches over visible candidate previews,
// handling Unicode spelling variants, mixed scripts, named homonyms and entity exclusion.
func MatchNamedEntityEvidence(query string, previews map[string]string, order []string) EntityMatch {
	qTokens := tokenizeForEvidenceEntity(query)
	if len(qTokens) == 0 || len(order) == 0 || len(previews) == 0 {
		return EntityMatch{}
	}

	allPreviewsChecked := true
	var matched []docEntityCandidate
	seenDocs := make(map[string]struct{}, len(order))
	for _, id := range order {
		if _, dup := seenDocs[id]; dup {
			continue
		}
		seenDocs[id] = struct{}{}
		preview := previews[id]
		if strings.TrimSpace(preview) == "" {
			allPreviewsChecked = false
			continue
		}
		phrases := extractNamedEntityPhrases(preview)
		if len(phrases) == 0 {
			continue
		}
		for _, phrase := range phrases {
			hits, excluded := matchPhraseInQuery(id, phrase, qTokens)
			if excluded {
				return EntityMatch{Negated: true}
			}
			matched = append(matched, hits...)
		}
	}

	if len(matched) == 0 {
		return EntityMatch{NegativeChecked: allPreviewsChecked}
	}

	// Longer or more exact matches can suppress another name only at
	// the same query location. Retain same-span ties and independent mentions.
	sort.SliceStable(matched, func(i, j int) bool {
		a, b := matched[i], matched[j]
		if a.spanStart != b.spanStart {
			return a.spanStart < b.spanStart
		}
		if a.spanEnd != b.spanEnd {
			return a.spanEnd > b.spanEnd
		}
		return a.matchClass > b.matchClass
	})
	var furthest [4]docEntityCandidate
	byDoc := make(map[string]docEntityCandidate)
	for _, hit := range matched {
		subsumed := false
		for class := hit.matchClass; class < len(furthest); class++ {
			other := furthest[class]
			if other.spanEnd > hit.spanEnd || other.spanEnd == hit.spanEnd &&
				(other.spanStart < hit.spanStart || class > hit.matchClass) {
				subsumed = true
				break
			}
		}
		if subsumed {
			continue
		}
		if hit.spanEnd > furthest[hit.matchClass].spanEnd {
			furthest[hit.matchClass] = hit
		}
		if best, exists := byDoc[hit.docID]; !exists || betterEntityCandidate(hit, best) {
			byDoc[hit.docID] = hit
		}
	}
	if len(byDoc) > 1 {
		return EntityMatch{Ambiguous: true, MatchedDocs: len(byDoc)}
	}
	for _, hit := range byDoc {
		return EntityMatch{Entity: hit.entity, DocumentID: hit.docID, MatchedDocs: 1, VariantMatch: hit.variant}
	}
	return EntityMatch{NegativeChecked: allPreviewsChecked}
}

func betterEntityCandidate(a, b docEntityCandidate) bool {
	if a.matchClass != b.matchClass {
		return a.matchClass > b.matchClass
	}
	if a.tokenCount != b.tokenCount {
		return a.tokenCount > b.tokenCount
	}
	return a.runeLen > b.runeLen
}

func matchPhraseInQuery(docID, phrase string, qTokens []entityQueryToken) ([]docEntityCandidate, bool) {
	pRaw := tokenizeForEvidenceEntity(phrase)
	if len(pRaw) < 2 {
		return nil, false
	}
	pTokens := make([]string, len(pRaw))
	for i, tok := range pRaw {
		pTokens[i] = tok.norm
	}

	var hits []docEntityCandidate
	negated := false
	trySpan := func(start, length, class int, variant bool, label string) {
		end := start + length
		clause := qTokens[start].clauseID
		for k := start + 1; k < end; k++ {
			if qTokens[k].clauseID != clause {
				return
			}
		}
		if isEntityMatchNegated(qTokens, start, length) {
			negated = true
			return
		}
		cand := docEntityCandidate{
			docID:      docID,
			entity:     label,
			matchClass: class,
			spanStart:  start,
			spanEnd:    end,
			tokenCount: length,
			runeLen:    utf8.RuneCountInString(label),
			variant:    variant,
		}
		hits = append(hits, cand)
	}

	// Full phrase match (exact or 1-variant on 2+ token phrases with at least 1 exact token).
	for start := 0; start+len(pTokens) <= len(qTokens); start++ {
		exactCount, variantCount := 0, 0
		allMatched := true
		for i, pt := range pTokens {
			matched, isVar := tokensMatchOrVariant(qTokens[start+i].norm, pt)
			if !matched {
				allMatched = false
				break
			}
			if isVar {
				variantCount++
			} else {
				exactCount++
			}
		}
		if allMatched && exactCount >= 1 && variantCount <= 1 {
			class := 3
			if variantCount > 0 {
				class = 2
			}
			trySpan(start, len(pTokens), class, variantCount > 0, phrase)
		}
	}

	// Contiguous 2+ proper-token sub-phrase match for multi-word titles like "Cedar Archive North".
	if len(pTokens) >= 3 {
		for subStart := 0; subStart+2 <= len(pTokens); subStart++ {
			for subEnd := len(pTokens); subEnd >= subStart+2; subEnd-- {
				if subStart == 0 && subEnd == len(pTokens) {
					continue
				}
				sub := pTokens[subStart:subEnd]
				if isNumericComponent(sub[0]) {
					continue
				}
				proper := 0
				for _, tok := range sub {
					if !nameConnectors[tok] && !evidenceStopWords[tok] && !stopWords[tok] {
						proper++
					}
				}
				if proper < 2 {
					continue
				}
				for start := 0; start+len(sub) <= len(qTokens); start++ {
					allExact := true
					for i, pt := range sub {
						if qTokens[start+i].norm != pt {
							allExact = false
							break
						}
					}
					if allExact && !conflictingTitleNumber(qTokens, pTokens, start, subStart, len(sub)) {
						trySpan(start, len(sub), 2, false, strings.Join(sub, " "))
					}
				}
			}
		}
	}

	return hits, negated
}

// conflictingTitleNumber checks both ends of an exact sub-phrase. A shorter
// match cannot bypass an explicit number in the surrounding title words.
func conflictingTitleNumber(query []entityQueryToken, title []string, qStart, tStart, length int) bool {
	clause := query[qStart].clauseID
	scan := func(q, t, step int) bool {
		for q >= 0 && q < len(query) && t >= 0 && t < len(title) && query[q].clauseID == clause {
			if query[q].norm != title[t] {
				return isNumericComponent(query[q].norm) && isNumericComponent(title[t])
			}
			q += step
			t += step
		}
		return false
	}
	return scan(qStart-1, tStart-1, -1) || scan(qStart+length, tStart+length, 1)
}

// exclusionScope describes how far an exclusion marker reaches toward the
// name that follows it.
type exclusionScope int

const (
	// scopeObject markers govern their object phrase, including noun
	// modifiers attached with of, for, from or about.
	scopeObject exclusionScope = iota + 1
	// scopeComplement markers govern a complement that ends at the first
	// preposition: an entry without a badge permitted at Cedar Archive
	// excludes the badge.
	scopeComplement
	// scopeParticiple markers name the excluded item before them. A name that
	// follows the participle is governed only when nothing separates them, so
	// the source in "excluded from Cedar Archive" stays included.
	scopeParticiple
)

var exclusionMarkers = map[string]exclusionScope{
	"neither": scopeObject, "nor": scopeObject, "ni": scopeObject, "tampoco": scopeObject,
	"ninguno": scopeObject, "ninguna": scopeObject, "besides": scopeObject,
	"except": scopeObject, "excepting": scopeObject, "excepto": scopeObject, "salvo": scopeObject,
	"excluding": scopeObject, "exclude": scopeObject, "excludes": scopeObject,
	"excluyendo": scopeObject, "excluir": scopeObject, "excluye": scopeObject,
	"omit": scopeObject, "omitting": scopeObject, "ignore": scopeObject, "ignoring": scopeObject,
	"skip": scopeObject, "skipping": scopeObject,
	"without": scopeComplement, "sin": scopeComplement,
	"excluded": scopeParticiple,
}

// exclusionPhrases are two-word markers keyed by their normalized tokens.
var exclusionPhrases = map[string]bool{
	"other than": true, "apart from": true, "instead of": true, "rather than": true,
	"fuera de": true, "en lugar": true,
}

// objectLinkers attach a noun modifier, such as a source or a location, to the
// noun before them. An exclusion reaches through them; a complement ends at them.
var objectLinkers = map[string]bool{
	"of": true, "for": true, "from": true, "about": true, "in": true, "at": true, "on": true, "within": true,
	"de": true, "del": true, "para": true, "sobre": true, "en": true,
}

// objectBoundaries begin a new constituent: prepositions that attach to a verb,
// auxiliaries, copulas and subordinators. An exclusion does not reach past them.
var objectBoundaries = map[string]bool{
	"to": true, "into": true, "onto": true, "with": true,
	"by": true, "as": true, "per": true, "via": true, "under": true,
	"over": true, "during": true, "after": true, "before": true, "between": true,
	"among": true, "through": true, "across": true, "against": true, "toward": true,
	"towards": true, "is": true, "are": true, "was": true, "were": true, "am": true,
	"be": true, "been": true, "being": true, "do": true, "does": true, "did": true,
	"can": true, "could": true, "will": true, "would": true, "shall": true,
	"should": true, "may": true, "might": true, "must": true, "has": true,
	"have": true, "had": true, "that": true, "which": true, "who": true, "whom": true,
	"whose": true, "where": true, "when": true, "while": true, "if": true,
	"because": true, "since": true, "whether": true, "then": true,
	"con": true, "por": true, "al": true, "ante": true, "bajo": true,
	"entre": true, "hacia": true, "hasta": true, "desde": true, "durante": true,
	"tras": true, "es": true, "son": true, "era": true, "eran": true, "fue": true,
	"fueron": true, "ser": true, "esta": true, "estan": true, "puede": true,
	"pueden": true, "debe": true, "deben": true, "tiene": true, "tienen": true,
	"hay": true, "que": true, "quien": true, "donde": true, "cuando": true,
	"si": true, "porque": true, "como": true, "pues": true,
}

// transparentTokens are determiners and inclusion words that do not separate an
// exclusion marker from the name it governs.
var transparentTokens = map[string]bool{
	"a": true, "an": true, "the": true, "any": true, "all": true, "every": true,
	"each": true, "some": true, "including": true, "include": true, "includes": true,
	"el": true, "la": true, "los": true, "las": true, "un": true, "una": true,
	"unos": true, "unas": true, "todo": true, "toda": true, "todos": true,
	"todas": true, "cualquier": true, "incluyendo": true, "incluir": true, "incluye": true,
}

var exclusionParticiples = map[string]bool{
	"excluded": true, "omitted": true, "excluido": true, "excluida": true,
	"omitido": true, "omitida": true,
}

type inclusionForm int

const (
	inclusionFinite inclusionForm = iota + 1
	inclusionParticiple
)

// inclusionVerbs introduce a named object that the request wants kept. A
// negator in the same predicate makes the request an exclusion. A base or
// finite form also starts a new constituent, so an earlier exclusion marker
// does not reach a name that a later inclusion verb governs, unless the verb
// is the marker's own object, as in "sin incluir". The gerund forms attach to
// the marker before them.
var inclusionVerbs = map[string]inclusionForm{
	"include": inclusionFinite, "includes": inclusionFinite,
	"incluir": inclusionFinite, "incluye": inclusionFinite, "incluya": inclusionFinite,
	"incluyas": inclusionFinite, "incluyan": inclusionFinite,
	"including": inclusionParticiple, "incluyendo": inclusionParticiple,
}

func isNegator(token string) bool {
	return token == "not" || token == "no" || token == "never"
}

func isPredicateModifier(word string) bool {
	return word == "also" || word == "already" || word == "ever" || strings.HasSuffix(word, "ly")
}

// startsNotOnly recognizes the affirmative opening of "not only ... but ...".
func startsNotOnly(tokens []entityQueryToken, i int) bool {
	return i+1 < len(tokens) && tokens[i].norm == "not" &&
		tokens[i+1].clauseID == tokens[i].clauseID && tokens[i+1].norm == "only"
}

// followsExclusionMarker reports whether the token at i directly follows an
// exclusion marker of the same clause.
func followsExclusionMarker(tokens []entityQueryToken, i int) bool {
	if i == 0 || tokens[i-1].clauseID != tokens[i].clauseID {
		return false
	}
	if exclusionMarkers[tokens[i-1].norm] != 0 {
		return true
	}
	return i > 1 && tokens[i-2].clauseID == tokens[i].clauseID && exclusionPhrases[tokens[i-2].norm+" "+tokens[i-1].norm]
}

func exclusionIsNegated(tokens []entityQueryToken, start int) bool {
	for i := start - 1; i >= 0 && tokens[i].clauseID == tokens[start].clauseID; i-- {
		word := tokens[i].norm
		if isNegator(word) {
			return !startsNotOnly(tokens, i)
		}
		if !isPredicateModifier(word) {
			break
		}
	}
	return false
}

var perfectAuxiliaries = map[string]bool{
	"has": true, "have": true, "had": true, "ha": true, "han": true, "habia": true, "habian": true,
}

var modalAuxiliaries = map[string]bool{
	"can": true, "could": true, "will": true, "would": true, "shall": true,
	"should": true, "may": true, "might": true, "must": true,
}

var passiveAuxiliaries = map[string]bool{
	"am": true, "is": true, "are": true, "was": true, "were": true,
	"es": true, "son": true, "era": true, "eran": true, "fue": true, "fueron": true,
	"esta": true, "estan": true, "estaba": true, "estaban": true,
}

// trailingEntityExclusion checks whether a following participle governs the
// name. A perfect auxiliary by itself marks an active subject, even when the
// auxiliary comes before the subject in a question.
func trailingEntityExclusion(tokens []entityQueryToken, start, end int) (excluded, negatorBeforeSubject bool) {
	clause := tokens[start].clauseID
	token := func(i int) string {
		if i < 0 || i >= len(tokens) || tokens[i].clauseID != clause {
			return ""
		}
		return tokens[i].norm
	}
	isAuxiliary := func(word string) bool {
		return passiveAuxiliaries[word] || perfectAuxiliaries[word] || modalAuxiliaries[word]
	}
	before := start - 1
	if token(before) == "the" {
		before--
	}
	preposedNegator := isNegator(token(before))
	if preposedNegator {
		before--
	}
	aux := token(before)
	inverted := isAuxiliary(aux)
	i := end
	if !inverted {
		aux = token(i)
		if isAuxiliary(aux) {
			i++
		}
	}
	negative := inverted && preposedNegator
	// Modifiers can appear on either side of a secondary auxiliary. Keep
	// scanning within the clause and remember negation for the participle.
	skipModifiers := func() {
		for {
			word := token(i)
			switch {
			case word == "not" && startsNotOnly(tokens, i):
				i++
			case isNegator(word):
				negative = true
			case isPredicateModifier(word):
			default:
				return
			}
			i++
		}
	}
	skipModifiers()

	passive := passiveAuxiliaries[aux]
	switch token(i) {
	case "been", "sido", "estado":
		passive = perfectAuxiliaries[aux]
		i++
	case "be":
		passive = modalAuxiliaries[aux]
		i++
	case "being", "siendo", "estando":
		i++
	}
	skipModifiers()

	participle := token(i)
	exclusion := exclusionParticiples[participle]
	inclusion := participle == "included" || participle == "incluido" || participle == "incluida"
	if !exclusion && !inclusion {
		return false, false
	}
	// A clause-initial name can head a reduced passive, as in
	// "Cedar Archive excluded from the schedule".
	if !isAuxiliary(aux) && token(before) == "" && !preposedNegator {
		link := token(i + 1)
		passive = exclusion && (link == "from" || link == "by" || link == "de" || link == "por") ||
			inclusion && (link == "in" || link == "en")
	}
	return passive && exclusion != negative, inverted && preposedNegator
}

// isEntityMatchNegated reports whether an exclusion governs the name span.
// A marker governs the name only through its own object phrase, so a marker
// elsewhere in the clause does not discard a name it does not govern.
func isEntityMatchNegated(qTokens []entityQueryToken, start, length int) bool {
	excluded, negatorBeforeSubject := trailingEntityExclusion(qTokens, start, start+length)
	if excluded {
		return true
	}
	clause := qTokens[start].clauseID
	linked, adjacent := false, true
	for i := start - 1; i >= 0 && qTokens[i].clauseID == clause; i-- {
		tok := qTokens[i].norm
		if !negatorBeforeSubject && isNegator(tok) &&
			(i == start-1 || i == start-2 && (qTokens[i+1].norm == "the" || qTokens[i+1].norm == "el" || qTokens[i+1].norm == "la")) {
			return true
		}
		if form := inclusionVerbs[tok]; form != 0 {
			if exclusionIsNegated(qTokens, i) {
				return true
			}
			if form == inclusionFinite && !followsExclusionMarker(qTokens, i) {
				break
			}
		}
		if i > 0 && qTokens[i-1].clauseID == clause && exclusionPhrases[qTokens[i-1].norm+" "+tok] {
			if !exclusionIsNegated(qTokens, i-1) {
				return true
			}
			break
		}
		scope := exclusionMarkers[tok]
		if scope == scopeObject || scope == scopeComplement && !linked || scope == scopeParticiple && adjacent {
			if !exclusionIsNegated(qTokens, i) {
				return true
			}
			break
		}
		if objectBoundaries[tok] {
			break
		}
		linked = linked || objectLinkers[tok]
		adjacent = adjacent && transparentTokens[tok]
	}
	return false
}

func extractNamedEntityPhrases(text string) []string {
	const previewCharacters = 120
	seenCharacters := 0
	for offset := range text {
		if seenCharacters == previewCharacters {
			text = text[:offset]
			break
		}
		seenCharacters++
	}
	cleaned := stripFormatChars(width.Fold.String(norm.NFC.String(text)))
	segments := strings.FieldsFunc(cleaned, func(r rune) bool {
		return strings.ContainsRune(".,;:!?。！？；：()[]{}（）「」『』【】\"«»“”/|\n\r\t—–", r)
	})
	var phrases []string
	seen := make(map[string]struct{})
	for _, seg := range segments {
		words := strings.Fields(strings.Join(splitByScriptRuns(seg), " "))
		i := 0
		for i < len(words) {
			word := trimSurroundingPunct(words[i])
			if word == "" || isEvidenceStopWord(word) || !isEvidenceNameWord(word) {
				i++
				continue
			}
			entityWords := []string{cleanEntityWord(word)}
			properWords := 1
			numberAllowed := true
			i++
			for i < len(words) {
				next := trimSurroundingPunct(words[i])
				if next == "" {
					i++
					continue
				}
				lower := foldLatinMarks(strings.ToLower(next))
				isConn := nameConnectors[lower]
				isNumber := numberAllowed && isNumericComponent(next)
				if isConn || isNumber || (isEvidenceNameWord(next) && !isEvidenceStopWord(next)) {
					entityWords = append(entityWords, cleanEntityWord(next))
					if !isConn {
						properWords++
					}
					// A number completes the title word before it but never
					// follows a connector or another number.
					numberAllowed = !isConn && !isNumber
					i++
				} else {
					break
				}
			}
			if properWords >= 2 {
				phrase := strings.ToLower(strings.Join(entityWords, " "))
				if _, ok := seen[phrase]; !ok {
					phrases = append(phrases, phrase)
					seen[phrase] = struct{}{}
				}
			}
		}
	}
	return phrases
}

// isNumericComponent reports whether a title word is a number or numbered
// designator such as 11 or 2B. It can complete a title but never begin one.
func isNumericComponent(word string) bool {
	r, _ := utf8.DecodeRuneInString(cleanEntityWord(word))
	return unicode.IsDigit(r)
}

func isEvidenceNameWord(word string) bool {
	r, _ := utf8.DecodeRuneInString(word)
	return isCapitalised(word) || unicode.IsLetter(r) && isDenseScriptRune(r)
}

func cleanEntityWord(word string) string {
	word = strings.TrimSuffix(strings.TrimSuffix(word, "'s"), "’s")
	return word
}

func trimSurroundingPunct(word string) string {
	return strings.TrimFunc(word, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

func splitByScriptRuns(s string) []string {
	var parts []string
	var cur strings.Builder
	prevDense := false
	hasPrev := false
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !unicode.IsMark(r) {
			cur.WriteRune(r)
			continue
		}
		dense := isDenseScriptRune(r)
		if hasPrev && !unicode.IsDigit(r) && !unicode.In(r, unicode.Common, unicode.Inherited) && dense != prevDense {
			if cur.Len() > 0 {
				parts = append(parts, cur.String())
				cur.Reset()
			}
		}
		if !unicode.In(r, unicode.Common, unicode.Inherited) {
			prevDense = dense
			hasPrev = true
		}
		cur.WriteRune(r)
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}

// contractionStems restores the stem of the irregular English negative
// contractions: can't, won't, shan't and ain't.
var contractionStems = map[string]string{"ca": "can", "wo": "will", "sha": "shall", "ai": "be"}

// isApostrophe reports straight and typographic apostrophes, and the marks
// people type in their place.
func isApostrophe(r rune) bool {
	switch r {
	case '\'', '\u2019', '\u2018', '\u02bc', '\u2032', '\u00b4', '`':
		return true
	}
	return false
}

// negativeContractionAt reports whether an apostrophe after word closes the
// English contraction n't: a Latin stem ending in n, then t as a whole word.
func negativeContractionAt(word, rest []rune) bool {
	if len(word) < 2 || len(rest) == 0 || unicode.ToLower(rest[0]) != 't' || unicode.ToLower(word[len(word)-1]) != 'n' {
		return false
	}
	if len(rest) > 1 && (unicode.IsLetter(rest[1]) || unicode.IsDigit(rest[1]) || unicode.IsMark(rest[1])) {
		return false
	}
	for _, r := range word {
		if !unicode.Is(unicode.Latin, r) {
			return false
		}
	}
	return true
}

// tokenizeForEvidenceEntity splits text into folded tokens with clause numbers.
// English negative contractions become the stem and not, so that "don't" and
// "do not" reach the polarity checks as the same tokens.
func tokenizeForEvidenceEntity(text string) []entityQueryToken {
	cleaned := []rune(stripFormatChars(width.Fold.String(norm.NFC.String(text))))
	var tokens []entityQueryToken
	var run []rune
	clauseID := 0
	prevGroup := 0 // 1 = dense, 2 = alphabetic/numeric
	emit := func(tok string) {
		if tok == "cannot" {
			tokens = append(tokens, entityQueryToken{norm: "can", clauseID: clauseID})
			tok = "not"
		}
		tokens = append(tokens, entityQueryToken{norm: tok, clauseID: clauseID})
	}
	flush := func() {
		if len(run) == 0 {
			return
		}
		if tok := foldLatinMarks(strings.ToLower(string(run))); tok != "" {
			emit(tok)
		}
		run = run[:0]
		prevGroup = 0
	}
	for i := 0; i < len(cleaned); i++ {
		r := cleaned[i]
		if isApostrophe(r) && negativeContractionAt(run, cleaned[i+1:]) {
			stem := strings.ToLower(string(run[:len(run)-1]))
			if full, irregular := contractionStems[stem]; irregular {
				stem = full
			}
			run = append(run[:0], []rune(stem)...)
			flush()
			emit("not")
			i++
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r) {
			group := 2
			if isDenseScriptRune(r) {
				group = 1
			} else if prevGroup != 0 && (unicode.IsMark(r) || unicode.In(r, unicode.Common, unicode.Inherited)) {
				group = prevGroup
			}
			if prevGroup != 0 && prevGroup != group {
				flush()
			}
			prevGroup = group
			run = append(run, r)
			continue
		}
		flush()
		if strings.ContainsRune(".,;:!?。！？；：()[]{}（）「」『』—–/|\n\r", r) {
			clauseID++
		}
	}
	flush()
	return tokens
}

func isDenseScriptRune(r rune) bool {
	return unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul,
		unicode.Thai, unicode.Lao, unicode.Khmer, unicode.Myanmar, unicode.Tibetan)
}

func stripFormatChars(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if unicode.Is(unicode.Cf, r) {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// foldLatinMarks removes non-spacing combining marks attached to Latin base letters
// while preserving combining marks on non-Latin scripts such as Japanese dakuten.
func foldLatinMarks(s string) string {
	decomposed := norm.NFD.String(s)
	var out []rune
	lastBaseLatin := false
	for _, r := range decomposed {
		if unicode.Is(unicode.Mn, r) {
			if lastBaseLatin {
				continue
			}
			out = append(out, r)
			continue
		}
		lastBaseLatin = unicode.Is(unicode.Latin, r)
		out = append(out, r)
	}
	return norm.NFC.String(string(out))
}

func tokensMatchOrVariant(a, b string) (matched, variant bool) {
	if a == b {
		return true, false
	}
	// A different number names a different title, so numbers match exactly.
	if strings.ContainsFunc(a, unicode.IsDigit) || strings.ContainsFunc(b, unicode.IsDigit) {
		return false, false
	}
	ra, rb := []rune(a), []rune(b)
	if len(ra) < 5 || len(rb) < 5 {
		return false, false
	}
	if editDistanceAtMostOne(ra, rb) {
		return true, true
	}
	if canonicalSpellingVariant(a) == canonicalSpellingVariant(b) {
		return true, true
	}
	return false, false
}

func canonicalSpellingVariant(tok string) string {
	if strings.HasSuffix(tok, "ogue") && len(tok) > 5 {
		return strings.TrimSuffix(tok, "ue")
	}
	if strings.HasSuffix(tok, "our") && len(tok) > 5 {
		return strings.TrimSuffix(tok, "our") + "or"
	}
	if strings.HasSuffix(tok, "tre") && len(tok) > 5 {
		return strings.TrimSuffix(tok, "tre") + "ter"
	}
	if strings.HasSuffix(tok, "ise") && len(tok) > 5 {
		return strings.TrimSuffix(tok, "ise") + "ize"
	}
	return tok
}

func editDistanceAtMostOne(a, b []rune) bool {
	la, lb := len(a), len(b)
	if la-lb > 1 || lb-la > 1 {
		return false
	}
	if la == lb {
		diffs := 0
		for i := range a {
			if a[i] != b[i] {
				diffs++
				if diffs > 1 {
					return false
				}
			}
		}
		return diffs == 1
	}
	if la > lb {
		a, b = b, a
	}
	i, j, skipped := 0, 0, false
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			i++
			j++
			continue
		}
		if skipped {
			return false
		}
		skipped = true
		j++
	}
	return true
}
