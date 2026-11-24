package pipeline

import (
	"cmp"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/text/language"
)

type noEvidenceMessage struct {
	tag    canonicalTag
	answer string
}

type builtinNoEvidenceMessage struct {
	tag    language.Tag
	answer string
}

var noEvidenceMessages = [...]builtinNoEvidenceMessage{
	{language.English, "No relevant information was found to answer this question."},
	{language.Spanish, "No se encontró información relevante para responder esta pregunta."},
	{language.Portuguese, "Não foi encontrada informação relevante para responder a esta pergunta."},
	{language.French, "Aucune information pertinente n'a été trouvée pour répondre à cette question."},
	{language.German, "Es wurden keine relevanten Informationen gefunden, um diese Frage zu beantworten."},
	{language.Italian, "Non sono state trovate informazioni pertinenti per rispondere a questa domanda."},
}

// NoEvidenceCatalog holds fixed translations and a matcher shared by queries.
// Its zero value uses the built-in translations. The matcher compares only the
// translations whose tags the language package represents; any other
// translation matches its exact tag.
type NoEvidenceCatalog struct {
	messages []noEvidenceMessage
	matcher  language.Matcher
	matched  []int // message index for each matcher tag
	exact    map[string]int
}

var builtinNoEvidenceCatalog = sync.OnceValue(func() *NoEvidenceCatalog {
	return newNoEvidenceCatalog(nil)
})

func newNoEvidenceCatalog(additions []noEvidenceMessage) *NoEvidenceCatalog {
	messages := make([]noEvidenceMessage, 0, len(noEvidenceMessages)+len(additions))
	for _, message := range noEvidenceMessages {
		messages = append(messages, noEvidenceMessage{packageTag(message.tag, nil), message.answer})
	}
	messages = slices.Concat(messages, additions)
	var tags []language.Tag
	var matched []int
	exact := make(map[string]int, len(messages))
	for i, message := range messages {
		if message.tag.known {
			tags = append(tags, message.tag.tag)
			matched = append(matched, i)
		}
		exact[strings.ToLower(message.tag.text)] = i
	}
	return &NoEvidenceCatalog{messages: messages, matcher: language.NewMatcher(tags, language.PreferSameScript(false)), matched: matched, exact: exact}
}

// maxAcceptLanguageBytes bounds header parsing; longer values use English.
const maxAcceptLanguageBytes = 1024

func noEvidenceAnswer(acceptLanguage string) (answer, tag string) {
	return builtinNoEvidenceCatalog().answer(acceptLanguage)
}

func (c *NoEvidenceCatalog) answer(header string) (answer, tag string) {
	if c == nil || c.matcher == nil {
		c = builtinNoEvidenceCatalog()
	}
	var index int
	if len(header) <= maxAcceptLanguageBytes {
		index = c.match(noEvidencePreferences(header))
	}
	message := c.messages[index]
	return message.answer, message.tag.text
}

func (c *NoEvidenceCatalog) match(preferences []languagePreference) int {
	var approximate []language.Tag
	for _, preference := range preferences {
		index, tag, matched := c.lookup(preference.languageRange)
		if matched {
			return index
		}
		// A private-use-only tag has no language, script or region to compare,
		// and a tag with an unknown language or script has no matcher form.
		if !tag.hasLocale {
			continue
		}
		// Preserve script and dialect comparisons before falling back to a base
		// translation. Related languages are considered after explicit matches.
		_, matcherIndex, confidence := c.matcher.Match(tag.locale)
		index = c.matched[matcherIndex]
		base, _, _ := tag.locale.Raw()
		catalogBase, _, _ := c.messages[index].tag.locale.Raw()
		if confidence != language.No && base == catalogBase {
			return index
		}
		approximate = append(approximate, tag.locale)
	}
	_, matcherIndex, _ := c.matcher.Match(approximate...)
	return c.matched[matcherIndex]
}

// Lookup truncates the requested range before canonicalizing each candidate.
// The matcher handles script and region comparisons using the closest locale.
func (c *NoEvidenceCatalog) lookup(raw string) (int, canonicalTag, bool) {
	raw = strings.ToLower(raw)
	floor := languageRangeLookupFloor(raw)
	var closest canonicalTag
	for candidate := raw; candidate != "" && len(candidate) >= floor; candidate = parentLanguageRange(candidate) {
		tag, err := canonicalNoEvidenceTag(candidate)
		if err != nil {
			continue
		}
		if index, ok := c.exact[strings.ToLower(tag.text)]; ok {
			return index, tag, true
		}
		if closest.text == "" {
			closest = tag
		}
	}
	return 0, closest, false
}

type languagePreference struct {
	languageRange string
	weight        float64
}

var languageWeight = regexp.MustCompile(`^[qQ]=(?:0(?:\.\d{0,3})?|1(?:\.0{0,3})?)$`)

func noEvidencePreferences(header string) []languagePreference {
	var preferences []languagePreference
	for item := range strings.SplitSeq(header, ",") {
		item = strings.Trim(item, " \t")
		if item == "" {
			continue
		}
		raw, parameter, weighted := strings.Cut(item, ";")
		languageRange := strings.TrimRight(raw, " \t")
		if languageRange != "*" && !basicLanguageRange.MatchString(languageRange) {
			return nil
		}
		weight := 1.0
		if weighted {
			parameter = strings.TrimLeft(parameter, " \t")
			if !languageWeight.MatchString(parameter) {
				return nil
			}
			var err error
			weight, err = strconv.ParseFloat(parameter[2:], 64)
			if err != nil {
				return nil
			}
		}
		// A wildcard contributes no preference to lookup. An explicit "mul"
		// range can still match a translation cataloged under that tag.
		if languageRange != "*" && weight > 0 {
			preferences = append(preferences, languagePreference{languageRange, weight})
		}
	}
	slices.SortStableFunc(preferences, func(a, b languagePreference) int { return cmp.Compare(b.weight, a.weight) })
	return preferences
}
