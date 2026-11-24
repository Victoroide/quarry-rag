package pipeline

import (
	"errors"
	"regexp"
	"slices"
	"strings"

	"golang.org/x/text/language"
)

// RFC 4647 basic language ranges have fewer restrictions than catalog tags.
var basicLanguageRange = regexp.MustCompile(`^[A-Za-z]{1,8}(?:-[A-Za-z0-9]{1,8})*$`)

const scriptAndRegion = `(?:-[a-z]{4})?(?:-(?:[a-z]{2}|\d{3}))?`

// RFC 5646 syntax with a capture for each component. The grandfathered list
// is fixed.
var (
	languageTagSyntax = regexp.MustCompile(`^(?:(?:(?P<language>[a-z]{2,3})(?:-(?P<extlang>[a-z]{3}))?|(?P<reserved>[a-z]{5,8}))` +
		`(?:-(?P<script>[a-z]{4}))?(?:-(?P<region>[a-z]{2}|\d{3}))?` +
		`(?P<variants>(?:-(?:[a-z0-9]{5,8}|\d[a-z0-9]{3}))*)(?P<extensions>(?:-[0-9a-wy-z](?:-[a-z0-9]{2,8})+)*)` +
		`(?P<private>-x(?:-[a-z0-9]{1,8})+)?|(?P<privateOnly>x(?:-[a-z0-9]{1,8})+))$`)
	grandfatheredLanguageTag = regexp.MustCompile(`^(?:art-lojban|cel-gaulish|en-gb-oed|` +
		`i-(?:ami|bnn|default|enochian|hak|klingon|lux|mingo|navajo|pwn|tao|tay|tsu)|` +
		`no-(?:bok|nyn)|sgn-(?:be-fr|be-nl|ch-de)|zh-(?:guoyu|hakka|min(?:-nan)?|xiang))$`)
	languageRangeCore = regexp.MustCompile(`^[a-z]{1,8}(?:-[a-z]{3})*` + scriptAndRegion + `(?:-|$)`)
)

var (
	errInvalidLanguageTag       = errors.New("unregistered or repeated subtag")
	errInvalidLanguageExtension = errors.New("invalid language tag extension")
)

// languageTag holds the components of a lowercase RFC 5646 tag.
type languageTag struct {
	language, extlang, script, region string
	variants                          []string
	extensions                        []string // sequences such as "u-nu-latn"
	private                           string   // "x-..." or empty
}

func parseLanguageTag(tag string) (languageTag, bool) {
	match := languageTagSyntax.FindStringSubmatch(tag)
	if match == nil {
		return languageTag{}, false
	}
	group := func(name string) string { return match[languageTagSyntax.SubexpIndex(name)] }
	parts := languageTag{
		language: group("language") + group("reserved"), extlang: group("extlang"),
		script: group("script"), region: group("region"),
		private: strings.TrimPrefix(group("private"), "-") + group("privateOnly"),
	}
	if variants := group("variants"); variants != "" {
		parts.variants = strings.Split(variants[1:], "-")
	}
	for subtag := range strings.SplitSeq(strings.TrimPrefix(group("extensions"), "-"), "-") {
		switch {
		case subtag == "":
		case len(subtag) == 1:
			parts.extensions = append(parts.extensions, subtag)
		default:
			parts.extensions[len(parts.extensions)-1] += "-" + subtag
		}
	}
	return parts, true
}

// core returns the language, script and region of a canonical tag.
func (t languageTag) core() string {
	return strings.Join(slices.DeleteFunc([]string{t.language, t.script, t.region}, func(s string) bool { return s == "" }), "-")
}

func (t languageTag) String() string {
	if t.language == "" {
		return t.private
	}
	subtags := []string{t.language}
	if t.extlang != "" {
		subtags = append(subtags, t.extlang)
	}
	if t.script != "" {
		subtags = append(subtags, strings.ToUpper(t.script[:1])+t.script[1:])
	}
	if t.region != "" {
		subtags = append(subtags, strings.ToUpper(t.region))
	}
	subtags = append(subtags, t.variants...)
	subtags = append(subtags, t.extensions...)
	if t.private != "" {
		subtags = append(subtags, t.private)
	}
	return strings.Join(subtags, "-")
}

func (t languageTag) usesUnmatchedSubtag() bool {
	return unmatchedLanguages[t.language] || unmatchedScripts[t.script] || slices.ContainsFunc(t.variants, func(v string) bool { return unmatchedVariants[v] })
}

func validCatalogLanguageTag(raw string) bool {
	if !basicLanguageRange.MatchString(raw) {
		return false
	}
	tag := strings.ToLower(raw)
	if grandfatheredLanguageTag.MatchString(tag) {
		return true
	}
	parts, ok := parseLanguageTag(tag)
	if !ok || (parts.extlang != "" && !registeredExtendedLanguage(parts.language, parts.extlang)) {
		return false
	}
	return !repeats(parts.variants) && !repeats(singletons(parts.extensions))
}

func singletons(extensions []string) []string {
	return slices.Collect(func(yield func(string) bool) {
		for _, extension := range extensions {
			if !yield(extension[:1]) {
				return
			}
		}
	})
}

func repeats(subtags []string) bool {
	seen := make(map[string]bool, len(subtags))
	for _, subtag := range subtags {
		if seen[subtag] {
			return true
		}
		seen[subtag] = true
	}
	return false
}

// canonicalTag is a language tag in canonical form. A tag that uses subtags the
// pinned language package does not know is carried as text and matched exactly.
type canonicalTag struct {
	text      string       // canonical spelling, used for Content-Language and exact lookup
	tag       language.Tag // text as a package tag; valid when known
	known     bool         // the package represents every subtag of text
	locale    language.Tag // core as a package tag; valid when hasLocale
	hasLocale bool
}

// The lookup floor preserves the requested language, script and region.
// A private-use-only range has no regular subtags to preserve.
func languageRangeLookupFloor(raw string) int {
	parts, ok := parseLanguageTag(raw)
	if !ok {
		// Basic ranges can include prefixes that are not complete language tags.
		return len(strings.TrimSuffix(languageRangeCore.FindString(raw), "-"))
	}
	floor := len(parts.language)
	for _, subtag := range [...]string{parts.extlang, parts.script, parts.region} {
		if subtag != "" {
			floor += 1 + len(subtag)
		}
	}
	return floor
}

// RFC 4647 removes a trailing singleton together with its last subtag.
func parentLanguageRange(tag string) string {
	end := strings.LastIndexByte(tag, '-')
	if end < 0 {
		return ""
	}
	parent := tag[:end]
	if singleton := strings.LastIndexByte(parent, '-'); len(parent)-singleton == 2 {
		return parent[:max(0, singleton)]
	}
	return parent
}

// canonicalNoEvidenceTag resolves the registry's preferred values first, so a
// deprecated spelling and its replacement become one tag before any comparison.
func canonicalNoEvidenceTag(raw string) (canonicalTag, error) {
	lower := strings.ToLower(raw)
	if grandfatheredLanguageTag.MatchString(lower) {
		tag, err := parseCanonicalPackageTag(lower)
		if err != nil {
			return canonicalTag{}, err
		}
		return packageTag(tag, nil), nil
	}
	parts, ok := parseLanguageTag(lower)
	if !ok || (parts.extlang != "" && !registeredExtendedLanguage(parts.language, parts.extlang)) ||
		repeats(parts.variants) || !parts.resolvePreferredValues() {
		return canonicalTag{}, errInvalidLanguageTag
	}
	if err := parts.canonicalizeExtensions(); err != nil {
		return canonicalTag{}, err
	}
	if parts.usesUnmatchedSubtag() {
		return canonicalUnmatchedTag(parts)
	}
	return canonicalKnownTag(parts)
}

// resolvePreferredValues applies RFC 5646 section 4.5: an extended language
// subtag replaces its prefix, and deprecated languages, redundant tags and
// variants give way to their preferred values. Private use is opaque.
func (t *languageTag) resolvePreferredValues() bool {
	if t.extlang != "" && registeredExtendedLanguage(t.language, t.extlang) {
		t.language, t.extlang = t.extlang, ""
	}
	if t.extlang == "" && t.script == "" && t.region != "" {
		if preferred, ok := preferredRedundantTags[t.language+"-"+t.region]; ok {
			t.language, t.region = preferred, ""
		}
	}
	if preferred, ok := preferredLanguages[t.language]; ok {
		t.language = preferred
	}
	replaced := false
	for i, variant := range t.variants {
		if preferred, ok := preferredVariants[variant]; ok {
			t.variants[i], replaced = preferred, true
		}
	}
	// A replacement can repeat a variant that the tag already carries.
	return !replaced || !repeats(t.variants)
}

func (t *languageTag) canonicalizeExtensions() error {
	if repeats(singletons(t.extensions)) {
		return errInvalidLanguageTag
	}
	for i, extension := range t.extensions {
		var err error
		switch extension[0] {
		case 'u':
			t.extensions[i], err = canonicalUnicodeExtension(strings.Split(extension[2:], "-"))
		case 't':
			t.extensions[i], err = canonicalTransformExtension(strings.Split(extension[2:], "-"))
		}
		if err != nil {
			return err
		}
	}
	slices.Sort(t.extensions)
	return nil
}

func canonicalUnicodeExtension(subtags []string) (string, error) {
	end := 0
	for end < len(subtags) && len(subtags[end]) > 2 {
		end++
	}
	attributes := subtags[:end]
	slices.Sort(attributes)
	attributes = slices.Compact(attributes)
	fields, err := canonicalExtensionFields(subtags[end:], false)
	if err != nil {
		return "", err
	}
	return "u-" + strings.Join(slices.Concat(attributes, fields), "-"), nil
}

func canonicalTransformExtension(subtags []string) (string, error) {
	end := slices.IndexFunc(subtags, isTransformKey)
	if end < 0 {
		end = len(subtags)
	}
	parts := []string{"t"}
	if end > 0 {
		// A transform source is a regular language tag, without extensions or
		// private use. The outer tag parser has already split off singletons.
		source := strings.Join(subtags[:end], "-")
		if _, ok := parseLanguageTag(source); !ok {
			return "", errInvalidLanguageExtension
		}
		tag, err := canonicalNoEvidenceTag(source)
		if err != nil {
			return "", err
		}
		parts = append(parts, strings.ToLower(tag.text))
	}
	fields, err := canonicalExtensionFields(subtags[end:], true)
	if err != nil {
		return "", err
	}
	return strings.Join(append(parts, fields...), "-"), nil
}

func isTransformKey(subtag string) bool {
	return len(subtag) == 2 && subtag[0] >= 'a' && subtag[0] <= 'z' && subtag[1] >= '0' && subtag[1] <= '9'
}

// The outer tag parser has already restricted subtags to lowercase alphanumerics.
func isUnicodeKey(subtag string) bool {
	return len(subtag) == 2 && subtag[1] >= 'a' && subtag[1] <= 'z'
}

// Fields are sorted by key. The order of the subtags in each value is preserved.
func canonicalExtensionFields(subtags []string, transformed bool) ([]string, error) {
	aliases := unicodeFieldTypeAliases
	if transformed {
		aliases = transformFieldTypeAliases
	}
	var fields []string
	seen := make(map[string]bool)
	for len(subtags) > 0 {
		key := subtags[0]
		end := 1
		for end < len(subtags) && len(subtags[end]) > 2 {
			end++
		}
		if seen[key] || (transformed && (!isTransformKey(key) || end == 1)) ||
			(!transformed && !isUnicodeKey(key)) {
			return nil, errInvalidLanguageExtension
		}
		seen[key] = true
		value := strings.Join(subtags[1:end], "-")
		if preferred := aliases[key][value]; preferred != "" {
			value = preferred
		}
		if !transformed && value == "true" {
			value = ""
		}
		field := key
		if value != "" {
			field += "-" + value
		}
		fields = append(fields, field)
		subtags = subtags[end:]
	}
	slices.Sort(fields)
	return fields, nil
}

// parseCanonicalPackageTag parses and canonicalizes a tag using the package's
// alias rules and casing.
func parseCanonicalPackageTag(raw string) (language.Tag, error) {
	// Resolve legacy aliases before suppressing scripts. Suppression depends
	// on the canonical base, so a single combined pass can miss an alias.
	tag, err := language.Parse(raw)
	if err != nil {
		return language.Tag{}, err
	}
	for {
		canonical, canonicalErr := (language.Default | language.BCP47).Canonicalize(tag)
		if canonicalErr != nil {
			return language.Tag{}, canonicalErr
		}
		if canonical == tag {
			break
		}
		tag = canonical
	}
	return tag, nil
}

func canonicalKnownTag(parts languageTag) (canonicalTag, error) {
	tag, err := parseCanonicalPackageTag(parts.String())
	if err == nil {
		return packageTag(tag, parts.extensions), nil
	}
	// The package cannot parse some valid extensions. Keep their validated
	// canonical text and use the rest of the tag for locale comparisons.
	if len(parts.extensions) == 0 {
		return canonicalTag{}, err
	}
	extensions := parts.extensions
	parts.extensions = nil
	tag, err = parseCanonicalPackageTag(parts.String())
	if err != nil {
		return canonicalTag{}, err
	}
	result := packageTag(tag, extensions)
	result.known = false
	return result, nil
}

// packageTag describes a canonical package tag.
func packageTag(tag language.Tag, extensions []string) canonicalTag {
	text := tag.String()
	result := canonicalTag{text: text, tag: tag, known: true}
	if parts, ok := parseLanguageTag(strings.ToLower(text)); ok {
		// The package maps some registry preferred values back to a deprecated
		// spelling, so resolve them again on the text.
		if parts.resolvePreferredValues() {
			if extensions != nil {
				parts.extensions = extensions
			}
			result.text = parts.String()
		}
	}
	if !strings.HasPrefix(text, "x-") {
		base, script, region := tag.Raw()
		result.locale, _ = language.Raw.Compose(base, script, region)
		result.hasLocale = true
	}
	return result
}

// canonicalUnmatchedTag validates and canonicalizes a tag that uses a
// registered subtag the package does not know. The package validates
// every other subtag.
func canonicalUnmatchedTag(parts languageTag) (canonicalTag, error) {
	// Normalize the representable core with the same alias and casing rules
	// as any other tag, then restore the registered subtags it could not parse.
	knownCore := parts
	unknownLanguage, unknownScript := unmatchedLanguages[parts.language], unmatchedScripts[parts.script]
	if unknownLanguage {
		knownCore.language = "und"
	}
	if unknownScript {
		knownCore.script = ""
	}
	core, err := parseCanonicalPackageTag(knownCore.core())
	if err != nil {
		return canonicalTag{}, err
	}
	canonicalCore, _ := parseLanguageTag(strings.ToLower(core.String()))
	if !unknownLanguage {
		parts.language = canonicalCore.language
	}
	if !unknownScript {
		parts.script = canonicalCore.script
	}
	parts.region = canonicalCore.region
	if !parts.resolvePreferredValues() {
		return canonicalTag{}, errInvalidLanguageTag
	}
	for _, variant := range parts.variants {
		if !unmatchedVariants[variant] && !knownVariant(variant) {
			return canonicalTag{}, errInvalidLanguageTag
		}
	}
	slices.Sort(parts.variants)
	return canonicalTag{
		text: parts.String(), locale: core,
		hasLocale: !unknownLanguage && !unknownScript,
	}, nil
}

func knownVariant(subtag string) bool {
	_, err := language.Parse("und-" + subtag)
	return err == nil
}
