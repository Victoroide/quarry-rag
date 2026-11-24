package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

const arabicNoEvidence = "لَمْ نَجِدْ معلوماتٍ ذات صلة."

func TestNoEvidenceCatalogFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	writeCatalog := func(content string) {
		t.Helper()
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	}
	_, err := LoadNoEvidenceCatalog(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = LoadNoEvidenceCatalog(filepath.Dir(path))
	require.Error(t, err)

	builtins, err := LoadNoEvidenceCatalog("")
	require.NoError(t, err)
	english, tag := builtins.answer("ar")
	require.Equal(t, "en", tag)
	writeCatalog(fmt.Sprintf(`{"AR":%q}`, arabicNoEvidence))
	catalog, err := LoadNoEvidenceCatalog(path)
	require.NoError(t, err)
	writeCatalog(`{"ar":"تحديث"}`)
	answer, tag := catalog.answer("ar")
	require.Equal(t, "ar", tag)
	require.Equal(t, arabicNoEvidence, answer)
	answer, tag = builtins.answer("ar")
	require.Equal(t, "en", tag)
	require.Equal(t, english, answer)
}

func TestNoEvidenceCatalogRejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct{ name, body, detail string }{
		{"empty file", "", "JSON object"},
		{"null root", `null`, "JSON object"},
		{"array root", `[]`, "JSON object"},
		{"unfinished", `{"ar":"text"`, "JSON object"},
		{"trailing data", `{"ar":"text"} {}`, "JSON object"},
		{"number", `{"ar":1}`, "JSON object"},
		{"nested value", `{"ar":{"other":"text"}}`, "JSON object"},
		{"null value", `{"ar":null}`, "empty"},
		{"empty message", `{"ar":""}`, "empty"},
		{"blank message", `{"ar":"　  "}`, "empty"},
		{"formatting-only message", `{"ar":"\u200f\u200d"}`, "empty"},
		{"ignorable marks only", `{"ar":"\u034f\ufe0f"}`, "empty"},
		{"raw UTF-8", "{\"ar\":\"\xff\"}", "JSON object"},
		{"unpaired surrogate", `{"ar":"\ud800"}`, "JSON object"},
		{"duplicate key", `{"ar":"first","ar":"second"}`, "JSON object"},
		{"escaped duplicate key", `{"ar":"first","a\u0072":"second"}`, "JSON object"},
		{"duplicate canonical case", `{"ar-IL":"first","AR-il":"second"}`, "duplicate"},
		{"duplicate canonical alias", `{"he":"first","iw":"second"}`, "duplicate"},
		{"duplicate canonical script", `{"ar":"first","ar-Arab":"second"}`, "duplicate"},
		{"built-in", `{"en":"replacement"}`, "duplicate"},
		{"built-in canonical alias", `{"eng":"replacement"}`, "duplicate"},
		{"built-in alias and script", `{"ger-Latn":"replacement"}`, "duplicate"},
		{"catalog alias and script", `{"yi":"first","ji-Hebr":"second"}`, "duplicate"},
		{"repeated variant", `{"sl-rozaj-ROZAJ":"text"}`, "language tag"},
		{"repeated extension", `{"ar-u-nu-latn-U-ca-gregory":"text"}`, "language tag"},
		{"transformed field without a value", `{"ar-t-h0":"text"}`, "language tag"},
		{"transformed field without a value on newer language", `{"isv-t-h0":"text"}`, "language tag"},
		{"repeated transformed field", `{"ar-t-d0-hex-d0-numeric":"text"}`, "language tag"},
		{"repeated Unicode locale key", `{"ar-u-kn-kn":"text"}`, "language tag"},
		{"wrong extended language prefix", `{"en-cmn":"text"}`, "invalid language tag"},
		{"multiple extended languages", `{"zh-cmn-yue":"text"}`, "invalid language tag"},
		{"non-extended language in extended position", `{"zh-eng":"text"}`, "invalid language tag"},
		{"reserved primary language", `{"root":"text"}`, "invalid language tag"},
		{"extended language alias collision", `{"cmn":"first","zh-cmn":"second"}`, "duplicate"},
		{"newline tag", `{"ar\r\nX-Test":"text"}`, "language tag"},
		{"space tag", `{" ar":"text"}`, "language tag"},
		{"underscore tag", `{"ar_EG":"text"}`, "language tag"},
		{"wildcard tag", `{"*":"text"}`, "language tag"},
		{"unrecognized tag", `{"fake-language":"text"}`, "language tag"},
		{"control", `{"ar":"before\u0000after"}`, "control"},
		{"C1 control", `{"ar":"before\u0085after"}`, "control"},
		{"newline message", `{"ar":"before\nafter"}`, "control"},
		{"bidi override", `{"ar":"before\u202eafter"}`, "control"},
		{"bidi isolate", `{"ar":"before\u2067after"}`, "control"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseNoEvidenceCatalog([]byte(tc.body))
			require.ErrorContains(t, err, tc.detail)
		})
	}
}

func TestNoEvidenceCatalogExtendedLanguagesRequireRegisteredPrefix(t *testing.T) {
	for prefix, subtags := range extendedLanguagesByPrefix {
		for subtag := range strings.FieldsSeq(subtags) {
			require.Truef(t, validCatalogLanguageTag(prefix+"-"+subtag), "%s-%s", prefix, subtag)
			require.Falsef(t, validCatalogLanguageTag("xx-"+subtag), "xx-%s", subtag)
			require.Falsef(t, validCatalogLanguageTag(prefix+"-"+subtag+"-"+subtag), "%s-%s-%[2]s", prefix, subtag)
		}
	}
}

func loadSingleTagCatalog(t *testing.T, entries map[string]string) *NoEvidenceCatalog {
	t.Helper()
	body, err := json.Marshal(entries)
	require.NoError(t, err)
	catalog, err := parseNoEvidenceCatalog(body)
	require.NoError(t, err)
	return catalog
}

func TestNoEvidenceCatalogServesBothSpellingsOfExtendedLanguages(t *testing.T) {
	for prefix, subtags := range extendedLanguagesByPrefix {
		for subtag := range strings.FieldsSeq(subtags) {
			extended := prefix + "-" + subtag
			byExtended := loadSingleTagCatalog(t, map[string]string{extended: "Catalog text"})
			byPrimary := loadSingleTagCatalog(t, map[string]string{subtag: "Catalog text"})
			wantAnswer, wantTag := byPrimary.answer(subtag)
			require.Equalf(t, "Catalog text", wantAnswer, "%s as a primary language", subtag)
			for _, request := range []string{subtag, extended, strings.ToUpper(extended)} {
				for name, catalog := range map[string]*NoEvidenceCatalog{"extended key": byExtended, "primary key": byPrimary} {
					answer, tag := catalog.answer(request)
					require.Equalf(t, wantTag, tag, "%s requested through %s", name, request)
					require.Equalf(t, "Catalog text", answer, "%s requested through %s", name, request)
				}
			}
		}
	}
}

func TestNoEvidenceCatalogServesRegisteredSubtagsUnknownToMatcher(t *testing.T) {
	for _, tc := range []struct{ name, key, request, tag string }{
		{"extended Hainanese", "zh-hnm", "hnm", "hnm"},
		{"primary Hainanese", "hnm", "zh-hnm", "hnm"},
		{"extended Leizhou", "ZH-LUH", "luh", "luh"},
		{"extended Shaojiang", "zh-sjc", "SJC", "sjc"},
		{"extended Bhutanese Sign Language", "sgn-dyl", "dyl", "dyl"},
		{"extended Kurdish Sign Language", "sgn-zhk", "sgn-zhk", "zhk"},
		{"Classical Sanskrit", "cls", "CLS", "cls"},
		{"Interslavic", "isv", "isv", "isv"},
		{"language with script", "isv-latn", "ISV-Latn", "isv-Latn"},
		{"language with script and region", "isv-Cyrl-rs", "isv-cyrl-RS", "isv-Cyrl-RS"},
		{"extended language with script", "zh-hnm-Hans", "hnm-hans", "hnm-Hans"},
		{"unknown script", "ff-gara", "FF-Gara", "ff-Gara"},
		{"unknown script and region", "ff-Gara-SN", "ff-gara-sn", "ff-Gara-SN"},
		{"unknown script with a sign language region", "sgn-Gara-US", "sgn-gara-us", "sgn-Gara-US"},
		{"unknown variant", "en-taglish", "EN-Taglish", "en-taglish"},
		{"unknown variant with script", "nan-Latn-pehoeji", "nan-latn-pehoeji", "nan-Latn-pehoeji"},
		{"known and unknown variants", "ca-taglish-valencia", "ca-valencia-taglish", "ca-taglish-valencia"},
		{"unknown language with extension and private use", "hnm-u-nu-latn-x-team", "HNM-u-nu-latn-x-team", "hnm-u-nu-latn-x-team"},
		{"extensions in any order", "hnm-u-nu-latn-a-aaa", "hnm-a-aaa-u-nu-latn", "hnm-a-aaa-u-nu-latn"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := loadSingleTagCatalog(t, map[string]string{tc.key: "Catalog text"})
			answer, tag := catalog.answer(tc.request)
			require.Equal(t, tc.tag, tag)
			require.Equal(t, "Catalog text", answer)
			answer, tag = catalog.answer("de;q=0.4, " + tc.request)
			require.Equal(t, tc.tag, tag)
			require.Equal(t, "Catalog text", answer)
		})
	}
	for _, key := range []string{"hnm-hnm", "zh-hnm-hnm", "zh-yue-hnm", "en-hnm", "xx-hnm", "xx-gara", "hnm-gara-gara", "en-taglish-taglish", "hnm-u-nu-latn-u-ca-gregory"} {
		_, err := parseNoEvidenceCatalog([]byte(fmt.Sprintf(`{%q:"text"}`, key)))
		require.ErrorContainsf(t, err, "invalid language tag", key)
	}
}

func TestNoEvidenceCatalogServesEveryUnmatchedSubtag(t *testing.T) {
	type kind struct {
		name    string
		subtags map[string]bool
		tag     func(subtag string) string
	}
	title := func(subtag string) string { return strings.ToUpper(subtag[:1]) + subtag[1:] }
	for _, k := range []kind{
		{"language", unmatchedLanguages, func(s string) string { return s }},
		{"language with script", unmatchedLanguages, func(s string) string { return s + "-Latn" }},
		{"script", unmatchedScripts, func(s string) string { return "ar-" + title(s) }},
		{"variant", unmatchedVariants, func(s string) string { return "ar-" + s }},
	} {
		for subtag := range k.subtags {
			key := k.tag(subtag)
			catalog := loadSingleTagCatalog(t, map[string]string{strings.ToLower(key): "Catalog text"})
			for _, request := range []string{key, strings.ToUpper(key), strings.ToLower(key)} {
				answer, tag := catalog.answer(request)
				require.Equalf(t, key, tag, "%s %s requested through %s", k.name, subtag, request)
				require.Equal(t, "Catalog text", answer)
			}
			_, tag := catalog.answer("ar")
			require.Equalf(t, "en", tag, "%s %s must not answer its base language", k.name, subtag)
		}
	}
	for _, table := range []map[string]string{preferredLanguages, preferredRedundantTags, preferredVariants} {
		for deprecated, preferred := range table {
			require.NotContains(t, preferredLanguages, preferred, "%s resolves through a second substitution", deprecated)
			require.NotContains(t, preferredVariants, preferred, "%s resolves through a second substitution", deprecated)
		}
	}
}

func TestNoEvidenceCatalogMatchesUnknownSubtagsExactly(t *testing.T) {
	catalog := loadSingleTagCatalog(t, map[string]string{"hnm": "Hainanese", "isv-Latn": "Latin Interslavic", "en-taglish": "Taglish", "zh": "Chinese"})
	for _, tc := range []struct{ header, tag string }{
		{"hnm", "hnm"},
		{"hnm-x-extra", "hnm"},
		{"hnm-u-nu-latn", "hnm"},
		{"hnm-CN", "en"},
		{"hnm-Hans", "en"},
		{"zh-hnm", "hnm"},
		{"isv-Latn", "isv-Latn"},
		{"isv-Latn-x-extra", "isv-Latn"},
		{"isv", "en"},
		{"isv-Cyrl", "en"},
		{"isv-Latn-RS", "en"},
		{"isv-Cyrl, de;q=0.4", "de"},
		{"hnm;q=0.2, de;q=0.9", "de"},
		{"en-taglish", "en-taglish"},
		{"en-taglish-x-extra", "en-taglish"},
		{"en", "en"},
		{"en-GB", "en"},
		{"en-GB-taglish", "en"},
		{"zh-CN", "zh"},
		{"zh-Gara", "en"},
		{"zh-Gara, de;q=0.4", "de"},
		{"cls", "en"},
	} {
		t.Run(tc.header, func(t *testing.T) {
			_, tag := catalog.answer(tc.header)
			require.Equal(t, tc.tag, tag)
		})
	}
	answer, _ := catalog.answer("en-GB")
	require.Equal(t, "No relevant information was found to answer this question.", answer)
}

func TestNoEvidenceCatalogUsesRegistryPreferredValues(t *testing.T) {
	pairs := []struct{ deprecated, preferred string }{{"dek", "sqm"}, {"nte", "eko"}, {"mrd", "mgp"}, {"shl", "mrh"}, {"yol", "enm"}, {"bh", "bih"}, {"mo", "ro"}, {"sgn-US", "ase"}, {"ja-Latn-hepburn-heploc", "ja-Latn-hepburn-alalc97"}}
	for _, pair := range pairs {
		t.Run(pair.deprecated, func(t *testing.T) {
			_, err := parseNoEvidenceCatalog([]byte(fmt.Sprintf(`{%q:"first",%q:"second"}`, pair.deprecated, pair.preferred)))
			require.ErrorContains(t, err, "duplicate language tag")
			_, err = parseNoEvidenceCatalog([]byte(fmt.Sprintf(`{%q:"first",%q:"second"}`, pair.preferred, strings.ToUpper(pair.deprecated))))
			require.ErrorContains(t, err, "duplicate language tag")

			byDeprecated := loadSingleTagCatalog(t, map[string]string{pair.deprecated: "Catalog text"})
			byPreferred := loadSingleTagCatalog(t, map[string]string{pair.preferred: "Catalog text"})
			_, wantTag := byPreferred.answer(pair.preferred)
			require.NotEqual(t, "en", wantTag)
			for _, request := range []string{pair.deprecated, pair.preferred, strings.ToLower(pair.deprecated)} {
				for name, catalog := range map[string]*NoEvidenceCatalog{"deprecated key": byDeprecated, "preferred key": byPreferred} {
					answer, tag := catalog.answer(request)
					require.Equalf(t, wantTag, tag, "%s requested through %s", name, request)
					require.Equalf(t, "Catalog text", answer, "%s requested through %s", name, request)
				}
			}
		})
	}

	catalog := loadSingleTagCatalog(t, map[string]string{"dek": "dek", "mo-Cyrl-MD": "Moldovan", "yol-x-team": "private"})
	for _, tc := range []struct{ header, tag string }{
		{"dek", "sqm"},
		{"SQM", "sqm"},
		{"mo-Cyrl-MD", "ro-Cyrl-MD"},
		{"RO-cyrl-md", "ro-Cyrl-MD"},
		{"yol-x-team", "enm-x-team"},
		{"enm-x-team", "enm-x-team"},
	} {
		t.Run(tc.header, func(t *testing.T) {
			_, tag := catalog.answer(tc.header)
			require.Equal(t, tc.tag, tag)
		})
	}
}

func TestNoEvidenceCatalogCanonicalizesCompoundTags(t *testing.T) {
	for _, pair := range []struct{ alternate, canonical string }{
		{"isv-BU", "isv-MM"},
		{"iw-Gara", "he-Gara"},
		{"iw-Gara-BU", "he-Gara-MM"},
		{"isv-Qaai-BU", "isv-Zinh-MM"},
		{"hnm-u-nu-latn-ca-gregory", "hnm-u-ca-gregory-nu-latn"},
		{"hnm-u-abczy-abcaaa-nu-latn-ca-islamic-civil", "hnm-u-abcaaa-abczy-ca-islamic-civil-nu-latn"},
		{"ar-u-abczy-abcaaa-nu-latn-ca-islamic-civil", "ar-u-abcaaa-abczy-ca-islamic-civil-nu-latn"},
		{"ar-BU", "ar-MM"},
		{"iw", "he"},
	} {
		t.Run(pair.alternate, func(t *testing.T) {
			_, err := parseNoEvidenceCatalog([]byte(fmt.Sprintf(`{%q:"first",%q:"second"}`, pair.alternate, pair.canonical)))
			require.ErrorContains(t, err, "duplicate language tag")
			for _, key := range []string{pair.alternate, pair.canonical} {
				catalog := loadSingleTagCatalog(t, map[string]string{key: "Catalog text"})
				for _, request := range []string{pair.alternate, pair.canonical} {
					answer, tag := catalog.answer(request)
					require.Equalf(t, pair.canonical, tag, "key %s with request %s", key, request)
					require.Equal(t, "Catalog text", answer)
				}
			}
		})
	}
}

func TestNoEvidenceCatalogCanonicalizesExtensions(t *testing.T) {
	for _, pair := range []struct{ alternate, canonical string }{
		{"ar-u-ca-islamicc", "ar-u-ca-islamic-civil"},
		{"isv-u-nu-latn-ca-islamicc", "isv-u-ca-islamic-civil-nu-latn"},
		{"ar-u-ca-ethiopic-amete-alem", "ar-u-ca-ethioaa"},
		{"ar-u-ms-imperial-tz-camtr", "ar-u-ms-uksystem-tz-cator"},
		{"ar-u-kn-true", "ar-u-kn"},
		{"isv-u-kn-true", "isv-u-kn"},
		{"ar-t-m0-xml-d0-hex", "ar-t-d0-hex-m0-xml"},
		{"isv-t-m0-xml-d0-hex", "isv-t-d0-hex-m0-xml"},
		{"ar-t-m0-names", "ar-t-m0-prprname"},
		{"isv-t-m0-names", "isv-t-m0-prprname"},
		{"ar-t-m0-ies-jes", "ar-t-m0-iesjes"},
		{"isv-t-m0-ies-jes", "isv-t-m0-iesjes"},
		{"ar-t-d0-name", "ar-t-d0-charname"},
		{"ar-t-m0-beta-metsehaf", "ar-t-m0-betamets"},
		{"ar-t-m0-tekie-alibekit", "ar-t-m0-tekieali"},
		{"ar-u-foo-foo-nu-latn", "ar-u-foo-nu-latn"},
		{"isv-u-foo-foo-nu-latn", "isv-u-foo-nu-latn"},
		{"und-t-es-m0-xml-d0-hex", "und-t-es-d0-hex-m0-xml"},
		{"ar-u-kn-true-nu-latn-t-m0-ungegn-2007-d0-hex", "ar-t-d0-hex-m0-ungegn-2007-u-kn-nu-latn"},
	} {
		t.Run(pair.canonical, func(t *testing.T) {
			_, err := parseNoEvidenceCatalog([]byte(fmt.Sprintf(`{%q:"first",%q:"second"}`, pair.alternate, pair.canonical)))
			require.ErrorContains(t, err, "duplicate language tag")
			spellings := []string{pair.alternate, pair.canonical}
			for _, key := range spellings {
				catalog := loadSingleTagCatalog(t, map[string]string{key: "Catalog text"})
				for _, request := range spellings {
					answer, tag := catalog.answer(request)
					require.Equalf(t, pair.canonical, tag, "key %s with request %s", key, request)
					require.Equal(t, "Catalog text", answer)
				}
			}
		})
	}
}

func TestNoEvidenceCatalogRejectsMalformedUnicodeKeys(t *testing.T) {
	for _, tag := range []string{"ar-u-m0-names", "isv-u-m0-names"} {
		t.Run(tag, func(t *testing.T) {
			_, err := parseNoEvidenceCatalog([]byte(fmt.Sprintf(`{%q:"text"}`, tag)))
			require.ErrorContains(t, err, "invalid language tag extension")
		})
	}
}

func TestNoEvidenceCatalogKeepsDistinctTransformTypes(t *testing.T) {
	entries := map[string]string{
		"ar-t-m0-prprname":    "Proper names",
		"ar-t-m0-ungegn":      "UNGEGN",
		"ar-t-m0-names-extra": "One compound value",
		"ar-t-m0-extra-names": "Another compound value",
		"ar-t-d0-names":       "An unrelated field",
		"ar-u-ca-gregory":     "Gregorian calendar",
		"ar-x-m0-names":       "First private text",
		"ar-x-m0-prprname":    "Second private text",
		"ar-x-u-m0-names":     "Third private text",
	}
	catalog := loadSingleTagCatalog(t, entries)
	for key, text := range entries {
		answer, tag := catalog.answer(key)
		require.Equal(t, key, tag)
		require.Equal(t, text, answer)
	}
}

func TestNoEvidenceCatalogKeepsDistinctUnicodeFieldTypes(t *testing.T) {
	entries := map[string]string{
		"ar-u-nu-islamicc":       "An unrelated field",
		"ar-u-ca-islamicc-extra": "One compound value",
		"ar-u-ca-extra-islamicc": "Another compound value",
		"ar-u-ca-islamic-civil":  "The preferred calendar",
	}
	catalog := loadSingleTagCatalog(t, entries)
	for key, text := range entries {
		answer, tag := catalog.answer(key)
		require.Equal(t, key, tag)
		require.Equal(t, text, answer)
	}
}

func TestNoEvidenceCatalogKeepsPrivateUseOpaque(t *testing.T) {
	for _, pair := range [][2]string{
		{"x-heploc", "x-alalc97"},
		{"x-team-heploc", "x-team-alalc97"},
		{"ja-x-heploc", "ja-x-alalc97"},
	} {
		t.Run(pair[0], func(t *testing.T) {
			catalog := loadSingleTagCatalog(t, map[string]string{pair[0]: "first", pair[1]: "second"})
			for _, entry := range []struct{ tag, text string }{{pair[0], "first"}, {pair[1], "second"}} {
				answer, tag := catalog.answer(strings.ToUpper(entry.tag))
				require.Equal(t, entry.tag, tag)
				require.Equal(t, entry.text, answer)
			}
		})
	}
}

func TestNoEvidenceCatalogRequestableTags(t *testing.T) {
	boundary := "x-" + strings.Repeat("abcdefg-", 127) + "abcdef"
	require.Len(t, boundary, 1024)
	for _, tc := range []struct{ name, key, tag string }{
		{"grandfathered alias", "i-klingon", "tlh"},
		{"grandfathered with private use", "i-default", "en-x-i-default"},
		{"regular grandfathered alias", "no-bok", "nb"},
		{"grandfathered with multiple three-letter subtags", "zh-min-nan", "nan"},
		{"registered Mandarin extended language", "zh-cmn", "cmn"},
		{"registered Cantonese extended language", "ZH-YUE", "yue"},
		{"registered sign extended language", "sgn-ase", "ase"},
		{"repeated private-use subtags", "x-example-a-a", "x-example-a-a"},
		{"maximum header length", boundary, boundary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(map[string]string{tc.key: "Catalog text"})
			require.NoError(t, err)
			catalog, err := parseNoEvidenceCatalog(body)
			require.NoError(t, err)
			answer, tag := catalog.answer(tc.key)
			require.Equal(t, tc.tag, tag)
			require.Equal(t, "Catalog text", answer)
		})
	}
	body, err := json.Marshal(map[string]string{boundary + "g": "Catalog text"})
	require.NoError(t, err)
	_, err = parseNoEvidenceCatalog(body)
	require.ErrorContains(t, err, "1024 bytes")
}

func TestNoEvidenceCatalogLimitsAndScriptMarks(t *testing.T) {
	const catalogLimit = 64 * 1024
	body := "{}" + strings.Repeat(" ", catalogLimit-2)
	path := filepath.Join(t.TempDir(), "catalog.json")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	_, err := LoadNoEvidenceCatalog(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte(body+" "), 0o600))
	_, err = LoadNoEvidenceCatalog(path)
	require.ErrorContains(t, err, "65536 bytes")

	entries := map[string]string{}
	for i := range 128 {
		entries[fmt.Sprintf("x-example-%d", i)] = "text"
	}
	bodyBytes, err := json.Marshal(entries)
	require.NoError(t, err)
	_, err = parseNoEvidenceCatalog(bodyBytes)
	require.NoError(t, err)
	entries["ar"] = "text"
	bodyBytes, err = json.Marshal(entries)
	require.NoError(t, err)
	_, err = parseNoEvidenceCatalog(bodyBytes)
	require.ErrorContains(t, err, "128 translations")

	message := strings.Repeat("あ", 341) + "!"
	catalog, err := parseNoEvidenceCatalog([]byte(fmt.Sprintf(`{"ja":%q}`, message)))
	require.NoError(t, err)
	answer, tag := catalog.answer("ja")
	require.Equal(t, "ja", tag)
	require.Equal(t, message, answer)
	_, err = parseNoEvidenceCatalog([]byte(fmt.Sprintf(`{"ja":%q}`, message+"!")))
	require.ErrorContains(t, err, "1024 bytes")

	marked := arabicNoEvidence + " \u061c\u200f\u200e می\u200cرود क्\u200dष"
	bodyBytes, err = json.Marshal(map[string]string{"ar": marked})
	require.NoError(t, err)
	catalog, err = parseNoEvidenceCatalog(bodyBytes)
	require.NoError(t, err)
	answer, tag = catalog.answer("ar")
	require.Equal(t, "ar", tag)
	require.Equal(t, marked, answer)
}

func FuzzNoEvidenceCatalog(f *testing.F) {
	for _, seed := range []string{"{}", `{"ar":"معلومات"}`, `{"ja":"\ud83d\ude00"}`, `{"he":"one","iw":"two"}`, `{"ja":"\ud800"}`, "{\"ar\":\"\xff\"}", `{"ca":"generic","ca-valencia":"Valencian","de-1901":"Traditional German"}`, `{"ar-u-nu-latn":"Latin numerals","ar-u-nu-arab":"Arabic numerals"}`, `{"zh-hnm":"Hainanese","isv-Latn":"Interslavic","en-taglish-fonipa":"Taglish"}`, `{"dek":"Suma","sqm":"Suma"}`, `{"hnm-u-nu-latn-a-aaa-x-team":"text","sgn-US":"sign language"}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		catalog, err := parseNoEvidenceCatalog(body)
		if err != nil {
			return
		}
		require.LessOrEqual(t, len(body), 64*1024)
		require.True(t, utf8.Valid(body))
		var entries map[string]string
		require.NoError(t, json.Unmarshal(body, &entries))
		require.NotNil(t, entries)
		require.LessOrEqual(t, len(entries), 128)
		for tag, answer := range entries {
			require.NotEmpty(t, strings.TrimSpace(answer))
			require.LessOrEqual(t, len(answer), 1024)
			require.NotContains(t, tag, "\r")
			require.NotContains(t, tag, "\n")
		}
		for _, message := range catalog.messages[len(noEvidenceMessages):] {
			canonical := message.tag.text
			require.LessOrEqual(t, len(canonical), 1024)
			answer, tag := catalog.answer(canonical)
			require.Equal(t, canonical, tag)
			require.Equal(t, message.answer, answer)
		}
		builtins, loadErr := LoadNoEvidenceCatalog("")
		require.NoError(t, loadErr)
		wantAnswer, wantTag := builtins.answer("")
		answer, tag := catalog.answer("")
		require.Equal(t, wantTag, tag)
		require.Equal(t, wantAnswer, answer)
	})
}
