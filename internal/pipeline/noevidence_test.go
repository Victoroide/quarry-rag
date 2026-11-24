package pipeline

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoEvidenceAnswer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
		tag    string
		answer string
	}{
		{"missing", "", "en", "No relevant information was found to answer this question."},
		{"english", "en-GB", "en", "No relevant information was found to answer this question."},
		{"spanish", "es-MX", "es", "No se encontró información relevante para responder esta pregunta."},
		{"portuguese", "pt-BR", "pt", "Não foi encontrada informação relevante para responder a esta pergunta."},
		{"french", "fr-CA", "fr", "Aucune information pertinente n'a été trouvée pour répondre à cette question."},
		{"german", "de-DE", "de", "Es wurden keine relevanten Informationen gefunden, um diese Frage zu beantworten."},
		{"italian", "it-IT", "it", "Non sono state trovate informazioni pertinenti per rispondere a questa domanda."},
		{"unsupported", "zh", "en", "No relevant information was found to answer this question."},
		{"malformed", "es;q=broken", "en", "No relevant information was found to answer this question."},
		{"partially malformed", "es, @", "en", "No relevant information was found to answer this question."},
		{"weighted", "en;q=0.2, es-MX;q=0.9", "es", "No se encontró información relevante para responder esta pregunta."},
		{"unsupported preferred", "zh, fr-CA;q=0.7", "fr", "Aucune information pertinente n'a été trouvée pour répondre à cette question."},
		{"unsupported script", "fr-Brai", "en", "No relevant information was found to answer this question."},
		{"unsupported script before supported language", "fr-Brai, de;q=0.8", "de", "Es wurden keine relevanten Informationen gefunden, um diese Frage zu beantworten."},
		{"excluded", "es;q=0, pt-BR;q=0.5", "pt", "Não foi encontrada informação relevante para responder a esta pergunta."},
		{"all excluded", "es;q=0", "en", "No relevant information was found to answer this question."},
		{"wildcard", "*", "en", "No relevant information was found to answer this question."},
		{"tie", "it, de", "it", "Non sono state trovate informazioni pertinenti per rispondere a questa domanda."},
		{"related language", "gl", "es", "No se encontró información relevante para responder esta pregunta."},
		{"listed language over related", "gl, en;q=0.9", "en", "No relevant information was found to answer this question."},
		{"longest accepted", "es" + strings.Repeat(" ", 1022), "es", "No se encontró información relevante para responder esta pregunta."},
		{"oversized", "es" + strings.Repeat(" ", 1023), "en", "No relevant information was found to answer this question."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			answer, tag := builtinNoEvidenceCatalog().answer(tc.header)
			require.Equal(t, tc.answer, answer)
			require.Equal(t, tc.tag, tag)
		})
	}
}

func TestNoEvidenceCatalogNegotiatesAdditionalLanguages(t *testing.T) {
	catalog, err := parseNoEvidenceCatalog([]byte(`{"ar":"لَمْ نَجِدْ معلوماتٍ ذات صلة.","ja":"情報がありません。","pt-PT":"Não há informação.","mul":"Multilingual answer"}`))
	require.NoError(t, err)
	for _, tc := range []struct{ name, header, tag string }{
		{"exact", "AR", "ar"},
		{"regional fallback", "ar-EG", "ar"},
		{"configured regional tag", "pt-PT", "pt-PT"},
		{"base remains available", "pt", "pt"},
		{"weighted", "ja;q=0.2, ar;q=0.9", "ar"},
		{"unsupported preferred", "zh, ar-EG;q=0.5", "ar"},
		{"excluded", "ar;q=0, ja;q=0.5", "ja"},
		{"all excluded", "ar;q=0, ja;q=0", "en"},
		{"wildcard", "*", "en"},
		{"wildcard with supported preference", "*, ar;q=0.5", "ar"},
		{"explicit multiple languages tag", "mul", "mul"},
		{"unmatched same script", "fa", "en"},
		{"unmatched different script", "zh", "en"},
		{"invalid with supported preference", "ar, @", "en"},
		{"invalid wildcard", "*;q=broken, ar", "en"},
		{"invalid weight", "ar;q=broken", "en"},
		{"weight above one", "ar;q=1.1, ja", "en"},
		{"negative weight", "ar;q=-1, ja", "en"},
		{"non-finite weight", "ar;q=NaN", "en"},
		{"uppercase weight", "ar;Q=0.9", "ar"},
		{"exponent weight", "ar;q=1e-1", "en"},
		{"hexadecimal weight", "ar;q=0x1p-1", "en"},
		{"excess precision", "ar;q=0.9000", "en"},
		{"OWS and valid precision", "ar \t;\tQ=0.900", "ar"},
		{"malformed range", "ar_EG", "en"},
		{"mixed list with malformed range", "ar, ja_JP;q=0.2", "en"},
		{"unknown but well formed range", "zz, ar;q=0.4", "ar"},
		{"unknown variant truncation", "ar-foobar, ja;q=0.4", "ar"},
		{"unsupported script with unknown variant", "ar-Brai-foobar, ja;q=0.4", "ja"},
		{"longest accepted", "ar" + strings.Repeat(" ", 1022), "ar"},
		{"oversized", "ar" + strings.Repeat(" ", 1023), "en"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			answer, tag := catalog.answer(tc.header)
			require.Equal(t, tc.tag, tag)
			if tag == "ar" {
				require.Equal(t, arabicNoEvidence, answer)
			}
		})
	}
}

func TestNoEvidenceCatalogComparesScriptAndRegionBeforeBase(t *testing.T) {
	catalog, err := parseNoEvidenceCatalog([]byte(`{"zh":"Simplified","zh-Hant-TW":"Traditional","pt-PT":"European Portuguese"}`))
	require.NoError(t, err)
	for _, tc := range []struct{ header, tag string }{
		{"zh-Hant-HK", "zh-Hant-TW"},
		{"zh-Hant-HK-x-extra", "zh-Hant-TW"},
		{"zh-TW", "zh-Hant-TW"},
		{"zh-Hant", "zh-Hant-TW"},
		{"zh-CN", "zh"},
		{"zh-Latn", "en"},
		{"zh-Latn, pt-PT;q=0.5", "pt-PT"},
		{"pt-AO", "pt-PT"},
		{"pt-AO-x-extra", "pt-PT"},
		{"pt-BR", "pt"},
		{"pt-Arab-AO, it;q=0.5", "it"},
	} {
		t.Run(tc.header, func(t *testing.T) {
			_, tag := catalog.answer(tc.header)
			require.Equal(t, tc.tag, tag)
		})
	}
}

func TestNoEvidenceCatalogPrefersExactTagWithinQualityOrder(t *testing.T) {
	catalog, err := parseNoEvidenceCatalog([]byte(`{"ca":"generic","ca-valencia":"Valencian","de-1901":"Traditional German","ar":"Arabic","ar-u-nu-latn":"Latin numerals","ar-u-nu-arab":"Arabic numerals"}`))
	require.NoError(t, err)
	for _, tc := range []struct{ header, tag string }{
		{"ca-valencia", "ca-valencia"},
		{"de-1901", "de-1901"},
		{"ger-Latn", "de"},
		{"ar-u-nu-latn", "ar-u-nu-latn"},
		{"ar-u-nu-arab-x-private", "ar-u-nu-arab"},
		{"ca-ES-valencia", "ca"},
		{"ca-ES;q=0.8, ca-valencia;q=0.5", "ca"},
		{"ca, ca-valencia", "ca"},
		{"ca;q=0.5, ca-valencia;q=0.8", "ca-valencia"},
		{"ar-MA;q=0.9, ca-valencia;q=0.5", "ar"},
	} {
		t.Run(tc.header, func(t *testing.T) {
			_, tag := catalog.answer(tc.header)
			require.Equal(t, tc.tag, tag)
		})
	}
}

func TestNoEvidenceCatalogEquallyMatchedTagsAreDeterministic(t *testing.T) {
	for _, body := range []string{`{"ar-EG":"Egypt","ar-SA":"Saudi Arabia"}`, `{"ar-SA":"Saudi Arabia","ar-EG":"Egypt"}`} {
		catalog, err := parseNoEvidenceCatalog([]byte(body))
		require.NoError(t, err)
		answer, tag := catalog.answer("ar")
		require.Equal(t, "ar-EG", tag)
		require.Equal(t, "Egypt", answer)
	}
}

func FuzzNoEvidenceAcceptLanguage(f *testing.F) {
	catalog, err := parseNoEvidenceCatalog([]byte(`{"ar":"معلومات","ja":"情報がありません。","hnm":"Hainanese","isv-Latn":"Interslavic"}`))
	require.NoError(f, err)
	answers := make(map[string]string)
	for _, tag := range []string{"en", "es", "pt", "fr", "de", "it", "ar", "ja", "hnm", "isv-Latn"} {
		answer, matched := catalog.answer(tag)
		require.Equal(f, tag, matched)
		answers[tag] = answer
	}
	for _, seed := range []string{"", "ar-EG", "ja;q=0.5, *;q=0.1", "ar;q=0", "es, @", "ar\r\n", "ar" + strings.Repeat(" ", 1023), "zh-hnm", "ISV-latn, ar;q=0.1", "hnm-u-nu-latn-x-team", "en-taglish-gara"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, header string) {
		answer, tag := catalog.answer(header)
		require.Contains(t, answers, tag)
		require.Equal(t, answers[tag], answer)
		if len(header) > 1024 {
			require.Equal(t, "en", tag)
		}
	})
}
