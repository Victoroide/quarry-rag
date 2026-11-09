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
			answer, tag := noEvidenceAnswer(tc.header)
			require.Equal(t, tc.answer, answer)
			require.Equal(t, tc.tag, tag)
		})
	}
}
