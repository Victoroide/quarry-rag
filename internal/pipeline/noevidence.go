package pipeline

import "golang.org/x/text/language"

var noEvidenceMessages = [...]struct {
	tag    language.Tag
	answer string
}{
	{language.English, "No relevant information was found to answer this question."},
	{language.Spanish, "No se encontró información relevante para responder esta pregunta."},
	{language.Portuguese, "Não foi encontrada informação relevante para responder a esta pergunta."},
	{language.French, "Aucune information pertinente n'a été trouvée pour répondre à cette question."},
	{language.German, "Es wurden keine relevanten Informationen gefunden, um diese Frage zu beantworten."},
	{language.Italian, "Non sono state trovate informazioni pertinenti per rispondere a questa domanda."},
}

var noEvidenceMatcher = func() language.Matcher {
	tags := make([]language.Tag, len(noEvidenceMessages))
	for i, message := range noEvidenceMessages {
		tags[i] = message.tag
	}
	return language.NewMatcher(tags)
}()

// maxAcceptLanguageBytes bounds header parsing; longer values use English.
const maxAcceptLanguageBytes = 1024

func noEvidenceAnswer(acceptLanguage string) (answer, tag string) {
	index := 0
	if len(acceptLanguage) <= maxAcceptLanguageBytes {
		if tags, _, err := language.ParseAcceptLanguage(acceptLanguage); err == nil {
			_, index, _ = noEvidenceMatcher.Match(tags...)
		}
	}
	message := noEvidenceMessages[index]
	return message.answer, message.tag.String()
}
