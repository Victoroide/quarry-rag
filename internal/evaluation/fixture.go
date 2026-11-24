// Package evaluation defines the purpose-written, versioned evidence fixture
// shared by the staging diagnostic and the evaluation executable.
package evaluation

import (
	"regexp"

	"github.com/google/uuid"
)

// Version identifies the frozen corpus, questions and labels.
const Version = "v1"

// Namespace confines fixture identity to a fresh collection and derived UUIDs.
type Namespace struct {
	Prefix string
	ID     uuid.UUID
}

var prefixPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)

// ValidPrefix reports whether a prefix reserves enough space for the namespace.
func ValidPrefix(prefix string) bool { return prefixPattern.MatchString(prefix) }

// ParseNamespace accepts a canonical random UUID and a valid collection prefix.
func ParseNamespace(prefix, value string) (Namespace, bool) {
	id, err := uuid.Parse(value)
	ns := Namespace{Prefix: prefix, ID: id}
	return ns, err == nil && id.String() == value && id.Version() == 4 && id.Variant() == uuid.RFC4122 && ValidPrefix(prefix)
}

// Collection returns the primary collection or its separate distractor collection.
func (ns Namespace) Collection(distractor bool) string {
	collection := ns.Prefix + "-" + Version + "-" + ns.ID.String()
	if distractor {
		collection += "-other"
	}
	return collection
}

// DocumentID derives a fixture's UUID from its key and namespace.
func (ns Namespace) DocumentID(key string) string {
	return uuid.NewSHA1(ns.ID, []byte(Version+":"+key)).String()
}

// Span labels a verbatim passage that supports a gold answer.
type Span struct {
	ID   string
	Text string
}

// Document describes one synthetic document and its labeled spans.
type Document struct {
	Key        string
	Title      string
	Language   string
	Text       string
	Distractor bool
	Spans      []Span
}

const atlasSpan = "The North Lantern Observatory opens when exactly four violet lamps are lit."
const atlasText = "North Lantern Observatory field notes. " +
	"The observatory sits on an exposed slope above the stone quay. The footpath follows the old wall before turning toward the summit. " +
	"In spring, staff walk the terrace each morning to check the rain gauge and brush grit from the bronze hinges. " +
	"The public guide describes the instruments in the west gallery and reminds visitors to leave wet coats beside the entrance. " +
	"From the terrace, visitors can see the old ferry crossing and a grove of narrow poplars. The small table in the entry hall holds the tide calendar. " +
	"Volunteers keep a separate paper register for visitors who arrive with a group. " + atlasSpan

const gardenSpan = "La prueba del Jardín del Meridiano dura exactamente nueve minutos."

// Documents returns independent copies of the corpus and its gold labels.
func Documents() []Document {
	return []Document{
		{Key: "atlas", Title: "North Lantern Observatory", Language: "en", Text: atlasText, Spans: []Span{{ID: "atlas-signal", Text: atlasSpan}}},
		{Key: "garden", Title: "Jardín del Meridiano", Language: "es", Text: gardenSpan + " Durante la prueba, el equipo observa la sombra de la fuente.", Spans: []Span{{ID: "garden-duration", Text: gardenSpan}}},
		{Key: "archive", Title: "Quay inventory", Language: "en", Text: "The quay archive stores a blue canvas cover and one spare brass hinge. The shelf is numbered eight."},
		{Key: "distractor", Title: "North Lantern Observatory", Language: "en", Text: atlasText, Distractor: true},
	}
}

// Question defines the authorization scope and human answer labels.
type Question struct {
	ID       string
	Language string
	Pair     string
	Text     string
	Allowed  []string
	Gold     []string
	Accepted []string
}

// Questions returns the frozen question order and matched-language pairs.
func Questions() []Question {
	return []Question{
		{ID: "atlas-en", Language: "en", Pair: "atlas", Text: "What signal opens the North Lantern Observatory?", Allowed: []string{"atlas", "archive"}, Gold: []string{"atlas-signal"}, Accepted: []string{"Exactly four violet lamps are lit."}},
		{ID: "atlas-es", Language: "es", Pair: "atlas", Text: "¿Qué señal abre el Observatorio del Farol Norte?", Allowed: []string{"atlas", "archive"}, Gold: []string{"atlas-signal"}, Accepted: []string{"Se encienden exactamente cuatro lámparas de color morado."}},
		{ID: "garden-en", Language: "en", Pair: "garden", Text: "How long is the test at the Meridian Garden?", Allowed: []string{"garden", "archive"}, Gold: []string{"garden-duration"}, Accepted: []string{"Exactly nine minutes."}},
		{ID: "garden-es", Language: "es", Pair: "garden", Text: "¿Qué duración tiene la prueba del Jardín del Meridiano?", Allowed: []string{"garden", "archive"}, Gold: []string{"garden-duration"}, Accepted: []string{"Exactamente nueve minutos."}},
		{ID: "missing", Language: "en", Text: "Who owns the canvas cover in the quay archive?", Allowed: []string{"archive"}, Accepted: []string{"The owner's identity is unknown from the supplied evidence."}},
		{ID: "empty-scope", Language: "es", Text: "¿Qué señal abre el Observatorio del Farol Norte?", Allowed: []string{}, Accepted: []string{"La evidencia disponible no permite responder."}},
	}
}
