package unit

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/stretchr/testify/assert"
)

func TestExtractNamedEntity(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{"archive name", "Cedar Archive stores technical records", "cedar archive"},
		{"name with connectors", "Parque de la Memoria archives reports", "parque de la memoria"},
		{"generic title ignored", "Document Overview: Reference Guide", ""},
		{"empty string", "", ""},
		{"generic words only", "Document Overview with Python", ""},
		{"single capitalised word", "Cedar stores records", ""},
		{"two capitalised words", "Alpine Station stores reports", "alpine station"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pipeline.ExtractNamedEntity(tt.text)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMatchNamedEntityEvidence(t *testing.T) {
	previews := map[string]string{
		"d-harbor-n": "Harbor Registry North Report. Manifest archiving for seven years.",
		"d-harbor-s": "Harbor Registry South Report. Manifest archiving for ten years.",
		"d-erable":   "Érable Central Operations. Vault humidity controls for microfilm.",
		"d-aurora":   "Aurora Telemetry Guide. Signed heartbeats every fifteen seconds.",
		"d-horizon":  "Horizon Terminal Guide. Horizon Terminalの運用手順 for cranes.",
	}
	order := []string{"d-harbor-n", "d-harbor-s", "d-erable", "d-aurora", "d-horizon"}

	homonym := pipeline.MatchNamedEntityEvidence("How long does Harbor Registry retain manifests?", previews, order)
	assert.True(t, homonym.Ambiguous)
	assert.GreaterOrEqual(t, homonym.MatchedDocs, 2)

	negated := pipeline.MatchNamedEntityEvidence("Manifest archiving, not Harbor Registry North", previews, order)
	assert.True(t, negated.Negated)

	diacritic := pipeline.MatchNamedEntityEvidence("What humidity does Erable Central keep?", previews, order)
	assert.False(t, diacritic.Ambiguous)
	assert.False(t, diacritic.Negated)
	assert.Equal(t, "d-erable", diacritic.DocumentID)

	variant := pipeline.MatchNamedEntityEvidence("How long do Aurore Telemetry heartbeats run?", previews, order)
	assert.Equal(t, "d-aurora", variant.DocumentID)
	assert.True(t, variant.VariantMatch)

	mixedScript := pipeline.MatchNamedEntityEvidence("Horizon Terminalの運用手順", previews, order)
	assert.Equal(t, "d-horizon", mixedScript.DocumentID)

	absent := pipeline.MatchNamedEntityEvidence("Unrelated cryogenic sensor storage policy", previews, order)
	assert.Empty(t, absent.DocumentID)
	assert.True(t, absent.NegativeChecked)
}

func TestDetermineModeWithQuery(t *testing.T) {
	ret := retriever.NewRetriever(nil, nil, 0.70, 10)

	tests := []struct {
		name          string
		subQueryCount int
		topScore      float64
		namedEntity   string
		want          retriever.RetrievalMode
	}{
		{"multi sub-queries -> GLOBAL", 3, 0.90, "", retriever.RetrievalModeGlobal},
		{"named entity single query -> LOCAL", 1, 0.30, "cedar archive", retriever.RetrievalModeLocal},
		{"high score no entity -> LOCAL", 1, 0.85, "", retriever.RetrievalModeLocal},
		{"low score no entity -> GLOBAL", 1, 0.50, "", retriever.RetrievalModeGlobal},
		{"at threshold -> LOCAL", 1, 0.70, "", retriever.RetrievalModeLocal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ret.DetermineModeWithQuery(tt.subQueryCount, tt.topScore, tt.namedEntity)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMatchNamedEntityEvidenceExclusionScope(t *testing.T) {
	previews := map[string]string{"maple": "Maple Station.", "cedar": "Cedar Archive."}
	order := []string{"maple", "cedar"}
	for _, tt := range []struct {
		query   string
		negated bool
	}{
		{"Can I enter Cedar Archive without a badge?", false},
		{"Is entry without a badge permitted at Cedar Archive?", false},
		{"Which material is excluded from the Cedar Archive collection?", false},
		{"What are the opening hours, excluding Cedar Archive?", true},
		{"What are the opening hours excluding Cedar Archive?", true},
		{"What are the hours except those in Cedar Archive?", true},
		{"Cedar Archive excluded from retention rules", true},
		{"Do not include Cedar Archive.", true},
		{"Don't include Cedar Archive.", true},
		{"Don\u2019t include Cedar Archive.", true},
		{"Not including Cedar Archive.", true},
		{"Cedar Archive isn't included.", true},
		{"Don't omit Cedar Archive.", false},
		{"Don\u2019t omit Cedar Archive.", false},
		{"Cedar Archive isn't excluded.", false},
		{"Cedar Archive isn\u2019t open on Mondays.", false},
		{"Cedar Archive doesn't include a badge desk.", false},
		{"Omit staff rules but include Cedar Archive.", false},
	} {
		got := pipeline.MatchNamedEntityEvidence(tt.query, previews, order)
		assert.Equal(t, tt.negated, got.Negated, tt.query)
		if !tt.negated {
			assert.Equal(t, "cedar", got.DocumentID, tt.query)
		}
	}
}

func TestMatchNamedEntityEvidenceNumberedTitles(t *testing.T) {
	previews := map[string]string{"a": "Apollo 11. Weekend hours.", "b": "Apollo 11. Weekday hours.", "c": "Apollo 13. Weekday hours."}
	both := pipeline.MatchNamedEntityEvidence("Apollo 11 hours?", previews, []string{"a", "b"})
	assert.True(t, both.Ambiguous)
	assert.Equal(t, 2, both.MatchedDocs)

	one := pipeline.MatchNamedEntityEvidence("Apollo 11 hours?", previews, []string{"b", "c"})
	assert.False(t, one.Ambiguous)
	assert.Equal(t, "b", one.DocumentID)

	none := pipeline.MatchNamedEntityEvidence("11 hours?", previews, []string{"a", "b", "c"})
	assert.Empty(t, none.DocumentID)
	assert.False(t, none.Ambiguous)
}
