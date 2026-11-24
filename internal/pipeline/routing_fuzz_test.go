package pipeline

import (
	"testing"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/retriever"
)

func FuzzMatchNamedEntityEvidence(f *testing.F) {
	f.Add("Cedar Archive retention", "Cedar Archive stores technical records.", "Cedar Archive North stores records.")
	f.Add("Erable Central", "Érable Central stores archival records.", "Maple Station stores logs.")
	f.Add("Policies excluding Cedar Archive", "Cedar Archive stores records.", "Maple Station stores logs.")
	f.Add("Is entry without a badge permitted at Cedar Archive?", "Cedar Archive stores records.", "Maple Station stores logs.")
	f.Add("Apollo 11 hours?", "Apollo 11. Weekend hours.", "Apollo 11. Weekday hours.")
	f.Add("Cedar Archiveの記録について", "Cedar Archive stores records.", "東京アーカイブ")
	f.Add("¿Cuál es el horario? ¿Dónde está?", "Archivo Central", "Informe Técnico")
	f.Add("\u202eＣｅｄａｒ\u200b Ａｒｃｈｉｖｅ\x00", "Cedar Archive", "")
	f.Fuzz(func(t *testing.T, query, previewA, previewB string) {
		if len(query)+len(previewA)+len(previewB) > 16<<10 {
			t.Skip()
		}
		previews := map[string]string{"doc-a": previewA, "doc-b": previewB, "outside-scope": query}
		order := []string{"doc-a", "doc-b"}
		match1 := MatchNamedEntityEvidence(query, previews, order)
		match2 := MatchNamedEntityEvidence(query, previews, order)
		if match1 != match2 {
			t.Fatalf("non-deterministic match: %+v != %+v", match1, match2)
		}
		reversed := MatchNamedEntityEvidence(query, previews, []string{"doc-b", "doc-a"})
		if match1 != reversed {
			t.Fatalf("match changes with candidate order: %+v != %+v", match1, reversed)
		}
		if match1.DocumentID != "" && match1.DocumentID != "doc-a" && match1.DocumentID != "doc-b" {
			t.Fatalf("unauthorized document ID returned: %q", match1.DocumentID)
		}
		if match1.Ambiguous && match1.DocumentID != "" {
			t.Fatalf("ambiguous match selected a local document: %+v", match1)
		}
		if match1.Negated && match1.DocumentID != "" {
			t.Fatalf("negated match selected a local document: %+v", match1)
		}
		if utf8.ValidString(query) && utf8.ValidString(previewA) && utf8.ValidString(previewB) && !utf8.ValidString(match1.Entity) {
			t.Fatalf("invalid UTF-8 entity output: %q", match1.Entity)
		}
		signals := analyzeQuerySignals(query, nil)
		ret := retriever.NewRetriever(nil, nil, 0.7, 10)
		docs := []retriever.DocumentCandidate{{DocID: "doc-a", BestScore: 0.9}, {DocID: "doc-b", BestScore: 0.5}}
		decision := ret.DetermineEvidenceRoute(retriever.RoutingConfig{Policy: retriever.RoutingPolicyEvidence, Margin: 0.08, Calibrated: true}, signals, docs, retriever.EntityDecision{
			DocumentID: match1.DocumentID,
			Ambiguous:  match1.Ambiguous,
			Negated:    match1.Negated,
		})
		if !retriever.ValidRouteReason(decision.Reason) {
			t.Fatalf("unbounded route reason: %q", decision.Reason)
		}
		if decision.Mode == retriever.RetrievalModeGlobal && decision.LocalDocumentID != "" {
			t.Fatalf("global decision leaked local document ID: %+v", decision)
		}
	})
}
