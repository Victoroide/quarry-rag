package pipeline

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/textutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumberReferenceBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name, format string
		a, b         int
		meaningful   bool
	}{
		{"hyphen before prose", "Page 3 - %d pallet positions have been marked.", 48, 49, true},
		{"en dash before prose", "Page 3 – %d pallet positions have been marked.", 48, 49, true},
		{"em dash after plural label", "Pages 3 — %d pallet positions have been marked.", 48, 49, true},
		{"temperature after dash", "Page 3 – %d°C was the measured temperature.", 48, 49, true},
		{"four digit copyright neighbor", "Copyright 2024 — %d Pallet positions are reserved for handling.", 2048, 2049, true},
		{"attached unit after connector", "Page 3 of %dkg of cargo remained dry.", 48, 49, true},
		{"dotted page", "See page 3.%d of the field manual for intake rules.", 1, 2, false},
		{"singular em dash range", "See page 20 — %d of the field manual.", 24, 25, false},
		{"spaced abbreviated copyright", "Copyright 2019 – %d Example Company", 24, 25, false},
		{"compact abbreviated copyright", "Copyright 2019—%d Example Company", 24, 25, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertNumberReferenceDifference(t, fmt.Sprintf(tt.format, tt.a), fmt.Sprintf(tt.format, tt.b), tt.meaningful)
		})
	}

	for _, separator := range []string{"\n", "\r\n", "\r", "\u0085", "\u2028", "\u2029"} {
		t.Run(fmt.Sprintf("ingest %q", separator), func(t *testing.T) {
			format := "Page 3" + separator + "Of %d pallet positions marked, six remained empty."
			a, b := fmt.Sprintf(format, 48), fmt.Sprintf(format, 49)
			assertNumberReferenceDifference(t, a, b, true)
			assertNumberReferenceDifference(t, textutil.NormalizeForIngest(a), textutil.NormalizeForIngest(b), true)
			assertNormalizedNumberSelection(t, a, b, "variant")
		})
	}
	t.Run("genuine total selection control", func(t *testing.T) {
		assertNormalizedNumberSelection(t, "Page 3 of 48.", "Page 3 of 49.", "other")
	})
}

const numberPairPrefix = "Inspectors reviewed the intake logs and confirmed that the shipment met the handling requirements. "

func assertNormalizedNumberSelection(t *testing.T, a, b string, wantSecond domain.ChunkID) {
	t.Helper()
	chunks := []domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: "first", DocumentID: "doc-a", Text: textutil.NormalizeForIngest(numberPairPrefix + a)}, Score: .99},
		{Chunk: domain.Chunk{ID: "variant", DocumentID: "doc-b", Text: textutil.NormalizeForIngest(numberPairPrefix + b)}, Score: .95},
		{Chunk: domain.Chunk{ID: "other", DocumentID: "doc-c", Text: "Only rail deliveries require a signed receipt."}, Score: .70},
	}
	selected, _, err := trimChunksForSynthesis(t.Context(), chunks, 1000, .55, config.SynthesisContextPolicyDiverse)
	require.NoError(t, err)
	require.Len(t, selected, len(chunks))
	assert.Equal(t, wantSecond, selected[1].Chunk.ID)
}

func assertNumberReferenceDifference(t *testing.T, a, b string, want bool) {
	t.Helper()
	const tail = " Keep this copy with the files in the records office."
	a, b = sanitizeExcerpt(numberPairPrefix+a), sanitizeExcerpt(numberPairPrefix+b)
	assert.Equal(t, want, meaningBearingDifference(a, b, false), "uncut")
	assert.Equal(t, want, meaningBearingDifference(b, a, false), "uncut reversed")
	assert.Equal(t, want, meaningBearingDifference(a, b+tail, true), "cut")
	assert.Equal(t, want, meaningBearingDifference(b, a+tail, true), "cut reversed")
}

func TestNumberGrammarInContext(t *testing.T) {
	type classified struct {
		runs  string
		class numberClass
	}
	check := func(t *testing.T, text string, want ...classified) {
		t.Helper()
		// Compose each production at both line boundaries and with prose on
		// either side. Assert the class and membership of every run.
		for _, prefix := range []string{"", "The archivist filed the records. "} {
			for _, suffix := range []string{"", "; copies are held in storage."} {
				s := extractMeaningSignals(sanitizeExcerpt(prefix + text + suffix))
				got := make([]classified, 0, len(s.tokens))
				for _, token := range s.tokens {
					got = append(got, classified{strings.Join(token.runs, " "), token.class})
				}
				assert.Equal(t, want, got, "%q", prefix+text+suffix)
			}
		}
	}

	for _, tt := range []struct {
		text string
		want []classified
	}{
		{"table3.1", []classified{{"3 1", numberValue}}},
		{"Article 7", []classified{{"7", numberValue}}},
		{"year 2024", []classified{{"2024", numberValue}}},
		{"Homepage 3", []classified{{"3", numberValue}}},
		{"page\n3", []classified{{"3", numberValue}}},
		{"there are 48\nページ", []classified{{"48", numberValue}}},
		{"48°C", []classified{{"48", numberQuantity}}},
		{"４８ kg", []classified{{"48", numberQuantity}}},
		{"48μs", []classified{{"48", numberQuantity}}},
		{"48‰", []classified{{"48", numberQuantity}}},
		{"-48.5%", []classified{{"48 5", numberQuantity}}},
		{"€1,248.50", []classified{{"1 248 50", numberQuantity}}},
		{"48 €", []classified{{"48", numberQuantity}}},
		{"2026-03-15", []classified{{"2026 03 15", numberDate}}},
		{"15/03/2026", []classified{{"15 03 2026", numberDate}}},
		{"2026.03.15", []classified{{"2026 03 15", numberDate}}},
		{"15 March 2026", []classified{{"15", numberDate}, {"2026", numberDate}}},
		{"v3.1.5", []classified{{"3 1 5", numberVersion}}},
		{"Version3.1", []classified{{"3 1", numberVersion}}},
		{"Version 2026-03-15", []classified{{"2026 03 15", numberVersion}}},
		{"ＰＡＧＥ：＃３．１", []classified{{"3 1", numberPage}}},
		{"p.3.1", []classified{{"3 1", numberPage}}},
		{"page 2026.03.15", []classified{{"2026 03 15", numberPage}}},
		{"Seite ３.１ von １０", []classified{{"3 1 10", numberPage}}},
		{"Pág. 3 de 10", []classified{{"3 10", numberPage}}},
		{"第 3.1 / 10 頁", []classified{{"3 1 10", numberPage}}},
		{"Page 3 of 48 in this volume contains the table.", []classified{{"3 48", numberPage}}},
		{"Page 3 of 48 describes the checks.", []classified{{"3 48", numberPage}}},
		{"See Page 3 of 48 and the diagram above.", []classified{{"3 48", numberPage}}},
		{"See Page 3.1 L of the handbook.", []classified{{"3 1", numberPage}}},
		{"See Page 3 L in this volume.", []classified{{"3", numberPage}}},
		{"See Page 3L of the handbook.", []classified{{"3", numberQuantity}}},
		{"See Page 3 l of the handbook.", []classified{{"3", numberQuantity}}},
		{"See Page 3 L of the handbook's solvent.", []classified{{"3", numberQuantity}}},
		{"See Page 3 L of water.", []classified{{"3", numberQuantity}}},
		{"3.1ページを参照", []classified{{"3 1", numberPage}}},
		{"— 3.1 —", []classified{{"3 1", numberPage}}},
		{"Page 3 of 10 of 48 crates arrived.", []classified{{"3 10", numberPage}, {"48", numberValue}}},
		{"Page 3 of 24% was recovered.", []classified{{"3", numberPage}, {"24", numberQuantity}}},
		{"Page 3 of 48,000 kg was recovered.", []classified{{"3", numberPage}, {"48 000", numberQuantity}}},
		{"Page 3/10/2048 crates arrived.", []classified{{"3 10", numberPage}, {"2048", numberValue}}},
		{"Page 3/10.48 crates arrived.", []classified{{"3 10", numberPage}, {"48", numberValue}}},
		{"Page 3/10:section 4.2 governs.", []classified{{"3 10", numberPage}, {"4 2", numberValue}}},
		{"Page 3/10:version 3.2 remains supported.", []classified{{"3 10", numberPage}, {"3 2", numberVersion}}},
		{"Page 3/10:$48 was refunded.", []classified{{"3 10", numberPage}, {"48", numberQuantity}}},
		{"Page 3/10:48kg was recovered.", []classified{{"3 10", numberPage}, {"48", numberQuantity}}},
		{"Copyright2024", []classified{{"2024", numberCopyright}}},
		{"(C) ２０２４", []classified{{"2024", numberCopyright}}},
		{"© 2019 – 24 example company", []classified{{"2019 24", numberCopyright}}},
		{"© 2024—2048 Pallet positions are reserved.", []classified{{"2024", numberCopyright}, {"2048", numberValue}}},
		{"© 2024—48°C", []classified{{"2024", numberCopyright}, {"48", numberQuantity}}},
	} {
		t.Run(tt.text, func(t *testing.T) { check(t, tt.text, tt.want...) })
	}

	for _, dash := range []string{"-", "–", "—"} {
		for _, spacing := range [][2]string{{"", ""}, {" ", ""}, {"", " "}, {" ", "　"}} {
			separator := spacing[0] + dash + spacing[1]
			for _, g := range []struct{ prefix, suffix string }{
				{"Page ", " of the manual"}, {"Pages ", " in the handbook"}, {"Página ", " de la guía"},
				{"Seite ", " im Handbuch"}, {"Pagine ", " nel capitolo"}, {"Page ", " dans le manuel"},
				{"第 ", " 页"}, {"", "ページ"}, {"", "페이지"}, {dash + " ", " " + dash},
			} {
				check(t, g.prefix+"２０"+separator+"２４"+g.suffix, classified{"20 24", numberPage})
			}
			for _, end := range []string{"24", "2024"} {
				for _, name := range []string{
					" Example Company", " The University of California. All rights reserved.",
					" state of ohio. All rights reserved.", " adidas. All rights reserved.",
					" all rights reserved.", ", all rights reserved.", " adidas; all rights reserved.",
					" van der Meer, LLC. All rights reserved.",
					" Union of Research Libraries.", " Éditions du Nord-Ouest.", " Acme.",
				} {
					check(t, "(C) 2019"+separator+end+name, classified{"2019 " + end, numberCopyright})
				}
			}
			for _, prose := range []string{
				" Pallets, only six were wet.", " Pallets: Six were wet.", " Pallets; six were wet.",
				" Pallets. Only six were wet.", " Pallet Positions Were Reserved.",
				" Pallets for the Foundation, only six were wet.",
			} {
				check(t, "Ｃｏｐｙｒｉｇｈｔ ２０２４"+separator+"２０４８"+prose,
					classified{"2024", numberCopyright}, classified{"2048", numberValue})
			}
			for _, shape := range []struct{ head, tail, digits string }{
				{"See pages 20", "24 for safety procedures.", "20 24"},
				{"See pages 20.1", "24.2 inclusive for the complete procedure.", "20 1 24 2"},
				{"Consult p. 20", "24 about safe handling.", "20 24"},
				{"Siehe Seite 20", "24 im zweiten Band.", "20 24"},
				{"Véase pág. 20.1", "24.2 para instrucciones.", "20 1 24 2"},
				{"See pages 20", "24 L of the handbook.", "20 24"},
				{"Pages 20", "24/50. The records were filed.", "20 24 50"},
			} {
				check(t, shape.head+separator+shape.tail, classified{shape.digits, numberPage})
			}
		}
		for _, label := range []string{"Page", "Pages", "Páginas", "Seiten"} {
			page := classified{"3", numberPage}
			prefix := label + " 3 " + dash + " "
			check(t, prefix+"48 pallets arrived.", page, classified{"48", numberValue})
			check(t, prefix+"48 of the pallets arrived.", page, classified{"48", numberValue})
			check(t, prefix+"48°C was measured.", page, classified{"48", numberQuantity})
			check(t, prefix+"48,000kg was recovered.", page, classified{"48 000", numberQuantity})
			check(t, prefix+"2026-03-15 was the deadline.", page, classified{"2026 03 15", numberDate})
			check(t, prefix+"48, plus six spaces for exceptions, were marked.", page, classified{"48", numberValue})
			check(t, prefix+"48: six positions were marked.", page, classified{"48", numberValue})
			for _, apostrophe := range []string{"'", "’", "ʼ"} {
				check(t, prefix+"48 of the report"+apostrophe+"s 50 recommendations were adopted.", page,
					classified{"48", numberValue}, classified{"50", numberValue})
			}
			check(t, prefix+"48 of the report recommendations were adopted.", page, classified{"48", numberValue})
			for _, slash := range []string{"/", " /", "/ ", " ／ "} {
				check(t, prefix+"48"+slash+"50 pallet positions were filled.", page, classified{"48 50", numberValue})
				check(t, prefix+"48"+slash+"50.5 positions were filled.", page, classified{"48 50 5", numberValue})
				check(t, prefix+"48"+slash+"50: six positions were filled.", page, classified{"48 50", numberValue})
				check(t, prefix+"48"+slash+"50% was recovered.", page, classified{"48 50", numberQuantity})
				check(t, prefix+"48"+slash+"50 kg was recovered.", page, classified{"48 50", numberQuantity})
			}
		}
	}
	for _, locator := range []struct{ label, connector, suffix string }{
		{"Page ", "of", ""}, {"Pág. ", "de", ""}, {"Seite ", "von", ""}, {"Pagine ", "di", ""},
		{"Page ", "sur", ""}, {"第", "de", "頁"}, {"", "of", "ページ"}, {"", "of", "페이지"},
	} {
		label := locator.label + "3" + locator.suffix
		total := label + " " + locator.connector + " "
		page := classified{"3", numberPage}
		check(t, total+"48. The records were filed.", classified{"3 48", numberPage})
		check(t, total+"48: six positions remained empty.", page, classified{"48", numberValue})
		check(t, total+"48 of the report’s 50 recommendations were adopted.", page,
			classified{"48", numberValue}, classified{"50", numberValue})
		check(t, total+"48 pallets the inspectors had marked remained empty.", page, classified{"48", numberValue})
		check(t, total+"48%", page, classified{"48", numberQuantity})
		check(t, total+"48 L of water.", page, classified{"48", numberQuantity})
		for _, slash := range []string{"/", " /", "/ ", " ／ "} {
			check(t, total+"48"+slash+"50 positions remained empty.", page, classified{"48 50", numberValue})
			check(t, total+"48"+slash+"50 kg was recovered.", page, classified{"48 50", numberQuantity})
			check(t, label+slash+"48. The records were filed.", classified{"3 48", numberPage})
		}
	}
}

func TestNumberReferenceEndpointEdits(t *testing.T) {
	for _, tt := range []struct {
		a, b string
		want bool
	}{
		{"Ｐａｇｅｓ ２０ — ２４ of the guide", "Ｐａｇｅｓ ２１ — ２４ of the guide", false},
		{"Seite ２０ — ２４ im Handbuch", "Seite ２０ — ２５ im Handbuch", false},
		{"第 ２０ – ２４ 頁に記載", "第 ２１ – ２４ 頁に記載", false},
		{"２０ — ２４ ページに記載", "２０ — ２５ ページに記載", false},
		{"See pages 20.1 — 24.2 inclusive for the procedure.", "See pages 20.3 — 24.2 inclusive for the procedure.", false},
		{"See pages 20.1 — 24.2 inclusive for the procedure.", "See pages 20.1 — 24.3 inclusive for the procedure.", false},
		{"See Page 48L of the handbook.", "See Page 49L of the handbook.", true},
		{"See Page 48 L of the handbook’s solvent.", "See Page 49 L of the handbook’s solvent.", true},
		{"Page 3 — 48 / 50 pallet positions were filled.", "Page 3 — 48 / 51 pallet positions were filled.", true},
		{"Page 3 of 48 / 50 pallet positions remained empty.", "Page 3 of 48 / 51 pallet positions remained empty.", true},
		{"© ２０１９ — ２４ Example Company", "© ２０２０ — ２４ Example Company", false},
		{"Page 3/10:Section 4.2 explains the claim.", "Page 3/10:Section 5.2 explains the claim.", true},
		{"Page 3/10:Version 3.7 supports the setting.", "Page 3/10:Version 3.8 supports the setting.", true},
		{"Page 3/10:$48 was refunded for the claim.", "Page 3/10:$49 was refunded for the claim.", true},
		{"Page 3/10:48kg was recovered from the cargo.", "Page 3/10:49kg was recovered from the cargo.", true},
	} {
		t.Run(tt.a, func(t *testing.T) { assertNumberReferenceDifference(t, tt.a, tt.b, tt.want) })
	}
}
