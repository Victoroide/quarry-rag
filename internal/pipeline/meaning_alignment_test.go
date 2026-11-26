package pipeline

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestAlignedEditBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name, candidate, excerpt string
		want                     bool
	}{
		{"han at start", "不予批准", "予批准", true},
		{"han at position one", "甲不予批准", "甲予批准", true},
		{"han at end", "设备均可用非", "设备均可用", true},
		{"katakana width folding", "端末ﾒﾓ不許可一覧", "端末メモ許可一覧", true},
		{"han at start with up to three replacement runes", "不予批准", "应该能予批准", true},
		{"han at start with four replacement runes", "不予批准", "按规定应予批准", false},
		{"short left anchor must match prefix", "甲不予批准", "乙甲予批准", false},
		{"short right anchor must match suffix", "已否决不准", "已否决准入", false},
		{"number at start", "50 staff attended", "45 staff attended", true},
		{"number at end", "The limit is 20", "The limit is 10", true},
		{"number with no shared boundary", "50 timber pallets", "45 chemical drums", false},
		{"short bodies", "甲不乙", "甲乙", true},
		{"no shared anchors", "50", "45", false},
		{"empty bodies", "", "", false},
		{"empty excerpt", "不", "", false},
		{"spaces", "  \t", "  ", false},
		{"format controls", "\u200b\u200d", "\ufeff", false},
		{"term in a short body", "no", "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, meaningBearingDifference(tt.candidate, tt.excerpt, false))
		})
	}
}

func TestIncidentalNumbers(t *testing.T) {
	for _, tt := range []struct {
		a, b string
		want bool
	}{
		{"Page: #3 of 10", "Page: #4 of 12", false},
		{"Pages 3/10", "Pages 4/12", false},
		{"Página 3 de 10", "Página 4 de 12", false},
		{"Páginas 3 de 10", "Páginas 4 de 12", false},
		{"Pág. 3 de 10", "Pág. 4 de 12", false},
		{"Pag. 3 di 10", "Pag. 4 di 12", false},
		{"Pagina 3 di 10", "Pagina 4 di 12", false},
		{"Pagine 3 di 10", "Pagine 4 di 12", false},
		{"Seite 3 von 10", "Seite 4 von 12", false},
		{"Seiten 3 von 10", "Seiten 4 von 12", false},
		{"S. 3 von 10", "S. 4 von 12", false},
		{"Page 3 sur 10", "Page 4 sur 12", false},
		{"P. 3", "P. 4", false},
		{"PP. 3", "PP. 4", false},
		{"Pg. 3", "Pg. 4", false},
		{"ＰＡＧＥ ３", "ＰＡＧＥ ４", false},
		{"第3页", "第4页", false},
		{"第3頁", "第4頁", false},
		{"3 頁", "4 頁", false},
		{"3 ページ", "4 ページ", false},
		{"3페이지", "4페이지", false},
		{"3쪽", "4쪽", false},
		{"- 3 -", "- 4 -", false},
		{"– 3 –", "– 4 –", false},
		{"— 3 —", "— 4 —", false},
		{"— 3 -", "— 4 -", true},
		{"© 2025", "© 2024", false},
		{"(C) 2025", "(C) 2024", false},
		{"Copyright 2020-2025", "Copyright 2020-2024", false},
		{"Homepage 3", "Homepage 4", true},
		{"Section 3 of 10", "Section 3 of 12", true},
	} {
		t.Run(tt.a, func(t *testing.T) {
			const prefix = "Meeting minutes follow the agenda. "
			const suffix = " | records office"
			require.Equal(t, tt.want, meaningBearingDifference(prefix+tt.a+suffix, prefix+tt.b+suffix, false))
		})
	}
}

func TestMeaningBearingCaps(t *testing.T) {
	const unrelated = "Agenda will be shared at the morning meeting."
	for _, n := range []int{16, 17} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			var months strings.Builder
			for i := range n {
				fmt.Fprintf(&months, "库存登记记录设备 %d 台，已及时提交。", 80+i)
			}
			require.Equal(t, n > 16, meaningBearingDifference(months.String(), unrelated, true))
			require.Equal(t, n > 16, meaningBearingDifference(strings.Repeat("非常感谢您的帮助。", n), unrelated, true))
			require.False(t, meaningBearingDifference(strings.Repeat("Page 3. ", n), unrelated, true))
			require.False(t, meaningBearingDifference(unrelated, months.String(), true))
		})
	}
}

func FuzzMeaningBearingDifference(f *testing.F) {
	f.Add("", "", false)
	f.Add("已登记人员不获批准。", "已登记人员获批准。", false)
	f.Add("Contract expires on 2026-03-15.", "Contract expires on 2026-04-15.", true)
	f.Add("賛成しない。 \u200b Page 3.", "賛成する。 Page 2.", false)
	f.Add("Ⅷ ５０ \xff", "50", true)
	f.Fuzz(func(t *testing.T, a, b string, cut bool) {
		if len(a)+len(b) > 16<<10 {
			t.Skip()
		}
		require.False(t, meaningBearingDifference(a, a, cut))
		if !cut {
			require.Equal(t, meaningBearingDifference(a, b, false), meaningBearingDifference(b, a, false))
		}
		cand, exc := extractMeaningSignals(a), extractMeaningSignals(b)
		want := hasMeaningBearingSignalDiff(&cand, &exc, cut)
		require.Equal(t, meaningBearingDifference(a, b, cut), want)
		require.Equal(t, want, hasMeaningBearingSignalDiff(&cand, &exc, cut))
	})
}

func BenchmarkDiverseSelectionNearDuplicates(b *testing.B) {
	const section = "The regional receiving team records each shipment on arrival and reviews the intake entry with the depot supervisor. The signed manifest lists %d containers for handling. "
	in := make([]domain.ScoredChunk, 100)
	for i := range in {
		var body strings.Builder
		for j := range 12 {
			fmt.Fprintf(&body, section, 100+i*12+j)
		}
		id := fmt.Sprintf("doc-%d", i)
		in[i] = domain.ScoredChunk{
			Chunk: domain.Chunk{ID: domain.ChunkID(id), DocumentID: domain.DocumentID(id), Text: body.String()},
			Score: .9,
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := trimChunksForSynthesis(b.Context(), in, 2400, .55, config.SynthesisContextPolicyDiverse); err != nil {
			b.Fatal(err)
		}
	}
}
