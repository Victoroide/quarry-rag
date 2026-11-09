package pipeline

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestVerifyCitations(t *testing.T) {
	for _, tc := range []struct {
		name, answer, cleaned string
		excerpts              []string
		numbers, offsets      []int
		overlaps              []float64
	}{
		{"valid", "Archive records persist [1].", "Archive records persist [1].", []string{"archive records persist"}, []int{1}, []int{24}, []float64{1}},
		{"range and rune offsets", "é [9] Archive [0,1,3] [2].", "é Archive [1] [2].", []string{"archive", "archive"}, []int{1, 2}, []int{10, 14}, []float64{1, 1}},
		{"lists and adjacent", "Archive records [1, 2][1].", "Archive records [1,2][1].", []string{"archive", "records"}, []int{1, 2, 1}, []int{16, 16, 21}, []float64{.5, .5, .5}},
		{"duplicate numbers", "Archive [1,1]", "Archive [1]", []string{"archive"}, []int{1}, []int{8}, []float64{1}},
		{"NFC and case", "CAFÉ cafe\u0301 records [1]", "CAFÉ cafe\u0301 records [1]", []string{"cafe\u0301"}, []int{1}, []int{19}, []float64{.5}},
		{"digits and short words", "a an v2 7 cat [1]", "a an v2 7 cat [1]", []string{"v2 cat"}, []int{1}, []int{14}, []float64{.67}},
		{"verification threshold", "one two three four five six seven eight nine ten [1]", "one two three four five six seven eight nine ten [1]", []string{"one two three"}, []int{1}, []int{49}, []float64{.3}},
		{"below verification threshold", "one two three four five six seven [1]", "one two three four five six seven [1]", []string{"one two"}, []int{1}, []int{34}, []float64{.29}},
		{"dense bigrams", "档案记录 [1]", "档案记录 [1]", []string{"档案保存"}, []int{1}, []int{5}, []float64{.33}},
		{"cross language", "Archive records [1]", "Archive records [1]", []string{"档案记录"}, []int{1}, []int{16}, []float64{0}},
		{"empty sentence", ". [1]", ". [1]", []string{"archive"}, []int{1}, []int{2}, []float64{0}},
		{"next sentence", "Archive records [1]. Unrelated words [2].", "Archive records [1]. Unrelated words [2].", []string{"archive records", "archive records"}, []int{1, 2}, []int{16, 37}, []float64{1, 0}},
		{"combining marks", "दस्तावेज़ सुरक्षित [1]", "दस्तावेज़ सुरक्षित [1]", []string{"दस्तावेज़"}, []int{1}, []int{19}, []float64{.5}},
		{"prolonged sound mark", "コーヒーサーバー [1]", "コーヒーサーバー [1]", []string{"コーヒー"}, []int{1}, []int{9}, []float64{.43}},
		{"format characters", "کتاب\u200cها [1]", "کتاب\u200cها [1]", []string{"کتابها"}, []int{1}, []int{8}, []float64{1}},
		{"line boundary", "Unrelated words\nArchive [1]", "Unrelated words\nArchive [1]", []string{"archive"}, []int{1}, []int{24}, []float64{1}},
		{"full width boundary", "不相关。档案 [1]", "不相关。档案 [1]", []string{"档案"}, []int{1}, []int{7}, []float64{1}},
		{"punctuation boundary", "Unrelated! Archive? Records [1]", "Unrelated! Archive? Records [1]", []string{"records"}, []int{1}, []int{28}, []float64{1}},
		{"no markers", "Archive [1000] [x]", "Archive [1000] [x]", nil, nil, nil, nil},
		{"no excerpts", "Archive [1] [2]", "Archive", nil, nil, nil, nil},
		{"remove one space", "Archive  [9]", "Archive ", nil, nil, nil, nil},
		{"new marker after removal", "Archive [[99]1]", "Archive [1]", []string{"archive"}, []int{1}, []int{8}, []float64{1}},
		{"nested removals", "Archive [9[99]9]", "Archive", []string{"archive"}, nil, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			excerpts := make([]domain.ScoredChunk, len(tc.excerpts))
			for i, text := range tc.excerpts {
				excerpts[i].Chunk = domain.Chunk{DocumentID: domain.DocumentID("source"), Index: i + 4, Text: text}
			}
			cleaned, citations := VerifyCitations(tc.answer, excerpts)
			require.Equal(t, tc.cleaned, cleaned)
			require.NotNil(t, citations)
			require.Len(t, citations, len(tc.numbers))
			for i, citation := range citations {
				require.Equal(t, tc.numbers[i], citation.Excerpt)
				require.Equal(t, domain.DocumentID("source"), citation.DocumentID)
				require.Equal(t, tc.numbers[i]+3, citation.ChunkIndex)
				require.Equal(t, tc.offsets[i], citation.Offset)
				require.Equal(t, tc.overlaps[i], citation.Overlap)
				require.Equal(t, tc.overlaps[i] >= .3, citation.Verified)
			}
			again, second := VerifyCitations(cleaned, excerpts)
			require.Equal(t, cleaned, again)
			require.Equal(t, citations, second)
		})
	}
}

func TestVerifyCitationsDenseScripts(t *testing.T) {
	for _, text := range []string{"档案记", "あいう", "アイウ", "가나다", "กขค", "ກຂຄ", "កខគ", "ကခဂ", "ཀཁག"} {
		t.Run(text, func(t *testing.T) {
			excerpt := string([]rune(text)[:2])
			_, citations := VerifyCitations(text+" [1]", []domain.ScoredChunk{{Chunk: domain.Chunk{Text: excerpt}}})
			require.Len(t, citations, 1)
			require.Equal(t, .5, citations[0].Overlap)
			require.True(t, citations[0].Verified)
		})
	}
}
