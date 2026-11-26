package ocr

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/stretchr/testify/require"
)

type evalTable struct {
	title     string
	relations []string // normalized "header: value" pairs
}

type layoutEvalFixture struct {
	name          string
	slice         string // "golden" or "heldout"
	blocks        []types.Block
	pages         int32
	wantPartial   bool
	wantReason    string
	orderUnits    []string    // expected reading-order tokens/phrases
	criticalPairs [][2]string // ordered pairs (a before b)
	tables        []evalTable
}

// normalizedLevenshteinEditDistance computes ReadOrderEdit in [0, 1].
func normalizedLevenshteinEditDistance(expected, actual []string) float64 {
	n, m := len(expected), len(actual)
	if n == 0 && m == 0 {
		return 0
	}
	dp := make([][]int, n+1)
	for i := range dp {
		dp[i] = make([]int, m+1)
		dp[i][0] = i
	}
	for j := range m + 1 {
		dp[0][j] = j
	}
	for i := 1; i <= n; i++ {
		for j := 1; j <= m; j++ {
			cost := 1
			if expected[i-1] == actual[j-1] {
				cost = 0
			}
			dp[i][j] = min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+cost)
		}
	}
	return float64(dp[n][m]) / float64(max(n, m))
}

func plainGeometricText(blocks []types.Block) string {
	var lines []detectedLine
	for _, b := range blocks {
		if b.BlockType != types.BlockTypeLine {
			continue
		}
		var top, left float32
		if b.Geometry != nil && b.Geometry.BoundingBox != nil {
			top, left = b.Geometry.BoundingBox.Top, b.Geometry.BoundingBox.Left
		}
		t := strings.TrimSpace(aws.ToString(b.Text))
		if t != "" {
			lines = append(lines, detectedLine{page: aws.ToInt32(b.Page), top: top, left: left, text: t})
		}
	}
	slices.SortStableFunc(lines, func(a, b detectedLine) int {
		if n := cmp.Compare(a.page, b.page); n != 0 {
			return n
		}
		if n := cmp.Compare(a.top, b.top); n != 0 {
			return n
		}
		return cmp.Compare(a.left, b.left)
	})
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = l.text
	}
	return strings.Join(out, "\n")
}

func extractUnitOrder(text string, units []string) []string {
	type hit struct {
		pos  int
		unit string
	}
	var hits []hit
	for _, u := range units {
		if idx := strings.Index(text, u); idx >= 0 {
			hits = append(hits, hit{pos: idx, unit: u})
		}
	}
	slices.SortStableFunc(hits, func(a, b hit) int {
		return cmp.Compare(a.pos, b.pos)
	})
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.unit
	}
	return out
}

func tableStructureScore(text string, expected []evalTable) (score float64, lostCells int) {
	if len(expected) == 0 {
		return 1, 0
	}
	total, matched := 0, 0
	for _, tbl := range expected {
		if tbl.title != "" {
			total++
			if strings.Contains(text, tbl.title) {
				matched++
			}
		}
		for _, rel := range tbl.relations {
			total++
			if strings.Contains(text, rel) {
				matched++
			} else {
				lostCells++
			}
		}
	}
	if total == 0 {
		return 1, 0
	}
	return float64(matched) / float64(total), lostCells
}

func goldenLayoutFixtures() []layoutEvalFixture {
	// 1. Two-column fixture where geometric (top, left) interleaves columns.
	twoCol := layoutEvalFixture{
		name:  "two-column",
		slice: "golden",
		pages: 1,
		blocks: []types.Block{
			blk("p1", types.BlockTypePage, 1),
			withRels(blk("lt1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l1", "l2")),
			withRels(blk("lt2", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l3", "l4")),
			bbox(blkText("l1", types.BlockTypeLine, 1, "Left column intro"), 0.1, 0.05, 0.4, 0.05),
			bbox(blkText("l2", types.BlockTypeLine, 1, "Left column conclusion"), 0.2, 0.05, 0.4, 0.05),
			bbox(blkText("l3", types.BlockTypeLine, 1, "Right column start"), 0.1, 0.55, 0.4, 0.05),
			bbox(blkText("l4", types.BlockTypeLine, 1, "Right column end"), 0.2, 0.55, 0.4, 0.05),
		},
		orderUnits: []string{"Left column intro", "Left column conclusion", "Right column start", "Right column end"},
		criticalPairs: [][2]string{
			{"Left column intro", "Left column conclusion"},
			{"Left column conclusion", "Right column start"},
			{"Right column start", "Right column end"},
		},
	}

	// 2. RTL fixture (Arabic two-column + labeled table).
	rtl := layoutEvalFixture{
		name:  "rtl",
		slice: "golden",
		pages: 1,
		blocks: []types.Block{
			blk("p1", types.BlockTypePage, 1),
			// Right column precedes left column in RTL reading order.
			withRels(blk("lt_r", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "lr1", "lr2")),
			withRels(blk("lt_l", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "ll1")),
			withRels(bbox(blk("lt_tbl", types.BlockTypeLayoutTable, 1), 0.4, 0.1, 0.8, 0.3), rel(types.RelationshipTypeChild, "tl_h1", "tl_h2", "tl_v1", "tl_v2")),
			bbox(blkText("lr1", types.BlockTypeLine, 1, "مقدمة التقرير السنوي"), 0.1, 0.55, 0.35, 0.05),
			bbox(blkText("lr2", types.BlockTypeLine, 1, "تفاصيل العمود الأيمن"), 0.2, 0.55, 0.35, 0.05),
			bbox(blkText("ll1", types.BlockTypeLine, 1, "ملاحظات العمود الأيسر"), 0.1, 0.05, 0.35, 0.05),
			bbox(withRels(blkText("tl_h1", types.BlockTypeLine, 1, "البند"), rel(types.RelationshipTypeChild, "wh1")), 0.4, 0.6, 0.2, 0.05),
			bbox(withRels(blkText("tl_h2", types.BlockTypeLine, 1, "القيمة"), rel(types.RelationshipTypeChild, "wh2")), 0.4, 0.2, 0.2, 0.05),
			bbox(withRels(blkText("tl_v1", types.BlockTypeLine, 1, "الإيرادات"), rel(types.RelationshipTypeChild, "wv1")), 0.5, 0.6, 0.2, 0.05),
			bbox(withRels(blkText("tl_v2", types.BlockTypeLine, 1, "١٠٠"), rel(types.RelationshipTypeChild, "wv2")), 0.5, 0.2, 0.2, 0.05),
			withRels(bbox(blk("tbl", types.BlockTypeTable, 1), 0.4, 0.1, 0.8, 0.3), rel(types.RelationshipTypeChild, "c1", "c2", "c3", "c4")),
			cellBlk("c1", 1, 1, 1, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "wh1"),
			cellBlk("c2", 1, 1, 2, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "wh2"),
			cellBlk("c3", 1, 2, 1, 1, 1, nil, "wv1"),
			cellBlk("c4", 1, 2, 2, 1, 1, nil, "wv2"),
			blkText("wh1", types.BlockTypeWord, 1, "البند"),
			blkText("wh2", types.BlockTypeWord, 1, "القيمة"),
			blkText("wv1", types.BlockTypeWord, 1, "الإيرادات"),
			blkText("wv2", types.BlockTypeWord, 1, "١٠٠"),
		},
		orderUnits:    []string{"مقدمة التقرير السنوي", "تفاصيل العمود الأيمن", "ملاحظات العمود الأيسر", "البند: الإيرادات", "القيمة: ١٠٠"},
		criticalPairs: [][2]string{{"مقدمة التقرير السنوي", "تفاصيل العمود الأيمن"}, {"تفاصيل العمود الأيمن", "ملاحظات العمود الأيسر"}},
		tables:        []evalTable{{relations: []string{"البند: الإيرادات", "القيمة: ١٠٠"}}},
	}

	// 3. Bilingual fixture (Spanish + Chinese).
	bilingual := layoutEvalFixture{
		name:  "bilingual",
		slice: "golden",
		pages: 1,
		blocks: []types.Block{
			blk("p1", types.BlockTypePage, 1),
			withRels(blk("lt1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l1")),
			withRels(blk("lt2", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l2")),
			withRels(bbox(blk("lt_tbl", types.BlockTypeLayoutTable, 1), 0.3, 0.1, 0.8, 0.3), rel(types.RelationshipTypeChild, "tl1", "tl2")),
			bbox(blkText("l1", types.BlockTypeLine, 1, "Informe bilingüe de operaciones"), 0.1, 0.1, 0.4, 0.05),
			bbox(blkText("l2", types.BlockTypeLine, 1, "双语运营报告摘要"), 0.1, 0.55, 0.4, 0.05),
			bbox(withRels(blkText("tl1", types.BlockTypeLine, 1, "Región / 地区"), rel(types.RelationshipTypeChild, "w1", "w2")), 0.3, 0.1, 0.6, 0.05),
			bbox(withRels(blkText("tl2", types.BlockTypeLine, 1, "Norte / 北区"), rel(types.RelationshipTypeChild, "w3", "w4")), 0.4, 0.1, 0.6, 0.05),
			withRels(bbox(blk("tbl", types.BlockTypeTable, 1), 0.3, 0.1, 0.8, 0.3), rel(types.RelationshipTypeChild, "c1", "c2", "c3", "c4")),
			cellBlk("c1", 1, 1, 1, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w1"),
			cellBlk("c2", 1, 1, 2, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w2"),
			cellBlk("c3", 1, 2, 1, 1, 1, nil, "w3"),
			cellBlk("c4", 1, 2, 2, 1, 1, nil, "w4"),
			blkText("w1", types.BlockTypeWord, 1, "Región"),
			blkText("w2", types.BlockTypeWord, 1, "Estado"),
			blkText("w3", types.BlockTypeWord, 1, "Norte"),
			blkText("w4", types.BlockTypeWord, 1, "Activo"),
		},
		orderUnits:    []string{"Informe bilingüe de operaciones", "双语运营报告摘要", "Región: Norte", "Estado: Activo"},
		criticalPairs: [][2]string{{"Informe bilingüe de operaciones", "双语运营报告摘要"}},
		tables:        []evalTable{{relations: []string{"Región: Norte", "Estado: Activo"}}},
	}

	// 4. Rotated fixture (RotationAngle 90 on lines).
	rot90 := float32(90)
	rotated := layoutEvalFixture{
		name:        "rotated",
		slice:       "golden",
		pages:       1,
		wantPartial: true,
		wantReason:  "rotated_text",
		blocks: []types.Block{
			blk("p1", types.BlockTypePage, 1),
			withRels(blk("lt1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l1")),
			withRels(blk("lt2", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l2")),
			withRels(bbox(blk("lt_tbl", types.BlockTypeLayoutTable, 1), 0.3, 0.2, 0.6, 0.4), rel(types.RelationshipTypeChild, "tl1")),
			{Id: aws.String("l1"), BlockType: types.BlockTypeLine, Page: aws.Int32(1), Text: aws.String("Rotated header note"), Geometry: &types.Geometry{RotationAngle: &rot90, BoundingBox: &types.BoundingBox{Top: 0.5, Left: 0.1, Width: 0.05, Height: 0.3}}},
			{Id: aws.String("l2"), BlockType: types.BlockTypeLine, Page: aws.Int32(1), Text: aws.String("Rotated body clause"), Geometry: &types.Geometry{RotationAngle: &rot90, BoundingBox: &types.BoundingBox{Top: 0.2, Left: 0.2, Width: 0.05, Height: 0.3}}},
			withRels(bbox(blkText("tl1", types.BlockTypeLine, 1, "Gauge Pressure"), 0.3, 0.2, 0.4, 0.1), rel(types.RelationshipTypeChild, "w1", "w2")),
			withRels(bbox(blk("tbl", types.BlockTypeTable, 1), 0.3, 0.2, 0.6, 0.4), rel(types.RelationshipTypeChild, "c1", "c2", "c3", "c4")),
			cellBlk("c1", 1, 1, 1, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w1"),
			cellBlk("c2", 1, 1, 2, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w2"),
			cellBlk("c3", 1, 2, 1, 1, 1, nil, "w3"),
			cellBlk("c4", 1, 2, 2, 1, 1, nil, "w4"),
			blkText("w1", types.BlockTypeWord, 1, "Gauge"),
			blkText("w2", types.BlockTypeWord, 1, "Reading"),
			blkText("w3", types.BlockTypeWord, 1, "Valve-A"),
			blkText("w4", types.BlockTypeWord, 1, "14psi"),
		},
		orderUnits:    []string{"Rotated header note", "Rotated body clause", "Gauge: Valve-A", "Reading: 14psi"},
		criticalPairs: [][2]string{{"Rotated header note", "Rotated body clause"}},
		tables:        []evalTable{{relations: []string{"Gauge: Valve-A", "Reading: 14psi"}}},
	}

	// 5. Merged-header fixture (multi-row merged column header + merged data row + caption + footer).
	mergedHeader := layoutEvalFixture{
		name:  "merged-header",
		slice: "golden",
		pages: 1,
		blocks: []types.Block{
			blk("p1", types.BlockTypePage, 1),
			withRels(bbox(blk("lt_tbl", types.BlockTypeLayoutTable, 1), 0.1, 0.1, 0.8, 0.6), rel(types.RelationshipTypeChild, "tl_raw")),
			bbox(withRels(blkText("tl_raw", types.BlockTypeLine, 1, "Raw ungrouped cells"), rel(types.RelationshipTypeChild, "w_reg")), 0.2, 0.1, 0.7, 0.1),
			withRels(bbox(blk("tbl", types.BlockTypeTable, 1), 0.1, 0.1, 0.8, 0.6),
				rel(types.RelationshipTypeTableTitle, "tt"),
				rel(types.RelationshipTypeMergedCell, "mc_hdr", "mc_reg"),
				rel(types.RelationshipTypeChild, "c_r1c1", "c_r1c2", "c_r1c3", "c_r2c2", "c_r2c3", "c_r3c1", "c_r3c2", "c_r3c3", "c_r4c1", "c_r4c2", "c_r4c3"),
				rel(types.RelationshipTypeTableFooter, "tf")),
			blkText("tt", types.BlockTypeTableTitle, 1, "Quarterly Revenue"),
			blkText("tf", types.BlockTypeTableFooter, 1, "Unaudited figures"),
			// Merged super-header "Fiscal 2026" across cols 2-3 in row 1
			{Id: aws.String("mc_hdr"), BlockType: types.BlockTypeMergedCell, Page: aws.Int32(1), RowIndex: aws.Int32(1), ColumnIndex: aws.Int32(2), RowSpan: aws.Int32(1), ColumnSpan: aws.Int32(2), EntityTypes: []types.EntityType{types.EntityTypeColumnHeader}, Relationships: []types.Relationship{rel(types.RelationshipTypeChild, "c_r1c2", "c_r1c3")}},
			// Merged region header "Region" across rows 1-2 in col 1
			{Id: aws.String("mc_reg"), BlockType: types.BlockTypeMergedCell, Page: aws.Int32(1), RowIndex: aws.Int32(1), ColumnIndex: aws.Int32(1), RowSpan: aws.Int32(2), ColumnSpan: aws.Int32(1), EntityTypes: []types.EntityType{types.EntityTypeColumnHeader}, Relationships: []types.Relationship{rel(types.RelationshipTypeChild, "c_r1c1")}},
			cellBlk("c_r1c1", 1, 1, 1, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w_reg"),
			cellBlk("c_r1c2", 1, 1, 2, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w_fy"),
			cellBlk("c_r1c3", 1, 1, 3, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}),
			cellBlk("c_r2c2", 1, 2, 2, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w_q1"),
			cellBlk("c_r2c3", 1, 2, 3, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w_q2"),
			cellBlk("c_r3c1", 1, 3, 1, 1, 1, nil, "w_emea"),
			cellBlk("c_r3c2", 1, 3, 2, 1, 1, nil, "w_v1"),
			cellBlk("c_r3c3", 1, 3, 3, 1, 1, nil, "w_v2"),
			cellBlk("c_r4c1", 1, 4, 1, 1, 1, nil, "w_apac"),
			cellBlk("c_r4c2", 1, 4, 2, 1, 1, nil, "w_v3"),
			cellBlk("c_r4c3", 1, 4, 3, 1, 1, nil, "w_v4"),
			blkText("w_reg", types.BlockTypeWord, 1, "Region"),
			blkText("w_fy", types.BlockTypeWord, 1, "Fiscal 2026"),
			blkText("w_q1", types.BlockTypeWord, 1, "Q1"),
			blkText("w_q2", types.BlockTypeWord, 1, "Q2"),
			blkText("w_emea", types.BlockTypeWord, 1, "EMEA"),
			blkText("w_v1", types.BlockTypeWord, 1, "1.2M"),
			blkText("w_v2", types.BlockTypeWord, 1, "1.5M"),
			blkText("w_apac", types.BlockTypeWord, 1, "APAC"),
			blkText("w_v3", types.BlockTypeWord, 1, "0.8M"),
			blkText("w_v4", types.BlockTypeWord, 1, "0.9M"),
		},
		orderUnits: []string{"Quarterly Revenue", "Region: EMEA", "Fiscal 2026 / Q1: 1.2M", "Fiscal 2026 / Q2: 1.5M", "Region: APAC", "Fiscal 2026 / Q1: 0.8M", "Fiscal 2026 / Q2: 0.9M", "Unaudited figures"},
		tables: []evalTable{{
			title:     "Quarterly Revenue",
			relations: []string{"Region: EMEA", "Fiscal 2026 / Q1: 1.2M", "Fiscal 2026 / Q2: 1.5M", "Region: APAC", "Fiscal 2026 / Q1: 0.8M", "Fiscal 2026 / Q2: 0.9M"},
		}},
	}

	return []layoutEvalFixture{twoCol, rtl, bilingual, rotated, mergedHeader}
}

func heldoutLayoutFixtures() []layoutEvalFixture {
	// Held-out set: multi-page two-column report + list + structured table.
	h1 := layoutEvalFixture{
		name:  "heldout-two-col-multipage",
		slice: "heldout",
		pages: 2,
		blocks: []types.Block{
			blk("p1", types.BlockTypePage, 1),
			blk("p2", types.BlockTypePage, 2),
			withRels(blk("p1_h", types.BlockTypeLayoutSectionHeader, 1), rel(types.RelationshipTypeChild, "p1_hl")),
			withRels(blk("p1_c1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "p1_c1a", "p1_c1b")),
			withRels(blk("p1_c2", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "p1_c2a")),
			bbox(blkText("p1_hl", types.BlockTypeLine, 1, "Section One"), 0.05, 0.1, 0.8, 0.04),
			bbox(blkText("p1_c1a", types.BlockTypeLine, 1, "Col1 para1"), 0.15, 0.05, 0.4, 0.04),
			bbox(blkText("p1_c1b", types.BlockTypeLine, 1, "Col1 para2"), 0.25, 0.05, 0.4, 0.04),
			bbox(blkText("p1_c2a", types.BlockTypeLine, 1, "Col2 para1"), 0.15, 0.55, 0.4, 0.04),
			withRels(bbox(blk("p2_lt", types.BlockTypeLayoutTable, 2), 0.1, 0.1, 0.8, 0.4), rel(types.RelationshipTypeChild, "p2_tl")),
			bbox(withRels(blkText("p2_tl", types.BlockTypeLine, 2, "Metric Value"), rel(types.RelationshipTypeChild, "pw1")), 0.1, 0.1, 0.5, 0.05),
			withRels(bbox(blk("p2_tbl", types.BlockTypeTable, 2), 0.1, 0.1, 0.8, 0.4), rel(types.RelationshipTypeChild, "pc1", "pc2", "pc3", "pc4")),
			cellBlk("pc1", 2, 1, 1, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "pw1"),
			cellBlk("pc2", 2, 1, 2, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "pw2"),
			cellBlk("pc3", 2, 2, 1, 1, 1, nil, "pw3"),
			cellBlk("pc4", 2, 2, 2, 1, 1, nil, "pw4"),
			blkText("pw1", types.BlockTypeWord, 2, "Latency"),
			blkText("pw2", types.BlockTypeWord, 2, "Budget"),
			blkText("pw3", types.BlockTypeWord, 2, "p95"),
			blkText("pw4", types.BlockTypeWord, 2, "120ms"),
		},
		orderUnits:    []string{"Section One", "Col1 para1", "Col1 para2", "Col2 para1", "Latency: p95", "Budget: 120ms"},
		criticalPairs: [][2]string{{"Col1 para1", "Col1 para2"}, {"Col1 para2", "Col2 para1"}},
		tables:        []evalTable{{relations: []string{"Latency: p95", "Budget: 120ms"}}},
	}
	return []layoutEvalFixture{h1}
}

func runPairedEval(t *testing.T, fixtures []layoutEvalFixture) (medianEditLayout, medianEditPlain, meanTableLayout, meanTablePlain float64) {
	t.Helper()
	var editsLayout, editsPlain, tablesLayout, tablesPlain []float64
	const perPageBudget = 50 * time.Millisecond

	for _, fx := range fixtures {
		t.Run(fx.slice+"/"+fx.name, func(t *testing.T) {
			plainText := plainGeometricText(fx.blocks)

			g := newAnalysisGraph(int(fx.pages), defaultLayoutBounds())
			start := time.Now()
			require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusSucceeded,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(fx.pages)},
				Blocks:           fx.blocks,
			}))
			res, err := g.decode(t.Context())
			elapsed := time.Since(start)
			require.NoError(t, err)
			if !raceDetectorEnabled {
				require.LessOrEqual(t, elapsed, time.Duration(fx.pages)*perPageBudget, "per-page decode budget exceeded")
			}

			if fx.wantPartial {
				require.True(t, res.Partial)
				require.Equal(t, fx.wantReason, res.Pages[1].Reason)
			}

			var joined strings.Builder
			for p := 1; p <= int(fx.pages); p++ {
				if p > 1 && joined.Len() > 0 && res.Pages[p].Text != "" {
					joined.WriteString("\n\n")
				}
				joined.WriteString(res.Pages[p].Text)
			}
			layoutText := joined.String()

			for _, pair := range fx.criticalPairs {
				iA := strings.Index(layoutText, pair[0])
				iB := strings.Index(layoutText, pair[1])
				require.GreaterOrEqual(t, iA, 0, "missing %q", pair[0])
				require.Greater(t, iB, iA, "critical order pair violated: %q before %q", pair[0], pair[1])
			}

			editL := normalizedLevenshteinEditDistance(fx.orderUnits, extractUnitOrder(layoutText, fx.orderUnits))
			editP := normalizedLevenshteinEditDistance(fx.orderUnits, extractUnitOrder(plainText, fx.orderUnits))
			tblL, lostL := tableStructureScore(layoutText, fx.tables)
			tblP, _ := tableStructureScore(plainText, fx.tables)

			if fx.slice == "golden" {
				require.Zero(t, lostL, "golden fixture %s lost reference cells", fx.name)
				require.Zero(t, editL, "golden fixture %s reading order mismatch", fx.name)
				require.Equal(t, 1.0, tblL, "golden fixture %s table structure score", fx.name)
			}

			t.Logf("slice=%s fixture=%s edit_plain=%.3f edit_layout=%.3f table_plain=%.3f table_layout=%.3f elapsed=%s",
				fx.slice, fx.name, editP, editL, tblP, tblL, elapsed)

			editsLayout = append(editsLayout, editL)
			editsPlain = append(editsPlain, editP)
			tablesLayout = append(tablesLayout, tblL)
			tablesPlain = append(tablesPlain, tblP)
		})
	}
	return median(editsLayout), median(editsPlain), mean(tablesLayout), mean(tablesPlain)
}

func median(vals []float64) float64 {
	s := slices.Clone(vals)
	slices.Sort(s)
	mid := len(s) / 2
	if len(s)%2 == 1 {
		return s[mid]
	}
	return (s[mid-1] + s[mid]) / 2
}

func mean(vals []float64) float64 {
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func TestLayoutPairedEvaluationGoldenAndHeldOut(t *testing.T) {
	medEditL, medEditP, meanTblL, meanTblP := runPairedEval(t, goldenLayoutFixtures())
	require.Less(t, medEditL, medEditP, fmt.Sprintf("expected lower median reading-order edit distance: layout=%.3f plain=%.3f", medEditL, medEditP))
	require.Greater(t, meanTblL, meanTblP, fmt.Sprintf("expected higher mean table-structure score: layout=%.3f plain=%.3f", meanTblL, meanTblP))

	heldEditL, heldEditP, heldTblL, heldTblP := runPairedEval(t, heldoutLayoutFixtures())
	require.Less(t, heldEditL, heldEditP, fmt.Sprintf("held-out expected lower reading-order edit distance: layout=%.3f plain=%.3f", heldEditL, heldEditP))
	require.Greater(t, heldTblL, heldTblP, fmt.Sprintf("held-out expected higher table-structure score: layout=%.3f plain=%.3f", heldTblL, heldTblP))
}
