package ocr

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/aws/aws-sdk-go-v2/service/textract/types"
)

// LayoutPage holds reconstructed reading-order sections and status for one page.
type LayoutPage struct {
	Present  bool
	Failed   bool
	Partial  bool
	Reason   string
	Sections []domain.LayoutSection
	Text     string
}

// LayoutResults preserves per-page layout output and provider warning flags.
type LayoutResults struct {
	Pages   map[int]LayoutPage
	Partial bool
}

type layoutBounds struct {
	maxBlocks                int
	maxRelationshipsPerBlock int
	maxTotalRelationships    int
	maxRawBytes              int
	maxOutputBytes           int
	maxTableRows             int
	maxTableCols             int
	maxCellSpan              int
	maxTableCells            int
	maxGraphDepth            int
	maxEntityTypesPerBlock   int
}

func defaultLayoutBounds() layoutBounds {
	return layoutBounds{
		maxBlocks:                250_000,
		maxRelationshipsPerBlock: 4_096,
		maxTotalRelationships:    1_000_000,
		maxRawBytes:              4 * domain.MaxContentBytes,
		maxOutputBytes:           domain.MaxContentBytes,
		maxTableRows:             1_000,
		maxTableCols:             200,
		maxCellSpan:              100,
		maxTableCells:            20_000,
		maxGraphDepth:            32,
		maxEntityTypesPerBlock:   16,
	}
}

// AnalyzePages submits the stored original for combined LAYOUT and TABLES analysis
// and reconstructs reading order and structured tables across paginated responses.
func (c *Client) AnalyzePages(ctx context.Context, key, profile, window string, pageCount int, accepted func(context.Context) error) (LayoutResults, error) {
	if pageCount < 1 || pageCount > domain.MaxPDFPages {
		return LayoutResults{}, domain.Permanent(ErrPageLimit)
	}
	if profile == "" || window == "" {
		return LayoutResults{}, domain.Permanent(ErrDetectionFailed)
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if err := timeoutCtx.Err(); err != nil {
		return LayoutResults{}, err
	}
	started, err := c.api.StartDocumentAnalysis(timeoutCtx, &textract.StartDocumentAnalysisInput{
		ClientRequestToken: aws.String(c.requestToken(key, profile, window)),
		DocumentLocation:   &types.DocumentLocation{S3Object: &types.S3Object{Bucket: aws.String(c.bucket), Name: aws.String(key)}},
		FeatureTypes:       []types.FeatureType{types.FeatureTypeLayout, types.FeatureTypeTables},
	})
	if err != nil {
		return LayoutResults{}, detectionError(err, true)
	}
	if started == nil || aws.ToString(started.JobId) == "" {
		return LayoutResults{}, domain.Permanent(ErrDetectionFailed)
	}
	if accepted != nil {
		if acceptedErr := accepted(timeoutCtx); acceptedErr != nil {
			return LayoutResults{}, acceptedErr
		}
	}
	return c.collectAnalysis(timeoutCtx, started.JobId, pageCount, defaultLayoutBounds())
}

func (c *Client) collectAnalysis(ctx context.Context, jobID *string, pageCount int, bounds layoutBounds) (LayoutResults, error) {
	var next *string
	seen := make(map[string]bool)
	graph := newAnalysisGraph(pageCount, bounds)
	delay := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return LayoutResults{}, err
		}
		if len(seen) >= maxResultBatches {
			return LayoutResults{}, domain.Permanent(ErrDetectionFailed)
		}
		out, err := c.api.GetDocumentAnalysis(ctx, &textract.GetDocumentAnalysisInput{
			JobId:      jobID,
			NextToken:  next,
			MaxResults: aws.Int32(1000),
		})
		if err != nil {
			return LayoutResults{}, detectionError(err, true)
		}
		if out == nil {
			return LayoutResults{}, domain.Permanent(ErrDetectionFailed)
		}
		if out.JobStatus == types.JobStatusInProgress {
			if delayErr := sleepPollDelay(ctx, delay); delayErr != nil {
				return LayoutResults{}, delayErr
			}
			delay = min(2*delay, 10*time.Second)
			continue
		}
		if out.JobStatus != types.JobStatusSucceeded && out.JobStatus != types.JobStatusPartialSuccess {
			return LayoutResults{}, domain.Permanent(ErrDetectionFailed)
		}
		if err = graph.addResponse(out); err != nil {
			return LayoutResults{}, domain.Permanent(err)
		}
		next, err = advanceNextToken(out.NextToken, seen, true)
		if err != nil {
			return LayoutResults{}, domain.Permanent(err)
		}
		if next == nil {
			break
		}
	}
	results, err := graph.decode(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return LayoutResults{}, err
		}
		return LayoutResults{}, domain.Permanent(err)
	}
	return results, ctx.Err()
}

type analysisGraph struct {
	pageCount int
	bounds    layoutBounds
	blocks    map[string]*types.Block
	followed  map[string]*followedRels
	pageOrder map[int][]string
	pages     map[int]LayoutPage
	rotated   map[int]bool
	partial   bool
	totalRels int
	rawBytes  int
}

func newAnalysisGraph(pageCount int, bounds layoutBounds) *analysisGraph {
	return &analysisGraph{
		pageCount: pageCount,
		bounds:    bounds,
		blocks:    make(map[string]*types.Block),
		followed:  make(map[string]*followedRels),
		pageOrder: make(map[int][]string, pageCount),
		pages:     make(map[int]LayoutPage, pageCount),
		rotated:   make(map[int]bool),
	}
}

func (g *analysisGraph) addResponse(out *textract.GetDocumentAnalysisOutput) error {
	if out.DocumentMetadata != nil {
		count := int(aws.ToInt32(out.DocumentMetadata.Pages))
		if count > 3000 {
			return ErrPageLimit
		}
		if g.pageCount > 0 && count != g.pageCount {
			return ErrDetectionFailed
		}
	}
	g.partial = g.partial || out.JobStatus == types.JobStatusPartialSuccess || len(out.Warnings) > 0
	for _, warning := range out.Warnings {
		for _, number := range warning.Pages {
			if number < 1 || int(number) > g.pageCount {
				return ErrDetectionFailed
			}
			page := g.pages[int(number)]
			page.Failed = true
			if page.Reason == "" {
				page.Reason = "provider_warning"
			}
			g.pages[int(number)] = page
		}
	}
	for _, block := range out.Blocks {
		if len(g.blocks) >= g.bounds.maxBlocks {
			return ErrTextLimit
		}
		id := aws.ToString(block.Id)
		if id == "" {
			return ErrDetectionFailed
		}
		if _, dup := g.blocks[id]; dup {
			return ErrDetectionFailed
		}
		pageNum, err := g.resolveBlockPage(block)
		if err != nil {
			return err
		}
		relCount := 0
		for _, rel := range block.Relationships {
			relCount += max(1, len(rel.Ids))
		}
		if relCount > g.bounds.maxRelationshipsPerBlock || g.totalRels+relCount > g.bounds.maxTotalRelationships {
			return ErrTextLimit
		}
		g.totalRels += relCount
		if len(block.EntityTypes) > g.bounds.maxEntityTypesPerBlock {
			return ErrTextLimit
		}

		if block.Text != nil {
			rawLen := len(*block.Text)
			if rawLen > g.bounds.maxRawBytes-g.rawBytes {
				return ErrTextLimit
			}
			g.rawBytes += rawLen
			cleaned := strings.TrimSpace(strings.ReplaceAll(*block.Text, "\x00", ""))
			block.Text = &cleaned
		}
		if err := g.validateCellBounds(block); err != nil {
			return err
		}
		if hasRotatedGeometry(block) {
			g.rotated[pageNum] = true
		}
		if block.BlockType == types.BlockTypePage {
			page := g.pages[pageNum]
			page.Present = true
			g.pages[pageNum] = page
		}
		stored := block
		g.blocks[id] = &stored
		if fr := indexFollowedRels(&stored); fr != nil {
			g.followed[id] = fr
		}
		g.pageOrder[pageNum] = append(g.pageOrder[pageNum], id)
	}
	return nil
}

func (g *analysisGraph) resolveBlockPage(block types.Block) (int, error) {
	if block.Page == nil {
		if g.pageCount == 1 {
			return 1, nil
		}
		return 0, ErrDetectionFailed
	}
	number := int(aws.ToInt32(block.Page))
	if number > 3000 {
		return 0, ErrPageLimit
	}
	if number < 1 || (g.pageCount > 0 && number > g.pageCount) {
		return 0, ErrDetectionFailed
	}
	return number, nil
}

func (g *analysisGraph) validateCellBounds(block types.Block) error {
	if block.BlockType != types.BlockTypeCell && block.BlockType != types.BlockTypeMergedCell {
		return nil
	}
	row := int(aws.ToInt32(block.RowIndex))
	col := int(aws.ToInt32(block.ColumnIndex))
	if row < 1 || row > g.bounds.maxTableRows || col < 1 || col > g.bounds.maxTableCols {
		return ErrTextLimit
	}
	rowSpan := 1
	if block.RowSpan != nil {
		rowSpan = int(*block.RowSpan)
		if rowSpan < 1 {
			return ErrDetectionFailed
		}
	}
	colSpan := 1
	if block.ColumnSpan != nil {
		colSpan = int(*block.ColumnSpan)
		if colSpan < 1 {
			return ErrDetectionFailed
		}
	}
	if rowSpan > g.bounds.maxCellSpan || colSpan > g.bounds.maxCellSpan {
		return ErrTextLimit
	}
	if row+rowSpan-1 > g.bounds.maxTableRows || col+colSpan-1 > g.bounds.maxTableCols {
		return ErrTextLimit
	}
	return nil
}

func hasRotatedGeometry(block types.Block) bool {
	if block.Geometry == nil {
		return false
	}
	if block.Geometry.RotationAngle != nil {
		angle := math.Mod(math.Abs(float64(*block.Geometry.RotationAngle)), 360)
		if angle > 180 {
			angle = 360 - angle
		}
		if angle > 10 {
			return true
		}
	}
	if (block.BlockType == types.BlockTypeLine || block.BlockType == types.BlockTypeWord) && len(block.Geometry.Polygon) >= 2 {
		p0, p1 := block.Geometry.Polygon[0], block.Geometry.Polygon[1]
		dx := math.Abs(float64(p1.X - p0.X))
		dy := math.Abs(float64(p1.Y - p0.Y))
		// Vertical baseline (90 or 270 deg) has |dy| > |dx|; horizontal RTL has |dx| >= |dy|.
		if dy > dx && dy > 1e-4 {
			return true
		}
	}
	return false
}

const maxTableIOUCandidates = 64

const (
	// textWorkPerRawByte bounds how many times each provider text byte may be
	// joined, normalized, escaped or hashed over the whole decode.
	textWorkPerRawByte = 8
	// gridWritesPerCell bounds span fills relative to the table cell budget.
	gridWritesPerCell = 4
)

// followedRels holds, per block, only the relationship targets that decode
// follows. Every other relationship entry is counted once in addResponse and is
// never scanned again, so decode cost cannot grow with entries it ignores.
type followedRels struct {
	child  []string
	merged []string
	title  []string
	footer []string
	// direct keeps TABLE and CHILD targets of a LAYOUT_TABLE in provider order.
	direct []string
}

var noFollowedRels followedRels

func indexFollowedRels(b *types.Block) *followedRels {
	var childN, mergedN, titleN, footerN, directN int
	isLayoutTable := b.BlockType == types.BlockTypeLayoutTable
	for _, r := range b.Relationships {
		n := len(r.Ids)
		if n == 0 {
			continue
		}
		switch r.Type {
		case types.RelationshipTypeChild:
			childN += n
			if isLayoutTable {
				directN += n
			}
		case types.RelationshipTypeMergedCell:
			mergedN += n
		case types.RelationshipTypeTableTitle:
			titleN += n
		case types.RelationshipTypeTableFooter:
			footerN += n
		case types.RelationshipTypeTable:
			if isLayoutTable {
				directN += n
			}
		}
	}
	if childN == 0 && mergedN == 0 && titleN == 0 && footerN == 0 && directN == 0 {
		return nil
	}
	var fr followedRels
	if childN > 0 {
		fr.child = make([]string, 0, childN)
	}
	if mergedN > 0 {
		fr.merged = make([]string, 0, mergedN)
	}
	if titleN > 0 {
		fr.title = make([]string, 0, titleN)
	}
	if footerN > 0 {
		fr.footer = make([]string, 0, footerN)
	}
	if directN > 0 {
		fr.direct = make([]string, 0, directN)
	}
	for _, r := range b.Relationships {
		if len(r.Ids) == 0 {
			continue
		}
		switch r.Type {
		case types.RelationshipTypeChild:
			fr.child = append(fr.child, r.Ids...)
			if isLayoutTable {
				fr.direct = append(fr.direct, r.Ids...)
			}
		case types.RelationshipTypeMergedCell:
			fr.merged = append(fr.merged, r.Ids...)
		case types.RelationshipTypeTableTitle:
			fr.title = append(fr.title, r.Ids...)
		case types.RelationshipTypeTableFooter:
			fr.footer = append(fr.footer, r.Ids...)
		case types.RelationshipTypeTable:
			if isLayoutTable {
				fr.direct = append(fr.direct, r.Ids...)
			}
		}
	}
	return &fr
}

func (g *analysisGraph) rels(id string) *followedRels {
	if fr := g.followed[id]; fr != nil {
		return fr
	}
	return &noFollowedRels
}

// decodeBudget is the single place where every unit of decode work is charged
// before it is performed: graph visits, text bytes, grid writes and output.
type decodeBudget struct {
	ctx           context.Context
	outputBytes   int
	pending       int
	maxOutput     int
	tableCells    int
	maxCells      int
	visits        int
	maxVisits     int
	textWork      int
	maxTextWork   int
	gridWrites    int
	maxGridWrites int
}

func newDecodeBudget(ctx context.Context, g *analysisGraph) *decodeBudget {
	return &decodeBudget{
		ctx:           ctx,
		maxOutput:     g.bounds.maxOutputBytes,
		maxCells:      g.bounds.maxTableCells,
		maxVisits:     max(64, 6*(len(g.blocks)+g.totalRels)),
		maxTextWork:   textWorkPerRawByte*g.rawBytes + g.bounds.maxOutputBytes,
		maxGridWrites: gridWritesPerCell * g.bounds.maxTableCells,
	}
}

func (b *decodeBudget) visit() error {
	return b.visitN(1)
}

func (b *decodeBudget) visitN(n int) error {
	if n <= 0 {
		return nil
	}
	if n > b.maxVisits-b.visits {
		return ErrTextLimit
	}
	before := b.visits
	b.visits += n
	if before>>10 != b.visits>>10 {
		return b.ctx.Err()
	}
	return nil
}

// chargeText charges n bytes of text about to be joined, normalized, escaped or
// hashed. The allowance is never reset, so re-reading shared text per table or
// per walk is bounded by the size of the response.
func (b *decodeBudget) chargeText(n int) error {
	if n <= 0 {
		return nil
	}
	if n > b.maxTextWork-b.textWork {
		return ErrTextLimit
	}
	b.textWork += n
	return nil
}

func (b *decodeBudget) chargeGrid(writes int) error {
	if writes < 0 || writes > b.maxGridWrites-b.gridWrites {
		return ErrTextLimit
	}
	b.gridWrites += writes
	return nil
}

func (b *decodeBudget) normalize(raw string) (string, error) {
	if err := b.chargeText(len(raw)); err != nil {
		return "", err
	}
	return normalizeUntrustedText(raw), nil
}

func (b *decodeBudget) escape(s string) (string, error) {
	if err := b.chargeText(len(s)); err != nil {
		return "", err
	}
	return escapeCellBoundary(s), nil
}

func (b *decodeBudget) join(parts []string, sep string) (string, error) {
	if len(parts) == 0 {
		return "", nil
	}
	n := len(sep) * (len(parts) - 1)
	for _, part := range parts {
		n += len(part)
	}
	if err := b.chargeText(n); err != nil {
		return "", err
	}
	return strings.Join(parts, sep), nil
}

func (b *decodeBudget) chargePending(n int) error {
	if n <= 0 {
		return nil
	}
	if n > b.maxOutput-b.outputBytes-b.pending {
		return ErrTextLimit
	}
	b.pending += n
	return nil
}

func (b *decodeBudget) resetPending() {
	b.pending = 0
}

func (b *decodeBudget) chargeCells(cells int) error {
	if cells < 0 || cells > b.maxCells-b.tableCells {
		return ErrTextLimit
	}
	b.tableCells += cells
	return nil
}

func (b *decodeBudget) commitSection(secText string) error {
	sep := 0
	if b.outputBytes > 0 {
		sep = 2
	}
	if len(secText) > b.maxOutput-b.outputBytes-sep {
		return ErrTextLimit
	}
	b.outputBytes += sep + len(secText)
	b.pending = 0
	return nil
}

func (g *analysisGraph) decode(ctx context.Context) (LayoutResults, error) {
	if err := ctx.Err(); err != nil {
		return LayoutResults{}, err
	}
	budget := newDecodeBudget(ctx, g)
	if err := g.validateGraph(budget); err != nil {
		return LayoutResults{}, err
	}
	results := LayoutResults{Pages: g.pages, Partial: g.partial}
	for pageNum := 1; pageNum <= g.pageCount; pageNum++ {
		if err := ctx.Err(); err != nil {
			return LayoutResults{}, err
		}
		page, err := g.decodePage(pageNum, budget)
		if err != nil {
			return LayoutResults{}, err
		}
		if page.Partial || page.Failed {
			results.Partial = true
		}
		results.Pages[pageNum] = page
	}
	return results, nil
}

func (g *analysisGraph) validateGraph(budget *decodeBudget) error {
	const (
		unvisited = 1
		visiting  = 2
		visited   = 3
	)
	state := make(map[string]uint8, len(g.blocks))
	for id := range g.blocks {
		state[id] = unvisited
	}
	var visit func(id string, depth int) error
	visit = func(id string, depth int) error {
		if depth > g.bounds.maxGraphDepth {
			return ErrDetectionFailed
		}
		if err := budget.visit(); err != nil {
			return err
		}
		state[id] = visiting
		block := g.blocks[id]
		for _, rel := range block.Relationships {
			if len(rel.Ids) == 0 {
				if err := budget.visit(); err != nil {
					return err
				}
			}
			for _, target := range rel.Ids {
				if err := budget.visit(); err != nil {
					return err
				}
				if target == "" {
					return ErrDetectionFailed
				}
				switch state[target] {
				case 0:
					return ErrDetectionFailed
				case visiting:
					return ErrDetectionFailed
				case visited:
					if depth+1 > g.bounds.maxGraphDepth {
						return ErrDetectionFailed
					}
				default:
					if err := visit(target, depth+1); err != nil {
						return err
					}
				}
			}
		}
		state[id] = visited
		return nil
	}
	for id := range g.blocks {
		if state[id] == unvisited {
			if err := visit(id, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *analysisGraph) decodePage(pageNum int, budget *decodeBudget) (LayoutPage, error) {
	page := g.pages[pageNum]
	ids := g.pageOrder[pageNum]
	if len(ids) > 0 && !page.Present {
		// Some provider stubs omit PAGE blocks when returning page-numbered blocks.
		page.Present = true
	}
	if g.rotated[pageNum] {
		page.Partial = true
		if page.Reason == "" {
			page.Reason = "rotated_text"
		}
	}

	nestedLayout := make(map[string]bool)
	var layoutIDs, tableIDs, lineIDs []string
	for _, id := range ids {
		b := g.blocks[id]
		switch {
		case isLayoutBlock(b.BlockType):
			layoutIDs = append(layoutIDs, id)
			for _, childID := range g.rels(id).child {
				if err := budget.visit(); err != nil {
					return LayoutPage{}, err
				}
				if isLayoutBlock(g.blocks[childID].BlockType) {
					nestedLayout[childID] = true
				}
			}
		case b.BlockType == types.BlockTypeTable:
			tableIDs = append(tableIDs, id)
		case b.BlockType == types.BlockTypeLine:
			lineIDs = append(lineIDs, id)
		}
	}

	tableWords := make(map[string]map[string]struct{}, len(tableIDs))
	for _, tid := range tableIDs {
		words, err := g.collectWordIDs(tid, budget)
		if err != nil {
			return LayoutPage{}, err
		}
		tableWords[tid] = words
	}
	layoutTableMatch, err := g.matchTables(layoutIDs, tableIDs, tableWords, budget)
	if err != nil {
		return LayoutPage{}, err
	}

	matchedTableUsed := make(map[string]bool, len(tableIDs))
	consumedLines := make(map[string]bool, len(lineIDs))
	consumedWords := make(map[string]bool)
	tableSeq := 0

	appendSection := func(sec domain.LayoutSection) error {
		if strings.TrimSpace(sec.Text) == "" {
			return nil
		}
		if err := budget.commitSection(sec.Text); err != nil {
			return err
		}
		page.Sections = append(page.Sections, sec)
		return nil
	}

	for _, lid := range layoutIDs {
		if nestedLayout[lid] {
			continue
		}
		lb := g.blocks[lid]
		switch lb.BlockType {
		case types.BlockTypeLayoutTable:
			if err := g.markBlockLinesAndWords(lid, consumedLines, consumedWords, budget); err != nil {
				return LayoutPage{}, err
			}
			if tid, ok := layoutTableMatch[lid]; ok {
				matchedTableUsed[tid] = true
				for wid := range tableWords[tid] {
					consumedWords[wid] = true
				}
				tableSeq++
				logicalTableID := fmt.Sprintf("p%d-t%d", pageNum, tableSeq)
				secs, ambiguous, err := g.decodeTable(pageNum, logicalTableID, tid, budget)
				if err != nil {
					return LayoutPage{}, err
				}
				page.Sections = append(page.Sections, secs...)
				if ambiguous {
					page.Partial = true
					if page.Reason == "" {
						page.Reason = "ambiguous_table"
					}
				}
			} else {
				// Fallback to the LAYOUT_TABLE's lines when no TABLE block matched.
				page.Partial = true
				if page.Reason == "" {
					page.Reason = "unstructured_table"
				}
				text, err := g.collectLayoutText(lid, make(map[string]bool), budget)
				if err != nil {
					return LayoutPage{}, err
				}
				budget.resetPending()
				if text != "" {
					if err := appendSection(domain.LayoutSection{Page: pageNum, Text: text}); err != nil {
						return LayoutPage{}, err
					}
				}
			}
		case types.BlockTypeLayoutPageNumber:
			if err := g.markBlockLinesAndWords(lid, consumedLines, consumedWords, budget); err != nil {
				return LayoutPage{}, err
			}
			text, err := g.collectLayoutText(lid, make(map[string]bool), budget)
			if err != nil {
				return LayoutPage{}, err
			}
			budget.resetPending()
			if err := budget.chargeText(len(text)); err != nil {
				return LayoutPage{}, err
			}
			if text != "" && !domain.PDFRunningLabel(text) {
				if err := appendSection(domain.LayoutSection{Page: pageNum, Text: text}); err != nil {
					return LayoutPage{}, err
				}
			}
		case types.BlockTypeLayoutList:
			if err := g.markBlockLinesAndWords(lid, consumedLines, consumedWords, budget); err != nil {
				return LayoutPage{}, err
			}
			items, err := g.collectListItems(lid, make(map[string]bool), budget)
			if err != nil {
				return LayoutPage{}, err
			}
			budget.resetPending()
			for _, itemText := range items {
				if err := appendSection(domain.LayoutSection{Page: pageNum, Text: itemText}); err != nil {
					return LayoutPage{}, err
				}
			}
		default:
			if err := g.markBlockLinesAndWords(lid, consumedLines, consumedWords, budget); err != nil {
				return LayoutPage{}, err
			}
			text, err := g.collectLayoutText(lid, make(map[string]bool), budget)
			if err != nil {
				return LayoutPage{}, err
			}
			budget.resetPending()
			if text != "" {
				if err := appendSection(domain.LayoutSection{Page: pageNum, Text: text}); err != nil {
					return LayoutPage{}, err
				}
			}
		}
	}

	// Emit any TABLE blocks not linked to a LAYOUT_TABLE, ordered by top/left.
	var unmatchedTables []string
	for _, tid := range tableIDs {
		if !matchedTableUsed[tid] {
			unmatchedTables = append(unmatchedTables, tid)
		}
	}
	slices.SortStableFunc(unmatchedTables, func(a, b string) int {
		return compareTopLeft(g.blocks[a], g.blocks[b])
	})
	for _, tid := range unmatchedTables {
		for wid := range tableWords[tid] {
			consumedWords[wid] = true
		}
		tableSeq++
		logicalTableID := fmt.Sprintf("p%d-t%d", pageNum, tableSeq)
		secs, ambiguous, err := g.decodeTable(pageNum, logicalTableID, tid, budget)
		if err != nil {
			return LayoutPage{}, err
		}
		page.Sections = append(page.Sections, secs...)
		if ambiguous {
			page.Partial = true
			if page.Reason == "" {
				page.Reason = "ambiguous_table"
			}
		}
	}

	// Emit any unconsumed LINE blocks not covered by LAYOUT_* or TABLE blocks.
	type unlinkedLine struct {
		block *types.Block
		text  string
	}
	var unlinked []unlinkedLine
	for _, lineID := range lineIDs {
		if consumedLines[lineID] {
			continue
		}
		line := g.blocks[lineID]
		covered, err := g.lineCoveredByWords(lineID, consumedWords, budget)
		if err != nil {
			return LayoutPage{}, err
		}
		if covered {
			continue
		}
		lineText, err := g.extractLineText(lineID, budget)
		if err != nil {
			return LayoutPage{}, err
		}
		if lineText != "" {
			unlinked = append(unlinked, unlinkedLine{block: line, text: lineText})
		}
	}
	if len(unlinked) > 0 {
		page.Partial = true
		if page.Reason == "" {
			page.Reason = "ambiguous_order"
		}
		slices.SortStableFunc(unlinked, func(a, b unlinkedLine) int {
			return compareTopLeft(a.block, b.block)
		})
		var joined strings.Builder
		for i, u := range unlinked {
			sep := 0
			if i > 0 {
				sep = 1
			}
			if err := budget.chargePending(sep + len(u.text)); err != nil {
				return LayoutPage{}, err
			}
			if i > 0 {
				joined.WriteByte('\n')
			}
			joined.WriteString(u.text)
		}
		budget.resetPending()
		if joined.Len() > 0 {
			if err := appendSection(domain.LayoutSection{Page: pageNum, Text: joined.String()}); err != nil {
				return LayoutPage{}, err
			}
		}
	}

	var pageText strings.Builder
	for i, sec := range page.Sections {
		if i > 0 {
			pageText.WriteString("\n\n")
		}
		pageText.WriteString(sec.Text)
	}
	page.Text = pageText.String()
	return page, nil
}

type tableBox struct {
	id     string
	order  int
	top    float32
	bottom float32
}

type tableBoxMinHeap []tableBox

func (h tableBoxMinHeap) Len() int           { return len(h) }
func (h tableBoxMinHeap) Less(i, j int) bool { return h[i].bottom < h[j].bottom }
func (h tableBoxMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *tableBoxMinHeap) Push(x any)        { *h = append(*h, x.(tableBox)) }
func (h *tableBoxMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (g *analysisGraph) matchTables(layoutIDs, tableIDs []string, tableWords map[string]map[string]struct{}, budget *decodeBudget) (map[string]string, error) {
	matches := make(map[string]string)
	usedTables := make(map[string]bool, len(tableIDs))
	tableOrder := make(map[string]int, len(tableIDs))
	wordToTable := make(map[string]string)
	for i, tid := range tableIDs {
		tableOrder[tid] = i
		for wid := range tableWords[tid] {
			if _, exists := wordToTable[wid]; !exists {
				wordToTable[wid] = tid
			}
		}
	}

	var layoutTables []string
	for _, lid := range layoutIDs {
		if g.blocks[lid].BlockType == types.BlockTypeLayoutTable {
			layoutTables = append(layoutTables, lid)
		}
	}

	var needIOU []string
	for _, lid := range layoutTables {
		lb := g.blocks[lid]
		// Direct relationship first.
		for _, target := range g.rels(lid).direct {
			if err := budget.visit(); err != nil {
				return nil, err
			}
			if g.blocks[target].BlockType == types.BlockTypeTable && !usedTables[target] {
				matches[lid] = target
				usedTables[target] = true
				break
			}
		}
		if matches[lid] != "" {
			continue
		}
		// Match by shared WORD IDs via prebuilt wordID -> tableID index.
		lWords, err := g.collectWordIDs(lid, budget)
		if err != nil {
			return nil, err
		}
		counts := make(map[string]int)
		bestID, bestWords := "", 0
		for wid := range lWords {
			tid := wordToTable[wid]
			if tid == "" || usedTables[tid] {
				continue
			}
			counts[tid]++
			c := counts[tid]
			if c > bestWords || (c == bestWords && bestID != "" && tableOrder[tid] < tableOrder[bestID]) {
				bestWords = c
				bestID = tid
			}
		}
		if bestID != "" {
			matches[lid] = bestID
			usedTables[bestID] = true
			continue
		}
		if lb.Geometry != nil && lb.Geometry.BoundingBox != nil {
			needIOU = append(needIOU, lid)
		}
	}

	if len(needIOU) == 0 || len(usedTables) == len(tableIDs) {
		return matches, nil
	}

	// Sweep-line IoU fallback: only vertically overlapping unused TABLE candidates
	// are active, and at most maxTableIOUCandidates are scored per LAYOUT_TABLE.
	boxes := make([]tableBox, 0, len(tableIDs)-len(usedTables))
	for i, tid := range tableIDs {
		if usedTables[tid] {
			continue
		}
		if tb := g.blocks[tid]; tb.Geometry != nil && tb.Geometry.BoundingBox != nil {
			bb := tb.Geometry.BoundingBox
			boxes = append(boxes, tableBox{id: tid, order: i, top: bb.Top, bottom: bb.Top + bb.Height})
		}
	}
	slices.SortStableFunc(boxes, func(a, b tableBox) int {
		if n := cmp.Compare(a.top, b.top); n != 0 {
			return n
		}
		return cmp.Compare(a.order, b.order)
	})
	slices.SortStableFunc(needIOU, func(a, b string) int {
		return cmp.Compare(g.blocks[a].Geometry.BoundingBox.Top, g.blocks[b].Geometry.BoundingBox.Top)
	})

	var active tableBoxMinHeap
	nextBox := 0
	for _, lid := range needIOU {
		lb := g.blocks[lid]
		ab := lb.Geometry.BoundingBox
		aBottom := ab.Top + ab.Height
		for nextBox < len(boxes) && boxes[nextBox].top < aBottom {
			heap.Push(&active, boxes[nextBox])
			nextBox++
		}
		for len(active) > 0 && (active[0].bottom <= ab.Top || usedTables[active[0].id]) {
			heap.Pop(&active)
		}
		bestID, bestIdx, bestIOU := "", -1, 0.3
		checked := 0
		for i := 0; i < len(active) && checked < maxTableIOUCandidates; i++ {
			cand := active[i]
			if usedTables[cand.id] {
				continue
			}
			if err := budget.visit(); err != nil {
				return nil, err
			}
			checked++
			if iou := bboxOverlap(lb.Geometry, g.blocks[cand.id].Geometry); iou > bestIOU ||
				(iou == bestIOU && bestID != "" && cand.order < tableOrder[bestID]) {
				bestIOU = iou
				bestID = cand.id
				bestIdx = i
			}
		}
		if bestID != "" {
			matches[lid] = bestID
			usedTables[bestID] = true
			heap.Remove(&active, bestIdx)
		}
	}
	return matches, nil
}

func bboxOverlap(a, b *types.Geometry) float64 {
	if a == nil || b == nil || a.BoundingBox == nil || b.BoundingBox == nil {
		return 0
	}
	ab, bb := a.BoundingBox, b.BoundingBox
	x0 := max(float64(ab.Left), float64(bb.Left))
	y0 := max(float64(ab.Top), float64(bb.Top))
	x1 := min(float64(ab.Left+ab.Width), float64(bb.Left+bb.Width))
	y1 := min(float64(ab.Top+ab.Height), float64(bb.Top+bb.Height))
	if x1 <= x0 || y1 <= y0 {
		return 0
	}
	inter := (x1 - x0) * (y1 - y0)
	areaA := float64(ab.Width) * float64(ab.Height)
	areaB := float64(bb.Width) * float64(bb.Height)
	union := areaA + areaB - inter
	if union <= 0 {
		return 0
	}
	iou := inter / union
	minArea := min(areaA, areaB)
	if minArea > 0 && inter/minArea > 0.5 && iou > 0.15 {
		return inter / minArea
	}
	return iou
}

type tableCellSlot struct {
	// src indexes tableTexts.texts; 0 is the empty text.
	src       int
	isHeader  bool
	isTitle   bool
	isFooter  bool
	populated bool
}

// tableTexts interns the distinct cell texts of one table so equal texts share
// one identity: overlapping spans, repeated headers and row joins compare and
// escape each distinct text once instead of once per grid slot.
type tableTexts struct {
	ids     map[string]int
	texts   []string
	escaped []string
}

func newTableTexts() *tableTexts {
	return &tableTexts{ids: make(map[string]int), texts: []string{""}, escaped: []string{""}}
}

func (t *tableTexts) intern(text string, budget *decodeBudget) (int, error) {
	if text == "" {
		return 0, nil
	}
	if err := budget.chargeText(len(text)); err != nil {
		return 0, err
	}
	if id, ok := t.ids[text]; ok {
		return id, nil
	}
	id := len(t.texts)
	t.ids[text] = id
	t.texts = append(t.texts, text)
	t.escaped = append(t.escaped, "")
	return id, nil
}

func (t *tableTexts) escapedText(id int, budget *decodeBudget) (string, error) {
	if t.escaped[id] == "" {
		esc, err := budget.escape(t.texts[id])
		if err != nil {
			return "", err
		}
		t.escaped[id] = esc
	}
	return t.escaped[id], nil
}

// joinUniqueRowText joins the consecutive-distinct texts of one grid row.
func (t *tableTexts) joinUniqueRowText(row []tableCellSlot, budget *decodeBudget) (string, error) {
	var parts []string
	last := 0
	for _, slot := range row {
		if slot.src != 0 && slot.src != last {
			parts = append(parts, t.texts[slot.src])
		}
		if slot.src != 0 {
			last = slot.src
		}
	}
	return budget.join(parts, " ")
}

// readTableNotes reads the distinct TABLE_TITLE or TABLE_FOOTER targets once each.
func (g *analysisGraph) readTableNotes(ids []string, budget *decodeBudget) ([]string, error) {
	var notes []string
	var seen map[string]struct{}
	for _, id := range ids {
		if err := budget.visit(); err != nil {
			return nil, err
		}
		if _, dup := seen[id]; dup {
			continue
		}
		if seen == nil {
			seen = make(map[string]struct{})
		}
		seen[id] = struct{}{}
		raw, err := g.collectBlockText(id, make(map[string]bool), budget)
		if err != nil {
			return nil, err
		}
		text, err := budget.normalize(raw)
		if err != nil {
			return nil, err
		}
		if text != "" {
			notes = append(notes, text)
		}
	}
	return notes, nil
}

func (g *analysisGraph) decodeTable(pageNum int, logicalTableID, tableBlockID string, budget *decodeBudget) ([]domain.LayoutSection, bool, error) {
	fr := g.rels(tableBlockID)
	titles, titleErr := g.readTableNotes(fr.title, budget)
	if titleErr != nil {
		return nil, false, titleErr
	}
	footers, footerErr := g.readTableNotes(fr.footer, budget)
	if footerErr != nil {
		return nil, false, footerErr
	}

	mergedIDs := fr.merged
	cellIDs := fr.child

	mergedChildSet := make(map[string]bool)
	maxRow, maxCol := 0, 0
	updateExtent := func(b *types.Block) {
		r := int(aws.ToInt32(b.RowIndex))
		c := int(aws.ToInt32(b.ColumnIndex))
		rs := max(1, int(aws.ToInt32(b.RowSpan)))
		cs := max(1, int(aws.ToInt32(b.ColumnSpan)))
		maxRow = max(maxRow, r+rs-1)
		maxCol = max(maxCol, c+cs-1)
	}
	for _, mid := range mergedIDs {
		if err := budget.visit(); err != nil {
			return nil, false, err
		}
		mb := g.blocks[mid]
		if mb.BlockType != types.BlockTypeMergedCell {
			continue
		}
		updateExtent(mb)
		for _, cid := range g.rels(mid).child {
			if err := budget.visit(); err != nil {
				return nil, false, err
			}
			mergedChildSet[cid] = true
		}
	}
	for _, cid := range cellIDs {
		if err := budget.visit(); err != nil {
			return nil, false, err
		}
		cb := g.blocks[cid]
		if cb.BlockType != types.BlockTypeCell {
			continue
		}
		updateExtent(cb)
	}
	commitNotes := func(notes []string) ([]domain.LayoutSection, error) {
		unique, err := budget.uniqueNonEmpty(notes)
		if err != nil {
			return nil, err
		}
		sections := make([]domain.LayoutSection, 0, len(unique))
		for _, note := range unique {
			if err := budget.commitSection(note); err != nil {
				return nil, err
			}
			sections = append(sections, domain.LayoutSection{Page: pageNum, TableID: logicalTableID, Text: note})
		}
		return sections, nil
	}
	if maxRow == 0 || maxCol == 0 {
		budget.resetPending()
		sections, err := commitNotes(titles)
		if err != nil {
			return nil, false, err
		}
		more, err := commitNotes(footers)
		return append(sections, more...), false, err
	}
	if err := budget.chargeCells(maxRow * maxCol); err != nil {
		return nil, false, err
	}

	grid := make([][]tableCellSlot, maxRow)
	for r := range grid {
		grid[r] = make([]tableCellSlot, maxCol)
	}
	tt := newTableTexts()
	ambiguous := false
	fillSpan := func(b *types.Block, text string) error {
		r0 := int(aws.ToInt32(b.RowIndex)) - 1
		c0 := int(aws.ToInt32(b.ColumnIndex)) - 1
		rs := max(1, int(aws.ToInt32(b.RowSpan)))
		cs := max(1, int(aws.ToInt32(b.ColumnSpan)))
		if err := budget.chargeGrid(rs * cs); err != nil {
			return err
		}
		id, err := tt.intern(text, budget)
		if err != nil {
			return err
		}
		isHeader := hasEntityType(b, types.EntityTypeColumnHeader)
		isTitle := hasEntityType(b, types.EntityTypeTableTitle)
		isFooter := hasEntityType(b, types.EntityTypeTableFooter)
		for r := r0; r < r0+rs; r++ {
			for c := c0; c < c0+cs; c++ {
				existing := grid[r][c]
				if existing.populated && existing.src != id && id != 0 {
					ambiguous = true
				}
				grid[r][c] = tableCellSlot{
					src:       id,
					isHeader:  existing.isHeader || isHeader,
					isTitle:   existing.isTitle || isTitle,
					isFooter:  existing.isFooter || isFooter,
					populated: true,
				}
			}
		}
		return nil
	}

	for _, mid := range mergedIDs {
		if err := budget.visit(); err != nil {
			return nil, false, err
		}
		mb := g.blocks[mid]
		if mb.BlockType != types.BlockTypeMergedCell {
			continue
		}
		var parts []string
		// The sort is charged one visit per child; its log factor is bounded by
		// maxRelationshipsPerBlock.
		children := slices.Clone(g.rels(mid).child)
		if err := budget.visitN(len(children)); err != nil {
			return nil, false, err
		}
		slices.SortStableFunc(children, func(a, b string) int {
			ba, bb := g.blocks[a], g.blocks[b]
			if n := cmp.Compare(aws.ToInt32(ba.RowIndex), aws.ToInt32(bb.RowIndex)); n != 0 {
				return n
			}
			return cmp.Compare(aws.ToInt32(ba.ColumnIndex), aws.ToInt32(bb.ColumnIndex))
		})
		for _, cid := range children {
			raw, err := g.collectBlockText(cid, make(map[string]bool), budget)
			if err != nil {
				return nil, false, err
			}
			t, err := budget.normalize(raw)
			if err != nil {
				return nil, false, err
			}
			if t != "" {
				parts = append(parts, t)
			}
		}
		if len(parts) == 0 {
			directRaw, err := g.collectDirectWords(mid, budget)
			if err != nil {
				return nil, false, err
			}
			direct, err := budget.normalize(directRaw)
			if err != nil {
				return nil, false, err
			}
			if direct != "" {
				if err := budget.chargePending(len(direct)); err != nil {
					return nil, false, err
				}
				parts = append(parts, direct)
			}
		}
		merged, err := budget.join(parts, " ")
		if err != nil {
			return nil, false, err
		}
		if err := fillSpan(mb, merged); err != nil {
			return nil, false, err
		}
	}
	for _, cid := range cellIDs {
		if err := budget.visit(); err != nil {
			return nil, false, err
		}
		if mergedChildSet[cid] {
			continue
		}
		cb := g.blocks[cid]
		if cb.BlockType != types.BlockTypeCell {
			continue
		}
		raw, err := g.collectBlockText(cid, make(map[string]bool), budget)
		if err != nil {
			return nil, false, err
		}
		text, err := budget.normalize(raw)
		if err != nil {
			return nil, false, err
		}
		if err := fillSpan(cb, text); err != nil {
			return nil, false, err
		}
	}

	headerRow := make([]bool, maxRow)
	titleRow := make([]bool, maxRow)
	footerRow := make([]bool, maxRow)
	for r := range maxRow {
		hasTitle, hasFooter, hasOther := false, false, false
		for c := range maxCol {
			slot := grid[r][c]
			if slot.isHeader {
				headerRow[r] = true
			}
			if slot.src == 0 {
				continue
			}
			switch {
			case slot.isTitle:
				hasTitle = true
			case slot.isFooter:
				hasFooter = true
			default:
				hasOther = true
			}
		}
		if hasTitle && !hasOther && !headerRow[r] {
			titleRow[r] = true
			text, err := tt.joinUniqueRowText(grid[r], budget)
			if err != nil {
				return nil, false, err
			}
			if text != "" {
				titles = append(titles, text)
			}
		}
		if hasFooter && !hasOther && !headerRow[r] {
			footerRow[r] = true
			text, err := tt.joinUniqueRowText(grid[r], budget)
			if err != nil {
				return nil, false, err
			}
			if text != "" {
				footers = append(footers, text)
			}
		}
	}

	// A column header is built once per distinct sequence of header cells and
	// only for a column that carries a data cell, because it is then repeated in
	// the output and must fit the output budget.
	colHeaders := make([]string, maxCol)
	colBuilt := make([]bool, maxCol)
	headerByLevels := make(map[string]string)
	columnHeader := func(c int) (string, error) {
		if colBuilt[c] {
			return colHeaders[c], nil
		}
		var levels []int
		for r := range maxRow {
			if !headerRow[r] || grid[r][c].src == 0 {
				continue
			}
			if id := grid[r][c].src; len(levels) == 0 || levels[len(levels)-1] != id {
				levels = append(levels, id)
			}
		}
		colBuilt[c] = true
		if len(levels) == 0 {
			return "", nil
		}
		key := make([]byte, 0, 4*len(levels))
		parts := make([]string, len(levels))
		size := 3 * (len(levels) - 1)
		for i, id := range levels {
			key = strconv.AppendInt(key, int64(id), 36)
			key = append(key, ',')
			parts[i] = tt.texts[id]
			size += len(parts[i])
		}
		if header, ok := headerByLevels[string(key)]; ok {
			colHeaders[c] = header
			return header, nil
		}
		if size > budget.maxOutput {
			return "", ErrTextLimit
		}
		joined, err := budget.join(parts, " / ")
		if err != nil {
			return "", err
		}
		header, err := budget.escape(joined)
		if err != nil {
			return "", err
		}
		headerByLevels[string(key)] = header
		colHeaders[c] = header
		return header, nil
	}

	budget.resetPending()
	uniqueTitles, uniqueErr := budget.uniqueNonEmpty(titles)
	if uniqueErr != nil {
		return nil, false, uniqueErr
	}
	var sections []domain.LayoutSection
	for _, title := range uniqueTitles {
		if err := budget.commitSection(title); err != nil {
			return nil, false, err
		}
		sections = append(sections, domain.LayoutSection{Page: pageNum, TableID: logicalTableID, Text: title})
	}

	dataRowNum := 0
	for r := range maxRow {
		if headerRow[r] || titleRow[r] || footerRow[r] {
			continue
		}
		var row strings.Builder
		for c := range maxCol {
			id := grid[r][c].src
			if id == 0 {
				continue
			}
			escaped, err := tt.escapedText(id, budget)
			if err != nil {
				return nil, false, err
			}
			header, err := columnHeader(c)
			if err != nil {
				return nil, false, err
			}
			cellLen := len(escaped)
			if header != "" {
				cellLen += len(header) + 2
			}
			sectionSep := 0
			if budget.outputBytes > 0 {
				sectionSep = 2
			}
			cellSep := 0
			if row.Len() > 0 {
				cellSep = 3
			}
			if sectionSep+row.Len()+cellSep+cellLen > budget.maxOutput-budget.outputBytes {
				return nil, false, ErrTextLimit
			}
			if row.Len() > 0 {
				row.WriteString(" | ")
			}
			if header != "" {
				row.WriteString(header)
				row.WriteString(": ")
			}
			row.WriteString(escaped)
		}
		if row.Len() == 0 {
			continue
		}
		rowText := row.String()
		if err := budget.commitSection(rowText); err != nil {
			return nil, false, err
		}
		dataRowNum++
		sections = append(sections, domain.LayoutSection{
			Page:    pageNum,
			TableID: logicalTableID,
			RowID:   fmt.Sprintf("%s-r%d", logicalTableID, dataRowNum),
			Text:    rowText,
		})
	}

	more, footerErr := commitNotes(footers)
	if footerErr != nil {
		return nil, false, footerErr
	}
	return append(sections, more...), ambiguous, nil
}

// uniqueNonEmpty trims the items and drops empty and repeated ones. Hashing is
// charged to the text budget because items can repeat large shared texts.
func (b *decodeBudget) uniqueNonEmpty(items []string) ([]string, error) {
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if err := b.chargeText(len(item)); err != nil {
			return nil, err
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out, nil
}

func (g *analysisGraph) collectListItems(listID string, visited map[string]bool, budget *decodeBudget) ([]string, error) {
	if err := budget.visit(); err != nil {
		return nil, err
	}
	if visited[listID] {
		return nil, nil
	}
	visited[listID] = true
	var items []string
	for _, cid := range g.rels(listID).child {
		cb := g.blocks[cid]
		var text string
		var err error
		if isLayoutBlock(cb.BlockType) {
			text, err = g.collectLayoutText(cid, visited, budget)
			if err != nil {
				return nil, err
			}
		} else {
			if visitErr := budget.visit(); visitErr != nil {
				return nil, visitErr
			}
			if cb.BlockType == types.BlockTypeLine && !visited[cid] {
				visited[cid] = true
				text, err = g.extractLineText(cid, budget)
				if err != nil {
					return nil, err
				}
				if text != "" {
					if chargeErr := budget.chargePending(len(text)); chargeErr != nil {
						return nil, chargeErr
					}
				}
			}
		}
		if text != "" {
			if len(items) > 0 {
				if chargeErr := budget.chargePending(2); chargeErr != nil {
					return nil, chargeErr
				}
			}
			items = append(items, text)
		}
	}
	return items, nil
}

func (g *analysisGraph) collectLayoutText(id string, visited map[string]bool, budget *decodeBudget) (string, error) {
	if err := budget.visit(); err != nil {
		return "", err
	}
	if visited[id] {
		return "", nil
	}
	visited[id] = true
	b := g.blocks[id]
	var lines []string
	for _, cid := range g.rels(id).child {
		cb := g.blocks[cid]
		var t string
		switch {
		case cb.BlockType == types.BlockTypeLine:
			if err := budget.visit(); err != nil {
				return "", err
			}
			if visited[cid] {
				continue
			}
			visited[cid] = true
			var err error
			t, err = g.extractLineText(cid, budget)
			if err != nil {
				return "", err
			}
			if t != "" {
				if err := budget.chargePending(len(t)); err != nil {
					return "", err
				}
			}
		case isLayoutBlock(cb.BlockType):
			var err error
			t, err = g.collectLayoutText(cid, visited, budget)
			if err != nil {
				return "", err
			}
		case cb.BlockType == types.BlockTypeWord:
			if err := budget.visit(); err != nil {
				return "", err
			}
			if visited[cid] {
				continue
			}
			visited[cid] = true
			var err error
			t, err = budget.normalize(aws.ToString(cb.Text))
			if err != nil {
				return "", err
			}
			if t != "" {
				if err := budget.chargePending(len(t)); err != nil {
					return "", err
				}
			}
		default:
			raw, err := g.collectBlockText(cid, visited, budget)
			if err != nil {
				return "", err
			}
			t, err = budget.normalize(raw)
			if err != nil {
				return "", err
			}
		}
		if t != "" {
			if len(lines) > 0 {
				if err := budget.chargePending(1); err != nil {
					return "", err
				}
			}
			lines = append(lines, t)
		}
	}
	if len(lines) == 0 {
		t, err := budget.normalize(aws.ToString(b.Text))
		if err != nil {
			return "", err
		}
		if t != "" {
			if err := budget.chargePending(len(t)); err != nil {
				return "", err
			}
		}
		return t, nil
	}
	return budget.join(lines, "\n")
}

func (g *analysisGraph) collectBlockText(id string, visited map[string]bool, budget *decodeBudget) (string, error) {
	if err := budget.visit(); err != nil {
		return "", err
	}
	if visited[id] {
		return "", nil
	}
	visited[id] = true
	b := g.blocks[id]
	switch b.BlockType {
	case types.BlockTypeWord:
		t := aws.ToString(b.Text)
		if t != "" {
			if err := budget.chargePending(len(t)); err != nil {
				return "", err
			}
		}
		return t, nil
	case types.BlockTypeLine:
		t, err := g.extractLineText(id, budget)
		if err != nil {
			return "", err
		}
		if t != "" {
			if err := budget.chargePending(len(t)); err != nil {
				return "", err
			}
		}
		return t, nil
	default:
		var parts []string
		for _, cid := range g.rels(id).child {
			raw, err := g.collectBlockText(cid, visited, budget)
			if err != nil {
				return "", err
			}
			if raw != "" {
				if len(parts) > 0 {
					if err := budget.chargePending(1); err != nil {
						return "", err
					}
				}
				parts = append(parts, raw)
			}
		}
		if len(parts) > 0 {
			return budget.join(parts, " ")
		}
		t := aws.ToString(b.Text)
		if t != "" {
			if err := budget.chargePending(len(t)); err != nil {
				return "", err
			}
		}
		return t, nil
	}
}

func (g *analysisGraph) collectDirectWords(id string, budget *decodeBudget) (string, error) {
	var words []string
	for _, cid := range g.rels(id).child {
		if err := budget.visit(); err != nil {
			return "", err
		}
		if cb := g.blocks[cid]; cb != nil && cb.BlockType == types.BlockTypeWord {
			if t := aws.ToString(cb.Text); t != "" {
				words = append(words, t)
			}
		}
	}
	return budget.join(words, " ")
}

func (g *analysisGraph) extractLineText(id string, budget *decodeBudget) (string, error) {
	raw := aws.ToString(g.blocks[id].Text)
	if raw == "" {
		var err error
		raw, err = g.collectDirectWords(id, budget)
		if err != nil {
			return "", err
		}
	}
	return budget.normalize(raw)
}

func (g *analysisGraph) collectWordIDs(rootID string, budget *decodeBudget) (map[string]struct{}, error) {
	words := make(map[string]struct{})
	visited := make(map[string]bool)
	var walk func(id string) error
	walk = func(id string) error {
		if err := budget.visit(); err != nil {
			return err
		}
		if visited[id] {
			return nil
		}
		visited[id] = true
		if g.blocks[id].BlockType == types.BlockTypeWord {
			words[id] = struct{}{}
			return nil
		}
		fr := g.rels(id)
		for _, ids := range [...][]string{fr.child, fr.merged, fr.title, fr.footer} {
			for _, cid := range ids {
				if err := walk(cid); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(rootID); err != nil {
		return nil, err
	}
	return words, nil
}

func (g *analysisGraph) markBlockLinesAndWords(rootID string, lines, words map[string]bool, budget *decodeBudget) error {
	visited := make(map[string]bool)
	var walk func(id string) error
	walk = func(id string) error {
		if err := budget.visit(); err != nil {
			return err
		}
		if visited[id] {
			return nil
		}
		visited[id] = true
		switch g.blocks[id].BlockType {
		case types.BlockTypeLine:
			lines[id] = true
		case types.BlockTypeWord:
			words[id] = true
		}
		for _, cid := range g.rels(id).child {
			if err := walk(cid); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(rootID)
}

func (g *analysisGraph) lineCoveredByWords(lineID string, consumedWords map[string]bool, budget *decodeBudget) (bool, error) {
	children := g.rels(lineID).child
	for _, wid := range children {
		if err := budget.visit(); err != nil {
			return false, err
		}
		if wb := g.blocks[wid]; wb != nil && wb.BlockType == types.BlockTypeWord && !consumedWords[wid] {
			return false, nil
		}
	}
	return len(children) > 0, nil
}

func hasEntityType(b *types.Block, target types.EntityType) bool {
	return slices.Contains(b.EntityTypes, target)
}

func isLayoutBlock(bt types.BlockType) bool {
	switch bt {
	case types.BlockTypeLayoutText,
		types.BlockTypeLayoutTitle,
		types.BlockTypeLayoutHeader,
		types.BlockTypeLayoutFooter,
		types.BlockTypeLayoutSectionHeader,
		types.BlockTypeLayoutPageNumber,
		types.BlockTypeLayoutList,
		types.BlockTypeLayoutFigure,
		types.BlockTypeLayoutTable,
		types.BlockTypeLayoutKeyValue:
		return true
	default:
		return false
	}
}

func compareTopLeft(a, b *types.Block) int {
	var aTop, aLeft, bTop, bLeft float32
	if a.Geometry != nil && a.Geometry.BoundingBox != nil {
		aTop, aLeft = a.Geometry.BoundingBox.Top, a.Geometry.BoundingBox.Left
	}
	if b.Geometry != nil && b.Geometry.BoundingBox != nil {
		bTop, bLeft = b.Geometry.BoundingBox.Top, b.Geometry.BoundingBox.Left
	}
	if n := cmp.Compare(aTop, bTop); n != 0 {
		return n
	}
	return cmp.Compare(aLeft, bLeft)
}

var (
	cellPromptMarkerEscaper = strings.NewReplacer("<excerpt-", "‹excerpt-", "</excerpt-", "‹/excerpt-")
	cellBoundaryEscaper     = strings.NewReplacer(`\`, `\\`, `|`, `\|`)
)

// normalizeUntrustedText removes NULs, Unicode format characters (including bidi
// overrides/isolates), replaces control characters with spaces, collapses runs
// of whitespace, and defangs excerpt prompt markers while preserving language text.
func normalizeUntrustedText(raw string) string {
	if raw == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(raw))
	lastSpace := true
	for _, r := range raw {
		switch {
		case r == 0 || unicode.Is(unicode.Cf, r):
			continue
		case unicode.IsControl(r) || unicode.IsSpace(r):
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
		default:
			b.WriteRune(r)
			lastSpace = false
		}
	}
	return strings.TrimSpace(cellPromptMarkerEscaper.Replace(b.String()))
}

func escapeCellBoundary(s string) string {
	return cellBoundaryEscaper.Replace(s)
}
