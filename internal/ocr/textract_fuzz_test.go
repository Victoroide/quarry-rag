package ocr

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/aws/aws-sdk-go-v2/service/textract/types"
)

func FuzzDecodeLayoutGraph(f *testing.F) {
	seed1, _ := json.Marshal([]types.Block{
		withRels(blk("p1", types.BlockTypePage, 1), rel(types.RelationshipTypeChild, "lt1")),
		withRels(blk("lt1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l1")),
		blkText("l1", types.BlockTypeLine, 1, "Synthetic paragraph"),
	})
	seed2, _ := json.Marshal([]types.Block{
		withRels(blk("a", types.BlockTypeLayoutList, 1), rel(types.RelationshipTypeChild, "b")),
		withRels(blk("b", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "a")),
	})
	seed3, _ := json.Marshal([]types.Block{
		withRels(blk("t1", types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "c1")),
		cellBlk("c1", 1, 1, 1, 200, 200, nil),
	})
	lattice := []types.Block{
		withRels(blk("root", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "n0a", "n0b")),
	}
	for layer := range 12 {
		nextA := fmt.Sprintf("n%da", layer+1)
		nextB := fmt.Sprintf("n%db", layer+1)
		if layer == 11 {
			nextA, nextB = "leafA", "leafB"
		}
		lattice = append(lattice,
			withRels(blk(fmt.Sprintf("n%da", layer), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, nextA, nextB)),
			withRels(blk(fmt.Sprintf("n%db", layer), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, nextA, nextB)),
		)
	}
	lattice = append(lattice,
		blkText("leafA", types.BlockTypeLine, 1, "a"),
		blkText("leafB", types.BlockTypeLine, 1, "b"),
	)
	seed4, _ := json.Marshal(lattice)

	fanInWords := make([]string, 32)
	fanIn := make([]types.Block, 0, 32+8+16)
	for i := range 32 {
		id := fmt.Sprintf("fw%d", i)
		fanInWords[i] = id
		fanIn = append(fanIn, blkText(id, types.BlockTypeWord, 1, ""))
	}
	fanInLines := make([]string, 8)
	for i := range 8 {
		id := fmt.Sprintf("fl%d", i)
		fanInLines[i] = id
		fanIn = append(fanIn, withRels(blkText(id, types.BlockTypeLine, 1, ""), rel(types.RelationshipTypeChild, fanInWords...)))
	}
	for i := range 16 {
		fanIn = append(fanIn, withRels(blk(fmt.Sprintf("fp%d", i), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, fanInLines...)))
	}
	seed5, _ := json.Marshal(fanIn)

	// Small versions of the decode-cost shapes: unfollowed relationship entries
	// on shared lines, spanning headers over shared escaped text, one header
	// cell shared by many tables, and overlapping spans.
	valueEntries := []types.Block{blkText("y", types.BlockTypeWord, 1, "")}
	valueLineIDs := make([]string, 2)
	for k := range valueLineIDs {
		id := fmt.Sprintf("x%d", k)
		valueLineIDs[k] = id
		line := blkText(id, types.BlockTypeLine, 1, "")
		for range 64 {
			line.Relationships = append(line.Relationships, rel(types.RelationshipTypeValue, "y"))
		}
		valueEntries = append(valueEntries, line)
	}
	for i := range 32 {
		parent := types.BlockTypeLayoutText
		if i%2 == 0 {
			parent = types.BlockTypeLayoutTable
		}
		valueEntries = append(valueEntries, withRels(blk(fmt.Sprintf("p%d", i), parent, 1), rel(types.RelationshipTypeChild, valueLineIDs...)))
	}
	seed6, _ := json.Marshal(valueEntries)

	spanHeaders := []types.Block{
		blkText("hw", types.BlockTypeWord, 1, strings.Repeat("|", 256)),
		cellBlk("h1", 1, 1, 1, 1, 4, []types.EntityType{types.EntityTypeColumnHeader}, "hw"),
		cellBlk("h2", 1, 1, 5, 1, 4, []types.EntityType{types.EntityTypeColumnHeader}, "hw"),
	}
	sharedCell := []types.Block{
		blkText("sw", types.BlockTypeWord, 1, strings.Repeat("a", 256)),
		cellBlk("s1", 1, 1, 1, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "sw"),
	}
	for i := range 16 {
		spanHeaders = append(spanHeaders, withRels(blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "h1", "h2")))
		sharedCell = append(sharedCell, withRels(blk(fmt.Sprintf("u%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "s1")))
	}
	seed7, _ := json.Marshal(spanHeaders)
	seed8, _ := json.Marshal(sharedCell)

	overlap := make([]types.Block, 0, 18)
	overlapIDs := make([]string, 16)
	for i := range overlapIDs {
		overlapIDs[i] = fmt.Sprintf("o%d", i)
		overlap = append(overlap, cellBlk(overlapIDs[i], 1, 1, 1, 10, 10, nil))
	}
	for i := range 2 {
		overlap = append(overlap, withRels(blk(fmt.Sprintf("v%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, overlapIDs...)))
	}
	seed9, _ := json.Marshal(overlap)

	// Small versions of repeated single-ID followed entries and shared whitespace
	// text across cell loops, lines, and merged cells.
	repeatedEntries := []types.Block{blkText("rw", types.BlockTypeWord, 1, "x")}
	for i, spec := range []struct {
		bt types.BlockType
		rt types.RelationshipType
	}{
		{types.BlockTypeLayoutText, types.RelationshipTypeChild},
		{types.BlockTypeLayoutTable, types.RelationshipTypeChild},
		{types.BlockTypeTable, types.RelationshipTypeMergedCell},
	} {
		b := blk(fmt.Sprintf("rp%d", i), spec.bt, 1)
		for range 32 {
			b.Relationships = append(b.Relationships, rel(spec.rt, "rw"))
		}
		repeatedEntries = append(repeatedEntries, b)
	}
	seed10, _ := json.Marshal(repeatedEntries)

	wsCell := []types.Block{
		blkText("wsw", types.BlockTypeWord, 1, strings.Repeat(" ", 256)),
		blkText("wsa", types.BlockTypeWord, 1, "a"+strings.Repeat(" ", 255)),
		cellBlk("wsc1", 1, 1, 1, 1, 1, nil, "wsw"),
		cellBlk("wsc2", 1, 1, 1, 1, 1, nil, "wsa"),
		withRels(blk("wst1", types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "wsc1", "wsc1", "wsc1", "wsc1")),
		withRels(blk("wst2", types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "wsc2", "wsc2", "wsc2", "wsc2")),
	}
	seed11, _ := json.Marshal(wsCell)

	wsLine := []types.Block{blkText("wsln", types.BlockTypeLine, 1, strings.Repeat(" ", 256))}
	for i := range 16 {
		wsLine = append(wsLine, withRels(blk(fmt.Sprintf("wslp%d", i), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "wsln")))
	}
	seed12, _ := json.Marshal(wsLine)

	wsMergedBlock := blk("wsm", types.BlockTypeMergedCell, 1)
	wsMergedBlock.RowIndex, wsMergedBlock.ColumnIndex = aws.Int32(1), aws.Int32(1)
	for range 16 {
		wsMergedBlock.Relationships = append(wsMergedBlock.Relationships, rel(types.RelationshipTypeChild, "wsmc"))
	}
	wsMerged := []types.Block{
		blkText("wsmw", types.BlockTypeWord, 1, strings.Repeat(" ", 256)),
		cellBlk("wsmc", 1, 1, 1, 1, 1, nil, "wsmw"),
		wsMergedBlock,
		withRels(blk("wsmt", types.BlockTypeTable, 1), rel(types.RelationshipTypeMergedCell, "wsm")),
	}
	seed13, _ := json.Marshal(wsMerged)

	f.Add(seed1, uint8(1))
	f.Add(seed2, uint8(1))
	f.Add(seed3, uint8(2))
	f.Add(seed4, uint8(1))
	f.Add(seed5, uint8(1))
	f.Add(seed6, uint8(1))
	f.Add(seed7, uint8(1))
	f.Add(seed8, uint8(1))
	f.Add(seed9, uint8(1))
	f.Add(seed10, uint8(1))
	f.Add(seed11, uint8(1))
	f.Add(seed12, uint8(1))
	f.Add(seed13, uint8(1))

	f.Fuzz(func(t *testing.T, data []byte, pageHint uint8) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		var blocks []types.Block
		if err := json.Unmarshal(data, &blocks); err != nil {
			return
		}
		pages := 1 + int(pageHint)%4
		bounds := defaultLayoutBounds()
		bounds.maxBlocks = 512
		bounds.maxTableCells = 512
		bounds.maxOutputBytes = 64 << 10
		g := newAnalysisGraph(pages, bounds)
		mid := len(blocks) / 2
		for _, chunk := range [][]types.Block{blocks[:mid], blocks[mid:]} {
			if err := g.addResponse(&textract.GetDocumentAnalysisOutput{
				JobStatus: types.JobStatusSucceeded,
				Blocks:    chunk,
			}); err != nil {
				return
			}
		}
		start := time.Now()
		res, err := g.decode(context.Background())
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("decode of %d bytes took %v", len(data), elapsed)
		}
		if err != nil {
			return
		}
		totalBytes := 0
		for p := 1; p <= pages; p++ {
			page := res.Pages[p]
			if !utf8.ValidString(page.Text) || strings.IndexByte(page.Text, 0) >= 0 {
				t.Fatalf("page %d produced invalid UTF-8 or NUL", p)
			}
			totalBytes += len(page.Text)
			if totalBytes > bounds.maxOutputBytes+2*pages {
				t.Fatalf("output bytes exceeded cap: %d", totalBytes)
			}
		}
		_ = domain.MaxContentBytes
	})
}
