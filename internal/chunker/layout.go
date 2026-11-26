package chunker

import (
	"context"
	"slices"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/Victoroide/quarry-rag/internal/textutil"
)

const layoutChunkVersion = "quarry.chunk.layout.v1\x00"

type layoutSectionSpan struct {
	start   int
	end     int
	page    int
	tableID string
	rowID   string
}

func buildLayoutSectionSpans(sections []domain.LayoutSection) (string, []layoutSectionSpan) {
	var joined strings.Builder
	sectionSpans := make([]layoutSectionSpan, 0, len(sections))
	for _, sec := range sections {
		cleaned := textutil.NormalizeForIngest(stripHTML(sec.Text))
		if cleaned == "" {
			continue
		}
		if joined.Len() > 0 {
			joined.WriteString("\n\n")
		}
		start := joined.Len()
		joined.WriteString(cleaned)
		sectionSpans = append(sectionSpans, layoutSectionSpan{
			start:   start,
			end:     joined.Len(),
			page:    sec.Page,
			tableID: sec.TableID,
			rowID:   sec.RowID,
		})
	}
	return joined.String(), sectionSpans
}

// ChunkLayout splits ordered layout sections into chunks and records the
// document-scoped page, table, and logical row provenance covered by each chunk.
func (c *Chunker) ChunkLayout(ctx context.Context, docID domain.DocumentID, sections []domain.LayoutSection, profile *embeddinginput.Profile) ([]domain.Chunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	joined, sectionSpans := buildLayoutSectionSpans(sections)
	if joined == "" {
		return nil, nil
	}
	chunks, spans, err := c.chunkNormalized(ctx, docID, joined, profile, layoutChunkVersion)
	if err != nil {
		return nil, err
	}
	for i := range chunks {
		attachLayoutProvenance(&chunks[i], spans[i], sectionSpans)
	}
	return chunks, nil
}

// AttachLayoutProvenanceToChunks restores page, table, and row metadata onto
// ordered chunks produced from sections, matching each chunk sequentially against
// the normalized section text.
func AttachLayoutProvenanceToChunks(chunks []domain.Chunk, sections []domain.LayoutSection) {
	if len(chunks) == 0 || len(sections) == 0 {
		return
	}
	joined, sectionSpans := buildLayoutSectionSpans(sections)
	if joined == "" {
		return
	}
	searchFrom := 0
	for i := range chunks {
		text := chunks[i].Text
		if text == "" {
			continue
		}
		idx := strings.Index(joined[searchFrom:], text)
		if idx < 0 {
			continue
		}
		start := searchFrom + idx
		attachLayoutProvenance(&chunks[i], chunkSpan{start: start, end: start + len(text)}, sectionSpans)
		if start+1 <= len(joined) {
			searchFrom = start + 1
		}
	}
}

func attachLayoutProvenance(ch *domain.Chunk, span chunkSpan, sections []layoutSectionSpan) {
	var pages []int
	var tableIDs, rowIDs []string
	for _, sec := range sections {
		if sec.end <= span.start || sec.start >= span.end {
			continue
		}
		if sec.page > 0 && !slices.Contains(pages, sec.page) {
			pages = append(pages, sec.page)
		}
		if sec.tableID != "" && !slices.Contains(tableIDs, sec.tableID) {
			tableIDs = append(tableIDs, sec.tableID)
		}
		if sec.rowID != "" && !slices.Contains(rowIDs, sec.rowID) {
			rowIDs = append(rowIDs, sec.rowID)
		}
	}
	if ch.Metadata == nil {
		ch.Metadata = make(map[string]any)
	}
	if len(pages) > 0 {
		ch.Metadata["pages"] = pages
	}
	if len(tableIDs) > 0 {
		ch.Metadata["table_ids"] = tableIDs
	}
	if len(rowIDs) > 0 {
		ch.Metadata["row_ids"] = rowIDs
	}
}
