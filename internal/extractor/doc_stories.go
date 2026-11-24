package extractor

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
)

// DOCStories selects optional Word binary stories. The zero value extracts the body.
type DOCStories uint8

const (
	docFootnotesSection = iota
	docEndnotesSection
	docHeadersSection
	docFootersSection
	docCommentsSection
	docSectionCount
)

const docBodySelection DOCStories = 1 << docSectionCount

var docStoryLabels = [docSectionCount]string{"Footnotes", "Endnotes", "Headers", "Footers", "Comments"}

// ParseDOCStories parses a comma-separated set. The body is always included.
func ParseDOCStories(value string) (DOCStories, error) {
	var selected DOCStories
	if value == "" {
		return selected, nil
	}
	for name := range strings.SplitSeq(value, ",") {
		name = strings.TrimSpace(name)
		var flag DOCStories
		if strings.EqualFold(name, "body") {
			flag = docBodySelection
		} else {
			for section, label := range docStoryLabels {
				if strings.EqualFold(name, label) {
					flag = 1 << section
					break
				}
			}
		}
		if flag == 0 || selected&flag != 0 {
			return 0, fmt.Errorf("unknown or duplicate DOC story %q", name)
		}
		selected |= flag
	}
	return selected &^ docBodySelection, nil
}

func (s DOCStories) valid() bool { return s < docBodySelection }

type docStoryIndex struct {
	part, textPair, refPair, refSize, section int
	mark                                      uint16
}

// Pair positions in FibRgFcLcb97 and the corresponding PLC data element sizes.
var docStoryIndexes = [...]docStoryIndex{
	{part: docFootnote, textPair: 3, refPair: 2, refSize: 2, section: docFootnotesSection, mark: 0x02},
	{part: docEndnote, textPair: 47, refPair: 46, refSize: 2, section: docEndnotesSection, mark: 0x02},
	{part: docHeader, textPair: 11, section: docHeadersSection},
	{part: docComment, textPair: 5, refPair: 4, refSize: 30, section: docCommentsSection, mark: 0x05},
}

const (
	docSeparatorSlots = 6
	docPageStorySlots = 6
	maxDOCPLCEntries  = 1_000_000
)

func (idx docStoryIndex) read(ctx context.Context, table []byte, fib docFIB, pieces docPieces) ([]byte, error) {
	part := fib.parts[idx.part]
	plc, ok := fib.tableRange(table, idx.textPair)
	if !ok {
		return nil, errDOCCorrupt
	}
	if err := validateDOCStoryPLC(ctx, plc, part.end-part.start, idx.part == docHeader); err != nil {
		return nil, err
	}
	if idx.part == docHeader && part.start != part.end {
		if err := validateDOCPageStories(ctx, table, plc, fib, pieces); err != nil {
			return nil, err
		}
	}
	if idx.refSize != 0 {
		refs, ok := fib.tableRange(table, idx.refPair)
		if !ok {
			return nil, errDOCCorrupt
		}
		if err := idx.validateReferences(ctx, refs, max(0, len(plc)/4-2), fib.parts[docBody].end, pieces); err != nil {
			return nil, err
		}
	}
	return plc, nil
}

func validateDOCStoryPLC(ctx context.Context, plc []byte, length uint32, headers bool) error {
	if length == 0 && len(plc) == 0 {
		return nil
	}
	count := len(plc) / 4
	if count > maxDOCPLCEntries || len(plc)%4 != 0 {
		return errDOCCorrupt
	}
	// Some producers retain a zero-filled header index without any header text.
	if length == 0 && headers {
		for i := range count {
			if err := ctx.Err(); err != nil {
				return err
			}
			if docUint32(plc, i) != 0 {
				return errDOCCorrupt
			}
		}
		return nil
	}
	if length == 0 || count < 2 || docUint32(plc, count-2) != length-1 {
		return errDOCCorrupt
	}
	if headers && (count-2 < docSeparatorSlots || (count-2-docSeparatorSlots)%docPageStorySlots != 0) {
		return errDOCCorrupt
	}
	// The last CP is undefined for each of these PLCs. Adjacent equal CPs
	// represent inherited or absent page headers and footers.
	for i := 0; i < count-2; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		start, end := docUint32(plc, i), docUint32(plc, i+1)
		if start > end || (start == end && !headers) {
			return errDOCCorrupt
		}
	}
	return nil
}

func validateDOCPageStories(ctx context.Context, table, headers []byte, fib docFIB, pieces docPieces) error {
	const sectionEntrySize = 4 + 12 // CP and Sed in PlcfSed.
	sections, ok := fib.tableRange(table, 6)
	if !ok || len(sections) < 4+sectionEntrySize || (len(sections)-4)%sectionEntrySize != 0 {
		return errDOCCorrupt
	}
	count := (len(sections) - 4) / sectionEntrySize
	groups := (len(headers)/4 - 2 - docSeparatorSlots) / docPageStorySlots
	bodyEnd := fib.parts[docBody].end
	last := docUint32(sections, count)
	if count != groups || docUint32(sections, 0) != 0 || last < bodyEnd || last > maxDOCCP {
		return errDOCCorrupt
	}
	var previous uint32
	for i := 1; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		cp := docUint32(sections, i)
		if cp <= previous || cp >= bodyEnd || pieces.codeUnit(cp-1) != 0x0C {
			return errDOCCorrupt
		}
		previous = cp
	}
	part := fib.parts[docHeader]
	for i := 0; i < len(headers)/4-2; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		start, end := part.start+docUint32(headers, i), part.start+docUint32(headers, i+1)
		if start == end {
			continue
		}
		if pieces.codeUnit(end-1) != '\r' {
			return errDOCCorrupt
		}
		// A page story contains a content paragraph mark and a separate guard.
		if i >= docSeparatorSlots && (end-start < 2 || pieces.codeUnit(end-2) != '\r') {
			return errDOCCorrupt
		}
	}
	return nil
}

func (idx docStoryIndex) validateReferences(ctx context.Context, refs []byte, stories int, bodyEnd uint32, pieces docPieces) error {
	if len(refs) == 0 {
		return nil // The text PLC still supplies explicit story boundaries.
	}
	if len(refs) < 4 || (len(refs)-4)%(4+idx.refSize) != 0 || (len(refs)-4)/(4+idx.refSize) != stories {
		return errDOCCorrupt
	}
	var previous uint32
	for i := range stories {
		if err := ctx.Err(); err != nil {
			return err
		}
		cp := docUint32(refs, i)
		if cp >= bodyEnd || (i > 0 && cp <= previous) {
			return errDOCCorrupt
		}
		previous = cp
		// A zero note index means a custom reference. Comment reference
		// records contain author data, which is never needed for extraction.
		if idx.refSize == 2 && binary.LittleEndian.Uint16(refs[4*(stories+1)+2*i:]) == 0 {
			continue
		}
		if pieces.codeUnit(cp) != idx.mark {
			return errDOCCorrupt
		}
	}
	return nil
}

func (pieces docPieces) codeUnit(cp uint32) uint16 {
	i := pieces.firstPiece(cp)
	offset, width := pieces.location(i)
	offset += width * uint64(cp-pieces.cp(i))
	if width == 1 {
		return uint16(pieces.word[offset])
	}
	return binary.LittleEndian.Uint16(pieces.word[offset:])
}

func (idx docStoryIndex) extract(ctx context.Context, plc []byte, part docCPRange, pieces docPieces, selected DOCStories, decodedBytes *int, out *docStoryOutput) error {
	if part.start == part.end {
		return nil
	}
	first := 0
	sections := DOCStories(1) << idx.section
	if idx.part == docHeader {
		first = docSeparatorSlots
		sections |= 1 << docFootersSection
	}
	if selected&sections == 0 {
		return nil
	}
	for i := first; i < len(plc)/4-2; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		section := idx.section
		if idx.part == docHeader {
			switch (i - first) % docPageStorySlots {
			case 2, 3, 5: // Even, odd and first-page footers.
				section = docFootersSection
			}
		}
		if selected&(1<<section) == 0 {
			continue
		}
		start, end := part.start+docUint32(plc, i), part.start+docUint32(plc, i+1)
		if start == end {
			continue
		}
		raw, err := pieces.decode(ctx, docCPRange{start, end}, decodedBytes)
		if err != nil {
			return err
		}
		if (idx.part != docHeader && !strings.HasSuffix(raw, "\r")) || (idx.part == docComment && !strings.HasPrefix(raw, "\x05")) {
			return errDOCCorrupt
		}
		if idx.mark != 0 {
			raw = strings.TrimPrefix(raw, string(rune(idx.mark)))
		}
		cleaned, err := cleanDOCText(ctx, raw, true)
		if err != nil {
			return err
		}
		if err := out.add(section, cleaned); err != nil {
			return err
		}
	}
	return nil
}

type docStoryOutput struct {
	body     string
	sections [docSectionCount]strings.Builder
	seen     [docSectionCount]map[string]struct{}
	size     int
}

func (out *docStoryOutput) add(section int, text string) error {
	if text == "" {
		return nil
	}
	if section == docHeadersSection || section == docFootersSection {
		if _, exists := out.seen[section][text]; exists {
			return nil
		}
		if out.seen[section] == nil {
			out.seen[section] = make(map[string]struct{})
		}
		out.seen[section][text] = struct{}{}
	}
	buf := &out.sections[section]
	added := len(text)
	if buf.Len() == 0 {
		added += len(docStoryLabels[section]) + 1
	}
	if out.size != 0 {
		added += 2 // Blank line before a section or between its stories.
	}
	out.size += added
	if err := checkTextLimit(out.size); err != nil {
		return err
	}
	// Section separators are written in display order once all PLCs are read.
	if buf.Len() == 0 {
		buf.WriteString(docStoryLabels[section] + "\n")
	} else {
		buf.WriteString("\n\n")
	}
	buf.WriteString(text)
	return nil
}

func (out *docStoryOutput) text() string {
	if out.size == len(out.body) {
		return out.body
	}
	var result strings.Builder
	result.Grow(out.size)
	result.WriteString(out.body)
	for i := range out.sections {
		if section := &out.sections[i]; section.Len() != 0 {
			if result.Len() != 0 {
				result.WriteString("\n\n")
			}
			result.WriteString(section.String())
		}
	}
	return result.String()
}
