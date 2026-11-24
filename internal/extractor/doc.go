package extractor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/richardlehane/mscfb"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
)

// ErrEncryptedDocument indicates that the document requires decryption.
var ErrEncryptedDocument = errors.New("extractor: doc: encrypted document is not supported")

// ErrUnsupportedDocumentVersion indicates an unsupported Word binary version.
var ErrUnsupportedDocumentVersion = errors.New("extractor: doc: unsupported document version")

var errDOCCorrupt = errors.New("extractor: doc: unsupported or corrupt .doc file")

type docExtractor struct{ stories DOCStories }

func (e *docExtractor) Type() FileType { return FileTypeDOC }

func (e *docExtractor) Extract(ctx context.Context, r io.Reader) (string, error) {
	data, err := readSource(ctx, r)
	if err != nil {
		return "", fmt.Errorf("extractor: doc: read: %w", err)
	}

	doc, err := mscfb.New(bytes.NewReader(data))
	if err != nil {
		return "", errDOCCorrupt
	}

	wordData, err := readDOCStream(ctx, doc, "WordDocument", len(data))
	if err != nil {
		return "", err
	}
	fib, err := readDOCFIB(wordData, e.stories != 0)
	if err != nil {
		return "", err
	}
	tableData, err := readDOCStream(ctx, doc, fib.table, len(data)-len(wordData))
	if err != nil {
		return "", err
	}
	clx, ok := fib.tableRange(tableData, 33) // fcClx/lcbClx
	if !ok {
		return "", errDOCCorrupt
	}
	plc, err := readDOCPieceTable(clx)
	if err != nil {
		return "", err
	}
	pieces, err := newDOCPieces(ctx, wordData, plc, fib)
	if err != nil {
		return "", err
	}
	var decodedBytes int
	raw, err := pieces.decode(ctx, fib.parts[docBody], &decodedBytes)
	if err != nil {
		return "", err
	}
	body, err := cleanDOCText(ctx, raw, false)
	if err != nil {
		return "", err
	}
	out := docStoryOutput{body: body, size: len(body)}
	if fib.withStories {
		for _, idx := range docStoryIndexes {
			stories, err := idx.read(ctx, tableData, fib, pieces)
			if err != nil {
				return "", err
			}
			if err := idx.extract(ctx, stories, fib.parts[idx.part], pieces, e.stories, &decodedBytes, &out); err != nil {
				return "", err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if out.size == 0 {
		return "", fmt.Errorf("extractor: doc: %w", ErrNoText)
	}
	return out.text(), nil
}

func readDOCStream(ctx context.Context, doc *mscfb.Reader, name string, sourceSize int) ([]byte, error) {
	var stream *mscfb.File
	for _, entry := range doc.File {
		if entry.Name != name || len(entry.Path) != 0 || !entry.FileInfo().Mode().IsRegular() {
			continue
		}
		if stream != nil {
			return nil, errDOCCorrupt
		}
		stream = entry
	}
	if stream == nil {
		return nil, errDOCCorrupt
	}
	if stream.Size <= 0 || stream.Size > int64(sourceSize) {
		return nil, fmt.Errorf("extractor: doc: %s stream size exceeds the file size", name)
	}
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: stream}, stream.Size))
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil || int64(len(data)) != stream.Size {
		return nil, errDOCCorrupt
	}
	return data, nil
}

type docCPRange struct{ start, end uint32 }

const maxDOCCP = 0x7FFFFFFF

// These are in the MS-DOC document-part order, which differs from the
// order of the indexes in FibRgFcLcb97.
const (
	docBody = iota
	docFootnote
	docHeader
	docComment
	docEndnote
	docTextbox
	docHeaderTextbox
	docPartCount
)

type docFIB struct {
	table              string
	wordStart, wordEnd uint64
	parts              [docPartCount]docCPRange
	piecesEnd          uint32
	pairs              []byte
	withStories        bool
}

func readDOCFIB(word []byte, withStories bool) (docFIB, error) {
	fib := docFIB{withStories: withStories}
	if len(word) < 0x22 || binary.LittleEndian.Uint16(word) != 0xA5EC {
		return fib, errDOCCorrupt
	}
	version := binary.LittleEndian.Uint16(word[2:])
	if version < 0x00C1 {
		return fib, ErrUnsupportedDocumentVersion
	}
	flags := binary.LittleEndian.Uint16(word[0x0A:])
	if flags&0x0100 != 0 {
		return fib, ErrEncryptedDocument
	}
	fib.table = "0Table"
	if flags&0x0200 != 0 {
		fib.table = "1Table"
	}
	csw := uint64(binary.LittleEndian.Uint16(word[0x20:]))
	cslwOffset := 0x22 + 2*csw
	cslwData, ok := docRange(word, cslwOffset, 2)
	if !ok {
		return fib, errDOCCorrupt
	}
	cslw := uint64(binary.LittleEndian.Uint16(cslwData))
	longs, ok := docRange(word, cslwOffset+2, 4*cslw)
	if !ok || cslw < 4 {
		return fib, errDOCCorrupt
	}
	countOffset := cslwOffset + 2 + 4*cslw
	countData, ok := docRange(word, countOffset, 2)
	if !ok {
		return fib, errDOCCorrupt
	}
	count := uint64(binary.LittleEndian.Uint16(countData))
	fib.pairs, ok = docRange(word, countOffset+2, 8*count)
	if !ok || count <= 33 {
		return fib, errDOCCorrupt
	}
	bodyEnd := docUint32(longs, 3)
	if bodyEnd > maxDOCCP {
		return fib, errDOCCorrupt
	}
	fib.parts[docBody] = docCPRange{end: bodyEnd}
	fib.piecesEnd = bodyEnd
	fib.wordStart, fib.wordEnd = countOffset+2+8*count, uint64(len(word))
	endOfFIB, extensionErr := readDOCFIBExtension(word, fib.wordStart, version)
	if endOfFIB != 0 {
		fib.wordStart = endOfFIB
	}
	// The body uses the original character count and the physical stream bound.
	if !withStories {
		return fib, nil
	}
	if extensionErr != nil {
		return fib, extensionErr
	}
	fib.wordEnd = uint64(docUint32(longs, 0)) // cbMac
	if cslw < 11 || fib.wordEnd > uint64(len(word)) || fib.wordEnd < fib.wordStart {
		return fib, errDOCCorrupt
	}
	// ccp fields in FibRgLw97, excluding the reserved slot at index 6.
	var end uint32
	for part, index := range [...]int{3, 4, 5, 7, 8, 9, 10} {
		length := docUint32(longs, index)
		if length > maxDOCCP-end {
			return fib, errDOCCorrupt
		}
		fib.parts[part] = docCPRange{end, end + length}
		end += length
	}
	if end > fib.parts[docBody].end {
		if end == maxDOCCP {
			return fib, errDOCCorrupt
		}
		end++ // PlcPcd ends one CP after the last non-main subdocument.
	}
	fib.piecesEnd = end
	return fib, nil
}

// docFIBExtensionWords maps each recognized effective FIB version to the size,
// in words, of the FibRgCswNew that declares it. A recognized nFib needs no
// extension, and Word 97 has none.
var docFIBExtensionWords = map[uint16]uint16{0x00C1: 0, 0x00D9: 2, 0x0101: 2, 0x010C: 2, 0x0112: 5}

// readDOCFIBExtension bounds-checks FibRgCswNew and authorizes the effective
// version, which is nFibNew when the extension is present and nFib otherwise.
// The returned end of the FIB is zero when the extension does not fit.
func readDOCFIBExtension(word []byte, offset uint64, baseVersion uint16) (end uint64, err error) {
	count, ok := docRange(word, offset, 2)
	if !ok {
		return 0, errDOCCorrupt
	}
	offset += 2
	words := binary.LittleEndian.Uint16(count)
	extension, ok := docRange(word, offset, 2*uint64(words))
	if !ok {
		return 0, errDOCCorrupt
	}
	end = offset + uint64(len(extension))
	version := baseVersion
	if words != 0 {
		version = binary.LittleEndian.Uint16(extension)
	}
	expected, known := docFIBExtensionWords[version]
	if !known {
		return end, ErrUnsupportedDocumentVersion
	}
	if words != 0 && words != expected {
		return end, errDOCCorrupt
	}
	return end, nil
}

func (fib docFIB) tableRange(table []byte, index int) ([]byte, bool) {
	if index >= len(fib.pairs)/8 {
		return nil, true
	}
	size := docUint32(fib.pairs, 2*index+1)
	if size == 0 {
		return nil, true // The offset is undefined when the size is zero.
	}
	return docRange(table, uint64(docUint32(fib.pairs, 2*index)), uint64(size))
}

func docUint32(data []byte, index int) uint32 {
	return binary.LittleEndian.Uint32(data[4*index:])
}

func docRange(data []byte, offset, size uint64) ([]byte, bool) {
	if offset > uint64(len(data)) || size > uint64(len(data))-offset {
		return nil, false
	}
	return data[offset : offset+size], true
}

func readDOCPieceTable(clx []byte) ([]byte, error) {
	for len(clx) > 0 {
		switch clx[0] {
		case 1:
			if len(clx) < 3 {
				return nil, errDOCCorrupt
			}
			size := uint64(binary.LittleEndian.Uint16(clx[1:]))
			if size > 0x7FFF || size > uint64(len(clx))-3 {
				return nil, errDOCCorrupt
			}
			clx = clx[3+size:]
		case 2:
			if len(clx) < 5 {
				return nil, errDOCCorrupt
			}
			size := binary.LittleEndian.Uint32(clx[1:])
			if uint64(size) != uint64(len(clx))-5 {
				return nil, errDOCCorrupt
			}
			return clx[5:], nil
		default:
			return nil, errDOCCorrupt
		}
	}
	return nil, errDOCCorrupt
}

type docPieces struct {
	word, plc []byte
	count     int
}

func newDOCPieces(ctx context.Context, word, plc []byte, fib docFIB) (docPieces, error) {
	if len(plc) < 16 || (len(plc)-4)%12 != 0 {
		return docPieces{}, errDOCCorrupt
	}
	word = word[:fib.wordEnd]
	pieces := docPieces{word: word, plc: plc, count: (len(plc) - 4) / 12}
	last := pieces.cp(pieces.count)
	if pieces.count > 1_000_000 || pieces.cp(0) != 0 || last < fib.piecesEnd || (fib.withStories && last != fib.piecesEnd) {
		return docPieces{}, errDOCCorrupt
	}
	var previous uint32
	for i := range pieces.count {
		if err := ctx.Err(); err != nil {
			return docPieces{}, err
		}
		cp := pieces.cp(i + 1)
		if cp < previous || cp > maxDOCCP || (fib.withStories && cp == previous) {
			return docPieces{}, errDOCCorrupt
		}
		offset, width := pieces.location(i)
		if cp != previous && offset < fib.wordStart && (fib.withStories || previous < fib.parts[docBody].end) {
			return docPieces{}, errDOCCorrupt
		}
		if _, ok := docRange(word, offset, width*uint64(cp-previous)); !ok {
			return docPieces{}, fmt.Errorf("%w: %w", errDOCCorrupt, io.ErrUnexpectedEOF)
		}
		previous = cp
	}
	return pieces, nil
}

func (pieces docPieces) cp(i int) uint32 { return docUint32(pieces.plc, i) }

func (pieces docPieces) location(i int) (offset, width uint64) {
	fc := binary.LittleEndian.Uint32(pieces.plc[4*(pieces.count+1)+8*i+2:])
	offset = uint64(fc & 0x3FFFFFFF)
	if fc&0x40000000 != 0 {
		return offset / 2, 1
	}
	return offset, 2
}

func (pieces docPieces) firstPiece(cp uint32) int {
	return sort.Search(pieces.count, func(i int) bool { return pieces.cp(i+1) > cp })
}

func (pieces docPieces) decode(ctx context.Context, span docCPRange, decodedBytes *int) (string, error) {
	if int64(*decodedBytes)+int64(span.end-span.start) > domain.MaxContentBytes {
		return "", ErrContentTooLarge
	}
	var text strings.Builder
	var pending uint16
	appendRune := func(r rune) error {
		*decodedBytes += utf8.RuneLen(r)
		if err := checkTextLimit(*decodedBytes); err != nil {
			return err
		}
		text.WriteRune(r)
		return nil
	}
	for i := pieces.firstPiece(span.start); i < pieces.count && pieces.cp(i) < span.end; i++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		cp := pieces.cp(i)
		start, end := max(span.start, cp), min(span.end, pieces.cp(i+1))
		if start == end {
			continue
		}
		if err := checkTextLimit(*decodedBytes + int(end-start)); err != nil {
			return "", err
		}
		offset, width := pieces.location(i)
		piece := pieces.word[offset+width*uint64(start-cp) : offset+width*uint64(end-cp)]
		if pending != 0 {
			r := utf8.RuneError
			if width == 2 {
				low := binary.LittleEndian.Uint16(piece)
				if low >= 0xDC00 && low <= 0xDFFF {
					r = utf16.DecodeRune(rune(pending), rune(low))
					piece = piece[2:]
				}
			}
			pending = 0
			if err := appendRune(r); err != nil {
				return "", err
			}
		}
		decoder := charmap.Windows1252.NewDecoder()
		if width == 2 && len(piece) != 0 {
			high := binary.LittleEndian.Uint16(piece[len(piece)-2:])
			if high >= 0xD800 && high <= 0xDBFF {
				pending = high
				piece = piece[:len(piece)-2]
			}
			decoder = unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder()
		}
		decoded, err := decoder.Bytes(piece)
		if err != nil {
			return "", errDOCCorrupt
		}
		*decodedBytes += len(decoded)
		if err := checkTextLimit(*decodedBytes); err != nil {
			return "", err
		}
		text.Write(decoded)
	}
	if pending != 0 {
		if err := appendRune(utf8.RuneError); err != nil {
			return "", err
		}
	}
	return text.String(), nil
}

func cleanDOCText(ctx context.Context, text string, completeFields bool) (string, error) {
	var result strings.Builder
	var fields []bool
	hidden := 0
	nextContextCheck := 0
	for offset, character := range text {
		if offset >= nextContextCheck {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			nextContextCheck = offset + 4096
		}
		switch character {
		case 0x13:
			fields = append(fields, false)
			hidden++
			continue
		case 0x14:
			if len(fields) > 0 && !fields[len(fields)-1] {
				fields[len(fields)-1] = true
				hidden--
			} else if completeFields {
				return "", errDOCCorrupt
			}
			continue
		case 0x15:
			if len(fields) > 0 {
				if !fields[len(fields)-1] {
					hidden--
				}
				fields = fields[:len(fields)-1]
			} else if completeFields {
				return "", errDOCCorrupt
			}
			continue
		}
		if hidden > 0 {
			continue
		}
		switch character {
		case 0x0D, 0x0B:
			result.WriteByte('\n')
		case 0x0C:
			result.WriteString("\n\n")
		case 0x07:
			result.WriteByte('\t')
		case 0x1E:
			result.WriteByte('-')
		case 0x1F, 0x01, 0x08:
			continue
		default:
			if character < 0x20 {
				character = ' '
			}
			result.WriteRune(character)
		}
		if err := checkTextLimit(result.Len()); err != nil {
			return "", err
		}
	}
	if completeFields && len(fields) != 0 {
		return "", errDOCCorrupt
	}
	return strings.TrimSpace(result.String()), nil
}
