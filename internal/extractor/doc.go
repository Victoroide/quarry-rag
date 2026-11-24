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

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/richardlehane/mscfb"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
)

// ErrEncryptedDocument indicates that the document requires decryption.
var ErrEncryptedDocument = errors.New("extractor: doc: encrypted document is not supported")

// ErrUnsupportedDocumentVersion indicates a document older than Word 97.
var ErrUnsupportedDocumentVersion = errors.New("extractor: doc: unsupported document version")

var errDOCCorrupt = errors.New("extractor: doc: unsupported or corrupt .doc file")

type docExtractor struct{}

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
	fib, err := readDOCFIB(wordData)
	if err != nil {
		return "", err
	}
	tableData, err := readDOCStream(ctx, doc, fib.table, len(data))
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
	pieces, err := newDOCPieces(ctx, wordData[:fib.wordEnd], plc, fib.piecesEnd)
	if err != nil {
		return "", err
	}
	var decodedBytes int
	text, err := pieces.decode(ctx, fib.parts[docBody], &decodedBytes)
	if err != nil {
		return "", err
	}
	return cleanDOCText(text)
}

func readDOCStream(ctx context.Context, doc *mscfb.Reader, name string, sourceSize int) ([]byte, error) {
	for _, entry := range doc.File {
		if entry.Name != name {
			continue
		}
		if entry.Size <= 0 || entry.Size > int64(sourceSize) {
			return nil, fmt.Errorf("extractor: doc: %s stream size exceeds the file size", name)
		}
		data, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: entry}, entry.Size))
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil || int64(len(data)) != entry.Size {
			return nil, errDOCCorrupt
		}
		return data, nil
	}
	return nil, errDOCCorrupt
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
	table     string
	wordEnd   uint32
	parts     [docPartCount]docCPRange
	piecesEnd uint32
	pairs     []byte
}

func readDOCFIB(word []byte) (docFIB, error) {
	var fib docFIB
	if len(word) < 0x22 || binary.LittleEndian.Uint16(word) != 0xA5EC {
		return fib, errDOCCorrupt
	}
	if binary.LittleEndian.Uint16(word[2:]) < 0x00C1 {
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
	if !ok || cslw < 11 {
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
	fib.wordEnd = docUint32(longs, 0) // cbMac
	if uint64(fib.wordEnd) > uint64(len(word)) || uint64(fib.wordEnd) < countOffset+2+8*count {
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

func newDOCPieces(ctx context.Context, word, plc []byte, end uint32) (docPieces, error) {
	if len(plc) < 16 || (len(plc)-4)%12 != 0 {
		return docPieces{}, errDOCCorrupt
	}
	pieces := docPieces{word: word, plc: plc, count: (len(plc) - 4) / 12}
	if pieces.count > 1_000_000 || pieces.cp(0) != 0 || pieces.cp(pieces.count) != end {
		return docPieces{}, errDOCCorrupt
	}
	var previous uint32
	for i := range pieces.count {
		if err := ctx.Err(); err != nil {
			return docPieces{}, err
		}
		cp := pieces.cp(i + 1)
		if cp <= previous || cp > end {
			return docPieces{}, errDOCCorrupt
		}
		offset, width := pieces.location(i)
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
	if uint64(*decodedBytes)+uint64(span.end-span.start) > domain.MaxContentBytes {
		return "", ErrContentTooLarge
	}
	var text strings.Builder
	for i := pieces.firstPiece(span.start); i < pieces.count && pieces.cp(i) < span.end; i++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		cp := pieces.cp(i)
		start, end := max(span.start, cp), min(span.end, pieces.cp(i+1))
		if err := checkTextLimit(*decodedBytes + int(end-start)); err != nil {
			return "", err
		}
		offset, width := pieces.location(i)
		piece := pieces.word[offset+width*uint64(start-cp) : offset+width*uint64(end-cp)]
		decoder := charmap.Windows1252.NewDecoder()
		if width == 2 {
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
	return text.String(), nil
}

func cleanDOCText(text string) (string, error) {
	var result strings.Builder
	var fields []bool
	hidden := 0
	for _, character := range text {
		switch character {
		case 0x13:
			fields = append(fields, false)
			hidden++
			continue
		case 0x14:
			if len(fields) > 0 && !fields[len(fields)-1] {
				fields[len(fields)-1] = true
				hidden--
			}
			continue
		case 0x15:
			if len(fields) > 0 {
				if !fields[len(fields)-1] {
					hidden--
				}
				fields = fields[:len(fields)-1]
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
	cleaned := strings.TrimSpace(result.String())
	if cleaned == "" {
		return "", fmt.Errorf("extractor: doc: %w", ErrNoText)
	}
	return cleaned, nil
}
