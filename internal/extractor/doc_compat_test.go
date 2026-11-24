package extractor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
)

func TestDOCBodyIgnoresOptionalLayout(t *testing.T) {
	word, table := docStoriesStreams(t, "Body €\x02\x05!\r", "\x02Note\r")
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"cbMac", func(w []byte) { binary.LittleEndian.PutUint32(w[0x40:], 0) }},
		{"unknown subdocument count", func(w []byte) { binary.LittleEndian.PutUint32(w[0x68:], 0xffffffff) }},
		{"out-of-file story index", func(w []byte) { binary.LittleEndian.PutUint32(w[0x9A+11*8:], 0xffffffff) }},
		{"short body-only long array", func(w []byte) {
			copy(w[0x50:], w[0x98:0x9E+8*0xA4])
			binary.LittleEndian.PutUint16(w[0x3E:], 4)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			altered := bytes.Clone(word)
			tc.mutate(altered)
			got, err := New(FileTypeDOC).Extract(t.Context(), bytes.NewReader(docStoriesCFB(t, altered, table)))
			if err != nil || got != "Body €  !" {
				t.Fatalf("body = %q, %v", got, err)
			}
		})
	}
}

func TestDOCStoriesRequireKnownFIBExtension(t *testing.T) {
	for _, tc := range []struct {
		name    string
		base    uint16 // FibBase nFib; Word 97 when zero.
		version uint16 // FibRgCswNew nFibNew.
		pairs   uint16
		words   uint16
		wantErr error
	}{
		{"Word 97", 0, 0, 0x5D, 0, nil},
		{"Word 2000", 0, 0x00D9, 0x6C, 2, nil},
		{"Word 2002", 0, 0x0101, 0x88, 2, nil},
		{"Word 2003", 0, 0x010C, 0xA4, 2, nil},
		{"Word 2007 binary layout", 0, 0x0112, 0xB7, 5, nil},
		{"recognized base version without extension", 0x010C, 0, 0xA4, 0, nil},
		{"unknown effective version", 0, 0x7777, 0xA4, 2, ErrUnsupportedDocumentVersion},
		{"unknown base version without extension", 0x00C2, 0, 0x5D, 0, ErrUnsupportedDocumentVersion},
		{"extension size of a later version", 0, 0x0112, 0xB7, 2, errDOCCorrupt},
		{"extension size of an earlier version", 0, 0x010C, 0xA4, 5, errDOCCorrupt},
		{"truncated extension", 0, 0x010C, 0xA4, 0xffff, errDOCCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			word, table := docStoriesStreams(t, "Body €\x02\x05!\r", "\x02Note\r")
			if tc.base != 0 {
				binary.LittleEndian.PutUint16(word[2:], tc.base)
			}
			binary.LittleEndian.PutUint16(word[0x98:], tc.pairs)
			extension := 0x9A + 8*int(tc.pairs)
			binary.LittleEndian.PutUint16(word[extension:], tc.words)
			if tc.words != 0 {
				binary.LittleEndian.PutUint16(word[extension+2:], tc.version)
			}
			data := docStoriesCFB(t, word, table)
			body, err := New(FileTypeDOC).Extract(t.Context(), bytes.NewReader(data))
			if err != nil || body != "Body €  !" {
				t.Fatalf("body = %q, %v", body, err)
			}
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, "footnotes")).Extract(t.Context(), bytes.NewReader(data))
			if !errors.Is(err, tc.wantErr) || (err == nil && !strings.HasSuffix(got, "\n\nFootnotes\nNote")) || (err != nil && got != "") {
				t.Fatalf("selected = %q, %v, want error %v", got, err, tc.wantErr)
			}
		})
	}
}

func TestDOCRejectsPieceOverlappingFIB(t *testing.T) {
	word, table := docStreams(t, docCompressedPiece(t, "Edge\r"))
	const fibEnd = 0x9A + 8*93 + 2
	for _, offset := range []int{fibEnd, fibEnd - 1} {
		logical := bytes.Clone(word)
		copy(logical[fibEnd:], "Edge\r")
		// Leave cswNew zero even when the piece starts within that field.
		fc := 0x40000000 | 2*docFixtureSize(t, offset)
		binary.LittleEndian.PutUint32(table[15:], fc)
		binary.LittleEndian.PutUint32(logical[0x4C:], 5)
		data := docStoriesCFB(t, logical, table)
		for _, profile := range []string{"body", "footnotes"} {
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, profile)).Extract(t.Context(), bytes.NewReader(data))
			if offset == fibEnd && (got != "Edge" || err != nil) {
				t.Fatalf("%s at FIB end: %q, %v", profile, got, err)
			}
			if offset < fibEnd && (got != "" || !errors.Is(err, errDOCCorrupt)) {
				t.Fatalf("%s overlapping FIB: %q, %v", profile, got, err)
			}
		}
	}
}

func TestDOCIgnoresUnusedPieceOverlappingFIBInBodyMode(t *testing.T) {
	word, table := docStreams(t, docCompressedPiece(t, "Body\r"), docCompressedPiece(t, "Unused\r"))
	binary.LittleEndian.PutUint32(word[0x4C:], 5)
	binary.LittleEndian.PutUint32(word[0x50:], 6)
	binary.LittleEndian.PutUint32(table[27:], 0x40000000|0x200)
	table = appendDOCFixturePLC(t, word, table, 3, 0, 5, 0)
	data := docStoriesCFB(t, word, table)
	got, err := New(FileTypeDOC).Extract(t.Context(), bytes.NewReader(data))
	if got != "Body" || err != nil {
		t.Fatalf("body = %q, %v", got, err)
	}
	got, err = NewWithDOCStories(FileTypeDOC, mustDOCStories(t, "footnotes")).Extract(t.Context(), bytes.NewReader(data))
	if got != "" || !errors.Is(err, errDOCCorrupt) {
		t.Fatalf("selected = %q, %v", got, err)
	}
}

func TestDOCUnicodeSurrogatesSpanLogicalPieces(t *testing.T) {
	const body = "Body 😀text"
	const footnote = "Foot😀note"
	for _, profile := range []string{"body", "footnotes"} {
		t.Run(profile, func(t *testing.T) {
			bodyBytes := docUnicodePiece(body + "\r").data
			noteBytes := docUnicodePiece(footnote + "\r\r\r").data
			want := body
			if profile == "footnotes" {
				want += "\n\nFootnotes\n" + footnote
			}
			wantFingerprint := chunker.ScopedFingerprint("archive", want)
			for i := range 3 {
				bodyBoundary := 2 * (len("Body ") + i)
				noteBoundary := 2 * (len("Foot") + i)
				word, table := docStreams(t, docPiece{data: bodyBytes[:bodyBoundary]}, docPiece{data: bodyBytes[bodyBoundary:]},
					docPiece{data: noteBytes[:noteBoundary]}, docPiece{data: noteBytes[noteBoundary:]})
				binary.LittleEndian.PutUint32(word[0x4C:], docFixtureSize(t, len(bodyBytes)/2))
				noteLength := docFixtureSize(t, len(noteBytes)/2-1)
				binary.LittleEndian.PutUint32(word[0x50:], noteLength)
				table = appendDOCFixturePLC(t, word, table, 3, 0, noteLength-1, 0)
				got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, profile)).Extract(t.Context(), bytes.NewReader(docStoriesCFB(t, word, table)))
				if err != nil || got != want || chunker.ScopedFingerprint("archive", got) != wantFingerprint {
					t.Fatalf("layout %d: %q, %v; want %q and its fingerprint", i, got, err, want)
				}
			}
		})
	}
}

func TestDOCUnicodeSurrogatesFlushAtRangeAndEncodingBoundaries(t *testing.T) {
	word, table := docStreams(t, docPiece{data: []byte{'A', 0, 0x3D, 0xD8}},
		docCompressedPiece(t, "X"), docPiece{data: []byte{0, 0xDE, 'B', 0}})
	fib, err := readDOCFIB(word, false)
	if err != nil {
		t.Fatal(err)
	}
	plc, err := readDOCPieceTable(table)
	if err != nil {
		t.Fatal(err)
	}
	pieces, err := newDOCPieces(t.Context(), word, plc, fib)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		span docCPRange
		want string
	}{
		{docCPRange{0, 5}, "A�X�B"},
		{docCPRange{0, 2}, "A�"},
		{docCPRange{3, 5}, "�B"},
	} {
		var budget int
		got, err := pieces.decode(t.Context(), tc.span, &budget)
		if err != nil || got != tc.want || budget != len(tc.want) {
			t.Fatalf("span %v: %q, %v, %d bytes", tc.span, got, err, budget)
		}
	}
	budget := domain.MaxContentBytes - 2
	if got, err := pieces.decode(t.Context(), docCPRange{1, 2}, &budget); got != "" || !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("replacement exceeds remaining budget: %q, %v", got, err)
	}
}

func TestDOCStreamsShareSourceBudget(t *testing.T) {
	data := docFixture(t, docCompressedPiece(t, "Body\r"))
	directory := (int(binary.LittleEndian.Uint32(data[48:])) + 1) * 512
	const wordSize = 4096
	tableSize := docFixtureSize(t, len(data)-wordSize+1)
	binary.LittleEndian.PutUint32(data[directory+2*128+120:], tableSize)
	got, err := New(FileTypeDOC).Extract(t.Context(), bytes.NewReader(data))
	if got != "" || err == nil || !strings.Contains(err.Error(), "0Table stream size exceeds the file size") {
		t.Fatalf("combined declared stream sizes: %q, %v", got, err)
	}
}

func TestDOCUsesRootStreamsFromTheSameDocument(t *testing.T) {
	outerWord, outerTable := docStoriesStreams(t, "Outer €\x05!\r", "Outer note\r")
	innerWord, innerTable := docStoriesStreams(t, "Inner €\x05!\r", "Inner note\r")
	for _, outer := range [][]cfbStream{
		{{name: "0Table", data: outerTable}, {name: "WordDocument", data: outerWord}},
		{{name: "WordDocument", data: outerWord}, {name: "0Table", data: outerTable}},
		{{name: "WordDocument", data: outerWord}},
		{{name: "0Table", data: outerTable}},
	} {
		data := buildCFB(t, outer, cfbOptions{embedded: []cfbStream{
			{name: "WordDocument", data: innerWord}, {name: "0Table", data: innerTable},
		}})
		if len(outer) == 2 && outer[0].name == "0Table" {
			directory := (int(binary.LittleEndian.Uint32(data[48:])) + 1) * 512
			storage := data[directory+3*128:]
			const name = "WordDocument\x00"
			clear(storage[:64])
			copy(storage, docUnicodePiece(name).data)
			binary.LittleEndian.PutUint16(storage[64:], uint16(2*len(name)))
		}
		for _, tc := range []struct{ profile, want string }{
			{"body", "Outer € !"},
			{"footnotes", "Outer € !\n\nFootnotes\nOuter note"},
		} {
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, tc.profile)).Extract(t.Context(), bytes.NewReader(data))
			if len(outer) == 1 {
				if got != "" || !errors.Is(err, errDOCCorrupt) {
					t.Fatalf("missing root stream: %q, %v", got, err)
				}
			} else if got != tc.want || err != nil {
				t.Fatalf("%s with root %s first: %q, %v; want %q", tc.profile, outer[0].name, got, err, tc.want)
			}
		}
	}
}

func TestDOCRejectsAmbiguousRootStreams(t *testing.T) {
	word, table := docStoriesStreams(t, "First €\x05!\r", "First note\r")
	other, _ := docStoriesStreams(t, "Second €\x05!\r", "Second note\r")
	for _, streams := range [][]cfbStream{
		{{name: "WordDocument", data: word}, {name: "WordDocument", data: other}, {name: "0Table", data: table}},
		{{name: "WordDocument", data: word}, {name: "0Table", data: table}, {name: "0Table", data: table}},
	} {
		data := buildCFB(t, streams, cfbOptions{})
		for _, profile := range []string{"body", "footnotes"} {
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, profile)).Extract(t.Context(), bytes.NewReader(data))
			if got != "" || !errors.Is(err, errDOCCorrupt) {
				t.Fatalf("%s with duplicate %s: %q, %v", profile, streams[1].name, got, err)
			}
		}
	}
}

func TestDOCPageStoriesMatchDocumentSections(t *testing.T) {
	for _, name := range []string{
		"extra group", "missing group", "partial page story group", "missing section mark", "missing page guard", "missing separator guard",
		"misaligned section index", "first section after the start", "sections end inside the body", "sections end beyond the format", "section break after the body",
	} {
		t.Run(name, func(t *testing.T) {
			word, table := docStoriesStreams(t, "Body €\x02\x05!\r", "Note\r")
			bodyEnd := binary.LittleEndian.Uint32(word[0x4C:])
			sectionTable := func(cps ...uint32) []byte {
				sections := make([]byte, 0, 4*len(cps))
				for _, cp := range cps {
					sections = binary.LittleEndian.AppendUint32(sections, cp)
				}
				return append(sections, make([]byte, 12*(len(cps)-1))...)
			}
			switch name {
			case "extra group":
				sections := binary.LittleEndian.AppendUint32(make([]byte, 4), bodyEnd)
				table = appendDOCFixtureTable(t, word, table, 6, append(sections, make([]byte, 12)...))
			case "misaligned section index":
				table = appendDOCFixtureTable(t, word, table, 6, append(sectionTable(0, bodyEnd-1, bodyEnd), 0))
			case "first section after the start":
				table = appendDOCFixtureTable(t, word, table, 6, sectionTable(1, bodyEnd-1, bodyEnd))
			case "sections end inside the body":
				table = appendDOCFixtureTable(t, word, table, 6, sectionTable(0, bodyEnd-1, bodyEnd-1))
			case "sections end beyond the format":
				table = appendDOCFixtureTable(t, word, table, 6, sectionTable(0, bodyEnd-1, 0xffffffff))
			case "section break after the body":
				table = appendDOCFixtureTable(t, word, table, 6, sectionTable(0, bodyEnd+1000, bodyEnd+1000))
			case "missing group":
				hdd := docFixturePLCOffset(word, 11)
				shortened := append(bytes.Clone(table[hdd:hdd+12*4]), table[hdd+18*4:hdd+20*4]...)
				table = appendDOCFixtureTable(t, word, table, 11, shortened)
			case "partial page story group":
				hdd := docFixturePLCOffset(word, 11)
				last := table[hdd+18*4 : hdd+19*4]
				extended := append(bytes.Clone(table[hdd:hdd+19*4]), last...)
				extended = append(append(extended, last...), table[hdd+19*4:hdd+20*4]...)
				table = appendDOCFixtureTable(t, word, table, 11, extended)
			case "missing section mark":
				word[0x800+len("Body \x80\x02\x05!\r")] = 'x'
			case "missing page guard":
				sequence := docUnicodePiece("頭頁 Header\r\r").data
				offset := bytes.Index(word, sequence)
				word[offset+len(sequence)-4] = 'x'
			case "missing separator guard":
				sequence := docUnicodePiece("separator noise\r").data
				offset := bytes.Index(word, sequence)
				word[offset+len(sequence)-2] = 'x'
			}
			data := docStoriesCFB(t, word, table)
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, "headers,footers")).Extract(t.Context(), bytes.NewReader(data))
			if got != "" || !errors.Is(err, errDOCCorrupt) {
				t.Fatalf("%q, %v", got, err)
			}
		})
	}
}

func TestDOCFieldsStayWithinAStory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		notes []string
		want  string
	}{
		{"nested field across pieces", []string{"\x13outer \x13inner\x14hidden\x15\x14visible\x15\r"}, "Body\n\nFootnotes\nvisible"},
		{"field crosses stories", []string{"\x13hidden\r", "instruction\x15\r"}, ""},
		{"unfinished field", []string{"\x13hidden\r"}, ""},
		{"unfinished field result", []string{"\x13hidden\x14shown\r"}, ""},
		{"stray separator", []string{"stray\x14\r"}, ""},
		{"stray end", []string{"stray\x15\r"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			note := docUnicodePiece(strings.Join(tc.notes, "") + "\r\r").data
			word, table := docStreams(t, docCompressedPiece(t, "Body\r"), docPiece{data: note[:14]}, docPiece{data: note[14:]})
			binary.LittleEndian.PutUint32(word[0x4C:], 5)
			binary.LittleEndian.PutUint32(word[0x50:], docFixtureSize(t, len(note)/2-1))
			cps := []uint32{0}
			for _, text := range tc.notes {
				cps = append(cps, cps[len(cps)-1]+docFixtureSize(t, len(docUnicodePiece(text).data)/2))
			}
			table = appendDOCFixturePLC(t, word, table, 3, append(cps, 0)...)
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, "footnotes")).Extract(t.Context(), bytes.NewReader(docStoriesCFB(t, word, table)))
			if got != tc.want || (tc.want == "" && !errors.Is(err, errDOCCorrupt)) || (tc.want != "" && err != nil) {
				t.Fatalf("%q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestDOCEmptyHeaderIndexMustBeZeroAndInBounds(t *testing.T) {
	for _, name := range []string{"zero-filled", "nonzero CP", "misaligned", "out of file", "comment index without text"} {
		t.Run(name, func(t *testing.T) {
			word, table := docStreams(t, docCompressedPiece(t, "Body\rNote\r\r\r"))
			binary.LittleEndian.PutUint32(word[0x4C:], 5)
			binary.LittleEndian.PutUint32(word[0x50:], 6)
			table = appendDOCFixturePLC(t, word, table, 3, 0, 5, 0)
			index := make([]byte, 13*4)
			switch name {
			case "nonzero CP":
				index[4] = 1
			case "misaligned":
				index = index[:len(index)-1]
			case "comment index without text":
				table = appendDOCFixturePLC(t, word, table, 5, 0, 0)
			}
			table = appendDOCFixtureTable(t, word, table, 11, index)
			if name == "out of file" {
				binary.LittleEndian.PutUint32(word[0x9A+11*8:], 0xffffffff)
			}
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, "footnotes,headers")).Extract(t.Context(), bytes.NewReader(docStoriesCFB(t, word, table)))
			if name == "zero-filled" {
				if got != "Body\n\nFootnotes\nNote" || err != nil {
					t.Fatalf("%q, %v", got, err)
				}
			} else if got != "" || !errors.Is(err, errDOCCorrupt) {
				t.Fatalf("%q, %v", got, err)
			}
		})
	}
}

func TestDOCStoryIndexMustCoverItsPart(t *testing.T) {
	const note = "\x02One\rTwo\r"
	word, table := docStoriesStreams(t, "Body €\x02\x05!\r", note)
	selected := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, "footnotes"))
	got, err := selected.Extract(t.Context(), bytes.NewReader(docStoriesCFB(t, word, table)))
	if err != nil || !strings.HasSuffix(got, "\n\nFootnotes\nOne\nTwo") {
		t.Fatalf("complete index: %q, %v", got, err)
	}
	// The first paragraph still ends with a paragraph mark but leaves the rest unread.
	binary.LittleEndian.PutUint32(table[docFixturePLCOffset(word, 3)+4:], docFixtureSize(t, len([]rune("\x02One\r"))))
	got, err = selected.Extract(t.Context(), bytes.NewReader(docStoriesCFB(t, word, table)))
	if got != "" || !errors.Is(err, errDOCCorrupt) {
		t.Fatalf("short index: %q, %v", got, err)
	}
}
