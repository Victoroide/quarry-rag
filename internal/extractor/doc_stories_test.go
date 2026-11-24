package extractor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

func docStoriesStreams(t testing.TB, body, footnote string) (word, table []byte) {
	t.Helper()
	body += "\f\r" // An empty second section supplies its own page stories.
	const (
		separator = "separator noise\r"
		header    = "頭頁 Header\r\r"
		footer    = "Footer café\r\r"
		comment   = "\x05\x13author field instruction\x14Private comment\x15\r"
		endnote   = "\x02末尾 Endnote\r"
		textbox   = "Textbox noise\r"
	)
	hdd := separator + header + footer + header
	word, table = docStreams(t,
		docCompressedPiece(t, body[:len(body)-1]),
		docUnicodePiece(body[len(body)-1:]+footnote+"\r"+separator+header),
		docCompressedPiece(t, footer),
		docUnicodePiece(header+"\r"+comment+"\r"+endnote+"\r"+textbox+"\r"),
	)
	for _, part := range []struct {
		offset int
		text   string
	}{
		{0x4C, body}, {0x50, footnote + "\r"}, {0x54, hdd + "\r"},
		{0x5C, comment + "\r"}, {0x60, endnote + "\r"}, {0x64, textbox},
	} {
		binary.LittleEndian.PutUint32(word[part.offset:], docFixtureSize(t, len(docUnicodePiece(part.text).data)/2))
	}
	copy(word[0x700:], "FIB and formatting metadata noise")
	// Word 2003 extends the Word 97 pair table and FIB without moving its indexes.
	binary.LittleEndian.PutUint16(word[0x98:], 0xA4)
	binary.LittleEndian.PutUint16(word[0x9A+8*0xA4:], 2)
	binary.LittleEndian.PutUint16(word[0x9C+8*0xA4:], 0x010C)
	separatorEnd := docFixtureSize(t, len(separator))
	headerEnd := separatorEnd + docFixtureSize(t, len([]rune(header)))
	footerEnd := headerEnd + docFixtureSize(t, len([]rune(footer)))
	secondHeaderEnd := footerEnd + docFixtureSize(t, len([]rune(header)))
	// Six separators, then even/odd header, even/odd footer, first header/footer.
	// Section two repeats the header and inherits the previous footer.
	table = appendDOCFixturePLC(t, word, table, 11,
		0, separatorEnd, separatorEnd, separatorEnd, separatorEnd, separatorEnd,
		separatorEnd, separatorEnd, headerEnd, headerEnd, footerEnd, footerEnd,
		footerEnd, footerEnd, secondHeaderEnd, secondHeaderEnd, secondHeaderEnd, secondHeaderEnd,
		secondHeaderEnd, 0xffffffff)
	bodyEnd := docFixtureSize(t, len(docUnicodePiece(body).data)/2)
	sections := []byte{0, 0, 0, 0}
	sections = binary.LittleEndian.AppendUint32(sections, bodyEnd-1)
	sections = binary.LittleEndian.AppendUint32(sections, bodyEnd)
	table = appendDOCFixtureTable(t, word, table, 6, append(sections, make([]byte, 24)...))
	table = appendDOCFixturePLC(t, word, table, 3, 0, docFixtureSize(t, len([]rune(footnote))), 0xffffffff)
	table = appendDOCFixturePLC(t, word, table, 5, 0, docFixtureSize(t, len(comment)), 0xffffffff)
	table = appendDOCFixturePLC(t, word, table, 47, 0, docFixtureSize(t, len([]rune(endnote))), 0xffffffff)
	table = appendDOCFixtureTable(t, word, table, 4, append([]byte{7, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}, bytes.Repeat([]byte("XY"), 15)...))
	return word, table
}

func docStoriesCFB(t testing.TB, word, table []byte) []byte {
	t.Helper()
	return buildCFB(t, []cfbStream{{name: "WordDocument", data: word}, {name: "0Table", data: table}}, cfbOptions{})
}

func mustDOCStories(t testing.TB, config string) DOCStories {
	t.Helper()
	stories, err := ParseDOCStories(config)
	if err != nil {
		t.Fatal(err)
	}
	return stories
}

func TestDOCStorySelection(t *testing.T) {
	word, table := docStoriesStreams(t, "Body €\x02\x05!\r", "\x02Footnote café\r")
	data := docStoriesCFB(t, word, table)
	for _, tc := range []struct{ profile, want string }{
		{"body", "Body €  !"},
		{"body,footnotes", "Body €  !\n\nFootnotes\nFootnote café"},
		{"body,endnotes", "Body €  !\n\nEndnotes\n末尾 Endnote"},
		{"body,headers", "Body €  !\n\nHeaders\n頭頁 Header"},
		{"body,footers", "Body €  !\n\nFooters\nFooter café"},
		{"body,comments", "Body €  !\n\nComments\nPrivate comment"},
		{"body,comments,headers,endnotes,footnotes,footers", "Body €  !\n\nFootnotes\nFootnote café\n\nEndnotes\n末尾 Endnote\n\nHeaders\n頭頁 Header\n\nFooters\nFooter café\n\nComments\nPrivate comment"},
	} {
		t.Run(tc.profile, func(t *testing.T) {
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, tc.profile)).Extract(t.Context(), bytes.NewReader(data))
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
	got, err := New(FileTypeDOC).Extract(t.Context(), bytes.NewReader(data))
	if err != nil || got != "Body €  !" {
		t.Fatalf("default got %q, %v", got, err)
	}
}

func TestDOCStoryIndexes(t *testing.T) {
	const footnote = "\x02Visible note\r"
	for _, tc := range []struct {
		name   string
		mutate func(word, table []byte)
	}{
		{"missing text index", func(w, _ []byte) { binary.LittleEndian.PutUint32(w[0x9A+3*8+4:], 0) }},
		{"out-of-file comment index", func(w, _ []byte) { binary.LittleEndian.PutUint32(w[0x9A+5*8:], 0xffffffff) }},
		{"non-multiple CP index", func(w, _ []byte) { binary.LittleEndian.PutUint32(w[0x9A+3*8+4:], 11) }},
		{"invalid count for header slots", func(w, _ []byte) { binary.LittleEndian.PutUint32(w[0x9A+11*8+4:], 76) }},
		{"descending header CP", func(w, tb []byte) { binary.LittleEndian.PutUint32(tb[docFixturePLCOffset(w, 11)+4*2:], 6) }},
		{"nonterminal note CP", func(w, tb []byte) { binary.LittleEndian.PutUint32(tb[docFixturePLCOffset(w, 3)+4:], 1) }},
		{"annotation reference out of body", func(w, tb []byte) { binary.LittleEndian.PutUint32(tb[docFixturePLCOffset(w, 4):], 100) }},
		{"annotation reference wrong mark", func(w, tb []byte) { binary.LittleEndian.PutUint32(tb[docFixturePLCOffset(w, 4):], 0) }},
		{"misaligned reference table", func(w, _ []byte) { binary.LittleEndian.PutUint32(w[0x9A+4*8+4:], 37) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			word, table := docStoriesStreams(t, "Body €\x02\x05!\r", footnote)
			tc.mutate(word, table)
			data := docStoriesCFB(t, word, table)
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, "headers,footnotes,comments")).Extract(t.Context(), bytes.NewReader(data))
			if got != "" || !errors.Is(err, errDOCCorrupt) {
				t.Fatalf("got %q, %v", got, err)
			}
		})
	}
}

func docFixturePLCOffset(word []byte, index int) uint32 {
	return binary.LittleEndian.Uint32(word[0x9A+8*index:])
}

func TestDOCEmptyStoryDoesNotAddAHeading(t *testing.T) {
	word, table := docStoriesStreams(t, "Body €\x02\x05!\r", "\x02\r")
	data, err := os.ReadFile("testdata/stories.doc")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, docStoriesCFB(t, word, table)) {
		t.Fatal("shared DOC fixture differs from its synthetic Word 2003 source")
	}
	got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, "body,footnotes")).Extract(t.Context(), bytes.NewReader(data))
	if err != nil || got != "Body €  !" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestDOCReferenceMarks(t *testing.T) {
	for _, tc := range []struct {
		name, body, note, want string
		index                  byte
	}{
		{"automatic", "Body €\x02\x05!\r", "\x02Note\r", "Body €  !\n\nFootnotes\nNote", 1},
		{"custom", "Body €*\x05!\r", "*Note\r", "Body €* !\n\nFootnotes\n*Note", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			word, table := docStoriesStreams(t, tc.body, tc.note)
			table = appendDOCFixtureTable(t, word, table, 2, []byte{6, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, tc.index, 0})
			data := docStoriesCFB(t, word, table)
			got, err := NewWithDOCStories(FileTypeDOC, mustDOCStories(t, "body,footnotes")).Extract(t.Context(), bytes.NewReader(data))
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func FuzzDOCStoryPLC(f *testing.F) {
	word, table := docStoriesStreams(f, "Body €\x02\x05!\r", "\x02Footnote café\r")
	fib, err := readDOCFIB(word, true)
	if err != nil {
		f.Fatal(err)
	}
	clx, _ := fib.tableRange(table, 33)
	plc, err := readDOCPieceTable(clx)
	if err != nil {
		f.Fatal(err)
	}
	selected := mustDOCStories(f, "footnotes,endnotes,headers,footers,comments")
	f.Add(table)
	f.Add(table[:len(table)-1])
	f.Fuzz(func(t *testing.T, table []byte) {
		pieces, err := newDOCPieces(t.Context(), word, plc, fib)
		if err != nil {
			t.Fatal(err)
		}
		var decodedBytes int
		var out docStoryOutput
		for _, index := range docStoryIndexes {
			storyPLC, err := index.read(t.Context(), table, fib, pieces)
			if err != nil {
				return
			}
			if err := index.extract(t.Context(), storyPLC, fib.parts[index.part], pieces, selected, &decodedBytes, &out); err != nil {
				return
			}
		}
		if got := len(out.text()); got != out.size || got > domain.MaxContentBytes {
			t.Fatalf("rendered %d bytes with a cumulative count of %d", got, out.size)
		}
	})
}

func TestSandboxDOCStorySelectionAndLimits(t *testing.T) {
	stories := mustDOCStories(t, "body,footnotes")
	sb, err := NewSandbox(SandboxConfig{Timeout: 10 * time.Second, DOCStories: stories})
	if err != nil {
		t.Fatal(err)
	}
	const body = "Body €\x02\x05!\r"
	word, table := docStoriesStreams(t, body, "\x02Footnote\r")
	data := docStoriesCFB(t, word, table)
	if got, extractErr := sb.Extract(t.Context(), FileTypeDOC, data); extractErr != nil || got != "Body €  !\n\nFootnotes\nFootnote" {
		t.Fatalf("isolated selection: %q, %v", got, extractErr)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, extractErr := sb.Extract(ctx, FileTypeDOC, data); got != "" || !errors.Is(extractErr, context.Canceled) {
		t.Fatalf("isolated cancellation: %q, %v", got, extractErr)
	}
	binary.LittleEndian.PutUint32(word[0x40:], 0x810)
	if got, extractErr := sb.Extract(t.Context(), FileTypeDOC, docStoriesCFB(t, word, table)); got != "" || !errors.Is(extractErr, io.ErrUnexpectedEOF) {
		t.Fatalf("isolated EOF: %q, %v", got, extractErr)
	}
	if got, extractErr := sb.Extract(t.Context(), FileTypeDOC, make([]byte, maxSourceBytes+1)); got != "" || extractErr == nil || !strings.Contains(extractErr.Error(), "source exceeds maximum size") {
		t.Fatalf("isolated source limit: %q, %v", got, extractErr)
	}
	for _, tc := range []struct {
		name string
		size int
	}{
		{"decoded text", domain.MaxContentBytes - len(body+"\f\r")},
		{"combined output including headings", domain.MaxContentBytes - len(body+"\f\r") - 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			word, table := docStoriesStreams(t, body, "\x02"+strings.Repeat("x", tc.size)+"\r")
			if got, extractErr := sb.Extract(t.Context(), FileTypeDOC, docStoriesCFB(t, word, table)); got != "" || !errors.Is(extractErr, ErrContentTooLarge) {
				t.Fatalf("isolated shared text limit: %d bytes, %v", len(got), extractErr)
			}
		})
	}
}
