package extractor

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

func FuzzExtractors(f *testing.F) {
	for _, seed := range extractorFuzzSeeds(f) {
		f.Add(seed.data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		for _, ft := range []FileType{FileTypeText, FileTypePDF, FileTypeDOCX, FileTypeDOC} {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			type result struct {
				text string
				err  error
			}
			done := make(chan result, 1)
			go func() {
				text, err := New(ft).Extract(ctx, bytes.NewReader(data))
				done <- result{text, err}
			}()
			timer := time.NewTimer(5 * time.Second)
			select {
			case got := <-done:
				timer.Stop()
				cancel()
				if got.err != nil {
					continue
				}
				if !utf8.ValidString(got.text) || len(got.text) > domain.MaxContentBytes {
					t.Fatalf("%s returned invalid UTF-8 or excessive text (%d bytes)", ft, len(got.text))
				}
				if strings.ContainsRune(got.text, 0) {
					t.Fatalf("%s extraction returned NUL", ft)
				}
			case <-timer.C:
				cancel()
				t.Fatalf("%s extraction exceeded 5 seconds", ft)
			}
		}
	})
}

func extractorFuzzSeeds(t testing.TB) []struct {
	name string
	data []byte
} {
	t.Helper()
	xml := `<w:document xmlns:w="` + docxWordNamespace + `"><w:body><w:p><w:r><w:t>Document preservation requires stable storage.</w:t></w:r></w:p></w:body></w:document>`
	return []struct {
		name string
		data []byte
	}{
		{"document.pdf", fuzzPDF(t, false)},
		{"document.pdf", fuzzPDF(t, true)},
		{"document.docx", docxBytes(t, xml)},
		{"document.docx", docxPartsBytes(t, map[string]string{
			"_rels/.rels":     `<Relationships><Relationship Type="office/officeDocument" Target="custom/main.xml"/></Relationships>`,
			"custom/main.xml": xml,
		})},
		{"document.doc", docFixture(t, docCompressedPiece(t, "Document preservation requires stable storage."))},
		{"document.doc", docFixture(t, docUnicodePiece("Document preservation 世界 🌍."))},
		{"document.doc", buildCFB(t, []cfbStream{{name: "WordDocument", data: bytes.Repeat([]byte("x"), 512)}}, cfbOptions{cyclic: true})},
		{"document.txt", []byte("Plain text 世界 🌍")},
		{"document.md", []byte("# Document\nReadable text with a paragraph.")},
		{"document.txt", []byte("\xef\xbb\xbfReadable plain text")},
		{"document.txt", []byte{0xff, 0xfe, 'H', 0, 'e', 0, 'l', 0, 'l', 0, 'o', 0, ' ', 0, 'w', 0, 'o', 0, 'r', 0, 'l', 0, 'd', 0}},
		{"document.txt", []byte{0xfe, 0xff, 0, 'H', 0, 'e', 0, 'l', 0, 'l', 0, 'o', 0, ' ', 0, 'w', 0, 'o', 0, 'r', 0, 'l', 0, 'd'}},
		{"document.txt", []byte("Readable\x00 text \xff")},
		{"document.txt", []byte(strings.Repeat("x", 8191) + "世界")},
		{"document.txt", nil},
		{"document.pdf", []byte("%PDF-1.7\n")},
		{"document.docx", []byte("PK\x03\x04")},
		{"document.doc", []byte{0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1}},
	}
}

func fuzzPDF(t testing.TB, compressed bool) []byte {
	t.Helper()
	data := []byte("BT /F1 12 Tf (Document preservation requires stable storage.) Tj ET")
	filter := ""
	if compressed {
		var encoded bytes.Buffer
		writer := zlib.NewWriter(&encoded)
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		data, filter = encoded.Bytes(), " /Filter /FlateDecode"
	}
	objects := [][]byte{
		[]byte("<< /Type /Catalog /Pages 2 0 R >>"),
		[]byte("<< /Type /Pages /Kids [3 0 R] /Count 1 >>"),
		[]byte("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>"),
		fmt.Appendf(nil, "<< /Length %d%s >>\nstream\n%s\nendstream", len(data), filter, data),
		[]byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"),
	}
	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n")
	offsets := make([]int, len(objects))
	for i, object := range objects {
		offsets[i] = out.Len()
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", i+1, object)
	}
	xref := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for _, offset := range offsets {
		fmt.Fprintf(&out, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xref)
	return out.Bytes()
}
