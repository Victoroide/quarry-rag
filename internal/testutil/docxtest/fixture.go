// Package docxtest builds WordprocessingML package fixtures.
package docxtest

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/wordprocessingml"
)

// Package writes the named parts to a ZIP archive.
func Package(t testing.TB, parts map[string]string) []byte {
	t.Helper()
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	for name, content := range parts {
		part, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(part, content); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

// Document wraps the provided body markup in a main document part.
func Document(word, office, body string) string {
	return fmt.Sprintf(`<w:document xmlns:w=%q xmlns:r=%q><w:body>%s</w:body></w:document>`, word, office, body)
}

// Story wraps the provided markup in a named story root.
func Story(word, root, inside string) string {
	return fmt.Sprintf(`<w:%s xmlns:w=%q>%s</w:%s>`, root, word, inside, root)
}

// Paragraph wraps text in a Word paragraph and run.
func Paragraph(text string) string {
	return "<w:p><w:r><w:t>" + text + "</w:t></w:r></w:p>"
}

// Relationships wraps relationship entries in the package relationship root.
func Relationships(entries ...string) string {
	return fmt.Sprintf(`<Relationships xmlns=%q>%s</Relationships>`, wordprocessingml.PackageRelationships, strings.Join(entries, ""))
}

// Relationship builds a relationship entry for a given Office vocabulary.
func Relationship(office, kind, id, target string) string {
	return fmt.Sprintf(`<Relationship Id=%q Type=%q Target=%q/>`, id, office+"/"+kind, target)
}

// RenamedMainAtWorkLimit builds a package whose expanded XML, counting the root
// relationship part twice, occupies the specified aggregate number of bytes.
func RenamedMainAtWorkLimit(t testing.TB, perPart, total int) []byte {
	t.Helper()
	word, office := wordprocessingml.Word, wordprocessingml.OfficeRelationships
	const root, main, related = "_rels/.rels", "parts/main.xml", "parts/_rels/main.xml.rels"
	parts := map[string]string{
		root: Relationships(Relationship(office, "officeDocument", "main", main)),
		main: Document(word, office, Paragraph("Readable body text.")+
			`<w:sectPr><w:headerReference r:id="h1"/><w:headerReference r:id="h2"/><w:headerReference r:id="h3"/></w:sectPr>`),
		related: Relationships(
			Relationship(office, "header", "h1", "header1.xml"),
			Relationship(office, "header", "h2", "header2.xml"),
			Relationship(office, "header", "h3", "header3.xml")),
	}
	remaining := total - 2*len(parts[root]) - len(parts[related])
	for _, name := range []string{main, "parts/header1.xml", "parts/header2.xml", "parts/header3.xml"} {
		if name != main {
			parts[name] = Story(word, "hdr", Paragraph("Readable header text."))
		}
		size := min(perPart, remaining)
		parts[name] += strings.Repeat(" ", size-len(parts[name]))
		remaining -= size
	}
	if remaining != 0 {
		t.Fatalf("fixture is %d bytes below the requested size", remaining)
	}
	return Package(t, parts)
}
