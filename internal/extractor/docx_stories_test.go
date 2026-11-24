package extractor

import (
	"bytes"
	"context"
	"encoding/xml"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/docxtest"
	"github.com/Victoroide/quarry-rag/internal/wordprocessingml"
	"github.com/stretchr/testify/require"
)

var (
	testDOCXOfficeNS = wordprocessingml.OfficeRelationships
	testDOCXStrictNS = wordprocessingml.StrictOfficeRelationships
)

func storyProfile(t testing.TB, raw string) DOCXStories {
	t.Helper()
	profile, err := ParseDOCXStories(raw)
	require.NoError(t, err)
	return profile
}

func storyFixture(wordNS, officeNS string) map[string]string {
	const (
		anchors = `<w:p><w:commentRangeStart w:id="1"/><w:r><w:t>Visible body text.</w:t></w:r><w:commentRangeEnd w:id="1"/><w:r><w:commentReference w:id="1"/><w:commentReference w:id="2"/><w:commentReference w:id="1"/></w:r><w:del><w:r><w:t>Deleted body text.</w:t><w:commentReference w:id="3"/></w:r></w:del><w:moveFrom><w:r><w:t>Moved away text.</w:t></w:r></w:moveFrom><w:ins><w:r><w:t> Inserted text.</w:t></w:r></w:ins></w:p>`
		history = `<w:tbl><w:tr><w:trPr><w:del w:id="5" w:author="Row author"/></w:trPr><w:tc><w:p><w:r><w:t>Deleted row text.</w:t><w:commentReference w:id="3"/></w:r></w:p></w:tc></w:tr></w:tbl><w:tbl><w:tr><w:tc><w:tcPr><w:cellDel w:id="6" w:author="Cell author"/></w:tcPr><w:p><w:r><w:t>Deleted cell text.</w:t></w:r></w:p></w:tc></w:tr></w:tbl>`
		section = `<w:p><w:pPr><w:sectPr><w:headerReference w:type="default" r:id="h2"/></w:sectPr></w:pPr></w:p><w:p><w:pPr><w:sectPr/></w:pPr></w:p><w:sectPr><w:headerReference w:type="default" r:id="h1"/><w:headerReference w:type="first" r:id="h3"/><w:footerReference w:type="default" r:id="f1"/><w:sectPrChange><w:sectPr><w:headerReference r:id="inactive"/></w:sectPr></w:sectPrChange></w:sectPr>`
	)
	return map[string]string{
		"_rels/.rels":   docxtest.Relationships(docxtest.Relationship(officeNS, "officeDocument", "main", "/docs/main.xml")),
		"docs/main.xml": docxtest.Document(wordNS, officeNS, anchors+history+`<w:p><w:commentRangeStart w:id="4"/><w:commentRangeEnd w:id="4"/></w:p>`+section),
		"docs/_rels/main.xml.rels": docxtest.Relationships(
			docxtest.Relationship(officeNS, "header", "h3", "/stories/shared.xml"),
			docxtest.Relationship(officeNS, "header", "h1", "header.xml"),
			docxtest.Relationship(officeNS, "header", "h2", "../stories/SHARED.xml"),
			docxtest.Relationship(officeNS, "header", "h2", "../stories/shared.xml"),
			docxtest.Relationship(officeNS, "header", "inactive", "inactive.xml"),
			docxtest.Relationship(officeNS, "footer", "f1", "../stories/footer%20name.xml"),
			docxtest.Relationship(officeNS, "footnotes", "notes", "../stories/notes.xml"),
			docxtest.Relationship(officeNS, "endnotes", "ends", "/stories/ends.xml"),
			docxtest.Relationship(officeNS, "comments", "comments", "../stories/discussion.xml"),
		),
		"docs/header.xml":           docxtest.Story(wordNS, "hdr", docxtest.Paragraph("Second header text.")),
		"docs/inactive.xml":         docxtest.Story(wordNS, "hdr", docxtest.Paragraph("Unreferenced header text.")),
		"stories/shared.xml":        docxtest.Story(wordNS, "hdr", docxtest.Paragraph("Shared header text.")),
		"stories/footer%20name.xml": docxtest.Story(wordNS, "ftr", docxtest.Paragraph("Footer text.")),
		"stories/notes.xml":         docxtest.Story(wordNS, "footnotes", `<w:footnote w:id="1">`+docxtest.Paragraph("Renamed footnote text.")+`</w:footnote>`),
		"stories/ends.xml":          docxtest.Story(wordNS, "endnotes", `<w:endnote w:id="1">`+docxtest.Paragraph("Renamed endnote text.")+`</w:endnote>`),
		"stories/discussion.xml": docxtest.Story(wordNS, "comments",
			`<w:comment w:id="1" w:author="Comment Author" w:initials="CA" w:date="2024-01-01T00:00:00Z">`+docxtest.Paragraph("Anchored comment text.")+
				`<w:del>`+docxtest.Paragraph("Deleted comment text.")+`</w:del></w:comment>`+
				`<w:comment w:id="2">`+docxtest.Paragraph("Point comment text.")+`</w:comment>`+
				`<w:comment w:id="3">`+docxtest.Paragraph("Deleted anchor comment.")+`</w:comment>`+
				`<w:comment w:id="4">`+docxtest.Paragraph("Range without a reference.")+`</w:comment>`+
				`<w:comment w:id="5">`+docxtest.Paragraph("Unanchored comment text.")+`</w:comment>`),
		"word/footnotes.xml": docxtest.Story(wordNS, "footnotes", `<w:footnote>`+docxtest.Paragraph("Unreferenced legacy notes.")+`</w:footnote>`),
	}
}

func TestDOCXSelectsRelatedStories(t *testing.T) {
	for _, ns := range []struct{ name, word, office string }{
		{"transitional", docxWordNamespace, testDOCXOfficeNS},
		{"strict", docxStrictNamespace, testDOCXStrictNS},
	} {
		t.Run(ns.name, func(t *testing.T) {
			data := docxPartsBytes(t, storyFixture(ns.word, ns.office))
			const body = "Visible body text. Inserted text."
			const defaults = body + "\nRenamed footnote text.\nRenamed endnote text."
			for _, tt := range []struct{ profile, want string }{
				{"", defaults},
				{"body", body},
				{"endnotes,footnotes", "Renamed footnote text.\nRenamed endnote text."},
				{"comments", "[DOCX comment]\nAnchored comment text.\n\n[DOCX comment]\nPoint comment text."},
				{"comments,footers,body,headers,footnotes,endnotes", defaults +
					"\n\n[DOCX header]\nShared header text.\n\n[DOCX header]\nSecond header text." +
					"\n\n[DOCX footer]\nFooter text.\n\n[DOCX comment]\nAnchored comment text.\n\n[DOCX comment]\nPoint comment text."},
			} {
				t.Run(tt.profile, func(t *testing.T) {
					got, err := NewDOCX(storyProfile(t, tt.profile)).Extract(t.Context(), bytes.NewReader(data))
					require.NoError(t, err)
					require.Equal(t, tt.want, got)
				})
			}
		})
	}
}

func TestDOCXRejectsUnsafeSelectedRelationships(t *testing.T) {
	link := func(target string) string {
		return docxtest.Relationship(testDOCXOfficeNS, "header", "h", target)
	}
	for _, tt := range []struct{ name, entries string }{
		{"external mode", strings.Replace(link("header.xml"), "/>", ` TargetMode="External"/>`, 1)},
		{"absolute URI", link("urn:fixture:header")},
		{"authority", link("//example.invalid/header.xml")},
		{"relative escape", link("../../header.xml")},
		{"absolute escape", link("/../header.xml")},
		{"encoded dot escape", link("%2e%2e/%2e%2e/header.xml")},
		{"encoded slash", link("%2fheader.xml")},
		{"backslash", link(`..\header.xml`)},
		{"fragment", link("header.xml#other")},
		{"cycle", link("./DOCUMENT.xml")},
		{"conflicting ID", link("header.xml") + link("another.xml")},
		{"missing target part", link("missing.xml")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := docxPartsBytes(t, map[string]string{
				"word/document.xml": docxtest.Document(docxWordNamespace, testDOCXOfficeNS,
					docxtest.Paragraph("Visible body text.")+`<w:sectPr><w:headerReference r:id="h"/></w:sectPr>`),
				"word/_rels/document.xml.rels": docxtest.Relationships(tt.entries),
				"word/header.xml":              docxtest.Story(docxWordNamespace, "hdr", docxtest.Paragraph("Valid header text.")),
			})
			body, err := New(FileTypeDOCX).Extract(t.Context(), bytes.NewReader(data))
			require.NoError(t, err)
			require.Equal(t, "Visible body text.", body)
			got, err := NewDOCX(storyProfile(t, "body,headers")).Extract(t.Context(), bytes.NewReader(data))
			require.Error(t, err)
			require.Empty(t, got)
		})
	}
}

func TestDOCXRejectsUnsafePackageMainRelationships(t *testing.T) {
	for _, target := range []string{"/../word/document.xml", "urn:fixture:document", "%2e%2e/word/document.xml", "_rels/.rels"} {
		t.Run(target, func(t *testing.T) {
			data := docxPartsBytes(t, map[string]string{
				"word/document.xml": docxtest.Document(docxWordNamespace, testDOCXOfficeNS, docxtest.Paragraph("Visible fallback body.")),
				"_rels/.rels":       docxtest.Relationships(docxtest.Relationship(testDOCXOfficeNS, "officeDocument", "main", target)),
			})
			got, err := New(FileTypeDOCX).Extract(t.Context(), bytes.NewReader(data))
			require.Error(t, err)
			require.Empty(t, got)
		})
	}
}

func TestDOCXValidatesSelectedXML(t *testing.T) {
	const section = `<w:sectPr><w:headerReference r:id="h"/></w:sectPr>`
	for _, tt := range []struct{ name, header string }{
		{"truncated", docxtest.Story(docxWordNamespace, "hdr", docxtest.Paragraph("Valid header text.")) + "<"},
		{"second root", docxtest.Story(docxWordNamespace, "hdr", "") + "<extra/>"},
		{"wrong story root", docxtest.Story(docxWordNamespace, "comments", docxtest.Paragraph("Wrong story text."))},
		{"DTD", `<!DOCTYPE hdr [<!ENTITY private "Expanded private text">]>` + docxtest.Story(docxWordNamespace, "hdr", docxtest.Paragraph("&private;"))},
		{"deep markup", docxtest.Story(docxWordNamespace, "hdr", strings.Repeat("<nested>", 1024)+strings.Repeat("</nested>", 1024))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := docxPartsBytes(t, map[string]string{
				"word/document.xml":            docxtest.Document(docxWordNamespace, testDOCXOfficeNS, docxtest.Paragraph("Visible body text.")+section),
				"word/_rels/document.xml.rels": docxtest.Relationships(docxtest.Relationship(testDOCXOfficeNS, "header", "h", "selected.xml")),
				"word/selected.xml":            tt.header,
			})
			got, err := NewDOCX(storyProfile(t, "body,headers")).Extract(t.Context(), bytes.NewReader(data))
			require.Error(t, err)
			require.Empty(t, got)
		})
	}
}

func TestDOCXMissingAndEmptyOptionalStories(t *testing.T) {
	parts := map[string]string{
		"word/document.xml":            docxtest.Document(docxWordNamespace, testDOCXOfficeNS, docxtest.Paragraph("Visible body text.")),
		"word/_rels/document.xml.rels": docxtest.Relationships(docxtest.Relationship(testDOCXOfficeNS, "comments", "c", "comments.xml")),
		"word/comments.xml":            "<unparsed",
	}
	selector := NewDOCX(storyProfile(t, "footnotes,endnotes,headers,footers,comments"))
	got, err := selector.Extract(t.Context(), bytes.NewReader(docxPartsBytes(t, parts)))
	require.ErrorIs(t, err, ErrNoText)
	require.Empty(t, got)

	parts["word/document.xml"] = docxtest.Document(docxWordNamespace, testDOCXOfficeNS, `<w:p><w:r><w:commentReference w:id="1"/></w:r></w:p>`)
	parts["word/comments.xml"] = docxtest.Story(docxWordNamespace, "comments", `<w:comment w:id="1">`+docxtest.Paragraph("Tiny")+`</w:comment>`)
	got, err = selector.Extract(t.Context(), bytes.NewReader(docxPartsBytes(t, parts)))
	require.ErrorIs(t, err, ErrNoText)
	require.Empty(t, got)
}

func TestDOCXOptionalLabelsShareTextLimit(t *testing.T) {
	const label = "[DOCX header]\n"
	for _, size := range []int{domain.MaxContentBytes - len(label), domain.MaxContentBytes - len(label) + 1} {
		data := docxPartsBytes(t, map[string]string{
			"word/document.xml":            docxtest.Document(docxWordNamespace, testDOCXOfficeNS, `<w:sectPr><w:headerReference r:id="h"/></w:sectPr>`),
			"word/_rels/document.xml.rels": docxtest.Relationships(docxtest.Relationship(testDOCXOfficeNS, "header", "h", "selected.xml")),
			"word/selected.xml":            docxtest.Story(docxWordNamespace, "hdr", docxtest.Paragraph(strings.Repeat("x", size))),
		})
		got, err := NewDOCX(storyProfile(t, "headers")).Extract(t.Context(), bytes.NewReader(data))
		if size+len(label) > domain.MaxContentBytes {
			require.ErrorIs(t, err, ErrContentTooLarge)
			require.Empty(t, got)
		} else {
			require.NoError(t, err)
			require.Len(t, got, domain.MaxContentBytes)
		}
	}
}

func TestDOCXSelectedXMLSharesWorkLimit(t *testing.T) {
	parts := storyFixture(docxWordNamespace, testDOCXOfficeNS)
	padding := strings.Repeat(" ", 13<<20)
	for _, p := range []struct{ path, root string }{
		{"stories/shared.xml", "hdr"}, {"docs/header.xml", "hdr"},
		{"stories/footer%20name.xml", "ftr"}, {"stories/discussion.xml", "comments"},
	} {
		parts[p.path] = docxtest.Story(docxWordNamespace, p.root, padding)
	}
	data := docxPartsBytes(t, parts)
	got, err := New(FileTypeDOCX).Extract(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Contains(t, got, "Visible body text.")
	got, err = NewDOCX(storyProfile(t, "body,headers,footers,comments")).Extract(t.Context(), bytes.NewReader(data))
	require.ErrorContains(t, err, "cumulative")
	require.Empty(t, got)
}

func TestDOCXXMLCancellationDiscardsBufferedTokens(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := strings.NewReader(docxtest.Story(docxWordNamespace, "hdr", docxtest.Paragraph("Still buffered text.")))
	tokens := 0
	err := readDOCXXML(ctx, r, "hdr", isDOCXWordNamespace, func(xml.Token, int, map[string]string) error {
		tokens++
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, tokens)
}

func TestDOCXAcceptsUTF8BOM(t *testing.T) {
	parts := storyFixture(docxStrictNamespace, testDOCXStrictNS)
	for _, name := range []string{"_rels/.rels", "docs/main.xml", "docs/_rels/main.xml.rels", "docs/header.xml", "stories/discussion.xml"} {
		parts[name] = "\ufeff" + parts[name]
	}
	got, err := NewDOCX(storyProfile(t, "body,headers,comments")).Extract(t.Context(), bytes.NewReader(docxPartsBytes(t, parts)))
	require.NoError(t, err)
	require.Contains(t, got, "[DOCX header]\nSecond header text.")
	require.Contains(t, got, "[DOCX comment]\nAnchored comment text.")
}

func FuzzDOCXStories(f *testing.F) {
	parts := storyFixture(docxStrictNamespace, testDOCXStrictNS)
	f.Add(parts["_rels/.rels"], parts["docs/_rels/main.xml.rels"], parts["docs/main.xml"], parts["stories/discussion.xml"])
	f.Add("", "", "<document", "<comments")
	f.Add(docxtest.Relationships(), docxtest.Relationships(docxtest.Relationship(testDOCXOfficeNS, "header", "h", "%2e%2e/%2fescape.xml")),
		docxtest.Document(docxWordNamespace, testDOCXOfficeNS, `<w:sectPr><w:headerReference r:id="h"/></w:sectPr>`), "<hdr/>")
	profile := storyProfile(f, "body,footnotes,endnotes,headers,footers,comments")
	f.Fuzz(func(t *testing.T, root, relationships, main, story string) {
		if len(root)+len(relationships)+len(main)+len(story) > 16<<10 {
			t.Skip()
		}
		data := docxPartsBytes(t, map[string]string{
			"_rels/.rels": root, "docs/main.xml": main, "docs/_rels/main.xml.rels": relationships,
			"word/document.xml": main, "word/_rels/document.xml.rels": relationships,
			"stories/discussion.xml": story, "word/selected.xml": story,
			"docs/header.xml": parts["docs/header.xml"], "stories/shared.xml": parts["stories/shared.xml"],
			"stories/footer%20name.xml": parts["stories/footer%20name.xml"],
			"stories/notes.xml":         parts["stories/notes.xml"], "stories/ends.xml": parts["stories/ends.xml"],
		})
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		got, err := NewDOCX(profile).Extract(ctx, bytes.NewReader(data))
		if err == nil {
			require.True(t, utf8.ValidString(got))
			require.LessOrEqual(t, len(got), domain.MaxContentBytes)
		} else {
			require.Empty(t, got)
		}
	})
}
