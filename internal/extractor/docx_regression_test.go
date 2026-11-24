package extractor

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/docxtest"
	"github.com/Victoroide/quarry-rag/internal/wordprocessingml"
	"github.com/stretchr/testify/require"
)

func extractStoryFixture(t *testing.T, profile string, parts map[string]string) (string, error) {
	t.Helper()
	return NewDOCX(storyProfile(t, profile)).Extract(t.Context(), bytes.NewReader(docxPartsBytes(t, parts)))
}

func TestDOCXRejectsDuplicateXMLAttributesInSkippedSubtrees(t *testing.T) {
	header := `<w:p xmlns:a="urn:fixture:attributes" xmlns:b="urn:fixture:attributes">
		<w:r><w:t>Readable heading text.</w:t></w:r>
		<w:del><w:r><w:t a:space="one" b:space="two">Deleted text.</w:t></w:r></w:del>
	</w:p>`
	parts := storyFixture(docxWordNamespace, testDOCXOfficeNS)
	parts["stories/shared.xml"] = docxtest.Story(docxWordNamespace, "hdr", header)
	got, err := extractStoryFixture(t, "body,headers", parts)
	require.ErrorContains(t, err, "duplicate XML attribute")
	require.Empty(t, got)
}

func TestDOCXRejectsMalformedNamespacesInSkippedSubtrees(t *testing.T) {
	for _, tt := range []struct{ name, markup string }{
		{"undeclared element prefix", `<x:r/>`},
		{"undeclared attribute prefix", `<w:r x:attr="value"/>`},
		{"empty prefixed binding", `<w:r xmlns:x=""/>`},
		{"reserved XML prefix", `<w:r xmlns:xml="urn:fixture:wrong"/>`},
		{"reserved declaration prefix", `<w:r xmlns:xmlns="urn:fixture:wrong"/>`},
		{"declaration prefix on element", `<xmlns:r/>`},
		{"reserved XML namespace as default", `<w:r xmlns="` + wordprocessingml.XML + `"/>`},
		{"reserved declaration namespace alias", `<w:r xmlns:x="` + wordprocessingml.XMLNS + `"/>`},
		{"duplicate namespace declaration", `<w:r xmlns:x="urn:fixture:one" xmlns:x="urn:fixture:two"/>`},
		{"expired namespace declaration", `<x:r xmlns:x="urn:fixture:name"/><x:r/>`},
		{"mismatched qualified end tag", `<x:r xmlns:x="urn:fixture:name" xmlns:y="urn:fixture:name"></y:r>`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parts := storyFixture(docxWordNamespace, testDOCXOfficeNS)
			parts["stories/shared.xml"] = docxtest.Story(docxWordNamespace, "hdr",
				docxtest.Paragraph("Readable heading text.")+`<w:del>`+tt.markup+`</w:del>`)
			got, err := extractStoryFixture(t, "headers", parts)
			require.Error(t, err)
			require.Empty(t, got)
		})
	}
}

func TestDOCXRequiresValidLiveCommentIDs(t *testing.T) {
	for _, id := range []string{"", "invalid", "+", "1.0", "1 2", "１２"} {
		t.Run(id, func(t *testing.T) {
			body := `<w:p><w:r><w:t>Visible body text.</w:t><w:commentReference w:id="` + id + `"/></w:r></w:p>`
			parts := map[string]string{"word/document.xml": docxtest.Document(docxWordNamespace, testDOCXOfficeNS, body)}
			got, err := extractStoryFixture(t, "body,comments", parts)
			require.ErrorContains(t, err, "invalid DOCX comment reference")
			require.Empty(t, got)
		})
	}
}

func TestDOCXMatchesUnboundedStrictStoryIDs(t *testing.T) {
	for _, id := range []string{"9223372036854775807", "9223372036854775808", "-9223372036854775809", "0"} {
		t.Run(id, func(t *testing.T) {
			word, office := docxStrictNamespace, testDOCXStrictNS
			definitionID := "+00" + id
			if strings.HasPrefix(id, "-") {
				definitionID = "-00" + id[1:]
			} else if id == "0" {
				definitionID = "-00"
			}
			parts := map[string]string{
				"word/document.xml": docxtest.Document(word, office,
					`<w:p><w:r><w:footnoteReference w:id="`+id+`"/><w:endnoteReference w:id="`+id+`"/><w:commentReference w:id="`+id+`"/></w:r></w:p>`),
				"word/_rels/document.xml.rels": docxtest.Relationships(
					docxtest.Relationship(office, "footnotes", "f", "footnotes.xml"),
					docxtest.Relationship(office, "endnotes", "e", "endnotes.xml"),
					docxtest.Relationship(office, "comments", "c", "comments.xml")),
				"word/footnotes.xml": docxtest.Story(word, "footnotes", `<w:footnote w:id=" `+definitionID+` ">`+docxtest.Paragraph("Matched strict footnote.")+`</w:footnote>`),
				"word/endnotes.xml":  docxtest.Story(word, "endnotes", `<w:endnote w:id="`+definitionID+`">`+docxtest.Paragraph("Matched strict endnote.")+`</w:endnote>`),
				"word/comments.xml":  docxtest.Story(word, "comments", `<w:comment w:id="`+definitionID+`">`+docxtest.Paragraph("Matched strict annotation.")+`</w:comment>`),
			}
			got, err := extractStoryFixture(t, "footnotes,endnotes,comments", parts)
			require.NoError(t, err)
			require.Equal(t, "Matched strict footnote.\nMatched strict endnote.\n\n[DOCX comment]\nMatched strict annotation.", got)
		})
	}
}

func TestDOCXMarkupCompatibility(t *testing.T) {
	wrap := func(inside string) string {
		return `<mc:AlternateContent xmlns:mc="` + docxCompatibilityNamespace + `">` + inside + `</mc:AlternateContent>`
	}
	choice := func(prefix, inside string) string {
		return `<mc:Choice Requires="` + prefix + `">` + inside + `</mc:Choice>`
	}
	fallback := func(inside string) string { return `<mc:Fallback>` + inside + `</mc:Fallback>` }
	for _, tt := range []struct{ name, body, header, profile, want string }{
		{"inline text", `<w:p><w:r>` + wrap(choice("w", `<w:t>The selected inline paragraph.</w:t>`)) + `</w:r></w:p>`, "", "body", "The selected inline paragraph."},
		{"process inline content", `<w:p xmlns:mc="` + docxCompatibilityNamespace + `" xmlns:ext="urn:fixture:extension" mc:Ignorable="ext" mc:ProcessContent="ext:wrapper"><w:r><ext:wrapper><w:t>The projected inline paragraph.</w:t></ext:wrapper></w:r></w:p>`, "", "body", "The projected inline paragraph."},
		{"section reference", docxtest.Paragraph("Visible body text.") + `<w:sectPr>` + wrap(choice("w", `<w:headerReference r:id="h"/>`)) + `</w:sectPr>`, docxtest.Paragraph("Visible header text."), "body,headers", "Visible body text.\n\n[DOCX header]\nVisible header text."},
		{"two understood choices", `<w:sectPr><w:headerReference r:id="h"/></w:sectPr>`, wrap(choice("w", docxtest.Paragraph("First header choice.")) + choice("w", docxtest.Paragraph("Second header choice."))), "headers", "[DOCX header]\nFirst header choice."},
		{"fallback", `<w:p><w:r>` + wrap(`<mc:Choice xmlns:f="urn:fixture:unsupported" Requires="w f"><w:t>Unsupported choice.</w:t></mc:Choice>`+fallback(`<w:t>Readable fallback text.</w:t>`)) + `</w:r></w:p>`, "", "body", "Readable fallback text."},
		{"nested and rebound prefixes", `<w:p><w:r>` + wrap(`<mc:Choice xmlns:w="urn:fixture:unsupported" Requires="w"><w:t>Unsupported text.</w:t></mc:Choice>`+fallback(wrap(choice("w", `<w:t>Nested readable text.</w:t>`)))) + `</w:r></w:p>`, "", "body", "Nested readable text."},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parts := map[string]string{
				"word/document.xml":            docxtest.Document(docxWordNamespace, testDOCXOfficeNS, tt.body),
				"word/_rels/document.xml.rels": docxtest.Relationships(docxtest.Relationship(testDOCXOfficeNS, "header", "h", "header.xml")),
				"word/header.xml":              docxtest.Story(docxWordNamespace, "hdr", tt.header),
			}
			got, err := extractStoryFixture(t, tt.profile, parts)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDOCXMarkupMustUnderstand(t *testing.T) {
	const (
		ordinary  = "Ordinary header line."
		fallback  = "Fallback header line."
		extension = "Extension-only line."
		wantBoth  = "[DOCX header]\n" + ordinary
		must      = ` mc:MustUnderstand="`
	)
	alternate := func(requires, alternateAttrs, choiceAttrs, fallbackAttrs, choiceExtra string) string {
		return `<mc:AlternateContent xmlns:mc="` + docxCompatibilityNamespace + `" xmlns:ex="urn:fixture:extension" mc:Ignorable="ex"` + alternateAttrs + `>` +
			`<mc:Choice Requires="` + requires + `"` + choiceAttrs + `>` + docxtest.Paragraph(ordinary) + `<ex:line>` + extension + `</ex:line>` + choiceExtra + `</mc:Choice>` +
			`<mc:Fallback` + fallbackAttrs + `>` + docxtest.Paragraph(fallback) + `</mc:Fallback></mc:AlternateContent>`
	}
	declared := `xmlns:mc="` + docxCompatibilityNamespace + `" xmlns:ex="urn:fixture:extension" mc:Ignorable="ex"`
	for _, tt := range []struct{ name, markup, want string }{
		{"understood selected choice", alternate("w", "", must+`w"`, "", ""), wantBoth},
		{"unsupported selected choice", alternate("w", "", must+`ex"`, "", ""), ""},
		{"unsupported alongside understood namespace", alternate("w", "", must+`w ex"`, "", ""), ""},
		{"unsupported unselected choice", alternate("ex", "", must+`ex"`, "", ""), "[DOCX header]\n" + fallback},
		{"unsupported selected fallback", alternate("ex", "", "", must+`ex"`, ""), ""},
		{"understood selected fallback", alternate("ex", "", "", must+`w"`, ""), "[DOCX header]\n" + fallback},
		{"unsupported unselected fallback", alternate("w", "", "", must+`ex"`, ""), wantBoth},
		{"unsupported alternate", alternate("w", must+`ex"`, "", "", ""), ""},
		{"unsupported alternate without a selection", alternate("ex", must+`ex"`, "", "", ""), ""},
		{"unsupported element in unselected choice", alternate("ex", "", "", "", `<w:p`+must+`ex"/>`), "[DOCX header]\n" + fallback},
		{"empty list", alternate("w", "", must+`"`, "", ""), wantBoth},
		{"spaced list", alternate("w", "", must+` w  w `+`"`, "", ""), wantBoth},
		{"unbound prefix", alternate("w", "", must+`missing"`, "", ""), ""},
		{"compatibility namespace", alternate("w", "", must+`mc"`, "", ""), ""},
		{"unsupported ordinary element", `<w:p ` + declared + must + `ex"><w:r><w:t>` + ordinary + `</w:t></w:r></w:p>`, ""},
		{"understood ordinary element", `<w:p ` + declared + must + `w"><w:r><w:t>` + ordinary + `</w:t></w:r></w:p>`, wantBoth},
		{"ignored element", `<w:p ` + declared + `><w:r><w:t>` + ordinary + `</w:t></w:r></w:p><ex:line ` + declared + must + `ex">` + extension + `</ex:line>`, wantBoth},
		{"unsupported processed element", `<w:p ` + declared + ` mc:ProcessContent="ex:wrapper"><w:r><ex:wrapper` + must + `ex"><w:t>` + ordinary + `</w:t></ex:wrapper></w:r></w:p>`, ""},
		{"understood processed element", `<w:p ` + declared + ` mc:ProcessContent="ex:wrapper"><w:r><ex:wrapper` + must + `w"><w:t>` + ordinary + `</w:t></ex:wrapper></w:r></w:p>`, wantBoth},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parts := map[string]string{
				"word/document.xml":            docxtest.Document(docxWordNamespace, testDOCXOfficeNS, docxtest.Paragraph("Readable body text.")+`<w:sectPr><w:headerReference w:type="default" r:id="h"/></w:sectPr>`),
				"word/_rels/document.xml.rels": docxtest.Relationships(docxtest.Relationship(testDOCXOfficeNS, "header", "h", "header.xml")),
				"word/header.xml":              docxtest.Story(docxWordNamespace, "hdr", tt.markup),
			}
			got, err := extractStoryFixture(t, "headers", parts)
			if tt.want == "" {
				require.ErrorContains(t, err, "MustUnderstand")
				require.Empty(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDOCXIgnorableMarkupControlsProseAndAnchors(t *testing.T) {
	const prose = "Extension paragraph."
	const visible = "Ordinary paragraph."
	const comment = "[DOCX comment]\n"
	for _, tt := range []struct {
		name, directives, open string
		process                bool
	}{
		{"ignorable", "", `<ext:wrapper>`, false},
		{"process content", ` mc:ProcessContent="alias:wrapper"`, `<ext:wrapper>`, true},
		{"process wildcard", ` mc:ProcessContent="ext:*"`, `<ext:wrapper>`, true},
		{"scoped process content", "", `<ext:wrapper mc:ProcessContent="ext:wrapper">`, true},
		{"rebound prefix", ` mc:ProcessContent="ext:wrapper"`, `<ext:wrapper xmlns:ext="urn:fixture:other" mc:Ignorable="ext">`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parts := map[string]string{
				"word/document.xml": docxtest.Document(docxWordNamespace, testDOCXOfficeNS,
					`<w:sdt xmlns:mc="`+docxCompatibilityNamespace+`" xmlns:ext="urn:fixture:extension" xmlns:alias="urn:fixture:extension" mc:Ignorable="ext"`+tt.directives+`>`+
						`<w:sdtContent>`+tt.open+`<w:p><w:r><w:t>`+prose+`</w:t><w:commentReference w:id="1"/></w:r></w:p></ext:wrapper>`+
						`<ext:other><w:p><w:r><w:commentReference w:id="1"/></w:r></w:p></ext:other>`+
						`</w:sdtContent></w:sdt><w:p><w:r><w:t>`+visible+`</w:t><w:commentReference w:id="2"/></w:r></w:p>`),
				"word/_rels/document.xml.rels": docxtest.Relationships(docxtest.Relationship(testDOCXOfficeNS, "comments", "c", "comments.xml")),
				"word/comments.xml": docxtest.Story(docxWordNamespace, "comments",
					`<w:comment w:id="1">`+docxtest.Paragraph("Extension annotation.")+`</w:comment>`+
						`<w:comment w:id="2">`+docxtest.Paragraph("Ordinary annotation.")+`</w:comment>`),
			}
			wantBody, wantComments := visible, comment+"Ordinary annotation."
			if tt.process {
				wantBody = prose + "\n" + wantBody
				wantComments = comment + "Extension annotation.\n\n" + wantComments
			}
			t.Run("body", func(t *testing.T) {
				got, err := extractStoryFixture(t, "body", parts)
				require.NoError(t, err)
				require.Equal(t, wantBody, got)
			})
			t.Run("comments", func(t *testing.T) {
				got, err := extractStoryFixture(t, "comments", parts)
				require.NoError(t, err)
				require.Equal(t, wantComments, got)
			})
		})
	}
}

func TestDOCXFiltersNotesWithOnlyDeletedReferences(t *testing.T) {
	for _, kind := range []string{"footnote", "endnote"} {
		t.Run(kind, func(t *testing.T) {
			ref := func(id string) string { return "<w:" + kind + "Reference w:id=\"" + id + "\"/>" }
			entry := func(id, text string) string {
				return "<w:" + kind + " w:id=\"" + id + "\">" + docxtest.Paragraph(text) + "</w:" + kind + ">"
			}
			parts := map[string]string{
				"word/document.xml": docxtest.Document(docxWordNamespace, testDOCXOfficeNS,
					`<w:p><w:r><w:t>Visible body text.</w:t>`+ref("1")+ref("3")+`</w:r><w:del><w:r>`+ref("2")+ref("3")+`</w:r></w:del></w:p>`),
				"word/" + kind + "s.xml": docxtest.Story(docxWordNamespace, kind+"s",
					entry("1", "Live note text.")+entry("2", "Deleted note text.")+entry("3", "Still referenced elsewhere.")),
			}
			got, err := extractStoryFixture(t, "", parts)
			require.NoError(t, err)
			require.Equal(t, "Visible body text.\nLive note text.\nStill referenced elsewhere.", got)
		})
	}
}

func TestDOCXMathRunsPreserveWordTextAndComments(t *testing.T) {
	for _, profile := range []struct{ word, office string }{
		{docxWordNamespace, testDOCXOfficeNS},
		{docxStrictNamespace, testDOCXStrictNS},
	} {
		math := strings.TrimSuffix(profile.office, "/relationships") + "/math"
		t.Run(strings.Split(math, "/")[2], func(t *testing.T) {
			parts := map[string]string{
				"word/document.xml": docxtest.Document(profile.word, profile.office, `<w:p><m:oMath xmlns:m="`+math+`"><m:r>
					<w:t>Equation annotation text.</w:t><w:tab/><w:t>Continued text.</w:t><w:commentReference w:id="7"/>
					</m:r></m:oMath></w:p>`),
				"word/_rels/document.xml.rels": docxtest.Relationships(docxtest.Relationship(profile.office, "comments", "c", "comments.xml")),
				"word/comments.xml":            docxtest.Story(profile.word, "comments", `<w:comment w:id="7">`+docxtest.Paragraph("Math comment text.")+`</w:comment>`),
			}
			got, err := extractStoryFixture(t, "body", parts)
			require.NoError(t, err)
			require.Equal(t, "Equation annotation text.\tContinued text.", got)
			got, err = extractStoryFixture(t, "comments", parts)
			require.NoError(t, err)
			require.Equal(t, "[DOCX comment]\nMath comment text.", got)
		})
	}
}

func docxUTF16(order binary.AppendByteOrder, text string) string {
	b := order.AppendUint16(nil, 0xfeff)
	for _, codepoint := range utf16.Encode([]rune(text)) {
		b = order.AppendUint16(b, codepoint)
	}
	return string(b)
}

func TestDOCXReadsUTF16RelationshipsAndStories(t *testing.T) {
	const declaration = `<?xml version="1.0" encoding="UTF-16"?>`
	parts := storyFixture(docxWordNamespace, testDOCXOfficeNS)
	parts["_rels/.rels"] = docxUTF16(binary.BigEndian, declaration+parts["_rels/.rels"])
	parts["docs/_rels/main.xml.rels"] = docxUTF16(binary.LittleEndian, declaration+parts["docs/_rels/main.xml.rels"])
	parts["docs/header.xml"] = docxUTF16(binary.BigEndian, declaration+docxtest.Story(docxWordNamespace, "hdr", docxtest.Paragraph("UTF-16 heading 😀.")))
	data := docxPartsBytes(t, parts)
	ft, err := ResolveContext(t.Context(), "renamed.docx", data)
	require.NoError(t, err)
	require.Equal(t, FileTypeDOCX, ft)
	got, err := NewDOCX(storyProfile(t, "headers")).Extract(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, "[DOCX header]\nShared header text.\n\n[DOCX header]\nUTF-16 heading 😀.", got)
}

func TestDOCXRejectsInvalidUTF16(t *testing.T) {
	valid := docxtest.Story(docxWordNamespace, "hdr", docxtest.Paragraph("Readable heading text."))
	for _, tt := range []struct{ name, part string }{
		{"UTF-8 declaration", docxUTF16(binary.BigEndian, `<?xml version="1.0" encoding="UTF-8"?>`+valid)},
		{"wrong endian declaration", docxUTF16(binary.BigEndian, `<?xml version="1.0" encoding="UTF-16LE"?>`+valid)},
		{"unpaired surrogate", strings.Replace(docxUTF16(binary.BigEndian, valid), "\x00R", "\xd8\x00", 1)},
		{"truncated unit", docxUTF16(binary.BigEndian, valid) + "\x00"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parts := storyFixture(docxWordNamespace, testDOCXOfficeNS)
			parts["docs/header.xml"] = tt.part
			got, err := extractStoryFixture(t, "body,headers", parts)
			require.Error(t, err)
			require.Empty(t, got)
		})
	}
}

func TestDOCXCommentsOnlyFindsAnchorsInOtherStories(t *testing.T) {
	const heading = `<w:sectPr><w:headerReference r:id="h"/><w:footerReference r:id="f"/></w:sectPr>`
	const refs = `<w:p><w:r><w:footnoteReference w:id="1"/><w:endnoteReference w:id="1"/></w:r><w:del><w:r><w:footnoteReference w:id="2"/></w:r></w:del></w:p>`
	withAnchor := func(id string) string {
		return `<w:p><w:r><w:t>Unselected prose.</w:t><w:commentReference w:id="` + id + `"/></w:r></w:p>`
	}
	note := func(kind, noteID, commentID string) string {
		return `<w:` + kind + ` w:id="` + noteID + `">` + withAnchor(commentID) + `</w:` + kind + `>`
	}
	parts := map[string]string{
		"word/document.xml": docxtest.Document(docxWordNamespace, testDOCXOfficeNS, docxtest.Paragraph("Unselected main prose.")+refs+heading),
		"word/_rels/document.xml.rels": docxtest.Relationships(
			docxtest.Relationship(testDOCXOfficeNS, "footnotes", "n", "footnotes.xml"),
			docxtest.Relationship(testDOCXOfficeNS, "endnotes", "e", "endnotes.xml"),
			docxtest.Relationship(testDOCXOfficeNS, "header", "h", "header.xml"),
			docxtest.Relationship(testDOCXOfficeNS, "footer", "f", "footer.xml"),
			docxtest.Relationship(testDOCXOfficeNS, "comments", "c", "comments.xml")),
		"word/footnotes.xml": docxtest.Story(docxWordNamespace, "footnotes", note("footnote", "1", "1")+note("footnote", "2", "5")),
		"word/endnotes.xml":  docxtest.Story(docxWordNamespace, "endnotes", note("endnote", "1", "2")),
		"word/header.xml":    docxtest.Story(docxWordNamespace, "hdr", withAnchor("3")),
		"word/footer.xml":    docxtest.Story(docxWordNamespace, "ftr", withAnchor("4")),
	}
	var comments strings.Builder
	for _, item := range []struct{ id, name string }{{"1", "Footnote annotation."}, {"2", "Endnote annotation."}, {"3", "Header annotation."}, {"4", "Footer annotation."}, {"5", "Deleted annotation."}} {
		comments.WriteString(`<w:comment w:id="` + item.id + `">` + docxtest.Paragraph(item.name) + `</w:comment>`)
	}
	parts["word/comments.xml"] = docxtest.Story(docxWordNamespace, "comments", comments.String())
	got, err := extractStoryFixture(t, "comments", parts)
	require.NoError(t, err)
	require.Equal(t, "[DOCX comment]\nFootnote annotation.\n\n[DOCX comment]\nEndnote annotation.\n\n[DOCX comment]\nHeader annotation.\n\n[DOCX comment]\nFooter annotation.", got)
}

func TestDOCXCommentsRequireLiveEnclosingNote(t *testing.T) {
	for _, kind := range []string{"footnote", "endnote"} {
		t.Run(kind, func(t *testing.T) {
			entry := func(id, prose string) string {
				return `<w:` + kind + ` w:id="` + id + `"><w:p><w:r><w:t>` + prose +
					`</w:t><w:commentReference w:id="` + id + `"/></w:r></w:p></w:` + kind + `>`
			}
			notes := kind + "s"
			parts := map[string]string{
				"word/document.xml": docxtest.Document(docxWordNamespace, testDOCXOfficeNS,
					`<w:p><w:r><w:`+kind+`Reference w:id="1"/></w:r></w:p>`),
				"word/_rels/document.xml.rels": docxtest.Relationships(
					docxtest.Relationship(testDOCXOfficeNS, notes, "n", notes+".xml"),
					docxtest.Relationship(testDOCXOfficeNS, "comments", "c", "comments.xml")),
				"word/" + notes + ".xml": docxtest.Story(docxWordNamespace, notes, entry("1", "Live note prose.")+entry("2", "Orphan note prose.")),
			}
			live := `<w:comment w:id="1">` + docxtest.Paragraph("Live note annotation.") + `</w:comment>`
			orphan := `<w:comment w:id="2">` + docxtest.Paragraph("Orphan note annotation.") + `</w:comment>`
			for _, tt := range []struct{ name, extra string }{{"defined orphan annotation", orphan}, {"missing orphan annotation", ""}} {
				t.Run(tt.name, func(t *testing.T) {
					parts["word/comments.xml"] = docxtest.Story(docxWordNamespace, "comments", live+tt.extra)
					got, err := extractStoryFixture(t, "comments", parts)
					require.NoError(t, err)
					require.Equal(t, "[DOCX comment]\nLive note annotation.", got)
				})
			}
			delete(parts, "word/_rels/document.xml.rels")
			got, err := extractStoryFixture(t, notes, parts)
			require.NoError(t, err)
			require.Equal(t, "Live note prose.\nOrphan note prose.", got)
		})
	}
}

func TestDOCXCommentBlocksAndLabelsShareTextLimit(t *testing.T) {
	const first = "[DOCX comment]\nFirst paragraph.\nSecond paragraph.\n\n[DOCX comment]\n"
	for _, extra := range []int{0, 1} {
		text := strings.Repeat("x", domain.MaxContentBytes-len(first)+extra)
		parts := map[string]string{
			"word/document.xml":            docxtest.Document(docxWordNamespace, testDOCXOfficeNS, `<w:p><w:r><w:commentReference w:id="1"/><w:commentReference w:id="2"/></w:r></w:p>`),
			"word/_rels/document.xml.rels": docxtest.Relationships(docxtest.Relationship(testDOCXOfficeNS, "comments", "c", "comments.xml")),
			"word/comments.xml": docxtest.Story(docxWordNamespace, "comments",
				`<w:comment w:id="1" w:author="Fixture author">`+docxtest.Paragraph("First paragraph.")+docxtest.Paragraph("Second paragraph.")+
					`</w:comment><w:comment w:id="2" w:author="Other author"><w:p/>`+docxtest.Paragraph(text)+`</w:comment>`),
		}
		got, err := extractStoryFixture(t, "comments", parts)
		if extra == 0 {
			require.NoError(t, err)
			require.True(t, got == first+text, "comment blocks or exact byte length differ")
		} else {
			require.ErrorIs(t, err, ErrContentTooLarge)
			require.Empty(t, got)
		}
	}
}

func TestDOCXRejectsConflictingRelationshipIDsInAllSelections(t *testing.T) {
	for _, tt := range []struct{ name, profile, part, entries string }{
		{"package main", "body", "_rels/.rels", docxtest.Relationship(testDOCXOfficeNS, "officeDocument", "h", "word/document.xml") + docxtest.Relationship(testDOCXOfficeNS, "header", "h", "word/header.xml")},
		{"implicit note", "", "word/_rels/document.xml.rels", docxtest.Relationship(testDOCXOfficeNS, "footnotes", "h", "footnotes.xml") + docxtest.Relationship(testDOCXOfficeNS, "header", "h", "header.xml")},
		{"explicit header", "body,headers", "word/_rels/document.xml.rels", docxtest.Relationship(testDOCXOfficeNS, "header", "h", "header.xml") + docxtest.Relationship(testDOCXOfficeNS, "footer", "h", "footer.xml")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parts := map[string]string{
				"word/document.xml":  docxtest.Document(docxWordNamespace, testDOCXOfficeNS, docxtest.Paragraph("Visible body text.")+`<w:sectPr><w:headerReference r:id="h"/></w:sectPr>`),
				"word/footnotes.xml": docxtest.Story(docxWordNamespace, "footnotes", `<w:footnote>`+docxtest.Paragraph("Readable note.")+`</w:footnote>`),
				"word/header.xml":    docxtest.Story(docxWordNamespace, "hdr", docxtest.Paragraph("Readable header.")),
				tt.part:              docxtest.Relationships(tt.entries),
			}
			got, err := extractStoryFixture(t, tt.profile, parts)
			require.Error(t, err)
			require.Empty(t, got)
		})
	}
}

func TestDOCXDeletedParagraphMarkJoinsRawText(t *testing.T) {
	const revised = `<w:p><w:pPr><w:rPr><w:del w:id="1"/></w:rPr></w:pPr><w:r><w:t xml:space="preserve">Compound </w:t></w:r></w:p>`
	parts := map[string]string{"word/document.xml": docxtest.Document(docxWordNamespace, testDOCXOfficeNS,
		revised+docxtest.Paragraph("paragraph after the tracked edit.")+docxtest.Paragraph("Another ordinary paragraph.")+docxtest.Paragraph("Final ordinary paragraph."))}
	got, err := extractStoryFixture(t, "body", parts)
	require.NoError(t, err)
	require.Equal(t, "Compound paragraph after the tracked edit.\nAnother ordinary paragraph.\nFinal ordinary paragraph.", got)
}

func TestDOCXMovedParagraphMarkJoinsLikeAcceptedRevision(t *testing.T) {
	const moveSource = `<w:moveFromRangeStart w:id="10" w:name="m" w:author="Writer" w:date="2026-01-01T00:00:00Z"/>` +
		`<w:p><w:pPr><w:rPr><w:moveFrom w:id="11" w:author="Writer"/></w:rPr></w:pPr><w:r><w:t xml:space="preserve">Retained </w:t></w:r></w:p>` +
		`<w:moveFromRangeEnd w:id="10"/>`
	const moveDestination = `<w:moveToRangeStart w:id="12" w:name="m" w:author="Writer" w:date="2026-01-01T00:00:00Z"/>` +
		`<w:p><w:pPr><w:rPr><w:moveTo w:id="13" w:author="Writer"/></w:rPr></w:pPr><w:r><w:t>Separate destination</w:t></w:r></w:p>` +
		`<w:moveToRangeEnd w:id="12"/>`
	last := docxtest.Paragraph("Last paragraph.")
	for _, tt := range []struct{ name, body string }{
		{"accepted text", docxtest.Paragraph("Retained suffix.") + docxtest.Paragraph("Separate destination") + last},
		{"tracked move", moveSource + docxtest.Paragraph("suffix.") + moveDestination + last},
	} {
		for _, namespaces := range []struct{ name, word, office string }{
			{"transitional", docxWordNamespace, testDOCXOfficeNS},
			{"strict", docxStrictNamespace, testDOCXStrictNS},
		} {
			t.Run(tt.name+"/"+namespaces.name, func(t *testing.T) {
				parts := map[string]string{"word/document.xml": docxtest.Document(namespaces.word, namespaces.office, tt.body)}
				got, err := extractStoryFixture(t, "body", parts)
				require.NoError(t, err)
				require.Equal(t, "Retained suffix.\nSeparate destination\nLast paragraph.", got)
			})
		}
	}
}

func TestDOCXDeletedParagraphMarkFollowsLogicalParagraphs(t *testing.T) {
	const revised = `<w:p><w:pPr><w:rPr><w:del w:id="1"/></w:rPr></w:pPr><w:r><w:t xml:space="preserve">First </w:t></w:r></w:p>`
	const controlStart = `<w:sdt><w:sdtPr/><w:sdtContent>`
	const controlEnd = `</w:sdtContent></w:sdt>`
	second := docxtest.Paragraph("second text.")
	third := docxtest.Paragraph("Third paragraph.")
	for _, tt := range []struct{ name, markup, want string }{
		{"no wrapper", revised + second + third, "First second text.\nThird paragraph."},
		{"join into content control", revised + controlStart + second + controlEnd + third, "First second text.\nThird paragraph."},
		{"join out of content control", controlStart + revised + controlEnd + second + third, "First second text.\nThird paragraph."},
		{"table interrupts body flow", revised + `<w:tbl><w:tr><w:tc>` + second + `</w:tc></w:tr></w:tbl>` + third, "First\nsecond text.\nThird paragraph."},
		{"cell boundary", `<w:tbl><w:tr><w:tc>` + revised + `</w:tc><w:tc>` + second + `</w:tc></w:tr></w:tbl>` + third, "First\nsecond text.\nThird paragraph."},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parts := map[string]string{"word/document.xml": docxtest.Document(docxWordNamespace, testDOCXOfficeNS, tt.markup)}
			got, err := extractStoryFixture(t, "body", parts)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
