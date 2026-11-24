package extractor

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/wordprocessingml"
)

var (
	docxWordNamespace          = wordprocessingml.Word
	docxStrictNamespace        = wordprocessingml.StrictWord
	docxCompatibilityNamespace = wordprocessingml.Compatibility
)

const maxDOCXXMLDepth = 256

func readDOCXXML(ctx context.Context, r io.Reader, root string, namespaceOK func(string) bool, visit func(xml.Token, int, map[string]string) error) error {
	source, encoding := docxXMLSource(ctx, r)
	decoder := xml.NewDecoder(source)
	decoder.CharsetReader = func(label string, input io.Reader) (io.Reader, error) {
		if !encoding.matches(label) {
			return nil, errors.New("XML encoding declaration disagrees with part encoding")
		}
		return input, nil
	}
	scope := docxXMLScope{namespaces: map[string]string{"xml": wordprocessingml.XML}}
	frames := make([]docxXMLFrame, 0, maxDOCXXMLDepth)
	seenRoot, first := false, true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		token, err := decoder.RawToken()
		if errors.Is(err, io.EOF) {
			if !seenRoot || len(frames) != 0 {
				return fmt.Errorf("incomplete %s XML", root)
			}
			return ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("parse XML: %w", err)
		}
		switch element := token.(type) {
		case xml.StartElement:
			if len(frames) == maxDOCXXMLDepth {
				return fmt.Errorf("XML exceeds maximum depth of %d", maxDOCXXMLDepth)
			}
			rawName, bindingStart := element.Name, len(scope.bindings)
			element, err = scope.start(element)
			if err != nil {
				return err
			}
			if len(frames) == 0 {
				if seenRoot || element.Name.Local != root || !namespaceOK(element.Name.Space) {
					return fmt.Errorf("expected a single %s XML root", root)
				}
				seenRoot = true
			}
			frames = append(frames, docxXMLFrame{raw: rawName, expanded: element.Name, bindingStart: bindingStart})
			token = element
		case xml.EndElement:
			if len(frames) == 0 {
				return fmt.Errorf("unexpected XML end element %v", element.Name)
			}
			last := len(frames) - 1
			frame := frames[last]
			if element.Name != frame.raw {
				return fmt.Errorf("unexpected XML end element %v", element.Name)
			}
			if err := visit(xml.EndElement{Name: frame.expanded}, len(frames), scope.namespaces); err != nil {
				return err
			}
			scope.restore(frame.bindingStart)
			frames[last] = docxXMLFrame{}
			frames = frames[:last]
			continue
		case xml.CharData:
			if len(frames) == 0 && len(bytes.Trim(element, " \t\r\n")) > 0 {
				return fmt.Errorf("unexpected text outside %s XML root", root)
			}
		case xml.Directive:
			return errors.New("XML directives are unsupported")
		case xml.ProcInst:
			if strings.EqualFold(element.Target, "xml") {
				if !first || element.Target != "xml" {
					return errors.New("unexpected XML declaration")
				}
				if err := validateDOCXXMLDeclaration(element.Inst, encoding); err != nil {
					return err
				}
			}
		}
		first = false
		if err := visit(token, len(frames), scope.namespaces); err != nil {
			return err
		}
	}
}

func isDOCXWordNamespace(namespace string) bool {
	return namespace == docxWordNamespace || namespace == docxStrictNamespace
}

func docxAttr(element xml.StartElement, local string, namespaceOK func(string) bool) string {
	for _, attr := range element.Attr {
		if attr.Name.Local == local && namespaceOK(attr.Name.Space) {
			return attr.Value
		}
	}
	return ""
}

type docxText struct {
	paragraphs    []string
	textBytes     int
	readableBytes int
}

func (text *docxText) addBytes(n int) error {
	text.textBytes += n
	return checkTextLimit(text.textBytes)
}

func (text *docxText) addParagraph(value, label string) error {
	overhead := 0
	if len(text.paragraphs) > 0 {
		overhead++
		text.readableBytes++
		if label != "" {
			label = "\n" + label
		}
	}
	if label != "" {
		overhead += len(label) + 1
	}
	if err := text.addBytes(overhead); err != nil {
		return err
	}
	if label != "" {
		text.paragraphs = append(text.paragraphs, label)
	}
	text.readableBytes += len(value)
	text.paragraphs = append(text.paragraphs, value)
	return nil
}

type docxStoryReferences struct {
	stories          DOCXStories
	headers, footers []string
	sectionIDs       map[string]bool
	comments         map[string]bool
	notes            map[docxNote]docxNoteReference
}

type docxNote struct {
	kind string
	id   string
}

type docxNoteReference uint8

const (
	docxLiveNote docxNoteReference = 1 << iota
	docxDeletedNote
)

func (refs *docxStoryReferences) collectNote(element xml.StartElement, parent xml.Name, reference docxNoteReference) {
	kind := element.Name.Local
	if (kind != "footnoteReference" && kind != "endnoteReference") || !isDOCXWordNamespace(element.Name.Space) || !isDOCXRun(parent) {
		return
	}
	id := docxStoryID(docxAttr(element, "id", isDOCXWordNamespace))
	if id == "" {
		return
	}
	if refs.notes == nil {
		refs.notes = make(map[docxNote]docxNoteReference)
	}
	note := docxNote{strings.TrimSuffix(kind, "Reference"), id}
	refs.notes[note] |= reference
}

func (refs *docxStoryReferences) collect(element xml.StartElement, main bool, parent, grandparent xml.Name) error {
	switch element.Name.Local {
	case "headerReference", "footerReference":
		section := docxElementIs(parent, "sectPr") && (docxElementIs(grandparent, "body") || docxElementIs(grandparent, "pPr"))
		if !main || !section {
			return nil
		}
		ids, story := &refs.headers, docxHeaders
		if element.Name.Local == "footerReference" {
			ids, story = &refs.footers, docxFooters
		}
		if refs.stories&(story|docxComments) == 0 {
			return nil
		}
		id := docxAttr(element, "id", isDOCXOfficeNamespace)
		key := element.Name.Local + "/" + id
		if refs.sectionIDs == nil {
			refs.sectionIDs = make(map[string]bool)
		}
		if !refs.sectionIDs[key] {
			refs.sectionIDs[key] = true
			*ids = append(*ids, id)
		}
	case "commentReference":
		if refs.stories&docxComments == 0 || !isDOCXRun(parent) {
			return nil
		}
		id := docxStoryID(docxAttr(element, "id", isDOCXWordNamespace))
		if id == "" {
			return errors.New("invalid DOCX comment reference ID")
		}
		if refs.comments == nil {
			refs.comments = make(map[string]bool)
		}
		refs.comments[id] = true
	case "footnoteReference", "endnoteReference":
		if main {
			refs.collectNote(element, parent, docxLiveNote)
		}
	}
	return nil
}

// docxStoryID canonicalizes the XML Schema integer lexical form. An empty
// result denotes an invalid ID. The expanded XML limit bounds the input.
func docxStoryID(value string) string {
	value = strings.Trim(value, " \t\r\n")
	if value == "" {
		return ""
	}
	digits := value
	negative := value[0] == '-'
	if negative || value[0] == '+' {
		digits = value[1:]
	}
	if digits == "" {
		return ""
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return ""
		}
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return "0"
	}
	if negative {
		return "-" + digits
	}
	return digits
}

func docxElementIs(name xml.Name, local string) bool {
	return name.Local == local && isDOCXWordNamespace(name.Space)
}

func isDOCXMathNamespace(namespace string) bool {
	return namespace == wordprocessingml.Math || namespace == wordprocessingml.StrictMath
}

func isDOCXRun(name xml.Name) bool {
	return name.Local == "r" && (isDOCXWordNamespace(name.Space) || isDOCXMathNamespace(name.Space))
}

type docxParagraph struct {
	text     *strings.Builder
	joinNext bool
}

type docxStoryParser struct {
	root, label string
	refs        *docxStoryReferences
	text        *docxText
	capture     bool
	noteIsLive  bool

	elements       [maxDOCXXMLDepth]xml.Name
	flowDepths     [maxDOCXXMLDepth]int
	skipDepth      int
	skipDeleted    bool
	skipProperties bool
	textDepth      int
	paragraphs     []docxParagraph
	joins          map[int]*strings.Builder
}

func (p *docxPackage) readStory(ctx context.Context, name, root, label string, refs *docxStoryReferences, text *docxText, capture bool) error {
	part, err := p.part(name)
	if err != nil {
		return err
	}
	if part == nil {
		return fmt.Errorf("missing DOCX part %q", name)
	}
	parser := docxStoryParser{root: root, label: label, refs: refs, text: text, capture: capture}
	markup := docxMarkup{visit: parser.visit}
	return readDOCXPart(part, &p.work, maxDocumentXMLBytes, func(r io.Reader) error {
		return readDOCXXML(ctx, r, root, func(namespace string) bool {
			return isDOCXWordNamespace(namespace) || (root == "document" && namespace == "")
		}, markup.token)
	})
}

func (p *docxStoryParser) visit(token xml.Token, depth int) error {
	if p.skipDepth > 0 {
		switch element := token.(type) {
		case xml.StartElement:
			if p.skipDeleted && p.root == "document" {
				p.refs.collectNote(element, p.elements[depth-2], docxDeletedNote)
				p.elements[depth-1] = element.Name
			}
			if p.skipProperties && depth == p.skipDepth+1 && (docxElementIs(element.Name, "del") || docxElementIs(element.Name, "moveFrom")) {
				p.paragraphs[len(p.paragraphs)-1].joinNext = true
			}
		case xml.EndElement:
			if depth == p.skipDepth {
				p.skipDepth, p.skipDeleted, p.skipProperties = 0, false, false
			}
		}
		return nil
	}
	switch element := token.(type) {
	case xml.StartElement:
		return p.start(element, depth)
	case xml.EndElement:
		if depth == p.textDepth {
			p.textDepth = 0
		}
		if p.capture {
			if err := p.flushJoin(depth); err != nil {
				return err
			}
			if docxElementIs(element.Name, "p") {
				return p.finishParagraph(p.flowDepths[depth-1])
			}
		}
	case xml.CharData:
		if p.textDepth != 0 && depth == p.textDepth {
			if err := p.text.addBytes(len(element)); err != nil {
				return err
			}
			_, _ = p.paragraphs[len(p.paragraphs)-1].text.Write(element)
		}
	}
	return nil
}

func (p *docxStoryParser) finishParagraph(flowDepth int) error {
	index := len(p.paragraphs) - 1
	paragraph := p.paragraphs[index]
	p.paragraphs = p.paragraphs[:index]
	if joined := p.joins[flowDepth]; joined != nil {
		joined.WriteString(paragraph.text.String())
		paragraph.text = joined
	}
	if paragraph.joinNext {
		if p.joins == nil {
			p.joins = make(map[int]*strings.Builder)
		}
		p.joins[flowDepth] = paragraph.text
		return nil
	}
	delete(p.joins, flowDepth)
	return p.emitParagraph(paragraph.text)
}

func (p *docxStoryParser) flushJoin(flowDepth int) error {
	joined := p.joins[flowDepth]
	if joined == nil {
		return nil
	}
	delete(p.joins, flowDepth)
	return p.emitParagraph(joined)
}

func (p *docxStoryParser) emitParagraph(paragraph *strings.Builder) error {
	value := strings.TrimSpace(paragraph.String())
	if value == "" {
		return nil
	}
	err := p.text.addParagraph(value, p.label)
	p.label = ""
	return err
}

func (p *docxStoryParser) start(element xml.StartElement, depth int) error {
	p.elements[depth-1] = element.Name
	if depth == 1 {
		p.flowDepths[0] = 1
		// An unqualified legacy document root has no recognized Word text.
		if element.Name.Space == "" {
			p.skipDepth = depth
		}
		return nil
	}
	if depth == 2 {
		active, err := p.storyChild(element)
		if err != nil {
			return err
		}
		if !active {
			p.skipDepth = depth
			return nil
		}
	}
	if p.capture {
		flowDepth := p.flowDepths[depth-2]
		if isDOCXWordNamespace(element.Name.Space) {
			// Keep pending joins across block wrappers within the same flow.
			// Tables, cells, text boxes, and individual notes and comments
			// have independent paragraph sequences.
			switch element.Name.Local {
			case "tbl", "tc", "txbxContent", "footnote", "endnote", "comment":
				if err := p.flushJoin(flowDepth); err != nil {
					return err
				}
				flowDepth = depth
			}
		}
		p.flowDepths[depth-1] = flowDepth
	}
	if !isDOCXWordNamespace(element.Name.Space) {
		return nil
	}
	parent := p.elements[depth-2]
	var grandparent xml.Name
	if depth > 2 {
		grandparent = p.elements[depth-3]
	}
	switch element.Name.Local {
	case "del", "moveFrom", "cellDel":
		p.skipDepth, p.skipDeleted = depth, true
		if docxElementIs(parent, "trPr") && docxElementIs(grandparent, "tr") && element.Name.Local == "del" ||
			docxElementIs(parent, "tcPr") && docxElementIs(grandparent, "tc") && element.Name.Local == "cellDel" {
			p.skipDepth = depth - 2
		}
		return nil
	case "rPr":
		p.skipDepth = depth
		p.skipProperties = p.capture && len(p.paragraphs) > 0 && docxElementIs(parent, "pPr") && docxElementIs(grandparent, "p")
		return nil
	case "pPrChange", "sectPrChange", "trPrChange", "tcPrChange", "sdtPr", "sdtEndPr":
		p.skipDepth = depth
		return nil
	case "comment", "footnote", "endnote":
		if depth != 2 || p.root != element.Name.Local+"s" {
			p.skipDepth = depth
		}
		return nil
	}
	if p.root != "comments" {
		note := p.root == "footnotes" || p.root == "endnotes"
		if element.Name.Local != "commentReference" || !note || p.noteIsLive {
			if err := p.refs.collect(element, p.root == "document", parent, grandparent); err != nil {
				return err
			}
		}
	}
	if !p.capture {
		return nil
	}
	switch element.Name.Local {
	case "p":
		p.paragraphs = append(p.paragraphs, docxParagraph{text: &strings.Builder{}})
	case "t":
		if len(p.paragraphs) > 0 && isDOCXRun(parent) {
			p.textDepth = depth
		}
	case "tab", "br", "cr", "noBreakHyphen":
		if len(p.paragraphs) > 0 && isDOCXRun(parent) {
			if err := p.text.addBytes(1); err != nil {
				return err
			}
			separator := byte('\n')
			switch element.Name.Local {
			case "tab":
				separator = '\t'
			case "noBreakHyphen":
				separator = '-'
			}
			_ = p.paragraphs[len(p.paragraphs)-1].text.WriteByte(separator)
		}
	}
	return nil
}

func (p *docxStoryParser) storyChild(element xml.StartElement) (bool, error) {
	switch p.root {
	case "document":
		return docxElementIs(element.Name, "body"), nil
	case "footnotes", "endnotes":
		kind := strings.TrimSuffix(p.root, "s")
		p.noteIsLive = false
		noteType := docxAttr(element, "type", isDOCXWordNamespace)
		if !docxElementIs(element.Name, kind) || noteType == "separator" || noteType == "continuationSeparator" {
			return false, nil
		}
		idValue := docxAttr(element, "id", isDOCXWordNamespace)
		if idValue == "" {
			return true, nil
		}
		id := docxStoryID(idValue)
		if id == "" {
			return false, fmt.Errorf("invalid DOCX %s ID", kind)
		}
		reference := p.refs.notes[docxNote{kind, id}]
		p.noteIsLive = reference&docxLiveNote != 0
		return reference != docxDeletedNote, nil
	case "comments":
		if !docxElementIs(element.Name, "comment") {
			return false, nil
		}
		id := docxStoryID(docxAttr(element, "id", isDOCXWordNamespace))
		if id == "" {
			return false, errors.New("invalid DOCX comment ID")
		}
		if !p.refs.comments[id] {
			return false, nil
		}
		delete(p.refs.comments, id)
		p.label = "[DOCX comment]"
	}
	return true, nil
}
