package extractor

import (
	"bytes"
	"encoding/xml"
	"errors"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/wordprocessingml"
)

type docxMarkupState uint8

const (
	docxMarkupVisible docxMarkupState = iota
	docxMarkupAlternate
	docxMarkupTransparent
	docxMarkupIgnored
)

type docxMarkupFrame struct {
	state                      docxMarkupState
	choice, fallback, selected bool
	ignorable                  []string
	processContent             []xml.Name
}

// docxMarkup projects selected branches and explicit ProcessContent children
// onto their logical parent. The XML reader still validates every physical token.
type docxMarkup struct {
	visit          func(xml.Token, int) error
	frames         [maxDOCXXMLDepth + 1]docxMarkupFrame
	ignorable      map[string]bool
	processContent map[xml.Name]bool
	logicalDepth   int
}

func (m *docxMarkup) token(token xml.Token, depth int, namespaces map[string]string) error {
	frame := &m.frames[depth]
	switch element := token.(type) {
	case xml.StartElement:
		return m.start(element, frame, &m.frames[depth-1], namespaces)
	case xml.EndElement:
		defer m.restore(frame)
		if frame.state == docxMarkupAlternate && !frame.choice {
			return errors.New("DOCX AlternateContent has no choice")
		}
		if frame.state == docxMarkupVisible {
			err := m.visit(token, m.logicalDepth)
			m.logicalDepth--
			return err
		}
	case xml.CharData:
		if frame.state == docxMarkupAlternate && len(bytes.Trim(element, " \t\r\n")) > 0 {
			return errors.New("unexpected text in DOCX AlternateContent")
		}
		if frame.state == docxMarkupVisible || frame.state == docxMarkupTransparent {
			return m.visit(token, m.logicalDepth)
		}
	}
	return nil
}

func (m *docxMarkup) start(element xml.StartElement, frame, parent *docxMarkupFrame, namespaces map[string]string) error {
	*frame = docxMarkupFrame{}
	if parent.state == docxMarkupIgnored {
		frame.state = docxMarkupIgnored
		return nil
	}
	if err := m.applyDirectives(element, frame, namespaces); err != nil {
		return err
	}
	namespace := element.Name.Space
	if m.ignorable[namespace] && !understandsDOCXMarkup(namespace) {
		frame.state = docxMarkupIgnored
		if parent.state != docxMarkupAlternate &&
			(m.processContent[element.Name] || m.processContent[xml.Name{Space: namespace, Local: "*"}]) {
			frame.state = docxMarkupTransparent
			return requireUnderstoodDOCXNamespaces(element, namespaces)
		}
		return nil
	}
	if parent.state == docxMarkupAlternate {
		return m.selectBranch(element, frame, parent, namespaces)
	}
	if err := requireUnderstoodDOCXNamespaces(element, namespaces); err != nil {
		return err
	}
	if element.Name.Space == docxCompatibilityNamespace {
		switch element.Name.Local {
		case "AlternateContent":
			frame.state = docxMarkupAlternate
			return nil
		case "Choice", "Fallback":
			return errors.New("DOCX markup choice outside AlternateContent")
		}
	}
	m.logicalDepth++
	return m.visit(element, m.logicalDepth)
}

func (m *docxMarkup) applyDirectives(element xml.StartElement, frame *docxMarkupFrame, namespaces map[string]string) error {
	compatibility := func(namespace string) bool { return namespace == docxCompatibilityNamespace }
	for _, prefix := range strings.Fields(docxAttr(element, "Ignorable", compatibility)) {
		namespace := namespaces[prefix]
		if namespace == "" || namespace == docxCompatibilityNamespace {
			return errors.New("invalid DOCX Ignorable namespace")
		}
		if !m.ignorable[namespace] {
			if m.ignorable == nil {
				m.ignorable = make(map[string]bool)
			}
			m.ignorable[namespace] = true
			frame.ignorable = append(frame.ignorable, namespace)
		}
	}
	for _, entry := range strings.Fields(docxAttr(element, "ProcessContent", compatibility)) {
		prefix, local, ok := strings.Cut(entry, ":")
		namespace := namespaces[prefix]
		if !ok || prefix == "" || !m.ignorable[namespace] || (local != "*" && !validDOCXXMLName(local)) {
			return errors.New("invalid DOCX ProcessContent name")
		}
		name := xml.Name{Space: namespace, Local: local}
		if !m.processContent[name] {
			if m.processContent == nil {
				m.processContent = make(map[xml.Name]bool)
			}
			m.processContent[name] = true
			frame.processContent = append(frame.processContent, name)
		}
	}
	return nil
}

func (m *docxMarkup) selectBranch(element xml.StartElement, frame, parent *docxMarkupFrame, namespaces map[string]string) error {
	if element.Name.Space != docxCompatibilityNamespace {
		return errors.New("unexpected DOCX AlternateContent child")
	}
	frame.state = docxMarkupIgnored
	switch element.Name.Local {
	case "Choice":
		if parent.fallback {
			return errors.New("DOCX markup choice follows fallback")
		}
		parent.choice = true
		requires := strings.Fields(docxAttr(element, "Requires", func(space string) bool { return space == "" }))
		if len(requires) == 0 {
			return errors.New("DOCX markup choice is missing Requires")
		}
		supported := true
		for _, prefix := range requires {
			namespace := namespaces[prefix]
			if namespace == "" {
				return errors.New("DOCX markup choice requires an unbound namespace")
			}
			supported = supported && understandsDOCXMarkup(namespace)
		}
		if !supported {
			return nil
		}
	case "Fallback":
		if !parent.choice || parent.fallback {
			return errors.New("misplaced DOCX markup fallback")
		}
		parent.fallback = true
	default:
		return errors.New("unexpected DOCX AlternateContent child")
	}
	if !parent.selected {
		parent.selected, frame.state = true, docxMarkupTransparent
		return requireUnderstoodDOCXNamespaces(element, namespaces)
	}
	return nil
}

func (m *docxMarkup) restore(frame *docxMarkupFrame) {
	for _, name := range frame.processContent {
		delete(m.processContent, name)
	}
	for _, namespace := range frame.ignorable {
		delete(m.ignorable, namespace)
	}
	*frame = docxMarkupFrame{}
}

// requireUnderstoodDOCXNamespaces implements the MustUnderstand examination
// of ECMA-376 Part 3 for an element that remains in the output: every declared
// namespace must be bound, distinct from the compatibility namespace and
// understood by this reader. Elements that are discarded, and choices that are
// not selected, are never examined.
func requireUnderstoodDOCXNamespaces(element xml.StartElement, namespaces map[string]string) error {
	compatibility := func(namespace string) bool { return namespace == docxCompatibilityNamespace }
	for _, prefix := range strings.Fields(docxAttr(element, "MustUnderstand", compatibility)) {
		namespace := namespaces[prefix]
		if namespace == "" || namespace == docxCompatibilityNamespace {
			return errors.New("invalid DOCX MustUnderstand namespace")
		}
		if !understandsDOCXMarkup(namespace) {
			return errors.New("DOCX MustUnderstand declares an unsupported namespace")
		}
	}
	return nil
}

func understandsDOCXMarkup(namespace string) bool {
	return isDOCXWordNamespace(namespace) || isDOCXMathNamespace(namespace) ||
		isDOCXOfficeNamespace(namespace) || namespace == wordprocessingml.WordShape
}
