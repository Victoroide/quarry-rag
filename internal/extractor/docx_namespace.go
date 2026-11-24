package extractor

import (
	"encoding/xml"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/wordprocessingml"
)

type docxNamespaceBinding struct {
	prefix, previous string
	hadPrevious      bool
}

type docxXMLFrame struct {
	raw, expanded xml.Name
	bindingStart  int
}

type docxXMLScope struct {
	namespaces map[string]string
	bindings   []docxNamespaceBinding
}

// start expands qualified names before the markup reader can hide any tokens.
// Namespace declarations are local to the element and never become story attributes.
func (scope *docxXMLScope) start(element xml.StartElement) (xml.StartElement, error) {
	var seen map[xml.Name]bool
	if len(element.Attr) > 1 {
		seen = make(map[xml.Name]bool, len(element.Attr))
	}
	for _, attr := range element.Attr {
		if attr.Name.Space != "xmlns" && attr.Name != (xml.Name{Local: "xmlns"}) {
			continue
		}
		decl := xml.Name{Space: wordprocessingml.XMLNS, Local: attr.Name.Local}
		if seen[decl] {
			return xml.StartElement{}, fmt.Errorf("duplicate XML attribute %q", attr.Name.Local)
		}
		if seen != nil {
			seen[decl] = true
		}
		prefix := ""
		if attr.Name.Space == "xmlns" {
			prefix = attr.Name.Local
		}
		if err := scope.bind(prefix, attr.Value); err != nil {
			return xml.StartElement{}, err
		}
	}
	name, err := scope.expand(element.Name, true)
	if err != nil {
		return xml.StartElement{}, err
	}
	element.Name = name
	attrs := element.Attr[:0]
	for _, attr := range element.Attr {
		if attr.Name.Space == "xmlns" || attr.Name == (xml.Name{Local: "xmlns"}) {
			continue
		}
		name, err := scope.expand(attr.Name, false)
		if err != nil {
			return xml.StartElement{}, err
		}
		if seen[name] {
			return xml.StartElement{}, fmt.Errorf("duplicate XML attribute %q", name.Local)
		}
		if seen != nil {
			seen[name] = true
		}
		attr.Name = name
		attrs = append(attrs, attr)
	}
	element.Attr = attrs
	return element, nil
}

func (scope *docxXMLScope) bind(prefix, namespace string) error {
	if prefix != "" && (!validDOCXXMLName(prefix) || namespace == "") ||
		prefix == "xmlns" || namespace == wordprocessingml.XMLNS ||
		(prefix == "xml") != (namespace == wordprocessingml.XML) {
		return fmt.Errorf("invalid XML namespace binding for prefix %q", prefix)
	}
	previous, exists := scope.namespaces[prefix]
	scope.bindings = append(scope.bindings, docxNamespaceBinding{prefix, previous, exists})
	scope.namespaces[prefix] = namespace
	return nil
}

func (scope *docxXMLScope) expand(raw xml.Name, element bool) (xml.Name, error) {
	if !validDOCXXMLName(raw.Local) || raw.Space == "xmlns" {
		return xml.Name{}, fmt.Errorf("invalid XML qualified name %v", raw)
	}
	if raw.Space == "" && !element {
		return raw, nil
	}
	namespace := scope.namespaces[raw.Space]
	if raw.Space != "" && namespace == "" {
		return xml.Name{}, fmt.Errorf("unbound XML namespace prefix %q", raw.Space)
	}
	return xml.Name{Space: namespace, Local: raw.Local}, nil
}

func (scope *docxXMLScope) restore(start int) {
	for i := len(scope.bindings) - 1; i >= start; i-- {
		binding := scope.bindings[i]
		if binding.hadPrevious {
			scope.namespaces[binding.prefix] = binding.previous
		} else {
			delete(scope.namespaces, binding.prefix)
		}
	}
	clear(scope.bindings[start:])
	scope.bindings = scope.bindings[:start]
}

// XML 1.0 NameStartChar and NameChar, excluding the namespace separator.
func validDOCXXMLName(name string) bool {
	for i, char := range name {
		switch {
		case char == '_', 'A' <= char && char <= 'Z', 'a' <= char && char <= 'z':
		case 0xc0 <= char && char <= 0xd6, 0xd8 <= char && char <= 0xf6, 0xf8 <= char && char <= 0x2ff,
			0x370 <= char && char <= 0x37d, 0x37f <= char && char <= 0x1fff, 0x200c <= char && char <= 0x200d,
			0x2070 <= char && char <= 0x218f, 0x2c00 <= char && char <= 0x2fef, 0x3001 <= char && char <= 0xd7ff,
			0xf900 <= char && char <= 0xfdcf, 0xfdf0 <= char && char <= 0xfffd, 0x10000 <= char && char <= 0xeffff:
		case i > 0 && (char == '-' || char == '.' || '0' <= char && char <= '9' ||
			char == 0xb7 || 0x300 <= char && char <= 0x36f || 0x203f <= char && char <= 0x2040):
		default:
			return false
		}
	}
	return name != ""
}
