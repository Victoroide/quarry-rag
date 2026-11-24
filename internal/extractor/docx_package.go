package extractor

import (
	"archive/zip"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"unicode"

	"github.com/Victoroide/quarry-rag/internal/wordprocessingml"
)

const (
	docxDefaultMainPart       = "word/document.xml"
	docxPackageRelationships  = "_rels/.rels"
	maxDOCXRelationshipsBytes = 1 << 20
	maxDOCXXMLWorkBytes       = 3*maxDocumentXMLBytes + 2*maxDOCXRelationshipsBytes
)

var docxPackageNamespace = wordprocessingml.PackageRelationships

type docxPackage struct {
	parts map[string]*zip.File
	work  uint64
}

func newDOCXPackage(zr *zip.Reader) *docxPackage {
	pkg := &docxPackage{parts: make(map[string]*zip.File, len(zr.File))}
	for _, part := range zr.File {
		name := strings.ToLower(part.Name)
		if _, exists := pkg.parts[name]; exists {
			pkg.parts[name] = nil
		} else {
			pkg.parts[name] = part
		}
	}
	return pkg
}

func docxPart(zr *zip.Reader, name string) *zip.File {
	for _, part := range zr.File {
		if strings.EqualFold(part.Name, name) {
			return part
		}
	}
	return nil
}

func (p *docxPackage) part(name string) (*zip.File, error) {
	part, exists := p.parts[name]
	if exists && part == nil {
		return nil, fmt.Errorf("duplicate package part %q", name)
	}
	return part, nil
}

func (p *docxPackage) relatedMainPart(ctx context.Context) (string, error) {
	part, err := p.part(docxPackageRelationships)
	if err != nil {
		return "", err
	}
	return docxRelatedMainPart(ctx, part, &p.work)
}

func docxRelatedMainPart(ctx context.Context, part *zip.File, work *uint64) (string, error) {
	rels, err := readDOCXRelationships(ctx, part, work)
	if err != nil {
		return "", err
	}
	return rels.implicit("", "officeDocument")
}

func docxRelationshipsName(part string) string {
	dir, file := path.Split(part)
	return dir + "_rels/" + file + ".rels"
}

func (p *docxPackage) relationships(ctx context.Context, name string) (docxRelationships, error) {
	part, err := p.part(name)
	if err != nil {
		return docxRelationships{}, err
	}
	return readDOCXRelationships(ctx, part, &p.work)
}

type docxRelationship struct {
	kind, id, target, mode string
}

type docxRelationships struct {
	byKind map[string][]docxRelationship
	byID   map[string][]docxRelationship
}

func isDOCXOfficeNamespace(namespace string) bool {
	return namespace == wordprocessingml.OfficeRelationships || namespace == wordprocessingml.StrictOfficeRelationships
}

func readDOCXRelationships(ctx context.Context, part *zip.File, work *uint64) (docxRelationships, error) {
	if part == nil {
		return docxRelationships{}, nil
	}
	rels := docxRelationships{
		byKind: make(map[string][]docxRelationship),
		byID:   make(map[string][]docxRelationship),
	}
	err := readDOCXPart(part, work, maxDOCXRelationshipsBytes, func(r io.Reader) error {
		return readDOCXXML(ctx, r, "Relationships", func(namespace string) bool { return namespace == docxPackageNamespace }, func(token xml.Token, depth int, _ map[string]string) error {
			element, ok := token.(xml.StartElement)
			if !ok || depth != 2 || element.Name != (xml.Name{Space: docxPackageNamespace, Local: "Relationship"}) {
				return nil
			}
			var rel docxRelationship
			for _, attr := range element.Attr {
				if attr.Name.Space != "" {
					continue
				}
				switch attr.Name.Local {
				case "Id":
					rel.id = attr.Value
				case "Type":
					namespace, kind := path.Split(attr.Value)
					if isDOCXOfficeNamespace(strings.TrimSuffix(namespace, "/")) {
						rel.kind = kind
					}
				case "Target":
					rel.target = attr.Value
				case "TargetMode":
					rel.mode = attr.Value
				}
			}
			rels.byKind[rel.kind] = append(rels.byKind[rel.kind], rel)
			rels.byID[rel.id] = append(rels.byID[rel.id], rel)
			return nil
		})
	})
	return rels, err
}

func (rels docxRelationships) implicit(source, kind string) (string, error) {
	links := rels.byKind[kind]
	if len(links) == 0 {
		return "", nil
	}
	var selected string
	seen := make(map[string]bool, len(links))
	for _, link := range links {
		if seen[link.id] {
			continue
		}
		seen[link.id] = true
		target, err := rels.explicit(source, kind, link.id)
		if err != nil {
			return "", err
		}
		if selected != "" && selected != target {
			return "", fmt.Errorf("conflicting DOCX %s relationships", kind)
		}
		selected = target
	}
	return selected, nil
}

func (rels docxRelationships) explicit(source, kind, id string) (string, error) {
	links := rels.byID[id]
	if len(links) == 0 {
		return "", fmt.Errorf("missing DOCX %s relationship %q", kind, id)
	}
	return resolveDOCXRelationship(source, kind, links)
}

func resolveDOCXRelationship(source, kind string, links []docxRelationship) (string, error) {
	var selected string
	for _, link := range links {
		if link.kind != kind || link.id == "" {
			return "", fmt.Errorf("invalid DOCX %s relationship", kind)
		}
		if link.mode != "" && link.mode != "Internal" {
			return "", fmt.Errorf("DOCX %s relationship is not internal", kind)
		}
		target, err := resolveDOCXTarget(source, link.target)
		if err != nil {
			return "", fmt.Errorf("DOCX %s relationship: %w", kind, err)
		}
		if target == docxPackageRelationships || target == docxRelationshipsName(source) {
			return "", fmt.Errorf("cyclic DOCX %s relationship", kind)
		}
		if selected != "" && selected != target {
			return "", fmt.Errorf("conflicting DOCX %s relationships", kind)
		}
		selected = target
	}
	return selected, nil
}

func resolveDOCXTarget(source, target string) (string, error) {
	invalid := fmt.Errorf("invalid package target %q", target)
	if target == "" || strings.HasPrefix(target, "//") || strings.ContainsAny(target, "?#\\") || strings.IndexFunc(target, unicode.IsSpace) >= 0 {
		return "", invalid
	}
	u, err := url.Parse(target)
	if err != nil || u.IsAbs() || u.Host != "" {
		return "", invalid
	}
	name := u.EscapedPath()
	var segments []string
	if !strings.HasPrefix(name, "/") && source != "" {
		dir, _ := path.Split(source)
		if dir != "" {
			segments = strings.Split(strings.TrimSuffix(dir, "/"), "/")
		}
	}
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	for i, segment := range parts {
		switch segment {
		case "":
			return "", invalid
		case ".", "..":
			if i == len(parts)-1 {
				return "", invalid
			}
			if segment == ".." {
				if len(segments) == 0 {
					return "", invalid
				}
				segments = segments[:len(segments)-1]
			}
		default:
			decoded, err := url.PathUnescape(segment)
			if err != nil || decoded == "." || decoded == ".." || strings.ContainsAny(decoded, "/\\") || strings.IndexFunc(decoded, unicode.IsControl) >= 0 {
				return "", invalid
			}
			segments = append(segments, segment)
		}
	}
	return strings.ToLower(strings.Join(segments, "/")), nil
}

func readDOCXPart(part *zip.File, work *uint64, perPartLimit uint64, parse func(io.Reader) error) error {
	size := part.UncompressedSize64
	if size > perPartLimit {
		return fmt.Errorf("%s XML exceeds maximum size of %d bytes", part.Name, perPartLimit)
	}
	if size > maxDOCXXMLWorkBytes-*work {
		return fmt.Errorf("cumulative DOCX XML exceeds maximum size of %d bytes", maxDOCXXMLWorkBytes)
	}
	*work += size
	// The ZIP reader checks both the expanded size and the CRC through EOF.
	rc, err := part.Open()
	if err != nil {
		return fmt.Errorf("open %s: %w", part.Name, err)
	}
	defer func() { _ = rc.Close() }()
	if err := parse(rc); err != nil {
		return fmt.Errorf("%s: %w", part.Name, err)
	}
	return nil
}
