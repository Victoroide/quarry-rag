package extractor

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// DOCXStories selects the stories to extract from a Word document.
type DOCXStories uint8

const (
	docxBody DOCXStories = 1 << iota
	docxFootnotes
	docxEndnotes
	docxHeaders
	docxFooters
	docxComments
	defaultDOCXStories = docxBody | docxFootnotes | docxEndnotes
	allDOCXStories     = defaultDOCXStories | docxHeaders | docxFooters | docxComments
)

// ParseDOCXStories parses a comma-separated set. An empty value selects the defaults.
func ParseDOCXStories(value string) (DOCXStories, error) {
	if value == "" {
		return defaultDOCXStories, nil
	}
	var stories DOCXStories
	for name := range strings.SplitSeq(value, ",") {
		var story DOCXStories
		switch strings.TrimSpace(name) {
		case "body":
			story = docxBody
		case "footnotes":
			story = docxFootnotes
		case "endnotes":
			story = docxEndnotes
		case "headers":
			story = docxHeaders
		case "footers":
			story = docxFooters
		case "comments":
			story = docxComments
		default:
			return 0, fmt.Errorf("invalid DOCX story %q", name)
		}
		if stories&story != 0 {
			return 0, fmt.Errorf("duplicate DOCX story %q", strings.TrimSpace(name))
		}
		stories |= story
	}
	return stories, nil
}

type docxExtractor struct {
	stories DOCXStories
}

// NewDOCX creates an extractor with the selected stories. Zero selects the defaults.
func NewDOCX(stories DOCXStories) Extractor {
	if stories == 0 {
		stories = defaultDOCXStories
	}
	return &docxExtractor{stories: stories}
}

func (e *docxExtractor) Type() FileType { return FileTypeDOCX }

func (e *docxExtractor) Extract(ctx context.Context, r io.Reader) (string, error) {
	data, err := readSource(ctx, r)
	if err != nil {
		return "", fmt.Errorf("extractor: docx: read: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("extractor: docx: open zip: %w", err)
	}
	pkg := newDOCXPackage(zr)
	pkg.work = docxDetectionWork(ctx)
	text, err := e.extractPackage(ctx, pkg)
	if err != nil {
		return "", fmt.Errorf("extractor: docx: %w", err)
	}
	return text, nil
}

func (e *docxExtractor) extractPackage(ctx context.Context, pkg *docxPackage) (string, error) {
	main, err := pkg.relatedMainPart(ctx)
	if err != nil {
		return "", err
	}
	if main == "" {
		main = docxDefaultMainPart
	}

	refs := docxStoryReferences{stories: e.stories}
	var text docxText
	if err := pkg.readStory(ctx, main, "document", "", &refs, &text, e.stories&docxBody != 0); err != nil {
		return "", err
	}
	if e.stories&^docxBody != 0 {
		if err := e.readRelatedStories(ctx, pkg, main, &refs, &text); err != nil {
			return "", err
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if text.readableBytes < 10 {
		return "", ErrNoText
	}
	return strings.Join(text.paragraphs, "\n"), nil
}

func (e *docxExtractor) readRelatedStories(ctx context.Context, pkg *docxPackage, main string, refs *docxStoryReferences, text *docxText) error {
	relationships, relationshipErr := pkg.relationships(ctx, docxRelationshipsName(main))
	if relationshipErr != nil {
		return relationshipErr
	}
	seen := map[string]string{main: "document"}
	read := func(name, root, label string, capture bool) error {
		if name == "" {
			return nil
		}
		if previous, exists := seen[name]; exists {
			if previous == root {
				return nil
			}
			return fmt.Errorf("cyclic or conflicting DOCX story target %q", name)
		}
		seen[name] = root
		return pkg.readStory(ctx, name, root, label, refs, text, capture)
	}
	for _, notes := range []struct {
		story DOCXStories
		kind  string
	}{{docxFootnotes, "footnotes"}, {docxEndnotes, "endnotes"}} {
		if e.stories&(notes.story|docxComments) == 0 {
			continue
		}
		name, err := relationships.implicit(main, notes.kind)
		if err != nil {
			return err
		}
		// Older packages were accepted with conventional notes and no relationship table.
		if relationships.byKind == nil && main == docxDefaultMainPart {
			conventional := "word/" + notes.kind + ".xml"
			if _, exists := pkg.parts[conventional]; exists {
				name = conventional
			}
		}
		if err := read(name, notes.kind, "", e.stories&notes.story != 0); err != nil {
			return err
		}
	}
	for _, story := range []struct {
		kind, root, label string
		ids               []string
		flag              DOCXStories
	}{
		{"header", "hdr", "[DOCX header]", refs.headers, docxHeaders},
		{"footer", "ftr", "[DOCX footer]", refs.footers, docxFooters},
	} {
		for _, id := range story.ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			name, err := relationships.explicit(main, story.kind, id)
			if err != nil {
				return err
			}
			if err := read(name, story.root, story.label, e.stories&story.flag != 0); err != nil {
				return err
			}
		}
	}
	if len(refs.comments) == 0 {
		return nil
	}
	name, err := relationships.implicit(main, "comments")
	if err != nil {
		return err
	}
	if name == "" {
		return errors.New("missing DOCX comments relationship")
	}
	if err := read(name, "comments", "", true); err != nil {
		return err
	}
	if len(refs.comments) != 0 {
		return errors.New("unresolved DOCX comment references")
	}
	return nil
}
