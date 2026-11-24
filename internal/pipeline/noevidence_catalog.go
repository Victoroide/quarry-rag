package pipeline

import (
	"cmp"
	json "encoding/json/v2"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
)

const (
	maxNoEvidenceCatalogBytes = 64 * 1024
	maxNoEvidenceTranslations = 128
	maxNoEvidenceMessageBytes = 1024
)

// LoadNoEvidenceCatalog reads an optional local JSON catalog at startup.
// An empty path selects the built-in translations.
func LoadNoEvidenceCatalog(path string) (*NoEvidenceCatalog, error) {
	if path == "" {
		return builtinNoEvidenceCatalog(), nil
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(filepath.Dir(resolved))
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	file, err := root.OpenFile(filepath.Base(resolved), catalogOpenFlags, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("catalog path must identify a regular file")
	}
	body, err := io.ReadAll(io.LimitReader(file, maxNoEvidenceCatalogBytes+1))
	if err != nil {
		return nil, err
	}
	return parseNoEvidenceCatalog(body)
}

func parseNoEvidenceCatalog(body []byte) (*NoEvidenceCatalog, error) {
	if len(body) > maxNoEvidenceCatalogBytes {
		return nil, fmt.Errorf("catalog exceeds %d bytes", maxNoEvidenceCatalogBytes)
	}
	var entries map[string]string
	// The v2 decoder rejects duplicate object names and malformed Unicode.
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("catalog must be a UTF-8 JSON object of strings: %w", err)
	}
	if entries == nil {
		return nil, fmt.Errorf("catalog must be a UTF-8 JSON object of strings")
	}
	if len(entries) > maxNoEvidenceTranslations {
		return nil, fmt.Errorf("catalog exceeds %d translations", maxNoEvidenceTranslations)
	}
	if len(entries) == 0 {
		return builtinNoEvidenceCatalog(), nil
	}
	seen := make(map[string]bool, len(noEvidenceMessages)+len(entries))
	for _, message := range noEvidenceMessages {
		seen[strings.ToLower(message.tag.String())] = true
	}
	additions := make([]noEvidenceMessage, 0, len(entries))
	for rawTag, answer := range entries {
		if !validCatalogLanguageTag(rawTag) {
			return nil, fmt.Errorf("invalid language tag %q", rawTag)
		}
		tag, err := canonicalNoEvidenceTag(rawTag)
		if err != nil {
			return nil, fmt.Errorf("invalid language tag %q: %w", rawTag, err)
		}
		if len(tag.text) > maxAcceptLanguageBytes {
			return nil, fmt.Errorf("canonical language tag %q exceeds %d bytes", tag.text, maxAcceptLanguageBytes)
		}
		if seen[strings.ToLower(tag.text)] {
			return nil, fmt.Errorf("duplicate language tag %q", tag.text)
		}
		if !strings.ContainsFunc(answer, visibleNoEvidenceRune) {
			return nil, fmt.Errorf("translation for %q is empty", tag.text)
		}
		if len(answer) > maxNoEvidenceMessageBytes {
			return nil, fmt.Errorf("translation for %q exceeds %d bytes", tag.text, maxNoEvidenceMessageBytes)
		}
		if strings.ContainsFunc(answer, invalidNoEvidenceRune) {
			return nil, fmt.Errorf("translation for %q contains a control or bidi formatting character", tag.text)
		}
		seen[strings.ToLower(tag.text)] = true
		additions = append(additions, noEvidenceMessage{tag: tag, answer: answer})
	}
	slices.SortFunc(additions, func(a, b noEvidenceMessage) int {
		return cmp.Compare(a.tag.text, b.tag.text)
	})
	return newNoEvidenceCatalog(additions), nil
}

func visibleNoEvidenceRune(r rune) bool {
	return unicode.IsGraphic(r) && !unicode.IsSpace(r) &&
		!unicode.Is(unicode.Other_Default_Ignorable_Code_Point, r) && !unicode.Is(unicode.Variation_Selector, r)
}

func invalidNoEvidenceRune(r rune) bool {
	switch r {
	case '\u061c', '\u200e', '\u200f': // Standalone Arabic, left-to-right and right-to-left marks.
		return false
	default:
		return unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r)
	}
}
