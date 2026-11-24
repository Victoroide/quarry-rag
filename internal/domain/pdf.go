package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxPDFPages caps the entire original document, including unselected pages.
const MaxPDFPages = 1000

// MinPDFTextBytes is the trimmed UTF-8 length required for usable PDF text.
const MinPDFTextBytes = 10

// PDFPageEvidence describes the parser's observations before OCR.
type PDFPageEvidence string

// Page evidence distinguishes visible text, imagery and undecoded content.
const (
	PDFEmbedded   PDFPageEvidence = "embedded"
	PDFBlank      PDFPageEvidence = "blank"
	PDFImage      PDFPageEvidence = "image"
	PDFSparse     PDFPageEvidence = "sparse"
	PDFUnreadable PDFPageEvidence = "unreadable"
	PDFMixed      PDFPageEvidence = "mixed"
)

// PDFPage records embedded text and evidence for one original page.
type PDFPage struct {
	Number   int             `json:"number"`
	Evidence PDFPageEvidence `json:"evidence"`
	Text     string          `json:"text,omitempty"`
}

// PDFPages contains one ordered entry per page of the original.
type PDFPages []PDFPage

// PDFCoverage records extraction separately from indexing.
type PDFCoverage string

// Coverage states distinguish pages awaiting detection, usable results and failures.
const (
	PDFPending  PDFCoverage = "pending"
	PDFComplete PDFCoverage = "complete"
	PDFPartial  PDFCoverage = "partial"
	PDFFailed   PDFCoverage = "failed"
)

const pdfPageWord = `(?:pages?|p[aáà]ginas?|p[aá]gs?|seiten?|pagin[ae]|страница|页|頁)\.?\s*:?\s*`
const pdfPageCountSeparator = `\s*(?:of|de|sur|von|di|из|/)\s*`

var (
	pdfRunningLabel = regexp.MustCompile(`(?i)^(?:` + pdfPageWord + `)?\p{Nd}+(?:` + pdfPageCountSeparator + `\p{Nd}+)?(?:\s*[页頁])?\.?$`)
	pdfRomanLabel   = regexp.MustCompile(`(?i)^` + pdfPageWord + `([ivxlcdm]{1,15})(?:` + pdfPageCountSeparator + `([ivxlcdm]{1,15}))?\.?$`)
	pdfRomanNumber  = regexp.MustCompile(`(?i)^m{0,3}(?:cm|cd|d?c{0,3})(?:xc|xl|l?x{0,3})(?:ix|iv|v?i{0,3})$`)
)

// PDFRunningLabel identifies a single running page number or page count label.
func PDFRunningLabel(text string) bool {
	text = strings.TrimSpace(text)
	if pdfRunningLabel.MatchString(text) {
		return true
	}
	match := pdfRomanLabel.FindStringSubmatch(text)
	return len(match) == 3 && pdfRomanNumber.MatchString(match[1]) && (match[2] == "" || pdfRomanNumber.MatchString(match[2]))
}

// PDFTextReadable requires enough decoded letters to distinguish page labels
// from substantive embedded text, independently of the document length.
func PDFTextReadable(text string) bool {
	if len(text) < MinPDFTextBytes || strings.ContainsRune(text, utf8.RuneError) {
		return false
	}
	letters, size := 0, 0
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if PDFRunningLabel(line) {
			continue
		}
		size += len(line)
		for _, r := range line {
			if unicode.IsLetter(r) {
				letters++
			}
		}
	}
	return letters >= 4 && size >= MinPDFTextBytes
}

// Text joins embedded text in original page order.
func (p PDFPages) Text() string {
	var text strings.Builder
	for _, page := range p {
		if page.Text == "" {
			continue
		}
		if text.Len() > 0 {
			text.WriteString("\n\n")
		}
		text.WriteString(page.Text)
	}
	return text.String()
}

// NeedsOCR reports whether any page needs text detection.
func (p PDFPages) NeedsOCR() bool {
	return p.CoverageBeforeOCR() == PDFPending
}

// CoverageBeforeOCR skips detection when unresolved pages already have usable
// text and the selection policy cannot safely combine regions.
func (p PDFPages) CoverageBeforeOCR() PDFCoverage {
	coverage := PDFComplete
	for _, page := range p {
		switch {
		case page.Evidence == PDFBlank || page.Evidence == PDFEmbedded:
		case page.Evidence == PDFMixed && PDFTextReadable(page.Text):
			coverage = PDFPartial
		default:
			return PDFPending
		}
	}
	return coverage
}

// Validate checks page ordering, evidence, and aggregate text size.
func (p PDFPages) Validate() error {
	if len(p) == 0 || len(p) > MaxPDFPages {
		return errors.New("invalid PDF manifest page count")
	}
	var size, nonempty int
	for i, page := range p {
		if page.Number != i+1 || strings.TrimSpace(page.Text) != page.Text || strings.IndexByte(page.Text, 0) >= 0 || !utf8.ValidString(page.Text) {
			return fmt.Errorf("invalid PDF manifest page %d", i+1)
		}
		switch page.Evidence {
		case PDFBlank, PDFImage:
			if page.Text != "" {
				return fmt.Errorf("unexpected PDF text on page %d", i+1)
			}
		case PDFEmbedded:
			if !PDFTextReadable(page.Text) {
				return fmt.Errorf("unreadable embedded PDF text on page %d", i+1)
			}
		case PDFSparse, PDFMixed:
			if page.Text == "" {
				return fmt.Errorf("missing PDF text on page %d", i+1)
			}
		case PDFUnreadable:
		default:
			return fmt.Errorf("invalid PDF evidence on page %d", i+1)
		}
		if page.Text != "" {
			size += len(page.Text)
			if nonempty > 0 {
				size += 2
			}
			nonempty++
		}
		if size > MaxContentBytes {
			return errors.New("PDF manifest text exceeds content limit")
		}
	}
	return nil
}

// DecodePDFPages validates a bounded manifest from a child or durable storage.
func DecodePDFPages(data []byte) (PDFPages, error) {
	if len(data) > 6*MaxContentBytes+128*MaxPDFPages || !utf8.Valid(data) {
		return nil, errors.New("invalid PDF manifest encoding")
	}
	var pages PDFPages
	if err := json.Unmarshal(data, &pages); err != nil {
		return nil, fmt.Errorf("decode PDF manifest: %w", err)
	}
	if err := pages.Validate(); err != nil {
		return nil, err
	}
	return pages, nil
}
