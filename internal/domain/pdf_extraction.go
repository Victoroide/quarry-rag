package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

// MixedPDFProfile versions page inspection, evidence selection and merge rules.
const MixedPDFProfile = "quarry.pdf.mixed.v1"

// LayoutPDFProfile versions Textract layout and table extraction rules.
const LayoutPDFProfile = "quarry.pdf.layout.v1"

// PDFOCRLifetime bounds paid retries to Textract's token and result lifetime.
const PDFOCRLifetime = 7 * 24 * time.Hour

// PDFOCRWindow identifies one provider submission and its automatic retry deadline.
type PDFOCRWindow struct {
	ID      string
	Expires time.Time
}

// ErrPDFOCRExpired indicates that only an explicit retry can restart detection.
var ErrPDFOCRExpired = errors.New("ocr_result_expired")

// LayoutSection is one ordered paragraph, heading, caption, or table row
// with document-scoped coordinate provenance.
type LayoutSection struct {
	Page    int    `json:"page"`
	TableID string `json:"table_id,omitempty"`
	RowID   string `json:"row_id,omitempty"`
	Text    string `json:"text"`
}

// PDFExtraction is the accepted page manifest and versioned selection policy.
type PDFExtraction struct {
	Profile     string
	Coverage    PDFCoverage
	Pages       PDFPages
	PageResults []PDFPageResult
}

// PDFPageResult records the selected source and resolution for one page.
type PDFPageResult struct {
	Number   int             `json:"number"`
	Source   string          `json:"source,omitempty"`
	State    string          `json:"state"`
	Reason   string          `json:"reason,omitempty"`
	Sections []LayoutSection `json:"sections,omitempty"`
}

// PDFResult carries the merged text and coverage saved before indexing.
type PDFResult struct {
	Text     string
	Coverage PDFCoverage
	Pages    []PDFPageResult
}

// Validate checks that the checkpoint accounts for every original page.
func (r PDFResult) Validate(pageCount int) error {
	if pageCount < 1 || len(r.Pages) != pageCount || len(r.Text) > MaxContentBytes || strings.IndexByte(r.Text, 0) >= 0 || !utf8.ValidString(r.Text) ||
		(r.Coverage != PDFComplete && r.Coverage != PDFPartial) {
		return errors.New("invalid PDF detection checkpoint")
	}
	if r.Coverage == PDFComplete && len(r.Text) < MinPDFTextBytes {
		return errors.New("complete PDF detection has insufficient text")
	}
	for i, page := range r.Pages {
		if page.Number != i+1 || (r.Coverage == PDFComplete && page.State != "text" && page.State != "blank") {
			return errors.New("PDF detection checkpoint has inconsistent page coverage")
		}
		for _, sec := range page.Sections {
			if sec.Page != page.Number || strings.TrimSpace(sec.Text) == "" || strings.IndexByte(sec.Text, 0) >= 0 || !utf8.ValidString(sec.Text) ||
				strings.IndexByte(sec.TableID, 0) >= 0 || !utf8.ValidString(sec.TableID) ||
				strings.IndexByte(sec.RowID, 0) >= 0 || !utf8.ValidString(sec.RowID) {
				return errors.New("PDF detection checkpoint has invalid layout section")
			}
		}
	}
	return nil
}
