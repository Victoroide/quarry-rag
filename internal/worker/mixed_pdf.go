package worker

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/ocr"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
)

type pdfDetector interface {
	ExtractPages(context.Context, string, string, string, int, func(context.Context) error) (ocr.PageResults, error)
}

type pdfAnalyzer interface {
	AnalyzePages(context.Context, string, string, string, int, func(context.Context) error) (ocr.LayoutResults, error)
}

func (w *Worker) extractPDFText(ctx context.Context, job domain.Job, ic *pipeline.IngestContext) error {
	store, ok := w.store.(interface {
		BeginPDFOCR(context.Context, domain.Job) (domain.PDFOCRWindow, error)
		RecordPDFOCRStart(context.Context, domain.Job, string) error
		StorePDFOCR(context.Context, domain.Job, domain.PDFResult) error
	})
	if !ok {
		return errors.New("ocr: PDF page storage is unavailable")
	}
	if job.Document.S3Key == nil || *job.Document.S3Key == "" {
		return domain.Permanent(errors.New("ocr: original document is missing"))
	}
	switch job.PDF.Profile {
	case domain.MixedPDFProfile, domain.LayoutPDFProfile:
	default:
		return domain.Permanent(errors.New("ocr: unsupported PDF extraction profile"))
	}
	window, err := store.BeginPDFOCR(ctx, job)
	if err != nil {
		return err
	}
	ocrCtx, cancel := context.WithDeadline(ctx, window.Expires)
	defer cancel()
	if contextErr := ocrCtx.Err(); contextErr != nil {
		return contextErr
	}
	recordStart := func(ctx context.Context) error {
		return store.RecordPDFOCRStart(ctx, job, window.ID)
	}

	var result domain.PDFResult
	var sections []domain.LayoutSection
	switch job.PDF.Profile {
	case domain.LayoutPDFProfile:
		analyzer, ok := w.opts.OCR.(pdfAnalyzer)
		if !ok {
			return errors.New("ocr: PDF layout analysis is unavailable")
		}
		analyzed, analyzeErr := analyzer.AnalyzePages(ocrCtx, *job.Document.S3Key, job.PDF.Profile, window.ID, len(job.PDF.Pages), recordStart)
		if analyzeErr != nil {
			return analyzeErr
		}
		result, sections, err = mergeLayoutPages(job.PDF.Pages, analyzed)
	default:
		detector, ok := w.opts.OCR.(pdfDetector)
		if !ok {
			return errors.New("ocr: PDF page detection is unavailable")
		}
		detected, detectErr := detector.ExtractPages(ocrCtx, *job.Document.S3Key, job.PDF.Profile, window.ID, len(job.PDF.Pages), recordStart)
		if detectErr != nil {
			return detectErr
		}
		result, err = mergePDFPages(job.PDF.Pages, detected)
	}
	if err != nil {
		return err
	}
	if storeErr := store.StorePDFOCR(ctx, job, result); storeErr != nil {
		return storeErr
	}
	if len(result.Text) < domain.MinPDFTextBytes {
		return domain.Permanent(ocr.ErrNoText)
	}
	ic.RawText = result.Text
	ic.LayoutSections = sections
	return nil
}

func layoutSectionsFromResults(pages []domain.PDFPageResult) []domain.LayoutSection {
	var sections []domain.LayoutSection
	for _, page := range pages {
		sections = append(sections, page.Sections...)
	}
	return sections
}

func mergeLayoutPages(pages domain.PDFPages, analyzed ocr.LayoutResults) (domain.PDFResult, []domain.LayoutSection, error) {
	result := domain.PDFResult{Coverage: domain.PDFComplete, Pages: make([]domain.PDFPageResult, 0, len(pages))}
	if analyzed.Partial {
		result.Coverage = domain.PDFPartial
	}
	var text strings.Builder
	var allSections []domain.LayoutSection
	for _, page := range pages {
		ap := analyzed.Pages[page.Number]
		pageRes := domain.PDFPageResult{Number: page.Number}
		var pageSections []domain.LayoutSection
		switch {
		case page.Evidence == domain.PDFBlank && (!ap.Present || strings.TrimSpace(ap.Text) == ""):
			pageRes.State = "blank"
		case ap.Failed:
			pageRes.State, pageRes.Reason = "failed", ap.Reason
			if pageRes.Reason == "" {
				pageRes.Reason = "provider_warning"
			}
			if page.Evidence == domain.PDFEmbedded && domain.PDFTextReadable(page.Text) {
				pageRes.Source, pageRes.State = "parser", "partial"
				pageSections = []domain.LayoutSection{{Page: page.Number, Text: page.Text}}
			}
		case !ap.Present:
			pageRes.State, pageRes.Reason = "missing", "missing_page"
			if page.Evidence == domain.PDFEmbedded && domain.PDFTextReadable(page.Text) {
				pageRes.Source, pageRes.State = "parser", "partial"
				pageSections = []domain.LayoutSection{{Page: page.Number, Text: page.Text}}
			}
		case ap.Text == "" || strings.ContainsRune(ap.Text, utf8.RuneError) || !utf8.ValidString(ap.Text):
			pageRes.State, pageRes.Reason = "unrecognized", ap.Reason
			if pageRes.Reason == "" {
				pageRes.Reason = "unrecognized"
			}
			if page.Evidence == domain.PDFEmbedded && domain.PDFTextReadable(page.Text) {
				pageRes.Source, pageRes.State = "parser", "partial"
				pageSections = []domain.LayoutSection{{Page: page.Number, Text: page.Text}}
			}
		default:
			pageRes.Source = "textract"
			if ap.Partial {
				pageRes.State, pageRes.Reason = "partial", ap.Reason
			} else {
				pageRes.State = "text"
			}
			pageSections = ap.Sections
		}
		if pageRes.State != "text" && pageRes.State != "blank" {
			result.Coverage = domain.PDFPartial
		}
		pageRes.Sections = pageSections
		result.Pages = append(result.Pages, pageRes)
		for _, sec := range pageSections {
			if strings.TrimSpace(sec.Text) == "" {
				continue
			}
			if text.Len() > 0 {
				text.WriteString("\n\n")
			}
			if len(sec.Text) > domain.MaxContentBytes-text.Len() {
				return domain.PDFResult{}, nil, domain.Permanent(ocr.ErrTextLimit)
			}
			text.WriteString(sec.Text)
			allSections = append(allSections, sec)
		}
	}
	result.Text = text.String()
	if len(result.Text) < domain.MinPDFTextBytes {
		result.Coverage = domain.PDFPartial
	}
	return result, allSections, nil
}

func mergePDFPages(pages domain.PDFPages, detected ocr.PageResults) (domain.PDFResult, error) {
	result := domain.PDFResult{Coverage: domain.PDFComplete, Pages: make([]domain.PDFPageResult, 0, len(pages))}
	if detected.Partial {
		result.Coverage = domain.PDFPartial
	}
	var text strings.Builder
	for _, page := range pages {
		chosen, status := selectPDFPage(page, detected.Pages[page.Number])
		result.Pages = append(result.Pages, status)
		if status.State != "text" && status.State != "blank" {
			result.Coverage = domain.PDFPartial
		}
		if chosen == "" {
			continue
		}
		if text.Len() > 0 {
			text.WriteString("\n\n")
		}
		if len(chosen) > domain.MaxContentBytes-text.Len() {
			return domain.PDFResult{}, domain.Permanent(ocr.ErrTextLimit)
		}
		text.WriteString(chosen)
	}
	result.Text = text.String()
	if len(result.Text) < domain.MinPDFTextBytes {
		result.Coverage = domain.PDFPartial
	}
	return result, nil
}

func selectPDFPage(page domain.PDFPage, detected ocr.Page) (string, domain.PDFPageResult) {
	result := domain.PDFPageResult{Number: page.Number}
	switch page.Evidence {
	case domain.PDFBlank:
		result.State = "blank"
		return "", result
	case domain.PDFEmbedded:
		result.Source, result.State = "parser", "text"
		return page.Text, result
	case domain.PDFMixed:
		if domain.PDFTextReadable(page.Text) {
			result.Source, result.State = "parser", "partial"
			return page.Text, result
		}
	}
	var chosen string
	if page.Text != "" && !strings.ContainsRune(page.Text, utf8.RuneError) {
		chosen, result.Source = page.Text, "parser"
	}
	switch {
	case detected.Failed:
		result.State = "failed"
	case !detected.Present:
		result.State = "missing"
	case detected.Text == "" || strings.ContainsRune(detected.Text, utf8.RuneError) || !utf8.ValidString(detected.Text):
		result.State = "unrecognized"
	default:
		result.State = "partial"
		readable := domain.PDFTextReadable(detected.Text)
		recovered := page.Evidence == domain.PDFUnreadable && readable && strings.Contains(detected.Text, chosen) && len(detected.Text) > len(chosen)
		if !pdfOnlyRunningLabels(detected.Text) && (chosen == "" || recovered || !domain.PDFTextReadable(chosen) && readable) {
			chosen, result.Source = detected.Text, "ocr"
		}
		if page.Evidence == domain.PDFImage && readable {
			result.State = "text"
		}
	}
	return chosen, result
}

func pdfOnlyRunningLabels(text string) bool {
	for line := range strings.SplitSeq(text, "\n") {
		if strings.TrimSpace(line) != "" && !domain.PDFRunningLabel(line) {
			return false
		}
	}
	return true
}
