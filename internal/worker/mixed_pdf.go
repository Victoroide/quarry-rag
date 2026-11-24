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

func (w *Worker) extractPDFText(ctx context.Context, job domain.Job, ic *pipeline.IngestContext) error {
	detector, ok := w.opts.OCR.(pdfDetector)
	if !ok {
		return errors.New("ocr: PDF page detection is unavailable")
	}
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
	if job.PDF.Profile != domain.MixedPDFProfile {
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
	detected, err := detector.ExtractPages(ocrCtx, *job.Document.S3Key, job.PDF.Profile, window.ID, len(job.PDF.Pages), func(ctx context.Context) error {
		return store.RecordPDFOCRStart(ctx, job, window.ID)
	})
	if err != nil {
		return err
	}
	result, err := mergePDFPages(job.PDF.Pages, detected)
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
	return nil
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
