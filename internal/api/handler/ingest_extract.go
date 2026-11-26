package handler

import (
	"bytes"
	"context"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/tracing"
)

// uploadText is the extraction result that admission needs.
type uploadText struct {
	text       string
	pdf        *domain.PDFExtraction
	ocrPending bool
}

// extractUploadText parses one upload. A returned extraction error is not final
// when it only reports a PDF without text that deferred detection can read.
func extractUploadText(ctx context.Context, opts ingestOptions, ft extractor.FileType, data []byte) (uploadText, *APIError, error) {
	var out uploadText
	var text string
	var extractErr error
	switch {
	case ft == extractor.FileTypePDF && opts.pdfLayout && opts.extractPages == nil:
		extractErr = extractor.ErrExtractionUnavailable
	case ft == extractor.FileTypePDF && opts.extractPages != nil:
		out.pdf, text, extractErr = inspectPDFUpload(ctx, opts.extractPages, data, opts.pdfLayout)
	case ft == extractor.FileTypeText:
		extract := tracing.WrapExtract(func(ctx context.Context, ft extractor.FileType, data []byte) (string, error) {
			return extractor.New(ft).Extract(ctx, bytes.NewReader(data))
		})
		text, extractErr = extract(ctx, ft, data)
	default:
		text, extractErr = opts.extract(ctx, ft, data)
	}
	out.ocrPending = extractErr != nil && opts.ocr && opts.extractPages == nil && ft == extractor.FileTypePDF && extractionErrorIs(extractErr, extractor.ErrNoText)
	if extractErr != nil && !out.ocrPending {
		return out, nil, extractErr
	}
	text = strings.ReplaceAll(text, "\x00", "")
	if out.ocrPending || out.pdf != nil && out.pdf.Coverage == domain.PDFPending {
		text = ""
	} else if apiErr := validateIngestContent(text); apiErr != nil {
		return out, apiErr, nil
	}
	out.text = text
	return out, nil, nil
}
