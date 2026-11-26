package handler

import (
	"context"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
)

type ingestOptions struct {
	maxPending            int
	extract               func(context.Context, extractor.FileType, []byte) (string, error)
	limiter               *extractor.Limiter
	extractPages          func(context.Context, []byte) (domain.PDFPages, error)
	pdfLayout             bool
	ocr                   bool
	contentDedup          domain.ContentDedup
	fixturePrefix         string
	fixtureVectorIdentity string
	journal               domain.SourceUploadJournal
	owned                 bool
}

// WithOwnedIngestion binds every upload to the verified principal in the
// request context. It requires a store that authorizes owners, private content
// identity and a handler guarded by the subject gate. The mode has no public
// switch yet; it is reachable only through this internal option.
func WithOwnedIngestion() IngestOption {
	return func(opts *ingestOptions) { opts.owned = true }
}

// WithEvaluationFixtures enables receipt-qualified admission for one staging prefix.
func WithEvaluationFixtures(prefix, vectorIdentity string) IngestOption {
	return func(opts *ingestOptions) {
		opts.fixturePrefix = prefix
		opts.fixtureVectorIdentity = vectorIdentity
	}
}

// WithMixedPDFOCR enables page-aware ingestion using the isolated PDF parser.
func WithMixedPDFOCR(extract func(context.Context, []byte) (domain.PDFPages, error)) IngestOption {
	return func(opts *ingestOptions) { opts.extractPages = extract }
}

// WithPDFLayout enables Textract layout and table extraction for PDF uploads.
func WithPDFLayout(enabled bool) IngestOption {
	return func(opts *ingestOptions) { opts.pdfLayout = enabled }
}

// WithOCR enables deferred text detection for image-only PDF uploads.
func WithOCR(enabled bool) IngestOption {
	return func(opts *ingestOptions) { opts.ocr = enabled }
}

// IngestOption configures file extraction and upload admission.
type IngestOption func(*ingestOptions)

// WithContentDedup selects how new uploads participate in collection-wide matching.
func WithContentDedup(policy domain.ContentDedup) IngestOption {
	return func(opts *ingestOptions) {
		if policy != "" {
			opts.contentDedup = policy
		}
	}
}

// WithExtractor selects the function used to extract uploaded document text.
func WithExtractor(extract func(context.Context, extractor.FileType, []byte) (string, error)) IngestOption {
	return func(opts *ingestOptions) { opts.extract = extract }
}

// WithUploadLimiter reserves an upload slot before reading multipart data.
func WithUploadLimiter(limiter *extractor.Limiter) IngestOption {
	return func(opts *ingestOptions) { opts.limiter = limiter }
}

// WithMaxPending sets the maximum admitted ingestion jobs across replicas.
func WithMaxPending(limit int) IngestOption {
	return func(opts *ingestOptions) {
		if limit > 0 {
			opts.maxPending = limit
		}
	}
}
