package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/ocr"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergePDFPagesRecoversPrintedFreeText(t *testing.T) {
	const heading = "Operations register"
	const instruction = "Delivery requires refrigeration"
	const scan = "Final scanned delivery record"
	const annotation = "<< /Type /Annot /Subtype /FreeText /Rect [12 40 330 125] /F %d /Contents (" + instruction + ") /DA (/F1 15 Tf 0 g) /Border [0 0 0] >>"

	for _, tc := range []struct {
		name, annotation, want string
		state                  domain.PDFPageResult
		coverage               domain.PDFCoverage
		needsScan              bool
	}{
		{name: "printable", annotation: fmt.Sprintf(annotation, 4), want: heading + "\n\n" + instruction, state: domain.PDFPageResult{Number: 2, Source: "ocr", State: "partial"}, coverage: domain.PDFPartial},
		{name: "printable with scan", annotation: fmt.Sprintf(annotation, 4), want: heading + "\n\n" + instruction + "\n\n" + scan, state: domain.PDFPageResult{Number: 2, Source: "ocr", State: "partial"}, coverage: domain.PDFPartial, needsScan: true},
		{name: "blank with scan", want: heading + "\n\n" + scan, state: domain.PDFPageResult{Number: 2, State: "blank"}, coverage: domain.PDFComplete, needsScan: true},
		{name: "hidden with scan", annotation: fmt.Sprintf(annotation, 6), want: heading + "\n\n" + scan, state: domain.PDFPageResult{Number: 2, State: "blank"}, coverage: domain.PDFComplete, needsScan: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			streams := []string{"BT /F1 14 Tf 20 150 Td (" + heading + ") Tj ET", ""}
			detected := ocr.PageResults{Pages: map[int]ocr.Page{2: {Present: true, Text: instruction}}}
			if tc.needsScan {
				streams = append(streams, "q 150 0 0 150 0 0 cm /Scan Do Q")
				detected.Pages[3] = ocr.Page{Present: true, Text: scan}
			}
			pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(testutil.PDFWithPageAnnotation(tc.annotation, streams...)))
			require.NoError(t, err)
			require.NoError(t, pages.Validate())
			require.True(t, pages.NeedsOCR())
			result, err := mergePDFPages(pages, detected)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Text)
			require.Equal(t, tc.coverage, result.Coverage)
			require.Equal(t, tc.state, result.Pages[1])
		})
	}
}

func TestMergePDFPagesPreservesOrderWithoutOverlayDuplicates(t *testing.T) {
	pages := domain.PDFPages{
		{Number: 1, Evidence: domain.PDFImage},
		{Number: 2, Evidence: domain.PDFEmbedded, Text: "Archive report\nEnglish and café français"},
		{Number: 3, Evidence: domain.PDFImage},
		{Number: 4, Evidence: domain.PDFBlank},
		{Number: 5, Evidence: domain.PDFEmbedded, Text: "Archive report"},
		{Number: 6, Evidence: domain.PDFImage},
	}
	detected := ocr.PageResults{Pages: map[int]ocr.Page{
		1: {Present: true, Text: "First scanned paragraph"},
		2: {Present: true, Text: "Duplicate digital text"},
		3: {Present: true, Text: "Middle scanned paragraph"},
		4: {Present: true, Text: "Blank-page false positive"},
		5: {Present: true, Text: "Duplicate repeated heading"},
		6: {Present: true, Text: "Final scanned paragraph"},
	}}
	result, err := mergePDFPages(pages, detected)
	require.NoError(t, err)
	require.Equal(t, domain.PDFComplete, result.Coverage)
	require.Equal(t, "First scanned paragraph\n\nArchive report\nEnglish and café français\n\nMiddle scanned paragraph\n\nArchive report\n\nFinal scanned paragraph", result.Text)
	require.Equal(t, []domain.PDFPageResult{{Number: 1, Source: "ocr", State: "text"}, {Number: 2, Source: "parser", State: "text"}, {Number: 3, Source: "ocr", State: "text"}, {Number: 4, State: "blank"}, {Number: 5, Source: "parser", State: "text"}, {Number: 6, Source: "ocr", State: "text"}}, result.Pages)
}

func TestMergePDFPagesMarksUnresolvedAndMissingPages(t *testing.T) {
	pages := domain.PDFPages{
		{Number: 1, Evidence: domain.PDFMixed, Text: "Embedded overlay"},
		{Number: 2, Evidence: domain.PDFMixed, Text: "7"},
		{Number: 3, Evidence: domain.PDFUnreadable, Text: "�"},
		{Number: 4, Evidence: domain.PDFImage},
		{Number: 5, Evidence: domain.PDFImage},
		{Number: 6, Evidence: domain.PDFImage},
	}
	detected := ocr.PageResults{Pages: map[int]ocr.Page{
		1: {Present: true, Text: "Embedded overlay plus scan"},
		2: {Present: true, Text: "Recovered scan 7"},
		3: {Present: true, Text: "", Failed: false},
		5: {Present: true, Failed: true, Text: "Provider warned about this page"},
		6: {Present: true},
	}}
	result, err := mergePDFPages(pages, detected)
	require.NoError(t, err)
	require.Equal(t, domain.PDFPartial, result.Coverage)
	require.Equal(t, "Embedded overlay\n\nRecovered scan 7", result.Text)
	require.Equal(t, []domain.PDFPageResult{{Number: 1, Source: "parser", State: "partial"}, {Number: 2, Source: "ocr", State: "partial"}, {Number: 3, State: "unrecognized"}, {Number: 4, State: "missing"}, {Number: 5, State: "failed"}, {Number: 6, State: "unrecognized"}}, result.Pages)
}

func TestMergePDFPagesTreatsRunningLabelsAsSparse(t *testing.T) {
	for _, label := range []string{"Page 12 of 40", "Página 12 de 40", "Страница 12 из 40", "Page: 12 of 40", "Page 12 of 40.", "Page XII of XL"} {
		t.Run(label, func(t *testing.T) {
			const body = "Digitized archival content."
			pages := domain.PDFPages{{Number: 1, Evidence: domain.PDFMixed, Text: label}}
			detected := ocr.PageResults{Pages: map[int]ocr.Page{1: {Present: true, Text: body + "\n" + label}}}
			merged, err := mergePDFPages(pages, detected)
			require.NoError(t, err)
			require.Equal(t, body+"\n"+label, merged.Text)
			require.Equal(t, domain.PDFPartial, merged.Coverage)
			merged, err = mergePDFPages(domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, ocr.PageResults{Pages: map[int]ocr.Page{1: {Present: true, Text: label}}})
			require.NoError(t, err)
			require.Equal(t, domain.PDFPartial, merged.Coverage)
		})
	}
}

func TestMergePDFKeepsDecodedFallbackForUnrecognizedRun(t *testing.T) {
	pages := domain.PDFPages{{Number: 1, Evidence: domain.PDFUnreadable, Text: "Readable heading"}}
	merged, err := mergePDFPages(pages, ocr.PageResults{Pages: map[int]ocr.Page{1: {Present: true}}})
	require.NoError(t, err)
	require.Equal(t, "Readable heading", merged.Text)
	require.Equal(t, domain.PDFPartial, merged.Coverage)
}

func TestMergePDFTextByteBoundary(t *testing.T) {
	pages := domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}
	nine, err := mergePDFPages(pages, ocr.PageResults{Pages: map[int]ocr.Page{1: {Present: true, Text: "Café doc"}}})
	require.NoError(t, err)
	require.Len(t, nine.Text, 9)
	require.Equal(t, domain.PDFPartial, nine.Coverage)
	ten, err := mergePDFPages(pages, ocr.PageResults{Pages: map[int]ocr.Page{1: {Present: true, Text: "Café data"}}})
	require.NoError(t, err)
	require.Len(t, ten.Text, 10)
	require.Equal(t, domain.PDFComplete, ten.Coverage)
}

type pdfTestDetector func(context.Context, string, string, int) (ocr.PageResults, error)

func (f pdfTestDetector) Extract(context.Context, string) (string, error) {
	return "", errors.New("legacy OCR called for mixed PDF")
}

func (f pdfTestDetector) ExtractPages(ctx context.Context, key, profile, _ string, count int, accepted func(context.Context) error) (ocr.PageResults, error) {
	result, err := f(ctx, key, profile, count)
	if err == nil {
		err = accepted(ctx)
	}
	return result, err
}

type pdfTestStore struct {
	*ocrLeaseStore
	beginErr error
	saved    *domain.PDFResult
}

func (s *pdfTestStore) BeginPDFOCR(context.Context, domain.Job) (domain.PDFOCRWindow, error) {
	return domain.PDFOCRWindow{ID: "detection-window", Expires: time.Now().Add(time.Hour)}, s.beginErr
}

func (s *pdfTestStore) RecordPDFOCRStart(context.Context, domain.Job, string) error { return nil }

func (s *pdfTestStore) StorePDFOCR(_ context.Context, _ domain.Job, result domain.PDFResult) error {
	s.saved = &result
	return s.storeErr
}

func TestMixedPDFWorkerSkipsOCRForEmptyFreeText(t *testing.T) {
	const heading = "Operations register"
	const text = "BT /F1 14 Tf 20 150 Td (" + heading + ") Tj ET"
	const annotation = "<< /Type /Annot /Subtype /FreeText /Rect [12 40 330 125] /F 4 /Contents (%s) /DA (/F1 15 Tf 0 g) /Border [0 0 0] >>"
	for _, tc := range []struct {
		name, annotation string
	}{
		{name: "blank"},
		{name: "empty", annotation: fmt.Sprintf(annotation, "")},
		{name: "whitespace", annotation: fmt.Sprintf(annotation, "  ")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(testutil.PDFWithPageAnnotation(tc.annotation, text, "")))
			require.NoError(t, err)
			job := ocrJob()
			job.Document.TextSource = "parser"
			job.Document.Content = pages.Text()
			if pages.NeedsOCR() {
				job.Document.Content = ""
			}
			job.PDF = &domain.PDFExtraction{Profile: domain.MixedPDFProfile, Coverage: pages.CoverageBeforeOCR(), Pages: pages}
			s := &pdfTestStore{ocrLeaseStore: &ocrLeaseStore{leaseTestStore: leaseStore(0)}}
			calls := 0
			var indexed string
			w := New(s, []pipeline.Stage{ocrStage{func(ic *pipeline.IngestContext) error {
				indexed = ic.RawText
				return nil
			}}}, nil, Options{OCR: pdfTestDetector(func(_ context.Context, _, _ string, count int) (ocr.PageResults, error) {
				calls++
				require.Equal(t, 2, count)
				return ocr.PageResults{Pages: map[int]ocr.Page{
					1: {Present: true, Text: heading},
					2: {Present: true, Text: "Unexpected provider text"},
				}}, nil
			})})
			w.process(t.Context(), job)
			assert.Zero(t, calls)
			assert.Equal(t, heading, indexed)
		})
	}
}

func TestMixedPDFWorkerCheckpointsBeforePipeline(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		coverage                          domain.PDFCoverage
		beginErr, providerErr, persistErr error
		calls, stages                     int
	}{
		{name: "ready", coverage: domain.PDFPending, calls: 1, stages: 1},
		{name: "digital", coverage: domain.PDFComplete, stages: 1},
		{name: "expired", coverage: domain.PDFPending, beginErr: domain.ErrPDFOCRExpired},
		{name: "lost lease", coverage: domain.PDFPending, beginErr: domain.ErrJobNotOwned},
		{name: "provider object failure", coverage: domain.PDFPending, providerErr: domain.Permanent(ocr.ErrDetectionFailed), calls: 1},
		{name: "persistence failure", coverage: domain.PDFPending, persistErr: errors.New("database unavailable"), calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &pdfTestStore{ocrLeaseStore: &ocrLeaseStore{leaseTestStore: leaseStore(0), storeErr: tc.persistErr}, beginErr: tc.beginErr}
			job := ocrJob()
			job.Document.TextSource = "parser"
			job.Document.Content = "Digital text for retry"
			job.PDF = &domain.PDFExtraction{Profile: domain.MixedPDFProfile, Coverage: tc.coverage, Pages: domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}, {Number: 2, Evidence: domain.PDFBlank}}}
			calls, stages := 0, 0
			wk := New(s, []pipeline.Stage{ocrStage{func(ic *pipeline.IngestContext) error {
				stages++
				if tc.coverage == domain.PDFPending {
					require.NotNil(t, s.saved)
					require.Equal(t, s.saved.Text, ic.RawText)
				}
				return nil
			}}}, nil, Options{OCR: pdfTestDetector(func(_ context.Context, key, profile string, count int) (ocr.PageResults, error) {
				calls++
				require.Equal(t, *job.Document.S3Key, key)
				require.Equal(t, domain.MixedPDFProfile, profile)
				require.Equal(t, 2, count)
				return ocr.PageResults{Pages: map[int]ocr.Page{1: {Present: true, Text: "Recognized scanned text"}}}, tc.providerErr
			})})
			wk.process(t.Context(), job)
			require.Equal(t, tc.calls, calls)
			require.Equal(t, tc.stages, stages)
		})
	}
}

type layoutTestAnalyzer struct {
	fn func(context.Context, string, string, string, int, func(context.Context) error) (ocr.LayoutResults, error)
}

func (a layoutTestAnalyzer) Extract(context.Context, string) (string, error) {
	return "", errors.New("legacy detection called for layout PDF")
}

func (a layoutTestAnalyzer) ExtractPages(context.Context, string, string, string, int, func(context.Context) error) (ocr.PageResults, error) {
	return ocr.PageResults{}, errors.New("mixed detection called for layout PDF")
}

func (a layoutTestAnalyzer) AnalyzePages(ctx context.Context, key, profile, window string, count int, accepted func(context.Context) error) (ocr.LayoutResults, error) {
	res, err := a.fn(ctx, key, profile, window, count, accepted)
	if err == nil && accepted != nil {
		err = accepted(ctx)
	}
	return res, err
}

func TestLayoutPDFWorkerUsesAnalyzePagesOnlyAndCarriesSections(t *testing.T) {
	s := &pdfTestStore{ocrLeaseStore: &ocrLeaseStore{leaseTestStore: leaseStore(0)}}
	job := ocrJob()
	job.Document.TextSource = "parser"
	job.Document.Content = ""
	job.PDF = &domain.PDFExtraction{
		Profile:  domain.LayoutPDFProfile,
		Coverage: domain.PDFPending,
		Pages: domain.PDFPages{
			{Number: 1, Evidence: domain.PDFEmbedded, Text: "Embedded digital text"},
			{Number: 2, Evidence: domain.PDFBlank},
		},
	}

	calls := 0
	var gotSections []domain.LayoutSection
	var gotText string
	wk := New(s, []pipeline.Stage{ocrStage{func(ic *pipeline.IngestContext) error {
		gotSections = ic.LayoutSections
		gotText = ic.RawText
		return nil
	}}}, nil, Options{OCR: layoutTestAnalyzer{fn: func(_ context.Context, key, profile, _ string, count int, _ func(context.Context) error) (ocr.LayoutResults, error) {
		calls++
		require.Equal(t, *job.Document.S3Key, key)
		require.Equal(t, domain.LayoutPDFProfile, profile)
		require.Equal(t, 2, count)
		return ocr.LayoutResults{Pages: map[int]ocr.LayoutPage{
			1: {Present: true, Sections: []domain.LayoutSection{{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "Column: Value"}}, Text: "Column: Value"},
			2: {Present: true},
		}}, nil
	}}})
	wk.process(t.Context(), job)
	require.Equal(t, 1, calls)
	require.NotNil(t, s.saved)
	require.Equal(t, domain.PDFComplete, s.saved.Coverage)
	require.Equal(t, "Column: Value", gotText)
	require.Equal(t, []domain.LayoutSection{{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "Column: Value"}}, gotSections)

	// Retry after checkpoint reuses persisted PageResults sections without calling Textract again.
	retryJob := ocrJob()
	retryJob.Document.Content = "Column: Value"
	retryJob.PDF = &domain.PDFExtraction{
		Profile:     domain.LayoutPDFProfile,
		Coverage:    domain.PDFComplete,
		PageResults: s.saved.Pages,
	}
	gotSections = nil
	wk.process(t.Context(), retryJob)
	require.Equal(t, 1, calls)
	require.Equal(t, []domain.LayoutSection{{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "Column: Value"}}, gotSections)
}
