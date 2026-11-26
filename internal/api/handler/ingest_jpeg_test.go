package handler

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestMixedPDFUploadAcceptsJPEGScanPage(t *testing.T) {
	var scan bytes.Buffer
	require.NoError(t, jpeg.Encode(&scan, image.NewGray(image.Rect(0, 0, 12, 12)), nil))
	jpegBytes := scan.Bytes()

	validObj := fmt.Appendf(nil, "<< /Type /XObject /Subtype /Image /Width 12 /Height 12 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode /Length %d >>\nstream\n", len(jpegBytes))
	validObj = append(validObj, jpegBytes...)
	validObj = append(validObj, "\nendstream"...)
	validPDF := testutil.PDFWithPageResources([]string{
		`BT /F1 12 Tf (Archive operating hours) Tj ET`,
		`q 180 0 0 180 10 10 cm /Photo Do Q`,
	}, "/Photo 5 0 R", [][]byte{validObj})

	truncatedBytes := jpegBytes[:len(jpegBytes)-2]
	truncatedObj := fmt.Appendf(nil, "<< /Type /XObject /Subtype /Image /Width 12 /Height 12 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode /Length %d >>\nstream\n", len(truncatedBytes))
	truncatedObj = append(truncatedObj, truncatedBytes...)
	truncatedObj = append(truncatedObj, "\nendstream"...)
	truncatedPDF := testutil.PDFWithPageResources([]string{
		`q 180 0 0 180 10 10 cm /Photo Do Q`,
	}, "/Photo 5 0 R", [][]byte{truncatedObj})

	extract := WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	})

	t.Run("valid JPEG scan", func(t *testing.T) {
		s := &ocrAcceptStore{outcome: domain.AcceptNeedsSource}
		objects := &ocrObjects{}
		h := IngestHandler(nil, s, objects, 1<<20, extract)
		rw := httptest.NewRecorder()
		h(rw, ocrUpload(t, "scan.pdf", "archive", validPDF))
		require.Equal(t, http.StatusAccepted, rw.Code, rw.Body.String())
		require.Equal(t, validPDF, objects.data)
		require.Len(t, s.accepted, 2)
		req := s.accepted[1]
		require.NotNil(t, req.PDF)
		require.Equal(t, domain.PDFPending, req.PDF.Coverage)
		require.Equal(t, domain.PDFPages{
			{Number: 1, Evidence: domain.PDFEmbedded, Text: "Archive operating hours"},
			{Number: 2, Evidence: domain.PDFImage},
		}, req.PDF.Pages)
		require.Empty(t, req.Document.Content)
	})

	t.Run("truncated JPEG", func(t *testing.T) {
		s := &ocrAcceptStore{outcome: domain.AcceptNeedsSource}
		objects := &ocrObjects{}
		h := IngestHandler(nil, s, objects, 1<<20, extract)
		rw := httptest.NewRecorder()
		h(rw, ocrUpload(t, "broken.pdf", "archive", truncatedPDF))
		require.Equal(t, http.StatusUnprocessableEntity, rw.Code, rw.Body.String())
		require.Contains(t, rw.Body.String(), `"code":"unparseable"`)
		require.Empty(t, objects.data)
		require.Empty(t, s.accepted)
	})
}
