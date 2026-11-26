package extractor_test

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/stretchr/testify/require"
)

func testGrayJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, jpeg.Encode(&buf, image.NewGray(image.Rect(0, 0, w, h)), nil))
	return buf.Bytes()
}

func testRGBJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := range h {
		for x := range w {
			img.SetRGBA(x, y, color.RGBA{
				R: uint8(((x * 17) ^ y) & 0xff),
				G: uint8(((y * 31) ^ x) & 0xff),
				B: uint8((x + y) & 0xff),
				A: 255,
			})
		}
	}
	var buf bytes.Buffer
	require.NoError(t, jpeg.Encode(&buf, img, &jpeg.Options{Quality: 95}))
	return buf.Bytes()
}

func testImageObject(dict string, payload []byte) []byte {
	obj := fmt.Appendf(nil, "<< %s /Length %d >>\nstream\n", dict, len(payload))
	obj = append(obj, payload...)
	obj = append(obj, "\nendstream"...)
	return obj
}

func TestPDFPageAcceptsJPEGScans(t *testing.T) {
	gray := testGrayJPEG(t, 12, 14)
	rgb := testRGBJPEG(t, 16, 12)
	iccProfile := []byte("<< /N 3 /Length 4 >>\nstream\n1234\nendstream")

	var compressedGray bytes.Buffer
	zw := zlib.NewWriter(&compressedGray)
	_, err := zw.Write(bytes.Repeat([]byte{0x80}, 12*14))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	t.Run("gray DeviceGray", func(t *testing.T) {
		obj := testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode", gray)
		data := testutil.PDFWithPageResources([]string{"q 100 0 0 100 0 0 cm /Photo Do Q"}, "/Photo 5 0 R", [][]byte{obj})
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, pages)
	})

	t.Run("RGB DeviceRGB", func(t *testing.T) {
		obj := testImageObject("/Type /XObject /Subtype /Image /Width 16 /Height 12 /BitsPerComponent 8 /ColorSpace /DeviceRGB /Filter /DCTDecode", rgb)
		data := testutil.PDFWithPageResources([]string{"q 100 0 0 100 0 0 cm /Photo Do Q"}, "/Photo 5 0 R", [][]byte{obj})
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, pages)
	})

	t.Run("array filter ColorTransform 0 and Decode array", func(t *testing.T) {
		obj := testImageObject("/Type /XObject /Subtype /Image /Width 16 /Height 12 /BitsPerComponent 8 /ColorSpace /DeviceRGB /Filter [/DCTDecode] /DecodeParms [<< /ColorTransform 0 >>] /Decode [0 1 0 1 0 1]", rgb)
		data := testutil.PDFWithPageResources([]string{"q 100 0 0 100 0 0 cm /Photo Do Q"}, "/Photo 5 0 R", [][]byte{obj})
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, pages)
	})

	t.Run("named color space resolving to ICCBased N=3", func(t *testing.T) {
		const content = "q 100 0 0 100 0 0 cm /Photo Do Q"
		objects := [][]byte{
			[]byte("<< /Type /Catalog /Pages 2 0 R >>"),
			[]byte("<< /Type /Pages /Kids [3 0 R] /Count 1 >>"),
			[]byte("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /ColorSpace << /CS1 [/ICCBased 6 0 R] >> /XObject << /Photo 5 0 R >> >> >>"),
			[]byte(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(content), content)),
			testImageObject("/Type /XObject /Subtype /Image /Width 16 /Height 12 /BitsPerComponent 8 /ColorSpace /CS1 /Filter /DCTDecode", rgb),
			iccProfile,
		}
		data := testutil.PDFWithObjects(objects)
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, pages)
	})

	t.Run("JPEG inside Form XObject", func(t *testing.T) {
		photo := testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode", gray)
		const formContent = "q 100 0 0 100 0 0 cm /Photo Do Q"
		form := []byte(fmt.Sprintf("<< /Type /XObject /Subtype /Form /BBox [0 0 100 100] /Resources << /XObject << /Photo 5 0 R >> >> /Length %d >>\nstream\n%s\nendstream", len(formContent), formContent))
		data := testutil.PDFWithPageResources([]string{"/Form Do"}, "/Photo 5 0 R /Form 6 0 R", [][]byte{photo, form})
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, pages)
	})

	t.Run("gray JPEG SMask on Flate image", func(t *testing.T) {
		smask := testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode", gray)
		flateImg := testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /FlateDecode /SMask 5 0 R", compressedGray.Bytes())
		data := testutil.PDFWithPageResources([]string{"q 100 0 0 100 0 0 cm /Masked Do Q"}, "/Masked 6 0 R", [][]byte{smask, flateImg})
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, pages)
	})

	t.Run("JPEG plus readable text yields mixed partial", func(t *testing.T) {
		obj := testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode", gray)
		data := testutil.PDFWithPageResources([]string{"q 100 0 0 100 0 0 cm /Photo Do Q BT /F1 12 Tf (Readable overlay archive text) Tj ET"}, "/Photo 5 0 R", [][]byte{obj})
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFMixed, Text: "Readable overlay archive text"}}, pages)
		require.Equal(t, domain.PDFPartial, pages.CoverageBeforeOCR())
		require.False(t, pages.NeedsOCR())
	})

	t.Run("stray bytes after DQT", func(t *testing.T) {
		gray16 := testGrayJPEG(t, 16, 16)
		dqtIdx := bytes.Index(gray16, []byte{0xFF, 0xDB})
		require.NotEqual(t, -1, dqtIdx)
		segLen := int(gray16[dqtIdx+2])<<8 | int(gray16[dqtIdx+3])
		insertAt := dqtIdx + 2 + segLen
		withStray := make([]byte, 0, len(gray16)+1)
		withStray = append(withStray, gray16[:insertAt]...)
		withStray = append(withStray, 0x00)
		withStray = append(withStray, gray16[insertAt:]...)

		_, err := jpeg.DecodeConfig(bytes.NewReader(withStray))
		require.NoError(t, err)

		obj := testImageObject("/Type /XObject /Subtype /Image /Width 16 /Height 16 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode", withStray)
		data := testutil.PDFWithPageResources([]string{"q 100 0 0 100 0 0 cm /Photo Do Q"}, "/Photo 5 0 R", [][]byte{obj})
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, pages)

		badObj := testImageObject("/Type /XObject /Subtype /Image /Width 17 /Height 16 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode", withStray)
		badData := testutil.PDFWithPageResources([]string{"q 100 0 0 100 0 0 cm /Photo Do Q"}, "/Photo 5 0 R", [][]byte{badObj})
		badPages, badErr := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(badData))
		require.ErrorContains(t, badErr, "JPEG frame does not match its image dictionary")
		require.Empty(t, badPages)
	})
}

func TestPDFPageRejectsUnverifiableJPEG(t *testing.T) {
	gray := testGrayJPEG(t, 12, 14)

	// Synthetic 10001x1 JPEG frame (only rejected by maxPDFImageSide check).
	hugeJPEG := []byte{
		0xFF, 0xD8,
		0xFF, 0xDB, 0x00, 0x43, 0x00,
	}
	hugeJPEG = append(hugeJPEG, bytes.Repeat([]byte{0x10}, 64)...)
	hugeJPEG = append(hugeJPEG,
		0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01, 0x27, 0x11, 0x01, 0x01, 0x11, 0x00,
		0xFF, 0xC4, 0x00, 0x14, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xFF, 0xC4, 0x00, 0x14, 0x10, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3F, 0x00, 0x00,
		0xFF, 0xD9,
	)

	// /Length past endstream
	lengthPastEndstream := fmt.Appendf(nil, "<< /Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode /Length %d >>\nstream\n", len(gray)+20)
	lengthPastEndstream = append(lengthPastEndstream, gray...)
	lengthPastEndstream = append(lengthPastEndstream, "\nendstream"...)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"width mismatch 13 vs 12", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 13 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode", gray)})},
		{"DeviceRGB with gray frame", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceRGB /Filter /DCTDecode", gray)})},
		{"BitsPerComponent 16", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 16 /ColorSpace /DeviceGray /Filter /DCTDecode", gray)})},
		{"Indexed color space", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace [/Indexed /DeviceRGB 0 <000000>] /Filter /DCTDecode", gray)})},
		{"ImageMask true", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ImageMask true /Filter /DCTDecode", gray)})},
		{"ICC N=2", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{
			testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace [/ICCBased 6 0 R] /Filter /DCTDecode", gray),
			[]byte("<< /N 2 /Length 4 >>\nstream\n1234\nendstream"),
		})},
		{"ICC missing N", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{
			testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace [/ICCBased 6 0 R] /Filter /DCTDecode", gray),
			[]byte("<< /Length 4 >>\nstream\n1234\nendstream"),
		})},
		{"filter chain ASCII85Decode DCTDecode", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter [/ASCII85Decode /DCTDecode]", gray)})},
		{"filter chain FlateDecode DCTDecode", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter [/FlateDecode /DCTDecode]", gray)})},
		{"ColorTransform 2", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode /DecodeParms << /ColorTransform 2 >>", gray)})},
		{"unknown DecodeParms key", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode /DecodeParms << /Quality 90 >>", gray)})},
		{"Length past endstream", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{lengthPastEndstream})},
		{"JPXDecode", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /JPXDecode", gray)})},
		{"JBIG2Decode", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /JBIG2Decode", gray)})},
		{"CCITTFaxDecode", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /CCITTFaxDecode", gray)})},
		{"RunLengthDecode", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /RunLengthDecode", gray)})},
		{"LZWDecode", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 12 /Height 14 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /LZWDecode", gray)})},
		{"inline image with /F /DCT", testutil.PDFWithPageStreams("q BI /W 1 /H 1 /BPC 8 /CS /G /F /DCT ID x EI Q")},
		{"Width 10001 with matching frame", testutil.PDFWithPageResources([]string{"/Photo Do"}, "/Photo 5 0 R", [][]byte{testImageObject("/Type /XObject /Subtype /Image /Width 10001 /Height 1 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode", hugeJPEG)})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(tc.data))
			require.Error(t, err)
			require.Empty(t, pages)
		})
	}
}

func TestPDFPageAcceptsICCBasedRasters(t *testing.T) {
	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	_, err := zw.Write(bytes.Repeat([]byte{0x11, 0x22, 0x33}, 8*8))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	raster := testImageObject("/Type /XObject /Subtype /Image /Width 8 /Height 8 /BitsPerComponent 8 /ColorSpace [/ICCBased 6 0 R] /Filter /FlateDecode", compressed.Bytes())
	profile := []byte("<< /N 3 /Length 4 >>\nstream\n1234\nendstream")
	data := testutil.PDFWithPageResources([]string{"q 100 0 0 100 0 0 cm /Raster Do Q"}, "/Raster 5 0 R", [][]byte{raster, profile})
	pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, pages)
}

func TestPDFJPEGRepeatedPaintReadsOnce(t *testing.T) {
	// Build a valid JPEG >= 70 KiB so 1,000 uncached reads would exceed the 64 MiB source budget.
	img := image.NewRGBA(image.Rect(0, 0, 512, 512))
	var state uint32 = 0x12345678
	for y := range 512 {
		for x := range 512 {
			state = state*1664525 + 1013904223
			r := uint8((state >> 24) & 0xff)
			state = state*1664525 + 1013904223
			g := uint8((state >> 24) & 0xff)
			state = state*1664525 + 1013904223
			b := uint8((state >> 24) & 0xff)
			img.SetRGBA(x, y, color.RGBA{R: r, G: g, B: b, A: 255})
		}
	}
	var jpegBuf bytes.Buffer
	require.NoError(t, jpeg.Encode(&jpegBuf, img, &jpeg.Options{Quality: 95}))
	jpegData := jpegBuf.Bytes()
	require.GreaterOrEqual(t, len(jpegData), 70<<10)

	photo := testImageObject("/Type /XObject /Subtype /Image /Width 512 /Height 512 /BitsPerComponent 8 /ColorSpace /DeviceRGB /Filter /DCTDecode", jpegData)
	streams := make([]string, domain.MaxPDFPages)
	for i := range streams {
		streams[i] = "q 100 0 0 100 0 0 cm /Photo Do Q"
	}
	data := testutil.PDFWithPageResources(streams, "/Photo 5 0 R", [][]byte{photo})
	pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Len(t, pages, domain.MaxPDFPages)
	for i, page := range pages {
		require.Equal(t, i+1, page.Number)
		require.Equal(t, domain.PDFImage, page.Evidence)
		require.Empty(t, page.Text)
	}
}
