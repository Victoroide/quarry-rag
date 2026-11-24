package extractor_test

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/stretchr/testify/require"
)

func generatedPDF(t *testing.T, stream string, compressed bool, unicodeMap ...string) []byte {
	t.Helper()
	data := []byte(stream)
	filter := ""
	if compressed {
		var encoded bytes.Buffer
		writer := zlib.NewWriter(&encoded)
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		data = encoded.Bytes()
		filter = " /Filter /FlateDecode"
	}
	objects := [][]byte{
		[]byte("<< /Type /Catalog /Pages 2 0 R >>"),
		[]byte("<< /Type /Pages /Kids [3 0 R] /Count 1 >>"),
		[]byte("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>"),
		append(append([]byte(fmt.Sprintf("<< /Length %d%s >>\nstream\n", len(data), filter)), data...), []byte("\nendstream")...),
		[]byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"),
	}
	if len(unicodeMap) > 0 {
		objects[4] = []byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /ToUnicode 6 0 R >>")
		objects = append(objects, []byte(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(unicodeMap[0]), unicodeMap[0])))
	}
	return testutil.PDFWithObjects(objects)
}

func TestPDFPageManifest(t *testing.T) {
	const scan = "q 612 0 0 792 0 0 cm /Scan Do Q"
	const header = `BT /F1 12 Tf (Archive report) Tj ET`
	data := testutil.PDFWithPageStreams(scan, header+` BT /F1 12 Tf (English and caf\351 fran\347ais) Tj ET`, scan,
		"", header, scan, scan+` BT /F1 12 Tf (7) Tj ET`,
		scan+` BT /F1 12 Tf (DRAFT watermark) Tj ET`, `BT /F1 12 Tf (9) Tj ET`)
	pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, domain.PDFPages{
		{Number: 1, Evidence: domain.PDFImage},
		{Number: 2, Evidence: domain.PDFEmbedded, Text: "Archive report\nEnglish and café français"},
		{Number: 3, Evidence: domain.PDFImage},
		{Number: 4, Evidence: domain.PDFBlank},
		{Number: 5, Evidence: domain.PDFEmbedded, Text: "Archive report"},
		{Number: 6, Evidence: domain.PDFImage},
		{Number: 7, Evidence: domain.PDFMixed, Text: "7"},
		{Number: 8, Evidence: domain.PDFMixed, Text: "DRAFT watermark"},
		{Number: 9, Evidence: domain.PDFSparse, Text: "9"},
	}, pages)
	legacy, err := extractor.New(extractor.FileTypePDF).Extract(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, legacy, pages.Text())
}

func TestPDFPageManifestSeparatesEmptyAndUndecodablePages(t *testing.T) {
	data := testutil.PDFWithPageStreams("", `BT /F1 12 Tf (\001) Tj ET`)
	pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, domain.PDFBlank, pages[0].Evidence)
	require.Equal(t, domain.PDFUnreadable, pages[1].Evidence)
	require.True(t, pages.NeedsOCR())
}

func TestPDFPageKeepsReadableTextWhenAnotherRunIsUndecodable(t *testing.T) {
	data := testutil.PDFWithPageStreams(`BT /F1 12 Tf (Readable heading) Tj ET BT /F1 12 Tf (\001) Tj ET`)
	pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFUnreadable, Text: "Readable heading"}}, pages)
}

func TestPDFPageReadsDigitalLayoutAndQuoteOperator(t *testing.T) {
	const heading = `BT /F1 12 Tf (Archive report) Tj ET`
	const form = `BT /F1 12 Tf (Archive report) Tj ET`
	data := testutil.PDFWithPageResources([]string{
		heading + ` 36 600 m 576 600 l S`, `/Heading Do`,
		`BT /F1 12 Tf 14 TL (Archive report) Tj 0 0 (Second line) " ET`,
	}, `/Heading 5 0 R`, [][]byte{
		[]byte(fmt.Sprintf("<< /Type /XObject /Subtype /Form /BBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Length %d >>\nstream\n%s\nendstream", len(form), form)),
	})
	pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, domain.PDFPages{
		{Number: 1, Evidence: domain.PDFEmbedded, Text: "Archive report"},
		{Number: 2, Evidence: domain.PDFEmbedded, Text: "Archive report"},
		{Number: 3, Evidence: domain.PDFEmbedded, Text: "Archive report\nSecond line"},
	}, pages)
	require.False(t, pages.NeedsOCR())
}

func TestPDFFormInheritsInvokingFont(t *testing.T) {
	const form = `BT 36 700 Td (Inherited archive heading) Tj ET`
	data := testutil.PDFWithPageResources([]string{`BT /F1 12 Tf ET /Heading Do`}, `/Heading 5 0 R`, [][]byte{
		[]byte(fmt.Sprintf("<< /Type /XObject /Subtype /Form /BBox [0 0 612 792] /Matrix [1 0 0 1 4 6] /Length %d >>\nstream\n%s\nendstream", len(form), form)),
	})
	pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Inherited archive heading"}}, pages)
	require.False(t, pages.NeedsOCR())
}

func TestPDFFormAppliesMatrixToLayoutRule(t *testing.T) {
	const form = `10 w 0 0 m 2000 0 l S BT 36 700 Td (Inherited archive heading) Tj ET`
	data := testutil.PDFWithPageResources([]string{`BT /F1 12 Tf ET /Heading Do BT (Outside the form) Tj ET`}, `/Heading 5 0 R`, [][]byte{
		[]byte(fmt.Sprintf("<< /Type /XObject /Subtype /Form /BBox [0 0 2000 792] /Matrix [0.1 0 0 0.1 4 6] /Length %d >>\nstream\n%s\nendstream", len(form), form)),
	})
	pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Inherited archive heading\nOutside the form"}}, pages)
}

// pdfWithGraphicsStates builds a one-page document whose page resources carry
// the supplied graphics-state dictionaries and a form that inherits them.
func pdfWithGraphicsStates(content, states, form string, extras ...[]byte) []byte {
	objects := [][]byte{
		[]byte("<< /Type /Catalog /Pages 2 0 R >>"),
		[]byte("<< /Type /Pages /Kids [4 0 R] /Count 1 >>"),
		[]byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"),
		[]byte(fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 5 0 R /Resources << /Font << /F1 3 0 R >> /ExtGState << %s >> /XObject << /Form 6 0 R >> >> >>", states)),
		[]byte(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(content), content)),
		[]byte(fmt.Sprintf("<< /Type /XObject /Subtype /Form /BBox [0 0 612 792] /Length %d >>\nstream\n%s\nendstream", len(form), form)),
	}
	return testutil.PDFWithObjects(append(objects, extras...))
}

func TestPDFGraphicsStateFontSelectsEmbeddedText(t *testing.T) {
	const heading = "Archived digital heading"
	const states = `/GS1 << /Type /ExtGState /Font [3 0 R 12] >> /GS0 << /Type /ExtGState /CA 0.5 /ca 0.5 >>`
	formStream := `q /GS1 gs BT 1 0 0 1 30 60 Tm (Archived digital heading) Tj ET Q`
	ownResources := testutil.PDFWithPageResources([]string{"/Heading Do"}, "/Heading 5 0 R", [][]byte{
		[]byte(fmt.Sprintf("<< /Type /XObject /Subtype /Form /BBox [0 0 500 500] /Resources << /ExtGState << /GS1 << /Type /ExtGState /Font [3 0 R 12] >> >> >> /Length %d >>\nstream\n%s\nendstream", len(formStream), formStream)),
	})
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"form resources", ownResources},
		{"page content", pdfWithGraphicsStates(`q /GS1 gs BT 30 60 Td (Archived digital heading) Tj ET Q`, states, "")},
		{"inherited by a form", pdfWithGraphicsStates(`q /GS1 gs /Form Do Q`, states, `BT 30 60 Td (Archived digital heading) Tj ET`)},
		{"unrelated states", pdfWithGraphicsStates(`/GS0 gs BT /F1 12 Tf 30 60 Td (Archived digital heading) Tj ET`, states, "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(tc.data))
			require.NoError(t, err)
			require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: heading}}, pages)
			require.False(t, pages.NeedsOCR())
		})
	}
}

func TestPDFGraphicsStateFontFollowsGraphicsStateStack(t *testing.T) {
	const states = `/GS1 << /Type /ExtGState /Font [3 0 R 12] >>`
	for _, tc := range []struct {
		name, content, form, text string
	}{
		{"restored by Q", `q /GS1 gs BT (Inside) Tj ET Q BT (Outside) Tj ET`, "", "Inside"},
		{"not leaked by a form", `/Form Do BT (Outside) Tj ET`, `/GS1 gs BT (Inside) Tj ET`, "Inside"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(pdfWithGraphicsStates(tc.content, states, tc.form)))
			require.NoError(t, err)
			require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFUnreadable, Text: tc.text}}, pages)
			require.True(t, pages.NeedsOCR())
		})
	}
}

func TestPDFGraphicsStateLineWidthClassifiesStrokes(t *testing.T) {
	const rule = ` 36 600 m 576 600 l S`
	const heading = `BT /F1 12 Tf (Archive heading) Tj ET `
	const states = `/Thin << /Type /ExtGState /LW 1 >> /Thick << /Type /ExtGState /LW 20 >>`
	for _, tc := range []struct {
		name, content string
		evidence      domain.PDFPageEvidence
	}{
		{"thin rule", heading + `/Thin gs` + rule, domain.PDFEmbedded},
		{"thick stroke", heading + `/Thick gs` + rule, domain.PDFMixed},
		{"restored width", heading + `q /Thick gs Q` + rule, domain.PDFEmbedded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(pdfWithGraphicsStates(tc.content, states, "")))
			require.NoError(t, err)
			require.Len(t, pages, 1)
			require.Equal(t, tc.evidence, pages[0].Evidence)
		})
	}
}

func TestPDFGraphicsStateRejectsInvalidDefinitions(t *testing.T) {
	differences := "/Differences [0" + strings.Repeat(" /a", 513) + "]"
	oversizedEncoding := []byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding << /Type /Encoding " + differences + " >> >>")
	const mapping = "1 begincodespacerange <00> <ff> endcodespacerange\n10000 beginbfchar endbfchar"
	oversizedMap := []byte(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(mapping), mapping))
	const show = ` BT 30 60 Td (Archived digital heading) Tj ET`
	for _, tc := range []struct {
		name, content, states string
		extras                [][]byte
	}{
		{"missing state", `/Missing gs` + show, `/GS1 << /Font [3 0 R 12] >>`, nil},
		{"state is not a dictionary", `/GS1 gs` + show, `/GS1 12`, nil},
		{"operand is not a name", `1 gs` + show, `/GS1 << /Font [3 0 R 12] >>`, nil},
		{"font without a size", `/GS1 gs` + show, `/GS1 << /Font [3 0 R] >>`, nil},
		{"font with a name size", `/GS1 gs` + show, `/GS1 << /Font [3 0 R /Big] >>`, nil},
		{"font is not an object", `/GS1 gs` + show, `/GS1 << /Font [(font) 12] >>`, nil},
		{"font entry is not an array", `/GS1 gs` + show, `/GS1 << /Font 3 0 R >>`, nil},
		{"line width is not a number", `/GS1 gs` + show, `/GS1 << /LW /Wide >>`, nil},
		{"oversized encoding differences", `/GS1 gs` + show, `/GS1 << /Font [7 0 R 12] >>`, [][]byte{oversizedEncoding}},
		{"oversized unicode map", `/GS1 gs` + show, `/GS1 << /Font [7 0 R 12] >>`, [][]byte{
			[]byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /ToUnicode 8 0 R >>"), oversizedMap,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(pdfWithGraphicsStates(tc.content, tc.states, "", tc.extras...)))
			require.Error(t, err)
		})
	}
}

func compressedPDFRaster(t testing.TB) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	_, err := writer.Write(bytes.Repeat([]byte{0x77}, 16*16))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

func pdfWithFlateRaster(params string, payload []byte) []byte {
	header := fmt.Sprintf("<< /Type /XObject /Subtype /Image /Width 16 /Height 16 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter /FlateDecode /Length %d %s >>\nstream\n", len(payload), params)
	obj := append(append([]byte(header), payload...), []byte("\nendstream")...)
	return testutil.PDFWithPageResources([]string{`BT /F1 12 Tf (Archive heading) Tj ET`, `q 612 0 0 792 0 0 cm /Raster Do Q`}, `/Raster 5 0 R`, [][]byte{obj})
}

func TestPDFImageAcceptsExplicitDefaultFlatePredictor(t *testing.T) {
	compressed := compressedPDFRaster(t)
	for _, tc := range []struct {
		name, params string
		payload      []byte
		valid        bool
	}{
		{"implicit", "", compressed, true},
		{"explicit", "/DecodeParms << /Predictor 1 >>", compressed, true},
		{"truncated", "/DecodeParms << /Predictor 1 >>", compressed[:len(compressed)-3], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := pdfWithFlateRaster(tc.params, tc.payload)
			pages, readErr := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
			if !tc.valid {
				require.Error(t, readErr)
				require.Empty(t, pages)
				return
			}
			require.NoError(t, readErr)
			require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Archive heading"}, {Number: 2, Evidence: domain.PDFImage}}, pages)
		})
	}
}

func FuzzPDFImageDefaultFlatePredictor(f *testing.F) {
	compressed := compressedPDFRaster(f)
	f.Add(compressed)
	f.Add(compressed[:len(compressed)-3])
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 64<<10 {
			t.Skip()
		}
		implicit, implicitErr := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(pdfWithFlateRaster("", payload)))
		explicit, explicitErr := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(pdfWithFlateRaster("/DecodeParms << /Predictor 1 >>", payload)))
		require.Equal(t, implicitErr == nil, explicitErr == nil)
		require.Equal(t, implicit, explicit)
	})
}

func TestPDFPageRejectsDamagedRasterAndMalformedQuote(t *testing.T) {
	valid := testutil.PDFWithPageStreams(`q 612 0 0 792 0 0 cm /Scan Do Q`)
	pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(valid))
	require.NoError(t, err)
	require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}}, pages)
	for _, damaged := range [][]byte{
		testutil.PDFWithPageResources([]string{`/Broken Do`}, `/Broken 5 0 R`, [][]byte{[]byte("<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceGray /BitsPerComponent 8 /Length 0 >>\nstream\n\nendstream")}),
		testutil.PDFWithPageStreams(`BT /F1 12 Tf 0 (Wrong count) " ET`),
		testutil.PDFWithPageStreams(`BT /F1 12 Tf /Bad 0 (Wrong type) " ET`),
	} {
		_, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(damaged))
		require.Error(t, err)
	}
}

func TestPDFLegacyUTF8BoundaryAndPageEvidence(t *testing.T) {
	for _, tc := range []struct {
		text string
		size int
	}{
		{`caf\351 abc`, 9},
		{`caf\351 abcd`, 10},
	} {
		data := testutil.PDFWithPageStreams(`BT /F1 12 Tf (  ` + tc.text + `  ) Tj ET`)
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.NoError(t, err)
		require.Len(t, pages.Text(), tc.size)
		legacy, err := extractor.New(extractor.FileTypePDF).Extract(t.Context(), bytes.NewReader(data))
		if tc.size == 9 {
			require.ErrorIs(t, err, extractor.ErrNoText)
		} else {
			require.NoError(t, err)
			require.Equal(t, pages.Text(), legacy)
		}
	}
}

func TestPDFPageManifestRejectsUnsafeStructures(t *testing.T) {
	for _, data := range [][]byte{
		[]byte("%PDF-1.4\nmalformed"),
		testutil.PDFWithPageStreams("BT /F1 Tf (text) Tj ET"),
		testutil.PDFWithPageStreams("/Missing Do"),
		testutil.PDFWithPageStreams("BI /W 1 /H 1 ID"),
		generatedPDF(t, strings.Repeat(" ", 9<<20), true),
		generatedPDF(t, "BT /F1 12 Tf (text) Tj ET", false, "10000 beginbfchar endbfchar"),
		testutil.PDFWithPageStreams("BT /F1 12 Tf (" + strings.Repeat("x", domain.MaxContentBytes+1) + ") Tj ET"),
		testutil.PDFWithPageStreams(make([]string, domain.MaxPDFPages+1)...),
	} {
		pages, err := extractor.ExtractPDFPages(t.Context(), bytes.NewReader(data))
		require.Error(t, err)
		require.Empty(t, pages)
	}
}
func TestPDFRejectsExtractedTextAboveContentLimit(t *testing.T) {
	data := generatedPDF(t, "BT /F1 12 Tf ("+strings.Repeat("x", 1_000_001)+") Tj ET", false)
	_, err := extractor.New(extractor.FileTypePDF).Extract(t.Context(), bytes.NewReader(data))
	if err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("oversized extracted text: %v", err)
	}
}
func TestPDFBoundsInflatedContentBeforeParsing(t *testing.T) {
	data := generatedPDF(t, strings.Repeat(" ", 9<<20), true)
	_, err := extractor.New(extractor.FileTypePDF).Extract(t.Context(), bytes.NewReader(data))
	if err == nil || !strings.Contains(err.Error(), "maximum decoded") {
		t.Fatalf("oversized decoded stream: %v", err)
	}
}

func TestPDFContentLimitReturnsExtractionError(t *testing.T) {
	data := generatedPDF(t, "BT /F1 12 Tf ("+strings.Repeat("x", 1_000_001)+") Tj ET", true)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "archive.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if _, writeErr := part.Write(data); writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()
	handler.IngestHandler(nil, nil, nil, 20<<20)(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unexpected status: %d %s", rec.Code, rec.Body)
	}
	var response handler.APIError
	if decodeErr := json.Unmarshal(rec.Body.Bytes(), &response); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if response.Error != "extraction" {
		t.Fatalf("unexpected error class: %+v", response)
	}
}

func TestPDFFontMapWorkIsBounded(t *testing.T) {
	mapping := "1 begincodespacerange <00> <ff> endcodespacerange\n10000 beginbfchar endbfchar"
	data := generatedPDF(t, "BT /F1 12 Tf (Archive operating hours.) Tj ET", false, mapping)
	_, err := extractor.New(extractor.FileTypePDF).Extract(t.Context(), bytes.NewReader(data))
	if err == nil || !strings.Contains(err.Error(), "maximum font map") {
		t.Fatalf("excessive mapping work accepted: %v", err)
	}
}
