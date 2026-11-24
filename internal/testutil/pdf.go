// Package testutil supplies synthetic document fixtures.
package testutil

import (
	"bytes"
	"fmt"
	"strings"
)

// PDFWithPageStreams creates a synthetic PDF with WinAnsi text and an image resource.
func PDFWithPageStreams(streams ...string) []byte {
	return PDFWithPageResources(streams, "", nil)
}

// PDFWithPageResources adds named XObjects after the standard scan image.
func PDFWithPageResources(streams []string, xobjects string, extras [][]byte) []byte {
	return pdfWithPageResources(streams, xobjects, extras, "")
}

// PDFWithPageAnnotation adds the supplied annotation dictionary to page two.
func PDFWithPageAnnotation(annotation string, streams ...string) []byte {
	return pdfWithPageResources(streams, "", nil, annotation)
}

func pdfWithPageResources(streams []string, xobjects string, extras [][]byte, annotation string) []byte {
	objects := [][]byte{
		[]byte("<< /Type /Catalog /Pages 2 0 R >>"), nil,
		[]byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"),
		[]byte("<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceGray /BitsPerComponent 8 /Length 1 >>\nstream\n\x00\nendstream"),
	}
	objects = append(objects, extras...)
	var kids strings.Builder
	for i, stream := range streams {
		annotations := ""
		if i == 1 && annotation != "" {
			objects = append(objects, []byte(annotation))
			annotations = fmt.Sprintf(" /Annots [%d 0 R]", len(objects))
		}
		page, contents := len(objects)+1, len(objects)+2
		fmt.Fprintf(&kids, "%d 0 R ", page)
		objects = append(objects,
			[]byte(fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents %d 0 R /Resources << /Font << /F1 3 0 R >> /XObject << /Scan 4 0 R %s >> >>%s >>", contents, xobjects, annotations)),
			[]byte(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(stream), stream)))
	}
	objects[1] = []byte(fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", kids.String(), len(streams)))
	return PDFWithObjects(objects)
}

// PDFWithObjects adds a standard xref table to synthetic PDF objects.
func PDFWithObjects(objects [][]byte) []byte {
	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n")
	offsets := make([]int, len(objects))
	for i, object := range objects {
		offsets[i] = out.Len()
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", i+1, object)
	}
	xref := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for _, offset := range offsets {
		fmt.Fprintf(&out, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xref)
	return out.Bytes()
}
