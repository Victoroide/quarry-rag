package domain

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodePDFPagesRejectsInvalidManifest(t *testing.T) {
	for _, input := range []string{
		`null`, `[]`,
		`[{"number":2,"evidence":"blank"}]`,
		`[{"number":1,"evidence":"blank"},{"number":1,"evidence":"image"}]`,
		`[{"number":1,"evidence":"imaginary"}]`,
		`[{"number":1,"evidence":"blank","text":"fabricated"}]`,
		`[{"number":1,"evidence":"embedded"}]`,
		`[{"number":1,"evidence":"sparse","text":"contains\u0000nul"}]`,
	} {
		_, err := DecodePDFPages([]byte(input))
		require.Error(t, err, input)
	}
}

func TestPDFTextReadableAcrossScripts(t *testing.T) {
	require.True(t, PDFTextReadable("Latin and Ελληνικά"))
	require.True(t, PDFTextReadable("報告書本文"))
	require.False(t, PDFTextReadable("123456789012"))
	require.False(t, PDFTextReadable("Text with an undecoded glyph �"))
	for _, text := range []string{"Page 12 describes 40 leases.", "Civic records from 1840", "Mix flour and water.", "Page civil"} {
		require.True(t, PDFTextReadable(text), text)
	}
}

func FuzzDecodePDFPages(f *testing.F) {
	f.Add([]byte(`[{"number":1,"evidence":"blank"},{"number":2,"evidence":"mixed","text":"2"}]`))
	f.Add([]byte(`[{"number":-1,"evidence":"missing"}]`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, input []byte) {
		pages, err := DecodePDFPages(input)
		if err != nil {
			return
		}
		require.LessOrEqual(t, len(pages.Text()), MaxContentBytes)
		encoded, err := json.Marshal(pages)
		require.NoError(t, err)
		decoded, err := DecodePDFPages(encoded)
		require.NoError(t, err)
		require.Equal(t, pages, decoded)
	})
}
