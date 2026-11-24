package api

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIngestIdentityReference(t *testing.T) {
	api, err := os.ReadFile("../../docs/api.md")
	require.NoError(t, err)
	for _, section := range []string{"#### Variant A", "#### Variant B"} {
		variant := ingestDocSection(t, string(api), section, "\n#### ")
		require.Regexp(t, "(?m)^\\| `document_id` \\|[^\\n]*\\(#document-identity\\)", variant)
	}
	readme, err := os.ReadFile("../../README.md")
	require.NoError(t, err)
	limitation := ingestDocSection(t, string(readme), "- **Content deduplication", "\n")
	require.Contains(t, limitation, "(docs/api.md#document-identity)")
	require.Contains(t, limitation, "registered for shared matching")
}

func TestIngestSourceStorageReference(t *testing.T) {
	api, err := os.ReadFile("../../docs/api.md")
	require.NoError(t, err)
	multipart := ingestDocSection(t, string(api), "#### Variant B", "Error Status Codes:")
	require.Contains(t, multipart, "best-effort")
	require.Contains(t, multipart, "confirmed")
	require.Contains(t, multipart, "uncertain")
	ocr := ingestDocSection(t, string(api), "#### Image-only PDF OCR", "\n---")
	require.Regexp(t, `Newly accepted[^.]*202`, ocr)
	require.Contains(t, ocr, "(#document-identity)")
}

func TestDOCReingestRefersToOmittedIDPolicy(t *testing.T) {
	api, err := os.ReadFile("../../docs/api.md")
	require.NoError(t, err)
	section := ingestDocSection(t, string(api), "#### Word binary extraction", "\n#### ")
	require.Regexp(t, "For uploads that omit `document_id`, see\\s+\\[document identity\\]\\(#document-identity\\)", section)
	require.NotContains(t, section, "to use collection matching")
}

func ingestDocSection(t *testing.T, doc, start, end string) string {
	t.Helper()
	_, section, found := strings.Cut(doc, start)
	require.True(t, found, "missing section %q", start)
	section, _, _ = strings.Cut(section, end)
	return section
}
