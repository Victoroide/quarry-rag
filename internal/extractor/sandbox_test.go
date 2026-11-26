package extractor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/Victoroide/quarry-rag/internal/testutil/docxtest"
	"github.com/stretchr/testify/require"
)

const docReadTestExecutable = "quarry-doc-reader"

func TestMain(m *testing.M) {
	childHooks = map[FileType]func() (string, error){
		"stderr": func() (string, error) {
			_, _ = io.WriteString(os.Stderr, "synthetic-extraction-child-diagnostic")
			os.Exit(1)
			return "", nil
		},
		"sleep": func() (string, error) { time.Sleep(time.Minute); return "late", nil },
		"env":   func() (string, error) { return strings.Join(os.Environ(), "\n"), nil },
		"huge":  func() (string, error) { return strings.Repeat("x", 9<<20), nil },
	}
	if len(os.Args) == 2 && os.Args[1] == ChildArg && os.Getenv(ChildEnv) == "1" {
		if filepath.Base(os.Args[0]) == docReadTestExecutable {
			signal := filepath.Join(filepath.Dir(os.Args[0]), "reading")
			os.Exit(RunChild(&signaledDOCReader{reader: bufio.NewReader(os.Stdin), signal: signal}, os.Stdout))
		}
		os.Exit(RunChild(os.Stdin, os.Stdout))
	}
	os.Exit(m.Run())
}

type signaledDOCReader struct {
	reader *bufio.Reader
	signal string
	header bool
}

func (r *signaledDOCReader) Read(data []byte) (int, error) {
	if !r.header {
		r.header = true
		header, err := r.reader.ReadSlice('\n')
		return copy(data, header), err
	}
	if err := os.WriteFile(r.signal, nil, 0o600); err != nil {
		return 0, err
	}
	time.Sleep(time.Minute) // The caller cancels while the child is reading the source.
	return r.reader.Read(data)
}

func TestSandboxDOCCancelsDuringSourceRead(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ready := filepath.Join(dir, "reading")
	shim := filepath.Join(dir, docReadTestExecutable)
	if linkErr := os.Symlink(self, shim); linkErr != nil {
		t.Fatal(linkErr)
	}
	sb, err := NewSandbox(SandboxConfig{Executable: shim, Timeout: 15 * time.Second, DOCStories: mustDOCStories(t, "footnotes")})
	if err != nil {
		t.Fatal(err)
	}
	source := docFixture(t, docCompressedPiece(t, "Body\r"))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type extraction struct {
		text string
		err  error
	}
	done := make(chan extraction, 1)
	go func() {
		text, err := sb.Extract(ctx, FileTypeDOC, source)
		done <- extraction{text, err}
	}()
	limit := time.NewTimer(8 * time.Second)
	defer limit.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case result := <-done:
			t.Fatalf("child returned before cancellation: %+v", result)
		case <-limit.C:
			t.Fatal("child did not begin reading DOC source")
		case <-tick.C:
			if _, err := os.Stat(ready); err == nil {
				cancel()
				select {
				case result := <-done:
					if result.text != "" || !errors.Is(result.err, context.Canceled) {
						t.Fatalf("canceled child returned %+v", result)
					}
				case <-limit.C:
					t.Fatal("canceled child did not exit")
				}
				return
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
	}
}

func sandboxPDF(pages string) []byte {
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 " + pages + " >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
		"<< /Length 48 >>\nstream\nBT /F1 12 Tf (Archive operating hours.) Tj ET\nendstream",
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
	}
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

func uploadSandbox(t *testing.T, timeout time.Duration) *Sandbox {
	t.Helper()
	sb, err := NewSandbox(SandboxConfig{Timeout: timeout, MemoryLimitMB: 512})
	if err != nil {
		t.Fatal(err)
	}
	return sb
}

func TestSandboxExtractsPDF(t *testing.T) {
	text, err := uploadSandbox(t, 5*time.Second).Extract(t.Context(), FileTypePDF, sandboxPDF(""))
	if err != nil || !strings.Contains(text, "Archive operating hours.") {
		t.Fatalf("PDF extraction: text=%q error=%v", text, err)
	}
}

func TestSandboxUsesDOCXStoryProfile(t *testing.T) {
	data := docxPartsBytes(t, storyFixture(docxStrictNamespace, testDOCXStrictNS))
	for _, list := range []string{"", "headers,comments"} {
		t.Run(list, func(t *testing.T) {
			profile := storyProfile(t, list)
			sb, err := NewSandbox(SandboxConfig{Timeout: 5 * time.Second, DOCXStories: profile})
			if err != nil {
				t.Fatal(err)
			}
			got, err := sb.Extract(t.Context(), FileTypeDOCX, data)
			if err != nil {
				t.Fatal(err)
			}
			want, err := NewDOCX(profile).Extract(t.Context(), bytes.NewReader(data))
			if err != nil || got != want {
				t.Fatalf("child got %q, want %q, error %v", got, want, err)
			}
			if _, err := sb.Extract(t.Context(), FileTypePDF, sandboxPDF("")); err != nil {
				t.Fatalf("DOCX settings interfered with PDF extraction: %v", err)
			}
		})
	}
}

func TestSandboxExtractsPDFPages(t *testing.T) {
	sb, err := NewSandbox(SandboxConfig{Timeout: 5 * time.Second, MemoryLimitMB: 512, DOCStories: mustDOCStories(t, "footnotes")})
	require.NoError(t, err)
	pages, err := sb.ExtractPDFPages(t.Context(), sandboxPDF(""))
	if err != nil || len(pages) != 1 || pages[0].Number != 1 || pages.Text() != "Archive operating hours." {
		t.Fatalf("PDF pages: %+v, %v", pages, err)
	}
}

func TestSandboxInspectsJPEGScanPages(t *testing.T) {
	var scan bytes.Buffer
	require.NoError(t, jpeg.Encode(&scan, image.NewGray(image.Rect(0, 0, 12, 12)), nil))
	raster := fmt.Appendf(nil, "<< /Type /XObject /Subtype /Image /Width 12 /Height 12 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode /Length %d >>\nstream\n", scan.Len())
	raster = append(raster, scan.Bytes()...)
	raster = append(raster, "\nendstream"...)
	data := testutil.PDFWithPageResources([]string{
		`BT /F1 12 Tf (Archive operating hours.) Tj ET`,
		`q 180 0 0 180 10 10 cm /Photo Do Q`,
	}, "/Photo 5 0 R", [][]byte{raster})
	pages, err := uploadSandbox(t, 5*time.Second).ExtractPDFPages(t.Context(), data)
	require.NoError(t, err)
	require.Equal(t, domain.PDFPages{
		{Number: 1, Evidence: domain.PDFEmbedded, Text: "Archive operating hours."},
		{Number: 2, Evidence: domain.PDFImage},
	}, pages)
}

func TestSandboxNormalizesPDFBeforeClassifyingPage(t *testing.T) {
	data := testutil.PDFWithPageStreams(`BT /F1 12 Tf (ab) Tj ET BT ET BT ET BT ET BT ET BT ET BT /F1 12 Tf (cd) Tj ET`)
	pages, err := uploadSandbox(t, 5*time.Second).ExtractPDFPages(t.Context(), data)
	require.NoError(t, err)
	require.Equal(t, domain.PDFPages{{Number: 1, Evidence: domain.PDFSparse, Text: "ab\n\ncd"}}, pages)
}

func TestSandboxSurvivesParserStackOverflow(t *testing.T) {
	sb := uploadSandbox(t, 15*time.Second)
	data := sandboxPDF("/Bomb " + strings.Repeat("[", 6_000_000) + strings.Repeat("]", 6_000_000))
	start := time.Now()
	if _, err := sb.Extract(t.Context(), FileTypePDF, data); !errors.Is(err, ErrExtractionFailed) {
		t.Fatalf("malformed PDF: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("malformed PDF took %s", elapsed)
	}
	if _, err := sb.Extract(t.Context(), FileTypePDF, sandboxPDF("")); err != nil {
		t.Fatalf("extraction after child crash: %v", err)
	}
}

func TestSandboxEnforcesTimeout(t *testing.T) {
	start := time.Now()
	_, err := uploadSandbox(t, 200*time.Millisecond).Extract(t.Context(), "sleep", nil)
	if !errors.Is(err, ErrExtractionTimeout) {
		t.Fatalf("timeout: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("child was not terminated promptly: %s", elapsed)
	}
}

func TestSandboxScrubsEnvironment(t *testing.T) {
	t.Setenv("SERVICE_TOKEN", "x")
	t.Setenv("AWS_ACCESS_KEY_ID", "x")
	t.Setenv("DATABASE_URL", "x")
	text, err := uploadSandbox(t, 5*time.Second).Extract(t.Context(), "env", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range strings.Split(text, "\n") {
		if strings.HasPrefix(entry, "SERVICE_TOKEN=") || strings.HasPrefix(entry, "AWS_") || strings.HasPrefix(entry, "DATABASE_URL=") {
			t.Fatalf("inherited environment entry: %q", entry)
		}
	}
	for _, entry := range []string{"QUARRY_EXTRACT_CHILD=1", "GOMAXPROCS=1", "GOMEMLIMIT=512MiB"} {
		if !strings.Contains(text, entry) {
			t.Fatalf("missing child setting %q in %q", entry, text)
		}
	}
}

func TestSandboxBoundsOutput(t *testing.T) {
	_, err := uploadSandbox(t, 5*time.Second).Extract(t.Context(), "huge", nil)
	if !errors.Is(err, ErrExtractionFailed) {
		t.Fatalf("oversized output: %v", err)
	}
}

func TestLimiterRejectsWhenBusy(t *testing.T) {
	limiter := NewLimiter(1, 10*time.Millisecond)
	release, err := limiter.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, acquireErr := limiter.Acquire(t.Context()); !errors.Is(acquireErr, ErrBusy) {
		t.Fatalf("occupied limiter: %v", acquireErr)
	}
	release()
	release()
	release, err = limiter.Acquire(t.Context())
	if err != nil {
		t.Fatalf("released limiter: %v", err)
	}
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := limiter.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquisition: %v", err)
	}
}

func TestRunChildRejectsInvalidInput(t *testing.T) {
	for _, input := range []string{
		strings.Repeat("x", 1025) + "\n",
		"{bad}\n",
		`{"type":"unknown","timeout_ms":1000,"max_stack":67108864}` + "\n",
		`{"type":"pdf","timeout_ms":0,"max_stack":67108864}` + "\n",
	} {
		var out bytes.Buffer
		if code := RunChild(strings.NewReader(input), &out); code == 0 && !strings.Contains(out.String(), `"error"`) {
			t.Fatalf("invalid input accepted: code=%d output=%s", code, &out)
		}
	}
	for _, tc := range []struct {
		header string
		source []byte
	}{
		{`{"type":"doc","timeout_ms":1000,"max_stack":67108864,"doc_stories":32}` + "\n", docFixture(t, docCompressedPiece(t, "Body\r"))},
		{`{"type":"pdf","timeout_ms":1000,"max_stack":67108864,"doc_stories":1}` + "\n", sandboxPDF("")},
	} {
		var out bytes.Buffer
		code := RunChild(bytes.NewReader(append([]byte(tc.header), tc.source...)), &out)
		var response childResponse
		if err := json.Unmarshal(out.Bytes(), &response); err != nil || code != 0 || response.Error != "extractor: invalid DOC stories" || response.Text != nil {
			t.Fatalf("invalid story mask: code=%d response=%s decode=%v", code, &out, err)
		}
	}
}

func TestRunChildValidatesDOCXStoryMaskWithValidPayload(t *testing.T) {
	docx := docxBytes(t, docxtest.Document(docxWordNamespace, testDOCXOfficeNS, docxtest.Paragraph("Readable source text.")))
	pdf := sandboxPDF("")
	for _, tt := range []struct {
		name   string
		header childHeader
		data   []byte
		want   string
	}{
		{"unknown DOCX mask bits", childHeader{Type: FileTypeDOCX, DOCXStories: 255}, docx, ""},
		{"DOCX mask on PDF", childHeader{Type: FileTypePDF, DOCXStories: docxBody}, pdf, ""},
		{"valid DOCX mask", childHeader{Type: FileTypeDOCX, DOCXStories: docxBody}, docx, "Readable source text."},
		{"valid PDF control", childHeader{Type: FileTypePDF}, pdf, "Archive operating hours."},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.header.TimeoutMS, tt.header.MaxStack = 5000, childMaxStack
			header, err := json.Marshal(tt.header)
			if err != nil {
				t.Fatal(err)
			}
			input := bytes.NewReader(append(append(header, '\n'), tt.data...))
			var out bytes.Buffer
			if code := RunChild(input, &out); code != 0 {
				t.Fatalf("child exited with %d: %s", code, &out)
			}
			var response childResponse
			if err := json.Unmarshal(out.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if tt.want == "" {
				if response.Error != "extractor: invalid extraction header" || response.Text != nil {
					t.Fatalf("expected only the invalid-header error, got %s", &out)
				}
			} else if response.Error != "" || response.Text == nil || *response.Text != tt.want {
				t.Fatalf("expected successful extraction of %q, got %s", tt.want, &out)
			}
		})
	}
}

func TestDOCXRenamedMainSharesUploadXMLWorkBudget(t *testing.T) {
	profile := storyProfile(t, "body,headers")
	// A race-instrumented child can take longer to read a full-budget fixture.
	sandbox, err := NewSandbox(SandboxConfig{Timeout: 2 * time.Minute, DOCXStories: profile})
	require.NoError(t, err)
	for _, tt := range []struct {
		name  string
		extra int
	}{{"exact limit", 0}, {"one byte above", 1}} {
		t.Run(tt.name, func(t *testing.T) {
			data := docxtest.RenamedMainAtWorkLimit(t, maxDocumentXMLBytes, maxDOCXXMLWorkBytes+tt.extra)
			ft, ctx, err := ResolveForExtraction(t.Context(), "renamed.docx", data)
			require.NoError(t, err)
			require.Equal(t, FileTypeDOCX, ft)
			got, err := NewDOCX(profile).Extract(ctx, bytes.NewReader(data))
			child, childErr := sandbox.Extract(ctx, ft, data)
			if tt.extra == 0 {
				require.NoError(t, err)
				require.NoError(t, childErr)
				require.Equal(t, 3, strings.Count(got, "[DOCX header]"))
				require.Equal(t, got, child)
			} else {
				require.ErrorContains(t, err, "cumulative")
				require.ErrorContains(t, childErr, "cumulative")
				require.Empty(t, got)
				require.Empty(t, child)
			}
		})
	}
}

func TestSandboxRedactsFailedChildDiagnostics(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, redact := range []bool{true, false} {
		logs.Reset()
		sb, err := NewSandbox(SandboxConfig{Timeout: 5 * time.Second, RedactTelemetry: redact})
		require.NoError(t, err)
		_, err = sb.Extract(t.Context(), "stderr", nil)
		require.ErrorIs(t, err, ErrExtractionFailed)
		require.Contains(t, logs.String(), "document extraction child failed")
		require.Equal(t, !redact, strings.Contains(logs.String(), "synthetic-extraction-child-diagnostic"))
	}
}
