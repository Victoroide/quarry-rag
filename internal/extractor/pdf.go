package extractor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Victoroide/quarry-rag/internal/domain"

	"github.com/ledongthuc/pdf"
)

type pdfExtractor struct{}

func (e *pdfExtractor) Type() FileType { return FileTypePDF }

var multiBlankLine = regexp.MustCompile(`\n{3,}`)

func (e *pdfExtractor) Extract(ctx context.Context, r io.Reader) (result string, err error) {
	_, result, err = extractPDF(ctx, r, false)
	return result, err
}

// ExtractPDFPages reports page evidence while applying the legacy parser budgets.
func ExtractPDFPages(ctx context.Context, r io.Reader) (domain.PDFPages, error) {
	pages, _, err := extractPDF(ctx, r, true)
	return pages, err
}

func extractPDF(ctx context.Context, r io.Reader, pageAware bool) (pages domain.PDFPages, result string, err error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			pages, result = nil, ""
			err = fmt.Errorf("extractor: pdf: invalid document: %v", recovered)
		}
	}()

	data, err := readSource(ctx, r)
	if err != nil {
		return nil, "", fmt.Errorf("extractor: pdf: read: %w", err)
	}

	source := &pdfSource{ctx: ctx, reader: bytes.NewReader(data), remaining: 64 << 20}
	reader, err := pdf.NewReader(source, int64(len(data)))
	if err != nil {
		return nil, "", fmt.Errorf("extractor: pdf: %w", err)
	}

	var buf strings.Builder
	numPages := reader.NumPage()
	if numPages > domain.MaxPDFPages {
		return nil, "", fmt.Errorf("extractor: pdf: maximum page count exceeded")
	}
	var strictPages []pdf.Page
	if pageAware {
		source.encrypted = !reader.Trailer().Key("Encrypt").IsNull()
		if treeErr := readPDFPageTree(ctx, reader.Trailer().Key("Root").Key("Pages"), &strictPages, 0); treeErr != nil {
			return nil, "", treeErr
		}
		pages = make(domain.PDFPages, 0, numPages)
	}
	decodedBudget := int64(8 << 20)

	for i := 1; i <= numPages; i++ {
		if err := ctx.Err(); err != nil {
			return nil, "", fmt.Errorf("extractor: pdf: %w", err)
		}
		var page pdf.Page
		if pageAware {
			page = strictPages[i-1]
		} else {
			page = reader.Page(i)
		}
		if page.V.IsNull() {
			continue
		}
		// Check expanded streams before the parser allocates text and font maps.
		contents := page.V.Key("Contents")
		if streamErr := checkPDFStream(ctx, contents, &decodedBudget, 0); streamErr != nil {
			return nil, "", streamErr
		}
		var text string
		var pErr error
		paint := pdfPaint{source: source}
		if pageAware {
			pErr = paint.inspectPage(ctx, page, contents, &decodedBudget)
			text = paint.text.String()
		} else {
			if fontErr := checkPDFFonts(ctx, page.Resources(), &decodedBudget); fontErr != nil {
				return nil, "", fontErr
			}
			text, pErr = page.GetPlainText(nil)
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, "", fmt.Errorf("extractor: pdf: %w", contextErr)
		}

		if pErr != nil {
			if pageAware {
				return nil, "", fmt.Errorf("extractor: pdf: invalid page %d: %w", i, pErr)
			}
			continue
		}
		cleaned := strings.TrimSpace(stripControlChars(text))
		if pageAware {
			normalized := multiBlankLine.ReplaceAllString(cleaned, "\n\n")
			pages = append(pages, domain.PDFPage{Number: i, Evidence: paint.evidence(normalized), Text: normalized})
		}
		if cleaned == "" {
			continue
		}
		separator := 0
		if buf.Len() > 0 {
			separator = 2
		}
		if len(cleaned) > domain.MaxContentBytes-buf.Len()-separator {
			return nil, "", fmt.Errorf("extractor: pdf: maximum text size exceeded: %w", ErrContentTooLarge)
		}
		if buf.Len() > 0 {
			buf.WriteString("\n\n")
		}
		buf.WriteString(cleaned)
	}
	if pageAware {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if source.limitErr != nil {
			return nil, "", source.limitErr
		}
		return pages, "", nil
	}

	result = multiBlankLine.ReplaceAllString(buf.String(), "\n\n")
	result = strings.TrimSpace(result)
	if len(result) < domain.MinPDFTextBytes {
		return nil, "", fmt.Errorf("extractor: pdf: %w", ErrNoText)
	}
	return nil, result, nil
}

func stripControlChars(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '\t' || r == '\n' || r == '\r' || (r >= 0x20 && r != 0x7F) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Stream and source budgets also bound repeated object reads in malformed files.
type pdfSource struct {
	ctx        context.Context
	reader     *bytes.Reader
	remaining  int64
	limitErr   error
	encrypted  bool
	jpegFrames map[int64]pdfJPEGFrame // validated JPEG frames by stream offset for this document
}

func (s *pdfSource) ReadAt(p []byte, offset int64) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(p)) > s.remaining {
		s.limitErr = fmt.Errorf("extractor: pdf: maximum source read budget exceeded")
		return 0, s.limitErr
	}
	s.remaining -= int64(len(p))
	return s.reader.ReadAt(p, offset)
}

func checkPDFStream(ctx context.Context, value pdf.Value, remaining *int64, depth int) error {
	if depth > 64 {
		return fmt.Errorf("extractor: pdf: maximum stream nesting exceeded")
	}
	switch value.Kind() {
	case pdf.Array:
		if value.Len() > 4096 {
			return fmt.Errorf("extractor: pdf: maximum stream count exceeded")
		}
		for i := 0; i < value.Len(); i++ {
			if err := checkPDFStream(ctx, value.Index(i), remaining, depth+1); err != nil {
				return err
			}
		}
	case pdf.Stream:
		reader := value.Reader()
		count, err := io.Copy(io.Discard, io.LimitReader(contextReader{ctx: ctx, reader: reader}, *remaining+1))
		closeErr := reader.Close()
		if err != nil {
			return fmt.Errorf("extractor: pdf: decode stream: %w", err)
		}
		if closeErr != nil {
			return fmt.Errorf("extractor: pdf: close stream: %w", closeErr)
		}
		*remaining -= count
		if *remaining < 0 {
			return fmt.Errorf("extractor: pdf: maximum decoded stream size exceeded")
		}
	}
	return nil
}

var (
	fontMapCounts = regexp.MustCompile(`(\S+)\s+begin(?:bfchar|bfrange|codespacerange)\b`)
	fontMapHex    = regexp.MustCompile(`<([[:xdigit:][:space:]]*)>`)
)

func checkPDFFonts(ctx context.Context, resources pdf.Value, budget *int64) error {
	fonts := resources.Key("Font")
	names := fonts.Keys()
	if len(names) > 128 {
		return fmt.Errorf("extractor: pdf: maximum font count exceeded")
	}
	for _, name := range names {
		if err := checkPDFFont(ctx, fonts.Key(name), budget); err != nil {
			return err
		}
	}
	return nil
}

func checkPDFFont(ctx context.Context, font pdf.Value, budget *int64) error {
	if font.Key("Encoding").Key("Differences").Len() > 512 {
		return fmt.Errorf("extractor: pdf: maximum font map size exceeded")
	}
	return checkPDFFontMap(ctx, font.Key("ToUnicode"), budget)
}

// CMap declarations can request far more work than their small streams suggest.
func checkPDFFontMap(ctx context.Context, value pdf.Value, remaining *int64) error {
	if value.Kind() != pdf.Stream {
		return nil
	}
	reader := value.Reader()
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: reader}, min(*remaining, 256<<10)+1))
	closeErr := reader.Close()
	if err != nil {
		return fmt.Errorf("extractor: pdf: read font map: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("extractor: pdf: close font map: %w", closeErr)
	}
	*remaining -= int64(len(data))
	if len(data) > 256<<10 || *remaining < 0 {
		return fmt.Errorf("extractor: pdf: maximum font map size exceeded")
	}
	entries := 0
	for _, match := range fontMapCounts.FindAllSubmatch(data, -1) {
		count, parseErr := strconv.Atoi(string(match[1]))
		if parseErr != nil || count < 0 || count > 4096-entries {
			return fmt.Errorf("extractor: pdf: maximum font map work exceeded")
		}
		entries += count
	}
	for _, match := range fontMapHex.FindAllSubmatch(data, -1) {
		digits := 0
		for _, digit := range string(match[1]) {
			if !unicode.IsSpace(digit) {
				digits++
			}
		}
		if digits > 16 {
			return fmt.Errorf("extractor: pdf: maximum font map character size exceeded")
		}
	}
	return nil
}
