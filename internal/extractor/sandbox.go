package extractor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

// ChildArg selects the extraction entry point in the service binary.
const ChildArg = "__quarry-extract"

// ChildEnv enables the extraction entry point when paired with ChildArg.
const ChildEnv = "QUARRY_EXTRACT_CHILD"

const childMaxStack = 64 << 20

// Extraction failures distinguish a timeout, parser failure and unavailable executable.
var (
	ErrExtractionTimeout     = errors.New("extractor: extraction timed out")
	ErrExtractionFailed      = errors.New("extractor: document could not be parsed")
	ErrExtractionUnavailable = errors.New("extractor: extraction is unavailable")
)

// SandboxConfig sets the child executable, resource limits and optional Word stories.
type SandboxConfig struct {
	Executable    string
	Timeout       time.Duration
	MemoryLimitMB int
	DOCStories    DOCStories
	DOCXStories   DOCXStories
	// RedactTelemetry suppresses raw child process errors and stderr in logs.
	RedactTelemetry bool
}

// Sandbox extracts documents in a separate process with bounded input and output.
type Sandbox struct {
	executable      string
	timeout         time.Duration
	memoryLimitMB   int
	docStories      DOCStories
	docxStories     DOCXStories
	redactTelemetry bool
}

// NewSandbox defaults to the current executable and checks its availability.
func NewSandbox(cfg SandboxConfig) (*Sandbox, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MemoryLimitMB == 0 {
		cfg.MemoryLimitMB = 512
	}
	if cfg.Timeout < time.Millisecond || cfg.Timeout > 10*time.Minute || cfg.MemoryLimitMB < 64 || cfg.MemoryLimitMB > 8192 {
		return nil, fmt.Errorf("%w: invalid extraction limits", ErrExtractionUnavailable)
	}
	if !cfg.DOCStories.valid() {
		return nil, fmt.Errorf("%w: invalid DOC stories", ErrExtractionUnavailable)
	}
	if cfg.DOCXStories&^allDOCXStories != 0 {
		return nil, fmt.Errorf("%w: invalid DOCX stories", ErrExtractionUnavailable)
	}
	if cfg.Executable == "" {
		var err error
		cfg.Executable, err = os.Executable()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrExtractionUnavailable, err)
		}
	}
	path, err := exec.LookPath(cfg.Executable)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrExtractionUnavailable, err)
	}
	return &Sandbox{executable: path, timeout: cfg.Timeout, memoryLimitMB: cfg.MemoryLimitMB, docStories: cfg.DOCStories, docxStories: cfg.DOCXStories, redactTelemetry: cfg.RedactTelemetry}, nil
}

type childHeader struct {
	Type              FileType    `json:"type"`
	TimeoutMS         int64       `json:"timeout_ms"`
	MaxStack          int         `json:"max_stack"`
	DOCStories        DOCStories  `json:"doc_stories,omitempty"`
	DOCXStories       DOCXStories `json:"docx_stories,omitempty"`
	DOCXDetectionWork uint64      `json:"docx_detection_work,omitempty"`
	PageAware         bool        `json:"page_aware,omitempty"`
}

type childResponse struct {
	Text      *string         `json:"text,omitempty"`
	Pages     json.RawMessage `json:"pages,omitempty"`
	Error     string          `json:"error,omitempty"`
	ErrorKind childErrorKind  `json:"error_kind,omitempty"`
}

type childErrorKind string

const (
	childErrorEOF         childErrorKind = "unexpected_eof"
	childErrorContentSize childErrorKind = "content_too_large"
)

type childExtractionError struct {
	message string
	cause   error
}

func (e childExtractionError) Error() string { return e.message }
func (e childExtractionError) Unwrap() error { return e.cause }

// Extract runs one child process, terminating it on cancellation or exceeded limits.
func (s *Sandbox) Extract(ctx context.Context, ft FileType, data []byte) (string, error) {
	response, err := s.extract(ctx, ft, data, false)
	if err != nil {
		return "", err
	}
	if response.Text == nil {
		return "", ErrExtractionFailed
	}
	return *response.Text, nil
}

// ExtractPDFPages runs page inspection in the same parser sandbox as Extract.
func (s *Sandbox) ExtractPDFPages(ctx context.Context, data []byte) (domain.PDFPages, error) {
	response, err := s.extract(ctx, FileTypePDF, data, true)
	if err != nil {
		return nil, err
	}
	pages, err := domain.DecodePDFPages(response.Pages)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrExtractionFailed, err)
	}
	return pages, nil
}

func (s *Sandbox) extract(ctx context.Context, ft FileType, data []byte, pageAware bool) (childResponse, error) {
	if len(data) > maxSourceBytes {
		return childResponse{}, fmt.Errorf("extractor: source exceeds maximum size of %d bytes", maxSourceBytes)
	}
	childCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	request := childHeader{Type: ft, TimeoutMS: s.timeout.Milliseconds(), MaxStack: childMaxStack, PageAware: pageAware}
	switch ft {
	case FileTypeDOC:
		request.DOCStories = s.docStories
	case FileTypeDOCX:
		request.DOCXStories = s.docxStories
		request.DOCXDetectionWork = docxDetectionWork(ctx)
	}
	header, err := json.Marshal(request)
	if err != nil {
		return childResponse{}, fmt.Errorf("%w: header: %w", ErrExtractionFailed, err)
	}
	cmd := exec.CommandContext(childCtx, s.executable, ChildArg) // #nosec G204 -- executable is resolved at construction, arguments are fixed.
	cmd.Env = []string{ChildEnv + "=1", "GOMAXPROCS=1", "GOMEMLIMIT=" + strconv.Itoa(s.memoryLimitMB) + "MiB"}
	cmd.Stdin = io.MultiReader(bytes.NewReader(append(header, '\n')), bytes.NewReader(data))
	stdout := &cappedBuffer{limit: 8 << 20, cancel: cancel}
	stderr := &cappedBuffer{limit: 64 << 10, cancel: cancel}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.WaitDelay = 2 * time.Second
	err = cmd.Run()
	if err != nil || stdout.exceeded || stderr.exceeded {
		if s.redactTelemetry {
			slog.WarnContext(ctx, "document extraction child failed")
		} else {
			slog.WarnContext(ctx, "document extraction child failed", "error", err, "stderr", stderr.buffer.String())
		}
		if stdout.exceeded || stderr.exceeded {
			return childResponse{}, ErrExtractionFailed
		}
		if errors.Is(childCtx.Err(), context.DeadlineExceeded) {
			return childResponse{}, ErrExtractionTimeout
		}
		if ctx.Err() != nil {
			return childResponse{}, ctx.Err()
		}
		var pathErr *os.PathError
		var execErr *exec.Error
		if errors.As(err, &pathErr) || errors.As(err, &execErr) {
			return childResponse{}, ErrExtractionUnavailable
		}
		return childResponse{}, ErrExtractionFailed
	}
	var response childResponse
	if err := json.Unmarshal(stdout.buffer.Bytes(), &response); err != nil {
		return childResponse{}, ErrExtractionFailed
	}
	if response.Error != "" {
		switch response.ErrorKind {
		case childErrorEOF:
			return childResponse{}, childExtractionError{response.Error, io.ErrUnexpectedEOF}
		case childErrorContentSize:
			return childResponse{}, childExtractionError{response.Error, ErrContentTooLarge}
		default:
			return childResponse{}, errors.New(response.Error)
		}
	}
	if err := childCtx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return childResponse{}, ErrExtractionTimeout
		}
		return childResponse{}, err
	}
	return response, nil
}

type cappedBuffer struct {
	buffer   bytes.Buffer
	limit    int
	exceeded bool
	cancel   context.CancelFunc
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.buffer.Len() {
		n, _ := b.buffer.Write(p[:b.limit-b.buffer.Len()])
		b.exceeded = true
		b.cancel()
		return n, ErrExtractionFailed
	}
	return b.buffer.Write(p)
}

var childHooks map[FileType]func() (string, error)

// RunChild reads one extraction request and writes one JSON response.
func RunChild(stdin io.Reader, stdout io.Writer) int {
	reader := bufio.NewReaderSize(stdin, 1024)
	line, err := reader.ReadSlice('\n')
	if err != nil || len(line) > 1024 {
		return writeChildResponse(stdout, "", errors.New("extractor: invalid extraction header"))
	}
	var header childHeader
	if decodeErr := json.Unmarshal(line, &header); decodeErr != nil || header.TimeoutMS < 1 || header.TimeoutMS > (10*time.Minute).Milliseconds() || header.MaxStack < 1 || header.MaxStack > childMaxStack {
		return writeChildResponse(stdout, "", errors.New("extractor: invalid extraction header"))
	}
	if !header.DOCStories.valid() || (header.Type != FileTypeDOC && header.DOCStories != 0) {
		return writeChildResponse(stdout, "", errors.New("extractor: invalid DOC stories"))
	}
	if header.DOCXStories&^allDOCXStories != 0 || header.DOCXDetectionWork > maxDOCXRelationshipsBytes ||
		(header.Type != FileTypeDOCX && (header.DOCXStories != 0 || header.DOCXDetectionWork != 0)) {
		return writeChildResponse(stdout, "", errors.New("extractor: invalid extraction header"))
	}
	debug.SetMaxStack(header.MaxStack)
	// Prefer terminating the parser if the kernel must reclaim memory.
	_ = os.WriteFile("/proc/self/oom_score_adj", []byte("1000"), 0o600)
	hook := childHooks[header.Type]
	if hook == nil && header.Type != FileTypePDF && header.Type != FileTypeDOCX && header.Type != FileTypeDOC {
		return writeChildResponse(stdout, "", errors.New("extractor: unsupported file type"))
	}
	if header.PageAware && header.Type != FileTypePDF {
		return writeChildResponse(stdout, "", errors.New("extractor: page inspection requires a PDF"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(header.TimeoutMS)*time.Millisecond)
	defer cancel()
	data, err := readSource(ctx, reader)
	if err != nil {
		return writeChildResponse(stdout, "", err)
	}
	if header.PageAware {
		return runPDFPages(ctx, stdout, data)
	}
	var text string
	switch {
	case hook != nil:
		text, err = hook()
	default:
		extractCtx := ctx
		if header.Type == FileTypeDOCX {
			extractCtx = withDOCXDetectionWork(ctx, header.DOCXDetectionWork)
		}
		text, err = NewWithStories(header.Type, header.DOCStories, header.DOCXStories).Extract(extractCtx, bytes.NewReader(data))
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return 1
	}
	return writeChildResponse(stdout, text, err)
}

func runPDFPages(ctx context.Context, stdout io.Writer, data []byte) int {
	pages, err := ExtractPDFPages(ctx, bytes.NewReader(data))
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return 1
	}
	if err != nil {
		return writeChildResponse(stdout, "", err)
	}
	encoded, err := json.Marshal(pages)
	if err != nil {
		return 1
	}
	if err = json.NewEncoder(stdout).Encode(childResponse{Pages: encoded}); err != nil {
		return 1
	}
	return 0
}

func writeChildResponse(stdout io.Writer, text string, err error) int {
	response := childResponse{Text: &text}
	if err != nil {
		response.Text, response.Error = nil, err.Error()
		switch {
		case errors.Is(err, io.ErrUnexpectedEOF):
			response.ErrorKind = childErrorEOF
		case errors.Is(err, ErrContentTooLarge):
			response.ErrorKind = childErrorContentSize
		}
	}
	if encodeErr := json.NewEncoder(stdout).Encode(response); encodeErr != nil {
		return 1
	}
	return 0
}
