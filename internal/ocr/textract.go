// Package ocr detects text in documents stored in S3.
package ocr

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/awsconf"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/aws/smithy-go"
)

// Detection errors identify terminal text and document limits.
var (
	ErrNoText          = errors.New("no_text")
	ErrPageLimit       = errors.New("ocr_page_limit")
	ErrTextLimit       = errors.New("ocr_text_too_large")
	ErrDetectionFailed = errors.New("ocr_detection_failed")
)

type textractAPI interface {
	StartDocumentTextDetection(context.Context, *textract.StartDocumentTextDetectionInput, ...func(*textract.Options)) (*textract.StartDocumentTextDetectionOutput, error)
	GetDocumentTextDetection(context.Context, *textract.GetDocumentTextDetectionInput, ...func(*textract.Options)) (*textract.GetDocumentTextDetectionOutput, error)
	StartDocumentAnalysis(context.Context, *textract.StartDocumentAnalysisInput, ...func(*textract.Options)) (*textract.StartDocumentAnalysisOutput, error)
	GetDocumentAnalysis(context.Context, *textract.GetDocumentAnalysisInput, ...func(*textract.Options)) (*textract.GetDocumentAnalysisOutput, error)
}

// Client performs asynchronous document text detection.
type Client struct {
	api     textractAPI
	bucket  string
	timeout time.Duration
}

// New creates a text detector with the shared AWS credentials and retry policy.
func New(ctx context.Context, cfg *config.Config) (*Client, error) {
	awsCfg, err := awsconf.Load(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("ocr: load aws config: %w", err)
	}
	return &Client{api: textract.NewFromConfig(awsCfg), bucket: cfg.S3Bucket, timeout: cfg.OCRTimeout}, nil
}

// Extract returns detected lines from the stored original document.
func (c *Client) Extract(parent context.Context, key string) (string, error) {
	result, err := c.extract(parent, key, "", "", 0, nil)
	if err != nil {
		return "", err
	}
	if len(result.lines) == 0 {
		return "", domain.Permanent(ErrNoText)
	}
	var text strings.Builder
	text.Grow(result.bytes)
	for i, line := range result.lines {
		if i > 0 {
			text.WriteByte('\n')
		}
		text.WriteString(line.text)
	}
	return text.String(), nil
}

// Page is the provider evidence for one page of the submitted document.
type Page struct {
	Present bool
	Failed  bool
	Text    string
}

// PageResults preserves page presence and provider warnings across pagination.
type PageResults struct {
	Pages   map[int]Page
	Partial bool
}

// ExtractPages submits the entire original and records acceptance before polling.
func (c *Client) ExtractPages(ctx context.Context, key, profile, window string, pageCount int, accepted func(context.Context) error) (PageResults, error) {
	if pageCount < 1 || pageCount > domain.MaxPDFPages {
		return PageResults{}, domain.Permanent(ErrPageLimit)
	}
	if profile == "" || window == "" {
		return PageResults{}, domain.Permanent(ErrDetectionFailed)
	}
	result, err := c.extract(ctx, key, profile, window, pageCount, accepted)
	if err != nil {
		return PageResults{}, err
	}
	for start := 0; start < len(result.lines); {
		end, number := start+1, result.lines[start].page
		for end < len(result.lines) && result.lines[end].page == number {
			end++
		}
		var text strings.Builder
		for i, line := range result.lines[start:end] {
			if i > 0 {
				text.WriteByte('\n')
			}
			text.WriteString(line.text)
		}
		page := result.pages.Pages[int(number)]
		page.Text = text.String()
		result.pages.Pages[int(number)] = page
		start = end
	}
	return result.pages, nil
}

func (c *Client) requestToken(key, profile, window string) string {
	// Originals have immutable keys. Mixed and layout retries also reuse their persisted window.
	identity := c.bucket + "\x00" + key
	if profile != "" {
		identity += "\x00" + profile + "\x00" + window
	}
	digest := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("%x", digest)
}

func (c *Client) extract(parent context.Context, key, profile, window string, pageCount int, accepted func(context.Context) error) (detection, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return detection{}, err
	}
	started, err := c.api.StartDocumentTextDetection(ctx, &textract.StartDocumentTextDetectionInput{
		ClientRequestToken: aws.String(c.requestToken(key, profile, window)),
		DocumentLocation:   &types.DocumentLocation{S3Object: &types.S3Object{Bucket: aws.String(c.bucket), Name: aws.String(key)}},
	})
	if err != nil {
		return detection{}, detectionError(err, pageCount > 0)
	}
	if started == nil || aws.ToString(started.JobId) == "" {
		return detection{}, domain.Permanent(ErrDetectionFailed)
	}
	if accepted != nil {
		if acceptedErr := accepted(ctx); acceptedErr != nil {
			return detection{}, acceptedErr
		}
	}
	return c.results(ctx, started.JobId, pageCount)
}

type detectedLine struct {
	page      int32
	top, left float32
	text      string
}

type detection struct {
	lines []detectedLine
	bytes int
	pages PageResults
}

const maxResultBatches = 1000

func (c *Client) results(ctx context.Context, jobID *string, pageCount int) (detection, error) {
	var next *string
	var result detection
	if pageCount > 0 {
		result.pages.Pages = make(map[int]Page, pageCount)
	}
	seen := make(map[string]bool)
	blockIDs := make(map[string]struct{})
	delay := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return detection{}, err
		}
		if pageCount > 0 && len(seen) >= maxResultBatches {
			return detection{}, domain.Permanent(ErrDetectionFailed)
		}
		out, err := c.api.GetDocumentTextDetection(ctx, &textract.GetDocumentTextDetectionInput{JobId: jobID, NextToken: next, MaxResults: aws.Int32(1000)})
		if err != nil {
			return detection{}, detectionError(err, pageCount > 0)
		}
		if out == nil {
			return detection{}, domain.Permanent(ErrDetectionFailed)
		}
		if out.JobStatus == types.JobStatusInProgress {
			if delayErr := sleepPollDelay(ctx, delay); delayErr != nil {
				return detection{}, delayErr
			}
			delay = min(2*delay, 10*time.Second)
			continue
		}
		if out.JobStatus != types.JobStatusSucceeded && (pageCount == 0 || out.JobStatus != types.JobStatusPartialSuccess) {
			return detection{}, domain.Permanent(ErrDetectionFailed)
		}
		if err = result.collect(out, pageCount, blockIDs); err != nil {
			return detection{}, domain.Permanent(err)
		}
		next, err = advanceNextToken(out.NextToken, seen, pageCount > 0)
		if err != nil {
			return detection{}, domain.Permanent(err)
		}
		if next == nil {
			break
		}
	}
	slices.SortStableFunc(result.lines, func(a, b detectedLine) int {
		if n := cmp.Compare(a.page, b.page); n != 0 {
			return n
		}
		if n := cmp.Compare(a.top, b.top); n != 0 {
			return n
		}
		return cmp.Compare(a.left, b.left)
	})
	return result, ctx.Err()
}

func (d *detection) collect(out *textract.GetDocumentTextDetectionOutput, pageCount int, blockIDs map[string]struct{}) error {
	if out.DocumentMetadata != nil {
		count := int(aws.ToInt32(out.DocumentMetadata.Pages))
		if count > 3000 {
			return ErrPageLimit
		}
		if pageCount > 0 && count != pageCount {
			return ErrDetectionFailed
		}
	}
	if pageCount > 0 {
		d.pages.Partial = d.pages.Partial || out.JobStatus == types.JobStatusPartialSuccess || len(out.Warnings) > 0
		for _, warning := range out.Warnings {
			for _, number := range warning.Pages {
				if number < 1 || int(number) > pageCount {
					return ErrDetectionFailed
				}
				page := d.pages.Pages[int(number)]
				page.Failed = true
				d.pages.Pages[int(number)] = page
			}
		}
	}
	for _, block := range out.Blocks {
		number := aws.ToInt32(block.Page)
		if number > 3000 {
			return ErrPageLimit
		}
		if pageCount > 0 {
			if number < 1 || int(number) > pageCount {
				return ErrDetectionFailed
			}
			if block.BlockType == types.BlockTypePage {
				page := d.pages.Pages[int(number)]
				page.Present = true
				d.pages.Pages[int(number)] = page
			}
		}
		if block.BlockType != types.BlockTypeLine {
			continue
		}
		text := strings.TrimSpace(strings.ReplaceAll(aws.ToString(block.Text), "\x00", ""))
		if text == "" {
			continue
		}
		if id := aws.ToString(block.Id); pageCount > 0 && id != "" {
			if _, duplicate := blockIDs[id]; duplicate {
				return ErrDetectionFailed
			}
			blockIDs[id] = struct{}{}
		}
		if len(d.lines) > 0 {
			d.bytes++
		}
		d.bytes += len(text)
		if d.bytes > domain.MaxContentBytes {
			return ErrTextLimit
		}
		line := detectedLine{page: number, text: text}
		if block.Geometry != nil && block.Geometry.BoundingBox != nil {
			line.top, line.left = block.Geometry.BoundingBox.Top, block.Geometry.BoundingBox.Left
		}
		d.lines = append(d.lines, line)
	}
	return nil
}

func sleepPollDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func advanceNextToken(nextToken *string, seen map[string]bool, bounded bool) (*string, error) {
	if aws.ToString(nextToken) == "" {
		return nil, nil
	}
	token := *nextToken
	if seen[token] || (bounded && len(token) > 1024) {
		return nil, ErrDetectionFailed
	}
	seen[token] = true
	return nextToken, nil
}

func detectionError(err error, pageAware bool) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "DocumentTooLargeException":
			return domain.Permanent(ErrPageLimit)
		case "BadDocumentException", "UnsupportedDocumentException", "InvalidParameterException", "IdempotentParameterMismatchException":
			return domain.Permanent(ErrDetectionFailed)
		case "InvalidS3ObjectException", "InvalidJobIdException":
			if pageAware {
				return domain.Permanent(ErrDetectionFailed)
			}
		}
	}
	return fmt.Errorf("ocr: detection request: %w", err)
}
