// Package ocr detects text in image-only documents stored in S3.
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
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// Originals have immutable acceptance-specific keys; retries reuse detection.
	digest := sha256.Sum256([]byte(c.bucket + "\x00" + key))
	started, err := c.api.StartDocumentTextDetection(ctx, &textract.StartDocumentTextDetectionInput{
		ClientRequestToken: aws.String(fmt.Sprintf("%x", digest)),
		DocumentLocation:   &types.DocumentLocation{S3Object: &types.S3Object{Bucket: aws.String(c.bucket), Name: aws.String(key)}},
	})
	if err != nil {
		return "", detectionError(err)
	}
	if started == nil || aws.ToString(started.JobId) == "" {
		return "", domain.Permanent(ErrDetectionFailed)
	}
	return c.results(ctx, started.JobId)
}

type detectedLine struct {
	page      int32
	top, left float32
	text      string
}

func (c *Client) results(ctx context.Context, jobID *string) (string, error) {
	var next *string
	var lines []detectedLine
	seen := make(map[string]bool)
	bytes := 0
	delay := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		out, err := c.api.GetDocumentTextDetection(ctx, &textract.GetDocumentTextDetectionInput{JobId: jobID, NextToken: next, MaxResults: aws.Int32(1000)})
		if err != nil {
			return "", detectionError(err)
		}
		if out == nil {
			return "", domain.Permanent(ErrDetectionFailed)
		}
		if out.JobStatus == types.JobStatusInProgress {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return "", ctx.Err()
			case <-timer.C:
			}
			delay = min(2*delay, 10*time.Second)
			continue
		}
		if out.JobStatus != types.JobStatusSucceeded {
			return "", domain.Permanent(ErrDetectionFailed)
		}
		if out.DocumentMetadata != nil && aws.ToInt32(out.DocumentMetadata.Pages) > 3000 {
			return "", domain.Permanent(ErrPageLimit)
		}
		for _, block := range out.Blocks {
			if aws.ToInt32(block.Page) > 3000 {
				return "", domain.Permanent(ErrPageLimit)
			}
			if block.BlockType != types.BlockTypeLine {
				continue
			}
			text := strings.TrimSpace(strings.ReplaceAll(aws.ToString(block.Text), "\x00", ""))
			if text == "" {
				continue
			}
			if len(lines) > 0 {
				bytes++
			}
			bytes += len(text)
			if bytes > domain.MaxContentBytes {
				return "", domain.Permanent(ErrTextLimit)
			}
			line := detectedLine{page: aws.ToInt32(block.Page), text: text}
			if block.Geometry != nil && block.Geometry.BoundingBox != nil {
				line.top, line.left = block.Geometry.BoundingBox.Top, block.Geometry.BoundingBox.Left
			}
			lines = append(lines, line)
		}
		if aws.ToString(out.NextToken) == "" {
			break
		}
		token := *out.NextToken
		if seen[token] {
			return "", domain.Permanent(ErrDetectionFailed)
		}
		seen[token] = true
		next = out.NextToken
	}
	if len(lines) == 0 {
		return "", domain.Permanent(ErrNoText)
	}
	slices.SortStableFunc(lines, func(a, b detectedLine) int {
		if n := cmp.Compare(a.page, b.page); n != 0 {
			return n
		}
		if n := cmp.Compare(a.top, b.top); n != 0 {
			return n
		}
		return cmp.Compare(a.left, b.left)
	})
	var text strings.Builder
	text.Grow(bytes)
	for i, line := range lines {
		if i > 0 {
			text.WriteByte('\n')
		}
		text.WriteString(line.text)
	}
	return text.String(), nil
}

func detectionError(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "DocumentTooLargeException":
			return domain.Permanent(ErrPageLimit)
		case "BadDocumentException", "UnsupportedDocumentException", "InvalidParameterException", "IdempotentParameterMismatchException":
			return domain.Permanent(ErrDetectionFailed)
		}
	}
	return fmt.Errorf("ocr: detection request: %w", err)
}
