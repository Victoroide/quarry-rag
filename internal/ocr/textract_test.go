package ocr

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/stretchr/testify/require"
)

type fakeTextract struct {
	start func(context.Context, *textract.StartDocumentTextDetectionInput) (*textract.StartDocumentTextDetectionOutput, error)
	get   func(context.Context, *textract.GetDocumentTextDetectionInput) (*textract.GetDocumentTextDetectionOutput, error)
}

func (f fakeTextract) StartDocumentTextDetection(ctx context.Context, in *textract.StartDocumentTextDetectionInput, _ ...func(*textract.Options)) (*textract.StartDocumentTextDetectionOutput, error) {
	if f.start != nil {
		return f.start(ctx, in)
	}
	return &textract.StartDocumentTextDetectionOutput{JobId: aws.String("synthetic-job")}, nil
}
func (f fakeTextract) GetDocumentTextDetection(ctx context.Context, in *textract.GetDocumentTextDetectionInput, _ ...func(*textract.Options)) (*textract.GetDocumentTextDetectionOutput, error) {
	return f.get(ctx, in)
}
func line(page int32, top, left float32, text string) types.Block {
	return types.Block{BlockType: types.BlockTypeLine, Page: aws.Int32(page), Text: aws.String(text), Geometry: &types.Geometry{BoundingBox: &types.BoundingBox{Top: top, Left: left}}}
}

func TestTextractPollsPaginatesAndOrdersLines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var tokens []string
		calls := 0
		api := fakeTextract{
			start: func(_ context.Context, in *textract.StartDocumentTextDetectionInput) (*textract.StartDocumentTextDetectionOutput, error) {
				require.Equal(t, "synthetic-bucket", aws.ToString(in.DocumentLocation.S3Object.Bucket))
				require.Equal(t, "synthetic-object", aws.ToString(in.DocumentLocation.S3Object.Name))
				tokens = append(tokens, aws.ToString(in.ClientRequestToken))
				return &textract.StartDocumentTextDetectionOutput{JobId: aws.String("synthetic-job")}, nil
			},
			get: func(_ context.Context, in *textract.GetDocumentTextDetectionInput) (*textract.GetDocumentTextDetectionOutput, error) {
				require.Equal(t, "synthetic-job", aws.ToString(in.JobId))
				calls++
				switch calls % 3 {
				case 1:
					require.Nil(t, in.NextToken)
					return &textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusInProgress}, nil
				case 2:
					require.Nil(t, in.NextToken)
					return &textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusSucceeded, NextToken: aws.String("next"), Blocks: []types.Block{line(2, .1, 0, "Page two"), line(1, .8, 0, "Last"), {BlockType: types.BlockTypeWord, Text: aws.String("ignored")}}}, nil
				default:
					require.Equal(t, "next", aws.ToString(in.NextToken))
					return &textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusSucceeded, Blocks: []types.Block{line(1, .1, .5, "Right"), line(1, .1, 0, "First\x00")}}, nil
				}
			},
		}
		c := &Client{api: api, bucket: "synthetic-bucket", timeout: time.Minute}
		for range 2 {
			text, err := c.Extract(t.Context(), "synthetic-object")
			require.NoError(t, err)
			require.Equal(t, "First\nRight\nLast\nPage two", text)
		}
		require.Len(t, tokens, 2)
		require.Len(t, tokens[0], 64)
		require.Equal(t, tokens[0], tokens[1])
	})
}

func TestTextractRejectsIncompleteAndOversizedResults(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  textract.GetDocumentTextDetectionOutput
		want error
	}{
		{"no text", textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusSucceeded}, ErrNoText},
		{"blank text", textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusSucceeded, Blocks: []types.Block{line(1, 0, 0, " \n\x00")}}, ErrNoText},
		{"page limit", textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusSucceeded, DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(3001)}}, ErrPageLimit},
		{"block page limit", textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusSucceeded, Blocks: []types.Block{line(3001, 0, 0, "text")}}, ErrPageLimit},
		{"text limit", textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusSucceeded, Blocks: []types.Block{line(1, 0, 0, strings.Repeat("x", domain.MaxContentBytes+1))}}, ErrTextLimit},
		{"partial", textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusPartialSuccess, Blocks: []types.Block{line(1, 0, 0, "partial")}}, ErrDetectionFailed},
		{"failed", textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusFailed, StatusMessage: aws.String("private upstream detail")}, ErrDetectionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{api: fakeTextract{get: func(context.Context, *textract.GetDocumentTextDetectionInput) (*textract.GetDocumentTextDetectionOutput, error) {
				return &tc.out, nil
			}}, bucket: "synthetic-bucket", timeout: time.Minute}
			text, err := c.Extract(t.Context(), "synthetic-object")
			require.Empty(t, text)
			require.ErrorIs(t, err, tc.want)
			require.ErrorIs(t, err, domain.ErrPermanent)
			require.NotContains(t, err.Error(), "private upstream detail")
		})
	}
}

func TestTextractCancellationAndTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var times []time.Time
		c := &Client{api: fakeTextract{get: func(context.Context, *textract.GetDocumentTextDetectionInput) (*textract.GetDocumentTextDetectionOutput, error) {
			times = append(times, time.Now())
			return &textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusInProgress}, nil
		}}, bucket: "synthetic-bucket", timeout: 4 * time.Second}
		_, err := c.Extract(t.Context(), "synthetic-object")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Len(t, times, 3)
		require.Equal(t, time.Second, times[1].Sub(times[0]))
		require.Equal(t, 2*time.Second, times[2].Sub(times[1]))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = c.Extract(ctx, "synthetic-object")
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestTextractStartErrors(t *testing.T) {
	for _, upstream := range []error{&types.DocumentTooLargeException{}, errors.New("temporary")} {
		c := &Client{api: fakeTextract{start: func(context.Context, *textract.StartDocumentTextDetectionInput) (*textract.StartDocumentTextDetectionOutput, error) {
			return nil, upstream
		}}, bucket: "synthetic-bucket", timeout: time.Minute}
		_, err := c.Extract(t.Context(), "synthetic-object")
		require.Error(t, err)
		var tooLarge *types.DocumentTooLargeException
		if errors.As(upstream, &tooLarge) {
			require.ErrorIs(t, err, domain.ErrPermanent)
		} else {
			require.NotErrorIs(t, err, domain.ErrPermanent)
		}
	}
}

func TestTextractPagePaginationAndRetryIdentity(t *testing.T) {
	var tokens []string
	page := func(n int32) types.Block { return types.Block{BlockType: types.BlockTypePage, Page: aws.Int32(n)} }
	c := &Client{bucket: "synthetic-bucket", timeout: time.Minute, api: fakeTextract{
		start: func(_ context.Context, in *textract.StartDocumentTextDetectionInput) (*textract.StartDocumentTextDetectionOutput, error) {
			key := aws.ToString(in.DocumentLocation.S3Object.Name)
			require.Equal(t, "original.pdf", key)
			tokens = append(tokens, aws.ToString(in.ClientRequestToken))
			return &textract.StartDocumentTextDetectionOutput{JobId: aws.String("stable")}, nil
		},
		get: func(_ context.Context, in *textract.GetDocumentTextDetectionInput) (*textract.GetDocumentTextDetectionOutput, error) {
			require.Equal(t, "stable", aws.ToString(in.JobId))
			out := &textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusPartialSuccess, DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(5)}}
			if in.NextToken == nil {
				out.Blocks = []types.Block{page(1), page(2), line(1, .7, .1, "Second"), line(5, 0, 0, "Final page")}
				out.NextToken = aws.String("next")
			} else {
				require.Equal(t, "next", aws.ToString(in.NextToken))
				out.Blocks = []types.Block{page(5), page(4), line(1, .1, .1, "First"), line(4, 0, 0, "Untrusted")}
				out.Warnings = []types.Warning{{Pages: []int32{4}}}
			}
			return out, nil
		},
	}}
	for range 2 {
		result, err := c.ExtractPages(t.Context(), "original.pdf", "profile-v1", "first-window", 5, nil)
		require.NoError(t, err)
		require.True(t, result.Partial)
		require.Equal(t, Page{Present: true, Text: "First\nSecond"}, result.Pages[1])
		require.Equal(t, Page{Present: true}, result.Pages[2])
		require.NotContains(t, result.Pages, 3)
		require.Equal(t, Page{Present: true, Failed: true, Text: "Untrusted"}, result.Pages[4])
		require.Equal(t, "Final page", result.Pages[5].Text)
	}
	require.Equal(t, tokens[0], tokens[1])
	_, err := c.ExtractPages(t.Context(), "original.pdf", "profile-v2", "first-window", 5, nil)
	require.NoError(t, err)
	require.NotEqual(t, tokens[0], tokens[2])
	_, err = c.ExtractPages(t.Context(), "original.pdf", "profile-v1", "second-window", 5, nil)
	require.NoError(t, err)
	require.NotEqual(t, tokens[0], tokens[3])
	_, err = c.ExtractPages(t.Context(), "original.pdf", "profile-v1", "first-window", domain.MaxPDFPages+1, nil)
	require.ErrorIs(t, err, ErrPageLimit)
	require.Len(t, tokens, 4)
}

func TestTextractMixedRequiresStartCheckpointBeforePolling(t *testing.T) {
	c := &Client{bucket: "synthetic-bucket", timeout: time.Minute, api: fakeTextract{get: func(context.Context, *textract.GetDocumentTextDetectionInput) (*textract.GetDocumentTextDetectionOutput, error) {
		t.Fatal("provider polled after the lease was lost")
		return nil, nil
	}}}
	_, err := c.ExtractPages(t.Context(), "original.pdf", "profile", "window", 1, func(context.Context) error {
		return domain.ErrJobNotOwned
	})
	require.ErrorIs(t, err, domain.ErrJobNotOwned)
}

func TestTextractMixedRejectsPageCountMismatch(t *testing.T) {
	c := &Client{bucket: "synthetic-bucket", timeout: time.Minute, api: fakeTextract{get: func(context.Context, *textract.GetDocumentTextDetectionInput) (*textract.GetDocumentTextDetectionOutput, error) {
		return &textract.GetDocumentTextDetectionOutput{JobStatus: types.JobStatusSucceeded, DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(8)}}, nil
	}}}
	_, err := c.ExtractPages(t.Context(), "original.pdf", "profile", "window", 7, nil)
	require.ErrorIs(t, err, ErrDetectionFailed)
	require.ErrorIs(t, err, domain.ErrPermanent)
}
