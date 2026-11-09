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
