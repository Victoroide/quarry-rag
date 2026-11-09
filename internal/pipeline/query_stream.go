package pipeline

import (
	"context"
	"errors"

	"github.com/Victoroide/quarry-rag/internal/llm"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

// QueryStream receives context once, followed by incremental answer text.
// Callbacks run synchronously and stop synthesis on their first error.
type QueryStream struct {
	Context func() error
	Delta   func(string) error
}

func (q *QueryStream) start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.Context == nil || q.Delta == nil {
		return errors.New("pipeline: stream callbacks are required")
	}
	return q.Context()
}

func streamNoEvidence(ctx context.Context, qc *QueryContext) error {
	if qc.Stream == nil {
		return nil
	}
	if err := qc.Stream.start(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return qc.Stream.Delta(qc.FinalAnswer)
}

func (s *SynthesisStage) converse(ctx context.Context, qc *QueryContext, model, system string, messages []brtypes.Message, temperature float32, maxTokens int) (*llm.ConverseRoutedResult, error) {
	var result *llm.ConverseRoutedResult
	var err error
	if qc.Stream == nil {
		result, err = s.ambassador.ConverseRoutedPlain(ctx, model, system, messages, temperature, maxTokens)
	} else {
		if startErr := qc.Stream.start(ctx); startErr != nil {
			return nil, startErr
		}
		result, err = s.ambassador.ConverseStreamRoutedPlain(ctx, model, system, messages, temperature, maxTokens, qc.Stream.Delta)
	}
	if err != nil {
		return nil, err
	}
	if result == nil || result.Output == nil {
		return nil, ErrSynthesisEmpty
	}
	return result, nil
}
