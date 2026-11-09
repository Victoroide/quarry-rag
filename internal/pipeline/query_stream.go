package pipeline

import (
	"context"
	"errors"

	"github.com/Victoroide/quarry-rag/internal/llm"
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

func (s *SynthesisStage) converse(ctx context.Context, qc *QueryContext, model, system string, messages []llm.Message, temperature float32, maxTokens int) (*llm.Response, error) {
	request := llm.Request{Model: model, System: system, Messages: messages, Temperature: temperature, MaxTokens: maxTokens}
	var result *llm.Response
	var err error
	if qc.Stream == nil {
		result, err = s.ambassador.Generate(ctx, request)
	} else {
		if startErr := qc.Stream.start(ctx); startErr != nil {
			return nil, startErr
		}
		result, err = s.ambassador.Stream(ctx, request, qc.Stream.Delta)
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrSynthesisEmpty
	}
	return result, nil
}
