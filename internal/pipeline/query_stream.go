package pipeline

import (
	"context"
	"errors"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
)

// QueryStream receives context once, followed by incremental answer text.
// Callbacks run synchronously and stop synthesis on their first error.
type QueryStream struct {
	Context func(context.Context) error
	Delta   func(context.Context, string) error
}

func (q *QueryStream) start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.Context == nil || q.Delta == nil {
		return errors.New("pipeline: stream callbacks are required")
	}
	return q.Context(ctx)
}

func (s *SynthesisStage) finishWithoutEvidence(ctx context.Context, qc *QueryContext) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	qc.FinishReason = "no_evidence"
	qc.FinalAnswer, qc.ContentLanguage = s.noEvidenceCatalog.answer(qc.AcceptLanguage)
	qc.ContextTokens, qc.ContextChunks = 0, 0
	qc.CountMethod, qc.ModelInputTokens = "", 0
	qc.TokenUsage = domain.TokenUsage{}
	qc.EarlyExit = true
	if qc.Stream == nil {
		return nil
	}
	if err := qc.Stream.start(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return qc.Stream.Delta(ctx, qc.FinalAnswer)
}

func (s *SynthesisStage) converse(ctx context.Context, qc *QueryContext, request llm.Request) (*llm.Response, error) {
	if qc.Stream != nil {
		if err := qc.Stream.start(ctx); err != nil {
			return nil, err
		}
	}
	ctx = qc.withProviderEvidenceCheck(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := domain.CheckEvidenceBeforeSend(ctx); err != nil {
		return nil, err
	}
	var result *llm.Response
	var err error
	if qc.Stream == nil {
		result, err = s.ambassador.Generate(ctx, request)
	} else {
		result, err = s.ambassador.Stream(ctx, request, func(delta string) error {
			return qc.Stream.Delta(ctx, delta)
		})
	}
	if guardErr := domain.RecordedEvidenceCheckError(ctx); guardErr != nil {
		return nil, guardErr
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrSynthesisEmpty
	}
	return result, nil
}

// withProviderEvidenceCheck attaches the current evidence scope check unless
// the caller already carries a check that includes it, as strict synthesis does.
func (qc *QueryContext) withProviderEvidenceCheck(ctx context.Context) context.Context {
	if qc.checksCurrentEvidence() && !domain.HasEvidenceCheck(ctx) {
		return domain.WithEvidenceCheck(ctx, qc.CheckEvidenceScope)
	}
	return ctx
}
