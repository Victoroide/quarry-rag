package tracing

import (
	"context"

	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type streamModelConverser struct {
	*modelConverser
	innerStream llm.StreamConverser
}

func (c *streamModelConverser) ConverseStreamWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	return c.call(ctx, model, system, messages, temperature, maxTokens, func(ctx context.Context) (*bedrockruntime.ConverseOutput, error) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("gen_ai.request.streaming", true))
		return c.innerStream.ConverseStreamWithModelAndSystem(ctx, model, system, messages, temperature, maxTokens, delta)
	})
}
