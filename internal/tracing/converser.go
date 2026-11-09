package tracing

import (
	"context"
	"fmt"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type converser struct {
	inner           llm.Converser
	provider, model string
}
type modelConverser struct {
	*converser
	innerModel llm.ModelConverser
}

// WrapConverser preserves per-call model selection when the client supports it.
func WrapConverser(inner llm.Converser, provider, model string) llm.Converser {
	c := &converser{inner, provider, model}
	if mc, ok := inner.(llm.ModelConverser); ok {
		if stream, ok := inner.(llm.StreamConverser); ok {
			return &streamModelConverser{&modelConverser{c, mc}, stream}
		}
		return &modelConverser{c, mc}
	}
	return c
}

func (c *converser) Converse(ctx context.Context, messages []brtypes.Message) (*bedrockruntime.ConverseOutput, error) {
	return c.call(ctx, c.model, "", messages, 0, llm.DefaultMaxTokens, func(ctx context.Context) (*bedrockruntime.ConverseOutput, error) {
		return c.inner.Converse(ctx, messages)
	})
}

func (c *modelConverser) ConverseWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int) (*bedrockruntime.ConverseOutput, error) {
	return c.call(ctx, model, system, messages, temperature, maxTokens, func(ctx context.Context) (*bedrockruntime.ConverseOutput, error) {
		return c.innerModel.ConverseWithModelAndSystem(ctx, model, system, messages, temperature, maxTokens)
	})
}

func (c *converser) call(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int, call func(context.Context) (*bedrockruntime.ConverseOutput, error)) (*bedrockruntime.ConverseOutput, error) {
	if maxTokens <= 0 {
		maxTokens = llm.DefaultMaxTokens
	}
	attrs := append(modelAttributes(c.provider, model, "chat"), attribute.Float64("gen_ai.request.temperature", float64(temperature)), attribute.Int("gen_ai.request.max_tokens", maxTokens))
	ctx, span := start(ctx, "chat "+model, "llm", trace.SpanKindClient, attrs...)
	defer span.End()
	capture := span.IsRecording() && ContentCapture(ctx)
	if capture {
		index := 0
		if system != "" {
			span.SetAttributes(attribute.String("gen_ai.prompt.0.role", "system"), attribute.String("gen_ai.prompt.0.content", system))
			index++
		}
		for _, message := range messages {
			var text strings.Builder
			for _, block := range message.Content {
				if value, ok := llm.BlockText(block); ok {
					text.WriteString(value)
				}
			}
			prefix := fmt.Sprintf("gen_ai.prompt.%d.", index)
			span.SetAttributes(attribute.String(prefix+"role", string(message.Role)), attribute.String(prefix+"content", text.String()))
			index++
		}
	}
	output, err := call(ctx)
	RecordError(span, err)
	if output != nil && span.IsRecording() {
		span.SetAttributes(attribute.StringSlice("gen_ai.response.finish_reasons", []string{string(output.StopReason)}))
		if output.Usage != nil {
			input, completion := int64(aws.ToInt32(output.Usage.InputTokens)), int64(aws.ToInt32(output.Usage.OutputTokens))
			span.SetAttributes(attribute.Int64("gen_ai.usage.input_tokens", input), attribute.Int64("gen_ai.usage.prompt_tokens", input), attribute.Int64("gen_ai.usage.output_tokens", completion), attribute.Int64("gen_ai.usage.completion_tokens", completion))
		}
		if capture {
			span.SetAttributes(attribute.String("gen_ai.completion.0.role", "assistant"), attribute.String("gen_ai.completion.0.content", llm.ExtractText(output)))
		}
	}
	return output, err
}
