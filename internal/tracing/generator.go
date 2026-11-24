package tracing

import (
	"context"
	"fmt"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/llm"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type generator struct {
	inner    llm.Generator
	provider string
}

type countingGenerator struct {
	*generator
	counter llm.RequestCounter
}

// WrapGenerator traces provider-neutral synthesis with optional content capture.
func WrapGenerator(inner llm.Generator, provider string) llm.Generator {
	g := &generator{inner, provider}
	if rc, ok := inner.(llm.RequestCounter); ok {
		counting := &countingGenerator{g, rc}
		if deployment, ok := inner.(llm.OpenAIRequestCounter); ok {
			return &deploymentCountingGenerator{counting, deployment}
		}
		return counting
	}
	return g
}

type deploymentCountingGenerator struct {
	*countingGenerator
	deployment llm.OpenAIRequestCounter
}

func (g *deploymentCountingGenerator) OpenAIDeployment() (base, model string) {
	return g.deployment.OpenAIDeployment()
}

func (g *countingGenerator) CountRequestInput(ctx context.Context, r llm.Request) (llm.CountResult, error) {
	ctx, span := start(ctx, "count_tokens "+r.Model, "tool", trace.SpanKindInternal,
		attribute.String("quarry.synthesis.count_provider", g.provider))
	defer span.End()
	res, err := g.counter.CountRequestInput(ctx, r)
	recordInputCount(span, res, err)
	return res, err
}

func recordInputCount(span trace.Span, res llm.CountResult, err error) {
	RecordError(span, err)
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("quarry.synthesis.count_attempts", res.Attempts))
		if err == nil {
			span.SetAttributes(
				attribute.Int("quarry.synthesis.count_input_tokens", res.InputTokens),
				attribute.String("quarry.synthesis.count_method", string(res.Method)),
			)
		}
	}
}

func (g *generator) Generate(ctx context.Context, r llm.Request) (*llm.Response, error) {
	return g.call(ctx, r, false, func(ctx context.Context) (*llm.Response, error) { return g.inner.Generate(ctx, r) })
}

func (g *generator) Stream(ctx context.Context, r llm.Request, delta func(string) error) (*llm.Response, error) {
	return g.call(ctx, r, true, func(ctx context.Context) (*llm.Response, error) { return g.inner.Stream(ctx, r, delta) })
}

func (g *generator) call(ctx context.Context, r llm.Request, stream bool, call func(context.Context) (*llm.Response, error)) (*llm.Response, error) {
	maxTokens := r.MaxTokens
	if maxTokens <= 0 {
		maxTokens = llm.DefaultMaxTokens
	}
	attrs := append(modelAttributes(g.provider, r.Model, "chat"), attribute.Float64("gen_ai.request.temperature", float64(r.Temperature)), attribute.Int("gen_ai.request.max_tokens", maxTokens))
	if stream {
		attrs = append(attrs, attribute.Bool("gen_ai.request.streaming", true))
	}
	ctx, span := start(ctx, "chat "+r.Model, "llm", trace.SpanKindClient, attrs...)
	defer span.End()
	capture := span.IsRecording() && ContentCapture(ctx)
	if capture {
		index := 0
		if r.System != "" {
			span.SetAttributes(attribute.String("gen_ai.prompt.0.role", "system"), attribute.String("gen_ai.prompt.0.content", r.System))
			index++
		}
		for _, message := range r.Messages {
			var text strings.Builder
			for _, block := range message.Content {
				text.WriteString(block.Text)
			}
			prefix := fmt.Sprintf("gen_ai.prompt.%d.", index)
			span.SetAttributes(attribute.String(prefix+"role", message.Role), attribute.String(prefix+"content", text.String()))
			index++
		}
	}
	out, err := call(ctx)
	RecordError(span, err)
	if out != nil && span.IsRecording() {
		span.SetAttributes(attribute.StringSlice("gen_ai.response.finish_reasons", []string{string(out.StopReason)}))
		if out.Usage != nil {
			input, output := int64(out.Usage.InputTokens), int64(out.Usage.OutputTokens)
			span.SetAttributes(attribute.Int64("gen_ai.usage.input_tokens", input), attribute.Int64("gen_ai.usage.prompt_tokens", input), attribute.Int64("gen_ai.usage.output_tokens", output), attribute.Int64("gen_ai.usage.completion_tokens", output))
		}
		if capture {
			span.SetAttributes(attribute.String("gen_ai.completion.0.role", "assistant"), attribute.String("gen_ai.completion.0.content", out.Text))
		}
	}
	return out, err
}
