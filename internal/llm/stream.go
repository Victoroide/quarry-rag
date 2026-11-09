package llm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

// StreamConverser emits synthesis text incrementally and returns final usage.
// The callback runs synchronously; its first error stops and closes the stream.
type StreamConverser interface {
	ModelConverser
	ConverseStreamWithModelAndSystem(ctx context.Context, modelID, systemPrompt string, messages []brtypes.Message, temperature float32, maxTokens int, delta func(string) error) (*bedrockruntime.ConverseOutput, error)
}

// ConverseStreamRoutedPlain streams a synthesis request with per-call settings.
func (a *Ambassador) ConverseStreamRoutedPlain(ctx context.Context, modelID, systemPrompt string, messages []brtypes.Message, temperature float32, maxTokens int, delta func(string) error) (*ConverseRoutedResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if delta == nil {
		return nil, errors.New("llm: stream callback is required")
	}
	c, ok := a.client.(StreamConverser)
	if !ok {
		return nil, errors.New("llm: synthesis streaming is unavailable")
	}
	out, err := c.ConverseStreamWithModelAndSystem(ctx, modelID, systemPrompt, messages, temperature, maxTokens, delta)
	if err != nil {
		return nil, fmt.Errorf("ambassador: stream: %w", err)
	}
	return &ConverseRoutedResult{Output: out, Text: ExtractText(out), ModelID: modelID}, nil
}

// ConverseStreamWithModelAndSystem streams Bedrock text with per-call settings.
func (c *Client) ConverseStreamWithModelAndSystem(ctx context.Context, modelID, systemPrompt string, messages []brtypes.Message, temperature float32, maxTokens int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if delta == nil {
		return nil, errors.New("llm: stream callback is required")
	}
	maxToks := c.maxToks
	switch {
	case maxTokens > math.MaxInt32:
		return nil, fmt.Errorf("llm: output token limit exceeds %d", math.MaxInt32)
	case maxTokens > 0:
		maxToks = int32(maxTokens)
	}
	topP := extractionTopP
	input := &bedrockruntime.ConverseStreamInput{
		ModelId: aws.String(modelID), Messages: messages,
		InferenceConfig: &brtypes.InferenceConfiguration{Temperature: &temperature, TopP: &topP, MaxTokens: &maxToks},
	}
	if systemPrompt != "" {
		input.System = []brtypes.SystemContentBlock{&brtypes.SystemContentBlockMemberText{Value: systemPrompt}}
	}
	if c.guardrailConfig != nil {
		input.GuardrailConfig = &brtypes.GuardrailStreamConfiguration{
			GuardrailIdentifier:  c.guardrailConfig.GuardrailIdentifier,
			GuardrailVersion:     c.guardrailConfig.GuardrailVersion,
			StreamProcessingMode: brtypes.GuardrailStreamProcessingModeSync,
		}
	}
	out, err := c.rt.ConverseStream(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("llm: converse stream (model=%s): %w", modelID, err)
	}
	if out == nil || out.GetStream() == nil {
		return nil, errors.New("llm: missing response stream")
	}
	return readConverseStream(ctx, out.GetStream(), delta)
}

type converseStream interface {
	Events() <-chan brtypes.ConverseStreamOutput
	Err() error
	Close() error
}

func readConverseStream(ctx context.Context, stream converseStream, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	defer func() { _ = stream.Close() }()
	out := &bedrockruntime.ConverseOutput{}
	var text strings.Builder
	stopped, metadata := false, false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case event, ok := <-stream.Events():
			if !ok {
				if err := stream.Err(); err != nil {
					return nil, fmt.Errorf("llm: read stream: %w", err)
				}
				if !stopped || !metadata {
					return nil, errors.New("llm: incomplete response stream")
				}
				out.Output = &brtypes.ConverseOutputMemberMessage{Value: brtypes.Message{Role: brtypes.ConversationRoleAssistant, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: text.String()}}}}
				return out, nil
			}
			switch e := event.(type) {
			case *brtypes.ConverseStreamOutputMemberContentBlockDelta:
				if d, ok := e.Value.Delta.(*brtypes.ContentBlockDeltaMemberText); ok && d.Value != "" {
					if err := delta(d.Value); err != nil {
						return nil, err
					}
					text.WriteString(d.Value)
				}
			case *brtypes.ConverseStreamOutputMemberMessageStop:
				out.StopReason, stopped = e.Value.StopReason, true
			case *brtypes.ConverseStreamOutputMemberMetadata:
				out.Usage, metadata = e.Value.Usage, e.Value.Usage != nil
			}
		}
	}
}

// ConverseStreamWithModelAndSystem streams the offline answer one word at a time.
func (f *FakeConverser) ConverseStreamWithModelAndSystem(ctx context.Context, _, _ string, messages []brtypes.Message, _ float32, _ int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if delta == nil {
		return nil, errors.New("llm: stream callback is required")
	}
	out := f.synthesize(messages)
	text := ExtractText(out)
	start, inWord := 0, false
	for i, r := range text {
		if unicode.IsSpace(r) {
			inWord = false
			continue
		}
		if !inWord && i > start {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := delta(text[start:i]); err != nil {
				return nil, err
			}
			start = i
		}
		inWord = true
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if start < len(text) {
		if err := delta(text[start:]); err != nil {
			return nil, err
		}
	}
	out.StopReason = brtypes.StopReasonEndTurn
	return out, nil
}
