package llm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

type bedrockGenerator struct{ ambassador *Ambassador }

func bedrockMessages(messages []Message) []brtypes.Message {
	result := make([]brtypes.Message, 0, len(messages))
	for _, message := range messages {
		blocks := make([]brtypes.ContentBlock, 0, len(message.Content))
		for _, block := range message.Content {
			if block.Qualifier != "" {
				blocks = append(blocks, &brtypes.ContentBlockMemberGuardContent{Value: &brtypes.GuardrailConverseContentBlockMemberText{Value: brtypes.GuardrailConverseTextBlock{Text: aws.String(block.Text), Qualifiers: []brtypes.GuardrailConverseContentQualifier{brtypes.GuardrailConverseContentQualifier(block.Qualifier)}}}})
			} else {
				blocks = append(blocks, &brtypes.ContentBlockMemberText{Value: block.Text})
			}
		}
		result = append(result, brtypes.Message{Role: brtypes.ConversationRole(message.Role), Content: blocks})
	}
	return result
}

func neutralResponse(result *ConverseRoutedResult) *Response {
	if result == nil || result.Output == nil {
		return nil
	}
	out := &Response{Text: result.Text, StopReason: StopReason(result.Output.StopReason)}
	if usage := result.Output.Usage; usage != nil {
		out.Usage = &Usage{InputTokens: aws.ToInt32(usage.InputTokens), OutputTokens: aws.ToInt32(usage.OutputTokens)}
	}
	return out
}

func (b bedrockGenerator) Generate(ctx context.Context, r Request) (*Response, error) {
	result, err := b.ambassador.ConverseRoutedPlain(ctx, r.Model, r.System, bedrockMessages(r.Messages), r.Temperature, r.MaxTokens)
	return neutralResponse(result), err
}

func (b bedrockGenerator) Stream(ctx context.Context, r Request, delta func(string) error) (*Response, error) {
	result, err := b.ambassador.ConverseStreamRoutedPlain(ctx, r.Model, r.System, bedrockMessages(r.Messages), r.Temperature, r.MaxTokens, delta)
	return neutralResponse(result), err
}
