package llm

import (
	"context"
	"errors"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

type neutralAdapterFixture struct{ t *testing.T }

func (f neutralAdapterFixture) Converse(context.Context, []brtypes.Message) (*bedrockruntime.ConverseOutput, error) {
	return nil, errors.New("per-call settings were lost")
}

func (f neutralAdapterFixture) ConverseWithModelAndSystem(_ context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int) (*bedrockruntime.ConverseOutput, error) {
	f.t.Helper()
	if model != "model" || system != "system" || temperature != .2 || maxTokens != 100 || len(messages) != 1 || messages[0].Role != brtypes.ConversationRoleUser || len(messages[0].Content) != 3 {
		f.t.Fatal("Bedrock request settings changed")
	}
	for i, want := range []brtypes.GuardrailConverseContentQualifier{brtypes.GuardrailConverseContentQualifierQuery, brtypes.GuardrailConverseContentQualifierGroundingSource} {
		guarded, ok := messages[0].Content[i].(*brtypes.ContentBlockMemberGuardContent)
		if !ok {
			f.t.Fatal("guarded text became plain text")
		}
		text, ok := guarded.Value.(*brtypes.GuardrailConverseContentBlockMemberText)
		if !ok || len(text.Value.Qualifiers) != 1 || text.Value.Qualifiers[0] != want {
			f.t.Fatal("guardrail qualifier changed")
		}
	}
	if text, ok := BlockText(messages[0].Content[2]); !ok || text != "instruction" {
		f.t.Fatal("plain instruction changed")
	}
	return &bedrockruntime.ConverseOutput{Output: &brtypes.ConverseOutputMemberMessage{Value: brtypes.Message{Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "answer"}}}}, StopReason: brtypes.StopReasonMaxTokens, Usage: &brtypes.TokenUsage{InputTokens: aws.Int32(7), OutputTokens: aws.Int32(3)}}, nil
}

func (f neutralAdapterFixture) ConverseStreamWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	out, err := f.ConverseWithModelAndSystem(ctx, model, system, messages, temperature, maxTokens)
	if err != nil {
		return nil, err
	}
	if err := delta(ExtractText(out)); err != nil {
		return nil, err
	}
	return out, nil
}

func TestNeutralBedrockAdapter(t *testing.T) {
	amb := NewAmbassador(&config.Config{})
	amb.SetClient(neutralAdapterFixture{t})
	request := Request{Model: "model", System: "system", Temperature: .2, MaxTokens: 100, Messages: []Message{{Role: "user", Content: []Content{{Text: "question", Qualifier: "query"}, {Text: "evidence", Qualifier: "grounding_source"}, {Text: "instruction"}}}}}
	for _, stream := range []bool{false, true} {
		var out *Response
		var err error
		if stream {
			out, err = amb.Stream(t.Context(), request, func(s string) error {
				if s != "answer" {
					t.Error("delta changed")
				}
				return nil
			})
		} else {
			out, err = amb.Generate(t.Context(), request)
		}
		if err != nil || out == nil || out.Text != "answer" || out.StopReason != StopMaxTokens || out.Usage == nil || out.Usage.InputTokens != 7 || out.Usage.OutputTokens != 3 {
			t.Fatalf("adapter changed response: %+v %v", out, err)
		}
	}
}
