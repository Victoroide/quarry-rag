package grantse2e

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

const Answer = "Amber archive records are kept in the northern cabinet"

// Model records the prompts and offers one cancellable pre-delta barrier.
type Model struct {
	mu      sync.Mutex
	inputs  []string
	barrier *StreamBarrier
}

// StreamBarrier signals receipt of a prompt and waits until released.
type StreamBarrier struct {
	Entered chan struct{}
	Release chan struct{}
}

// PauseNextStream arms a gate for one streaming request.
func (m *Model) PauseNextStream() *StreamBarrier {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.barrier = &StreamBarrier{Entered: make(chan struct{}), Release: make(chan struct{})}
	return m.barrier
}

// Inputs returns a stable snapshot of model inputs.
func (m *Model) Inputs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.inputs)
}

func (m *Model) record(messages []brtypes.Message, stream bool) *StreamBarrier {
	var input strings.Builder
	for _, message := range messages {
		for _, block := range message.Content {
			if value, ok := llm.BlockText(block); ok {
				input.WriteString(value)
				input.WriteByte('\n')
			}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inputs = append(m.inputs, input.String())
	if stream {
		gate := m.barrier
		m.barrier = nil
		return gate
	}
	return nil
}

func (*Model) output() *bedrockruntime.ConverseOutput {
	return &bedrockruntime.ConverseOutput{
		Output: &brtypes.ConverseOutputMemberMessage{Value: brtypes.Message{
			Role:    brtypes.ConversationRoleAssistant,
			Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: Answer + " [1]."}},
		}},
		StopReason: brtypes.StopReasonEndTurn,
		Usage:      &brtypes.TokenUsage{InputTokens: aws.Int32(70), OutputTokens: aws.Int32(12), TotalTokens: aws.Int32(82)},
	}
}

// Converse returns deterministic output for the captured input.
func (m *Model) Converse(ctx context.Context, messages []brtypes.Message) (*bedrockruntime.ConverseOutput, error) {
	return m.ConverseWithModelAndSystem(ctx, "", "", messages, 0, 0)
}

// ConverseWithModelAndSystem implements the production model interface.
func (m *Model) ConverseWithModelAndSystem(_ context.Context, _, _ string, messages []brtypes.Message, _ float32, _ int) (*bedrockruntime.ConverseOutput, error) {
	m.record(messages, false)
	return m.output(), nil
}

// ConverseStreamWithModelAndSystem emits a single controlled delta.
func (m *Model) ConverseStreamWithModelAndSystem(ctx context.Context, _, _ string, messages []brtypes.Message, _ float32, _ int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	if gate := m.record(messages, true); gate != nil {
		close(gate.Entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-gate.Release:
		}
	}
	out := m.output()
	if err := delta(llm.ExtractText(out)); err != nil {
		return nil, err
	}
	return out, nil
}

type fixtureEmbedder struct{}

func (fixtureEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	vector := make([]float32, dimensions)
	vector[0] = 1
	if strings.Contains(input, Answer) {
		vector[1] = .3
	}
	return vector, ctx.Err()
}
