package llm

import "context"

// Content is a text block with an optional Bedrock guardrail qualifier.
type Content struct {
	Text      string
	Qualifier string
}

// Message carries provider-neutral conversation content.
type Message struct {
	Role    string
	Content []Content
}

// Request specifies a synthesis call independently of its transport.
type Request struct {
	Model       string
	System      string
	Messages    []Message
	Temperature float32
	MaxTokens   int
}

// StopReason describes why synthesis ended.
type StopReason string

// Stop reasons retain Bedrock compatibility at the synthesis boundary.
const (
	StopEndTurn         StopReason = "end_turn"
	StopSequence        StopReason = "stop_sequence"
	StopMaxTokens       StopReason = "max_tokens"
	StopFiltered        StopReason = "content_filtered"
	StopGuardrail       StopReason = "guardrail_intervened"
	StopContextExceeded StopReason = "model_context_window_exceeded"
)

// Usage counts tokens reported by the provider.
type Usage struct {
	InputTokens  int32
	OutputTokens int32
}

// Response is the final synthesized text and optional token usage.
type Response struct {
	Text       string
	StopReason StopReason
	Usage      *Usage
}

// Generator supports synchronous and incremental synthesis.
// Stream invokes delta synchronously and stops on its first error.
type Generator interface {
	Generate(context.Context, Request) (*Response, error)
	Stream(context.Context, Request, func(string) error) (*Response, error)
}

// SetGenerator selects a provider-neutral synthesis implementation.
func (a *Ambassador) SetGenerator(g Generator) { a.generator = g }

// Generate executes a provider-neutral synthesis request.
func (a *Ambassador) Generate(ctx context.Context, request Request) (*Response, error) {
	return a.synthesisGenerator().Generate(ctx, request)
}

// Stream executes a provider-neutral streaming synthesis request.
func (a *Ambassador) Stream(ctx context.Context, request Request, delta func(string) error) (*Response, error) {
	return a.synthesisGenerator().Stream(ctx, request, delta)
}

func (a *Ambassador) synthesisGenerator() Generator {
	if a.generator != nil {
		return a.generator
	}
	return bedrockGenerator{a}
}
