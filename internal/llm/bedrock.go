package llm

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/awsconf"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

const (
	// DefaultMaxTokens bounds responses when no positive token limit is supplied.
	DefaultMaxTokens = 4096

	extractionTopP float32 = 0.9
)

// Converser abstracts the Converse call for testability.
type Converser interface {
	Converse(ctx context.Context, messages []brtypes.Message) (*bedrockruntime.ConverseOutput, error)
}

// ModelConverser extends Converser with per-call model selection.
type ModelConverser interface {
	Converser
	ConverseWithModelAndSystem(ctx context.Context, modelID, systemPrompt string, messages []brtypes.Message, temperature float32, maxTokens int) (*bedrockruntime.ConverseOutput, error)
}

// Client wraps the AWS Bedrock Runtime client.
type Client struct {
	rt      *bedrockruntime.Client
	region  string
	modelID string
	maxToks int32

	guardrailConfig *brtypes.GuardrailConfiguration
}

// NewClient creates a Bedrock client from the service configuration.
func NewClient(ctx context.Context, cfg *config.Config) (*Client, error) {
	awsCfg, err := awsconf.Load(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("llm: load aws config: %w", err)
	}
	rt := bedrockruntime.NewFromConfig(awsCfg, func(options *bedrockruntime.Options) {
		options.HTTPClient = evaluation.ObserveBedrockHTTP(options.HTTPClient, evaluation.SynthesisCall)
	})
	if cfg.SynthesisInputProfile == "strict" && rt.Options().BaseEndpoint != nil {
		return nil, fmt.Errorf("%w: custom Bedrock runtime endpoint", ErrUnsupportedProfile)
	}
	region := awsCfg.Region
	if region == "" {
		region = cfg.AWSRegion
	}
	var guardrailConfig *brtypes.GuardrailConfiguration
	if cfg.BedrockGuardrailID != "" {
		guardrailConfig = &brtypes.GuardrailConfiguration{
			GuardrailIdentifier: aws.String(cfg.BedrockGuardrailID),
			GuardrailVersion:    aws.String(cfg.BedrockGuardrailVersion),
		}
	}
	return &Client{
		rt:              rt,
		region:          region,
		modelID:         cfg.BedrockFallbackModel,
		maxToks:         DefaultMaxTokens,
		guardrailConfig: guardrailConfig,
	}, nil
}

// AWSRegion returns the configured AWS region for capability verification.
func (c *Client) AWSRegion() string {
	if c == nil {
		return ""
	}
	if c.region != "" {
		return c.region
	}
	if c.rt != nil {
		return c.rt.Options().Region
	}
	return ""
}

// Converse sends a synchronous Converse API call to the configured model.
func (c *Client) Converse(ctx context.Context, messages []brtypes.Message) (*bedrockruntime.ConverseOutput, error) {
	var temp float32
	maxToks := c.maxToks
	input := &bedrockruntime.ConverseInput{
		GuardrailConfig: c.guardrailConfig,
		ModelId:         aws.String(c.modelID),
		Messages:        messages,
		InferenceConfig: inferenceConfig(c.modelID, temp, maxToks),
	}
	output, err := c.rt.Converse(ctx, input, func(o *bedrockruntime.Options) {
		o.HTTPClient = awsconf.GuardEvidenceSends(ctx, o.HTTPClient)
	})
	if err != nil {
		return nil, fmt.Errorf("llm: converse: %w", err)
	}
	return output, nil
}

// ConverseWithModelAndSystem sends a request with per-call settings and a system prompt.
func (c *Client) ConverseWithModelAndSystem(ctx context.Context, modelID, systemPrompt string, messages []brtypes.Message, temperature float32, maxTokens int) (*bedrockruntime.ConverseOutput, error) {
	var maxToks int32
	switch {
	case maxTokens <= 0:
		maxToks = c.maxToks
	case maxTokens > math.MaxInt32:
		return nil, fmt.Errorf("llm: output token limit exceeds %d", math.MaxInt32)
	default:
		maxToks = int32(maxTokens)
	}

	input := &bedrockruntime.ConverseInput{
		GuardrailConfig: c.guardrailConfig,
		ModelId:         aws.String(modelID),
		Messages:        messages,
		InferenceConfig: inferenceConfig(modelID, temperature, maxToks),
	}
	if systemPrompt != "" {
		input.System = []brtypes.SystemContentBlock{
			&brtypes.SystemContentBlockMemberText{Value: systemPrompt},
		}
	}
	output, err := c.rt.Converse(ctx, input, func(o *bedrockruntime.Options) {
		o.HTTPClient = awsconf.GuardEvidenceSends(ctx, o.HTTPClient)
	})
	if err != nil {
		return nil, fmt.Errorf("llm: converse (model=%s): %w", modelID, err)
	}
	return output, nil
}

func inferenceConfig(model string, temperature float32, maxTokens int32) *brtypes.InferenceConfiguration {
	inference := &brtypes.InferenceConfiguration{Temperature: &temperature, MaxTokens: &maxTokens}
	// Claude 4.5 accepts either temperature or topP. Inference-profile IDs
	// and foundation model ARNs also end in the revision ID.
	if !strings.HasSuffix(model, "anthropic.claude-sonnet-4-5-20250929-v1:0") &&
		!strings.HasSuffix(model, "anthropic.claude-haiku-4-5-20251001-v1:0") {
		inference.TopP = aws.Float32(extractionTopP)
	}
	return inference
}

// BlockText returns the text carried by a text block or a guarded text block.
func BlockText(block brtypes.ContentBlock) (string, bool) {
	switch b := block.(type) {
	case *brtypes.ContentBlockMemberText:
		return b.Value, true
	case *brtypes.ContentBlockMemberGuardContent:
		if guarded, ok := b.Value.(*brtypes.GuardrailConverseContentBlockMemberText); ok {
			return aws.ToString(guarded.Value.Text), true
		}
	}
	return "", false
}

// ExtractText pulls text from a ConverseOutput.
func ExtractText(output *bedrockruntime.ConverseOutput) string {
	if output == nil || output.Output == nil {
		return ""
	}
	msg, ok := output.Output.(*brtypes.ConverseOutputMemberMessage)
	if !ok {
		return ""
	}
	var b strings.Builder
	for _, block := range msg.Value.Content {
		if text, ok := block.(*brtypes.ContentBlockMemberText); ok {
			b.WriteString(text.Value)
		}
	}
	return b.String()
}
