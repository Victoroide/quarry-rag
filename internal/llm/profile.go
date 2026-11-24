package llm

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/config"
)

// CountMethod identifies how complete synthesis model inputs are measured.
type CountMethod string

// Supported synthesis input counting methods.
const (
	CountMethodBedrockCountTokens CountMethod = "bedrock_count_tokens"
	CountMethodOpenAIO200kChat    CountMethod = "openai_o200k_chat"
	CountMethodOpenAICl100kChat   CountMethod = "openai_cl100k_chat"
)

// Typed capability and input-counting errors.
var (
	ErrUnsupportedProfile    = errors.New("llm: synthesis input profile unsupported for model")
	ErrUnsupportedRegion     = errors.New("llm: synthesis input counting unsupported in region")
	ErrUnsupportedInputShape = errors.New("llm: synthesis input shape unsupported")
	ErrInvalidTokenCount     = errors.New("llm: provider returned invalid token count")
	ErrCountAttemptsExceeded = errors.New("llm: remote token count attempt ceiling exceeded")
	ErrProviderCountFailed   = errors.New("llm: provider token count failed")
)

// ModelProfile declares context limits and counting support for an exact routed model ID.
type ModelProfile struct {
	ModelID          string
	Provider         string
	ContextWindow    int
	OutputReserve    int
	MaxOutputTokens  int
	CountMethod      CountMethod
	TiktokenEncoding string
	SupportedRegions map[string]struct{}
}

// IsRemote reports whether counting this profile invokes a remote provider endpoint.
func (p ModelProfile) IsRemote() bool {
	return p.CountMethod == CountMethodBedrockCountTokens
}

func bedrockProfile(modelID string, window, maxOutput int, regions ...string) ModelProfile {
	supported := make(map[string]struct{}, len(regions))
	for _, region := range regions {
		supported[region] = struct{}{}
	}
	return ModelProfile{
		ModelID: modelID, Provider: "bedrock", ContextWindow: window, OutputReserve: 1024,
		MaxOutputTokens: maxOutput, CountMethod: CountMethodBedrockCountTokens, SupportedRegions: supported,
	}
}

func openAIChatProfile(modelID string, contextWindow, maxOutput int, encoding string, method CountMethod) ModelProfile {
	return ModelProfile{
		ModelID:          modelID,
		Provider:         "openai",
		ContextWindow:    contextWindow,
		OutputReserve:    1024,
		MaxOutputTokens:  maxOutput,
		CountMethod:      method,
		TiktokenEncoding: encoding,
	}
}

var synthesisProfiles = map[string]ModelProfile{
	// Bedrock deployments must pass a startup probe using the configured
	// CountTokens input shape in the selected source Region.
	"amazon.nova-micro-v1:0":                      bedrockProfile("amazon.nova-micro-v1:0", 128_000, 10_000, "us-east-1", "eu-west-2", "ap-southeast-2"),
	"us.anthropic.claude-haiku-4-5-20251001-v1:0": bedrockProfile("us.anthropic.claude-haiku-4-5-20251001-v1:0", 200_000, 64_000, "us-east-1", "us-east-2", "us-west-1", "us-west-2", "ca-central-1"),

	// An exact revision is necessary, along with a separately matched
	// deployment that implements the ChatML v1 text template.
	"gpt-4o-2024-08-06":      openAIChatProfile("gpt-4o-2024-08-06", 128_000, 16_384, "o200k_base", CountMethodOpenAIO200kChat),
	"gpt-4o-mini-2024-07-18": openAIChatProfile("gpt-4o-mini-2024-07-18", 128_000, 16_384, "o200k_base", CountMethodOpenAIO200kChat),
	"gpt-4-0613":             openAIChatProfile("gpt-4-0613", 8192, 4096, "cl100k_base", CountMethodOpenAICl100kChat),
}

// LookupModelProfile returns the declared input profile for an exact provider and model ID.
func LookupModelProfile(provider, modelID string) (ModelProfile, bool) {
	if provider == "" {
		provider = "bedrock"
	}
	profile, ok := synthesisProfiles[modelID]
	if !ok || profile.Provider != provider {
		return ModelProfile{}, false
	}
	return profile, true
}

func resolveProfileForConfig(cfg *config.Config, modelID, clientRegion string) (ModelProfile, error) {
	if cfg == nil {
		return ModelProfile{}, fmt.Errorf("%w: configuration is required", ErrUnsupportedProfile)
	}
	if cfg.SynthesisInputProfile != "strict" {
		return ModelProfile{}, fmt.Errorf("%w: %q", ErrUnsupportedProfile, cfg.SynthesisInputProfile)
	}
	if cfg.FakeMode {
		return ModelProfile{}, fmt.Errorf("%w: offline fake mode cannot verify synthesis input budgets", ErrUnsupportedProfile)
	}
	provider := cfg.LLMProvider
	if provider == "" {
		provider = "bedrock"
	}
	if provider != "bedrock" && provider != "openai" {
		return ModelProfile{}, fmt.Errorf("%w: provider %s", ErrUnsupportedProfile, provider)
	}
	profile, ok := LookupModelProfile(provider, modelID)
	if !ok {
		return ModelProfile{}, fmt.Errorf("%w: %s/%s", ErrUnsupportedProfile, provider, modelID)
	}
	if provider == "openai" {
		if cfg.BedrockGuardrailID != "" || cfg.BedrockGuardrailVersion != "" {
			return ModelProfile{}, fmt.Errorf("%w: Bedrock guardrails are unsupported on openai", ErrUnsupportedInputShape)
		}
		base := normalizeOpenAIBase(cfg.OpenAIBaseURL)
		declaration := "chatml-v1|" + modelID + "|" + base
		if base == "" || modelID != cfg.OpenAIChatModel || cfg.SynthesisOpenAIDeployment != declaration {
			return ModelProfile{}, fmt.Errorf("%w: OpenAI endpoint and model revision require an explicit chatml-v1 deployment match", ErrUnsupportedProfile)
		}
	} else {
		region := cfg.AWSRegion
		if clientRegion != "" && region != "" && clientRegion != region {
			return ModelProfile{}, fmt.Errorf("%w: configured region %s differs from client region %s", ErrUnsupportedRegion, region, clientRegion)
		}
		if region == "" {
			region = clientRegion
		}
		if _, ok := profile.SupportedRegions[region]; !ok {
			return ModelProfile{}, fmt.Errorf("%w: %s for %s", ErrUnsupportedRegion, region, modelID)
		}
	}
	return profile, nil
}

// ValidateSynthesisProfile checks the configuration and prepares its local tokenizer.
func ValidateSynthesisProfile(cfg *config.Config) error {
	if cfg == nil || cfg.SynthesisInputProfile == "" {
		return nil
	}
	if cfg.SynthesisInputProfile != "strict" {
		return fmt.Errorf("%w: %q", ErrUnsupportedProfile, cfg.SynthesisInputProfile)
	}
	if cfg.FakeMode {
		return fmt.Errorf("%w: offline fake mode cannot verify synthesis input budgets", ErrUnsupportedProfile)
	}
	if cfg.LLMProvider == "openai" {
		profile, err := resolveProfileForConfig(cfg, cfg.OpenAIChatModel, "")
		if err != nil {
			return err
		}
		_, err = cachedTiktoken(profile.TiktokenEncoding)
		return err
	}
	for _, modelID := range synthesisBedrockModels(cfg) {
		if _, err := resolveProfileForConfig(cfg, modelID, cfg.AWSRegion); err != nil {
			return err
		}
	}
	return nil
}

func synthesisBedrockModels(cfg *config.Config) []string {
	// Prompt framing and the question can also trigger the context-limit route.
	if (cfg.SynthesisModel == "primary" || cfg.SynthesisModel == "hybrid") && cfg.BedrockPrimaryModel != cfg.BedrockFallbackModel {
		return []string{cfg.BedrockPrimaryModel, cfg.BedrockFallbackModel}
	}
	return []string{cfg.BedrockFallbackModel}
}

func normalizeOpenAIBase(base string) string { return strings.TrimRight(base, "/") }
