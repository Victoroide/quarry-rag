package llm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Victoroide/quarry-rag/internal/awsconf"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// MaxRemoteCountAttempts bounds remote token count requests, including SDK retries, per query.
const MaxRemoteCountAttempts = 3

// CountResult holds the measured input token count and remote attempt count.
type CountResult struct {
	InputTokens int
	Attempts    int
	Method      CountMethod
}

// CountBudget tracks remaining remote token count attempts within one query.
type CountBudget struct {
	call      sync.Mutex
	remaining atomic.Int64
}

// NewCountBudget initializes an attempt budget capped at maxAttempts.
func NewCountBudget(maxAttempts int) *CountBudget {
	if maxAttempts < 0 {
		maxAttempts = 0
	}
	b := &CountBudget{}
	b.remaining.Store(int64(maxAttempts))
	return b
}

// Remaining reports the number of remote count attempts still available.
func (b *CountBudget) Remaining() int {
	if b == nil {
		return 0
	}
	if n := b.remaining.Load(); n > 0 {
		return int(n)
	}
	return 0
}

// Consume deducts n attempts from the budget, clamping at zero.
func (b *CountBudget) Consume(n int) bool {
	if b == nil || n <= 0 {
		return n == 0
	}
	for {
		cur := b.remaining.Load()
		if cur < int64(n) {
			b.remaining.CompareAndSwap(cur, 0)
			return false
		}
		if b.remaining.CompareAndSwap(cur, cur-int64(n)) {
			return true
		}
	}
}

// AttemptTokenCounter counts tokens while enforcing a shared per-query attempt limit.
type AttemptTokenCounter interface {
	CountTokensWithMaxAttempts(ctx context.Context, modelID, systemPrompt string, messages []brtypes.Message, maxAttempts int) (CountResult, error)
}

// RequestCounter counts tokens for a provider-neutral Request.
type RequestCounter interface {
	CountRequestInput(ctx context.Context, r Request) (CountResult, error)
}

// OpenAIRequestCounter identifies the counting generator's deployment.
type OpenAIRequestCounter interface {
	RequestCounter
	OpenAIDeployment() (base, model string)
}

// RegionProvider exposes the configured AWS region of a Bedrock client.
type RegionProvider interface {
	AWSRegion() string
}

// ValidateRequestInput validates provider-neutral messages and guard-content shapes.
func ValidateRequestInput(provider string, req Request) error {
	if len(req.Messages) == 0 {
		return fmt.Errorf("%w: at least one message is required", ErrUnsupportedInputShape)
	}
	for _, msg := range req.Messages {
		switch msg.Role {
		case "user", "assistant":
		default:
			return fmt.Errorf("%w: unsupported role %q", ErrUnsupportedInputShape, msg.Role)
		}
		if len(msg.Content) == 0 {
			return fmt.Errorf("%w: message has no content blocks", ErrUnsupportedInputShape)
		}
		for _, block := range msg.Content {
			if block.Qualifier == "" {
				continue
			}
			if provider == "openai" {
				return fmt.Errorf("%w: Bedrock guardrail content is unsupported on openai", ErrUnsupportedInputShape)
			}
			switch block.Qualifier {
			case "query", "grounding_source", "guard_content":
			default:
				return fmt.Errorf("%w: unsupported guard qualifier %q", ErrUnsupportedInputShape, block.Qualifier)
			}
			if block.Text == "" {
				return fmt.Errorf("%w: guard content text cannot be empty", ErrUnsupportedInputShape)
			}
		}
	}
	return nil
}

// ValidateBedrockMessages validates Bedrock Converse message blocks before token counting.
func ValidateBedrockMessages(messages []brtypes.Message) error {
	if len(messages) == 0 {
		return fmt.Errorf("%w: at least one message is required", ErrUnsupportedInputShape)
	}
	for _, msg := range messages {
		if msg.Role != brtypes.ConversationRoleUser && msg.Role != brtypes.ConversationRoleAssistant {
			return fmt.Errorf("%w: unsupported Bedrock role %q", ErrUnsupportedInputShape, msg.Role)
		}
		if len(msg.Content) == 0 {
			return fmt.Errorf("%w: Bedrock message has no content blocks", ErrUnsupportedInputShape)
		}
		for _, block := range msg.Content {
			switch b := block.(type) {
			case *brtypes.ContentBlockMemberText:
				if b == nil {
					return fmt.Errorf("%w: missing Bedrock text block", ErrUnsupportedInputShape)
				}
			case *brtypes.ContentBlockMemberGuardContent:
				if b == nil {
					return fmt.Errorf("%w: missing Bedrock guardContent block", ErrUnsupportedInputShape)
				}
				guarded, ok := b.Value.(*brtypes.GuardrailConverseContentBlockMemberText)
				if !ok || guarded == nil || aws.ToString(guarded.Value.Text) == "" || len(guarded.Value.Qualifiers) == 0 {
					return fmt.Errorf("%w: unsupported guardContent block shape", ErrUnsupportedInputShape)
				}
				for _, q := range guarded.Value.Qualifiers {
					switch q {
					case brtypes.GuardrailConverseContentQualifierQuery,
						brtypes.GuardrailConverseContentQualifierGroundingSource,
						brtypes.GuardrailConverseContentQualifierGuardContent:
					default:
						return fmt.Errorf("%w: unsupported guard qualifier %q", ErrUnsupportedInputShape, q)
					}
				}
			default:
				return fmt.Errorf("%w: unsupported Bedrock content block %T", ErrUnsupportedInputShape, block)
			}
		}
	}
	return nil
}

// CountOpenAIChatTokens counts tokens for an OpenAI chat completion request using
// an explicitly matched model encoding and OpenAI's documented message framing.
func CountOpenAIChatTokens(modelID string, r Request) (int, CountMethod, error) {
	return countOpenAIChatTokens(context.Background(), modelID, r)
}

func countOpenAIChatTokens(ctx context.Context, modelID string, r Request) (int, CountMethod, error) {
	profile, ok := LookupModelProfile("openai", modelID)
	if !ok {
		return 0, "", fmt.Errorf("%w: openai/%s", ErrUnsupportedProfile, modelID)
	}
	if err := ValidateRequestInput("openai", r); err != nil {
		return 0, "", err
	}
	codec, err := cachedTiktoken(profile.TiktokenEncoding)
	if err != nil {
		return 0, "", err
	}
	chatReq, err := (&OpenAIClient{model: modelID}).request(r, false)
	if err != nil {
		return 0, "", fmt.Errorf("%w: %w", ErrUnsupportedInputShape, err)
	}
	const tokensPerMessage = 3
	const replyPrimingTokens = 3
	total := replyPrimingTokens
	memo := make(map[string]int)
	for _, msg := range chatReq.Messages {
		roleTokens, err := codec.count(ctx, msg.Role, memo)
		if err != nil {
			return 0, "", fmt.Errorf("%w: %w", ErrProviderCountFailed, err)
		}
		contentTokens, err := codec.count(ctx, msg.Content, memo)
		if err != nil {
			return 0, "", fmt.Errorf("%w: %w", ErrProviderCountFailed, err)
		}
		total += tokensPerMessage + roleTokens + contentTokens
	}
	return total, profile.CountMethod, nil
}

// CountTokensWithMaxAttempts calls Bedrock CountTokens while bounding SDK HTTP attempts to maxAttempts.
func (c *Client) CountTokensWithMaxAttempts(ctx context.Context, modelID, systemPrompt string, messages []brtypes.Message, maxAttempts int) (CountResult, error) {
	if err := ctx.Err(); err != nil {
		return CountResult{}, err
	}
	if maxAttempts <= 0 {
		return CountResult{}, ErrCountAttemptsExceeded
	}
	if c == nil || c.rt == nil {
		return CountResult{}, fmt.Errorf("%w: Bedrock client is not initialized", ErrUnsupportedProfile)
	}
	if err := ValidateBedrockMessages(messages); err != nil {
		return CountResult{}, err
	}
	converseReq := brtypes.ConverseTokensRequest{Messages: messages}
	if systemPrompt != "" {
		converseReq.System = []brtypes.SystemContentBlock{&brtypes.SystemContentBlockMemberText{Value: systemPrompt}}
	}
	input := &bedrockruntime.CountTokensInput{
		ModelId: aws.String(modelID),
		Input:   &brtypes.CountTokensInputMemberConverse{Value: converseReq},
	}
	var used atomic.Int64
	out, err := c.rt.CountTokens(ctx, input, func(o *bedrockruntime.Options) {
		if limit := o.Retryer.MaxAttempts(); limit > 0 {
			maxAttempts = min(limit, maxAttempts)
		}
		o.RetryMaxAttempts = maxAttempts
		baseHTTP := o.HTTPClient
		// The evidence guard runs first, so a rejected or canceled attempt
		// never consumes one of the paid count attempts.
		o.HTTPClient = awsconf.GuardEvidenceSends(ctx, smithyhttp.ClientDoFunc(func(r *http.Request) (*http.Response, error) {
			if err := r.Context().Err(); err != nil {
				return nil, err
			}
			if int(used.Add(1)) > maxAttempts {
				used.Add(-1)
				return nil, ErrCountAttemptsExceeded
			}
			if baseHTTP == nil {
				return nil, errors.New("llm: missing Bedrock HTTP client")
			}
			return baseHTTP.Do(r)
		}))
	})
	attempts := int(used.Load())
	if err != nil {
		return CountResult{Attempts: attempts, Method: CountMethodBedrockCountTokens}, translateBedrockCountError(err)
	}
	if out == nil || out.InputTokens == nil || *out.InputTokens <= 0 {
		return CountResult{Attempts: attempts, Method: CountMethodBedrockCountTokens}, ErrInvalidTokenCount
	}
	return CountResult{
		InputTokens: int(*out.InputTokens),
		Attempts:    attempts,
		Method:      CountMethodBedrockCountTokens,
	}, nil
}

func translateBedrockCountError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrCountAttemptsExceeded) || errors.Is(err, domain.ErrEvidenceCheck) {
		return err
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "ValidationException" && strings.Contains(strings.ToLower(apiErr.ErrorMessage()), "guard") {
			return fmt.Errorf("%w: %w", ErrUnsupportedInputShape, err)
		}
		return fmt.Errorf("%w: %w", ErrProviderCountFailed, err)
	}
	return fmt.Errorf("%w: %w", ErrProviderCountFailed, err)
}

// VerifySynthesisDeployment tests Converse-form counting on the configured
// regional Runtime client before strict synthesis starts serving.
func (c *Client) VerifySynthesisDeployment(ctx context.Context, cfg *config.Config) error {
	if cfg.SynthesisInputProfile == "" {
		return nil
	}
	const question = "<question>Which color?</question>\n"
	const excerpt = "\n<excerpt-1 id=\"1\">\nBlue.\n</excerpt-1>\n"
	const instruction = "\nAnswer using only the excerpt."
	content := []Content{{Text: question + excerpt + instruction}}
	if cfg.BedrockGuardrailID != "" {
		content = []Content{
			{Text: question, Qualifier: "query"},
			{Text: excerpt, Qualifier: "grounding_source"},
			{Text: instruction},
		}
	}
	messages := bedrockMessages([]Message{{Role: "user", Content: content}})
	for _, modelID := range synthesisBedrockModels(cfg) {
		profile, err := resolveProfileForConfig(cfg, modelID, c.AWSRegion())
		if err != nil {
			return err
		}
		count, err := c.CountTokensWithMaxAttempts(ctx, modelID, "Answer using the provided evidence.", messages, 1)
		if err != nil {
			return fmt.Errorf("%w: verify Bedrock counting for %s in %s: %w", ErrUnsupportedProfile, modelID, c.AWSRegion(), err)
		}
		if count.InputTokens > profile.ContextWindow-profile.OutputReserve {
			return fmt.Errorf("%w: test input exceeds window for %s", ErrUnsupportedProfile, modelID)
		}
	}
	return nil
}

// ResolveInputProfile validates and returns the active ModelProfile for the selected model ID.
func (a *Ambassador) ResolveInputProfile(cfg *config.Config, modelID string) (ModelProfile, error) {
	var region string
	if a != nil {
		if rp, ok := a.client.(RegionProvider); ok {
			region = rp.AWSRegion()
		}
	}
	profile, err := resolveProfileForConfig(cfg, modelID, region)
	if err != nil {
		return ModelProfile{}, err
	}
	if a == nil {
		return ModelProfile{}, fmt.Errorf("%w: synthesis client is unavailable", ErrUnsupportedProfile)
	}
	if profile.Provider == "openai" {
		counter, ok := a.generator.(OpenAIRequestCounter)
		if !ok {
			return ModelProfile{}, fmt.Errorf("%w: generator does not support input counting", ErrUnsupportedProfile)
		}
		if base, model := counter.OpenAIDeployment(); base != normalizeOpenAIBase(cfg.OpenAIBaseURL) || model != modelID {
			return ModelProfile{}, fmt.Errorf("%w: counting generator deployment does not match configuration", ErrUnsupportedProfile)
		}
		return profile, nil
	}
	if a.generator != nil || region == "" {
		return ModelProfile{}, fmt.Errorf("%w: Bedrock counting requires the regional sending adapter", ErrUnsupportedProfile)
	}
	if _, ok := a.client.(ModelConverser); !ok {
		return ModelProfile{}, fmt.Errorf("%w: converser cannot send model-specific input", ErrUnsupportedProfile)
	}
	if _, ok := a.client.(AttemptTokenCounter); !ok {
		return ModelProfile{}, fmt.Errorf("%w: converser cannot bound Bedrock CountTokens attempts", ErrUnsupportedProfile)
	}
	return profile, nil
}

// ValidateSynthesisTransport checks transport capabilities before publishing context.
func (a *Ambassador) ValidateSynthesisTransport(profile ModelProfile, stream bool) error {
	if profile.IsRemote() && stream {
		if _, ok := a.client.(StreamConverser); !ok {
			return fmt.Errorf("%w: Bedrock synthesis streaming is unavailable", ErrUnsupportedProfile)
		}
	}
	return nil
}

// CountInput counts the serialized synthesis request according to its strict model profile.
func (a *Ambassador) CountInput(ctx context.Context, cfg *config.Config, req Request, budget *CountBudget) (CountResult, error) {
	if err := ctx.Err(); err != nil {
		return CountResult{}, err
	}
	profile, err := a.ResolveInputProfile(cfg, req.Model)
	if err != nil {
		return CountResult{}, err
	}
	if shapeErr := ValidateRequestInput(profile.Provider, req); shapeErr != nil {
		return CountResult{}, shapeErr
	}
	if profile.Provider == "openai" {
		rc := a.generator.(RequestCounter)
		res, countErr := rc.CountRequestInput(ctx, req)
		if countErr != nil {
			return CountResult{}, countErr
		}
		if res.InputTokens <= 0 || res.Method != profile.CountMethod || res.Attempts != 0 {
			return CountResult{}, ErrInvalidTokenCount
		}
		return res, nil
	}
	if budget == nil {
		return CountResult{}, ErrCountAttemptsExceeded
	}
	budget.call.Lock()
	defer budget.call.Unlock()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return CountResult{}, ctxErr
	}
	remaining := budget.Remaining()
	if remaining == 0 {
		return CountResult{}, ErrCountAttemptsExceeded
	}
	messages := bedrockMessages(req.Messages)
	if msgErr := ValidateBedrockMessages(messages); msgErr != nil {
		return CountResult{}, msgErr
	}
	res, err := a.client.(AttemptTokenCounter).CountTokensWithMaxAttempts(ctx, req.Model, req.System, messages, remaining)
	if !budget.Consume(res.Attempts) {
		return CountResult{}, ErrCountAttemptsExceeded
	}
	if err != nil {
		return res, err
	}
	if res.Attempts == 0 || res.InputTokens <= 0 || res.Method != profile.CountMethod {
		return CountResult{}, ErrInvalidTokenCount
	}
	return res, nil
}
