package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookupModelProfileExactMatch(t *testing.T) {
	for _, tc := range []struct {
		provider, model string
		wantMethod      CountMethod
		wantWindow      int
		wantOK          bool
	}{
		{"bedrock", "us.anthropic.claude-haiku-4-5-20251001-v1:0", CountMethodBedrockCountTokens, 200_000, true},
		{"bedrock", "amazon.nova-micro-v1:0", CountMethodBedrockCountTokens, 128_000, true},
		{"bedrock", "amazon.nova-pro-v1:0", "", 0, false},
		{"bedrock", "anthropic.claude-sonnet-4-5-20250929-v1:0", "", 0, false},
		{"bedrock", "us.amazon.nova-pro-v1:0", "", 0, false},
		{"bedrock", "gpt-4o-2024-08-06", "", 0, false},
		{"openai", "gpt-4o-2024-08-06", CountMethodOpenAIO200kChat, 128_000, true},
		{"openai", "gpt-4o-mini-2024-07-18", CountMethodOpenAIO200kChat, 128_000, true},
		{"openai", "gpt-4-0613", CountMethodOpenAICl100kChat, 8192, true},
		{"openai", "gpt-4o", "", 0, false},
		{"openai", "gpt-4", "", 0, false},
		{"openai", "gpt-4o-custom", "", 0, false},
		{"openai", "chat", "", 0, false},
	} {
		t.Run(tc.provider+"/"+tc.model, func(t *testing.T) {
			profile, ok := LookupModelProfile(tc.provider, tc.model)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantMethod, profile.CountMethod)
				require.Equal(t, tc.wantWindow, profile.ContextWindow)
				require.Positive(t, profile.OutputReserve)
			}
		})
	}
}

func TestValidateSynthesisProfile(t *testing.T) {
	srv := httptest.NewServer(nil)
	t.Cleanup(srv.Close)
	baseBedrock := func() *config.Config {
		return &config.Config{
			LLMProvider:           "bedrock",
			AWSRegion:             "us-east-1",
			SynthesisModel:        "fallback",
			SynthesisInputProfile: "strict",
			BedrockPrimaryModel:   "amazon.nova-micro-v1:0",
			BedrockFallbackModel:  "us.anthropic.claude-haiku-4-5-20251001-v1:0",
		}
	}
	require.NoError(t, ValidateSynthesisProfile(baseBedrock()))

	cfg := baseBedrock()
	cfg.SynthesisInputProfile = ""
	cfg.BedrockFallbackModel = "us.meta.llama4-maverick-17b-instruct-v1:0"
	require.NoError(t, ValidateSynthesisProfile(cfg))

	for _, tc := range []struct {
		name   string
		mutate func(*config.Config)
		want   error
	}{
		{"fake mode", func(c *config.Config) { c.FakeMode = true }, ErrUnsupportedProfile},
		{"unsupported fallback", func(c *config.Config) { c.BedrockFallbackModel = "us.meta.llama4-maverick-17b-instruct-v1:0" }, ErrUnsupportedProfile},
		{"unsupported primary", func(c *config.Config) {
			c.SynthesisModel = "primary"
			c.BedrockPrimaryModel = "unsupported"
		}, ErrUnsupportedProfile},
		{"hybrid checks both", func(c *config.Config) {
			c.SynthesisModel = "hybrid"
			c.BedrockPrimaryModel = "unsupported"
		}, ErrUnsupportedProfile},
		{"cross region only", func(c *config.Config) { c.BedrockFallbackModel = "anthropic.claude-sonnet-4-5-20250929-v1:0" }, ErrUnsupportedProfile},
		{"primary route overflow from framing reaches fallback", func(c *config.Config) {
			c.SynthesisModel = "primary"
			c.SynthesisContextTokens = 59999
			c.BedrockFallbackModel = "unknown"
		}, ErrUnsupportedProfile},
		{"unsupported region", func(c *config.Config) { c.AWSRegion = "us-gov-west-1" }, ErrUnsupportedRegion},
		{"model-specific region", func(c *config.Config) {
			c.AWSRegion = "ap-southeast-3"
			c.BedrockFallbackModel = "amazon.nova-micro-v1:0"
		}, ErrUnsupportedRegion},
		{"empty region", func(c *config.Config) { c.AWSRegion = "" }, ErrUnsupportedRegion},
		{"openai supported", func(c *config.Config) {
			c.LLMProvider = "openai"
			c.OpenAIChatModel = "gpt-4o-2024-08-06"
			c.OpenAIBaseURL = srv.URL
			c.SynthesisOpenAIDeployment = "chatml-v1|gpt-4o-2024-08-06|" + srv.URL
		}, nil},
		{"openai unsupported model", func(c *config.Config) {
			c.LLMProvider = "openai"
			c.OpenAIChatModel = "local-llama"
		}, ErrUnsupportedProfile},
		{"openai with guardrail", func(c *config.Config) {
			c.LLMProvider = "openai"
			c.OpenAIChatModel = "gpt-4o-2024-08-06"
			c.OpenAIBaseURL = srv.URL
			c.SynthesisOpenAIDeployment = "chatml-v1|gpt-4o-2024-08-06|" + srv.URL
			c.BedrockGuardrailID = "guardrail"
		}, ErrUnsupportedInputShape},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := baseBedrock()
			tc.mutate(c)
			err := ValidateSynthesisProfile(c)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}

func TestCountOpenAIChatTokensGolden(t *testing.T) {
	system := "Answer strictly.\n<excerpt-0123456789abcdef id=\"N\">"
	user := "<question>¿Cuál es el horario del archivo?</question>\n\n<excerpt-0123456789abcdef id=\"1\">\n日本語テスト\tline2\n</excerpt-0123456789abcdef>\n\nAnswer."
	// Reference counts use tiktoken 0.14.0 encode_ordinary and the two-message ChatML v1 frame.
	for _, tc := range []struct {
		model  string
		method CountMethod
		want   int
	}{
		{"gpt-4o-2024-08-06", CountMethodOpenAIO200kChat, 69},
		{"gpt-4-0613", CountMethodOpenAICl100kChat, 73},
	} {
		t.Run(tc.model, func(t *testing.T) {
			req := Request{Model: tc.model, System: system, MaxTokens: 1024, Messages: []Message{{Role: "user", Content: []Content{{Text: user}}}}}
			got, method, err := CountOpenAIChatTokens(tc.model, req)
			require.NoError(t, err)
			require.Equal(t, tc.method, method)
			require.Equal(t, tc.want, got)
			req.Messages[0].Content[0].Text += "\n" + strings.Repeat("\u088f.com", 1000)
			got, _, err = CountOpenAIChatTokens(tc.model, req)
			require.NoError(t, err)
			require.Equal(t, tc.want+5000, got)
		})
	}

	_, _, err := CountOpenAIChatTokens("gpt-4o-2024-08-06", Request{
		Model:    "gpt-4o",
		Messages: []Message{{Role: "user", Content: []Content{{Text: "q", Qualifier: "query"}}}},
	})
	require.ErrorIs(t, err, ErrUnsupportedInputShape)

	_, _, err = CountOpenAIChatTokens("unknown-chat", Request{
		Model:    "unknown-chat",
		Messages: []Message{{Role: "user", Content: []Content{{Text: "q"}}}},
	})
	require.ErrorIs(t, err, ErrUnsupportedProfile)
}

func TestBedrockCountTokensWireAndAttemptCeiling(t *testing.T) {
	t.Run("serializes system and guard blocks", func(t *testing.T) {
		var captured map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/model/us.anthropic.claude-haiku-4-5-20251001-v1:0/count-tokens", r.URL.Path)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&captured))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"inputTokens":142}`))
		}))
		defer srv.Close()

		cfg := &config.Config{
			AWSRegion:             "us-east-1",
			AWSAccessKeyID:        "test-key",
			AWSSecretKey:          "test-secret",
			AWSMaxAttempts:        4,
			BedrockFallbackModel:  "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			SynthesisInputProfile: "strict",
		}
		client, err := NewClient(t.Context(), cfg)
		require.NoError(t, err)
		client.rt = bedrockruntime.NewFromConfig(aws.Config{
			Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		}, func(o *bedrockruntime.Options) { o.BaseEndpoint = aws.String(srv.URL) })

		amb := NewAmbassador(cfg)
		amb.SetClient(client)
		budget := NewCountBudget(MaxRemoteCountAttempts)
		res, err := amb.CountInput(t.Context(), cfg, Request{
			Model:  "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			System: "sys instruction",
			Messages: []Message{{
				Role: "user",
				Content: []Content{
					{Text: "<question>Q</question>\n", Qualifier: "query"},
					{Text: "\n<excerpt-abc id=\"1\">\nE\n</excerpt-abc>\n", Qualifier: "grounding_source"},
					{Text: "\nAnswer."},
				},
			}},
		}, budget)
		require.NoError(t, err)
		require.Equal(t, 142, res.InputTokens)
		require.Equal(t, CountMethodBedrockCountTokens, res.Method)
		require.Equal(t, 1, res.Attempts)
		require.Equal(t, 2, budget.Remaining())

		inputObj, ok := captured["input"].(map[string]any)
		require.True(t, ok)
		converse, ok := inputObj["converse"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, []any{map[string]any{"text": "sys instruction"}}, converse["system"])
		msgs := converse["messages"].([]any)
		require.Len(t, msgs, 1)
		blocks := msgs[0].(map[string]any)["content"].([]any)
		require.Len(t, blocks, 3)
		require.Equal(t, map[string]any{"guardContent": map[string]any{"text": map[string]any{
			"text": "<question>Q</question>\n", "qualifiers": []any{"query"},
		}}}, blocks[0])
		require.Equal(t, map[string]any{"guardContent": map[string]any{"text": map[string]any{
			"text": "\n<excerpt-abc id=\"1\">\nE\n</excerpt-abc>\n", "qualifiers": []any{"grounding_source"},
		}}}, blocks[1])
		require.Equal(t, map[string]any{"text": "\nAnswer."}, blocks[2])
	})

	t.Run("caps SDK retries at 3 total attempts per query", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			n := calls.Add(1)
			if n <= 2 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"__type":"ServiceUnavailableException","message":"busy"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"inputTokens":99}`))
		}))
		defer srv.Close()

		cfg := &config.Config{
			AWSRegion:             "us-east-1",
			AWSAccessKeyID:        "test-key",
			AWSSecretKey:          "test-secret",
			AWSMaxAttempts:        8,
			BedrockFallbackModel:  "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			SynthesisInputProfile: "strict",
		}
		client, err := NewClient(t.Context(), cfg)
		require.NoError(t, err)
		client.rt = bedrockruntime.NewFromConfig(aws.Config{
			Region:           "us-east-1",
			Credentials:      credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
			RetryMaxAttempts: 8,
		}, func(o *bedrockruntime.Options) { o.BaseEndpoint = aws.String(srv.URL) })

		amb := NewAmbassador(cfg)
		amb.SetClient(client)
		budget := NewCountBudget(MaxRemoteCountAttempts)
		req := Request{
			Model:    "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			Messages: []Message{{Role: "user", Content: []Content{{Text: "question"}}}},
		}
		res, err := amb.CountInput(t.Context(), cfg, req, budget)
		require.NoError(t, err)
		require.Equal(t, 99, res.InputTokens)
		require.Equal(t, 3, int(calls.Load()))
		require.Equal(t, 0, budget.Remaining())

		_, err = amb.CountInput(t.Context(), cfg, req, budget)
		require.ErrorIs(t, err, ErrCountAttemptsExceeded)
		require.Equal(t, 3, int(calls.Load()))
	})

	t.Run("stops failing retries at 3 attempts", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"__type":"ThrottlingException","message":"secret-host"}`))
		}))
		defer srv.Close()

		cfg := &config.Config{
			AWSRegion:             "us-east-1",
			AWSAccessKeyID:        "test-key",
			AWSSecretKey:          "test-secret",
			AWSMaxAttempts:        10,
			BedrockFallbackModel:  "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			SynthesisInputProfile: "strict",
		}
		client, err := NewClient(t.Context(), cfg)
		require.NoError(t, err)
		client.rt = bedrockruntime.NewFromConfig(aws.Config{
			Region:           "us-east-1",
			Credentials:      credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
			RetryMaxAttempts: 10,
		}, func(o *bedrockruntime.Options) { o.BaseEndpoint = aws.String(srv.URL) })

		amb := NewAmbassador(cfg)
		amb.SetClient(client)
		budget := NewCountBudget(MaxRemoteCountAttempts)
		_, err = amb.CountInput(t.Context(), cfg, Request{
			Model:    "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			Messages: []Message{{Role: "user", Content: []Content{{Text: "question"}}}},
		}, budget)
		require.Error(t, err)
		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, "ThrottlingException", apiErr.ErrorCode())
		require.Equal(t, 3, int(calls.Load()))
		require.Equal(t, 0, budget.Remaining())
	})

	t.Run("maps invalid counts and validation exceptions", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			status int
			body   string
			want   error
		}{
			{"zero count", http.StatusOK, `{"inputTokens":0}`, ErrInvalidTokenCount},
			{"negative count", http.StatusOK, `{"inputTokens":-7}`, ErrInvalidTokenCount},
			{"guard validation", http.StatusBadRequest, `{"__type":"ValidationException","message":"guardContent not supported"}`, ErrUnsupportedInputShape},
			{"generic validation", http.StatusBadRequest, `{"__type":"ValidationException","message":"bad input"}`, ErrProviderCountFailed},
		} {
			t.Run(tc.name, func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()

				cfg := &config.Config{
					AWSRegion:             "us-east-1",
					AWSAccessKeyID:        "test-key",
					AWSSecretKey:          "test-secret",
					BedrockFallbackModel:  "us.anthropic.claude-haiku-4-5-20251001-v1:0",
					SynthesisInputProfile: "strict",
				}
				client, err := NewClient(t.Context(), cfg)
				require.NoError(t, err)
				client.rt = bedrockruntime.NewFromConfig(aws.Config{
					Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
				}, func(o *bedrockruntime.Options) { o.BaseEndpoint = aws.String(srv.URL) })

				amb := NewAmbassador(cfg)
				amb.SetClient(client)
				_, err = amb.CountInput(t.Context(), cfg, Request{
					Model:    "us.anthropic.claude-haiku-4-5-20251001-v1:0",
					Messages: []Message{{Role: "user", Content: []Content{{Text: "q", Qualifier: "query"}}}},
				}, NewCountBudget(MaxRemoteCountAttempts))
				require.ErrorIs(t, err, tc.want)
			})
		}
	})
}

func TestBedrockSynthesisStartupCountsConfiguredConverseShape(t *testing.T) {
	const fallback = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
	for _, tc := range []struct {
		name, guardrail string
		wantErr         error
	}{
		{name: "plain text"},
		{name: "guarded text", guardrail: "guard-id"},
		{name: "unsupported guard blocks on fallback", guardrail: "guard-id", wantErr: ErrUnsupportedInputShape},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var payload struct {
					Input struct {
						Converse struct {
							System   []map[string]string
							Messages []struct {
								Role    string
								Content []map[string]json.RawMessage
							}
						}
					}
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				converse := payload.Input.Converse
				assert.Len(t, converse.System, 1)
				if !assert.Len(t, converse.Messages, 1) {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				msg := converse.Messages[0]
				assert.Equal(t, "user", msg.Role)
				if tc.guardrail == "" && assert.Len(t, msg.Content, 1) {
					assert.Contains(t, string(msg.Content[0]["text"]), "Blue.")
				}
				if tc.guardrail != "" && assert.Len(t, msg.Content, 3) {
					assert.Contains(t, string(msg.Content[0]["guardContent"]), `"qualifiers":["query"]`)
					assert.Contains(t, string(msg.Content[1]["guardContent"]), `"qualifiers":["grounding_source"]`)
					assert.NotEmpty(t, msg.Content[2]["text"])
				}
				if requests.Load() == 1 {
					assert.Equal(t, "/model/amazon.nova-micro-v1:0/count-tokens", r.URL.Path)
				} else {
					assert.Equal(t, "/model/"+fallback+"/count-tokens", r.URL.Path)
					if tc.wantErr != nil {
						w.Header().Set("X-Amzn-Errortype", "ValidationException")
						w.WriteHeader(http.StatusBadRequest)
						_, _ = fmt.Fprint(w, `{"message":"unsupported guard content"}`)
						return
					}
				}
				_, _ = fmt.Fprint(w, `{"inputTokens":142}`)
			}))
			t.Cleanup(server.Close)
			cfg := &config.Config{
				AWSRegion: "us-east-1", SynthesisModel: "hybrid", SynthesisInputProfile: "strict",
				BedrockPrimaryModel: "amazon.nova-micro-v1:0", BedrockFallbackModel: fallback,
				BedrockGuardrailID: tc.guardrail,
			}
			err := countTestClient(server.URL, 3).VerifySynthesisDeployment(t.Context(), cfg)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, ErrUnsupportedProfile)
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, 2, requests.Load())
		})
	}
}

type stubTokenCounter struct {
	neutralAdapterFixture
	count int
	err   error
	calls int
}

func (*stubTokenCounter) AWSRegion() string { return "us-east-1" }

func (s *stubTokenCounter) CountTokensWithMaxAttempts(ctx context.Context, _, _ string, _ []brtypes.Message, _ int) (CountResult, error) {
	s.calls++
	if err := ctx.Err(); err != nil {
		return CountResult{}, err
	}
	return CountResult{InputTokens: s.count, Attempts: 1, Method: CountMethodBedrockCountTokens}, s.err
}

type plainGeneratorStub struct{}

func (plainGeneratorStub) Generate(context.Context, Request) (*Response, error) {
	return &Response{Text: "ok", StopReason: StopEndTurn}, nil
}
func (plainGeneratorStub) Stream(context.Context, Request, func(string) error) (*Response, error) {
	return &Response{Text: "ok", StopReason: StopEndTurn}, nil
}

func TestAmbassadorStrictCapabilityAndDoubles(t *testing.T) {
	cfg := &config.Config{
		LLMProvider:           "bedrock",
		AWSRegion:             "us-east-1",
		BedrockFallbackModel:  "us.anthropic.claude-haiku-4-5-20251001-v1:0",
		SynthesisInputProfile: "strict",
	}
	req := Request{
		Model:    "us.anthropic.claude-haiku-4-5-20251001-v1:0",
		Messages: []Message{{Role: "user", Content: []Content{{Text: "question"}}}},
	}

	// Plain FakeConverser lacks TokenCounter capability -> must fail in strict mode.
	amb := NewAmbassador(cfg)
	amb.SetClient(NewFakeConverser())
	_, err := amb.CountInput(t.Context(), cfg, req, NewCountBudget(MaxRemoteCountAttempts))
	require.ErrorIs(t, err, ErrUnsupportedProfile)

	// Negative count from signed/calibrated double -> ErrInvalidTokenCount.
	double := &stubTokenCounter{count: -3}
	amb.SetClient(double)
	_, err = amb.CountInput(t.Context(), cfg, req, NewCountBudget(MaxRemoteCountAttempts))
	require.ErrorIs(t, err, ErrInvalidTokenCount)

	// Canceled context -> no count call.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	double.calls = 0
	_, err = amb.CountInput(ctx, cfg, req, NewCountBudget(MaxRemoteCountAttempts))
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, double.calls)

	// Deadline exceeded inside double.
	double.err = fmt.Errorf("upstream: %w", context.DeadlineExceeded)
	double.count = 0
	_, err = amb.CountInput(t.Context(), cfg, req, NewCountBudget(MaxRemoteCountAttempts))
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Throttled inside double.
	double.err = fmt.Errorf("upstream: %w", domain.ErrUpstreamThrottled)
	_, err = amb.CountInput(t.Context(), cfg, req, NewCountBudget(MaxRemoteCountAttempts))
	require.ErrorIs(t, err, domain.ErrUpstreamThrottled)

	// OpenAI plain Generator without RequestCounter -> rejected.
	srv := httptest.NewServer(nil)
	t.Cleanup(srv.Close)
	oaiCfg := &config.Config{LLMProvider: "openai", OpenAIBaseURL: srv.URL, OpenAIChatModel: "gpt-4o-2024-08-06", SynthesisInputProfile: "strict", SynthesisOpenAIDeployment: "chatml-v1|gpt-4o-2024-08-06|" + srv.URL}
	oaiAmb := NewAmbassador(oaiCfg)
	oaiAmb.SetGenerator(plainGeneratorStub{})
	_, err = oaiAmb.CountInput(t.Context(), oaiCfg, Request{Model: "gpt-4o-2024-08-06", Messages: []Message{{Role: "user", Content: []Content{{Text: "hi"}}}}}, NewCountBudget(MaxRemoteCountAttempts))
	require.ErrorIs(t, err, ErrUnsupportedProfile)

	// Real OpenAIClient implements RequestCounter via local tiktoken without network calls.
	oaiClient, err := NewOpenAIClient(oaiCfg)
	require.NoError(t, err)
	oaiAmb.SetGenerator(oaiClient)
	res, err := oaiAmb.CountInput(t.Context(), oaiCfg, Request{Model: "gpt-4o-2024-08-06", System: "sys", Messages: []Message{{Role: "user", Content: []Content{{Text: "hi"}}}}}, NewCountBudget(MaxRemoteCountAttempts))
	require.NoError(t, err)
	require.Equal(t, CountMethodOpenAIO200kChat, res.Method)
	require.Positive(t, res.InputTokens)
	require.Zero(t, res.Attempts)

	// Unsupported qualifier shape on Bedrock.
	_, err = amb.CountInput(t.Context(), cfg, Request{
		Model:    "us.anthropic.claude-haiku-4-5-20251001-v1:0",
		Messages: []Message{{Role: "user", Content: []Content{{Text: "hi", Qualifier: "unknown_qualifier"}}}},
	}, NewCountBudget(MaxRemoteCountAttempts))
	require.ErrorIs(t, err, ErrUnsupportedInputShape)
}

func TestBedrockCountHonorsRetryLimitsAndRevalidates(t *testing.T) {
	for _, tc := range []struct {
		configured, want int
	}{{1, 1}, {2, 2}, {6, 3}} {
		t.Run(fmt.Sprint(tc.configured), func(t *testing.T) {
			var sends atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				sends.Add(1)
				w.Header().Set("X-Amzn-Errortype", "ServiceUnavailableException")
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer srv.Close()
			client := countTestClient(srv.URL, tc.configured)
			result, err := client.CountTokensWithMaxAttempts(t.Context(), "us.anthropic.claude-haiku-4-5-20251001-v1:0", "", countTestMessages(), 3)
			require.Error(t, err)
			require.Equal(t, tc.want, result.Attempts)
			require.EqualValues(t, tc.want, sends.Load())
		})
	}

	t.Run("recount shares attempts already used by retry", func(t *testing.T) {
		var sends atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if sends.Add(1) == 2 {
				_, _ = fmt.Fprint(w, `{"inputTokens":142}`)
				return
			}
			w.Header().Set("X-Amzn-Errortype", "ServiceUnavailableException")
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()
		cfg := &config.Config{AWSRegion: "us-east-1", SynthesisInputProfile: "strict"}
		amb := NewAmbassador(cfg)
		amb.SetClient(countTestClient(srv.URL, 6))
		budget := NewCountBudget(3)
		req := Request{Model: "us.anthropic.claude-haiku-4-5-20251001-v1:0", Messages: []Message{{Role: "user", Content: []Content{{Text: "evidence"}}}}}
		first, err := amb.CountInput(t.Context(), cfg, req, budget)
		require.NoError(t, err)
		require.Equal(t, 2, first.Attempts)
		second, err := amb.CountInput(t.Context(), cfg, req, budget)
		require.Error(t, err)
		require.Equal(t, 1, second.Attempts)
		require.Zero(t, budget.Remaining())
		require.EqualValues(t, 3, sends.Load())
	})

	t.Run("visibility loss blocks SDK retry before transmission", func(t *testing.T) {
		var sends atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			sends.Add(1)
			w.Header().Set("X-Amzn-Errortype", "ServiceUnavailableException")
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()
		ctx := domain.WithEvidenceCheck(t.Context(), func(context.Context) error {
			if sends.Load() != 0 {
				return domain.ErrEvidenceCheck
			}
			return nil
		})
		res, err := countTestClient(srv.URL, 3).CountTokensWithMaxAttempts(ctx, "us.anthropic.claude-haiku-4-5-20251001-v1:0", "", countTestMessages(), 3)
		require.ErrorIs(t, err, domain.ErrEvidenceCheck)
		require.Equal(t, 1, res.Attempts)
		require.EqualValues(t, 1, sends.Load())
	})
}

func TestBedrockClaude45GenerationSamplingAndRegionBinding(t *testing.T) {
	const sonnet = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
	requests := make(chan struct {
		path      string
		inference map[string]any
	}, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Inference map[string]any `json:"inferenceConfig"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requests <- struct {
			path      string
			inference map[string]any
		}{r.URL.Path, body.Inference}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	client := countTestClient(srv.URL, 1)
	_, err := client.ConverseWithModelAndSystem(t.Context(), sonnet, "instructions", countTestMessages(), .2, 1024)
	require.Error(t, err)
	_, err = client.ConverseStreamWithModelAndSystem(t.Context(), sonnet, "instructions", countTestMessages(), .2, 1024, func(string) error { return nil })
	require.Error(t, err)
	for _, suffix := range []string{"/converse", "/converse-stream"} {
		req := <-requests
		require.Equal(t, "/model/"+sonnet+suffix, req.path)
		require.Equal(t, map[string]any{"temperature": .2, "maxTokens": 1024.0}, req.inference)
	}
	cfg := &config.Config{AWSRegion: "ap-southeast-3", SynthesisInputProfile: "strict"}
	amb := NewAmbassador(cfg)
	amb.SetClient(client)
	_, err = amb.ResolveInputProfile(cfg, "us.anthropic.claude-haiku-4-5-20251001-v1:0")
	require.ErrorIs(t, err, ErrUnsupportedRegion)
}

func countTestClient(address string, maxAttempts int) *Client {
	rt := bedrockruntime.NewFromConfig(aws.Config{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = maxAttempts
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
			})
		},
	}, func(o *bedrockruntime.Options) { o.BaseEndpoint = aws.String(address) })
	return &Client{rt: rt}
}

func countTestMessages() []brtypes.Message {
	return []brtypes.Message{{Role: brtypes.ConversationRoleUser, Content: []brtypes.ContentBlock{
		&brtypes.ContentBlockMemberText{Value: "evidence"},
	}}}
}
