package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

type handlerBudgetConverser struct {
	llm.FakeConverser
	region        string
	countFn       func(context.Context, string, string, []brtypes.Message, int) (llm.CountResult, error)
	converseErr   error
	countCalls    atomic.Int32
	converseCalls atomic.Int32
}

func (h *handlerBudgetConverser) AWSRegion() string { return h.region }

func (h *handlerBudgetConverser) ConverseWithModelAndSystem(ctx context.Context, modelID, systemPrompt string, messages []brtypes.Message, temperature float32, maxTokens int) (*bedrockruntime.ConverseOutput, error) {
	return h.ConverseStreamWithModelAndSystem(ctx, modelID, systemPrompt, messages, temperature, maxTokens, func(string) error { return nil })
}

func (h *handlerBudgetConverser) ConverseStreamWithModelAndSystem(ctx context.Context, _, _ string, _ []brtypes.Message, _ float32, _ int, onDelta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	h.converseCalls.Add(1)
	if h.converseErr != nil {
		return nil, h.converseErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := onDelta("Grounded answer [1]."); err != nil {
		return nil, err
	}
	return &bedrockruntime.ConverseOutput{
		Output:     &brtypes.ConverseOutputMemberMessage{Value: brtypes.Message{Role: brtypes.ConversationRoleAssistant, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "Grounded answer [1]."}}}},
		StopReason: brtypes.StopReasonEndTurn,
		Usage:      &brtypes.TokenUsage{InputTokens: aws.Int32(120), OutputTokens: aws.Int32(6), TotalTokens: aws.Int32(126)},
	}, nil
}

func (h *handlerBudgetConverser) CountTokensWithMaxAttempts(ctx context.Context, modelID, systemPrompt string, messages []brtypes.Message, maxAttempts int) (llm.CountResult, error) {
	h.countCalls.Add(1)
	if h.countFn != nil {
		return h.countFn(ctx, modelID, systemPrompt, messages, maxAttempts)
	}
	return llm.CountResult{InputTokens: 64, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
}

func handlerBedrockStrictConfig() *config.Config {
	return &config.Config{
		LLMProvider:            "bedrock",
		AWSRegion:              "us-east-1",
		BedrockFallbackModel:   "us.anthropic.claude-haiku-4-5-20251001-v1:0",
		SynthesisInputProfile:  "strict",
		SynthesisContextTokens: 20_000,
		SynthesisTimeout:       4 * time.Second,
		QueryTimeout:           5 * time.Second,
		MaxChunksPerQuery:      10,
	}
}

func TestQueryStrictBudgetJSONAndStreamParity(t *testing.T) {
	for _, fixture := range []struct {
		name, question, first, second string
		wantSecondWords               int
	}{
		{"Unicode body", "Summarize alpha", strings.Repeat("\u088f.com", 1000), strings.Repeat("beta ", 4500), 1930},
		{"short later source", "Q?", strings.Repeat("㐀", 2000) + strings.Repeat(" a", 853), strings.TrimSpace(strings.Repeat("beta ", 90)), 80},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			chunks := []domain.ScoredChunk{
				{Chunk: domain.Chunk{ID: "c1", DocumentID: "00000000-0000-4000-8000-000000000001", Text: fixture.first}, Score: .9},
				{Chunk: domain.Chunk{ID: "c2", DocumentID: "00000000-0000-4000-8000-000000000002", Text: fixture.second}, Score: .8},
			}
			type modelRequest struct {
				Model     string `json:"model"`
				Stream    bool   `json:"stream"`
				MaxTokens int    `json:"max_tokens"`
				Messages  []struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				} `json:"messages"`
			}
			requests := make(chan modelRequest, 2)
			oaiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req modelRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				requests <- req
				if req.Stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Grounded [1].\"}}]}\n\n")
					_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":7100,\"completion_tokens\":5}}\n\n")
					_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, `{"choices":[{"index":0,"message":{"content":"Grounded [1]."},"finish_reason":"stop"}],"usage":{"prompt_tokens":7100,"completion_tokens":5}}`)
			}))
			defer oaiServer.Close()

			cfg := &config.Config{
				LLMProvider:               "openai",
				OpenAIBaseURL:             oaiServer.URL,
				OpenAIChatModel:           "gpt-4-0613",
				SynthesisOpenAIDeployment: "chatml-v1|gpt-4-0613|" + oaiServer.URL,
				SynthesisInputProfile:     "strict",
				SynthesisContextTokens:    20_000,
				SynthesisTimeout:          4 * time.Second,
				QueryTimeout:              5 * time.Second,
				MaxChunksPerQuery:         10,
			}
			require.NoError(t, llm.ValidateSynthesisProfile(cfg))
			oaiClient, err := llm.NewOpenAIClient(cfg)
			require.NoError(t, err)
			amb := llm.NewAmbassador(cfg)
			amb.SetGenerator(oaiClient)
			ret := retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10)
			h := QueryHandler(ret, amb, cfg, nil)

			body, err := json.Marshal(QueryRequest{Query: fixture.question, Mode: "global"})
			require.NoError(t, err)
			jsonReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(string(body)))
			jsonReq.Header.Set("Content-Type", "application/json")
			jsonRec := httptest.NewRecorder()
			h.ServeHTTP(jsonRec, jsonReq)
			require.Equal(t, http.StatusOK, jsonRec.Code, jsonRec.Body.String())

			var jsonResp QueryResponse
			require.NoError(t, json.Unmarshal(jsonRec.Body.Bytes(), &jsonResp))
			require.Equal(t, string(llm.CountMethodOpenAICl100kChat), jsonResp.CountMethod)
			require.Positive(t, jsonResp.ModelInputTokens)
			require.LessOrEqual(t, jsonResp.ModelInputTokens+1024, 8192)
			require.Positive(t, jsonResp.ContextTokens)
			require.Equal(t, 2, jsonResp.ContextChunks)

			sseRec := httptest.NewRecorder()
			h.ServeHTTP(sseRec, streamQueryRequest(t.Context(), string(body)))
			require.Equal(t, http.StatusOK, sseRec.Code, sseRec.Body.String())
			events := recordedEvents(t, sseRec.Body.String())
			require.Len(t, events, 3)
			require.Equal(t, "context", events[0].name)
			require.Equal(t, "done", events[2].name)

			var ctxEvent QueryStreamContext
			require.NoError(t, json.Unmarshal(events[0].data["scored_chunks"], &ctxEvent.ScoredChunks))
			require.Equal(t, jsonResp.ScoredChunks, ctxEvent.ScoredChunks)

			var doneEvent QueryStreamDone
			require.NoError(t, json.Unmarshal(bytesOrRaw(events[2].data), &doneEvent))
			require.Equal(t, jsonResp.CountMethod, doneEvent.CountMethod)
			require.Equal(t, jsonResp.ModelInputTokens, doneEvent.ModelInputTokens)
			require.Equal(t, jsonResp.ContextTokens, doneEvent.ContextTokens)

			wireJSON, wireSSE := <-requests, <-requests
			require.False(t, wireJSON.Stream)
			require.True(t, wireSSE.Stream)
			marker := regexp.MustCompile(`(?s)<excerpt-(\d{21}) id="(\d+)">\n(.*?)\n</excerpt-\d{21}>`)
			jsonBodies := marker.FindAllStringSubmatch(wireJSON.Messages[1].Content, -1)
			sseBodies := marker.FindAllStringSubmatch(wireSSE.Messages[1].Content, -1)
			require.Len(t, jsonBodies, 2)
			require.Len(t, sseBodies, 2)
			require.NotEqual(t, jsonBodies[0][1], sseBodies[0][1])
			for i := range jsonBodies {
				require.Equal(t, fmt.Sprint(i+1), jsonBodies[i][2])
				require.Equal(t, jsonBodies[i][2:], sseBodies[i][2:])
			}
			require.Equal(t, fixture.first, jsonBodies[0][3])
			require.Equal(t, strings.TrimSpace(strings.Repeat("beta ", fixture.wantSecondWords)), jsonBodies[1][3])
			for _, wire := range []modelRequest{wireJSON, wireSSE} {
				require.Equal(t, "gpt-4-0613", wire.Model)
				require.Equal(t, 1024, wire.MaxTokens)
				require.Equal(t, "system", wire.Messages[0].Role)
				require.Equal(t, "user", wire.Messages[1].Role)
			}
			// Reference tiktoken counts the complete selected request at 7,168 input tokens.
			require.Equal(t, 8192, jsonResp.ModelInputTokens+wireJSON.MaxTokens)
		})
	}

	t.Run("bedrock strict recutting keeps count calls <= 3 and matches JSON/SSE", func(t *testing.T) {
		cfg := handlerBedrockStrictConfig()
		smallChunks := []domain.ScoredChunk{
			{Chunk: domain.Chunk{ID: "c1", DocumentID: "00000000-0000-4000-8000-000000000001", Index: 0, Text: strings.Repeat("First fact. ", 25)}, Score: .9},
			{Chunk: domain.Chunk{ID: "c2", DocumentID: "00000000-0000-4000-8000-000000000002", Index: 1, Text: strings.Repeat("Second fact. ", 25)}, Score: .8},
		}
		double := &handlerBudgetConverser{
			region: cfg.AWSRegion,
			countFn: func(_ context.Context, _ string, _ string, messages []brtypes.Message, _ int) (llm.CountResult, error) {
				txt := ""
				for _, m := range messages {
					for _, blk := range m.Content {
						if tb, ok := blk.(*brtypes.ContentBlockMemberText); ok {
							txt += tb.Value
						}
					}
				}
				if strings.Contains(txt, `id="2"`) {
					return llm.CountResult{InputTokens: 299_000, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
				}
				return llm.CountResult{InputTokens: 198_500, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
			},
		}
		amb := llm.NewAmbassador(cfg)
		amb.SetClient(double)
		ret := retriever.NewRetriever(citationQueryStore{chunks: smallChunks}, embedder.NewFakeEmbedder(8), .7, 10)
		h := QueryHandler(ret, amb, cfg, nil)

		jsonReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(`{"query":"Facts?","mode":"global"}`))
		jsonReq.Header.Set("Content-Type", "application/json")
		jsonRec := httptest.NewRecorder()
		h.ServeHTTP(jsonRec, jsonReq)
		require.Equal(t, http.StatusOK, jsonRec.Code, jsonRec.Body.String())

		var jsonResp QueryResponse
		require.NoError(t, json.Unmarshal(jsonRec.Body.Bytes(), &jsonResp))
		require.Equal(t, string(llm.CountMethodBedrockCountTokens), jsonResp.CountMethod)
		require.Equal(t, 198_500, jsonResp.ModelInputTokens)
		require.LessOrEqual(t, jsonResp.ModelInputTokens+1024, 200_000)
		require.LessOrEqual(t, int(double.countCalls.Load()), llm.MaxRemoteCountAttempts)

		double.countCalls.Store(0)
		sseRec := httptest.NewRecorder()
		h.ServeHTTP(sseRec, streamQueryRequest(t.Context(), `{"query":"Facts?","mode":"global"}`))
		require.Equal(t, http.StatusOK, sseRec.Code, sseRec.Body.String())
		require.LessOrEqual(t, int(double.countCalls.Load()), llm.MaxRemoteCountAttempts)
		events := recordedEvents(t, sseRec.Body.String())
		require.Len(t, events, 3)

		var ctxChunks []ScoredChunkResponse
		require.NoError(t, json.Unmarshal(events[0].data["scored_chunks"], &ctxChunks))
		require.Equal(t, jsonResp.ScoredChunks, ctxChunks)

		var doneEvent QueryStreamDone
		require.NoError(t, json.Unmarshal(bytesOrRaw(events[2].data), &doneEvent))
		require.Equal(t, jsonResp.CountMethod, doneEvent.CountMethod)
		require.Equal(t, jsonResp.ModelInputTokens, doneEvent.ModelInputTokens)
	})

	t.Run("legacy profile omits count_method and model_input_tokens", func(t *testing.T) {
		cfg := handlerBedrockStrictConfig()
		cfg.SynthesisInputProfile = ""
		h := streamQueryHandler(cfg, true, llm.NewFakeConverser())
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(`{"query":"Archive evidence","mode":"global"}`))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		var raw map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
		require.NotContains(t, raw, "count_method")
		require.NotContains(t, raw, "model_input_tokens")
		require.Contains(t, raw, "context_tokens")
	})
}

func TestQueryStrictBudgetErrorsBeforeFirstSSEFrame(t *testing.T) {
	chunks := []domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: "c1", DocumentID: "00000000-0000-4000-8000-000000000001", Text: "Evidence."}, Score: .9},
	}
	for _, tc := range []struct {
		name       string
		model      string
		timeout    time.Duration
		countFn    func(context.Context, string, string, []brtypes.Message, int) (llm.CountResult, error)
		wantStatus int
		wantError  string
		wantRetry  string
	}{
		{
			name:  "input_too_large maps to 413 before SSE",
			model: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			countFn: func(context.Context, string, string, []brtypes.Message, int) (llm.CountResult, error) {
				return llm.CountResult{InputTokens: 299_500, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
			},
			wantStatus: http.StatusRequestEntityTooLarge,
			wantError:  "input_too_large",
		},
		{
			name:  "count_failed maps to 502 before SSE",
			model: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			countFn: func(context.Context, string, string, []brtypes.Message, int) (llm.CountResult, error) {
				return llm.CountResult{}, fmt.Errorf("%w: status 400", llm.ErrProviderCountFailed)
			},
			wantStatus: http.StatusBadGateway,
			wantError:  "synthesis",
		},
		{
			name:  "negative count maps to 502 before SSE",
			model: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			countFn: func(context.Context, string, string, []brtypes.Message, int) (llm.CountResult, error) {
				return llm.CountResult{InputTokens: -5, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
			},
			wantStatus: http.StatusBadGateway,
			wantError:  "synthesis",
		},
		{
			name:  "throttled count maps to 503 Retry-After 2 before SSE",
			model: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			countFn: func(context.Context, string, string, []brtypes.Message, int) (llm.CountResult, error) {
				return llm.CountResult{}, fmt.Errorf("%w: %w", llm.ErrProviderCountFailed, &smithy.GenericAPIError{Code: "ThrottlingException", Message: "rate limit"})
			},
			wantStatus: http.StatusServiceUnavailable,
			wantError:  "upstream_throttled",
			wantRetry:  "2",
		},
		{
			name:    "count deadline exceeded maps to 504 before SSE",
			model:   "us.anthropic.claude-haiku-4-5-20251001-v1:0",
			timeout: 15 * time.Millisecond,
			countFn: func(ctx context.Context, _ string, _ string, _ []brtypes.Message, _ int) (llm.CountResult, error) {
				<-ctx.Done()
				return llm.CountResult{}, ctx.Err()
			},
			wantStatus: http.StatusGatewayTimeout,
			wantError:  "timeout",
		},
		{
			name:       "unsupported model in strict mode maps to 500 pipeline error before SSE",
			model:      "anthropic.claude-sonnet-4-5-20250929-v1:0",
			wantStatus: http.StatusInternalServerError,
			wantError:  "pipeline",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := handlerBedrockStrictConfig()
			cfg.BedrockFallbackModel = tc.model
			if tc.timeout > 0 {
				cfg.SynthesisTimeout = tc.timeout
			}
			double := &handlerBudgetConverser{region: cfg.AWSRegion, countFn: tc.countFn}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(double)
			ret := retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10)
			h := QueryHandler(ret, amb, cfg, nil)

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, streamQueryRequest(t.Context(), `{"query":"Question?","mode":"global"}`))
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.NotContains(t, rec.Body.String(), "event: context")
			require.Zero(t, double.converseCalls.Load())
			if tc.wantRetry != "" {
				require.Equal(t, tc.wantRetry, rec.Header().Get("Retry-After"))
			}
			var apiErr APIError
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiErr))
			require.Equal(t, tc.wantError, apiErr.Error)
		})
	}

	t.Run("disconnect during recount skips further recounts and synthesis", func(t *testing.T) {
		cfg := handlerBedrockStrictConfig()
		reqCtx, cancel := context.WithCancel(t.Context())
		var double *handlerBudgetConverser
		double = &handlerBudgetConverser{
			region: cfg.AWSRegion,
			countFn: func(context.Context, string, string, []brtypes.Message, int) (llm.CountResult, error) {
				if double.countCalls.Load() == 1 {
					cancel()
					return llm.CountResult{InputTokens: 205_000, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
				}
				return llm.CountResult{InputTokens: 100, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
			},
		}
		amb := llm.NewAmbassador(cfg)
		amb.SetClient(double)
		ret := retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10)
		rec := httptest.NewRecorder()
		QueryHandler(ret, amb, cfg, nil).ServeHTTP(rec, streamQueryRequest(reqCtx, `{"query":"Question?","mode":"global"}`))
		require.Equal(t, int32(1), double.countCalls.Load())
		require.Zero(t, double.converseCalls.Load())
	})
}

func bytesOrRaw(fields map[string]json.RawMessage) []byte {
	b, _ := json.Marshal(fields)
	return b
}

func TestQueryStrictBlankEvidenceSkipsProviders(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			cfg := handlerBedrockStrictConfig()
			provider := &handlerBudgetConverser{region: cfg.AWSRegion}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(provider)
			chunks := []domain.ScoredChunk{
				{Chunk: domain.Chunk{ID: "c1", DocumentID: "00000000-0000-4000-8000-000000000001", Text: "\u200b"}, Score: .9},
				{Chunk: domain.Chunk{ID: "c2", DocumentID: "00000000-0000-4000-8000-000000000002", Text: " \t\u2060"}, Score: .8},
			}
			ret := retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10)
			h := QueryHandler(ret, amb, cfg, nil)
			const body = `{"query":"Q?","mode":"global"}`
			req := streamQueryRequest(t.Context(), body)
			if !stream {
				req.Header.Del("Accept")
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Zero(t, provider.countCalls.Load())
			require.Zero(t, provider.converseCalls.Load())
			if stream {
				events := recordedEvents(t, rec.Body.String())
				require.Len(t, events, 3)
				require.Equal(t, "context", events[0].name)
				require.JSONEq(t, "0", string(events[0].data["context_chunks"]))
				require.Equal(t, "done", events[2].name)
				var done QueryStreamDone
				require.NoError(t, json.Unmarshal(bytesOrRaw(events[2].data), &done))
				require.Equal(t, "no_evidence", done.FinishReason)
				require.Zero(t, done.ModelInputTokens)
				require.Empty(t, done.CountMethod)
				require.True(t, done.EarlyExit)
			} else {
				var out QueryResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.Equal(t, "no_evidence", out.FinishReason)
				require.Zero(t, out.ContextChunks)
				require.Zero(t, out.ModelInputTokens)
				require.Empty(t, out.CountMethod)
				require.True(t, out.EarlyExit)
			}
		})
	}
}
