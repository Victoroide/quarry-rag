package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBedrockConverseStreamProtocol(t *testing.T) {
	for _, limit := range []int{0, 80} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/model/selected/converse-stream", r.URL.Path)
				assert.Equal(t, http.MethodPost, r.Method)
				var input struct {
					System []struct {
						Text string `json:"text"`
					} `json:"system"`
					Messages []struct {
						Role string `json:"role"`
					} `json:"messages"`
					InferenceConfig struct {
						MaxTokens   int     `json:"maxTokens"`
						Temperature float32 `json:"temperature"`
						TopP        float32 `json:"topP"`
					} `json:"inferenceConfig"`
				}
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&input))
				assert.Len(t, input.System, 1)
				if len(input.System) == 1 {
					assert.Equal(t, "instructions", input.System[0].Text)
				}
				want := limit
				if want == 0 {
					want = 4096
				}
				assert.Equal(t, want, input.InferenceConfig.MaxTokens)
				assert.InDelta(t, .2, input.InferenceConfig.Temperature, .0001)
				assert.InDelta(t, .9, input.InferenceConfig.TopP, .0001)
				assert.Len(t, input.Messages, 1)
				w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
				encoder := eventstream.NewEncoder()
				for _, event := range []struct{ name, payload string }{
					{"messageStart", `{"role":"assistant"}`},
					{"contentBlockDelta", `{"contentBlockIndex":0,"delta":{"text":"Archive "}}`},
					{"contentBlockDelta", `{"contentBlockIndex":0,"delta":{"text":"evidence"}}`},
					{"messageStop", `{"stopReason":"end_turn"}`},
					{"metadata", `{"usage":{"inputTokens":11,"outputTokens":3,"totalTokens":14},"metrics":{"latencyMs":1}}`},
				} {
					message := eventstream.Message{Payload: []byte(event.payload)}
					message.Headers.Set(":message-type", eventstream.StringValue("event"))
					message.Headers.Set(":event-type", eventstream.StringValue(event.name))
					message.Headers.Set(":content-type", eventstream.StringValue("application/json"))
					assert.NoError(t, encoder.Encode(w, message))
					assert.NoError(t, http.NewResponseController(w).Flush())
				}
			}))
			defer srv.Close()
			rt := bedrockruntime.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("synthetic-key", "synthetic-secret", ""), HTTPClient: srv.Client()}, func(o *bedrockruntime.Options) { o.BaseEndpoint = aws.String(srv.URL) })
			c := &Client{rt: rt, maxToks: DefaultMaxTokens}
			var text strings.Builder
			messages := []brtypes.Message{{Role: brtypes.ConversationRoleUser, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "Archive question"}}}}
			out, err := c.ConverseStreamWithModelAndSystem(t.Context(), "selected", "instructions", messages, .2, limit, func(delta string) error { text.WriteString(delta); return nil })
			require.NoError(t, err)
			require.Equal(t, "Archive evidence", text.String())
			require.Equal(t, text.String(), ExtractText(out))
			require.EqualValues(t, 14, *out.Usage.TotalTokens)
		})
	}
}

func TestBedrockStreamRejectsInvalidCalls(t *testing.T) {
	c := &Client{maxToks: DefaultMaxTokens}
	_, err := c.ConverseStreamWithModelAndSystem(t.Context(), "selected", "", nil, 0, 0, nil)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.ConverseStreamWithModelAndSystem(ctx, "selected", "", nil, 0, 0, func(string) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.ConverseStreamWithModelAndSystem(t.Context(), "selected", "", nil, 0, 1<<32, func(string) error { return nil })
	require.Error(t, err)
}

func TestBedrockConverseStreamGuardrail(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			var request map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
				enc := eventstream.NewEncoder()
				for _, e := range []struct{ name, payload string }{
					{"contentBlockDelta", `{"contentBlockIndex":0,"delta":{"text":"ok"}}`},
					{"messageStop", `{"stopReason":"end_turn"}`},
					{"metadata", `{"usage":{"inputTokens":1,"outputTokens":1,"totalTokens":2},"metrics":{"latencyMs":1}}`},
				} {
					m := eventstream.Message{Payload: []byte(e.payload)}
					m.Headers.Set(":message-type", eventstream.StringValue("event"))
					m.Headers.Set(":event-type", eventstream.StringValue(e.name))
					m.Headers.Set(":content-type", eventstream.StringValue("application/json"))
					assert.NoError(t, enc.Encode(w, m))
				}
			}))
			defer srv.Close()
			rt := bedrockruntime.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("synthetic-key", "synthetic-secret", ""), HTTPClient: srv.Client()}, func(o *bedrockruntime.Options) { o.BaseEndpoint = aws.String(srv.URL) })
			c := &Client{rt: rt, maxToks: DefaultMaxTokens}
			if enabled {
				c.guardrailConfig = &brtypes.GuardrailConfiguration{GuardrailIdentifier: aws.String("guardrail-id"), GuardrailVersion: aws.String("1")}
			}
			_, err := c.ConverseStreamWithModelAndSystem(t.Context(), "selected", "", []brtypes.Message{{Role: brtypes.ConversationRoleUser, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "q"}}}}, .2, 0, func(string) error { return nil })
			require.NoError(t, err)
			if enabled {
				require.Equal(t, map[string]any{"guardrailIdentifier": "guardrail-id", "guardrailVersion": "1", "streamProcessingMode": "sync"}, request["guardrailConfig"])
			} else {
				require.NotContains(t, request, "guardrailConfig")
			}
		})
	}
}

func TestBedrockConverseStreamFailureIncludesModel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(`{"__type":"ValidationException","message":"invalid configuration"}`))
		assert.NoError(t, err)
	}))
	defer srv.Close()
	rt := bedrockruntime.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("synthetic-key", "synthetic-secret", ""), HTTPClient: srv.Client()}, func(o *bedrockruntime.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	c := &Client{rt: rt, maxToks: DefaultMaxTokens}
	_, err := c.ConverseStreamWithModelAndSystem(t.Context(), "selected", "", nil, .2, 0, func(string) error { return nil })
	require.ErrorContains(t, err, "llm: converse stream (model=selected)")
	var validation *brtypes.ValidationException
	require.ErrorAs(t, err, &validation)
}
