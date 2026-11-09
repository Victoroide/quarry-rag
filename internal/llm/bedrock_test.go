package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/require"
)

func TestConverseGuardrailConfiguration(t *testing.T) {
	for _, version := range []string{"", "DRAFT", "12"} {
		for _, routed := range []bool{false, true} {
			t.Run(fmt.Sprintf("version=%q/routed=%t", version, routed), func(t *testing.T) {
				t.Setenv("GO_RAG_FAKE_MODE", "true")
				t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
				t.Setenv("AWS_REGION", "us-east-1")
				t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
				t.Setenv("BEDROCK_GUARDRAIL_ID", "")
				if version != "" {
					t.Setenv("BEDROCK_GUARDRAIL_ID", "guardrail-id")
				}
				t.Setenv("BEDROCK_GUARDRAIL_VERSION", version)
				cfg, err := config.Load()
				require.NoError(t, err)
				client, err := NewClient(t.Context(), cfg)
				require.NoError(t, err)
				var request map[string]any
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if decodeErr := json.NewDecoder(r.Body).Decode(&request); decodeErr != nil {
						t.Error(decodeErr)
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"output":{"message":{"role":"assistant","content":[{"text":"answer"}]}},"stopReason":"end_turn","usage":{"inputTokens":1,"outputTokens":1,"totalTokens":2},"metrics":{"latencyMs":1}}`))
				}))
				t.Cleanup(server.Close)
				client.rt = bedrockruntime.NewFromConfig(aws.Config{
					Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
				}, func(o *bedrockruntime.Options) { o.BaseEndpoint = aws.String(server.URL) })
				messages := []brtypes.Message{{Role: brtypes.ConversationRoleUser, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "question"}}}}
				var output *bedrockruntime.ConverseOutput
				if routed {
					output, err = client.ConverseWithModelAndSystem(t.Context(), "selected-model", "system instruction", messages, .2, 1024)
				} else {
					output, err = client.Converse(t.Context(), messages)
				}
				require.NoError(t, err)
				require.Equal(t, "answer", ExtractText(output))
				if version == "" {
					require.NotContains(t, request, "guardrailConfig")
				} else {
					require.Equal(t, map[string]any{"guardrailIdentifier": "guardrail-id", "guardrailVersion": version}, request["guardrailConfig"])
				}
				require.Equal(t, []any{map[string]any{"role": "user", "content": []any{map[string]any{"text": "question"}}}}, request["messages"])
			})
		}
	}
}

func TestFakeConverserReadsGuardContent(t *testing.T) {
	messages := []brtypes.Message{{Role: brtypes.ConversationRoleUser, Content: []brtypes.ContentBlock{
		&brtypes.ContentBlockMemberGuardContent{Value: &brtypes.GuardrailConverseContentBlockMemberText{Value: brtypes.GuardrailConverseTextBlock{Text: aws.String("guarded question"), Qualifiers: []brtypes.GuardrailConverseContentQualifier{brtypes.GuardrailConverseContentQualifierQuery}}}},
		&brtypes.ContentBlockMemberGuardContent{Value: &brtypes.GuardrailConverseContentBlockMemberText{Value: brtypes.GuardrailConverseTextBlock{Text: aws.String("guarded evidence"), Qualifiers: []brtypes.GuardrailConverseContentQualifier{brtypes.GuardrailConverseContentQualifierGroundingSource}}}},
		&brtypes.ContentBlockMemberText{Value: "closing instruction"},
	}}}
	output, err := NewFakeConverser().ConverseWithModelAndSystem(context.Background(), "offline", "", messages, .2, 1024)
	require.NoError(t, err)
	require.Equal(t, "[FAKE MODE]\n\n- guarded question\n- guarded evidence\n- closing instruction", ExtractText(output))
}
