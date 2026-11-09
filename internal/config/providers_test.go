package config

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func providerEnvironment(t *testing.T) {
	t.Helper()
	awsConfigEnvironment(t)
	t.Setenv("LLM_PROVIDER", "openai")
	t.Setenv("EMBEDDING_PROVIDER", "openai")
	server := httptest.NewServer(nil)
	t.Cleanup(server.Close)
	t.Setenv("OPENAI_BASE_URL", server.URL+"/v1/")
	t.Setenv("OPENAI_CHAT_MODEL", "chat")
	t.Setenv("OPENAI_EMBEDDING_MODEL", "embed")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("S3_BUCKET", "")
	t.Setenv("EMBEDDING_DIMENSIONS", "3")
}

func TestOpenAIProviderConfiguration(t *testing.T) {
	for _, dimensions := range []string{"1", "3", "4096"} {
		t.Run(dimensions, func(t *testing.T) {
			providerEnvironment(t)
			t.Setenv("EMBEDDING_DIMENSIONS", dimensions)
			if _, err := Load(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBedrockMissingVariableOrder(t *testing.T) {
	awsConfigEnvironment(t)
	t.Setenv("LLM_PROVIDER", "")
	t.Setenv("EMBEDDING_PROVIDER", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("PINECONE_API_KEY", "")
	t.Setenv("PINECONE_INDEX_NAME", "")
	_, err := Load()
	const want = "config: missing required environment variables: [AWS_REGION PINECONE_API_KEY PINECONE_INDEX_NAME]"
	if err == nil || err.Error() != want {
		t.Fatalf("expected %q, got %v", want, err)
	}
}

func TestProviderValidation(t *testing.T) {
	for _, tc := range []struct{ key, value, want string }{
		{"LLM_PROVIDER", "unknown", "LLM_PROVIDER"},
		{"EMBEDDING_PROVIDER", "unknown", "EMBEDDING_PROVIDER"},
		{"OPENAI_BASE_URL", "", "OPENAI_BASE_URL"},
		{"OPENAI_BASE_URL", "relative/path", "OPENAI_BASE_URL"},
		{"OPENAI_BASE_URL", "file:/tmp/socket", "OPENAI_BASE_URL"},
		{"OPENAI_CHAT_MODEL", "", "OPENAI_CHAT_MODEL"},
		{"OPENAI_EMBEDDING_MODEL", " ", "OPENAI_EMBEDDING_MODEL"},
		{"OPENAI_API_KEY", "private\nkey", "OPENAI_API_KEY"},
		{"EMBEDDING_DIMENSIONS", "0", "EMBEDDING_DIMENSIONS"},
		{"EMBEDDING_DIMENSIONS", "4097", "EMBEDDING_DIMENSIONS"},
		{"LLM_PROVIDER", "bedrock", "AWS_REGION"},
		{"EMBEDDING_PROVIDER", "bedrock", "AWS_REGION"},
		{"S3_BUCKET", "bucket", "AWS_REGION"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			providerEnvironment(t)
			t.Setenv(tc.key, tc.value)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %s validation error, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("validation exposed a secret")
			}
		})
	}
}

func TestOpenAIRejectsBedrockGuardrails(t *testing.T) {
	providerEnvironment(t)
	t.Setenv("BEDROCK_GUARDRAIL_ID", "guardrail")
	t.Setenv("BEDROCK_GUARDRAIL_VERSION", "1")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "LLM_PROVIDER") {
		t.Fatalf("expected guardrail provider error, got %v", err)
	}
}

func TestMixedProviderConfiguration(t *testing.T) {
	for _, provider := range []string{"chat", "embedding"} {
		t.Run(provider, func(t *testing.T) {
			providerEnvironment(t)
			t.Setenv("AWS_REGION", "us-east-1")
			if provider == "chat" {
				t.Setenv("EMBEDDING_PROVIDER", "bedrock")
				t.Setenv("EMBEDDING_DIMENSIONS", "1024")
				t.Setenv("OPENAI_EMBEDDING_MODEL", "")
			} else {
				t.Setenv("LLM_PROVIDER", "bedrock")
				t.Setenv("OPENAI_CHAT_MODEL", "")
				t.Setenv("BEDROCK_GUARDRAIL_ID", "guardrail")
				t.Setenv("BEDROCK_GUARDRAIL_VERSION", "1")
			}
			if _, err := Load(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenAIBaseAddressValidation(t *testing.T) {
	for _, suffix := range []string{"?key=private", "#private", "?"} {
		t.Run(suffix, func(t *testing.T) {
			providerEnvironment(t)
			server := httptest.NewServer(nil)
			defer server.Close()
			t.Setenv("OPENAI_BASE_URL", server.URL+suffix)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "OPENAI_BASE_URL") || strings.Contains(err.Error(), "private") {
				t.Fatalf("unsafe validation: %v", err)
			}
		})
	}
}
